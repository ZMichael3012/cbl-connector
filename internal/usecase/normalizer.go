package usecase

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-connector/internal/domain"
)

// Topics and identifiers of the normalization pipeline.
const (
	// TopicGatewayWebhooksRaw specifies the topic of raw provider webhooks.
	TopicGatewayWebhooksRaw = "gateway.webhooks.raw"
	// TopicGatewayEventsCanonical specifies the topic of canonical gateway events.
	TopicGatewayEventsCanonical = "gateway.events.canonical"
	// ProducerConnector specifies the producer name in the event envelope.
	ProducerConnector = "cbl-connector"
	// schemaVersion specifies the schema version of the canonical event.
	schemaVersion = "v1"
	// maxRawMessageBytes limits the size of the incoming envelope: untrusted
	// input must not inflate retry/DLQ messages or block the partition.
	maxRawMessageBytes = 512 << 10
)

// traceparentPattern validates the W3C trace context format before propagation.
var traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// Normalizer turns raw webhooks into canonical events through the outbox.
// It implements inbox.Handler: it is called inside the guard transaction, so the
// envelope is published atomically with the processing mark.
type Normalizer struct {
	registry *DecoderRegistry
	store    *outbox.Store
	log      *zap.Logger
}

// NewNormalizer creates a Normalizer on top of the decoder registry.
func NewNormalizer(registry *DecoderRegistry, log *zap.Logger) *Normalizer {
	if log == nil {
		log = zap.NewNop()
	}
	return &Normalizer{registry: registry, store: outbox.NewStore(), log: log}
}

// Handle parses a raw webhook, decodes it and writes the canonical event to the outbox.
// An unsigned webhook, an unknown provider and broken bodies return an error:
// the inbox Processor sends the message to the retry topic and then to the DLQ.
func (n *Normalizer) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	if len(msg.Value) > maxRawMessageBytes {
		return fmt.Errorf("%w: raw message too large", domain.ErrInvalidWebhook)
	}
	var raw eventsv1.GatewayWebhookRaw
	if err := proto.Unmarshal(msg.Value, &raw); err != nil {
		return fmt.Errorf("%w: unmarshal raw webhook: %w", domain.ErrInvalidWebhook, err)
	}
	provider := strings.TrimSpace(raw.GetProvider())
	if !raw.GetSignatureVerified() {
		return fmt.Errorf("%w: provider %q", domain.ErrUnsignedWebhook, provider)
	}
	decoder, ok := n.registry.Decoder(provider)
	if !ok {
		return fmt.Errorf("%w: %q", domain.ErrUnknownProvider, provider)
	}
	event, err := decoder.Decode(ctx, &raw)
	if err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		return err
	}
	envelope, err := buildCanonicalEnvelope(event, msg.Headers)
	if err != nil {
		return err
	}
	if err := n.store.Append(ctx, tx, envelope); err != nil {
		return fmt.Errorf("usecase: append canonical event: %w", err)
	}
	n.log.Info("canonical event queued in outbox",
		zap.String("payment_id", event.PaymentID),
		zap.String("gateway_id", event.GatewayID),
		zap.String("provider", event.Provider),
		zap.String("type", string(event.Type)),
	)
	return nil
}

// buildCanonicalEnvelope builds the canonical event envelope: the partition key is
// payment_id (CBL-003), occurred_at is the provider event time for monotonicity.
func buildCanonicalEnvelope(event domain.CanonicalEvent, incoming map[string]string) (outbox.Envelope, error) {
	payload, err := proto.Marshal(&eventsv1.CanonicalPaymentEvent{
		PaymentId: event.PaymentID,
		GatewayId: event.GatewayID,
		Provider:  event.Provider,
		Type:      canonicalEventType(event.Type),
		ReceivedAmount: &commonv1.Money{
			MinorUnits: event.ReceivedAmount.MinorUnits,
			Asset:      event.ReceivedAmount.Asset,
			Decimals:   event.ReceivedAmount.Decimals,
		},
		Confirmations:  event.Confirmations,
		Txid:           event.TXID,
		Network:        event.Network,
		GatewayEventTs: timestamppb.New(event.OccurredAt),
	})
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: marshal canonical event: %w", err)
	}
	headers := map[string]string{
		outbox.HeaderProducer:      ProducerConnector,
		outbox.HeaderSchemaVersion: schemaVersion,
		outbox.HeaderOccurredAt:    event.OccurredAt.UTC().Format(time.RFC3339Nano),
		"provider":                 event.Provider,
		"gateway_id":               event.GatewayID,
	}
	if traceparent := strings.TrimSpace(incoming[outbox.HeaderTraceparent]); traceparentPattern.MatchString(traceparent) {
		headers[outbox.HeaderTraceparent] = traceparent
	}
	envelope, err := outbox.NewEnvelope("gateway.payment."+string(event.Type),
		TopicGatewayEventsCanonical, event.PaymentID, headers, payload)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: build envelope: %w", err)
	}
	return envelope, nil
}

// canonicalEventType maps the domain state to the proto enum;
// unknown values must not reach this point after Validate.
func canonicalEventType(state domain.State) eventsv1.CanonicalPaymentEventType {
	switch state {
	case domain.StateDetected:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED
	case domain.StateConfirmed:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED
	case domain.StateFailed:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED
	case domain.StateExpired:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_EXPIRED
	case domain.StateUnderpaid:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID
	case domain.StateOverpaid:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID
	default:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNSPECIFIED
	}
}
