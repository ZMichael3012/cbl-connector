package usecase

import (
	"context"
	"errors"
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

// PaymentMapping resolves the platform payment of a provider invoice; the
// provider_invoices repository implements it (17.2, T-CONNECTOR-03).
type PaymentMapping interface {
	// ByGatewayID returns the mapping of a provider invoice.
	ByGatewayID(ctx context.Context, provider, gatewayID string) (domain.ProviderInvoice, error)
}

// PaymentStatusFetcher fetches the authoritative payment status at the
// provider; fetch_required deliveries (CoinGate, BitPay) are never trusted from
// the webhook body alone (CF-4 hardening).
type PaymentStatusFetcher interface {
	// FetchPaymentStatus returns the canonical status of the provider invoice.
	FetchPaymentStatus(ctx context.Context, provider, gatewayID string) (domain.PaymentStatus, error)
}

// NormalizerOption configures the Normalizer.
type NormalizerOption func(*Normalizer)

// WithPaymentMapping sets the provider invoice mapping used to resolve
// payment_id when the delivery does not carry it.
func WithPaymentMapping(mapping PaymentMapping) NormalizerOption {
	return func(n *Normalizer) {
		if mapping != nil {
			n.mapping = mapping
		}
	}
}

// WithPaymentFetcher sets the fetch-to-verify source of fetch_required
// deliveries; without it such deliveries fail closed.
func WithPaymentFetcher(fetcher PaymentStatusFetcher) NormalizerOption {
	return func(n *Normalizer) {
		if fetcher != nil {
			n.fetcher = fetcher
		}
	}
}

// WithFetchMandatory sets the providers whose deliveries always go through the
// fetch-to-verify re-check, regardless of the raw fetch_required flag (the
// manifest mandates of CoinGate and BitPay, CBL-003 section 4); the predicate
// is backed by the manifest registry in main.
func WithFetchMandatory(mandatory func(provider string) bool) NormalizerOption {
	return func(n *Normalizer) {
		if mandatory != nil {
			n.fetchMandatory = mandatory
		}
	}
}

// Normalizer turns raw webhooks into canonical events through the outbox.
// It implements inbox.Handler: it is called inside the guard transaction, so the
// envelope is published atomically with the processing mark.
type Normalizer struct {
	registry       *DecoderRegistry
	mapping        PaymentMapping
	fetcher        PaymentStatusFetcher
	fetchMandatory func(provider string) bool
	store          *outbox.Store
	log            *zap.Logger
}

// NewNormalizer creates a Normalizer on top of the decoder registry.
func NewNormalizer(registry *DecoderRegistry, log *zap.Logger, opts ...NormalizerOption) *Normalizer {
	if log == nil {
		log = zap.NewNop()
	}
	normalizer := &Normalizer{registry: registry, store: outbox.NewStore(), log: log}
	for _, opt := range opts {
		opt(normalizer)
	}
	return normalizer
}

// Handle parses a raw webhook, decodes it and writes the canonical event to the outbox.
// A non-verified webhook, an unknown provider, a missing mapping and broken bodies
// return an error: the inbox Processor sends the message to the retry topic and
// then to the DLQ (fail closed, CF-4).
func (n *Normalizer) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	if len(msg.Value) > maxRawMessageBytes {
		return fmt.Errorf("%w: raw message too large", domain.ErrInvalidWebhook)
	}
	var raw eventsv1.GatewayWebhookRaw
	if err := proto.Unmarshal(msg.Value, &raw); err != nil {
		return fmt.Errorf("%w: unmarshal raw webhook: %w", domain.ErrInvalidWebhook, err)
	}
	provider := strings.ToLower(strings.TrimSpace(raw.GetProvider()))
	if state := verificationState(&raw); state != eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_VERIFIED {
		return fmt.Errorf("%w: provider %q state %s", domain.ErrUnsignedWebhook, provider, state)
	}
	var (
		event domain.CanonicalEvent
		err   error
	)
	// The provider key is normalized before every lookup (decoder registry,
	// mapping, mandate predicate), so a differently-cased raw value cannot
	// skip the fetch mandate of its provider.
	if raw.GetFetchRequired() || n.fetchMandatoryFor(provider) {
		event, err = n.fetchCanonical(ctx, &raw, provider)
	} else {
		event, err = n.decodeCanonical(ctx, &raw, provider)
	}
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

// fetchMandatoryFor reports whether the provider manifest mandates the fetch
// regardless of the raw flag; a nil predicate means "raw flag only".
func (n *Normalizer) fetchMandatoryFor(provider string) bool {
	return n.fetchMandatory != nil && n.fetchMandatory(provider)
}

// verificationState resolves the CF-4 verification result with the transition
// fallback: a producer that has not migrated sets only the deprecated
// signature_verified flag (gateway_events.proto, cf-4).
func verificationState(raw *eventsv1.GatewayWebhookRaw) eventsv1.WebhookSignatureState {
	if state := raw.GetSignatureState(); state != eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_UNSPECIFIED {
		return state
	}
	if raw.GetSignatureVerified() { //nolint:staticcheck // SA1019: transition fallback until all producers set signature_state
		return eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_VERIFIED
	}
	return eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_ABSENT
}

// decodeCanonical decodes the delivery through the provider decoder and
// resolves payment_id through the mapping when the body does not carry it.
func (n *Normalizer) decodeCanonical(ctx context.Context, raw *eventsv1.GatewayWebhookRaw, provider string) (domain.CanonicalEvent, error) {
	decoder, ok := n.registry.Decoder(provider)
	if !ok {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %q", domain.ErrUnknownProvider, provider)
	}
	event, err := decoder.Decode(ctx, raw)
	if err != nil {
		return domain.CanonicalEvent{}, err
	}
	if strings.TrimSpace(event.PaymentID) == "" {
		paymentID, resolveErr := n.resolvePayment(ctx, provider, raw.GetGatewayId())
		if resolveErr != nil {
			return domain.CanonicalEvent{}, resolveErr
		}
		event.PaymentID = paymentID
	}
	return event, nil
}

// fetchCanonical builds the canonical event from the provider status re-check:
// fetch_required deliveries must never be trusted from the body, and without an
// adapter the delivery fails closed (the status model decides, 3.4). The
// platform payment is reconciled with the invoice mapping: a status PaymentID
// that contradicts the mapping is a mismatch and goes to the DLQ.
func (n *Normalizer) fetchCanonical(ctx context.Context, raw *eventsv1.GatewayWebhookRaw, provider string) (domain.CanonicalEvent, error) {
	if n.fetcher == nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: provider %q requires fetch but no adapter is registered", domain.ErrProviderUnavailable, provider)
	}
	gatewayID := strings.TrimSpace(raw.GetGatewayId())
	if gatewayID == "" || gatewayID == "unknown" {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: provider %q fetch requires a gateway id", domain.ErrInvalidWebhook, provider)
	}
	status, err := n.fetcher.FetchPaymentStatus(ctx, provider, gatewayID)
	if err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("usecase: fetch payment status for provider %q: %w", provider, err)
	}
	if status.State == domain.StatePending {
		// A pre-deposit status carries no canonical event yet: retry instead of
		// publishing a fabricated detection (KI-connector-14).
		return domain.CanonicalEvent{}, fmt.Errorf("%w: provider %q reports a pre-deposit status %q", domain.ErrTemporary, provider, status.RawStatus)
	}
	paymentID, err := n.reconcilePayment(ctx, provider, gatewayID, status.PaymentID)
	if err != nil {
		return domain.CanonicalEvent{}, err
	}
	occurredAt := status.GatewayEventTS
	if occurredAt.IsZero() {
		// The receipt time is the ordering anchor: a fabricated "now" would let
		// a retried delivery outrun later states (T-CONNECTOR-05). AsTime on an
		// unset timestamp returns the epoch, so the presence is checked first.
		receipt := raw.GetReceivedAt()
		if receipt == nil {
			return domain.CanonicalEvent{}, fmt.Errorf("%w: provider %q has no event time", domain.ErrInvalidWebhook, provider)
		}
		occurredAt = receipt.AsTime()
	}
	return domain.CanonicalEvent{
		Type:           status.State,
		PaymentID:      paymentID,
		GatewayID:      gatewayID,
		Provider:       provider,
		RawStatus:      status.RawStatus,
		TXID:           status.TXID,
		Network:        status.Network,
		ReceivedAmount: status.ReceivedAmount,
		Confirmations:  status.Confirmations,
		OccurredAt:     occurredAt,
	}, nil
}

// reconcilePayment attributes the fetched status to a platform payment: the
// invoice mapping is authoritative, an adapter-provided id must either match it
// or stand alone when no mapping is configured, and a contradiction fails closed.
// A mapping miss with an adapter-provided id falls through to the adapter id:
// the NOWPayments IPN carries the provider payment id while the mapping stores
// the invoice id, and the fetched order_id echoes the platform payment
// (KI-connector-15).
func (n *Normalizer) reconcilePayment(ctx context.Context, provider, gatewayID, adapterPaymentID string) (string, error) {
	mapped := ""
	if n.mapping != nil {
		resolved, err := n.resolvePayment(ctx, provider, gatewayID)
		switch {
		case err == nil:
			mapped = resolved
		case errors.Is(err, domain.ErrProviderInvoiceNotFound):
			// No mapping for this gateway id: the adapter evidence decides.
		default:
			return "", err
		}
	}
	adapterPaymentID = strings.TrimSpace(adapterPaymentID)
	switch {
	case mapped != "" && adapterPaymentID != "" && mapped != adapterPaymentID:
		return "", fmt.Errorf("%w: provider %q status payment contradicts the mapping", domain.ErrInvalidWebhook, provider)
	case mapped != "":
		return mapped, nil
	case adapterPaymentID != "":
		return adapterPaymentID, nil
	default:
		return "", fmt.Errorf("%w: provider %q status carries no payment and no mapping is configured", domain.ErrInvalidWebhook, provider)
	}
}

// resolvePayment resolves the platform payment through the provider invoice
// mapping: real provider webhooks do not carry our payment_id.
func (n *Normalizer) resolvePayment(ctx context.Context, provider, gatewayID string) (string, error) {
	if n.mapping == nil {
		return "", fmt.Errorf("%w: provider %q has no payment mapping", domain.ErrInvalidWebhook, provider)
	}
	gatewayID = strings.TrimSpace(gatewayID)
	if gatewayID == "" || gatewayID == "unknown" {
		return "", fmt.Errorf("%w: provider %q has no gateway id for the mapping", domain.ErrInvalidWebhook, provider)
	}
	invoice, err := n.mapping.ByGatewayID(ctx, provider, gatewayID)
	if err != nil {
		return "", fmt.Errorf("usecase: resolve payment of provider %q: %w", provider, err)
	}
	return invoice.PaymentID, nil
}

// buildCanonicalEnvelope builds the canonical event envelope: the partition key is
// payment_id (CBL-003), occurred_at is the provider event time for monotonicity.
func buildCanonicalEnvelope(event domain.CanonicalEvent, incoming map[string]string) (outbox.Envelope, error) {
	payload, err := proto.Marshal(&eventsv1.CanonicalPaymentEvent{
		PaymentId: event.PaymentID,
		GatewayId: event.GatewayID,
		Provider:  event.Provider,
		Type:      canonicalEventType(event.Type),
		RawStatus: event.RawStatus,
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
