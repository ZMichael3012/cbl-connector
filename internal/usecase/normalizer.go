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

// Топики и идентификаторы конвейера нормализации.
const (
	// TopicGatewayWebhooksRaw задает топик сырых вебхуков провайдера.
	TopicGatewayWebhooksRaw = "gateway.webhooks.raw"
	// TopicGatewayEventsCanonical задает топик канонических событий шлюза.
	TopicGatewayEventsCanonical = "gateway.events.canonical"
	// ProducerConnector задает имя продюсера в конверте событий.
	ProducerConnector = "cbl-connector"
	// schemaVersion задает версию схемы канонического события.
	schemaVersion = "v1"
	// maxRawMessageBytes ограничивает размер входящего конверта: недоверенный
	// вход не должен раздувать retry/DLQ-сообщения и блокировать партицию.
	maxRawMessageBytes = 512 << 10
)

// traceparentPattern проверяет формат W3C trace context перед пробросом.
var traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// Normalizer нормализует сырые вебхуки в канонические события через outbox.
// Реализует inbox.Handler: вызывается в транзакции guard, поэтому конверт
// публикуется атомарно с отметкой обработки.
type Normalizer struct {
	registry *DecoderRegistry
	store    *outbox.Store
	log      *zap.Logger
}

// NewNormalizer создает Normalizer поверх реестра декодеров.
func NewNormalizer(registry *DecoderRegistry, log *zap.Logger) *Normalizer {
	if log == nil {
		log = zap.NewNop()
	}
	return &Normalizer{registry: registry, store: outbox.NewStore(), log: log}
}

// Handle разбирает сырой вебхук, декодирует его и пишет каноническое событие в outbox.
// Неподписанный вебхук, неизвестный провайдер и битые тела возвращают ошибку:
// inbox Processor отправит сообщение в retry-топик, затем в DLQ.
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

// buildCanonicalEnvelope собирает конверт канонического события: ключ партиции
// payment_id (CBL-003), occurred_at - время события у провайдера для монотонности.
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

// canonicalEventType переводит доменный статус в enum proto;
// неизвестные значения не должны доходить сюда после Validate.
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
