package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// sandboxWebhook описывает JSON-тело вебхука sandbox-провайдера (контракт CBL-3.5).
type sandboxWebhook struct {
	// PaymentID задает платеж платформы.
	PaymentID string `json:"payment_id"`
	// GatewayID задает идентификатор счета у провайдера.
	GatewayID string `json:"gateway_id"`
	// Status задает провайдерский статус из таблицы псевдонимов 3.1.
	Status string `json:"status"`
	// ReceivedAmount задает полученную сумму.
	ReceivedAmount *sandboxAmount `json:"received_amount"`
	// Confirmations задает глубину подтверждений.
	Confirmations uint32 `json:"confirmations"`
	// TXID задает хеш транзакции.
	TXID string `json:"txid"`
	// Network задает сеть оплаты.
	Network string `json:"network"`
	// EventTS задает время события у провайдера в RFC 3339.
	EventTS string `json:"event_ts"`
}

// sandboxAmount описывает сумму в минимальных единицах.
type sandboxAmount struct {
	// MinorUnits задает сумму в минимальных единицах актива.
	MinorUnits int64 `json:"minor_units"`
	// Asset задает код актива.
	Asset string `json:"asset"`
	// Decimals задает количество знаков после точки.
	Decimals uint32 `json:"decimals"`
}

// SandboxDecoder декодирует вебхуки sandbox-провайдера (mock-gateway, 3.6)
// до выбора реального провайдера (KI-01).
type SandboxDecoder struct {
	provider string
}

// NewSandboxDecoder создает декодер для провайдера; имя сохраняется в нижнем регистре.
func NewSandboxDecoder(provider string) (*SandboxDecoder, error) {
	trimmed := strings.ToLower(strings.TrimSpace(provider))
	if trimmed == "" {
		return nil, errors.New("usecase: empty sandbox provider")
	}
	return &SandboxDecoder{provider: trimmed}, nil
}

// Provider возвращает имя провайдера декодера.
func (d *SandboxDecoder) Provider() string { return d.provider }

// Ограничения недоверенного входа sandbox-вебхука: превышение отсекается
// до парсинга, чтобы сообщения retry/DLQ и логи оставались ограниченными.
const (
	maxWebhookBodyBytes = 256 << 10
	maxStatusLen        = 32
	maxEventTSLen       = 64
)

// Decode разбирает JSON-тело sandbox-вебхука; неизвестные поля игнорируются
// для обратной совместимости при добавлении полей провайдером.
func (d *SandboxDecoder) Decode(_ context.Context, raw *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error) {
	if raw == nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: nil webhook", domain.ErrInvalidWebhook)
	}
	if len(raw.GetRawBody()) > maxWebhookBodyBytes {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: webhook body too large", domain.ErrInvalidWebhook)
	}
	var payload sandboxWebhook
	if err := json.Unmarshal(raw.GetRawBody(), &payload); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: decode json: %w", domain.ErrInvalidWebhook, err)
	}
	paymentID := strings.TrimSpace(payload.PaymentID)
	if paymentID == "" {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: empty payment_id", domain.ErrInvalidWebhook)
	}
	gatewayID := strings.TrimSpace(payload.GatewayID)
	if gatewayID == "" {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: empty gateway_id", domain.ErrInvalidWebhook)
	}
	status := strings.TrimSpace(payload.Status)
	if status == "" || len(status) > maxStatusLen {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: invalid status", domain.ErrInvalidWebhook)
	}
	state, err := domain.CanonicalState(status)
	if err != nil {
		return domain.CanonicalEvent{}, err
	}
	if payload.ReceivedAmount == nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: missing received_amount", domain.ErrInvalidWebhook)
	}
	amount, err := money.New(payload.ReceivedAmount.MinorUnits, payload.ReceivedAmount.Asset, payload.ReceivedAmount.Decimals)
	if err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: received_amount: %w", domain.ErrInvalidWebhook, err)
	}
	if amount.MinorUnits < 0 {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: negative received_amount", domain.ErrInvalidWebhook)
	}
	eventTS := strings.TrimSpace(payload.EventTS)
	if eventTS == "" || len(eventTS) > maxEventTSLen {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: invalid event_ts", domain.ErrInvalidWebhook)
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, eventTS)
	if err != nil {
		// Текст ошибки time.Parse содержит входное значение: наружу идет общая ошибка.
		return domain.CanonicalEvent{}, fmt.Errorf("%w: invalid event_ts", domain.ErrInvalidWebhook)
	}
	return domain.CanonicalEvent{
		Type:           state,
		PaymentID:      paymentID,
		GatewayID:      gatewayID,
		Provider:       d.provider,
		TXID:           strings.TrimSpace(payload.TXID),
		Network:        strings.TrimSpace(payload.Network),
		ReceivedAmount: amount,
		Confirmations:  payload.Confirmations,
		OccurredAt:     occurredAt.UTC(),
	}, nil
}
