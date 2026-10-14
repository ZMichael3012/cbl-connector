// Пакет domain содержит модель внешнего крипто-шлюза и каноническую модель
// статусов и событий, к которой приводятся все адаптеры провайдеров.
package domain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cbl/cbl-kit/money"
)

// State описывает канонический статус платежа у провайдера.
type State string

// Канонические статусы платежа.
const (
	// StateDetected описывает обнаружение оплаты.
	StateDetected State = "detected"
	// StateConfirmed описывает финальное подтверждение оплаты.
	StateConfirmed State = "confirmed"
	// StateFailed описывает неуспешную оплату.
	StateFailed State = "failed"
	// StateExpired описывает истечение счета.
	StateExpired State = "expired"
	// StateUnderpaid описывает недоплату.
	StateUnderpaid State = "underpaid"
	// StateOverpaid описывает переплату.
	StateOverpaid State = "overpaid"
)

// PayoutState описывает канонический статус выплаты у провайдера.
type PayoutState string

// Канонические статусы выплаты.
const (
	// PayoutSubmitted описывает отправку выплаты провайдеру.
	PayoutSubmitted PayoutState = "submitted"
	// PayoutConfirmed описывает подтверждение выплаты.
	PayoutConfirmed PayoutState = "confirmed"
	// PayoutFailed описывает отказ выплаты.
	PayoutFailed PayoutState = "failed"
)

// paymentStateAliases сопоставляет провайдерские строки каноническим статусам платежа.
var paymentStateAliases = map[string]State{
	"detected":  StateDetected,
	"pending":   StateDetected,
	"seen":      StateDetected,
	"confirmed": StateConfirmed,
	"completed": StateConfirmed,
	"success":   StateConfirmed,
	"failed":    StateFailed,
	"error":     StateFailed,
	"expired":   StateExpired,
	"underpaid": StateUnderpaid,
	"overpaid":  StateOverpaid,
}

// payoutStateAliases сопоставляет провайдерские строки каноническим статусам выплаты.
var payoutStateAliases = map[string]PayoutState{
	"submitted": PayoutSubmitted,
	"pending":   PayoutSubmitted,
	"sent":      PayoutSubmitted,
	"confirmed": PayoutConfirmed,
	"completed": PayoutConfirmed,
	"failed":    PayoutFailed,
	"rejected":  PayoutFailed,
}

// CanonicalState переводит строку провайдера в канонический статус платежа.
func CanonicalState(raw string) (State, error) {
	state, ok := paymentStateAliases[strings.ToLower(strings.TrimSpace(raw))]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownState, raw)
	}
	return state, nil
}

// CanonicalPayoutState переводит строку провайдера в канонический статус выплаты.
func CanonicalPayoutState(raw string) (PayoutState, error) {
	state, ok := payoutStateAliases[strings.ToLower(strings.TrimSpace(raw))]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownState, raw)
	}
	return state, nil
}

// Invoice описывает счет, созданный во внешнем шлюзе.
type Invoice struct {
	// GatewayID задает идентификатор счета у провайдера.
	GatewayID string
	// Provider задает имя провайдера.
	Provider string
	// Address задает адрес оплаты.
	Address string
	// Network задает сеть оплаты.
	Network string
	// Amount задает ожидаемую сумму.
	Amount money.Money
	// ExpiresAt задает срок действия счета.
	ExpiresAt time.Time
}

// CreateInvoiceRequest описывает запрос создания счета.
type CreateInvoiceRequest struct {
	// IdempotencyKey задает ключ идемпотентности вызова.
	IdempotencyKey string
	// PaymentID задает платеж платформы.
	PaymentID string
	// Amount задает сумму счета.
	Amount money.Money
	// Network задает сеть оплаты.
	Network string
	// ExpiresAt задает срок действия счета.
	ExpiresAt time.Time
}

// PaymentStatus описывает канонический статус платежа у провайдера.
type PaymentStatus struct {
	// GatewayID задает идентификатор платежа у провайдера.
	GatewayID string
	// PaymentID задает платеж платформы.
	PaymentID string
	// Provider задает имя провайдера.
	Provider string
	// State задает канонический статус.
	State State
	// ReceivedAmount задает полученную сумму.
	ReceivedAmount money.Money
	// Confirmations задает глубину подтверждений.
	Confirmations uint32
	// TXID задает хеш транзакции.
	TXID string
	// Network задает сеть.
	Network string
	// GatewayEventTS задает время события у провайдера.
	GatewayEventTS time.Time
}

// CanonicalEvent описывает нормализованное событие провайдера.
type CanonicalEvent struct {
	// Type задает канонический тип события.
	Type State
	// PaymentID задает платеж платформы.
	PaymentID string
	// GatewayID задает идентификатор у провайдера.
	GatewayID string
	// Provider задает имя провайдера.
	Provider string
	// TXID задает хеш транзакции.
	TXID string
	// Network задает сеть.
	Network string
	// ReceivedAmount задает полученную сумму.
	ReceivedAmount money.Money
	// Confirmations задает глубину подтверждений.
	Confirmations uint32
	// OccurredAt задает время события.
	OccurredAt time.Time
}

// Ограничения полей канонического события: недоверенный вход не должен
// раздувать ключи Kafka, конверт outbox и сообщения retry/DLQ (review 3.5).
const (
	maxGatewayIDLen = 128
	maxTXIDLen      = 256
	maxNetworkLen   = 64
	maxProviderLen  = 64
	minAssetLen     = 2
	maxAssetLen     = 16
)

// Validate проверяет обязательные поля канонического события перед публикацией.
func (e CanonicalEvent) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(e.PaymentID)); err != nil {
		return fmt.Errorf("%w: payment_id must be uuid", ErrInvalidWebhook)
	}
	if !validPrintable(e.GatewayID, maxGatewayIDLen) {
		return fmt.Errorf("%w: invalid gateway_id", ErrInvalidWebhook)
	}
	if !validPrintable(e.Provider, maxProviderLen) {
		return fmt.Errorf("%w: invalid provider", ErrInvalidWebhook)
	}
	switch e.Type {
	case StateDetected, StateConfirmed, StateFailed, StateExpired, StateUnderpaid, StateOverpaid:
	default:
		return fmt.Errorf("%w: unknown event type %q", ErrInvalidWebhook, e.Type)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: zero occurred_at", ErrInvalidWebhook)
	}
	if e.TXID != "" && !validPrintable(e.TXID, maxTXIDLen) {
		return fmt.Errorf("%w: invalid txid", ErrInvalidWebhook)
	}
	if e.Network != "" && !validCode(e.Network, maxNetworkLen) {
		return fmt.Errorf("%w: invalid network", ErrInvalidWebhook)
	}
	if !validCode(e.ReceivedAmount.Asset, maxAssetLen) || len(e.ReceivedAmount.Asset) < minAssetLen {
		return fmt.Errorf("%w: invalid received_amount asset", ErrInvalidWebhook)
	}
	if e.ReceivedAmount.Decimals > money.MaxDecimals {
		return fmt.Errorf("%w: decimals %d over limit", ErrInvalidWebhook, e.ReceivedAmount.Decimals)
	}
	if e.ReceivedAmount.MinorUnits < 0 {
		return fmt.Errorf("%w: negative received_amount", ErrInvalidWebhook)
	}
	return nil
}

// validPrintable проверяет непустое значение с видимыми ASCII-символами в пределах длины.
func validPrintable(value string, maxLen int) bool {
	if value == "" || len(value) > maxLen {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

// validCode проверяет код (актив, сеть): латиница, цифры, дефис и подчеркивание.
func validCode(value string, maxLen int) bool {
	if value == "" || len(value) > maxLen {
		return false
	}
	for i := 0; i < len(value); i++ {
		b := value[i]
		switch {
		case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9', b == '-', b == '_':
		default:
			return false
		}
	}
	return true
}

// PayoutRequest описывает запрос выплаты провайдеру.
type PayoutRequest struct {
	// IdempotencyKey задает ключ идемпотентности вызова.
	IdempotencyKey string
	// PayoutID задает выплату платформы.
	PayoutID string
	// Amount задает сумму выплаты.
	Amount money.Money
	// Network задает сеть выплаты.
	Network string
	// Address задает адрес получателя.
	Address string
}

// Payout описывает выплату, созданную у провайдера.
type Payout struct {
	// GatewayPayoutID задает идентификатор выплаты у провайдера.
	GatewayPayoutID string
	// Provider задает имя провайдера.
	Provider string
	// State задает канонический статус.
	State PayoutState
}

// PayoutStatus описывает канонический статус выплаты.
type PayoutStatus struct {
	// State задает канонический статус.
	State PayoutState
	// TXID задает хеш транзакции.
	TXID string
	// Fee задает комиссию провайдера.
	Fee money.Money
	// Reason задает причину отказа.
	Reason string
}

// NetworkCapability описывает возможности провайдера по сети.
type NetworkCapability struct {
	// Asset задает поддерживаемый актив.
	Asset string
	// MinAmount задает минимальную сумму.
	MinAmount money.Money
	// ConfirmationDepth задает глубину подтверждений.
	ConfirmationDepth uint32
}

// Capabilities описывает возможности провайдера.
type Capabilities struct {
	// Provider задает имя провайдера.
	Provider string
	// Networks задает возможности по сетям.
	Networks map[string]NetworkCapability
}

// PaymentGateway описывает порт внешнего крипто-шлюза.
type PaymentGateway interface {
	// CreateInvoice создает счет во внешнем шлюзе.
	CreateInvoice(ctx context.Context, req CreateInvoiceRequest) (Invoice, error)
	// GetPayment возвращает канонический статус платежа.
	GetPayment(ctx context.Context, gatewayID string) (PaymentStatus, error)
	// VerifyWebhook проверяет подпись вебхука по сырому телу и разбирает событие.
	VerifyWebhook(headers map[string][]string, rawBody []byte) (CanonicalEvent, error)
	// CreatePayout создает выплату во внешнем шлюзе.
	CreatePayout(ctx context.Context, req PayoutRequest) (Payout, error)
	// GetPayout возвращает канонический статус выплаты.
	GetPayout(ctx context.Context, gatewayPayoutID string) (PayoutStatus, error)
	// Capabilities возвращает возможности провайдера.
	Capabilities() Capabilities
}

// RouteRequest описывает запрос выбора провайдера.
type RouteRequest struct {
	// Network задает сеть операции.
	Network string
	// Asset задает актив операции.
	Asset string
	// Amount задает сумму операции.
	Amount money.Money
	// Operation задает тип операции (invoice, payout).
	Operation string
}

// Validate проверяет обязательные поля запроса маршрутизации.
func (r RouteRequest) Validate() error {
	if strings.TrimSpace(r.Network) == "" {
		return fmt.Errorf("%w: empty network", ErrInvalidRoute)
	}
	if strings.TrimSpace(r.Asset) == "" {
		return fmt.Errorf("%w: empty asset", ErrInvalidRoute)
	}
	if strings.TrimSpace(r.Operation) == "" {
		return fmt.Errorf("%w: empty operation", ErrInvalidRoute)
	}
	return nil
}

// Router выбирает провайдера по правилам маршрутизации.
type Router interface {
	// Select возвращает шлюз для запроса.
	Select(ctx context.Context, req RouteRequest) (PaymentGateway, error)
}
