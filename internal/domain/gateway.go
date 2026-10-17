// Package domain contains the external crypto gateway model and the canonical
// model of statuses and events that all provider adapters normalize to.
package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cbl/cbl-kit/money"
)

// State describes the canonical payment status at the provider.
type State string

// Canonical payment statuses.
const (
	// StateDetected describes detection of a payment.
	StateDetected State = "detected"
	// StateConfirmed describes final confirmation of a payment.
	StateConfirmed State = "confirmed"
	// StateFailed describes an unsuccessful payment.
	StateFailed State = "failed"
	// StateExpired describes invoice expiration.
	StateExpired State = "expired"
	// StateUnderpaid describes an underpayment.
	StateUnderpaid State = "underpaid"
	// StateOverpaid describes an overpayment.
	StateOverpaid State = "overpaid"
)

// PayoutState describes the canonical payout status at the provider.
type PayoutState string

// Canonical payout statuses.
const (
	// PayoutSubmitted describes submission of a payout to the provider.
	PayoutSubmitted PayoutState = "submitted"
	// PayoutConfirmed describes payout confirmation.
	PayoutConfirmed PayoutState = "confirmed"
	// PayoutFailed describes payout failure.
	PayoutFailed PayoutState = "failed"
)

// paymentStateAliases maps provider strings to canonical payment statuses.
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

// payoutStateAliases maps provider strings to canonical payout statuses.
var payoutStateAliases = map[string]PayoutState{
	"submitted": PayoutSubmitted,
	"pending":   PayoutSubmitted,
	"sent":      PayoutSubmitted,
	"confirmed": PayoutConfirmed,
	"completed": PayoutConfirmed,
	"failed":    PayoutFailed,
	"rejected":  PayoutFailed,
}

// CanonicalState converts a provider string into a canonical payment status.
func CanonicalState(raw string) (State, error) {
	state, ok := paymentStateAliases[strings.ToLower(strings.TrimSpace(raw))]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownState, raw)
	}
	return state, nil
}

// CanonicalPayoutState converts a provider string into a canonical payout status.
func CanonicalPayoutState(raw string) (PayoutState, error) {
	state, ok := payoutStateAliases[strings.ToLower(strings.TrimSpace(raw))]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownState, raw)
	}
	return state, nil
}

// Invoice describes an invoice created in the external gateway.
type Invoice struct {
	// GatewayID sets the invoice identifier at the provider.
	GatewayID string
	// Provider sets the provider name.
	Provider string
	// Address sets the payment address.
	Address string
	// Network sets the payment network.
	Network string
	// Amount sets the expected amount.
	Amount money.Money
	// ExpiresAt sets the invoice expiration time.
	ExpiresAt time.Time
}

// CreateInvoiceRequest describes an invoice creation request.
type CreateInvoiceRequest struct {
	// IdempotencyKey sets the call idempotency key.
	IdempotencyKey string
	// PaymentID sets the platform payment.
	PaymentID string
	// Amount sets the invoice amount.
	Amount money.Money
	// Network sets the payment network.
	Network string
	// ExpiresAt sets the invoice expiration time.
	ExpiresAt time.Time
}

// PaymentStatus describes the canonical payment status at the provider.
type PaymentStatus struct {
	// GatewayID sets the payment identifier at the provider.
	GatewayID string
	// PaymentID sets the platform payment.
	PaymentID string
	// Provider sets the provider name.
	Provider string
	// State sets the canonical status.
	State State
	// RawStatus keeps the provider status string untouched (CF-4 raw_status).
	RawStatus string
	// ReceivedAmount sets the received amount.
	ReceivedAmount money.Money
	// Confirmations sets the confirmation depth.
	Confirmations uint32
	// TXID sets the transaction hash.
	TXID string
	// Network sets the network.
	Network string
	// GatewayEventTS sets the event time at the provider.
	GatewayEventTS time.Time
}

// CanonicalEvent describes a normalized provider event.
type CanonicalEvent struct {
	// Type sets the canonical event type.
	Type State
	// PaymentID sets the platform payment.
	PaymentID string
	// GatewayID sets the identifier at the provider.
	GatewayID string
	// Provider sets the provider name.
	Provider string
	// RawStatus keeps the provider status string untouched (CF-4 raw_status)
	// for reconciliation, reporting and support.
	RawStatus string
	// TXID sets the transaction hash.
	TXID string
	// Network sets the network.
	Network string
	// ReceivedAmount sets the received amount.
	ReceivedAmount money.Money
	// Confirmations sets the confirmation depth.
	Confirmations uint32
	// OccurredAt sets the event time.
	OccurredAt time.Time
}

// Canonical event field limits: untrusted input must not inflate Kafka keys,
// the outbox envelope, or retry/DLQ messages (review 3.5).
const (
	maxGatewayIDLen = 128
	maxTXIDLen      = 256
	maxNetworkLen   = 64
	maxProviderLen  = 64
	maxRawStatusLen = 64
	minAssetLen     = 2
	maxAssetLen     = 16
)

// Validate checks the mandatory fields of the canonical event before publication.
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
	if e.RawStatus != "" && !validPrintable(e.RawStatus, maxRawStatusLen) {
		return fmt.Errorf("%w: invalid raw_status", ErrInvalidWebhook)
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

// validPrintable checks a non-empty value with printable ASCII characters within the length limit.
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

// validCode checks a code (asset, network): Latin letters, digits, hyphen, and underscore.
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

// PayoutRequest describes a payout request to the provider.
type PayoutRequest struct {
	// IdempotencyKey sets the call idempotency key.
	IdempotencyKey string
	// PayoutID sets the platform payout.
	PayoutID string
	// Amount sets the payout amount.
	Amount money.Money
	// Network sets the payout network.
	Network string
	// Address sets the recipient address.
	Address string
}

// Payout describes a payout created at the provider.
type Payout struct {
	// GatewayPayoutID sets the payout identifier at the provider.
	GatewayPayoutID string
	// Provider sets the provider name.
	Provider string
	// State sets the canonical status.
	State PayoutState
}

// PayoutStatus describes the canonical payout status.
type PayoutStatus struct {
	// State sets the canonical status.
	State PayoutState
	// TXID sets the transaction hash.
	TXID string
	// Fee sets the provider fee.
	Fee money.Money
	// Reason sets the failure reason.
	Reason string
}

// NetworkCapability describes the provider capabilities for a network.
type NetworkCapability struct {
	// Asset sets the supported asset.
	Asset string
	// MinAmount sets the minimum amount.
	MinAmount money.Money
	// MaxAmount sets the maximum amount (0 means no limit).
	MaxAmount money.Money
	// ConfirmationDepth sets the confirmation depth.
	ConfirmationDepth uint32
}

// Capabilities describes the provider capabilities.
type Capabilities struct {
	// Provider sets the provider name.
	Provider string
	// Networks sets the capabilities per network.
	Networks map[string]NetworkCapability
}

// PaymentGateway describes the external crypto gateway port (SPI v1 core).
// Adapters implement it plus the optional SPI v1 interfaces they support
// (LookupGateway, Refunder); the core depends only on these interfaces and the
// provider manifest, so adding a provider does not change the core (17.2).
type PaymentGateway interface {
	// CreateInvoice creates an invoice in the external gateway.
	CreateInvoice(ctx context.Context, req CreateInvoiceRequest) (Invoice, error)
	// GetPayment returns the canonical payment status.
	GetPayment(ctx context.Context, gatewayID string) (PaymentStatus, error)
	// VerifyWebhook verifies the webhook signature over the raw body and parses the event.
	VerifyWebhook(headers map[string][]string, rawBody []byte) (CanonicalEvent, error)
	// CreatePayout creates a payout in the external gateway.
	CreatePayout(ctx context.Context, req PayoutRequest) (Payout, error)
	// GetPayout returns the canonical payout status.
	GetPayout(ctx context.Context, gatewayPayoutID string) (PayoutStatus, error)
	// Capabilities returns the provider capabilities.
	Capabilities() Capabilities
}

// LookupGateway is the optional SPI v1 interface for invoice recovery after a
// timeout (17.2): a lookup by our order id (the platform payment id) prevents a
// second invoice when a create call ends without a response.
type LookupGateway interface {
	// LookupByOrderID returns the canonical status of the invoice created for
	// the order; ErrNotFound means no invoice exists for the order.
	LookupByOrderID(ctx context.Context, orderID string) (PaymentStatus, error)
}

// RefundRequest describes a refund request to the provider.
type RefundRequest struct {
	// IdempotencyKey sets the call idempotency key.
	IdempotencyKey string
	// RefundInvoiceID sets the platform refund reference.
	RefundInvoiceID string
	// PaymentID sets the platform payment being refunded.
	PaymentID string
	// GatewayID sets the provider invoice identifier.
	GatewayID string
	// Amount sets the refund amount.
	Amount money.Money
	// Address sets an optional destination address for the refund.
	Address string
}

// Refund describes a refund created at the provider.
type Refund struct {
	// GatewayRefundID sets the refund identifier at the provider.
	GatewayRefundID string
	// Provider sets the provider name.
	Provider string
	// Amount sets the refunded amount.
	Amount money.Money
}

// Refunder is the optional SPI v1 interface of providers with a refunds API;
// providers without it (NOWPayments) go through the manual path (decision 8).
type Refunder interface {
	// CreateRefund creates a refund in the external gateway.
	CreateRefund(ctx context.Context, req RefundRequest) (Refund, error)
}

// ProviderInvoice maps a platform payment to the provider invoice: real
// webhooks do not carry our payment_id, so the mapping resolves it by
// provider and gateway_id (T-CONNECTOR-03, 17.2). order_id is the platform
// order reference sent to the provider (the payment UUID).
type ProviderInvoice struct {
	// Provider sets the provider name.
	Provider string
	// GatewayID sets the invoice identifier at the provider.
	GatewayID string
	// PaymentID sets the platform payment UUID.
	PaymentID string
	// OrderID sets the order reference sent to the provider.
	OrderID string
	// RequestHash sets the hash of the creation request for conflict checks.
	RequestHash string
	// CreatedAt sets the mapping creation time.
	CreatedAt time.Time
}

// ErrInvalidProviderInvoice reports an invalid provider invoice mapping.
var ErrInvalidProviderInvoice = errors.New("connector: invalid provider invoice mapping")

// ErrProviderInvoiceConflict reports a gateway id already mapped to another
// payment: silently remapping the invoice would attribute events to a wrong
// platform payment.
var ErrProviderInvoiceConflict = errors.New("connector: provider invoice mapping conflict")

// Validate checks the mapping invariants before it reaches the storage.
func (m ProviderInvoice) Validate() error {
	if !validCode(m.Provider, maxProviderLen) {
		return fmt.Errorf("%w: invalid provider %q", ErrInvalidProviderInvoice, m.Provider)
	}
	if !validPrintable(m.GatewayID, maxGatewayIDLen) {
		return fmt.Errorf("%w: invalid gateway_id", ErrInvalidProviderInvoice)
	}
	if _, err := uuid.Parse(strings.TrimSpace(m.PaymentID)); err != nil {
		return fmt.Errorf("%w: payment_id must be uuid", ErrInvalidProviderInvoice)
	}
	if !validPrintable(m.OrderID, maxGatewayIDLen) {
		return fmt.Errorf("%w: invalid order_id", ErrInvalidProviderInvoice)
	}
	return nil
}

// RouteRequest describes a provider selection request.
type RouteRequest struct {
	// Network sets the operation network.
	Network string
	// Asset sets the operation asset.
	Asset string
	// Amount sets the operation amount.
	Amount money.Money
	// Operation sets the operation type (invoice, payout).
	Operation string
}

// Validate checks the mandatory fields of the routing request:
// the operation, network, asset, and a positive amount in the same asset.
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
	if r.Amount.MinorUnits <= 0 {
		return fmt.Errorf("%w: non-positive amount", ErrInvalidRoute)
	}
	if r.Amount.Asset != r.Asset {
		return fmt.Errorf("%w: amount asset %q does not match %q", ErrInvalidRoute, r.Amount.Asset, r.Asset)
	}
	return nil
}

// Router selects a provider according to the routing rules.
type Router interface {
	// Select returns the gateway for the request.
	Select(ctx context.Context, req RouteRequest) (PaymentGateway, error)
}
