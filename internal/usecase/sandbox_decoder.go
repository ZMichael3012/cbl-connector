package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// sandboxWebhook describes the JSON body of a sandbox provider webhook (contract CBL-3.5).
type sandboxWebhook struct {
	// PaymentID specifies the platform payment.
	PaymentID string `json:"payment_id"`
	// GatewayID specifies the invoice identifier at the provider.
	GatewayID string `json:"gateway_id"`
	// Status specifies the provider status from the alias table of 3.1.
	Status string `json:"status"`
	// ReceivedAmount specifies the received amount.
	ReceivedAmount *sandboxAmount `json:"received_amount"`
	// Confirmations specifies the confirmation depth.
	Confirmations uint32 `json:"confirmations"`
	// TXID specifies the transaction hash.
	TXID string `json:"txid"`
	// Network specifies the payment network.
	Network string `json:"network"`
	// EventTS specifies the provider event time in RFC 3339.
	EventTS string `json:"event_ts"`
}

// sandboxAmount describes the amount in minor units.
type sandboxAmount struct {
	// MinorUnits specifies the amount in the asset minor units.
	MinorUnits int64 `json:"minor_units"`
	// Asset specifies the asset code.
	Asset string `json:"asset"`
	// Decimals specifies the number of decimal places.
	Decimals uint32 `json:"decimals"`
}

// SandboxDecoder decodes webhooks of the sandbox provider (mock-gateway, 3.6)
// until a real provider is chosen (KI-01).
type SandboxDecoder struct {
	provider string
}

// sandboxDecoderProviders is the closed set of profiles served by the sandbox
// decoder: the platform mock gateway wire contract (CBL-3.5). A real provider
// is never decoded from the sandbox JSON: it is fetch-mandatory or its adapter
// owns the decoder (CBL-17.28 review).
var sandboxDecoderProviders = map[string]struct{}{
	"mock": {},
}

// NewSandboxDecoder creates a decoder for the sandbox provider; only the known
// sandbox profiles are accepted and the name is stored in lower case.
func NewSandboxDecoder(provider string) (*SandboxDecoder, error) {
	trimmed := strings.ToLower(strings.TrimSpace(provider))
	if trimmed == "" {
		return nil, errors.New("usecase: empty sandbox provider")
	}
	if _, known := sandboxDecoderProviders[trimmed]; !known {
		return nil, fmt.Errorf("usecase: unknown sandbox provider %q", provider)
	}
	return &SandboxDecoder{provider: trimmed}, nil
}

// Provider returns the decoder provider name.
func (d *SandboxDecoder) Provider() string { return d.provider }

// Limits of the untrusted sandbox webhook input: an excess is cut off
// before parsing so that retry/DLQ messages and logs stay bounded.
const (
	maxWebhookBodyBytes = 256 << 10
	maxStatusLen        = 32
	maxEventTSLen       = 64
)

// Decode parses the JSON body of a sandbox webhook; unknown fields are ignored
// for backward compatibility when the provider adds fields.
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
		// The time.Parse error text contains the input value: a generic error is exposed instead.
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
