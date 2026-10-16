package domain_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// testPaymentID sets the payment UUID used by the event tests.
const testPaymentID = "01a138fd-0000-7000-8000-000000000001"

// validEvent builds a valid canonical event.
func validEvent() domain.CanonicalEvent {
	return domain.CanonicalEvent{
		Type:           domain.StateConfirmed,
		PaymentID:      testPaymentID,
		GatewayID:      "gw-1",
		Provider:       "mock",
		TXID:           "0xabc",
		Network:        "trc20",
		ReceivedAmount: money.Money{MinorUnits: 100_000, Asset: "USDT", Decimals: 2},
		Confirmations:  3,
		OccurredAt:     time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC),
	}
}

func TestCanonicalEventValidate(t *testing.T) {
	t.Parallel()

	require.NoError(t, validEvent().Validate())

	cases := map[string]func(*domain.CanonicalEvent){
		"empty payment_id":     func(e *domain.CanonicalEvent) { e.PaymentID = " " },
		"payment_id not uuid":  func(e *domain.CanonicalEvent) { e.PaymentID = "pay-1" },
		"empty gateway_id":     func(e *domain.CanonicalEvent) { e.GatewayID = "" },
		"long gateway_id":      func(e *domain.CanonicalEvent) { e.GatewayID = strings.Repeat("g", 129) },
		"control character":    func(e *domain.CanonicalEvent) { e.GatewayID = "gw\x001" },
		"empty provider":       func(e *domain.CanonicalEvent) { e.Provider = "" },
		"unknown type":         func(e *domain.CanonicalEvent) { e.Type = domain.State("unexpected") },
		"zero time":            func(e *domain.CanonicalEvent) { e.OccurredAt = time.Time{} },
		"long txid":            func(e *domain.CanonicalEvent) { e.TXID = strings.Repeat("t", 257) },
		"bad network":          func(e *domain.CanonicalEvent) { e.Network = "trc 20" },
		"long network":         func(e *domain.CanonicalEvent) { e.Network = strings.Repeat("n", 65) },
		"empty asset":          func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = "" },
		"short asset":          func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = "U" },
		"long asset":           func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = strings.Repeat("A", 17) },
		"asset with garbage":   func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = "US DT" },
		"decimals above limit": func(e *domain.CanonicalEvent) { e.ReceivedAmount.Decimals = 19 },
		"negative amount":      func(e *domain.CanonicalEvent) { e.ReceivedAmount.MinorUnits = -1 },
		"zero amount is allowed": func(e *domain.CanonicalEvent) {
			e.ReceivedAmount.MinorUnits = 0
		},
		"empty txid is allowed": func(e *domain.CanonicalEvent) { e.TXID = "" },
		"empty network is allowed": func(e *domain.CanonicalEvent) {
			e.Network = ""
		},
	}
	for name, mutate := range cases {
		event := validEvent()
		mutate(&event)
		err := event.Validate()
		if name == "zero amount is allowed" || name == "empty txid is allowed" || name == "empty network is allowed" {
			assert.NoError(t, err, name)
			continue
		}
		assert.ErrorIs(t, err, domain.ErrInvalidWebhook, name)
	}
}

func TestCanonicalEventValidateAllStates(t *testing.T) {
	t.Parallel()

	states := []domain.State{
		domain.StateDetected, domain.StateConfirmed, domain.StateFailed,
		domain.StateExpired, domain.StateUnderpaid, domain.StateOverpaid,
	}
	for _, state := range states {
		event := validEvent()
		event.Type = state
		assert.NoError(t, event.Validate(), string(state))
	}
}
