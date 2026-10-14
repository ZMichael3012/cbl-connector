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

// testPaymentID задает UUID платежа для тестов событий.
const testPaymentID = "01a138fd-0000-7000-8000-000000000001"

// validEvent собирает корректное каноническое событие.
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
		"пустой payment_id":     func(e *domain.CanonicalEvent) { e.PaymentID = " " },
		"payment_id не uuid":    func(e *domain.CanonicalEvent) { e.PaymentID = "pay-1" },
		"пустой gateway_id":     func(e *domain.CanonicalEvent) { e.GatewayID = "" },
		"длинный gateway_id":    func(e *domain.CanonicalEvent) { e.GatewayID = strings.Repeat("g", 129) },
		"control-символ":        func(e *domain.CanonicalEvent) { e.GatewayID = "gw\x001" },
		"пустой provider":       func(e *domain.CanonicalEvent) { e.Provider = "" },
		"неизвестный тип":       func(e *domain.CanonicalEvent) { e.Type = domain.State("unexpected") },
		"нулевое время":         func(e *domain.CanonicalEvent) { e.OccurredAt = time.Time{} },
		"длинный txid":          func(e *domain.CanonicalEvent) { e.TXID = strings.Repeat("t", 257) },
		"плохой network":        func(e *domain.CanonicalEvent) { e.Network = "trc 20" },
		"длинный network":       func(e *domain.CanonicalEvent) { e.Network = strings.Repeat("n", 65) },
		"пустой актив":          func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = "" },
		"короткий актив":        func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = "U" },
		"длинный актив":         func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = strings.Repeat("A", 17) },
		"актив с мусором":       func(e *domain.CanonicalEvent) { e.ReceivedAmount.Asset = "US DT" },
		"decimals выше предела": func(e *domain.CanonicalEvent) { e.ReceivedAmount.Decimals = 19 },
		"отрицательная сумма":   func(e *domain.CanonicalEvent) { e.ReceivedAmount.MinorUnits = -1 },
		"нулевая сумма допустима": func(e *domain.CanonicalEvent) {
			e.ReceivedAmount.MinorUnits = 0
		},
		"пустой txid допустим": func(e *domain.CanonicalEvent) { e.TXID = "" },
		"пустая сеть допустима": func(e *domain.CanonicalEvent) {
			e.Network = ""
		},
	}
	for name, mutate := range cases {
		event := validEvent()
		mutate(&event)
		err := event.Validate()
		if name == "нулевая сумма допустима" || name == "пустой txid допустим" || name == "пустая сеть допустима" {
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
