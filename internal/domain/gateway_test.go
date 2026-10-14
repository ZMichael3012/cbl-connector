package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-kit/money"
)

func TestCanonicalState(t *testing.T) {
	t.Parallel()

	tests := map[string]domain.State{
		"detected":  domain.StateDetected,
		"DETECTED":  domain.StateDetected,
		" pending":  domain.StateDetected,
		"confirmed": domain.StateConfirmed,
		"completed": domain.StateConfirmed,
		"failed":    domain.StateFailed,
		"expired":   domain.StateExpired,
		"underpaid": domain.StateUnderpaid,
		"overpaid":  domain.StateOverpaid,
	}
	for raw, want := range tests {
		state, err := domain.CanonicalState(raw)
		require.NoErrorf(t, err, "raw=%q", raw)
		assert.Equalf(t, want, state, "raw=%q", raw)
	}

	_, err := domain.CanonicalState("reorg")
	require.ErrorIs(t, err, domain.ErrUnknownState)
}

func TestCanonicalPayoutState(t *testing.T) {
	t.Parallel()

	tests := map[string]domain.PayoutState{
		"submitted": domain.PayoutSubmitted,
		"pending":   domain.PayoutSubmitted,
		"confirmed": domain.PayoutConfirmed,
		"completed": domain.PayoutConfirmed,
		"failed":    domain.PayoutFailed,
		"rejected":  domain.PayoutFailed,
	}
	for raw, want := range tests {
		state, err := domain.CanonicalPayoutState(raw)
		require.NoErrorf(t, err, "raw=%q", raw)
		assert.Equalf(t, want, state, "raw=%q", raw)
	}

	_, err := domain.CanonicalPayoutState("reversed")
	require.ErrorIs(t, err, domain.ErrUnknownState)
}

func TestRouteRequestValidate(t *testing.T) {
	t.Parallel()

	valid := domain.RouteRequest{
		Network:   "tron",
		Asset:     "USDT",
		Amount:    money.MustParse("10.00", "USDT", 2),
		Operation: "invoice",
	}
	require.NoError(t, valid.Validate())

	invalid := []domain.RouteRequest{
		{Network: "", Asset: "USDT", Operation: "invoice"},
		{Network: "tron", Asset: " ", Operation: "invoice"},
		{Network: "tron", Asset: "USDT", Operation: ""},
	}
	for _, req := range invalid {
		require.ErrorIs(t, req.Validate(), domain.ErrInvalidRoute)
	}
}

func TestCapabilitiesLookup(t *testing.T) {
	t.Parallel()

	capabilities := domain.Capabilities{
		Provider: "mock",
		Networks: map[string]domain.NetworkCapability{
			"tron": {
				Asset:             "USDT",
				MinAmount:         money.MustParse("1.00", "USDT", 2),
				ConfirmationDepth: 1,
			},
		},
	}
	require.Contains(t, capabilities.Networks, "tron")
	assert.Equal(t, uint32(1), capabilities.Networks["tron"].ConfirmationDepth)
}
