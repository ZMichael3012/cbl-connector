package domain_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
)

func TestRouteRuleValidate(t *testing.T) {
	t.Parallel()

	valid := domain.RouteRule{
		Provider:  "sandbox",
		Operation: domain.OperationInvoice,
		Networks:  []string{"trc20"},
		Assets:    []string{"USDT"},
		Priority:  1,
		Weight:    5,
		MinAmount: 100,
		MaxAmount: 1_000_000,
		Enabled:   true,
	}
	require.NoError(t, valid.Validate())

	tests := map[string]func(rule *domain.RouteRule){
		"empty provider":        func(r *domain.RouteRule) { r.Provider = " " },
		"unknown operation":     func(r *domain.RouteRule) { r.Operation = "withdraw" },
		"negative priority":     func(r *domain.RouteRule) { r.Priority = -1 },
		"priority out of range": func(r *domain.RouteRule) { r.Priority = 2_000_000 },
		"zero weight":           func(r *domain.RouteRule) { r.Weight = 0 },
		"negative weight":       func(r *domain.RouteRule) { r.Weight = -1 },
		"weight out of range":   func(r *domain.RouteRule) { r.Weight = 2_000_000 },
		"invalid network":       func(r *domain.RouteRule) { r.Networks = []string{"bad network"} },
		"invalid asset":         func(r *domain.RouteRule) { r.Assets = []string{"usdt-!"} },
		"short asset":           func(r *domain.RouteRule) { r.Assets = []string{"U"} },
		"negative min":          func(r *domain.RouteRule) { r.MinAmount = -1 },
		"negative max":          func(r *domain.RouteRule) { r.MaxAmount = -1 },
		"min above max":         func(r *domain.RouteRule) { r.MinAmount = 10; r.MaxAmount = 5 },
		"long network":          func(r *domain.RouteRule) { r.Networks = []string{strings.Repeat("n", 65)} },
		"provider with spaces":  func(r *domain.RouteRule) { r.Provider = "  " },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rule := valid
			rule.Networks = append([]string(nil), valid.Networks...)
			rule.Assets = append([]string(nil), valid.Assets...)
			mutate(&rule)
			require.ErrorIs(t, rule.Validate(), domain.ErrInvalidRoute)
		})
	}

	t.Run("any operation allowed", func(t *testing.T) {
		rule := valid
		rule.Operation = ""
		require.NoError(t, rule.Validate())
	})
}
