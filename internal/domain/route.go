package domain

import (
	"context"
	"fmt"
	"strings"
)

// Routing operations.
const (
	// OperationInvoice sets the invoice creation operation.
	OperationInvoice = "invoice"
	// OperationPayout sets the payout operation.
	OperationPayout = "payout"
	// OperationRefund sets the refund operation (17.2).
	OperationRefund = "refund"
)

// RouteRule describes a provider selection rule.
type RouteRule struct {
	// Provider sets the rule provider.
	Provider string `json:"provider"`
	// Operation sets the rule operation; empty means any.
	Operation string `json:"operation"`
	// Networks sets the rule networks; empty means any.
	Networks []string `json:"networks"`
	// Assets sets the rule assets; empty means any.
	Assets []string `json:"assets"`
	// Priority sets the priority: a lower value wins.
	Priority int `json:"priority"`
	// Weight sets the weight within the priority.
	Weight int `json:"weight"`
	// MinAmount sets the lower amount bound (0 means no bound).
	MinAmount int64 `json:"min_amount"`
	// MaxAmount sets the upper amount bound (0 means no bound).
	MaxAmount int64 `json:"max_amount"`
	// Enabled reports whether the rule is active.
	Enabled bool `json:"enabled"`
}

// Rule field limits: they protect the weighted selection from overflow of the weight sum.
const (
	maxRulePriority = 1_000_000
	maxRuleWeight   = 1_000_000
)

// Validate checks a routing rule for correctness.
func (r RouteRule) Validate() error {
	if !validPrintable(strings.TrimSpace(r.Provider), maxProviderLen) {
		return fmt.Errorf("%w: invalid provider %q", ErrInvalidRoute, r.Provider)
	}
	switch r.Operation {
	case "", OperationInvoice, OperationPayout:
	default:
		return fmt.Errorf("%w: unknown operation %q", ErrInvalidRoute, r.Operation)
	}
	if r.Priority < 0 || r.Priority > maxRulePriority {
		return fmt.Errorf("%w: priority %d out of range", ErrInvalidRoute, r.Priority)
	}
	if r.Weight <= 0 || r.Weight > maxRuleWeight {
		return fmt.Errorf("%w: weight %d out of range", ErrInvalidRoute, r.Weight)
	}
	for _, network := range r.Networks {
		if !validCode(network, maxNetworkLen) {
			return fmt.Errorf("%w: invalid network %q", ErrInvalidRoute, network)
		}
	}
	for _, asset := range r.Assets {
		if !validCode(asset, maxAssetLen) || len(asset) < minAssetLen {
			return fmt.Errorf("%w: invalid asset %q", ErrInvalidRoute, asset)
		}
	}
	if r.MinAmount < 0 || r.MaxAmount < 0 {
		return fmt.Errorf("%w: negative amount limit", ErrInvalidRoute)
	}
	if r.MinAmount > 0 && r.MaxAmount > 0 && r.MinAmount > r.MaxAmount {
		return fmt.Errorf("%w: min_amount %d > max_amount %d", ErrInvalidRoute, r.MinAmount, r.MaxAmount)
	}
	return nil
}

// RouteRulesSource loads the active routing rules.
type RouteRulesSource interface {
	// Load returns the enabled rules ordered by priority.
	Load(ctx context.Context) ([]RouteRule, error)
}
