package usecase

import (
	"context"
	"fmt"
	"sort"

	"github.com/cbl/cbl-connector/internal/domain"
)

// StaticSelector is the interim provider selector until the rule-based routing
// and failover of task 17.17: it returns the registered gateway with the
// smallest provider id, so the behaviour is deterministic and a single-adapter
// stand (the BTCPay dev contour) routes every request to it.
type StaticSelector struct {
	ordered []domain.PaymentGateway
}

// NewStaticSelector builds the selector over the registered gateways; an empty
// registry fails closed with the provider-unavailable class.
func NewStaticSelector(gateways map[string]domain.PaymentGateway) (domain.Router, error) {
	if len(gateways) == 0 {
		return nil, fmt.Errorf("%w: no provider gateways registered", domain.ErrProviderUnavailable)
	}
	ids := make([]string, 0, len(gateways))
	for id := range gateways {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordered := make([]domain.PaymentGateway, 0, len(ids))
	for _, id := range ids {
		gateway := gateways[id]
		if gateway == nil {
			return nil, fmt.Errorf("usecase: nil gateway for provider %q", id)
		}
		ordered = append(ordered, gateway)
	}
	return &StaticSelector{ordered: ordered}, nil
}

// Select returns the first registered gateway; the route request is not used
// until the rule-based routing of 17.17.
func (s *StaticSelector) Select(_ context.Context, _ domain.RouteRequest) (domain.PaymentGateway, error) {
	if len(s.ordered) == 0 {
		return nil, domain.ErrProviderUnavailable
	}
	return s.ordered[0], nil
}
