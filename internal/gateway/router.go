package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// ProviderRegistration describes a provider registration in the router.
type ProviderRegistration struct {
	// Name sets the provider name (it matches the routing rules).
	Name string
	// Gateway sets the provider adapter; it is wrapped into Resilient.
	Gateway domain.PaymentGateway
}

// RouterOption configures Router.
type RouterOption func(*Router)

// WithRouterRand attaches a randomness source for the weighted selection
// (deterministic tests); the wrapper makes access thread-safe.
func WithRouterRand(source *rand.Rand) RouterOption {
	return func(r *Router) {
		if source == nil {
			return
		}
		var mu sync.Mutex
		r.pick = func(total int) int {
			mu.Lock()
			defer mu.Unlock()
			return source.IntN(total)
		}
	}
}

// WithRouterLogger attaches the router logger.
func WithRouterLogger(log *zap.Logger) RouterOption {
	return func(r *Router) {
		if log != nil {
			r.log = log
		}
	}
}

// WithResilientOptions sets the options of the Resilient wrappers for all providers.
func WithResilientOptions(opts ...Option) RouterOption {
	return func(r *Router) { r.resilientOpts = append(r.resilientOpts, opts...) }
}

// Router selects a provider according to the routing rules; each provider
// is wrapped into a single Resilient instance (3.3).
type Router struct {
	providers     map[string]*Resilient
	cfg           Config
	rules         domain.RouteRulesSource
	pick          func(total int) int
	log           *zap.Logger
	resilientOpts []Option
}

// NewRouter creates a router from the rules and the registered providers.
func NewRouter(rules domain.RouteRulesSource, registrations []ProviderRegistration, cfg Config, opts ...RouterOption) (*Router, error) {
	if rules == nil {
		return nil, errors.New("gateway: nil route rules source")
	}
	router := &Router{
		providers: make(map[string]*Resilient, len(registrations)),
		cfg:       cfg,
		rules:     rules,
		// Package-level rand.IntN is thread-safe; an injected source
		// is wrapped with a mutex (WithRouterRand).
		pick: rand.IntN, //nolint:gosec // G404: weighted provider selection, not cryptography
		log:  zap.NewNop(),
	}
	for _, opt := range opts {
		opt(router)
	}
	for _, registration := range registrations {
		name := strings.TrimSpace(registration.Name)
		if name == "" {
			return nil, errors.New("gateway: provider name is empty")
		}
		if registration.Gateway == nil {
			return nil, fmt.Errorf("gateway: provider %s has nil gateway", name)
		}
		if _, exists := router.providers[name]; exists {
			return nil, fmt.Errorf("gateway: duplicate provider %s", name)
		}
		if provider := registration.Gateway.Capabilities().Provider; provider != "" && provider != name {
			// The registration name is the key for rules and logs; a mismatch
			// with the adapter name breaks GetGatewayPayment(provider, ...) for consumers.
			return nil, fmt.Errorf("gateway: provider name %q does not match capabilities %q", name, provider)
		}
		router.providers[name] = New(registration.Gateway, cfg, router.resilientOpts...)
	}
	return router, nil
}

// Select returns a provider for the request: the minimum priority among matching
// rules, with a weighted choice within the priority; providers with an open breaker
// are excluded while an alternative exists.
func (r *Router) Select(ctx context.Context, req domain.RouteRequest) (domain.PaymentGateway, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	rules, err := r.rules.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("gateway: load routing rules: %w", err)
	}

	type candidate struct {
		name     string
		gateway  *Resilient
		priority int
		weight   int
	}
	candidates := make([]candidate, 0, len(rules))
	breakerBlocked := false
	weightInvalid := false
	for _, rule := range rules {
		if !rule.Enabled || !ruleMatches(rule, req) {
			continue
		}
		gateway, ok := r.providers[rule.Provider]
		if !ok {
			continue
		}
		if rule.Weight <= 0 {
			// Defense-in-depth: an untrusted source (cache) with a zero weight
			// must not cause an IntN panic; the configuration is considered broken.
			weightInvalid = true
			continue
		}
		if !capabilitySupports(gateway.Capabilities(), req) {
			continue
		}
		if !gateway.Available() {
			breakerBlocked = true
			continue
		}
		candidates = append(candidates, candidate{
			name:     rule.Provider,
			gateway:  gateway,
			priority: rule.Priority,
			weight:   rule.Weight,
		})
	}
	if len(candidates) == 0 {
		if breakerBlocked || weightInvalid {
			return nil, fmt.Errorf("%w: no available provider for %s/%s", domain.ErrProviderUnavailable, req.Network, req.Asset)
		}
		return nil, fmt.Errorf("%w: no provider for %s/%s (%s)", domain.ErrUnsupportedNetwork, req.Network, req.Asset, req.Operation)
	}

	best := candidates[0].priority
	for _, candidate := range candidates[1:] {
		if candidate.priority < best {
			best = candidate.priority
		}
	}
	totalWeight := 0
	for _, candidate := range candidates {
		if candidate.priority == best {
			totalWeight += candidate.weight
		}
	}
	if totalWeight <= 0 {
		// Defense-in-depth: an untrusted source with zeroed weights must not
		// cause a selection panic.
		return nil, fmt.Errorf("%w: zero weight for %s/%s", domain.ErrProviderUnavailable, req.Network, req.Asset)
	}
	pick := r.pick(totalWeight)
	for _, candidate := range candidates {
		if candidate.priority != best {
			continue
		}
		if pick < candidate.weight {
			r.log.Debug("provider selected",
				zap.String("provider", candidate.name),
				zap.String("network", req.Network),
				zap.String("asset", req.Asset),
				zap.String("operation", req.Operation))
			return candidate.gateway, nil
		}
		pick -= candidate.weight
	}
	// Unreachable: the weight sum covers pick.
	return nil, fmt.Errorf("%w: weighted selection failed", domain.ErrProviderUnavailable)
}

// Capabilities returns the capabilities of every registered provider.
func (r *Router) Capabilities() map[string]domain.Capabilities {
	capabilities := make(map[string]domain.Capabilities, len(r.providers))
	for name, provider := range r.providers {
		capabilities[name] = provider.Capabilities()
	}
	return capabilities
}

// Providers returns the names of the registered providers in sorted order.
func (r *Router) Providers() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ruleMatches checks the operation, network, asset, and amount bounds of the rule.
func ruleMatches(rule domain.RouteRule, req domain.RouteRequest) bool {
	if rule.Operation != "" && rule.Operation != req.Operation {
		return false
	}
	if len(rule.Networks) > 0 && !contains(rule.Networks, req.Network) {
		return false
	}
	if len(rule.Assets) > 0 && !contains(rule.Assets, req.Asset) {
		return false
	}
	amount := req.Amount.MinorUnits
	if rule.MinAmount > 0 && amount < rule.MinAmount {
		return false
	}
	if rule.MaxAmount > 0 && amount > rule.MaxAmount {
		return false
	}
	return true
}

// capabilitySupports checks the provider support for the network and asset and the amount limits.
func capabilitySupports(capabilities domain.Capabilities, req domain.RouteRequest) bool {
	network, ok := capabilities.Networks[req.Network]
	if !ok || network.Asset != req.Asset {
		return false
	}
	amount := req.Amount.MinorUnits
	if !limitWithin(network.MinAmount, req, amount, false) {
		return false
	}
	return limitWithin(network.MaxAmount, req, amount, true)
}

// limitWithin checks a provider amount bound: a zero bound (0)
// means no limit; a set bound checks the asset, decimals, and value.
func limitWithin(limit money.Money, req domain.RouteRequest, amount int64, upper bool) bool {
	if limit.MinorUnits == 0 {
		return true
	}
	if limit.Asset != req.Asset || limit.Decimals != req.Amount.Decimals {
		return false
	}
	if upper {
		return amount <= limit.MinorUnits
	}
	return amount >= limit.MinorUnits
}

// contains reports that the list contains the value.
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
