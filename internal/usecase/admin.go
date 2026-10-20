package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
)

// Limits of the admin contour (CBL-12.2 FR-3): the rule count matches the
// contract maxItems; the provider and reason formats follow the operator
// identity conventions of 10.5.
const (
	maxAdminRules       = 1000
	maxProviderNameLen  = 64
	maxDisableReasonLen = 255
)

// AdminRoutingStore is the storage side of the admin contour.
type AdminRoutingStore interface {
	// Revision returns the current routing configuration revision.
	Revision(ctx context.Context) (int64, error)
	// LoadAll returns every rule, enabled and disabled.
	LoadAll(ctx context.Context) ([]domain.RouteRule, error)
	// ReplaceWithRevision replaces the configuration with optimistic concurrency.
	ReplaceWithRevision(ctx context.Context, expected int64, rules []domain.RouteRule) (int64, error)
	// DisableProvider records a manual disable; already is true for a repeat.
	DisableProvider(ctx context.Context, provider, reason string) (bool, error)
	// EnableProvider removes a manual disable; already is true for a repeat.
	EnableProvider(ctx context.Context, provider string) (bool, error)
	// ProviderNames returns the distinct provider names present in the rules.
	ProviderNames(ctx context.Context) ([]string, error)
	// ListDisabled returns the disabled provider names.
	ListDisabled(ctx context.Context) ([]string, error)
}

// CacheInvalidator drops the cached route configuration.
type CacheInvalidator interface {
	// Invalidate removes the configuration from the cache.
	Invalidate(ctx context.Context) error
}

// AdminRouting serves the administrative routing operations (CBL-12.2).
type AdminRouting struct {
	store             AdminRoutingStore
	cache             CacheInvalidator
	registryProviders func() []string
	log               *zap.Logger
}

// NewAdminRouting creates the admin service; registryProviders reports the
// provider names of the adapter registry (nil until task 3.2 registers them).
func NewAdminRouting(store AdminRoutingStore, cache CacheInvalidator, registryProviders func() []string, log *zap.Logger) *AdminRouting {
	if log == nil {
		log = zap.NewNop()
	}
	if registryProviders == nil {
		registryProviders = func() []string { return nil }
	}
	return &AdminRouting{
		store:             store,
		cache:             cache,
		registryProviders: registryProviders,
		log:               log,
	}
}

// GetRules returns the current revision and every rule (disabled ones included).
func (a *AdminRouting) GetRules(ctx context.Context) (int64, []domain.RouteRule, error) {
	revision, err := a.store.Revision(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("usecase: read routing revision: %w", err)
	}
	rules, err := a.store.LoadAll(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("usecase: load routing rules: %w", err)
	}
	return revision, rules, nil
}

// PutRules replaces the configuration with optimistic concurrency: invalid input
// is ErrInvalidAdminInput, a revision mismatch passes through as
// ErrRouteRevisionConflict; on success the cache is invalidated.
func (a *AdminRouting) PutRules(ctx context.Context, expected int64, rules []domain.RouteRule) (int64, []domain.RouteRule, error) {
	if expected < 0 {
		return 0, nil, fmt.Errorf("%w: revision must not be negative", domain.ErrInvalidAdminInput)
	}
	if len(rules) > maxAdminRules {
		return 0, nil, fmt.Errorf("%w: at most %d rules are allowed", domain.ErrInvalidAdminInput, maxAdminRules)
	}
	for i := range rules {
		if err := rules[i].Validate(); err != nil {
			return 0, nil, fmt.Errorf("%w: rule %d: %w", domain.ErrInvalidAdminInput, i, err)
		}
	}
	revision, err := a.store.ReplaceWithRevision(ctx, expected, rules)
	if err != nil {
		if errors.Is(err, domain.ErrRouteRevisionConflict) {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("usecase: replace routing rules: %w", err)
	}
	a.invalidate(ctx)
	return revision, rules, nil
}

// DisableProvider validates the input, checks that the provider is known to the
// registry, the rules or the disabled set (a repeated disable of a provider
// whose rules were replaced must stay idempotent, review finding), and records
// the disable; the cache is invalidated on every attempt so a failed
// invalidation cannot pin a stale payload until the TTL.
func (a *AdminRouting) DisableProvider(ctx context.Context, provider, reason string) (bool, error) {
	if err := validateProviderName(provider); err != nil {
		return false, err
	}
	if err := validateDisableReason(reason); err != nil {
		return false, err
	}
	known, err := a.knownProviders(ctx)
	if err != nil {
		return false, fmt.Errorf("usecase: load known providers: %w", err)
	}
	if !known[provider] {
		return false, fmt.Errorf("%w: %s", domain.ErrProviderNotFound, provider)
	}
	already, err := a.store.DisableProvider(ctx, provider, reason)
	if err != nil {
		return false, fmt.Errorf("usecase: disable provider: %w", err)
	}
	a.invalidate(ctx)
	return already, nil
}

// EnableProvider validates the input, requires the provider to be known through
// the registry, the rules or the disabled set, and removes the manual disable;
// the cache is invalidated on every attempt so a re-enabled provider is visible
// on the next read even when invalidation fails (the TTL bounds staleness).
func (a *AdminRouting) EnableProvider(ctx context.Context, provider string) (bool, error) {
	if err := validateProviderName(provider); err != nil {
		return false, err
	}
	known, err := a.knownProviders(ctx)
	if err != nil {
		return false, fmt.Errorf("usecase: load known providers: %w", err)
	}
	if !known[provider] {
		return false, fmt.Errorf("%w: %s", domain.ErrProviderNotFound, provider)
	}
	already, err := a.store.EnableProvider(ctx, provider)
	if err != nil {
		return false, fmt.Errorf("usecase: enable provider: %w", err)
	}
	a.invalidate(ctx)
	return already, nil
}

// invalidate drops the cache; a failure is logged because the TTL bounds staleness.
func (a *AdminRouting) invalidate(ctx context.Context) {
	if a.cache == nil {
		return
	}
	if err := a.cache.Invalidate(ctx); err != nil {
		a.log.Warn("route config cache invalidate failed", zap.Error(err))
	}
}

// knownProviders unions the adapter registry, the providers present in the
// rules and the already disabled providers: a repeated disable of a provider
// whose rules were replaced must stay idempotent.
func (a *AdminRouting) knownProviders(ctx context.Context) (map[string]bool, error) {
	known := make(map[string]bool)
	for _, name := range a.registryProviders() {
		known[name] = true
	}
	names, err := a.store.ProviderNames(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		known[name] = true
	}
	disabled, err := a.store.ListDisabled(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range disabled {
		known[name] = true
	}
	return known, nil
}

// validateProviderName checks a provider name: visible ASCII up to 64 characters.
func validateProviderName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" || utf8.RuneCountInString(trimmed) > maxProviderNameLen {
		return fmt.Errorf("%w: provider name must be 1..%d visible characters", domain.ErrInvalidAdminInput, maxProviderNameLen)
	}
	for _, r := range trimmed {
		if r < 0x21 || r > 0x7E {
			return fmt.Errorf("%w: provider name must be visible ASCII", domain.ErrInvalidAdminInput)
		}
	}
	return nil
}

// validateDisableReason checks the optional reason: up to 255 characters
// without control characters.
func validateDisableReason(reason string) error {
	if utf8.RuneCountInString(reason) > maxDisableReasonLen {
		return fmt.Errorf("%w: reason must be at most %d characters", domain.ErrInvalidAdminInput, maxDisableReasonLen)
	}
	for _, r := range reason {
		if r < 0x20 || r == 0x7F {
			return fmt.Errorf("%w: reason must not contain control characters", domain.ErrInvalidAdminInput)
		}
	}
	return nil
}
