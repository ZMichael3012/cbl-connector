package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
)

// TTL bounds of the route configuration cache: CBL-003 (section 7) fixes
// the 30 - 60 s range for route:config.
const (
	defaultRouteCacheTTL = 30 * time.Second
	minRouteCacheTTL     = 30 * time.Second
	maxRouteCacheTTL     = 60 * time.Second
	// maxDisabledNameLen bounds a disabled provider name in a cached payload.
	maxDisabledNameLen = 128
)

// RouteRulesStore stores the routing rules (source of truth).
type RouteRulesStore interface {
	// LoadEnabled returns the enabled rules.
	LoadEnabled(ctx context.Context) ([]domain.RouteRule, error)
	// ListDisabled returns the disabled provider names.
	ListDisabled(ctx context.Context) ([]string, error)
	// Save replaces the whole rule configuration.
	Save(ctx context.Context, rules []domain.RouteRule) error
}

// RouteConfigCacheStore caches the rule configuration.
type RouteConfigCacheStore interface {
	// Get returns the configuration from the cache; a miss is ok=false.
	Get(ctx context.Context) (domain.RouteConfigSnapshot, bool, error)
	// Set stores the configuration with a TTL.
	Set(ctx context.Context, snapshot domain.RouteConfigSnapshot, ttl time.Duration) error
	// Invalidate removes the configuration from the cache.
	Invalidate(ctx context.Context) error
}

// NormalizeRouteCacheTTL clamps the TTL to the 30 - 60 s range (CBL-003)
// and replaces an invalid value with the default.
func NormalizeRouteCacheTTL(ttl time.Duration) time.Duration {
	switch {
	case ttl <= 0:
		return defaultRouteCacheTTL
	case ttl < minRouteCacheTTL:
		return minRouteCacheTTL
	case ttl > maxRouteCacheTTL:
		return maxRouteCacheTTL
	default:
		return ttl
	}
}

// RouteCacheTTLFromEnv reads CONNECTOR_ROUTE_CACHE_TTL with clamping.
func RouteCacheTTLFromEnv(getenv func(string) string) time.Duration {
	raw := getenv("CONNECTOR_ROUTE_CACHE_TTL")
	if raw == "" {
		return defaultRouteCacheTTL
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil {
		return defaultRouteCacheTTL
	}
	return NormalizeRouteCacheTTL(ttl)
}

// CachedRouteRules reads the rules through the Redis cache with a fallback to storage:
// the source of truth is PostgreSQL, a cache failure does not affect selection (T-02, T-11).
// Rules of disabled providers are filtered out here, so the router never sees them
// (CBL-12.2 FR-4).
type CachedRouteRules struct {
	store RouteRulesStore
	cache RouteConfigCacheStore
	ttl   time.Duration
	log   *zap.Logger
}

// NewCachedRouteRules creates the rule source with a cache.
func NewCachedRouteRules(store RouteRulesStore, cache RouteConfigCacheStore, ttl time.Duration, log *zap.Logger) *CachedRouteRules {
	if log == nil {
		log = zap.NewNop()
	}
	return &CachedRouteRules{
		store: store,
		cache: cache,
		ttl:   NormalizeRouteCacheTTL(ttl),
		log:   log,
	}
}

// Load returns the enabled rules of active providers from the cache or from
// storage (cache-aside). The cache is not the source of truth: a semantically
// invalid payload (for example, a zeroed weight) counts as a miss and never
// reaches the router.
func (s *CachedRouteRules) Load(ctx context.Context) ([]domain.RouteRule, error) {
	if s.cache != nil {
		snapshot, ok, err := s.cache.Get(ctx)
		if err != nil {
			s.log.Warn("route config cache read failed", zap.Error(err))
		} else if ok {
			if snapshotValid(snapshot) {
				return activeRules(snapshot), nil
			}
			s.log.Warn("route config cache payload is invalid: falling back to storage")
		}
	}
	rules, err := s.store.LoadEnabled(ctx)
	if err != nil {
		return nil, fmt.Errorf("usecase: load routing rules: %w", err)
	}
	disabled, err := s.store.ListDisabled(ctx)
	if err != nil {
		return nil, fmt.Errorf("usecase: load disabled providers: %w", err)
	}
	snapshot := domain.RouteConfigSnapshot{Rules: rules, Disabled: disabled}
	if !snapshotValid(snapshot) {
		return nil, errors.New("usecase: routing rules from storage are invalid")
	}
	if s.cache != nil {
		if err := s.cache.Set(ctx, snapshot, s.ttl); err != nil {
			s.log.Warn("route config cache write failed", zap.Error(err))
		}
	}
	return activeRules(snapshot), nil
}

// Save replaces the rules in storage and invalidates the cache.
func (s *CachedRouteRules) Save(ctx context.Context, rules []domain.RouteRule) error {
	if err := s.store.Save(ctx, rules); err != nil {
		return err
	}
	return s.Invalidate(ctx)
}

// Invalidate drops the cached configuration; a failure is logged because the
// TTL bounds staleness (CBL-12.2 FR-3).
func (s *CachedRouteRules) Invalidate(ctx context.Context) error {
	if s.cache == nil {
		return nil
	}
	if err := s.cache.Invalidate(ctx); err != nil {
		s.log.Warn("route config cache invalidate failed", zap.Error(err))
	}
	return nil
}

// rulesValid checks that every rule passes domain validation.
func rulesValid(rules []domain.RouteRule) bool {
	for i := range rules {
		if err := rules[i].Validate(); err != nil {
			return false
		}
	}
	return true
}

// snapshotValid checks the cached payload: the rules are valid and the disabled
// names are plausible non-empty identifiers.
func snapshotValid(snapshot domain.RouteConfigSnapshot) bool {
	if !rulesValid(snapshot.Rules) {
		return false
	}
	for _, name := range snapshot.Disabled {
		if trimmed := strings.TrimSpace(name); trimmed == "" || len(trimmed) > maxDisabledNameLen {
			return false
		}
	}
	return true
}

// activeRules drops the rules of disabled providers.
func activeRules(snapshot domain.RouteConfigSnapshot) []domain.RouteRule {
	if len(snapshot.Disabled) == 0 {
		return snapshot.Rules
	}
	disabled := make(map[string]bool, len(snapshot.Disabled))
	for _, name := range snapshot.Disabled {
		disabled[name] = true
	}
	active := make([]domain.RouteRule, 0, len(snapshot.Rules))
	for i := range snapshot.Rules {
		if !disabled[snapshot.Rules[i].Provider] {
			active = append(active, snapshot.Rules[i])
		}
	}
	return active
}
