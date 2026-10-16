package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// fakeRouteStore substitutes the rule repository.
type fakeRouteStore struct {
	rules     []domain.RouteRule
	loadErr   error
	saveErr   error
	saveCalls int
	saved     []domain.RouteRule
}

// LoadEnabled returns the prepared rules.
func (s *fakeRouteStore) LoadEnabled(context.Context) ([]domain.RouteRule, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.rules, nil
}

// Save records the rule replacement.
func (s *fakeRouteStore) Save(_ context.Context, rules []domain.RouteRule) error {
	s.saveCalls++
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = rules
	s.rules = rules
	return nil
}

// fakeRouteCache substitutes the Redis cache.
type fakeRouteCache struct {
	rules           []domain.RouteRule
	present         bool
	getErr          error
	setErr          error
	invalidateErr   error
	setCalls        int
	invalidateCalls int
	lastTTL         time.Duration
}

// Get returns the prepared cache value.
func (c *fakeRouteCache) Get(context.Context) ([]domain.RouteRule, bool, error) {
	if c.getErr != nil {
		return nil, false, c.getErr
	}
	return c.rules, c.present, nil
}

// Set records the configuration write.
func (c *fakeRouteCache) Set(_ context.Context, rules []domain.RouteRule, ttl time.Duration) error {
	c.setCalls++
	c.lastTTL = ttl
	if c.setErr != nil {
		return c.setErr
	}
	c.rules = rules
	c.present = true
	return nil
}

// Invalidate records the invalidation.
func (c *fakeRouteCache) Invalidate(context.Context) error {
	c.invalidateCalls++
	c.present = false
	return c.invalidateErr
}

func sampleRules() []domain.RouteRule {
	return []domain.RouteRule{{Provider: "sandbox", Priority: 0, Weight: 1, Enabled: true}}
}

func TestNormalizeRouteCacheTTL(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 30*time.Second, usecase.NormalizeRouteCacheTTL(0))
	assert.Equal(t, 30*time.Second, usecase.NormalizeRouteCacheTTL(500*time.Millisecond), "below the system minimum")
	assert.Equal(t, 45*time.Second, usecase.NormalizeRouteCacheTTL(45*time.Second))
	assert.Equal(t, time.Minute, usecase.NormalizeRouteCacheTTL(time.Hour), "above the system maximum")
}

func TestRouteCacheTTLFromEnv(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 30*time.Second, usecase.RouteCacheTTLFromEnv(func(string) string { return "" }))
	assert.Equal(t, time.Minute, usecase.RouteCacheTTLFromEnv(func(key string) string {
		if key == "CONNECTOR_ROUTE_CACHE_TTL" {
			return "2m"
		}
		return ""
	}))
	assert.Equal(t, 30*time.Second, usecase.RouteCacheTTLFromEnv(func(string) string { return "soon" }))
	assert.Equal(t, time.Minute, usecase.RouteCacheTTLFromEnv(func(string) string { return "99m" }))
}

func TestCachedRouteRulesLoadMissWritesCache(t *testing.T) {
	t.Parallel()

	store := &fakeRouteStore{rules: sampleRules()}
	cache := &fakeRouteCache{}
	source := usecase.NewCachedRouteRules(store, cache, 45*time.Second, nil)

	rules, err := source.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, sampleRules(), rules)
	assert.Equal(t, 1, cache.setCalls)
	assert.Equal(t, 45*time.Second, cache.lastTTL)
}

func TestCachedRouteRulesLoadHitSkipsStore(t *testing.T) {
	t.Parallel()

	store := &fakeRouteStore{loadErr: errors.New("db down")}
	cache := &fakeRouteCache{rules: sampleRules(), present: true}
	source := usecase.NewCachedRouteRules(store, cache, 0, nil)

	rules, err := source.Load(context.Background())
	require.NoError(t, err, "a cache hit does not touch the database")
	assert.Equal(t, sampleRules(), rules)
}

func TestCachedRouteRulesRejectsInvalidCachePayload(t *testing.T) {
	t.Parallel()

	// A semantically poisoned cache (weight 0) is a miss: the rules come from the database,
	// otherwise the router would get a zero weight sum.
	poisoned := []domain.RouteRule{{Provider: "sandbox", Priority: 0, Weight: 0, Enabled: true}}
	store := &fakeRouteStore{rules: sampleRules()}
	cache := &fakeRouteCache{rules: poisoned, present: true}
	source := usecase.NewCachedRouteRules(store, cache, 0, nil)

	rules, err := source.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, sampleRules(), rules, "the poisoned cache is overridden by storage data")
	assert.Equal(t, 1, cache.setCalls, "the valid payload is warmed into the cache")
}

func TestCachedRouteRulesRejectsInvalidStorageRules(t *testing.T) {
	t.Parallel()

	store := &fakeRouteStore{rules: []domain.RouteRule{{Provider: "sandbox", Weight: -1, Enabled: true}}}
	source := usecase.NewCachedRouteRules(store, &fakeRouteCache{}, 0, nil)

	_, err := source.Load(context.Background())
	require.Error(t, err, "corruption of the source of truth must not reach the router")
}

func TestCachedRouteRulesCacheErrorsFallback(t *testing.T) {
	t.Parallel()

	store := &fakeRouteStore{rules: sampleRules()}
	cache := &fakeRouteCache{getErr: errors.New("redis down"), setErr: errors.New("redis down")}
	source := usecase.NewCachedRouteRules(store, cache, 0, nil)

	rules, err := source.Load(context.Background())
	require.NoError(t, err, "a Redis failure does not break selection")
	assert.Equal(t, sampleRules(), rules)
	assert.Equal(t, 1, cache.setCalls, "a cache warm-up attempt is made")
}

func TestCachedRouteRulesStoreError(t *testing.T) {
	t.Parallel()

	store := &fakeRouteStore{loadErr: errors.New("db down")}
	source := usecase.NewCachedRouteRules(store, &fakeRouteCache{}, 0, nil)

	_, err := source.Load(context.Background())
	require.ErrorIs(t, err, store.loadErr)
}

func TestCachedRouteRulesSaveInvalidatesCache(t *testing.T) {
	t.Parallel()

	store := &fakeRouteStore{}
	cache := &fakeRouteCache{rules: sampleRules(), present: true}
	source := usecase.NewCachedRouteRules(store, cache, 0, nil)

	rules := sampleRules()
	require.NoError(t, source.Save(context.Background(), rules))
	assert.Equal(t, 1, store.saveCalls)
	assert.Equal(t, 1, cache.invalidateCalls)
	assert.False(t, cache.present, "the cache is invalidated")

	_, err := source.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, cache.setCalls, "after invalidation the cache is warmed again")
}
