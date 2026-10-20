package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// fakeAdminStore substitutes the administrative storage.
type fakeAdminStore struct {
	revision        int64
	rules           []domain.RouteRule
	providerNames   []string
	disabledNames   []string
	disabled        map[string]bool
	revisionErr     error
	loadErr         error
	replaceErr      error
	disableErr      error
	enableErr       error
	replaceCalls    int
	replaceExpected int64
	disableCalls    int
	enableCalls     int
	lastReason      string
}

// ListDisabled implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) ListDisabled(context.Context) ([]string, error) {
	return s.disabledNames, nil
}

// Revision implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) Revision(context.Context) (int64, error) {
	if s.revisionErr != nil {
		return 0, s.revisionErr
	}
	return s.revision, nil
}

// LoadAll implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) LoadAll(context.Context) ([]domain.RouteRule, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.rules, nil
}

// ReplaceWithRevision implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) ReplaceWithRevision(_ context.Context, expected int64, rules []domain.RouteRule) (int64, error) {
	s.replaceCalls++
	s.replaceExpected = expected
	if s.replaceErr != nil {
		return 0, s.replaceErr
	}
	s.rules = rules
	s.revision++
	return s.revision, nil
}

// DisableProvider implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) DisableProvider(_ context.Context, provider, reason string) (bool, error) {
	s.disableCalls++
	s.lastReason = reason
	if s.disableErr != nil {
		return false, s.disableErr
	}
	if s.disabled == nil {
		s.disabled = map[string]bool{}
	}
	already := s.disabled[provider]
	s.disabled[provider] = true
	return already, nil
}

// EnableProvider implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) EnableProvider(_ context.Context, provider string) (bool, error) {
	s.enableCalls++
	if s.enableErr != nil {
		return false, s.enableErr
	}
	already := true
	if s.disabled != nil && s.disabled[provider] {
		already = false
		delete(s.disabled, provider)
	}
	return already, nil
}

// ProviderNames implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) ProviderNames(context.Context) ([]string, error) {
	return s.providerNames, nil
}

// fakeInvalidator counts cache invalidations.
type fakeInvalidator struct {
	calls int
	err   error
}

// Invalidate implements usecase.CacheInvalidator.
func (f *fakeInvalidator) Invalidate(context.Context) error {
	f.calls++
	return f.err
}

func adminRule(provider string) domain.RouteRule {
	return domain.RouteRule{Provider: provider, Weight: 1, Enabled: true}
}

func TestAdminGetRules(t *testing.T) {
	t.Parallel()

	store := &fakeAdminStore{revision: 7, rules: []domain.RouteRule{adminRule("mock")}}
	admin := usecase.NewAdminRouting(store, nil, nil, nil)

	revision, rules, err := admin.GetRules(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 7, revision)
	assert.Len(t, rules, 1)

	store.loadErr = errors.New("db down")
	_, _, err = admin.GetRules(context.Background())
	require.Error(t, err)
}

func TestAdminPutRulesValidation(t *testing.T) {
	t.Parallel()

	admin := usecase.NewAdminRouting(&fakeAdminStore{}, nil, nil, nil)
	cases := []struct {
		name     string
		revision int64
		rules    []domain.RouteRule
	}{
		{name: "negative revision", revision: -1, rules: nil},
		{name: "too many rules", revision: 0, rules: make([]domain.RouteRule, 1001)},
		{name: "invalid rule", revision: 0, rules: []domain.RouteRule{{Provider: "mock", Weight: 0, Enabled: true}}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := admin.PutRules(context.Background(), tc.revision, tc.rules)
			require.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrInvalidAdminInput)
		})
	}
}

func TestAdminPutRulesSuccessAndConflict(t *testing.T) {
	t.Parallel()

	store := &fakeAdminStore{}
	cache := &fakeInvalidator{}
	admin := usecase.NewAdminRouting(store, cache, nil, nil)

	rules := []domain.RouteRule{adminRule("mock")}
	revision, stored, err := admin.PutRules(context.Background(), 0, rules)
	require.NoError(t, err)
	assert.EqualValues(t, 1, revision)
	assert.Equal(t, rules, stored)
	assert.Equal(t, 1, cache.calls, "a successful replacement invalidates the cache")

	store.replaceErr = domain.ErrRouteRevisionConflict
	_, _, err = admin.PutRules(context.Background(), 0, rules)
	require.ErrorIs(t, err, domain.ErrRouteRevisionConflict)
	assert.Equal(t, 1, cache.calls, "a conflict changes nothing")

	store.replaceErr = errors.New("db down")
	_, _, err = admin.PutRules(context.Background(), 1, rules)
	require.Error(t, err)
	assert.NotErrorIs(t, err, domain.ErrInvalidAdminInput)
}

func TestAdminDisableProviderValidation(t *testing.T) {
	t.Parallel()

	admin := usecase.NewAdminRouting(&fakeAdminStore{}, nil, nil, nil)
	cases := []struct {
		name     string
		provider string
		reason   string
	}{
		{name: "empty provider", provider: "", reason: ""},
		{name: "long provider", provider: string(make([]byte, 65)) + "x", reason: ""},
		{name: "non ascii provider", provider: "p\u00e9p\u00e8re", reason: ""},
		{name: "long reason", provider: "mock", reason: string(make([]rune, 256))},
		{name: "control characters", provider: "mock", reason: "bad\u0007reason"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := admin.DisableProvider(context.Background(), tc.provider, tc.reason)
			require.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrInvalidAdminInput)
		})
	}
}

func TestAdminDisableProvider(t *testing.T) {
	t.Parallel()

	store := &fakeAdminStore{providerNames: []string{"mock"}}
	cache := &fakeInvalidator{}
	admin := usecase.NewAdminRouting(store, cache, nil, nil)

	already, err := admin.DisableProvider(context.Background(), "mock", "maintenance")
	require.NoError(t, err)
	assert.False(t, already)
	assert.Equal(t, "maintenance", store.lastReason)
	assert.Equal(t, 1, cache.calls)

	already, err = admin.DisableProvider(context.Background(), "mock", "")
	require.NoError(t, err)
	assert.True(t, already, "a repeat is idempotent")
	assert.Equal(t, 2, cache.calls, "every attempt invalidates the cache (a failed earlier invalidation cannot pin a stale payload)")

	_, err = admin.DisableProvider(context.Background(), "unknown", "")
	require.ErrorIs(t, err, domain.ErrProviderNotFound)
}

func TestAdminDisableProviderDisabledSetUnion(t *testing.T) {
	t.Parallel()

	// The provider disappeared from the rules after the first disable: the
	// disabled set keeps it known, so the repeat stays idempotent (review finding).
	store := &fakeAdminStore{providerNames: nil, disabledNames: []string{"muted"}}
	admin := usecase.NewAdminRouting(store, nil, nil, nil)

	already, err := admin.DisableProvider(context.Background(), "muted", "")
	require.NoError(t, err)
	assert.False(t, already)
}

func TestAdminDisableProviderRegistryUnion(t *testing.T) {
	t.Parallel()

	store := &fakeAdminStore{providerNames: []string{"rule-provider"}}
	registry := func() []string { return []string{"registry-provider"} }
	admin := usecase.NewAdminRouting(store, nil, registry, nil)

	already, err := admin.DisableProvider(context.Background(), "registry-provider", "")
	require.NoError(t, err)
	assert.False(t, already)

	already, err = admin.DisableProvider(context.Background(), "rule-provider", "")
	require.NoError(t, err)
	assert.False(t, already)
}

func TestAdminCacheFailureIsLoggedNotFatal(t *testing.T) {
	t.Parallel()

	store := &fakeAdminStore{}
	cache := &fakeInvalidator{err: errors.New("redis down")}
	admin := usecase.NewAdminRouting(store, cache, nil, nil)

	_, _, err := admin.PutRules(context.Background(), 0, nil)
	require.NoError(t, err, "the TTL bounds staleness, the operation succeeds")
	assert.Equal(t, 1, cache.calls)
}

func TestAdminEnableProvider(t *testing.T) {
	t.Parallel()

	store := &fakeAdminStore{providerNames: []string{"mock"}, disabled: map[string]bool{"mock": true}}
	cache := &fakeInvalidator{}
	admin := usecase.NewAdminRouting(store, cache, nil, nil)

	already, err := admin.EnableProvider(context.Background(), "mock")
	require.NoError(t, err)
	assert.False(t, already)
	assert.Equal(t, 1, cache.calls)

	already, err = admin.EnableProvider(context.Background(), "mock")
	require.NoError(t, err)
	assert.True(t, already, "a repeat is idempotent")
	assert.Equal(t, 2, cache.calls, "every attempt invalidates the cache")

	_, err = admin.EnableProvider(context.Background(), "unknown")
	require.ErrorIs(t, err, domain.ErrProviderNotFound)

	_, err = admin.EnableProvider(context.Background(), "")
	require.ErrorIs(t, err, domain.ErrInvalidAdminInput)
}
