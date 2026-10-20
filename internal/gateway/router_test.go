package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	moneykit "github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// fakeRules returns a fixed set of rules or an error.
type fakeRules struct {
	rules []domain.RouteRule
	err   error
}

// Load returns the prepared rules.
func (f *fakeRules) Load(context.Context) ([]domain.RouteRule, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.rules, nil
}

// capabilities builds the provider capabilities for a single network.
func capabilities(provider, network, asset string, minAmount, maxAmount int64) domain.Capabilities {
	return domain.Capabilities{
		Provider: provider,
		Networks: map[string]domain.NetworkCapability{
			network: {
				Asset:     asset,
				MinAmount: moneykit.Money{MinorUnits: minAmount, Asset: asset, Decimals: 2},
				MaxAmount: moneykit.Money{MinorUnits: maxAmount, Asset: asset, Decimals: 2},
			},
		},
	}
}

func invoiceRequest(network, asset string, amount int64) domain.RouteRequest {
	return domain.RouteRequest{
		Network:   network,
		Asset:     asset,
		Amount:    moneykit.Money{MinorUnits: amount, Asset: asset, Decimals: 2},
		Operation: domain.OperationInvoice,
	}
}

func newTestRouter(t *testing.T, rules []domain.RouteRule, registrations []ProviderRegistration, cfg Config) *Router {
	t.Helper()
	router, err := NewRouter(&fakeRules{rules: rules}, registrations, cfg,
		WithRouterRand(rand.New(rand.NewPCG(1, 2)))) //nolint:gosec // G404: deterministic test rand
	require.NoError(t, err)
	return router
}

func TestRouterSelectsMinimalPriority(t *testing.T) {
	t.Parallel()

	primary := &fakeGateway{capability: capabilities("primary", "trc20", "USDT", 0, 0)}
	fallback := &fakeGateway{capability: capabilities("fallback", "trc20", "USDT", 0, 0)}
	rules := []domain.RouteRule{
		{Provider: "fallback", Priority: 5, Weight: 1, Enabled: true},
		{Provider: "primary", Priority: 0, Weight: 1, Enabled: true},
	}
	router := newTestRouter(t, rules, []ProviderRegistration{
		{Name: "primary", Gateway: primary},
		{Name: "fallback", Gateway: fallback},
	}, testConfig())

	selected, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 1_000))
	require.NoError(t, err)
	assert.Equal(t, "primary", selected.Capabilities().Provider)
}

func TestRouterWeightedChoiceWithinPriority(t *testing.T) {
	t.Parallel()

	light := &fakeGateway{capability: capabilities("light", "trc20", "USDT", 0, 0)}
	heavy := &fakeGateway{capability: capabilities("heavy", "trc20", "USDT", 0, 0)}
	rules := []domain.RouteRule{
		{Provider: "light", Priority: 0, Weight: 1, Enabled: true},
		{Provider: "heavy", Priority: 0, Weight: 9, Enabled: true},
	}
	router := newTestRouter(t, rules, []ProviderRegistration{
		{Name: "light", Gateway: light},
		{Name: "heavy", Gateway: heavy},
	}, testConfig())

	heavyPicks := 0
	const draws = 1_000
	for i := 0; i < draws; i++ {
		selected, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 1_000))
		require.NoError(t, err)
		if selected.Capabilities().Provider == "heavy" {
			heavyPicks++
		}
	}
	assert.Greater(t, heavyPicks, 800, "a weight of 9/10 must clearly dominate")
	assert.Less(t, heavyPicks, 980)
}

func TestRouterFilters(t *testing.T) {
	t.Parallel()

	gateway := &fakeGateway{capability: capabilities("sandbox", "trc20", "USDT", 100, 1_000_000)}
	base := domain.RouteRule{
		Provider: "sandbox", Priority: 0, Weight: 1, Enabled: true,
	}
	routerWith := func(rule domain.RouteRule) *Router {
		return newTestRouter(t, []domain.RouteRule{rule}, []ProviderRegistration{
			{Name: "sandbox", Gateway: gateway},
		}, testConfig())
	}

	t.Run("operation mismatch", func(t *testing.T) {
		rule := base
		rule.Operation = domain.OperationPayout
		_, err := routerWith(rule).Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("network mismatch", func(t *testing.T) {
		rule := base
		rule.Networks = []string{"erc20"}
		_, err := routerWith(rule).Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("asset mismatch", func(t *testing.T) {
		rule := base
		rule.Assets = []string{"BTC"}
		_, err := routerWith(rule).Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("rule amount below min", func(t *testing.T) {
		rule := base
		rule.MinAmount = 1_000
		_, err := routerWith(rule).Select(context.Background(), invoiceRequest("trc20", "USDT", 999))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("rule amount above max", func(t *testing.T) {
		rule := base
		rule.MaxAmount = 1_000
		_, err := routerWith(rule).Select(context.Background(), invoiceRequest("trc20", "USDT", 1_001))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("provider min amount", func(t *testing.T) {
		_, err := routerWith(base).Select(context.Background(), invoiceRequest("trc20", "USDT", 99))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("provider max amount", func(t *testing.T) {
		_, err := routerWith(base).Select(context.Background(), invoiceRequest("trc20", "USDT", 1_000_001))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("disabled rule skipped", func(t *testing.T) {
		rule := base
		rule.Enabled = false
		_, err := routerWith(rule).Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("unknown provider skipped", func(t *testing.T) {
		rule := base
		rule.Provider = "ghost"
		_, err := routerWith(rule).Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})

	t.Run("valid selection", func(t *testing.T) {
		selected, err := routerWith(base).Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
		require.NoError(t, err)
		assert.Equal(t, "sandbox", selected.Capabilities().Provider)
	})
}

func TestRouterSkipsOpenBreaker(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BreakerFailureThreshold = 1
	cfg.BreakerTimeout = 30 * time.Second

	primary := &fakeGateway{
		capability: capabilities("primary", "trc20", "USDT", 0, 0),
		alwaysErr:  fmt.Errorf("%w: down", domain.ErrTemporary),
	}
	fallback := &fakeGateway{capability: capabilities("fallback", "trc20", "USDT", 0, 0)}
	rules := []domain.RouteRule{
		{Provider: "primary", Priority: 0, Weight: 1, Enabled: true},
		{Provider: "fallback", Priority: 1, Weight: 1, Enabled: true},
	}
	router := newTestRouter(t, rules, []ProviderRegistration{
		{Name: "primary", Gateway: primary},
		{Name: "fallback", Gateway: fallback},
	}, cfg)

	selected, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
	require.NoError(t, err)
	assert.Equal(t, "primary", selected.Capabilities().Provider)

	_, err = selected.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "k-1"})
	require.Error(t, err, "a failing attempt opens the primary breaker")

	selected, err = router.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
	require.NoError(t, err)
	assert.Equal(t, "fallback", selected.Capabilities().Provider, "the open breaker is excluded from the selection")

	// Both providers are unavailable: the selection is rejected.
	fallback.alwaysErr = fmt.Errorf("%w: down", domain.ErrTemporary)
	_, err = selected.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "k-2"})
	require.Error(t, err)
	_, err = router.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
}

func TestRouterCapsAndRuleLists(t *testing.T) {
	t.Parallel()

	gateway := &fakeGateway{capability: capabilities("sandbox", "trc20", "USDT", 0, 0)}
	rules := []domain.RouteRule{{
		Provider: "sandbox",
		Networks: []string{"erc20", "trc20"},
		Assets:   []string{"BTC", "USDT"},
		Priority: 0,
		Weight:   1,
		Enabled:  true,
	}}
	router := newTestRouter(t, rules, []ProviderRegistration{{Name: "sandbox", Gateway: gateway}}, testConfig())

	selected, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
	require.NoError(t, err, "match on the network and asset lists")
	assert.Equal(t, "sandbox", selected.Capabilities().Provider)

	// The provider network asset does not match the request.
	ethGateway := &fakeGateway{capability: capabilities("sandbox", "trc20", "ETH", 0, 0)}
	otherRouter := newTestRouter(t, rules, []ProviderRegistration{{Name: "sandbox", Gateway: ethGateway}}, testConfig())
	_, err = otherRouter.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
	require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
}

func TestRouterZeroWeightGuards(t *testing.T) {
	t.Parallel()

	first := &fakeGateway{capability: capabilities("first", "trc20", "USDT", 0, 0)}
	second := &fakeGateway{capability: capabilities("second", "trc20", "USDT", 0, 0)}

	// An invalid (corrupted) source returns weight 0: the candidate is discarded.
	rules := []domain.RouteRule{
		{Provider: "first", Priority: 0, Weight: 0, Enabled: true},
		{Provider: "second", Priority: 1, Weight: 1, Enabled: true},
	}
	router := newTestRouter(t, rules, []ProviderRegistration{
		{Name: "first", Gateway: first},
		{Name: "second", Gateway: second},
	}, testConfig())
	selected, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
	require.NoError(t, err)
	assert.Equal(t, "second", selected.Capabilities().Provider)

	// All weights are zero: a rejection instead of an IntN panic.
	broken := newTestRouter(t, []domain.RouteRule{
		{Provider: "first", Priority: 0, Weight: 0, Enabled: true},
	}, []ProviderRegistration{{Name: "first", Gateway: first}}, testConfig())
	_, err = broken.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
}

func TestRouterDuplicateRulesSameProvider(t *testing.T) {
	t.Parallel()

	gateway := &fakeGateway{capability: capabilities("sandbox", "trc20", "USDT", 0, 0)}
	rules := []domain.RouteRule{
		{Provider: "sandbox", Priority: 0, Weight: 1, Enabled: true},
		{Provider: "sandbox", Priority: 0, Weight: 2, Enabled: true},
	}
	router := newTestRouter(t, rules, []ProviderRegistration{{Name: "sandbox", Gateway: gateway}}, testConfig())

	for i := 0; i < 20; i++ {
		selected, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 500))
		require.NoError(t, err)
		assert.Equal(t, "sandbox", selected.Capabilities().Provider)
	}
}

func TestRouterConcurrentSelect(t *testing.T) {
	t.Parallel()

	gateway := &fakeGateway{capability: capabilities("sandbox", "trc20", "USDT", 0, 0)}
	router := newTestRouter(t, []domain.RouteRule{
		{Provider: "sandbox", Priority: 0, Weight: 3, Enabled: true},
	}, []ProviderRegistration{{Name: "sandbox", Gateway: gateway}}, testConfig())
	// Without WithRouterRand, the thread-safe package-level rand.IntN is used.
	router.pick = rand.IntN

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for worker := 0; worker < len(errs); worker++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 500)); err != nil {
					errs[idx] = err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
}

func TestRouterCapabilitiesAndProviders(t *testing.T) {
	t.Parallel()

	first := &fakeGateway{capability: capabilities("alpha", "trc20", "USDT", 0, 0)}
	second := &fakeGateway{capability: capabilities("beta", "erc20", "ETH", 0, 0)}
	router := newTestRouter(t, nil, []ProviderRegistration{
		{Name: "beta", Gateway: second},
		{Name: "alpha", Gateway: first},
	}, testConfig())

	assert.Equal(t, []string{"alpha", "beta"}, router.Providers())
	all := router.Capabilities()
	require.Len(t, all, 2)
	_, alphaHasTRC20 := all["alpha"].Networks["trc20"]
	assert.True(t, alphaHasTRC20)
	assert.Equal(t, "beta", all["beta"].Provider)
}

func TestRouterErrors(t *testing.T) {
	t.Parallel()

	gateway := &fakeGateway{capability: capabilities("sandbox", "trc20", "USDT", 0, 0)}

	t.Run("nil rules", func(t *testing.T) {
		_, err := NewRouter(nil, nil, testConfig())
		require.Error(t, err)
	})

	t.Run("empty provider name", func(t *testing.T) {
		_, err := NewRouter(&fakeRules{}, []ProviderRegistration{{Name: " ", Gateway: gateway}}, testConfig())
		require.Error(t, err)
	})

	t.Run("nil gateway", func(t *testing.T) {
		_, err := NewRouter(&fakeRules{}, []ProviderRegistration{{Name: "sandbox"}}, testConfig())
		require.Error(t, err)
	})

	t.Run("duplicate provider", func(t *testing.T) {
		_, err := NewRouter(&fakeRules{}, []ProviderRegistration{
			{Name: "sandbox", Gateway: gateway},
			{Name: "sandbox", Gateway: gateway},
		}, testConfig())
		require.Error(t, err)
	})

	t.Run("provider name mismatch", func(t *testing.T) {
		other := &fakeGateway{capability: capabilities("other", "trc20", "USDT", 0, 0)}
		_, err := NewRouter(&fakeRules{}, []ProviderRegistration{
			{Name: "sandbox", Gateway: other},
		}, testConfig())
		require.Error(t, err, "the registration name must match the adapter provider")
	})

	t.Run("rules source error", func(t *testing.T) {
		sourceErr := errors.New("db down")
		router, err := NewRouter(&fakeRules{err: sourceErr}, nil, testConfig())
		require.NoError(t, err)
		_, err = router.Select(context.Background(), invoiceRequest("trc20", "USDT", 1))
		require.ErrorIs(t, err, sourceErr)
	})

	t.Run("invalid request", func(t *testing.T) {
		router := newTestRouter(t, nil, nil, testConfig())
		_, err := router.Select(context.Background(), domain.RouteRequest{})
		require.ErrorIs(t, err, domain.ErrInvalidRoute)
	})

	t.Run("empty registry", func(t *testing.T) {
		router := newTestRouter(t, []domain.RouteRule{
			{Provider: "sandbox", Priority: 0, Weight: 1, Enabled: true},
		}, nil, testConfig())
		_, err := router.Select(context.Background(), invoiceRequest("trc20", "USDT", 1))
		require.ErrorIs(t, err, domain.ErrUnsupportedNetwork)
	})
}
