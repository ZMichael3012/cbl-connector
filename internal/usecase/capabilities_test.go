package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

type fakeSource struct {
	id   string
	caps domain.Capabilities
	used int
}

func (f *fakeSource) Provider() string { return f.id }

func (f *fakeSource) Capabilities() domain.Capabilities {
	f.used++
	return f.caps
}

type fakeCapCache struct {
	snapshot  []domain.Capabilities
	found     bool
	loadErr   error
	saveErr   error
	loadCalls int
	saved     []domain.Capabilities
	savedTTL  time.Duration
}

func (f *fakeCapCache) Load(_ context.Context) ([]domain.Capabilities, bool, error) {
	f.loadCalls++
	return f.snapshot, f.found, f.loadErr
}

func (f *fakeCapCache) Save(_ context.Context, capabilities []domain.Capabilities, ttl time.Duration) error {
	f.saved = capabilities
	f.savedTTL = ttl
	return f.saveErr
}

func capsFor(provider string) domain.Capabilities {
	return domain.Capabilities{
		Provider: provider,
		Networks: map[string]domain.NetworkCapability{
			"bitcoin": {Asset: "BTC", ConfirmationDepth: 2},
		},
	}
}

func TestCapabilitiesLoaderAggregatesAndCaches(t *testing.T) {
	t.Parallel()
	nowpayments := &fakeSource{id: "nowpayments", caps: capsFor("")}
	coingate := &fakeSource{id: "coingate", caps: capsFor("coingate")}
	cache := &fakeCapCache{}

	loader, err := usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{nowpayments, coingate}, cache, time.Minute, nil)
	require.NoError(t, err)

	list, err := loader.List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, list, 2)
	require.Equal(t, "coingate", list[0].Provider)
	require.Equal(t, "nowpayments", list[1].Provider, "the source id stamps the row set")
	require.Equal(t, 1, cache.loadCalls)
	require.Len(t, cache.saved, 2)
	require.Equal(t, time.Minute, cache.savedTTL)
	require.Len(t, loader.Snapshot(), 2)
}

func TestCapabilitiesLoaderServesCacheHit(t *testing.T) {
	t.Parallel()
	source := &fakeSource{id: "coingate", caps: capsFor("coingate")}
	cached := []domain.Capabilities{capsFor("coingate")}
	cache := &fakeCapCache{snapshot: cached, found: true}

	loader, err := usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{source}, cache, time.Minute, nil)
	require.NoError(t, err)

	list, err := loader.List(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, cached, list)
	require.Zero(t, source.used, "a cache hit must not touch the adapters")
	require.Nil(t, cache.saved)
}

func TestCapabilitiesLoaderFallsBackOnCacheError(t *testing.T) {
	t.Parallel()
	source := &fakeSource{id: "bitpay", caps: capsFor("bitpay")}
	cache := &fakeCapCache{loadErr: errors.New("redis down")}

	loader, err := usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{source}, cache, time.Minute, nil)
	require.NoError(t, err)

	list, err := loader.List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, 1, source.used)
	require.Len(t, cache.saved, 1, "the live aggregate still refreshes the cache")
}

func TestCapabilitiesLoaderFiltersProvider(t *testing.T) {
	t.Parallel()
	coingate := &fakeSource{id: "coingate", caps: capsFor("coingate")}
	cache := &fakeCapCache{}

	loader, err := usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{coingate}, cache, time.Minute, nil)
	require.NoError(t, err)

	list, err := loader.List(context.Background(), " coingate ")
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "coingate", list[0].Provider)
	require.Zero(t, cache.loadCalls, "the filtered view comes from the adapter directly")

	_, err = loader.List(context.Background(), "unknown")
	require.ErrorIs(t, err, domain.ErrProviderNotFound)
}

func TestCapabilitiesLoaderNilCacheAndSaveError(t *testing.T) {
	t.Parallel()
	source := &fakeSource{id: "btcpay", caps: capsFor("btcpay")}

	loader, err := usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{source}, nil, time.Minute, nil)
	require.NoError(t, err)
	list, err := loader.List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, list, 1)

	cache := &fakeCapCache{saveErr: errors.New("redis down")}
	loader, err = usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{source}, cache, time.Minute, nil)
	require.NoError(t, err)
	_, err = loader.List(context.Background(), "")
	require.NoError(t, err, "a cache save failure is not fatal")
}

func TestNewCapabilitiesLoaderValidation(t *testing.T) {
	t.Parallel()
	_, err := usecase.NewCapabilitiesLoader(nil, nil, 0, nil)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
	_, err = usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{nil}, nil, time.Minute, nil)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
	_, err = usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{&fakeSource{id: "Bad Id"}}, nil, time.Minute, nil)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
	_, err = usecase.NewCapabilitiesLoader([]usecase.CapabilitySource{&fakeSource{id: "coin"}, &fakeSource{id: "coin"}}, nil, time.Minute, nil)
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
}
