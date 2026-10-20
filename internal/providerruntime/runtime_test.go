package providerruntime_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providerruntime"
)

// fakeGateway satisfies the gateway contract without provider behavior; the
// runtime tests never call it.
type fakeGateway struct{ domain.PaymentGateway }

// fakeRuntime records its close in the shared log.
type fakeRuntime struct {
	id       string
	closed   *[]string
	closeErr error
}

// Provider returns the provider id of the fake.
func (f *fakeRuntime) Provider() string { return f.id }

// Manifest returns a manifest with the fake provider id.
func (f *fakeRuntime) Manifest() domain.ProviderManifest {
	return domain.ProviderManifest{ID: f.id}
}

// Gateway returns the fake gateway.
func (f *fakeRuntime) Gateway() domain.PaymentGateway { return fakeGateway{} }

// Close records the close and returns the configured error.
func (f *fakeRuntime) Close() error {
	if f.closed != nil {
		*f.closed = append(*f.closed, f.id)
	}
	return f.closeErr
}

// buildConfig returns the minimal valid registry configuration.
func buildConfig() providerruntime.Config {
	return providerruntime.Config{
		Getenv: func(string) string { return "" },
		Log:    zap.NewNop(),
		Manifests: func(string) (domain.ProviderManifest, bool) {
			return domain.ProviderManifest{}, false
		},
	}
}

// runtimeFactory registers a factory with a fixed runtime.
func runtimeFactory(t *testing.T, id string, runtime *fakeRuntime) providerruntime.Factory {
	t.Helper()
	return providerruntime.Factory{
		Provider: id,
		Build: func(providerruntime.Config) (providerruntime.ProviderRuntime, error) {
			return runtime, nil
		},
	}
}

func TestRegistryBuildOrder(t *testing.T) {
	closed := make([]string, 0, 2)
	registry, err := providerruntime.NewRegistry(
		runtimeFactory(t, "alpha", &fakeRuntime{id: "alpha", closed: &closed}),
		runtimeFactory(t, "beta", &fakeRuntime{id: "beta", closed: &closed}),
	)
	require.NoError(t, err)

	runtimes, err := registry.Build(buildConfig())
	require.NoError(t, err)
	require.Len(t, runtimes, 2)
	assert.Equal(t, "alpha", runtimes[0].Provider())
	assert.Equal(t, "beta", runtimes[1].Provider())
	assert.Empty(t, closed, "build does not close healthy runtimes")
	require.NoError(t, providerruntime.Close(runtimes))
	assert.Equal(t, []string{"beta", "alpha"}, closed, "close runs in reverse order")
}

func TestRegistryEnabledFilter(t *testing.T) {
	called := make([]string, 0, 2)
	factory := func(id string) providerruntime.Factory {
		return providerruntime.Factory{
			Provider: id,
			Build: func(providerruntime.Config) (providerruntime.ProviderRuntime, error) {
				called = append(called, id)
				return &fakeRuntime{id: id}, nil
			},
		}
	}
	registry, err := providerruntime.NewRegistry(factory("alpha"), factory("beta"))
	require.NoError(t, err)
	config := buildConfig()
	config.Enabled = func(provider string) bool { return provider == "beta" }

	runtimes, err := registry.Build(config)
	require.NoError(t, err)
	require.Len(t, runtimes, 1)
	assert.Equal(t, "beta", runtimes[0].Provider())
	assert.Equal(t, []string{"beta"}, called, "a disabled factory never runs")
}

func TestRegistrySkipsUnconfiguredFactory(t *testing.T) {
	registry, err := providerruntime.NewRegistry(providerruntime.Factory{
		Provider: "alpha",
		Build: func(providerruntime.Config) (providerruntime.ProviderRuntime, error) {
			return nil, nil
		},
	})
	require.NoError(t, err)

	runtimes, err := registry.Build(buildConfig())
	require.NoError(t, err)
	assert.Empty(t, runtimes)
}

func TestRegistryClosesBuiltRuntimesOnError(t *testing.T) {
	closed := make([]string, 0, 1)
	boom := errors.New("boom")
	registry, err := providerruntime.NewRegistry(
		runtimeFactory(t, "alpha", &fakeRuntime{id: "alpha", closed: &closed}),
		providerruntime.Factory{
			Provider: "beta",
			Build: func(providerruntime.Config) (providerruntime.ProviderRuntime, error) {
				return nil, boom
			},
		},
	)
	require.NoError(t, err)

	_, err = registry.Build(buildConfig())
	require.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"alpha"}, closed, "a failed build closes what was built")
}

func TestNewRegistryRejectsInvalidFactories(t *testing.T) {
	valid := runtimeFactory(t, "alpha", &fakeRuntime{id: "alpha"})
	cases := []struct {
		id        string
		factories []providerruntime.Factory
	}{
		{"R5 duplicate", []providerruntime.Factory{valid, valid}},
		{"R6 empty provider", []providerruntime.Factory{{Build: valid.Build}}},
		{"R6 nil build", []providerruntime.Factory{{Provider: "alpha"}}},
		{"R6 invalid provider", []providerruntime.Factory{{Provider: "Alpha!", Build: valid.Build}}},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			_, err := providerruntime.NewRegistry(tc.factories...)
			require.Error(t, err)
			assert.ErrorIs(t, err, providerruntime.ErrInvalidRegistry)
		})
	}
}

func TestRegistryRejectsProviderMismatch(t *testing.T) {
	closed := make([]string, 0, 1)
	registry, err := providerruntime.NewRegistry(runtimeFactory(t, "alpha", &fakeRuntime{id: "beta", closed: &closed}))
	require.NoError(t, err)

	_, err = registry.Build(buildConfig())
	require.ErrorIs(t, err, providerruntime.ErrInvalidRegistry)
	assert.Equal(t, []string{"beta"}, closed)
}

func TestCloseJoinsErrors(t *testing.T) {
	firstErr := errors.New("first close")
	secondErr := errors.New("second close")
	closed := make([]string, 0, 2)
	err := providerruntime.Close([]providerruntime.ProviderRuntime{
		&fakeRuntime{id: "alpha", closed: &closed, closeErr: firstErr},
		&fakeRuntime{id: "beta", closed: &closed, closeErr: secondErr},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, firstErr)
	assert.ErrorIs(t, err, secondErr)
	assert.Equal(t, []string{"beta", "alpha"}, closed)
	assert.NoError(t, providerruntime.Close(nil))
}
