package providerruntime_test

import (
	"encoding/asn1"
	"encoding/pem"
	"testing"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-kit/ratelimit"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/gateway"
	"github.com/cbl/cbl-connector/internal/providerruntime"
)

// capabilitiesEnv is one valid capability row for every adapter.
const capabilitiesEnv = `[{"network":"usdt_trc20","asset":"USDT","min_minor":1000000,"decimals":6,"confirmations":2}]`

// envFunc returns a getter over a map.
func envFunc(env map[string]string) func(string) string {
	return func(key string) string { return env[key] }
}

// builtinConfig builds the process configuration for the in-process factories.
func builtinConfig(env map[string]string, limiters func(string, domain.RateLimitSpec) (gateway.Limiter, error)) providerruntime.Config {
	manifests := make(map[string]domain.ProviderManifest)
	for _, manifest := range domain.BuiltinManifests() {
		manifests[manifest.ID] = manifest
	}
	return providerruntime.Config{
		Getenv: envFunc(env),
		Log:    zap.NewNop(),
		Manifests: func(provider string) (domain.ProviderManifest, bool) {
			manifest, ok := manifests[provider]
			return manifest, ok
		},
		Limiters: limiters,
	}
}

// localLimiters records the providers that request a limiter.
func localLimiters(calls *[]string) func(string, domain.RateLimitSpec) (gateway.Limiter, error) {
	return func(provider string, spec domain.RateLimitSpec) (gateway.Limiter, error) {
		*calls = append(*calls, provider)
		return ratelimit.NewLocal(ratelimit.Config{Burst: spec.Burst, RefillInterval: spec.RefillInterval})
	}
}

// testKeyPEM generates a throwaway secp256k1 SEC1 key for the BitPay factory.
func testKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := secp256k1.GeneratePrivateKey()
	require.NoError(t, err)
	der, err := asn1.Marshal(struct {
		Version    int
		PrivateKey []byte
	}{Version: 1, PrivateKey: key.Serialize()})
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))
}

// capabilitySource is the subset of the adapter SPI the test asserts.
type capabilitySource interface {
	Provider() string
	Capabilities() domain.Capabilities
}

func TestBuiltinNoConfiguration(t *testing.T) {
	limiterCalls := make([]string, 0)
	registry, err := providerruntime.NewRegistry(providerruntime.BuiltinFactories()...)
	require.NoError(t, err)

	runtimes, err := registry.Build(builtinConfig(map[string]string{}, localLimiters(&limiterCalls)))
	require.NoError(t, err)
	assert.Empty(t, runtimes, "every provider is unconfigured without a base URL")
	assert.Empty(t, limiterCalls)
}

func TestBuiltinBTCPay(t *testing.T) {
	limiterCalls := make([]string, 0)
	env := map[string]string{
		"CONNECTOR_BTCPAY_BASE_URL":       "http://mock-provider.local",
		"CONNECTOR_BTCPAY_STORE_ID":       "store-1",
		"CONNECTOR_BTCPAY_API_TOKEN":      "token",
		"CONNECTOR_BTCPAY_WEBHOOK_SECRET": "secret",
		"CONNECTOR_BTCPAY_CAPABILITIES":   capabilitiesEnv,
		"CONNECTOR_BTCPAY_TIMEOUT":        "2s",
	}
	registry, err := providerruntime.NewRegistry(providerruntime.BuiltinFactories()...)
	require.NoError(t, err)

	runtimes, err := registry.Build(builtinConfig(env, localLimiters(&limiterCalls)))
	require.NoError(t, err)
	require.Len(t, runtimes, 1)
	assert.Equal(t, "btcpay", runtimes[0].Provider())
	assert.Equal(t, "btcpay", runtimes[0].Manifest().ID)
	assert.True(t, runtimes[0].Manifest().FetchMandatory)
	require.NotNil(t, runtimes[0].Gateway())
	assert.Empty(t, limiterCalls, "the BTCPay manifest declares no rate limit")

	source, ok := runtimes[0].Gateway().(capabilitySource)
	require.True(t, ok, "the runtime gateway exposes the adapter capabilities")
	assert.Equal(t, "btcpay", source.Provider())
	assert.Contains(t, source.Capabilities().Networks, "usdt_trc20")
}

func TestBuiltinCoinGateAndNowPayments(t *testing.T) {
	limiterCalls := make([]string, 0)
	env := map[string]string{
		"CONNECTOR_COINGATE_BASE_URL":        "http://mock-provider.local",
		"CONNECTOR_COINGATE_API_TOKEN":       "token",
		"CONNECTOR_COINGATE_IPN_TOKEN":       "ipn",
		"CONNECTOR_COINGATE_CAPABILITIES":    capabilitiesEnv,
		"CONNECTOR_NOWPAYMENTS_BASE_URL":     "http://mock-provider.local",
		"CONNECTOR_NOWPAYMENTS_API_KEY":      "token",
		"CONNECTOR_NOWPAYMENTS_IPN_SECRET":   "secret",
		"CONNECTOR_NOWPAYMENTS_CAPABILITIES": capabilitiesEnv,
	}
	registry, err := providerruntime.NewRegistry(providerruntime.BuiltinFactories()...)
	require.NoError(t, err)

	runtimes, err := registry.Build(builtinConfig(env, localLimiters(&limiterCalls)))
	require.NoError(t, err)
	require.Len(t, runtimes, 2)
	assert.Equal(t, []string{"coingate", "nowpayments"}, limiterCalls, "both manifests declare rate limits")
	providers := []string{runtimes[0].Provider(), runtimes[1].Provider()}
	assert.ElementsMatch(t, []string{"coingate", "nowpayments"}, providers)
	require.NoError(t, providerruntime.Close(runtimes))
}

func TestBuiltinBitPay(t *testing.T) {
	limiterCalls := make([]string, 0)
	env := map[string]string{
		"CONNECTOR_BITPAY_BASE_URL":        "http://mock-provider.local",
		"CONNECTOR_BITPAY_API_TOKEN":       "token",
		"CONNECTOR_BITPAY_PRIVATE_KEY_PEM": testKeyPEM(t),
		"CONNECTOR_BITPAY_WEBHOOK_SECRET":  "secret",
		"CONNECTOR_BITPAY_CAPABILITIES":    capabilitiesEnv,
	}
	registry, err := providerruntime.NewRegistry(providerruntime.BuiltinFactories()...)
	require.NoError(t, err)

	runtimes, err := registry.Build(builtinConfig(env, localLimiters(&limiterCalls)))
	require.NoError(t, err)
	require.Len(t, runtimes, 1)
	assert.Equal(t, "bitpay", runtimes[0].Provider())
	assert.Equal(t, []string{"bitpay"}, limiterCalls)
}

func TestBuiltinEnabledAllowlist(t *testing.T) {
	env := map[string]string{
		"CONNECTOR_COINGATE_BASE_URL":     "http://mock-provider.local",
		"CONNECTOR_COINGATE_API_TOKEN":    "token",
		"CONNECTOR_COINGATE_IPN_TOKEN":    "ipn",
		"CONNECTOR_COINGATE_CAPABILITIES": capabilitiesEnv,
		"CONNECTOR_BTCPAY_BASE_URL":       "http://mock-provider.local",
		"CONNECTOR_BTCPAY_STORE_ID":       "store-1",
		"CONNECTOR_BTCPAY_API_TOKEN":      "token",
		"CONNECTOR_BTCPAY_WEBHOOK_SECRET": "secret",
		"CONNECTOR_BTCPAY_CAPABILITIES":   capabilitiesEnv,
	}
	calls := make([]string, 0)
	config := builtinConfig(env, localLimiters(&calls))
	config.Enabled = func(provider string) bool { return provider == "coingate" }
	registry, err := providerruntime.NewRegistry(providerruntime.BuiltinFactories()...)
	require.NoError(t, err)

	runtimes, err := registry.Build(config)
	require.NoError(t, err)
	require.Len(t, runtimes, 1)
	assert.Equal(t, "coingate", runtimes[0].Provider())
}

func TestBuiltinErrors(t *testing.T) {
	cases := []struct {
		id  string
		env map[string]string
	}{
		{"R13 invalid capabilities", map[string]string{
			"CONNECTOR_COINGATE_BASE_URL":     "http://mock-provider.local",
			"CONNECTOR_COINGATE_API_TOKEN":    "token",
			"CONNECTOR_COINGATE_IPN_TOKEN":    "ipn",
			"CONNECTOR_COINGATE_CAPABILITIES": `[{"network":"usdt_trc20"}]`,
		}},
		{"R15 missing secret", map[string]string{
			"CONNECTOR_COINGATE_BASE_URL":     "http://mock-provider.local",
			"CONNECTOR_COINGATE_IPN_TOKEN":    "ipn",
			"CONNECTOR_COINGATE_CAPABILITIES": capabilitiesEnv,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			registry, err := providerruntime.NewRegistry(providerruntime.BuiltinFactories()...)
			require.NoError(t, err)
			calls := make([]string, 0)
			_, err = registry.Build(builtinConfig(tc.env, localLimiters(&calls)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "coingate")
		})
	}
}

func TestBuiltinMissingManifest(t *testing.T) {
	registry, err := providerruntime.NewRegistry(providerruntime.Factory{
		Provider: "coingate",
		Build:    providerruntime.BuiltinFactories()[2].Build,
	})
	require.NoError(t, err)
	config := providerruntime.Config{
		Getenv: envFunc(map[string]string{"CONNECTOR_COINGATE_BASE_URL": "http://mock-provider.local"}),
		Log:    zap.NewNop(),
		Manifests: func(string) (domain.ProviderManifest, bool) {
			return domain.ProviderManifest{}, false
		},
	}
	_, err = registry.Build(config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manifest")
}
