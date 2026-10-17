package coingate_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/conformance"
	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/coingate"
)

func TestConformanceCoinGate(t *testing.T) {
	fixtures, ok := conformance.Fixtures("coingate")
	require.True(t, ok)

	conformance.Run(t, conformance.Harness{
		Provider: "coingate",
		Manifest: manifestOf(t),
		New: func(_ *testing.T, baseURL string) domain.PaymentGateway {
			adapter, err := coingate.New(coingate.Config{
				BaseURL:  baseURL,
				APIToken: "conf-token",
				IPNToken: fixtures.Secret,
				Capabilities: domain.Capabilities{
					Networks: map[string]domain.NetworkCapability{
						"usdt_trc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
					},
				},
			})
			require.NoError(t, err)
			return adapter
		},
	})
}

// manifestOf returns the built-in coingate manifest.
func manifestOf(t *testing.T) domain.ProviderManifest {
	t.Helper()
	for _, manifest := range domain.BuiltinManifests() {
		if manifest.ID == "coingate" {
			return manifest
		}
	}
	require.FailNow(t, "the coingate manifest must exist")
	return domain.ProviderManifest{}
}
