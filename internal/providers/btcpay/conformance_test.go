package btcpay_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/conformance"
	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/btcpay"
)

// manifestOf returns the built-in btcpay manifest.
func manifestOf(t *testing.T) domain.ProviderManifest {
	t.Helper()
	for _, manifest := range domain.BuiltinManifests() {
		if manifest.ID == "btcpay" {
			return manifest
		}
	}
	require.FailNow(t, "the btcpay manifest must exist")
	return domain.ProviderManifest{}
}

func TestConformanceBTCPay(t *testing.T) {
	fixtures, ok := conformance.Fixtures("btcpay")
	require.True(t, ok)

	conformance.Run(t, conformance.Harness{
		Provider: "btcpay",
		Manifest: manifestOf(t),
		New: func(_ *testing.T, baseURL string) domain.PaymentGateway {
			config := testConfig(baseURL)
			config.StoreID = "mock-store"
			config.WebhookSecret = fixtures.Secret
			config.Capabilities = domain.Capabilities{
				Networks: map[string]domain.NetworkCapability{
					"usdt_trc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
				},
			}
			adapter, err := btcpay.New(config)
			require.NoError(t, err)
			return adapter
		},
	})
}
