package nowpayments_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/conformance"
	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/nowpayments"
)

func TestConformanceNowPayments(t *testing.T) {
	fixtures, ok := conformance.Fixtures("nowpayments")
	require.True(t, ok)

	conformance.Run(t, conformance.Harness{
		Provider: "nowpayments",
		Manifest: manifestOf(t),
		New: func(_ *testing.T, baseURL string) domain.PaymentGateway {
			adapter, err := nowpayments.New(nowpayments.Config{
				BaseURL:   baseURL,
				APIKey:    "conf-key",
				IPNSecret: fixtures.Secret,
				Capabilities: domain.Capabilities{
					Networks: map[string]domain.NetworkCapability{
						"usdttrc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
					},
				},
			})
			require.NoError(t, err)
			return adapter
		},
	})
}

// manifestOf returns the built-in nowpayments manifest.
func manifestOf(t *testing.T) domain.ProviderManifest {
	t.Helper()
	for _, manifest := range domain.BuiltinManifests() {
		if manifest.ID == "nowpayments" {
			return manifest
		}
	}
	require.FailNow(t, "the nowpayments manifest must exist")
	return domain.ProviderManifest{}
}
