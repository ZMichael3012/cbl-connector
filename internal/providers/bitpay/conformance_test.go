package bitpay_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/conformance"
	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/bitpay"
)

func TestConformanceBitPay(t *testing.T) {
	fixtures, ok := conformance.Fixtures("bitpay")
	require.True(t, ok)

	conformance.Run(t, conformance.Harness{
		Provider: "bitpay",
		Manifest: manifestOf(t),
		New: func(t *testing.T, baseURL string) domain.PaymentGateway {
			keyPEM, _ := testKeyPEM(t, "sec1")
			adapter, err := bitpay.New(bitpay.Config{
				BaseURL:       baseURL,
				APIToken:      "conf-token",
				PrivateKeyPEM: keyPEM,
				WebhookSecret: fixtures.Secret,
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

// manifestOf returns the built-in bitpay manifest.
func manifestOf(t *testing.T) domain.ProviderManifest {
	t.Helper()
	for _, manifest := range domain.BuiltinManifests() {
		if manifest.ID == "bitpay" {
			return manifest
		}
	}
	require.FailNow(t, "the bitpay manifest must exist")
	return domain.ProviderManifest{}
}
