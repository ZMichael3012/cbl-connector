package domain_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
)

func TestManifestVerifyFieldGating(t *testing.T) {
	t.Parallel()
	base := domain.ProviderManifest{ //nolint:gosec // G101: secret keys are Vault reference names, not credentials
		Version:            domain.ManifestVersionV1,
		ID:                 "provider",
		DisplayName:        "Provider",
		Operations:         []string{domain.OperationInvoice},
		VerifyStrategy:     domain.VerifyStrategyRawHMAC,
		VerifyHeader:       "x-signature",
		SecretPath:         "secret/cbl/{env}/provider",
		SecretKeys:         []string{"secret"},
		CapabilitiesSource: domain.CapabilitiesSourceAPI,
	}
	require.NoError(t, base.Validate())

	encoded := base
	encoded.VerifyEncoding = "base64"
	require.NoError(t, encoded.Validate(), "raw_hmac accepts an encoding")

	canonical := base
	canonical.VerifyStrategy = domain.VerifyStrategyCanonicalJSONHMAC
	canonical.VerifyEncoding = "base64"
	require.Error(t, canonical.Validate(), "canonical_json_hmac must reject verify_encoding")

	unknown := base
	unknown.VerifyEncoding = "rot13"
	require.Error(t, unknown.Validate(), "an unknown encoding must be rejected")

	prefixed := base
	prefixed.VerifyPrefix = "sha256="
	require.NoError(t, prefixed.Validate())

	tokenEcho := base
	tokenEcho.VerifyStrategy = domain.VerifyStrategyTokenEcho
	tokenEcho.VerifyHeader = ""
	tokenEcho.VerifyField = "token"
	tokenEcho.FetchMandatory = true
	tokenEcho.VerifyPrefix = "sha256="
	require.Error(t, tokenEcho.Validate(), "token_echo must reject verify_prefix")

	tooLong := base
	tooLong.VerifyPrefix = "prefix-that-is-far-too-long="
	require.Error(t, tooLong.Validate(), "an oversized prefix must be rejected")
}

func TestBuiltinManifestsAreValid(t *testing.T) {
	t.Parallel()
	manifests := domain.BuiltinManifests()
	require.NotEmpty(t, manifests)
	seen := make(map[string]struct{}, len(manifests))
	for _, manifest := range manifests {
		require.NoError(t, manifest.Validate(), "manifest %q", manifest.ID)
		_, duplicate := seen[manifest.ID]
		require.False(t, duplicate, "duplicate manifest %q", manifest.ID)
		seen[manifest.ID] = struct{}{}
	}
	require.Contains(t, seen, "coingate")
	require.Contains(t, seen, "nowpayments")
	require.Contains(t, seen, "bitpay")
	require.Contains(t, seen, "btcpay")
	require.Contains(t, seen, "mock")

	for _, manifest := range manifests {
		if manifest.ID == "btcpay" {
			require.True(t, manifest.FetchMandatory,
				"BTCPay webhooks are always fetch-to-verified (17.9)")
		}
	}
}

func TestBuiltinManifestLimits(t *testing.T) {
	t.Parallel()
	byID := make(map[string]domain.ProviderManifest)
	for _, manifest := range domain.BuiltinManifests() {
		byID[manifest.ID] = manifest
	}
	coingate := byID["coingate"]
	require.Equal(t, domain.VerifyStrategyTokenEcho, coingate.VerifyStrategy)
	require.True(t, coingate.FetchMandatory, "the CoinGate status model always needs the fetch")
	require.Positive(t, coingate.RateLimit.Burst)
	require.Less(t, coingate.RateLimit.RefillInterval, time.Second, "180/min is below one token per second")

	nowpayments := byID["nowpayments"]
	require.Equal(t, domain.VerifyStrategyCanonicalJSONHMAC, nowpayments.VerifyStrategy)
	require.Equal(t, "x-nowpayments-sig", nowpayments.VerifyHeader)
	require.False(t, contains(nowpayments.Operations, domain.OperationRefund), "NOWPayments refunds are manual (decision 8)")

	bitpay := byID["bitpay"]
	require.Equal(t, domain.VerifyStrategyRawHMAC, bitpay.VerifyStrategy)
	require.True(t, bitpay.FetchMandatory)

	btcpay := byID["btcpay"]
	require.Zero(t, btcpay.RateLimit.Burst, "our own stand has no provider limits")

	mock := byID["mock"]
	require.True(t, mock.Sandbox)
	require.Equal(t, domain.VerifyStrategyNone, mock.VerifyStrategy)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestManifestValidation(t *testing.T) {
	t.Parallel()
	valid := domain.ProviderManifest{ //nolint:gosec // G101: secret keys are Vault reference names, not credentials
		Version:            domain.ManifestVersionV1,
		ID:                 "provider",
		DisplayName:        "Provider",
		Operations:         []string{domain.OperationInvoice},
		VerifyStrategy:     domain.VerifyStrategyRawHMAC,
		VerifyHeader:       "X-Signature",
		CapabilitiesSource: domain.CapabilitiesSourceAPI,
		SecretPath:         "secret/cbl/{env}/provider",
		SecretKeys:         []string{"api_key"},
		RateLimit:          domain.RateLimitSpec{RefillInterval: time.Second, Burst: 10},
	}
	require.NoError(t, valid.Validate())

	cases := map[string]func(m *domain.ProviderManifest){
		"bad version":        func(m *domain.ProviderManifest) { m.Version = "v2" },
		"bad id":             func(m *domain.ProviderManifest) { m.ID = "Bad Id!" },
		"empty display name": func(m *domain.ProviderManifest) { m.DisplayName = "" },
		"no operations":      func(m *domain.ProviderManifest) { m.Operations = nil },
		"unknown operation":  func(m *domain.ProviderManifest) { m.Operations = []string{"swap"} },
		"duplicate operation": func(m *domain.ProviderManifest) {
			m.Operations = []string{domain.OperationInvoice, domain.OperationInvoice}
		},
		"raw without header":  func(m *domain.ProviderManifest) { m.VerifyHeader = "" },
		"token without field": func(m *domain.ProviderManifest) { m.VerifyStrategy = domain.VerifyStrategyTokenEcho },
		"token without mandate": func(m *domain.ProviderManifest) {
			m.VerifyStrategy = domain.VerifyStrategyTokenEcho
			m.VerifyField = "token"
		},
		"missing secrets":      func(m *domain.ProviderManifest) { m.SecretPath = "" },
		"unknown strategy":     func(m *domain.ProviderManifest) { m.VerifyStrategy = "magic" },
		"bad capabilities":     func(m *domain.ProviderManifest) { m.CapabilitiesSource = "manual" },
		"bad secret path":      func(m *domain.ProviderManifest) { m.SecretPath = "vault/provider" },
		"empty secret key":     func(m *domain.ProviderManifest) { m.SecretKeys = []string{" "} },
		"burst over limit":     func(m *domain.ProviderManifest) { m.RateLimit.Burst = 2_000_000 },
		"interval no burst":    func(m *domain.ProviderManifest) { m.RateLimit.Burst = 0 },
		"tiny refill interval": func(m *domain.ProviderManifest) { m.RateLimit.RefillInterval = time.Nanosecond },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			manifest := valid
			mutate(&manifest)
			require.ErrorIs(t, manifest.Validate(), domain.ErrInvalidManifest)
		})
	}
}

func TestProviderInvoiceValidation(t *testing.T) {
	t.Parallel()
	valid := domain.ProviderInvoice{
		Provider:  "coingate",
		GatewayID: "1234567",
		PaymentID: "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		OrderID:   "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
	}
	require.NoError(t, valid.Validate())

	invalid := valid
	invalid.PaymentID = "not-a-uuid"
	require.ErrorIs(t, invalid.Validate(), domain.ErrInvalidProviderInvoice)

	invalid = valid
	invalid.GatewayID = ""
	require.ErrorIs(t, invalid.Validate(), domain.ErrInvalidProviderInvoice)

	invalid = valid
	invalid.Provider = ""
	require.ErrorIs(t, invalid.Validate(), domain.ErrInvalidProviderInvoice)
}
