package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Provider manifest constants (SPI v1, CBL-17.2).
const (
	// ManifestVersionV1 sets the current manifest schema version.
	ManifestVersionV1 = "v1"
	// Verify strategies of the connector defense-in-depth verification
	// (T-CONNECTOR-01, CF-4 states): the ingress verifies first, the connector
	// re-checks when it has the secret.
	VerifyStrategyRawHMAC           = "raw_hmac"
	VerifyStrategyCanonicalJSONHMAC = "canonical_json_hmac"
	VerifyStrategyTokenEcho         = "token_echo"
	VerifyStrategyNone              = "none"
	// Capabilities sources of a manifest.
	CapabilitiesSourceAPI    = "api"
	CapabilitiesSourceStatic = "static"
)

// maxManifestBurst bounds the manifest bucket capacity, aligned with the kit
// limiter limit.
const maxManifestBurst = 1_000_000

// ErrInvalidManifest reports an invalid provider manifest.
var ErrInvalidManifest = errors.New("connector: invalid provider manifest")

// RateLimitSpec describes the per-provider token bucket limits; a zero value
// disables the limiter for the provider (for example our own BTCPay stand).
type RateLimitSpec struct {
	// RefillInterval sets the time to refill one token.
	RefillInterval time.Duration `json:"refill_interval"`
	// Burst sets the bucket capacity.
	Burst int `json:"burst"`
}

// ProviderManifest describes one provider with the versioned schema v1: the
// registry and the onboarding kit (17.22) validate it, and the core knows
// providers only through manifests and the adapter SPI (CBL-004, functional
// extensibility guarantee).
type ProviderManifest struct {
	// Version sets the manifest schema version; only ManifestVersionV1 is valid.
	Version string `json:"version"`
	// ID sets the provider identifier (registry key, routing and event values).
	ID string `json:"id"`
	// DisplayName sets the operator-facing name.
	DisplayName string `json:"display_name"`
	// Operations lists the supported operations: invoice, payout, refund.
	Operations []string `json:"operations"`
	// VerifyStrategy selects the connector-side re-verification strategy.
	VerifyStrategy string `json:"verify_strategy"`
	// VerifyHeader sets the signature header of raw_hmac/canonical_json_hmac.
	VerifyHeader string `json:"verify_header"`
	// VerifyField sets the token echo body field of token_echo.
	VerifyField string `json:"verify_field"`
	// CapabilitiesSource selects where capabilities come from: the provider API
	// (with a static fallback) or a static configuration only.
	CapabilitiesSource string `json:"capabilities_source"`
	// FetchMandatory forces the fetch-to-verify status re-check for every
	// delivery of the provider, regardless of the raw fetch_required flag
	// (CoinGate and BitPay status models, CBL-003 section 4).
	FetchMandatory bool `json:"fetch_mandatory"`
	// SecretPath sets the Vault path template; {env} is replaced by the stand.
	SecretPath string `json:"secret_path"`
	// SecretKeys lists the Vault keys under SecretPath.
	SecretKeys []string `json:"secret_keys"`
	// RateLimit sets the per-provider token bucket limits.
	RateLimit RateLimitSpec `json:"rate_limit"`
	// Sandbox marks dev/sandbox manifests (the mock gateway).
	Sandbox bool `json:"sandbox"`
}

// Validate checks the manifest invariants of schema v1.
func (m ProviderManifest) Validate() error {
	if m.Version != ManifestVersionV1 {
		return fmt.Errorf("%w: unsupported version %q", ErrInvalidManifest, m.Version)
	}
	if !validCode(m.ID, maxProviderLen) {
		return fmt.Errorf("%w: invalid id %q", ErrInvalidManifest, m.ID)
	}
	if strings.TrimSpace(m.DisplayName) == "" || len(m.DisplayName) > maxProviderLen {
		return fmt.Errorf("%w: invalid display_name of provider %q", ErrInvalidManifest, m.ID)
	}
	if len(m.Operations) == 0 {
		return fmt.Errorf("%w: provider %q has no operations", ErrInvalidManifest, m.ID)
	}
	seen := make(map[string]struct{}, len(m.Operations))
	for _, operation := range m.Operations {
		switch operation {
		case OperationInvoice, OperationPayout, OperationRefund:
		default:
			return fmt.Errorf("%w: provider %q has unknown operation %q", ErrInvalidManifest, m.ID, operation)
		}
		if _, duplicate := seen[operation]; duplicate {
			return fmt.Errorf("%w: provider %q repeats operation %q", ErrInvalidManifest, m.ID, operation)
		}
		seen[operation] = struct{}{}
	}
	switch m.VerifyStrategy {
	case VerifyStrategyRawHMAC, VerifyStrategyCanonicalJSONHMAC:
		if strings.TrimSpace(m.VerifyHeader) == "" {
			return fmt.Errorf("%w: provider %q: %s requires verify_header", ErrInvalidManifest, m.ID, m.VerifyStrategy)
		}
		if err := m.validateSecrets(); err != nil {
			return err
		}
	case VerifyStrategyTokenEcho:
		if strings.TrimSpace(m.VerifyField) == "" {
			return fmt.Errorf("%w: provider %q: token_echo requires verify_field", ErrInvalidManifest, m.ID)
		}
		if err := m.validateSecrets(); err != nil {
			return err
		}
		if !m.FetchMandatory {
			// The CoinGate status model always needs the provider fetch; a
			// manifest that omits the mandate would ship raws the core cannot
			// trust (mirror of the ingress rule).
			return fmt.Errorf("%w: provider %q: token_echo requires fetch_mandatory", ErrInvalidManifest, m.ID)
		}
	case VerifyStrategyNone:
		// The ingress signature_state stays the only verification (mock raws).
	default:
		return fmt.Errorf("%w: provider %q has unknown verify_strategy %q", ErrInvalidManifest, m.ID, m.VerifyStrategy)
	}
	switch m.CapabilitiesSource {
	case CapabilitiesSourceAPI, CapabilitiesSourceStatic:
	default:
		return fmt.Errorf("%w: provider %q has unknown capabilities_source %q", ErrInvalidManifest, m.ID, m.CapabilitiesSource)
	}
	if m.SecretPath != "" && !strings.HasPrefix(m.SecretPath, "secret/cbl/") {
		return fmt.Errorf("%w: provider %q: secret_path must start with secret/cbl/", ErrInvalidManifest, m.ID)
	}
	for _, key := range m.SecretKeys {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%w: provider %q has an empty secret key", ErrInvalidManifest, m.ID)
		}
	}
	if m.RateLimit.Burst < 0 || m.RateLimit.Burst > maxManifestBurst {
		return fmt.Errorf("%w: provider %q: burst %d out of range", ErrInvalidManifest, m.ID, m.RateLimit.Burst)
	}
	if m.RateLimit.Burst > 0 {
		if m.RateLimit.RefillInterval < time.Microsecond {
			return fmt.Errorf("%w: provider %q: refill interval %s is too small", ErrInvalidManifest, m.ID, m.RateLimit.RefillInterval)
		}
	} else if m.RateLimit.RefillInterval != 0 {
		return fmt.Errorf("%w: provider %q: refill interval without burst", ErrInvalidManifest, m.ID)
	}
	return nil
}

// validateSecrets checks the Vault references of a re-verifying provider: a
// provider that cannot read its secret can never verify a raw webhook
// (T-CONNECTOR-01), so the manifest must reference one.
func (m ProviderManifest) validateSecrets() error {
	if strings.TrimSpace(m.SecretPath) == "" || len(m.SecretKeys) == 0 {
		return fmt.Errorf("%w: provider %q with strategy %s requires secret_path and secret_keys",
			ErrInvalidManifest, m.ID, m.VerifyStrategy)
	}
	return nil
}

// BuiltinManifests returns the manifest catalogue of the evaluated providers
// (multi-provider decision of 16.10.2026). Limits follow the plan decisions:
// CoinGate 180/min, NOWPayments 60 rpm (width margin over the official 3 rps),
// BitPay 30 rpm; BTCPay is our own stand without our-side limits. The mock
// provider keeps the current sandbox pipeline working until 17.5.
func BuiltinManifests() []ProviderManifest {
	return []ProviderManifest{
		{ //nolint:gosec // G101: secret keys are Vault reference names, not credentials
			Version:            ManifestVersionV1,
			ID:                 "coingate",
			DisplayName:        "CoinGate",
			Operations:         []string{OperationInvoice, OperationPayout, OperationRefund},
			VerifyStrategy:     VerifyStrategyTokenEcho,
			VerifyField:        "token",
			FetchMandatory:     true,
			CapabilitiesSource: CapabilitiesSourceAPI,
			SecretPath:         "secret/cbl/{env}/coingate",
			SecretKeys:         []string{"api_token", "ipn_token"},
			RateLimit:          RateLimitSpec{RefillInterval: 60 * time.Second / 180, Burst: 60},
		},
		{ //nolint:gosec // G101: secret keys are Vault reference names, not credentials
			Version:            ManifestVersionV1,
			ID:                 "nowpayments",
			DisplayName:        "NOWPayments",
			Operations:         []string{OperationInvoice, OperationPayout},
			VerifyStrategy:     VerifyStrategyCanonicalJSONHMAC,
			VerifyHeader:       "x-nowpayments-sig",
			CapabilitiesSource: CapabilitiesSourceAPI,
			SecretPath:         "secret/cbl/{env}/nowpayments",
			SecretKeys:         []string{"api_key", "ipn_secret", "jwt_password", "totp_secret"},
			RateLimit:          RateLimitSpec{RefillInterval: time.Second, Burst: 60},
		},
		{ //nolint:gosec // G101: secret keys are Vault reference names, not credentials
			Version:            ManifestVersionV1,
			ID:                 "bitpay",
			DisplayName:        "BitPay",
			Operations:         []string{OperationInvoice, OperationPayout, OperationRefund},
			VerifyStrategy:     VerifyStrategyRawHMAC,
			VerifyHeader:       "x-signature",
			FetchMandatory:     true,
			CapabilitiesSource: CapabilitiesSourceAPI,
			SecretPath:         "secret/cbl/{env}/bitpay",
			SecretKeys:         []string{"api_token", "private_key_pem"},
			RateLimit:          RateLimitSpec{RefillInterval: 2 * time.Second, Burst: 30},
		},
		{ //nolint:gosec // G101: secret keys are Vault reference names, not credentials
			Version:            ManifestVersionV1,
			ID:                 "btcpay",
			DisplayName:        "BTCPay Server",
			Operations:         []string{OperationInvoice, OperationPayout, OperationRefund},
			VerifyStrategy:     VerifyStrategyRawHMAC,
			VerifyHeader:       "BTCPay-Sig",
			CapabilitiesSource: CapabilitiesSourceAPI,
			SecretPath:         "secret/cbl/{env}/btcpay",
			SecretKeys:         []string{"api_key", "webhook_secret"},
		},
		{
			Version:            ManifestVersionV1,
			ID:                 "mock",
			DisplayName:        "Mock gateway (dev)",
			Operations:         []string{OperationInvoice, OperationPayout},
			VerifyStrategy:     VerifyStrategyNone,
			CapabilitiesSource: CapabilitiesSourceStatic,
			Sandbox:            true,
		},
	}
}
