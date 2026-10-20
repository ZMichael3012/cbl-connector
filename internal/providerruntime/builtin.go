package providerruntime

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/gateway"
	"github.com/cbl/cbl-connector/internal/providers/bitpay"
	"github.com/cbl/cbl-connector/internal/providers/btcpay"
	"github.com/cbl/cbl-connector/internal/providers/coingate"
	"github.com/cbl/cbl-connector/internal/providers/nowpayments"
)

// defaultProviderTimeout bounds one provider HTTP call.
const defaultProviderTimeout = 10 * time.Second

// BuiltinFactories returns the in-process factories of the evaluated providers
// in a stable order; the list is the only place the process names adapters, so
// extracting a runtime is a factory replacement.
func BuiltinFactories() []Factory {
	return []Factory{
		{Provider: "bitpay", Build: buildBitPay},
		{Provider: "btcpay", Build: buildBTCPay},
		{Provider: "coingate", Build: buildCoinGate},
		{Provider: "nowpayments", Build: buildNowPayments},
	}
}

// inProcessRuntime is the in-process implementation of ProviderRuntime.
type inProcessRuntime struct {
	provider string
	manifest domain.ProviderManifest
	gateway  domain.PaymentGateway
}

// Provider returns the provider id.
func (r *inProcessRuntime) Provider() string { return r.provider }

// Manifest returns the provider manifest.
func (r *inProcessRuntime) Manifest() domain.ProviderManifest { return r.manifest }

// Gateway returns the wrapped gateway.
func (r *inProcessRuntime) Gateway() domain.PaymentGateway { return r.gateway }

// Close releases the runtime; the adapters own no closable resources.
func (r *inProcessRuntime) Close() error { return nil }

// buildBitPay constructs the BitPay runtime from the process environment.
func buildBitPay(cfg Config) (ProviderRuntime, error) {
	const provider = "bitpay"
	manifest, err := manifestFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSpace(cfg.Getenv("CONNECTOR_BITPAY_BASE_URL"))
	if base == "" {
		return nil, nil
	}
	capabilities, err := capabilitiesFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	adapter, err := bitpay.New(bitpay.Config{
		BaseURL:       base,
		APIToken:      cfg.Getenv("CONNECTOR_BITPAY_API_TOKEN"),
		PrivateKeyPEM: cfg.Getenv("CONNECTOR_BITPAY_PRIVATE_KEY_PEM"),
		WebhookSecret: []byte(cfg.Getenv("CONNECTOR_BITPAY_WEBHOOK_SECRET")),
		WebhookURL:    cfg.Getenv("CONNECTOR_BITPAY_WEBHOOK_URL"),
		BuyerEmail:    cfg.Getenv("CONNECTOR_BITPAY_BUYER_EMAIL"),
		Capabilities:  capabilities,
		Timeout:       durationEnv(cfg.Log, cfg.Getenv, "CONNECTOR_BITPAY_TIMEOUT", defaultProviderTimeout),
	})
	if err != nil {
		return nil, err
	}
	wrapped, err := wrapGateway(provider, adapter, manifest, cfg)
	if err != nil {
		return nil, err
	}
	cfg.Log.Info("bitpay adapter configured", zap.String("base_url", base))
	return &inProcessRuntime{provider: provider, manifest: manifest, gateway: wrapped}, nil
}

// buildBTCPay constructs the BTCPay runtime from the process environment.
func buildBTCPay(cfg Config) (ProviderRuntime, error) {
	const provider = "btcpay"
	manifest, err := manifestFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSpace(cfg.Getenv("CONNECTOR_BTCPAY_BASE_URL"))
	if base == "" {
		return nil, nil
	}
	capabilities, err := capabilitiesFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	adapter, err := btcpay.New(btcpay.Config{
		BaseURL:       base,
		StoreID:       cfg.Getenv("CONNECTOR_BTCPAY_STORE_ID"),
		APIToken:      cfg.Getenv("CONNECTOR_BTCPAY_API_TOKEN"),
		WebhookSecret: []byte(cfg.Getenv("CONNECTOR_BTCPAY_WEBHOOK_SECRET")),
		Capabilities:  capabilities,
		Timeout:       durationEnv(cfg.Log, cfg.Getenv, "CONNECTOR_BTCPAY_TIMEOUT", defaultProviderTimeout),
	})
	if err != nil {
		return nil, err
	}
	// Every external call goes through the shared resilience stack (plan
	// 14.5.3): bulkhead, retries with jitter, breaker and attempt timeouts.
	wrapped, err := wrapGateway(provider, adapter, manifest, cfg)
	if err != nil {
		return nil, err
	}
	cfg.Log.Info("btcpay adapter configured", zap.String("base_url", base))
	return &inProcessRuntime{provider: provider, manifest: manifest, gateway: wrapped}, nil
}

// buildCoinGate constructs the CoinGate runtime from the process environment.
func buildCoinGate(cfg Config) (ProviderRuntime, error) {
	const provider = "coingate"
	manifest, err := manifestFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSpace(cfg.Getenv("CONNECTOR_COINGATE_BASE_URL"))
	if base == "" {
		return nil, nil
	}
	capabilities, err := capabilitiesFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	adapter, err := coingate.New(coingate.Config{
		BaseURL:       base,
		APIToken:      cfg.Getenv("CONNECTOR_COINGATE_API_TOKEN"),
		IPNToken:      []byte(cfg.Getenv("CONNECTOR_COINGATE_IPN_TOKEN")),
		CallbackURL:   cfg.Getenv("CONNECTOR_COINGATE_CALLBACK_URL"),
		PriceCurrency: cfg.Getenv("CONNECTOR_COINGATE_PRICE_CURRENCY"),
		Capabilities:  capabilities,
		Timeout:       durationEnv(cfg.Log, cfg.Getenv, "CONNECTOR_COINGATE_TIMEOUT", defaultProviderTimeout),
	})
	if err != nil {
		return nil, err
	}
	wrapped, err := wrapGateway(provider, adapter, manifest, cfg)
	if err != nil {
		return nil, err
	}
	cfg.Log.Info("coingate adapter configured", zap.String("base_url", base))
	return &inProcessRuntime{provider: provider, manifest: manifest, gateway: wrapped}, nil
}

// buildNowPayments constructs the NOWPayments runtime from the process
// environment.
func buildNowPayments(cfg Config) (ProviderRuntime, error) {
	const provider = "nowpayments"
	manifest, err := manifestFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSpace(cfg.Getenv("CONNECTOR_NOWPAYMENTS_BASE_URL"))
	if base == "" {
		return nil, nil
	}
	capabilities, err := capabilitiesFor(provider, cfg)
	if err != nil {
		return nil, err
	}
	adapter, err := nowpayments.New(nowpayments.Config{
		BaseURL:       base,
		APIKey:        cfg.Getenv("CONNECTOR_NOWPAYMENTS_API_KEY"),
		IPNSecret:     []byte(cfg.Getenv("CONNECTOR_NOWPAYMENTS_IPN_SECRET")),
		PriceCurrency: cfg.Getenv("CONNECTOR_NOWPAYMENTS_PRICE_CURRENCY"),
		CallbackURL:   cfg.Getenv("CONNECTOR_NOWPAYMENTS_CALLBACK_URL"),
		Capabilities:  capabilities,
		Timeout:       durationEnv(cfg.Log, cfg.Getenv, "CONNECTOR_NOWPAYMENTS_TIMEOUT", defaultProviderTimeout),
	})
	if err != nil {
		return nil, err
	}
	wrapped, err := wrapGateway(provider, adapter, manifest, cfg)
	if err != nil {
		return nil, err
	}
	cfg.Log.Info("nowpayments adapter configured", zap.String("base_url", base))
	return &inProcessRuntime{provider: provider, manifest: manifest, gateway: wrapped}, nil
}

// manifestFor resolves the manifest of a provider; a missing manifest is a
// startup error, never a silently unconfigured provider.
func manifestFor(provider string, cfg Config) (domain.ProviderManifest, error) {
	manifest, ok := cfg.Manifests(provider)
	if !ok {
		return domain.ProviderManifest{}, fmt.Errorf("provider %q manifest is not registered", provider)
	}
	return manifest, nil
}

// capabilitiesFor parses the capability rows of a provider.
func capabilitiesFor(provider string, cfg Config) (domain.Capabilities, error) {
	env := "CONNECTOR_" + strings.ToUpper(provider) + "_CAPABILITIES"
	capabilities, err := capabilitiesFromEnv(cfg.Getenv(env))
	if err != nil {
		return domain.Capabilities{}, fmt.Errorf("%s: %w", env, err)
	}
	return capabilities, nil
}

// wrapGateway adds the shared resilience stack and, when the manifest declares
// a rate limit, the rate-limit decorator.
func wrapGateway(provider string, inner domain.PaymentGateway, manifest domain.ProviderManifest, cfg Config) (domain.PaymentGateway, error) {
	wrapped := gateway.New(inner, gateway.ConfigFromEnv(cfg.Getenv), gateway.WithLogger(cfg.Log))
	if manifest.RateLimit.Burst <= 0 {
		return wrapped, nil
	}
	if cfg.Limiters == nil {
		return nil, errors.New("limiter factory is required for a provider with a rate limit")
	}
	limiter, err := cfg.Limiters(provider, manifest.RateLimit)
	if err != nil {
		return nil, fmt.Errorf("limiter: %w", err)
	}
	limited, err := gateway.NewLimited(wrapped, limiter, provider)
	if err != nil {
		return nil, err
	}
	cfg.Log.Info("provider rate limiter wired", zap.String("provider", provider), zap.Int("burst", manifest.RateLimit.Burst))
	return limited, nil
}
