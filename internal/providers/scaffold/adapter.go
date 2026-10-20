// Package scaffold is the compile-checked starting point for a new provider
// adapter (CBL-17.22). It is not registered in the runtime: copy the directory
// to internal/providers/<provider>, rename the package and the provider id,
// then implement the methods. Optional SPI interfaces are an explicit opt-in
// and are intentionally absent here.
package scaffold

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cbl/cbl-connector/internal/domain"
)

// ErrNotImplemented is returned by every scaffold method until it is replaced.
var ErrNotImplemented = errors.New("scaffold: not implemented")

// defaultTimeout bounds one provider HTTP call in the copied adapter.
const defaultTimeout = 10 * time.Second

// Config is the adapter configuration; a copied adapter reads it from the
// provider factory (CONNECTOR_<PROVIDER>_* variables) and not from this
// package.
type Config struct {
	// BaseURL is the provider API root (http or https, no credentials).
	BaseURL string
	// APIToken authenticates the adapter against the provider.
	APIToken string
	// Capabilities lists the enabled networks of the provider.
	Capabilities domain.Capabilities
	// Timeout bounds one provider call; zero means the default.
	Timeout time.Duration
}

// Adapter is the scaffold implementation of the provider SPI.
type Adapter struct {
	baseURL      *url.URL
	apiToken     string
	capabilities domain.Capabilities
	client       *http.Client
}

// Compile-time proof that the scaffold implements the SPI.
var _ domain.PaymentGateway = (*Adapter)(nil)

// New validates the configuration and builds the adapter.
func New(cfg Config) (*Adapter, error) {
	base, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, errors.New("scaffold: base url must be an absolute http(s) url")
	}
	if base.User != nil {
		return nil, errors.New("scaffold: base url must not carry userinfo")
	}
	if base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("scaffold: base url must not carry a query or fragment")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	token := strings.TrimSpace(cfg.APIToken)
	if token == "" {
		return nil, errors.New("scaffold: api token is required")
	}
	if len(cfg.Capabilities.Networks) == 0 {
		return nil, errors.New("scaffold: at least one network capability is required")
	}
	for name, network := range cfg.Capabilities.Networks {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(network.Asset) == "" {
			return nil, fmt.Errorf("scaffold: network %q needs a name and an asset", name)
		}
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	if timeout < 0 {
		return nil, errors.New("scaffold: timeout must not be negative")
	}
	return &Adapter{
		baseURL:      base,
		apiToken:     token,
		capabilities: cfg.Capabilities,
		client:       &http.Client{Timeout: timeout},
	}, nil
}

// Provider returns the provider id; a copied adapter renames it.
func (a *Adapter) Provider() string { return "scaffold" }

// Capabilities returns the configured networks.
func (a *Adapter) Capabilities() domain.Capabilities {
	networks := make(map[string]domain.NetworkCapability, len(a.capabilities.Networks))
	for name, network := range a.capabilities.Networks {
		networks[name] = network
	}
	return domain.Capabilities{Networks: networks}
}

// CreateInvoice creates a hosted invoice at the provider; replace the body with
// the provider request and response mapping.
func (a *Adapter) CreateInvoice(context.Context, domain.CreateInvoiceRequest) (domain.Invoice, error) {
	return domain.Invoice{}, ErrNotImplemented
}

// GetPayment reads one provider payment; replace the body with the provider
// request and status mapping.
func (a *Adapter) GetPayment(context.Context, string) (domain.PaymentStatus, error) {
	return domain.PaymentStatus{}, ErrNotImplemented
}

// VerifyWebhook verifies and decodes a provider delivery; replace the body with
// the provider signature scheme from the manifest.
func (a *Adapter) VerifyWebhook(map[string][]string, []byte) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{}, ErrNotImplemented
}

// CreatePayout requests a provider payout; replace the body with the provider
// request and response mapping.
func (a *Adapter) CreatePayout(context.Context, domain.PayoutRequest) (domain.Payout, error) {
	return domain.Payout{}, ErrNotImplemented
}

// GetPayout reads one provider payout; replace the body with the provider
// request and status mapping.
func (a *Adapter) GetPayout(context.Context, string) (domain.PayoutStatus, error) {
	return domain.PayoutStatus{}, ErrNotImplemented
}
