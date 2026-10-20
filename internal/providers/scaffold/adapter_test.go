package scaffold_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/scaffold"
)

// validConfig returns the minimal accepted scaffold configuration.
func validConfig() scaffold.Config {
	return scaffold.Config{
		BaseURL:  "https://provider.example/",
		APIToken: "token",
		Capabilities: domain.Capabilities{Networks: map[string]domain.NetworkCapability{
			"usdt_trc20": {Asset: "USDT"},
		}},
	}
}

func TestNewValid(t *testing.T) {
	adapter, err := scaffold.New(validConfig())
	require.NoError(t, err)
	require.NotNil(t, adapter)
	assert.Equal(t, "scaffold", adapter.Provider())
	capabilities := adapter.Capabilities()
	assert.Contains(t, capabilities.Networks, "usdt_trc20")
}

func TestNewRejects(t *testing.T) {
	cases := []struct {
		id     string
		mutate func(*scaffold.Config)
	}{
		{"invalid url", func(c *scaffold.Config) { c.BaseURL = "://bad" }},
		{"unsupported scheme", func(c *scaffold.Config) { c.BaseURL = "ftp://provider.example" }},
		{"userinfo", func(c *scaffold.Config) { c.BaseURL = "https://user:pass@provider.example" }},
		{"query", func(c *scaffold.Config) { c.BaseURL = "https://provider.example?x=1" }},
		{"empty token", func(c *scaffold.Config) { c.APIToken = " " }},
		{"no networks", func(c *scaffold.Config) { c.Capabilities = domain.Capabilities{} }},
		{"negative timeout", func(c *scaffold.Config) { c.Timeout = -time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			config := validConfig()
			tc.mutate(&config)
			_, err := scaffold.New(config)
			require.Error(t, err)
		})
	}
}

func TestMethodsAreNotImplemented(t *testing.T) {
	adapter, err := scaffold.New(validConfig())
	require.NoError(t, err)
	ctx := context.Background()

	_, err = adapter.CreateInvoice(ctx, domain.CreateInvoiceRequest{})
	require.ErrorIs(t, err, scaffold.ErrNotImplemented)
	_, err = adapter.GetPayment(ctx, "id")
	require.ErrorIs(t, err, scaffold.ErrNotImplemented)
	_, err = adapter.VerifyWebhook(nil, nil)
	require.ErrorIs(t, err, scaffold.ErrNotImplemented)
	_, err = adapter.CreatePayout(ctx, domain.PayoutRequest{})
	require.ErrorIs(t, err, scaffold.ErrNotImplemented)
	_, err = adapter.GetPayout(ctx, "id")
	require.ErrorIs(t, err, scaffold.ErrNotImplemented)
}
