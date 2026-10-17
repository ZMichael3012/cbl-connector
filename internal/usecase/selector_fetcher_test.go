package usecase_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// selectorGateway is a PaymentGateway stub for the selector and fetcher tests.
type selectorGateway struct {
	provider string
	status   domain.PaymentStatus
	err      error
}

// CreateInvoice is a placeholder.
func (g *selectorGateway) CreateInvoice(context.Context, domain.CreateInvoiceRequest) (domain.Invoice, error) {
	return domain.Invoice{}, nil
}

// GetPayment returns the configured status.
func (g *selectorGateway) GetPayment(context.Context, string) (domain.PaymentStatus, error) {
	return g.status, g.err
}

// VerifyWebhook is a placeholder.
func (g *selectorGateway) VerifyWebhook(map[string][]string, []byte) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{}, nil
}

// CreatePayout is a placeholder.
func (g *selectorGateway) CreatePayout(context.Context, domain.PayoutRequest) (domain.Payout, error) {
	return domain.Payout{}, nil
}

// GetPayout is a placeholder.
func (g *selectorGateway) GetPayout(context.Context, string) (domain.PayoutStatus, error) {
	return domain.PayoutStatus{}, nil
}

// Capabilities returns the provider identity.
func (g *selectorGateway) Capabilities() domain.Capabilities {
	return domain.Capabilities{Provider: g.provider}
}

func TestStaticSelector(t *testing.T) {
	t.Parallel()
	_, err := usecase.NewStaticSelector(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrProviderUnavailable, "an empty registry fails closed with the unavailable class")

	btcpay := &selectorGateway{provider: "btcpay"}
	selector, err := usecase.NewStaticSelector(map[string]domain.PaymentGateway{"btcpay": btcpay})
	require.NoError(t, err)
	selected, err := selector.Select(context.Background(), domain.RouteRequest{Operation: "invoice", Network: "btc", Asset: "BTC"})
	require.NoError(t, err)
	assert.Equal(t, "btcpay", selected.Capabilities().Provider)

	// With two gateways the interim selector is deterministic (sorted ids).
	coingate := &selectorGateway{provider: "coingate"}
	selector, err = usecase.NewStaticSelector(map[string]domain.PaymentGateway{"coingate": coingate, "btcpay": btcpay})
	require.NoError(t, err)
	selected, err = selector.Select(context.Background(), domain.RouteRequest{Operation: "invoice", Network: "btc", Asset: "BTC"})
	require.NoError(t, err)
	assert.Equal(t, "btcpay", selected.Capabilities().Provider)
}

func TestGatewayStatusFetcher(t *testing.T) {
	t.Parallel()
	btcpay := &selectorGateway{provider: "btcpay", status: domain.PaymentStatus{
		GatewayID: "gw-1",
		Provider:  "btcpay",
		State:     domain.StateConfirmed,
		RawStatus: "Settled",
	}}
	fetcher, err := usecase.NewGatewayStatusFetcher(map[string]domain.PaymentGateway{"btcpay": btcpay})
	require.NoError(t, err)

	status, err := fetcher.FetchPaymentStatus(context.Background(), "btcpay", "gw-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, status.State)
	assert.Equal(t, "Settled", status.RawStatus)

	_, err = fetcher.FetchPaymentStatus(context.Background(), "nope", "gw-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownProvider)

	failing, err := usecase.NewGatewayStatusFetcher(map[string]domain.PaymentGateway{
		"btcpay": &selectorGateway{provider: "btcpay", err: domain.ErrTemporary},
	})
	require.NoError(t, err)
	_, err = failing.FetchPaymentStatus(context.Background(), "btcpay", "gw-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrTemporary, "the adapter error must pass through the fetcher")

	_, err = usecase.NewGatewayStatusFetcher(nil)
	require.Error(t, err)
}
