//go:build integration

package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
)

func testMapping(paymentID string) domain.ProviderInvoice {
	return domain.ProviderInvoice{
		Provider:    "coingate",
		GatewayID:   "order-777",
		PaymentID:   paymentID,
		OrderID:     paymentID,
		RequestHash: "hash-1",
	}
}

func TestProviderInvoicesRepositoryRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := setupConnectorDB(t)
	repo := storage.NewProviderInvoicesRepository(db)

	paymentID := "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f"
	saved, created, err := repo.Save(ctx, testMapping(paymentID))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, paymentID, saved.PaymentID)
	require.False(t, saved.CreatedAt.IsZero())

	duplicate, created, err := repo.Save(ctx, testMapping(paymentID))
	require.NoError(t, err)
	require.False(t, created, "a repeated save must not create a second mapping")
	require.Equal(t, saved.GatewayID, duplicate.GatewayID)

	byGateway, err := repo.ByGatewayID(ctx, "coingate", "order-777")
	require.NoError(t, err)
	require.Equal(t, paymentID, byGateway.PaymentID)

	byOrder, err := repo.ByOrderID(ctx, "coingate", paymentID)
	require.NoError(t, err)
	require.Equal(t, "order-777", byOrder.GatewayID)

	_, err = repo.ByGatewayID(ctx, "bitpay", "order-777")
	require.ErrorIs(t, err, storage.ErrProviderInvoiceNotFound)

	_, err = repo.ByOrderID(ctx, "coingate", "018f8f5c-0000-7000-8000-000000000000")
	require.ErrorIs(t, err, storage.ErrProviderInvoiceNotFound)
}

func TestProviderInvoicesRepositoryValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := setupConnectorDB(t)
	repo := storage.NewProviderInvoicesRepository(db)

	invalid := testMapping("not-a-uuid")
	_, _, err := repo.Save(ctx, invalid)
	require.ErrorIs(t, err, domain.ErrInvalidProviderInvoice)
}

func TestProviderInvoicesDestinationRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := setupConnectorDB(t)
	repo := storage.NewProviderInvoicesRepository(db)

	// The destination survives the mapping round trip so the idempotent replay
	// of a lost create response can return it (4.2).
	invoice := testMapping("018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e70")
	invoice.Address = "TE2eAddress"
	invoice.CheckoutURL = "https://checkout.example/42"
	_, created, err := repo.Save(ctx, invoice)
	require.NoError(t, err)
	require.True(t, created)

	replayed, err := repo.ByOrderID(ctx, "coingate", invoice.PaymentID)
	require.NoError(t, err)
	require.Equal(t, "TE2eAddress", replayed.Address)
	require.Equal(t, "https://checkout.example/42", replayed.CheckoutURL)
}
