//go:build integration

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
)

func TestIntegrationProviderPayoutsRefundColumn(t *testing.T) {
	db := setupProviderPayoutsDB(t)
	repo := storage.NewProviderPayoutsRepository(db)
	ctx := context.Background()
	const payoutID = "01a14f70-0000-7000-8000-000000000701"
	refundID := "01a14f70-0000-7000-8000-000000000702"

	row := providerPayoutRow(payoutID)
	row.RefundInvoiceID = &refundID
	inserted, err := repo.InsertSubmittedTx(ctx, nil, row)
	require.NoError(t, err)
	require.True(t, inserted)

	stored, err := repo.Get(ctx, payoutID)
	require.NoError(t, err)
	require.NotNil(t, stored.RefundInvoiceID)
	assert.Equal(t, refundID, *stored.RefundInvoiceID)

	plain := providerPayoutRow("01a14f70-0000-7000-8000-000000000703")
	inserted, err = repo.InsertSubmittedTx(ctx, nil, plain)
	require.NoError(t, err)
	require.True(t, inserted)
	stored, err = repo.Get(ctx, plain.PayoutID)
	require.NoError(t, err)
	assert.Nil(t, stored.RefundInvoiceID, "a normal payout has no refund marker")
}

func TestIntegrationProviderInvoicesByPaymentID(t *testing.T) {
	db := setupProviderPayoutsDB(t)
	repo := storage.NewProviderInvoicesRepository(db)
	ctx := context.Background()
	const paymentID = "01a14f70-0000-7000-8000-000000000704"

	_, _, err := repo.Save(ctx, domain.ProviderInvoice{
		Provider:    "coingate",
		GatewayID:   "inv-1",
		PaymentID:   paymentID,
		OrderID:     paymentID,
		RequestHash: "hash-1",
		CreatedAt:   time.Now().UTC(),
	})
	require.NoError(t, err)

	invoice, err := repo.ByPaymentID(ctx, paymentID)
	require.NoError(t, err)
	assert.Equal(t, "coingate", invoice.Provider)
	assert.Equal(t, "inv-1", invoice.GatewayID)

	// A payment with mappings at several providers is ambiguous: a refund
	// cannot pick the provider that took the money (17.12).
	_, _, err = repo.Save(ctx, domain.ProviderInvoice{
		Provider:    "bitpay",
		GatewayID:   "inv-2",
		PaymentID:   paymentID,
		OrderID:     paymentID,
		RequestHash: "hash-2",
		CreatedAt:   time.Now().UTC(),
	})
	require.NoError(t, err)
	_, err = repo.ByPaymentID(ctx, paymentID)
	require.ErrorIs(t, err, storage.ErrProviderInvoiceAmbiguous)

	_, err = repo.ByPaymentID(ctx, "01a14f70-0000-7000-8000-0000000007ff")
	require.ErrorIs(t, err, storage.ErrProviderInvoiceNotFound)
}
