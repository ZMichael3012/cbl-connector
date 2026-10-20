//go:build integration

package storage_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-connector/internal/storage"
)

func setupProviderPayoutsDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("connector_payouts"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func providerPayoutRow(payoutID string) storage.ProviderPayoutRow {
	gatewayPayoutID := "po-" + payoutID[len(payoutID)-4:]
	now := time.Now().UTC()
	return storage.ProviderPayoutRow{
		PayoutID:        payoutID,
		Provider:        "coingate",
		GatewayPayoutID: &gatewayPayoutID,
		AmountMinor:     100_000,
		Asset:           "USDT",
		Decimals:        2,
		Network:         "trc20",
		AddressHash:     "deadbeef",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func TestIntegrationProviderPayoutsReplayBarrier(t *testing.T) {
	db := setupProviderPayoutsDB(t)
	repo := storage.NewProviderPayoutsRepository(db)
	ctx := context.Background()
	const payoutID = "01a14f70-0000-7000-8000-000000000401"

	inserted, err := repo.InsertSubmittedTx(ctx, nil, providerPayoutRow(payoutID))
	require.NoError(t, err)
	assert.True(t, inserted)

	inserted, err = repo.InsertSubmittedTx(ctx, nil, providerPayoutRow(payoutID))
	require.NoError(t, err)
	assert.False(t, inserted, "a second insert of the payout id is a replay")

	row, err := repo.Get(ctx, payoutID)
	require.NoError(t, err)
	assert.Equal(t, "submitted", row.State)
	assert.Equal(t, "coingate", row.Provider)

	_, err = repo.Get(ctx, "01a14f70-0000-7000-8000-0000000004ff")
	require.ErrorIs(t, err, storage.ErrProviderPayoutNotFound)
}

func TestIntegrationProviderPayoutsPendingAndResolve(t *testing.T) {
	db := setupProviderPayoutsDB(t)
	repo := storage.NewProviderPayoutsRepository(db)
	ctx := context.Background()
	const payoutID = "01a14f70-0000-7000-8000-000000000402"

	row := providerPayoutRow(payoutID)
	row.UpdatedAt = time.Now().UTC().Add(-time.Hour)
	inserted, err := repo.InsertSubmittedTx(ctx, nil, row)
	require.NoError(t, err)
	require.True(t, inserted)

	pending, err := repo.ListPending(ctx, 10, time.Now().UTC().Add(-time.Minute))
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, payoutID, pending[0].PayoutID)

	fresh, err := repo.ListPending(ctx, 10, time.Now().UTC().Add(-time.Hour*2))
	require.NoError(t, err)
	assert.Empty(t, fresh, "a freshly touched row is not pending")

	txid := "tx-1"
	feeMinor := int64(250)
	feeAsset := "USDT"
	feeDecimals := 2
	applied, err := repo.ResolveTx(ctx, nil, payoutID, "confirmed", storage.ProviderPayoutPatch{
		TXID: &txid, FeeMinor: &feeMinor, FeeAsset: &feeAsset, FeeDecimals: &feeDecimals,
	}, time.Now().UTC())
	require.NoError(t, err)
	assert.True(t, applied)

	applied, err = repo.ResolveTx(ctx, nil, payoutID, "failed", storage.ProviderPayoutPatch{}, time.Now().UTC())
	require.NoError(t, err)
	assert.False(t, applied, "a resolved payout cannot be resolved twice")

	resolved, err := repo.Get(ctx, payoutID)
	require.NoError(t, err)
	assert.Equal(t, "confirmed", resolved.State)
	require.NotNil(t, resolved.TxID)
	assert.Equal(t, "tx-1", *resolved.TxID)
	require.NotNil(t, resolved.ConfirmedAt)

	// Touching a resolved payout changes nothing (no longer submitted).
	require.NoError(t, repo.TouchSubmittedTx(ctx, nil, payoutID, time.Now().UTC().Add(time.Minute)))
	after, err := repo.Get(ctx, payoutID)
	require.NoError(t, err)
	assert.Equal(t, resolved.UpdatedAt.Unix(), after.UpdatedAt.Unix())

	// The stored mapping is not PII: the row has the address hash only.
	assert.NotContains(t, resolved.AddressHash, "TXyz")

	var count int64
	require.NoError(t, db.Table("provider_payouts").Where("payout_id = ?", payoutID).Count(&count).Error)
	assert.EqualValues(t, 1, count)
}

func TestIntegrationProviderPayoutsLockSerializes(t *testing.T) {
	db := setupProviderPayoutsDB(t)
	repo := storage.NewProviderPayoutsRepository(db)
	ctx := context.Background()
	const payoutID = "01a14f70-0000-7000-8000-000000000403"

	holder := db.Begin()
	require.NoError(t, holder.Error)
	require.NoError(t, repo.LockPayoutTx(ctx, holder, payoutID))

	acquired := make(chan error, 1)
	go func() {
		tx := db.Begin()
		if tx.Error != nil {
			acquired <- tx.Error
			return
		}
		err := repo.LockPayoutTx(ctx, tx, payoutID)
		_ = tx.Rollback()
		acquired <- err
	}()

	select {
	case err := <-acquired:
		t.Fatalf("the advisory lock must block the second transaction, got %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, holder.Commit().Error)
	require.NoError(t, <-acquired, "the second transaction proceeds after the first commit")
}
