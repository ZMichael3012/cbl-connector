package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrProviderPayoutNotFound reports an absent provider payout mapping.
var ErrProviderPayoutNotFound = errors.New("storage: provider payout not found")

// ProviderPayoutRow describes a provider_payouts table row (17.11).
type ProviderPayoutRow struct {
	// PayoutID sets the platform payout UUID (the primary key).
	PayoutID string `gorm:"column:payout_id;type:uuid;primaryKey"`
	// Provider sets the provider that accepted the payout.
	Provider string `gorm:"column:provider"`
	// GatewayPayoutID sets the payout identifier at the provider.
	GatewayPayoutID *string `gorm:"column:gateway_payout_id"`
	// State sets the mapping state: submitted, confirmed or failed.
	State string `gorm:"column:state"`
	// AmountMinor sets the payout amount in minor units.
	AmountMinor int64 `gorm:"column:amount_minor"`
	// Asset sets the payout asset.
	Asset string `gorm:"column:asset"`
	// Decimals sets the payout scale.
	Decimals int `gorm:"column:decimals"`
	// Network sets the payout network.
	Network string `gorm:"column:network"`
	// AddressHash sets the sha256 of the destination address (no PII at rest).
	AddressHash string `gorm:"column:address_hash"`
	// TxID sets the provider transaction hash after confirmation.
	TxID *string `gorm:"column:txid"`
	// FeeMinor sets the provider fee in minor units.
	FeeMinor *int64 `gorm:"column:fee_minor"`
	// FeeAsset sets the provider fee asset.
	FeeAsset *string `gorm:"column:fee_asset"`
	// FeeDecimals sets the provider fee scale.
	FeeDecimals *int `gorm:"column:fee_decimals"`
	// Reason sets the failure reason.
	Reason *string `gorm:"column:reason"`
	// CreatedAt sets the row creation time.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt sets the last polling time.
	UpdatedAt time.Time `gorm:"column:updated_at"`
	// ConfirmedAt sets the confirmation time.
	ConfirmedAt *time.Time `gorm:"column:confirmed_at"`
	// FailedAt sets the failure time.
	FailedAt *time.Time `gorm:"column:failed_at"`
}

// TableName specifies the provider_payouts table name.
func (ProviderPayoutRow) TableName() string { return "provider_payouts" }

// ProviderPayoutPatch carries the resolution fields of the status worker.
type ProviderPayoutPatch struct {
	// TXID sets the provider transaction hash.
	TXID *string
	// FeeMinor sets the provider fee in minor units.
	FeeMinor *int64
	// FeeAsset sets the provider fee asset.
	FeeAsset *string
	// FeeDecimals sets the provider fee scale.
	FeeDecimals *int
	// Reason sets the failure reason.
	Reason *string
}

// ProviderPayoutsRepository stores the provider payout mapping.
type ProviderPayoutsRepository struct {
	db *gorm.DB
}

// NewProviderPayoutsRepository creates a ProviderPayoutsRepository.
func NewProviderPayoutsRepository(db *gorm.DB) *ProviderPayoutsRepository {
	return &ProviderPayoutsRepository{db: db}
}

// LockPayoutTx serializes deliveries of one payout inside the transaction: the
// advisory transaction lock is released on commit or rollback, so concurrent
// commands of one payout cannot both pass the replay check (17.11).
func (r *ProviderPayoutsRepository) LockPayoutTx(ctx context.Context, tx *gorm.DB, payoutID string) error {
	db := tx
	if db == nil {
		db = r.db
	}
	err := db.WithContext(ctx).Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", payoutID).Error
	if err != nil {
		return fmt.Errorf("storage: lock provider payout %s: %w", payoutID, err)
	}
	return nil
}

// Get returns the mapping of a platform payout.
func (r *ProviderPayoutsRepository) Get(ctx context.Context, payoutID string) (ProviderPayoutRow, error) {
	var row ProviderPayoutRow
	err := r.db.WithContext(ctx).Where("payout_id = ?", payoutID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ProviderPayoutRow{}, fmt.Errorf("%w: %s", ErrProviderPayoutNotFound, payoutID)
	}
	if err != nil {
		return ProviderPayoutRow{}, fmt.Errorf("storage: get provider payout %s: %w", payoutID, err)
	}
	return row, nil
}

// InsertSubmittedTx records a submitted payout in the given transaction. The
// boolean reports whether the row was created: false means the payout is
// already recorded and the provider must not be called again.
func (r *ProviderPayoutsRepository) InsertSubmittedTx(ctx context.Context, tx *gorm.DB, row ProviderPayoutRow) (bool, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	row.State = "submitted"
	result := db.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "payout_id"}}, DoNothing: true}).
		Create(&row)
	if result.Error != nil {
		return false, fmt.Errorf("storage: insert provider payout %s: %w", row.PayoutID, result.Error)
	}
	return result.RowsAffected == 1, nil
}

// ListPending returns submitted payouts not refreshed after the given time,
// oldest first.
func (r *ProviderPayoutsRepository) ListPending(ctx context.Context, limit int, refreshedBefore time.Time) ([]ProviderPayoutRow, error) {
	var rows []ProviderPayoutRow
	err := r.db.WithContext(ctx).
		Where("state = ? AND updated_at <= ?", "submitted", refreshedBefore).
		Order("updated_at").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("storage: list pending provider payouts: %w", err)
	}
	return rows, nil
}

// TouchSubmittedTx refreshes the polling timestamp of a still-submitted payout.
func (r *ProviderPayoutsRepository) TouchSubmittedTx(ctx context.Context, tx *gorm.DB, payoutID string, at time.Time) error {
	db := tx
	if db == nil {
		db = r.db
	}
	err := db.WithContext(ctx).
		Model(&ProviderPayoutRow{}).
		Where("payout_id = ? AND state = ?", payoutID, "submitted").
		Update("updated_at", at).Error
	if err != nil {
		return fmt.Errorf("storage: touch provider payout %s: %w", payoutID, err)
	}
	return nil
}

// ResolveTx moves a submitted payout to a terminal state in the given
// transaction. The boolean reports whether this call won the race: false means
// the payout was already resolved and no event may be published.
func (r *ProviderPayoutsRepository) ResolveTx(ctx context.Context, tx *gorm.DB, payoutID, state string, patch ProviderPayoutPatch, at time.Time) (bool, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	updates := map[string]any{
		"state":        state,
		"updated_at":   at,
		"txid":         patch.TXID,
		"fee_minor":    patch.FeeMinor,
		"fee_asset":    patch.FeeAsset,
		"fee_decimals": patch.FeeDecimals,
		"reason":       patch.Reason,
	}
	switch state {
	case "confirmed":
		updates["confirmed_at"] = at
	case "failed":
		updates["failed_at"] = at
	}
	result := db.WithContext(ctx).
		Model(&ProviderPayoutRow{}).
		Where("payout_id = ? AND state = ?", payoutID, "submitted").
		Updates(updates)
	if result.Error != nil {
		return false, fmt.Errorf("storage: resolve provider payout %s: %w", payoutID, result.Error)
	}
	return result.RowsAffected == 1, nil
}
