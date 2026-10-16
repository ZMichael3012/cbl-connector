package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cbl/cbl-connector/internal/domain"
)

// ErrProviderInvoiceNotFound reports a missing provider invoice mapping.
var ErrProviderInvoiceNotFound = errors.New("storage: provider invoice mapping not found")

// ProviderInvoicesRepository stores the payment to provider invoice mapping
// (17.2): webhook normalization and timeout recovery resolve the platform
// payment through it.
type ProviderInvoicesRepository struct {
	db *gorm.DB
}

// NewProviderInvoicesRepository creates the mapping repository.
func NewProviderInvoicesRepository(db *gorm.DB) *ProviderInvoicesRepository {
	return &ProviderInvoicesRepository{db: db}
}

// Save inserts the mapping; a repeated save of the same (provider, gateway_id)
// returns the stored row with created=false, so a retried invoice creation
// never forks the mapping (idempotency, T-CONNECTOR-07).
func (r *ProviderInvoicesRepository) Save(ctx context.Context, invoice domain.ProviderInvoice) (domain.ProviderInvoice, bool, error) {
	if err := invoice.Validate(); err != nil {
		return domain.ProviderInvoice{}, false, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return domain.ProviderInvoice{}, false, fmt.Errorf("storage: generate mapping id: %w", err)
	}
	row := ProviderInvoiceRow{
		ID:          id.String(),
		Provider:    invoice.Provider,
		GatewayID:   invoice.GatewayID,
		PaymentID:   invoice.PaymentID,
		OrderID:     invoice.OrderID,
		RequestHash: invoice.RequestHash,
		CreatedAt:   time.Now().UTC(),
	}
	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "provider"}, {Name: "gateway_id"}},
			DoNothing: true,
		}).
		Create(&row)
	if result.Error != nil {
		return domain.ProviderInvoice{}, false, fmt.Errorf("storage: save provider invoice: %w", result.Error)
	}
	if result.RowsAffected == 1 {
		return rowToDomain(row), true, nil
	}
	stored, err := r.byGatewayID(ctx, invoice.Provider, invoice.GatewayID)
	if err != nil {
		return domain.ProviderInvoice{}, false, err
	}
	return stored, false, nil
}

// ByGatewayID resolves the mapping of a provider webhook by its gateway id.
func (r *ProviderInvoicesRepository) ByGatewayID(ctx context.Context, provider, gatewayID string) (domain.ProviderInvoice, error) {
	return r.byGatewayID(ctx, provider, gatewayID)
}

// ByOrderID resolves the mapping by the order reference sent to the provider;
// it serves the invoice recovery after a create timeout (LookupByOrderID).
func (r *ProviderInvoicesRepository) ByOrderID(ctx context.Context, provider, orderID string) (domain.ProviderInvoice, error) {
	var row ProviderInvoiceRow
	err := r.db.WithContext(ctx).
		Where("provider = ? AND order_id = ?", provider, orderID).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.ProviderInvoice{}, fmt.Errorf("%w: provider %q order %q", ErrProviderInvoiceNotFound, provider, orderID)
	}
	if err != nil {
		return domain.ProviderInvoice{}, fmt.Errorf("storage: load provider invoice by order: %w", err)
	}
	return rowToDomain(row), nil
}

// byGatewayID loads the mapping inside the repository.
func (r *ProviderInvoicesRepository) byGatewayID(ctx context.Context, provider, gatewayID string) (domain.ProviderInvoice, error) {
	var row ProviderInvoiceRow
	err := r.db.WithContext(ctx).
		Where("provider = ? AND gateway_id = ?", provider, gatewayID).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.ProviderInvoice{}, fmt.Errorf("%w: provider %q gateway %q", ErrProviderInvoiceNotFound, provider, gatewayID)
	}
	if err != nil {
		return domain.ProviderInvoice{}, fmt.Errorf("storage: load provider invoice by gateway: %w", err)
	}
	return rowToDomain(row), nil
}

// rowToDomain converts a storage row to the domain mapping.
func rowToDomain(row ProviderInvoiceRow) domain.ProviderInvoice {
	return domain.ProviderInvoice{
		Provider:    row.Provider,
		GatewayID:   row.GatewayID,
		PaymentID:   row.PaymentID,
		OrderID:     row.OrderID,
		RequestHash: row.RequestHash,
		CreatedAt:   row.CreatedAt,
	}
}
