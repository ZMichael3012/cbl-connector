package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/cbl/cbl-connector/internal/domain"
)

// RouteRulesRepository stores the routing rules in PostgreSQL.
type RouteRulesRepository struct {
	db *gorm.DB
}

// routingConfigRow maps the single routing_config row (CBL-12.2).
type routingConfigRow struct {
	Revision int64 `gorm:"column:revision"`
}

// NewRouteRulesRepository creates the rule repository.
func NewRouteRulesRepository(db *gorm.DB) *RouteRulesRepository {
	return &RouteRulesRepository{db: db}
}

// LoadEnabled returns the enabled rules ordered by priority.
func (r *RouteRulesRepository) LoadEnabled(ctx context.Context) ([]domain.RouteRule, error) {
	var rows []RouteRuleRow
	err := r.db.WithContext(ctx).
		Where("enabled = ?", true).
		Order("priority ASC, provider ASC").
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("storage: load routing rules: %w", err)
	}
	return toDomainRules(rows), nil
}

// LoadAll returns every rule (enabled and disabled), ordered by priority: the
// administrative read shows the disabled rules as well (CBL-12.2 FR-2).
func (r *RouteRulesRepository) LoadAll(ctx context.Context) ([]domain.RouteRule, error) {
	var rows []RouteRuleRow
	err := r.db.WithContext(ctx).
		Order("priority ASC, provider ASC").
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("storage: load all routing rules: %w", err)
	}
	return toDomainRules(rows), nil
}

// Revision returns the current routing configuration revision.
func (r *RouteRulesRepository) Revision(ctx context.Context) (int64, error) {
	var revision int64
	row := r.db.WithContext(ctx).Raw("SELECT revision FROM routing_config WHERE id = 1").Row()
	if err := row.Scan(&revision); err != nil {
		return 0, fmt.Errorf("storage: read routing revision: %w", err)
	}
	return revision, nil
}

// ProviderNames returns the distinct provider names present in the rules: the
// known-provider check of the admin contour unions them with the registry
// (CBL-12.2 FR-3; the registry is filled by task 3.2).
func (r *RouteRulesRepository) ProviderNames(ctx context.Context) ([]string, error) {
	var names []string
	err := r.db.WithContext(ctx).
		Model(&RouteRuleRow{}).
		Distinct("provider").
		Order("provider ASC").
		Pluck("provider", &names).Error
	if err != nil {
		return nil, fmt.Errorf("storage: load rule providers: %w", err)
	}
	return names, nil
}

// DisableProvider records a manual provider disable; already is true when the
// provider was disabled before (the insert is idempotent, CBL-12.2 FR-2).
func (r *RouteRulesRepository) DisableProvider(ctx context.Context, provider, reason string) (bool, error) {
	result := r.db.WithContext(ctx).Exec(
		`INSERT INTO disabled_providers (provider, reason) VALUES (?, ?) ON CONFLICT (provider) DO NOTHING`,
		provider, reason)
	if result.Error != nil {
		return false, fmt.Errorf("storage: disable provider: %w", result.Error)
	}
	return result.RowsAffected == 0, nil
}

// ListDisabled returns the disabled provider names ordered alphabetically.
func (r *RouteRulesRepository) ListDisabled(ctx context.Context) ([]string, error) {
	var names []string
	err := r.db.WithContext(ctx).
		Table("disabled_providers").
		Order("provider ASC").
		Pluck("provider", &names).Error
	if err != nil {
		return nil, fmt.Errorf("storage: load disabled providers: %w", err)
	}
	return names, nil
}

// routeRulesLockKey specifies the advisory lock key of configuration replacement:
// it serializes concurrent Saves so that replace-all does not leave a merge of two sets.
const routeRulesLockKey = int64(0x43424C34524F5554) // "CBL4ROUT"

// Save replaces the whole rule configuration in a single transaction and bumps
// the revision, so a later ReplaceWithRevision cannot be defeated by a legacy
// unconditional write (review finding).
func (r *RouteRulesRepository) Save(ctx context.Context, rules []domain.RouteRule) error {
	rows, err := buildRuleRows(rules)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockRouteRules(tx); err != nil {
			return err
		}
		if err := replaceRuleRows(tx, rows); err != nil {
			return err
		}
		if err := tx.Exec("UPDATE routing_config SET revision = revision + 1, updated_at = now() WHERE id = 1").Error; err != nil {
			return fmt.Errorf("storage: bump routing revision: %w", err)
		}
		return nil
	})
}

// ReplaceWithRevision replaces the whole configuration with optimistic
// concurrency (CBL-12.2 FR-2): the revision row is locked, a mismatch leaves
// everything untouched, a successful replacement increments the revision.
func (r *RouteRulesRepository) ReplaceWithRevision(ctx context.Context, expected int64, rules []domain.RouteRule) (int64, error) {
	rows, err := buildRuleRows(rules)
	if err != nil {
		return 0, err
	}
	var newRevision int64
	txErr := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockRouteRules(tx); err != nil {
			return err
		}
		var config routingConfigRow
		row := tx.Raw("SELECT revision FROM routing_config WHERE id = 1 FOR UPDATE").Row()
		if err := row.Scan(&config.Revision); err != nil {
			return fmt.Errorf("storage: lock routing revision: %w", err)
		}
		if config.Revision != expected {
			return domain.ErrRouteRevisionConflict
		}
		if err := replaceRuleRows(tx, rows); err != nil {
			return err
		}
		if err := tx.Exec("UPDATE routing_config SET revision = revision + 1, updated_at = now() WHERE id = 1").Error; err != nil {
			return fmt.Errorf("storage: bump routing revision: %w", err)
		}
		newRevision = config.Revision + 1
		return nil
	})
	if txErr != nil {
		return 0, txErr
	}
	return newRevision, nil
}

// lockRouteRules serializes concurrent configuration replacements.
func lockRouteRules(tx *gorm.DB) error {
	if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", routeRulesLockKey).Error; err != nil {
		return fmt.Errorf("storage: lock routing rules: %w", err)
	}
	return nil
}

// replaceRuleRows clears and inserts the given rows inside an open transaction.
func replaceRuleRows(tx *gorm.DB, rows []RouteRuleRow) error {
	if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&RouteRuleRow{}).Error; err != nil {
		return fmt.Errorf("storage: clear routing rules: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}
	if err := tx.Create(&rows).Error; err != nil {
		return fmt.Errorf("storage: insert routing rules: %w", err)
	}
	return nil
}

// buildRuleRows validates the rules and converts them into table rows.
func buildRuleRows(rules []domain.RouteRule) ([]RouteRuleRow, error) {
	rows := make([]RouteRuleRow, 0, len(rules))
	now := time.Now().UTC()
	for i := range rules {
		rule := rules[i]
		if err := rule.Validate(); err != nil {
			return nil, err
		}
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("storage: generate rule id: %w", err)
		}
		rows = append(rows, RouteRuleRow{
			ID:        id.String(),
			Provider:  rule.Provider,
			Operation: rule.Operation,
			Networks:  StringList(rule.Networks),
			Assets:    StringList(rule.Assets),
			Priority:  rule.Priority,
			Weight:    rule.Weight,
			MinAmount: rule.MinAmount,
			MaxAmount: rule.MaxAmount,
			Enabled:   rule.Enabled,
			UpdatedAt: now,
		})
	}
	return rows, nil
}

// toDomainRules converts table rows into domain rules.
func toDomainRules(rows []RouteRuleRow) []domain.RouteRule {
	rules := make([]domain.RouteRule, 0, len(rows))
	for i := range rows {
		rules = append(rules, toDomainRule(&rows[i]))
	}
	return rules
}

// toDomainRule converts a table row into a domain rule.
func toDomainRule(row *RouteRuleRow) domain.RouteRule {
	return domain.RouteRule{
		Provider:  row.Provider,
		Operation: row.Operation,
		Networks:  []string(row.Networks),
		Assets:    []string(row.Assets),
		Priority:  row.Priority,
		Weight:    row.Weight,
		MinAmount: row.MinAmount,
		MaxAmount: row.MaxAmount,
		Enabled:   row.Enabled,
	}
}
