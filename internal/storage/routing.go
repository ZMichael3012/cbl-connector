package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/cbl/cbl-connector/internal/domain"
)

// RouteRulesRepository хранит правила маршрутизации в PostgreSQL.
type RouteRulesRepository struct {
	db *gorm.DB
}

// NewRouteRulesRepository создает репозиторий правил.
func NewRouteRulesRepository(db *gorm.DB) *RouteRulesRepository {
	return &RouteRulesRepository{db: db}
}

// LoadEnabled возвращает включенные правила в порядке приоритета.
func (r *RouteRulesRepository) LoadEnabled(ctx context.Context) ([]domain.RouteRule, error) {
	var rows []RouteRuleRow
	err := r.db.WithContext(ctx).
		Where("enabled = ?", true).
		Order("priority ASC, provider ASC").
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("storage: load routing rules: %w", err)
	}
	rules := make([]domain.RouteRule, 0, len(rows))
	for i := range rows {
		rules = append(rules, toDomainRule(&rows[i]))
	}
	return rules, nil
}

// routeRulesLockKey задает ключ advisory-лока замены конфигурации:
// сериализует конкурентные Save, чтобы replace-all не оставил объединение двух наборов.
const routeRulesLockKey = int64(0x43424C34524F5554) // "CBL4ROUT"

// Save заменяет конфигурацию правил целиком в одной транзакции.
func (r *RouteRulesRepository) Save(ctx context.Context, rules []domain.RouteRule) error {
	rows := make([]RouteRuleRow, 0, len(rules))
	now := time.Now().UTC()
	for i := range rules {
		rule := rules[i]
		if err := rule.Validate(); err != nil {
			return err
		}
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("storage: generate rule id: %w", err)
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
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", routeRulesLockKey).Error; err != nil {
			return fmt.Errorf("storage: lock routing rules: %w", err)
		}
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
	})
}

// toDomainRule переводит строку таблицы в доменное правило.
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
