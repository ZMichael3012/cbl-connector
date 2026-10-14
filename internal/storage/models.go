package storage

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// StringList сериализует список строк в jsonb.
type StringList []string

// Value реализует driver.Valuer для jsonb.
func (l StringList) Value() (driver.Value, error) {
	if l == nil {
		return []byte("[]"), nil
	}
	raw, err := json.Marshal([]string(l))
	if err != nil {
		return nil, fmt.Errorf("storage: encode string list: %w", err)
	}
	return raw, nil
}

// Scan реализует sql.Scanner для jsonb.
func (l *StringList) Scan(value any) error {
	if value == nil {
		*l = nil
		return nil
	}
	var raw []byte
	switch typed := value.(type) {
	case []byte:
		raw = typed
	case string:
		raw = []byte(typed)
	default:
		return fmt.Errorf("storage: unsupported string list type %T", value)
	}
	parsed := []string{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("storage: decode string list: %w", err)
	}
	*l = parsed
	return nil
}

// RouteRuleRow описывает строку таблицы routing_rules.
type RouteRuleRow struct {
	// ID задает UUIDv7 правила.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// Provider задает провайдера правила.
	Provider string `gorm:"column:provider"`
	// Operation задает операцию правила; пусто - любая.
	Operation string `gorm:"column:operation"`
	// Networks задает сети правила.
	Networks StringList `gorm:"column:networks;type:jsonb"`
	// Assets задает активы правила.
	Assets StringList `gorm:"column:assets;type:jsonb"`
	// Priority задает приоритет: меньшее значение важнее.
	Priority int `gorm:"column:priority"`
	// Weight задает вес внутри приоритета.
	Weight int `gorm:"column:weight"`
	// MinAmount задает нижнюю границу суммы (0 - без границы).
	MinAmount int64 `gorm:"column:min_amount"`
	// MaxAmount задает верхнюю границу суммы (0 - без границы).
	MaxAmount int64 `gorm:"column:max_amount"`
	// Enabled сообщает, что правило активно.
	Enabled bool `gorm:"column:enabled"`
	// UpdatedAt задает время последнего изменения правила.
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName задает имя таблицы routing_rules.
func (RouteRuleRow) TableName() string { return "routing_rules" }
