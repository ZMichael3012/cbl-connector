package storage

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// StringList serializes a string list into jsonb.
type StringList []string

// Value implements driver.Valuer for jsonb.
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

// Scan implements sql.Scanner for jsonb.
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

// RouteRuleRow describes a routing_rules table row.
type RouteRuleRow struct {
	// ID specifies the rule UUIDv7.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// Provider specifies the rule provider.
	Provider string `gorm:"column:provider"`
	// Operation specifies the rule operation; empty means any.
	Operation string `gorm:"column:operation"`
	// Networks specifies the rule networks.
	Networks StringList `gorm:"column:networks;type:jsonb"`
	// Assets specifies the rule assets.
	Assets StringList `gorm:"column:assets;type:jsonb"`
	// Priority specifies the priority: a lower value wins.
	Priority int `gorm:"column:priority"`
	// Weight specifies the weight within the priority.
	Weight int `gorm:"column:weight"`
	// MinAmount specifies the lower amount bound (0 means no bound).
	MinAmount int64 `gorm:"column:min_amount"`
	// MaxAmount specifies the upper amount bound (0 means no bound).
	MaxAmount int64 `gorm:"column:max_amount"`
	// Enabled reports whether the rule is active.
	Enabled bool `gorm:"column:enabled"`
	// UpdatedAt specifies the last modification time of the rule.
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName specifies the routing_rules table name.
func (RouteRuleRow) TableName() string { return "routing_rules" }

// ProviderInvoiceRow describes a provider_invoices table row (17.2).
type ProviderInvoiceRow struct {
	// ID specifies the mapping UUIDv7.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// Provider specifies the provider name.
	Provider string `gorm:"column:provider"`
	// GatewayID specifies the provider invoice identifier.
	GatewayID string `gorm:"column:gateway_id"`
	// PaymentID specifies the platform payment UUID.
	PaymentID string `gorm:"column:payment_id"`
	// OrderID specifies the order reference sent to the provider.
	OrderID string `gorm:"column:order_id"`
	// RequestHash specifies the hash of the creation request.
	RequestHash string `gorm:"column:request_hash"`
	// Address specifies the deposit address of the created invoice (4.2).
	Address *string `gorm:"column:address"`
	// CheckoutURL specifies the hosted checkout page of the created invoice (4.2).
	CheckoutURL *string `gorm:"column:checkout_url"`
	// CreatedAt specifies the mapping creation time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName specifies the provider_invoices table name.
func (ProviderInvoiceRow) TableName() string { return "provider_invoices" }
