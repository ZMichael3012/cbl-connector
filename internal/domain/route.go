package domain

import (
	"context"
	"fmt"
	"strings"
)

// Операции маршрутизации.
const (
	// OperationInvoice задает операцию создания счета.
	OperationInvoice = "invoice"
	// OperationPayout задает операцию выплаты.
	OperationPayout = "payout"
)

// RouteRule описывает правило выбора провайдера.
type RouteRule struct {
	// Provider задает провайдера правила.
	Provider string `json:"provider"`
	// Operation задает операцию правила; пусто - любая.
	Operation string `json:"operation"`
	// Networks задает сети правила; пусто - любые.
	Networks []string `json:"networks"`
	// Assets задает активы правила; пусто - любые.
	Assets []string `json:"assets"`
	// Priority задает приоритет: меньшее значение важнее.
	Priority int `json:"priority"`
	// Weight задает вес внутри приоритета.
	Weight int `json:"weight"`
	// MinAmount задает нижнюю границу суммы (0 - без границы).
	MinAmount int64 `json:"min_amount"`
	// MaxAmount задает верхнюю границу суммы (0 - без границы).
	MaxAmount int64 `json:"max_amount"`
	// Enabled сообщает, что правило активно.
	Enabled bool `json:"enabled"`
}

// Пределы полей правила: защищают взвешенный выбор от переполнения суммы весов.
const (
	maxRulePriority = 1_000_000
	maxRuleWeight   = 1_000_000
)

// Validate проверяет корректность правила маршрутизации.
func (r RouteRule) Validate() error {
	if !validPrintable(strings.TrimSpace(r.Provider), maxProviderLen) {
		return fmt.Errorf("%w: invalid provider %q", ErrInvalidRoute, r.Provider)
	}
	switch r.Operation {
	case "", OperationInvoice, OperationPayout:
	default:
		return fmt.Errorf("%w: unknown operation %q", ErrInvalidRoute, r.Operation)
	}
	if r.Priority < 0 || r.Priority > maxRulePriority {
		return fmt.Errorf("%w: priority %d out of range", ErrInvalidRoute, r.Priority)
	}
	if r.Weight <= 0 || r.Weight > maxRuleWeight {
		return fmt.Errorf("%w: weight %d out of range", ErrInvalidRoute, r.Weight)
	}
	for _, network := range r.Networks {
		if !validCode(network, maxNetworkLen) {
			return fmt.Errorf("%w: invalid network %q", ErrInvalidRoute, network)
		}
	}
	for _, asset := range r.Assets {
		if !validCode(asset, maxAssetLen) || len(asset) < minAssetLen {
			return fmt.Errorf("%w: invalid asset %q", ErrInvalidRoute, asset)
		}
	}
	if r.MinAmount < 0 || r.MaxAmount < 0 {
		return fmt.Errorf("%w: negative amount limit", ErrInvalidRoute)
	}
	if r.MinAmount > 0 && r.MaxAmount > 0 && r.MinAmount > r.MaxAmount {
		return fmt.Errorf("%w: min_amount %d > max_amount %d", ErrInvalidRoute, r.MinAmount, r.MaxAmount)
	}
	return nil
}

// RouteRulesSource загружает активные правила маршрутизации.
type RouteRulesSource interface {
	// Load возвращает включенные правила в порядке приоритета.
	Load(ctx context.Context) ([]RouteRule, error)
}
