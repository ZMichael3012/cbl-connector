package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/cbl/cbl-connector/internal/domain"
)

// routeConfigKey задает ключ кеша конфигурации маршрутов (CBL-003, раздел 7).
const routeConfigKey = "route:config"

// routeConfigPayload описывает JSON-значение кеша конфигурации маршрутов.
type routeConfigPayload struct {
	Rules []domain.RouteRule `json:"rules"`
}

// RouteConfigCache кеширует конфигурацию правил в Redis с TTL.
type RouteConfigCache struct {
	client *redis.Client
}

// NewRouteConfigCache создает кеш конфигурации маршрутов.
func NewRouteConfigCache(client *redis.Client) *RouteConfigCache {
	return &RouteConfigCache{client: client}
}

// Get возвращает конфигурацию из кеша; miss и битый JSON - ok=false.
func (c *RouteConfigCache) Get(ctx context.Context) ([]domain.RouteRule, bool, error) {
	raw, err := c.client.Get(ctx, routeConfigKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("storage: get route config: %w", err)
	}
	var payload routeConfigPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		// Поврежденное значение кеша не должно ломать выбор: считаем промахом.
		return nil, false, nil
	}
	return payload.Rules, true, nil
}

// Set сохраняет конфигурацию в кеше с TTL.
func (c *RouteConfigCache) Set(ctx context.Context, rules []domain.RouteRule, ttl time.Duration) error {
	raw, err := json.Marshal(routeConfigPayload{Rules: rules})
	if err != nil {
		return fmt.Errorf("storage: encode route config: %w", err)
	}
	if err := c.client.Set(ctx, routeConfigKey, raw, ttl).Err(); err != nil {
		return fmt.Errorf("storage: set route config: %w", err)
	}
	return nil
}

// Invalidate удаляет конфигурацию из кеша.
func (c *RouteConfigCache) Invalidate(ctx context.Context) error {
	if err := c.client.Del(ctx, routeConfigKey).Err(); err != nil {
		return fmt.Errorf("storage: invalidate route config: %w", err)
	}
	return nil
}
