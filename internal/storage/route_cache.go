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

// routeConfigKey specifies the route configuration cache key (CBL-003, section 7).
const routeConfigKey = "route:config"

// routeConfigPayload describes the JSON value of the route configuration cache:
// the enabled rules and the disabled provider names (CBL-12.2).
type routeConfigPayload struct {
	// Rules are the enabled routing rules.
	Rules []domain.RouteRule `json:"rules"`
	// Disabled lists provider names disabled in the admin contour.
	Disabled []string `json:"disabled,omitempty"`
}

// RouteConfigCache caches the rule configuration in Redis with a TTL.
type RouteConfigCache struct {
	client *redis.Client
}

// NewRouteConfigCache creates the route configuration cache.
func NewRouteConfigCache(client *redis.Client) *RouteConfigCache {
	return &RouteConfigCache{client: client}
}

// Get returns the configuration from the cache; a miss and broken JSON are ok=false.
func (c *RouteConfigCache) Get(ctx context.Context) (domain.RouteConfigSnapshot, bool, error) {
	raw, err := c.client.Get(ctx, routeConfigKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return domain.RouteConfigSnapshot{}, false, nil
	}
	if err != nil {
		return domain.RouteConfigSnapshot{}, false, fmt.Errorf("storage: get route config: %w", err)
	}
	var payload routeConfigPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		// A corrupted cache value must not break selection: treat it as a miss.
		return domain.RouteConfigSnapshot{}, false, nil
	}
	return domain.RouteConfigSnapshot{
		Rules:    payload.Rules,
		Disabled: payload.Disabled,
	}, true, nil
}

// Set stores the configuration in the cache with a TTL.
func (c *RouteConfigCache) Set(ctx context.Context, snapshot domain.RouteConfigSnapshot, ttl time.Duration) error {
	raw, err := json.Marshal(routeConfigPayload{
		Rules:    snapshot.Rules,
		Disabled: snapshot.Disabled,
	})
	if err != nil {
		return fmt.Errorf("storage: encode route config: %w", err)
	}
	if err := c.client.Set(ctx, routeConfigKey, raw, ttl).Err(); err != nil {
		return fmt.Errorf("storage: set route config: %w", err)
	}
	return nil
}

// Invalidate removes the configuration from the cache.
func (c *RouteConfigCache) Invalidate(ctx context.Context) error {
	if err := c.client.Del(ctx, routeConfigKey).Err(); err != nil {
		return fmt.Errorf("storage: invalidate route config: %w", err)
	}
	return nil
}
