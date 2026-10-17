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

// capabilitiesCacheKey namespaces the aggregated capability snapshot.
const capabilitiesCacheKey = "connector:capabilities:v1"

// CapabilitiesCache stores the aggregated capability snapshot in Redis between
// adapter refreshes (17.2): the loader treats a read failure as a miss and a
// write failure as non-fatal, so the cache is an optimization only.
type CapabilitiesCache struct {
	client *redis.Client
}

// NewCapabilitiesCache creates the cache on a Redis client.
func NewCapabilitiesCache(client *redis.Client) *CapabilitiesCache {
	return &CapabilitiesCache{client: client}
}

// Load returns the cached snapshot; found is false on a miss.
func (c *CapabilitiesCache) Load(ctx context.Context) ([]domain.Capabilities, bool, error) {
	raw, err := c.client.Get(ctx, capabilitiesCacheKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("storage: read capabilities cache: %w", err)
	}
	var capabilities []domain.Capabilities
	if err := json.Unmarshal(raw, &capabilities); err != nil {
		return nil, false, fmt.Errorf("storage: decode capabilities cache: %w", err)
	}
	return capabilities, true, nil
}

// Save stores the snapshot with the TTL.
func (c *CapabilitiesCache) Save(ctx context.Context, capabilities []domain.Capabilities, ttl time.Duration) error {
	raw, err := json.Marshal(capabilities)
	if err != nil {
		return fmt.Errorf("storage: encode capabilities cache: %w", err)
	}
	if err := c.client.Set(ctx, capabilitiesCacheKey, raw, ttl).Err(); err != nil {
		return fmt.Errorf("storage: write capabilities cache: %w", err)
	}
	return nil
}
