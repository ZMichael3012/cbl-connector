//go:build integration

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
)

func TestCapabilitiesCacheRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := setupRedis(t)
	cache := storage.NewCapabilitiesCache(client)

	_, found, err := cache.Load(ctx)
	require.NoError(t, err)
	require.False(t, found, "an empty cache is a miss")

	snapshot := []domain.Capabilities{{
		Provider: "coingate",
		Networks: map[string]domain.NetworkCapability{
			"bitcoin": {
				Asset:             "BTC",
				MinAmount:         money.MustParse("0.0001", "BTC", 8),
				ConfirmationDepth: 2,
			},
		},
	}}
	require.NoError(t, cache.Save(ctx, snapshot, time.Minute))

	loaded, found, err := cache.Load(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, snapshot, loaded)

	// A corrupted payload is a read error, so the loader falls back to the live
	// aggregate instead of trusting garbage.
	require.NoError(t, client.Set(ctx, "connector:capabilities:v1", "not-json", time.Minute).Err())
	_, _, err = cache.Load(ctx)
	require.Error(t, err)
}
