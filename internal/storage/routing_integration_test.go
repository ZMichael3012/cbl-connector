//go:build integration

package storage_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/usecase"
)

func setupConnectorDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("connector"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func setupRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: endpoint})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// defaultRules specifies a rule set for round-trip checks.
func defaultRules() []domain.RouteRule {
	return []domain.RouteRule{
		{Provider: "sandbox", Operation: domain.OperationInvoice, Networks: []string{"trc20"}, Assets: []string{"USDT"}, Priority: 0, Weight: 3, MinAmount: 100, MaxAmount: 1_000_000, Enabled: true},
		{Provider: "fallback", Priority: 5, Weight: 1, Enabled: true},
		{Provider: "disabled", Priority: 1, Weight: 1, Enabled: false},
	}
}

func TestIntegrationRoutingRulesRoundTrip(t *testing.T) {
	db := setupConnectorDB(t)
	repo := storage.NewRouteRulesRepository(db)
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, defaultRules()))
	rules, err := repo.LoadEnabled(ctx)
	require.NoError(t, err)
	require.Len(t, rules, 2, "a disabled rule is not loaded")

	assert.Equal(t, "sandbox", rules[0].Provider, "ordered by priority")
	assert.Equal(t, domain.OperationInvoice, rules[0].Operation)
	assert.Equal(t, []string{"trc20"}, rules[0].Networks)
	assert.Equal(t, []string{"USDT"}, rules[0].Assets)
	assert.Equal(t, 3, rules[0].Weight)
	assert.Equal(t, int64(100), rules[0].MinAmount)
	assert.Equal(t, "fallback", rules[1].Provider)
	assert.Empty(t, rules[1].Networks, "empty arrays are stored as empty")

	// Full configuration replacement.
	require.NoError(t, repo.Save(ctx, []domain.RouteRule{
		{Provider: "only", Priority: 0, Weight: 1, Enabled: true},
	}))
	rules, err = repo.LoadEnabled(ctx)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "only", rules[0].Provider)
}

func TestIntegrationRoutingRulesConstraints(t *testing.T) {
	db := setupConnectorDB(t)
	repo := storage.NewRouteRulesRepository(db)
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, defaultRules()))

	// Database CHECKs: the weight and amount bounds guard against direct writes.
	err := db.Exec(`INSERT INTO routing_rules (id, provider, weight) VALUES (gen_random_uuid(), 'x', 0)`).Error
	require.Error(t, err, "weight 0 is rejected by the database")

	err = db.Exec(`INSERT INTO routing_rules (id, provider, min_amount, max_amount) VALUES (gen_random_uuid(), 'x', 10, 5)`).Error
	require.Error(t, err, "min > max is rejected by the database")

	err = repo.Save(ctx, []domain.RouteRule{{Provider: "x", Weight: 0, Enabled: true}})
	require.ErrorIs(t, err, domain.ErrInvalidRoute, "domain validation before the write")
}

func TestIntegrationRoutingRulesConcurrentSave(t *testing.T) {
	db := setupConnectorDB(t)
	repo := storage.NewRouteRulesRepository(db)
	ctx := context.Background()

	// Concurrent replace-alls must not leave a merge of configurations:
	// the advisory lock serializes Save.
	configs := [][]domain.RouteRule{
		{{Provider: "alpha", Priority: 0, Weight: 1, Enabled: true}},
		{{Provider: "beta", Priority: 0, Weight: 1, Enabled: true}},
	}
	for iteration := 0; iteration < 5; iteration++ {
		done := make(chan error, len(configs))
		for _, config := range configs {
			go func(rules []domain.RouteRule) { done <- repo.Save(ctx, rules) }(config)
		}
		for range configs {
			require.NoError(t, <-done)
		}
		rules, err := repo.LoadEnabled(ctx)
		require.NoError(t, err)
		require.Len(t, rules, 1, "the final configuration is one of the sets, with no merge")
	}
}

func TestIntegrationRouteConfigCache(t *testing.T) {
	client := setupRedis(t)
	cache := storage.NewRouteConfigCache(client)
	ctx := context.Background()

	_, ok, err := cache.Get(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "an empty cache is a miss")

	snapshot := domain.RouteConfigSnapshot{Rules: defaultRules(), Disabled: []string{"muted"}}
	require.NoError(t, cache.Set(ctx, snapshot, time.Minute))
	got, ok, err := cache.Get(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, snapshot, got, "rules and the disabled set round-trip")

	require.NoError(t, cache.Invalidate(ctx))
	_, ok, err = cache.Get(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "after invalidation it is a miss")

	// Corrupted JSON counts as a miss and does not break selection.
	require.NoError(t, client.Set(ctx, "route:config", "{broken", time.Minute).Err())
	_, ok, err = cache.Get(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "broken JSON is a miss")
}

func TestIntegrationCachedRouteRules(t *testing.T) {
	db := setupConnectorDB(t)
	client := setupRedis(t)
	ctx := context.Background()

	repo := storage.NewRouteRulesRepository(db)
	cache := storage.NewRouteConfigCache(client)
	source := usecase.NewCachedRouteRules(repo, cache, time.Minute, zap.NewNop())

	require.NoError(t, repo.Save(ctx, defaultRules()))
	first, err := source.Load(ctx)
	require.NoError(t, err)
	require.Len(t, first, 2)

	// A direct replacement in the database is not visible because of the cache TTL:
	// the source of truth is updated through Save (invalidation).
	require.NoError(t, repo.Save(ctx, []domain.RouteRule{{Provider: "only", Priority: 0, Weight: 1, Enabled: true}}))
	cached, err := source.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, "sandbox", cached[0].Provider, "the second read is served from the cache")

	require.NoError(t, source.Save(ctx, []domain.RouteRule{{Provider: "only", Priority: 0, Weight: 1, Enabled: true}}))
	fresh, err := source.Load(ctx)
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	assert.Equal(t, "only", fresh[0].Provider, "Save invalidates the cache")
}

func TestIntegrationCachedRouteRulesRedisDown(t *testing.T) {
	db := setupConnectorDB(t)
	ctx := context.Background()

	repo := storage.NewRouteRulesRepository(db)
	require.NoError(t, repo.Save(ctx, defaultRules()))

	// A client of a deliberately unreachable Redis: selection must work through the database (T-11).
	downClient := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
	})
	defer func() { _ = downClient.Close() }()
	source := usecase.NewCachedRouteRules(repo, storage.NewRouteConfigCache(downClient), time.Minute, zap.NewNop())

	rules, err := source.Load(ctx)
	require.NoError(t, err, "a Redis failure does not break rule loading")
	assert.Len(t, rules, 2)
}

func TestIntegrationAdminRevisionFlow(t *testing.T) {
	db := setupConnectorDB(t)
	repo := storage.NewRouteRulesRepository(db)
	ctx := context.Background()

	revision, err := repo.Revision(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, revision, "a fresh configuration starts at revision 0")

	revision, err = repo.ReplaceWithRevision(ctx, 0, defaultRules())
	require.NoError(t, err)
	assert.EqualValues(t, 1, revision)

	all, err := repo.LoadAll(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 3, "the admin read includes disabled rules")
	stored, err := repo.Revision(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, stored)

	// A stale revision conflicts and leaves the configuration untouched.
	_, err = repo.ReplaceWithRevision(ctx, 0, []domain.RouteRule{{Provider: "other", Weight: 1, Enabled: true}})
	require.ErrorIs(t, err, domain.ErrRouteRevisionConflict)
	all, err = repo.LoadAll(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 3, "a conflict writes nothing")

	// Concurrent replacements with the same expected revision: exactly one wins.
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(n int) {
			_, replaceErr := repo.ReplaceWithRevision(ctx, 1, []domain.RouteRule{{Provider: "winner", Weight: 1, Enabled: true}})
			results <- replaceErr
		}(i)
	}
	successes, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		switch err := <-results; {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrRouteRevisionConflict):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, conflicts)
}

func TestIntegrationDisableProvider(t *testing.T) {
	db := setupConnectorDB(t)
	repo := storage.NewRouteRulesRepository(db)
	ctx := context.Background()

	require.NoError(t, repo.Save(ctx, defaultRules()))
	names, err := repo.ProviderNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"disabled", "fallback", "sandbox"}, names, "distinct providers, sorted")

	already, err := repo.DisableProvider(ctx, "sandbox", "maintenance")
	require.NoError(t, err)
	assert.False(t, already)
	already, err = repo.DisableProvider(ctx, "sandbox", "again")
	require.NoError(t, err)
	assert.True(t, already, "the repeat is idempotent")

	disabled, err := repo.ListDisabled(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"sandbox"}, disabled)
}

func TestIntegrationAdminUsecaseDisableFiltersRules(t *testing.T) {
	db := setupConnectorDB(t)
	client := setupRedis(t)
	ctx := context.Background()

	repo := storage.NewRouteRulesRepository(db)
	cache := storage.NewRouteConfigCache(client)
	source := usecase.NewCachedRouteRules(repo, cache, time.Minute, zap.NewNop())
	admin := usecase.NewAdminRouting(repo, source, nil, zap.NewNop())

	require.NoError(t, repo.Save(ctx, defaultRules()))
	warm, err := source.Load(ctx)
	require.NoError(t, err)
	require.Len(t, warm, 2, "the cache is warmed with both active rules")

	already, err := admin.DisableProvider(ctx, "sandbox", "maintenance")
	require.NoError(t, err)
	assert.False(t, already)

	active, err := source.Load(ctx)
	require.NoError(t, err)
	assert.Len(t, active, 1)
	assert.Equal(t, "fallback", active[0].Provider, "the disabled provider disappears from the rule source")

	// The admin read still shows the full configuration (the operator can edit it).
	_, all, err := admin.GetRules(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

// TestIntegrationAdminDisableSurvivesRuleReplacement verifies the review
// finding: a repeated disable of a provider whose rules were replaced stays
// idempotent (the disabled set keeps the provider known).
func TestIntegrationAdminDisableSurvivesRuleReplacement(t *testing.T) {
	db := setupConnectorDB(t)
	ctx := context.Background()

	repo := storage.NewRouteRulesRepository(db)
	admin := usecase.NewAdminRouting(repo, nil, nil, zap.NewNop())

	require.NoError(t, repo.Save(ctx, defaultRules()))
	already, err := admin.DisableProvider(ctx, "sandbox", "maintenance")
	require.NoError(t, err)
	assert.False(t, already)

	// Replace the rules without the disabled provider.
	revision, _, err := admin.GetRules(ctx)
	require.NoError(t, err)
	_, _, err = admin.PutRules(ctx, revision, []domain.RouteRule{{Provider: "fallback", Weight: 1, Enabled: true}})
	require.NoError(t, err)

	already, err = admin.DisableProvider(ctx, "sandbox", "")
	require.NoError(t, err, "the disabled provider stays known")
	assert.True(t, already)
}

func TestIntegrationAdminUsecasePutRevision(t *testing.T) {
	db := setupConnectorDB(t)
	repo := storage.NewRouteRulesRepository(db)
	ctx := context.Background()
	admin := usecase.NewAdminRouting(repo, nil, nil, zap.NewNop())

	revision, stored, err := admin.PutRules(ctx, 0, defaultRules())
	require.NoError(t, err)
	assert.EqualValues(t, 1, revision)
	assert.Len(t, stored, 3)

	_, _, err = admin.PutRules(ctx, 0, defaultRules())
	require.ErrorIs(t, err, domain.ErrRouteRevisionConflict)

	revision, all, err := admin.GetRules(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, revision)
	assert.Len(t, all, 3)
}
