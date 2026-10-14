//go:build integration

package storage_test

import (
	"context"
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

// defaultRules задает набор правил для round-trip проверок.
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
	require.Len(t, rules, 2, "отключенное правило не загружается")

	assert.Equal(t, "sandbox", rules[0].Provider, "сортировка по приоритету")
	assert.Equal(t, domain.OperationInvoice, rules[0].Operation)
	assert.Equal(t, []string{"trc20"}, rules[0].Networks)
	assert.Equal(t, []string{"USDT"}, rules[0].Assets)
	assert.Equal(t, 3, rules[0].Weight)
	assert.Equal(t, int64(100), rules[0].MinAmount)
	assert.Equal(t, "fallback", rules[1].Provider)
	assert.Empty(t, rules[1].Networks, "пустые массивы сохраняются как пустые")

	// Замена конфигурации целиком.
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

	// CHECK-и БД: вес и границы сумм защищают от прямых записей.
	err := db.Exec(`INSERT INTO routing_rules (id, provider, weight) VALUES (gen_random_uuid(), 'x', 0)`).Error
	require.Error(t, err, "вес 0 отклоняется БД")

	err = db.Exec(`INSERT INTO routing_rules (id, provider, min_amount, max_amount) VALUES (gen_random_uuid(), 'x', 10, 5)`).Error
	require.Error(t, err, "min > max отклоняется БД")

	err = repo.Save(ctx, []domain.RouteRule{{Provider: "x", Weight: 0, Enabled: true}})
	require.ErrorIs(t, err, domain.ErrInvalidRoute, "валидация домена до записи")
}

func TestIntegrationRoutingRulesConcurrentSave(t *testing.T) {
	db := setupConnectorDB(t)
	repo := storage.NewRouteRulesRepository(db)
	ctx := context.Background()

	// Конкурентные replace-all не должны оставить объединение конфигураций:
	// advisory-лок сериализует Save.
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
		require.Len(t, rules, 1, "итоговая конфигурация - один из наборов, без объединения")
	}
}

func TestIntegrationRouteConfigCache(t *testing.T) {
	client := setupRedis(t)
	cache := storage.NewRouteConfigCache(client)
	ctx := context.Background()

	_, ok, err := cache.Get(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "пустой кеш - промах")

	require.NoError(t, cache.Set(ctx, defaultRules(), time.Minute))
	rules, ok, err := cache.Get(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, defaultRules(), rules)

	require.NoError(t, cache.Invalidate(ctx))
	_, ok, err = cache.Get(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "после инвалидации - промах")

	// Поврежденный JSON считается промахом и не ломает выбор.
	require.NoError(t, client.Set(ctx, "route:config", "{broken", time.Minute).Err())
	_, ok, err = cache.Get(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "битый JSON - промах")
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

	// Прямая замена в БД не видна из-за TTL кеша: источник истины обновляется
	// через Save (инвалидация).
	require.NoError(t, repo.Save(ctx, []domain.RouteRule{{Provider: "only", Priority: 0, Weight: 1, Enabled: true}}))
	cached, err := source.Load(ctx)
	require.NoError(t, err)
	assert.Equal(t, "sandbox", cached[0].Provider, "второе чтение обслуживается кешем")

	require.NoError(t, source.Save(ctx, []domain.RouteRule{{Provider: "only", Priority: 0, Weight: 1, Enabled: true}}))
	fresh, err := source.Load(ctx)
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	assert.Equal(t, "only", fresh[0].Provider, "Save инвалидирует кеш")
}

func TestIntegrationCachedRouteRulesRedisDown(t *testing.T) {
	db := setupConnectorDB(t)
	ctx := context.Background()

	repo := storage.NewRouteRulesRepository(db)
	require.NoError(t, repo.Save(ctx, defaultRules()))

	// Клиент заведомо недоступного Redis: выбор обязан работать через БД (T-11).
	downClient := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  50 * time.Millisecond,
		ReadTimeout:  50 * time.Millisecond,
		WriteTimeout: 50 * time.Millisecond,
	})
	defer func() { _ = downClient.Close() }()
	source := usecase.NewCachedRouteRules(repo, storage.NewRouteConfigCache(downClient), time.Minute, zap.NewNop())

	rules, err := source.Load(ctx)
	require.NoError(t, err, "сбой Redis не ломает загрузку правил")
	assert.Len(t, rules, 2)
}
