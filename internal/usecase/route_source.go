package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
)

// Пределы TTL кеша конфигурации маршрутов: CBL-003 (раздел 7) фиксирует
// для route:config диапазон 30 - 60 с.
const (
	defaultRouteCacheTTL = 30 * time.Second
	minRouteCacheTTL     = 30 * time.Second
	maxRouteCacheTTL     = 60 * time.Second
)

// RouteRulesStore хранит правила маршрутизации (источник истины).
type RouteRulesStore interface {
	// LoadEnabled возвращает включенные правила.
	LoadEnabled(ctx context.Context) ([]domain.RouteRule, error)
	// Save заменяет конфигурацию правил целиком.
	Save(ctx context.Context, rules []domain.RouteRule) error
}

// RouteConfigCacheStore кеширует конфигурацию правил.
type RouteConfigCacheStore interface {
	// Get возвращает конфигурацию из кеша; miss - ok=false.
	Get(ctx context.Context) ([]domain.RouteRule, bool, error)
	// Set сохраняет конфигурацию с TTL.
	Set(ctx context.Context, rules []domain.RouteRule, ttl time.Duration) error
	// Invalidate удаляет конфигурацию из кеша.
	Invalidate(ctx context.Context) error
}

// NormalizeRouteCacheTTL приводит TTL к диапазону 30 - 60 с (CBL-003),
// некорректное значение заменяет значением по умолчанию.
func NormalizeRouteCacheTTL(ttl time.Duration) time.Duration {
	switch {
	case ttl <= 0:
		return defaultRouteCacheTTL
	case ttl < minRouteCacheTTL:
		return minRouteCacheTTL
	case ttl > maxRouteCacheTTL:
		return maxRouteCacheTTL
	default:
		return ttl
	}
}

// RouteCacheTTLFromEnv читает CONNECTOR_ROUTE_CACHE_TTL с клэмпами.
func RouteCacheTTLFromEnv(getenv func(string) string) time.Duration {
	raw := getenv("CONNECTOR_ROUTE_CACHE_TTL")
	if raw == "" {
		return defaultRouteCacheTTL
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil {
		return defaultRouteCacheTTL
	}
	return NormalizeRouteCacheTTL(ttl)
}

// CachedRouteRules читает правила через Redis-кеш со фолбэком на хранилище:
// источник истины - PostgreSQL, сбой кеша не влияет на выбор (T-02, T-11).
type CachedRouteRules struct {
	store RouteRulesStore
	cache RouteConfigCacheStore
	ttl   time.Duration
	log   *zap.Logger
}

// NewCachedRouteRules создает источник правил с кешем.
func NewCachedRouteRules(store RouteRulesStore, cache RouteConfigCacheStore, ttl time.Duration, log *zap.Logger) *CachedRouteRules {
	if log == nil {
		log = zap.NewNop()
	}
	return &CachedRouteRules{
		store: store,
		cache: cache,
		ttl:   NormalizeRouteCacheTTL(ttl),
		log:   log,
	}
}

// Load возвращает правила из кеша или из хранилища (cache-aside).
// Кеш не является источником истины: семантически некорректный payload
// (например, обнуленный вес) считается промахом и не доходит до роутера.
func (s *CachedRouteRules) Load(ctx context.Context) ([]domain.RouteRule, error) {
	if s.cache != nil {
		cached, ok, err := s.cache.Get(ctx)
		if err != nil {
			s.log.Warn("route config cache read failed", zap.Error(err))
		} else if ok {
			if rulesValid(cached) {
				return cached, nil
			}
			s.log.Warn("route config cache payload is invalid: falling back to storage")
		}
	}
	rules, err := s.store.LoadEnabled(ctx)
	if err != nil {
		return nil, fmt.Errorf("usecase: load routing rules: %w", err)
	}
	if !rulesValid(rules) {
		return nil, errors.New("usecase: routing rules from storage are invalid")
	}
	if s.cache != nil {
		if err := s.cache.Set(ctx, rules, s.ttl); err != nil {
			s.log.Warn("route config cache write failed", zap.Error(err))
		}
	}
	return rules, nil
}

// Save заменяет правила в хранилище и инвалидирует кеш.
func (s *CachedRouteRules) Save(ctx context.Context, rules []domain.RouteRule) error {
	if err := s.store.Save(ctx, rules); err != nil {
		return err
	}
	if s.cache != nil {
		if err := s.cache.Invalidate(ctx); err != nil {
			// TTL ограничит устаревание даже при сбое инвалидации.
			s.log.Warn("route config cache invalidate failed", zap.Error(err))
		}
	}
	return nil
}

// rulesValid проверяет, что все правила проходят доменную валидацию.
func rulesValid(rules []domain.RouteRule) bool {
	for i := range rules {
		if err := rules[i].Validate(); err != nil {
			return false
		}
	}
	return true
}
