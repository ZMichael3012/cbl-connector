package gateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// ProviderRegistration описывает регистрацию провайдера в роутере.
type ProviderRegistration struct {
	// Name задает имя провайдера (совпадает с правилами маршрутизации).
	Name string
	// Gateway задает адаптер провайдера; оборачивается в Resilient.
	Gateway domain.PaymentGateway
}

// RouterOption настраивает Router.
type RouterOption func(*Router)

// WithRouterRand подключает источник случайности для взвешенного выбора
// (детерминированные тесты); обертка делает доступ потокобезопасным.
func WithRouterRand(source *rand.Rand) RouterOption {
	return func(r *Router) {
		if source == nil {
			return
		}
		var mu sync.Mutex
		r.pick = func(total int) int {
			mu.Lock()
			defer mu.Unlock()
			return source.IntN(total)
		}
	}
}

// WithRouterLogger подключает логгер роутера.
func WithRouterLogger(log *zap.Logger) RouterOption {
	return func(r *Router) {
		if log != nil {
			r.log = log
		}
	}
}

// WithResilientOptions задает опции оберток Resilient для всех провайдеров.
func WithResilientOptions(opts ...Option) RouterOption {
	return func(r *Router) { r.resilientOpts = append(r.resilientOpts, opts...) }
}

// Router выбирает провайдера по правилам маршрутизации; каждый провайдер
// оборачивается одним экземпляром Resilient (3.3).
type Router struct {
	providers     map[string]*Resilient
	cfg           Config
	rules         domain.RouteRulesSource
	pick          func(total int) int
	log           *zap.Logger
	resilientOpts []Option
}

// NewRouter создает роутер из правил и зарегистрированных провайдеров.
func NewRouter(rules domain.RouteRulesSource, registrations []ProviderRegistration, cfg Config, opts ...RouterOption) (*Router, error) {
	if rules == nil {
		return nil, errors.New("gateway: nil route rules source")
	}
	router := &Router{
		providers: make(map[string]*Resilient, len(registrations)),
		cfg:       cfg,
		rules:     rules,
		// rand.IntN уровня пакета потокобезопасен; инжектируемый источник
		// оборачивается мьютексом (WithRouterRand).
		pick: rand.IntN, //nolint:gosec // G404: взвешенный выбор провайдера, не криптография
		log:  zap.NewNop(),
	}
	for _, opt := range opts {
		opt(router)
	}
	for _, registration := range registrations {
		name := strings.TrimSpace(registration.Name)
		if name == "" {
			return nil, errors.New("gateway: provider name is empty")
		}
		if registration.Gateway == nil {
			return nil, fmt.Errorf("gateway: provider %s has nil gateway", name)
		}
		if _, exists := router.providers[name]; exists {
			return nil, fmt.Errorf("gateway: duplicate provider %s", name)
		}
		if provider := registration.Gateway.Capabilities().Provider; provider != "" && provider != name {
			// Имя регистрации - ключ правил и логов; расхождение с именем
			// адаптера ломает GetGatewayPayment(provider, ...) у потребителей.
			return nil, fmt.Errorf("gateway: provider name %q does not match capabilities %q", name, provider)
		}
		router.providers[name] = New(registration.Gateway, cfg, router.resilientOpts...)
	}
	return router, nil
}

// Select возвращает провайдера для запроса: минимальный приоритет подходящего
// правила, внутри приоритета - взвешенный выбор; провайдеры с открытым breaker
// исключаются, пока есть альтернатива.
func (r *Router) Select(ctx context.Context, req domain.RouteRequest) (domain.PaymentGateway, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	rules, err := r.rules.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("gateway: load routing rules: %w", err)
	}

	type candidate struct {
		name     string
		gateway  *Resilient
		priority int
		weight   int
	}
	candidates := make([]candidate, 0, len(rules))
	breakerBlocked := false
	weightInvalid := false
	for _, rule := range rules {
		if !rule.Enabled || !ruleMatches(rule, req) {
			continue
		}
		gateway, ok := r.providers[rule.Provider]
		if !ok {
			continue
		}
		if rule.Weight <= 0 {
			// Defense-in-depth: недоверенный источник (кеш) с нулевым весом
			// не должен приводить к панике IntN; конфигурация считается сломанной.
			weightInvalid = true
			continue
		}
		if !capabilitySupports(gateway.Capabilities(), req) {
			continue
		}
		if !gateway.Available() {
			breakerBlocked = true
			continue
		}
		candidates = append(candidates, candidate{
			name:     rule.Provider,
			gateway:  gateway,
			priority: rule.Priority,
			weight:   rule.Weight,
		})
	}
	if len(candidates) == 0 {
		if breakerBlocked || weightInvalid {
			return nil, fmt.Errorf("%w: no available provider for %s/%s", domain.ErrProviderUnavailable, req.Network, req.Asset)
		}
		return nil, fmt.Errorf("%w: no provider for %s/%s (%s)", domain.ErrUnsupportedNetwork, req.Network, req.Asset, req.Operation)
	}

	best := candidates[0].priority
	for _, candidate := range candidates[1:] {
		if candidate.priority < best {
			best = candidate.priority
		}
	}
	totalWeight := 0
	for _, candidate := range candidates {
		if candidate.priority == best {
			totalWeight += candidate.weight
		}
	}
	if totalWeight <= 0 {
		// Defense-in-depth: недоверенный источник с обнуленными весами не
		// должен приводить к панике выбора.
		return nil, fmt.Errorf("%w: zero weight for %s/%s", domain.ErrProviderUnavailable, req.Network, req.Asset)
	}
	pick := r.pick(totalWeight)
	for _, candidate := range candidates {
		if candidate.priority != best {
			continue
		}
		if pick < candidate.weight {
			r.log.Debug("provider selected",
				zap.String("provider", candidate.name),
				zap.String("network", req.Network),
				zap.String("asset", req.Asset),
				zap.String("operation", req.Operation))
			return candidate.gateway, nil
		}
		pick -= candidate.weight
	}
	// Недостижимо: сумма весов покрывает pick.
	return nil, fmt.Errorf("%w: weighted selection failed", domain.ErrProviderUnavailable)
}

// Capabilities возвращает возможности каждого зарегистрированного провайдера.
func (r *Router) Capabilities() map[string]domain.Capabilities {
	capabilities := make(map[string]domain.Capabilities, len(r.providers))
	for name, provider := range r.providers {
		capabilities[name] = provider.Capabilities()
	}
	return capabilities
}

// Providers возвращает имена зарегистрированных провайдеров в порядке сортировки.
func (r *Router) Providers() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ruleMatches проверяет операцию, сеть, актив и границы сумм правила.
func ruleMatches(rule domain.RouteRule, req domain.RouteRequest) bool {
	if rule.Operation != "" && rule.Operation != req.Operation {
		return false
	}
	if len(rule.Networks) > 0 && !contains(rule.Networks, req.Network) {
		return false
	}
	if len(rule.Assets) > 0 && !contains(rule.Assets, req.Asset) {
		return false
	}
	amount := req.Amount.MinorUnits
	if rule.MinAmount > 0 && amount < rule.MinAmount {
		return false
	}
	if rule.MaxAmount > 0 && amount > rule.MaxAmount {
		return false
	}
	return true
}

// capabilitySupports проверяет поддержку сети и актива провайдером и лимиты суммы.
func capabilitySupports(capabilities domain.Capabilities, req domain.RouteRequest) bool {
	network, ok := capabilities.Networks[req.Network]
	if !ok || network.Asset != req.Asset {
		return false
	}
	amount := req.Amount.MinorUnits
	if !limitWithin(network.MinAmount, req, amount, false) {
		return false
	}
	return limitWithin(network.MaxAmount, req, amount, true)
}

// limitWithin проверяет границу суммы провайдера: нулевая граница (0)
// означает отсутствие лимита, заданная - сверяет актив, decimals и величину.
func limitWithin(limit money.Money, req domain.RouteRequest, amount int64, upper bool) bool {
	if limit.MinorUnits == 0 {
		return true
	}
	if limit.Asset != req.Asset || limit.Decimals != req.Amount.Decimals {
		return false
	}
	if upper {
		return amount <= limit.MinorUnits
	}
	return amount >= limit.MinorUnits
}

// contains сообщает, что список содержит значение.
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
