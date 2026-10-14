// Пакет gateway содержит адаптеры и декораторы порта внешнего шлюза:
// устойчивость вызовов провайдера (таймауты, ретраи, circuit breaker, bulkhead).
package gateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sony/gobreaker"
	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
)

// Бюджеты попыток: дедлайн RPC против провайдера (CBL-003) делится между
// попытками с запасом, чтобы одна попытка оставляла место на повтор.
const (
	attemptTimeoutInvoice      = 2 * time.Second
	attemptTimeoutPayment      = time.Second
	attemptTimeoutPayout       = 2 * time.Second
	attemptTimeoutPayoutStatus = time.Second
)

// Config описывает параметры устойчивости вызовов провайдера.
type Config struct {
	// MaxAttempts задает предельное число попыток одной операции.
	MaxAttempts int
	// RetryBase задает базовую задержку между попытками.
	RetryBase time.Duration
	// RetryMax задает потолок задержки между попытками.
	RetryMax time.Duration
	// RetryJitter задает долю случайного разброса задержки (0 - без jitter).
	RetryJitter float64
	// BreakerFailureThreshold задает число подряд неудачных попыток до размыкания.
	BreakerFailureThreshold uint32
	// BreakerTimeout задает время нахождения breaker в открытом состоянии.
	BreakerTimeout time.Duration
	// BreakerHalfOpenRequests задает число пробных вызовов в half-open.
	BreakerHalfOpenRequests uint32
	// BulkheadMax задает предельное число одновременных вызовов провайдера.
	BulkheadMax int
	// AttemptTimeout перекрывает пер-методные таймауты попыток (0 - defaults).
	AttemptTimeout time.Duration
}

// DefaultConfig возвращает параметры устойчивости по умолчанию.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:             3,
		RetryBase:               100 * time.Millisecond,
		RetryMax:                time.Second,
		RetryJitter:             0.5,
		BreakerFailureThreshold: 5,
		BreakerTimeout:          30 * time.Second,
		BreakerHalfOpenRequests: 1,
		BulkheadMax:             32,
	}
}

// ConfigFromEnv собирает конфигурацию из переменных окружения; некорректные
// значения игнорируются и остается значение по умолчанию.
func ConfigFromEnv(getenv func(string) string) Config {
	cfg := DefaultConfig()
	cfg.MaxAttempts = intFromEnv(getenv, "CONNECTOR_RETRY_MAX_ATTEMPTS", cfg.MaxAttempts)
	cfg.RetryBase = durationFromEnv(getenv, "CONNECTOR_RETRY_BASE_DELAY", cfg.RetryBase)
	cfg.RetryMax = durationFromEnv(getenv, "CONNECTOR_RETRY_MAX_DELAY", cfg.RetryMax)
	cfg.BreakerFailureThreshold = uint32FromEnv(getenv, "CONNECTOR_BREAKER_FAILURE_THRESHOLD", cfg.BreakerFailureThreshold)
	cfg.BreakerTimeout = durationFromEnv(getenv, "CONNECTOR_BREAKER_TIMEOUT", cfg.BreakerTimeout)
	cfg.BreakerHalfOpenRequests = uint32FromEnv(getenv, "CONNECTOR_BREAKER_HALF_OPEN_REQUESTS", cfg.BreakerHalfOpenRequests)
	cfg.BulkheadMax = intFromEnv(getenv, "CONNECTOR_BULKHEAD_MAX_CONCURRENT", cfg.BulkheadMax)
	cfg.AttemptTimeout = durationFromEnv(getenv, "CONNECTOR_PROVIDER_ATTEMPT_TIMEOUT", cfg.AttemptTimeout)
	return cfg
}

// intFromEnv читает положительное целое; пустое или некорректное - fallback.
func intFromEnv(getenv func(string) string, key string, fallback int) int {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

// uint32FromEnv читает положительное целое в пределах uint32; пустое или
// некорректное значение дает fallback.
func uint32FromEnv(getenv func(string) string, key string, fallback uint32) uint32 {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value == 0 {
		return fallback
	}
	return uint32(value)
}

// durationFromEnv читает положительную длительность; пустая или некорректная - fallback.
func durationFromEnv(getenv func(string) string, key string, fallback time.Duration) time.Duration {
	raw := getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

// Пределы конфигурации: защищают от «валидных, но безумных» значений env,
// которые отключили бы защиту провайдера или выели слоты bulkhead.
const (
	maxAttemptsLimit        = 10
	maxRetryDelayLimit      = 30 * time.Second
	maxBreakerFailuresLimit = 100
	maxBreakerTimeoutLimit  = time.Hour
	maxBreakerHalfOpenLimit = 16
	maxBulkheadLimit        = 1024
	maxAttemptTimeoutLimit  = 30 * time.Second
)

// normalizeConfig приводит параметры к рабочему диапазону: нули и перекосы
// заменяются значениями по умолчанию или ограничиваются сверху.
func normalizeConfig(cfg Config) Config {
	defaults := DefaultConfig()
	switch {
	case cfg.MaxAttempts <= 0:
		cfg.MaxAttempts = defaults.MaxAttempts
	case cfg.MaxAttempts > maxAttemptsLimit:
		cfg.MaxAttempts = maxAttemptsLimit
	}
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = defaults.RetryBase
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = defaults.RetryMax
	}
	if cfg.RetryMax > maxRetryDelayLimit {
		cfg.RetryMax = maxRetryDelayLimit
	}
	if cfg.RetryBase > cfg.RetryMax {
		cfg.RetryBase = cfg.RetryMax
	}
	if cfg.RetryJitter < 0 {
		cfg.RetryJitter = 0
	}
	if cfg.RetryJitter > 1 {
		cfg.RetryJitter = 1
	}
	switch {
	case cfg.BreakerFailureThreshold == 0:
		cfg.BreakerFailureThreshold = defaults.BreakerFailureThreshold
	case cfg.BreakerFailureThreshold > maxBreakerFailuresLimit:
		cfg.BreakerFailureThreshold = maxBreakerFailuresLimit
	}
	if cfg.BreakerTimeout <= 0 {
		cfg.BreakerTimeout = defaults.BreakerTimeout
	}
	if cfg.BreakerTimeout > maxBreakerTimeoutLimit {
		cfg.BreakerTimeout = maxBreakerTimeoutLimit
	}
	switch {
	case cfg.BreakerHalfOpenRequests == 0:
		cfg.BreakerHalfOpenRequests = defaults.BreakerHalfOpenRequests
	case cfg.BreakerHalfOpenRequests > maxBreakerHalfOpenLimit:
		cfg.BreakerHalfOpenRequests = maxBreakerHalfOpenLimit
	}
	switch {
	case cfg.BulkheadMax <= 0:
		cfg.BulkheadMax = defaults.BulkheadMax
	case cfg.BulkheadMax > maxBulkheadLimit:
		cfg.BulkheadMax = maxBulkheadLimit
	}
	if cfg.AttemptTimeout < 0 {
		cfg.AttemptTimeout = 0
	}
	if cfg.AttemptTimeout > maxAttemptTimeoutLimit {
		cfg.AttemptTimeout = maxAttemptTimeoutLimit
	}
	return cfg
}

// Option настраивает Resilient.
type Option func(*Resilient)

// WithLogger подключает логгер наблюдения за повторами и breaker.
func WithLogger(log *zap.Logger) Option {
	return func(r *Resilient) {
		if log != nil {
			r.log = log
		}
	}
}

// WithTimer подключает таймер backoff (детерминированные тесты).
func WithTimer(timer backoff.Timer) Option {
	return func(r *Resilient) {
		if timer != nil {
			r.timer = timer
		}
	}
}

// Resilient оборачивает порт провайдера устойчивостью: bulkhead -> retry ->
// circuit breaker -> таймаут попытки -> адаптер (FR-1).
type Resilient struct {
	inner   domain.PaymentGateway
	cfg     Config
	log     *zap.Logger
	breaker *gobreaker.CircuitBreaker
	timer   backoff.Timer
	slots   chan struct{}
}

// New создает декоратор; inner обязателен (nil отклоняется ошибкой при вызове).
// Конфигурация нормализуется: нулевые и выходящие за пределы значения
// заменяются значениями по умолчанию или ограничиваются.
func New(inner domain.PaymentGateway, cfg Config, opts ...Option) *Resilient {
	resilient := &Resilient{
		inner: inner,
		cfg:   normalizeConfig(cfg),
		log:   zap.NewNop(),
	}
	for _, opt := range opts {
		opt(resilient)
	}
	cfg = resilient.cfg
	resilient.slots = make(chan struct{}, cfg.BulkheadMax)
	name := "provider"
	if inner != nil {
		if provider := inner.Capabilities().Provider; provider != "" {
			name = provider
		}
	}
	resilient.breaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: cfg.BreakerHalfOpenRequests,
		Timeout:     cfg.BreakerTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cfg.BreakerFailureThreshold
		},
		IsSuccessful: func(err error) bool {
			// Неудача провайдера - только временные сбои и таймауты попыток;
			// бизнес-отказы и отмена вызывающего не являются деградацией
			// провайдера и не должны размыкать breaker для всех мерчантов.
			if err == nil {
				return true
			}
			return isBusinessOutcome(err) || errors.Is(err, context.Canceled)
		},
		OnStateChange: func(stateName string, from, to gobreaker.State) {
			resilient.log.Warn("provider breaker state changed",
				zap.String("provider", stateName),
				zap.String("from", from.String()),
				zap.String("to", to.String()))
		},
	})
	return resilient
}

// CreateInvoice создает счет во внешнем шлюзе с ретраями временных сбоев.
func (r *Resilient) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		// Ретрай мутирующего вызова без ключа идемпотентности мог бы создать
		// второй счет: fail-fast до обращения к провайдеру.
		return domain.Invoice{}, fmt.Errorf("%w: CreateInvoice: empty idempotency_key", domain.ErrInvalidGatewayRequest)
	}
	return call(r, ctx, "CreateInvoice", attemptTimeoutInvoice, func(attemptCtx context.Context) (domain.Invoice, error) {
		return r.inner.CreateInvoice(attemptCtx, req)
	})
}

// GetPayment возвращает статус платежа с ретраями временных сбоев.
func (r *Resilient) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	return call(r, ctx, "GetPayment", attemptTimeoutPayment, func(attemptCtx context.Context) (domain.PaymentStatus, error) {
		return r.inner.GetPayment(attemptCtx, gatewayID)
	})
}

// CreatePayout создает выплату с ретраями временных сбоев.
func (r *Resilient) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		// Повтор выплаты без ключа идемпотентности мог бы отправить деньги дважды.
		return domain.Payout{}, fmt.Errorf("%w: CreatePayout: empty idempotency_key", domain.ErrInvalidGatewayRequest)
	}
	return call(r, ctx, "CreatePayout", attemptTimeoutPayout, func(attemptCtx context.Context) (domain.Payout, error) {
		return r.inner.CreatePayout(attemptCtx, req)
	})
}

// GetPayout возвращает статус выплаты с ретраями временных сбоев.
func (r *Resilient) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	return call(r, ctx, "GetPayout", attemptTimeoutPayoutStatus, func(attemptCtx context.Context) (domain.PayoutStatus, error) {
		return r.inner.GetPayout(attemptCtx, gatewayPayoutID)
	})
}

// VerifyWebhook пробрасывается напрямую: локальная проверка подписи без сети.
func (r *Resilient) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	if r.inner == nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: nil gateway", domain.ErrProviderUnavailable)
	}
	return r.inner.VerifyWebhook(headers, rawBody)
}

// Capabilities пробрасывается напрямую: локальные данные адаптера.
func (r *Resilient) Capabilities() domain.Capabilities {
	if r.inner == nil {
		return domain.Capabilities{}
	}
	return r.inner.Capabilities()
}

// call выполняет операцию провайдера с bulkhead, ретраями, breaker и таймаутом попытки.
func call[T any](r *Resilient, ctx context.Context, operation string, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if r.inner == nil {
		return zero, fmt.Errorf("%w: %s: nil gateway", domain.ErrProviderUnavailable, operation)
	}
	if !r.tryAcquire() {
		return zero, fmt.Errorf("%w: %s", domain.ErrProviderOverloaded, operation)
	}
	defer r.release()

	if r.cfg.AttemptTimeout > 0 {
		timeout = r.cfg.AttemptTimeout
	}
	attempts := 0
	var result T
	operationFn := func() error {
		if err := ctx.Err(); err != nil {
			return backoff.Permanent(err)
		}
		attempts++
		attemptCtx := ctx
		cancel := func() {}
		if timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()

		_, err := r.breaker.Execute(func() (any, error) {
			var callErr error
			result, callErr = fn(attemptCtx)
			return result, callErr
		})
		if err == nil {
			return nil
		}
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return backoff.Permanent(fmt.Errorf("%w: %s: %w", domain.ErrProviderUnavailable, operation, err))
		}
		if !isRetryable(err) {
			return backoff.Permanent(err)
		}
		if attempts >= r.cfg.MaxAttempts {
			return backoff.Permanent(fmt.Errorf("%w: %s attempts exhausted: %w", domain.ErrProviderUnavailable, operation, err))
		}
		return err
	}
	notify := func(err error, delay time.Duration) {
		r.log.Warn("provider call retry",
			zap.String("operation", operation),
			zap.Int("attempt", attempts),
			zap.Duration("delay", delay),
			zap.Error(err))
	}
	if err := backoff.RetryNotifyWithTimer(operationFn, backoff.WithContext(r.newBackoff(), ctx), notify, r.timer); err != nil {
		return zero, err
	}
	return result, nil
}

// newBackoff собирает экспоненциальный backoff с потолком и jitter.
func (r *Resilient) newBackoff() *backoff.ExponentialBackOff {
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = r.cfg.RetryBase
	policy.MaxInterval = r.cfg.RetryMax
	policy.RandomizationFactor = r.cfg.RetryJitter
	policy.MaxElapsedTime = 0 // остановка по числу попыток, не по времени
	return policy
}

// tryAcquire занимает слот bulkhead без ожидания.
func (r *Resilient) tryAcquire() bool {
	select {
	case r.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release освобождает слот bulkhead; вызывается строго после успешного acquire.
func (r *Resilient) release() {
	<-r.slots
}

// isBusinessOutcome сообщает, что ошибка - ответ провайдера на бизнес-уровне
// (или баг вызывающего), а не признак недоступности: breaker ее не учитывает.
func isBusinessOutcome(err error) bool {
	return errors.Is(err, domain.ErrRejected) ||
		errors.Is(err, domain.ErrInvalidRoute) ||
		errors.Is(err, domain.ErrUnsupportedNetwork) ||
		errors.Is(err, domain.ErrInvalidGatewayRequest) ||
		errors.Is(err, domain.ErrUnknownState)
}

// isRetryable сообщает, что ошибку можно повторить: временные сбои и таймауты
// попыток; постоянные отказы, отмена вызывающего и открытый breaker - нет.
func isRetryable(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, context.Canceled):
		return false
	case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
		return false
	case isBusinessOutcome(err):
		return false
	case errors.Is(err, domain.ErrTemporary):
		return true
	case errors.Is(err, context.DeadlineExceeded):
		return true
	default:
		return false
	}
}

// ConfigFromOSEnv собирает конфигурацию из переменных окружения процесса.
func ConfigFromOSEnv() Config {
	return ConfigFromEnv(envFromOS)
}

// envFromOS читает переменные окружения процесса (адаптер для ConfigFromEnv).
func envFromOS(key string) string {
	return os.Getenv(key)
}
