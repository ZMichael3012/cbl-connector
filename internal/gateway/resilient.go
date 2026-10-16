// Package gateway contains adapters and decorators of the external gateway port:
// provider call resilience (timeouts, retries, circuit breaker, bulkhead).
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

// Attempt budgets: the RPC deadline against the provider (CBL-003) is split
// between attempts with headroom so that one attempt leaves room for a retry.
const (
	attemptTimeoutInvoice      = 2 * time.Second
	attemptTimeoutPayment      = time.Second
	attemptTimeoutPayout       = 2 * time.Second
	attemptTimeoutPayoutStatus = time.Second
)

// Config describes the resilience parameters of provider calls.
type Config struct {
	// MaxAttempts sets the maximum number of attempts for one operation.
	MaxAttempts int
	// RetryBase sets the base delay between attempts.
	RetryBase time.Duration
	// RetryMax sets the delay ceiling between attempts.
	RetryMax time.Duration
	// RetryJitter sets the fraction of random delay spread (0 means no jitter).
	RetryJitter float64
	// BreakerFailureThreshold sets the number of consecutive failed attempts before tripping.
	BreakerFailureThreshold uint32
	// BreakerTimeout sets how long the breaker stays open.
	BreakerTimeout time.Duration
	// BreakerHalfOpenRequests sets the number of trial calls in half-open state.
	BreakerHalfOpenRequests uint32
	// BulkheadMax sets the maximum number of concurrent provider calls.
	BulkheadMax int
	// AttemptTimeout overrides the per-method attempt timeouts (0 means defaults).
	AttemptTimeout time.Duration
}

// DefaultConfig returns the default resilience parameters.
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

// ConfigFromEnv builds the configuration from environment variables; invalid
// values are ignored and the default value remains.
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

// intFromEnv reads a positive integer; empty or invalid means fallback.
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

// uint32FromEnv reads a positive integer within the uint32 range; an empty or
// invalid value yields the fallback.
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

// durationFromEnv reads a positive duration; an empty or invalid value means fallback.
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

// Configuration limits: they protect against "valid but insane" env values
// that would disable provider protection or eat up the bulkhead slots.
const (
	maxAttemptsLimit        = 10
	maxRetryDelayLimit      = 30 * time.Second
	maxBreakerFailuresLimit = 100
	maxBreakerTimeoutLimit  = time.Hour
	maxBreakerHalfOpenLimit = 16
	maxBulkheadLimit        = 1024
	maxAttemptTimeoutLimit  = 30 * time.Second
)

// normalizeConfig brings the parameters into the working range: zeros and
// outliers are replaced with defaults or capped from above.
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

// Option configures Resilient.
type Option func(*Resilient)

// WithLogger attaches a logger for observing retries and the breaker.
func WithLogger(log *zap.Logger) Option {
	return func(r *Resilient) {
		if log != nil {
			r.log = log
		}
	}
}

// WithTimer attaches the backoff timer (deterministic tests).
func WithTimer(timer backoff.Timer) Option {
	return func(r *Resilient) {
		if timer != nil {
			r.timer = timer
		}
	}
}

// Resilient wraps the provider port with resilience: bulkhead -> retry ->
// circuit breaker -> attempt timeout -> adapter (FR-1).
type Resilient struct {
	inner   domain.PaymentGateway
	cfg     Config
	log     *zap.Logger
	breaker *gobreaker.CircuitBreaker
	timer   backoff.Timer
	slots   chan struct{}
}

// New creates the decorator; inner is mandatory (nil is rejected with an error on call).
// The configuration is normalized: zero and out-of-range values are replaced
// with defaults or capped.
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
			// A provider failure is only a temporary failure or an attempt
			// timeout; business rejections and caller cancellation are not
			// provider degradation and must not trip the breaker for all merchants.
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

// CreateInvoice creates an invoice in the external gateway with retries on temporary failures.
func (r *Resilient) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		// Retrying a mutating call without an idempotency key could create a
		// second invoice: fail fast before calling the provider.
		return domain.Invoice{}, fmt.Errorf("%w: CreateInvoice: empty idempotency_key", domain.ErrInvalidGatewayRequest)
	}
	return call(r, ctx, "CreateInvoice", attemptTimeoutInvoice, func(attemptCtx context.Context) (domain.Invoice, error) {
		return r.inner.CreateInvoice(attemptCtx, req)
	})
}

// GetPayment returns the payment status with retries on temporary failures.
func (r *Resilient) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	return call(r, ctx, "GetPayment", attemptTimeoutPayment, func(attemptCtx context.Context) (domain.PaymentStatus, error) {
		return r.inner.GetPayment(attemptCtx, gatewayID)
	})
}

// CreatePayout creates a payout with retries on temporary failures.
func (r *Resilient) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		// Retrying a payout without an idempotency key could send the money twice.
		return domain.Payout{}, fmt.Errorf("%w: CreatePayout: empty idempotency_key", domain.ErrInvalidGatewayRequest)
	}
	return call(r, ctx, "CreatePayout", attemptTimeoutPayout, func(attemptCtx context.Context) (domain.Payout, error) {
		return r.inner.CreatePayout(attemptCtx, req)
	})
}

// GetPayout returns the payout status with retries on temporary failures.
func (r *Resilient) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	return call(r, ctx, "GetPayout", attemptTimeoutPayoutStatus, func(attemptCtx context.Context) (domain.PayoutStatus, error) {
		return r.inner.GetPayout(attemptCtx, gatewayPayoutID)
	})
}

// VerifyWebhook is passed through directly: a local signature check without network access.
func (r *Resilient) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	if r.inner == nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: nil gateway", domain.ErrProviderUnavailable)
	}
	return r.inner.VerifyWebhook(headers, rawBody)
}

// Available reports that the breaker is not open: the provider can receive traffic
// (the router 3.4 excludes unavailable providers while an alternative exists).
func (r *Resilient) Available() bool {
	return r.breaker.State() != gobreaker.StateOpen
}

// Capabilities is passed through directly: local adapter data.
func (r *Resilient) Capabilities() domain.Capabilities {
	if r.inner == nil {
		return domain.Capabilities{}
	}
	return r.inner.Capabilities()
}

// call executes a provider operation with the bulkhead, retries, breaker, and attempt timeout.
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

// newBackoff builds an exponential backoff with a ceiling and jitter.
func (r *Resilient) newBackoff() *backoff.ExponentialBackOff {
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = r.cfg.RetryBase
	policy.MaxInterval = r.cfg.RetryMax
	policy.RandomizationFactor = r.cfg.RetryJitter
	policy.MaxElapsedTime = 0 // stopped by the attempt count, not by time
	return policy
}

// tryAcquire takes a bulkhead slot without waiting.
func (r *Resilient) tryAcquire() bool {
	select {
	case r.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release frees the bulkhead slot; it is called strictly after a successful acquire.
func (r *Resilient) release() {
	<-r.slots
}

// isBusinessOutcome reports that the error is a provider business-level response
// (or a caller bug), not a sign of unavailability: the breaker does not count it.
func isBusinessOutcome(err error) bool {
	return errors.Is(err, domain.ErrRejected) ||
		errors.Is(err, domain.ErrInvalidRoute) ||
		errors.Is(err, domain.ErrUnsupportedNetwork) ||
		errors.Is(err, domain.ErrInvalidGatewayRequest) ||
		errors.Is(err, domain.ErrUnknownState)
}

// isRetryable reports that the error can be retried: temporary failures and
// attempt timeouts; permanent failures, caller cancellation, and an open breaker cannot.
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

// ConfigFromOSEnv builds the configuration from the process environment variables.
func ConfigFromOSEnv() Config {
	return ConfigFromEnv(envFromOS)
}

// envFromOS reads the process environment variables (adapter for ConfigFromEnv).
func envFromOS(key string) string {
	return os.Getenv(key)
}
