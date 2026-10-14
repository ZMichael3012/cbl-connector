package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/cbl/cbl-connector/internal/domain"
)

// recordingTimer фиксирует задержки повторов и срабатывает мгновенно,
// что делает тесты детерминированными и быстрыми.
type recordingTimer struct {
	mu        sync.Mutex
	durations []time.Duration
	fired     chan time.Time
}

// newRecordingTimer создает таймер с уже сработавшим каналом.
func newRecordingTimer() *recordingTimer {
	fired := make(chan time.Time, 1)
	fired <- time.Unix(0, 0)
	return &recordingTimer{fired: fired}
}

// Start запоминает запрошенную задержку.
func (t *recordingTimer) Start(duration time.Duration) {
	t.mu.Lock()
	t.durations = append(t.durations, duration)
	t.mu.Unlock()
}

// Stop ничего не останавливает: таймер мгновенный.
func (t *recordingTimer) Stop() {}

// C возвращает сработавший канал, пополняя его при необходимости.
func (t *recordingTimer) C() <-chan time.Time {
	select {
	case t.fired <- time.Unix(0, 0):
	default:
	}
	return t.fired
}

// delays возвращает запрошенные задержки.
func (t *recordingTimer) delays() []time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]time.Duration(nil), t.durations...)
}

// fakeGateway управляемый порт провайдера: очередь ошибок, задержка и блокировка.
type fakeGateway struct {
	mu         sync.Mutex
	calls      int
	errs       []error
	alwaysErr  error
	delay      time.Duration
	block      chan struct{}
	onCall     func(ctx context.Context)
	panicNext  bool
	capability domain.Capabilities
}

// next возвращает ошибку очередной попытки.
func (g *fakeGateway) next(ctx context.Context) error {
	g.mu.Lock()
	g.calls++
	var err error
	if len(g.errs) > 0 {
		err = g.errs[0]
		g.errs = g.errs[1:]
	}
	if g.alwaysErr != nil {
		err = g.alwaysErr
	}
	delay := g.delay
	block := g.block
	onCall := g.onCall
	shouldPanic := g.panicNext
	g.panicNext = false
	g.mu.Unlock()

	if shouldPanic {
		panic("adapter panic")
	}
	if onCall != nil {
		onCall(ctx)
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// callCount возвращает число попыток адаптера.
func (g *fakeGateway) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// CreateInvoice выполняет поддельный вызов создания счета.
func (g *fakeGateway) CreateInvoice(ctx context.Context, _ domain.CreateInvoiceRequest) (domain.Invoice, error) {
	if err := g.next(ctx); err != nil {
		return domain.Invoice{}, err
	}
	return domain.Invoice{GatewayID: "inv-1", Provider: "fake", Address: "addr"}, nil
}

// GetPayment выполняет поддельный вызов статуса платежа.
func (g *fakeGateway) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	if err := g.next(ctx); err != nil {
		return domain.PaymentStatus{}, err
	}
	return domain.PaymentStatus{GatewayID: gatewayID, State: domain.StateDetected}, nil
}

// CreatePayout выполняет поддельный вызов создания выплаты.
func (g *fakeGateway) CreatePayout(ctx context.Context, _ domain.PayoutRequest) (domain.Payout, error) {
	if err := g.next(ctx); err != nil {
		return domain.Payout{}, err
	}
	return domain.Payout{GatewayPayoutID: "po-1", Provider: "fake", State: domain.PayoutSubmitted}, nil
}

// GetPayout выполняет поддельный вызов статуса выплаты.
func (g *fakeGateway) GetPayout(ctx context.Context, _ string) (domain.PayoutStatus, error) {
	if err := g.next(ctx); err != nil {
		return domain.PayoutStatus{}, err
	}
	return domain.PayoutStatus{State: domain.PayoutConfirmed}, nil
}

// VerifyWebhook возвращает каноническое событие без сети.
func (g *fakeGateway) VerifyWebhook(_ map[string][]string, _ []byte) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{Type: domain.StateDetected, Provider: "fake"}, nil
}

// Capabilities возвращает возможности поддельного провайдера.
func (g *fakeGateway) Capabilities() domain.Capabilities {
	return g.capability
}

// testConfig возвращает конфигурацию с мгновенными детерминированными ретраями.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.RetryJitter = 0
	return cfg
}

// newTestResilient собирает декоратор с мгновенным записывающим таймером.
func newTestResilient(t *testing.T, inner domain.PaymentGateway, cfg Config) (*Resilient, *recordingTimer) {
	t.Helper()
	timer := newRecordingTimer()
	return New(inner, cfg, WithTimer(timer)), timer
}

func TestResilientSuccessPassthrough(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{}
	resilient, clock := newTestResilient(t, inner, testConfig())

	invoice, err := resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "test-key"})
	require.NoError(t, err)
	assert.Equal(t, "inv-1", invoice.GatewayID)
	assert.Equal(t, 1, inner.callCount())
	assert.Empty(t, clock.delays(), "успех не планирует повторов")
}

func TestResilientRetriesTemporary(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{errs: []error{
		fmt.Errorf("%w: connect refused", domain.ErrTemporary),
		fmt.Errorf("%w: 502", domain.ErrTemporary),
	}}
	resilient, clock := newTestResilient(t, inner, testConfig())

	status, err := resilient.GetPayment(context.Background(), "gw-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateDetected, status.State)
	assert.Equal(t, 3, inner.callCount())
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 150 * time.Millisecond}, clock.delays())
}

func TestResilientStopsOnPermanent(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{errs: []error{fmt.Errorf("%w: address rejected", domain.ErrRejected)}}
	resilient, clock := newTestResilient(t, inner, testConfig())

	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.ErrorIs(t, err, domain.ErrRejected)
	assert.Equal(t, 1, inner.callCount(), "постоянный отказ не повторяется")
	assert.Empty(t, clock.delays())
}

func TestResilientUnknownErrorIsPermanent(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{errs: []error{errors.New("weird")}}
	resilient, _ := newTestResilient(t, inner, testConfig())

	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.Error(t, err)
	assert.Equal(t, 1, inner.callCount(), "неизвестная ошибка не повторяется (fail-fast)")
}

func TestResilientExhaustsAttempts(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{errs: []error{
		fmt.Errorf("%w: a", domain.ErrTemporary),
		fmt.Errorf("%w: b", domain.ErrTemporary),
		fmt.Errorf("%w: c", domain.ErrTemporary),
	}}
	resilient, clock := newTestResilient(t, inner, testConfig())

	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "причина сохранена в цепочке")
	assert.Equal(t, 3, inner.callCount())
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 150 * time.Millisecond}, clock.delays())
}

func TestResilientAttemptTimeoutRetried(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.AttemptTimeout = 10 * time.Millisecond
	cfg.MaxAttempts = 2
	inner := &fakeGateway{delay: 50 * time.Millisecond}
	resilient, _ := newTestResilient(t, inner, cfg)

	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "таймаут попытки классифицируется как временный")
	assert.Equal(t, 2, inner.callCount())
}

func TestResilientCallerCancelStopsRetries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	inner := &fakeGateway{
		onCall: func(context.Context) { cancel() },
		errs:   []error{fmt.Errorf("%w: temporary", domain.ErrTemporary)},
	}
	cfg := testConfig()
	resilient, _ := newTestResilient(t, inner, cfg)

	_, err := resilient.CreateInvoice(ctx, domain.CreateInvoiceRequest{IdempotencyKey: "test-key"})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, inner.callCount(), "отмена вызывающего не запускает новые попытки")
}

func TestResilientBreakerOpensAndRecovers(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BreakerFailureThreshold = 2
	cfg.BreakerTimeout = 20 * time.Millisecond
	inner := &fakeGateway{errs: []error{
		fmt.Errorf("%w: a", domain.ErrTemporary),
		fmt.Errorf("%w: b", domain.ErrTemporary),
	}}
	resilient, _ := newTestResilient(t, inner, cfg)

	for i := 0; i < 2; i++ {
		_, err := resilient.GetPayment(context.Background(), "gw-1")
		require.Error(t, err, "попытка %d", i)
	}
	require.Eventually(t, func() bool { return resilient.breaker.State() == gobreaker.StateOpen },
		200*time.Millisecond, time.Millisecond, "breaker открыт после порога")

	callsAfterFailures := inner.callCount()
	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.ErrorIs(t, err, domain.ErrProviderUnavailable, "breaker открыт")
	assert.Equal(t, callsAfterFailures, inner.callCount(), "открытый breaker не обращается к провайдеру")

	require.Eventually(t, func() bool { return resilient.breaker.State() == gobreaker.StateHalfOpen },
		500*time.Millisecond, time.Millisecond, "breaker переходит в half-open")
	status, err := resilient.GetPayment(context.Background(), "gw-1")
	require.NoError(t, err, "успешная проба закрывает breaker")
	assert.Equal(t, domain.StateDetected, status.State)
	assert.Equal(t, gobreaker.StateClosed, resilient.breaker.State())
}

func TestResilientBreakerIgnoresBusinessRejections(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BreakerFailureThreshold = 2
	inner := &fakeGateway{errs: []error{
		fmt.Errorf("%w: address rejected", domain.ErrRejected),
		fmt.Errorf("%w: address rejected", domain.ErrRejected),
		fmt.Errorf("%w: address rejected", domain.ErrRejected),
	}}
	resilient, _ := newTestResilient(t, inner, cfg)

	for i := 0; i < 3; i++ {
		_, err := resilient.GetPayment(context.Background(), "gw-1")
		require.ErrorIs(t, err, domain.ErrRejected, "бизнес-отказ %d", i)
	}
	assert.Equal(t, gobreaker.StateClosed, resilient.breaker.State(),
		"бизнес-отказы не размыкают breaker для всех мерчантов")

	status, err := resilient.GetPayment(context.Background(), "gw-1")
	require.NoError(t, err, "провайдер здоров, вызовы продолжаются")
	assert.Equal(t, domain.StateDetected, status.State)
}

func TestResilientBreakerHalfOpenFailureReopens(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BreakerFailureThreshold = 1
	cfg.BreakerTimeout = 20 * time.Millisecond
	inner := &fakeGateway{errs: []error{
		fmt.Errorf("%w: a", domain.ErrTemporary),
		fmt.Errorf("%w: b", domain.ErrTemporary),
		fmt.Errorf("%w: c", domain.ErrTemporary),
	}}
	resilient, _ := newTestResilient(t, inner, cfg)

	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.Error(t, err)
	require.Eventually(t, func() bool { return resilient.breaker.State() == gobreaker.StateHalfOpen },
		500*time.Millisecond, time.Millisecond)

	_, err = resilient.GetPayment(context.Background(), "gw-1")
	require.Error(t, err, "неудачная проба")
	require.Eventually(t, func() bool { return resilient.breaker.State() == gobreaker.StateOpen },
		200*time.Millisecond, time.Millisecond, "неудачная проба снова размыкает breaker")
}

func TestResilientBulkheadRejectsOverload(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.BulkheadMax = 1
	block := make(chan struct{})
	inner := &fakeGateway{block: block}
	resilient, _ := newTestResilient(t, inner, cfg)

	done := make(chan error, 1)
	go func() {
		_, err := resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "test-key"})
		done <- err
	}()
	require.Eventually(t, func() bool { return inner.callCount() == 1 }, time.Second, time.Millisecond, "первый вызов занял слот")

	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.ErrorIs(t, err, domain.ErrProviderOverloaded, "второй вызов отклоняется мгновенно")
	assert.Equal(t, 1, inner.callCount())

	close(block)
	require.NoError(t, <-done)
	_, err = resilient.GetPayment(context.Background(), "gw-1")
	require.NoError(t, err, "слот освобожден")
}

func TestResilientBulkheadReleasedOnFailure(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BulkheadMax = 1
	inner := &fakeGateway{errs: []error{fmt.Errorf("%w: x", domain.ErrTemporary)}}
	resilient, _ := newTestResilient(t, inner, cfg)

	_, err := resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "test-key"})
	require.Error(t, err)

	_, err = resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "test-key"})
	require.NoError(t, err, "после ошибки слот bulkhead освобожден")
}

func TestResilientLocalOperationsBypassBulkhead(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.BulkheadMax = 1
	block := make(chan struct{})
	inner := &fakeGateway{block: block, capability: domain.Capabilities{
		Provider: "fake",
		Networks: map[string]domain.NetworkCapability{"trc20": {Asset: "USDT"}},
	}}
	resilient, _ := newTestResilient(t, inner, cfg)

	done := make(chan error, 1)
	go func() {
		_, err := resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "test-key"})
		done <- err
	}()
	require.Eventually(t, func() bool { return inner.callCount() == 1 }, time.Second, time.Millisecond)

	event, err := resilient.VerifyWebhook(nil, nil)
	require.NoError(t, err, "локальная проверка вебхука не требует bulkhead")
	assert.Equal(t, domain.StateDetected, event.Type)
	assert.Equal(t, "fake", resilient.Capabilities().Provider)

	close(block)
	require.NoError(t, <-done)
}

func TestResilientNilGateway(t *testing.T) {
	t.Parallel()

	resilient := New(nil, testConfig())
	_, err := resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "test-key"})
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
	_, err = resilient.VerifyWebhook(nil, nil)
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
	assert.Empty(t, resilient.Capabilities().Networks)
}

func TestResilientBreakerNameFromCapabilities(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{capability: domain.Capabilities{Provider: "sandbox"}}
	resilient := New(inner, testConfig())
	assert.Equal(t, "sandbox", resilient.breaker.Name())
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"temporary", fmt.Errorf("wrap: %w", domain.ErrTemporary), true},
		{"attempt deadline", fmt.Errorf("wrap: %w", context.DeadlineExceeded), true},
		{"caller canceled", context.Canceled, false},
		{"rejected", domain.ErrRejected, false},
		{"invalid route", domain.ErrInvalidRoute, false},
		{"unsupported network", domain.ErrUnsupportedNetwork, false},
		{"breaker open", gobreaker.ErrOpenState, false},
		{"breaker too many", gobreaker.ErrTooManyRequests, false},
		{"unknown", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isRetryable(tt.err))
		})
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"CONNECTOR_RETRY_MAX_ATTEMPTS":         "5",
		"CONNECTOR_RETRY_BASE_DELAY":           "250ms",
		"CONNECTOR_RETRY_MAX_DELAY":            "2s",
		"CONNECTOR_BREAKER_FAILURE_THRESHOLD":  "7",
		"CONNECTOR_BREAKER_TIMEOUT":            "1m",
		"CONNECTOR_BREAKER_HALF_OPEN_REQUESTS": "2",
		"CONNECTOR_BULKHEAD_MAX_CONCURRENT":    "16",
		"CONNECTOR_PROVIDER_ATTEMPT_TIMEOUT":   "1500ms",
	}
	cfg := ConfigFromEnv(func(key string) string { return values[key] })
	assert.Equal(t, 5, cfg.MaxAttempts)
	assert.Equal(t, 250*time.Millisecond, cfg.RetryBase)
	assert.Equal(t, 2*time.Second, cfg.RetryMax)
	assert.Equal(t, uint32(7), cfg.BreakerFailureThreshold)
	assert.Equal(t, time.Minute, cfg.BreakerTimeout)
	assert.Equal(t, uint32(2), cfg.BreakerHalfOpenRequests)
	assert.Equal(t, 16, cfg.BulkheadMax)
	assert.Equal(t, 1500*time.Millisecond, cfg.AttemptTimeout)
}

func TestConfigFromEnvInvalidFallsBack(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"CONNECTOR_RETRY_MAX_ATTEMPTS":       "zero",
		"CONNECTOR_RETRY_BASE_DELAY":         "-1s",
		"CONNECTOR_BREAKER_TIMEOUT":          "soon",
		"CONNECTOR_BULKHEAD_MAX_CONCURRENT":  "0",
		"CONNECTOR_PROVIDER_ATTEMPT_TIMEOUT": "never",
	}
	cfg := ConfigFromEnv(func(key string) string { return values[key] })
	assert.Equal(t, DefaultConfig(), cfg, "некорректные значения игнорируются")
}

func TestResilientBulkheadReleasedOnPanic(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BulkheadMax = 1
	inner := &fakeGateway{panicNext: true}
	resilient, _ := newTestResilient(t, inner, cfg)

	require.Panics(t, func() {
		_, _ = resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "k-1"})
	}, "паника адаптера пробрасывается наружу")

	_, err := resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{IdempotencyKey: "k-2"})
	require.NoError(t, err, "слот bulkhead освобожден при панике")
}

func TestResilientMissingIdempotencyKey(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{}
	resilient, _ := newTestResilient(t, inner, testConfig())

	_, err := resilient.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{})
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest, "пустой ключ идемпотентности отклоняется")

	_, err = resilient.CreatePayout(context.Background(), domain.PayoutRequest{})
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)

	assert.Zero(t, inner.callCount(), "провайдер не вызывается без ключа идемпотентности")
}

func TestResilientPayoutMethods(t *testing.T) {
	t.Parallel()

	inner := &fakeGateway{}
	resilient, _ := newTestResilient(t, inner, testConfig())

	payout, err := resilient.CreatePayout(context.Background(), domain.PayoutRequest{IdempotencyKey: "po-1"})
	require.NoError(t, err)
	assert.Equal(t, "po-1", payout.GatewayPayoutID)

	status, err := resilient.GetPayout(context.Background(), "gateway-po-1")
	require.NoError(t, err)
	assert.Equal(t, domain.PayoutConfirmed, status.State)
	assert.Equal(t, 2, inner.callCount())
}

func TestNormalizeConfig(t *testing.T) {
	t.Parallel()

	resilient := New(&fakeGateway{}, Config{
		MaxAttempts:             -1,
		RetryBase:               2 * time.Hour,
		RetryMax:                time.Second,
		RetryJitter:             5,
		BreakerFailureThreshold: 0,
		BreakerHalfOpenRequests: 1000,
		BulkheadMax:             1000000,
		AttemptTimeout:          2 * time.Hour,
	})
	assert.Equal(t, DefaultConfig().MaxAttempts, resilient.cfg.MaxAttempts)
	assert.Equal(t, time.Second, resilient.cfg.RetryBase, "база не превышает потолок")
	assert.Equal(t, time.Second, resilient.cfg.RetryMax)
	assert.Equal(t, 1.0, resilient.cfg.RetryJitter)
	assert.Equal(t, DefaultConfig().BreakerFailureThreshold, resilient.cfg.BreakerFailureThreshold)
	assert.Equal(t, uint32(maxBreakerHalfOpenLimit), resilient.cfg.BreakerHalfOpenRequests)
	assert.Equal(t, maxBulkheadLimit, resilient.cfg.BulkheadMax)
	assert.Equal(t, maxAttemptTimeoutLimit, resilient.cfg.AttemptTimeout)

	zero := New(&fakeGateway{}, Config{})
	defaults := DefaultConfig()
	assert.Equal(t, defaults.MaxAttempts, zero.cfg.MaxAttempts)
	assert.Equal(t, defaults.RetryBase, zero.cfg.RetryBase)
	assert.Equal(t, defaults.RetryMax, zero.cfg.RetryMax)
	assert.Equal(t, defaults.BreakerFailureThreshold, zero.cfg.BreakerFailureThreshold)
	assert.Equal(t, defaults.BreakerTimeout, zero.cfg.BreakerTimeout)
	assert.Equal(t, defaults.BreakerHalfOpenRequests, zero.cfg.BreakerHalfOpenRequests)
	assert.Equal(t, defaults.BulkheadMax, zero.cfg.BulkheadMax)
	assert.Zero(t, zero.cfg.RetryJitter, "нулевой jitter допустим (детерминированные тесты)")
}

func TestResilientBreakerLogsStateChange(t *testing.T) {
	t.Parallel()

	core, observed := observer.New(zap.WarnLevel)
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.BreakerFailureThreshold = 1
	inner := &fakeGateway{errs: []error{fmt.Errorf("%w: a", domain.ErrTemporary)}}
	resilient := New(inner, cfg, WithTimer(newRecordingTimer()), WithLogger(zap.New(core)))

	_, err := resilient.GetPayment(context.Background(), "gw-1")
	require.Error(t, err)

	require.Eventually(t, func() bool { return observed.Len() > 0 }, time.Second, time.Millisecond,
		"размыкание breaker обязано логироваться")
	entry := observed.FilterMessage("provider breaker state changed").All()
	require.NotEmpty(t, entry)
	contextMap := entry[0].ContextMap()
	assert.Equal(t, "open", contextMap["to"])
}

func TestResilientCallerDeadlineCapsAttempts(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxAttempts = 10
	cfg.AttemptTimeout = 50 * time.Millisecond
	inner := &fakeGateway{
		delay:     40 * time.Millisecond,
		alwaysErr: fmt.Errorf("%w: x", domain.ErrTemporary),
	}
	resilient, _ := newTestResilient(t, inner, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := resilient.GetPayment(ctx, "gw-1")
	require.ErrorIs(t, err, context.DeadlineExceeded, "дедлайн вызывающего прекращает попытки")
	assert.Less(t, inner.callCount(), cfg.MaxAttempts, "попытки ограничены бюджетом вызывающего")
}

func TestResilientDefaultJitter(t *testing.T) {
	t.Parallel()

	resilient := New(&fakeGateway{}, DefaultConfig())
	policy := resilient.newBackoff()
	assert.Equal(t, DefaultConfig().RetryJitter, policy.RandomizationFactor)
	assert.InDelta(t, 1.5, policy.Multiplier, 0.0001, "множитель backoff зафиксирован библиотекой")
}

func TestBackoffPolicyShape(t *testing.T) {
	t.Parallel()

	resilient := New(&fakeGateway{}, testConfig())
	policy := resilient.newBackoff()
	_ = backoff.WithContext(policy, context.Background())
	assert.Equal(t, 100*time.Millisecond, policy.InitialInterval)
	assert.Equal(t, time.Second, policy.MaxInterval)
	assert.Zero(t, policy.RandomizationFactor)
	assert.Zero(t, policy.MaxElapsedTime)
}
