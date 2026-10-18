package gateway_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/ratelimit"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/gateway"
)

type fakeGateway struct {
	calls []string
}

func (f *fakeGateway) CreateInvoice(_ context.Context, _ domain.CreateInvoiceRequest) (domain.Invoice, error) {
	f.calls = append(f.calls, "create_invoice")
	return domain.Invoice{GatewayID: "inv-1"}, nil
}

func (f *fakeGateway) GetPayment(_ context.Context, _ string) (domain.PaymentStatus, error) {
	f.calls = append(f.calls, "get_payment")
	return domain.PaymentStatus{State: domain.StateDetected}, nil
}

func (f *fakeGateway) VerifyWebhook(_ map[string][]string, _ []byte) (domain.CanonicalEvent, error) {
	f.calls = append(f.calls, "verify_webhook")
	return domain.CanonicalEvent{}, nil
}

func (f *fakeGateway) CreatePayout(_ context.Context, _ domain.PayoutRequest) (domain.Payout, error) {
	f.calls = append(f.calls, "create_payout")
	return domain.Payout{GatewayPayoutID: "po-1"}, nil
}

func (f *fakeGateway) GetPayout(_ context.Context, _ string) (domain.PayoutStatus, error) {
	f.calls = append(f.calls, "get_payout")
	return domain.PayoutStatus{State: domain.PayoutSubmitted}, nil
}

func (f *fakeGateway) Capabilities() domain.Capabilities {
	f.calls = append(f.calls, "capabilities")
	return domain.Capabilities{Provider: "coingate"}
}

type fakeLimiter struct {
	decision ratelimit.Decision
	err      error
	keys     []string
	paused   map[string]time.Duration
	pauseErr error
}

func (f *fakeLimiter) Allow(_ context.Context, key string) (ratelimit.Decision, error) {
	f.keys = append(f.keys, key)
	return f.decision, f.err
}

func (f *fakeLimiter) Pause(_ context.Context, key string, d time.Duration) error {
	if f.paused == nil {
		f.paused = make(map[string]time.Duration)
	}
	f.paused[key] = d
	return f.pauseErr
}

func newLimited(t *testing.T, limiter gateway.Limiter) (*gateway.Limited, *fakeGateway) {
	t.Helper()
	next := &fakeGateway{}
	limited, err := gateway.NewLimited(next, limiter, "coingate")
	require.NoError(t, err)
	return limited, next
}

func TestLimitedAllowsAndKeysOperations(t *testing.T) {
	t.Parallel()
	limiter := &fakeLimiter{decision: ratelimit.Decision{Allowed: true, Remaining: 5}}
	limited, next := newLimited(t, limiter)
	ctx := context.Background()

	_, err := limited.CreateInvoice(ctx, domain.CreateInvoiceRequest{PaymentID: "p"})
	require.NoError(t, err)
	_, err = limited.GetPayment(ctx, "inv-1")
	require.NoError(t, err)
	_, err = limited.CreatePayout(ctx, domain.PayoutRequest{PayoutID: "po"})
	require.NoError(t, err)
	_, err = limited.GetPayout(ctx, "po-1")
	require.NoError(t, err)

	require.Equal(t, []string{"coingate:invoice", "coingate:status", "coingate:payout", "coingate:status"}, limiter.keys)
	require.Len(t, next.calls, 4)
}

func TestLimitedDeniesOverloaded(t *testing.T) {
	t.Parallel()
	limiter := &fakeLimiter{decision: ratelimit.Decision{Allowed: false, RetryAfter: 1500 * time.Millisecond}}
	limited, next := newLimited(t, limiter)

	_, err := limited.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{PaymentID: "p"})
	require.ErrorIs(t, err, domain.ErrProviderOverloaded)
	require.Contains(t, err.Error(), "1.5s")
	require.Empty(t, next.calls, "a denied token must not reach the provider")
}

func TestLimitedFailsOpenOnLimiterError(t *testing.T) {
	t.Parallel()
	limiter := &fakeLimiter{err: errors.New("redis down")}
	limited, next := newLimited(t, limiter)

	_, err := limited.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{PaymentID: "p"})
	require.NoError(t, err)
	require.Len(t, next.calls, 1, "a limiter storage failure must not block the money path")
}

func TestLimitedPassesThroughWithoutTokens(t *testing.T) {
	t.Parallel()
	limiter := &fakeLimiter{decision: ratelimit.Decision{Allowed: false}}
	limited, next := newLimited(t, limiter)

	_, err := limited.VerifyWebhook(map[string][]string{}, []byte("{}"))
	require.NoError(t, err)
	_ = limited.Capabilities()
	require.Empty(t, limiter.keys)
	require.Equal(t, []string{"verify_webhook", "capabilities"}, next.calls)
}

func TestLimitedPauseProvider(t *testing.T) {
	t.Parallel()
	limiter := &fakeLimiter{decision: ratelimit.Decision{Allowed: true}}
	limited, _ := newLimited(t, limiter)

	require.NoError(t, limited.PauseProvider(context.Background(), 30*time.Second))
	require.Equal(t, map[string]time.Duration{
		"coingate:invoice": 30 * time.Second,
		"coingate:status":  30 * time.Second,
		"coingate:payout":  30 * time.Second,
		"coingate:refund":  30 * time.Second,
	}, limiter.paused)

	limiter.pauseErr = errors.New("redis down")
	require.ErrorIs(t, limited.PauseProvider(context.Background(), time.Second), limiter.pauseErr)
}

func TestNewLimitedValidation(t *testing.T) {
	t.Parallel()
	_, err := gateway.NewLimited(nil, &fakeLimiter{}, "coingate")
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
	_, err = gateway.NewLimited(&fakeGateway{}, nil, "coingate")
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
	_, err = gateway.NewLimited(&fakeGateway{}, &fakeLimiter{}, "  ")
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
}

// limitedRefundStatus adds the optional refund status interface to the fake.
type limitedRefundStatus struct {
	*fakeGateway
	status domain.PayoutStatus
}

func (f *limitedRefundStatus) GetRefund(context.Context, string) (domain.PayoutStatus, error) {
	return f.status, nil
}

func TestLimitedGetRefund(t *testing.T) {
	t.Parallel()
	limiter := &fakeLimiter{decision: ratelimit.Decision{Allowed: true}}
	inner := &limitedRefundStatus{fakeGateway: &fakeGateway{}, status: domain.PayoutStatus{State: domain.PayoutConfirmed}}
	limited, err := gateway.NewLimited(inner, limiter, "coingate")
	require.NoError(t, err)

	status, err := limited.GetRefund(context.Background(), "rf-1")
	require.NoError(t, err)
	assert.Equal(t, domain.PayoutConfirmed, status.State)
	assert.Equal(t, []string{"coingate:status"}, limiter.keys)
}

func TestLimitedGetRefundUnsupported(t *testing.T) {
	t.Parallel()
	limited, _ := newLimited(t, &fakeLimiter{decision: ratelimit.Decision{Allowed: true}})
	_, err := limited.GetRefund(context.Background(), "rf-1")
	require.ErrorIs(t, err, domain.ErrRefundsUnsupported)
}

func TestLimitedCreateRefundUnsupported(t *testing.T) {
	t.Parallel()
	limited, _ := newLimited(t, &fakeLimiter{decision: ratelimit.Decision{Allowed: true}})
	_, err := limited.CreateRefund(context.Background(), domain.RefundRequest{IdempotencyKey: "k"})
	require.ErrorIs(t, err, domain.ErrRefundsUnsupported)
}
