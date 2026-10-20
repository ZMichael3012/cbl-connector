package usecase_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/money"
	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/usecase"
)

func TestPayoutProcessorTooLargeMessage(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}
	processor := newPayoutProcessor(t, gateway, &payoutStore{}, &payoutAppender{})

	err := processor.Handle(context.Background(), nil, inbox.Message{Value: make([]byte, 70*1024)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too large")
}

func TestPayoutProcessorInvalidCommands(t *testing.T) {
	t.Parallel()
	cases := map[string]*eventsv1.PayoutCommand{
		"bad payout id": {
			PayoutId: "not-a-uuid", Amount: &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
			Network: "trc20", Address: "addr", IdempotencyKey: "key",
		},
		"zero amount": {
			PayoutId: payoutTestID, Amount: &commonv1.Money{MinorUnits: 0, Asset: "USDT", Decimals: 2},
			Network: "trc20", Address: "addr", IdempotencyKey: "key",
		},
		"bad network": {
			PayoutId: payoutTestID, Amount: &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
			Network: "has space", Address: "addr", IdempotencyKey: "key",
		},
		"empty key": {
			PayoutId: payoutTestID, Amount: &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
			Network: "trc20", Address: "addr", IdempotencyKey: " ",
		},
		"control runes in address": {
			PayoutId: payoutTestID, Amount: &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
			Network: "trc20", Address: "addr\nnext", IdempotencyKey: "key",
		},
	}
	for name, command := range cases {
		command := command
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gateway := &payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}
			processor := newPayoutProcessor(t, gateway, &payoutStore{}, &payoutAppender{})
			body, err := proto.Marshal(command)
			require.NoError(t, err)
			handleErr := processor.Handle(context.Background(), nil, inbox.Message{Value: body})
			require.Error(t, handleErr)
			assert.Contains(t, handleErr.Error(), "invalid payout command")
		})
	}
}

func TestPayoutProcessorConcurrentInsertReplay(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{GatewayPayoutID: "po-1", Provider: "coingate"}, nil
		},
	}
	store := &payoutStore{duplicate: true}
	appender := &payoutAppender{}
	processor := newPayoutProcessor(t, gateway, store, appender)

	require.NoError(t, processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000)))
	assert.Empty(t, appender.envelopes, "the losing concurrent delivery publishes nothing")
}

func TestPayoutProcessorProviderMismatch(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{GatewayPayoutID: "po-1", Provider: "bitpay"}, nil
		},
	}
	processor := newPayoutProcessor(t, gateway, &payoutStore{}, &payoutAppender{})

	err := processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attributed")
}

func TestPayoutProcessorMissingGatewayID(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{Provider: "coingate"}, nil
		},
	}
	processor := newPayoutProcessor(t, gateway, &payoutStore{}, &payoutAppender{})

	err := processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no usable payout id")
}

func TestPayoutRouterFailsClosed(t *testing.T) {
	t.Parallel()
	_, err := usecase.NewPayoutRouter(map[string]domain.PaymentGateway{}, nil)
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)

	gateway := &payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "btc", "BTC")}
	router, err := usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{"coingate": gateway},
		map[string]bool{"coingate": true},
	)
	require.NoError(t, err)
	_, _, selectErr := router.SelectPayout(context.Background(), domain.PayoutRequest{
		Network: "trc20",
		Amount:  mustMoney(t, 100),
	})
	require.ErrorIs(t, selectErr, domain.ErrUnsupportedNetwork)

	// The allowlist excludes the registered provider.
	_, err = usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{"coingate": gateway},
		map[string]bool{"bitpay": true},
	)
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
}

func TestPayoutStatusWorkerUnknownProviderSkips(t *testing.T) {
	t.Parallel()
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{submittedRow("po-1")}}
	store.rows[0].Provider = "unknown"
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil, map[string]domain.PaymentGateway{}, store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, resolved)
	assert.Empty(t, store.resolved)
	assert.Equal(t, []string{payoutTestID}, store.touched, "a parked row is touched to keep the batch rotating")
}

func TestPayoutProcessorLocksBeforeReplayCheck(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{GatewayPayoutID: "po-1", Provider: "coingate"}, nil
		},
	}
	store := &payoutStore{}
	processor := newPayoutProcessor(t, gateway, store, &payoutAppender{})

	require.NoError(t, processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000)))
	assert.Equal(t, []string{"lock", "get", "insert"}, store.calls,
		"the advisory lock is taken before the replay check (B1)")
}

func TestPayoutRouterScaleAndBounds(t *testing.T) {
	t.Parallel()
	min, err := money.New(10_000, "USDT", 2)
	require.NoError(t, err)
	maxAmount, err := money.New(200_000, "USDT", 2)
	require.NoError(t, err)
	gateway := &payoutGateway{
		provider: "coingate",
		caps: domain.Capabilities{
			Provider: "coingate",
			Networks: map[string]domain.NetworkCapability{
				"trc20": {Asset: "USDT", MinAmount: min, MaxAmount: maxAmount},
			},
		},
	}
	router, err := usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{"coingate": gateway},
		map[string]bool{"coingate": true},
	)
	require.NoError(t, err)
	ctx := context.Background()

	_, provider, err := router.SelectPayout(ctx, domain.PayoutRequest{Network: "trc20", Amount: mustMoney(t, 100_000)})
	require.NoError(t, err)
	assert.Equal(t, "coingate", provider)

	_, _, err = router.SelectPayout(ctx, domain.PayoutRequest{Network: "trc20", Amount: mustMoney(t, 5_000)})
	require.ErrorIs(t, err, domain.ErrUnsupportedNetwork, "below the minimum")

	_, _, err = router.SelectPayout(ctx, domain.PayoutRequest{Network: "trc20", Amount: mustMoney(t, 300_000)})
	require.ErrorIs(t, err, domain.ErrUnsupportedNetwork, "above the maximum")

	// A capability in another scale cannot cover the request.
	scaledAmount, err := money.New(10_000, "USDT", 6)
	require.NoError(t, err)
	scaled := &payoutGateway{
		provider: "coingate",
		caps: domain.Capabilities{
			Provider: "coingate",
			Networks: map[string]domain.NetworkCapability{"trc20": {Asset: "USDT", MinAmount: scaledAmount}},
		},
	}
	scaledRouter, err := usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{"coingate": scaled},
		map[string]bool{"coingate": true},
	)
	require.NoError(t, err)
	_, _, err = scaledRouter.SelectPayout(ctx, domain.PayoutRequest{Network: "trc20", Amount: mustMoney(t, 100_000)})
	require.ErrorIs(t, err, domain.ErrUnsupportedNetwork, "the capability scale differs")
}

func TestPayoutStatusWorkerRunStopsOnCancel(t *testing.T) {
	t.Parallel()
	worker := usecase.NewPayoutStatusWorker(nil, map[string]domain.PaymentGateway{}, &payoutWorkerStore{}, &payoutAppender{}, time.Second, 10, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := worker.Run(ctx)
	assert.True(t, errors.Is(err, context.Canceled))
}

func TestPayoutStatusWorkerSkipsRowWithoutGatewayID(t *testing.T) {
	t.Parallel()
	row := submittedRow("")
	row.GatewayPayoutID = nil
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{row}}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil, map[string]domain.PaymentGateway{}, store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, resolved)
	assert.Empty(t, store.resolved)
	assert.Equal(t, []string{payoutTestID}, store.touched, "a row without a gateway id is parked and touched")
}

func TestPayoutStatusWorkerSanitizesVerdictFields(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{
				State:  domain.PayoutConfirmed,
				TXID:   "bad\ntx",
				Fee:    money.Money{MinorUnits: 100, Asset: "USDT", Decimals: 30},
				Reason: "unused",
			}, nil
		},
	}
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{submittedRow("po-1")}, resolveApplied: true}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, resolved, "an unusable txid/fee must not wedge the verdict")
	require.Len(t, store.patches, 1)
	assert.Nil(t, store.patches[0].TXID, "the control-character txid is skipped")
	assert.Nil(t, store.patches[0].FeeMinor, "the invalid fee scale is skipped")

	// A provider failure reason is cleaned of control runes and still resolves.
	failedGateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{State: domain.PayoutFailed, Reason: "bad\n\x01reason"}, nil
		},
	}
	failedStore := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{submittedRow("po-1")}, resolveApplied: true}
	failedAppender := &payoutAppender{}
	failedWorker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": failedGateway},
		failedStore, failedAppender, time.Second, 100, nil)
	resolved, err = failedWorker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, resolved)
	require.Len(t, failedStore.patches, 1)
	require.NotNil(t, failedStore.patches[0].Reason)
	assert.Equal(t, "badreason", *failedStore.patches[0].Reason)
}

func TestPayoutStatusWorkerTruncatesFailureReason(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{State: domain.PayoutFailed, Reason: strings.Repeat("x", 400)}, nil
		},
	}
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{submittedRow("po-1")}, resolveApplied: true}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, resolved)
	require.Len(t, store.patches, 1)
	require.NotNil(t, store.patches[0].Reason)
	assert.LessOrEqual(t, len(*store.patches[0].Reason), 260)
	assert.True(t, strings.HasPrefix(*store.patches[0].Reason, "xxx"))
	assert.Equal(t, "gateway.payout.failed", appender.envelopes[0].Headers[outbox.HeaderEventType])
}
