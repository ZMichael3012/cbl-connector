package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/usecase"
)

const payoutTestID = "01a14f70-0000-7000-8000-000000000301"

// payoutGateway is a controllable provider gateway for the payout pipeline.
type payoutGateway struct {
	provider string
	caps     domain.Capabilities
	create   func(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error)
	status   func(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error)
	calls    int
}

func (g *payoutGateway) CreateInvoice(context.Context, domain.CreateInvoiceRequest) (domain.Invoice, error) {
	return domain.Invoice{}, errors.New("unused")
}

func (g *payoutGateway) GetPayment(context.Context, string) (domain.PaymentStatus, error) {
	return domain.PaymentStatus{}, errors.New("unused")
}

func (g *payoutGateway) VerifyWebhook(map[string][]string, []byte) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{}, errors.New("unused")
}

func (g *payoutGateway) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	g.calls++
	if g.create == nil {
		return domain.Payout{}, errors.New("create not configured")
	}
	return g.create(ctx, req)
}

func (g *payoutGateway) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	if g.status == nil {
		return domain.PayoutStatus{}, errors.New("status not configured")
	}
	return g.status(ctx, gatewayPayoutID)
}

func (g *payoutGateway) Capabilities() domain.Capabilities { return g.caps }

// payoutStore is an in-memory PayoutStore.
type payoutStore struct {
	existing  *storage.ProviderPayoutRow
	inserted  *storage.ProviderPayoutRow
	duplicate bool
	calls     []string
}

func (s *payoutStore) LockPayoutTx(context.Context, *gorm.DB, string) error {
	s.calls = append(s.calls, "lock")
	return nil
}

func (s *payoutStore) Get(context.Context, string) (storage.ProviderPayoutRow, error) {
	s.calls = append(s.calls, "get")
	if s.existing != nil {
		return *s.existing, nil
	}
	return storage.ProviderPayoutRow{}, storage.ErrProviderPayoutNotFound
}

func (s *payoutStore) InsertSubmittedTx(_ context.Context, _ *gorm.DB, row storage.ProviderPayoutRow) (bool, error) {
	s.calls = append(s.calls, "insert")
	if s.duplicate {
		return false, nil
	}
	s.inserted = &row
	return true, nil
}

// payoutAppender records outbox envelopes.
type payoutAppender struct {
	envelopes []outbox.Envelope
}

func (a *payoutAppender) Append(_ context.Context, _ *gorm.DB, env outbox.Envelope) error {
	a.envelopes = append(a.envelopes, env)
	return nil
}

func payoutCapabilities(provider, network, asset string) domain.Capabilities {
	return domain.Capabilities{
		Provider: provider,
		Networks: map[string]domain.NetworkCapability{
			network: {Asset: asset},
		},
	}
}

func payoutCommandMessage(t *testing.T, payoutID string, minor int64) inbox.Message {
	t.Helper()
	command := &eventsv1.PayoutCommand{
		PayoutId:       payoutID,
		Amount:         &commonv1.Money{MinorUnits: minor, Asset: "USDT", Decimals: 2},
		Network:        "trc20",
		Address:        "TXyzAddress0123456789",
		IdempotencyKey: "payout-key-1",
	}
	body, err := proto.Marshal(command)
	require.NoError(t, err)
	return inbox.Message{Value: body, Headers: map[string]string{outbox.HeaderEventID: "evt-1"}}
}

func newPayoutProcessor(t *testing.T, gateway *payoutGateway, store *payoutStore, appender *payoutAppender) *usecase.PayoutProcessor {
	t.Helper()
	router, err := usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{gateway.provider: gateway},
		map[string]bool{gateway.provider: true},
	)
	require.NoError(t, err)
	return usecase.NewPayoutProcessor(router, store, appender, nil)
}

func TestPayoutProcessorSubmits(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{GatewayPayoutID: "po-1", Provider: "coingate", State: domain.PayoutSubmitted}, nil
		},
	}
	store := &payoutStore{}
	appender := &payoutAppender{}
	processor := newPayoutProcessor(t, gateway, store, appender)

	require.NoError(t, processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000)))
	assert.Equal(t, 1, gateway.calls)
	require.Len(t, appender.envelopes, 1)
	envelope := appender.envelopes[0]
	assert.Equal(t, usecase.TopicGatewayPayoutEvents, envelope.Topic)
	assert.Equal(t, payoutTestID, envelope.Key)
	assert.Equal(t, "gateway.payout.submitted", envelope.Headers[outbox.HeaderEventType])
	assert.Equal(t, "coingate", envelope.Headers["provider"])
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(envelope.Payload, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED, event.GetType())
	assert.Equal(t, "po-1", event.GetGatewayPayoutId())
	assert.Equal(t, "coingate", event.GetProvider())

	require.NotNil(t, store.inserted)
	assert.Equal(t, "coingate", store.inserted.Provider)
	assert.Equal(t, int64(100_000), store.inserted.AmountMinor)
	assert.NotEmpty(t, store.inserted.AddressHash)
	assert.NotContains(t, store.inserted.AddressHash, "TXyzAddress", "the mapping stores no address itself")
}

func TestPayoutProcessorReplaySkipsProvider(t *testing.T) {
	t.Parallel()
	existing := storage.ProviderPayoutRow{PayoutID: payoutTestID, Provider: "coingate", State: "submitted"}
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{GatewayPayoutID: "po-1"}, nil
		},
	}
	store := &payoutStore{existing: &existing}
	appender := &payoutAppender{}
	processor := newPayoutProcessor(t, gateway, store, appender)

	require.NoError(t, processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000)))
	assert.Zero(t, gateway.calls, "a recorded payout never reaches the provider again")
	assert.Empty(t, appender.envelopes)
}

func TestPayoutProcessorRejectionPublishesFailed(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{}, fmt.Errorf("%w: address blocked, token abc123", domain.ErrRejected)
		},
	}
	store := &payoutStore{}
	appender := &payoutAppender{}
	processor := newPayoutProcessor(t, gateway, store, appender)

	require.NoError(t, processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000)))
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED, event.GetType())
	assert.Equal(t, "provider rejected the payout", event.GetReason())
	assert.NotContains(t, event.GetReason(), "token", "the provider text stays in the log")
	assert.Nil(t, store.inserted, "a rejected payout is not recorded")
}

func TestPayoutProcessorUnsupportedNetwork(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "btc", "BTC"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{GatewayPayoutID: "po-1"}, nil
		},
	}
	store := &payoutStore{}
	appender := &payoutAppender{}
	processor := newPayoutProcessor(t, gateway, store, appender)

	require.NoError(t, processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000)))
	assert.Zero(t, gateway.calls)
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED, event.GetType())
	assert.Equal(t, "no provider supports the payout", event.GetReason())
}

func TestPayoutProcessorTemporaryFailureRetries(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
		create: func(context.Context, domain.PayoutRequest) (domain.Payout, error) {
			return domain.Payout{}, fmt.Errorf("%w: upstream timeout", domain.ErrTemporary)
		},
	}
	store := &payoutStore{}
	appender := &payoutAppender{}
	processor := newPayoutProcessor(t, gateway, store, appender)

	err := processor.Handle(context.Background(), nil, payoutCommandMessage(t, payoutTestID, 100_000))
	require.ErrorIs(t, err, domain.ErrTemporary)
	assert.Empty(t, appender.envelopes)
	assert.Nil(t, store.inserted)
}

func TestPayoutProcessorInvalidCommand(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		caps:     payoutCapabilities("coingate", "trc20", "USDT"),
	}
	processor := newPayoutProcessor(t, gateway, &payoutStore{}, &payoutAppender{})

	command := &eventsv1.PayoutCommand{
		PayoutId:       payoutTestID,
		Amount:         &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
		Network:        "trc20",
		Address:        " ",
		IdempotencyKey: "key",
	}
	body, err := proto.Marshal(command)
	require.NoError(t, err)
	handleErr := processor.Handle(context.Background(), nil, inbox.Message{Value: body})
	require.Error(t, handleErr)
	assert.Contains(t, handleErr.Error(), "invalid payout command")
}

func TestPayoutRouterSelectsSmallestCapableProvider(t *testing.T) {
	t.Parallel()
	bitpay := &payoutGateway{provider: "bitpay", caps: payoutCapabilities("bitpay", "trc20", "USDT")}
	coingate := &payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}
	router, err := usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{"coingate": coingate, "bitpay": bitpay},
		map[string]bool{"coingate": true, "bitpay": true},
	)
	require.NoError(t, err)

	gateway, provider, err := router.SelectPayout(context.Background(), domain.PayoutRequest{
		Network: "trc20",
		Amount:  mustMoney(t, 100_000),
	})
	require.NoError(t, err)
	assert.Equal(t, "bitpay", provider)
	assert.Same(t, bitpay, gateway)
}
