package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// mustMoney builds a Money value for tests.
func mustMoney(t *testing.T, minor int64) money.Money {
	t.Helper()
	value, err := money.New(minor, "USDT", 2)
	require.NoError(t, err)
	return value
}

// payoutWorkerStore is an in-memory PayoutStatusStore.
type payoutWorkerStore struct {
	rows           []storage.ProviderPayoutRow
	resolveApplied bool
	touched        []string
	resolved       []string
	patches        []storage.ProviderPayoutPatch
}

func (s *payoutWorkerStore) ListPending(context.Context, int, time.Time) ([]storage.ProviderPayoutRow, error) {
	return s.rows, nil
}

func (s *payoutWorkerStore) TouchSubmittedTx(_ context.Context, _ *gorm.DB, payoutID string, _ time.Time) error {
	s.touched = append(s.touched, payoutID)
	return nil
}

func (s *payoutWorkerStore) ResolveTx(_ context.Context, _ *gorm.DB, payoutID, state string, patch storage.ProviderPayoutPatch, _ time.Time) (bool, error) {
	s.resolved = append(s.resolved, state)
	s.patches = append(s.patches, patch)
	return s.resolveApplied, nil
}

func submittedRow(gatewayPayoutID string) storage.ProviderPayoutRow {
	return storage.ProviderPayoutRow{
		PayoutID:        payoutTestID,
		Provider:        "coingate",
		GatewayPayoutID: &gatewayPayoutID,
		State:           "submitted",
		AmountMinor:     100_000,
		Asset:           "USDT",
		Decimals:        2,
		Network:         "trc20",
	}
}

func TestPayoutStatusWorkerResolvesConfirmed(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{
				State: domain.PayoutConfirmed,
				TXID:  "tx-1",
				Fee:   mustMoney(t, 250),
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
	assert.Equal(t, 1, resolved)
	require.Len(t, store.resolved, 1)
	assert.Equal(t, "confirmed", store.resolved[0])
	require.Len(t, store.patches, 1)
	require.NotNil(t, store.patches[0].TXID)
	assert.Equal(t, "tx-1", *store.patches[0].TXID)
	require.NotNil(t, store.patches[0].FeeMinor)
	assert.Equal(t, int64(250), *store.patches[0].FeeMinor)

	require.Len(t, appender.envelopes, 1)
	assert.Equal(t, "gateway.payout.confirmed", appender.envelopes[0].Headers["event_type"])
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED, event.GetType())
	assert.Equal(t, "tx-1", event.GetTxid())
	assert.Equal(t, int64(250), event.GetFee().GetMinorUnits())
	assert.Equal(t, "coingate", event.GetProvider())
}

func TestPayoutStatusWorkerRepeatedVerdictDoesNotRepublish(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{State: domain.PayoutConfirmed, TXID: "tx-1"}, nil
		},
	}
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{submittedRow("po-1")}, resolveApplied: false}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, resolved, "a lost CAS publishes nothing")
	assert.Empty(t, appender.envelopes)
}

func TestPayoutStatusWorkerTouchesNonTerminal(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{State: domain.PayoutSubmitted}, nil
		},
	}
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{submittedRow("po-1")}}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, resolved)
	assert.Equal(t, []string{payoutTestID}, store.touched)
	assert.Empty(t, store.resolved)
	assert.Empty(t, appender.envelopes)
}

func TestPayoutStatusWorkerFailsMissingGatewayPayout(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{}, fmt.Errorf("%w: gone", domain.ErrGatewayNotFound)
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
	require.Len(t, store.resolved, 1)
	assert.Equal(t, "failed", store.resolved[0])
	require.Len(t, store.patches, 1)
	require.NotNil(t, store.patches[0].Reason)
	assert.Equal(t, "gateway payout not found", *store.patches[0].Reason)
}

func TestPayoutStatusWorkerTemporaryReadKeepsRow(t *testing.T) {
	t.Parallel()
	gateway := &payoutGateway{
		provider: "coingate",
		status: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{}, errors.New("temporary read failure")
		},
	}
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{submittedRow("po-1")}}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, resolved)
	assert.Equal(t, []string{payoutTestID}, store.touched)
	assert.Empty(t, store.resolved)
}
