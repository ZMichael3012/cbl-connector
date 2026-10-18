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

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/usecase"
)

const refundTestID = "01a14f70-0000-7000-8000-000000000601"
const refundInvoiceTestID = "01a14f70-0000-7000-8000-000000000602"

// refundGateway implements the full H1 refund surface of a provider.
type refundGateway struct {
	payoutGateway
	refundCalls int
	refund      func(ctx context.Context, req domain.RefundRequest) (domain.Refund, error)
	getRefund   func(ctx context.Context, gatewayRefundID string) (domain.PayoutStatus, error)
}

func (g *refundGateway) CreateRefund(ctx context.Context, req domain.RefundRequest) (domain.Refund, error) {
	g.refundCalls++
	if g.refund == nil {
		return domain.Refund{}, errors.New("refund not configured")
	}
	return g.refund(ctx, req)
}

func (g *refundGateway) GetRefund(ctx context.Context, gatewayRefundID string) (domain.PayoutStatus, error) {
	if g.getRefund == nil {
		return domain.PayoutStatus{}, errors.New("refund status not configured")
	}
	return g.getRefund(ctx, gatewayRefundID)
}

// payoutOnlyGateway deliberately lacks the Refunder interface.
type payoutOnlyGateway struct{ payoutGateway }

// fakeRefundMapping implements the refund invoice mapping.
type fakeRefundMapping struct {
	invoice      domain.ProviderInvoice
	err          error
	gotPaymentID string
}

func (m *fakeRefundMapping) ByPaymentID(_ context.Context, paymentID string) (domain.ProviderInvoice, error) {
	m.gotPaymentID = paymentID
	if m.err != nil {
		return domain.ProviderInvoice{}, m.err
	}
	return m.invoice, nil
}

func refundCommandMessage(t *testing.T, minor int64) inbox.Message {
	t.Helper()
	command := &eventsv1.PayoutCommand{
		PayoutId:        refundTestID,
		Amount:          &commonv1.Money{MinorUnits: minor, Asset: "USDT", Decimals: 2},
		Network:         "trc20",
		Address:         "TXyzAddress0123456789",
		IdempotencyKey:  "payout:" + refundTestID + ":command",
		RefundInvoiceId: refundInvoiceTestID,
	}
	body, err := proto.Marshal(command)
	require.NoError(t, err)
	return inbox.Message{Value: body, Headers: map[string]string{outbox.HeaderEventID: "evt-refund"}}
}

func newRefundProcessor(t *testing.T, gateway domain.PaymentGateway, enabled string, mapping usecase.RefundInvoiceMapping) (*usecase.PayoutProcessor, *payoutStore, *payoutAppender) {
	t.Helper()
	router, err := usecase.NewPayoutRouter(
		map[string]domain.PaymentGateway{enabled: gateway},
		map[string]bool{enabled: true},
	)
	require.NoError(t, err)
	store := &payoutStore{}
	appender := &payoutAppender{}
	processor := usecase.NewPayoutProcessor(router, store, appender, nil,
		usecase.WithRefundMapping(mapping))
	return processor, store, appender
}

func TestPayoutProcessorRefundSuccess(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")},
		refund: func(_ context.Context, req domain.RefundRequest) (domain.Refund, error) {
			assert.Equal(t, "inv-1", req.GatewayID)
			assert.Equal(t, refundInvoiceTestID, req.PaymentID)
			return domain.Refund{GatewayRefundID: "rf-1", Provider: "coingate"}, nil
		},
	}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "coingate", GatewayID: "inv-1", PaymentID: refundInvoiceTestID}}
	processor, store, appender := newRefundProcessor(t, gateway, "coingate", mapping)

	require.NoError(t, processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000)))
	assert.Equal(t, refundInvoiceTestID, mapping.gotPaymentID)
	assert.Equal(t, 1, gateway.refundCalls)
	assert.Equal(t, 0, gateway.calls, "a refund never calls the payout API")

	require.NotNil(t, store.inserted)
	assert.Equal(t, "coingate", store.inserted.Provider)
	require.NotNil(t, store.inserted.RefundInvoiceID)
	assert.Equal(t, refundInvoiceTestID, *store.inserted.RefundInvoiceID)
	require.NotNil(t, store.inserted.GatewayPayoutID)
	assert.Equal(t, "rf-1", *store.inserted.GatewayPayoutID)

	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED, event.GetType())
	assert.Equal(t, "coingate", event.GetProvider())
	assert.Equal(t, "rf-1", event.GetGatewayPayoutId())
}

func TestPayoutProcessorRefundMissingMapping(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}}
	mapping := &fakeRefundMapping{err: fmt.Errorf("%w: none", storage.ErrProviderInvoiceNotFound)}
	processor, store, appender := newRefundProcessor(t, gateway, "coingate", mapping)

	require.NoError(t, processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000)))
	assert.Zero(t, gateway.refundCalls)
	assert.Nil(t, store.inserted)
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED, event.GetType())
	assert.Equal(t, "refund invoice has no provider mapping", event.GetReason())
}

func TestPayoutProcessorRefundProviderDisabled(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "bitpay", GatewayID: "inv-1"}}
	processor, _, appender := newRefundProcessor(t, gateway, "coingate", mapping)

	require.NoError(t, processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000)))
	assert.Zero(t, gateway.refundCalls)
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, "provider refunds are not enabled", event.GetReason())
}

func TestPayoutProcessorRefundUnsupportedProvider(t *testing.T) {
	t.Parallel()
	gateway := &payoutOnlyGateway{payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "coingate", GatewayID: "inv-1"}}
	processor, _, appender := newRefundProcessor(t, gateway, "coingate", mapping)

	require.NoError(t, processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000)))
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, "provider does not support refunds", event.GetReason())
}

func TestPayoutProcessorRefundMappingErrorRetries(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}}
	mapping := &fakeRefundMapping{err: errors.New("db down")}
	processor, _, _ := newRefundProcessor(t, gateway, "coingate", mapping)

	err := processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mapping")
}

func TestPayoutStatusWorkerRefundUsesRefundStatus(t *testing.T) {
	t.Parallel()
	getRefundCalls := 0
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate"},
		getRefund: func(context.Context, string) (domain.PayoutStatus, error) {
			getRefundCalls++
			return domain.PayoutStatus{State: domain.PayoutConfirmed, TXID: "rf-tx-1"}, nil
		},
	}
	row := submittedRow("rf-1")
	row.PayoutID = refundTestID
	row.RefundInvoiceID = stringPtrValue(refundInvoiceTestID)
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{row}, resolveApplied: true}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, resolved)
	assert.Equal(t, 1, getRefundCalls)
	require.Len(t, appender.envelopes, 1)
	assert.Equal(t, "gateway.payout.confirmed", appender.envelopes[0].Headers["event_type"])
}

func TestPayoutStatusWorkerRefundWithoutStatusTouches(t *testing.T) {
	t.Parallel()
	gateway := &payoutOnlyGateway{payoutGateway: payoutGateway{provider: "coingate"}}
	row := submittedRow("rf-1")
	row.PayoutID = refundTestID
	row.RefundInvoiceID = stringPtrValue(refundInvoiceTestID)
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{row}}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, &payoutAppender{}, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, resolved)
	assert.Equal(t, []string{refundTestID}, store.touched)
}

// stringPtrValue is a tiny local helper.
func stringPtrValue(value string) *string { return &value }
