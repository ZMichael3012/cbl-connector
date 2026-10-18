package usecase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
	"github.com/cbl/cbl-connector/internal/usecase"
)

func TestPayoutProcessorRefundRejected(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")},
		refund: func(context.Context, domain.RefundRequest) (domain.Refund, error) {
			return domain.Refund{}, fmt.Errorf("%w: no funds", domain.ErrRejected)
		},
	}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "coingate", GatewayID: "inv-1"}}
	processor, _, appender := newRefundProcessor(t, gateway, "coingate", mapping)
	require.NoError(t, processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000)))
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, "provider rejected the refund", event.GetReason())
}

func TestPayoutProcessorRefundUnusableProviderID(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")},
		refund: func(context.Context, domain.RefundRequest) (domain.Refund, error) {
			return domain.Refund{GatewayRefundID: "bad\nid", Provider: "coingate"}, nil
		},
	}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "coingate", GatewayID: "inv-1"}}
	processor, _, _ := newRefundProcessor(t, gateway, "coingate", mapping)
	err := processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no usable refund id")
}

func TestPayoutProcessorRefundAttributionMismatch(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")},
		refund: func(context.Context, domain.RefundRequest) (domain.Refund, error) {
			return domain.Refund{GatewayRefundID: "rf-1", Provider: "bitpay"}, nil
		},
	}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "coingate", GatewayID: "inv-1"}}
	processor, _, _ := newRefundProcessor(t, gateway, "coingate", mapping)
	err := processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attributed")
}

func TestPayoutProcessorRefundWithoutMappingWiring(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}}
	router, err := usecase.NewPayoutRouter(map[string]domain.PaymentGateway{"coingate": gateway}, map[string]bool{"coingate": true})
	require.NoError(t, err)
	processor := usecase.NewPayoutProcessor(router, &payoutStore{}, &payoutAppender{}, nil)
	handleErr := processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000))
	require.Error(t, handleErr)
	assert.Contains(t, handleErr.Error(), "not configured")
}

func TestPayoutProcessorRefundInvalidMarker(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "coingate", GatewayID: "inv-1"}}
	processor, _, _ := newRefundProcessor(t, gateway, "coingate", mapping)
	command := &eventsv1.PayoutCommand{
		PayoutId: refundTestID, Amount: &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
		Network: "trc20", Address: "addr", IdempotencyKey: "key", RefundInvoiceId: "not-a-uuid",
	}
	body, err := proto.Marshal(command)
	require.NoError(t, err)
	handleErr := processor.Handle(context.Background(), nil, inbox.Message{Value: body})
	require.Error(t, handleErr)
	assert.Contains(t, handleErr.Error(), "invalid payout command")
}

func TestPayoutStatusWorkerRefundFailedVerdict(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate"},
		getRefund: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{State: domain.PayoutFailed, Reason: "refund rejected"}, nil
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
	require.Len(t, appender.envelopes, 1)
	assert.Equal(t, "gateway.payout.failed", appender.envelopes[0].Headers["event_type"])
}

func TestPayoutStatusWorkerRefundReadErrorTouches(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate"},
		getRefund: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{}, fmt.Errorf("%w: timeout", domain.ErrTemporary)
		},
	}
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

func TestPayoutStatusWorkerRefundUnsupportedSentinelTouches(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate"},
		getRefund: func(context.Context, string) (domain.PayoutStatus, error) {
			return domain.PayoutStatus{}, fmt.Errorf("%w: nope", domain.ErrRefundsUnsupported)
		},
	}
	row := submittedRow("rf-1")
	row.PayoutID = refundTestID
	row.RefundInvoiceID = stringPtrValue(refundInvoiceTestID)
	store := &payoutWorkerStore{rows: []storage.ProviderPayoutRow{row}}
	appender := &payoutAppender{}
	worker := usecase.NewPayoutStatusWorker(nil,
		map[string]domain.PaymentGateway{"coingate": gateway},
		store, appender, time.Second, 100, nil)

	resolved, err := worker.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, resolved)
	assert.Equal(t, []string{refundTestID}, store.touched)
	assert.Empty(t, appender.envelopes)
}

func TestPayoutProcessorRefundWrappedUnsupported(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{
		payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")},
		refund: func(context.Context, domain.RefundRequest) (domain.Refund, error) {
			// The decorators always satisfy Refunder and report the unsupported
			// capability with this sentinel (T-CONNECTOR-20).
			return domain.Refund{}, fmt.Errorf("%w: refunds are not supported", domain.ErrRefundsUnsupported)
		},
	}
	mapping := &fakeRefundMapping{invoice: domain.ProviderInvoice{Provider: "coingate", GatewayID: "inv-1"}}
	processor, _, appender := newRefundProcessor(t, gateway, "coingate", mapping)

	require.NoError(t, processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000)))
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, "provider does not support refunds", event.GetReason())
}

func TestPayoutProcessorRefundAmbiguousMapping(t *testing.T) {
	t.Parallel()
	gateway := &refundGateway{payoutGateway: payoutGateway{provider: "coingate", caps: payoutCapabilities("coingate", "trc20", "USDT")}}
	mapping := &fakeRefundMapping{err: fmt.Errorf("%w: payment", storage.ErrProviderInvoiceAmbiguous)}
	processor, _, appender := newRefundProcessor(t, gateway, "coingate", mapping)

	require.NoError(t, processor.Handle(context.Background(), nil, refundCommandMessage(t, 50_000)))
	assert.Zero(t, gateway.refundCalls)
	require.Len(t, appender.envelopes, 1)
	var event eventsv1.GatewayPayoutEvent
	require.NoError(t, proto.Unmarshal(appender.envelopes[0].Payload, &event))
	assert.Equal(t, "refund invoice has multiple provider mappings", event.GetReason())
}
