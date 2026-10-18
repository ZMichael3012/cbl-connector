package nowpayments_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/nowpayments"
)

func TestExportPaymentsPage(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/payment/", r.URL.Path)
		query := r.URL.Query()
		assert.Equal(t, "3", query.Get("limit"))
		assert.Equal(t, "3", query.Get("page"))
		assert.Equal(t, "2026-10-18T00:00:00Z", query.Get("dateFrom"))
		assert.Equal(t, "2026-10-18T01:00:00Z", query.Get("dateTo"))
		_, _ = w.Write([]byte(`{"data":[
			{"payment_id":"gw-1","order_id":"01a14f70-0000-7000-8000-000000000001","payment_status":"finished","actually_paid":5,"pay_currency":"usdttrc20","updated_at":"2026-10-18T00:30:00Z"},
			{"payment_id":"gw-2","order_id":"01a14f70-0000-7000-8000-000000000002","payment_status":"partially_paid","actually_paid":2.5,"pay_currency":"usdttrc20"},
			{"payment_id":"gw-3","order_id":"01a14f70-0000-7000-8000-000000000003","payment_status":"strange","actually_paid":1,"pay_currency":"usdttrc20"}
		]}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:   time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit:  3,
		Cursor: domain.EncodeExportCursor(6),
	})
	require.NoError(t, err)
	require.Len(t, page.Payments, 3)
	first := page.Payments[0]
	assert.Equal(t, "gw-1", first.GatewayID)
	assert.Equal(t, "01a14f70-0000-7000-8000-000000000001", first.PaymentID)
	assert.Equal(t, domain.StateConfirmed, first.State)
	assert.Equal(t, "finished", first.RawStatus)
	assert.Equal(t, int64(5000000), first.ReceivedAmount.MinorUnits)
	assert.Equal(t, "USDT", first.ReceivedAmount.Asset)
	assert.Equal(t, uint32(6), first.ReceivedAmount.Decimals)
	assert.Equal(t, "usdttrc20", first.Network)
	assert.Equal(t, time.Date(2026, 10, 18, 0, 30, 0, 0, time.UTC), first.GatewayEventTS)

	soft := page.Payments[2]
	assert.Empty(t, soft.State, "an unknown provider word is not fabricated")
	assert.Equal(t, "strange", soft.RawStatus)
	assert.Equal(t, domain.EncodeExportCursor(9), page.NextCursor, "a full page carries the next cursor")
}

func TestExportPaymentsSoftRows(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[
			{"payment_id":"gw-1","order_id":"o-1","payment_status":"finished","actually_paid":9,"pay_currency":"xrp"},
			{"payment_id":"gw-2","order_id":"o-2","payment_status":"strange","actually_paid":1,"pay_currency":"usdttrc20"}
		]}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:  time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit: 2,
	})
	require.NoError(t, err, "row level evidence problems must not fail the page")
	require.Len(t, page.Payments, 2)
	assert.Equal(t, domain.StateConfirmed, page.Payments[0].State)
	assert.True(t, page.Payments[0].ReceivedAmount.IsZero(), "an unparsable amount stays zero")
	assert.Empty(t, page.Payments[1].State)
	assert.Equal(t, "strange", page.Payments[1].RawStatus)
}

func TestExportPaymentsShapesAndCursorErrors(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			_, _ = w.Write([]byte(`{"nope":[]}`))
			return
		}
		_, _ = w.Write([]byte(`[
			{"payment_id":"gw-1","order_id":"o-1","payment_status":"finished","actually_paid":1,"pay_currency":"usdttrc20"}
		]`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	from := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{From: from, To: to, Limit: 3})
	require.NoError(t, err, "a bare array answer is tolerated")
	require.Len(t, page.Payments, 1)
	assert.Empty(t, page.NextCursor, "a short page terminates the walk")

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{From: from, To: to, Limit: 3, Cursor: "%%%"})
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest, "a malformed cursor never reaches the provider")

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From: from, To: to, Limit: 3, Cursor: domain.EncodeExportCursor(3),
	})
	require.Error(t, err, "a list envelope without the data key is a decode error")
	require.NotErrorIs(t, err, domain.ErrInvalidGatewayRequest)
}

func TestExportPaymentsCursorAlignment(t *testing.T) {
	t.Parallel()
	called := false
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		called = true
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:   time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit:  2,
		Cursor: domain.EncodeExportCursor(3),
	})
	require.ErrorIs(t, err, domain.ErrInvalidGatewayRequest)
	assert.False(t, called, "an unaligned cursor is rejected before the provider call")
}

func TestExportPaymentsEmptyPageAndErrors(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("page") {
		case "2":
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:  time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit: 2,
	})
	require.NoError(t, err)
	assert.Empty(t, page.Payments)
	assert.Empty(t, page.NextCursor, "an empty page terminates the walk")

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:   time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit:  2,
		Cursor: domain.EncodeExportCursor(2),
	})
	require.ErrorIs(t, err, domain.ErrTemporary)
}
