package bitpay_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/bitpay"
)

func TestExportPaymentsPage(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/invoices", r.URL.Path)
		query := r.URL.Query()
		assert.Equal(t, "2026-10-18", query.Get("dateStart"))
		assert.Equal(t, "2026-10-18", query.Get("dateEnd"))
		assert.Equal(t, "2", query.Get("limit"))
		assert.Equal(t, "0", query.Get("offset"))
		_, _ = w.Write([]byte(`{"data":[
			{"id":"bp-1","status":"complete","price":"5.00","currency":"USDT","orderId":"01a14f70-0000-7000-8000-000000000001","amountPaid":"5.00","confirmations":6,"invoiceTime":1792288200000},
			{"id":"bp-2","status":"new","price":"5.00","currency":"USDT","orderId":"01a14f70-0000-7000-8000-000000000002"}
		]}`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:  time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit: 2,
	})
	require.NoError(t, err)
	require.Len(t, page.Payments, 2)
	first := page.Payments[0]
	assert.Equal(t, "bp-1", first.GatewayID)
	assert.Equal(t, "01a14f70-0000-7000-8000-000000000001", first.PaymentID)
	assert.Equal(t, domain.StateConfirmed, first.State)
	assert.Equal(t, "complete", first.RawStatus)
	assert.Equal(t, int64(5000000), first.ReceivedAmount.MinorUnits)
	assert.Equal(t, "USDT", first.ReceivedAmount.Asset)
	assert.Equal(t, uint32(6), first.Confirmations)
	assert.Equal(t, time.UnixMilli(1792288200000).UTC(), first.GatewayEventTS)

	assert.Equal(t, domain.StatePending, page.Payments[1].State, "a new invoice is a pre-deposit status")
	assert.Equal(t, domain.EncodeExportCursor(2), page.NextCursor)
}

func TestExportPaymentsShapeViolation(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:  time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit: 2,
	})
	require.Error(t, err, "an envelope without the data key is a decode error")
}

func TestExportPaymentsSoftRowsAndBareArray(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"id":"bp-3","status":"weird","currency":"USDT","orderId":"o-3","amountPaid":"1.00"}
		]`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:  time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit: 2,
	})
	require.NoError(t, err, "a bare array answer and an unknown word must not fail the page")
	require.Len(t, page.Payments, 1)
	assert.Empty(t, page.Payments[0].State)
	assert.Equal(t, "weird", page.Payments[0].RawStatus)
	assert.Empty(t, page.NextCursor)
}
