package btcpay_test

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/btcpay"
)

func TestExportPaymentsPage(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/stores/store-1/invoices", r.URL.Path)
		query := r.URL.Query()
		assert.Equal(t, "0", query.Get("skip"))
		assert.Equal(t, "2", query.Get("take"))
		assert.Equal(t, strconv.FormatInt(from.Unix(), 10), query.Get("startDate"))
		assert.Equal(t, strconv.FormatInt(to.Unix(), 10), query.Get("endDate"))
		_, _ = w.Write([]byte(`[
			{"id":"abcdef0123456789abcdef0123456789","storeId":"store-1","amount":"0.001","currency":"BTC","status":"Settled","createdTime":1792288200,"amountPaid":"0.0005","metadata":{"orderId":"01a14f70-0000-7000-8000-000000000001"}},
			{"id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","storeId":"store-1","amount":"0.001","currency":"BTC","status":"Weird","amountPaid":"0","metadata":{}}
		]`))
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{From: from, To: to, Limit: 2})
	require.NoError(t, err)
	require.Len(t, page.Payments, 2)
	first := page.Payments[0]
	assert.Equal(t, "abcdef0123456789abcdef0123456789", first.GatewayID)
	assert.Equal(t, "01a14f70-0000-7000-8000-000000000001", first.PaymentID, "the payment id comes from the invoice metadata")
	assert.Equal(t, domain.StateConfirmed, first.State)
	assert.Equal(t, "Settled", first.RawStatus)
	assert.Equal(t, int64(50000), first.ReceivedAmount.MinorUnits)
	assert.Equal(t, "BTC", first.ReceivedAmount.Asset)
	assert.Equal(t, uint32(8), first.ReceivedAmount.Decimals)

	soft := page.Payments[1]
	assert.Empty(t, soft.State)
	assert.Equal(t, "Weird", soft.RawStatus)
	assert.Equal(t, "BTC", soft.ReceivedAmount.Asset, "a genuine zero keeps the asset, unlike missing evidence")
	assert.Zero(t, soft.ReceivedAmount.MinorUnits)
	assert.Equal(t, domain.EncodeExportCursor(2), page.NextCursor)
}

func TestExportPaymentsEmptyAndErrors(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("skip") == "2" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)

	from := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC)
	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{From: from, To: to, Limit: 2})
	require.NoError(t, err)
	assert.Empty(t, page.Payments)
	assert.Empty(t, page.NextCursor)

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From: from, To: to, Limit: 2, Cursor: domain.EncodeExportCursor(2),
	})
	require.ErrorIs(t, err, domain.ErrTemporary)
}
