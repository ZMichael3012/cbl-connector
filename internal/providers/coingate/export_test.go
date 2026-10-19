package coingate_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/coingate"
)

func TestExportPaymentsPage(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/orders", r.URL.Path)
		query := r.URL.Query()
		assert.Equal(t, "2", query.Get("page"))
		assert.Equal(t, "2", query.Get("per_page"))
		assert.Equal(t, "2026-10-18", query.Get("created_at[from]"),
			"the real API window uses created_at dates; from/to are ignored by CoinGate")
		assert.Equal(t, "2026-10-18", query.Get("created_at[to]"))
		assert.Equal(t, "created_at_asc", query.Get("sort"), "the ascending walk keeps the offset cursor stable")
		_, _ = w.Write([]byte(`{"orders":[
			{"id":1042,"status":"paid","order_id":"01a14f70-0000-7000-8000-000000000001","pay_amount":"12.340000","pay_currency":"USDT_TRC20","created_at":"2026-10-18T00:30:00Z"},
			{"id":1043,"status":"weird","order_id":"01a14f70-0000-7000-8000-000000000002","pay_currency":"USDT_TRC20"}
		]}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:   time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit:  2,
		Cursor: domain.EncodeExportCursor(2),
	})
	require.NoError(t, err)
	require.Len(t, page.Payments, 2)
	first := page.Payments[0]
	assert.Equal(t, "1042", first.GatewayID)
	assert.Equal(t, "01a14f70-0000-7000-8000-000000000001", first.PaymentID)
	assert.Equal(t, domain.StateConfirmed, first.State)
	assert.Equal(t, "paid", first.RawStatus)
	assert.Equal(t, int64(12340000), first.ReceivedAmount.MinorUnits)
	assert.Equal(t, "USDT", first.ReceivedAmount.Asset)
	assert.Equal(t, "usdt_trc20", first.Network)
	assert.Equal(t, time.Date(2026, 10, 18, 0, 30, 0, 0, time.UTC), first.GatewayEventTS)

	soft := page.Payments[1]
	assert.Empty(t, soft.State)
	assert.Equal(t, "weird", soft.RawStatus)
	assert.Equal(t, domain.EncodeExportCursor(4), page.NextCursor)
}

func TestExportPaymentsShapeViolation(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"count":3}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:  time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit: 2,
	})
	require.Error(t, err, "an envelope without the orders key is a decode error")
}

func TestExportPaymentsArrayAnswerAndErrors(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`[
			{"id":1044,"status":"new","order_id":"01a14f70-0000-7000-8000-000000000003","pay_currency":"USDT_TRC20"}
		]`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)

	page, err := adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:  time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:    time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit: 2,
	})
	require.NoError(t, err, "a bare array answer is tolerated")
	require.Len(t, page.Payments, 1)
	assert.Equal(t, domain.StatePending, page.Payments[0].State, "a new order is a pre-deposit status")
	assert.Empty(t, page.NextCursor, "a short page terminates the walk")

	_, err = adapter.ExportPayments(context.Background(), domain.ExportRequest{
		From:   time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC),
		To:     time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC),
		Limit:  2,
		Cursor: domain.EncodeExportCursor(2),
	})
	require.ErrorIs(t, err, domain.ErrTemporary)
}
