package btcpay_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/btcpay"
)

func TestGetRefundReadsTheStorePayout(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/stores/"+testStoreID+"/payouts/rf-1", r.URL.Path)
		_, _ = w.Write([]byte(`{"id":"rf-1","state":"Completed"}`))
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)
	status, err := adapter.GetRefund(context.Background(), "rf-1")
	require.NoError(t, err)
	assert.Equal(t, domain.PayoutConfirmed, status.State, "a refund is read through the store payout endpoint")
}
