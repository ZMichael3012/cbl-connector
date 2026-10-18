package coingate_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/coingate"
)

func TestGetRefund(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/refunds/rf-1", r.URL.Path)
		_, _ = w.Write([]byte(`{"id":"rf-1","status":"refunded"}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	status, err := adapter.GetRefund(context.Background(), "rf-1")
	require.NoError(t, err)
	assert.Equal(t, domain.PayoutConfirmed, status.State)
}

func TestGetRefundUnknownStateFailsClosed(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"rf-1","status":"teleported"}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.GetRefund(context.Background(), "rf-1")
	require.ErrorIs(t, err, domain.ErrUnknownState)
}
