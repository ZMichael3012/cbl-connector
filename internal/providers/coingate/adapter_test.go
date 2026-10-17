package coingate_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/coingate"
)

//nolint:gosec // G101: a stand-only test credential of the wire mock.
const testToken = "cg-api-token"

// testConfig builds a valid adapter configuration for a wire base URL.
func testConfig(baseURL string) coingate.Config {
	return coingate.Config{
		BaseURL:  baseURL,
		APIToken: testToken,
		IPNToken: []byte("ipn-token"),
		Capabilities: domain.Capabilities{
			Networks: map[string]domain.NetworkCapability{
				"usdt_trc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
			},
		},
		Timeout: 2 * time.Second,
	}
}

// orderBody renders a CoinGate order response.
func orderBody(id int, status string) string {
	return `{"id":` + strconv.Itoa(id) + `,"status":"` + status + `","order_id":"order-1",` +
		`"price_amount":"12.340000","price_currency":"USDT","pay_amount":"12.340000","pay_currency":"USDT_TRC20",` +
		`"payment_address":"MOCKADDR","created_at":"2026-10-17T12:00:00Z"}`
}

// createRequest builds the fixture invoice request.
func createRequest() domain.CreateInvoiceRequest {
	return domain.CreateInvoiceRequest{
		IdempotencyKey: "k-1",
		PaymentID:      "order-1",
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        "USDT_TRC20",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	}
}

// wireServer builds an httptest server with a single handler.
func wireServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

// readBody reads a request body.
func readBody(t *testing.T, r *http.Request) string {
	t.Helper()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	require.NoError(t, err)
	return string(body)
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	base := testConfig("http://example.test")

	cases := map[string]func(*coingate.Config){
		"missing base url":  func(c *coingate.Config) { c.BaseURL = "" },
		"bad scheme":        func(c *coingate.Config) { c.BaseURL = "ftp://example.test" },
		"userinfo":          func(c *coingate.Config) { c.BaseURL = "http://user:pass@example.test" },
		"missing token":     func(c *coingate.Config) { c.APIToken = "" },
		"missing ipn token": func(c *coingate.Config) { c.IPNToken = nil },
		"no networks":       func(c *coingate.Config) { c.Capabilities = domain.Capabilities{} },
	}
	for name, mutate := range cases {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := base
			mutate(&config)
			_, err := coingate.New(config)
			require.Error(t, err)
		})
	}

	clamped := base
	clamped.Timeout = 0
	adapter, err := coingate.New(clamped)
	require.NoError(t, err)
	assert.Equal(t, "coingate", adapter.Capabilities().Provider)
	assert.Equal(t, "coingate", adapter.Provider())

	_, err = adapter.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{
		IdempotencyKey: "k",
		PaymentID:      "order-1",
		Amount:         money.MustParse("1", "XYZ", 2),
		Network:        "xyz",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	})
	require.Error(t, err, "an asset outside the configured capabilities is a business rejection")
	assert.True(t, errors.Is(err, domain.ErrRejected), "got %v", err)
}

func TestCreateInvoiceHappyPath(t *testing.T) {
	t.Parallel()
	var captured struct {
		path   string
		auth   string
		method string
		body   string
	}
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		captured.path = r.URL.Path
		captured.auth = r.Header.Get("Authorization")
		captured.method = r.Method
		captured.body = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(orderBody(1042, "new")))
	})

	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	request := createRequest()
	invoice, err := adapter.CreateInvoice(context.Background(), request)
	require.NoError(t, err)

	assert.Equal(t, "/v2/orders", captured.path)
	assert.Equal(t, http.MethodPost, captured.method)
	assert.Equal(t, "Bearer "+testToken, captured.auth)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(captured.body), &sent))
	assert.Equal(t, "order-1", sent["order_id"])
	assert.Equal(t, "12.340000", sent["price_amount"])
	assert.Equal(t, "USDT", sent["price_currency"])
	assert.Equal(t, "USDT_TRC20", sent["pay_currency"], "the pay currency is the uppercased request network")

	assert.Equal(t, "1042", invoice.GatewayID, "the numeric order id becomes a decimal string")
	assert.Equal(t, "coingate", invoice.Provider)
	assert.Equal(t, "MOCKADDR", invoice.Address)
	assert.Equal(t, "USDT_TRC20", invoice.Network)
	assert.Equal(t, int64(12340000), invoice.Amount.MinorUnits)
	assert.Equal(t, request.ExpiresAt, invoice.ExpiresAt, "CoinGate orders carry no expiry: the request horizon is echoed")
}

func TestCreateInvoiceErrorClassification(t *testing.T) {
	t.Parallel()
	request := createRequest()

	cases := []struct {
		name   string
		status int
		want   error
	}{
		{"5xx is temporary", http.StatusServiceUnavailable, domain.ErrTemporary},
		{"429 is temporary", http.StatusTooManyRequests, domain.ErrTemporary},
		{"302 is temporary", http.StatusFound, domain.ErrTemporary},
		{"400 is rejected", http.StatusBadRequest, domain.ErrRejected},
		{"401 is rejected", http.StatusUnauthorized, domain.ErrRejected},
		{"403 is rejected", http.StatusForbidden, domain.ErrRejected},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			})
			adapter, err := coingate.New(testConfig(server.URL))
			require.NoError(t, err)
			_, err = adapter.CreateInvoice(context.Background(), request)
			require.Error(t, err)
			assert.True(t, errors.Is(err, tc.want), "got %v, want %v", err, tc.want)
		})
	}

	closed := wireServer(t, func(http.ResponseWriter, *http.Request) {})
	url := closed.URL
	closed.Close()
	adapter, err := coingate.New(testConfig(url))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "a transport failure is temporary: %v", err)
}

func TestGetPaymentStatusMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]domain.State{
		"new":        domain.StateDetected,
		"pending":    domain.StateDetected,
		"confirming": domain.StateDetected,
		"paid":       domain.StateConfirmed,
		"invalid":    domain.StateFailed,
		"expired":    domain.StateExpired,
		"canceled":   domain.StateFailed,
		"refunded":   domain.StateFailed,
	}
	for raw, expected := range cases {
		raw, expected := raw, expected
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/v2/orders/1042", r.URL.Path)
				_, _ = w.Write([]byte(orderBody(1042, raw)))
			})
			adapter, err := coingate.New(testConfig(server.URL))
			require.NoError(t, err)
			status, err := adapter.GetPayment(context.Background(), "1042")
			require.NoError(t, err)
			assert.Equal(t, expected, status.State)
			assert.Equal(t, raw, status.RawStatus)
			assert.Equal(t, "1042", status.GatewayID)
			assert.Equal(t, "coingate", status.Provider)
			assert.Equal(t, int64(1792238400), status.GatewayEventTS.Unix())
			assert.Equal(t, int64(12340000), status.ReceivedAmount.MinorUnits, "the fetch path requires amount evidence (E-9)")
			assert.Equal(t, "USDT", status.ReceivedAmount.Asset)
			assert.Equal(t, "usdt_trc20", status.Network)
		})
	}

	unknown := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(orderBody(1042, "weird")))
	})
	adapter, err := coingate.New(testConfig(unknown.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "1042")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState), "an unknown status must not be mapped: %v", err)
}

func TestGetPaymentNotFound(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "404")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestFetchedStatusFeedsCanonicalEvent(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(orderBody(1042, "paid")))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	status, err := adapter.GetPayment(context.Background(), "1042")
	require.NoError(t, err)

	event := domain.CanonicalEvent{
		Type:           status.State,
		PaymentID:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		GatewayID:      status.GatewayID,
		Provider:       status.Provider,
		RawStatus:      status.RawStatus,
		Network:        status.Network,
		ReceivedAmount: status.ReceivedAmount,
		OccurredAt:     time.Now().UTC(),
	}
	require.NoError(t, event.Validate(),
		"the fetch-to-verify path builds canonical events from this status and must validate")
}

func TestMalformedCreatedAtFallsBackToZeroTime(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":1042,"status":"new","pay_amount":"1.000000","pay_currency":"USDT_TRC20","created_at":"not-a-time"}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	status, err := adapter.GetPayment(context.Background(), "1042")
	require.NoError(t, err)
	assert.True(t, status.GatewayEventTS.IsZero(), "the normalizer falls back to the receipt time (E-8)")
}

func TestAmountScaleGuard(t *testing.T) {
	t.Parallel()
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(orderBody(1042, "new")))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)

	request := createRequest()
	request.Amount.Decimals = 0
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a scale mismatch must not reach the provider: %v", err)

	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-1",
		Amount:   money.Money{MinorUnits: 1234, Asset: "USDT", Decimals: 0},
		Network:  "usdt_trc20",
		Address:  "TR7example",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))

	assert.Zero(t, calls, "the guard rejects before any provider call")
}

func TestVerifyWebhookTokenEcho(t *testing.T) {
	t.Parallel()
	adapter, err := coingate.New(testConfig("http://example.test"))
	require.NoError(t, err)
	body := `{"id":1042,"order_id":"order-1","status":"paid","price_amount":"12.340000","price_currency":"USDT","pay_amount":"12.340000","pay_currency":"USDT_TRC20","token":"ipn-token","created_at":"2026-10-17T12:00:00Z"}`

	event, err := adapter.VerifyWebhook(nil, []byte(body))
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, event.Type)
	assert.Equal(t, "paid", event.RawStatus)
	assert.Equal(t, "1042", event.GatewayID)
	assert.Equal(t, "order-1", event.PaymentID)
	assert.Equal(t, "coingate", event.Provider)
	assert.False(t, event.OccurredAt.IsZero())

	tampered := strings.Replace(body, `"token":"ipn-token"`, `"token":"wrong-token"`, 1)
	_, err = adapter.VerifyWebhook(nil, []byte(tampered))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a wrong token must be rejected: %v", err)

	missing := strings.Replace(body, `,"token":"ipn-token"`, "", 1)
	_, err = adapter.VerifyWebhook(nil, []byte(missing))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a missing token must be rejected: %v", err)

	unknown := strings.Replace(body, `"status":"paid"`, `"status":"weird"`, 1)
	_, err = adapter.VerifyWebhook(nil, []byte(unknown))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState), "an unknown status must not be mapped: %v", err)

	_, err = adapter.VerifyWebhook(nil, []byte(body+" trailing"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "trailing data is malformed: %v", err)
}

func TestLookupByOrderID(t *testing.T) {
	t.Parallel()
	objectWire := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/orders", r.URL.Path)
		assert.Equal(t, "order-1", r.URL.Query().Get("order_id"))
		_, _ = w.Write([]byte(orderBody(1042, "paid")))
	})
	adapter, err := coingate.New(testConfig(objectWire.URL))
	require.NoError(t, err)
	status, err := adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, status.State)

	arrayWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[" + orderBody(1042, "new") + "]"))
	})
	adapter, err = coingate.New(testConfig(arrayWire.URL))
	require.NoError(t, err)
	status, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateDetected, status.State)

	emptyWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[]"))
	})
	adapter, err = coingate.New(testConfig(emptyWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))

	missWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err = coingate.New(testConfig(missWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestCreatePayoutProvisionalShape(t *testing.T) {
	t.Parallel()
	var captured string
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		assert.Equal(t, "/v2/send_request", r.URL.Path)
		captured = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":777,"state":"new"}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	payout, err := adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		IdempotencyKey: "k-1",
		PayoutID:       "payout-1",
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        "usdt_trc20",
		Address:        "TR7example",
	})
	require.NoError(t, err)
	assert.Equal(t, "777", payout.GatewayPayoutID)
	assert.Equal(t, domain.PayoutSubmitted, payout.State)
	assert.Equal(t, "coingate", payout.Provider)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(captured), &sent))
	assert.Equal(t, "payout-1", sent["request_id"])
	assert.Equal(t, "12.340000", sent["amount"])
	assert.Equal(t, "USDT", sent["currency"])
	assert.Equal(t, "TR7example", sent["address"])

	before := calls
	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-2",
		Amount:   money.MustParse("12.34", "USDT", 6),
		Network:  "usdt_trc20",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
	assert.Equal(t, before, calls, "the rejection happens before any provider call")
}

func TestGetPayoutProvisionalShape(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/send_requests/777", r.URL.Path)
		_, _ = w.Write([]byte(`{"id":777,"state":"confirmed"}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	status, err := adapter.GetPayout(context.Background(), "777")
	require.NoError(t, err)
	assert.Equal(t, domain.PayoutConfirmed, status.State)
}

func TestCreateRefundProvisionalShape(t *testing.T) {
	t.Parallel()
	var captured string
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v2/refunds", r.URL.Path)
		captured = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"refund-777","status":"new"}`))
	})
	adapter, err := coingate.New(testConfig(server.URL))
	require.NoError(t, err)
	refund, err := adapter.CreateRefund(context.Background(), domain.RefundRequest{
		IdempotencyKey:  "k-1",
		RefundInvoiceID: "refund-1",
		PaymentID:       "order-1",
		GatewayID:       "1042",
		Amount:          money.MustParse("5", "USDT", 6),
	})
	require.NoError(t, err)
	assert.Equal(t, "refund-777", refund.GatewayRefundID)
	assert.Equal(t, "coingate", refund.Provider)
	assert.Equal(t, int64(5000000), refund.Amount.MinorUnits)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(captured), &sent))
	assert.Equal(t, "1042", sent["order_id"])
	assert.Equal(t, "5.000000", sent["amount"])

	_, err = adapter.CreateRefund(context.Background(), domain.RefundRequest{
		PaymentID: "order-1",
		Amount:    money.MustParse("5", "USDT", 6),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a refund without a reference must be rejected")
}
