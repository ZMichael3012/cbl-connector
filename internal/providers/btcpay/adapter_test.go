package btcpay_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/btcpay"
)

const (
	testStoreID = "store-1"
	testToken   = "api-token"
)

// testCapabilities describes the dev network rows.
func testCapabilities() domain.Capabilities {
	return domain.Capabilities{
		Networks: map[string]domain.NetworkCapability{
			"btc":        {Asset: "BTC", MinAmount: money.MustParse("0.0001", "BTC", 8)},
			"usdt_trc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
		},
	}
}

// testConfig builds a valid adapter configuration for a wire base URL.
func testConfig(baseURL string) btcpay.Config {
	return btcpay.Config{
		BaseURL:       baseURL,
		StoreID:       testStoreID,
		APIToken:      testToken,
		WebhookSecret: []byte("wh-secret"),
		Capabilities:  testCapabilities(),
		Timeout:       2 * time.Second,
	}
}

// invoiceBody renders a BTCPay invoice response.
func invoiceBody(id, status string, created, expiration int64) string {
	return `{"id":"` + id + `","storeId":"` + testStoreID + `","amount":"0.00100000","currency":"BTC","status":"` + status + `",` +
		`"checkoutLink":"https://btcpay.local/i/` + id + `","createdTime":` + strconv.FormatInt(created, 10) + `,"expirationTime":` + strconv.FormatInt(expiration, 10) + `,` +
		`"metadata":{"orderId":"order-1","orderNetwork":"btc"}}`
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

// signWebhook computes the BTCPay-Sig header value of a body.
func signWebhook(secret []byte, body string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	base := testConfig("http://example.test")

	unsupported := base
	unsupported.BaseURL = "ftp://example.test"
	_, err := btcpay.New(unsupported)
	require.Error(t, err)

	broken := base
	broken.BaseURL = ""
	_, err = btcpay.New(broken)
	require.Error(t, err)

	broken = base
	broken.BaseURL = "://bad"
	_, err = btcpay.New(broken)
	require.Error(t, err)

	broken = base
	broken.StoreID = ""
	_, err = btcpay.New(broken)
	require.Error(t, err)

	broken = base
	broken.APIToken = ""
	_, err = btcpay.New(broken)
	require.Error(t, err)

	broken = base
	broken.WebhookSecret = nil
	_, err = btcpay.New(broken)
	require.Error(t, err)

	broken = base
	broken.Capabilities = domain.Capabilities{}
	_, err = btcpay.New(broken)
	require.Error(t, err)

	clamped := base
	clamped.Timeout = 0
	adapter, err := btcpay.New(clamped)
	require.NoError(t, err)
	assert.Equal(t, "btcpay", adapter.Capabilities().Provider, "the provider id is forced")
	assert.Equal(t, "btcpay", adapter.Provider())

	userinfo := base
	userinfo.BaseURL = "http://user:pass@example.test"
	_, err = btcpay.New(userinfo)
	require.Error(t, err, "a base url with userinfo must be rejected")

	conflicting := base
	conflicting.Capabilities = domain.Capabilities{
		Networks: map[string]domain.NetworkCapability{
			"btc":        {Asset: "BTC", MinAmount: money.MustParse("0.0001", "BTC", 8)},
			"btc_second": {Asset: "BTC", MinAmount: money.MustParse("0.001", "BTC", 6)},
		},
	}
	_, err = btcpay.New(conflicting)
	require.Error(t, err, "conflicting decimals for one asset must be rejected")

	_, err = adapter.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{
		IdempotencyKey: "k",
		PaymentID:      "order-1",
		Amount:         money.MustParse("1", "XYZ", 2),
		Network:        "xyz",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	})
	require.Error(t, err, "an asset outside the configured capabilities is a business rejection")
	assert.True(t, errors.Is(err, domain.ErrRejected), "got %v", err)

	_, err = adapter.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{
		IdempotencyKey: "k",
		PaymentID:      "order-1",
		Amount:         money.Money{MinorUnits: 1, Asset: "BTC", Decimals: 0},
		Network:        "btc",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	})
	require.Error(t, err, "a scale mismatch must not reach the provider")
	assert.True(t, errors.Is(err, domain.ErrRejected), "got %v", err)
}

func TestCreateInvoiceHappyPath(t *testing.T) {
	t.Parallel()
	var captured struct {
		mu     sync.Mutex
		path   string
		auth   string
		method string
		body   string
	}
	providerExpiry := time.Now().UTC().Add(time.Hour)
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		captured.mu.Lock()
		captured.path = r.URL.Path
		captured.auth = r.Header.Get("Authorization")
		captured.method = r.Method
		captured.body = readBody(t, r)
		captured.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(invoiceBody("abcdef0123456789abcdef0123456789", "New", time.Now().Unix(), providerExpiry.Unix())))
	})

	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)
	request := domain.CreateInvoiceRequest{
		IdempotencyKey: "k-1",
		PaymentID:      "order-1",
		Amount:         money.MustParse("0.001", "BTC", 8),
		Network:        "btc",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	}
	invoice, err := adapter.CreateInvoice(context.Background(), request)
	require.NoError(t, err)

	assert.Equal(t, "/api/v1/stores/"+testStoreID+"/invoices", captured.path)
	assert.Equal(t, http.MethodPost, captured.method)
	assert.Equal(t, "token "+testToken, captured.auth)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(captured.body), &sent))
	assert.Equal(t, "0.00100000", sent["amount"])
	assert.Equal(t, "BTC", sent["currency"])
	metadata, ok := sent["metadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "order-1", metadata["orderId"])
	assert.Equal(t, "btc", metadata["orderNetwork"])

	assert.Equal(t, "abcdef0123456789abcdef0123456789", invoice.GatewayID)
	assert.Equal(t, "btcpay", invoice.Provider)
	assert.Equal(t, "https://btcpay.local/i/abcdef0123456789abcdef0123456789", invoice.CheckoutURL)
	assert.Equal(t, "btc", invoice.Network)
	assert.Equal(t, int64(100000), invoice.Amount.MinorUnits)
	assert.Equal(t, request.ExpiresAt, invoice.ExpiresAt,
		"a provider expiry beyond the request horizon is clamped to the request")
}

func TestCreateInvoiceProviderExpiryEarlier(t *testing.T) {
	t.Parallel()
	earlier := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(invoiceBody(strings.Repeat("a", 32), "New", time.Now().Unix(), earlier.Unix())))
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)
	invoice, err := adapter.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{
		IdempotencyKey: "k-2",
		PaymentID:      "order-1",
		Amount:         money.MustParse("0.001", "BTC", 8),
		Network:        "btc",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	})
	require.NoError(t, err)
	assert.Equal(t, earlier.Unix(), invoice.ExpiresAt.Unix(), "an earlier provider expiry is kept")
}

func TestAdapterErrorBranches(t *testing.T) {
	t.Parallel()
	adapter, err := btcpay.New(testConfig("http://example.test"))
	require.NoError(t, err)

	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-1",
		Amount:   money.MustParse("0.001", "BTC", 8),
		Network:  "btc",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a payout without a destination must be rejected")

	_, err = adapter.CreateRefund(context.Background(), domain.RefundRequest{
		RefundInvoiceID: "refund-1",
		Amount:          money.MustParse("0.001", "BTC", 8),
		Address:         "bcrt1qexample",
	})
	require.Error(t, err, "a refund without a platform reference must be rejected")

	_, err = adapter.CreateRefund(context.Background(), domain.RefundRequest{
		RefundInvoiceID: "refund-1",
		Amount:          money.MustParse("0.001", "BTC", 8),
	})
	require.Error(t, err)

	invalidJSON := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{`))
	})
	broken, err := btcpay.New(testConfig(invalidJSON.URL))
	require.NoError(t, err)
	_, err = broken.GetPayment(context.Background(), "gw-1")
	require.Error(t, err)
	_, err = broken.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)

	badPaid := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"gw-1","status":"Settled","amountPaid":"0.123456789","currency":"BTC","createdTime":1}`))
	})
	broken, err = btcpay.New(testConfig(badPaid.URL))
	require.NoError(t, err)
	_, err = broken.GetPayment(context.Background(), "gw-1")
	require.Error(t, err, "an over-precise amount paid is a provider data fault")

	missingPayout := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	broken, err = btcpay.New(testConfig(missingPayout.URL))
	require.NoError(t, err)
	_, err = broken.GetPayout(context.Background(), "pp-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestVerifyWebhookTrailingData(t *testing.T) {
	t.Parallel()
	adapter, err := btcpay.New(testConfig("http://example.test"))
	require.NoError(t, err)
	secret := []byte("wh-secret")
	body := `{"deliveryId":"d","webhookId":"w","type":"InvoiceSettled","invoiceId":"inv-1","storeId":"store-1"} trailing`
	_, err = adapter.VerifyWebhook(map[string][]string{"BTCPay-Sig": {signWebhook(secret, body)}}, []byte(body))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidWebhook), "trailing data must be rejected: %v", err)

	noInvoice := `{"deliveryId":"d","webhookId":"w","type":"InvoiceSettled","storeId":"store-1"}`
	_, err = adapter.VerifyWebhook(map[string][]string{"BTCPay-Sig": {signWebhook(secret, noInvoice)}}, []byte(noInvoice))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidWebhook), "a webhook without an invoice id must be rejected: %v", err)
}

func TestCreateInvoiceErrorClassification(t *testing.T) {
	t.Parallel()
	request := domain.CreateInvoiceRequest{
		IdempotencyKey: "k-3",
		PaymentID:      "order-1",
		Amount:         money.MustParse("0.001", "BTC", 8),
		Network:        "btc",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	}

	unavailable := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	adapter, err := btcpay.New(testConfig(unavailable.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "5xx must classify as temporary: %v", err)

	rejected := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	adapter, err = btcpay.New(testConfig(rejected.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "4xx must classify as rejected: %v", err)

	closed := wireServer(t, func(http.ResponseWriter, *http.Request) {})
	url := closed.URL
	closed.Close()
	adapter, err = btcpay.New(testConfig(url))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "a transport failure must classify as temporary: %v", err)

	redirect := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://elsewhere.test/steal")
		w.WriteHeader(http.StatusFound)
	})
	adapter, err = btcpay.New(testConfig(redirect.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "an unfollowed redirect must not be a success: %v", err)
}

func TestCreateInvoiceBadAmount(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"abcdef0123456789abcdef0123456789","storeId":"store-1","amount":"0.001000001","currency":"BTC","status":"New","checkoutLink":"https://btcpay.local/i/x","createdTime":1792248000,"expirationTime":1792249800,"metadata":{}}`))
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{
		IdempotencyKey: "k-4",
		PaymentID:      "order-1",
		Amount:         money.MustParse("0.001", "BTC", 8),
		Network:        "btc",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	})
	require.Error(t, err, "an amount with excessive fraction digits is a provider data fault")
}

func TestGetPaymentStatusMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]domain.State{
		"New":        domain.StateDetected,
		"Processing": domain.StateDetected,
		"Settled":    domain.StateConfirmed,
		"Expired":    domain.StateExpired,
		"Invalid":    domain.StateFailed,
	}
	for raw, expected := range cases {
		raw, expected := raw, expected
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(invoiceBody("abcdef0123456789abcdef0123456789", raw, 1792248000, 1792249800)))
			})
			adapter, err := btcpay.New(testConfig(server.URL))
			require.NoError(t, err)
			status, err := adapter.GetPayment(context.Background(), "abcdef0123456789abcdef0123456789")
			require.NoError(t, err)
			assert.Equal(t, expected, status.State)
			assert.Equal(t, raw, status.RawStatus)
			assert.Equal(t, "abcdef0123456789abcdef0123456789", status.GatewayID)
			assert.Equal(t, "btcpay", status.Provider)
			assert.Equal(t, int64(1792248000), status.GatewayEventTS.Unix())
		})
	}

	unknown := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(invoiceBody("abcdef0123456789abcdef0123456789", "Weird", 1792248000, 1792249800)))
	})
	adapter, err := btcpay.New(testConfig(unknown.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "abcdef0123456789abcdef0123456789")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState), "an unknown status must not be mapped: %v", err)
}

func TestGetPaymentNotFound(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "missing")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound), "a 404 must classify as gateway not found: %v", err)
}

func TestVerifyWebhook(t *testing.T) {
	t.Parallel()
	adapter, err := btcpay.New(testConfig("http://example.test"))
	require.NoError(t, err)
	secret := []byte("wh-secret")
	body := `{"deliveryId":"01a138fd-0000-7000-8000-0000000000d1","webhookId":"01a138fd-0000-7000-8000-0000000000d2","type":"InvoiceSettled","invoiceId":"abcdef0123456789abcdef0123456789","storeId":"store-1","metadata":{"orderId":"order-1"}}`

	event, err := adapter.VerifyWebhook(map[string][]string{"BTCPay-Sig": {signWebhook(secret, body)}}, []byte(body))
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, event.Type)
	assert.Equal(t, "Settled", event.RawStatus)
	assert.Equal(t, "abcdef0123456789abcdef0123456789", event.GatewayID)
	assert.Equal(t, "order-1", event.PaymentID)
	assert.Equal(t, "btcpay", event.Provider)
	assert.False(t, event.OccurredAt.IsZero())

	_, err = adapter.VerifyWebhook(map[string][]string{"BTCPay-Sig": {signWebhook(secret, body)}}, []byte(body+" "))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a tampered body must be rejected: %v", err)

	_, err = adapter.VerifyWebhook(nil, []byte(body))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a missing header must be rejected: %v", err)

	unknownType := `{"deliveryId":"d","webhookId":"w","type":"InvoiceWeird","invoiceId":"i","storeId":"store-1"}`
	_, err = adapter.VerifyWebhook(map[string][]string{"BTCPay-Sig": {signWebhook(secret, unknownType)}}, []byte(unknownType))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState), "an unknown type must not be mapped: %v", err)

	foreignStore := `{"deliveryId":"d","webhookId":"w","type":"InvoiceSettled","invoiceId":"inv-1","storeId":"other-store"}`
	_, err = adapter.VerifyWebhook(map[string][]string{"BTCPay-Sig": {signWebhook(secret, foreignStore)}}, []byte(foreignStore))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidWebhook), "a delivery of another store must not be attributed here: %v", err)
}

func TestLookupByOrderID(t *testing.T) {
	t.Parallel()

	objectWire := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/stores/"+testStoreID+"/invoices", r.URL.Path)
		assert.Equal(t, "order-1", r.URL.Query().Get("orderId"))
		_, _ = w.Write([]byte(invoiceBody("abcdef0123456789abcdef0123456789", "Settled", 1792248000, 1792249800)))
	})
	adapter, err := btcpay.New(testConfig(objectWire.URL))
	require.NoError(t, err)
	status, err := adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, status.State)

	arrayWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[" + invoiceBody("abcdef0123456789abcdef0123456789", "New", 1792248000, 1792249800) + "]"))
	})
	adapter, err = btcpay.New(testConfig(arrayWire.URL))
	require.NoError(t, err)
	status, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateDetected, status.State)

	emptyWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[]"))
	})
	adapter, err = btcpay.New(testConfig(emptyWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))

	missWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err = btcpay.New(testConfig(missWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestCreatePayout(t *testing.T) {
	t.Parallel()
	var pullBody, payoutBody string
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/stores/" + testStoreID + "/pull-payments":
			pullBody = readBody(t, r)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"pp-1","state":"Active"}`))
		case "/api/v1/pull-payments/pp-1/payouts":
			payoutBody = readBody(t, r)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"po-1","state":"AwaitingApproval","pullPaymentId":"pp-1"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)
	payout, err := adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		IdempotencyKey: "k-1",
		PayoutID:       "payout-1",
		Amount:         money.MustParse("0.001", "BTC", 8),
		Network:        "btc",
		Address:        "bcrt1qexample",
	})
	require.NoError(t, err)
	assert.Equal(t, "po-1", payout.GatewayPayoutID)
	assert.Equal(t, domain.PayoutSubmitted, payout.State)
	assert.Equal(t, "btcpay", payout.Provider)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(pullBody), &sent))
	assert.Equal(t, "payout-1", sent["name"])
	assert.Equal(t, "0.00100000", sent["amount"])
	assert.Equal(t, "BTC", sent["currency"])
	methods, ok := sent["payoutMethods"].([]any)
	require.True(t, ok)
	require.Len(t, methods, 1)
	assert.Equal(t, "BTC", methods[0], "the pull payment lists method ids as strings (verified against the regtest stand)")

	require.NoError(t, json.Unmarshal([]byte(payoutBody), &sent))
	assert.Equal(t, "bcrt1qexample", sent["destination"])
	assert.Equal(t, "0.00100000", sent["amount"])
	assert.Equal(t, "BTC-CHAIN", sent["payoutMethodId"], "the payout carries the destination and the chain method id")
}

func TestGetPayoutStatusMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]domain.PayoutState{
		"AwaitingApproval": domain.PayoutSubmitted,
		"AwaitingPayment":  domain.PayoutSubmitted,
		"InProgress":       domain.PayoutSubmitted,
		"Completed":        domain.PayoutConfirmed,
		"Cancelled":        domain.PayoutFailed,
	}
	for raw, expected := range cases {
		raw, expected := raw, expected
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/api/v1/stores/"+testStoreID+"/payouts/po-1", r.URL.Path)
				_, _ = w.Write([]byte(`{"id":"po-1","state":"` + raw + `"}`))
			})
			adapter, err := btcpay.New(testConfig(server.URL))
			require.NoError(t, err)
			status, err := adapter.GetPayout(context.Background(), "po-1")
			require.NoError(t, err)
			assert.Equal(t, expected, status.State)
		})
	}

	unknown := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"po-1","state":"Weird"}`))
	})
	adapter, err := btcpay.New(testConfig(unknown.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayout(context.Background(), "po-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState))
}

func TestCreateRefund(t *testing.T) {
	t.Parallel()
	var pullBody, payoutBody string
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.URL.Path {
		case "/api/v1/stores/" + testStoreID + "/pull-payments":
			pullBody = readBody(t, r)
			_, _ = w.Write([]byte(`{"id":"pp-9","state":"Active"}`))
		case "/api/v1/pull-payments/pp-9/payouts":
			payoutBody = readBody(t, r)
			_, _ = w.Write([]byte(`{"id":"po-9","state":"AwaitingApproval","pullPaymentId":"pp-9"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	adapter, err := btcpay.New(testConfig(server.URL))
	require.NoError(t, err)
	refund, err := adapter.CreateRefund(context.Background(), domain.RefundRequest{
		IdempotencyKey:  "k-1",
		PayoutID:        "po-77",
		RefundInvoiceID: "refund-1",
		PaymentID:       "order-1",
		GatewayID:       "abcdef0123456789abcdef0123456789",
		Amount:          money.MustParse("0.0005", "BTC", 8),
		Address:         "bcrt1qrefund",
	})
	require.NoError(t, err)
	assert.Equal(t, "po-9", refund.GatewayRefundID)
	assert.Equal(t, "btcpay", refund.Provider)
	assert.Equal(t, int64(50000), refund.Amount.MinorUnits)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(pullBody), &sent))
	assert.Equal(t, "refund-po-77", sent["name"], "the per-command pull-payment name carries the payout id (17.12)")
	require.NoError(t, json.Unmarshal([]byte(payoutBody), &sent))
	assert.Equal(t, "bcrt1qrefund", sent["destination"])
	assert.Equal(t, "BTC-CHAIN", sent["payoutMethodId"])

	before := calls
	_, err = adapter.CreateRefund(context.Background(), domain.RefundRequest{
		IdempotencyKey:  "k-2",
		RefundInvoiceID: "refund-2",
		PaymentID:       "order-1",
		GatewayID:       "abcdef0123456789abcdef0123456789",
		Amount:          money.MustParse("0.0005", "BTC", 8),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a refund without a destination address must be rejected: %v", err)
	assert.Equal(t, before, calls, "the rejection happens before any provider call")
}
