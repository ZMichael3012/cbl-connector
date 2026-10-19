package nowpayments_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
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
	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/nowpayments"
)

const testAPIKey = "now-api-key"

// testConfig builds a valid adapter configuration for a wire base URL.
func testConfig(baseURL string) nowpayments.Config {
	return nowpayments.Config{
		BaseURL:   baseURL,
		APIKey:    testAPIKey,
		IPNSecret: []byte("now-ipn-secret"),
		Capabilities: domain.Capabilities{
			Networks: map[string]domain.NetworkCapability{
				"usdttrc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
			},
		},
		Timeout: 2 * time.Second,
	}
}

// invoiceBody renders a NOWPayments invoice response.
func invoiceBody(status string, expiration time.Time) string {
	return `{"id":"np-gw-1","order_id":"order-1","price_amount":12.34,"price_currency":"USD",` +
		`"pay_currency":"usdttrc20","invoice_url":"https://pay.nowpayments.io/invoice/1",` +
		`"created_at":"2026-10-17T12:00:00Z","expiration_estimate_date":"` + expiration.UTC().Format(time.RFC3339) + `","status":"` + status + `"}`
}

// paymentBody renders a NOWPayments payment response.
func paymentBody(status string, actuallyPaid float64) string {
	return `{"payment_id":"np-gw-1","order_id":"order-1","payment_status":"` + status + `",` +
		`"price_amount":12.34,"actually_paid":` + strconv.FormatFloat(actuallyPaid, 'f', 2, 64) + `,` +
		`"pay_currency":"usdttrc20","updated_at":"2026-10-17T12:00:00Z"}`
}

// createRequest builds the fixture invoice request.
// TestCreateInvoiceCallbackURL verifies the per-invoice IPN URL is sent only
// when configured (T-CONNECTOR-30).
func TestCreateInvoiceCallbackURL(t *testing.T) {
	t.Parallel()
	var sent map[string]any
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &sent)
		_, _ = w.Write([]byte(`{"id":"np-gw-1","order_id":"order-1","price_amount":12.34,"price_currency":"USD","pay_currency":"usdttrc20","invoice_url":"https://pay.nowpayments.io/invoice/1"}`))
	})
	config := testConfig(server.URL)
	config.CallbackURL = "https://hooks.example.test/hooks/nowpayments/stand"
	adapter, err := nowpayments.New(config)
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)
	assert.Equal(t, "https://hooks.example.test/hooks/nowpayments/stand", sent["ipn_callback_url"])

	sent = nil
	server2 := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &sent)
		_, _ = w.Write([]byte(`{"id":"np-gw-1","order_id":"order-1","price_amount":12.34,"price_currency":"USD","pay_currency":"usdttrc20","invoice_url":"https://pay.nowpayments.io/invoice/1"}`))
	})
	adapter2, err := nowpayments.New(testConfig(server2.URL))
	require.NoError(t, err)
	_, err = adapter2.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)
	assert.NotContains(t, sent, "ipn_callback_url", "without a configured callback URL the field is omitted")
}

func createRequest() domain.CreateInvoiceRequest {
	return domain.CreateInvoiceRequest{
		IdempotencyKey: "k-1",
		PaymentID:      "order-1",
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        "usdttrc20",
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

// signIPN computes the canonical JSON HMAC-SHA512 signature of a body.
func signIPN(t *testing.T, secret []byte, body string) string {
	t.Helper()
	canonical, err := webhooksig.CanonicalJSON([]byte(body))
	require.NoError(t, err)
	mac := hmac.New(sha512.New, secret)
	mac.Write(canonical)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestWebhookRejectsMalformedDocuments(t *testing.T) {
	t.Parallel()
	adapter, err := nowpayments.New(testConfig("http://example.test"))
	require.NoError(t, err)
	header := map[string][]string{"x-nowpayments-sig": {"00"}}

	valid := `{"payment_id":"np-gw-1","order_id":"order-1","payment_status":"finished","actually_paid":12.34,"pay_currency":"usdttrc20","updated_at":"2026-10-17T12:00:00Z"}`
	_, err = adapter.VerifyWebhook(header, []byte(valid+` {"x":1}`))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "trailing data is malformed for the canonical decoder")

	duplicate := `{"payment_id":"a","payment_id":"b","order_id":"order-1","payment_status":"finished"}`
	_, err = adapter.VerifyWebhook(header, []byte(duplicate))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "duplicate keys are malformed for the canonical decoder")
}

func TestGetPaymentUnknownPayCurrency(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"payment_id":"np-gw-1","order_id":"order-1","payment_status":"finished","actually_paid":1.0,"pay_currency":"ethw","updated_at":"2026-10-17T12:00:00Z"}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "np-gw-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "an unmapped pay currency fails closed: %v", err)
}

func TestCreateInvoiceNetworkValidation(t *testing.T) {
	t.Parallel()
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(invoiceBody("waiting", time.Now().UTC().Add(time.Hour))))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)

	unknown := createRequest()
	unknown.Network = "ethw"
	_, err = adapter.CreateInvoice(context.Background(), unknown)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "an unconfigured network must not reach the provider")

	mismatch := createRequest()
	mismatch.Amount = money.MustParse("12.34", "USDC", 6)
	_, err = adapter.CreateInvoice(context.Background(), mismatch)
	require.Error(t, err, "the network row asset must match the amount asset")
	assert.True(t, errors.Is(err, domain.ErrRejected))
	assert.Zero(t, calls)
}

func TestCreateInvoiceExpiryBranches(t *testing.T) {
	t.Parallel()
	earlier := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Second)
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"np-gw-1","order_id":"order-1","price_amount":12.34,"price_currency":"USD","pay_currency":"usdttrc20","invoice_url":"https://pay.nowpayments.io/invoice/1","expiration_estimate_date":"` + earlier.Format(time.RFC3339) + `"}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	invoice, err := adapter.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)
	assert.Equal(t, earlier.Unix(), invoice.ExpiresAt.Unix(), "an earlier provider estimate is kept")

	absent := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"np-gw-1","order_id":"order-1","price_amount":12.34,"price_currency":"USD","pay_currency":"usdttrc20","invoice_url":"https://pay.nowpayments.io/invoice/1"}`))
	})
	adapter, err = nowpayments.New(testConfig(absent.URL))
	require.NoError(t, err)
	request := createRequest()
	invoice, err = adapter.CreateInvoice(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, request.ExpiresAt, invoice.ExpiresAt, "an absent estimate echoes the request horizon")
}

func TestLookupUnknownStatus(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[` + paymentBody("weird", 1) + `]}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState))
}

func TestCreatePayoutScaleGuard(t *testing.T) {
	t.Parallel()
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"np-payout-1","status":"waiting"}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-1",
		Amount:   money.Money{MinorUnits: 1234, Asset: "USDT", Decimals: 0},
		Network:  "usdttrc20",
		Address:  "TXexample",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
	assert.Zero(t, calls)
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	base := testConfig("http://example.test")

	cases := map[string]func(*nowpayments.Config){
		"missing base url": func(c *nowpayments.Config) { c.BaseURL = "" },
		"bad scheme":       func(c *nowpayments.Config) { c.BaseURL = "ftp://example.test" },
		"userinfo":         func(c *nowpayments.Config) { c.BaseURL = "http://user:pass@example.test" },
		"missing key":      func(c *nowpayments.Config) { c.APIKey = "" },
		"missing ipn":      func(c *nowpayments.Config) { c.IPNSecret = nil },
		"no networks":      func(c *nowpayments.Config) { c.Capabilities = domain.Capabilities{} },
		"stablecoin price currency": func(c *nowpayments.Config) {
			c.PriceCurrency = "USDT"
		},
		"non-letter price currency": func(c *nowpayments.Config) { c.PriceCurrency = "1$A" },
	}
	for name, mutate := range cases {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := base
			mutate(&config)
			_, err := nowpayments.New(config)
			require.Error(t, err)
		})
	}

	clamped := base
	clamped.Timeout = 0
	adapter, err := nowpayments.New(clamped)
	require.NoError(t, err)
	assert.Equal(t, "nowpayments", adapter.Capabilities().Provider)
	assert.Equal(t, "nowpayments", adapter.Provider())

	_, err = adapter.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{
		IdempotencyKey: "k",
		PaymentID:      "order-1",
		Amount:         money.MustParse("1", "XYZ", 2),
		Network:        "xyz",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	})
	require.Error(t, err, "an asset outside the configured capabilities is a business rejection")
	assert.True(t, errors.Is(err, domain.ErrRejected), "got %v", err)

	scale := createRequest()
	scale.Amount.Decimals = 0
	_, err = adapter.CreateInvoice(context.Background(), scale)
	require.Error(t, err, "a scale mismatch must not reach the provider")
	assert.True(t, errors.Is(err, domain.ErrRejected))
}

func TestCreateInvoiceHappyPath(t *testing.T) {
	t.Parallel()
	var captured struct {
		path   string
		auth   string
		method string
		body   string
	}
	providerExpiry := time.Now().UTC().Add(time.Hour)
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		captured.path = r.URL.Path
		captured.auth = r.Header.Get("x-api-key")
		captured.method = r.Method
		captured.body = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(invoiceBody("waiting", providerExpiry)))
	})

	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	request := createRequest()
	invoice, err := adapter.CreateInvoice(context.Background(), request)
	require.NoError(t, err)

	assert.Equal(t, "/v1/invoice", captured.path)
	assert.Equal(t, http.MethodPost, captured.method)
	assert.Equal(t, testAPIKey, captured.auth)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(captured.body), &sent))
	assert.Equal(t, "order-1", sent["order_id"])
	assert.InDelta(t, 12.34, sent["price_amount"], 0.0000001)
	assert.Equal(t, "USD", sent["price_currency"],
		"the sandbox cannot estimate a stablecoin price currency: the order is fiat-priced at par (T-CONNECTOR-28)")
	assert.Equal(t, "usdttrc20", sent["pay_currency"])

	assert.Equal(t, "np-gw-1", invoice.GatewayID)
	assert.Equal(t, "nowpayments", invoice.Provider)
	assert.Equal(t, "https://pay.nowpayments.io/invoice/1", invoice.CheckoutURL)
	assert.Equal(t, "usdttrc20", invoice.Network)
	assert.Equal(t, int64(12340000), invoice.Amount.MinorUnits)
	assert.Equal(t, request.ExpiresAt, invoice.ExpiresAt, "a provider expiry beyond the request horizon is clamped")
}

func TestCreateInvoicePriceGuards(t *testing.T) {
	t.Parallel()

	// A drifted provider price is rejected.
	drift := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"np-gw-1","order_id":"order-1","price_amount":12.35,"price_currency":"USD","pay_currency":"usdttrc20","invoice_url":"https://pay.nowpayments.io/invoice/1"}`))
	})
	adapter, err := nowpayments.New(testConfig(drift.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a price drift must be rejected: %v", err)

	// A non-stablecoin asset cannot be priced in fiat without a rate.
	eth := testConfig("http://example.test")
	eth.Capabilities.Networks = map[string]domain.NetworkCapability{
		"eth": {Asset: "ETH", MinAmount: money.MustParse("0.01", "ETH", 8)},
	}
	ethAdapter, err := nowpayments.New(eth)
	require.NoError(t, err)
	_, err = ethAdapter.CreateInvoice(context.Background(), domain.CreateInvoiceRequest{
		IdempotencyKey: "k-eth",
		PaymentID:      "order-1",
		Amount:         money.MustParse("0.01", "ETH", 8),
		Network:        "eth",
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a rate is not wired yet: %v", err)
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
		{"404 write is rejected", http.StatusNotFound, domain.ErrRejected},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			})
			adapter, err := nowpayments.New(testConfig(server.URL))
			require.NoError(t, err)
			_, err = adapter.CreateInvoice(context.Background(), request)
			require.Error(t, err)
			assert.True(t, errors.Is(err, tc.want), "got %v, want %v", err, tc.want)
		})
	}
}

func TestGetPaymentStatusMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]domain.State{
		"waiting":        domain.StatePending,
		"confirming":     domain.StateDetected,
		"confirmed":      domain.StateDetected,
		"sending":        domain.StateDetected,
		"partially_paid": domain.StateUnderpaid,
		"finished":       domain.StateConfirmed,
		"failed":         domain.StateFailed,
		"refunded":       domain.StateFailed,
		"expired":        domain.StateExpired,
	}
	for raw, expected := range cases {
		raw, expected := raw, expected
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/v1/payment/np-gw-1", r.URL.Path)
				_, _ = w.Write([]byte(paymentBody(raw, 12.34)))
			})
			adapter, err := nowpayments.New(testConfig(server.URL))
			require.NoError(t, err)
			status, err := adapter.GetPayment(context.Background(), "np-gw-1")
			require.NoError(t, err)
			assert.Equal(t, expected, status.State)
			assert.Equal(t, raw, status.RawStatus)
			assert.Equal(t, "np-gw-1", status.GatewayID)
			assert.Equal(t, "nowpayments", status.Provider)
			assert.Equal(t, int64(12340000), status.ReceivedAmount.MinorUnits)
			assert.Equal(t, "USDT", status.ReceivedAmount.Asset)
			assert.Equal(t, "usdttrc20", status.Network)
			assert.Equal(t, int64(1792238400), status.GatewayEventTS.Unix())
		})
	}

	unknown := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(paymentBody("weird", 12.34)))
	})
	adapter, err := nowpayments.New(testConfig(unknown.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "np-gw-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState))
}

func TestGetPaymentNotFound(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "missing")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestVerifyWebhookCanonicalJSON(t *testing.T) {
	t.Parallel()
	adapter, err := nowpayments.New(testConfig("http://example.test"))
	require.NoError(t, err)
	secret := []byte("now-ipn-secret")
	body := `{"payment_id":"np-gw-1","order_id":"order-1","payment_status":"finished","price_amount":12.34,"actually_paid":12.34,"pay_currency":"usdttrc20","updated_at":"2026-10-17T12:00:00Z"}`

	event, err := adapter.VerifyWebhook(map[string][]string{"x-nowpayments-sig": {signIPN(t, secret, body)}}, []byte(body))
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, event.Type)
	assert.Equal(t, "finished", event.RawStatus)
	assert.Equal(t, "np-gw-1", event.GatewayID)
	assert.Equal(t, "order-1", event.PaymentID)
	assert.Equal(t, "nowpayments", event.Provider)
	assert.Equal(t, int64(1792238400), event.OccurredAt.Unix())
	assert.Equal(t, int64(12340000), event.ReceivedAmount.MinorUnits)

	// Canonical JSON is whitespace-insensitive: tamper a value, not the spacing.
	tampered := strings.Replace(body, `"price_amount":12.34`, `"price_amount":13.34`, 1)
	_, err = adapter.VerifyWebhook(map[string][]string{"x-nowpayments-sig": {signIPN(t, secret, body)}}, []byte(tampered))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a tampered value must be rejected")

	_, err = adapter.VerifyWebhook(nil, []byte(body))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a missing header must be rejected")

	unknown := strings.Replace(body, `"payment_status":"finished"`, `"payment_status":"weird"`, 1)
	_, err = adapter.VerifyWebhook(map[string][]string{"x-nowpayments-sig": {signIPN(t, secret, unknown)}}, []byte(unknown))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState), "an unknown status must not be mapped")

	pending := strings.Replace(body, `"payment_status":"finished"`, `"payment_status":"waiting"`, 1)
	_, err = adapter.VerifyWebhook(map[string][]string{"x-nowpayments-sig": {signIPN(t, secret, pending)}}, []byte(pending))
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidWebhook), "a pre-deposit status carries no event")
}

func TestLookupByOrderID(t *testing.T) {
	t.Parallel()
	envelope := `{"data":[` + paymentBody("finished", 12.34) + `],"total":1}`
	envelopeWire := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/payment/", r.URL.Path)
		assert.Equal(t, "order-1", r.URL.Query().Get("order_id"))
		_, _ = w.Write([]byte(envelope))
	})
	adapter, err := nowpayments.New(testConfig(envelopeWire.URL))
	require.NoError(t, err)
	status, err := adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, status.State)

	arrayWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[" + paymentBody("waiting", 0) + "]"))
	})
	adapter, err = nowpayments.New(testConfig(arrayWire.URL))
	require.NoError(t, err)
	status, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StatePending, status.State)

	objectWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(paymentBody("partially_paid", 5)))
	})
	adapter, err = nowpayments.New(testConfig(objectWire.URL))
	require.NoError(t, err)
	status, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateUnderpaid, status.State)

	emptyWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[],"total":0}`))
	})
	adapter, err = nowpayments.New(testConfig(emptyWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))

	missWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err = nowpayments.New(testConfig(missWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestFetchedStatusFeedsCanonicalEvent(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(paymentBody("finished", 12.34)))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	status, err := adapter.GetPayment(context.Background(), "np-gw-1")
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
	require.NoError(t, event.Validate(), "the fetch-to-verify path builds canonical events from this status")
}

func TestMalformedUpdatedAtFallsBackToZeroTime(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"payment_id":"np-gw-1","payment_status":"waiting","actually_paid":1.0,"pay_currency":"usdttrc20","updated_at":"not-a-time"}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	status, err := adapter.GetPayment(context.Background(), "np-gw-1")
	require.NoError(t, err)
	assert.True(t, status.GatewayEventTS.IsZero())
}

func TestCreatePayoutProvisionalShape(t *testing.T) {
	t.Parallel()
	var captured string
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		assert.Equal(t, "/v1/payout", r.URL.Path)
		captured = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"np-payout-1","status":"waiting"}`))
	})
	adapter, err := nowpayments.New(testConfig(server.URL))
	require.NoError(t, err)
	payout, err := adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		IdempotencyKey: "k-1",
		PayoutID:       "payout-1",
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        "usdttrc20",
		Address:        "TXexample",
	})
	require.NoError(t, err)
	assert.Equal(t, "np-payout-1", payout.GatewayPayoutID)
	assert.Equal(t, domain.PayoutSubmitted, payout.State)

	var sent map[string]any
	require.NoError(t, json.Unmarshal([]byte(captured), &sent))
	assert.Equal(t, "12.340000", sent["amount"])
	assert.Equal(t, "USDT", sent["currency"])
	assert.Equal(t, "TXexample", sent["address"])

	before := calls
	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-2",
		Amount:   money.MustParse("12.34", "USDT", 6),
		Network:  "usdttrc20",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
	assert.Equal(t, before, calls, "the rejection happens before any provider call")
}

func TestGetPayoutStatusMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]domain.PayoutState{
		"waiting":    domain.PayoutSubmitted,
		"processing": domain.PayoutSubmitted,
		"sending":    domain.PayoutSubmitted,
		"finished":   domain.PayoutConfirmed,
		"failed":     domain.PayoutFailed,
	}
	for raw, expected := range cases {
		raw, expected := raw, expected
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/v1/payout/np-payout-1", r.URL.Path)
				_, _ = w.Write([]byte(`{"id":"np-payout-1","status":"` + raw + `"}`))
			})
			adapter, err := nowpayments.New(testConfig(server.URL))
			require.NoError(t, err)
			status, err := adapter.GetPayout(context.Background(), "np-payout-1")
			require.NoError(t, err)
			assert.Equal(t, expected, status.State)
		})
	}

	unknown := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"np-payout-1","status":"weird"}`))
	})
	adapter, err := nowpayments.New(testConfig(unknown.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayout(context.Background(), "np-payout-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState))
}
