package bitpay_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/providers/bitpay"
)

//nolint:gosec // G101: a stand-only test credential of the wire mock.
const testWebhookSecret = "bp-secret"

// OIDs used by the PEM test vectors.
var (
	oidECPublicKey = asn1.ObjectIdentifier{1, 2, 840, 10045, 2, 1}
	oidSecp256k1   = asn1.ObjectIdentifier{1, 3, 132, 0, 10}
)

// sec1Key mirrors the SEC1 EC private key structure without the curve params.
type sec1Key struct {
	Version    int
	PrivateKey []byte
	Curve      asn1.ObjectIdentifier `asn1:"optional,explicit,tag:0"`
}

// pkcs8Envelope mirrors the PKCS8 private key structure.
type pkcs8Envelope struct {
	Version    int
	Algo       pkix.AlgorithmIdentifier
	PrivateKey []byte
}

// testKeyPEM generates a secp256k1 key and renders it as a PEM block; the
// variants cover the accepted and rejected shapes of the key parser.
func testKeyPEM(t *testing.T, variant string) (string, *secp256k1.PrivateKey) {
	t.Helper()
	key, err := secp256k1.GeneratePrivateKey()
	require.NoError(t, err)
	serialized := key.Serialize()

	blockType := "EC PRIVATE KEY"
	var der []byte
	switch variant {
	case "sec1":
		der, err = asn1.Marshal(sec1Key{Version: 1, PrivateKey: serialized})
	case "pkcs8":
		params, marshalErr := asn1.Marshal(oidSecp256k1)
		require.NoError(t, marshalErr)
		seed, marshalErr := asn1.Marshal(sec1Key{Version: 1, PrivateKey: serialized})
		require.NoError(t, marshalErr)
		der, err = asn1.Marshal(pkcs8Envelope{
			Version:    0,
			Algo:       pkix.AlgorithmIdentifier{Algorithm: oidECPublicKey, Parameters: asn1.RawValue{FullBytes: params}},
			PrivateKey: seed,
		})
		blockType = "PRIVATE KEY"
	case "pkcs8-noparams":
		seed, marshalErr := asn1.Marshal(sec1Key{Version: 1, PrivateKey: serialized})
		require.NoError(t, marshalErr)
		der, err = asn1.Marshal(pkcs8Envelope{
			Version:    0,
			Algo:       pkix.AlgorithmIdentifier{Algorithm: oidECPublicKey},
			PrivateKey: seed,
		})
		blockType = "PRIVATE KEY"
	case "wrongcurve":
		der, err = asn1.Marshal(sec1Key{Version: 1, PrivateKey: serialized, Curve: asn1.ObjectIdentifier{1, 2, 840, 10045, 3, 1, 7}})
	case "short":
		der, err = asn1.Marshal(sec1Key{Version: 1, PrivateKey: serialized[:16]})
	default:
		require.FailNow(t, "unknown key variant", variant)
	}
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})), key
}

// testConfig builds a valid adapter configuration for a wire base URL.
func testConfig(t *testing.T, baseURL string) bitpay.Config {
	t.Helper()
	keyPEM, _ := testKeyPEM(t, "sec1")
	return bitpay.Config{
		BaseURL:       baseURL,
		APIToken:      "pair-token",
		PrivateKeyPEM: keyPEM,
		WebhookSecret: []byte(testWebhookSecret),
		Capabilities: domain.Capabilities{
			Networks: map[string]domain.NetworkCapability{
				"usdt_trc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
			},
		},
		Timeout: 2 * time.Second,
	}
}

// invoiceBody renders a BitPay invoice response in the real facade envelope
// with decimal major-unit amounts.
func invoiceBody(id, status, amountPaid string, expirationMS int64) string {
	return `{"facade":"merchant/invoice","data":{"id":` + strconv.Quote(id) + `,"url":"https://mock-provider.local/bitpay/checkout/` + id + `",` +
		`"status":` + strconv.Quote(status) + `,"price":12.340000,"currency":"USDT","orderId":"order-1",` +
		`"amountPaid":` + amountPaid + `,"confirmations":2,` +
		`"invoiceTime":1792248000000,"expirationTime":` + strconv.FormatInt(expirationMS, 10) + `,` +
		`"bitcoinAddress":"MOCKADDR"}}`
}

// flatInvoiceBody renders the flat stand form still accepted by the decoder.
func flatInvoiceBody(id, status string) string {
	return `{"id":` + strconv.Quote(id) + `,"url":"https://mock-provider.local/bitpay/checkout/` + id + `",` +
		`"status":` + strconv.Quote(status) + `,"price":12.340000,"currency":"USDT","orderId":"order-1",` +
		`"amountPaid":12.340000,"confirmations":2,"invoiceTime":0,"expirationTime":0,"bitcoinAddress":"MOCKADDR"}`
}

// webhookBody renders a BitPay IPN body.
func webhookBody(status string, invoiceTime int64) string {
	return `{"event":{"code":1003,"name":"invoice_completed"},"data":{"id":"bp-gw-1","orderId":"order-1",` +
		`"status":` + strconv.Quote(status) + `,"price":12.34,"currency":"USDT","amountPaid":12.34,` +
		`"confirmations":2,"invoiceTime":` + strconv.FormatInt(invoiceTime, 10) + `}}`
}

// signWebhook computes the base64 HMAC-SHA256 signature of the raw body.
func signWebhook(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// createRequest builds the fixture invoice request.
func createRequest() domain.CreateInvoiceRequest {
	return domain.CreateInvoiceRequest{
		IdempotencyKey: "k-1",
		PaymentID:      "order-1",
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        "usdt_trc20",
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
func readBody(t *testing.T, r *http.Request) []byte {
	t.Helper()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	require.NoError(t, err)
	return body
}

// verifyRequestSignature verifies X-Identity and X-Signature of a recorded
// request against the generated key: the identity is the compressed public key
// hex and the signature is the hex DER of ECDSA over url plus body.
func verifyRequestSignature(t *testing.T, key *secp256k1.PrivateKey, fullURL string, body []byte, headers http.Header) {
	t.Helper()
	pub := key.PubKey()
	assert.Equal(t, hex.EncodeToString(pub.SerializeCompressed()), headers.Get("X-Identity"))
	raw, err := hex.DecodeString(headers.Get("X-Signature"))
	require.NoError(t, err)
	signature, err := ecdsa.ParseDERSignature(raw)
	require.NoError(t, err)
	digest := sha256.Sum256(append([]byte(fullURL), body...))
	assert.True(t, signature.Verify(digest[:], pub), "the request signature must verify over url plus body")
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	base := testConfig(t, "http://example.test")

	cases := map[string]func(*bitpay.Config){
		"missing base url":  func(c *bitpay.Config) { c.BaseURL = "" },
		"bad scheme":        func(c *bitpay.Config) { c.BaseURL = "ftp://example.test" },
		"userinfo":          func(c *bitpay.Config) { c.BaseURL = "http://user:pass@example.test" },
		"base url query":    func(c *bitpay.Config) { c.BaseURL = "http://example.test?a=1" },
		"base url fragment": func(c *bitpay.Config) { c.BaseURL = "http://example.test#x" },
		"missing token":     func(c *bitpay.Config) { c.APIToken = "" },
		"missing key":       func(c *bitpay.Config) { c.PrivateKeyPEM = "" },
		"garbage pem":       func(c *bitpay.Config) { c.PrivateKeyPEM = "not a pem" },
		"missing webhook":   func(c *bitpay.Config) { c.WebhookSecret = nil },
		"sandbox buyer email on the production host": func(c *bitpay.Config) {
			c.BaseURL = "https://bitpay.com"
			c.BuyerEmail = "sandbox@cbl.test"
		},
		"malformed buyer email on the test host": func(c *bitpay.Config) {
			c.BaseURL = "https://test.bitpay.com"
			c.BuyerEmail = "not-an-email"
		},
		"no networks": func(c *bitpay.Config) { c.Capabilities = domain.Capabilities{} },
		"empty capability row": func(c *bitpay.Config) {
			c.Capabilities.Networks = map[string]domain.NetworkCapability{"usdt_trc20": {}}
		},
		"conflicting decimals": func(c *bitpay.Config) {
			c.Capabilities.Networks = map[string]domain.NetworkCapability{
				"usdt_trc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
				"usdt_erc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 8)},
			}
		},
	}
	for name, mutate := range cases {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := base
			mutate(&config)
			_, err := bitpay.New(config)
			require.Error(t, err)
		})
	}

	clamped := base
	clamped.Timeout = 0
	adapter, err := bitpay.New(clamped)
	require.NoError(t, err)
	assert.Equal(t, "bitpay", adapter.Capabilities().Provider)
	assert.Equal(t, "bitpay", adapter.Provider())

	sandbox := base
	sandbox.BaseURL = "https://test.bitpay.com"
	sandbox.BuyerEmail = "sandbox@cbl.test"
	_, err = bitpay.New(sandbox)
	require.NoError(t, err, "the sandbox buyer contact is accepted on the test host")

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

func TestPEMKeyForms(t *testing.T) {
	t.Parallel()

	for _, variant := range []string{"sec1", "pkcs8"} {
		variant := variant
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			keyPEM, key := testKeyPEM(t, variant)
			var recorded struct {
				fullURL string
				body    []byte
				header  http.Header
			}
			server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
				recorded.fullURL = "http://" + r.Host + r.URL.RequestURI()
				recorded.body = readBody(t, r)
				recorded.header = r.Header.Clone()
				_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", 0)))
			})
			config := testConfig(t, server.URL)
			config.PrivateKeyPEM = keyPEM
			adapter, err := bitpay.New(config)
			require.NoError(t, err)
			_, err = adapter.CreateInvoice(context.Background(), createRequest())
			require.NoError(t, err)
			verifyRequestSignature(t, key, recorded.fullURL, recorded.body, recorded.header)
		})
	}

	for _, variant := range []string{"wrongcurve", "short", "pkcs8-noparams", "garbage"} {
		variant := variant
		t.Run("reject "+variant, func(t *testing.T) {
			t.Parallel()
			config := testConfig(t, "http://example.test")
			if variant == "garbage" {
				config.PrivateKeyPEM = "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n"
			} else {
				keyPEM, _ := testKeyPEM(t, variant)
				config.PrivateKeyPEM = keyPEM
			}
			_, err := bitpay.New(config)
			require.Error(t, err)
		})
	}
}

func TestRequestSigning(t *testing.T) {
	t.Parallel()
	keyPEM, key := testKeyPEM(t, "sec1")
	var recorded struct {
		fullURL string
		body    []byte
		header  http.Header
	}
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		recorded.fullURL = "http://" + r.Host + r.URL.RequestURI()
		recorded.body = readBody(t, r)
		recorded.header = r.Header.Clone()
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", 0)))
	})
	config := testConfig(t, server.URL)
	config.PrivateKeyPEM = keyPEM
	config.APIToken = "pairing-token"
	adapter, err := bitpay.New(config)
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)

	assert.Equal(t, "2.0.0", recorded.header.Get("X-Accept-Version"))
	assert.Empty(t, recorded.header.Get("Authorization"),
		"BitPay ignores the Authorization header; the token travels in the body or query (KI-connector-10)")
	assert.NotEmpty(t, recorded.header.Get("X-Signature"))
	verifyRequestSignature(t, key, recorded.fullURL, recorded.body, recorded.header)
}

func TestGetRequestCarriesTokenQuery(t *testing.T) {
	t.Parallel()
	keyPEM, key := testKeyPEM(t, "sec1")
	var recorded struct {
		fullURL string
		header  http.Header
	}
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		recorded.fullURL = "http://" + r.Host + r.URL.RequestURI()
		recorded.header = r.Header.Clone()
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "complete", "12.340000", 0)))
	})
	config := testConfig(t, server.URL)
	config.PrivateKeyPEM = keyPEM
	config.APIToken = "pairing-token"
	adapter, err := bitpay.New(config)
	require.NoError(t, err)

	_, err = adapter.GetPayment(context.Background(), "bp-gw-1")
	require.NoError(t, err)

	assert.Contains(t, recorded.fullURL, "token=pairing-token",
		"merchant reads carry the token as a query parameter (verified live 19.10.2026)")
	assert.Empty(t, recorded.header.Get("Authorization"))
	verifyRequestSignature(t, key, recorded.fullURL, nil, recorded.header)

	// A GET that already carries a query keeps its parameters and gains the
	// token; the signature covers the merged URL.
	recorded.fullURL = ""
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Contains(t, recorded.fullURL, "orderId=order-1")
	assert.Contains(t, recorded.fullURL, "token=pairing-token")
	verifyRequestSignature(t, key, recorded.fullURL, nil, recorded.header)
}

func TestCreateInvoiceSandboxBuyerContact(t *testing.T) {
	t.Parallel()
	var captured []byte
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		captured = readBody(t, r)
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", 0)))
	})
	config := testConfig(t, server.URL)
	config.BuyerEmail = "sandbox@cbl.test"
	adapter, err := bitpay.New(config)
	require.NoError(t, err)

	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)

	var sent struct {
		Buyer struct {
			Email string `json:"email"`
		} `json:"buyer"`
		Wallet   string `json:"forcedBuyerSelectedWallet"`
		Currency string `json:"forcedBuyerSelectedTransactionCurrency"`
	}
	require.NoError(t, json.Unmarshal(captured, &sent))
	assert.Equal(t, "sandbox@cbl.test", sent.Buyer.Email, "the mock pay requires a contact email")
	assert.Equal(t, "bitpay", sent.Wallet)
	assert.Equal(t, "USDT", sent.Currency, "the forced transaction currency is the request asset")

	plain := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		captured = readBody(t, r)
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", 0)))
	})
	adapter, err = bitpay.New(testConfig(t, plain.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(captured, &raw))
	assert.NotContains(t, raw, "buyer", "without the sandbox buyer email the buyer fields stay absent")
	assert.NotContains(t, raw, "forcedBuyerSelectedWallet")
}

func TestCreateInvoiceHappyPath(t *testing.T) {
	t.Parallel()
	expiration := time.Now().UTC().Add(2 * time.Hour)
	var captured []byte
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/invoices", r.URL.Path)
		captured = readBody(t, r)
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", expiration.UnixMilli())))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)

	request := createRequest()
	invoice, err := adapter.CreateInvoice(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, "bp-gw-1", invoice.GatewayID)
	assert.Equal(t, "bitpay", invoice.Provider)
	assert.Equal(t, "MOCKADDR", invoice.Address)
	assert.Equal(t, "https://mock-provider.local/bitpay/checkout/bp-gw-1", invoice.CheckoutURL)
	assert.Equal(t, request.Network, invoice.Network)
	assert.Equal(t, int64(12340000), invoice.Amount.MinorUnits)
	assert.Equal(t, "USDT", invoice.Amount.Asset)
	assert.Equal(t, request.ExpiresAt, invoice.ExpiresAt, "a provider expiry beyond the request horizon is clamped")

	var sent struct {
		Price   json.Number `json:"price"`
		Curr    string      `json:"currency"`
		Token   string      `json:"token"`
		OrderID string      `json:"orderId"`
		PosData string      `json:"posData"`
	}
	require.NoError(t, json.Unmarshal(captured, &sent))
	assert.Equal(t, "12.340000", sent.Price.String(), "the price crosses the wire as a major-unit decimal literal")
	assert.Equal(t, "USDT", sent.Curr)
	assert.Equal(t, "pair-token", sent.Token)
	assert.Equal(t, "order-1", sent.OrderID)
	assert.Equal(t, "usdt_trc20", sent.PosData)

	missingWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", 0)))
	})
	adapter, err = bitpay.New(testConfig(t, missingWire.URL))
	require.NoError(t, err)
	invoice, err = adapter.CreateInvoice(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, request.ExpiresAt, invoice.ExpiresAt, "an absent expiration echoes the request horizon")
}

func TestCreateInvoiceNetworkValidation(t *testing.T) {
	t.Parallel()
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", 0)))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)

	request := createRequest()
	request.Network = "usdt_erc20"
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a network outside the capabilities must not reach the provider: %v", err)
	assert.Zero(t, calls)
}

func TestCreateInvoiceAmountMismatchRejected(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"facade":"merchant/invoice","data":{"id":"bp-gw-1","url":"https://mock-provider.local/bitpay/checkout/bp-gw-1","status":"new","price":1.000000,"currency":"USDT","orderId":"order-1","amountPaid":0.000000,"confirmations":0,"invoiceTime":1792248000000,"expirationTime":0}}`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a provider amount that differs from the request fails closed: %v", err)
}

func TestNonPositiveAmountRejected(t *testing.T) {
	t.Parallel()
	adapter, err := bitpay.New(testConfig(t, "http://example.test"))
	require.NoError(t, err)

	request := createRequest()
	request.Amount = money.Money{MinorUnits: 0, Asset: "USDT", Decimals: 6}
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))

	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-1",
		Amount:   money.Money{MinorUnits: -5, Asset: "USDT", Decimals: 6},
		Network:  "usdt_trc20",
		Address:  "MOCKADDR",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
}

func TestEmptyIdentifiersRejected(t *testing.T) {
	t.Parallel()
	adapter, err := bitpay.New(testConfig(t, "http://example.test"))
	require.NoError(t, err)

	_, err = adapter.GetPayment(context.Background(), "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))

	_, err = adapter.LookupByOrderID(context.Background(), "  ")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))

	_, err = adapter.GetPayout(context.Background(), "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
}

func TestErrorClassification(t *testing.T) {
	t.Parallel()
	status := 0
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		if status == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(status)
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)

	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "503 must be temporary: %v", err)
	assert.False(t, errors.Is(err, domain.ErrRejected))

	for _, code := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden} {
		status = code
		_, err = adapter.CreateInvoice(context.Background(), createRequest())
		require.Error(t, err)
		assert.True(t, errors.Is(err, domain.ErrRejected), "%d must be a rejection: %v", code, err)
	}

	status = http.StatusFound
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "an unfollowed redirect is temporary: %v", err)

	dialWire := wireServer(t, func(http.ResponseWriter, *http.Request) {})
	dialWire.Close()
	adapter, err = bitpay.New(testConfig(t, dialWire.URL))
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "a dial error is temporary: %v", err)

	// A GET transport failure embeds the request URL: the token query
	// parameter must not leak into the error text (KI-connector-10).
	_, err = adapter.GetPayment(context.Background(), "bp-gw-1")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "pair-token", "the token must not leak into transport error texts: %v", err)
}

func TestProviderErrorBodyDoesNotLeakToken(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		//nolint:gosec // G705: a test wire server echoing its own request URI to simulate a WAF page.
		_, _ = w.Write([]byte(`{"error":"blocked uri ` + r.URL.RequestURI() + `"}`))
	})
	config := testConfig(t, server.URL)
	config.APIToken = "pairing-token"
	adapter, err := bitpay.New(config)
	require.NoError(t, err)

	_, err = adapter.GetPayment(context.Background(), "bp-gw-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
	assert.NotContains(t, err.Error(), "pairing-token",
		"a provider excerpt echoing the request URI must be redacted (security review 17.26): %v", err)
	assert.Contains(t, err.Error(), "[redacted]")
}

func TestStatusMapping(t *testing.T) {
	t.Parallel()
	expected := map[string]domain.State{
		"new":       domain.StatePending,
		"paid":      domain.StateDetected,
		"confirmed": domain.StateDetected,
		"complete":  domain.StateConfirmed,
		"expired":   domain.StateExpired,
		"invalid":   domain.StateFailed,
	}
	for raw, state := range expected {
		raw, state := raw, state
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(invoiceBody("bp-gw-1", raw, "12.340000", 0)))
			})
			adapter, err := bitpay.New(testConfig(t, server.URL))
			require.NoError(t, err)
			status, err := adapter.GetPayment(context.Background(), "bp-gw-1")
			require.NoError(t, err)
			assert.Equal(t, state, status.State)
			assert.Equal(t, raw, status.RawStatus)
			assert.Equal(t, "bp-gw-1", status.GatewayID)
			assert.Equal(t, "bitpay", status.Provider)
			assert.Equal(t, int64(12340000), status.ReceivedAmount.MinorUnits)
			assert.Equal(t, "USDT", status.ReceivedAmount.Asset)
			assert.Equal(t, uint32(2), status.Confirmations)
			assert.Equal(t, time.UnixMilli(1792248000000).UTC(), status.GatewayEventTS)
		})
	}

	unknownWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "totally_unknown", "0.000000", 0)))
	})
	adapter, err := bitpay.New(testConfig(t, unknownWire.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "bp-gw-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState))
}

func TestNegativeProviderAmountRejected(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "paid", "-12.340000", 0)))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "bp-gw-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a negative provider amount must fail closed: %v", err)
}

func TestGetPaymentNotFound(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "bp-gw-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestRequestIdentifierEscaping(t *testing.T) {
	t.Parallel()
	var escaped string
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		escaped = r.URL.EscapedPath()
		_, _ = w.Write([]byte(flatInvoiceBody("bp-gw-1", "new")))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	_, err = adapter.GetPayment(context.Background(), "a/../b?x=1")
	require.NoError(t, err)
	assert.Contains(t, escaped, "%2F")
	assert.NotContains(t, escaped, "?")

	var rawQuery string
	queryWire := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		rawQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(flatInvoiceBody("bp-gw-1", "new")))
	})
	adapter, err = bitpay.New(testConfig(t, queryWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order&x=1")
	require.NoError(t, err)
	assert.Contains(t, rawQuery, "orderId=order%26x%3D1")
}

func TestVerifyWebhookRawHMAC(t *testing.T) {
	t.Parallel()
	adapter, err := bitpay.New(testConfig(t, "http://example.test"))
	require.NoError(t, err)
	body := []byte(webhookBody("complete", 1792248000000))
	headers := map[string][]string{"x-signature": {signWebhook([]byte(testWebhookSecret), body)}}

	event, err := adapter.VerifyWebhook(headers, body)
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, event.Type)
	assert.Equal(t, "complete", event.RawStatus)
	assert.Equal(t, "bp-gw-1", event.GatewayID)
	assert.Equal(t, "order-1", event.PaymentID)
	assert.Equal(t, "bitpay", event.Provider)
	assert.Equal(t, int64(12340000), event.ReceivedAmount.MinorUnits)
	assert.Equal(t, "USDT", event.ReceivedAmount.Asset)
	assert.Equal(t, uint32(2), event.Confirmations)
	assert.Equal(t, time.UnixMilli(1792248000000).UTC(), event.OccurredAt)

	tampered := []byte(strings.Replace(string(body), `"status":"complete"`, `"status":"invalid"`, 1))
	_, err = adapter.VerifyWebhook(headers, tampered)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a tampered body must be rejected: %v", err)

	_, err = adapter.VerifyWebhook(nil, body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a missing signature must be rejected: %v", err)

	wrong := map[string][]string{"x-signature": {signWebhook([]byte("wrong-secret"), body)}}
	_, err = adapter.VerifyWebhook(wrong, body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnsignedWebhook), "a wrong secret must be rejected: %v", err)

	unknown := []byte(webhookBody("weird", 1792248000000))
	_, err = adapter.VerifyWebhook(map[string][]string{"x-signature": {signWebhook([]byte(testWebhookSecret), unknown)}}, unknown)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState), "an unknown status must not be mapped: %v", err)

	pending := []byte(webhookBody("new", 1792248000000))
	_, err = adapter.VerifyWebhook(map[string][]string{"x-signature": {signWebhook([]byte(testWebhookSecret), pending)}}, pending)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidWebhook), "a pre-deposit status carries no event: %v", err)

	malformed := []byte("not json")
	_, err = adapter.VerifyWebhook(map[string][]string{"x-signature": {signWebhook([]byte(testWebhookSecret), malformed)}}, malformed)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidWebhook), "a malformed body must be rejected: %v", err)

	zeroTime := []byte(webhookBody("paid", 0))
	event, err = adapter.VerifyWebhook(map[string][]string{"x-signature": {signWebhook([]byte(testWebhookSecret), zeroTime)}}, zeroTime)
	require.NoError(t, err)
	assert.True(t, event.OccurredAt.IsZero(), "an absent stamp leaves the event time zero for the receipt-time fallback")

	skewed := []byte(webhookBody("paid", time.Now().UTC().Add(72*time.Hour).UnixMilli()))
	event, err = adapter.VerifyWebhook(map[string][]string{"x-signature": {signWebhook([]byte(testWebhookSecret), skewed)}}, skewed)
	require.NoError(t, err)
	assert.True(t, event.OccurredAt.IsZero(), "a stamp beyond the future skew bound is ignored")
}

func TestLookupByOrderID(t *testing.T) {
	t.Parallel()
	objectWire := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/invoices", r.URL.Path)
		assert.Equal(t, "order-1", r.URL.Query().Get("orderId"))
		_, _ = w.Write([]byte(flatInvoiceBody("bp-gw-1", "paid")))
	})
	adapter, err := bitpay.New(testConfig(t, objectWire.URL))
	require.NoError(t, err)
	status, err := adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateDetected, status.State, "the flat stand form stays accepted")

	arrayWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[" + invoiceBody("bp-gw-1", "complete", "12.340000", 0) + "]"))
	})
	adapter, err = bitpay.New(testConfig(t, arrayWire.URL))
	require.NoError(t, err)
	status, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, status.State)

	emptyWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("[]"))
	})
	adapter, err = bitpay.New(testConfig(t, emptyWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))

	missWire := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	adapter, err = bitpay.New(testConfig(t, missWire.URL))
	require.NoError(t, err)
	_, err = adapter.LookupByOrderID(context.Background(), "order-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrGatewayNotFound))
}

func TestCreatePayoutProvisionalShape(t *testing.T) {
	t.Parallel()
	var captured []byte
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		assert.Equal(t, "/payouts", r.URL.Path)
		captured = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"bp-payout-1","status":"new"}`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	payout, err := adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		IdempotencyKey: "k-1",
		PayoutID:       "payout-1",
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        "usdt_trc20",
		Address:        "MOCKADDR",
	})
	require.NoError(t, err)
	assert.Equal(t, "bp-payout-1", payout.GatewayPayoutID)
	assert.Equal(t, domain.PayoutSubmitted, payout.State)
	assert.Equal(t, "bitpay", payout.Provider)

	var sent struct {
		Amount  json.Number `json:"amount"`
		Curr    string      `json:"currency"`
		Address string      `json:"address"`
		Ref     string      `json:"reference"`
		Token   string      `json:"token"`
	}
	require.NoError(t, json.Unmarshal(captured, &sent))
	assert.Equal(t, "12340000", sent.Amount.String())
	assert.Equal(t, "USDT", sent.Curr)
	assert.Equal(t, "MOCKADDR", sent.Address)
	assert.Equal(t, "payout-1", sent.Ref)
	assert.Equal(t, "pair-token", sent.Token, "POST bodies carry the token: BitPay ignores Authorization (KI-connector-10)")

	before := calls
	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-2",
		Amount:   money.MustParse("12.34", "USDT", 6),
		Network:  "usdt_trc20",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
	assert.Equal(t, before, calls, "a payout without a destination rejects before any provider call")

	_, err = adapter.CreatePayout(context.Background(), domain.PayoutRequest{
		PayoutID: "payout-3",
		Amount:   money.Money{MinorUnits: 1234, Asset: "USDT", Decimals: 0},
		Network:  "usdt_trc20",
		Address:  "MOCKADDR",
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "the payout scale guard rejects before any provider call")
	assert.Equal(t, before, calls)

	request := createRequest()
	request.Amount.Decimals = 0
	_, err = adapter.CreateInvoice(context.Background(), request)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected), "a scale mismatch must not reach the provider: %v", err)
	assert.Equal(t, before, calls, "the scale guard rejects before any provider call")
}

func TestGetPayoutProvisionalShape(t *testing.T) {
	t.Parallel()
	status := "new"
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/payouts/bp-payout-1", r.URL.Path)
		_, _ = w.Write([]byte(`{"id":"bp-payout-1","status":"` + status + `"}`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)

	for raw, expected := range map[string]domain.PayoutState{
		"new":        domain.PayoutSubmitted,
		"processing": domain.PayoutSubmitted,
		"complete":   domain.PayoutConfirmed,
		"invalid":    domain.PayoutFailed,
		"cancelled":  domain.PayoutFailed,
	} {
		status = raw
		result, err := adapter.GetPayout(context.Background(), "bp-payout-1")
		require.NoError(t, err)
		assert.Equal(t, expected, result.State, "status %q", raw)
	}

	status = "totally_unknown"
	_, err = adapter.GetPayout(context.Background(), "bp-payout-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrUnknownState))
}

func TestCreateRefundProvisionalShape(t *testing.T) {
	t.Parallel()
	var captured []byte
	calls := 0
	server := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		assert.Equal(t, "/refunds", r.URL.Path)
		captured = readBody(t, r)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"bp-refund-1","status":"pending"}`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	refund, err := adapter.CreateRefund(context.Background(), domain.RefundRequest{
		IdempotencyKey:  "k-1",
		PayoutID:        "po-77",
		RefundInvoiceID: "refund-1",
		PaymentID:       "order-1",
		GatewayID:       "bp-gw-1",
		Amount:          money.MustParse("5", "USDT", 6),
		Address:         "MOCKADDR",
	})
	require.NoError(t, err)
	assert.Equal(t, "bp-refund-1", refund.GatewayRefundID)
	assert.Equal(t, "bitpay", refund.Provider)
	assert.Equal(t, int64(5000000), refund.Amount.MinorUnits)

	var sent struct {
		InvoiceID string      `json:"invoiceId"`
		Amount    json.Number `json:"amount"`
		Curr      string      `json:"currency"`
		Ref       string      `json:"reference"`
		Address   string      `json:"address"`
		Token     string      `json:"token"`
	}
	require.NoError(t, json.Unmarshal(captured, &sent))
	assert.Equal(t, "bp-gw-1", sent.InvoiceID)
	assert.Equal(t, "5000000", sent.Amount.String())
	assert.Equal(t, "USDT", sent.Curr)
	assert.Equal(t, "po-77", sent.Ref, "the per-command reference carries the payout id (17.12)")
	assert.Equal(t, "MOCKADDR", sent.Address)
	assert.Equal(t, "pair-token", sent.Token, "POST bodies carry the token (KI-connector-10)")

	before := calls
	_, err = adapter.CreateRefund(context.Background(), domain.RefundRequest{
		RefundInvoiceID: "refund-2",
		Amount:          money.MustParse("5", "USDT", 6),
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrRejected))
	assert.Equal(t, before, calls, "a refund without the provider invoice id rejects before any provider call")
}

func TestFetchedStatusFeedsCanonicalEvent(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "complete", "12.340000", 0)))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	status, err := adapter.GetPayment(context.Background(), "bp-gw-1")
	require.NoError(t, err)
	assert.False(t, status.GatewayEventTS.IsZero(), "the fixture carries an invoiceTime stamp")
	event := domain.CanonicalEvent{
		Type:           status.State,
		PaymentID:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		GatewayID:      status.GatewayID,
		Provider:       status.Provider,
		RawStatus:      status.RawStatus,
		Network:        status.Network,
		ReceivedAmount: status.ReceivedAmount,
		OccurredAt:     status.GatewayEventTS,
	}
	require.NoError(t, event.Validate(), "the fetch-to-verify path builds canonical events from this status")
}

func TestMalformedInvoiceTimeFallsBackToZeroTime(t *testing.T) {
	t.Parallel()
	server := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"facade":"merchant/invoice","data":{"id":"bp-gw-1","status":"new","price":12.340000,"currency":"USDT","orderId":"order-1","amountPaid":0.000000,"confirmations":0,"invoiceTime":0,"expirationTime":0,"bitcoinAddress":"MOCKADDR"}}`))
	})
	adapter, err := bitpay.New(testConfig(t, server.URL))
	require.NoError(t, err)
	status, err := adapter.GetPayment(context.Background(), "bp-gw-1")
	require.NoError(t, err)
	assert.True(t, status.GatewayEventTS.IsZero())
}

func TestCallerHTTPClientHardened(t *testing.T) {
	t.Parallel()
	var leaked int
	target := wireServer(t, func(w http.ResponseWriter, _ *http.Request) {
		leaked++
		_, _ = w.Write([]byte(invoiceBody("bp-gw-1", "new", "0.000000", 0)))
	})
	redirect := wireServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/invoices", http.StatusFound)
	})
	config := testConfig(t, redirect.URL)
	// The caller client would follow redirects and has no timeout by default;
	// the adapter must force the hardened policy on a copy.
	config.HTTPClient = &http.Client{}
	adapter, err := bitpay.New(config)
	require.NoError(t, err)
	_, err = adapter.CreateInvoice(context.Background(), createRequest())
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrTemporary), "an unfollowed redirect is temporary: %v", err)
	assert.Zero(t, leaked, "the signed request must not follow a redirect to another host")
}
