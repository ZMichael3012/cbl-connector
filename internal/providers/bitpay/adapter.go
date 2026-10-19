// Package bitpay implements the SPI v1 adapter of the BitPay API v2: invoice
// creation and reads, order lookup, ECDSA secp256k1 request signing with PEM key
// loading, raw-HMAC webhook verification and the provisional payout/refund
// shapes. Provider-specific code lives only here and in the manifest (plan
// 14.5.1).
package bitpay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	providerwire "github.com/cbl/cbl-connector/internal/providers/wire"
)

// Adapter bounds.
const (
	// defaultTimeout sets the provider call timeout when the config omits it.
	defaultTimeout = 10 * time.Second
	// maxResponseBytes bounds a provider response body.
	maxResponseBytes = 1 << 20
	// acceptVersion is the BitPay API version of every request.
	acceptVersion = "2.0.0"
	// maxEventSkew bounds how far a provider event timestamp may lie in the
	// future before it is ignored (a hostile stamp must not pin a payment state).
	maxEventSkew = 24 * time.Hour
)

// ErrConfiguration reports an invalid adapter configuration.
var ErrConfiguration = errors.New("bitpay: invalid configuration")

// PEM object identifiers of the accepted key material.
var (
	oidECPublicKey = asn1.ObjectIdentifier{1, 2, 840, 10045, 2, 1}
	oidSecp256k1   = asn1.ObjectIdentifier{1, 3, 132, 0, 10}
)

// ecPrivateKeyDER describes the SEC1 EC private key structure (RFC 5915).
type ecPrivateKeyDER struct {
	Version       int
	PrivateKey    []byte
	NamedCurveOID asn1.ObjectIdentifier `asn1:"optional,explicit,tag:0"`
	PublicKey     asn1.BitString        `asn1:"optional,explicit,tag:1"`
}

// pkcs8Envelope describes the PKCS8 private key structure (RFC 5208).
type pkcs8Envelope struct {
	Version    int
	Algo       pkix.AlgorithmIdentifier
	PrivateKey []byte
}

// Config describes an adapter instance.
type Config struct {
	// BaseURL sets the API root, for example https://bitpay.com.
	BaseURL string
	// APIToken sets the pairing token: POST/PUT bodies carry it, GET requests
	// carry it as the token query parameter (BitPay ignores Authorization).
	APIToken string
	// PrivateKeyPEM sets the secp256k1 request-signing key (SEC1 or PKCS8 PEM).
	PrivateKeyPEM string
	// WebhookSecret verifies the raw body HMAC of callbacks.
	WebhookSecret []byte
	// WebhookURL sets the optional notificationURL sent at invoice creation.
	WebhookURL string
	// BuyerEmail enables the sandbox mock pay: when set, the create body gains
	// the buyer contact email and the forced buyer selections (wallet bitpay,
	// transaction currency = the request asset). Never set in production.
	BuyerEmail string
	// Capabilities sets the network rows; Provider is forced to "bitpay".
	Capabilities domain.Capabilities
	// Timeout sets the HTTP client timeout; <= 0 means the default.
	Timeout time.Duration
	// HTTPClient overrides the client; used by tests.
	HTTPClient *http.Client
}

// Adapter implements domain.PaymentGateway plus the optional SPI v1 interfaces.
type Adapter struct {
	baseURL       string
	apiToken      string
	webhookURL    string
	webhookSecret []byte
	buyerEmail    string
	privateKey    *secp256k1.PrivateKey
	identityHex   string
	capabilities  domain.Capabilities
	decimals      map[string]uint32
	client        *http.Client
}

var (
	_ domain.PaymentGateway  = (*Adapter)(nil)
	_ domain.LookupGateway   = (*Adapter)(nil)
	_ domain.Refunder        = (*Adapter)(nil)
	_ domain.PaymentExporter = (*Adapter)(nil)
)

// New validates the configuration and builds the adapter.
func New(cfg Config) (*Adapter, error) {
	base := strings.TrimSpace(cfg.BaseURL)
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("%w: invalid base url", ErrConfiguration)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: unsupported scheme %q", ErrConfiguration, parsed.Scheme)
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("%w: base url must not carry userinfo", ErrConfiguration)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: base url must not carry a query or fragment", ErrConfiguration)
	}
	apiToken := strings.TrimSpace(cfg.APIToken)
	if apiToken == "" {
		return nil, fmt.Errorf("%w: api token is required", ErrConfiguration)
	}
	if strings.TrimSpace(cfg.PrivateKeyPEM) == "" {
		return nil, fmt.Errorf("%w: private key pem is required", ErrConfiguration)
	}
	privateKey, err := parsePrivateKeyPEM(cfg.PrivateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConfiguration, err)
	}
	if len(cfg.WebhookSecret) == 0 {
		return nil, fmt.Errorf("%w: webhook secret is required", ErrConfiguration)
	}
	buyerEmail := strings.TrimSpace(cfg.BuyerEmail)
	if buyerEmail != "" {
		// The buyer contact exists for the sandbox mock pay only: production
		// must not preselect the buyer wallet (T-CONNECTOR-26), so the option
		// fails closed on every BitPay production host.
		host := strings.ToLower(parsed.Hostname())
		if host == "bitpay.com" || (strings.HasSuffix(host, ".bitpay.com") && host != "test.bitpay.com") {
			return nil, fmt.Errorf("%w: buyer email is a sandbox-only option", ErrConfiguration)
		}
		if len(buyerEmail) > 254 || !strings.Contains(buyerEmail, "@") {
			return nil, fmt.Errorf("%w: buyer email is malformed", ErrConfiguration)
		}
	}
	if len(cfg.Capabilities.Networks) == 0 {
		return nil, fmt.Errorf("%w: at least one network capability is required", ErrConfiguration)
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: timeout,
			// Provider API redirects are not followed: an unexpected redirect
			// must not forward the request signature to another host.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	} else {
		// A caller-provided client must not weaken the transport guarantees of
		// the adapter: redirects stay blocked and the timeout is enforced.
		hardened := *client
		hardened.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
		if hardened.Timeout <= 0 {
			hardened.Timeout = timeout
		}
		client = &hardened
	}
	capabilities := cfg.Capabilities
	capabilities.Provider = "bitpay"
	decimals := make(map[string]uint32, len(capabilities.Networks))
	for name, network := range capabilities.Networks {
		asset := strings.ToUpper(strings.TrimSpace(network.Asset))
		scale := network.MinAmount.Decimals
		if scale == 0 {
			scale = network.MaxAmount.Decimals
		}
		if asset == "" || scale == 0 {
			return nil, fmt.Errorf("%w: network %q needs an asset and decimals", ErrConfiguration, name)
		}
		if existing, ok := decimals[asset]; ok && existing != scale {
			return nil, fmt.Errorf("%w: asset %q has conflicting decimals", ErrConfiguration, asset)
		}
		decimals[asset] = scale
	}
	return &Adapter{
		baseURL:       strings.TrimRight(base, "/"),
		apiToken:      apiToken,
		webhookURL:    strings.TrimSpace(cfg.WebhookURL),
		webhookSecret: cfg.WebhookSecret,
		buyerEmail:    buyerEmail,
		privateKey:    privateKey,
		identityHex:   hex.EncodeToString(privateKey.PubKey().SerializeCompressed()),
		capabilities:  capabilities,
		decimals:      decimals,
		client:        client,
	}, nil
}

// parsePrivateKeyPEM parses a SEC1 or PKCS8 PEM block of a secp256k1 key; the
// error text never includes the key material (E-1).
func parsePrivateKeyPEM(text string) (*secp256k1.PrivateKey, error) {
	block, _ := pem.Decode([]byte(text))
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	var decoded ecPrivateKeyDER
	switch block.Type {
	case "EC PRIVATE KEY":
		if err := unmarshalDER(block.Bytes, &decoded); err != nil {
			return nil, fmt.Errorf("parse SEC1 key: %v", err)
		}
	case "PRIVATE KEY":
		var envelope pkcs8Envelope
		if err := unmarshalDER(block.Bytes, &envelope); err != nil {
			return nil, fmt.Errorf("parse PKCS8 key: %v", err)
		}
		if !envelope.Algo.Algorithm.Equal(oidECPublicKey) {
			return nil, fmt.Errorf("unsupported PKCS8 algorithm %v", envelope.Algo.Algorithm)
		}
		if len(envelope.Algo.Parameters.FullBytes) == 0 {
			return nil, errors.New("PKCS8 key is missing the curve parameters")
		}
		var curve asn1.ObjectIdentifier
		if err := unmarshalDER(envelope.Algo.Parameters.FullBytes, &curve); err != nil || !curve.Equal(oidSecp256k1) {
			return nil, errors.New("PKCS8 key is not on the secp256k1 curve")
		}
		if err := unmarshalDER(envelope.PrivateKey, &decoded); err != nil {
			return nil, fmt.Errorf("parse PKCS8 payload: %v", err)
		}
	default:
		return nil, fmt.Errorf("unsupported PEM block %q", block.Type)
	}
	if decoded.NamedCurveOID != nil && !decoded.NamedCurveOID.Equal(oidSecp256k1) {
		return nil, fmt.Errorf("key is not on the secp256k1 curve")
	}
	if len(decoded.PrivateKey) != 32 {
		return nil, fmt.Errorf("key is %d bytes, want 32", len(decoded.PrivateKey))
	}
	var scalar secp256k1.ModNScalar
	if overflow := scalar.SetByteSlice(decoded.PrivateKey); overflow || scalar.IsZero() {
		return nil, errors.New("key scalar is out of range")
	}
	return secp256k1.NewPrivateKey(&scalar), nil
}

// unmarshalDER decodes a strict DER structure.
func unmarshalDER(der []byte, target any) error {
	rest, err := asn1.Unmarshal(der, target)
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return errors.New("trailing DER data")
	}
	return nil
}

// Capabilities returns the configured network rows with the bitpay provider id.
func (a *Adapter) Capabilities() domain.Capabilities { return a.capabilities }

// Provider returns the manifest id of the adapter (the capability source port).
func (a *Adapter) Provider() string { return "bitpay" }

// sign builds the X-Signature value: hex DER of the ECDSA secp256k1 signature
// over SHA256(url + body), the scheme of the official BitPay SDK key utilities.
func (a *Adapter) sign(fullURL string, body []byte) string {
	digest := sha256.Sum256(append([]byte(fullURL), body...))
	return hex.EncodeToString(ecdsa.Sign(a.privateKey, digest[:]).Serialize())
}

// invoiceResponse describes the BitPay invoice body; amounts are decimal
// major-unit numbers in the pricing currency.
type invoiceResponse struct {
	ID             string          `json:"id"`
	URL            string          `json:"url"`
	Status         string          `json:"status"`
	Price          json.RawMessage `json:"price"`
	Currency       string          `json:"currency"`
	OrderID        string          `json:"orderId"`
	AmountPaid     json.RawMessage `json:"amountPaid"`
	Confirmations  uint32          `json:"confirmations"`
	InvoiceTime    int64           `json:"invoiceTime"`
	ExpirationTime int64           `json:"expirationTime"`
	BitcoinAddress string          `json:"bitcoinAddress"`
}

// invoiceEnvelope is the facade wrapper of the real API.
type invoiceEnvelope struct {
	Data json.RawMessage `json:"data"`
}

// decodeInvoice unwraps the facade envelope when present and decodes the
// invoice body; the flat form stays accepted for the stand fixtures.
func decodeInvoice(payload []byte) (invoiceResponse, error) {
	var envelope invoiceEnvelope
	if err := json.Unmarshal(payload, &envelope); err == nil && len(envelope.Data) > 0 {
		payload = envelope.Data
	}
	var decoded invoiceResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return invoiceResponse{}, err
	}
	return decoded, nil
}

// eventTime converts Unix milliseconds into the event time; an absent stamp or
// one beyond the future skew bound leaves a zero time for the receipt fallback
// (E-5).
func eventTime(millis int64) time.Time {
	if millis <= 0 {
		return time.Time{}
	}
	stamp := time.UnixMilli(millis).UTC()
	if stamp.After(time.Now().UTC().Add(maxEventSkew)) {
		return time.Time{}
	}
	return stamp
}

// CreateInvoice creates an invoice in the provider.
func (a *Adapter) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Invoice{}, err
	}
	if err := a.ensureNetwork(req.Network, req.Amount.Asset); err != nil {
		return domain.Invoice{}, err
	}
	body := map[string]any{
		// The official API takes the price as a decimal number in major units
		// of the pricing currency; the raw literal keeps the exact scale.
		"price":    json.RawMessage(req.Amount.String()),
		"currency": strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		// token is the merchant pairing token (the secret of the adapter).
		"token":   a.apiToken,
		"orderId": req.PaymentID,
		// posData carries the requested network: the response has no network
		// field (D2).
		"posData": req.Network,
	}
	if a.webhookURL != "" {
		body["notificationURL"] = a.webhookURL
	}
	if a.buyerEmail != "" {
		// The sandbox mock pay requires a contact email and the preselected
		// wallet/currency; production must not preselect the buyer's wallet
		// (T-CONNECTOR-26).
		body["buyer"] = map[string]string{"email": a.buyerEmail}
		body["forcedBuyerSelectedWallet"] = "bitpay"
		body["forcedBuyerSelectedTransactionCurrency"] = strings.ToUpper(strings.TrimSpace(req.Amount.Asset))
	}
	payload, err := a.request(ctx, http.MethodPost, "/invoices", body, domain.ErrRejected)
	if err != nil {
		return domain.Invoice{}, err
	}
	decoded, err := decodeInvoice(payload)
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("bitpay: decode invoice response: %w", err)
	}
	if decoded.ID == "" {
		return domain.Invoice{}, errors.New("bitpay: invoice response has no id")
	}
	if !strings.EqualFold(strings.TrimSpace(decoded.Currency), strings.TrimSpace(req.Amount.Asset)) {
		return domain.Invoice{}, fmt.Errorf("%w: provider invoice currency %q does not match the request asset", domain.ErrRejected, decoded.Currency)
	}
	amount, err := a.parseAmountLiteral(decoded.Price, decoded.Currency)
	if err != nil {
		return domain.Invoice{}, err
	}
	if amount.MinorUnits != req.Amount.MinorUnits {
		return domain.Invoice{}, fmt.Errorf("%w: provider invoice amount does not match the request", domain.ErrRejected)
	}
	return domain.Invoice{
		GatewayID:   decoded.ID,
		Provider:    "bitpay",
		Address:     decoded.BitcoinAddress,
		CheckoutURL: decoded.URL,
		Network:     req.Network,
		Amount:      amount,
		ExpiresAt:   clampExpiry(decoded.ExpirationTime, req.ExpiresAt),
	}, nil
}

// clampExpiry bounds the provider expiration by the request horizon; an absent
// stamp echoes the request horizon (E-2).
func clampExpiry(expirationMS int64, horizon time.Time) time.Time {
	if expirationMS <= 0 {
		return horizon.UTC()
	}
	expiry := time.UnixMilli(expirationMS).UTC()
	if !horizon.IsZero() && expiry.After(horizon) {
		return horizon.UTC()
	}
	return expiry
}

// GetPayment reads the authoritative invoice status.
func (a *Adapter) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	if strings.TrimSpace(gatewayID) == "" {
		return domain.PaymentStatus{}, fmt.Errorf("%w: gateway id is required", domain.ErrRejected)
	}
	payload, err := a.request(ctx, http.MethodGet, "/invoices/"+url.PathEscape(gatewayID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	decoded, err := decodeInvoice(payload)
	if err != nil {
		return domain.PaymentStatus{}, fmt.Errorf("bitpay: decode invoice response: %w", err)
	}
	return a.statusOf(gatewayID, decoded)
}

// LookupByOrderID finds the invoice created for the order reference; the real
// list answers an array, the mock profile answers a single object (E-3).
func (a *Adapter) LookupByOrderID(ctx context.Context, orderID string) (domain.PaymentStatus, error) {
	if strings.TrimSpace(orderID) == "" {
		return domain.PaymentStatus{}, fmt.Errorf("%w: order reference is required", domain.ErrRejected)
	}
	path := "/invoices?orderId=" + url.QueryEscape(orderID)
	payload, err := a.request(ctx, http.MethodGet, path, nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	decoded, err := a.decodeLookup(payload, orderID)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	return a.statusOf(decoded.ID, decoded)
}

// decodeLookup tolerates the array answer of the real list endpoint and the
// single-object answer of the stand; elements may carry the facade envelope.
func (a *Adapter) decodeLookup(payload []byte, orderID string) (invoiceResponse, error) {
	notFound := fmt.Errorf("%w: no invoice for reference %q", domain.ErrGatewayNotFound, orderID)
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return invoiceResponse{}, fmt.Errorf("bitpay: decode invoice list: %w", err)
		}
		if len(items) == 0 {
			return invoiceResponse{}, notFound
		}
		decoded, err := decodeInvoice(items[0])
		if err != nil {
			return invoiceResponse{}, fmt.Errorf("bitpay: decode invoice: %w", err)
		}
		return decoded, nil
	}
	decoded, err := decodeInvoice(trimmed)
	if err != nil {
		return invoiceResponse{}, fmt.Errorf("bitpay: decode invoice: %w", err)
	}
	return decoded, nil
}

// statusOf maps an invoice body into the canonical status; an unknown status
// word is never fabricated (E-4). The received amount is amountPaid; an absent
// or skewed invoiceTime leaves the event time zero (E-5).
func (a *Adapter) statusOf(gatewayID string, decoded invoiceResponse) (domain.PaymentStatus, error) {
	state, ok := invoiceStates[decoded.Status]
	if !ok {
		return domain.PaymentStatus{}, fmt.Errorf("%w: status %q", domain.ErrUnknownState, truncateText([]byte(decoded.Status)))
	}
	received := money.Money{}
	if len(decoded.AmountPaid) > 0 && strings.TrimSpace(decoded.Currency) != "" {
		parsed, err := a.parseAmountLiteral(decoded.AmountPaid, decoded.Currency)
		if err != nil {
			return domain.PaymentStatus{}, err
		}
		received = parsed
	}
	return domain.PaymentStatus{
		GatewayID:      gatewayID,
		Provider:       "bitpay",
		State:          state,
		RawStatus:      decoded.Status,
		ReceivedAmount: received,
		Confirmations:  decoded.Confirmations,
		GatewayEventTS: eventTime(decoded.InvoiceTime),
	}, nil
}

// ExportPayments returns one page of invoice records for reconciliation
// (17.13): the walk is offset-based, the window crosses the wire as dates
// (provisional until 17.16) and rows are mapped fail-soft.
func (a *Adapter) ExportPayments(ctx context.Context, req domain.ExportRequest) (domain.ExportPage, error) {
	offset, err := domain.ExportOffsetParams(req.Cursor, req.Limit)
	if err != nil {
		return domain.ExportPage{}, err
	}
	query := url.Values{}
	query.Set("dateStart", req.From.UTC().Format("2006-01-02"))
	query.Set("dateEnd", req.To.UTC().Format("2006-01-02"))
	query.Set("limit", strconv.Itoa(req.Limit))
	query.Set("offset", strconv.Itoa(offset))
	payload, err := a.request(ctx, http.MethodGet, "/invoices?"+query.Encode(), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.ExportPage{}, err
	}
	rows, err := providerwire.DecodeEnvelopeOrArray[invoiceResponse](payload, "data")
	if err != nil {
		return domain.ExportPage{}, fmt.Errorf("bitpay: decode invoice export: %w", err)
	}
	payments := make([]domain.PaymentStatus, 0, len(rows))
	for _, row := range rows {
		payments = append(payments, a.exportStatusOf(row))
	}
	return domain.ExportPage{
		Payments:   payments,
		NextCursor: domain.ExportNextCursor(offset, len(payments), req.Limit),
	}, nil
}

// exportStatusOf maps an invoice body fail-soft (17.13): an unknown status
// word leaves the canonical state empty and an unparsable amount stays zero.
func (a *Adapter) exportStatusOf(decoded invoiceResponse) domain.PaymentStatus {
	status := domain.PaymentStatus{
		GatewayID:      decoded.ID,
		PaymentID:      decoded.OrderID,
		Provider:       "bitpay",
		State:          invoiceStates[decoded.Status],
		RawStatus:      decoded.Status,
		Confirmations:  decoded.Confirmations,
		GatewayEventTS: eventTime(decoded.InvoiceTime),
	}
	if len(decoded.AmountPaid) > 0 && strings.TrimSpace(decoded.Currency) != "" {
		if parsed, err := a.parseAmountLiteral(decoded.AmountPaid, decoded.Currency); err == nil {
			status.ReceivedAmount = parsed
		}
	}
	return status
}

// invoiceStates maps BitPay invoice statuses to canonical states; `new` is a
// pre-deposit status and maps to pending (KI-connector-14).
var invoiceStates = map[string]domain.State{
	"new":       domain.StatePending,
	"paid":      domain.StateDetected,
	"confirmed": domain.StateDetected,
	"complete":  domain.StateConfirmed,
	"expired":   domain.StateExpired,
	"invalid":   domain.StateFailed,
}

// payoutResponse describes the provisional payout body (17.11).
type payoutResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// getPayoutStates maps the provisional payout states (17.11 owns the complete
// table; the stand re-validates the shapes in 17.16).
var getPayoutStates = map[string]domain.PayoutState{
	"new":        domain.PayoutSubmitted,
	"processing": domain.PayoutSubmitted,
	"complete":   domain.PayoutConfirmed,
	"invalid":    domain.PayoutFailed,
	"cancelled":  domain.PayoutFailed,
	"canceled":   domain.PayoutFailed,
	"failed":     domain.PayoutFailed,
}

// CreatePayout creates a provisional payout (17.11 owns the real batch flow);
// the creation reports the submitted state whatever the provider word (E-6 of
// 17.7).
func (a *Adapter) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	if strings.TrimSpace(req.Address) == "" {
		return domain.Payout{}, fmt.Errorf("%w: payout destination is required", domain.ErrRejected)
	}
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Payout{}, err
	}
	body := map[string]any{
		"amount":    req.Amount.MinorUnits,
		"currency":  strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		"address":   req.Address,
		"reference": req.PayoutID,
		"token":     a.apiToken,
	}
	payload, err := a.request(ctx, http.MethodPost, "/payouts", body, domain.ErrRejected)
	if err != nil {
		return domain.Payout{}, err
	}
	var decoded payoutResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Payout{}, fmt.Errorf("bitpay: decode payout response: %w", err)
	}
	if decoded.ID == "" {
		return domain.Payout{}, errors.New("bitpay: payout response has no id")
	}
	return domain.Payout{GatewayPayoutID: decoded.ID, Provider: "bitpay", State: domain.PayoutSubmitted}, nil
}

// GetPayout reads the provisional payout state (17.11).
func (a *Adapter) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	if strings.TrimSpace(gatewayPayoutID) == "" {
		return domain.PayoutStatus{}, fmt.Errorf("%w: payout id is required", domain.ErrRejected)
	}
	payload, err := a.request(ctx, http.MethodGet, "/payouts/"+url.PathEscape(gatewayPayoutID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PayoutStatus{}, err
	}
	var decoded payoutResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PayoutStatus{}, fmt.Errorf("bitpay: decode payout status: %w", err)
	}
	state, ok := getPayoutStates[decoded.Status]
	if !ok {
		return domain.PayoutStatus{}, fmt.Errorf("%w: payout state %q", domain.ErrUnknownState, truncateText([]byte(decoded.Status)))
	}
	return domain.PayoutStatus{State: state}, nil
}

// refundResponse describes the provisional refund body (17.12).
type refundResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// CreateRefund creates a provisional refund (17.12 owns the real flow).
func (a *Adapter) CreateRefund(ctx context.Context, req domain.RefundRequest) (domain.Refund, error) {
	if strings.TrimSpace(req.RefundInvoiceID) == "" {
		return domain.Refund{}, fmt.Errorf("%w: refund reference is required", domain.ErrRejected)
	}
	if strings.TrimSpace(req.GatewayID) == "" {
		return domain.Refund{}, fmt.Errorf("%w: provider invoice id is required", domain.ErrRejected)
	}
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Refund{}, err
	}
	body := map[string]any{
		"invoiceId": req.GatewayID,
		"amount":    req.Amount.MinorUnits,
		"currency":  strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		"reference": referenceOf(req),
		"token":     a.apiToken,
	}
	if address := strings.TrimSpace(req.Address); address != "" {
		body["address"] = address
	}
	payload, err := a.request(ctx, http.MethodPost, "/refunds", body, domain.ErrRejected)
	if err != nil {
		return domain.Refund{}, err
	}
	var decoded refundResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Refund{}, fmt.Errorf("bitpay: decode refund response: %w", err)
	}
	if decoded.ID == "" {
		return domain.Refund{}, errors.New("bitpay: refund response has no id")
	}
	return domain.Refund{GatewayRefundID: decoded.ID, Provider: "bitpay", Amount: req.Amount}, nil
}

// refundStates maps provider refund statuses to canonical payout states (17.12,
// provisional until the 17.16 sandbox run).
var refundStates = map[string]domain.PayoutState{
	"completed":  domain.PayoutConfirmed,
	"success":    domain.PayoutConfirmed,
	"refunded":   domain.PayoutConfirmed,
	"pending":    domain.PayoutSubmitted,
	"processing": domain.PayoutSubmitted,
	"created":    domain.PayoutSubmitted,
	"failed":     domain.PayoutFailed,
	"failure":    domain.PayoutFailed,
	"canceled":   domain.PayoutFailed,
	"rejected":   domain.PayoutFailed,
}

// GetRefund reads the refund state (17.12); an unknown status fails closed.
func (a *Adapter) GetRefund(ctx context.Context, gatewayRefundID string) (domain.PayoutStatus, error) {
	if strings.TrimSpace(gatewayRefundID) == "" {
		return domain.PayoutStatus{}, fmt.Errorf("%w: refund id is required", domain.ErrRejected)
	}
	payload, err := a.request(ctx, http.MethodGet, "/refunds/"+url.PathEscape(gatewayRefundID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PayoutStatus{}, err
	}
	var decoded refundResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PayoutStatus{}, fmt.Errorf("bitpay: decode refund status: %w", err)
	}
	state, ok := refundStates[strings.ToLower(strings.TrimSpace(decoded.Status))]
	if !ok {
		return domain.PayoutStatus{}, fmt.Errorf("%w: refund state %q", domain.ErrUnknownState, truncateText([]byte(decoded.Status)))
	}
	return domain.PayoutStatus{State: state}, nil
}

// referenceOf returns the per-command reference of a refund: the payout id is
// unique per refund command, so a retry or a redelivery cannot create a second
// refund (17.12); the source invoice is only a fallback for legacy callers.
func referenceOf(req domain.RefundRequest) string {
	if payoutID := strings.TrimSpace(req.PayoutID); payoutID != "" {
		return payoutID
	}
	return req.RefundInvoiceID
}

// parseAmountLiteral parses a decimal major-unit JSON number at the configured
// asset scale; an absent or negative provider amount fails closed.
func (a *Adapter) parseAmountLiteral(raw json.RawMessage, asset string) (money.Money, error) {
	decimals, err := a.decimalsFor(asset)
	if err != nil {
		return money.Money{}, err
	}
	text := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	if text == "" || text == "null" {
		return money.Money{}, fmt.Errorf("%w: amount is missing", domain.ErrRejected)
	}
	parsed, err := money.Parse(text, strings.ToUpper(strings.TrimSpace(asset)), decimals)
	if err != nil {
		return money.Money{}, fmt.Errorf("bitpay: parse amount: %w", err)
	}
	if parsed.MinorUnits < 0 {
		return money.Money{}, fmt.Errorf("%w: negative provider amount", domain.ErrRejected)
	}
	return parsed, nil
}

// decimalsFor resolves the asset decimals from the configuration index; an
// asset outside the configured capabilities is a business rejection at runtime.
func (a *Adapter) decimalsFor(asset string) (uint32, error) {
	scale, ok := a.decimals[strings.ToUpper(strings.TrimSpace(asset))]
	if !ok {
		return 0, fmt.Errorf("%w: asset %q is outside the configured capabilities", domain.ErrRejected, asset)
	}
	return scale, nil
}

// ensureAmountScale rejects an amount whose scale differs from the configured
// asset scale (rendering it would mis-price the provider invoice by a power of
// ten) and a non-positive amount (E-8).
func (a *Adapter) ensureAmountScale(amount money.Money) (uint32, error) {
	if amount.MinorUnits <= 0 {
		return 0, fmt.Errorf("%w: amount must be positive", domain.ErrRejected)
	}
	scale, err := a.decimalsFor(amount.Asset)
	if err != nil {
		return 0, err
	}
	if amount.Decimals != scale {
		return 0, fmt.Errorf("%w: amount decimals %d do not match the asset scale %d", domain.ErrRejected, amount.Decimals, scale)
	}
	return scale, nil
}

// ensureNetwork validates the requested network row and its asset against the
// configured capabilities: a direct SPI call must not invoice a foreign asset.
func (a *Adapter) ensureNetwork(network, asset string) error {
	row, ok := a.capabilities.Networks[strings.ToLower(strings.TrimSpace(network))]
	if !ok {
		return fmt.Errorf("%w: network %q is outside the configured capabilities", domain.ErrRejected, network)
	}
	if !strings.EqualFold(strings.TrimSpace(row.Asset), strings.TrimSpace(asset)) {
		return fmt.Errorf("%w: network %q does not serve asset %q", domain.ErrRejected, network, asset)
	}
	return nil
}

// request performs a signed provider call; 404 answers the given class, 429/5xx
// and an unfollowed 3xx are temporary, other 4xx are business rejections
// (incl. 401/403, E-7).
func (a *Adapter) request(ctx context.Context, method, path string, body any, notFound error) ([]byte, error) {
	var payload []byte
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("bitpay: encode request: %w", err)
		}
		payload = encoded
	}
	fullURL := a.baseURL + path
	if method == http.MethodGet {
		// Merchant reads carry the token as a query parameter and the
		// signature covers the URL including it (verified live 19.10.2026,
		// KI-connector-10); BitPay ignores the Authorization header.
		parsed, parseErr := url.Parse(fullURL)
		if parseErr != nil {
			return nil, fmt.Errorf("bitpay: build request: %w", parseErr)
		}
		query := parsed.Query()
		query.Set("token", a.apiToken)
		parsed.RawQuery = query.Encode()
		fullURL = parsed.String()
	}
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, fullURL, reader)
	if err != nil {
		// A build failure wraps the URL (which may carry the token query):
		// unwrap it so the token never reaches logs (security review 17.26).
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("bitpay: build request: %w", err)
	}
	request.Header.Set("X-Accept-Version", acceptVersion)
	request.Header.Set("X-Identity", a.identityHex)
	request.Header.Set("X-Signature", a.sign(fullURL, payload))
	request.Header.Set("Accept", "application/json")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := a.client.Do(request)
	if err != nil {
		// The url.Error of the HTTP client embeds the full URL, which carries
		// the token query parameter: a transport failure must not leak the
		// token into logs or DLQ texts (KI-connector-10).
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("%w: %v", domain.ErrTemporary, err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: reading provider response: %v", domain.ErrTemporary, err)
	}
	switch {
	case response.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("%w: %s %s", notFound, method, path)
	case response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest:
		// Redirects are not followed: a 3xx is a blocked or misconfigured
		// route, never a provider success.
		return nil, fmt.Errorf("%w: provider redirect status %d", domain.ErrTemporary, response.StatusCode)
	case response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError:
		return nil, fmt.Errorf("%w: provider status %d", domain.ErrTemporary, response.StatusCode)
	case response.StatusCode >= http.StatusBadRequest:
		// The excerpt may echo the request URI (which carries the token query
		// parameter on reads): redact configured secrets before it reaches
		// logs or DLQ texts (security review 17.26).
		return nil, fmt.Errorf("%w: provider status %d: %s", domain.ErrRejected, response.StatusCode, truncateText(providerwire.Redact(payload, a.apiToken)))
	default:
		return payload, nil
	}
}

// truncateText bounds a provider error excerpt and replaces non-printable bytes
// so a hostile provider cannot forge log lines.
func truncateText(payload []byte) string {
	const limit = 200
	text := strings.TrimSpace(string(payload))
	if len(text) > limit {
		text = text[:limit]
	}
	var builder strings.Builder
	builder.Grow(len(text))
	for i := 0; i < len(text); i++ {
		b := text[i]
		if b >= 0x20 && b <= 0x7e {
			builder.WriteByte(b)
		} else {
			builder.WriteByte('.')
		}
	}
	return builder.String()
}
