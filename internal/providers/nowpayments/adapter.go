// Package nowpayments implements the SPI v1 adapter of the NOWPayments API:
// hosted invoices, payment reads and lookup, canonical JSON HMAC webhook
// verification and the provisional payout shapes. Provider-specific code lives
// only here and in the manifest (plan 14.5.1). Refunds are manual (decision 8),
// so the adapter implements no Refunder.
package nowpayments

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	providerwire "github.com/cbl/cbl-connector/internal/providers/wire"
)

// Adapter bounds.
const (
	// defaultTimeout sets the provider call timeout when the config omits it.
	defaultTimeout = 10 * time.Second
	// maxResponseBytes bounds a provider response body.
	maxResponseBytes = 1 << 20
)

// ErrConfiguration reports an invalid adapter configuration.
var ErrConfiguration = errors.New("nowpayments: invalid configuration")

// Config describes an adapter instance.
type Config struct {
	// BaseURL sets the API root, for example https://api.nowpayments.io.
	BaseURL string
	// APIKey sets the merchant key (x-api-key header).
	APIKey string
	// IPNSecret verifies the canonical JSON HMAC-SHA512 signature.
	IPNSecret []byte
	// PriceCurrency sets the fiat currency of stablecoin order prices: the
	// sandbox cannot estimate a stablecoin price currency for a stablecoin pay
	// currency (verified live 19.10.2026), so a stablecoin asset is priced at
	// par in this currency while the configured currency itself is priced
	// directly (T-CONNECTOR-28); any other asset needs a rate and is rejected.
	// Empty means "USD".
	PriceCurrency string
	// CallbackURL sets the per-invoice IPN URL (our ingress); empty leaves the
	// account-level dashboard setting in charge.
	CallbackURL string
	// Capabilities sets the network rows; Provider is forced to "nowpayments".
	Capabilities domain.Capabilities
	// Timeout sets the HTTP client timeout; <= 0 means the default.
	Timeout time.Duration
	// HTTPClient overrides the client; used by tests.
	HTTPClient *http.Client
}

// Adapter implements domain.PaymentGateway plus the optional SPI v1 interfaces.
type Adapter struct {
	baseURL       string
	apiKey        string
	ipnSecret     []byte
	priceCurrency string
	callbackURL   string
	capabilities  domain.Capabilities
	decimals      map[string]uint32
	client        *http.Client
}

var (
	_ domain.PaymentGateway  = (*Adapter)(nil)
	_ domain.LookupGateway   = (*Adapter)(nil)
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
	apiKey := strings.TrimSpace(cfg.APIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("%w: api key is required", ErrConfiguration)
	}
	if len(cfg.IPNSecret) == 0 {
		return nil, fmt.Errorf("%w: ipn secret is required", ErrConfiguration)
	}
	if len(cfg.Capabilities.Networks) == 0 {
		return nil, fmt.Errorf("%w: at least one network capability is required", ErrConfiguration)
	}
	priceCurrency, err := providerwire.ValidatePriceCurrency(cfg.PriceCurrency)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConfiguration, err)
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
			// must not forward the API key to another host.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	capabilities := cfg.Capabilities
	capabilities.Provider = "nowpayments"
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
		apiKey:        apiKey,
		ipnSecret:     cfg.IPNSecret,
		callbackURL:   strings.TrimSpace(cfg.CallbackURL),
		priceCurrency: priceCurrency,
		capabilities:  capabilities,
		decimals:      decimals,
		client:        client,
	}, nil
}

// Capabilities returns the configured network rows with the provider id.
func (a *Adapter) Capabilities() domain.Capabilities { return a.capabilities }

// Provider returns the manifest id of the adapter (the capability source port).
func (a *Adapter) Provider() string { return "nowpayments" }

// invoiceResponse describes the NOWPayments invoice body (both directions).
type invoiceResponse struct {
	ID                     string          `json:"id"`
	OrderID                string          `json:"order_id"`
	PriceAmount            json.Number     `json:"price_amount"`
	PriceCurrency          string          `json:"price_currency"`
	PayCurrency            string          `json:"pay_currency"`
	InvoiceURL             string          `json:"invoice_url"`
	CreatedAt              string          `json:"created_at"`
	ExpirationEstimateDate string          `json:"expiration_estimate_date"`
	Status                 json.RawMessage `json:"status"`
}

// paymentResponse describes the NOWPayments payment body.
type paymentResponse struct {
	PaymentID     idText      `json:"payment_id"`
	OrderID       string      `json:"order_id"`
	PaymentStatus string      `json:"payment_status"`
	PriceAmount   json.Number `json:"price_amount"`
	ActuallyPaid  json.Number `json:"actually_paid"`
	PayCurrency   string      `json:"pay_currency"`
	UpdatedAt     string      `json:"updated_at"`
}

// idText accepts a JSON number or a quoted string: the live NOW payment wire
// returns payment_id as a number while the stand profile and older payloads
// quote it (found live, KI-connector-15).
type idText string

// UnmarshalJSON decodes a number or a string into the identifier text.
func (v *idText) UnmarshalJSON(data []byte) error {
	text := strings.TrimSpace(string(data))
	if text == "null" {
		*v = ""
		return nil
	}
	if len(text) >= 2 && text[0] == '"' && text[len(text)-1] == '"' {
		var quoted string
		if err := json.Unmarshal(data, &quoted); err != nil {
			return err
		}
		*v = idText(quoted)
		return nil
	}
	*v = idText(text)
	return nil
}

// priceCurrencyFor resolves the NOWPayments price currency of an amount (fiat
// at par for stablecoins, T-CONNECTOR-28).
func (a *Adapter) priceCurrencyFor(asset string) (string, error) {
	currency, err := providerwire.FiatPriceCurrency(asset, a.priceCurrency)
	if err != nil {
		return "", fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	return currency, nil
}

// priceAmount renders the platform amount as the NOWPayments order price (fiat
// at par, half-up to cents); the conversion is overflow-checked.
func (a *Adapter) priceAmount(amount money.Money, scale uint32) (string, error) {
	price, err := providerwire.FiatPriceAmount(amount, a.priceCurrency, scale)
	if err != nil {
		return "", fmt.Errorf("%w: %v", domain.ErrRejected, err)
	}
	return price, nil
}

// CreateInvoice creates a hosted invoice in the provider.
func (a *Adapter) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	scale, err := a.ensureAmountScale(req.Amount)
	if err != nil {
		return domain.Invoice{}, err
	}
	networkKey := strings.ToLower(strings.TrimSpace(req.Network))
	row, ok := a.capabilities.Networks[networkKey]
	if !ok {
		// The network must be a configured row: an unvalidated pay currency
		// could invoice a different asset (17.6 review, fail closed).
		return domain.Invoice{}, fmt.Errorf("%w: network %q is outside the configured capabilities", domain.ErrRejected, req.Network)
	}
	if !strings.EqualFold(row.Asset, req.Amount.Asset) {
		return domain.Invoice{}, fmt.Errorf("%w: network %q carries asset %q, not %q", domain.ErrRejected, networkKey, row.Asset, req.Amount.Asset)
	}
	priceCurrency, err := a.priceCurrencyFor(req.Amount.Asset)
	if err != nil {
		return domain.Invoice{}, err
	}
	priceText, err := a.priceAmount(req.Amount, scale)
	if err != nil {
		return domain.Invoice{}, err
	}
	body := map[string]any{
		"order_id":       req.PaymentID,
		"price_amount":   json.RawMessage(priceText),
		"price_currency": priceCurrency,
		"pay_currency":   networkKey,
	}
	if a.callbackURL != "" {
		// The per-invoice IPN URL points the provider at our ingress directly,
		// so the account-level dashboard setting is not required (T-CONNECTOR-30).
		body["ipn_callback_url"] = a.callbackURL
	}
	payload, err := a.request(ctx, http.MethodPost, "/v1/invoice", body, domain.ErrRejected)
	if err != nil {
		return domain.Invoice{}, err
	}
	var decoded invoiceResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Invoice{}, fmt.Errorf("nowpayments: decode invoice response: %w", err)
	}
	if decoded.ID == "" {
		return domain.Invoice{}, errors.New("nowpayments: invoice response has no id")
	}
	if !strings.EqualFold(strings.TrimSpace(decoded.PriceCurrency), priceCurrency) {
		return domain.Invoice{}, fmt.Errorf("%w: provider invoice price currency %q does not match %q", domain.ErrRejected, truncateText([]byte(decoded.PriceCurrency)), priceCurrency)
	}
	// The provider price must echo the rendered fiat price: a drifted price
	// would mis-attribute the deposit against the platform amount.
	expected, err := money.Parse(priceText, priceCurrency, providerwire.FiatScale)
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("nowpayments: parse rendered price: %w", err)
	}
	echo, err := money.Parse(decoded.PriceAmount.String(), priceCurrency, providerwire.FiatScale)
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("%w: provider invoice price is not a valid %s amount", domain.ErrRejected, priceCurrency)
	}
	if echo.MinorUnits != expected.MinorUnits {
		return domain.Invoice{}, fmt.Errorf("%w: provider invoice price %s does not match the request", domain.ErrRejected, truncateText([]byte(decoded.PriceAmount.String())))
	}
	expiresAt := req.ExpiresAt.UTC()
	if decoded.ExpirationEstimateDate != "" {
		if providerExpiry, parseErr := time.Parse(time.RFC3339, decoded.ExpirationEstimateDate); parseErr == nil {
			providerExpiry = providerExpiry.UTC()
			// The provider estimate may exceed the platform horizon; the horizon
			// wins so the invoice never outlives the request (E-1).
			if expiresAt.IsZero() || providerExpiry.Before(expiresAt) {
				expiresAt = providerExpiry
			}
		}
	}
	return domain.Invoice{
		GatewayID:   decoded.ID,
		Provider:    "nowpayments",
		CheckoutURL: decoded.InvoiceURL,
		Network:     req.Network,
		// The platform amount is what the payment service expects; the fiat
		// price is the provider-side representation at par (T-CONNECTOR-28).
		Amount:    req.Amount,
		ExpiresAt: expiresAt,
	}, nil
}

// GetPayment reads the authoritative payment status.
func (a *Adapter) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	payload, err := a.request(ctx, http.MethodGet, "/v1/payment/"+url.PathEscape(gatewayID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	var decoded paymentResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PaymentStatus{}, fmt.Errorf("nowpayments: decode payment response: %w", err)
	}
	return a.statusOf(gatewayID, decoded)
}

// LookupByOrderID finds the payment created for the order reference; the real
// endpoint answers a list envelope, the mock profile answers a single object,
// and a bare array is tolerated (D1).
func (a *Adapter) LookupByOrderID(ctx context.Context, orderID string) (domain.PaymentStatus, error) {
	path := "/v1/payment/?order_id=" + url.QueryEscape(orderID)
	payload, err := a.request(ctx, http.MethodGet, path, nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	notFound := fmt.Errorf("%w: no payment for order %q", domain.ErrGatewayNotFound, orderID)
	trimmed := bytes.TrimSpace(payload)
	var decoded paymentResponse
	if len(trimmed) > 0 && trimmed[0] == '[' {
		decoded, err = providerwire.DecodeArrayOrObject[paymentResponse](trimmed, notFound)
		if err != nil {
			if !errors.Is(err, domain.ErrGatewayNotFound) {
				return domain.PaymentStatus{}, fmt.Errorf("nowpayments: %w", err)
			}
			return domain.PaymentStatus{}, err
		}
	} else {
		// The list envelope places the rows under "data"; the embedded struct
		// captures the bare object answer of the stand.
		var envelope struct {
			Rows []paymentResponse `json:"data"`
			paymentResponse
		}
		if err := json.Unmarshal(trimmed, &envelope); err != nil {
			return domain.PaymentStatus{}, fmt.Errorf("nowpayments: decode payment lookup: %w", err)
		}
		switch {
		case envelope.Rows != nil:
			if len(envelope.Rows) == 0 {
				return domain.PaymentStatus{}, notFound
			}
			decoded = envelope.Rows[0]
		default:
			decoded = envelope.paymentResponse
		}
	}
	return a.statusOf(string(decoded.PaymentID), decoded)
}

// statusOf maps a payment body into the canonical status; an unknown status
// word is never fabricated (E-3). A malformed updated_at leaves the event time
// zero and the normalizer falls back to the receipt time (E-5).
func (a *Adapter) statusOf(gatewayID string, decoded paymentResponse) (domain.PaymentStatus, error) {
	state, ok := paymentStates[decoded.PaymentStatus]
	if !ok {
		return domain.PaymentStatus{}, fmt.Errorf("%w: status %q", domain.ErrUnknownState, decoded.PaymentStatus)
	}
	eventTS := time.Time{}
	if decoded.UpdatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, decoded.UpdatedAt); err == nil {
			eventTS = parsed.UTC()
		}
	}
	received := money.Money{}
	if decoded.ActuallyPaid.String() != "" && decoded.PayCurrency != "" {
		asset, scale, err := a.resolvePayAsset(decoded.PayCurrency)
		if err != nil {
			return domain.PaymentStatus{}, err
		}
		parsed, err := money.ParseJSONNumber(decoded.ActuallyPaid, asset, scale)
		if err != nil {
			return domain.PaymentStatus{}, fmt.Errorf("nowpayments: parse actually paid: %w", err)
		}
		received = parsed
	}
	network := ""
	if decoded.PayCurrency != "" {
		network = strings.ToLower(strings.TrimSpace(decoded.PayCurrency))
	}
	return domain.PaymentStatus{
		GatewayID: gatewayID,
		// PaymentID is the platform payment echoed in order_id: the live IPN
		// carries the provider payment id, so the mapping lookup by the gateway
		// id misses and the fetched order_id is the resolution evidence
		// (KI-connector-15).
		PaymentID:      strings.TrimSpace(decoded.OrderID),
		Provider:       "nowpayments",
		State:          state,
		RawStatus:      decoded.PaymentStatus,
		ReceivedAmount: received,
		Network:        network,
		GatewayEventTS: eventTS,
	}, nil
}

// ExportPayments returns one page of payment records for reconciliation
// (17.13): the walk is page-based, a full page continues the walk and a row
// whose status word or amount cannot be resolved is exported fail-soft (the
// raw word is the reconciliation evidence, the page keeps going).
func (a *Adapter) ExportPayments(ctx context.Context, req domain.ExportRequest) (domain.ExportPage, error) {
	offset, page, err := domain.ExportPageParams(req.Cursor, req.Limit)
	if err != nil {
		return domain.ExportPage{}, err
	}
	query := url.Values{}
	query.Set("limit", strconv.Itoa(req.Limit))
	query.Set("page", strconv.Itoa(page))
	query.Set("dateFrom", req.From.UTC().Format(time.RFC3339))
	query.Set("dateTo", req.To.UTC().Format(time.RFC3339))
	payload, err := a.request(ctx, http.MethodGet, "/v1/payment/?"+query.Encode(), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.ExportPage{}, err
	}
	rows, err := providerwire.DecodeEnvelopeOrArray[paymentResponse](payload, "data")
	if err != nil {
		return domain.ExportPage{}, fmt.Errorf("nowpayments: decode payment export: %w", err)
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

// exportStatusOf maps a payment body fail-soft (17.13): an unknown status word
// leaves the canonical state empty and an unparsable amount stays zero, so one
// odd row cannot block the reconciliation window.
func (a *Adapter) exportStatusOf(decoded paymentResponse) domain.PaymentStatus {
	status := domain.PaymentStatus{
		GatewayID: string(decoded.PaymentID),
		PaymentID: decoded.OrderID,
		Provider:  "nowpayments",
		State:     paymentStates[decoded.PaymentStatus],
		RawStatus: decoded.PaymentStatus,
	}
	if decoded.UpdatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, decoded.UpdatedAt); err == nil {
			status.GatewayEventTS = parsed.UTC()
		}
	}
	if decoded.ActuallyPaid.String() != "" && decoded.PayCurrency != "" {
		if asset, scale, err := a.resolvePayAsset(decoded.PayCurrency); err == nil {
			if parsed, err := money.ParseJSONNumber(decoded.ActuallyPaid, asset, scale); err == nil {
				status.ReceivedAmount = parsed
			}
		}
	}
	if decoded.PayCurrency != "" {
		status.Network = strings.ToLower(strings.TrimSpace(decoded.PayCurrency))
	}
	return status
}

// paymentStates maps NOWPayments statuses to canonical states; `waiting` is a
// pre-deposit status and maps to pending (KI-connector-14).
var paymentStates = map[string]domain.State{
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

// payoutResponse describes the provisional payout body (17.11).
type payoutResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// payoutStates maps the provisional payout statuses (17.11 owns the real flow).
var payoutStates = map[string]domain.PayoutState{
	"waiting":    domain.PayoutSubmitted,
	"processing": domain.PayoutSubmitted,
	"sending":    domain.PayoutSubmitted,
	"finished":   domain.PayoutConfirmed,
	"failed":     domain.PayoutFailed,
}

// CreatePayout creates a provisional payout (17.11 owns the JWT/TOTP flow); the
// creation reports the submitted state whatever the provider word (E-7).
func (a *Adapter) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	if strings.TrimSpace(req.Address) == "" {
		return domain.Payout{}, fmt.Errorf("%w: payout destination is required", domain.ErrRejected)
	}
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Payout{}, err
	}
	body := map[string]any{
		"amount":   req.Amount.String(),
		"currency": strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		"address":  req.Address,
	}
	payload, err := a.request(ctx, http.MethodPost, "/v1/payout", body, domain.ErrRejected)
	if err != nil {
		return domain.Payout{}, err
	}
	var decoded payoutResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Payout{}, fmt.Errorf("nowpayments: decode payout response: %w", err)
	}
	if decoded.ID == "" {
		return domain.Payout{}, errors.New("nowpayments: payout response has no id")
	}
	return domain.Payout{GatewayPayoutID: decoded.ID, Provider: "nowpayments", State: domain.PayoutSubmitted}, nil
}

// GetPayout reads the provisional payout state (17.11).
func (a *Adapter) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	payload, err := a.request(ctx, http.MethodGet, "/v1/payout/"+url.PathEscape(gatewayPayoutID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PayoutStatus{}, err
	}
	var decoded payoutResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PayoutStatus{}, fmt.Errorf("nowpayments: decode payout status: %w", err)
	}
	state, ok := payoutStates[decoded.Status]
	if !ok {
		return domain.PayoutStatus{}, fmt.Errorf("%w: payout status %q", domain.ErrUnknownState, decoded.Status)
	}
	return domain.PayoutStatus{State: state}, nil
}

// resolvePayAsset resolves the configured row of a pay currency code by the
// exact network key (usdttrc20 -> the row asset USDT): an unmapped currency
// fails closed so the received evidence cannot be attributed to a foreign
// asset by a prefix coincidence (17.6 review; the catalog 17.21 replaces the
// configuration).
func (a *Adapter) resolvePayAsset(payCurrency string) (string, uint32, error) {
	row, ok := a.capabilities.Networks[strings.ToLower(strings.TrimSpace(payCurrency))]
	if !ok {
		return "", 0, fmt.Errorf("%w: pay currency %q is outside the configured capabilities", domain.ErrRejected, payCurrency)
	}
	scale, err := a.decimalsFor(row.Asset)
	if err != nil {
		return "", 0, err
	}
	return strings.ToUpper(strings.TrimSpace(row.Asset)), scale, nil
}

// decimalsFor resolves the asset decimals from the configuration index.
func (a *Adapter) decimalsFor(asset string) (uint32, error) {
	scale, ok := a.decimals[strings.ToUpper(strings.TrimSpace(asset))]
	if !ok {
		return 0, fmt.Errorf("%w: asset %q is outside the configured capabilities", domain.ErrRejected, asset)
	}
	return scale, nil
}

// ensureAmountScale rejects an amount whose scale differs from the configured
// asset scale (the shared guard of 17.7).
func (a *Adapter) ensureAmountScale(amount money.Money) (uint32, error) {
	scale, err := a.decimalsFor(amount.Asset)
	if err != nil {
		return 0, err
	}
	if amount.Decimals != scale {
		return 0, fmt.Errorf("%w: amount decimals %d do not match the asset scale %d", domain.ErrRejected, amount.Decimals, scale)
	}
	return scale, nil
}

// request performs an authenticated provider call; 404 answers the given
// class, 429/5xx and an unfollowed 3xx are temporary, other 4xx are business
// rejections.
func (a *Adapter) request(ctx context.Context, method, path string, body any, notFound error) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("nowpayments: encode request: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("nowpayments: build request: %w", err)
	}
	request.Header.Set("x-api-key", a.apiKey)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := a.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrTemporary, err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
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
		return nil, fmt.Errorf("%w: provider status %d: %s", domain.ErrRejected, response.StatusCode, truncateText(providerwire.Redact(payload, a.apiKey)))
	default:
		return payload, nil
	}
}

// truncateText bounds a provider error excerpt and replaces non-printable bytes.
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
