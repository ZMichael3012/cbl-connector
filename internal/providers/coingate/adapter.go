// Package coingate implements the SPI v1 adapter of the CoinGate v2 API:
// order creation and reads, order lookup, token-echo webhook verification and
// the provisional payout/refund shapes. Provider-specific code lives only here
// and in the manifest (plan 14.5.1).
package coingate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

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
	// defaultPriceCurrency prices stablecoin orders when the config omits the
	// currency (T-CONNECTOR-27).
	defaultPriceCurrency = "USD"
	// fiatScale is the price scale of CoinGate order prices (cents).
	fiatScale = 2
)

// stablecoinAssets are priced at par in the configured fiat price currency:
// CoinGate rejects stablecoins as price currencies (KI-connector-14 extension,
// verified live 19.10.2026), and the platform amount is already denominated in
// the stablecoin.
var stablecoinAssets = map[string]struct{}{
	"USDT": {}, "USDC": {}, "DAI": {}, "GUSD": {}, "BUSD": {}, "USDP": {},
}

// ErrConfiguration reports an invalid adapter configuration.
var ErrConfiguration = errors.New("coingate: invalid configuration")

// Config describes an adapter instance.
type Config struct {
	// BaseURL sets the API root, for example https://api.coingate.com.
	BaseURL string
	// APIToken sets the merchant API token (Authorization: Bearer <token>).
	APIToken string
	// IPNToken verifies the callback token echo and is sent as the order token
	// at creation so CoinGate echoes our secret back (KI-connector-14).
	IPNToken []byte
	// CallbackURL sets the per-order callback URL (our ingress); empty omits
	// the field and leaves the account-level callback setting in charge.
	CallbackURL string
	// PriceCurrency sets the fiat currency of CoinGate order prices: CoinGate
	// rejects stablecoins as price currencies, so a stablecoin asset is priced
	// at par in this currency while the configured currency itself is priced
	// directly (T-CONNECTOR-27); any other asset needs a rate and is rejected.
	// Empty means "USD".
	PriceCurrency string
	// Capabilities sets the network rows; Provider is forced to "coingate".
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
	ipnToken      []byte
	callbackURL   string
	priceCurrency string
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
	apiToken := strings.TrimSpace(cfg.APIToken)
	if apiToken == "" {
		return nil, fmt.Errorf("%w: api token is required", ErrConfiguration)
	}
	if len(cfg.IPNToken) == 0 {
		return nil, fmt.Errorf("%w: ipn token is required", ErrConfiguration)
	}
	if len(cfg.Capabilities.Networks) == 0 {
		return nil, fmt.Errorf("%w: at least one network capability is required", ErrConfiguration)
	}
	priceCurrency := strings.ToUpper(strings.TrimSpace(cfg.PriceCurrency))
	if priceCurrency == "" {
		priceCurrency = defaultPriceCurrency
	}
	if len(priceCurrency) != 3 {
		return nil, fmt.Errorf("%w: price currency %q must be a 3-letter code", ErrConfiguration, cfg.PriceCurrency)
	}
	for i := 0; i < len(priceCurrency); i++ {
		if priceCurrency[i] < 'A' || priceCurrency[i] > 'Z' {
			return nil, fmt.Errorf("%w: price currency %q must be 3 ASCII letters", ErrConfiguration, cfg.PriceCurrency)
		}
	}
	if _, isStable := stablecoinAssets[priceCurrency]; isStable {
		return nil, fmt.Errorf("%w: price currency %q is a stablecoin: CoinGate order prices are fiat", ErrConfiguration, priceCurrency)
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
			// must not forward the API token to another host.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	capabilities := cfg.Capabilities
	capabilities.Provider = "coingate"
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
		ipnToken:      cfg.IPNToken,
		callbackURL:   strings.TrimSpace(cfg.CallbackURL),
		priceCurrency: priceCurrency,
		capabilities:  capabilities,
		decimals:      decimals,
		client:        client,
	}, nil
}

// Capabilities returns the configured network rows with the coingate provider id.
func (a *Adapter) Capabilities() domain.Capabilities { return a.capabilities }

// Provider returns the manifest id of the adapter (the capability source port).
func (a *Adapter) Provider() string { return "coingate" }

// orderResponse describes the CoinGate order body (both directions).
type orderResponse struct {
	ID            json.Number `json:"id"`
	Status        string      `json:"status"`
	OrderID       string      `json:"order_id"`
	PriceAmount   string      `json:"price_amount"`
	PriceCurrency string      `json:"price_currency"`
	PayAmount     string      `json:"pay_amount"`
	PayCurrency   string      `json:"pay_currency"`
	PaymentAddr   string      `json:"payment_address"`
	PaymentURL    string      `json:"payment_url"`
	CreatedAt     string      `json:"created_at"`
}

// priceCurrencyFor resolves the CoinGate price currency of an amount: the
// configured fiat prices itself, a known stablecoin is priced at par in the
// configured fiat, and anything else is rejected until a real rate is wired
// (T-CONNECTOR-27).
func (a *Adapter) priceCurrencyFor(asset string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(asset))
	if upper == a.priceCurrency {
		return upper, nil
	}
	if _, ok := stablecoinAssets[upper]; ok {
		return a.priceCurrency, nil
	}
	return "", fmt.Errorf("%w: asset %q cannot be priced in %s: a rate is not wired yet", domain.ErrRejected, asset, a.priceCurrency)
}

// priceAmount renders the platform amount as the CoinGate order price: the
// amount is converted to the fiat scale (half-up) and denominated in the
// configured price currency, because CoinGate prices are fiat and a stablecoin
// order is priced at par (T-CONNECTOR-27). The conversions are
// overflow-checked: a wrapped price must never reach the provider.
func (a *Adapter) priceAmount(amount money.Money) (string, error) {
	scale, err := a.decimalsFor(amount.Asset)
	if err != nil {
		return "", err
	}
	units := amount.MinorUnits
	switch {
	case scale > fiatScale:
		divisor := pow10(scale - fiatScale)
		if units > math.MaxInt64-divisor/2 {
			return "", fmt.Errorf("%w: amount is too large to price in %s", domain.ErrRejected, a.priceCurrency)
		}
		units = (units + divisor/2) / divisor
	case scale < fiatScale:
		factor := pow10(fiatScale - scale)
		if units > math.MaxInt64/factor {
			return "", fmt.Errorf("%w: amount is too large to price in %s", domain.ErrRejected, a.priceCurrency)
		}
		units *= factor
	}
	return money.Money{MinorUnits: units, Asset: a.priceCurrency, Decimals: fiatScale}.String(), nil
}

// pow10 returns 10^n for the scale conversions of priceAmount.
func pow10(n uint32) int64 {
	value := int64(1)
	for i := uint32(0); i < n; i++ {
		value *= 10
	}
	return value
}

// CreateInvoice creates an order in the provider.
func (a *Adapter) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Invoice{}, err
	}
	priceCurrency, err := a.priceCurrencyFor(req.Amount.Asset)
	if err != nil {
		return domain.Invoice{}, err
	}
	priceText, err := a.priceAmount(req.Amount)
	if err != nil {
		return domain.Invoice{}, err
	}
	body := map[string]any{
		"order_id":       req.PaymentID,
		"price_amount":   priceText,
		"price_currency": priceCurrency,
		"pay_currency":   strings.ToUpper(strings.TrimSpace(req.Network)),
		"title":          "CBL payment",
		"description":    req.PaymentID,
		// The token is our IPN secret: CoinGate echoes it in every callback so
		// the ingress token echo verifies (KI-connector-14).
		"token": string(a.ipnToken),
	}
	if a.callbackURL != "" {
		body["callback_url"] = a.callbackURL
	}
	payload, err := a.request(ctx, http.MethodPost, "/v2/orders", body, domain.ErrRejected)
	if err != nil {
		return domain.Invoice{}, err
	}
	var decoded orderResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Invoice{}, fmt.Errorf("coingate: decode order response: %w", err)
	}
	if decoded.ID.String() == "" {
		return domain.Invoice{}, errors.New("coingate: order response has no id")
	}
	if !strings.EqualFold(strings.TrimSpace(decoded.PriceCurrency), priceCurrency) {
		return domain.Invoice{}, fmt.Errorf("%w: provider order price currency %q does not match %q", domain.ErrRejected, truncateStatus(decoded.PriceCurrency), priceCurrency)
	}
	// The provider price must echo the rendered fiat price: a drifted price
	// would mis-attribute the deposit against the platform amount.
	expected, err := money.Parse(priceText, priceCurrency, fiatScale)
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("coingate: parse rendered price: %w", err)
	}
	echo, err := money.Parse(decoded.PriceAmount, priceCurrency, fiatScale)
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("%w: provider order price is not a valid %s amount", domain.ErrRejected, priceCurrency)
	}
	if echo.MinorUnits != expected.MinorUnits {
		return domain.Invoice{}, fmt.Errorf("%w: provider order price %s does not match the request", domain.ErrRejected, truncateStatus(decoded.PriceAmount))
	}
	return domain.Invoice{
		GatewayID: decoded.ID.String(),
		Provider:  "coingate",
		Address:   decoded.PaymentAddr,
		// The hosted checkout page is the destination when no address is
		// issued at creation (KI-connector-14).
		CheckoutURL: decoded.PaymentURL,
		Network:     req.Network,
		// The platform amount is what the payment service expects; the fiat
		// price is the provider-side representation at par (T-CONNECTOR-27).
		Amount: req.Amount,
		// CoinGate orders carry no expiry: the request horizon is echoed (E-1).
		ExpiresAt: req.ExpiresAt.UTC(),
	}, nil
}

// GetPayment reads the authoritative order status.
func (a *Adapter) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	payload, err := a.request(ctx, http.MethodGet, "/v2/orders/"+url.PathEscape(gatewayID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	var decoded orderResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PaymentStatus{}, fmt.Errorf("coingate: decode order response: %w", err)
	}
	return a.statusOf(gatewayID, decoded)
}

// LookupByOrderID finds the order created for the order reference; the real
// list answers an array, the mock profile answers a single object (E-2).
func (a *Adapter) LookupByOrderID(ctx context.Context, orderID string) (domain.PaymentStatus, error) {
	path := "/v2/orders?order_id=" + url.QueryEscape(orderID)
	payload, err := a.request(ctx, http.MethodGet, path, nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	decoded, err := providerwire.DecodeArrayOrObject[orderResponse](payload,
		fmt.Errorf("%w: no order for reference %q", domain.ErrGatewayNotFound, orderID))
	if err != nil {
		if !errors.Is(err, domain.ErrGatewayNotFound) {
			return domain.PaymentStatus{}, fmt.Errorf("coingate: %w", err)
		}
		return domain.PaymentStatus{}, err
	}
	return a.statusOf(decoded.ID.String(), decoded)
}

// statusOf maps an order body into the canonical status; an unknown status
// word is never fabricated (E-3). The received amount is the order pay amount
// (the fetch path requires amount evidence, E-9); a malformed created_at leaves
// the event time zero and the normalizer falls back to the receipt time (E-8).
func (a *Adapter) statusOf(gatewayID string, decoded orderResponse) (domain.PaymentStatus, error) {
	state, ok := orderStates[decoded.Status]
	if !ok {
		return domain.PaymentStatus{}, fmt.Errorf("%w: status %q", domain.ErrUnknownState, decoded.Status)
	}
	eventTS := time.Time{}
	if decoded.CreatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, decoded.CreatedAt); err == nil {
			eventTS = parsed.UTC()
		}
	}
	received := money.Money{}
	if decoded.PayAmount != "" && decoded.PayCurrency != "" {
		// The pay currency carries the network suffix (USDT_TRC20); the asset
		// of the amount is its first segment.
		parsed, err := a.parseAmount(decoded.PayAmount, payAsset(decoded.PayCurrency))
		if err != nil {
			return domain.PaymentStatus{}, err
		}
		received = parsed
	}
	network := ""
	if decoded.PayCurrency != "" {
		network = strings.ToLower(strings.TrimSpace(decoded.PayCurrency))
	}
	return domain.PaymentStatus{
		GatewayID:      gatewayID,
		Provider:       "coingate",
		State:          state,
		RawStatus:      decoded.Status,
		ReceivedAmount: received,
		Network:        network,
		GatewayEventTS: eventTS,
	}, nil
}

// ExportPayments returns one page of order records for reconciliation (17.13):
// the walk is page-based and rows are mapped fail-soft so one odd row cannot
// block the window.
func (a *Adapter) ExportPayments(ctx context.Context, req domain.ExportRequest) (domain.ExportPage, error) {
	offset, page, err := domain.ExportPageParams(req.Cursor, req.Limit)
	if err != nil {
		return domain.ExportPage{}, err
	}
	query := url.Values{}
	query.Set("page", strconv.Itoa(page))
	query.Set("per_page", strconv.Itoa(req.Limit))
	// The real API ignores from/to and windows by created_at; the ascending
	// sort keeps the offset cursor stable while new orders arrive
	// (KI-connector-14).
	query.Set("created_at[from]", req.From.UTC().Format("2006-01-02"))
	query.Set("created_at[to]", req.To.UTC().Format("2006-01-02"))
	query.Set("sort", "created_at_asc")
	payload, err := a.request(ctx, http.MethodGet, "/v2/orders?"+query.Encode(), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.ExportPage{}, err
	}
	rows, err := providerwire.DecodeEnvelopeOrArray[orderResponse](payload, "orders")
	if err != nil {
		return domain.ExportPage{}, fmt.Errorf("coingate: decode order export: %w", err)
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

// exportStatusOf maps an order body fail-soft (17.13): an unknown status word
// leaves the canonical state empty and an unparsable amount stays zero.
func (a *Adapter) exportStatusOf(decoded orderResponse) domain.PaymentStatus {
	status := domain.PaymentStatus{
		GatewayID: decoded.ID.String(),
		PaymentID: decoded.OrderID,
		Provider:  "coingate",
		State:     orderStates[decoded.Status],
		RawStatus: decoded.Status,
	}
	if decoded.CreatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, decoded.CreatedAt); err == nil {
			status.GatewayEventTS = parsed.UTC()
		}
	}
	if decoded.PayAmount != "" && decoded.PayCurrency != "" {
		if parsed, err := a.parseAmount(decoded.PayAmount, payAsset(decoded.PayCurrency)); err == nil {
			status.ReceivedAmount = parsed
		}
	}
	if decoded.PayCurrency != "" {
		status.Network = strings.ToLower(strings.TrimSpace(decoded.PayCurrency))
	}
	return status
}

// orderStates maps CoinGate order statuses to canonical states; a pre-deposit
// status (`new`) is pending, never a fabricated detection (KI-connector-14).
var orderStates = map[string]domain.State{
	"new":        domain.StatePending,
	"pending":    domain.StateDetected,
	"confirming": domain.StateDetected,
	"paid":       domain.StateConfirmed,
	"invalid":    domain.StateFailed,
	"expired":    domain.StateExpired,
	"canceled":   domain.StateFailed,
	"refunded":   domain.StateFailed,
}

// sendRequestResponse describes the provisional send request body (17.11).
type sendRequestResponse struct {
	ID    json.Number `json:"id"`
	State string      `json:"state"`
}

// getPayoutStates maps the provisional send request states (17.11 owns the
// complete table; the stand re-validates the shapes in 17.16).
var getPayoutStates = map[string]domain.PayoutState{
	"new":       domain.PayoutSubmitted,
	"pending":   domain.PayoutSubmitted,
	"confirmed": domain.PayoutConfirmed,
	"cancelled": domain.PayoutFailed,
	"canceled":  domain.PayoutFailed,
	"failed":    domain.PayoutFailed,
}

// CreatePayout creates a provisional send request (17.11 owns the real flow);
// the creation reports the submitted state whatever the provider word (E-6).
func (a *Adapter) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	if strings.TrimSpace(req.Address) == "" {
		return domain.Payout{}, fmt.Errorf("%w: payout destination is required", domain.ErrRejected)
	}
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Payout{}, err
	}
	body := map[string]any{
		"request_id": req.PayoutID,
		"amount":     req.Amount.String(),
		"currency":   strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		"address":    req.Address,
	}
	payload, err := a.request(ctx, http.MethodPost, "/v2/send_request", body, domain.ErrRejected)
	if err != nil {
		return domain.Payout{}, err
	}
	var decoded sendRequestResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Payout{}, fmt.Errorf("coingate: decode payout response: %w", err)
	}
	if decoded.ID.String() == "" {
		return domain.Payout{}, errors.New("coingate: payout response has no id")
	}
	return domain.Payout{GatewayPayoutID: decoded.ID.String(), Provider: "coingate", State: domain.PayoutSubmitted}, nil
}

// GetPayout reads the provisional send request state (17.11).
func (a *Adapter) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	payload, err := a.request(ctx, http.MethodGet, "/v2/send_requests/"+url.PathEscape(gatewayPayoutID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PayoutStatus{}, err
	}
	var decoded sendRequestResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PayoutStatus{}, fmt.Errorf("coingate: decode payout status: %w", err)
	}
	state, ok := getPayoutStates[decoded.State]
	if !ok {
		return domain.PayoutStatus{}, fmt.Errorf("%w: payout state %q", domain.ErrUnknownState, decoded.State)
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
		return domain.Refund{}, fmt.Errorf("%w: provider order id is required", domain.ErrRejected)
	}
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Refund{}, err
	}
	body := map[string]any{
		"order_id": req.GatewayID,
		"amount":   req.Amount.String(),
		"currency": strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
	}
	// The per-command platform id is the dedupe reference: a retry must not
	// create a second refund (17.12).
	if payoutID := strings.TrimSpace(req.PayoutID); payoutID != "" {
		body["platform_id"] = payoutID
	}
	if address := strings.TrimSpace(req.Address); address != "" {
		body["refund_address"] = address
	}
	payload, err := a.request(ctx, http.MethodPost, "/v2/refunds", body, domain.ErrRejected)
	if err != nil {
		return domain.Refund{}, err
	}
	var decoded refundResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Refund{}, fmt.Errorf("coingate: decode refund response: %w", err)
	}
	if decoded.ID == "" {
		return domain.Refund{}, errors.New("coingate: refund response has no id")
	}
	return domain.Refund{GatewayRefundID: decoded.ID, Provider: "coingate", Amount: req.Amount}, nil
}

// refundStates maps provider refund statuses to canonical payout states (17.12).
var refundStates = map[string]domain.PayoutState{
	"refunded":   domain.PayoutConfirmed,
	"completed":  domain.PayoutConfirmed,
	"processing": domain.PayoutSubmitted,
	"pending":    domain.PayoutSubmitted,
	"new":        domain.PayoutSubmitted,
	"rejected":   domain.PayoutFailed,
	"failed":     domain.PayoutFailed,
	"canceled":   domain.PayoutFailed,
	"cancelled":  domain.PayoutFailed,
}

// GetRefund reads the refund state (17.12); an unknown status fails closed.
func (a *Adapter) GetRefund(ctx context.Context, gatewayRefundID string) (domain.PayoutStatus, error) {
	if strings.TrimSpace(gatewayRefundID) == "" {
		return domain.PayoutStatus{}, fmt.Errorf("%w: refund id is required", domain.ErrRejected)
	}
	payload, err := a.request(ctx, http.MethodGet, "/v2/refunds/"+url.PathEscape(gatewayRefundID), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PayoutStatus{}, err
	}
	var decoded refundResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PayoutStatus{}, fmt.Errorf("coingate: decode refund status: %w", err)
	}
	state, ok := refundStates[strings.ToLower(strings.TrimSpace(decoded.Status))]
	if !ok {
		return domain.PayoutStatus{}, fmt.Errorf("%w: refund state %q", domain.ErrUnknownState, truncateStatus(decoded.Status))
	}
	return domain.PayoutStatus{State: state}, nil
}

// truncateStatus bounds an untrusted provider status text for errors and logs.
func truncateStatus(value string) string {
	const limit = 64
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

// parseAmount parses a decimal amount at the configured asset scale.
func (a *Adapter) parseAmount(text, asset string) (money.Money, error) {
	decimals, err := a.decimalsFor(asset)
	if err != nil {
		return money.Money{}, err
	}
	parsed, err := money.Parse(text, strings.ToUpper(strings.TrimSpace(asset)), decimals)
	if err != nil {
		return money.Money{}, fmt.Errorf("coingate: parse amount: %w", err)
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

// payAsset extracts the asset code of a CoinGate pay currency
// (for example USDT_TRC20 -> USDT); a code without a network suffix is the
// asset itself.
func payAsset(payCurrency string) string {
	upper := strings.ToUpper(strings.TrimSpace(payCurrency))
	if index := strings.IndexByte(upper, '_'); index > 0 {
		return upper[:index]
	}
	return upper
}

// ensureAmountScale rejects an amount whose scale differs from the configured
// asset scale: rendering it would mis-price the provider order by a power of
// ten (17.7 review).
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
// rejections (incl. 401/403, E-5).
func (a *Adapter) request(ctx context.Context, method, path string, body any, notFound error) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("coingate: encode request: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("coingate: build request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+a.apiToken)
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
		// The excerpt may echo the request (headers or body) carrying the API
		// or IPN token: redact configured secrets before it reaches logs or
		// DLQ texts (security review 17.26).
		return nil, fmt.Errorf("%w: provider status %d: %s", domain.ErrRejected, response.StatusCode, truncateText(providerwire.Redact(payload, a.apiToken, string(a.ipnToken))))
	default:
		return payload, nil
	}
}

// truncateText bounds a provider error excerpt and replaces non-printable bytes
// so a hostile provider cannot forge log lines (17.7 review).
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
