// Package btcpay implements the SPI v1 adapter of BTCPay Server (Greenfield
// API): invoice creation and reads, order lookup, webhook verification with
// BTCPay-Sig, and payouts/refunds through pull payments. Provider-specific code
// lives only here and in the manifest (plan 14.5.1).
package btcpay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
)

// ErrConfiguration reports an invalid adapter configuration.
var ErrConfiguration = errors.New("btcpay: invalid configuration")

// Config describes an adapter instance.
type Config struct {
	// BaseURL sets the Greenfield root, for example http://btcpay:49392.
	BaseURL string
	// StoreID sets the BTCPay store.
	StoreID string
	// APIToken sets the API key (Authorization: token <key>).
	APIToken string
	// WebhookSecret verifies the BTCPay-Sig webhook header.
	WebhookSecret []byte
	// Capabilities sets the network rows; Provider is forced to "btcpay".
	Capabilities domain.Capabilities
	// Timeout sets the HTTP client timeout; <= 0 means the default.
	Timeout time.Duration
	// HTTPClient overrides the client; used by tests.
	HTTPClient *http.Client
}

// Adapter implements domain.PaymentGateway plus the optional SPI v1 interfaces.
type Adapter struct {
	baseURL       string
	storeID       string
	token         string
	webhookSecret []byte
	capabilities  domain.Capabilities
	decimals      map[string]uint32
	client        *http.Client
}

var (
	_ domain.PaymentGateway = (*Adapter)(nil)
	_ domain.LookupGateway  = (*Adapter)(nil)
	_ domain.Refunder       = (*Adapter)(nil)
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
		// An embedded userinfo would leak credentials into logs and errors.
		return nil, fmt.Errorf("%w: base url must not carry userinfo", ErrConfiguration)
	}
	storeID := strings.TrimSpace(cfg.StoreID)
	if storeID == "" {
		return nil, fmt.Errorf("%w: store id is required", ErrConfiguration)
	}
	token := strings.TrimSpace(cfg.APIToken)
	if token == "" {
		return nil, fmt.Errorf("%w: api token is required", ErrConfiguration)
	}
	if len(cfg.WebhookSecret) == 0 {
		return nil, fmt.Errorf("%w: webhook secret is required", ErrConfiguration)
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
			// must not forward the API token to another host.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	capabilities := cfg.Capabilities
	capabilities.Provider = "btcpay"
	// The asset scale index is built once: a nondeterministic row pick for the
	// same asset would parse amounts at different scales (17.9 review).
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
		storeID:       storeID,
		token:         token,
		webhookSecret: cfg.WebhookSecret,
		capabilities:  capabilities,
		decimals:      decimals,
		client:        client,
	}, nil
}

// Capabilities returns the configured network rows with the btcpay provider id.
func (a *Adapter) Capabilities() domain.Capabilities { return a.capabilities }

// Provider returns the manifest id of the adapter (the capability source port).
func (a *Adapter) Provider() string { return "btcpay" }

// invoiceResponse describes the Greenfield invoice body (both directions).
type invoiceResponse struct {
	ID             string            `json:"id"`
	StoreID        string            `json:"storeId"`
	Amount         string            `json:"amount"`
	Currency       string            `json:"currency"`
	Status         string            `json:"status"`
	CheckoutLink   string            `json:"checkoutLink"`
	CreatedTime    int64             `json:"createdTime"`
	ExpirationTime int64             `json:"expirationTime"`
	AmountPaid     string            `json:"amountPaid"`
	Metadata       map[string]string `json:"metadata"`
}

// pullPaymentResponse describes the Greenfield pull payment body (only the id
// is consumed; the stand returns the full object).
type pullPaymentResponse struct {
	ID string `json:"id"`
}

// payoutResponse describes the Greenfield payout body.
type payoutResponse struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	PullPaymentID string `json:"pullPaymentId"`
}

// CreateInvoice creates a hosted invoice in the store.
func (a *Adapter) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	if _, err := a.ensureAmountScale(req.Amount); err != nil {
		return domain.Invoice{}, err
	}
	body := map[string]any{
		"amount":   req.Amount.String(),
		"currency": strings.ToUpper(strings.TrimSpace(req.Amount.Asset)),
		"metadata": map[string]string{
			"orderId":      req.PaymentID,
			"orderNetwork": req.Network,
		},
	}
	payload, err := a.request(ctx, http.MethodPost, a.storePath("/invoices"), body, domain.ErrRejected)
	if err != nil {
		return domain.Invoice{}, err
	}
	var decoded invoiceResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Invoice{}, fmt.Errorf("btcpay: decode invoice response: %w", err)
	}
	if decoded.ID == "" {
		return domain.Invoice{}, errors.New("btcpay: invoice response has no id")
	}
	decimals, err := a.decimalsFor(decoded.Currency)
	if err != nil {
		return domain.Invoice{}, err
	}
	amount, err := money.Parse(decoded.Amount, strings.ToUpper(decoded.Currency), decimals)
	if err != nil {
		return domain.Invoice{}, fmt.Errorf("btcpay: parse invoice amount: %w", err)
	}
	expiresAt := req.ExpiresAt.UTC()
	if decoded.ExpirationTime > 0 {
		providerExpiry := time.Unix(decoded.ExpirationTime, 0).UTC()
		// The store may set its own expiry; the platform horizon wins so the
		// invoice never outlives the requested window (spec E-8).
		if expiresAt.IsZero() || providerExpiry.Before(expiresAt) {
			expiresAt = providerExpiry
		}
	}
	return domain.Invoice{
		GatewayID:   decoded.ID,
		Provider:    "btcpay",
		CheckoutURL: decoded.CheckoutLink,
		Network:     req.Network,
		Amount:      amount,
		ExpiresAt:   expiresAt,
	}, nil
}

// GetPayment reads the authoritative invoice status.
func (a *Adapter) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	payload, err := a.request(ctx, http.MethodGet, a.storePath("/invoices/"+url.PathEscape(gatewayID)), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	var decoded invoiceResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PaymentStatus{}, fmt.Errorf("btcpay: decode payment response: %w", err)
	}
	return a.statusOf(gatewayID, decoded)
}

// LookupByOrderID finds the invoice created for the order reference; the real
// Greenfield list answers an array, the mock profile answers a single object
// (spec D4), so both shapes are accepted through the shared wire decoder.
func (a *Adapter) LookupByOrderID(ctx context.Context, orderID string) (domain.PaymentStatus, error) {
	path := a.storePath("/invoices") + "?orderId=" + url.QueryEscape(orderID)
	payload, err := a.request(ctx, http.MethodGet, path, nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	decoded, err := providerwire.DecodeArrayOrObject[invoiceResponse](payload,
		fmt.Errorf("%w: no invoice for order %q", domain.ErrGatewayNotFound, orderID))
	if err != nil {
		if !errors.Is(err, domain.ErrGatewayNotFound) {
			return domain.PaymentStatus{}, fmt.Errorf("btcpay: %w", err)
		}
		return domain.PaymentStatus{}, err
	}
	return a.statusOf(decoded.ID, decoded)
}

// statusOf maps an invoice body into the canonical status; an unknown status
// word is never fabricated (CONF-5).
func (a *Adapter) statusOf(gatewayID string, decoded invoiceResponse) (domain.PaymentStatus, error) {
	state, ok := invoiceStates[decoded.Status]
	if !ok {
		return domain.PaymentStatus{}, fmt.Errorf("%w: status %q", domain.ErrUnknownState, decoded.Status)
	}
	received := money.Money{}
	if decoded.AmountPaid != "" {
		decimals, err := a.decimalsFor(decoded.Currency)
		if err != nil {
			return domain.PaymentStatus{}, err
		}
		paid, err := money.Parse(decoded.AmountPaid, strings.ToUpper(decoded.Currency), decimals)
		if err != nil {
			return domain.PaymentStatus{}, fmt.Errorf("btcpay: parse amount paid: %w", err)
		}
		received = paid
	}
	eventTS := time.Time{}
	if decoded.CreatedTime > 0 {
		eventTS = time.Unix(decoded.CreatedTime, 0).UTC()
	}
	return domain.PaymentStatus{
		GatewayID:      gatewayID,
		Provider:       "btcpay",
		State:          state,
		RawStatus:      decoded.Status,
		ReceivedAmount: received,
		GatewayEventTS: eventTS,
	}, nil
}

// invoiceStates maps Greenfield invoice statuses to canonical states.
var invoiceStates = map[string]domain.State{
	"New":        domain.StateDetected,
	"Processing": domain.StateDetected,
	"Settled":    domain.StateConfirmed,
	"Expired":    domain.StateExpired,
	"Invalid":    domain.StateFailed,
}

// payoutStates maps Greenfield payout states to canonical payout states.
var payoutStates = map[string]domain.PayoutState{
	"AwaitingApproval": domain.PayoutSubmitted,
	"AwaitingPayment":  domain.PayoutSubmitted,
	"InProgress":       domain.PayoutSubmitted,
	"Completed":        domain.PayoutConfirmed,
	"Cancelled":        domain.PayoutFailed,
}

// CreatePayout pays the recipient through the two-step pull payment flow of the
// provider (verified against the regtest stand in 17.9): the pull payment holds
// the funds and lists the methods, the payout carries the destination. The
// created payout usually awaits operator approval (17.11) and is reported as
// submitted whatever its state word (spec E-5).
func (a *Adapter) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	if strings.TrimSpace(req.Address) == "" {
		return domain.Payout{}, fmt.Errorf("%w: payout destination is required", domain.ErrRejected)
	}
	payoutID, err := a.createPayout(ctx, req.PayoutID, req.Amount, req.Address)
	if err != nil {
		return domain.Payout{}, err
	}
	return domain.Payout{GatewayPayoutID: payoutID, Provider: "btcpay", State: domain.PayoutSubmitted}, nil
}

// GetPayout reads the payout state through the store payout endpoint.
func (a *Adapter) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	payload, err := a.request(ctx, http.MethodGet, a.storePath("/payouts/"+url.PathEscape(gatewayPayoutID)), nil, domain.ErrGatewayNotFound)
	if err != nil {
		return domain.PayoutStatus{}, err
	}
	var decoded payoutResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PayoutStatus{}, fmt.Errorf("btcpay: decode payout status: %w", err)
	}
	state, ok := payoutStates[decoded.State]
	if !ok {
		return domain.PayoutStatus{}, fmt.Errorf("%w: payout state %q", domain.ErrUnknownState, decoded.State)
	}
	return domain.PayoutStatus{State: state}, nil
}

// CreateRefund creates a pull payment and a payout for the refund destination;
// the refund orchestration is task 17.12.
func (a *Adapter) CreateRefund(ctx context.Context, req domain.RefundRequest) (domain.Refund, error) {
	if strings.TrimSpace(req.RefundInvoiceID) == "" {
		return domain.Refund{}, fmt.Errorf("%w: refund reference is required", domain.ErrRejected)
	}
	if strings.TrimSpace(req.Address) == "" {
		return domain.Refund{}, fmt.Errorf("%w: refund destination is required", domain.ErrRejected)
	}
	payoutID, err := a.createPayout(ctx, "refund-"+req.RefundInvoiceID, req.Amount, req.Address)
	if err != nil {
		return domain.Refund{}, err
	}
	return domain.Refund{GatewayRefundID: payoutID, Provider: "btcpay", Amount: req.Amount}, nil
}

// createPayout performs the two provider calls and returns the payout id.
func (a *Adapter) createPayout(ctx context.Context, name string, amount money.Money, destination string) (string, error) {
	if _, err := a.ensureAmountScale(amount); err != nil {
		return "", err
	}
	pullBody := map[string]any{
		"name":     name,
		"amount":   amount.String(),
		"currency": strings.ToUpper(strings.TrimSpace(amount.Asset)),
		"payoutMethods": []string{
			strings.ToUpper(strings.TrimSpace(amount.Asset)),
		},
	}
	payload, err := a.request(ctx, http.MethodPost, a.storePath("/pull-payments"), pullBody, domain.ErrRejected)
	if err != nil {
		return "", err
	}
	var pullPayment pullPaymentResponse
	if err := json.Unmarshal(payload, &pullPayment); err != nil {
		return "", fmt.Errorf("btcpay: decode pull payment response: %w", err)
	}
	if pullPayment.ID == "" {
		return "", errors.New("btcpay: pull payment response has no id")
	}
	payoutBody := map[string]any{
		"destination":    destination,
		"amount":         amount.String(),
		"payoutMethodId": strings.ToUpper(strings.TrimSpace(amount.Asset)) + "-CHAIN",
	}
	payload, err = a.request(ctx, http.MethodPost, "/api/v1/pull-payments/"+url.PathEscape(pullPayment.ID)+"/payouts", payoutBody, domain.ErrRejected)
	if err != nil {
		return "", err
	}
	var payout payoutResponse
	if err := json.Unmarshal(payload, &payout); err != nil {
		return "", fmt.Errorf("btcpay: decode payout response: %w", err)
	}
	if payout.ID == "" {
		return "", errors.New("btcpay: payout response has no id")
	}
	return payout.ID, nil
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
// asset scale: rendering it would mis-price the provider invoice by a power of
// ten (17.7 review, mirror in both adapters).
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

// storePath prefixes a store-scoped Greenfield path.
func (a *Adapter) storePath(suffix string) string {
	return "/api/v1/stores/" + url.PathEscape(a.storeID) + suffix
}

// request performs an authenticated provider call; 404 answers the given
// class (a read miss is ErrGatewayNotFound, a write miss is a rejection),
// 429/5xx are temporary and other 4xx are business rejections.
func (a *Adapter) request(ctx context.Context, method, path string, body any, notFound error) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("btcpay: encode request: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("btcpay: build request: %w", err)
	}
	request.Header.Set("Authorization", "token "+a.token)
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
		return nil, fmt.Errorf("%w: provider status %d: %s", domain.ErrRejected, response.StatusCode, truncateText(payload))
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
