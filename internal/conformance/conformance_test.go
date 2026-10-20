package conformance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ZMichael3012/cbl-kit/money"
	"github.com/ZMichael3012/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/conformance"
	"github.com/cbl/cbl-connector/internal/domain"
)

// mutators toggles the violating behaviors of the meta-tests.
type mutators struct {
	emptyCreateID       bool
	unknownAsDetected   bool
	acceptTampered      bool
	noLookupError       bool
	mutateOrder         bool
	temporaryAsRejected bool
}

// exampleAdapter is a minimal SPI v1 implementation over the NOWPayments
// fixtures; it is the reference the meta-tests run the suite against.
type exampleAdapter struct {
	base    string
	client  *http.Client
	fix     conformance.ProfileFixtures
	mutants mutators
	created atomic.Int64
}

// exampleCapabilities describe the fixture network.
var exampleCapabilities = domain.Capabilities{
	Provider: "nowpayments",
	Networks: map[string]domain.NetworkCapability{
		"usdttrc20": {
			Asset:             "USDT",
			MinAmount:         money.MustParse("1", "USDT", 6),
			MaxAmount:         money.MustParse("10000", "USDT", 6),
			ConfirmationDepth: 2,
		},
	},
}

// Capabilities returns the fixture capabilities.
func (a *exampleAdapter) Capabilities() domain.Capabilities { return exampleCapabilities }

// CreateInvoice creates an invoice on the wire.
func (a *exampleAdapter) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	order := req.PaymentID
	if a.mutants.mutateOrder && a.created.Add(1) > 1 {
		order = "other-order"
	}
	body := fmt.Sprintf(`{"order_id":%q,"price_amount":%s,"price_currency":%q,"pay_currency":%q}`,
		order, req.Amount.String(), req.Amount.Asset, req.Network)
	status, payload, err := a.do(ctx, http.MethodPost, a.fix.CreatePath, []byte(body))
	if err != nil {
		return domain.Invoice{}, err
	}
	if status >= 500 {
		if a.mutants.temporaryAsRejected {
			return domain.Invoice{}, fmt.Errorf("%w: status %d", domain.ErrRejected, status)
		}
		return domain.Invoice{}, fmt.Errorf("%w: status %d", domain.ErrTemporary, status)
	}
	if status >= 400 {
		return domain.Invoice{}, fmt.Errorf("%w: status %d", domain.ErrRejected, status)
	}
	var decoded struct {
		ID         string `json:"id"`
		InvoiceURL string `json:"invoice_url"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Invoice{}, fmt.Errorf("example adapter: decode create: %w", err)
	}
	if a.mutants.emptyCreateID {
		decoded.ID = ""
	}
	return domain.Invoice{
		GatewayID:   decoded.ID,
		Provider:    "nowpayments",
		CheckoutURL: decoded.InvoiceURL,
		Network:     req.Network,
		Amount:      req.Amount,
		ExpiresAt:   req.ExpiresAt,
	}, nil
}

// GetPayment reads the payment state of the wire.
func (a *exampleAdapter) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	path := strings.Replace(a.fix.PaymentPath, "{gateway_id}", gatewayID, 1)
	status, payload, err := a.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	if status == http.StatusNotFound {
		return domain.PaymentStatus{}, fmt.Errorf("%w: %s", domain.ErrGatewayNotFound, gatewayID)
	}
	if status >= 400 {
		return domain.PaymentStatus{}, fmt.Errorf("%w: status %d", domain.ErrRejected, status)
	}
	return a.parsePayment(gatewayID, payload)
}

// parsePayment maps the payment payload into the canonical status.
func (a *exampleAdapter) parsePayment(gatewayID string, payload []byte) (domain.PaymentStatus, error) {
	var decoded struct {
		PaymentID     string `json:"payment_id"`
		OrderID       string `json:"order_id"`
		PaymentStatus string `json:"payment_status"`
		UpdatedAt     string `json:"updated_at"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.PaymentStatus{}, fmt.Errorf("example adapter: decode payment: %w", err)
	}
	state, ok := a.stateOf(decoded.PaymentStatus)
	if !ok {
		if a.mutants.unknownAsDetected {
			state = domain.StateDetected
		} else {
			return domain.PaymentStatus{}, fmt.Errorf("%w: %q", domain.ErrUnknownState, decoded.PaymentStatus)
		}
	}
	occurred, _ := time.Parse(time.RFC3339, decoded.UpdatedAt)
	return domain.PaymentStatus{
		GatewayID:      gatewayID,
		PaymentID:      decoded.OrderID,
		Provider:       "nowpayments",
		State:          state,
		RawStatus:      decoded.PaymentStatus,
		GatewayEventTS: occurred,
	}, nil
}

// stateOf returns the canonical state of a raw status.
func (a *exampleAdapter) stateOf(raw string) (domain.State, bool) {
	for _, tc := range a.fix.Statuses {
		if tc.Raw == raw {
			return tc.Expected, true
		}
	}
	return "", false
}

// VerifyWebhook verifies the profile signature and parses the event.
func (a *exampleAdapter) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	header := http.Header{}
	for key, values := range headers {
		for _, value := range values {
			header.Add(key, value)
		}
	}
	strategy := webhooksig.CanonicalJSONHMAC{Header: "x-nowpayments-sig", Algorithm: webhooksig.SHA512, Encoding: webhooksig.Hex}
	if err := strategy.Verify(a.fix.Secret, webhooksig.Request{Header: header, Body: rawBody}); err != nil {
		if a.mutants.acceptTampered && header.Get("x-nowpayments-sig") != "" {
			return a.parseEvent(rawBody)
		}
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrUnsignedWebhook, err)
	}
	return a.parseEvent(rawBody)
}

// parseEvent parses the event payload of the wire.
func (a *exampleAdapter) parseEvent(rawBody []byte) (domain.CanonicalEvent, error) {
	var decoded struct {
		PaymentID     string `json:"payment_id"`
		OrderID       string `json:"order_id"`
		PaymentStatus string `json:"payment_status"`
		UpdatedAt     string `json:"updated_at"`
	}
	if err := json.Unmarshal(rawBody, &decoded); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("example adapter: decode event: %w", err)
	}
	state, ok := a.stateOf(decoded.PaymentStatus)
	if !ok {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %q", domain.ErrUnknownState, decoded.PaymentStatus)
	}
	occurred, _ := time.Parse(time.RFC3339, decoded.UpdatedAt)
	return domain.CanonicalEvent{
		Type:       state,
		PaymentID:  decoded.OrderID,
		GatewayID:  decoded.PaymentID,
		Provider:   "nowpayments",
		RawStatus:  decoded.PaymentStatus,
		OccurredAt: occurred,
	}, nil
}

// CreatePayout creates a payout on the wire.
func (a *exampleAdapter) CreatePayout(ctx context.Context, _ domain.PayoutRequest) (domain.Payout, error) {
	status, payload, err := a.do(ctx, http.MethodPost, a.fix.PayoutPath, []byte(`{}`))
	if err != nil {
		return domain.Payout{}, err
	}
	if status >= 500 {
		return domain.Payout{}, fmt.Errorf("%w: status %d", domain.ErrTemporary, status)
	}
	if status >= 400 {
		return domain.Payout{}, fmt.Errorf("%w: status %d", domain.ErrRejected, status)
	}
	var decoded struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return domain.Payout{}, fmt.Errorf("example adapter: decode payout: %w", err)
	}
	return domain.Payout{GatewayPayoutID: decoded.ID, Provider: "nowpayments", State: domain.PayoutSubmitted}, nil
}

// GetPayout reads a payout state (not exercised by the CONF checks yet).
func (a *exampleAdapter) GetPayout(context.Context, string) (domain.PayoutStatus, error) {
	return domain.PayoutStatus{State: domain.PayoutSubmitted}, nil
}

// LookupByOrderID finds the invoice of an order.
func (a *exampleAdapter) LookupByOrderID(ctx context.Context, orderID string) (domain.PaymentStatus, error) {
	path := strings.Replace(a.fix.LookupPath, "{order}", orderID, 1)
	status, payload, err := a.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return domain.PaymentStatus{}, err
	}
	if status == http.StatusNotFound {
		if a.mutants.noLookupError {
			return domain.PaymentStatus{State: domain.StateDetected}, nil
		}
		return domain.PaymentStatus{}, fmt.Errorf("%w: %s", domain.ErrGatewayNotFound, orderID)
	}
	return a.parsePayment(a.fix.PaymentGatewayID, payload)
}

// do performs an HTTP request against the wire.
func (a *exampleAdapter) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = strings.NewReader(string(body))
	}
	request, err := http.NewRequestWithContext(ctx, method, a.base+httpPath(path), reader)
	if err != nil {
		return 0, nil, err
	}
	response, err := a.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, payload, nil
}

// httpPath strips the method prefix of a fixture path.
func httpPath(fixturePath string) string {
	if space := strings.IndexByte(fixturePath, ' '); space >= 0 {
		return fixturePath[space+1:]
	}
	return fixturePath
}

// newHarness builds the meta-test harness of a provider.
func newHarness(t *testing.T, provider string, m mutators) conformance.Harness {
	t.Helper()
	fixtures, ok := conformance.Fixtures(provider)
	require.True(t, ok)
	manifest, ok := manifestOf(provider)
	require.True(t, ok)
	return conformance.Harness{
		Provider: provider,
		Manifest: manifest,
		New: func(_ *testing.T, baseURL string) domain.PaymentGateway {
			return &exampleAdapter{base: baseURL, client: http.DefaultClient, fix: fixtures, mutants: m}
		},
	}
}

// manifestOf returns the built-in manifest of a provider.
func manifestOf(provider string) (domain.ProviderManifest, bool) {
	for _, manifest := range domain.BuiltinManifests() {
		if manifest.ID == provider {
			return manifest, true
		}
	}
	return domain.ProviderManifest{}, false
}

func TestRunChecksCompliantAdapterPasses(t *testing.T) {
	h := newHarness(t, "nowpayments", mutators{})
	conformance.Run(t, h)
}

func TestRunChecksMutants(t *testing.T) {
	tests := []struct {
		name  string
		m     mutators
		check string
	}{
		{"empty create id", mutators{emptyCreateID: true}, "CONF-3"},
		{"unknown status fabricated", mutators{unknownAsDetected: true}, "CONF-5"},
		{"tampered signature accepted", mutators{acceptTampered: true}, "CONF-6"},
		{"lookup hides a miss", mutators{noLookupError: true}, "CONF-8"},
		{"mutated order reference", mutators{mutateOrder: true}, "CONF-9"},
		{"503 as rejection", mutators{temporaryAsRejected: true}, "CONF-10"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, "nowpayments", tc.m)
			report := conformance.RunChecks(t, h)
			require.Error(t, report.Err(tc.check), "check %s must fail for the mutant", tc.check)
			failed := 0
			for _, result := range report.Results {
				if result.Err != nil && !result.Skipped {
					failed++
				}
			}
			require.Equal(t, 1, failed, "exactly the named check must fail")
		})
	}
}

func TestRunChecksMissingRefunder(t *testing.T) {
	h := newHarness(t, "nowpayments", mutators{})
	h.Manifest.Operations = append(h.Manifest.Operations, domain.OperationRefund)
	report := conformance.RunChecks(t, h)
	require.Error(t, report.Err("CONF-7"), "a manifest with refunds requires a Refunder")
}
