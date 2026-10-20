package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// Harness describes an adapter under test.
type Harness struct {
	// Provider sets the manifest id; the fixtures must exist for it.
	Provider string
	// Manifest sets the provider manifest the adapter claims to implement.
	Manifest domain.ProviderManifest
	// New builds the adapter against the wire base URL.
	New func(t *testing.T, baseURL string) domain.PaymentGateway
}

// CheckResult describes the outcome of one suite check.
type CheckResult struct {
	// ID sets the check identifier (CONF-1..CONF-10).
	ID string
	// Skipped reports that the check is not applicable (a recorded skip reason is Err).
	Skipped bool
	// Err sets the failure; a non-nil Err on a skipped check is the skip reason.
	Err error
}

// Report describes the suite outcome.
type Report struct {
	// Results lists the check results in execution order.
	Results []CheckResult
}

// Err returns the failure of a check; a skip reason counts as a pass.
func (r Report) Err(id string) error {
	for _, result := range r.Results {
		if result.ID == id {
			if result.Skipped {
				return nil
			}
			return result.Err
		}
	}
	return fmt.Errorf("conformance: unknown check %s", id)
}

// Fail reports every failed check through the testing interface.
func (r Report) Fail(t *testing.T) {
	t.Helper()
	for _, result := range r.Results {
		if result.Err != nil && !result.Skipped {
			t.Errorf("%s: %v", result.ID, result.Err)
		}
	}
}

// Run executes the full suite and fails the test on any check violation; it is
// the entry point of adapter test packages.
func Run(t *testing.T, h Harness) Report {
	t.Helper()
	report := RunChecks(t, h)
	report.Fail(t)
	return report
}

// RunChecks executes the suite without failing the test; it is the entry point
// of the suite meta-tests.
func RunChecks(t *testing.T, h Harness) Report {
	t.Helper()
	fixtures, ok := Fixtures(h.Provider)
	if !ok {
		return Report{Results: []CheckResult{{ID: "CONF-0", Err: fmt.Errorf("no fixtures for provider %q", h.Provider)}}}
	}
	if h.New == nil {
		return Report{Results: []CheckResult{{ID: "CONF-0", Err: errors.New("harness has no adapter factory")}}}
	}
	if h.Manifest.ID != h.Provider {
		return Report{Results: []CheckResult{{ID: "CONF-0", Err: fmt.Errorf("manifest id %q does not match provider %q", h.Manifest.ID, h.Provider)}}}
	}

	wire := NewWire(t, fixtures)
	defer wire.Close()
	adapter := h.New(t, wire.URL())

	results := make([]CheckResult, 0, 10)
	check := func(id string, fn func() error) {
		err := fn()
		results = append(results, CheckResult{ID: id, Skipped: errors.Is(err, errSkip), Err: err})
	}
	check("CONF-1", func() error { return checkIdentity(adapter, h.Manifest) })
	check("CONF-2", func() error { return checkCapabilities(adapter) })
	check("CONF-3", func() error { return checkCreateInvoice(adapter, wire, fixtures) })
	check("CONF-4", func() error { return checkStatusMapping(adapter, wire, fixtures) })
	check("CONF-5", func() error { return checkUnknownStatus(adapter, wire, fixtures) })
	check("CONF-6", func() error {
		if h.Manifest.VerifyStrategy == domain.VerifyStrategyNone {
			return skipCheck("provider %q uses the none strategy", h.Provider)
		}
		return checkWebhook(adapter, fixtures, h.Manifest)
	})
	check("CONF-7", func() error { return checkOperations(adapter, wire, fixtures, h.Manifest) })
	check("CONF-8", func() error {
		if _, ok := adapter.(domain.LookupGateway); !ok {
			return skipCheck("the adapter does not implement LookupGateway")
		}
		if fixtures.LookupPath == "" {
			return skipCheck("provider %q has no lookup fixture", h.Provider)
		}
		return checkLookup(adapter, wire, fixtures)
	})
	check("CONF-9", func() error { return checkIdempotentCreate(adapter, wire, fixtures) })
	check("CONF-10", func() error { return checkErrorClassification(adapter, wire, fixtures) })
	return Report{Results: results}
}

// errSkip marks a check that is not applicable; the reason is carried in the error.
var errSkip = errors.New("conformance: check skipped")

// skipError carries the recorded skip reason.
type skipError struct {
	reason string
}

// Error implements the error interface.
func (e skipError) Error() string { return "skipped: " + e.reason }

// Is reports the sentinel for errors.Is.
func (e skipError) Is(target error) bool { return target == errSkip }

// skipCheck builds a skip reason.
func skipCheck(format string, args ...any) error {
	return skipError{reason: fmt.Sprintf(format, args...)}
}

// checkIdentity verifies CONF-1: the adapter reports the manifest provider.
func checkIdentity(adapter domain.PaymentGateway, manifest domain.ProviderManifest) error {
	caps := adapter.Capabilities()
	if caps.Provider != manifest.ID {
		return fmt.Errorf("capabilities provider %q does not match the manifest %q", caps.Provider, manifest.ID)
	}
	return nil
}

// checkCapabilities verifies CONF-2: the capability sanity.
func checkCapabilities(adapter domain.PaymentGateway) error {
	caps := adapter.Capabilities()
	if len(caps.Networks) == 0 {
		return errors.New("capabilities have no networks")
	}
	for name, network := range caps.Networks {
		if network.Asset == "" {
			return fmt.Errorf("network %q has an empty asset", name)
		}
		if network.MinAmount.MinorUnits <= 0 {
			return fmt.Errorf("network %q has a non-positive minimum", name)
		}
		if network.MaxAmount.MinorUnits != 0 && network.MaxAmount.MinorUnits < network.MinAmount.MinorUnits {
			return fmt.Errorf("network %q has a maximum below the minimum", name)
		}
		if network.MinAmount.Decimals > money.MaxDecimals {
			return fmt.Errorf("network %q has decimals over the limit", name)
		}
	}
	return nil
}

// checkCreateInvoice verifies CONF-3: the invoice round trip echo.
func checkCreateInvoice(adapter domain.PaymentGateway, wire *Wire, fixtures ProfileFixtures) error {
	request := fixtures.CreateInvoiceRequest("conf-3")
	invoice, err := adapter.CreateInvoice(context.Background(), request)
	if err != nil {
		return fmt.Errorf("create invoice failed: %w", err)
	}
	if invoice.GatewayID != fixtures.CreateGatewayID {
		return fmt.Errorf("gateway id %q does not match the fixture %q", invoice.GatewayID, fixtures.CreateGatewayID)
	}
	if invoice.Network != "" && invoice.Network != request.Network {
		return fmt.Errorf("network %q does not echo the request %q", invoice.Network, request.Network)
	}
	if invoice.Amount.MinorUnits != 0 && invoice.Amount.MinorUnits != request.Amount.MinorUnits {
		return fmt.Errorf("amount %d does not echo the request %d", invoice.Amount.MinorUnits, request.Amount.MinorUnits)
	}
	if !invoice.ExpiresAt.IsZero() {
		if invoice.ExpiresAt.After(request.ExpiresAt) {
			return fmt.Errorf("expiry %s exceeds the requested horizon %s", invoice.ExpiresAt, request.ExpiresAt)
		}
		if !invoice.ExpiresAt.After(time.Now().UTC().Add(-time.Minute)) {
			return fmt.Errorf("expiry %s is not in the future", invoice.ExpiresAt)
		}
	}
	recorded, err := lastRequest(wire, fixtures.CreatePath)
	if err != nil {
		return err
	}
	order, err := jsonPathValue(recorded.Body, fixtures.OrderField)
	if err != nil {
		return err
	}
	if order != request.PaymentID {
		return fmt.Errorf("order reference %q does not equal the request payment %q", order, request.PaymentID)
	}
	return nil
}

// checkStatusMapping verifies CONF-4: every documented status maps correctly.
func checkStatusMapping(adapter domain.PaymentGateway, wire *Wire, fixtures ProfileFixtures) error {
	for _, statusCase := range fixtures.Statuses {
		if err := wire.SetPaymentStatus(statusCase.Raw); err != nil {
			return err
		}
		status, err := adapter.GetPayment(context.Background(), fixtures.PaymentGatewayID)
		if err != nil {
			return fmt.Errorf("status %q: get payment failed: %w", statusCase.Raw, err)
		}
		if status.State != statusCase.Expected {
			return fmt.Errorf("status %q mapped to %q, expected %q", statusCase.Raw, status.State, statusCase.Expected)
		}
		if status.RawStatus != statusCase.Raw {
			return fmt.Errorf("status %q: raw status %q is not preserved", statusCase.Raw, status.RawStatus)
		}
		if status.GatewayID != "" && status.GatewayID != fixtures.PaymentGatewayID {
			return fmt.Errorf("status %q: gateway id %q does not match the fixture", statusCase.Raw, status.GatewayID)
		}
	}
	return nil
}

// checkUnknownStatus verifies CONF-5: no fabricated canonical state.
func checkUnknownStatus(adapter domain.PaymentGateway, wire *Wire, fixtures ProfileFixtures) error {
	wire.SetPaymentUnknown()
	status, err := adapter.GetPayment(context.Background(), fixtures.PaymentGatewayID)
	if err == nil {
		return fmt.Errorf("unknown status returned state %q without an error", status.State)
	}
	if !errors.Is(err, domain.ErrUnknownState) {
		return fmt.Errorf("unknown status error %v does not wrap ErrUnknownState", err)
	}
	if status.State != "" {
		return fmt.Errorf("unknown status fabricated canonical state %q alongside the error", status.State)
	}
	return nil
}

// checkWebhook verifies CONF-6: signature verification of the valid, tampered
// and missing deliveries; the none strategy is skipped.
func checkWebhook(adapter domain.PaymentGateway, fixtures ProfileFixtures, manifest domain.ProviderManifest) error {
	if manifest.VerifyStrategy == domain.VerifyStrategyNone {
		return nil
	}
	validHeaders := headerMap(fixtures.WebhookHeaders)
	event, err := adapter.VerifyWebhook(validHeaders, fixtures.WebhookValid)
	if err != nil {
		return fmt.Errorf("valid webhook rejected: %w", err)
	}
	if event.GatewayID != fixtures.ExpectedEvent.GatewayID {
		return fmt.Errorf("event gateway id %q does not match %q", event.GatewayID, fixtures.ExpectedEvent.GatewayID)
	}
	if event.RawStatus != fixtures.ExpectedEvent.RawStatus {
		return fmt.Errorf("event raw status %q does not match %q", event.RawStatus, fixtures.ExpectedEvent.RawStatus)
	}
	if event.Type != fixtures.ExpectedEvent.Type {
		return fmt.Errorf("event type %q does not match %q", event.Type, fixtures.ExpectedEvent.Type)
	}
	if _, err := adapter.VerifyWebhook(headerMap(fixtures.TamperedHeaders), fixtures.WebhookTampered); err == nil {
		return errors.New("tampered webhook was accepted")
	}
	if _, err := adapter.VerifyWebhook(nil, fixtures.WebhookMissing); err == nil {
		return errors.New("webhook without a signature was accepted")
	}
	return nil
}

// checkOperations verifies CONF-7: the declared operations match the SPI
// surface and the payout round trip works.
func checkOperations(adapter domain.PaymentGateway, wire *Wire, fixtures ProfileFixtures, manifest domain.ProviderManifest) error {
	if slices.Contains(manifest.Operations, domain.OperationRefund) {
		if _, ok := adapter.(domain.Refunder); !ok {
			return errors.New("the manifest declares refunds but the adapter does not implement Refunder")
		}
	}
	if !slices.Contains(manifest.Operations, domain.OperationPayout) {
		before := len(wire.Requests())
		if _, err := adapter.CreatePayout(context.Background(), fixtures.PayoutRequest("conf-7")); err == nil {
			return errors.New("the manifest has no payout operation but CreatePayout succeeded")
		} else if !errors.Is(err, domain.ErrRejected) {
			return fmt.Errorf("a payout outside the manifest must be rejected, got %v", err)
		}
		if len(wire.Requests()) != before {
			return errors.New("a payout outside the manifest must not reach the wire")
		}
		return nil
	}
	if fixtures.PayoutPath == "" {
		return errors.New("the manifest declares payouts but the fixtures have no payout route")
	}
	payout, err := adapter.CreatePayout(context.Background(), fixtures.PayoutRequest("conf-7"))
	if err != nil {
		return fmt.Errorf("create payout failed: %w", err)
	}
	expectedID := fixtures.PayoutExecutionGatewayID
	if expectedID == "" {
		expectedID = fixtures.PayoutGatewayID
	}
	if payout.GatewayPayoutID != expectedID {
		return fmt.Errorf("payout id %q does not match the fixture %q", payout.GatewayPayoutID, expectedID)
	}
	return nil
}

// checkLookup verifies CONF-8: the optional lookup interface semantics.
func checkLookup(adapter domain.PaymentGateway, wire *Wire, fixtures ProfileFixtures) error {
	lookup, ok := adapter.(domain.LookupGateway)
	if !ok || fixtures.LookupPath == "" {
		return nil
	}
	wire.SetLookupFound(false)
	if _, err := lookup.LookupByOrderID(context.Background(), "unknown-order"); err == nil {
		return errors.New("a lookup miss must return an error")
	} else if !errors.Is(err, domain.ErrGatewayNotFound) {
		return fmt.Errorf("a lookup miss error %v does not wrap ErrGatewayNotFound", err)
	}
	wire.SetLookupFound(true)
	if err := wire.SetPaymentStatus(fixtures.Statuses[0].Raw); err != nil {
		return err
	}
	status, err := lookup.LookupByOrderID(context.Background(), "order-1")
	if err != nil {
		return fmt.Errorf("lookup of a known order failed: %w", err)
	}
	if status.State != fixtures.Statuses[0].Expected {
		return fmt.Errorf("lookup state %q does not match %q", status.State, fixtures.Statuses[0].Expected)
	}
	return nil
}

// checkIdempotentCreate verifies CONF-9: repeated creates keep the order
// reference stable.
func checkIdempotentCreate(adapter domain.PaymentGateway, wire *Wire, fixtures ProfileFixtures) error {
	request := fixtures.CreateInvoiceRequest("conf-9")
	first, err := adapter.CreateInvoice(context.Background(), request)
	if err != nil {
		return fmt.Errorf("first create failed: %w", err)
	}
	second, err := adapter.CreateInvoice(context.Background(), request)
	if err != nil {
		return fmt.Errorf("second create failed: %w", err)
	}
	if first.GatewayID != second.GatewayID {
		return fmt.Errorf("repeated create returned %q then %q", first.GatewayID, second.GatewayID)
	}
	recorded := wire.Requests()
	createMethod, createPathPattern := splitFixture(fixtures.CreatePath)
	creates := make([]RecordedRequest, 0, 2)
	for _, entry := range recorded {
		if entry.Method == createMethod && matchesPattern(createPathPattern, entry.Path) {
			creates = append(creates, entry)
		}
	}
	if len(creates) < 2 {
		return fmt.Errorf("expected two recorded creates, got %d", len(creates))
	}
	for _, create := range creates[len(creates)-2:] {
		order, err := jsonPathValue(create.Body, fixtures.OrderField)
		if err != nil {
			return err
		}
		if order != request.PaymentID {
			return fmt.Errorf("a repeated create sent order reference %q instead of %q", order, request.PaymentID)
		}
	}
	return nil
}

// checkErrorClassification verifies CONF-10: temporary and rejected classes.
func checkErrorClassification(adapter domain.PaymentGateway, wire *Wire, fixtures ProfileFixtures) error {
	wire.FailNext(503, 30)
	_, err := adapter.CreateInvoice(context.Background(), fixtures.CreateInvoiceRequest("conf-10"))
	if err == nil {
		return errors.New("a 503 response must fail the create")
	}
	if !errors.Is(err, domain.ErrTemporary) {
		return fmt.Errorf("a 503 response wrapped %v instead of ErrTemporary", err)
	}
	if errors.Is(err, domain.ErrRejected) {
		return fmt.Errorf("a temporary failure must not wrap ErrRejected: %v", err)
	}
	wire.FailNext(400, 0)
	_, err = adapter.CreateInvoice(context.Background(), fixtures.CreateInvoiceRequest("conf-10"))
	if err == nil {
		return errors.New("a 400 response must fail the create")
	}
	if !errors.Is(err, domain.ErrRejected) {
		return fmt.Errorf("a 400 rejection wrapped %v instead of ErrRejected", err)
	}
	return nil
}

// CreateInvoiceRequest builds the fixture create request of a check; the
// network comes from the fixtures (the shared default otherwise).
func (f ProfileFixtures) CreateInvoiceRequest(suffix string) domain.CreateInvoiceRequest {
	network := f.CheckNetwork
	if network == "" {
		network = "usdt_trc20"
	}
	return domain.CreateInvoiceRequest{
		IdempotencyKey: "conf-" + suffix,
		PaymentID:      "order-" + suffix,
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        network,
		ExpiresAt:      time.Now().UTC().Add(15 * time.Minute),
	}
}

// PayoutRequest builds the fixture payout request of a check.
func (f ProfileFixtures) PayoutRequest(suffix string) domain.PayoutRequest {
	return domain.PayoutRequest{
		IdempotencyKey: "conf-" + suffix,
		PayoutID:       "payout-" + suffix,
		Amount:         money.MustParse("12.34", "USDT", 6),
		Network:        "usdt_trc20",
		Address:        "MOCKADDR",
	}
}

// lastRequest returns the last recorded request matching the fixture route.
func lastRequest(wire *Wire, fixture string) (RecordedRequest, error) {
	method, path := splitFixture(fixture)
	recorded := wire.Requests()
	for i := len(recorded) - 1; i >= 0; i-- {
		if recorded[i].Method == method && matchesPattern(path, recorded[i].Path) {
			return recorded[i], nil
		}
	}
	return RecordedRequest{}, fmt.Errorf("conformance: no recorded request for %q", fixture)
}

// headerMap converts a fixture header set into the SPI header form.
func headerMap(headers map[string]string) map[string][]string {
	if len(headers) == 0 {
		return nil
	}
	result := make(map[string][]string, len(headers))
	for key, value := range headers {
		result[key] = []string{value}
	}
	return result
}

// jsonPathValue resolves a dotted path over a JSON object and renders the
// scalar string form.
func jsonPathValue(body []byte, path string) (string, error) {
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return "", fmt.Errorf("conformance: decode request body: %w", err)
	}
	value, ok := lookupJSONPath(decoded, path)
	if !ok {
		return "", fmt.Errorf("conformance: request body has no %q", path)
	}
	if text, ok := value.(string); ok {
		return text, nil
	}
	rendered, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(rendered), nil
}
