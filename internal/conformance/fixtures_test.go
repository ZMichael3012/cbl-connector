package conformance_test

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/conformance"
	"github.com/cbl/cbl-connector/internal/domain"
)

// fixtureProviders lists the providers with fixture sets.
var fixtureProviders = []string{"nowpayments", "coingate", "bitpay", "btcpay", "mock"}

func TestFixturesCoverProviders(t *testing.T) {
	for _, provider := range fixtureProviders {
		t.Run(provider, func(t *testing.T) {
			fixtures, ok := conformance.Fixtures(provider)
			require.True(t, ok)
			assert.Equal(t, provider, fixtures.Provider)
			assert.Equal(t, "POST", providerMethod(fixtures.CreatePath))
			assert.NotEmpty(t, fixtures.CreateTemplate)
			assert.NotEmpty(t, fixtures.CreateGatewayID)
			assert.NotEmpty(t, fixtures.OrderField)
			assert.Contains(t, fixtures.PaymentPath, "{gateway_id}")
			assert.NotEmpty(t, fixtures.PaymentGatewayID)
			assert.NotEmpty(t, fixtures.PaymentUnknown)
			assert.NotEmpty(t, fixtures.Secret)
			assert.NotEmpty(t, fixtures.WebhookValid)
			assert.NotEmpty(t, fixtures.WebhookMissing)
			assert.NotEmpty(t, fixtures.ExpectedEvent.GatewayID)
			assert.NotEmpty(t, fixtures.Statuses)
			if provider != "mock" {
				assert.NotEmpty(t, fixtures.LookupPath, "real providers expose an order lookup")
				assert.NotEmpty(t, fixtures.PayoutPath, "real providers list the payout operation")
			}
		})
	}
	_, ok := conformance.Fixtures("nope")
	assert.False(t, ok)
}

// providerMethod returns the HTTP method of a fixture path ("POST /v1/invoice").
func providerMethod(fixturePath string) string {
	if space := strings.IndexByte(fixturePath, ' '); space >= 0 {
		return fixturePath[:space]
	}
	return ""
}

func TestFixturesStatuses(t *testing.T) {
	for _, provider := range fixtureProviders {
		t.Run(provider, func(t *testing.T) {
			fixtures, ok := conformance.Fixtures(provider)
			require.True(t, ok)
			seen := map[string]bool{}
			for _, statusCase := range fixtures.Statuses {
				require.NotEmpty(t, statusCase.Raw)
				require.NotEmpty(t, statusCase.Expected)
				assert.False(t, seen[statusCase.Raw], "raw status %q repeats", statusCase.Raw)
				seen[statusCase.Raw] = true
				payload, ok := fixtures.PaymentByStatus[statusCase.Raw]
				require.True(t, ok, "status %q has no payment fixture", statusCase.Raw)
				assert.Contains(t, string(payload), statusCase.Raw)
			}
			assert.Contains(t, string(fixtures.PaymentUnknown), "unknown")
		})
	}
}

func TestFixtureSignatureVectors(t *testing.T) {
	for _, provider := range fixtureProviders {
		t.Run(provider, func(t *testing.T) {
			fixtures, ok := conformance.Fixtures(provider)
			require.True(t, ok)
			manifest, ok := manifestOf(provider)
			require.True(t, ok)

			strategy, err := conformance.StrategyFromManifest(manifest)
			if manifest.VerifyStrategy == domain.VerifyStrategyNone {
				require.Error(t, err, "the none strategy has no verifier")
				return
			}
			require.NoError(t, err)

			headers := http.Header{}
			for key, value := range fixtures.WebhookHeaders {
				headers.Set(key, value)
			}
			require.NoError(t, strategy.Verify(fixtures.Secret, webhooksig.Request{Header: headers, Body: fixtures.WebhookValid}),
				"the valid webhook fixture must verify")

			tamperedHeaders := http.Header{}
			for key, value := range fixtures.TamperedHeaders {
				tamperedHeaders.Set(key, value)
			}
			require.Error(t, strategy.Verify(fixtures.Secret, webhooksig.Request{Header: tamperedHeaders, Body: fixtures.WebhookTampered}),
				"the tampered webhook fixture must not verify")

			if manifest.VerifyStrategy != domain.VerifyStrategyTokenEcho {
				header, value, err := conformance.SignBody(manifest, fixtures.Secret, fixtures.WebhookValid)
				require.NoError(t, err)
				assert.Equal(t, fixtures.WebhookHeaders[header], value, "fixture generation must use SignBody")
			}
		})
	}
}

func TestStrategyFromManifestUnknown(t *testing.T) {
	_, err := conformance.StrategyFromManifest(domain.ProviderManifest{
		Version:        "v1",
		ID:             "x",
		VerifyStrategy: "nonsense",
	})
	require.Error(t, err)
}

func TestWireBehaviors(t *testing.T) {
	fixtures, ok := conformance.Fixtures("nowpayments")
	require.True(t, ok)
	wire := conformance.NewWire(t, fixtures)
	defer wire.Close()

	createURL := wire.URL() + httpPath(fixtures.CreatePath)
	//nolint:gosec // G107: the URL points to the in-process wire mock of the test.
	response, err := http.Post(createURL, "application/json", bytes.NewReader([]byte(`{"order_id":"order-echo-1"}`)))
	require.NoError(t, err)
	payload, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	_ = response.Body.Close()
	assert.Equal(t, fixtures.CreateStatus, response.StatusCode)
	assert.Contains(t, string(payload), "order-echo-1", "the template must echo the request order id")
	assert.Contains(t, string(payload), fixtures.CreateGatewayID)

	requests := wire.Requests()
	require.NotEmpty(t, requests)
	last := requests[len(requests)-1]
	assert.Equal(t, http.MethodPost, last.Method)
	assert.Contains(t, string(last.Body), "order-echo-1")

	wire.FailNext(429, 30)
	limited, err := http.Get(wire.URL() + httpPath(strings.Replace(fixtures.PaymentPath, "{gateway_id}", "x", 1)))
	require.NoError(t, err)
	_ = limited.Body.Close()
	assert.Equal(t, http.StatusTooManyRequests, limited.StatusCode)
	assert.Equal(t, "30", limited.Header.Get("Retry-After"))

	require.NoError(t, wire.SetPaymentStatus("finished"))
	payment, err := http.Get(wire.URL() + httpPath(strings.Replace(fixtures.PaymentPath, "{gateway_id}", "x", 1)))
	require.NoError(t, err)
	paymentBody, err := io.ReadAll(payment.Body)
	require.NoError(t, err)
	_ = payment.Body.Close()
	assert.Equal(t, http.StatusOK, payment.StatusCode)
	assert.Contains(t, string(paymentBody), "finished")

	wire.SetPaymentUnknown()
	unknown, err := http.Get(wire.URL() + httpPath(strings.Replace(fixtures.PaymentPath, "{gateway_id}", "x", 1)))
	require.NoError(t, err)
	unknownBody, err := io.ReadAll(unknown.Body)
	require.NoError(t, err)
	_ = unknown.Body.Close()
	assert.Contains(t, string(unknownBody), "unknown")

	wire.SetLookupFound(false)
	miss, err := http.Get(wire.URL() + httpPath(strings.Replace(fixtures.LookupPath, "{order}", "missing", 1)))
	require.NoError(t, err)
	_ = miss.Body.Close()
	assert.Equal(t, http.StatusNotFound, miss.StatusCode)

	wire.SetLookupFound(true)
	found, err := http.Get(wire.URL() + httpPath(strings.Replace(fixtures.LookupPath, "{order}", "known", 1)))
	require.NoError(t, err)
	_ = found.Body.Close()
	assert.Equal(t, http.StatusOK, found.StatusCode)

	require.Error(t, wire.SetPaymentStatus("no-such-status"))
}

func TestWireTemplateUnresolvedPlaceholder(t *testing.T) {
	fixtures, ok := conformance.Fixtures("nowpayments")
	require.True(t, ok)
	fixtures.CreateTemplate = []byte(`{"id":"np-gw-1","order_id":{{request.no_such_field}}}`)
	wire := conformance.NewWire(t, fixtures)
	defer wire.Close()

	response, err := http.Post(wire.URL()+httpPath(fixtures.CreatePath), "application/json", bytes.NewReader([]byte(`{"order_id":"x"}`)))
	require.NoError(t, err)
	_ = response.Body.Close()
	assert.Equal(t, http.StatusBadRequest, response.StatusCode, "an unresolved placeholder must answer 400 (fixture bug, spec E-3)")
}

func TestRunChecksHarnessValidation(t *testing.T) {
	report := conformance.RunChecks(t, conformance.Harness{Provider: "nope"})
	require.Error(t, report.Err("CONF-0"))

	report = conformance.RunChecks(t, conformance.Harness{Provider: "nowpayments", Manifest: domain.ProviderManifest{ID: "nowpayments"}})
	require.Error(t, report.Err("CONF-0"))

	report = conformance.RunChecks(t, conformance.Harness{
		Provider: "nowpayments",
		Manifest: domain.ProviderManifest{ID: "coingate"},
		New:      func(_ *testing.T, _ string) domain.PaymentGateway { return nil },
	})
	require.Error(t, report.Err("CONF-0"))

	require.Error(t, conformance.Report{}.Err("CONF-42"))
}

func TestWireUnknownRouteAndFixtureGuard(t *testing.T) {
	fixtures, ok := conformance.Fixtures("bitpay")
	require.True(t, ok)
	wire := conformance.NewWire(t, fixtures)
	defer wire.Close()

	response, err := http.Get(wire.URL() + "/no/such/route")
	require.NoError(t, err)
	_ = response.Body.Close()
	assert.Equal(t, http.StatusNotFound, response.StatusCode)

	require.Error(t, wire.SetPaymentStatus("no-such-status"))
}
