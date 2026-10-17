// Package conformance internal tests cover the individual checks against
// focused stub adapters, beyond the meta-tests of the external package.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/webhooksig"

	"github.com/cbl/cbl-connector/internal/domain"
)

// tokenEchoAdapter implements only VerifyWebhook with the token echo strategy;
// other methods are placeholders for the focused check tests.
type tokenEchoAdapter struct {
	fixtures ProfileFixtures
}

// CreateInvoice is a placeholder.
func (a *tokenEchoAdapter) CreateInvoice(context.Context, domain.CreateInvoiceRequest) (domain.Invoice, error) {
	return domain.Invoice{}, nil
}

// GetPayment is a placeholder.
func (a *tokenEchoAdapter) GetPayment(context.Context, string) (domain.PaymentStatus, error) {
	return domain.PaymentStatus{}, nil
}

// VerifyWebhook verifies the token echo of the fixture set.
func (a *tokenEchoAdapter) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	header := http.Header{}
	for key, values := range headers {
		for _, value := range values {
			header.Add(key, value)
		}
	}
	strategy := webhooksig.TokenEcho{Field: "token"}
	if err := strategy.Verify(a.fixtures.Secret, webhooksig.Request{Header: header, Body: rawBody}); err != nil {
		return domain.CanonicalEvent{}, fmt.Errorf("%w: %v", domain.ErrUnsignedWebhook, err)
	}
	return domain.CanonicalEvent{
		Type:      domain.StateConfirmed,
		GatewayID: a.fixtures.ExpectedEvent.GatewayID,
		RawStatus: a.fixtures.ExpectedEvent.RawStatus,
	}, nil
}

// CreatePayout is a placeholder.
func (a *tokenEchoAdapter) CreatePayout(context.Context, domain.PayoutRequest) (domain.Payout, error) {
	return domain.Payout{}, nil
}

// GetPayout is a placeholder.
func (a *tokenEchoAdapter) GetPayout(context.Context, string) (domain.PayoutStatus, error) {
	return domain.PayoutStatus{}, nil
}

// Capabilities is a placeholder.
func (a *tokenEchoAdapter) Capabilities() domain.Capabilities {
	return domain.Capabilities{Provider: "coingate", Networks: map[string]domain.NetworkCapability{
		"usdt_trc20": {Asset: "USDT", MinAmount: money.MustParse("1", "USDT", 6)},
	}}
}

func TestCheckWebhookTokenEchoPasses(t *testing.T) {
	fixtures, ok := Fixtures("coingate")
	require.True(t, ok)
	adapter := &tokenEchoAdapter{fixtures: fixtures}
	manifest := manifestOf("coingate")

	require.NoError(t, checkWebhook(adapter, fixtures, manifest),
		"a compliant token echo adapter must pass the valid, tampered and missing probes")

	// The missing-signature fixture is the valid body without the token field.
	require.NotEqual(t, string(fixtures.WebhookValid), string(fixtures.WebhookMissing))
	require.Contains(t, string(fixtures.WebhookMissing), `"status":"paid"`)
	require.NotContains(t, string(fixtures.WebhookMissing), "token")
}

func TestCheckWebhookTokenEchoTamperedFails(t *testing.T) {
	fixtures, ok := Fixtures("coingate")
	require.True(t, ok)
	tampered := &tamperAcceptingAdapter{tokenEchoAdapter: tokenEchoAdapter{fixtures: fixtures}}
	err := checkWebhook(tampered, fixtures, manifestOf("coingate"))
	require.Error(t, err, "accepting a tampered delivery must fail CONF-6")
}

// tamperAcceptingAdapter accepts any delivery with a token field.
type tamperAcceptingAdapter struct {
	tokenEchoAdapter
}

// VerifyWebhook accepts a body carrying any token.
func (a *tamperAcceptingAdapter) VerifyWebhook(_ map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	if len(rawBody) == 0 {
		return domain.CanonicalEvent{}, errors.New("empty body")
	}
	return domain.CanonicalEvent{Type: domain.StateConfirmed}, nil
}

func TestSkipRecording(t *testing.T) {
	require.True(t, errors.Is(skipCheck("reason %d", 1), errSkip))
	report := Report{Results: []CheckResult{{ID: "CONF-6", Skipped: true, Err: skipCheck("none strategy")}}}
	require.NoError(t, report.Err("CONF-6"))
}
