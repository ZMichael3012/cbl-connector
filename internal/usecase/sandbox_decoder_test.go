package usecase_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// sandboxPaymentID specifies the payment UUID for sandbox webhook bodies.
const sandboxPaymentID = "01a138fd-0000-7000-8000-000000000002"

// rawWebhook builds a raw sandbox provider webhook.
func rawWebhook(body string) *eventsv1.GatewayWebhookRaw {
	return &eventsv1.GatewayWebhookRaw{
		Provider:          "mock",
		GatewayId:         "inv-1",
		ReceivedAt:        timestamppb.New(time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)),
		RawBody:           []byte(body),
		SignatureVerified: true, //nolint:staticcheck // SA1019: exercises the deprecated transition fallback covered by 17.2
	}
}

const sandboxValidBody = `{
  "payment_id": "01a138fd-0000-7000-8000-000000000002", "gateway_id": "inv-1", "status": "confirmed",
  "received_amount": {"minor_units": 100000, "asset": "USDT", "decimals": 2},
  "confirmations": 3, "txid": "0xabc", "network": "trc20",
  "event_ts": "2026-10-14T12:00:05Z"
}`

func newSandboxDecoder(t *testing.T) *usecase.SandboxDecoder {
	t.Helper()
	decoder, err := usecase.NewSandboxDecoder("Mock")
	require.NoError(t, err)
	return decoder
}

func TestSandboxDecoderValid(t *testing.T) {
	t.Parallel()

	decoder := newSandboxDecoder(t)
	assert.Equal(t, "mock", decoder.Provider())

	event, err := decoder.Decode(context.Background(), rawWebhook(sandboxValidBody))
	require.NoError(t, err)
	assert.Equal(t, domain.StateConfirmed, event.Type)
	assert.Equal(t, sandboxPaymentID, event.PaymentID)
	assert.Equal(t, "inv-1", event.GatewayID)
	assert.Equal(t, "mock", event.Provider)
	assert.Equal(t, int64(100_000), event.ReceivedAmount.MinorUnits)
	assert.Equal(t, "USDT", event.ReceivedAmount.Asset)
	assert.Equal(t, uint32(2), event.ReceivedAmount.Decimals)
	assert.Equal(t, uint32(3), event.Confirmations)
	assert.Equal(t, "0xabc", event.TXID)
	assert.Equal(t, "trc20", event.Network)
	assert.Equal(t, time.Date(2026, 10, 14, 12, 0, 5, 0, time.UTC), event.OccurredAt)
}

func TestSandboxDecoderStatusAliases(t *testing.T) {
	t.Parallel()

	decoder := newSandboxDecoder(t)
	cases := map[string]domain.State{
		"pending":   domain.StateDetected,
		"completed": domain.StateConfirmed,
		"error":     domain.StateFailed,
		"EXPIRED":   domain.StateExpired,
	}
	for status, want := range cases {
		body := `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"` + status + `",` +
			`"received_amount":{"minor_units":1,"asset":"USDT","decimals":2},` +
			`"event_ts":"2026-10-14T12:00:00Z"}`
		event, err := decoder.Decode(context.Background(), rawWebhook(body))
		require.NoError(t, err, status)
		assert.Equal(t, want, event.Type, status)
	}
}

func TestSandboxDecoderRejections(t *testing.T) {
	t.Parallel()

	decoder := newSandboxDecoder(t)
	cases := map[string]string{
		"broken json":              `{not-json`,
		"empty payment_id":         `{"payment_id":" ","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`,
		"empty gateway_id":         `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`,
		"missing received_amount":  `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","event_ts":"2026-10-14T12:00:00Z"}`,
		"missing event_ts":         `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":2}}`,
		"broken event_ts":          `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"yesterday"}`,
		"empty status":             `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":" ","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`,
		"long status":              `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"` + strings.Repeat("s", 33) + `","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`,
		"long event_ts":            `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"` + strings.Repeat("2", 65) + `"}`,
		"negative amount":          `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":-5,"asset":"USDT","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`,
		"empty asset":              `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`,
		"decimals above the limit": `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":30},"event_ts":"2026-10-14T12:00:00Z"}`,
	}
	for name, body := range cases {
		_, err := decoder.Decode(context.Background(), rawWebhook(body))
		require.ErrorIs(t, err, domain.ErrInvalidWebhook, name)
	}

	_, err := decoder.Decode(context.Background(), rawWebhook(
		`{"payment_id":"`+sandboxPaymentID+`","gateway_id":"g","status":"weird","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"2026-10-14T12:00:00Z"}`))
	require.ErrorIs(t, err, domain.ErrUnknownState)
}

func TestSandboxDecoderRejectsHugeInput(t *testing.T) {
	t.Parallel()

	decoder := newSandboxDecoder(t)

	huge := `{"payment_id":"` + sandboxPaymentID + `","gateway_id":"g","status":"confirmed","received_amount":{"minor_units":1,"asset":"USDT","decimals":2},"event_ts":"` +
		strings.Repeat("2", 300*(1<<10)) + `"}`
	_, err := decoder.Decode(context.Background(), rawWebhook(huge))
	require.ErrorIs(t, err, domain.ErrInvalidWebhook)
	assert.Less(t, len(err.Error()), 200, "the giant input does not leak into the error text")

	bigBody := `{"padding":"` + strings.Repeat("p", 300*(1<<10)) + `"}`
	_, err = decoder.Decode(context.Background(), rawWebhook(bigBody))
	require.ErrorIs(t, err, domain.ErrInvalidWebhook)
	assert.Less(t, len(err.Error()), 200, "an oversized body is cut off before parsing")
}

func TestSandboxDecoderEmptyProvider(t *testing.T) {
	t.Parallel()

	_, err := usecase.NewSandboxDecoder("  ")
	require.Error(t, err)
}

func TestSandboxDecoderRejectsRealProviders(t *testing.T) {
	t.Parallel()

	for _, provider := range []string{"coingate", "btcpay", "nowpayments", "bitpay", "unknown"} {
		_, err := usecase.NewSandboxDecoder(provider)
		require.Error(t, err, "provider %q must not get a sandbox decoder", provider)
	}
}
