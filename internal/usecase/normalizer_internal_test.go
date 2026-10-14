package usecase

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-connector/internal/domain"
)

// testPaymentID задает UUID платежа для тестов конверта.
const testPaymentID = "01a138fd-0000-7000-8000-000000000001"

func canonicalEvent(state domain.State) domain.CanonicalEvent {
	return domain.CanonicalEvent{
		Type:           state,
		PaymentID:      testPaymentID,
		GatewayID:      "gw-1",
		Provider:       "mock",
		TXID:           "0xabc",
		Network:        "trc20",
		ReceivedAmount: money.Money{MinorUnits: 100_000, Asset: "USDT", Decimals: 2},
		Confirmations:  3,
		OccurredAt:     time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC),
	}
}

func TestBuildCanonicalEnvelope(t *testing.T) {
	t.Parallel()

	incoming := map[string]string{
		outbox.HeaderTraceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"vendor_event_id":        "vendor-1",
	}
	envelope, err := buildCanonicalEnvelope(canonicalEvent(domain.StateConfirmed), incoming)
	require.NoError(t, err)

	assert.Equal(t, TopicGatewayEventsCanonical, envelope.Topic)
	assert.Equal(t, testPaymentID, envelope.Key)
	assert.Equal(t, "gateway.payment.confirmed", envelope.Headers[outbox.HeaderEventType])
	assert.Equal(t, ProducerConnector, envelope.Headers[outbox.HeaderProducer])
	assert.Equal(t, "v1", envelope.Headers[outbox.HeaderSchemaVersion])
	assert.Equal(t, "mock", envelope.Headers["provider"])
	assert.Equal(t, "gw-1", envelope.Headers["gateway_id"])
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		envelope.Headers[outbox.HeaderTraceparent], "traceparent пробрасывается")
	assert.Equal(t, "2026-10-14T12:00:00Z", envelope.Headers[outbox.HeaderOccurredAt])

	var payload eventsv1.CanonicalPaymentEvent
	require.NoError(t, proto.Unmarshal(envelope.Payload, &payload))
	assert.Equal(t, testPaymentID, payload.GetPaymentId())
	assert.Equal(t, "gw-1", payload.GetGatewayId())
	assert.Equal(t, "mock", payload.GetProvider())
	assert.Equal(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, payload.GetType())
	assert.Equal(t, int64(100_000), payload.GetReceivedAmount().GetMinorUnits())
	assert.Equal(t, "USDT", payload.GetReceivedAmount().GetAsset())
	assert.Equal(t, uint32(2), payload.GetReceivedAmount().GetDecimals())
	assert.Equal(t, uint32(3), payload.GetConfirmations())
	assert.Equal(t, "0xabc", payload.GetTxid())
	assert.Equal(t, "trc20", payload.GetNetwork())
	assert.Equal(t, time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC), payload.GetGatewayEventTs().AsTime())
}

func TestBuildCanonicalEnvelopeDropsInvalidTraceparent(t *testing.T) {
	t.Parallel()

	for _, traceparent := range []string{"", "00-trace-span-01", strings.Repeat("a", 200)} {
		incoming := map[string]string{outbox.HeaderTraceparent: traceparent}
		envelope, err := buildCanonicalEnvelope(canonicalEvent(domain.StateDetected), incoming)
		require.NoError(t, err)
		_, ok := envelope.Headers[outbox.HeaderTraceparent]
		assert.False(t, ok, "некорректный traceparent %q отбрасывается", traceparent)
	}
}

func TestBuildCanonicalEnvelopeWithoutTraceparent(t *testing.T) {
	t.Parallel()

	envelope, err := buildCanonicalEnvelope(canonicalEvent(domain.StateDetected), nil)
	require.NoError(t, err)
	_, ok := envelope.Headers[outbox.HeaderTraceparent]
	assert.False(t, ok, "без входящего traceparent заголовок не добавляется")
}

func TestCanonicalEventTypeMapping(t *testing.T) {
	t.Parallel()

	cases := map[domain.State]eventsv1.CanonicalPaymentEventType{
		domain.StateDetected:  eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED,
		domain.StateConfirmed: eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED,
		domain.StateFailed:    eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED,
		domain.StateExpired:   eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_EXPIRED,
		domain.StateUnderpaid: eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID,
		domain.StateOverpaid:  eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID,
		domain.State("?"):     eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNSPECIFIED,
	}
	for state, want := range cases {
		assert.Equal(t, want, canonicalEventType(state), string(state))
	}
}
