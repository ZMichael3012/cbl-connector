package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// fakeDecoder is a minimal decoder for registries that are not exercised.
type fakeDecoder struct{}

func (fakeDecoder) Provider() string { return "mock" }

func (fakeDecoder) Decode(_ context.Context, _ *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{}, nil
}

type fakeMapping struct {
	invoice domain.ProviderInvoice
	err     error
}

func (f *fakeMapping) ByGatewayID(_ context.Context, _, _ string) (domain.ProviderInvoice, error) {
	return f.invoice, f.err
}

type fakeFetcher struct {
	status domain.PaymentStatus
	err    error
	calls  int
}

func (f *fakeFetcher) FetchPaymentStatus(_ context.Context, _, _ string) (domain.PaymentStatus, error) {
	f.calls++
	return f.status, f.err
}

func TestHandleMandatoryFetchFailsClosed(t *testing.T) {
	t.Parallel()
	registry, err := NewDecoderRegistry(&fakeDecoder{})
	require.NoError(t, err)
	normalizer := NewNormalizer(registry, nil, WithFetchMandatory(func(provider string) bool {
		return provider == "coingate"
	}))

	// The mandate routes the delivery to the fetch path even without the raw
	// flag; without a fetcher it fails closed before any storage access.
	// A differently-cased provider must not slip past the mandate lookup.
	payload, err := proto.Marshal(&eventsv1.GatewayWebhookRaw{
		Provider:       "CoinGate",
		GatewayId:      "order-1",
		SignatureState: eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_VERIFIED,
	})
	require.NoError(t, err)
	err = normalizer.Handle(context.Background(), nil, inbox.Message{Value: payload})
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)
}

func TestFetchMandatoryFor(t *testing.T) {
	t.Parallel()
	withoutPredicate := newFetchNormalizer(t, nil, nil)
	require.False(t, withoutPredicate.fetchMandatoryFor("coingate"))

	registry, err := NewDecoderRegistry(&fakeDecoder{})
	require.NoError(t, err)
	withPredicate := NewNormalizer(registry, nil, WithFetchMandatory(func(provider string) bool {
		return provider == "coingate"
	}))
	require.True(t, withPredicate.fetchMandatoryFor("coingate"))
	require.False(t, withPredicate.fetchMandatoryFor("btcpay"))
}

func TestVerificationState(t *testing.T) {
	t.Parallel()
	verified := &eventsv1.GatewayWebhookRaw{
		SignatureState: eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_VERIFIED,
		// A contradicted legacy flag must not override the authoritative state.
		SignatureVerified: false, //nolint:staticcheck // SA1019: explicit transition test
	}
	require.Equal(t, eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_VERIFIED, verificationState(verified))

	legacy := &eventsv1.GatewayWebhookRaw{
		SignatureVerified: true, //nolint:staticcheck // SA1019: the fallback under test
	}
	require.Equal(t, eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_VERIFIED, verificationState(legacy))

	absent := &eventsv1.GatewayWebhookRaw{}
	require.Equal(t, eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_ABSENT, verificationState(absent))

	mismatch := &eventsv1.GatewayWebhookRaw{
		SignatureState:    eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_ECHO_MISMATCH,
		SignatureVerified: true, //nolint:staticcheck // SA1019: explicit transition test
	}
	require.Equal(t, eventsv1.WebhookSignatureState_WEBHOOK_SIGNATURE_STATE_ECHO_MISMATCH, verificationState(mismatch))
}

func newFetchNormalizer(t *testing.T, mapping PaymentMapping, fetcher PaymentStatusFetcher) *Normalizer {
	t.Helper()
	registry, err := NewDecoderRegistry(&fakeDecoder{})
	require.NoError(t, err)
	normalizer := NewNormalizer(registry, nil, WithPaymentMapping(mapping), WithPaymentFetcher(fetcher))
	return normalizer
}

func TestFetchCanonicalBuildsFromStatus(t *testing.T) {
	t.Parallel()
	eventTS := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	fetcher := &fakeFetcher{status: domain.PaymentStatus{
		PaymentID:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		State:          domain.StateConfirmed,
		TXID:           "tx-1",
		Network:        "bitcoin",
		ReceivedAmount: money.MustParse("10.5", "BTC", 8),
		Confirmations:  3,
		GatewayEventTS: eventTS,
	}}
	normalizer := newFetchNormalizer(t, nil, fetcher)

	raw := &eventsv1.GatewayWebhookRaw{Provider: "coingate", GatewayId: "order-1", FetchRequired: true}
	event, err := normalizer.fetchCanonical(context.Background(), raw, "coingate")
	require.NoError(t, err)
	require.Equal(t, 1, fetcher.calls)
	require.Equal(t, domain.StateConfirmed, event.Type)
	require.Equal(t, "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f", event.PaymentID)
	require.Equal(t, "order-1", event.GatewayID)
	require.Equal(t, eventTS, event.OccurredAt)
	require.Equal(t, "tx-1", event.TXID)
	require.NoError(t, event.Validate())
}

func TestFetchCanonicalPendingRetries(t *testing.T) {
	t.Parallel()
	fetcher := &fakeFetcher{status: domain.PaymentStatus{
		PaymentID: "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		State:     domain.StatePending,
		RawStatus: "new",
	}}
	normalizer := newFetchNormalizer(t, nil, fetcher)
	raw := &eventsv1.GatewayWebhookRaw{Provider: "coingate", GatewayId: "order-1", FetchRequired: true}

	_, err := normalizer.fetchCanonical(context.Background(), raw, "coingate")
	require.ErrorIs(t, err, domain.ErrTemporary,
		"a pre-deposit status retries instead of publishing an event (KI-connector-14)")
}

func TestFetchCanonicalOccurredAtFallback(t *testing.T) {
	t.Parallel()
	fetcher := &fakeFetcher{status: domain.PaymentStatus{
		PaymentID:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		State:          domain.StateDetected,
		ReceivedAmount: money.MustParse("1", "BTC", 8),
	}}
	normalizer := newFetchNormalizer(t, nil, fetcher)
	receipt := time.Date(2026, 10, 16, 11, 0, 0, 0, time.UTC)
	raw := &eventsv1.GatewayWebhookRaw{
		Provider:      "coingate",
		GatewayId:     "order-1",
		FetchRequired: true,
		ReceivedAt:    timestamppb.New(receipt),
	}
	event, err := normalizer.fetchCanonical(context.Background(), raw, "coingate")
	require.NoError(t, err)
	require.Equal(t, receipt, event.OccurredAt, "a retried delivery must keep the receipt time, not the wall clock")

	// Without any event time the delivery fails closed instead of inventing one.
	noTime := &eventsv1.GatewayWebhookRaw{Provider: "coingate", GatewayId: "order-1", FetchRequired: true}
	_, err = normalizer.fetchCanonical(context.Background(), noTime, "coingate")
	require.ErrorIs(t, err, domain.ErrInvalidWebhook)
}

func TestFetchCanonicalMappingMismatchFailsClosed(t *testing.T) {
	t.Parallel()
	mapping := &fakeMapping{invoice: domain.ProviderInvoice{PaymentID: "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d0001"}}
	fetcher := &fakeFetcher{status: domain.PaymentStatus{
		PaymentID:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d0002",
		State:          domain.StateDetected,
		ReceivedAmount: money.MustParse("1", "BTC", 8),
		GatewayEventTS: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
	}}
	normalizer := newFetchNormalizer(t, mapping, fetcher)
	raw := &eventsv1.GatewayWebhookRaw{Provider: "coingate", GatewayId: "order-1", FetchRequired: true}

	_, err := normalizer.fetchCanonical(context.Background(), raw, "coingate")
	require.ErrorIs(t, err, domain.ErrInvalidWebhook, "a status payment contradicting the mapping must not be attributed")
}

func TestFetchCanonicalFailsClosed(t *testing.T) {
	t.Parallel()
	normalizer := newFetchNormalizer(t, nil, nil)
	raw := &eventsv1.GatewayWebhookRaw{Provider: "coingate", GatewayId: "order-1", FetchRequired: true}
	_, err := normalizer.fetchCanonical(context.Background(), raw, "coingate")
	require.ErrorIs(t, err, domain.ErrProviderUnavailable)

	fetcher := &fakeFetcher{}
	normalizer = newFetchNormalizer(t, nil, fetcher)
	unknown := &eventsv1.GatewayWebhookRaw{Provider: "coingate", GatewayId: "unknown", FetchRequired: true}
	_, err = normalizer.fetchCanonical(context.Background(), unknown, "coingate")
	require.ErrorIs(t, err, domain.ErrInvalidWebhook)

	fetchErr := errors.New("provider down")
	normalizer = newFetchNormalizer(t, nil, &fakeFetcher{err: fetchErr})
	_, err = normalizer.fetchCanonical(context.Background(), raw, "coingate")
	require.ErrorIs(t, err, fetchErr)
}

func TestFetchCanonicalResolvesPaymentViaMapping(t *testing.T) {
	t.Parallel()
	mapping := &fakeMapping{invoice: domain.ProviderInvoice{PaymentID: "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f"}}
	fetcher := &fakeFetcher{status: domain.PaymentStatus{
		State:          domain.StateDetected,
		ReceivedAmount: money.MustParse("1", "BTC", 8),
		GatewayEventTS: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
	}}
	normalizer := newFetchNormalizer(t, mapping, fetcher)
	raw := &eventsv1.GatewayWebhookRaw{Provider: "coingate", GatewayId: "order-1", FetchRequired: true}

	event, err := normalizer.fetchCanonical(context.Background(), raw, "coingate")
	require.NoError(t, err)
	require.Equal(t, "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f", event.PaymentID)
}

// TestReconcilePaymentMappingMissFallsToAdapterID verifies the NOWPayments IPN
// resolution: the mapping is keyed by the invoice id while the IPN carries the
// provider payment id, so a mapping miss falls through to the order_id echoed by
// the fetched status (KI-connector-15).
func TestReconcilePaymentMappingMissFallsToAdapterID(t *testing.T) {
	t.Parallel()
	miss := fmt.Errorf("%w: provider invoice mapping not found", domain.ErrProviderInvoiceNotFound)
	normalizer := newFetchNormalizer(t, &fakeMapping{err: miss}, nil)
	resolved, err := normalizer.reconcilePayment(context.Background(), "nowpayments", "5957104571", "7d89a827-7d45-4831-bc72-506aa451ceb0")
	require.NoError(t, err)
	assert.Equal(t, "7d89a827-7d45-4831-bc72-506aa451ceb0", resolved)

	// A miss without adapter evidence still fails closed.
	_, err = normalizer.reconcilePayment(context.Background(), "nowpayments", "5957104571", "")
	require.ErrorIs(t, err, domain.ErrInvalidWebhook)
}

func TestResolvePaymentValidation(t *testing.T) {
	t.Parallel()
	withMapping := newFetchNormalizer(t, &fakeMapping{}, nil)
	_, err := withMapping.resolvePayment(context.Background(), "coingate", "unknown")
	require.ErrorIs(t, err, domain.ErrInvalidWebhook)

	withoutMapping := newFetchNormalizer(t, nil, nil)
	_, err = withoutMapping.resolvePayment(context.Background(), "coingate", "order-1")
	require.ErrorIs(t, err, domain.ErrInvalidWebhook)

	mappingErr := errors.New("not found")
	failing := newFetchNormalizer(t, &fakeMapping{err: mappingErr}, nil)
	_, err = failing.resolvePayment(context.Background(), "coingate", "order-1")
	require.ErrorIs(t, err, mappingErr)
}
