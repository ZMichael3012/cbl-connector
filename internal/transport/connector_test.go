package transport_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/transport"
)

type fakeGateway struct {
	createErr   error
	createCalls int
	status      domain.PaymentStatus
	statusErr   error
}

func (f *fakeGateway) CreateInvoice(_ context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	f.createCalls++
	if f.createErr != nil {
		return domain.Invoice{}, f.createErr
	}
	return domain.Invoice{
		GatewayID:   "order-777",
		Provider:    "coingate",
		CheckoutURL: "https://checkout.example/777",
		ExpiresAt:   req.ExpiresAt,
	}, nil
}

func (f *fakeGateway) GetPayment(_ context.Context, _ string) (domain.PaymentStatus, error) {
	return f.status, f.statusErr
}

func (f *fakeGateway) VerifyWebhook(_ map[string][]string, _ []byte) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{}, nil
}

func (f *fakeGateway) CreatePayout(_ context.Context, _ domain.PayoutRequest) (domain.Payout, error) {
	return domain.Payout{}, nil
}

func (f *fakeGateway) GetPayout(_ context.Context, _ string) (domain.PayoutStatus, error) {
	return domain.PayoutStatus{}, nil
}

func (f *fakeGateway) Capabilities() domain.Capabilities {
	return domain.Capabilities{Provider: "coingate"}
}

type fakeSelector struct {
	selected  domain.PaymentGateway
	err       error
	lastRoute domain.RouteRequest
}

func (f *fakeSelector) Select(_ context.Context, req domain.RouteRequest) (domain.PaymentGateway, error) {
	f.lastRoute = req
	return f.selected, f.err
}

type fakeMapping struct {
	saved      []domain.ProviderInvoice
	saveErr    error
	saveHits   int
	existing   domain.ProviderInvoice
	byOrderErr error
}

// ByOrderID returns the configured existing mapping; the zero value reports the
// domain not-found sentinel, which is the first-creation path.
func (f *fakeMapping) ByOrderID(_ context.Context, _, _ string) (domain.ProviderInvoice, error) {
	if f.byOrderErr != nil {
		return domain.ProviderInvoice{}, f.byOrderErr
	}
	if f.existing.GatewayID == "" {
		return domain.ProviderInvoice{}, domain.ErrProviderInvoiceNotFound
	}
	return f.existing, nil
}

func (f *fakeMapping) Save(_ context.Context, invoice domain.ProviderInvoice) (domain.ProviderInvoice, bool, error) {
	f.saveHits++
	if f.saveErr != nil {
		return domain.ProviderInvoice{}, false, f.saveErr
	}
	f.saved = append(f.saved, invoice)
	return invoice, true, nil
}

type fakeCaps struct {
	rows     []domain.Capabilities
	err      error
	provider string
	calls    int
}

func (f *fakeCaps) List(_ context.Context, provider string) ([]domain.Capabilities, error) {
	f.calls++
	f.provider = provider
	return f.rows, f.err
}

func newServer(t *testing.T, gateway *fakeGateway, selector *fakeSelector, mapping *fakeMapping, caps *fakeCaps) *transport.ConnectorServer {
	t.Helper()
	return transport.NewConnectorServer(
		selector,
		map[string]domain.PaymentGateway{"coingate": gateway},
		mapping,
		caps,
		nil,
	)
}

// testRequestExpiry sets the shared create expiry of the test requests: a fixed
// value keeps the request hash stable across repeated createRequest calls (the
// replay test compares hashes; time.Now would differ by nanoseconds).
var testRequestExpiry = timestamppb.New(time.Now().Add(30 * time.Minute))

func createRequest() *connectorv1.CreateInvoiceRequest {
	return &connectorv1.CreateInvoiceRequest{
		IdempotencyKey: "idem-1",
		PaymentId:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		Amount:         &commonv1.Money{MinorUnits: 1500000000, Asset: "BTC", Decimals: 8},
		Asset:          "BTC",
		Network:        "bitcoin",
		ExpiresAt:      testRequestExpiry,
	}
}

func TestCreateInvoiceSuccess(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{}
	selector := &fakeSelector{selected: gateway}
	mapping := &fakeMapping{}
	server := newServer(t, gateway, selector, mapping, &fakeCaps{})

	response, err := server.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)
	require.Equal(t, "order-777", response.GetGatewayInvoiceId())
	require.Equal(t, "coingate", response.GetProvider())
	require.Equal(t, "https://checkout.example/777", response.GetCheckoutUrl())
	require.NotNil(t, response.GetExpiresAt())

	require.Equal(t, domain.OperationInvoice, selector.lastRoute.Operation)
	require.Equal(t, "bitcoin", selector.lastRoute.Network)
	require.Equal(t, int64(1500000000), selector.lastRoute.Amount.MinorUnits)

	require.Equal(t, 1, mapping.saveHits)
	require.Len(t, mapping.saved, 1)
	require.Equal(t, "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f", mapping.saved[0].OrderID, "the order reference is the payment id")
	require.NotEmpty(t, mapping.saved[0].RequestHash)
	require.Equal(t, "coingate", mapping.saved[0].Provider)
}

func TestCreateInvoiceValidation(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{}
	selector := &fakeSelector{selected: gateway}
	server := newServer(t, gateway, selector, &fakeMapping{}, &fakeCaps{})

	cases := map[string]func(*connectorv1.CreateInvoiceRequest){
		"bad uuid":          func(r *connectorv1.CreateInvoiceRequest) { r.PaymentId = "nope" },
		"empty idempotency": func(r *connectorv1.CreateInvoiceRequest) { r.IdempotencyKey = "" },
		"zero amount":       func(r *connectorv1.CreateInvoiceRequest) { r.Amount.MinorUnits = 0 },
		"empty network":     func(r *connectorv1.CreateInvoiceRequest) { r.Network = "" },
		"past expiry":       func(r *connectorv1.CreateInvoiceRequest) { r.ExpiresAt = timestamppb.New(time.Now().Add(-time.Minute)) },
		"decimals over limit": func(r *connectorv1.CreateInvoiceRequest) {
			r.Amount.Decimals = 99
		},
		"asset mismatch": func(r *connectorv1.CreateInvoiceRequest) { r.Amount.Asset = "ETH" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			request := createRequest()
			mutate(request)
			_, err := server.CreateInvoice(context.Background(), request)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestCreateInvoiceErrorMapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "router unavailable", err: domain.ErrProviderUnavailable, want: codes.Unavailable},
		{name: "unsupported route", err: domain.ErrUnsupportedNetwork, want: codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gateway := &fakeGateway{}
			server := newServer(t, gateway, &fakeSelector{err: tc.err}, &fakeMapping{}, &fakeCaps{})
			_, err := server.CreateInvoice(context.Background(), createRequest())
			require.Equal(t, tc.want, status.Code(err))
		})
	}

	rejected := &fakeGateway{createErr: domain.ErrRejected}
	server := newServer(t, rejected, &fakeSelector{selected: rejected}, &fakeMapping{}, &fakeCaps{})
	_, err := server.CreateInvoice(context.Background(), createRequest())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	overloaded := &fakeGateway{createErr: domain.ErrProviderOverloaded}
	server = newServer(t, overloaded, &fakeSelector{selected: overloaded}, &fakeMapping{}, &fakeCaps{})
	_, err = server.CreateInvoice(context.Background(), createRequest())
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestCreateInvoiceMappingFailureIsFatal(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{}
	mapping := &fakeMapping{saveErr: errors.New("pg down")}
	server := newServer(t, gateway, &fakeSelector{selected: gateway}, mapping, &fakeCaps{})

	_, err := server.CreateInvoice(context.Background(), createRequest())
	require.Equal(t, 1, mapping.saveHits)
	require.Equal(t, codes.Unavailable, status.Code(err), "without the mapping real webhooks cannot be attributed, so the retry must be safe")
}

func TestCreateInvoiceReplaysExistingOrder(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{}
	mapping := &fakeMapping{}
	server := newServer(t, gateway, &fakeSelector{selected: gateway}, mapping, &fakeCaps{})

	first, err := server.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)
	require.Equal(t, 1, gateway.createCalls)
	require.Len(t, mapping.saved, 1)

	// A retry of the same request replays the stored invoice: no second invoice
	// is created at the provider (T-CONNECTOR-07).
	mapping.existing = mapping.saved[0]
	mapping.saveHits = 0
	gateway.createErr = errors.New("must not be called")
	second, err := server.CreateInvoice(context.Background(), createRequest())
	require.NoError(t, err)
	require.Equal(t, first.GetGatewayInvoiceId(), second.GetGatewayInvoiceId())
	require.Equal(t, 1, gateway.createCalls)
	require.Zero(t, mapping.saveHits)
}

func TestCreateInvoiceReplayConflict(t *testing.T) {
	t.Parallel()
	mapping := &fakeMapping{existing: domain.ProviderInvoice{
		GatewayID:   "order-777",
		RequestHash: "another-request",
	}}
	gateway := &fakeGateway{}
	server := newServer(t, gateway, &fakeSelector{selected: gateway}, mapping, &fakeCaps{})

	_, err := server.CreateInvoice(context.Background(), createRequest())
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Zero(t, gateway.createCalls)
}

func TestCreateInvoiceDeadlineNeverBecomesInternal(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{}
	server := newServer(t, gateway, &fakeSelector{err: context.DeadlineExceeded}, &fakeMapping{}, &fakeCaps{})

	_, err := server.CreateInvoice(context.Background(), createRequest())
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "a deadline must stay retryable for the caller")
}

func TestGetGatewayPaymentUnknownState(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{status: domain.PaymentStatus{State: domain.State("mystery")}}
	server := newServer(t, gateway, &fakeSelector{selected: gateway}, &fakeMapping{}, &fakeCaps{})

	_, err := server.GetGatewayPayment(context.Background(), &connectorv1.GetGatewayPaymentRequest{
		Provider:  "coingate",
		GatewayId: "order-777",
	})
	require.Equal(t, codes.Internal, status.Code(err), "an unknown adapter state must not be silently mapped")
}

func TestGetCapabilitiesNotWired(t *testing.T) {
	t.Parallel()
	server := transport.NewConnectorServer(&fakeSelector{}, map[string]domain.PaymentGateway{}, &fakeMapping{}, nil, nil)
	_, err := server.GetCapabilities(context.Background(), &connectorv1.GetCapabilitiesRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestGetGatewayPayment(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{status: domain.PaymentStatus{
		PaymentID:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		GatewayID:      "order-777",
		Provider:       "coingate",
		State:          domain.StateConfirmed,
		RawStatus:      "paid",
		Confirmations:  3,
		TXID:           "tx-1",
		Network:        "bitcoin",
		ReceivedAmount: money.MustParse("0.5", "BTC", 8),
		GatewayEventTS: time.Now().UTC(),
	}}
	server := newServer(t, gateway, &fakeSelector{selected: gateway}, &fakeMapping{}, &fakeCaps{})

	response, err := server.GetGatewayPayment(context.Background(), &connectorv1.GetGatewayPaymentRequest{
		Provider:  " CoinGate ",
		GatewayId: "order-777",
	})
	require.NoError(t, err)
	require.Equal(t, connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED, response.GetPayment().GetStatus())
	require.Equal(t, "paid", response.GetPayment().GetRawStatus())
	require.Equal(t, int64(50000000), response.GetPayment().GetReceivedAmount().GetMinorUnits())

	_, err = server.GetGatewayPayment(context.Background(), &connectorv1.GetGatewayPaymentRequest{Provider: "unknown", GatewayId: "x"})
	require.Equal(t, codes.NotFound, status.Code(err))

	_, err = server.GetGatewayPayment(context.Background(), &connectorv1.GetGatewayPaymentRequest{Provider: "coingate"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGatewayNotFoundMapping(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{statusErr: domain.ErrGatewayNotFound}
	server := newServer(t, gateway, &fakeSelector{selected: gateway}, &fakeMapping{}, &fakeCaps{})

	_, err := server.GetGatewayPayment(context.Background(), &connectorv1.GetGatewayPaymentRequest{
		Provider:  "coingate",
		GatewayId: "missing",
	})
	require.Equal(t, codes.NotFound, status.Code(err), "a missing provider payment must map to NotFound")
}

func TestGetCapabilities(t *testing.T) {
	t.Parallel()
	caps := &fakeCaps{rows: []domain.Capabilities{{
		Provider: "coingate",
		Networks: map[string]domain.NetworkCapability{
			"bitcoin": {
				Asset:             "BTC",
				MinAmount:         money.MustParse("0.0001", "BTC", 8),
				ConfirmationDepth: 2,
			},
			"ethereum": {
				Asset:             "USDT",
				MinAmount:         money.MustParse("10", "USDT", 6),
				MaxAmount:         money.MustParse("100000", "USDT", 6),
				ConfirmationDepth: 12,
			},
		},
	}}}
	server := newServer(t, &fakeGateway{}, &fakeSelector{}, &fakeMapping{}, caps)

	response, err := server.GetCapabilities(context.Background(), &connectorv1.GetCapabilitiesRequest{Provider: " CoinGate "})
	require.NoError(t, err)
	require.Equal(t, "coingate", caps.provider, "the filter is normalized")
	require.Len(t, response.GetCapabilities(), 2)
	byNetwork := make(map[string]*connectorv1.Capability)
	for _, capability := range response.GetCapabilities() {
		byNetwork[capability.GetNetwork()] = capability
	}
	require.Equal(t, "coingate", byNetwork["bitcoin"].GetProvider())
	require.Equal(t, uint32(8), byNetwork["bitcoin"].GetDecimals())
	require.Nil(t, byNetwork["bitcoin"].GetMaxAmount(), "an unset max bound is not sent")
	require.NotNil(t, byNetwork["ethereum"].GetMaxAmount())
}

func TestListGatewayPaymentsDeferred(t *testing.T) {
	t.Parallel()
	server := newServer(t, &fakeGateway{}, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
	_, err := server.ListGatewayPayments(context.Background(), &connectorv1.ListGatewayPaymentsRequest{Provider: "coingate"})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
