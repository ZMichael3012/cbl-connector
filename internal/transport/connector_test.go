package transport_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	exportPage  domain.ExportPage
	exportErr   error
	exportCalls int
	lastExport  domain.ExportRequest
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

func (f *fakeGateway) ExportPayments(_ context.Context, req domain.ExportRequest) (domain.ExportPage, error) {
	f.exportCalls++
	f.lastExport = req
	if f.exportErr != nil {
		return domain.ExportPage{}, f.exportErr
	}
	return f.exportPage, nil
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
	require.Equal(t, "https://checkout.example/777", second.GetCheckoutUrl(),
		"the replay returns the stored destination so the payment can reach pending (4.2)")
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

func TestGetGatewayPaymentPending(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{status: domain.PaymentStatus{
		PaymentID: "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
		GatewayID: "order-778",
		Provider:  "coingate",
		State:     domain.StatePending,
		RawStatus: "new",
	}}
	server := newServer(t, gateway, &fakeSelector{selected: gateway}, &fakeMapping{}, &fakeCaps{})

	response, err := server.GetGatewayPayment(context.Background(), &connectorv1.GetGatewayPaymentRequest{
		Provider:  "coingate",
		GatewayId: "order-778",
	})
	require.NoError(t, err)
	require.Equal(t, connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_PENDING,
		response.GetPayment().GetStatus(), "a pre-deposit status travels as PENDING (7)")
	require.Equal(t, "new", response.GetPayment().GetRawStatus())
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

// bareGateway presents a gateway port without the optional PaymentExporter
// capability: embedding the interface hides the fake export method, so the
// missing-capability path is exercised.
type bareGateway struct{ domain.PaymentGateway }

// newServerWith builds a server over the given gateways map.
func newServerWith(gateways map[string]domain.PaymentGateway) *transport.ConnectorServer {
	return transport.NewConnectorServer(&fakeSelector{}, gateways, &fakeMapping{}, &fakeCaps{}, nil)
}

// listRequest builds a valid export request (provider casing varies on purpose).
func listRequest() *connectorv1.ListGatewayPaymentsRequest {
	now := time.Now().UTC()
	return &connectorv1.ListGatewayPaymentsRequest{
		Provider: " CoinGate ",
		From:     timestamppb.New(now.Add(-2 * time.Hour)),
		To:       timestamppb.New(now.Add(-time.Hour)),
	}
}

func TestListGatewayPaymentsPage(t *testing.T) {
	t.Parallel()
	eventTS := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	gateway := &fakeGateway{exportPage: domain.ExportPage{
		Payments: []domain.PaymentStatus{{
			GatewayID:      "gw-1",
			PaymentID:      "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f",
			Provider:       "coingate",
			State:          domain.StateConfirmed,
			RawStatus:      "paid",
			ReceivedAmount: money.MustParse("5", "USDT", 6),
			Confirmations:  3,
			TXID:           "0xabc",
			Network:        "usdt_trc20",
			GatewayEventTS: eventTS,
		}},
		NextCursor: domain.EncodeExportCursor(2),
	}}
	server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})

	request := listRequest()
	request.Cursor = domain.EncodeExportCursor(100)
	response, err := server.ListGatewayPayments(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, response.GetPayments(), 1)
	row := response.GetPayments()[0]
	require.Equal(t, "gw-1", row.GetGatewayId())
	require.Equal(t, "018f8f5c-3f7a-7b2e-9c4d-2a1b3c4d5e6f", row.GetPaymentId())
	require.Equal(t, connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED, row.GetStatus())
	require.Equal(t, "paid", row.GetRawStatus())
	require.Equal(t, int64(5000000), row.GetReceivedAmount().GetMinorUnits())
	require.Equal(t, "USDT", row.GetReceivedAmount().GetAsset())
	require.Equal(t, uint32(6), row.GetReceivedAmount().GetDecimals())
	require.Equal(t, uint32(3), row.GetConfirmations())
	require.Equal(t, "0xabc", row.GetTxid())
	require.Equal(t, "usdt_trc20", row.GetNetwork())
	require.Equal(t, eventTS, row.GetGatewayEventTs().AsTime())
	require.Equal(t, domain.EncodeExportCursor(2), response.GetNextCursor())

	require.Equal(t, 1, gateway.exportCalls)
	require.Equal(t, 100, gateway.lastExport.Limit, "an unset limit defaults to 100")
	require.Equal(t, domain.EncodeExportCursor(100), gateway.lastExport.Cursor, "the cursor is passed to the exporter")
	require.False(t, gateway.lastExport.From.IsZero())
	require.True(t, gateway.lastExport.To.After(gateway.lastExport.From))
}

func TestListGatewayPaymentsUnmappedRow(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{exportPage: domain.ExportPage{
		Payments: []domain.PaymentStatus{{
			GatewayID: "gw-9",
			State:     "",
			RawStatus: "strange_new_state",
		}},
	}}
	server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})

	response, err := server.ListGatewayPayments(context.Background(), listRequest())
	require.NoError(t, err)
	require.Len(t, response.GetPayments(), 1)
	row := response.GetPayments()[0]
	require.Equal(t, connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_UNSPECIFIED, row.GetStatus())
	require.Equal(t, "strange_new_state", row.GetRawStatus(), "the raw provider word is preserved")
	require.Nil(t, row.GetReceivedAmount(), "missing amount evidence leaves the field unset, not zero")
	require.Empty(t, response.GetNextCursor())
}

func TestListGatewayPaymentsUnknownProvider(t *testing.T) {
	t.Parallel()
	server := newServer(t, &fakeGateway{}, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
	request := listRequest()
	request.Provider = "nope"

	_, err := server.ListGatewayPayments(context.Background(), request)
	require.Equal(t, codes.NotFound, status.Code(err))

	request.Provider = " "
	_, err = server.ListGatewayPayments(context.Background(), request)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an empty provider is invalid")
}

func TestListGatewayPaymentsWindowValidation(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	base := func() *connectorv1.ListGatewayPaymentsRequest {
		return &connectorv1.ListGatewayPaymentsRequest{
			Provider: "coingate",
			From:     timestamppb.New(now.Add(-2 * time.Hour)),
			To:       timestamppb.New(now.Add(-time.Hour)),
		}
	}
	cases := map[string]func(*connectorv1.ListGatewayPaymentsRequest){
		"missing from": func(r *connectorv1.ListGatewayPaymentsRequest) { r.From = nil },
		"missing to":   func(r *connectorv1.ListGatewayPaymentsRequest) { r.To = nil },
		"reversed":     func(r *connectorv1.ListGatewayPaymentsRequest) { r.From = timestamppb.New(now) },
		"window over 31 days": func(r *connectorv1.ListGatewayPaymentsRequest) {
			r.From = timestamppb.New(now.Add(-40 * 24 * time.Hour))
		},
		"future bound": func(r *connectorv1.ListGatewayPaymentsRequest) {
			r.To = timestamppb.New(now.Add(30 * time.Minute))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			gateway := &fakeGateway{}
			server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
			request := base()
			mutate(request)
			_, err := server.ListGatewayPayments(context.Background(), request)
			require.Equal(t, codes.InvalidArgument, status.Code(err), name)
			require.Zero(t, gateway.exportCalls, "invalid input must not reach the provider")
		})
	}
}

func TestListGatewayPaymentsLimitBounds(t *testing.T) {
	t.Parallel()
	gateway := &fakeGateway{}
	server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})

	request := listRequest()
	request.Limit = 500
	_, err := server.ListGatewayPayments(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 500, gateway.lastExport.Limit)

	request = listRequest()
	request.Limit = 501
	_, err = server.ListGatewayPayments(context.Background(), request)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, 1, gateway.exportCalls, "the oversized limit must not reach the provider")

	request = listRequest()
	request.Cursor = strings.Repeat("a", 65)
	_, err = server.ListGatewayPayments(context.Background(), request)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, 1, gateway.exportCalls, "an oversized cursor must not reach the provider")
}

func TestListGatewayPaymentsErrorMapping(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		err  error
		code codes.Code
	}{
		"temporary":       {err: domain.ErrTemporary, code: codes.Unavailable},
		"gateway missing": {err: domain.ErrGatewayNotFound, code: codes.NotFound},
		"bad cursor":      {err: fmt.Errorf("%w: cursor", domain.ErrInvalidGatewayRequest), code: codes.InvalidArgument},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gateway := &fakeGateway{exportErr: tc.err}
			server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
			_, err := server.ListGatewayPayments(context.Background(), listRequest())
			require.Equal(t, tc.code, status.Code(err), name)
		})
	}
}

func TestListGatewayPaymentsWithoutCapability(t *testing.T) {
	t.Parallel()
	server := newServerWith(map[string]domain.PaymentGateway{"coingate": &bareGateway{PaymentGateway: &fakeGateway{}}})
	_, err := server.ListGatewayPayments(context.Background(), listRequest())
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestListGatewayPaymentsRejectsAdapterViolations(t *testing.T) {
	t.Parallel()
	t.Run("mapped state outside the allowlist", func(t *testing.T) {
		gateway := &fakeGateway{exportPage: domain.ExportPage{
			Payments: []domain.PaymentStatus{{GatewayID: "gw-1", State: domain.State("weird")}},
		}}
		server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
		_, err := server.ListGatewayPayments(context.Background(), listRequest())
		require.Equal(t, codes.Internal, status.Code(err))
	})
	t.Run("pending row maps to the pending enum", func(t *testing.T) {
		gateway := &fakeGateway{exportPage: domain.ExportPage{
			Payments: []domain.PaymentStatus{{GatewayID: "gw-1", State: domain.StatePending, RawStatus: "new"}},
		}}
		server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
		response, err := server.ListGatewayPayments(context.Background(), listRequest())
		require.NoError(t, err)
		require.Len(t, response.GetPayments(), 1)
		require.Equal(t, connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_PENDING,
			response.GetPayments()[0].GetStatus())
	})
	t.Run("page above the requested limit", func(t *testing.T) {
		gateway := &fakeGateway{exportPage: domain.ExportPage{
			Payments: []domain.PaymentStatus{
				{GatewayID: "gw-1", State: domain.StateDetected},
				{GatewayID: "gw-2", State: domain.StateDetected},
				{GatewayID: "gw-3", State: domain.StateDetected},
			},
		}}
		server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
		request := listRequest()
		request.Limit = 2
		_, err := server.ListGatewayPayments(context.Background(), request)
		require.Equal(t, codes.Internal, status.Code(err))
	})
	t.Run("row without any status evidence", func(t *testing.T) {
		gateway := &fakeGateway{exportPage: domain.ExportPage{
			Payments: []domain.PaymentStatus{{GatewayID: "gw-1"}},
		}}
		server := newServer(t, gateway, &fakeSelector{}, &fakeMapping{}, &fakeCaps{})
		_, err := server.ListGatewayPayments(context.Background(), listRequest())
		require.Equal(t, codes.Internal, status.Code(err), "a row with no state and no raw word is an adapter violation")
	})
}
