package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/connector/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ZMichael3012/cbl-kit/money"

	"github.com/cbl/cbl-connector/internal/domain"
)

// Request validation bounds and the independent mapping save budget: the save
// must not share the caller deadline, otherwise an expired budget after a
// provider side effect would orphan the invoice (review finding).
const (
	maxIdempotencyKeyLen = 255
	maxInvoiceExpiry     = 7 * 24 * time.Hour
	mappingSaveTimeout   = 5 * time.Second
)

// Export bounds (17.13): one page is bounded, the window keeps a provider
// export call predictable, the cursor is a short opaque token and the future
// skew rejects nonsensical windows while tolerating clock drift.
const (
	defaultExportLimit = 100
	maxExportLimit     = 500
	maxExportWindow    = 31 * 24 * time.Hour
	exportFutureSkew   = 5 * time.Minute
	maxExportCursorLen = 64
)

// InvoiceMappingStore persists the payment to provider invoice mapping (17.2):
// Save is idempotent per (provider, gateway_id), ByOrderID backs the idempotent
// replay of a retried creation (T-CONNECTOR-07).
type InvoiceMappingStore interface {
	Save(ctx context.Context, invoice domain.ProviderInvoice) (domain.ProviderInvoice, bool, error)
	ByOrderID(ctx context.Context, provider, orderID string) (domain.ProviderInvoice, error)
}

// CapabilitiesProvider lists provider capabilities (the 17.2 loader).
type CapabilitiesProvider interface {
	List(ctx context.Context, provider string) ([]domain.Capabilities, error)
}

// ConnectorServer implements the money-path ConnectorService: invoice creation
// through the router, status reads, capabilities and the per-provider
// reconciliation export (17.13).
type ConnectorServer struct {
	connectorv1.UnimplementedConnectorServiceServer

	selector     domain.Router
	gateways     map[string]domain.PaymentGateway
	mapping      InvoiceMappingStore
	capabilities CapabilitiesProvider
	log          *zap.Logger
}

// NewConnectorServer creates the money-path server; gateways maps normalized
// provider ids (manifest ids) to adapter instances.
func NewConnectorServer(selector domain.Router, gateways map[string]domain.PaymentGateway, mapping InvoiceMappingStore, capabilities CapabilitiesProvider, log *zap.Logger) *ConnectorServer {
	if log == nil {
		log = zap.NewNop()
	}
	return &ConnectorServer{
		selector:     selector,
		gateways:     gateways,
		mapping:      mapping,
		capabilities: capabilities,
		log:          log,
	}
}

// CreateInvoice routes the request, creates the invoice at the provider and
// stores the payment mapping before answering (T-CONNECTOR-03): the webhook
// normalization resolves the platform payment through it.
func (s *ConnectorServer) CreateInvoice(ctx context.Context, req *connectorv1.CreateInvoiceRequest) (*connectorv1.CreateInvoiceResponse, error) {
	if err := validateCreateInvoice(ctx, req); err != nil {
		return nil, err
	}
	amount := domainMoney(req.GetAmount())
	selected, err := s.selector.Select(ctx, domain.RouteRequest{
		Network:   strings.TrimSpace(req.GetNetwork()),
		Asset:     strings.TrimSpace(req.GetAsset()),
		Amount:    amount,
		Operation: domain.OperationInvoice,
	})
	if err != nil {
		return nil, statusFromError(err)
	}
	provider := providerOf(selected)
	if provider == "" {
		return nil, status.Error(codes.Internal, "selected gateway has no provider id")
	}
	paymentID := strings.TrimSpace(req.GetPaymentId())
	hash := requestHash(req)
	// Idempotent replay (T-CONNECTOR-07): an order (the payment id) maps to at
	// most one provider invoice; a retry returns the stored one instead of
	// creating a second invoice, and a different request under the same order is
	// a conflict.
	if existing, lookupErr := s.mapping.ByOrderID(ctx, provider, paymentID); lookupErr == nil {
		if existing.RequestHash != hash {
			return nil, status.Error(codes.AlreadyExists, "the order is already mapped with a different request")
		}
		// The replay returns the stored destination so a payment that lost the
		// first response can still reach its address (4.2).
		return &connectorv1.CreateInvoiceResponse{
			GatewayInvoiceId: existing.GatewayID,
			Provider:         provider,
			Address:          existing.Address,
			CheckoutUrl:      existing.CheckoutURL,
		}, nil
	} else if !errors.Is(lookupErr, domain.ErrProviderInvoiceNotFound) {
		s.log.Error("invoice mapping lookup failed", zap.String("provider", provider), zap.Error(lookupErr))
		return nil, status.Error(codes.Unavailable, "invoice mapping is unavailable")
	}
	expiresAt := time.Time{}
	if req.GetExpiresAt() != nil {
		expiresAt = req.GetExpiresAt().AsTime()
	}
	invoice, err := selected.CreateInvoice(ctx, domain.CreateInvoiceRequest{
		IdempotencyKey: strings.TrimSpace(req.GetIdempotencyKey()),
		PaymentID:      paymentID,
		Amount:         amount,
		Network:        strings.TrimSpace(req.GetNetwork()),
		ExpiresAt:      expiresAt,
	})
	if err != nil {
		return nil, statusFromError(err)
	}
	if invoice.CheckoutURL != "" &&
		!strings.HasPrefix(invoice.CheckoutURL, "https://") && !strings.HasPrefix(invoice.CheckoutURL, "http://") {
		// A non-http(s) destination must never reach merchant clients.
		s.log.Error("provider returned an invalid checkout url", zap.String("provider", provider))
		return nil, status.Error(codes.Internal, "provider returned an invalid checkout url")
	}
	if s.mapping != nil {
		saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mappingSaveTimeout)
		defer cancel()
		_, _, mapErr := s.mapping.Save(saveCtx, domain.ProviderInvoice{
			Provider:    provider,
			GatewayID:   invoice.GatewayID,
			PaymentID:   paymentID,
			OrderID:     paymentID,
			RequestHash: hash,
			Address:     invoice.Address,
			CheckoutURL: invoice.CheckoutURL,
		})
		if mapErr != nil {
			// Without the mapping real webhooks cannot attribute the invoice:
			// the call fails retryably and the replay path answers the retry.
			s.log.Error("invoice mapping save failed",
				zap.String("provider", provider),
				zap.String("gateway_id", invoice.GatewayID),
				zap.Error(mapErr),
			)
			return nil, status.Error(codes.Unavailable, "invoice mapping is unavailable")
		}
	}
	response := &connectorv1.CreateInvoiceResponse{
		GatewayInvoiceId: invoice.GatewayID,
		Provider:         provider,
		Address:          invoice.Address,
	}
	responseExpires := invoice.ExpiresAt
	if responseExpires.IsZero() {
		responseExpires = expiresAt
	}
	if !responseExpires.IsZero() {
		response.ExpiresAt = timestamppb.New(responseExpires)
	}
	if invoice.CheckoutURL != "" {
		response.CheckoutUrl = invoice.CheckoutURL
	}
	return response, nil
}

// GetGatewayPayment returns the canonical status of a provider invoice.
func (s *ConnectorServer) GetGatewayPayment(ctx context.Context, req *connectorv1.GetGatewayPaymentRequest) (*connectorv1.GetGatewayPaymentResponse, error) {
	provider := strings.ToLower(strings.TrimSpace(req.GetProvider()))
	gatewayID := strings.TrimSpace(req.GetGatewayId())
	if provider == "" || gatewayID == "" {
		return nil, status.Error(codes.InvalidArgument, "provider and gateway_id are required")
	}
	selected, ok := s.gateways[provider]
	if !ok {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("provider %q is not registered", provider))
	}
	payment, err := selected.GetPayment(ctx, gatewayID)
	if err != nil {
		return nil, statusFromError(err)
	}
	switch payment.State {
	case domain.StatePending, domain.StateDetected, domain.StateConfirmed, domain.StateFailed,
		domain.StateExpired, domain.StateUnderpaid, domain.StateOverpaid:
	default:
		s.log.Error("provider returned an unknown state",
			zap.String("provider", provider),
			zap.String("state", string(payment.State)),
		)
		return nil, status.Error(codes.Internal, "provider returned an unknown state")
	}
	return &connectorv1.GetGatewayPaymentResponse{Payment: canonicalStatus(payment)}, nil
}

// GetCapabilities returns provider-qualified capabilities, optionally filtered.
// The sandbox and supports_payouts/supports_refunds context fields arrive with
// the adapter capability data (17.6 - 17.9).
func (s *ConnectorServer) GetCapabilities(ctx context.Context, req *connectorv1.GetCapabilitiesRequest) (*connectorv1.GetCapabilitiesResponse, error) {
	if s.capabilities == nil {
		return nil, status.Error(codes.Unavailable, "capabilities are not wired yet")
	}
	rows, err := s.capabilities.List(ctx, strings.ToLower(strings.TrimSpace(req.GetProvider())))
	if err != nil {
		return nil, statusFromError(err)
	}
	capabilities := make([]*connectorv1.Capability, 0)
	for _, providerCaps := range rows {
		for network, networkCaps := range providerCaps.Networks {
			var maxAmount *commonv1.Money
			if !networkCaps.MaxAmount.IsZero() {
				maxAmount = protoMoney(networkCaps.MaxAmount)
			}
			capability := &connectorv1.Capability{
				Network:           network,
				Asset:             networkCaps.Asset,
				MinAmount:         protoMoney(networkCaps.MinAmount),
				ConfirmationDepth: networkCaps.ConfirmationDepth,
				Provider:          providerCaps.Provider,
				MaxAmount:         maxAmount,
				Decimals:          networkCaps.MinAmount.Decimals,
			}
			capabilities = append(capabilities, capability)
		}
	}
	return &connectorv1.GetCapabilitiesResponse{Capabilities: capabilities}, nil
}

// ListGatewayPayments returns one page of provider operations for the
// reconciliation export (17.13): the request is validated before any provider
// call, the provider is resolved from the wired gateways and the page is
// serialized with the canonical status. A row the adapter could not map
// arrives with the raw provider word and an UNSPECIFIED status: the raw word
// is the reconciliation evidence and the state is never fabricated.
func (s *ConnectorServer) ListGatewayPayments(ctx context.Context, req *connectorv1.ListGatewayPaymentsRequest) (*connectorv1.ListGatewayPaymentsResponse, error) {
	provider := strings.ToLower(strings.TrimSpace(req.GetProvider()))
	if provider == "" {
		return nil, status.Error(codes.InvalidArgument, "provider is required")
	}
	selected, ok := s.gateways[provider]
	if !ok {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("provider %q is not registered", provider))
	}
	if req.GetFrom() == nil || req.GetTo() == nil {
		return nil, status.Error(codes.InvalidArgument, "from and to are required")
	}
	from := req.GetFrom().AsTime().UTC()
	to := req.GetTo().AsTime().UTC()
	switch {
	case !from.Before(to):
		return nil, status.Error(codes.InvalidArgument, "from must be before to")
	case to.Sub(from) > maxExportWindow:
		return nil, status.Error(codes.InvalidArgument, "the export window exceeds 31 days")
	case to.After(time.Now().UTC().Add(exportFutureSkew)):
		return nil, status.Error(codes.InvalidArgument, "to must not be in the future")
	}
	limit := int(req.GetLimit())
	if limit == 0 {
		limit = defaultExportLimit
	}
	if limit > maxExportLimit {
		return nil, status.Error(codes.InvalidArgument, "limit exceeds the export bound")
	}
	cursor := strings.TrimSpace(req.GetCursor())
	if len(cursor) > maxExportCursorLen {
		return nil, status.Error(codes.InvalidArgument, "cursor exceeds the export bound")
	}
	exporter, ok := selected.(domain.PaymentExporter)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("provider %q does not support payment exports", provider))
	}
	page, err := exporter.ExportPayments(ctx, domain.ExportRequest{
		From:   from,
		To:     to,
		Limit:  limit,
		Cursor: cursor,
	})
	if err != nil {
		return nil, statusFromError(err)
	}
	if len(page.Payments) > limit {
		s.log.Error("provider export exceeded the page limit",
			zap.String("provider", provider),
			zap.Int("rows", len(page.Payments)),
			zap.Int("limit", limit),
		)
		return nil, status.Error(codes.Internal, "provider export exceeded the page limit")
	}
	payments := make([]*connectorv1.CanonicalPaymentStatus, 0, len(page.Payments))
	for _, payment := range page.Payments {
		if payment.State == "" && payment.RawStatus == "" {
			s.log.Error("provider export row carries no state", zap.String("provider", provider))
			return nil, status.Error(codes.Internal, "provider export row carries no state")
		}
		if payment.State != "" {
			switch payment.State {
			case domain.StatePending, domain.StateDetected, domain.StateConfirmed, domain.StateFailed,
				domain.StateExpired, domain.StateUnderpaid, domain.StateOverpaid:
			default:
				s.log.Error("provider returned an unknown state in the export",
					zap.String("provider", provider),
					zap.String("state", string(payment.State)),
				)
				return nil, status.Error(codes.Internal, "provider returned an unknown state in the export")
			}
		}
		payments = append(payments, canonicalStatus(payment))
	}
	return &connectorv1.ListGatewayPaymentsResponse{
		Payments:   payments,
		NextCursor: page.NextCursor,
	}, nil
}

// validateCreateInvoice checks the request invariants before any provider call.
func validateCreateInvoice(ctx context.Context, req *connectorv1.CreateInvoiceRequest) error {
	if _, err := uuid.Parse(strings.TrimSpace(req.GetPaymentId())); err != nil {
		return status.Error(codes.InvalidArgument, "payment_id must be a uuid")
	}
	if key := strings.TrimSpace(req.GetIdempotencyKey()); key == "" || len(key) > maxIdempotencyKeyLen {
		return status.Error(codes.InvalidArgument, "idempotency_key is required and bounded")
	}
	if asset := strings.TrimSpace(req.GetAsset()); asset == "" || len(asset) > 16 {
		return status.Error(codes.InvalidArgument, "asset is required and bounded")
	}
	if network := strings.TrimSpace(req.GetNetwork()); network == "" || len(network) > 64 {
		return status.Error(codes.InvalidArgument, "network is required and bounded")
	}
	if req.GetAmount() == nil || req.GetAmount().GetMinorUnits() <= 0 {
		return status.Error(codes.InvalidArgument, "amount must be positive")
	}
	if req.GetAmount().GetDecimals() > money.MaxDecimals {
		// An unbounded scale reaches money.String() allocation: reject it here
		// and never build values beyond the platform limit (17.9 review).
		return status.Error(codes.InvalidArgument, "amount decimals over the limit")
	}
	if !strings.EqualFold(strings.TrimSpace(req.GetAmount().GetAsset()), strings.TrimSpace(req.GetAsset())) {
		// The provider invoice must be created in the routed asset, otherwise
		// settlement and reconciliation see a different asset than the payment.
		return status.Error(codes.InvalidArgument, "amount asset must match the request asset")
	}
	if req.GetExpiresAt() != nil {
		expiresAt := req.GetExpiresAt().AsTime()
		if !expiresAt.After(time.Now()) {
			return status.Error(codes.InvalidArgument, "expires_at must be in the future")
		}
		if expiresAt.After(time.Now().Add(maxInvoiceExpiry)) {
			return status.Error(codes.InvalidArgument, "expires_at exceeds the allowed horizon")
		}
	}
	if err := ctx.Err(); err != nil {
		return statusFromError(err)
	}
	return nil
}

// providerOf returns the normalized provider id of a gateway.
func providerOf(gateway domain.PaymentGateway) string {
	return strings.ToLower(strings.TrimSpace(gateway.Capabilities().Provider))
}

// requestHash fingerprints the trimmed invoice request for mapping conflict
// checks; the digest and the stored mapping must see the same field values, so
// whitespace-equivalent retries replay instead of conflicting.
func requestHash(req *connectorv1.CreateInvoiceRequest) string {
	expires := ""
	if req.GetExpiresAt() != nil {
		expires = req.GetExpiresAt().AsTime().UTC().Format(time.RFC3339Nano)
	}
	raw := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%s",
		strings.TrimSpace(req.GetPaymentId()),
		strings.TrimSpace(req.GetAsset()),
		strings.TrimSpace(req.GetNetwork()),
		strings.TrimSpace(req.GetAmount().GetAsset()),
		req.GetAmount().GetMinorUnits(), req.GetAmount().GetDecimals(), expires)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// canonicalStatus maps the domain status to the contract message. A missing
// amount evidence (empty asset) leaves received_amount unset on the wire: an
// absent field and a genuine zero must not be confused by consumers.
func canonicalStatus(payment domain.PaymentStatus) *connectorv1.CanonicalPaymentStatus {
	message := &connectorv1.CanonicalPaymentStatus{
		PaymentId:     payment.PaymentID,
		GatewayId:     payment.GatewayID,
		Provider:      payment.Provider,
		Status:        canonicalStatusValue(payment.State),
		Confirmations: payment.Confirmations,
		Txid:          payment.TXID,
		Network:       payment.Network,
		RawStatus:     payment.RawStatus,
	}
	if payment.ReceivedAmount.Asset != "" {
		message.ReceivedAmount = protoMoney(payment.ReceivedAmount)
	}
	if !payment.GatewayEventTS.IsZero() {
		message.GatewayEventTs = timestamppb.New(payment.GatewayEventTS)
	}
	return message
}

// canonicalStatusValue maps the domain state to the contract enum; unknown
// states are rejected by the caller before this point.
func canonicalStatusValue(state domain.State) connectorv1.CanonicalPaymentStatusValue {
	switch state {
	case domain.StatePending:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_PENDING
	case domain.StateDetected:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_DETECTED
	case domain.StateConfirmed:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED
	case domain.StateFailed:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_FAILED
	case domain.StateExpired:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_EXPIRED
	case domain.StateUnderpaid:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_UNDERPAID
	case domain.StateOverpaid:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_OVERPAID
	default:
		return connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_UNSPECIFIED
	}
}

// protoMoney converts the domain money to the contract message.
func protoMoney(value money.Money) *commonv1.Money {
	return &commonv1.Money{MinorUnits: value.MinorUnits, Asset: value.Asset, Decimals: value.Decimals}
}

// domainMoney converts the contract money to the domain value.
func domainMoney(value *commonv1.Money) money.Money {
	if value == nil {
		return money.Money{}
	}
	return money.Money{MinorUnits: value.GetMinorUnits(), Asset: value.GetAsset(), Decimals: value.GetDecimals()}
}

// statusFromError maps the connector error classification to gRPC codes
// (CBL-003 section 5): caller cancellation and deadlines keep their retryable
// codes, unavailable and overloaded stay retryable, a business rejection is a
// precondition failure, everything else is a sanitized internal error.
func statusFromError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "canceled")
	case errors.Is(err, domain.ErrProviderOverloaded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, domain.ErrProviderUnavailable), errors.Is(err, domain.ErrTemporary):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, domain.ErrRejected):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrUnsupportedNetwork), errors.Is(err, domain.ErrInvalidRoute),
		errors.Is(err, domain.ErrInvalidGatewayRequest), errors.Is(err, domain.ErrInvalidProviderInvoice):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrUnknownProvider), errors.Is(err, domain.ErrProviderNotFound), errors.Is(err, domain.ErrGatewayNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrProviderInvoiceConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
