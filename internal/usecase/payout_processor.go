package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/storage"
)

// Topics and identifiers of the payout pipeline (17.11).
const (
	// TopicPayoutCommands specifies the topic of payout commands from the payout saga.
	TopicPayoutCommands = "payout.commands"
	// TopicGatewayPayoutEvents specifies the topic of payout results for the saga.
	TopicGatewayPayoutEvents = "gateway.payout.events"
	// ConsumerGroupPayouts specifies the default payout consumer group.
	ConsumerGroupPayouts = "cbl-connector-payouts"
	// maxPayoutMessageBytes limits the size of an incoming command.
	maxPayoutMessageBytes = 64 << 10
	// maxPayoutAddressBytes limits the destination address length.
	maxPayoutAddressBytes = 512
	// maxPayoutKeyBytes limits the idempotency key length.
	maxPayoutKeyBytes = 128
)

var (
	// errInvalidPayoutCommand reports an invalid command: it goes to the DLQ by design.
	errInvalidPayoutCommand = errors.New("usecase: invalid payout command")
	// payoutAssetPattern validates the asset code of an untrusted command.
	payoutAssetPattern = regexp.MustCompile(`^[A-Z0-9]{2,16}$`)
	// payoutNetworkPattern validates the network of an untrusted command.
	payoutNetworkPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$`)
)

// PayoutStore is the storage port of the command processor (17.11).
type PayoutStore interface {
	// LockPayoutTx serializes concurrent deliveries of one payout in the transaction.
	LockPayoutTx(ctx context.Context, tx *gorm.DB, payoutID string) error
	// Get returns the mapping of a platform payout.
	Get(ctx context.Context, payoutID string) (storage.ProviderPayoutRow, error)
	// InsertSubmittedTx records a submitted payout; false means a replay.
	InsertSubmittedTx(ctx context.Context, tx *gorm.DB, row storage.ProviderPayoutRow) (bool, error)
}

// RefundInvoiceMapping resolves the provider invoice of the original payment
// of a refund; the provider_invoices repository implements it (17.12).
type RefundInvoiceMapping interface {
	// ByPaymentID returns the mapping of the platform payment.
	ByPaymentID(ctx context.Context, paymentID string) (domain.ProviderInvoice, error)
}

// OutboxAppender writes event envelopes into the transactional outbox.
type OutboxAppender interface {
	// Append writes the envelope in the given transaction.
	Append(ctx context.Context, tx *gorm.DB, env outbox.Envelope) error
}

// PayoutRouter selects exactly one enabled provider whose capabilities cover
// the payout network, asset and amount range (17.11): deterministic by the
// smallest provider id, no failover after submission.
type PayoutRouter struct {
	ordered []payoutProvider
}

// payoutProvider couples a provider id with its gateway.
type payoutProvider struct {
	id      string
	gateway domain.PaymentGateway
}

// NewPayoutRouter builds the payout router over the enabled providers; an empty
// selection fails closed with the provider-unavailable class.
func NewPayoutRouter(gateways map[string]domain.PaymentGateway, allowed map[string]bool) (*PayoutRouter, error) {
	ids := make([]string, 0, len(gateways))
	for id := range gateways {
		if !allowed[strings.ToLower(strings.TrimSpace(id))] {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: no payout providers enabled", domain.ErrProviderUnavailable)
	}
	sort.Strings(ids)
	ordered := make([]payoutProvider, 0, len(ids))
	for _, id := range ids {
		gateway := gateways[id]
		if gateway == nil {
			return nil, fmt.Errorf("usecase: nil gateway for payout provider %q", id)
		}
		ordered = append(ordered, payoutProvider{id: id, gateway: gateway})
	}
	return &PayoutRouter{ordered: ordered}, nil
}

// Gateway returns the wrapped gateway of an enabled provider.
func (r *PayoutRouter) Gateway(provider string) (domain.PaymentGateway, bool) {
	for _, item := range r.ordered {
		if strings.EqualFold(item.id, provider) {
			return item.gateway, true
		}
	}
	return nil, false
}

// SelectPayout returns the first enabled provider whose capabilities cover the
// request; ErrUnsupportedNetwork means no provider can pay it.
func (r *PayoutRouter) SelectPayout(_ context.Context, req domain.PayoutRequest) (domain.PaymentGateway, string, error) {
	for _, provider := range r.ordered {
		if _, ok := payoutCapability(provider.gateway, req); ok {
			return provider.gateway, provider.id, nil
		}
	}
	return nil, "", fmt.Errorf("%w: payout network %q asset %q amount %d", domain.ErrUnsupportedNetwork,
		req.Network, req.Amount.Asset, req.Amount.MinorUnits)
}

// payoutCapability reports whether the provider supports the payout amount on
// the network; amount zero limits mean "no bound".
func payoutCapability(gateway domain.PaymentGateway, req domain.PayoutRequest) (domain.NetworkCapability, bool) {
	capabilities := gateway.Capabilities()
	for network, capability := range capabilities.Networks {
		if !strings.EqualFold(network, req.Network) || !strings.EqualFold(capability.Asset, req.Amount.Asset) {
			continue
		}
		// The scale must match before the raw minor units are compared:
		// comparing different scales would let a wrong amount through.
		scale := capability.MinAmount.Decimals
		if capability.MinAmount.MinorUnits == 0 {
			scale = capability.MaxAmount.Decimals
		}
		if (capability.MinAmount.MinorUnits > 0 || capability.MaxAmount.MinorUnits > 0) && scale != req.Amount.Decimals {
			return capability, false
		}
		if capability.MinAmount.MinorUnits > 0 && req.Amount.MinorUnits < capability.MinAmount.MinorUnits {
			return capability, false
		}
		if capability.MaxAmount.MinorUnits > 0 && req.Amount.MinorUnits > capability.MaxAmount.MinorUnits {
			return capability, false
		}
		return capability, true
	}
	return domain.NetworkCapability{}, false
}

// PayoutProcessor consumes payout.commands in the Guard transaction: it calls
// the selected provider, records the mapping and publishes the submitted event;
// a business rejection publishes a failed event, a temporary failure is retried
// through the retry topics (17.11).
type PayoutProcessor struct {
	router        *PayoutRouter
	store         PayoutStore
	outbox        OutboxAppender
	refundMapping RefundInvoiceMapping
	log           *zap.Logger
	clock         func() time.Time
}

// PayoutOption configures the PayoutProcessor.
type PayoutOption func(*PayoutProcessor)

// WithRefundMapping sets the mapping used to resolve the original provider
// invoice of a refund command (17.12).
func WithRefundMapping(mapping RefundInvoiceMapping) PayoutOption {
	return func(p *PayoutProcessor) {
		if mapping != nil {
			p.refundMapping = mapping
		}
	}
}

// WithPayoutClock sets the time source used by the events; tests use it.
func WithPayoutClock(clock func() time.Time) PayoutOption {
	return func(p *PayoutProcessor) {
		if clock != nil {
			p.clock = clock
		}
	}
}

// NewPayoutProcessor creates a PayoutProcessor.
func NewPayoutProcessor(router *PayoutRouter, store PayoutStore, appender OutboxAppender, log *zap.Logger, opts ...PayoutOption) *PayoutProcessor {
	if log == nil {
		log = zap.NewNop()
	}
	processor := &PayoutProcessor{router: router, store: store, outbox: appender, log: log, clock: time.Now}
	for _, opt := range opts {
		opt(processor)
	}
	return processor
}

// Handle processes one payout command inside the Guard transaction.
func (p *PayoutProcessor) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	if len(msg.Value) > maxPayoutMessageBytes {
		return fmt.Errorf("%w: message too large", errInvalidPayoutCommand)
	}
	var command eventsv1.PayoutCommand
	if err := proto.Unmarshal(msg.Value, &command); err != nil {
		return fmt.Errorf("%w: unmarshal: %w", errInvalidPayoutCommand, err)
	}
	payoutID, request, err := validatePayoutCommand(&command)
	if err != nil {
		return err
	}
	// One delivery of a payout at a time: the advisory lock plus the mapping
	// re-check below stop two concurrent commands (different event ids) from
	// both calling the provider (17.11, review B1).
	if err := p.store.LockPayoutTx(ctx, tx, payoutID); err != nil {
		return err
	}
	refundInvoiceID, refundErr := validateRefundMarker(command.GetRefundInvoiceId())
	if refundErr != nil {
		return refundErr
	}

	recorded, err := p.store.Get(ctx, payoutID)
	switch {
	case err == nil:
		p.log.Info("payout command replayed",
			zap.String("payout_id", payoutID),
			zap.String("provider", recorded.Provider),
			zap.String("state", recorded.State))
		return nil
	case !errors.Is(err, storage.ErrProviderPayoutNotFound):
		return fmt.Errorf("usecase: payout mapping of %s: %w", payoutID, err)
	}
	if refundInvoiceID != "" {
		return p.handleRefund(ctx, tx, payoutID, refundInvoiceID, request, msg)
	}

	gateway, provider, err := p.router.SelectPayout(ctx, request)
	if err != nil {
		if errors.Is(err, domain.ErrUnsupportedNetwork) {
			p.log.Warn("payout has no capable provider", zap.String("payout_id", payoutID), zap.Error(err))
			return p.publishFailed(ctx, tx, payoutID, "", "no provider supports the payout", msg.Headers)
		}
		return fmt.Errorf("usecase: select provider for payout %s: %w", payoutID, err)
	}

	payout, err := gateway.CreatePayout(ctx, request)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrRejected), errors.Is(err, domain.ErrUnsupportedNetwork),
			errors.Is(err, domain.ErrGatewayNotFound):
			// The provider text may contain internals: the event gets a fixed
			// reason, the details stay in the log.
			p.log.Warn("payout rejected by provider",
				zap.String("payout_id", payoutID),
				zap.String("provider", provider),
				zap.Error(err))
			return p.publishFailed(ctx, tx, payoutID, provider, "provider rejected the payout", msg.Headers)
		default:
			return fmt.Errorf("usecase: create payout %s at %s: %w", payoutID, provider, err)
		}
	}
	gatewayPayoutID := sanitizeIdentifier(payout.GatewayPayoutID, 128)
	if gatewayPayoutID == "" {
		return fmt.Errorf("usecase: provider %s returned no usable payout id for %s", provider, payoutID)
	}
	if payout.Provider != "" && !strings.EqualFold(payout.Provider, provider) {
		return fmt.Errorf("usecase: provider %s returned a payout attributed to %s", provider, payout.Provider)
	}

	now := p.clock().UTC()
	row := storage.ProviderPayoutRow{
		PayoutID:        payoutID,
		Provider:        provider,
		GatewayPayoutID: &gatewayPayoutID,
		AmountMinor:     request.Amount.MinorUnits,
		Asset:           request.Amount.Asset,
		Decimals:        int(request.Amount.Decimals),
		Network:         request.Network,
		AddressHash:     addressHash(request.Address),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	inserted, err := p.store.InsertSubmittedTx(ctx, tx, row)
	if err != nil {
		return err
	}
	if !inserted {
		p.log.Info("payout already recorded by a concurrent delivery", zap.String("payout_id", payoutID))
		return nil
	}
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payoutID,
		GatewayPayoutId: gatewayPayoutID,
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
		Provider:        provider,
	}
	envelope, err := buildPayoutEnvelope("gateway.payout.submitted", event, msg.Headers, now)
	if err != nil {
		return err
	}
	if err := p.outbox.Append(ctx, tx, envelope); err != nil {
		return fmt.Errorf("usecase: append payout submitted event: %w", err)
	}
	p.log.Info("payout submitted to provider",
		zap.String("payout_id", payoutID),
		zap.String("provider", provider),
		zap.String("gateway_payout_id", gatewayPayoutID))
	return nil
}

// validateRefundMarker canonicalizes the optional refund marker of a command:
// a non-empty value is the platform payment id being refunded (cf-4.2).
func validateRefundMarker(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	parsed, err := uuid.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("%w: refund_invoice_id", errInvalidPayoutCommand)
	}
	return parsed.String(), nil
}

// handleRefund processes a refund command: the provider comes from the original
// invoice mapping (the provider that accepted the payment), the adapter refunds
// API is called and the mapping records the refund marker (17.12).
func (p *PayoutProcessor) handleRefund(ctx context.Context, tx *gorm.DB, payoutID, refundInvoiceID string, request domain.PayoutRequest, msg inbox.Message) error {
	if p.refundMapping == nil {
		return fmt.Errorf("usecase: refund mapping is not configured for payout %s", payoutID)
	}
	invoice, err := p.refundMapping.ByPaymentID(ctx, refundInvoiceID)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrProviderInvoiceNotFound):
			p.log.Warn("refund invoice has no provider mapping",
				zap.String("payout_id", payoutID),
				zap.String("refund_invoice_id", refundInvoiceID))
			return p.publishFailed(ctx, tx, payoutID, "", "refund invoice has no provider mapping", msg.Headers)
		case errors.Is(err, storage.ErrProviderInvoiceAmbiguous):
			p.log.Warn("refund invoice has multiple provider mappings",
				zap.String("payout_id", payoutID),
				zap.String("refund_invoice_id", refundInvoiceID))
			return p.publishFailed(ctx, tx, payoutID, "", "refund invoice has multiple provider mappings", msg.Headers)
		default:
			return fmt.Errorf("usecase: refund mapping of %s: %w", payoutID, err)
		}
	}
	provider := strings.ToLower(strings.TrimSpace(invoice.Provider))
	gateway, enabled := p.router.Gateway(provider)
	if !enabled {
		p.log.Warn("refund provider is not enabled",
			zap.String("payout_id", payoutID),
			zap.String("provider", provider))
		return p.publishFailed(ctx, tx, payoutID, provider, "provider refunds are not enabled", msg.Headers)
	}
	refunder, ok := gateway.(domain.Refunder)
	if !ok {
		p.log.Warn("refund provider does not support refunds",
			zap.String("payout_id", payoutID),
			zap.String("provider", provider))
		return p.publishFailed(ctx, tx, payoutID, provider, "provider does not support refunds", msg.Headers)
	}
	refund, err := refunder.CreateRefund(ctx, domain.RefundRequest{
		IdempotencyKey:  request.IdempotencyKey,
		PayoutID:        payoutID,
		RefundInvoiceID: refundInvoiceID,
		PaymentID:       refundInvoiceID,
		GatewayID:       invoice.GatewayID,
		Amount:          request.Amount,
		Address:         request.Address,
	})
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrRefundsUnsupported):
			p.log.Warn("refund provider does not support refunds",
				zap.String("payout_id", payoutID),
				zap.String("provider", provider))
			return p.publishFailed(ctx, tx, payoutID, provider, "provider does not support refunds", msg.Headers)
		case errors.Is(err, domain.ErrRejected), errors.Is(err, domain.ErrUnsupportedNetwork),
			errors.Is(err, domain.ErrGatewayNotFound):
			p.log.Warn("refund rejected by provider",
				zap.String("payout_id", payoutID),
				zap.String("provider", provider),
				zap.Error(err))
			return p.publishFailed(ctx, tx, payoutID, provider, "provider rejected the refund", msg.Headers)
		default:
			return fmt.Errorf("usecase: create refund %s at %s: %w", payoutID, provider, err)
		}
	}
	gatewayRefundID := sanitizeIdentifier(refund.GatewayRefundID, 128)
	if gatewayRefundID == "" {
		return fmt.Errorf("usecase: provider %s returned no usable refund id for %s", provider, payoutID)
	}
	if refund.Provider != "" && !strings.EqualFold(refund.Provider, provider) {
		return fmt.Errorf("usecase: provider %s returned a refund attributed to %s", provider, refund.Provider)
	}
	now := p.clock().UTC()
	row := storage.ProviderPayoutRow{
		PayoutID:        payoutID,
		Provider:        provider,
		GatewayPayoutID: &gatewayRefundID,
		AmountMinor:     request.Amount.MinorUnits,
		Asset:           request.Amount.Asset,
		Decimals:        int(request.Amount.Decimals),
		Network:         request.Network,
		AddressHash:     addressHash(request.Address),
		RefundInvoiceID: &refundInvoiceID,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	inserted, err := p.store.InsertSubmittedTx(ctx, tx, row)
	if err != nil {
		return err
	}
	if !inserted {
		p.log.Info("refund already recorded by a concurrent delivery", zap.String("payout_id", payoutID))
		return nil
	}
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payoutID,
		GatewayPayoutId: gatewayRefundID,
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
		Provider:        provider,
	}
	envelope, err := buildPayoutEnvelope("gateway.payout.submitted", event, msg.Headers, now)
	if err != nil {
		return err
	}
	if err := p.outbox.Append(ctx, tx, envelope); err != nil {
		return fmt.Errorf("usecase: append refund submitted event: %w", err)
	}
	p.log.Info("refund submitted to provider",
		zap.String("payout_id", payoutID),
		zap.String("refund_invoice_id", refundInvoiceID),
		zap.String("provider", provider),
		zap.String("gateway_refund_id", gatewayRefundID))
	return nil
}

// publishFailed publishes a failed event for a payout that never reached the
// provider (no mapping is recorded: there is no submission to poll).
func (p *PayoutProcessor) publishFailed(ctx context.Context, tx *gorm.DB, payoutID, provider, reason string, incoming map[string]string) error {
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId: payoutID,
		Type:     eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
		Provider: provider,
		Reason:   reason,
	}
	envelope, err := buildPayoutEnvelope("gateway.payout.failed", event, incoming, p.clock().UTC())
	if err != nil {
		return err
	}
	if err := p.outbox.Append(ctx, tx, envelope); err != nil {
		return fmt.Errorf("usecase: append payout failed event: %w", err)
	}
	p.log.Info("payout failed before submission",
		zap.String("payout_id", payoutID),
		zap.String("provider", provider),
		zap.String("reason", reason))
	return nil
}

// validatePayoutCommand validates the untrusted command and builds the gateway
// request: identifiers, bounds and the money scale.
func validatePayoutCommand(command *eventsv1.PayoutCommand) (string, domain.PayoutRequest, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(command.GetPayoutId()))
	if err != nil {
		return "", domain.PayoutRequest{}, fmt.Errorf("%w: payout_id", errInvalidPayoutCommand)
	}
	amount := command.GetAmount()
	if amount == nil || amount.GetMinorUnits() <= 0 {
		return "", domain.PayoutRequest{}, fmt.Errorf("%w: amount must be positive", errInvalidPayoutCommand)
	}
	asset := strings.ToUpper(strings.TrimSpace(amount.GetAsset()))
	if !payoutAssetPattern.MatchString(asset) {
		return "", domain.PayoutRequest{}, fmt.Errorf("%w: amount asset", errInvalidPayoutCommand)
	}
	value, err := money.New(amount.GetMinorUnits(), asset, amount.GetDecimals())
	if err != nil {
		return "", domain.PayoutRequest{}, fmt.Errorf("%w: amount: %w", errInvalidPayoutCommand, err)
	}
	network := strings.TrimSpace(command.GetNetwork())
	if !payoutNetworkPattern.MatchString(network) {
		return "", domain.PayoutRequest{}, fmt.Errorf("%w: network", errInvalidPayoutCommand)
	}
	address := strings.TrimSpace(command.GetAddress())
	if address == "" || len(address) > maxPayoutAddressBytes || hasControlRunes(address) {
		return "", domain.PayoutRequest{}, fmt.Errorf("%w: address", errInvalidPayoutCommand)
	}
	key := strings.TrimSpace(command.GetIdempotencyKey())
	if key == "" || len(key) > maxPayoutKeyBytes || hasControlRunes(key) {
		return "", domain.PayoutRequest{}, fmt.Errorf("%w: idempotency_key", errInvalidPayoutCommand)
	}
	return parsed.String(), domain.PayoutRequest{
		IdempotencyKey: key,
		PayoutID:       parsed.String(),
		Amount:         value,
		Network:        network,
		Address:        address,
	}, nil
}

// addressHash returns the sha256 hex of the destination: the mapping keeps a
// correlation digest instead of the address itself.
func addressHash(address string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(address)))
	return hex.EncodeToString(digest[:])
}

// hasControlRunes reports control characters in untrusted text.
func hasControlRunes(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7F {
			return true
		}
	}
	return false
}

// sanitizeIdentifier bounds an untrusted provider identifier: an over-long or
// control-containing value is unusable and becomes empty.
func sanitizeIdentifier(value string, limit int) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || len(trimmed) > limit || hasControlRunes(trimmed) {
		return ""
	}
	return trimmed
}

// sanitizeReason cleans an untrusted provider text for an event: invalid UTF-8
// and control runes are dropped and the result is rune-bounded, because a
// protobuf string must be valid UTF-8 and a failed marshal would leave the
// payout unresolved on every poll.
func sanitizeReason(value string) string {
	cleaned := strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7F {
			return -1
		}
		return r
	}, strings.ToValidUTF8(value, ""))
	cleaned = strings.TrimSpace(cleaned)
	runes := []rune(cleaned)
	if len(runes) > 256 {
		cleaned = string(runes[:256]) + "..."
	}
	return cleaned
}

// buildPayoutEnvelope builds the gateway payout event envelope: key payout_id,
// provider header, traceparent propagated from the command.
func buildPayoutEnvelope(eventType string, event *eventsv1.GatewayPayoutEvent, incoming map[string]string, now time.Time) (outbox.Envelope, error) {
	payload, err := proto.Marshal(event)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: marshal payout event: %w", err)
	}
	headers := map[string]string{
		outbox.HeaderProducer:      ProducerConnector,
		outbox.HeaderSchemaVersion: schemaVersion,
		outbox.HeaderOccurredAt:    now.UTC().Format(time.RFC3339Nano),
		"provider":                 event.GetProvider(),
	}
	if traceparent := strings.TrimSpace(incoming[outbox.HeaderTraceparent]); traceparentPattern.MatchString(traceparent) {
		headers[outbox.HeaderTraceparent] = traceparent
	}
	envelope, err := outbox.NewEnvelope(eventType, TopicGatewayPayoutEvents, event.GetPayoutId(), headers, payload)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: build payout envelope: %w", err)
	}
	return envelope, nil
}

// payoutFee renders the provider fee of a status as a Money value; a zero fee
// means the provider reported none.
func payoutFee(fee money.Money) *commonv1.Money {
	if fee.MinorUnits <= 0 {
		return nil
	}
	return &commonv1.Money{MinorUnits: fee.MinorUnits, Asset: fee.Asset, Decimals: fee.Decimals}
}
