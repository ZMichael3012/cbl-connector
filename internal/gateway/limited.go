package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cbl/cbl-kit/ratelimit"

	"github.com/cbl/cbl-connector/internal/domain"
)

// Limiter is the limiter port of the decorator; the kit ratelimit limiters
// (Redis with the local fallback) implement it.
type Limiter interface {
	// Allow consumes one token of the key bucket.
	Allow(ctx context.Context, key string) (ratelimit.Decision, error)
	// Pause empties the key bucket and delays the refill, honoring a provider
	// Retry-After answer.
	Pause(ctx context.Context, key string, d time.Duration) error
}

// Operation keys of the provider bucket: the kit Redis limiter prefixes them
// with rl:provider:, so the final key matches CBL-003 section 7
// (rl:provider:<provider>:<op>).
const (
	limitOperationInvoice = "invoice"
	limitOperationStatus  = "status"
	limitOperationPayout  = "payout"
	limitOperationRefund  = "refund"
)

// providerLimitOperations lists the buckets paused on a provider-wide
// Retry-After.
var providerLimitOperations = []string{limitOperationInvoice, limitOperationStatus, limitOperationPayout, limitOperationRefund}

// Limited guards provider calls with the per-provider token bucket (17.2,
// COMMON-2): a denied token returns ErrProviderOverloaded without touching the
// adapter, and a provider Retry-After answer is honored through PauseProvider.
// It forwards the optional SPI v1 interfaces: the assertions succeed and the
// wrapped gateway reports whether it really supports the operation.
type Limited struct {
	next    domain.PaymentGateway
	limiter Limiter
	key     string
}

// NewLimited wraps a gateway; the provider id becomes the limiter key scope.
func NewLimited(next domain.PaymentGateway, limiter Limiter, provider string) (*Limited, error) {
	if next == nil || limiter == nil {
		return nil, fmt.Errorf("%w: limited requires a gateway and a limiter", domain.ErrInvalidGatewayRequest)
	}
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return nil, fmt.Errorf("%w: limited requires a provider id", domain.ErrInvalidGatewayRequest)
	}
	return &Limited{next: next, limiter: limiter, key: provider}, nil
}

// CreateInvoice implements the port with the invoice bucket.
func (l *Limited) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error) {
	if err := l.guard(ctx, limitOperationInvoice); err != nil {
		return domain.Invoice{}, err
	}
	return l.next.CreateInvoice(ctx, req)
}

// GetPayment implements the port with the status bucket.
func (l *Limited) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error) {
	if err := l.guard(ctx, limitOperationStatus); err != nil {
		return domain.PaymentStatus{}, err
	}
	return l.next.GetPayment(ctx, gatewayID)
}

// CreatePayout implements the port with the payout bucket.
func (l *Limited) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error) {
	if err := l.guard(ctx, limitOperationPayout); err != nil {
		return domain.Payout{}, err
	}
	return l.next.CreatePayout(ctx, req)
}

// GetPayout implements the port with the status bucket.
func (l *Limited) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error) {
	if err := l.guard(ctx, limitOperationStatus); err != nil {
		return domain.PayoutStatus{}, err
	}
	return l.next.GetPayout(ctx, gatewayPayoutID)
}

// VerifyWebhook implements the port without a token: inbound webhooks are
// limited by the ingress (T-WH-06), not by the provider outbound bucket.
func (l *Limited) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error) {
	return l.next.VerifyWebhook(headers, rawBody)
}

// LookupByOrderID implements the optional SPI v1 interface: the wrapped gateway
// reports ErrRejected when it does not support the recovery lookup.
func (l *Limited) LookupByOrderID(ctx context.Context, orderID string) (domain.PaymentStatus, error) {
	lookup, ok := l.next.(domain.LookupGateway)
	if !ok {
		return domain.PaymentStatus{}, fmt.Errorf("%w: provider %q does not support the order lookup", domain.ErrRejected, l.key)
	}
	if err := l.guard(ctx, limitOperationStatus); err != nil {
		return domain.PaymentStatus{}, err
	}
	return lookup.LookupByOrderID(ctx, orderID)
}

// CreateRefund implements the optional SPI v1 interface: the wrapped gateway
// reports ErrRejected when it has no refunds API (the manual path, decision 8).
func (l *Limited) CreateRefund(ctx context.Context, req domain.RefundRequest) (domain.Refund, error) {
	refunder, ok := l.next.(domain.Refunder)
	if !ok {
		return domain.Refund{}, fmt.Errorf("%w: provider %q does not support refunds", domain.ErrRejected, l.key)
	}
	if err := l.guard(ctx, limitOperationRefund); err != nil {
		return domain.Refund{}, err
	}
	return refunder.CreateRefund(ctx, req)
}

// Capabilities implements the port without a token: it is a local read.
func (l *Limited) Capabilities() domain.Capabilities {
	return l.next.Capabilities()
}

// Provider forwards the manifest id of the wrapped gateway (capability source port).
func (l *Limited) Provider() string {
	if l.next == nil {
		return ""
	}
	return l.next.Capabilities().Provider
}

// PauseProvider empties every operation bucket of the provider for d: adapters
// call it with the provider Retry-After after a 429/503 answer.
func (l *Limited) PauseProvider(ctx context.Context, d time.Duration) error {
	var firstErr error
	for _, operation := range providerLimitOperations {
		if err := l.limiter.Pause(ctx, l.key+":"+operation, d); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return fmt.Errorf("gateway: pause provider %q: %w", l.key, firstErr)
	}
	return nil
}

// guard consumes a token of the operation bucket. A limiter failure is
// fail-open by design: the limiter protects the provider (a lost token risks a
// provider 429, which PauseProvider then absorbs), and it must never block the
// money path on its own storage problem. The error is swallowed only here; the
// wiring layer observes it through the kit metrics port.
func (l *Limited) guard(ctx context.Context, operation string) error {
	decision, err := l.limiter.Allow(ctx, l.key+":"+operation)
	if err != nil {
		return nil
	}
	if !decision.Allowed {
		return fmt.Errorf("%w: %s:%s retry in %s", domain.ErrProviderOverloaded, l.key, operation, decision.RetryAfter.Round(time.Millisecond))
	}
	return nil
}
