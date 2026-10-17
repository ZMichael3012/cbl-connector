package usecase

import (
	"context"
	"fmt"
	"strings"

	"github.com/cbl/cbl-connector/internal/domain"
)

// GatewayStatusFetcher adapts the registered provider gateways to the
// normalizer fetch-to-verify port (17.2): fetch_required deliveries resolve
// their authoritative status through the adapter of the provider.
type GatewayStatusFetcher struct {
	gateways map[string]domain.PaymentGateway
}

// NewGatewayStatusFetcher builds the fetcher over the registered gateways.
func NewGatewayStatusFetcher(gateways map[string]domain.PaymentGateway) (PaymentStatusFetcher, error) {
	if len(gateways) == 0 {
		return nil, fmt.Errorf("usecase: no provider gateways registered")
	}
	copied := make(map[string]domain.PaymentGateway, len(gateways))
	for id, gateway := range gateways {
		if gateway == nil {
			return nil, fmt.Errorf("usecase: nil gateway for provider %q", id)
		}
		copied[id] = gateway
	}
	return &GatewayStatusFetcher{gateways: copied}, nil
}

// FetchPaymentStatus resolves the provider adapter and reads the status.
func (f *GatewayStatusFetcher) FetchPaymentStatus(ctx context.Context, provider, gatewayID string) (domain.PaymentStatus, error) {
	gateway, ok := f.gateways[strings.TrimSpace(provider)]
	if !ok {
		return domain.PaymentStatus{}, fmt.Errorf("%w: %q", domain.ErrUnknownProvider, provider)
	}
	return gateway.GetPayment(ctx, gatewayID)
}
