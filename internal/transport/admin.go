package transport

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// AdminServer serves billing.connector.v1.ConnectorAdminService (CBL-12.2).
type AdminServer struct {
	connectorv1.UnimplementedConnectorAdminServiceServer
	admin *usecase.AdminRouting
	log   *zap.Logger
}

// NewAdminServer creates the administrative gRPC server.
func NewAdminServer(admin *usecase.AdminRouting, log *zap.Logger) *AdminServer {
	if log == nil {
		log = zap.NewNop()
	}
	return &AdminServer{admin: admin, log: log}
}

// GetRoutingRules returns the current revision and rules.
func (s *AdminServer) GetRoutingRules(ctx context.Context, _ *connectorv1.GetRoutingRulesRequest) (*connectorv1.GetRoutingRulesResponse, error) {
	revision, rules, err := s.admin.GetRules(ctx)
	if err != nil {
		return nil, s.mapError(ctx, err)
	}
	return &connectorv1.GetRoutingRulesResponse{
		Revision: revision,
		Items:    toProtoRules(rules),
	}, nil
}

// PutRoutingRules replaces the configuration with optimistic concurrency.
func (s *AdminServer) PutRoutingRules(ctx context.Context, req *connectorv1.PutRoutingRulesRequest) (*connectorv1.PutRoutingRulesResponse, error) {
	revision, stored, err := s.admin.PutRules(ctx, req.GetRevision(), fromProtoRules(req.GetItems()))
	if err != nil {
		return nil, s.mapError(ctx, err)
	}
	return &connectorv1.PutRoutingRulesResponse{
		Revision: revision,
		Items:    toProtoRules(stored),
	}, nil
}

// DisableProvider disables a provider manually; a repeat is idempotent.
func (s *AdminServer) DisableProvider(ctx context.Context, req *connectorv1.DisableProviderRequest) (*connectorv1.DisableProviderResponse, error) {
	already, err := s.admin.DisableProvider(ctx, req.GetProvider(), req.GetReason())
	if err != nil {
		return nil, s.mapError(ctx, err)
	}
	return &connectorv1.DisableProviderResponse{AlreadyDisabled: already}, nil
}

// mapError converts a domain error into a gRPC status; internal details never
// reach the client.
func (s *AdminServer) mapError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidAdminInput), errors.Is(err, domain.ErrInvalidRoute):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrRouteRevisionConflict):
		return status.Error(codes.FailedPrecondition, "routing revision conflict")
	case errors.Is(err, domain.ErrProviderNotFound):
		return status.Error(codes.NotFound, "provider not found")
	default:
		s.log.Error("admin request failed", zap.Error(err), zap.String("method", "connector-admin"))
		return status.Error(codes.Internal, "internal error")
	}
}

// toProtoRules converts domain rules into the contract form.
func toProtoRules(rules []domain.RouteRule) []*connectorv1.RoutingRuleAdmin {
	items := make([]*connectorv1.RoutingRuleAdmin, 0, len(rules))
	for i := range rules {
		rule := rules[i]
		items = append(items, &connectorv1.RoutingRuleAdmin{
			Provider:  rule.Provider,
			Operation: rule.Operation,
			Networks:  rule.Networks,
			Assets:    rule.Assets,
			// RouteRule.Validate bounds priority and weight far below the int32 range.
			Priority:  int32(rule.Priority), //nolint:gosec // G115: bounded by RouteRule.Validate
			Weight:    int32(rule.Weight),   //nolint:gosec // G115: bounded by RouteRule.Validate
			MinAmount: rule.MinAmount,
			MaxAmount: rule.MaxAmount,
			Enabled:   rule.Enabled,
		})
	}
	return items
}

// fromProtoRules converts contract rules into the domain form.
func fromProtoRules(items []*connectorv1.RoutingRuleAdmin) []domain.RouteRule {
	rules := make([]domain.RouteRule, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		rules = append(rules, domain.RouteRule{
			Provider:  item.GetProvider(),
			Operation: item.GetOperation(),
			Networks:  item.GetNetworks(),
			Assets:    item.GetAssets(),
			Priority:  int(item.GetPriority()),
			Weight:    int(item.GetWeight()),
			MinAmount: item.GetMinAmount(),
			MaxAmount: item.GetMaxAmount(),
			Enabled:   item.GetEnabled(),
		})
	}
	return rules
}
