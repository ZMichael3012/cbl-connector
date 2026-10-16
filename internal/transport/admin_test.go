package transport_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	connectorv1 "github.com/cbl/cbl-protos/gen/go/billing/connector/v1"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/transport"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// fakeAdminStore substitutes the administrative storage.
type fakeAdminStore struct {
	revision   int64
	rules      []domain.RouteRule
	names      []string
	replaceErr error
	disableErr error
}

// Revision implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) Revision(context.Context) (int64, error) { return s.revision, nil }

// LoadAll implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) LoadAll(context.Context) ([]domain.RouteRule, error) { return s.rules, nil }

// ReplaceWithRevision implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) ReplaceWithRevision(_ context.Context, _ int64, rules []domain.RouteRule) (int64, error) {
	if s.replaceErr != nil {
		return 0, s.replaceErr
	}
	s.rules = rules
	s.revision++
	return s.revision, nil
}

// DisableProvider implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) DisableProvider(context.Context, string, string) (bool, error) {
	if s.disableErr != nil {
		return false, s.disableErr
	}
	return false, nil
}

// ProviderNames implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) ProviderNames(context.Context) ([]string, error) { return s.names, nil }

// ListDisabled implements usecase.AdminRoutingStore.
func (s *fakeAdminStore) ListDisabled(context.Context) ([]string, error) { return nil, nil }

// newAdmin builds the server over the fake store.
func newAdmin(store *fakeAdminStore) *transport.AdminServer {
	admin := usecase.NewAdminRouting(store, nil, nil, nil)
	return transport.NewAdminServer(admin, nil)
}

func TestGetRoutingRules(t *testing.T) {
	t.Parallel()

	store := &fakeAdminStore{
		revision: 3,
		rules: []domain.RouteRule{{
			Provider:  "mock",
			Operation: "invoice",
			Networks:  []string{"tron"},
			Assets:    []string{"USDT"},
			Priority:  1,
			Weight:    5,
			MinAmount: 100,
			MaxAmount: 900,
			Enabled:   true,
		}},
	}
	server := newAdmin(store)

	response, err := server.GetRoutingRules(context.Background(), &connectorv1.GetRoutingRulesRequest{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, response.GetRevision())
	require.Len(t, response.GetItems(), 1)
	item := response.GetItems()[0]
	assert.Equal(t, "mock", item.GetProvider())
	assert.Equal(t, "invoice", item.GetOperation())
	assert.Equal(t, []string{"tron"}, item.GetNetworks())
	assert.Equal(t, []string{"USDT"}, item.GetAssets())
	assert.EqualValues(t, 1, item.GetPriority())
	assert.EqualValues(t, 5, item.GetWeight())
	assert.EqualValues(t, 100, item.GetMinAmount())
	assert.EqualValues(t, 900, item.GetMaxAmount())
	assert.True(t, item.GetEnabled())
}

func TestPutRoutingRulesRoundTrip(t *testing.T) {
	t.Parallel()

	server := newAdmin(&fakeAdminStore{})
	request := &connectorv1.PutRoutingRulesRequest{
		Revision: 0,
		Items: []*connectorv1.RoutingRuleAdmin{{
			Provider: "mock",
			Weight:   2,
			Enabled:  true,
		}},
	}
	response, err := server.PutRoutingRules(context.Background(), request)
	require.NoError(t, err)
	assert.EqualValues(t, 1, response.GetRevision())
	require.Len(t, response.GetItems(), 1)
	assert.Equal(t, "mock", response.GetItems()[0].GetProvider())
}

func TestAdminErrorMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "invalid input", err: domain.ErrInvalidAdminInput, want: codes.InvalidArgument},
		{name: "invalid route", err: domain.ErrInvalidRoute, want: codes.InvalidArgument},
		{name: "revision conflict", err: domain.ErrRouteRevisionConflict, want: codes.FailedPrecondition},
		{name: "provider not found", err: domain.ErrProviderNotFound, want: codes.NotFound},
		{name: "internal", err: errors.New("db down"), want: codes.Internal},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			putServer := newAdmin(&fakeAdminStore{replaceErr: tc.err})
			_, err := putServer.PutRoutingRules(context.Background(), &connectorv1.PutRoutingRulesRequest{})
			require.Error(t, err)
			assert.Equal(t, tc.want, status.Code(err))

			disableServer := newAdmin(&fakeAdminStore{names: []string{"mock"}, disableErr: tc.err})
			_, err = disableServer.DisableProvider(context.Background(), &connectorv1.DisableProviderRequest{Provider: "mock"})
			require.Error(t, err)
			assert.Equal(t, tc.want, status.Code(err))
			if tc.want == codes.Internal {
				assert.NotContains(t, err.Error(), "db down", "internal details are hidden")
			}
		})
	}
}

func TestDisableProviderConversion(t *testing.T) {
	t.Parallel()

	server := newAdmin(&fakeAdminStore{names: []string{"mock"}})
	response, err := server.DisableProvider(context.Background(), &connectorv1.DisableProviderRequest{
		Provider: "mock",
		Reason:   "maintenance",
	})
	require.NoError(t, err)
	assert.False(t, response.GetAlreadyDisabled())

	_, err = server.DisableProvider(context.Background(), &connectorv1.DisableProviderRequest{Provider: "unknown"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPutRoutingRulesSkipsNilItems(t *testing.T) {
	t.Parallel()

	server := newAdmin(&fakeAdminStore{})
	response, err := server.PutRoutingRules(context.Background(), &connectorv1.PutRoutingRulesRequest{
		Items: []*connectorv1.RoutingRuleAdmin{nil, {Provider: "mock", Weight: 1, Enabled: true}},
	})
	require.NoError(t, err)
	assert.Len(t, response.GetItems(), 1)
}
