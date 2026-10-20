package usecase_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// namedDecoder is a decoder with a fixed provider name.
type namedDecoder struct {
	provider string
}

// Provider implements usecase.WebhookDecoder.
func (d namedDecoder) Provider() string { return d.provider }

// Decode implements usecase.WebhookDecoder.
func (d namedDecoder) Decode(context.Context, *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error) {
	return domain.CanonicalEvent{}, nil
}

// legacyManifest is a valid non-fetch-mandatory provider manifest.
func legacyManifest(id string) domain.ProviderManifest {
	return domain.ProviderManifest{
		Version:            domain.ManifestVersionV1,
		ID:                 id,
		DisplayName:        "Legacy " + id,
		Operations:         []string{domain.OperationInvoice},
		VerifyStrategy:     domain.VerifyStrategyNone,
		CapabilitiesSource: domain.CapabilitiesSourceStatic,
	}
}

func TestMissingDecoders(t *testing.T) {
	t.Parallel()

	manifests := append(domain.BuiltinManifests(), legacyManifest("legacy"), legacyManifest("alpha"))
	allConfigured := func(string) bool { return true }
	noneConfigured := func(string) bool { return false }
	only := func(provider string) func(string) bool {
		return func(candidate string) bool { return candidate == provider }
	}

	cases := []struct {
		id         string
		configured func(string) bool
		decoders   []usecase.WebhookDecoder
		want       []string
	}{
		{
			id:         "D1 fetch-mandatory only, no decoders",
			configured: only("btcpay"),
			want:       nil,
		},
		{
			id:         "D2 legacy provider needs a decoder",
			configured: only("legacy"),
			want:       []string{"legacy"},
		},
		{
			id:         "D3 legacy provider has its decoder",
			configured: only("legacy"),
			decoders:   []usecase.WebhookDecoder{namedDecoder{provider: "legacy"}},
			want:       nil,
		},
		{
			id:         "D4 legacy provider is not configured",
			configured: noneConfigured,
			want:       nil,
		},
		{
			id:         "D5 sandbox provider needs no decoder at startup",
			configured: only("mock"),
			want:       nil,
		},
		{
			id:         "D6 sorted deterministic result",
			configured: allConfigured,
			want:       []string{"alpha", "legacy"},
		},
		{
			id:         "D6 a decoder for another provider does not help",
			configured: only("legacy"),
			decoders:   []usecase.WebhookDecoder{namedDecoder{provider: "mock"}},
			want:       []string{"legacy"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, usecase.MissingDecoders(manifests, tc.configured, tc.decoders))
		})
	}
}

func TestMissingDecodersNilConfigured(t *testing.T) {
	t.Parallel()

	// A nil predicate means every manifest is considered configured; the
	// helper stays total for the table tests and future callers.
	missing := usecase.MissingDecoders([]domain.ProviderManifest{legacyManifest("legacy")}, nil, nil)
	assert.Equal(t, []string{"legacy"}, missing)
}

func TestMissingDecodersNormalizesNames(t *testing.T) {
	t.Parallel()

	manifests := []domain.ProviderManifest{legacyManifest("Legacy")}
	missing := usecase.MissingDecoders(manifests, func(string) bool { return true },
		[]usecase.WebhookDecoder{namedDecoder{provider: " LEGACY "}})
	assert.Empty(t, missing, "manifest ids and decoder names are compared case-insensitively")
	require.True(t, true)
}
