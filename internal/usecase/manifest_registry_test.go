package usecase_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

func TestManifestRegistryBuiltins(t *testing.T) {
	t.Parallel()
	registry, err := usecase.NewManifestRegistry(domain.BuiltinManifests()...)
	require.NoError(t, err)

	ids := registry.IDs()
	require.Equal(t, []string{"bitpay", "btcpay", "coingate", "mock", "nowpayments"}, ids)

	manifest, ok := registry.Lookup(" coingate ")
	require.True(t, ok)
	require.Equal(t, "CoinGate", manifest.DisplayName)

	_, ok = registry.Lookup("unknown")
	require.False(t, ok)

	listed := registry.List()
	require.Len(t, listed, len(ids))
	require.Equal(t, "bitpay", listed[0].ID)
}

func TestManifestRegistryRejectsDuplicates(t *testing.T) {
	t.Parallel()
	manifest := domain.BuiltinManifests()[0]
	_, err := usecase.NewManifestRegistry(manifest, manifest)
	require.ErrorIs(t, err, domain.ErrInvalidManifest)
}

func TestManifestRegistryRejectsInvalid(t *testing.T) {
	t.Parallel()
	invalid := domain.BuiltinManifests()[0]
	invalid.Version = "v99"
	_, err := usecase.NewManifestRegistry(invalid)
	require.ErrorIs(t, err, domain.ErrInvalidManifest)
}
