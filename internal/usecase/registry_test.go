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

// fakeDecoder implements usecase.WebhookDecoder for registry and normalizer tests.
type fakeDecoder struct {
	provider string
	event    domain.CanonicalEvent
	err      error
}

// Provider returns the provider name.
func (d *fakeDecoder) Provider() string { return d.provider }

// Decode returns the preconfigured event or error.
func (d *fakeDecoder) Decode(_ context.Context, _ *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error) {
	if d.err != nil {
		return domain.CanonicalEvent{}, d.err
	}
	return d.event, nil
}

func TestDecoderRegistry(t *testing.T) {
	t.Parallel()

	first := &fakeDecoder{provider: "Mock"}
	second := &fakeDecoder{provider: "other"}
	registry, err := usecase.NewDecoderRegistry(first, second)
	require.NoError(t, err)

	found, ok := registry.Decoder("mock")
	require.True(t, ok)
	assert.Equal(t, first, found, "the provider is looked up case-insensitively")

	found, ok = registry.Decoder(" other ")
	require.True(t, ok)
	assert.Equal(t, second, found)

	_, ok = registry.Decoder("unknown")
	assert.False(t, ok)
}

func TestDecoderRegistryRejectsDuplicate(t *testing.T) {
	t.Parallel()

	_, err := usecase.NewDecoderRegistry(&fakeDecoder{provider: "mock"}, &fakeDecoder{provider: "MOCK"})
	require.Error(t, err)
}

func TestDecoderRegistryRejectsEmptyProvider(t *testing.T) {
	t.Parallel()

	_, err := usecase.NewDecoderRegistry(&fakeDecoder{provider: "  "})
	require.Error(t, err)

	registry, err := usecase.NewDecoderRegistry()
	require.NoError(t, err, "an empty registry is allowed")
	_, ok := registry.Decoder("mock")
	assert.False(t, ok)
}
