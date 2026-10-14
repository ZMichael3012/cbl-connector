package usecase_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/usecase"
)

// fakeDecoder реализует usecase.WebhookDecoder для тестов реестра и нормализатора.
type fakeDecoder struct {
	provider string
	event    domain.CanonicalEvent
	err      error
}

// Provider возвращает имя провайдера.
func (d *fakeDecoder) Provider() string { return d.provider }

// Decode возвращает заранее заданное событие или ошибку.
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
	assert.Equal(t, first, found, "провайдер ищется без учета регистра")

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
	require.NoError(t, err, "пустой реестр допустим")
	_, ok := registry.Decoder("mock")
	assert.False(t, ok)
}
