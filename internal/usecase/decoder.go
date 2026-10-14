// Пакет usecase: сценарии указаны в doc.go.
package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-connector/internal/domain"
)

// WebhookDecoder переводит сырой вебхук провайдера в каноническое событие.
// Реализации предоставляют адаптеры провайдеров (3.2, 10.3) и sandbox-декодер.
type WebhookDecoder interface {
	// Provider задает имя провайдера, за который отвечает декодер.
	Provider() string
	// Decode разбирает тело вебхука и возвращает каноническое событие.
	Decode(ctx context.Context, raw *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error)
}

// DecoderRegistry хранит декодеры по имени провайдера.
type DecoderRegistry struct {
	decoders map[string]WebhookDecoder
}

// NewDecoderRegistry создает реестр; дубликат или пустое имя провайдера - ошибка конфигурации.
func NewDecoderRegistry(decoders ...WebhookDecoder) (*DecoderRegistry, error) {
	registry := &DecoderRegistry{decoders: make(map[string]WebhookDecoder, len(decoders))}
	for _, decoder := range decoders {
		if decoder == nil {
			return nil, errors.New("usecase: nil webhook decoder")
		}
		provider := strings.ToLower(strings.TrimSpace(decoder.Provider()))
		if provider == "" {
			return nil, errors.New("usecase: decoder with empty provider")
		}
		if _, exists := registry.decoders[provider]; exists {
			return nil, fmt.Errorf("usecase: duplicate decoder for provider %q", provider)
		}
		registry.decoders[provider] = decoder
	}
	return registry, nil
}

// Decoder возвращает декодер провайдера; поиск не зависит от регистра.
func (r *DecoderRegistry) Decoder(provider string) (WebhookDecoder, bool) {
	decoder, ok := r.decoders[strings.ToLower(strings.TrimSpace(provider))]
	return decoder, ok
}
