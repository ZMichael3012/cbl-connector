// Package usecase: the use cases are specified in doc.go.
package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-connector/internal/domain"
)

// WebhookDecoder converts a raw provider webhook into a canonical event.
// Implementations are provided by provider adapters (3.2, 10.3) and the sandbox decoder.
type WebhookDecoder interface {
	// Provider returns the name of the provider served by the decoder.
	Provider() string
	// Decode parses the webhook body and returns the canonical event.
	Decode(ctx context.Context, raw *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error)
}

// DecoderRegistry stores decoders by provider name.
type DecoderRegistry struct {
	decoders map[string]WebhookDecoder
}

// NewDecoderRegistry creates the registry; a duplicate or empty provider name is a configuration error.
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

// Decoder returns the provider decoder; the lookup is case-insensitive.
func (r *DecoderRegistry) Decoder(provider string) (WebhookDecoder, bool) {
	decoder, ok := r.decoders[strings.ToLower(strings.TrimSpace(provider))]
	return decoder, ok
}
