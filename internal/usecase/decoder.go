// Package usecase: the use cases are specified in doc.go.
package usecase

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

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

// MissingDecoders returns the configured providers that require a decoder but
// have none: a provider is required when it is configured (the runtime
// predicate, for example a registered gateway) and its manifest is not
// fetch-mandatory. Sandbox providers are excluded: without a decoder their
// deliveries fail closed at runtime (ErrUnknownProvider, retry/DLQ). The result
// is sorted so the startup error and log are deterministic.
func MissingDecoders(manifests []domain.ProviderManifest, configured func(provider string) bool, decoders []WebhookDecoder) []string {
	available := make(map[string]struct{}, len(decoders))
	for _, decoder := range decoders {
		if decoder == nil {
			continue
		}
		available[strings.ToLower(strings.TrimSpace(decoder.Provider()))] = struct{}{}
	}
	missing := make([]string, 0)
	for _, manifest := range manifests {
		provider := strings.ToLower(strings.TrimSpace(manifest.ID))
		if manifest.Sandbox || manifest.FetchMandatory {
			continue
		}
		if configured != nil && !configured(provider) {
			continue
		}
		if _, ok := available[provider]; !ok {
			missing = append(missing, provider)
		}
	}
	sort.Strings(missing)
	if len(missing) == 0 {
		return nil
	}
	return missing
}
