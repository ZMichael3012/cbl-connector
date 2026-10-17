package usecase

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
)

// CapabilitySource exposes the capabilities of one provider adapter; the source
// owns the refresh of its provider-side data (API polling with its own cache).
type CapabilitySource interface {
	// Provider returns the provider id of the source.
	Provider() string
	// Capabilities returns the current capabilities of the provider.
	Capabilities() domain.Capabilities
}

// CapabilityCache stores the aggregated capability snapshot between refreshes
// (Redis in production); a cache miss or failure falls back to live sources.
type CapabilityCache interface {
	// Load returns the cached snapshot; found is false on a miss.
	Load(ctx context.Context) ([]domain.Capabilities, bool, error)
	// Save stores the snapshot with the TTL.
	Save(ctx context.Context, capabilities []domain.Capabilities, ttl time.Duration) error
}

// CapabilitiesLoader serves GetCapabilities (17.2): the provider-filtered view
// comes straight from the adapter, the full view goes through the cache with a
// live fallback, so a Redis outage only costs latency, never correctness.
type CapabilitiesLoader struct {
	sources []CapabilitySource
	cache   CapabilityCache
	ttl     time.Duration
	log     *zap.Logger

	mu   sync.Mutex
	last []domain.Capabilities
}

// NewCapabilitiesLoader creates the loader; duplicate providers and empty ids
// are errors, a nil cache serves live reads only.
func NewCapabilitiesLoader(sources []CapabilitySource, cache CapabilityCache, ttl time.Duration, log *zap.Logger) (*CapabilitiesLoader, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("%w: invalid capabilities ttl", domain.ErrInvalidGatewayRequest)
	}
	if log == nil {
		log = zap.NewNop()
	}
	seen := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if source == nil {
			return nil, fmt.Errorf("%w: nil capability source", domain.ErrInvalidGatewayRequest)
		}
		id := strings.TrimSpace(source.Provider())
		if !validProviderID(id) {
			return nil, fmt.Errorf("%w: invalid capability source provider %q", domain.ErrInvalidGatewayRequest, source.Provider())
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("%w: duplicate capability source %q", domain.ErrInvalidGatewayRequest, id)
		}
		seen[id] = struct{}{}
	}
	return &CapabilitiesLoader{sources: sources, cache: cache, ttl: ttl, log: log}, nil
}

// List returns the capabilities of one provider or of all providers ordered by
// provider id; every row set is provider-qualified (CF-4), so consumers can
// filter by asset, network and provider.
func (l *CapabilitiesLoader) List(ctx context.Context, provider string) ([]domain.Capabilities, error) {
	provider = strings.TrimSpace(provider)
	if provider != "" {
		for _, source := range l.sources {
			if strings.TrimSpace(source.Provider()) == provider {
				return []domain.Capabilities{normalizedCapabilities(source)}, nil
			}
		}
		return nil, fmt.Errorf("%w: %q", domain.ErrProviderNotFound, provider)
	}
	if l.cache != nil {
		cached, found, err := l.cache.Load(ctx)
		if err != nil {
			l.log.Warn("capabilities cache read failed", zap.Error(err))
		} else if found {
			if l.cachedSnapshotTrusted(cached) {
				l.remember(cached)
				return cached, nil
			}
			// A semantically corrupted payload (forged providers) is treated as
			// a miss: the cache is never trusted over the registered sources.
			l.log.Warn("capabilities cache payload rejected", zap.Int("rows", len(cached)))
		}
	}
	aggregated := make([]domain.Capabilities, 0, len(l.sources))
	for _, source := range l.sources {
		aggregated = append(aggregated, normalizedCapabilities(source))
	}
	sortCapabilities(aggregated)
	l.remember(aggregated)
	if l.cache != nil {
		if err := l.cache.Save(ctx, aggregated, l.ttl); err != nil {
			// The cache is an optimization: a save failure is logged, not fatal.
			l.log.Warn("capabilities cache save failed", zap.Error(err))
		}
	}
	return aggregated, nil
}

// cachedSnapshotTrusted checks a cached snapshot before serving it: every row
// must carry a valid provider that is registered as a source, and the row set
// must not be empty. Semantically corrupted payloads fall back to live sources
// (the route:config read-time validation discipline, T-CONNECTOR-08).
func (l *CapabilitiesLoader) cachedSnapshotTrusted(cached []domain.Capabilities) bool {
	if len(cached) == 0 {
		return false
	}
	registered := make(map[string]struct{}, len(l.sources))
	for _, source := range l.sources {
		registered[strings.TrimSpace(source.Provider())] = struct{}{}
	}
	for _, row := range cached {
		if _, ok := registered[row.Provider]; !ok {
			return false
		}
	}
	return true
}

// Snapshot returns the last aggregated view without touching the cache; a
// process that has never listed gets an empty snapshot.
func (l *CapabilitiesLoader) Snapshot() []domain.Capabilities {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.last)
}

// remember stores the last snapshot for diagnostics and tests.
func (l *CapabilitiesLoader) remember(capabilities []domain.Capabilities) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.last = slices.Clone(capabilities)
}

// normalizedCapabilities stamps the source provider id onto the row set so the
// aggregated list stays provider-qualified even if an adapter forgets the field.
func normalizedCapabilities(source CapabilitySource) domain.Capabilities {
	capabilities := source.Capabilities()
	capabilities.Provider = strings.TrimSpace(source.Provider())
	return capabilities
}

// sortCapabilities orders the aggregated view by provider id.
func sortCapabilities(capabilities []domain.Capabilities) {
	sort.Slice(capabilities, func(i, j int) bool { return capabilities[i].Provider < capabilities[j].Provider })
}

// validProviderID checks the provider code format of a capability source.
func validProviderID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		b := id[i]
		switch {
		case b >= 'a' && b <= 'z', b >= '0' && b <= '9', b == '-', b == '_':
		default:
			return false
		}
	}
	return true
}
