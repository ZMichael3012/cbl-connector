package usecase

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/cbl/cbl-connector/internal/domain"
)

// ManifestRegistry holds provider manifests (SPI v1) and serves lookups; the
// admin contour unions its IDs into the known-provider set (CBL-12.2 FR-3),
// and the router/adapters resolve providers through it (17.2).
type ManifestRegistry struct {
	mu   sync.RWMutex
	byID map[string]domain.ProviderManifest
}

// NewManifestRegistry builds a registry from manifests; every manifest is
// validated, and a duplicate ID is an error because duplicates hide
// configuration mistakes (a reload builds a fresh registry).
func NewManifestRegistry(manifests ...domain.ProviderManifest) (*ManifestRegistry, error) {
	registry := &ManifestRegistry{byID: make(map[string]domain.ProviderManifest, len(manifests))}
	for _, manifest := range manifests {
		if err := manifest.Validate(); err != nil {
			return nil, fmt.Errorf("usecase: register manifest: %w", err)
		}
		key := normalizeProviderID(manifest.ID)
		if _, duplicate := registry.byID[key]; duplicate {
			return nil, fmt.Errorf("%w: provider %q is already registered", domain.ErrInvalidManifest, manifest.ID)
		}
		registry.byID[key] = manifest
	}
	return registry, nil
}

// Lookup returns the manifest of a provider; the id is normalized the same way
// as on registration, so casing can never bypass manifest lookups.
func (r *ManifestRegistry) Lookup(id string) (domain.ProviderManifest, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	manifest, ok := r.byID[normalizeProviderID(id)]
	return manifest, ok
}

// normalizeProviderID normalizes provider identifiers of registry keys.
func normalizeProviderID(id string) string {
	return strings.ToLower(strings.TrimSpace(id))
}

// IDs returns the registered provider identifiers in a stable order; the
// admin contour uses it as the registry side of the known-provider union.
func (r *ManifestRegistry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// List returns the registered manifests ordered by ID.
func (r *ManifestRegistry) List() []domain.ProviderManifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	manifests := make([]domain.ProviderManifest, 0, len(r.byID))
	for _, manifest := range r.byID {
		manifests = append(manifests, manifest)
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].ID < manifests[j].ID })
	return manifests
}
