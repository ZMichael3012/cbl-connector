// Package providerruntime is the seam between the connector core and one
// provider runtime (CBL-17.20): the core consumes gateways and manifests, the
// factories own configuration and construction, and a per-provider runtime can
// later be extracted behind the same contract without core changes.
package providerruntime

import (
	"errors"
	"fmt"
	"regexp"

	"go.uber.org/zap"

	"github.com/cbl/cbl-connector/internal/domain"
	"github.com/cbl/cbl-connector/internal/gateway"
)

// ErrInvalidRegistry reports an invalid factory registration or a factory
// contract violation.
var ErrInvalidRegistry = errors.New("providerruntime: invalid registry")

// providerIDPattern bounds provider identifiers registered by factories.
var providerIDPattern = regexp.MustCompile(`^[a-z0-9_-]{2,32}$`)

// ProviderRuntime is one provider runtime: the gateway the core calls, the
// manifest that describes the provider and the resources it owns.
type ProviderRuntime interface {
	// Provider returns the provider id of the runtime.
	Provider() string
	// Manifest returns the validated provider manifest.
	Manifest() domain.ProviderManifest
	// Gateway returns the gateway the core calls; it includes the resilience
	// and rate-limit wrappers of the runtime.
	Gateway() domain.PaymentGateway
	// Close releases the resources owned by the runtime.
	Close() error
}

// Config carries the process dependencies injected into the factories.
type Config struct {
	// Getenv reads process configuration; required.
	Getenv func(string) string
	// Log is the process logger; required.
	Log *zap.Logger
	// Enabled filters providers before their factory runs; nil enables all.
	Enabled func(provider string) bool
	// Manifests resolves a provider manifest; required (fail closed on a miss).
	Manifests func(provider string) (domain.ProviderManifest, bool)
	// Limiters builds the rate limiter of a provider; required only for a
	// provider whose manifest declares a limit.
	Limiters func(provider string, spec domain.RateLimitSpec) (gateway.Limiter, error)
}

// Factory builds one provider runtime from the process configuration. A nil
// runtime with a nil error means the provider is not configured on this stand.
type Factory struct {
	// Provider is the provider id the factory owns.
	Provider string
	// Build constructs the runtime.
	Build func(Config) (ProviderRuntime, error)
}

// Registry builds runtimes from registered factories.
type Registry struct {
	factories []Factory
}

// NewRegistry validates the factories and returns a registry; it rejects empty
// or malformed provider ids, nil build functions and duplicate providers.
func NewRegistry(factories ...Factory) (*Registry, error) {
	seen := make(map[string]struct{}, len(factories))
	for _, factory := range factories {
		if !providerIDPattern.MatchString(factory.Provider) {
			return nil, fmt.Errorf("%w: invalid provider id %q", ErrInvalidRegistry, factory.Provider)
		}
		if factory.Build == nil {
			return nil, fmt.Errorf("%w: provider %q has no build function", ErrInvalidRegistry, factory.Provider)
		}
		if _, duplicate := seen[factory.Provider]; duplicate {
			return nil, fmt.Errorf("%w: provider %q is registered twice", ErrInvalidRegistry, factory.Provider)
		}
		seen[factory.Provider] = struct{}{}
	}
	return &Registry{factories: append([]Factory(nil), factories...)}, nil
}

// Build constructs every enabled runtime in factory order. A disabled provider
// is filtered before its factory runs; an unconfigured factory (nil, nil) is
// skipped. On an error the already-built runtimes are closed and the error is
// joined with any close error.
func (r *Registry) Build(cfg Config) ([]ProviderRuntime, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: nil registry", ErrInvalidRegistry)
	}
	if cfg.Getenv == nil || cfg.Log == nil || cfg.Manifests == nil {
		return nil, fmt.Errorf("%w: getenv, log and manifests are required", ErrInvalidRegistry)
	}
	built := make([]ProviderRuntime, 0, len(r.factories))
	for _, factory := range r.factories {
		if cfg.Enabled != nil && !cfg.Enabled(factory.Provider) {
			continue
		}
		runtime, err := factory.Build(cfg)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("providerruntime: build %s: %w", factory.Provider, err), Close(built))
		}
		if runtime == nil {
			continue
		}
		if runtime.Provider() != factory.Provider {
			return nil, errors.Join(
				fmt.Errorf("%w: factory %q built runtime %q", ErrInvalidRegistry, factory.Provider, runtime.Provider()),
				runtime.Close(),
				Close(built),
			)
		}
		built = append(built, runtime)
	}
	return built, nil
}

// Close closes the runtimes in reverse build order and joins their errors.
func Close(runtimes []ProviderRuntime) error {
	errs := make([]error, 0, len(runtimes))
	for i := len(runtimes) - 1; i >= 0; i-- {
		if runtimes[i] == nil {
			continue
		}
		if err := runtimes[i].Close(); err != nil {
			errs = append(errs, fmt.Errorf("providerruntime: close %s: %w", runtimes[i].Provider(), err))
		}
	}
	return errors.Join(errs...)
}
