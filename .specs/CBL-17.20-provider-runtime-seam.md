# CBL-17.20 - Provider runtime seam (cbl-connector)

Task: CBL-17.20 in `CBL-004` (E17, W5-SCALE), depends on 17.2 (done). Repository:
`cbl-connector`. Status: in progress.

## 1. Problem and scope

Provider construction lives inline in `cmd/connector/main.go`: four environment blocks
build adapters, then a loop wraps them with rate limiters. The core (normalizer, router,
transport, payout) already works through `domain.PaymentGateway` and manifests, but the
process has no single contract for "one provider runtime", so extracting a provider into
its own process would require touching the core wiring and duplicating construction logic.

In scope: an internal `providerruntime` package with the `ProviderRuntime` contract, a
registry that builds runtimes from factories, and the in-process factories for the four
providers (bitpay, btcpay, coingate, nowpayments). Out of scope: behavior changes, new
providers, decoder wiring (17.28), capability caching, and any network call at startup
(config parsing only).

## 2. Architecture

```
cbl-connector/
  internal/providerruntime/
    runtime.go      # ProviderRuntime, Config, Factory, Registry, Build, Close
    builtin.go      # BuiltinFactories: bitpay, btcpay, coingate, nowpayments
    env.go          # capabilitiesFromEnv, durationEnv (moved from main)
    runtime_test.go # registry contract with fake factories
    builtin_test.go # env matrix against the real adapters
    env_test.go     # capability/env parsing matrix
  cmd/connector/main.go # builds the registry, maps runtimes to gateways
```

Public contract:

```go
type ProviderRuntime interface {
    Provider() string
    Manifest() domain.ProviderManifest
    Gateway() domain.PaymentGateway
    Close() error
}

type Config struct {
    Getenv    func(string) string
    Log       *zap.Logger
    Enabled   func(provider string) bool
    Manifests func(provider string) (domain.ProviderManifest, bool)
    Limiters  func(provider string, spec domain.RateLimitSpec) (gateway.Limiter, error)
}

type Factory struct {
    Provider string
    Build    func(Config) (ProviderRuntime, error)
}

func NewRegistry(factories ...Factory) (*Registry, error)
func (r *Registry) Build(cfg Config) ([]ProviderRuntime, error)
func Close(runtimes []ProviderRuntime) error
```

Design decisions:

- The factory owns configuration parsing and adapter construction; the core consumes only
  `ProviderRuntime`, so an out-of-process runtime (factory returning a client-side gateway)
  plugs in without core changes. The extraction path is a different factory list.
- `Build` applies the `Enabled` allowlist before a factory runs, so a disabled provider
  never parses its environment.
- A factory returning `(nil, nil)` means "not configured on this stand" (empty base URL);
  the registry skips it and does not treat it as an error.
- A factory that returns a runtime with a provider different from its registration is a
  programming error: the runtime is closed and `Build` fails.
- On a build error, already-built runtimes are closed in reverse order; close errors are
  joined with the primary error. `Close` joins errors from all runtimes.
- The in-process runtime owns its resilience wrapper and rate limiter: a provider with
  `RateLimit.Burst > 0` gets `gateway.Limited` around `gateway.New`. The limiter factory is
  injected because Redis is a process dependency, not a provider one.
- Manifests come from the manifest registry through `Config.Manifests`; a missing manifest
  is a startup error (fail closed), never a silent unconfigured provider.
- `Close` on the in-process runtime returns nil: the adapters own HTTP clients with no
  background goroutines and no closable transport. The method exists for runtimes that do
  (a future out-of-process runtime owns a connection).

## 3. Validation rules

V1. Factory provider ids match `^[a-z0-9_-]{2,32}$`; duplicates are rejected.
V2. `Build` requires `Getenv`, `Log` and `Manifests`; `Limiters` is required only when a
    runtime needs it and its absence fails that factory with a clear error.
V3. `Enabled` nil means every registered provider is enabled.
V4. Build results are ordered by factory registration (stable, unlike the previous map).
V5. Provider construction errors (invalid base URL, missing secrets, bad capabilities) fail
    the build; the process keeps the current fail-closed startup behavior.

## 4. Data flow

`main` builds the manifest registry -> `providerruntime.NewRegistry(BuiltinFactories()...)`
-> `Build(Config)` -> runtimes -> gateways map -> normalizer, router, payout pipeline and
the money path. The normalizer still resolves webhook decoders separately (17.28).

## 5. Error taxonomy

`ErrInvalidRegistry` for registry errors (duplicate/invalid factory, wrong provider id) and
wrapped factory errors with the provider id: `providerruntime: build coingate: ...`.
No panics.

## 6. Test plan

Unit matrix (fake factories and a fake gateway; in-process tests use generated keys and
capabilities fixtures, no network):

| ID | Case | Expectation |
| :-- | :-- | :-- |
| R1 | two factories, both enabled | two runtimes in factory order |
| R2 | allowlist selects one provider | only it is built; the other factory never runs |
| R3 | factory answers nil, nil | skipped, no error |
| R4 | second factory fails | first runtime closed, error returned |
| R5 | duplicate factory provider | NewRegistry error |
| R6 | empty provider or nil Build | NewRegistry error |
| R7 | factory returns a different provider | runtime closed, error |
| R8 | Close with one error and two runtimes | reverse order, both closed, error joined |
| R9 | in-process, no base URLs | no runtimes, no error |
| R10 | in-process, valid btcpay env | provider btcpay, manifest, gateway, no limiter call |
| R11 | in-process, valid coingate and nowpayments env | both built, limiter called for each |
| R12 | in-process, valid bitpay env (dev PEM) | built, limiter called |
| R13 | in-process, invalid capabilities JSON | error names the provider |
| R14 | in-process, missing manifest | error |
| R15 | in-process, base URL without secret | adapter error propagated |
| R16 | capabilitiesFromEnv: valid, unknown field, empty, missing fields, zero decimals, negative bounds, inverted bounds, duplicate network, trailing JSON | matching results |
| R17 | durationEnv: empty, invalid, non-positive, valid | fallback or parsed value |

Integration: not applicable, the package performs no I/O.
E2E: `E2E_Connector_Smoke` (the service starts with the default BTCPay runtime, migrations,
normalizer, admin contour) and `E2E_Connector_BTCPayProfile` (the runtime built from env
serves CreateInvoice/GetGatewayPayment against the mock profile). No scenario changes; the
task must keep them green.

## 7. Red evidence

Recorded 20.10.2026 before the implementation: `go test ./internal/providerruntime`
failed with `no non-test Go files in .../internal/providerruntime`; the registry and
env tests referenced undefined `providerruntime.*`, `capabilitiesFromEnv` and
`durationEnv`.

## 8. Results and review closure (20.10.2026)

- Unit: the registry matrix R1 - R8 and the in-process matrix R9 - R17 green; package
  coverage 87.9%, repository unit coverage 82.1% with `-coverpkg=./internal/...`.
- Integration: `go test -tags integration ./internal/...` green (testcontainers where the
  existing suites require them).
- E2E testcontainers: `go test -tags e2e ./test/e2e/` green (normalizer and payout
  pipeline unchanged).
- Compose e2e: mark `20261020t072334z` PASS for `E2E_Connector_Smoke` (migrations v9,
  manifests 5, `btcpay adapter configured`, normalizer started, admin revision 0) and
  `E2E_Connector_BTCPayProfile` (32-hex invoice, PENDING/New, mock simulate, CONFIRMED/
  Settled); artifacts in `test/e2e/.runs/20261020t072334z/`.
- Gates: golangci-lint 0 issues, go vet clean, em dash check green.
- Review pass 1 (no-context): the spec matrix and the tests agree; the factory contract
  includes the provider-mismatch rule and the partial-build close.
- Review pass 2 (architecture): the core packages do not import adapters after the
  refactor; `internal/providerruntime` is the only composition package; the extraction
  path is a factory-list replacement. No import cycle.
- Review pass 3 (with-context, blocking): the refactor must not change startup behavior.
  Closed: env names, log lines (`adapter configured`, `webhook normalizer started`,
  migrations), the allowlist check, fail-closed startup and the limiter composition are
  unchanged; Redis now connects before the providers (no observable difference).
- Review pass 4 (security): the allowlist is applied before any env parsing, a missing
  manifest fails closed, adapter errors are secret-free by contract and only the base URL
  is logged; no new inputs or network calls at startup.
- MINOR: `capabilitiesFromEnv`/`durationEnv` were previously untested in `main`; the move
  added unit matrices (R16/R17).
