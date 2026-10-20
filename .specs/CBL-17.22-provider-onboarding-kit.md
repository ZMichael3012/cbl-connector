# CBL-17.22 - Provider onboarding kit and CI guard (cbl-connector, cbl-mock-gateway)

Task: CBL-17.22 in `CBL-004` (E17, W5-SCALE), depends on 17.5 (done). Repositories:
`cbl-connector` (this spec) and `cbl-mock-gateway` (profile onboarding note and parity
test). Status: in progress.

## 1. Problem and scope

A new provider touches several places: the adapter package, the manifest, the runtime
factory, the mock profile, the conformance suite and the e2e scenario. The rules exist
spread across specs and AGENTS.md, and nothing prevents a provider-scoped change from
reaching into the core (`usecase`, `transport`, `storage`) or a core change from importing
adapter packages. A reviewer has to remember the boundaries.

In scope: a documented onboarding checklist, a compile-checked adapter scaffold, an
import-boundary guard that runs with `go test`, a manifest/factory parity test, and the
mock-gateway profile checklist with its own parity test. Out of scope: a code generator,
new providers, changing the SPI, and any runtime behavior change.

## 2. Architecture

```
cbl-connector/
  docs/provider-onboarding.md          # the checklist and the file map
  internal/providers/scaffold/         # compile-checked adapter skeleton (not registered)
    adapter.go
    adapter_test.go
  internal/onboarding/
    guard_test.go                      # import boundaries, manifest/factory parity
cbl-mock-gateway/
  AGENTS.md                            # "Adding a provider profile" checklist
  internal/domain/profiles_parity_test.go
```

Scaffold contract: `scaffold.New(Config)` validates a base URL (http/https, no userinfo),
an API token and a non-empty capability set, exactly like the production adapters; the six
`domain.PaymentGateway` methods return `ErrNotImplemented` and the package is never
imported by the runtime. Optional interfaces (`LookupGateway`, `Refunder`,
`RefundStatusGateway`, `PaymentExporter`) are intentionally not implemented: adding one is
an explicit opt-in.

TD-CONNECTOR-1 (new): the guard is import-based, not diff-based. It fails the build when a
provider package imports the core or a core package imports providers, which is stronger
than checking a staged diff and also catches a later refactor. The only allowed importer
of `internal/providers/*` is `internal/providerruntime` (the composition point).

## 3. Guard rules

G1. No package under `internal/providers/` imports `internal/usecase`,
    `internal/transport`, `internal/storage`, `internal/gateway` or
    `internal/providerruntime`.
G2. No core package (`internal/usecase`, `internal/transport`, `internal/storage`,
    `internal/domain`, `internal/gateway`) imports `internal/providers/*` or
    `internal/providerruntime`.
G3. The only package outside `internal/providers/*` that imports provider packages is
    `internal/providerruntime`; any other importer fails.
G4. Every non-sandbox manifest has exactly one in-process factory, every sandbox manifest
    has none, and every factory id is a registered manifest.
G5. The mock gateway emulates every connector real provider (`nowpayments`, `coingate`,
    `bitpay`, `btcpay`); the parity test names the set so a new connector provider forces
    a mock update.

## 4. Onboarding checklist (docs/provider-onboarding.md)

1. Freeze the wire contract: .proto/OpenAPI if the provider needs new fields; otherwise
   record the provider API facts and fixtures in the task spec.
2. Copy `internal/providers/scaffold` to `internal/providers/<provider>`, implement the
   six methods, keep optional interfaces off until they are tested.
3. Add the manifest in `domain.BuiltinManifests` and validate the strategy, secrets,
   operations and rate limit.
4. Register the in-process factory in `providerruntime.BuiltinFactories` and the env
   variables in the deploy compose and `.env.example`.
5. Add the conformance fixture set and run `conformance.Run`; add adapter unit tests.
6. Add the mock profile (cbl-mock-gateway) or an explicit reason why the provider is not
   emulated, then run the profile e2e.
7. Add an `E2E.md` scenario (or extend an existing one) and run a compose scenario; record
   artifacts under `test/e2e/.runs/<mark>/`.
8. Update AGENTS.md (task journal, trade-offs, known issues) and this checklist when the
   process changes.

## 5. Test plan

| ID | Case | Expectation |
| :-- | :-- | :-- |
| O1 | provider package imports `usecase` | guard fails |
| O2 | core package imports a provider package | guard fails |
| O3 | a non-runtime package imports a provider package | guard fails |
| O4 | manifest/factory parity over the built-ins | pass |
| O5 | scaffold constructor: valid config | adapter built, capabilities returned |
| O6 | scaffold constructor: invalid URL, missing token, empty capabilities | error |
| O7 | scaffold methods | `ErrNotImplemented` |
| O8 | mock profile parity: all connector providers emulated | pass |

O1 - O3 are proven by the guard itself against a mutation fixture: the test exposes a
`checkImports` helper and runs it on synthetic import sets (so the guard logic is tested
without breaking the real tree), plus the real-tree assertions.

Integration: not applicable (tests and documentation only).
E2E: `E2E_Connector_Smoke` on the compose stand to prove the binary is unchanged by the
kit (the scaffold is not linked); the mock profile set is unchanged.

## 6. Red evidence

Recorded 20.10.2026: `go test ./internal/providers/scaffold` failed with `no non-test Go
files in .../internal/providers/scaffold`; the scaffold test referenced undefined
`scaffold.New`/`Config`/`ErrNotImplemented`. The guard and the mock parity test are
enforcement tests: they were green on the current tree, and the guard rules O1 - O3 are
additionally proven against synthetic mutated import graphs in `TestImportGuardRules`.

## 7. Results and review closure (20.10.2026)

- Guard matrix O1 - O8 green: synthetic O1 - O3 violations caught, real-tree import
  boundaries clean, manifest/factory parity holds, scaffold matrix green, mock profile
  parity green.
- Unit: connector `go test ./...` green, coverage 82.2% with `-coverpkg`; mock unit and
  in-process e2e green.
- Compose e2e: `E2E_Connector_Smoke` PASS, mark `20261020t074148z` (migrations v9,
  manifests 5, runtime factory path, admin revision 0); artifacts in the run directory.
- Gates: golangci-lint 0 issues in both repositories, go vet clean, em dash check green.
- Review pass 1 (no-context): the checklist matches the guard rules and the real file
  map; the scaffold states the optional-interface opt-in and has no generator.
- Review pass 2 (architecture): G1 - G3 hold; `internal/providerruntime` is the only
  composition importer; the guard logic is tested independently of the tree.
- Review pass 3 (with-context, blocking): the scaffold must not change the binary or the
  startup. Closed: `main` imports `providerruntime` only, the scaffold is not linked,
  the compose smoke passes and no env/log behavior changes.
- Review pass 4 (security): the scaffold URL validation rejects userinfo and
  query/fragment (the production adapter rule); no secrets in errors; the guard runs
  locally without network.
- MINOR: the guard inspects the non-test import graph (the production binary); test-only
  imports are outside the boundary, documented in the checklist.
