# Provider onboarding checklist (cbl-connector)

Use this checklist when adding a provider. The rules are enforced by tests where possible;
the rest is a review checklist. Task: CBL-17.22.

## 1. Freeze the wire contract

- Record the provider API facts in the task spec: URLs, auth, signing, statuses, amount
  scale, webhook scheme and replay protection.
- Add `.proto`/OpenAPI fields only when the provider integration needs new contract data;
  otherwise no contract change. The adapter must not invent statuses or amounts: an
  unknown status arrives as `UNSPECIFIED` with `raw_status`.
- Put fixtures under `internal/providers/<provider>/testdata` or the adapter tests; the
  conformance suite (CBL-17.5) consumes them.

## 2. Copy the scaffold

```text
cp -r internal/providers/scaffold internal/providers/<provider>
```

- Rename the package and the `Provider()` value.
- Implement the six `domain.PaymentGateway` methods. The scaffold returns
  `ErrNotImplemented` until each method is real.
- Optional interfaces (`LookupGateway`, `Refunder`, `RefundStatusGateway`,
  `PaymentExporter`) are an explicit opt-in: add and test one only when the provider
  supports it. Do not return "not implemented" from an optional interface.

## 3. Manifest

- Add the manifest in `domain.BuiltinManifests` and validate it: id, operations, verify
  strategy with its header/field and secrets, capabilities source, fetch mandate and rate
  limit (official provider limit with margin).
- Add the manifest test row (the built-ins are validated at startup and by
  `internal/onboarding`).

## 4. Runtime factory and environment

- Register the factory in `providerruntime.BuiltinFactories` with the stable provider id.
- Read only `CONNECTOR_<PROVIDER>_*` variables inside the factory; no other package may
  read adapter configuration.
- Add the variables to `deploy/docker-compose.yml`, `.env.example` and AGENTS.md.
- The manifest/factory parity test (`internal/onboarding`) fails until the factory exists.

## 5. Tests

- Adapter unit tests: request shape, signing, status mapping, amount parsing, error
  classification, webhook verification.
- `conformance.Run` with the provider fixture set (CONF-1..CONF-10). The suite is
  mandatory for every adapter.
- Add the mock gateway profile or record why the provider is not emulated (a sandbox
  account only). The mock parity test names every connector provider.

## 6. End to end

- Add or extend a scenario in `test/e2e/E2E.md`; run the compose stand and save artifacts
  under `test/e2e/.runs/<mark>/` (summary, compose ps/logs, checks).
- The in-process factory path is covered by the compose smoke; a provider profile run
  covers the money path (create, read, simulate, confirm).

## 7. Documentation and review

- Update `AGENTS.md`: journal row, trade-offs for provider-specific decisions, known
  issues for unverified wire details.
- Add the spec `.specs/CBL-<task>-<slug>.md` with the test matrix and e2e descriptions.
- Four review passes: no-context, architecture, with-context, security.

## Import boundaries (enforced)

`internal/onboarding/guard_test.go` fails the build when:

- a package under `internal/providers/` imports `usecase`, `transport`, `storage`,
  `gateway` or `providerruntime`;
- a core package (`usecase`, `transport`, `storage`, `domain`, `gateway`) imports
  `internal/providers/*` or `providerruntime`;
- any package outside `internal/providers/*` other than `internal/providerruntime`
  imports provider packages.

The guard is import-based and inspects the non-test import graph (the production binary):
it protects the boundary continuously, not only the diff of one task. Test-only imports
are outside the guard. Run it with `go test ./internal/onboarding`.
