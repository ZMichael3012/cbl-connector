# CBL-17.15 (connector increment) - EnableProvider administration

Task: CBL-17.15 in `CBL-004` (E17), connector part of "provider reporting and
administration"; depends on 17.10. Repository: `cbl-connector`. Status: in progress.

## 1. Problem and scope

`DisableProvider` exists (CBL-12.2) and the CF-4 contract already defines
`EnableProvider`, but the implementation is missing: a provider disabled by mistake can
only be re-enabled with SQL (KI-connector-09). In scope: the storage delete, the usecase
method with the same known-provider union and cache invalidation as disable, the gRPC
handler and unit/integration coverage. Out of scope: retention/archiving of
`disabled_providers` (remains KI-connector-09), the back-office route and audit (the
back-office commit under the same task), and contract changes (the RPC exists since cf-4).

## 2. Architecture

- `storage.RouteRulesRepository.EnableProvider(ctx, provider) (already bool, err)`:
  `DELETE FROM disabled_providers WHERE provider = $1`; `already` is true when no row was
  deleted (the provider was enabled before), mirroring the disable insert.
- `usecase.AdminRouting.EnableProvider(ctx, provider) (already bool, err)`: validates the
  provider name, requires the provider to be known through the registry, the rules or the
  disabled set, deletes the row and invalidates the route cache on every attempt.
- `transport.AdminServer.EnableProvider`: maps the RPC and the domain errors with the
  existing `mapError` taxonomy.

## 3. Test plan

| ID | Case | Expectation |
| :-- | :-- | :-- |
| E1 | disable then enable (storage) | enable reports false, repeat reports true, the set is empty |
| E2 | enable a known provider (usecase) | false, cache invalidated |
| E3 | repeated enable | true, cache invalidated again |
| E4 | unknown provider | ErrProviderNotFound |
| E5 | invalid provider name | ErrInvalidAdminInput |
| E6 | RPC conversion | AlreadyEnabled passthrough, NotFound mapping |
| E7 | storage error | wrapped, not a domain error |

Integration: storage testcontainers case E1 on PostgreSQL; the usecase/transport matrices
are unit tests.
E2E: no new scenario; the connector compose smoke and the admin gRPC call remain green
(the RPC is additive and not used by the money path).

## 4. Red evidence

Recorded 20.10.2026: `go test ./internal/usecase ./internal/transport` failed with
`admin.EnableProvider undefined` and the fake store missing `EnableProvider`; the
storage integration test referenced `repo.EnableProvider` before the method existed.

## 5. Results and review closure (20.10.2026)

- Unit E2 - E7 and storage integration E1 green; connector unit and integration suites
  green.
- Gates: golangci-lint 0, go vet clean, em dash check green; compose behavior unchanged
  (the RPC is additive and unused by the money path).
- Review pass 1 (no-context): the enable mirror matches the disable contract and the
  spec matrix; the `already` semantics are the inverse of disable.
- Review pass 2 (architecture): storage/usecase/transport layers follow the existing
  admin path; the interface change is implemented by both fakes; no contract change.
- Review pass 3 (with-context, blocking): the cache must be invalidated on every
  attempt and a provider missing from the rules but present in the disabled set must
  stay known. Closed: `knownProviders` union reused; invalidation before the return; the
  integration test covers disable -> enable with the route source recovering the provider.
- Review pass 4 (security): no new input surface beyond the provider name (validated);
  the RPC remains on the trusted admin port (KI-connector-03); the audit is owned by the
  back-office increment.
