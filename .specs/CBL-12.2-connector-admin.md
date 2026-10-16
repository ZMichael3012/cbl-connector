# CBL-12.2. Connector admin contour: routing configuration and provider disable

| Field | Value |
| :-- | :-- |
| Epic | E12 (task CBL-12.2, the `cbl-connector` part) |
| Repository | `cbl-connector` |
| Size | M |
| Wave and parallel group | W4-ADMIN |
| Depends on | 3.4 (rules repository, `route:config` cache), `cbl-protos@cf-3.7` (`ConnectorAdminService`) |
| Contract version | `cbl-protos@cf-3.7` (additive: `ConnectorAdminService`, `RoutingRuleAdmin`) |
| Status | done, review closed |

## 1. Context and goal

KI-connector-08: the routing rules cannot be changed in production (the admin path is
missing). Task 12.2 implements the server side of the frozen `ConnectorAdminService`
(CBL-12.2 in `cbl-protos`): the revision-based configuration replacement and the manual
provider disable; the back-office is the client with RBAC and audit. The source of truth
stays PostgreSQL connector (3.4 ADR); `route:config` remains a cache.

## 2. Boundaries

In scope: the `routing_config` revision row and the `disabled_providers` table
(migration 00004); repository methods (`LoadAll`, `Revision`, `ReplaceWithRevision` under
the advisory lock, `DisableProvider`, `ListDisabled`, `ProviderNames`); the admin usecase
(validation, revision semantics, known-provider check, cache invalidation); the gRPC
transport of `ConnectorAdminService` registered on the existing server (:9050); the cache
payload carrying the disabled set (tolerant read, fallback on any inconsistency); the
router excluding disabled providers via the rule source; unit/integration tests and the
e2e smoke extension (grpcurl `GetRoutingRules`).

Out of scope: the provider registry and adapters (3.2, KI-01 - the known-provider check
uses the union of the registry and the provider names present in the rules); automatic
failover (3.4 open question); provider enable (the reverse action arrives with a later
additive contract if needed); RBAC/audit (the back-office side of 12.2); mTLS (0.6).

## 3. Functional requirements

FR-1. Migration 00004: `routing_config` (single row, `revision bigint NOT NULL DEFAULT 0`)
and `disabled_providers` (`provider text PRIMARY KEY`, `reason`, `disabled_at`).

FR-2. Repository: `ReplaceWithRevision(ctx, expected, rules)` runs in one transaction under
the same advisory lock as `Save`: serializes on the `routing_config` row
(`SELECT ... FOR UPDATE`), a revision mismatch is `ErrRouteRevisionConflict` (nothing is
written), otherwise the rules are replaced as a whole, the revision increments and the
new value is returned; `LoadAll` returns every rule (enabled and disabled, ordered);
`DisableProvider(ctx, provider, reason)` inserts into `disabled_providers`
(`ON CONFLICT DO NOTHING`; `already_disabled=true` when the row existed);
`ProviderNames` returns the distinct provider names of the rules.

FR-3. Admin usecase (`usecase.AdminRouting`):

- `GetRules` -> revision and all rules;
- `PutRules(expected, rules)`: every rule passes `RouteRule.Validate`, the count is at
  most 1000, `expected >= 0`, otherwise `ErrInvalidAdminInput`; storage conflict ->
  `ErrRouteRevisionConflict`; on success the cache is invalidated (a failure is logged,
  the TTL bounds staleness) and the new revision with the stored rules is returned;
- `DisableProvider(provider, reason)`: provider is a non-empty visible-ASCII string up
  to 64 characters, reason up to 255 characters without control characters
  (`ErrInvalidAdminInput`); the provider must be known - present in the registry, in
  the rules or in the disabled set (a repeat after a rule replacement stays
  idempotent; review finding), otherwise `ErrProviderNotFound`; the repository insert
  is idempotent; the cache is invalidated on every attempt (a failed earlier
  invalidation cannot pin a stale payload until the TTL).

FR-4. Rule source and router: the cache payload carries `rules` and `disabled`
(`domain.RouteConfigSnapshot`); `CachedRouteRules.Load` filters rules of disabled
providers (a cache payload failing validation or an unknown shape is a miss with a DB
fallback, as in 3.4); the router itself is unchanged - a disabled provider is simply
absent from the loaded rules, so `Select` never returns it. `Save` (the legacy
unconditional replacement) bumps the revision as well, so it cannot silently defeat
the optimistic concurrency of `ReplaceWithRevision`.

FR-5. Transport: `ConnectorAdminService` is registered on the existing gRPC server;
error mapping - `ErrInvalidAdminInput`/`ErrInvalidRoute` -> `InvalidArgument`,
`ErrRouteRevisionConflict` -> `FailedPrecondition`, `ErrProviderNotFound` -> `NotFound`,
storage failures -> `Internal` (hidden details). No authN on the internal contour yet
(mTLS - 0.6, the same posture as the other internal services).

FR-6. Tests: unit - usecase validation/conflict/known-provider/idempotence, transport
error mapping and conversion round-trips, cache payload filtering; integration
(testcontainers PostgreSQL) - revision flow (get/put/bump/conflict), disable idempotence,
`LoadAll`, `ProviderNames`, disabled filtering through `CachedRouteRules` (Redis cached
and fallback paths in the existing style); e2e - the connector smoke scenario gains a
grpcurl `GetRoutingRules` check (revision 0 on a clean stand).

## 4. Contracts

`billing/connector/v1.ConnectorAdminService` (`cbl-protos@cf-3.7`); no changes to
`ConnectorService`, Kafka topics or ClickHouse. `route:config` payload is internal to the
service (CBL-003 section 7 names the key, not the JSON shape).

## 5. Edge cases

Empty rule set (a valid replacement clearing routing); duplicate provider rules
(kept: the 3.4 semantics sum weights); revision overflow (bigint, ignored); two
concurrent `ReplaceWithRevision` (advisory lock + row lock, exactly one wins, the loser
gets the conflict); concurrent disable (idempotent); disable of a provider present only
in the rules (known, works); disable of an unknown provider (NotFound; an empty registry
and empty rules mean every provider is unknown); cache poisoned or stale (fallback +
TTL); Redis absent (no cache, DB path); replacement while the cache holds the old set
(invalidation; a failed invalidation is bounded by TTL); a disabled provider's rules
stay visible to the admin `GetRules` (the operator sees and can re-enable by editing).

## 6. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | usecase: validation (item count, revision, provider/reason formats), conflict mapping, known-provider union, disable idempotence, invalidate on success only; transport: conversions round-trip, gRPC codes for every domain error, nil-safety; cache: disabled filtering, invalid payload is a miss |
| Integration | PostgreSQL (testcontainers): revision get/put/bump, conflict leaves the rules untouched, `LoadAll` includes disabled rules, disable idempotence and `ProviderNames`; `CachedRouteRules` with a real Redis: disabled filtering on the cached and fallback paths |
| E2E | the existing `E2E_Connector_Smoke` plus a `GetRoutingRules` grpcurl check (service registered, revision 0) |
| Load | not in this scope |

Target unit+integration coverage: at least 80%.

## 7. Work plan and commit

Migration -> repository -> cache payload -> rule source filtering -> admin usecase ->
transport -> main registration -> tests -> e2e smoke update and run -> reviews (shared
with the back-office part of 12.2) -> AGENTS/README -> commit
`CBL-12.2: connector admin contour: routing revision and provider disable`.

## 8. Reviews

The shared four-pass review round of CBL-12.2 (16.10.2026); connector-specific
closures: the known-provider union now includes the disabled set (a repeat after a
rule replacement stays idempotent) and the cache is invalidated on every disable
attempt; `Save` bumps the revision; the smoke instruction matches the protojson
output (`{}` on a fresh configuration). Coverage 91.6%; the smoke run
`test/e2e/.runs/20261016t140201z/` PASS 6/6. The full review table lives in the
back-office spec (`.specs/CBL-12.2-payout-routing-providers.md`).

## 9. Open decisions

- The revision lives in `routing_config` (a single row) rather than a hash of the rules:
  a monotonic counter is unambiguous for optimistic concurrency and cheap to read.
- Disabled providers are excluded by filtering the rule source instead of inside the
  router: the router stays untouched and the exclusion is visible in `Load`.
- Provider enable is an additive follow-up (the contract has no `EnableProvider` yet);
  editing rules does not re-enable a provider (the disabled set wins) - SQL on
  `disabled_providers` stays the only path until the RPC lands (KI-connector-09).
- The admin contour has no authN at the connector level: the back-office enforces RBAC
  and is the only client (mTLS - 0.6, platform posture for internal services).
