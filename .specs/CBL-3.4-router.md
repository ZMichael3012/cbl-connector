# CBL-3.4. Provider router: rules, Capabilities, configuration cache

| Field | Value |
| :-- | :-- |
| Epic | E3, Connector |
| Repository | `cbl-connector` |
| Size | M |
| Wave and parallel group | W1-CONNECTOR |
| Depends on | 3.1, 3.3 |
| Contract version | `cbl-protos@cf-1` (unchanged) |
| Status | done, review closed |

## 1. Context and goal

Provider selection for an operation (`CBL-000`: Connector - "routing"; `CBL-003`, section 3:
routing rules - priority, networks, limits, weights). Task 3.4 implements: the rule
model, the source of truth in PostgreSQL connector, the configuration cache in Redis (`route:config`,
TTL 30 - 60 s), a provider selector with capability and limit filters, weighted
selection within a priority and aggregated `Capabilities`. Every provider is wrapped in
one `gateway.Resilient` instance (3.3), and an open breaker excludes the provider from
selection while an alternative exists.

## 2. Boundaries

In scope: the `RouteRule` model, the `routing_rules` migration, the GORM repository, the Redis cache
of the configuration with a DB fallback, `Router` (implementation of `domain.Router`), the extension of
`NetworkCapability.MaxAmount`, unit/integration tests.
Out of scope: provider adapters and their registration (3.2, KI-01), the admin rules API
(12.1 - 12.4, the contract will appear separately), the e2e of end-to-end selection with
mock-gateway (after 3.2), metrics (0.7).

## 3. Functional requirements

FR-1. Routing rule: `provider` (required, visible ASCII up to 64 characters),
`operation` (`""` - any, `invoice`, `payout`), `networks[]` and `assets[]` (empty -
any; values follow the code rules from 3.1, asset at least 2 characters), `priority`
(0 - 1 000 000, lower is more important), `weight` (1 - 1 000 000, weight within a priority),
`min_amount`/`max_amount` (optional bounds in the asset's minor units,
0 - no bound; with both bounds `min <= max`), `enabled`. Validation is in the domain;
violations -> `ErrInvalidRoute`; weight limits protect the sum from overflow.

FR-2. The source of truth is PostgreSQL connector (`routing_rules`, migration 00003): arrays in
`jsonb`, `weight > 0` and `min <= max` are CHECK constraints; the repository loads only
enabled rules (sorted by `priority ASC, provider ASC`) and replaces the configuration
as a whole in one transaction under an advisory lock (concurrent `Save` calls are serialized,
the result is exactly one of the configurations, without merging).

FR-3. Configuration cache in Redis: key `route:config` (`CBL-003`, section 7), the value is the
JSON configuration of rules, TTL `CONNECTOR_ROUTE_CACHE_TTL` (default 30 s, the range
30 - 60 s per `CBL-003`). Reading is cache-aside: a hit returns the cache; a miss, a Redis error,
broken JSON and a semantically invalid payload (a rule fails validation,
for example a zeroed weight) trigger loading from the DB and writing to the cache (a write failure
is logged and does not break selection). Rules from the DB are also validated: corruption of the
source of truth does not reach the router. `Save` invalidates the key; Redis is not the
source of truth (T-02).

FR-4. `Router.Select(ctx, route)`:
- the request must contain the operation, network, asset and a positive amount of the same asset
  (`RouteRequest.Validate`); zero/negative amounts and a foreign asset are rejected with
  `ErrInvalidRoute` (the router is the last filter before a money operation);
- the provider registration name must match the adapter's `Capabilities().Provider`
  (otherwise `GetGatewayPayment(provider, ...)` of consumers breaks);
- rules filtered by `operation`, `network`, `asset` and the `min/max` bounds;
- the provider must support the network with the required asset and an amount not below `MinAmount`
  (and not above `MaxAmount`); the provider's set bounds are checked against the asset and decimals
  of the request (zero bound - no limit);
- rules with a non-positive weight are dropped (defense-in-depth against a corrupted
  source); a zero sum of weights is `ErrProviderUnavailable`, not a panic;
- providers with an open breaker are excluded; if suitable ones exist but all are excluded by the
  breaker - `ErrProviderUnavailable`; if they do not fit by capabilities -
  `ErrUnsupportedNetwork`;
- selection: the minimum `priority` among the candidates; within a priority - weighted random
  selection by `weight` (thread-safe source: package-level `rand.IntN`; the injectable
  source is wrapped in a mutex);
- the `Resilient` decorator is returned (one instance per provider).

FR-5. `Router.Capabilities()` returns the capabilities of every registered
provider (for `GetCapabilities` in 3.2/4.x); `NetworkCapability` is extended with an
optional `MaxAmount` (0 - no limit), the supplier of the checks is the adapter's `Capabilities`.

FR-6. Configuration: `CONNECTOR_ROUTE_CACHE_TTL` (cache); the provider registry is filled by
task 3.2 after the provider is chosen (KI-01); the repository and the cache are wired in main
immediately (a log of the number of loaded rules); with an empty registry `Select` returns
`ErrUnsupportedNetwork`.

## 4. Contracts

External contracts do not change: `Router` implements `domain.Router` (CF-1), `route:config` is an
existing key of `CBL-003` (section 7). The form of the rules corresponds to `GET/PUT
/admin/v1/routing-rules` from `CBL-003` (section 3): priority, networks, limits, weights; the
admin path contract is finalized in 12.x (an open question of the spec).
Error mapping for the transport (3.2): `ErrUnsupportedNetwork` -> `INVALID_ARGUMENT`,
`ErrProviderUnavailable` (including all breaker open and a failed rules source) ->
`UNAVAILABLE`, a failure to load rules from the DB/Redis -> `INTERNAL` (client retries will not
help), `ErrInvalidRoute` -> `INVALID_ARGUMENT`.

## 5. Data model and migrations

`migrations/00003_routing_rules.sql`:

```sql
CREATE TABLE routing_rules (
    id         uuid PRIMARY KEY,
    provider   text NOT NULL,
    operation  text NOT NULL DEFAULT '' CHECK (operation IN ('', 'invoice', 'payout')),
    networks   jsonb NOT NULL DEFAULT '[]',
    assets     jsonb NOT NULL DEFAULT '[]',
    priority   int NOT NULL DEFAULT 0 CHECK (priority >= 0),
    weight     int NOT NULL DEFAULT 1 CHECK (weight > 0),
    min_amount bigint NOT NULL DEFAULT 0 CHECK (min_amount >= 0),
    max_amount bigint NOT NULL DEFAULT 0 CHECK (max_amount >= 0),
    enabled    boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (min_amount = 0 OR max_amount = 0 OR min_amount <= max_amount)
);
CREATE INDEX routing_rules_route_idx ON routing_rules (enabled, priority);
```

Arrays are stored in `jsonb` (the GORM type `StringList` in storage); `AutoMigrate` is forbidden.

## 6. State machines

Not applicable: rules are configuration; provider selection is stateless, breaker state
is read from `Resilient`.

## 7. Edge cases

An empty provider registry and an empty rules list; a rule for an unregistered
provider; a mismatch between the registration name and `Capabilities().Provider` (constructor
rejection); a disabled rule; `weight = 0`, negative and above the limit (validation, CHECK and
defense-in-depth in the router); `min > max`; several rules of one priority (weights) and
duplicates of rules of one provider (weights are summed by selection); a zero/negative request
amount and an asset that does not match `Amount.Asset` (rejection); the provider does not support
the network/asset; the provider's set bounds with a foreign asset/decimals; an amount below
`MinAmount` and outside the rule bounds; all candidates with an open breaker; mixed candidates;
half-open lets one trial through, excess trials get `ErrTooManyRequests` -> `ErrProviderUnavailable`
(the recovery window is accepted, failover is an open question); cache miss/hit;
corrupted JSON and a semantically corrupted cache payload (weight 0) - a miss with a fallback;
rule corruption in the DB - an error before the router; Redis unavailable; `Save` invalidates the
cache; concurrent `Save` (advisory lock, without merging configurations); TTL 30 - 60 s
(clamps); concurrent `Select` (thread-safe selection).

## 8. Non-functional requirements

`Select` on a warm cache is one Redis GET and in-memory filtering (the `GetCapabilities`
deadline of 200 ms in `CBL-003` is met with margin; the provider selection is called
inside CreateInvoice with a 5 s deadline). A Redis failure does not affect selection (fallback to
the DB, T-02/T-11). Coverage of router/domain/checks not below 85%; integration tests - real
PostgreSQL and Redis (testcontainers).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | domain: validation of rules and the request (amount, asset); `Router`: filters (operation/network/asset/rule and provider limits, network and asset lists), priorities, weighted selection (deterministic rand), duplicate rules, breaker-open skip and full rejection, zero weights (no panic), provider name mismatch, `Capabilities()` per provider, empty registry, concurrent `Select` |
| Integration (storage) | PostgreSQL: rule round-trip (jsonb arrays), enabled only, sorting, CHECKs, replace-all, concurrent `Save`; Redis: miss/hit, broken JSON, unavailable Redis (fallback), invalidation on `Save`; usecase: semantically corrupted cache - a miss, DB corruption - an error, TTL clamps 30 - 60 s |
| E2E (compose) | not extended: providers will appear in 3.2; compose smoke runs follow the `test/e2e/E2E.md` instruction (scenario E2E_Connector_Smoke, compose stand) without changes |
| Load | not extended |

## 10. Work plan and commit

Commit `CBL-3.4: provider router with rules and cache`; the spec is part of the commit;
AGENTS (architecture, env, trade-offs T-CONNECTOR-08/09, KI-connector-08, journal) and
`CBL-004` are updated.

## 11. Open questions and decisions

- ADR-3.4: the source of truth for rules is PostgreSQL connector; Redis is a cache with TTL and
  fallback (T-02, T-11): a Redis failure does not affect selection, cache loss is safe; a
  semantically corrupted cache payload (weight 0 and the like) is treated as a miss, not a
  trusted value.
- ADR: an open breaker excludes the provider from selection (3.3 + 3.4); in half-open the provider
  participates again, excess trials get `ErrProviderTooManyRequests` ->
  `ErrProviderUnavailable` - the recovery window is accepted, automatic failover
  to the next provider is an open question after 3.2.
- ADR: weighted selection within the minimum priority, not globally by weights: priority
  expresses policy ("primary provider"), weight - distribution within the echelon.
- ADR: concurrent `Save` calls are serialized by an advisory lock (replace-all without merging
  configurations); the cache staleness window in the "Load before the Save commit" race is limited
  by TTL (no more than 60 s) and is accepted until the admin path, where a configuration version
  will appear.
- Open question: the admin path for writing rules (`GET/PUT /admin/v1/routing-rules`) will appear
  in 12.x and requires a new gRPC contract in connector, RBAC and audit (`CBL-003`, section 3);
  until then rules are changed by direct configuration replacement - KI-connector-08.
- Open question: failover to the next provider when the selected one fails (automatic retry
  of the operation at another provider) is out of scope of 3.4; if needed - a separate task
  after 3.2.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). Blocker: the `route:config` cache was accepted without
semantic validation (weight 0 -> an `IntN` panic, configuration poisoning) - closed by payload
validation on read and guards in the router. Major: the `*rand.Rand` race, a non-atomic concurrent
`Save`, TTL divergence from `CBL-003` - closed; rand became thread-safe, `Save` under an advisory
lock, TTL aligned (30 - 60 s).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | `*rand.Rand` race in concurrent `Select`; panic on a corrupted cache; main wires the source only with Redis; test gaps (network/asset lists, duplicate rules); `cover` artifact | thread-safe `pick` (rand.IntN + mutex for the injectable one); cache validation and weight guards; the repository is always wired, Redis is optional; tests added; the artifact removed, `.gitignore` extended |
| Architecture | rand race; cache panic; TTL clamps vs `CBL-003`; Redis in main without timeouts; half-open not described in the ADR; registration name vs Capabilities; the rules error not classified; the admin path requires a new contract | all closed: TTL 30 - 60 s, Redis timeouts, half-open in the ADR, name validation, mapping in §4, KI-connector-08 extended (RPC/RBAC/audit) |
| With-context | rand race; cache panic; TTL vs CBL-003; Redis without timeouts; main tied to REDIS_ADDR; AGENTS/CBL-004/.env.example loose ends; cover artifact; test gaps and `RouteRequest` without amount validation | closed: the rand wrapper, cache validation/guard, TTL, timeouts modeled on cbl-quote, the source is always wired, documentation by this commit, amount and asset are validated |
| Security | BLOCKER: `route:config` poisoning (panic/routing rejection/limit removal); major: limits without asset/decimals and an amount without a sign; rand race; concurrent `Save` + staleness window; Redis without timeouts | cache validation + weight guards; `RouteRequest.Validate` (positive amount, asset match), asset/decimals check of the set bounds; thread-safe pick; advisory locks and a fixed window <= TTL; Redis timeouts |
