# AGENTS.md - cbl-connector

Abstraction of external crypto gateways: adapters, routing, resilience, webhook normalization.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: the PaymentGateway interface, provider adapters, circuit breaker, event normalization.

Out of scope: other platform services; interaction only via `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: ConnectorService server; ConnectorAdminService server (routing configuration
  and manual provider disable, CBL-12.2, cf-3.7); outbound HTTPS to gateways
- Kafka produces: gateway.events.canonical, gateway.payout.events
- Kafka consumes: gateway.webhooks.raw, payout.commands
- Storage: PostgreSQL connector (`routing_rules`, `routing_config` revision,
  `disabled_providers`, `provider_invoices`), Redis (routes, circuit breaker)

Contracts are frozen in `cbl-protos` (CF-1 for gRPC, CF-2/CF-3 for events), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
The `internal/gateway` package contains adapters and decorators of the `PaymentGateway` port:
`Resilient` (timeouts, retries with backoff, an in-process circuit breaker per provider,
bulkhead) and `Router` (routing rules from `routing_rules`, the `route:config` cache,
weighted selection, exclusion of providers with an open breaker); wired by the router in 3.2
as one `Resilient` instance per provider.
Provider manifests (17.2): the versioned schema v1 (`internal/domain/manifest.go`) and the
`usecase.ManifestRegistry` are the source of truth of known providers; SPI v1 adds the
optional `LookupGateway` and `Refunder` interfaces, and `provider_invoices` maps platform
payments to provider invoices.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose only, forward only.

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers PostgreSQL)
make test-e2e     # runs the normalizer end-to-end scenario on testcontainers Kafka+PostgreSQL
make vet          # go vet ./...
make lint         # golangci-lint in Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

Compose stand smoke runs follow the `test/e2e/E2E.md` instruction (scenario E2E_Connector_Smoke; artifacts in `test/e2e/.runs/<mark>/`); there are no PowerShell run scripts.

## 5. Ports and environment

HTTP `8050`, gRPC `9050`, metrics `9250`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`MIGRATIONS_DIR`, `KAFKA_BROKERS`, `KAFKA_CONSUMER_GROUP` (default `cbl-connector-webhooks`),
`CONNECTOR_SANDBOX_PROVIDERS` (default `mock`), `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.
Provider resilience (3.3, default values): `CONNECTOR_RETRY_MAX_ATTEMPTS` (3),
`CONNECTOR_RETRY_BASE_DELAY` (100ms), `CONNECTOR_RETRY_MAX_DELAY` (1s),
`CONNECTOR_BREAKER_FAILURE_THRESHOLD` (5), `CONNECTOR_BREAKER_TIMEOUT` (30s),
`CONNECTOR_BREAKER_HALF_OPEN_REQUESTS` (1), `CONNECTOR_BULKHEAD_MAX_CONCURRENT` (32),
`CONNECTOR_PROVIDER_ATTEMPT_TIMEOUT` (overrides per-method timeouts).
Routing (3.4): `CONNECTOR_ROUTE_CACHE_TTL` (`route:config` cache TTL, 30 - 60 s),
`REDIS_PASSWORD` (Redis password, if enabled).

## 6. Development rules

- Pipeline: SDD spec in `.specs/` -> architecture -> contracts -> tests first -> code ->
  review by 4 agents (no-context, architecture, context, security - blocking) ->
  AGENTS.md -> final runs -> linters -> commit.
- Unit+integration coverage is at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); local e2e in docker-compose is mandatory.
- Money is int64 in minor units plus decimals only; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name and
  ends with a period; comments explain why; errors are wrapped with `%w`; panics in business
  code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix in the same
  commit.

## 7. SLO and metrics

SLO is refined in task 14.4. Base metrics: latency p95/p99, error share, outbox lag,
consumer lag; dashboards after task 0.7.

## 8. Trade-offs

| ID | Decision | Pros | Cons | Compensation |
| :-- | :-- | :-- | :-- | :-- |
| T-CONNECTOR-01 | `VerifyWebhook` is declared in the gateway port although the signature is verified by Webhook Ingress | defense-in-depth and port symmetry | the signature contract is duplicated in two services | boundaries are fixed: Ingress verifies, Connector trusts the ingress `signature_state` (CF-4) and re-fetches `fetch_required` deliveries at the provider (17.2); secrets only via Vault |
| T-CONNECTOR-02 | Until the provider is selected (KI-01) a sandbox JSON decoder works instead of adapters | the normalization pipeline is unblocked and covered by e2e without a real provider | the webhook format is our contract, not tested on a live gateway | the format is fixed in CBL-3.5, implemented by mock-gateway (3.6); adapters 3.2 are enabled via the decoder registry |
| T-CONNECTOR-03 | `payment_id` is taken from the webhook body (sandbox) | simplicity, no reverse mapping | real providers do not know the platform payment_id | mapping landed (17.2): real webhooks resolve payment_id through `provider_invoices`, fetch-to-verify re-checks the status; the sandbox decoder path keeps the body value |
| T-CONNECTOR-04 | Inbox retries are published to retry topics without delayed delivery (kit mechanics) | simple pipeline, retries are not lost | the retry window is virtually zero: a transient DB failure dumps the backlog into DLQ in milliseconds | x-error truncation and field limits prevent the poison pill from blocking the partition; delayed retries and parking - 14.2 (KI-connector-05) |
| T-CONNECTOR-05 | Event order by `gateway_event_ts` is controlled by Payment (4.3), the normalizer only passes the time through | no ordering state in the connector | on retry an event may be published late | the requirement is mirrored in the Payment spec 4.3; the payment_id partition key preserves the order of one payment in the normal case |
| T-CONNECTOR-06 | Circuit breaker is in-process (gobreaker per provider), the `cb:{provider}` key is reserved | no Redis on the money path, state loss is safe, no false shared trips under partial degradation | with I replicas the effective trip threshold grows proportionally, half-open probes multiply | consolidate shared state when a multi-instance requirement appears (KI-connector-07); thresholds are configurable |
| T-CONNECTOR-07 | Only temporary failures and timeouts count as a breaker failure; business rejections and caller cancellation are not counted; an empty `idempotency_key` is fail-fast; configuration is normalized with clamps | a stream of rejected addresses and caller bugs do not block a healthy provider; a retry does not create a second invoice/payout; "insane" env values do not disable protection | adapters must classify errors (`ErrTemporary`/`ErrRejected`) and sanitize their text | classification and mapping are fixed in CBL-3.3 (§4) and CBL-003; contract tests of classification - 3.7 |
| T-CONNECTOR-08 | Routing rules: the source of truth is PostgreSQL, Redis `route:config` is a cache with a 30 - 60 s TTL and a fallback; a semantically corrupted payload (weight 0) counts as a miss | a Redis failure does not affect selection; cache poisoning does not reach the router (validation + weight guard) | rules from the cache require read-time validation; the cache staleness window applies to admin changes too | the admin path is implemented in 12.2 (revision + disabled set, T-CONNECTOR-10); invalidation on every admin write plus the TTL bound staleness |
| T-CONNECTOR-09 | Concurrent rule `Save` operations are serialized by an advisory lock; the cache staleness window during a Load/Save race is bounded by TTL (≤ 60 s); a half-open provider participates in selection again, excess probes are rejected | replace-all without merging configurations; provider recovery without manual intervention | automatic failover to another provider is not performed; staleness window up to 60 s | failover is a separate task after 3.2; the admin path adds the configuration revision (12.2) |
| T-CONNECTOR-10 | Admin contour (12.2): the revision lives in `routing_config` (a single row, advisory lock + `FOR UPDATE`); the disabled set is persisted in `disabled_providers` and applied by filtering the rule source (the router is untouched); `ConnectorAdminService` is a separate gRPC service so the money-path `ConnectorService` server interface and its fakes stay source-compatible; the back-office is the only client and enforces RBAC and audit | optimistic concurrency between operators; cache consistency via invalidation on every attempt plus the TTL; the exclusion is visible in `Load` | the admin RPCs have no authN inside the trusted network (mTLS - 0.6) and mutate the money routing: port 9050 must not leave the trusted zone (KI-connector-03) | provider enable is an additive follow-up (KI-connector-09); disabled rows have no retention yet |
| T-CONNECTOR-11 | Provider manifests are versioned data (schema v1) and the SPI stays a set of small Go interfaces (core port plus optional `LookupGateway`/`Refunder`), added instead of widening `PaymentGateway` | the core and the existing fakes stay source-compatible; adding a provider touches only its adapter package, manifest and mapping (functional extensibility guarantee, plan 14.5) | optional interfaces need type assertions at the use sites; a manifest schema change needs a versioned migration of the catalogue | the manifest validation is centralized in `BuiltinManifests`/`Validate`; a new schema version is introduced with the onboarding kit (17.22) |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-connector-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |
| KI-connector-02 | No real provider adapter (KI-01) | normalization is verified by the sandbox decoder and e2e on testcontainers, not by a live gateway | E17 adapters 17.6 - 17.9 after the core: manifests and the invoice mapping landed in 17.2 part 1 | open |
| KI-connector-03 | No mTLS on the gRPC server (port 9050), now including the money-routing admin RPCs (`PutRoutingRules`, `DisableProvider`); the admin contour has no authN | do not expose port 9050 outside the trusted network: an unauthorized caller could reroute the platform | task 0.6 (mTLS); the back-office is the only intended client | open |
| KI-connector-04 | No cleanup of published outbox (canonical payload) and old inbox records; raw webhook bodies live in the webhook-ingress outbox and Kafka retry/DLQ topics | tables and topics grow, including sensitive provider headers | retention task (0.4/14.x), ACL/retention DLQ (0.5/5.3) | open |
| KI-connector-05 | Kit inbox: `x-error` is not truncated, a failed retry/DLQ publication is not parked, retries have no delay | with giant/broken events a redelivery storm is possible; in 3.5 bounded by field and body size limits | kit hardening (error truncation, parking, delays) - tasks 1.3-revision/14.2 | open |
| KI-connector-06 | Kafka in compose is PLAINTEXT without SASL/ACL: writing to gateway.webhooks.raw is not restricted to Ingress; Connector trusts `signature_verified` | spoofing the signature flag with topic access bypasses verification | SASL/TLS and ACL for writing to the raw topic - task 0.5; alert on producer mismatch - 14.4 | open |
| KI-connector-07 | Circuit breaker state is in-process: with several replicas the trip threshold and half-open probes are not coordinated; the Redis key `cb:{provider}` is reserved | with multiple instances there may be extra calls to a degrading provider | consolidate shared state when a multi-instance requirement appears (T-CONNECTOR-06); thresholds are configurable | open |
| KI-connector-08 | The admin path for routing rules was missing: `GET/PUT /admin/v1/routing-rules` (12.x) required a new gRPC contract in connector (cbl-protos), RBAC and audit; until then rules were changed by direct configuration replacement | there was no way to change rules in production without SQL | CLOSED 16.10.2026: `ConnectorAdminService` (cf-3.7) implemented in 12.2 (revision-based `PutRoutingRules`, `GetRoutingRules`, `DisableProvider`; migration 00004; RBAC and audit on the back-office side) | closed |
| KI-connector-09 | No provider enable and no retention on `disabled_providers`: the table grows monotonically and a disabled provider can only be re-enabled by SQL | an erroneous or temporary disable requires manual DB work; the table grows without bounds | the `EnableProvider` RPC now exists in the contract (cf-4); implementation with RBAC and audit - 17.15; a retention/archive decision with 14.2 when the volume warrants it | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-3.1 | 2026-10-14 | done, review closed (PaymentGateway interface, canonical statuses, domain coverage 100%) | `.specs/CBL-3.1-gateway-interface.md` | d105801 |
| CBL-3.3 | 2026-10-14 | done, review closed (timeouts, backoff retries, breaker with a failure predicate, bulkhead; gateway coverage 93.3%) | `.specs/CBL-3.3-resilience.md` | 60db799 |
| CBL-3.4 | 2026-10-14 | done, review closed (rules in PG, route:config cache with fallback, weighted selection, breaker-aware; coverage 91.8%) | `.specs/CBL-3.4-router.md` | 5c3204e |
| CBL-3.5 | 2026-10-14 | done, review closed (normalization pipeline, sandbox decoder, untrusted input limits, e2e Kafka+PG with a poison-pill scenario, coverage 93.1%) | `.specs/CBL-3.5-normalizer.md` | 04aac61 |
| CBL-16.8 | 2026-10-16 | done (English comments and documentation across the repository: code, tests, migrations, configs, all four specs, AGENTS.md, README; e2e runs via `test/e2e/E2E.md` with scenario E2E_Connector_Smoke, `smoke.ps1` and `make smoke` removed; validation run PASS; coverage unchanged) | `.specs/CBL-16.8-conventions.md` | this commit |
| CBL-12.2 | 2026-10-16 | done, review closed (admin contour: migration 00004 with the routing revision and disabled providers; revision-based ReplaceWithRevision under the advisory lock; DisableProvider idempotent, known-provider union incl. the disabled set; `ConnectorAdminService` registered on :9050; the rule source filters disabled providers and the cache payload carries the disabled set; smoke extended with the grpcurl read; coverage 91.6%) | `.specs/CBL-12.2-connector-admin.md` | this commit; contracts `cbl-protos@cf-3.7..cf-3.10` |
| CBL-17.2 (part 1) | 2026-10-16 | done, review closed (provider manifest schema v1 with validation and the four built-in manifests plus mock; `usecase.ManifestRegistry` wired into main and the admin known-provider union; SPI v1 optional interfaces `LookupGateway`/`Refunder`; migration 00005 `provider_invoices` and the repository with idempotent save; unit and PostgreSQL integration tests; part 2: fetch-to-verify, capabilities loader, limiter decorator, ConnectorService transport) | `.specs/CBL-17.2-provider-core.md` | 6f04569 |
| CBL-17.2 (part 2, increment 1) | 2026-10-16 | done, review closed (additive modules: `gateway.Limited` limiter decorator over the kit `ratelimit` port with per-operation keys, `ErrProviderOverloaded` on deny, fail-open on limiter storage errors and `PauseProvider` for provider Retry-After; `usecase.CapabilitiesLoader` with an injectable cache port, live fallback, provider filter and provider-qualified rows; unit tests; no runtime wiring yet, e2e unchanged) | `.specs/CBL-17.2-provider-core.md` | 6938525 |
| CBL-17.2 (part 2, increment 2) | 2026-10-16 | done, review closed (fetch-to-verify in the normalizer: CF-4 `signature_state` with the deprecated-flag transition fallback, fail closed on non-verified; `payment_id` resolution through the `provider_invoices` mapping when the body does not carry it; `fetch_required` deliveries build the canonical event from the adapter status re-check and fail closed without a fetcher/gateway id/mapping; main wires the mapping; unit tests; integration and e2e green; coverage 91.1%) | `.specs/CBL-17.2-provider-core.md` | f0d5d26 |
| CBL-17.2 (part 2, increment 3) | 2026-10-16 | done, review closed (Redis capabilities cache adapter `storage.CapabilitiesCache`: JSON snapshot under `connector:capabilities:v1`, a miss is not an error and a corrupted payload is a read error so the loader falls back to the live aggregate; real-Redis integration test; loader/limiter wiring and the ConnectorService transport remain) | `.specs/CBL-17.2-provider-core.md` | 6a3555a |
| CBL-17.2 (part 2, increment 4) | 2026-10-16 | done, review closed (retroactive 4x review of increments 1-3; findings closed: occurred_at uses the receipt time and fails closed without any event time, the adapter status PaymentID is reconciled with the invoice mapping (a mismatch goes to the DLQ), `gateway.Limited` forwards `LookupGateway`/`Refunder` with a refund bucket, cached capability snapshots are validated against registered sources, `provider_invoices` rejects a gateway id remapped to another payment (payment and request hash check), manifests of re-verifying providers require Vault secret references, `raw_status` added to the canonical event and status; unit, integration and e2e runs green) | `.specs/CBL-17.2-provider-core.md` | e0b81eb |
| CBL-17.2 (part 2, increment 5) | 2026-10-17 | done, review closed (fetch mandate enforcement: manifest `FetchMandatory` with the token_echo validation and the CoinGate/BitPay built-ins, normalizer predicate `WithFetchMandatory` wired from the registry, the provider key normalized before every lookup so casing cannot bypass the mandate; process fact: 2 review passes (no-context, security) ran before the commit, the architecture and with-context passes ran on it and their findings are closed in the follow-up commit; unit, integration and e2e green) | `.specs/CBL-17.2-provider-core.md` | fa73400, follow-up in this commit |

## 11. Runbook

Normalization pipeline:

1. Lag: consumer lag metrics (0.7), logs `webhook normalizer started`, `canonical event queued in outbox`.
2. DLQ: topic `gateway.webhooks.raw.dlq`, the reason in the `x-error` header; frequent reasons -
   an unsigned webhook, an unknown provider, a broken body; analyze manually.
3. Outbox lag: relay logs `outbox published batch`, `outbox publish batch failed`;
   when Kafka is unavailable events remain in the table and are published later.
4. A Redis failure does not affect the pipeline (Redis is used by task 3.4).

Provider resilience (3.3):

1. Logs `provider call retry` (operation, attempt, delay) and `provider breaker state
   changed` - frequent tripping indicates provider degradation or unclassified adapter
   errors (check that business rejections are wrapped in `ErrRejected`).
2. `ErrProviderOverloaded`: the bulkhead is exhausted or the per-provider limiter
   (`gateway.Limited`, 17.2) denied a token - the provider received too many concurrent
   operations; check `CONNECTOR_BULKHEAD_MAX_CONCURRENT`, the limiter config and stuck calls.
3. `ErrProviderUnavailable` without a breaker record in the log: attempts are exhausted;
   look at the reason in the error chain (attempt timeout, `ErrTemporary`). With many
   replicas remember KI-connector-07 (breaker state is not shared).

Routing (3.4, admin contour 12.2):

1. Log `routing rules loaded` at startup: 0 rules with connected providers means that
   selection will return `ErrUnsupportedNetwork`; rules are changed through the
   back-office admin API (12.2) or the ConnectorAdminService directly.
2. Log `route config cache payload is invalid` / `cache read failed`: Redis returns garbage
   or is unavailable - selection keeps working via the DB; check the `route:config` content
   (the key may be poisoned: T-02, read-time validation already filters).
3. `ErrProviderUnavailable` with a live provider: all breakers are open due to temporary
   failures; check logs `provider breaker state changed` and recovery to half-open.
4. `ErrUnsupportedNetwork`: no rule covers the operation/network/asset/amount;
   verify the rules and `Capabilities` of registered providers (3.2).
5. Revision conflicts from `PutRoutingRules` are normal under concurrent operators: the
   back-office surfaces 409; the operator repeats from a fresh `GetRoutingRules`.
6. A disabled provider disappears from selection immediately after cache invalidation
   (30 - 60 s worst case via TTL): `SELECT provider, reason, disabled_at FROM
   disabled_providers`; re-enabling needs SQL until the `EnableProvider` RPC
   (KI-connector-09).
