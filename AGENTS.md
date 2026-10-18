# AGENTS.md - cbl-connector

Abstraction of external crypto gateways: adapters, routing, resilience, webhook normalization.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: the PaymentGateway interface, provider adapters (`internal/providers/*`),
circuit breaker, event normalization, the administrative routing contour.

Out of scope: other platform services; interaction only via `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: ConnectorService server (17.2: `CreateInvoice`, `GetGatewayPayment`, `GetCapabilities`; `ListGatewayPayments` - the per-provider reconciliation export, 17.13) and ConnectorAdminService server (routing configuration
  and manual provider disable, CBL-12.2, cf-3.7); outbound HTTPS to gateways
- Kafka produces: gateway.events.canonical, gateway.payout.events
- Kafka consumes: gateway.webhooks.raw, payout.commands
- Storage: PostgreSQL connector (`routing_rules`, `routing_config` revision,
  `disabled_providers`, `provider_invoices`, `provider_payouts`), Redis (routes,
  circuit breaker)

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
optional `LookupGateway`, `Refunder` and `PaymentExporter` interfaces, and `provider_invoices` maps platform
payments to provider invoices. The schema is extended additively in 17.5 with
`verify_encoding`/`verify_prefix` (BitPay base64, BTCPay `sha256=`) so the declared
verification strategy is data, not code.
Adapter conformance suite (17.5): `internal/conformance` is the contract testkit every
adapter (including future ones) must pass; it drives an adapter against provider wire
fixtures (`Fixtures`) through the `CONF-1..CONF-10` checks (`Run`, `RunChecks`), builds
verification strategies from manifests (`StrategyFromManifest`, `SignBody`) and proves
itself with meta-tests (compliant adapter passes, mutants fail exactly one check).
BTCPay adapter (17.9): `internal/providers/btcpay` implements SPI v1 (PaymentGateway,
LookupGateway, Refunder) against the Greenfield API (hosted invoices, order lookup,
`BTCPay-Sig` webhooks, pull payments for payouts and refunds); main wires it from the
`CONNECTOR_BTCPAY_*` environment into the gateways map, the interim static selector
(`usecase.NewStaticSelector`, replaced by the rule-based routing of 17.17), the
capabilities loader (the adapter satisfies `usecase.CapabilitySource`) and the
normalizer fetch-to-verify bridge (`usecase.NewGatewayStatusFetcher`).
CoinGate adapter (17.7): `internal/providers/coingate` implements SPI v1 against the
CoinGate v2 API (orders create/get, order lookup, token-echo verification with the
ingress strategy, provisional payout/refund shapes owned by 17.11/17.12); the shared
provider-agnostic payload helper `internal/providers/wire.DecodeArrayOrObject`
(extracted at the second adapter, decision 21) is used by both adapters for the
list-or-object lookup answers; `CONNECTOR_PROVIDERS` (CSV allowlist) selects the
adapters to wire.
NOWPayments adapter (17.6): `internal/providers/nowpayments` implements SPI v1 against
the NOWPayments API (hosted invoices with JSON-number amounts and the request-horizon
expiry clamp, payment reads and the envelope/array/object lookup, canonical JSON
HMAC-SHA512 IPN verification, the pay asset resolved by the exact configured network
row, the manifest fetch mandate so the normalizer resolves NOW deliveries through the
adapter, provisional payout shapes; no Refunder: refunds are manual by decision 8).
BitPay adapter (17.8): `internal/providers/bitpay` implements SPI v1 against the
BitPay API v2 (invoice create/get with the request-horizon expiry clamp and the
`merchant/invoice` facade envelope with decimal major-unit amounts, array-or-
object order lookup, `x-signature` raw-HMAC base64 verification, ECDSA secp256k1
request signing (hex DER `X-Signature` over url plus body, compressed-key
`X-Identity`) with SEC1/PKCS8 PEM key loading via the decred library, statuses
without fabrication, provisional payout/refund shapes).
Payout pipeline (17.11): `internal/usecase/payout_processor.go` consumes
`payout.commands` in the Guard transaction, validates the untrusted command,
selects exactly one enabled provider by `CONNECTOR_PAYOUT_PROVIDERS` and the
capability network/asset/amount range (`PayoutRouter`, deterministic smallest
id, no failover), calls `CreatePayout`, records the `provider_payouts` mapping
and publishes the submitted `gateway.payout.events` event with the provider; a
business rejection publishes failed (sanitized reason), a temporary failure
uses the retry topics. `internal/usecase/payout_status_worker.go` polls
submitted rows (`CONNECTOR_PAYOUT_POLL_INTERVAL`, default 30 s), touches
non-terminal rows and resolves the first confirmed/failed provider status with
a CAS in the same transaction as the event (exactly one publisher). Migration
00007 owns the mapping; the payout-side provider propagation is the
`cbl-payout` increment. NOWPayments payouts (JWT/TOTP batch) are H2 and
excluded from the default allowlist.
Provider refunds (17.12): a command with `refund_invoice_id` (cf-4.2) is a
refund: the processor resolves the original provider invoice through
`provider_invoices.ByPaymentID`, requires the provider to be enabled and to
implement `domain.Refunder`, calls `CreateRefund` (provider refunds API:
CoinGate `/v2/refunds`, BitPay `/refunds`, BTCPay refund pull payment), records
`provider_payouts.refund_invoice_id` (migration 00008) and publishes the same
submitted event. The status worker polls refunds through the optional
`domain.RefundStatusGateway` (BTCPay aliases the store payout read, CoinGate
and BitPay read their refund endpoints); without it the row stays on the manual
path. NOWPayments refunds are manual (decision 8).
Provider exports (17.13): `domain.PaymentExporter` is the optional SPI v1
interface behind `ListGatewayPayments`: the transport validates the window
(required, ordered, bounded by 31 days), the page limit (default 100, bound
500) and the opaque cursor (base64url `x1:<offset>`, bound 64 bytes), resolves
the provider from the wired map and serializes rows with the canonical status;
a row whose provider word cannot be mapped arrives as `UNSPECIFIED` with
`raw_status` and a missing amount evidence (empty asset) leaves
`received_amount` unset, so consumers can tell "no evidence" from a genuine
zero. The adapters page the provider list endpoints (NOWPayments and CoinGate
page-based, BitPay and BTCPay offset-based; the wire shapes are provisional
until 17.16, KI-connector-13) through the shared
`providers/wire.DecodeEnvelopeOrArray` helper and map rows fail-soft
(`exportStatusOf`); `gateway.Limited` throttles the export with the status
bucket and `gateway.Resilient` guards it with a 3 s attempt budget against the
10 s RPC deadline.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose only, forward only.

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers PostgreSQL)
make test-e2e     # runs the e2e scenarios on testcontainers Kafka+PostgreSQL (normalizer, payout pipeline)
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
BTCPay adapter (17.9): `CONNECTOR_BTCPAY_BASE_URL` (Greenfield root; unset keeps the
runtime fail-closed), `CONNECTOR_BTCPAY_STORE_ID`, `CONNECTOR_BTCPAY_API_TOKEN`,
`CONNECTOR_BTCPAY_WEBHOOK_SECRET`, `CONNECTOR_BTCPAY_CAPABILITIES` (JSON list of
`{"network","asset","min_minor","max_minor","decimals","confirmations"}` rows),
`CONNECTOR_BTCPAY_TIMEOUT` (default 10s). The compose stand points the adapter at the
mock-gateway `btcpay` profile.
CoinGate adapter (17.7): `CONNECTOR_COINGATE_BASE_URL` (API root; unset disables),
`CONNECTOR_COINGATE_API_TOKEN`, `CONNECTOR_COINGATE_IPN_TOKEN` (token echo secret),
`CONNECTOR_COINGATE_CAPABILITIES` (same row format), `CONNECTOR_COINGATE_TIMEOUT`.
NOWPayments adapter (17.6): `CONNECTOR_NOWPAYMENTS_BASE_URL` (API root; unset
disables), `CONNECTOR_NOWPAYMENTS_API_KEY`, `CONNECTOR_NOWPAYMENTS_IPN_SECRET`,
`CONNECTOR_NOWPAYMENTS_CAPABILITIES`, `CONNECTOR_NOWPAYMENTS_TIMEOUT`.
BitPay adapter (17.8): `CONNECTOR_BITPAY_BASE_URL` (API root; unset disables),
`CONNECTOR_BITPAY_API_TOKEN` (optional pairing token), `CONNECTOR_BITPAY_PRIVATE_KEY_PEM`
(secp256k1 request-signing key, SEC1 or PKCS8; a Vault secret in production, the e2e
scenario uses a dev key), `CONNECTOR_BITPAY_WEBHOOK_SECRET`, `CONNECTOR_BITPAY_WEBHOOK_URL`
(optional notificationURL), `CONNECTOR_BITPAY_CAPABILITIES`, `CONNECTOR_BITPAY_TIMEOUT`.
Payout pipeline (17.11): `CONNECTOR_PAYOUT_PROVIDERS` (CSV allowlist of providers
that may pay out; default `btcpay,coingate,bitpay`; empty disables the pipeline),
`CONNECTOR_PAYOUT_CONSUMER_GROUP` (default `cbl-connector-payouts`),
`CONNECTOR_PAYOUT_POLL_INTERVAL` (status polling cadence, default 30s, minimum 1s;
0 disables the worker).
`CONNECTOR_PROVIDERS` (CSV allowlist) restricts the wired adapters; empty means every
provider with a configured base URL (the e2e scenarios select one provider).

## 6. Development rules

- Pipeline (TDD-first, CBL-001): an SDD spec with the architecture sketch and the full test
  plan (unit and integration matrices, e2e scenario descriptions) -> contracts -> red tests
  with recorded evidence -> implementation -> 4-agent review on the staged diff (no-context,
  architecture, with-context, security - blocking) -> AGENTS.md -> final runs (unit +
  integration + local e2e) -> linters -> commit.
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
| T-CONNECTOR-11 | Provider manifests are versioned data (schema v1) and the SPI stays a set of small Go interfaces (core port plus optional `LookupGateway`/`Refunder`), added instead of widening `PaymentGateway` | the core and the existing fakes stay source-compatible; adding a provider touches only its adapter package, manifest and mapping (functional extensibility guarantee, plan 14.5) | optional interfaces need type assertions at the use sites; a manifest schema change needs a versioned migration of the catalogue; provider-specific composition wiring currently lives in `cmd/connector` and repeats per adapter | the manifest validation is centralized in `BuiltinManifests`/`Validate`; the catalog artifact (17.21) and the runtime seam plus onboarding kit (17.20/17.22) standardize the wiring; the shared adapter request/classification plumbing (request, error classes, truncation) is the next extraction candidate for the 17.20 seam |
| T-CONNECTOR-12 | The adapter conformance suite is an internal testkit (`internal/conformance`) whose fixtures mirror the mock-gateway profiles instead of importing them; the suite must be passed by every adapter (plan 14.5.8, checklist 14.4) | one contract for all current and future adapters; the meta-tests prove the suite catches violating adapters and the signature/shape tests keep the fixtures self-consistent | the fixture mirror must be maintained by hand across two repositories; the suite tests cannot detect cross-repository drift by themselves | the wire shapes are fixed by the mock spec CBL-17.5 section 3.2; the mock side e2e and the 17.16 sandbox runs against live APIs detect drift |
| T-CONNECTOR-13 | The interim provider selector (17.9) returns the registered gateway with the smallest provider id; the rule-based routing replaces it in 17.17 | the first adapter works end to end without pre-empting the 17.17 routing decisions; a single-adapter dev stand routes correctly | multi-provider selection ignores network/asset until 17.17, and the selector does not consult the routing rules or the disabled set: the admin disable is not a money-path kill-switch yet; the exposure is the dev/CI stand (BTCPay production hosting is postponed) | the selector is a core port (`domain.Router`); 17.17 swaps the implementation, adapters untouched; until then the disabled set must be enforced by disabling the adapter configuration |
| T-CONNECTOR-16 | Resilience and limiter wiring (17.9): the adapter instance registered in the gateways map is wrapped by `gateway.Resilient` (shared knobs `CONNECTOR_RETRY_*`, `CONNECTOR_BREAKER_*`, `CONNECTOR_BULKHEAD_*`, `CONNECTOR_PROVIDER_ATTEMPT_TIMEOUT`); since 17.6 every provider whose manifest configures a bucket is additionally wrapped by `gateway.Limited` with the kit rate limiter (the Redis bucket when Redis is configured, the local bucket otherwise) | plan 14.5.3 holds for the first real providers: every external call passes retries, breaker, bulkhead, attempt timeouts and the manifest rate limit (NOWPayments 60/s bucket, CoinGate 180/min) | `Resilient` and `Limited` always satisfy `LookupGateway`/`Refunder`/`PaymentExporter` (delegation errors when the inner adapter lacks the capability), so interface assertions on wrapped adapters are optimistic; conformance runs against the bare adapter | `Limited.Provider`/`Resilient.Provider` keep the capability source working through the wrappers; the 17.20 runtime seam standardizes the composition root |
| T-CONNECTOR-14 | Adapter capabilities come from `CONNECTOR_BTCPAY_CAPABILITIES` until the catalog artifact (17.21) and the capabilities API source (17.0) land | the amount decimals and network rows are explicit per stand; no invented catalog entries | a misconfigured env row silently changes amount parsing thresholds; production needs the catalog | fail-fast parsing at startup (E-7); the manifest keeps `capabilities_source: api` as the target |
| T-CONNECTOR-15 | BTCPay payouts and refunds use the two-step pull payment flow (verified against the regtest stand); approval flows and payout execution stay in 17.11/17.12 | the real API shape is respected (string method ids on the pull payment, destination and `BTC-CHAIN` on the payout); one flow covers payout and refund destinations | created payouts wait for operator approval before funds move (operations job); the whole `CreatePayout` is retried by the resilience stack, so a lost response can leave an orphan pull payment or payout at the provider (E-11; funds still cannot move); the stand wallet is watch-only by design so signing happens on the provider | `CreatePayout`/`CreateRefund` return the payout id with the submitted state and the pull payment name carries the platform payout id for dedupe; 17.11 approves only the returned id and owns the compensation, 17.12 the refund orchestration; 17.16 keeps the wire shapes re-validated |
| T-CONNECTOR-17 | The CoinGate payout/refund and NOWPayments payout wire shapes are provisional (`POST /v2/send_request` with `request_id`, `GET /v2/send_requests/{id}`, `POST /v2/refunds`; `POST /v1/payout`), pinned by the 17.5 fixtures and exercised per the conformance suite | the adapter surface is complete for CONF-7 without pre-empting the provider confirmation flows (CoinGate Send Request 2FA, NOW JWT + TOTP batches) and the refund callbacks | the real provider endpoints and their states may differ from the provisional shapes | 17.11 owns the payout flows and 17.12 the refunds; the sandbox runs of 17.16 re-validate both; the adapter shapes are confined to `internal/providers/<provider>` |
| T-CONNECTOR-18 | BitPay request signing (17.8) lives in the adapter with the new dependency `github.com/decred/dcrd/dcrec/secp256k1/v4` (the only default-set deviation): the key from `CONNECTOR_BITPAY_PRIVATE_KEY_PEM` (Vault in production) signs `sha256(url + body)` as hex DER and `X-Identity` carries the compressed public key, exactly the official SDK key utilities; the wire shape follows the official API (decimal major-unit `price`/`amountPaid`, the `merchant/invoice` facade envelope, the `token` create-body field, `posData` as the network carrier) - the initial 17.5 approximation of integer minor units, a flat body and a base64 signature was disproved by the 17.8 security review and realigned here and in the mock (cbl-mock-gateway@808c38b); the mock only checks header presence (T-MOCK-06) | the only adapter that can authenticate against the real BitPay; the unit suite proves the scheme offline (hex DER parse, verify over url + body, compressed identity); no key material in fixtures, logs or error texts (E-1/E-10); the mocked stand now emulates the documented contract | a connector compromise exposes the merchant signing key; the exact `amountPaid` unit assumption (major decimal of the pricing currency) and the token transport on reads are validated only by the 17.16 sandbox run (KI-connector-10) | the key is a secret isolated to the adapter config and never rendered; a wrong key is a business rejection (`ErrRejected`, E-7) and never trips the breaker; the egress vault task moves signing behind a dedicated service |
| T-CONNECTOR-19 | Payout pipeline (17.11): one enabled provider per payout is selected deterministically by the allowlist and capabilities (no cross-provider failover after submission, because a lost response could have created a payout elsewhere); the `provider_payouts` mapping is the replay barrier (the inbox covers exact redelivery, the payout id covers business retries and the advisory transaction lock serializes concurrent deliveries of one payout) and the status source (polling worker, CAS plus outbox in one transaction); business rejections become failed events with a fixed reason, temporary failures use the retry topics | the saga receives submitted/confirmed/failed with the provider and never sees double credits or double events; provider texts stay in logs | a crash between the provider call and the commit re-calls the provider: the recovery relies on provider-side dedupe by the platform payout id (the command reference: BTCPay pull-payment name, CoinGate `request_id`, BitPay `reference`) and the operator runbook; provider status polling is interval-based and a payout can stay submitted while an operator approves at the provider; the payout wire shapes remain provisional until the 17.16 sandbox runs; NOWPayments payout batches (JWT/TOTP) are H2; the admin disabled set is not consulted by the payout router yet (disable through the allowlist/restart, T-CONNECTOR-13) | KI-connector-11 records the polling limits; the provider dispatch layer (17.20) standardizes the composition; a lookup-based recovery is a candidate for 17.13/17.16 |
| T-CONNECTOR-20 | Provider refunds (17.12): the refund marker of the command routes to the provider of the original invoice mapping (not the capability router - a refund must return through the provider that took the money), the adapter refunds API is called with the provider invoice id and the platform amount, the mapping records the refund marker and the status worker polls the optional `RefundStatusGateway` | the refund lands at the right provider with an audit trail; a disabled or non-refunding provider fails closed with a fixed reason and the saga compensates; exactly-once resolution shares the payout CAS mechanics | a crash window between the refund call and the commit relies on the provider reference (`refund-<payout_id>` / `reference` / `platform_id`); refund wire shapes are provisional until 17.16 (KI-connector-12); BitPay automatic under/over refunds are deferred (KI-connector-12); NOWPayments refunds stay manual (decision 8) | unit P-01..P-10, storage I-01/I-02, the refund stage of the Kafka e2e and the adapter endpoint tests pin the behaviour |
| T-CONNECTOR-21 | Per-provider payment exports (17.13): the optional `PaymentExporter` SPI behind `ListGatewayPayments`; the cursor is an opaque offset (base64url `x1:<n>`, page-based providers derive `page`, offset-based pass `skip`/`offset`), rows are mapped fail-soft (an unmapped provider word exports as `UNSPECIFIED` + `raw_status`, missing amount evidence leaves `received_amount` unset on the wire), the export resolves the provider from the wired gateways map (not the router or the disabled set) and the transient failure classes keep the standard gRPC mapping | the reconciler pulls any wired provider without core changes (SPI v1 pattern); one odd provider row or amount cannot block the reconciliation window; the offset cursor is simple and replay-safe for the reconciler dedup | the offset cursor has no provider snapshot, so concurrent provider-side changes can duplicate or skip rows (the reconciler dedupes by `(provider, gateway_id)`); a provider clamping the page size below the request limit silently shortens the walk (KI-connector-13); the export ignores the admin disabled set on purpose (reconciliation must still see a disabled provider, T-CONNECTOR-13 scope); wire shapes are provisional until the 17.16 sandbox runs | transports X-01..X-25, the four adapter export suites and the wrapper delegation rows pin the behaviour; the shapes are re-validated by the 17.16 runs |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-connector-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |
| KI-connector-02 | No real provider adapter for NOWPayments/CoinGate/BitPay | normalization of those providers is verified by the sandbox decoder and e2e on testcontainers | the four adapters landed (17.6 NOWPayments, 17.7 CoinGate, 17.8 BitPay, 17.9 BTCPay); the remaining exposure is the mock-profile-only verification: real sandbox runs - 17.16 (needs the 17.0 accounts) | open |
| KI-connector-03 | No mTLS on the gRPC server (port 9050), now including the money-routing admin RPCs (`PutRoutingRules`, `DisableProvider`); the admin contour has no authN | do not expose port 9050 outside the trusted network: an unauthorized caller could reroute the platform | task 0.6 (mTLS); the back-office is the only intended client | open |
| KI-connector-04 | No cleanup of published outbox (canonical payload) and old inbox records; raw webhook bodies live in the webhook-ingress outbox and Kafka retry/DLQ topics | tables and topics grow, including sensitive provider headers | retention task (0.4/14.x), ACL/retention DLQ (0.5/5.3) | open |
| KI-connector-05 | Kit inbox: `x-error` is not truncated, a failed retry/DLQ publication is not parked, retries have no delay | with giant/broken events a redelivery storm is possible; in 3.5 bounded by field and body size limits | kit hardening (error truncation, parking, delays) - tasks 1.3-revision/14.2 | open |
| KI-connector-06 | Kafka in compose is PLAINTEXT without SASL/ACL: writing to gateway.webhooks.raw is not restricted to Ingress; Connector trusts `signature_verified` | spoofing the signature flag with topic access bypasses verification | SASL/TLS and ACL for writing to the raw topic - task 0.5; alert on producer mismatch - 14.4 | open |
| KI-connector-07 | Circuit breaker state is in-process: with several replicas the trip threshold and half-open probes are not coordinated; the Redis key `cb:{provider}` is reserved | with multiple instances there may be extra calls to a degrading provider | consolidate shared state when a multi-instance requirement appears (T-CONNECTOR-06); thresholds are configurable | open |
| KI-connector-08 | The admin path for routing rules was missing: `GET/PUT /admin/v1/routing-rules` (12.x) required a new gRPC contract in connector (cbl-protos), RBAC and audit; until then rules were changed by direct configuration replacement | there was no way to change rules in production without SQL | CLOSED 16.10.2026: `ConnectorAdminService` (cf-3.7) implemented in 12.2 (revision-based `PutRoutingRules`, `GetRoutingRules`, `DisableProvider`; migration 00004; RBAC and audit on the back-office side) | closed |
| KI-connector-09 | No provider enable and no retention on `disabled_providers`: the table grows monotonically and a disabled provider can only be re-enabled by SQL | an erroneous or temporary disable requires manual DB work; the table grows without bounds | the `EnableProvider` RPC now exists in the contract (cf-4); implementation with RBAC and audit - 17.15; a retention/archive decision with 14.2 when the volume warrants it | open |
| KI-connector-10 | BitPay details the official docs leave open are assumed: `amountPaid` is read as a decimal major-unit amount of the pricing currency, and the pairing token on reads is sent as a Bearer header (the create body always carries it) | a wrong assumption fails the 17.16 sandbox run loudly (the fetch path and the amount echo fail closed); no silent money movement | re-validated and fixed with the 17.0 account in the 17.16 sandbox run (the wire details are confined to `internal/providers/bitpay`) | open |
| KI-connector-11 | Payout status is read by interval polling only (no payout webhooks/confirmations are wired); a payout waiting for a provider-side approval stays submitted; `provider_payouts` has no retention | resolution latency equals the polling interval; manual provider approval needs the operator; table growth | per-provider payout confirmations as their APIs expose them (17.16); retention with 14.2 | open |
| KI-connector-12 | BitPay automatic under/over refunds are not implemented (the invoice refund flags at creation and the payment-side over/under decision are not wired); the refund wire shapes of CoinGate/BitPay are provisional until the 17.16 sandbox runs | under/over refunds stay manual; a wrong provisional shape fails loudly at the provider (no silent money movement) | 17.16 validates the refund shapes; the automatic over/under flow needs a payment-side decision task | open |
| KI-connector-13 | The export wire shapes (NOWPayments `GET /v1/payment/` window/page parameters, CoinGate `GET /v2/orders` from/to, BitPay date window, BTCPay unix window) are provisional; the conformance suite has no export check yet; a provider clamping the page size below the requested limit silently shortens the walk; the offset cursor has no provider snapshot | a drifted provider list shape fails the page loudly (decode errors at the adapter); a clamped page can miss rows between windows until the next run; the reconciler dedupes by `(provider, gateway_id)` and tolerates overlap | the 17.16 sandbox runs validate the shapes and the clamp behavior; the conformance check and provider-keyed cursors are candidates for 17.5-revision/17.16 | open |

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
| CBL-17.2 (part 2, increment 5) | 2026-10-17 | done, review closed (fetch mandate enforcement: manifest `FetchMandatory` with the token_echo validation and the CoinGate/BitPay built-ins, normalizer predicate `WithFetchMandatory` wired from the registry, the provider key normalized before every lookup so casing cannot bypass the mandate; process fact: 2 review passes (no-context, security) ran before the commit, the architecture and with-context passes ran on it and their findings are closed in the follow-up commit; unit, integration and e2e green) | `.specs/CBL-17.2-provider-core.md` | fa73400, follow-up f2f7388 |
| CBL-17.2 (part 2, increment 6) | 2026-10-17 | done, review closed (money-path `ConnectorService`: `CreateInvoice` with the idempotent replay by order id (unique index, migration 00006) and a conflict on a different request hash, mapping save on an independent budget and retryable `Unavailable` on failure, checkout_url scheme validation; `GetGatewayPayment` rejects unknown adapter states instead of mapping them; `GetCapabilities` serves the loader (empty aggregate no longer touches the cache) and answers `Unavailable` unwired; CBL-003 error mapping incl. caller deadline/cancel and sanitized internals; `ListGatewayPayments` Unimplemented; main registers the server fail-closed; 4 review passes on the staged diff, findings closed in this commit: provider_identity note for adapters recorded in the spec) | `.specs/CBL-17.2-provider-core.md` | this commit |
| CBL-17.5 | 2026-10-17 | done, review closed (adapter conformance suite: `internal/conformance` testkit with the `CONF-1..CONF-10` checks, data-driven wire mock (route patterns, `{{request.<path>}}` templates, failure injection, request recording), fixture sets for the five profiles mirroring the mock-gateway wire contract (statuses, create/payment/payout/lookup payloads, webhook valid/tampered/missing vectors incl. the token-echo unsigned body); `StrategyFromManifest`/`SignBody`; meta-tests prove the compliant adapter passes and each mutant fails exactly one check; focused internal tests cover the token-echo CONF-6 path; core changes: `domain.ErrGatewayNotFound` with the `NotFound` mapping (also fixes a flaky replay test by a deterministic request expiry), additive manifest fields `verify_encoding`/`verify_prefix` with strategy-gated validation and the BitPay/BTCPay built-ins; 4 review passes on the staged diff, findings closed: CONF-6 missing-signature fixture, template 400 on unresolved placeholders, strengthened CONF-3/5/7/10 assertions, skip recording, documentation alignment; unit coverage total 86.6%, conformance package 80.5%, integration and e2e regression runs green, lint 0) | `.specs/CBL-17.5-conformance-suite.md` | 197ed10 |
| CBL-17.9 (increment 1) | 2026-10-17 | done, review closed (BTCPay adapter: `internal/providers/btcpay` implements SPI v1 against Greenfield (hosted invoice create/get with the request-horizon expiry clamp, order lookup tolerant of list/object answers, `BTCPay-Sig` verification via kit raw HMAC, pull payments for payouts/refunds, status/type tables without fabrication); conformance `Run` green for the btcpay fixtures (CONF-1..CONF-10); core additions: `usecase.NewStaticSelector` (interim until 17.17) and `usecase.NewGatewayStatusFetcher` wired into main, capabilities loader fed by the adapter; the compose stand gained the mock-gateway service and `CONNECTOR_BTCPAY_*` and the cross-service scenario E2E_Connector_BTCPayProfile PASSES (create through ConnectorService, canonical reads before/after the mock simulate); the adapter is wrapped by the shared resilience stack (T-CONNECTOR-16) and the manifest carries the fetch mandate; unit coverage total 87.3%, btcpay package 89.1%, integration and e2e regression runs green, lint 0; the local BTCPay regtest stand is increment 2) | `.specs/CBL-17.9-btcpay-adapter.md` | 019cd9a |
| CBL-17.9 (increment 2) | 2026-10-17 | done, review closed (local BTCPay regtest stand `test/e2e/btcpay-stand` (bitcoind regtest, NBXplorer 2.5.14, BTCPay Server 2.0.6, postgres, a signature-logging sink, the connector) with the programmatic setup flow (admin, API key, store, webhook, watch-only wallet, regtest funding); the sandbox scenario E2E_Connector_BTCPayRegtest PASSES 18/18 (create through ConnectorService, real regtest payment, CONFIRMED/Settled through the adapter, provider order lookup, deliveries with offline `BTCPay-Sig` verification, refund wire shape accepted); the run caught and corrected a real API mismatch: the payout flow is two-step (pull payment `payoutMethods` are method-id strings; the destination and the `BTC-CHAIN` method belong to the payout), the adapter, conformance fixtures (second payout route) and tests updated; E2E.md section 9 documents the stand | `.specs/CBL-17.9-btcpay-adapter.md` | f0427d5 |
| CBL-17.7 | 2026-10-17 | done, review closed (CoinGate adapter: `internal/providers/coingate` implements SPI v1 against the v2 API (orders create/get with the numeric id and the request-horizon expiry echo, order lookup, token-echo verification over the raw body via kit `webhooksig`, provisional payout/refund shapes owned by 17.11/17.12, statuses without fabrication); the shared provider-agnostic helper `internal/providers/wire.DecodeArrayOrObject` extracted at the second adapter (decision 21) and the BTCPay lookup refactored onto it; `CONNECTOR_COINGATE_*` wiring plus the `CONNECTOR_PROVIDERS` CSV allowlist selecting adapters (the e2e scenario selects one; an empty PowerShell env value deletes a variable, so the allowlist replaced the empty-BTpay-URL switch); conformance `Run` green for the coingate fixtures (CONF-1..CONF-10); cross-service scenario E2E_Connector_CoinGateProfile PASS (mark `20261017t074107z`); unit coverage total 87.0%, coingate package 83.3%, integration and e2e regression runs green, lint 0; the real CoinGate sandbox run needs the 17.0 account and moves to 17.16) | `.specs/CBL-17.7-coingate-adapter.md` | 6c04530 |
| CBL-17.6 | 2026-10-17 | done, review closed (NOWPayments adapter: `internal/providers/nowpayments` implements SPI v1 against the NOWPayments API (hosted invoices with JSON-number amounts and the request-horizon expiry clamp, payment reads with the received evidence from `actually_paid` and the pay asset resolved by the exact configured network row, the envelope/array/object lookup, canonical JSON HMAC-SHA512 IPN verification via kit `webhooksig`, nine statuses without fabrication, the scale guard, create-network validation, provisional payout shapes; no Refunder, refunds are manual by decision 8); the manifest gained `fetch_mandatory` (the normalizer resolves NOW deliveries through the adapter) and every provider with a manifest bucket is now wrapped by `gateway.Limited` with the kit limiter (T-CONNECTOR-16, `Limited.Provider` added for the capability source); `CONNECTOR_NOWPAYMENTS_*` wiring through the allowlist; conformance `Run` green (CONF-1..CONF-10); E2E_Connector_NowPaymentsProfile PASS (mark `20261017t082057z`); unit coverage total 86.7%, nowpayments package 86.0%, integration and e2e regression runs green, lint 0; the real NOWPayments sandbox run needs the 17.0 account and moves to 17.16) | `.specs/CBL-17.6-nowpayments-adapter.md` | this commit |
| CBL-17.8 | 2026-10-17 | done, review closed (BitPay adapter: `internal/providers/bitpay` implements SPI v1 against the official BitPay API v2 contract (invoice create/get with the `merchant/invoice` facade envelope and decimal major-unit amounts, the required `token` body field, `posData` as the network carrier, array-or-object order lookup, `x-signature` raw-HMAC base64 verification via kit `webhooksig`, statuses without fabrication, provisional payout/refund shapes); every request is ECDSA secp256k1-signed (hex DER `X-Signature` over url plus body, compressed-key `X-Identity`) with SEC1/PKCS8 PEM loading over the new dependency `decred/dcrd/dcrec/secp256k1/v4` (T-CONNECTOR-18; the unit suite verifies the signature offline per key form); the blocking security-review finding (the 17.5 wire approximation would have mispriced invoices against the real API) was closed by realigning the adapter, the mock profile (`cbl-mock-gateway@808c38b`), the fixtures and the manifest rate limit (official 100 invoices/hour, D5), plus guards for non-positive/negative amounts, provider amount echo, network rows, empty identifiers, base-URL fragments and caller HTTP clients; conformance `Run` green (CONF-1..CONF-10); E2E_Connector_BitPayProfile PASS 11/11 (mark `20261017t161450z`); unit coverage total 86.6%, bitpay package 85.4%, integration and e2e regression runs green, lint 0; the real BitPay sandbox run needs the 17.0 account and moves to 17.16 (KI-connector-10) | `.specs/CBL-17.8-bitpay-adapter.md` | this commit |
| CBL-17.11 (increment 1, connector) | 2026-10-18 | done, review closed (payout pipeline H1: migration 00007 provider_payouts with the replay barrier and pending index; PayoutProcessor consumes payout.commands in the Guard transaction, validates untrusted commands, selects one enabled provider by CONNECTOR_PAYOUT_PROVIDERS and capabilities, records the mapping and publishes gateway.payout.events submitted with provider and gateway id; business rejections publish failed with a fixed reason, temporary failures retry; PayoutStatusWorker polls submitted rows, touches non-terminal ones and resolves confirmed/failed with CAS plus outbox in one transaction (exactly one publisher); main wires the second consumer and the worker; unit P-01..P-08, storage integration I-01..I-03, Kafka e2e E2E_Connector_PayoutPipeline green; coverage 86.9% unit+integration; NOW payouts H2 deferred; the payout-side provider propagation follows in cbl-payout) | `.specs/CBL-17.11-payout-pipeline.md` | this commit |
| CBL-17.12 (increment 2, connector) | 2026-10-18 | done, review closed (provider refunds: the refund marker routes to the original provider invoice (`provider_invoices.ByPaymentID`), requires an enabled provider with `domain.Refunder` and calls the provider refunds API (CoinGate `/v2/refunds`, BitPay `/refunds`, BTCPay refund pull payment); `provider_payouts.refund_invoice_id` (migration 00008) records the marker; the worker polls the optional `RefundStatusGateway` (BTCPay alias, CoinGate/BitPay endpoints) and keeps other providers on the manual path; NOW refunds manual, BitPay automatic under/over refunds deferred (KI-connector-12); red-first run recorded; coverage 87.0% unit+integration (no regression); the Kafka e2e gained the refund stage; lint 0) | `.specs/CBL-17.12-connector-refunds.md` | this commit |
| CBL-17.13 (increment 1, connector) | 2026-10-18 | done, review closed (per-provider payment exports: `domain.PaymentExporter` implements the `ListGatewayPayments` RPC deferred in 17.2; the opaque offset cursor (`x1:<n>`) pages NOWPayments/CoinGate by page and BitPay/BTCPay by skip/offset through `providers/wire.DecodeEnvelopeOrArray`; rows are mapped fail-soft (`exportStatusOf`: an unmapped word exports as `UNSPECIFIED` with `raw_status`, missing amount evidence leaves `received_amount` unset); the transport validates provider/window (31 d)/limit (100/500)/cursor (64 B) and keeps the standard error taxonomy; `gateway.Limited` throttles with the status bucket, `gateway.Resilient` uses a 3 s attempt budget against the 10 s RPC deadline; red-first run recorded; unit+integration coverage 87.7% (no regression); lint 0 and the em dash check green; the export wire shapes are provisional (KI-connector-13); the reconciler service is the next increment) | `.specs/CBL-17.13-connector-export.md` | this commit |
| CBL-4.2 (connector increment) | 2026-10-19 | done (invoice replay destination: migration 00009 stores `provider_invoices.address`/`checkout_url` (bounded), the mapping Save/scan and the idempotent `CreateInvoice` replay return them, so the payment creation flow can finish after a lost response (E-3); a blocking finding of the CBL-4.2 review round; storage integration and transport replay tests green) | `.specs/CBL-4.2-connector-invoice-destination.md` | this commit |

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

Adapter conformance (17.5):

1. Every adapter test package calls `conformance.Run(t, conformance.Harness{...})`; each
   failed `CONF-*` check names the fixture and the violated property: fix the adapter, not
   the suite or the fixture.
2. `CONF-0` is a harness or fixture problem (unknown provider, manifest id mismatch, nil
   factory); `CONF-4` names the raw status whose mapping diverged; `CONF-6` distinguishes
   a valid, a tampered and a missing signature; `CONF-10` separates the temporary (503)
   from the rejected (4xx) class.
3. `StrategyFromManifest` is the mapping from manifest fields to kit `webhooksig`
   strategies (the suite is its first consumer; the defense-in-depth re-check reuses
   it); a provider with a new scheme extends the manifest (encoding, prefix),
   not the core.

BTCPay adapter (17.9):

1. Startup without `CONNECTOR_BTCPAY_BASE_URL`: the adapter is absent by design
   (`unavailableSelector`, empty gateways); a configured adapter logs
   `btcpay adapter configured`. A startup error names the missing or invalid env
   (`btcpay: invalid configuration`, `capabilities env`).
2. `CreateInvoice` answers provider authentication failures as
   `FailedPrecondition`/`Unavailable`: check `CONNECTOR_BTCPAY_API_TOKEN`
   (`Authorization: token`), the store id and the base URL reachability in the
   logs (`provider status 401`).
3. `GetGatewayPayment` with an unknown provider status maps to the sanitized
   `Internal` (`provider returned an unknown state`); the raw status word is
   visible in the normalizer DLQ context. Extend the adapter tables, never map
   silently.
4. Webhook deliveries: the ingress verifies first; the adapter re-verification
   (`BTCPay-Sig`) is exercised by the conformance suite and adapter tests, and
   the fetch-to-verify path reads the authoritative status through
   `GetPayment`. A fetch or status mismatch means the ingress secret and
   `CONNECTOR_BTCPAY_WEBHOOK_SECRET` differ.
5. Payouts/refunds use the two-step provider flow (verified against the BTCPay
   regtest stand, 17.9 increment 2): the pull payment lists the method ids as
   strings and holds the funds; the payout carries the destination and the
   `BTC-CHAIN` method and usually waits for operator approval (reported as
   `PayoutSubmitted`); `GetPayout` classifies `AwaitingApproval`/`AwaitingPayment`/
   `InProgress` as submitted, `Completed` as confirmed, `Cancelled` as failed. A
   temporary failure of the second call can leave an orphan pull payment at the
   provider (E-11): dedupe by the pull payment name (the platform payout id)
   before approving; the approval and execution flows are operated manually at
   the provider: the connector payout pipeline (17.11) submits and polls the
   payout, a provider-side approval keeps it submitted (KI-connector-11).
6. Amounts of an asset outside `CONNECTOR_BTCPAY_CAPABILITIES` are rejected
   (`FailedPrecondition`): add the asset row (catalog 17.21 replaces the env).

CoinGate adapter (17.7):

1. Startup without `CONNECTOR_COINGATE_BASE_URL` (or with the provider excluded
   from `CONNECTOR_PROVIDERS`): the adapter is absent by design; a configured
   adapter logs `coingate adapter configured`.
2. `CreateInvoice` with a 401/403: the token is wrong
   (`FailedPrecondition`, `provider status 401`); fix `CONNECTOR_COINGATE_API_TOKEN`.
   A 429 means the provider rate limit: the manifest limiter numbers (180/min)
   apply when the limiter is wired; check the limiter metrics.
3. `GetGatewayPayment` with an unknown provider status maps to the sanitized
   `Internal`; extend the adapter table, never map silently.
4. Callbacks: the ingress verifies the token echo first and records the state;
   the adapter re-verification (`CONNECTOR_COINGATE_IPN_TOKEN`) is exercised by
   the conformance suite and the fetch path reads the authoritative order.
5. Order lookup answers `[]` -> `ErrGatewayNotFound`: the provider has no order
   for the reference (normal before creation or after archive).

NOWPayments adapter (17.6):

1. Startup without `CONNECTOR_NOWPAYMENTS_BASE_URL` (or excluded from
   `CONNECTOR_PROVIDERS`): the adapter is absent; a configured adapter logs
   `nowpayments adapter configured`.
2. A canonical JSON signature mismatch (`x-nowpayments-sig`) means
   `CONNECTOR_NOWPAYMENTS_IPN_SECRET` differs from the NOW dashboard value;
   the signature is whitespace-insensitive (the canonical form is signed).
3. `partially_paid` maps to the canonical underpaid state (a final status for
   NOW); the payment rules live in 17.10.
4. The pay asset is resolved from `pay_currency` by the exact configured network
   row (`usdttrc20` -> `USDT`); an unmapped pay currency fails the fetch closed:
   add the capability row (catalog 17.21 replaces the configuration).
5. Payouts are H2: the default `CONNECTOR_PAYOUT_PROVIDERS` excludes
   NOWPayments until the JWT login and the TOTP batch confirmation land; the
   provisional adapter shape is not selected by the pipeline.

BitPay adapter (17.8):

1. Startup without `CONNECTOR_BITPAY_BASE_URL` (or excluded from
   `CONNECTOR_PROVIDERS`): the adapter is absent; a configured adapter logs
   `bitpay adapter configured`. A startup error names the fault
   (`bitpay: invalid configuration`): an unparsable
   `CONNECTOR_BITPAY_PRIVATE_KEY_PEM` (want a SEC1/PKCS8 PEM on the secp256k1
   curve), a missing webhook secret or malformed capability rows.
2. Every request is ECDSA-signed (`X-Identity` compressed-key hex, `X-Signature`
   hex DER over url plus body; the official SDK scheme); a 401/403 is a key or
   token fault classified as a rejection (the breaker does not trip): check the
   key pair and the pairing token. The create body always carries the token
   (required by the real API).
3. A signature mismatch on the IPN (`x-signature`, base64 raw HMAC-SHA256)
   means `CONNECTOR_BITPAY_WEBHOOK_SECRET` differs from the BitPay dashboard
   value; the fetch-to-verify path then reads the authoritative invoice.
4. Amounts cross the wire as decimal major-unit numbers of the pricing currency
   (the amount scale is asserted against `CONNECTOR_BITPAY_CAPABILITIES` before
   any call and the returned price must echo the request); an asset outside the
   capabilities, a network row without a matching asset and a non-positive
   amount are rejected (catalog 17.21 replaces the configuration).
5. Payout/refund wire shapes are exercised by the payout pipeline (17.11); the
   sandbox run of 17.16 re-validates them together with the amountPaid unit
   assumption and the token transport (KI-connector-10).

Payout pipeline (17.11):

1. Logs `payout submitted to provider` (payout_id, provider, gateway_payout_id),
   `payout command replayed`, `payout rejected by provider` and
   `payout failed before submission`.
2. DLQ `payout.commands.dlq`: the reason is in the `x-error` header; typical
   reasons - an invalid command (payout id, amount, network, address,
   idempotency key) or exhausted retries; fix the producer and replay manually.
3. A payout stuck in submitted:
   `SELECT provider, gateway_payout_id, state, updated_at FROM provider_payouts
   WHERE payout_id = '...'`; the worker polls every
   `CONNECTOR_PAYOUT_POLL_INTERVAL`; a provider-side approval (for example a
   BTCPay pull payment) is manual; `payout status read failed` WARN means a
   temporary read error and the row is polled again.
4. `payout resolution failed` WARN: the CAS or the outbox write failed, the next
   sweep retries; a resolved payout never publishes twice.
5. A failed event without a `provider_payouts` row proves the payout never
   reached the provider (no submission; the saga releases the hold); with a row
   the provider reported the failure or the status read found no payout - check
   the gateway before any manual reversal.
6. Refunds (17.12): logs `refund submitted to provider`; a refund without a
   mapping fails with `refund invoice has no provider mapping`, a disabled or
   non-refunding provider with `provider refunds are not enabled` or
   `provider does not support refunds`; the row is found by
   `SELECT provider, gateway_payout_id, refund_invoice_id FROM provider_payouts
   WHERE payout_id = '...'`; provider refund statuses are polled through the
   adapter refund endpoint (a provider without it keeps the manual path).

Provider exports (17.13):

1. `ListGatewayPayments` answers `InvalidArgument` for a missing/oversized
   window or limit, a malformed or oversized cursor and an empty provider;
   `NotFound` for an unregistered provider; `FailedPrecondition` for a
   wrapped gateway without the export capability. A cursor is opaque: always
   continue with `next_cursor`, never construct one by hand.
2. A row with `status = UNSPECIFIED` and a non-empty `raw_status` is an
   unmapped provider word (COMMON-3): the reconciler raises a status alert
   and never moves money; extend the adapter status table, never map silently.
3. An unset `received_amount` (no field on the wire) means the connector has
   no amount evidence for the row (unparsable provider amount or no deposit):
   it is not a zero; a genuine zero carries the asset and the decimals.
4. `Unavailable` on an export means a transient provider/DB path failure (the
   standard retryable class); a shaped provider answer that no longer decodes
   surfaces as `Internal` and indicates provider API drift (KI-connector-13):
   compare against the fixtures and update the adapter.
5. A full page carries `next_cursor`; a short page ends the walk. A provider
   that clamps the page size below the requested limit shortens the walk
   silently (KI-connector-13): reconciliation windows and the reconciler
   dedup absorb the gap until the 17.16 sandbox runs validate the behavior.


