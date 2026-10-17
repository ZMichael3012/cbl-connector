# CBL-17.2. Connector SPI revision: manifests, SPI v1, provider invoice mapping

| Field | Value |
| :-- | :-- |
| Epic | E17 (task CBL-17.2) |
| Repository | `cbl-connector` |
| Size | L (delivered in two parts) |
| Wave and parallel group | W5-PROVIDER, stage B |
| Depends on | CF-4 (`cbl-protos@cf-4`/`cf-4.1`), `cbl-kit` 7900510 (`webhooksig`, `ratelimit`, `money` parsing) |
| Contract version | consumes `cbl-protos@cf-4.1`, `cbl-kit` 7900510 |
| Status | in progress: FR-1 - FR-7 delivered and reviewed; the ConnectorService transport, the main wiring (limiters from manifests, capabilities loader, fetcher) and FR-8 remain |

## 1. Context and goal

The connector becomes the provider contour core (CBL-004, E17; plan sections 3.1 - 3.3 and
14.2): a versioned provider manifest registry, the SPI v1 revision with optional interfaces,
the provider invoice mapping that resolves real webhooks to platform payments
(T-CONNECTOR-03), the fetch-to-verify pipeline, the capabilities loader and per-provider
limiter wiring. The task is delivered in two parts because the adapters (17.6 - 17.9)
consume the foundation incrementally; part 1 (this commit) adds the registry, the SPI v1
interfaces and the mapping; part 2 adds the pipelines and wiring.

## 2. Requirements

FR-1 (part 1, done). Provider manifest schema v1 (`internal/domain/manifest.go`): version,
id, display name, operations (invoice/payout/refund), verification strategy
(raw_hmac/canonical_json_hmac/token_echo/none, defense in depth T-CONNECTOR-01),
capabilities source (api/static), Vault secret path and keys, token bucket limits, sandbox
flag; `Validate` enforces the invariants (unknown version/strategy/operations, missing
strategy fields, burst and refill bounds aligned with the kit limiter). `BuiltinManifests`
carries the evaluated providers with the plan decision limits: CoinGate 180/min Burst 60,
NOWPayments 60 rpm (the conservative width of the official 3 rps), BitPay 30 rpm, BTCPay
unlimited (own stand), mock sandbox with `none` verification.

FR-2 (part 1, done). `usecase.ManifestRegistry`: validated registration (duplicates and
invalid manifests are errors), `Lookup`, `IDs` (stable order), `List`; main wires the
built-ins and passes `IDs` as the registry side of the admin known-provider union
(CBL-12.2 FR-3).

FR-3 (part 1, done). SPI v1 revision (`internal/domain/gateway.go`), additive by
construction: the `PaymentGateway` core port stays source-compatible; optional interfaces
`LookupGateway` (`LookupByOrderID`, invoice recovery after timeouts) and `Refunder`
(`CreateRefund`; providers without it use the manual path, decision 8) join it; the SPI v1
doc comment fixes the rule "the core depends on interfaces and manifests only".

FR-4 (part 1, done). Provider invoice mapping: migration `00005_provider_invoices.sql`
(unique `(provider, gateway_id)`, indexes on `payment_id` and `(provider, order_id)`),
`domain.ProviderInvoice` with validation, `storage.ProviderInvoicesRepository`
(`Save` idempotent insert with created flag, `ByGatewayID`, `ByOrderID`, not-found
sentinel); integration tests on real PostgreSQL cover the round trip, the duplicate save
and the validation.

FR-5 (part 2, done). Fetch-to-verify pipeline: the normalizer resolves the CF-4 state
(`signature_state`, with the transition fallback to the deprecated `signature_verified`) and
fails closed on any non-verified delivery; ordinary deliveries decode through the provider
decoder and resolve `payment_id` through the provider invoice mapping when the body does not
carry it; `fetch_required` deliveries (CoinGate, BitPay) skip the decoders and build the
canonical event from the adapter `FetchPaymentStatus` re-check (the status model decides,
occurred_at falls back to the receipt time), and without a registered fetcher, gateway id or
mapping they fail closed into retry/DLQ. `main` wires the mapping repository; the fetcher
arrives with the adapters 17.6 - 17.9.

FR-6 (part 2, done). Capabilities loader: `usecase.CapabilitiesLoader` aggregates
adapter capabilities through an injectable cache (Redis in production) with live fallback on
a cache miss or error, provider filtering (`ErrProviderNotFound`), provider-qualified rows and
stable ordering; `CapabilityCache` (Load/Save with TTL) and `CapabilitySource` ports keep the
usecase infrastructure-free; the Redis adapter is `storage.CapabilitiesCache` (JSON snapshot
under `connector:capabilities:v1`, corruption is a read error, so the loader never trusts
garbage) with a real-Redis integration test. Remaining: constructing the loader in `main`
and serving the `GetCapabilities` provider filter once the transport step lands.

FR-7 (part 2, limiter done). Per-provider limiter: `gateway.Limited` decorates any
`PaymentGateway` with the kit `ratelimit` port (shared Redis bucket, local fallback), keys
`<provider>:<op>` so the kit prefix yields the CBL-003 `rl:provider:<provider>:<op>` form;
a denied token returns `ErrProviderOverloaded` without touching the adapter, a limiter
storage failure fails open (the provider 429 is absorbed by `PauseProvider`, which drains the
invoice/status/payout buckets with the provider `Retry-After`). Remaining: manifest-limit
wiring in main and the `ConnectorService` gRPC transport over the Router.

FR-8 (part 2, NOT STARTED). `providers` table and admin visibility (17.15 consumes it);
payout/refund operations surface through `ConnectorService` once adapters exist
(17.6 - 17.9, 17.11).

Review closure (4 reviewers, 16.10.2026, covering commits 6f04569 - 6a3555a): findings closed
in increment 4: occurred_at anchors on the receipt time and fails closed without any event
time (T-CONNECTOR-05); the fetched status PaymentID is reconciled with the invoice mapping and
a contradiction goes to the DLQ; `gateway.Limited` forwards the optional SPI v1 interfaces
(`LookupGateway`, `Refunder`) with a refund bucket and reports unsupported operations;
cached capability snapshots pass a read-time validation against registered sources (the
route:config discipline); `provider_invoices` replays only a matching payment and request
hash, a remap is a conflict; manifests of re-verifying providers require Vault secret
references; `raw_status` is carried by the canonical event; a receipt-time regression test
and a mapping-mismatch test were added. Review follow-ups recorded as pending scope:
enforcement of the fetch mandate for CoinGate/BitPay via the registry (the ingress flag is
relied on today), manifest base URL and the CoinGate orders/hour queue limit,
credential-scoped limiter keys, the CF-4 `Capabilities` context fields (decimals, sandbox,
supports_payouts/refunds) for the transport, and the `providers` table.

## 3. Work plan

Part 1 (this commit): spec -> manifest registry, SPI v1 interfaces, mapping migration and
repository with integration tests -> main wiring of the registry and the admin union ->
battery -> commit `CBL-17.2: provider manifests, SPI v1 interfaces and invoice mapping`.

Part 2 (next): fetch-to-verify, capabilities loader, limiter decorator, `ConnectorService`
transport; then adapters 17.6 - 17.9.

Delivered checks (part 1): unit tests for manifests, registry, mapping validation;
integration tests on PostgreSQL; lint, vet and em dash checks green.
