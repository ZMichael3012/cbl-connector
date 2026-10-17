# CBL-17.9. BTCPay Server adapter

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.2 (SPI v1, manifests), 17.4 (ingress strategies), 17.5 (conformance suite, mock profile) |
| Contract version | none (adapter implements the existing SPI; no `.proto` change) |
| Status | in progress (increment 1: adapter core, wiring, conformance and the mock-profile e2e; increment 2: the local BTCPay regtest stand run) |

## 1. Context and goal

BTCPay Server is the dev/CI provider (plan section 5): our own stand, no external
accounts, the first adapter to prove the onboarding pipeline (manifest,
conformance, sandbox, routing, runbook) end to end (checklist 14.4). This task
implements the SPI v1 adapter against the Greenfield API and wires it into the
connector runtime, using the CBL-17.5 conformance suite and the mock-gateway
`btcpay` profile as the offline contract.

## 2. Scope

In scope (increment 1): `internal/providers/btcpay` adapter (invoice create/get,
order lookup, webhook verification with `BTCPay-Sig`, payouts and refunds via pull
payments), the interim static selector and the status-fetcher bridge, main
wiring from environment, the connector compose stand with the mock-gateway
`btcpay` profile, conformance and cross-service e2e runs.

Out of scope: the local BTCPay regtest stand (increment 2, plan section 5);
routing rules and failover (17.17 replaces the static selector); capabilities
from the provider API and the asset catalog (17.21, 17.0); the payout approval
flow details (17.11) and the refund orchestration (17.12); sandbox regressions
(17.16); multi-store deployments (one store per adapter instance; a delivery of
another store is rejected, E-9).

## 3. Architecture sketch

### 3.1. Package, config and ports

```go
// internal/providers/btcpay/adapter.go
// Config describes the adapter instance.
type Config struct {
    BaseURL       string           // Greenfield root, e.g. http://btcpay:49392
    StoreID       string
    APIToken      string           // Authorization: token <key>
    WebhookSecret []byte           // BTCPay-Sig HMAC secret
    Capabilities  domain.Capabilities // network rows; Provider is forced to "btcpay"
    Timeout       time.Duration    // default 10s
    HTTPClient    *http.Client     // optional, tests
}

// New validates the config and builds the adapter.
func New(cfg Config) (*Adapter, error)

// Adapter implements SPI v1: PaymentGateway plus LookupGateway and Refunder.
var _ domain.PaymentGateway = (*Adapter)(nil)
var _ domain.LookupGateway = (*Adapter)(nil)
var _ domain.Refunder = (*Adapter)(nil)

func (a *Adapter) CreateInvoice(ctx context.Context, req domain.CreateInvoiceRequest) (domain.Invoice, error)
func (a *Adapter) GetPayment(ctx context.Context, gatewayID string) (domain.PaymentStatus, error)
func (a *Adapter) VerifyWebhook(headers map[string][]string, rawBody []byte) (domain.CanonicalEvent, error)
func (a *Adapter) CreatePayout(ctx context.Context, req domain.PayoutRequest) (domain.Payout, error)
func (a *Adapter) GetPayout(ctx context.Context, gatewayPayoutID string) (domain.PayoutStatus, error)
func (a *Adapter) Capabilities() domain.Capabilities
func (a *Adapter) LookupByOrderID(ctx context.Context, orderID string) (domain.PaymentStatus, error)
func (a *Adapter) CreateRefund(ctx context.Context, req domain.RefundRequest) (domain.Refund, error)
```

Bridge and selector (core, provider-agnostic):

```go
// internal/usecase/selector.go
// NewStaticSelector is the interim selector until the rule-based routing of
// 17.17: it returns the only registered gateway or fails with
// ErrProviderUnavailable; the provider order is deterministic (sorted ids).
func NewStaticSelector(gateways map[string]domain.PaymentGateway) (domain.Router, error)

// internal/usecase/fetcher.go
// NewGatewayStatusFetcher adapts the registered gateways to the normalizer
// fetch-to-verify port (PaymentStatusFetcher).
func NewGatewayStatusFetcher(gateways map[string]domain.PaymentGateway) (PaymentStatusFetcher, error)
```

Main wiring (cmd/connector): when `CONNECTOR_BTCPAY_BASE_URL` is set, build the
adapter from `CONNECTOR_BTCPAY_STORE_ID`, `CONNECTOR_BTCPAY_API_TOKEN`,
`CONNECTOR_BTCPAY_WEBHOOK_SECRET`, `CONNECTOR_BTCPAY_CAPABILITIES` (a JSON list
of `{"network","asset","min_minor","max_minor","decimals","confirmations"}`
rows), optional `CONNECTOR_BTCPAY_TIMEOUT`; register it in the gateways map and
pass it to the selector, the capabilities loader sources (the adapter satisfies
`usecase.CapabilitySource` via `Provider()`) and the normalizer fetcher. Without
the env the runtime stays fail-closed exactly as today (`unavailableSelector`,
empty map).

### 3.2. Wire contract (Greenfield)

| Operation | Request | Response |
| :-- | :-- | :-- |
| Create invoice | `POST /api/v1/stores/{store}/invoices` with `{"amount","currency","metadata":{"orderId","orderNetwork"}}` | `{"id","storeId","amount","currency","status","checkoutLink","createdTime","expirationTime","metadata"}` |
| Get invoice | `GET /api/v1/stores/{store}/invoices/{id}` | the same invoice body |
| Invoice list for lookup | `GET /api/v1/stores/{store}/invoices?orderId={order}` | an array of invoice bodies (the mock profile answers a single object: the adapter parses both, D4) |
| Create pull payment | `POST /api/v1/stores/{store}/pull-payments` with `{"name","amount","currency","payoutMethods":[{"destination","paymentMethod"}]}` | `{"id","state"}` |
| Get pull payment | `GET /api/v1/stores/{store}/pull-payments/{id}` | `{"id","state"}` |

Auth: `Authorization: token <APIToken>` on every call. Webhooks:
`BTCPay-Sig: sha256=<hex HMAC-SHA256 over the raw body, WebhookSecret>`; a missing
or malformed header or a digest mismatch is rejected (constant-time via the kit
`webhooksig` raw HMAC strategy).

Status tables:

| Provider status | Canonical | Raw keyword |
| :-- | :-- | :-- |
| New | detected | New |
| Processing | detected | Processing |
| Settled | confirmed | Settled |
| Expired | expired | Expired |
| Invalid | failed | Invalid |

| Webhook type | Canonical | Raw keyword |
| :-- | :-- | :-- |
| InvoiceCreated | detected | New |
| InvoiceReceivedPayment | detected | Processing |
| InvoiceProcessing | detected | Processing |
| InvoiceSettled | confirmed | Settled |
| InvoiceExpired | expired | Expired |
| InvoiceInvalid | failed | Invalid |

Any other status or type is `ErrUnknownState` (no fabricated canonical state,
CONF-5). Webhook `payment_id` comes from `metadata.orderId` (set at creation);
a missing metadata is tolerated at ingress-time parsing and resolved by the
normalizer mapping (T-CONNECTOR-03).

Amounts: decimal strings parsed with the kit `money` package using the decimals
of the asset from the adapter capabilities; `amount` is emitted via
`money.String()` (exact, no float). Times: Unix seconds -> RFC 3339 UTC.

Error classification: transport errors, 429 and 5xx -> `ErrTemporary`; an
unfollowed 3xx redirect -> `ErrTemporary`; 404 on a payment or pull payment read
-> `ErrGatewayNotFound`; other 4xx -> `ErrRejected`; an asset outside the
configured capabilities at runtime -> `ErrRejected` (config faults fail at
startup); malformed JSON -> a wrapped plain error (sanitized upstream).

### 3.3. Data flows

CreateInvoice: build the invoice body (orderId = request.PaymentID,
orderNetwork = request.Network) -> POST -> parse id/link/expiry -> `domain.Invoice`
(Provider "btcpay", CheckoutURL = checkoutLink, Network echo, Amount echo). The
expiry is min(provider `expirationTime`, request horizon), so the invoice never
outlives the requested window (E-8). The idempotent replay is the core contract
(ConnectorService, 17.2): the adapter is called only for a fresh order.

GetPayment/Lookup: GET (by id or by order) -> parse -> canonical status with
`RawStatus` and `GatewayEventTS` (createdTime).

VerifyWebhook: kit raw-HMAC strategy (header `BTCPay-Sig`, SHA-256, hex,
prefix `sha256=`) over the raw body -> parse type -> canonical event
(OccurredAt = receipt time; the webhook carries no event time).

CreatePayout/CreateRefund: pull payment creation (`name` = platform payout or
refund reference, `payoutMethods[0].destination` = the recipient address); the
approval flow is 17.11/17.12.

### 3.4. Storage and migrations

None (the adapter is stateless; the mapping lives in `provider_invoices`).

### 3.5. Error taxonomy

As section 3.2; constructor validation errors are plain errors (configuration
faults, fail fast at startup). No new core error classes.

### 3.6. Decisions and rejected alternatives

- D1. The interim selector returns the only registered gateway; rule-based
  routing and failover belong to 17.17 (the plan replaces 15.2). Rejected:
  wiring the 3.4 router now, because routing rules for real providers are the
  subject of 17.17. Until then the selector also ignores the routing rules and
  the disabled set: the admin disable is not a money-path kill-switch yet
  (recorded in T-CONNECTOR-13); the exposure is the dev/CI stand.
- D2. Capabilities come from configuration (`CONNECTOR_BTCPAY_CAPABILITIES`)
  until the catalog artifact (17.21) and the provider API source (17.0) land;
  the manifest keeps `capabilities_source: api` as the target.
- D3. Payouts and refunds use pull payments (plan section 3.7/3.8); the
  two-phase approval and the refund statuses are 17.11/17.12.
- D4. The lookup parser accepts an array (real Greenfield) and a single object
  (the conformance wire answers a single object); the mock stand itself has no
  lookup route (17.5 defined lookup as fixture-side), so against the compose
  stand a lookup answers 404 and surfs as `ErrGatewayNotFound`. Documented stand
  aid, owned by 17.16.
- D5. `paymentMethod` of pull payments defaults to the asset code (the regtest
  stand accepts it); the network-to-payment-method mapping arrives with the
  catalog (17.21). Rejected: inventing `BTC-Bitcoin` style strings now.
- D6. The adapter is wrapped by `gateway.Resilient` in the composition root
  (T-CONNECTOR-16): every provider call passes the shared retry/breaker/bulkhead
  and attempt timeouts; the manifest rate limiter is wired when a manifest
  configures one (BTCPay declares none). The BTCPay manifest sets
  `fetch_mandatory: true` (D7).
- D7. BTCPay webhook deliveries are always fetch-to-verified: the delivery body
  carries the event type but no authoritative amount evidence, so the
  normalizer resolves the canonical event through the adapter (`GetPayment`),
  mirroring CoinGate/BitPay (17.2 increment 5); the ingress sets
  `fetch_required: true` for the provider in its configuration.
- Rejected: a per-provider runtime service (plan 14.3 forbids it before the
  17.25 triggers); the adapter lives in `internal/providers/btcpay` (14.5.1).

## 4. Functional requirements

FR-1. The adapter implements PaymentGateway, LookupGateway and Refunder with the
wire contract and status tables of section 3.2; constructor validation fails
fast on missing configuration; a webhook of another store is rejected (E-9).

FR-2. `conformance.Run` passes for the adapter against the `btcpay` fixtures
(CONF-1..CONF-10), including CONF-6 (BTCPay-Sig), CONF-7 (payout round trip,
Refunder presence) and CONF-8 (lookup miss wraps ErrGatewayNotFound).

FR-3. Runtime wiring: with `CONNECTOR_BTCPAY_*` configured, ConnectorService
routes invoices to the adapter (interim static selector), GetGatewayPayment
resolves by provider, capabilities come from the adapter, the normalizer
fetch-to-verify resolves btcpay deliveries through the adapter (the manifest
carries `fetch_mandatory`), and every provider call passes the shared resilience
stack (`gateway.Resilient`). Without the env the runtime stays fail-closed.

FR-4. The connector compose stand gains the mock-gateway service and the btcpay
adapter environment; the new e2e scenario proves the cross-service contract:
create an invoice through `ConnectorService`, read it back through the adapter,
advance the mock invoice state, read the canonical status through the adapter.

FR-5. Documentation: AGENTS.md (contracts, architecture, env, runbook, journal),
README, E2E.md scenario.

## 5. Contracts

No `.proto`/Kafka changes; the adapter implements the frozen SPI. The E2E.md
scenario uses the existing `ConnectorService` gRPC contract.

## 6. Data model and migrations

None.

## 7. State machines and rules

The adapter maps statuses as pure functions (section 3.2); no local state.

## 8. Edge cases

E-1. Create answer without `checkoutLink` -> empty CheckoutURL (address flows are
not applicable to hosted BTCPay invoices; callers rely on the URL when present).
E-2. Lookup array empty -> `ErrGatewayNotFound` (no invoice for the order).
E-3. Webhook with an unknown `type` -> `ErrUnknownState`; the normalizer routes
it to the DLQ (fail closed).
E-4. Webhook without `metadata.orderId` -> PaymentID empty; the normalizer
resolves the payment through `provider_invoices` (17.2).
E-5. Pull payment creation answered with an unknown state -> `GetPayout`
returns `ErrUnknownState`; `CreatePayout` accepts the created id whatever the
state (the status read classifies).
E-6. Amount with more fraction digits than the asset decimals in a provider
response -> parse error (provider data fault, sanitized upstream).
E-7. `CONNECTOR_BTCPAY_CAPABILITIES` missing or invalid at startup -> the
service fails fast with a clear log (no silent empty routing); unknown fields,
duplicate networks, missing decimals and inverted bounds are invalid.
E-8. The provider invoice expiry is clamped to min(provider `expirationTime`,
request `ExpiresAt`): a store-configured longer expiry never extends beyond the
requested horizon.
E-9. A webhook whose `storeId` differs from the configured store is rejected
with `ErrInvalidWebhook` (multi-store deployments are out of scope).
E-10. A runtime asset outside the configured capabilities is rejected with
`ErrRejected` (the transport maps it to `FailedPrecondition`); configuration
gaps still fail at startup.

## 9. Non-functional requirements

- NFR-1. Unit+integration coverage of the repository stays at least 80%; the
  adapter package is covered by the conformance run and the unit matrix.
- NFR-2. No new dependencies (stdlib, kit money/webhooksig, existing set).
- NFR-3. Timeouts: the HTTP client carries `Config.Timeout` (default 10s); no
  unbounded reads (response body limit 1 MiB).
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix (no Docker)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| A-01 | constructor validation | missing base URL / store / token / secret / networks; invalid URL; userinfo in the URL; conflicting decimals for one asset; timeout <= 0 clamps; a runtime asset outside the capabilities | errors; valid config passes, Provider forced to btcpay; the runtime asset answers `ErrRejected` |
| A-02 | create happy path | httptest wire asserts POST path, `Authorization: token`, body amount string and metadata | request shape matched; invoice id/link/expiry/echo parsed |
| A-03 | create classification | 503 / 400 / dial error | ErrTemporary / ErrRejected / ErrTemporary |
| A-04 | status mapping | the five provider statuses; unknown word | canonical + raw preserved; unknown -> ErrUnknownState |
| A-05 | payment 404 | wire answers 404 | ErrGatewayNotFound |
| A-06 | webhook verification | valid signature; tampered body; missing header; unknown type | event with mapped state/raw/ids; rejections wrap ErrUnsignedWebhook; unknown -> ErrUnknownState |
| A-07 | lookup | object answer; array answer; empty array; 404 | status; first element; ErrGatewayNotFound; ErrGatewayNotFound |
| A-08 | create payout | wire asserts pull-payment body (name, destination, amount) | id and state parsed; Created -> submitted |
| A-09 | get payout | Created / Completed / Cancelled / unknown | submitted / confirmed / failed / ErrUnknownState |
| A-10 | create refund | address present; address missing | pull payment created with the refund name; ErrRejected without a destination |
| A-11 | amount parse fault | response amount with too many digits | error |

### 10.2. Integration matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| I-01 | conformance run | `conformance.Run` for btcpay fixtures | CONF-1..CONF-10 pass (in-process wire, no Docker) |
| I-02 | selector | one gateway / none / two gateways | returns it / error wrapping ErrProviderUnavailable / deterministic order by provider id |
| I-03 | fetcher | registered provider / unknown provider / adapter error | status passthrough / ErrUnknownProvider / the adapter error passes through |

### 10.3. Smoke set

`make build`, `make test`, `make test-integration`, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Connector_BTCPayProfile (increment 1) | compose stand: connector with `CONNECTOR_BTCPAY_*` pointing at the mock-gateway `btcpay` profile; `CreateInvoice` via grpcurl returns a 32-hex id and a checkout link; `GetGatewayPayment` answers the canonical state of `New`; the mock invoice is advanced with `POST /api/v1/simulate` `provider=btcpay` `status=confirmed`; `GetGatewayPayment` then answers `CONFIRMED` with raw `Settled` | `test/e2e/E2E.md` |
| E2E_Connector_BTCPayRegtest (increment 2) | local BTCPay regtest stand (server, NBXplorer, bitcoind): store config, invoice creation, regtest payment, webhook to the ingress contract, refund pull payment | plan section 5, run artifacts in `.runs/` |

### 10.5. Load plan

Not applicable (adapter of an existing service; the connector load profile is
unchanged). Rate limits of the stand are trivial.

### 10.6. Test-first evidence

Red run recorded before implementation (17.10.2026): `go test ./internal/providers/...
./internal/usecase/...` failed with
`internal/providers/btcpay: no non-test Go files` and
`undefined: usecase.NewStaticSelector`, `undefined: usecase.NewGatewayStatusFetcher`;
the rest of the suite stayed green. After implementation: conformance `Run` green for
the btcpay fixtures, unit coverage total 87.3% (btcpay package 89.1%), integration and
e2e regression runs green, the compose cross-service scenario
`E2E_Connector_BTCPayProfile` PASS (mark `20261017t061326z`), lint 0. Review closure
round: findings of the four reviewers closed (relative fixture times instead of
wall-clock bombs, the resilience wrap `gateway.Resilient` with optional SPI
forwarding, the BTCPay fetch mandate, the amount decimals/asset validation in the
transport, asset-scale classification and the capability index, the store check of
webhooks, env validation hardening, and the documentation alignment).

## 11. Work plan and commits

Increment 1 (this commit): spec, adapter, bridges, main wiring, tests, compose
and E2E.md additions, AGENTS/README. Increment 2 (follow-up commit): the BTCPay
regtest stand and its run.
