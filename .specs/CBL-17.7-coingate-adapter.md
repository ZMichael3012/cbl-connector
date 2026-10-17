# CBL-17.7. CoinGate adapter

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.2 (SPI v1, manifests), 17.4 (ingress token echo), 17.5 (conformance suite, mock profile), 17.9 (wiring pattern, wire helpers) |
| Contract version | none (adapter implements the existing SPI; no `.proto` change) |
| Status | done (increment 1: adapter, conformance, wiring and the mock-profile e2e `E2E_Connector_CoinGateProfile` PASS 17.10.2026, mark `20261017t074107z`; the real sandbox run needs the 17.0 account and moves to 17.16) |

## 1. Context and goal

CoinGate is the first production provider (plan section 5): orders create/get/list,
token-echo callbacks with the fetch-to-verify mandate, refunds API and the
documented rate limits. This task delivers the SPI v1 adapter against the
CoinGate v2 API, using the CBL-17.5 conformance suite and the mock-gateway
`coingate` profile as the offline contract; the real sandbox run (public callback
address required, 17.0) is scheduled with 17.16.

## 2. Scope

In scope: `internal/providers/coingate` (order create/get, order lookup, token
echo verification per the ingress strategy, statuses, payouts and refunds as
provisional shapes owned by 17.11/17.12), the shared provider wire helper
extracted now that a second adapter confirms the pattern (decision 21), main
wiring from environment, the compose stand scenario, the conformance run.

Out of scope: the real sandbox run (17.0 account, 17.16 regression); the payout
two-phase confirmation (CoinGate "Send Request" with 2FA) and the refund
callbacks (17.11/17.12); the CoinGate IP allowlist (17.4, ingress); the rate
limiter wiring beyond the shared stack (the manifest declares 180/min; the
limited decorator is wired when a limiter instance is provided - the same
pattern as 17.9 T-CONNECTOR-16).

## 3. Architecture sketch

### 3.1. Package, config and ports

```go
// internal/providers/coingate/adapter.go
type Config struct {
    BaseURL       string           // https://api.coingate.com (prod), api-sandbox.coingate.com (sandbox), mock stand
    APIToken      string           // Authorization: Bearer <token>
    IPNToken      []byte           // token echo secret of the callback body
    Capabilities  domain.Capabilities
    Timeout       time.Duration    // default 10s
    HTTPClient    *http.Client
}
func New(cfg Config) (*Adapter, error)

var (
    _ domain.PaymentGateway = (*Adapter)(nil)
    _ domain.LookupGateway  = (*Adapter)(nil)
    _ domain.Refunder       = (*Adapter)(nil)
)
```

Shared helper (new, provider-agnostic):

```go
// internal/providers/wire/wire.go
// DecodeArrayOrObject decodes a provider body that may be a JSON array (first
// element; an empty array reports notFound) or a single object (the mock stand
// answers). The BTCPay lookup refactors onto it (17.9 D4) and CoinGate uses it.
func DecodeArrayOrObject[T any](payload []byte, notFound error) (T, error)
```

### 3.2. Wire contract (CoinGate v2)

| Operation | Request | Response |
| :-- | :-- | :-- |
| Create order | `POST /v2/orders` with `{"order_id","price_amount","price_currency","pay_currency","callback_url"?,"cancel_url"?}` | `{"id","status","order_id","price_amount","price_currency","pay_amount","pay_currency","payment_address","created_at"}` |
| Get order | `GET /v2/orders/{id}` | the order body |
| Order list for lookup | `GET /v2/orders?order_id={order}` | an array of order bodies (the mock profile answers a single object) |
| Send request (payout, provisional 17.11) | `POST /v2/send_request` with `{"request_id","amount","currency","address"}` | `{"id","state"}` |
| Get send request (provisional 17.11) | `GET /v2/send_requests/{id}` | `{"id","state"}` |
| Refund (provisional 17.12) | `POST /v2/refunds` with `{"order_id","amount","currency","refund_address"?}` | `{"id","status"}` |

Auth: `Authorization: Bearer <APIToken>` on every call. The order id is an
integer; `gateway_id` is its decimal string. Amounts are decimal strings parsed
with the kit `money` package at the configured asset decimals; responses re-emit
via `money.String()`. Times: ISO 8601 (`created_at`) -> RFC 3339 UTC.

Status table (verified against the 17.5 profile contract):

| Provider status | Canonical |
| :-- | :-- |
| new | detected |
| pending | detected |
| confirming | detected |
| paid | confirmed |
| invalid | failed |
| expired | expired |
| canceled | failed |
| refunded | failed |

Any other status word -> `ErrUnknownState` (no fabrication).

Webhook (token echo): the callback body carries `token`; verification uses the
kit `webhooksig.TokenEcho` strategy (constant-time, strict JSON, duplicate keys
and trailing data rejected); the parsed event takes `id` as `gateway_id`,
`order_id` as `payment_id`, the raw `status` with the canonical mapping, and the
receipt time (the callback carries no event time in the verified shape).

Error classification: transport errors, 429 and 5xx -> `ErrTemporary`; 3xx
(unfollowed) -> `ErrTemporary`; 404 on a read -> `ErrGatewayNotFound`, on a
write -> `ErrRejected`; other 4xx (incl. 401/403) -> `ErrRejected`; malformed
JSON -> wrapped plain error.

### 3.3. Data flows

CreateInvoice: `order_id = request.PaymentID`, `price_amount =
request.Amount.String()`, `price_currency = request.Amount.Asset`,
`pay_currency = upper(request.Network)` (the stand and the sandbox use the
pair USDT/USDT_TRC20; the catalog 17.21 refines the mapping) -> POST -> parse
the numeric id, `payment_address`, `created_at + expires?` (CoinGate orders do
not carry an expiry; the request horizon is echoed, mirror of E-1 of 17.9).

GetPayment/Lookup: GET (by id or by order) -> array-or-object decode -> status
mapping with `RawStatus` and `GatewayEventTS` (created_at).

VerifyWebhook: kit TokenEcho over the body with `IPNToken` -> parse -> canonical
event (fail closed on any error).

CreatePayout/CreateRefund: provisional shapes of 17.11/17.12 (send request with
`request_id` = platform reference; refund with the order id); the fixtures pin
the stand behavior and the sandbox run re-validates the shapes (17.16).

### 3.4. Storage and migrations

None.

### 3.5. Error taxonomy

As 3.2; constructor faults are plain errors (`ErrConfiguration`).

### 3.6. Decisions and rejected alternatives

- D1. The tolerant array-or-object decoder moves to `internal/providers/wire`
  now that a second adapter confirms the pattern (decision 21: the onboarding
  pipeline extracts shared provider-agnostic helpers); BTCPay refactors onto it.
- D2. `pay_currency` is the uppercased request network (the numbers/network
  mapping belongs to the catalog 17.21); the CoinGate currency tables (ids,
  platforms) are not modelled in the stand.
- D3. Payouts and refunds are provisional wire shapes pinned by the 17.5
  fixtures and owned by 17.11/17.12 (the real Send Request 2FA flow and the
  refund callbacks are not automated here).
- D4. The callback expiry: the verified CoinGate callback carries no event time;
  the receipt time is used (mirror of the ingress rule and 17.9).
- Rejected: a per-provider service (plan 14.3); modelling CoinGate platform
  currency ids now (catalog 17.21).

## 4. Functional requirements

FR-1. The adapter implements PaymentGateway, LookupGateway and Refunder with the
wire contract and statuses of section 3.2; constructor validation fails fast.

FR-2. `conformance.Run` passes for the coingate fixtures (CONF-1..CONF-10),
including CONF-6 (token echo valid/tampered/missing), CONF-7 (payout shape and
Refunder presence) and CONF-8 (lookup miss wraps ErrGatewayNotFound).

FR-3. Runtime wiring: `CONNECTOR_COINGATE_*` builds the adapter into the
gateways map (resilience-wrapped, capabilities source, fetcher); the compose
stand defaults stay as in 17.9 and the CoinGate scenario overrides the provider
envs.

FR-4. The cross-service e2e `E2E_Connector_CoinGateProfile` passes against the
mock profile: create through `ConnectorService` answers a numeric id and a
payment address, the read answers detected/`new`, the mock simulate advances the
order, the read answers confirmed/`paid`.

FR-5. Documentation: AGENTS.md (architecture, env, journal, runbook), README,
E2E.md scenario.

## 5. Contracts

No `.proto`/Kafka changes; the manifest already exists (token echo, fetch
mandatory, 180/min). The compose stand connector env gains CoinGate variables
with defaults that preserve the BTCPay scenario (`${VAR-default}` form).

## 6. Data model and migrations

None.

## 7. State machines and rules

Pure mapping functions (section 3.2); no local state.

## 8. Edge cases

E-1. CoinGate orders carry no expiry: the adapter answers the request horizon
(clamped like 17.9).
E-2. Lookup array empty -> `ErrGatewayNotFound`; a single-object answer (mock)
parses.
E-3. Unknown status in get/lookup/callback -> `ErrUnknownState`; the normalizer
routes fetch-mandated deliveries through `GetPayment` and fails closed.
E-4. A callback without the token or with a wrong token -> `ErrUnsignedWebhook`
(the ingress already answers 200 and records the state; the connector drops
non-verified raws before this adapter is consulted).
E-5. A 401/403 from the provider is a configuration or token fault: classified
`ErrRejected` (the breaker does not trip; the operator fixes the token).
E-6. The payout/refund provisional endpoints answer unknown state words: the
creation reports `PayoutSubmitted` whatever the word (mirror of 17.9 E-5).
E-7. `pay_currency` outside the stand/prod tables is accepted as sent (the
provider rejects it; classification applies).
E-8. A malformed `created_at` leaves `GatewayEventTS` zero; the normalizer falls
back to the receipt time (mirror of the 17.9 E-8 class of concerns).
E-9. The received amount evidence of the fetch path is the order `pay_amount` /
`pay_currency` (the pay asset is the first segment of the pay currency code,
for example USDT_TRC20 -> USDT); the reconciliation task 17.13 refines the
amount attribution for underpayments. A missing pay pair leaves the received
amount empty and the normalizer rejects the delivery (fail closed).
E-10. An amount whose `decimals` differ from the configured asset scale is
rejected with `ErrRejected` before any provider call: rendering it would
mis-price the order by a power of ten (the same guard exists in the BTCPay
adapter).

## 9. Non-functional requirements

- NFR-1. Repository unit+integration coverage stays at least 80%; the adapter
  is covered by the conformance run and the unit matrix.
- NFR-2. No new dependencies.
- NFR-3. Timeouts via `Config.Timeout` (default 10s); response bodies bounded
  (1 MiB); redirects not followed.
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix (no Docker)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| C-01 | constructor validation | missing base url / token / ipn token / networks; userinfo; timeout <= 0 clamps | errors; valid config passes; Provider forced to coingate |
| C-02 | create happy path | wire asserts POST /v2/orders, `Authorization: Bearer`, body (order_id echo, price_amount string, price_currency, pay_currency upper network) | numeric id, address, expiry echo parsed |
| C-03 | error classification | 503 / 400 / 401 / dial error / 302 | ErrTemporary / ErrRejected / ErrRejected / ErrTemporary / ErrTemporary |
| C-04 | status mapping | the eight statuses; unknown word | canonical + raw preserved; unknown -> ErrUnknownState |
| C-05 | order 404 | wire answers 404 | ErrGatewayNotFound |
| C-06 | webhook token echo | valid token; tampered token; missing token; unknown status; trailing json | event with mapped state/ids/receipt time; rejections wrap ErrUnsignedWebhook; unknown -> ErrUnknownState |
| C-07 | lookup | object answer; array answer; empty array; 404 | status; first element; ErrGatewayNotFound; ErrGatewayNotFound |
| C-08 | payout provisional | wire asserts POST /v2/send_request body (request_id, amount, address) | id parsed, state submitted; missing address -> ErrRejected without calls |
| C-09 | refund provisional | wire asserts POST /v2/refunds body (order_id, amount) | id parsed; missing reference -> ErrRejected |
| C-10 | wire helper | array/object/empty/invalid payloads; a long list decodes only the first element (streaming) | first element/object parsed/notFound/wrapped error |
| C-11 | wire helper refactor | BTCPay lookup tests stay green on the shared decoder | no behavior change (regression) |
| C-12 | fetch-path evidence | a status built from `GetPayment` feeds `domain.CanonicalEvent` and validates (received amount + network); a malformed `created_at` leaves a zero time; a decimals mismatch is rejected before any call (both adapters) | no validation error; zero time; `ErrRejected` |

### 10.2. Integration matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| I-01 | conformance run | `conformance.Run` for coingate fixtures | CONF-1..CONF-10 pass |
| I-02 | wiring regression | selector/fetcher with the coingate gateway | deterministic selection; fetch passthrough |

### 10.3. Smoke set

`make build`, `make test`, `make test-integration`, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Connector_CoinGateProfile | compose stand with the mock-gateway `coingate` profile selected (provider envs override): create through `ConnectorService` answers the numeric order id and the payment address; `GetGatewayPayment` answers detected/`new`; the mock simulate advances the order; the read answers confirmed/`paid` | `test/e2e/E2E.md` |
| E2E_Connector_CoinGateSandbox (17.16) | real `api-sandbox.coingate.com` run with the 17.0 account and a public callback address; contract calls, statuses, refunds API | scheduled with 17.16 (requires 17.0) |

### 10.5. Load plan

Not applicable (existing service; the connector load profile is unchanged).
CoinGate 180/min is enforced by the manifest limiter when a limiter instance is
wired (same deferral as 17.9; the manifest carries the numbers).

### 10.6. Test-first evidence

Red run recorded before implementation (17.10.2026):
`go test ./internal/providers/...` failed with
`github.com/cbl/cbl-connector/internal/providers/coingate: no non-test Go files`
and the same for `internal/providers/wire`; the existing suites stayed green.
After implementation: conformance `Run` green for the coingate fixtures, unit
coverage total 87.0% (coingate package 83.3%), the cross-service scenario
`E2E_Connector_CoinGateProfile` PASS (mark `20261017t074107z`), integration and
e2e regression runs green, lint 0. Review closure round: findings of the four
reviewers on the increment diff closed before the commit (recorded in the task
journal).

## 11. Work plan and commits

Increment 1 (this commit): spec, wire helper plus the BTCPay refactor, CoinGate
adapter, wiring, tests, compose env defaults with the CoinGate scenario, E2E.md,
AGENTS/README. The real sandbox run is a 17.16 follow-up (needs 17.0).
