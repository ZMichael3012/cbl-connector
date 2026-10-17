# CBL-17.6. NOWPayments adapter

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.2 (SPI v1, manifests), 17.4 (ingress canonical JSON HMAC), 17.5 (conformance suite, mock profile), 17.7 (wire helper, provider allowlist) |
| Contract version | none (adapter implements the existing SPI; no `.proto` change) |
| Status | done (increment 1: adapter, conformance, wiring and the mock-profile e2e `E2E_Connector_NowPaymentsProfile` PASS 17.10.2026, mark `20261017t082057z`; the real sandbox run needs the 17.0 account and moves to 17.16) |

## 1. Context and goal

NOWPayments is the hosted-invoice provider with float-risk JSON numbers, a
canonical JSON HMAC-SHA512 IPN signature and the `partially_paid` final status
(plan section 6). This task delivers the SPI v1 adapter against the NOWPayments
API, using the CBL-17.5 conformance suite and the mock-gateway `nowpayments`
profile as the offline contract; payouts (JWT + TOTP batch flow) stay provisional
(17.11), refunds are manual by decision 8 (no `Refunder`), and the real sandbox
run moves to 17.16 with the 17.0 account.

## 2. Scope

In scope: `internal/providers/nowpayments` (invoice create/get, payment lookup
by order with the envelope shape, canonical JSON HMAC verification, statuses,
provisional payout shapes), main wiring from environment, the compose stand
scenario, the conformance run.

Out of scope: the JWT auth and TOTP batch payout flow (17.11); refunds (manual,
decision 8); the estimate/min-amount quote source (17.10); the real sandbox run
(17.0, 17.16); scheduling the payment listing for reconciliation (17.13).

## 3. Architecture sketch

### 3.1. Package, config and ports

```go
// internal/providers/nowpayments/adapter.go
type Config struct {
    BaseURL       string           // https://api.nowpayments.io (sandbox: account-sandbox.nowpayments.io), mock stand
    APIKey        string           // x-api-key header
    IPNSecret     []byte           // canonical JSON HMAC-SHA512 secret
    Capabilities  domain.Capabilities
    Timeout       time.Duration    // default 10s
    HTTPClient    *http.Client
}
func New(cfg Config) (*Adapter, error)

var (
    _ domain.PaymentGateway = (*Adapter)(nil)
    _ domain.LookupGateway  = (*Adapter)(nil)
)
```

The adapter implements no `Refunder`: the manifest declares no refund operation
and the manual path is decision 8.

### 3.2. Wire contract

| Operation | Request | Response |
| :-- | :-- | :-- |
| Create invoice | `POST /v1/invoice` with `{"order_id","price_amount","price_currency","pay_currency"}` | `{"id","order_id","price_amount","price_currency","pay_currency","invoice_url","created_at","expiration_estimate_date"}` |
| Get payment | `GET /v1/payment/{payment_id}` | `{"payment_id","order_id","payment_status","price_amount","actually_paid","pay_currency","updated_at"}` |
| Payment list for lookup | `GET /v1/payment/?order_id={order}` | a list envelope (`{"data":[payment...]}`), a single object (the mock profile) or an array |
| Payout (provisional 17.11) | `POST /v1/payout` with `{"amount","currency","address"}` | `{"id","status"}` |

Auth: `x-api-key: <APIKey>` on every call. Amounts: `price_amount` and
`actually_paid` are JSON numbers parsed with the kit `money` package
(no float); the emitted `price_amount` is a number literal (money.String via
RawMessage). The pay asset is resolved from `pay_currency` by the exact
configured network row (lowercase concatenated forms, for example `usdttrc20`);
an unmapped currency fails closed.

Status table:

| Provider status | Canonical |
| :-- | :-- |
| waiting | detected |
| confirming | detected |
| confirmed | detected |
| sending | detected |
| partially_paid | underpaid |
| finished | confirmed |
| failed | failed |
| refunded | failed |
| expired | expired |

Any other status word -> `ErrUnknownState`.

Webhook: canonical JSON HMAC-SHA512 hex, header `x-nowpayments-sig`; the adjoint
ingress strategy has the same parameters (17.4). The IPN body parses to the
canonical event: `payment_id` -> `gateway_id`, `order_id` -> `payment_id`,
`payment_status` -> raw status with the mapping, `updated_at` (RFC 3339) ->
`OccurredAt`, and `actually_paid` -> the received amount evidence.

Error classification: transport errors, 429 and 5xx -> `ErrTemporary`; an
unfollowed 3xx -> `ErrTemporary`; 404 on a read -> `ErrGatewayNotFound`, on a
write -> `ErrRejected`; other 4xx (incl. 401/403) -> `ErrRejected`; malformed
JSON -> a wrapped plain error.

### 3.3. Data flows

CreateInvoice: `order_id = request.PaymentID`, `price_amount =
request.Amount` (JSON number), `price_currency = asset`, `pay_currency =
lower(request.Network)` -> POST -> parse id, `invoice_url` (CheckoutURL) and
`expiration_estimate_date` clamped to the request horizon (the 17.9 E-8 class).
`Network` echoes the request.

GetPayment/Lookup: GET -> parse -> canonical status with `RawStatus`,
`ReceivedAmount` from `actually_paid` (pay asset resolved from `pay_currency`),
`GatewayEventTS` from `updated_at` (a malformed time leaves it zero; the
normalizer falls back to the receipt time, the 17.7 E-8 class). A missing pay
pair leaves the received amount empty and the fetch path fails closed.

VerifyWebhook: kit `CanonicalJSONHMAC` (SHA-512, hex) over the raw body -> parse
-> canonical event (fail closed).

CreatePayout/GetPayout: provisional shapes pinned by the 17.5 fixtures; the JWT
and TOTP batch flow is 17.11.

### 3.4. Storage and migrations

None.

### 3.5. Error taxonomy

As 3.2; constructor faults are `ErrConfiguration`.

### 3.6. Decisions and rejected alternatives

- D1. The lookup parser accepts the list envelope (`data`), a bare array and a
  single object: NOW expressions differ by endpoint; the mock answers objects.
- D2. The pay asset and network are resolved from the configured rows by the
  exact network key (`usdttrc20` -> the row asset USDT); the create request
  validates its network against the rows and requires the row asset to equal
  the amount asset, so an unmapped pay currency fails closed instead of a
  prefix coincidence (17.6 review). The catalog (17.21) replaces the
  configuration with the canonical table.
- D3. Payouts are provisional (fixture-pinned), the JWT/TOTP flow is 17.11;
  refunds stay manual (decision 8; no `Refunder`).
- Rejected: modelling the JWT session now (one consumer, 17.11); float parsing
  of NOW numbers (kit money only).

## 4. Functional requirements

FR-1. The adapter implements PaymentGateway and LookupGateway with the wire
contract and statuses of 3.2; constructor validation fails fast.

FR-2. `conformance.Run` passes for the nowpayments fixtures (CONF-1..CONF-10),
including CONF-6 (canonical JSON HMAC valid/tampered/missing), CONF-7 (payout
shape; no refund declared) and CONF-8 (lookup miss wraps ErrGatewayNotFound).

FR-3. Runtime wiring: `CONNECTOR_NOWPAYMENTS_*` builds the adapter into the
allowlist-gated gateways map (resilience-wrapped, capabilities source, fetcher);
the manifest rate limit (60/s bucket) is enforced by the `gateway.Limited`
wrapper with the kit limiter (the manifest declares `fetch_mandatory`, so the
normalizer resolves NOW deliveries through the adapter).

FR-4. The cross-service e2e `E2E_Connector_NowPaymentsProfile` passes: create
answers the mock invoice id and the checkout URL, the read answers
detected/`waiting`, the simulate advances, the read answers confirmed/`finished`.

FR-5. Documentation: AGENTS.md (architecture, env, journal, runbook), README,
E2E.md scenario.

## 5. Contracts

No `.proto`/Kafka changes; the manifest exists (canonical JSON HMAC, the fetch
mandate, no refund operation). The compose stand connector env gains
NOWPayments variables.

## 6. Data model and migrations

None.

## 7. State machines and rules

Pure mapping functions; no local state.

## 8. Edge cases

E-1. `expiration_estimate_date` absent or later than the request horizon: the
request horizon is answered (clamp, mirror of 17.9 E-8).
E-2. Lookup answers the envelope, an array (first element) or a single object;
an empty answer -> `ErrGatewayNotFound`.
E-3. Unknown status in get/lookup/callback -> `ErrUnknownState` (fail closed).
E-4. A webhook with an invalid or missing `x-nowpayments-sig` -> error wrapping
`ErrUnsignedWebhook`.
E-5. `updated_at` malformed -> zero event time; the normalizer falls back to the
receipt time.
E-6. `actually_paid` with a pay currency outside the configured network rows
-> error wrapping `ErrRejected` (fail closed); a missing pay pair leaves the
received amount empty. A create request network outside the rows, or a row
whose asset differs from the amount asset, is rejected before any provider
call.
E-7. Payout creation answers unknown state words: reported as submitted (mirror
of the 17.9 E-5 class).
E-8. Amounts whose `decimals` differ from the configured asset scale are
rejected with `ErrRejected` before any provider call (the shared guard of 17.7).

## 9. Non-functional requirements

- NFR-1. Repository unit+integration coverage stays at least 80%.
- NFR-2. No new dependencies.
- NFR-3. Timeouts via `Config.Timeout` (default 10s); 1 MiB body bound;
  redirects not followed.
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix (no Docker)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| N-01 | constructor validation | missing base url / key / ipn secret / networks; userinfo; timeout clamps | errors; valid config passes; Provider forced |
| N-02 | create happy path | wire asserts POST /v1/invoice, `x-api-key`, body (order_id echo, price_amount number, pay_currency lower network) | id, invoice_url, clamped expiry, network echo |
| N-03 | error classification | 503/429/302/400/401/404-write | temporary/rejected classes |
| N-04 | status mapping | nine statuses; unknown word | canonical + raw; unknown -> ErrUnknownState |
| N-05 | payment 404 | wire 404 | ErrGatewayNotFound |
| N-06 | webhook | valid signature; tampered value; missing header; trailing json; duplicate keys; unknown status | event with state/raw/ids/time; rejections; malformed documents wrap ErrUnsignedWebhook; unknown -> ErrUnknownState |
| N-07 | lookup | envelope; array; object; empty envelope; 404 | first element; ErrGatewayNotFound variants |
| N-08 | received evidence | actually_paid number parsed with the pay asset; malformed updated_at -> zero time; unresolvable asset -> error | assertions |
| N-09 | canonical event | built from the fetched status, validates | no error |
| N-10 | payout provisional | wire asserts POST /v1/payout body | id parsed, submitted; missing address -> ErrRejected without calls |
| N-11 | scale guard | decimals mismatch in create/payout | ErrRejected before any call |
| N-12 | network validation | unconfigured network; row asset differs from the amount asset | ErrRejected before any call |

### 10.2. Integration matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| NI-01 | conformance run | `conformance.Run` for nowpayments fixtures | CONF-1..CONF-10 pass (no refunds declared) |
| NI-02 | wiring regression | allowlist `nowpayments` with the other adapter URLs defaulted (BTCPay on the stand) | only nowpayments wired |

### 10.3. Smoke set

`make build`, `make test`, `make test-integration`, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Connector_NowPaymentsProfile | compose stand with the mock-gateway `nowpayments` profile selected: create through `ConnectorService` answers the mock invoice id and the checkout URL; the read answers detected/`waiting`; the mock simulate advances; the read answers confirmed/`finished` | `test/e2e/E2E.md` |
| E2E_Connector_NowPaymentsSandbox (17.16) | real `account-sandbox.nowpayments.io` run with the 17.0 account (statuses, IPN signature, min/estimate) | scheduled with 17.16 |

### 10.5. Load plan

Not applicable (existing service; the manifest carries the 60 rpm numbers).

### 10.6. Test-first evidence

Red run recorded before implementation (17.10.2026):
`go test ./internal/providers/...` failed with
`github.com/cbl/cbl-connector/internal/providers/nowpayments: no non-test Go files`;
the existing suites stayed green. Implementation note: the canonical JSON
signature is whitespace-insensitive, so the tamper probe changes a value, not
the spacing (a first-draft test asserted a trailing-space rejection and was
corrected). After implementation: conformance `Run` green for the nowpayments
fixtures, unit coverage total 86.7% (nowpayments package 86.0%), the
cross-service scenario `E2E_Connector_NowPaymentsProfile` PASS
(mark `20261017t082057z`), integration and e2e regression runs green, lint 0;
review closure is recorded in the journal (final figures there).

## 11. Work plan and commits

Increment 1 (this commit): spec, adapter, wiring, tests, compose env with the
NowPayments scenario, E2E.md, AGENTS/README. The real sandbox run is a 17.16
follow-up (needs 17.0).
