# CBL-17.8. BitPay adapter

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.2 (SPI v1, manifests), 17.5 (conformance suite, mock profile), 17.7 (wire helpers, allowlist wiring), 17.9 (wiring pattern) |
| Contract version | none (adapter implements the existing SPI; no `.proto` change) |
| Status | done (increment 1: adapter with ECDSA signing on the official wire contract, conformance, wiring and the mock-profile e2e `E2E_Connector_BitPayProfile` PASS 17.10.2026, mark `20261017t161450z`; the real sandbox run needs the 17.0 account and moves to 17.16) |

## 1. Context and goal

BitPay is the fourth evaluated provider (plan section 5). Its wire contract differs
from the other three in one significant way: every API call is authenticated with an
ECDSA secp256k1 request signature (`X-Identity` + `X-Signature` over the request URL
and body) instead of a bearer token, so the adapter owns the only private-key-bearing
signing path of the platform until the egress gateway (egress vault task). Inbound
callbacks use a raw HMAC-SHA256 signature in base64 (`x-signature`), already declared
by the manifest (`verify_encoding: base64`). This task delivers the SPI v1 adapter
against the BitPay API v2, using the CBL-17.5 conformance suite and the mock-gateway
`bitpay` profile as the offline contract; the real sandbox run (portal account, KYC,
17.0) is scheduled with 17.16.

## 2. Scope

In scope: `internal/providers/bitpay` (invoice create/get, order lookup, raw-HMAC
webhook verification, the ECDSA secp256k1 request signing with PEM key loading,
statuses, provisional payout/refund shapes owned by 17.11/17.12), main wiring from
environment (including the PEM private key secret), the compose stand scenario, the
conformance run.

Out of scope: the real sandbox run (17.0 account, 17.16 regression); the BitPay
pairing flow (the token is an operator-provisioned secret); the payout batch
semantics and the refund callbacks (17.11/17.12); moving the key material into a
signing service (egress vault task, kit item 11 keeps provider token/TOTP/ECDSA
handling in the owning service until the egress seam arrives).

## 3. Architecture sketch

### 3.1. Package, config and ports

```go
// internal/providers/bitpay/adapter.go
type Config struct {
    BaseURL       string        // https://bitpay.com (prod), test.bitpay.com (sandbox), mock stand
    APIToken      string        // optional: Authorization: Bearer <token> (pairing token)
    PrivateKeyPEM string        // required: secp256k1 EC private key (SEC1 or PKCS8 PEM)
    WebhookSecret []byte        // x-signature verification secret
    WebhookURL    string        // optional notificationURL sent at invoice creation
    Capabilities  domain.Capabilities
    Timeout       time.Duration // default 10s
    HTTPClient    *http.Client
}
func New(cfg Config) (*Adapter, error)

var (
    _ domain.PaymentGateway = (*Adapter)(nil)
    _ domain.LookupGateway  = (*Adapter)(nil)
    _ domain.Refunder       = (*Adapter)(nil)
)
```

Request signing (D1): every call sends `X-Identity: <compressed public key hex>`
and `X-Signature: hex(DER(ECDSA_secp256k1(SHA256(url + body))))`, exactly the
scheme of the official SDK key utilities; `X-Accept-Version: 2.0.0`; the pairing
token additionally travels as `Authorization: Bearer` and is always present in
the invoice create body (the real API requires it). The PEM parser accepts SEC1
(`EC PRIVATE KEY`) and PKCS8 (`PRIVATE KEY`, curve parameters required) blocks
without standard `crypto/ecdsa` support for the curve; the new dependency is
`github.com/decred/dcrd/dcrec/secp256k1/v4` (the maintained pure-Go secp256k1
library).

### 3.2. Wire contract (BitPay API v2)

| Operation | Request | Response |
| :-- | :-- | :-- |
| Create invoice | `POST /invoices` with `{"price":<decimal major number>,"currency","token","orderId","posData"?,"notificationURL"?}` | the facade envelope `{"facade":"merchant/invoice","data":{...}}` |
| Get invoice | `GET /invoices/{id}` | the same envelope |
| Invoice list for lookup | `GET /invoices?orderId={order}` | an array of invoice bodies (the mock profile answers a single object; the flat form stays accepted) |
| Payout (provisional 17.11) | `POST /payouts` with `{"amount":<int minor>,"currency","address","reference"}` | `{"id","status"}` |
| Get payout (provisional 17.11) | `GET /payouts/{id}` | `{"id","status"}` |
| Refund (provisional 17.12) | `POST /refunds` with `{"invoiceId","amount":<int minor>,"currency","reference","address"?}` | `{"id","status"}` |

The invoice body carries `id`, `url`, `status`, `price` and `amountPaid` as
decimal major-unit numbers of the pricing currency, `currency`, `orderId`,
`confirmations`, `invoiceTime`/`expirationTime` in Unix milliseconds and
`bitcoinAddress`. The adapter asserts the capability scale of the asset, the
requested network row and its asset, and that the returned currency/price echo
the request (a mismatch fails closed with `ErrRejected`); `posData` carries the
requested network for traceability, the response has no network field, so
`Invoice.Network` echoes the request and `PaymentStatus.Network` stays empty
(mirror of the 17.9 expiry echo). The manifest rate limit follows the official
merchant limit of 100 invoices per hour (burst 10, 36 s refill, D5).

Status table (verified against the 17.5 profile contract):

| Provider status | Canonical |
| :-- | :-- |
| new | detected |
| paid | detected |
| confirmed | detected |
| complete | confirmed |
| expired | expired |
| invalid | failed |

Any other status word -> `ErrUnknownState` (no fabrication).

Webhook: the raw body is verified with the kit `webhooksig.RawHMAC` strategy
(`x-signature`, SHA256, base64, no prefix) per the manifest fields; the verified body
is `{"event":{"code","name"},"data":{invoice fields}}`; the parsed event takes
`data.id` as `gateway_id`, `data.orderId` as `payment_id`, `data.status` with the
canonical mapping, `data.amountPaid` as the received amount evidence, `data.invoiceTime`
ms as the event time, and the receipt time when the stamp is absent (mirror of the
17.9 receipt-time rule).

Error classification: transport errors, 429 and 5xx -> `ErrTemporary`; 3xx
(unfollowed) -> `ErrTemporary`; 404 on a read -> `ErrGatewayNotFound`, on a write ->
`ErrRejected`; other 4xx (incl. 401/403) -> `ErrRejected`; malformed JSON -> wrapped
plain error.

### 3.3. Data flows

CreateInvoice: scale guard and network-row guard -> `price = request.Amount.String()`
as a raw JSON number literal (major units), `currency = asset`, `token`, `orderId =
request.PaymentID`, `posData = request.Network` -> POST -> unwrap the facade
envelope, require the returned currency and price to equal the request, parse id/url/
address, `ExpiresAt = min(expirationTime, request horizon)` (a missing stamp echoes
the request horizon, E-2).

GetPayment/Lookup: GET (by id or by order) -> envelope unwrap (array-or-object for
the list; an empty array is `ErrGatewayNotFound`) -> status mapping with
`RawStatus`, the received amount from `amountPaid` at the asset scale,
`GatewayEventTS` from `invoiceTime` (zero on an absent or future-skewed stamp, E-5).

VerifyWebhook: kit raw HMAC over the raw body (fail closed) -> parse -> canonical
event with the decimal `amountPaid` received evidence; the event type must
validate (17.2 rules).

CreatePayout/CreateRefund: provisional shapes of 17.11/17.12 pinned by the fixtures;
the address is required for payouts and amounts must be positive; the sandbox run
re-validates the shapes (17.16).

### 3.4. Storage and migrations

None.

### 3.5. Error taxonomy

As 3.2; constructor faults (URL, missing token-free config, PEM parsing, capability
rows) are plain errors (`ErrConfiguration`).

### 3.6. Decisions and rejected alternatives

- D1. Real ECDSA secp256k1 signing lands now with the decred library and follows
  the official SDK key utilities exactly (hex DER signature over `url + body`,
  compressed public key hex as `X-Identity`): the mock only checks header presence
  (T-MOCK-06), but the adapter without signing could never authenticate against
  the real BitPay, and the unit suite verifies the signature offline (sign ->
  parse DER -> verify against the derived public key). The initial 17.5
  approximation (flat body, integer minor units, base64 signature, uncompressed
  identity) was disproved by the 17.8 security review against the official
  documentation and SDK sources and realigned here, in the mock
  (`cbl-mock-gateway@808c38b`) and in the conformance fixtures.
- D2. `currency` is the upper request asset (the platform prices in the crypto
  asset), the price is a decimal major-unit number of that currency and `posData`
  carries the network; the fiat-pricing mode of BitPay (price in USD, pay in
  crypto) is not modelled until the catalog task 17.21 decides the pricing
  convention.
- D3. The webhook strategy is data: `verify_encoding: base64` was declared by the
  17.5/17.2 manifest review, so the adapter builds the same kit strategy the
  conformance suite builds (`StrategyFromManifest`).
- D4. Payouts and refunds are provisional wire shapes pinned by the 17.5 fixtures
  and owned by 17.11/17.12 (the real payout facade and refund callbacks are not
  automated here; their amount encoding is realigned there).
- D5. The manifest rate limit follows the official merchant limit (100 invoices
  per hour): burst 10 with a 36 s refill interval; the limiter wrapper of 17.6
  enforces it for every operation.
- Rejected: storing the private key in a sidecar now (egress vault task); mocking
  the signature shape only (D1); keeping the 17.5 wire approximation after the
  security review disproved it (money mispricing risk on a real base URL).

## 4. Functional requirements

FR-1. The adapter implements PaymentGateway, LookupGateway and Refunder with the
wire contract and statuses of section 3.2; constructor validation fails fast (invalid
base URL, userinfo, missing/invalid PEM, missing webhook secret, empty or
conflicting capabilities).

FR-2. Request signing: every call carries `X-Identity` (the compressed public key
hex derived from the configured key) and a hex DER `X-Signature` verifiable over
`url + body`; a unit test verifies the signature offline with the library.

FR-3. `conformance.Run` passes for the bitpay fixtures (CONF-1..CONF-10), including
CONF-6 (base64 HMAC valid/tampered/missing), CONF-7 (payout shape, Refunder
presence) and CONF-8 (lookup miss wraps ErrGatewayNotFound).

FR-4. Runtime wiring: `CONNECTOR_BITPAY_*` builds the adapter into the gateways map
through the `CONNECTOR_PROVIDERS` allowlist (resilience-wrapped, limiter-wrapped by
the manifest 10 burst/36 s bucket, capabilities source, fetcher); the compose stand defaults
stay as in 17.9 and the BitPay scenario overrides the provider envs.

FR-5. The cross-service e2e `E2E_Connector_BitPayProfile` passes against the mock
profile: create through `ConnectorService` answers an invoice id and a checkout url,
the read answers detected/`new`, the mock simulate advances the invoice, the read
answers confirmed/`complete`.

FR-6. Documentation: AGENTS.md (architecture, env, journal, runbook, trade-offs),
README, E2E.md scenario.

## 5. Contracts

No `.proto`/Kafka changes; the manifest already exists (raw HMAC base64, the fetch
mandate, 10 burst/36 s). The compose stand connector env gains BitPay variables with
defaults that preserve the other scenarios (`${VAR-}` form).

## 6. Data model and migrations

None.

## 7. State machines and rules

Pure mapping functions (section 3.2); no local state. The payout provisional states:
new/processing -> submitted, complete -> confirmed, invalid/cancelled/failed ->
failed; the creation reports submitted whatever the provider word (mirror of 17.7
E-6).

## 8. Edge cases

E-1. The PEM key is a secret: it never appears in logs or errors; a parse failure
names the block type only.
E-2. A response without `expirationTime` echoes the request horizon; a stamp beyond
the horizon is clamped (mirror of 17.9 E-1).
E-3. Lookup array empty -> `ErrGatewayNotFound`; a single-object answer (mock)
parses.
E-4. Unknown status in get/lookup/callback -> `ErrUnknownState`; the normalizer
routes fetch-mandated deliveries through `GetPayment` and fails closed.
E-5. A malformed `invoiceTime` leaves `GatewayEventTS` zero and the normalizer falls
back to the receipt time (mirror of the 17.9 E-8 class).
E-6. A callback without the signature or with a wrong one -> `ErrUnsignedWebhook`
(fail closed; the ingress already answers 200 and records the state). A captured
valid callback can be replayed, but the manifest fetch mandate makes the
normalizer re-read the authoritative invoice, so a replay cannot advance a state
on its own; the adapter implements no separate freshness window.
E-7. A 401/403 from the provider is a configuration or key fault: classified
`ErrRejected` (the breaker does not trip; the operator fixes the key/token).
E-8. An amount whose `decimals` differ from the configured asset scale is rejected
with `ErrRejected` before any provider call (the guard of 17.6/17.7).
E-9. A payout without a destination address is rejected with `ErrRejected` before
any provider call, and a non-positive amount is rejected the same way on every
operation (create, payout, refund).
E-10. `X-Signature` must not leak the private key on any path: the DER signature is
deterministic input only; the key stays in the config struct.
E-11. An empty provider id or order reference is rejected with `ErrRejected` before
the URL is built: an empty `orderId` filter must never return the merchant's
invoice list as if it belonged to the caller (cross-payment attribution).
E-12. A provider response whose currency or price does not echo the request fails
closed with `ErrRejected`; a negative `amountPaid` fails closed as well.
E-13. The facade envelope is unwrapped when present and the flat form stays
accepted (the stand); a lookup answer may be an array (first element) or an
object.
E-14. A provider `invoiceTime` beyond the future skew bound (24 h) is ignored and
the event time stays zero for the receipt-time fallback: a hostile stamp must not
pin the payment ordering.
E-15. A caller-provided `HTTPClient` is hardened: redirects stay blocked and the
configured timeout is enforced; the base URL must not carry a query or fragment
(the signature covers the exact transmitted URL).

## 9. Non-functional requirements

- NFR-1. Repository unit+integration coverage stays at least 80%; the adapter is
  covered by the conformance run and the unit matrix.
- NFR-2. The only new dependency is `github.com/decred/dcrd/dcrec/secp256k1/v4`
  (D1), recorded in the AGENTS trade-offs (the default set deviation).
- NFR-3. Timeouts via `Config.Timeout` (default 10s); response bodies bounded
  (1 MiB); redirects not followed.
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix (no Docker)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| B-01 | constructor validation | missing base url / token / key / webhook secret / networks; userinfo; base url query or fragment; bad scheme; garbage PEM; PKCS8 without curve parameters; timeout <= 0 clamps | errors; valid config passes; Provider forced to bitpay |
| B-02 | PEM key forms | SEC1 block; PKCS8 block; wrong curve OID; PKCS8 without parameters; short key | identity derived for both accepted forms (offline signature verify per form); errors otherwise |
| B-03 | request signing | wire records `X-Identity`/`X-Signature`/`X-Accept-Version`; the test decodes the hex DER signature and verifies it over `url + body` with the compressed public key | signature verifies; identity is the compressed public key hex |
| B-04 | create happy path | wire asserts POST /invoices body (decimal major price literal, currency, token, orderId, posData network); expiry clamp; missing expiration echo; returned amount mismatch and unknown network fail closed without calls | id, checkout url, address, amount, clamped expiry |
| B-05 | error classification | 503 / 400 / 401 / dial error / 302 | ErrTemporary / ErrRejected / ErrRejected / ErrTemporary / ErrTemporary |
| B-06 | status mapping | the six statuses; unknown word; negative amountPaid; skewed invoiceTime | canonical + raw preserved; unknown -> ErrUnknownState; negative -> ErrRejected; zero time |
| B-07 | invoice 404 | wire answers 404 on get | ErrGatewayNotFound |
| B-08 | webhook raw HMAC | valid base64 signature; tampered; missing; unknown status; malformed body; zero invoiceTime; future-skewed invoiceTime | event with mapped state/ids/received amount; rejections wrap ErrUnsignedWebhook; unknown -> ErrUnknownState; zero time preserved |
| B-09 | lookup | object answer (flat form); array answer (envelope element); empty array; 404 | status; first element; ErrGatewayNotFound; ErrGatewayNotFound |
| B-10 | payout provisional | wire asserts POST /payouts body; missing address; scale guard; non-positive amount | id parsed, submitted; ErrRejected without calls |
| B-11 | refund provisional | wire asserts POST /refunds body (invoiceId, integer amount) | id parsed; missing gateway id -> ErrRejected |
| B-12 | fetch-path evidence | a status from `GetPayment` validates as `domain.CanonicalEvent` (received amount + the provider event ts) | no validation error |
| B-13 | identifier hygiene | empty gateway id / order reference / payout id; path and query escaping of hostile ids | ErrRejected before any call; the escaped path/query carries no separators |

### 10.2. Integration matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| I-01 | conformance run | `conformance.Run` for bitpay fixtures | CONF-1..CONF-10 pass |
| I-02 | wiring regression | selector/fetcher with the bitpay gateway | deterministic selection; fetch passthrough |

### 10.3. Smoke set

`make build`, `make test`, `make test-integration`, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Connector_BitPayProfile | compose stand with the mock-gateway `bitpay` profile selected (provider envs override, a dev PEM key): create through `ConnectorService` answers the invoice id and a checkout url; `GetGatewayPayment` answers detected/`new`; the mock simulate advances the invoice; the read answers confirmed/`complete` | `test/e2e/E2E.md` |
| E2E_Connector_BitPaySandbox (17.16) | real `test.bitpay.com` run with the 17.0 account, pairing token and a public callback address; contract calls, statuses, refunds API | scheduled with 17.16 (requires 17.0) |

### 10.5. Load plan

Not applicable (existing service; the connector load profile is unchanged). The
manifest 10 burst/36 s bucket is enforced by the limiter wrapper of 17.6.

### 10.6. Test-first evidence

Red run recorded before implementation (17.10.2026):
`go test ./internal/providers/bitpay/...` failed with
`github.com/cbl/cbl-connector/internal/providers/bitpay: no non-test Go files`;
the existing suites stayed green. After the implementation the four-reviewer
round found a blocking wire-contract defect (the initial 17.5 approximation:
flat bodies, integer minor units, base64 signature, uncompressed identity) that
the official BitPay documentation and SDK key utilities disproved; the contract
was realigned (hex DER signature over url plus body, compressed-key identity,
decimal major-unit `price`/`amountPaid`, the `merchant/invoice` facade envelope,
the required `token` body field, `posData` as the network carrier) together with
the mock profile (`cbl-mock-gateway@808c38b`), the conformance fixtures and the
tests, and every remaining review finding was closed (non-positive/negative
amounts, empty identifiers, provider amount echo, network rows, base-URL and
HTTP-client hardening, invoiceTime skew bound). Final evidence: conformance
`Run` green for the bitpay fixtures (CONF-1..CONF-10; the first e2e pass
reported the mock invoice id as a 22-hex BitPay id, the scenario check was
aligned), unit coverage total 86.6% (bitpay package 85.4%), the cross-service
scenario `E2E_Connector_BitPayProfile` PASS 11/11 (mark `20261017t161450z`),
integration and e2e regression runs green, lint 0.

## 11. Work plan and commits

Increment 1 (this commit): spec, dependency `dcrd/dcrec/secp256k1/v4`, BitPay
adapter with signing and PEM loading, wiring, tests, compose env defaults with the
BitPay scenario, E2E.md, AGENTS/README. The real sandbox run is a 17.16 follow-up
(needs 17.0).
