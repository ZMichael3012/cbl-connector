# CBL-17.26 connector increment: sandbox wire fixes (CoinGate, NOWPayments, BitPay)

Status: in progress (19.10.2026). Task 17.26 (account-gated provider acceptance),
connector increment. Repository: `cbl-connector`. Related: umbrella
`Внешняя подготовка 17.0 (аккаунты, запросы, Vault).md` sections 6.2/7/10.

## 1. Goal

Close the connector-side wire gaps found by the 17.0 sandbox probes (KI-connector-14
CoinGate, KI-connector-15 NOWPayments, KI-connector-10 BitPay) so the CoinGate,
NOWPayments and BTCPay sandbox runs can pass, and align the BitPay adapter with the
live-verified contract. The BitPay live run stays postponed (account-side mock pay
blocker); its fixes land with unit coverage and fixtures only.

Additional finding from the reconnaissance (separate ingress increment): the ingress
`gateway_id` extraction reads only top-level `gateway_id`/`gatewayId`/`id`, so a NOW IPN
(`payment_id`) and a BitPay IPN (`data.id`) arrive with `gateway_id=unknown` and the
connector fetch path fails closed. The connector contract stays unchanged (the raw
delivery must carry the resolved gateway id); the ingress gains a per-provider
`gateway_id` template.

## 2. Scope

In scope:

1. `domain.StatePending` - a read-only canonical state (reads and exports). The mock
   sandbox alias map (`CanonicalState("pending") -> detected`) and the canonical event
   validation stay unchanged: pending is never published as an event (T-CONNECTOR-02
   mock contract untouched).
2. Adapter status tables: CoinGate `new`, NOWPayments `waiting`, BTCPay `New`,
   BitPay `new` -> pending. CoinGate `pending`/`confirming`, BTCPay `Processing`,
   BitPay `paid`/`confirmed` stay detected (a deposit exists).
3. Adapter `VerifyWebhook`: a pending-mapped status is rejected (no event; the
   fetch-mandatory path decides), so a pending type can never surface as an event.
4. Transport: `canonicalStatusValue` gains the PENDING enum (7);
   `GetGatewayPayment` and `ListGatewayPayments` accept the pending state.
5. Normalizer fetch path: a fetched pending state returns a temporary error (retry,
   fail closed); no canonical event is built.
6. CoinGate: map `payment_url` -> `checkout_url`; the create body gains `token` (the
   configured IPN token), an optional `callback_url` (new config, env
   `CONNECTOR_COINGATE_CALLBACK_URL`), `title` and `description`; the export window
   switches to `created_at[from]`/`created_at[to]` (dates) plus
   `sort=created_at_asc`.
7. BitPay: GET requests carry the token as a query parameter covered by the signature
   (the `Authorization` header is dropped; BitPay ignores it); POST/PUT bodies carry
   `token` (create already does; payouts and refunds gain it); a new optional
   `Config.BuyerEmail` (env `CONNECTOR_BITPAY_BUYER_EMAIL`, sandbox only) adds
   `buyer.email` plus the forced buyer selections to the create body.
8. Wiring: `cmd/connector/main.go`, `.env.example`, AGENTS.md (envs, KI, journal).

Out of scope: the sandbox run execution (increment B); the NOW invoice->payment fetch
resolution and JWT (KI-connector-15 stays open until the first deposit answers the
open question); the BitPay live run (postponed).

## 3. Architecture sketch

Layers touched: `internal/domain` (state), `internal/providers/*` (tables, create,
export, request transport), `internal/transport` (enum mapping), `internal/usecase`
(fetch guard), `internal/conformance` (fixture expectations), `cmd/connector` (envs).

State flow (unchanged shape, one new state):

    provider word -> adapter table -> domain.State -> transport enum -> reconciler
                       |                |                 |
                       |                |                 +-- PENDING (7), reads/exports only
                       |                +-- events: pending never passes Validate
                       +-- fetch path: pending -> temporary error (retry)

CoinGate create body (after): `order_id`, `price_amount`, `price_currency`,
`pay_currency`, `title`, `description`, `token`, optional `callback_url`.

BitPay request transport (after):

    GET  -> url + ?token=<apiToken> (signed over the full URL, no Authorization)
    POST -> body carries "token" (create/payout/refund), signed over url + body
    PUT  -> body carries "token" (mock-pay-like shapes owned by 17.11/17.12)

## 4. Decisions

| ID | Decision | Rationale |
| :-- | :-- | :-- |
| T-CONNECTOR-22 | `StatePending` is read-only: never an event, never a mock alias; the fetch path retries on it | KI-connector-14(3): pre-deposit words must not fabricate DETECTED in exports/reads; the mock contract (T-CONNECTOR-02) and the event validation stay untouched |
| T-CONNECTOR-23 | CoinGate sends our IPN token and the per-order callback URL at creation (env-gated; empty = omitted) | KI-connector-14(2): without them CoinGate generates its own token and the ingress token echo fails closed on every callback |
| T-CONNECTOR-24 | BitPay token transport: body for POST/PUT, query for GET, `Authorization` dropped (verified live 19.10.2026) | KI-connector-10: Bearer reads resolve to the public/user facade and fail; the signature must cover the URL including the token parameter |
| T-CONNECTOR-25 | CoinGate export window: `created_at[from]`/`created_at[to]` + `sort=created_at_asc` | KI-connector-14(4): the API ignores `from`/`to`; the ascending walk keeps the offset cursor stable; the window becomes whole-day granular and the reconciler dedup `(provider, gateway_id)` absorbs the overlap |
| T-CONNECTOR-26 | The BitPay sandbox buyer contact is env-gated (`CONNECTOR_BITPAY_BUYER_EMAIL`): it adds `buyer.email` and the forced wallet/currency (asset of the request) to the create body | mock pay requires a contact email and preselected wallet/currency; production behavior must not preselect the buyer's wallet |
| T-CONNECTOR-27 | CoinGate orders are fiat-priced (found live in increment B: the API rejects stablecoins as `price_currency`, 422 "Price currency USDT is not supported"): the configured price currency prices itself, a known stablecoin is priced at par in `CONNECTOR_COINGATE_PRICE_CURRENCY` (default USD) rounded half-up to cents, any other asset is rejected; the provider price echo is compared at the fiat scale | CoinGate has no crypto price currencies, so a stablecoin invoice must be represented as its fiat value; the platform amount stays the expected amount and the received evidence comes from the USDT pay side | the par approximation ignores the stablecoin/fiat spread and rounds to cents (a non-round amount lands as under/overpaid, absorbed by the 17.10 rules); a real rate conversion belongs to the 17.14/quote follow-up | unit tests pin the rendered price, the echo drift rejection and the non-stablecoin rejection; the sandbox run validates the real order |

## 5. Error taxonomy

- Fetch of a pending status: `ErrTemporary` (inbox retry, then DLQ; fail closed).
- `VerifyWebhook` of a pending-mapped status: `ErrInvalidWebhook` (the delivery
  carries no event).
- Everything else keeps the 17.7 - 17.13 taxonomy.

## 6. Test plan

Unit matrix (red first, then implementation):

| ID | Case | Expected |
| :-- | :-- | :-- |
| D-01 | `domain.StatePending` value | `"pending"`; `CanonicalState("pending")` still returns detected (mock alias untouched) |
| CG-01 | CoinGate create response carries `payment_url`, no address | `Invoice.CheckoutURL` = payment_url, `Address` empty |
| CG-02 | CoinGate create body with a configured callback URL | body carries `token`, `callback_url`, `title`, `description` |
| CG-03 | CoinGate create body without a callback URL | no `callback_url` key; `token`/`title`/`description` still present |
| CG-04 | CoinGate status table | `new` -> pending; `pending`/`confirming` -> detected; `paid` -> confirmed (unchanged) |
| CG-05 | CoinGate VerifyWebhook with status `new` | error (`ErrInvalidWebhook`), no event |
| CG-06 | CoinGate export query | `created_at[from]`/`created_at[to]` dates + `sort=created_at_asc`; a `new` row exports pending |
| NP-01 | NOW status table / export | `waiting` -> pending; others unchanged |
| NP-02 | NOW VerifyWebhook with status `waiting` | error, no event |
| BP-01 | BitPay status table / export | `new` -> pending; `paid`/`confirmed` -> detected; `complete` -> confirmed |
| BP-02 | BitPay VerifyWebhook with status `new` | error, no event |
| BP-03 | BitPay GET request | URL carries `token`; no `Authorization`; signature verifies over the URL with the token |
| BP-04 | BitPay POST bodies (create, payout, refund) | body carries `token` |
| BP-05 | BitPay create with `BuyerEmail` set | body carries `buyer.email`, `forcedBuyerSelectedWallet=bitpay`, `forcedBuyerSelectedTransactionCurrency` = request asset |
| BP-06 | BitPay create without `BuyerEmail` | the buyer fields are absent |
| BT-01 | BTCPay status table | `New` -> pending; `Processing` -> detected (unchanged) |
| BT-02 | BTCPay VerifyWebhook with type `InvoiceCreated` | error, no event |
| SEC-01 | Provider error excerpt echoing the request URI/token (BitPay GET, CoinGate create) | the configured secrets are redacted from the error text (`providers/wire.Redact`) |
| SEC-02 | BitPay config with the sandbox buyer email on a BitPay production host | configuration error (fails closed) |
| CG-07 | CoinGate create with a stablecoin asset (increment B finding) | the body carries the fiat `price_currency` and the amount rendered at par to cents; a fiat asset prices itself |
| CG-08 | CoinGate create with a non-stablecoin asset (no rate wired) | business rejection before any provider call |
| CG-09 | CoinGate response price currency or price amount drifting from the rendered price | business rejection |
| TR-01 | `GetGatewayPayment` with a pending-state gateway | status PENDING on the wire |
| TR-02 | `ListGatewayPayments` with a pending export row | status PENDING on the wire |
| NF-01 | Fetch returns pending | temporary error; no outbox envelope |
| CF-01 | Conformance status tables for the four providers | updated expectations; the suite stays green |

Integration: no storage changes; the existing PostgreSQL/Kafka suites re-run as
regression.

E2E (testcontainers, regression): `E2E_Connector_Smoke`,
`E2E_Connector_CoinGateProfile`, `E2E_Connector_BitPayProfile`,
`E2E_Connector_NowPaymentsProfile`, `E2E_Connector_PayoutPipeline` re-run green.

Sandbox scenarios (increment B, described in the umbrella 17.0 section 10): CoinGate
create -> checkout_url -> sandbox payment -> token-echo callback; NOW create -> IPN
(HMAC) -> read by payment_id; BTCPay regtest regression; BitPay postponed.

## 7. Red evidence

Run 19.10.2026, `go test ./internal/domain/ ./internal/providers/... ./internal/transport/ ./internal/usecase/ ./internal/conformance/`:

```
internal\conformance\profiles.go:166:37: undefined: domain.StatePending
internal\conformance\profiles.go:211:33: undefined: domain.StatePending
internal\conformance\profiles.go:259:33: undefined: domain.StatePending
internal\conformance\profiles.go:306:33: undefined: domain.StatePending
internal\domain\gateway_test.go:40:50: undefined: domain.StatePending
FAIL	github.com/cbl/cbl-connector/internal/domain [build failed]
FAIL	github.com/cbl/cbl-connector/internal/providers/bitpay [build failed]
FAIL	github.com/cbl/cbl-connector/internal/providers/btcpay [build failed]
FAIL	github.com/cbl/cbl-connector/internal/providers/coingate [build failed]
FAIL	github.com/cbl/cbl-connector/internal/providers/nowpayments [build failed]
internal\usecase\normalizer_fetch_test.go:146:21: undefined: domain.StatePending
internal\transport\connector_test.go:358:21: undefined: domain.StatePending
internal\transport\connector_test.go:617:71: undefined: domain.StatePending
FAIL	github.com/cbl/cbl-connector/internal/transport [build failed]
FAIL	github.com/cbl/cbl-connector/internal/usecase [build failed]
FAIL	github.com/cbl/cbl-connector/internal/conformance [build failed]
```

Every failing package names the new contract: the pending state, the token transport and the
CoinGate create/export expectations are not implemented yet.

Green evidence (19.10.2026): `go test ./...` green; `go test -tags integration -count=1 ./internal/...`
green (storage on testcontainers PostgreSQL); `go test -tags e2e -count=1 ./test/e2e/` green
(testcontainers Kafka+PostgreSQL, 58 s); unit+integration coverage 87.8% (was 87.7%);
`go vet` clean; golangci-lint (Docker, latest-alpine) 0 issues; the em dash check passes.

Review closure (4 passes, 19.10.2026): findings closed in this commit - the BTCPay
`InvoiceCreated` webhook guard (a pre-deposit type carries no event), the secret redaction of
provider error excerpts (`providers/wire.Redact`, applied to all four adapters, with leak tests
for the BitPay query token and the CoinGate IPN/API tokens), the BitPay sandbox buyer email
production-host guard plus email validation, the `url.Error` unwrapping on transport and build
failures, the NOW export pending-row test, the BitPay query-bearing GET test, the E2E.md
profile expectations (PENDING), the AGENTS.md envs/trade-offs/KI/journal updates and this spec.

## 8. Open questions

- KI-connector-15: does the NOW IPN carry `invoice_id`, or does `payment_id` equal the
  invoice id after the first deposit? The sandbox run answers it; until then the NOW
  fetch path is not exercised live.
- CoinGate: does the create response echo the configured token? Verified in the
  sandbox run; the ingress token echo is authoritative either way.
