# CBL-17.12. Connector provider refunds

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` |
| Size | M |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.11 (payout pipeline), `cbl-protos@cf-4.2` (refund marker), 17.6-17.9 (adapters) |
| Contract version | `cbl-protos@cf-4.2` (consumed) |
| Status | done |

## 1. Context and goal

The payout saga emits refund commands with the source invoice
(`PayoutCommand.refund_invoice_id`, cf-4.2). A refund must go back through the
provider that accepted the original payment and use that provider refunds API
(CoinGate refunds, BitPay refunds, BTCPay refund pull payments). This increment
branches the payout pipeline: a refund command resolves the original provider
invoice from the `provider_invoices` mapping, calls the adapter `CreateRefund`,
records the mapping with the refund marker and polls the refund status.

## 2. Scope

In scope: the refund branch in `PayoutProcessor`, the `ByPaymentID` mapping
lookup, `provider_payouts.refund_invoice_id` (migration 00008), the optional
`RefundStatusGateway` SPI with the three H1 adapters (BTCPay aliases its store
payout read; CoinGate and BitPay read their refund endpoints), wrapper
forwarding, the worker refund branch, unit/integration/e2e tests, docs.

Out of scope: NOWPayments refunds (manual path, decision 8); BitPay automatic
under/over refunds (KI-connector-12: needs invoice refund flags at creation);
real sandbox validation of the refund wire shapes (17.16).

## 3. Architecture sketch

- `domain.RefundStatusGateway` (optional SPI v1): `GetRefund(gatewayRefundID)
  (PayoutStatus, error)`; `gateway.Limited`/`gateway.Resilient` forward it like
  `CreateRefund`; BTCPay delegates to `GetPayout`, CoinGate reads
  `GET /v2/refunds/{id}`, BitPay reads `GET /refunds/{id}` with status tables.
- `storage.ProviderInvoicesRepository.ByPaymentID(paymentID)`: the mapping of
  the original payment (the refund invoice id is the platform payment id).
- `PayoutProcessor`: when `refund_invoice_id` is set, the router is not used for
  capabilities: the provider comes from the mapping, must be enabled by
  `CONNECTOR_PAYOUT_PROVIDERS`, and its gateway must implement `domain.Refunder`;
  `CreateRefund` receives the provider invoice id, the refund amount and the
  optional destination; the mapping row records `refund_invoice_id`.
- `PayoutStatusWorker`: rows with a refund marker poll `GetRefund` when the
  provider implements it, otherwise the row is only touched (KI-connector-12).

## 4. Functional requirements

FR-1. A refund command creates exactly one provider refund through the original
provider and publishes a submitted event with the provider refund id; the
mapping stores the refund marker.

FR-2. A missing mapping, a disabled provider or a provider without the refunds
API publishes a failed event with a sanitized reason.

FR-3. Replays never reach the provider twice (the payout-id lock, the mapping
and the provider reference `refund-<payout_id>`/`reference`/`platform_id`).

FR-4. Refund status polling resolves confirmed/failed exactly once; without a
refund status implementation the row stays submitted (touched) and the manual
path applies.

## 5. Contracts

`PayoutCommand.refund_invoice_id` (cf-4.2) is consumed; no further change.

## 6. Data model and migrations

Migration 00008: `provider_payouts.refund_invoice_id text`.

## 7. State machines and rules

The connector mapping stays three-state; the refund branch shares the replay,
event and polling machinery of the payout pipeline.

## 8. Edge cases

E-1. Refund command without a mapping -> failed, the saga releases.
E-2. Provider outside `CONNECTOR_PAYOUT_PROVIDERS` -> failed.
E-3. Provider without `Refunder` -> failed (NOWPayments manual path).
E-4. Provider refund id unusable (empty/over-long/control) -> error (retry/DLQ).
E-5. Provider refund status unknown -> WARN, row touched.

## 9. Non-functional requirements

- NFR-1. Coverage stays at least 80%.
- NFR-2. No new dependencies.
- NFR-3. English only, no em dash, godoc.

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Expected |
| :-- | :-- | :-- |
| P-01 | refund success | CreateRefund with the mapping gateway id; marker stored; submitted event |
| P-02 | missing mapping | failed event with the no-mapping reason |
| P-03 | provider not enabled | failed event |
| P-04 | no Refunder | failed event |
| P-05 | mapping error | retryable error |
| P-06 | worker refund confirmed | resolved via GetRefund once |
| P-07 | worker refund without GetRefund | touched only |
| P-08 | adapter GetRefund | request path and status mapping (CoinGate, BitPay, BTCPay alias) |
| P-09 | wrapped unsupported provider | `ErrRefundsUnsupported` from the decorators -> failed "provider does not support refunds" |
| P-10 | ambiguous mapping | a payment with mappings at several providers -> failed "refund invoice has multiple provider mappings" |

### 10.2. Integration matrix (PostgreSQL)

| ID | Case | Expected |
| :-- | :-- | :-- |
| I-01 | ByPaymentID | resolves the mapping; missing -> sentinel |
| I-02 | refund column | roundtrip and replay barrier |

### 10.3. Smoke set

`make test`, `make test-integration`, e2e payout pipeline, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Connector_PayoutPipeline (refund stage) | testcontainers Kafka/PostgreSQL: a refund command produces a submitted event via `CreateRefund`; the worker resolves it through `GetRefund`; a replay is a no-op | `go test -tags e2e ./test/e2e/ -run Payout` |

### 10.5. Load plan

Not applicable.

### 10.6. Test-first evidence

Red run recorded before implementation: the refund tests failed to compile on
HEAD (`undefined: usecase.RefundInvoiceMapping`, `undefined:
usecase.WithRefundMapping`, `storage.ProviderPayoutRow has no field or method
RefundInvoiceID`), i.e. the feature was absent.

Final batteries: unit green; the full battery with integration
(`go test -tags integration -coverpkg=./internal/... ./...`) green, total
coverage 87.0% (no regression against the 17.11 baseline 86.9%); the storage
integration covers the refund column and `ByPaymentID`; the adapter tests cover
the CoinGate/BitPay refund endpoints and the BTCPay alias; the Kafka e2e
`TestE2EPayoutPipeline` includes the refund stage (a refund command through
`CreateRefund`, exactly-once `GetRefund` resolution, the marker row, a replay
no-op) and is green; `go vet`, lint and the em dash check pass. Review round
one (four reviewers) produced one blocking finding and several major/minor
ones; all were closed on the staged diff: the refund call is now idempotent per
command (`domain.RefundRequest.PayoutID`: CoinGate `platform_id`, BitPay
`reference`, BTCPay pull-payment name), a payment mapped at several providers
is detected as ambiguous and fails closed, the `ErrRefundsUnsupported` sentinel
travels through the decorators and maps to the fixed failed reason (a silent
touch in the worker instead of a per-sweep WARN), `GetRefund` uses the status
bucket, migration 00008 checks the marker is a canonical UUID, provider status
texts are bounded, the e2e includes the refund replay no-op, and the
documentation ranges/status are aligned. The closure round confirmed no
blockers (journal).

## 11. Work plan and commits

Single commit: spec, SPI, mapping lookup, migration, processor and worker
branches, adapter reads, tests, docs.



