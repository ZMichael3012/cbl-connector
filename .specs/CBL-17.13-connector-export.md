# CBL-17.13. Connector per-provider payment exports

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` |
| Size | M |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.2 (`ListGatewayPayments` deferred), 17.6-17.9 (adapters), 17.12 |
| Contract version | `cbl-protos@cf-4.2` (consumed, `ListGatewayPayments` frozen at CF-1) |
| Status | done |

## 1. Context and goal

Task 17.13 (per-provider reconciliation) supersedes 9.1-9.3: the reconciler pulls
provider operations with cursors and matches them with Payment, Ledger and the
gateway events. The connector owns the provider-facing half: the
`ListGatewayPayments` RPC was deliberately deferred in 17.2 and answers
`Unimplemented`. This increment implements it for all four H1 adapters
(NOWPayments, CoinGate, BitPay, BTCPay) behind a new optional SPI v1 interface
`PaymentExporter`, with an opaque cursor, provider statuses without fabrication
and a hard limit per page.

## 2. Scope

In scope: the optional `domain.PaymentExporter` SPI, cursor encode/decode, the
transport implementation of `ListGatewayPayments` (validation, provider
resolution, canonical serialization, error mapping), wrapper forwarding
(`Limited` uses the status bucket, `Resilient` gets a dedicated attempt
timeout), adapter export methods for the four H1 providers, unit tests for
every layer, docs.

Out of scope: the reconciler service itself (the next increment in
`cbl-reconciler`: scheduling, matching, `recon.alerts`, replay); payout/refund
exports (an additive `ListGatewayPayouts` contract if 17.13 needs it after the
payment contour); real sandbox validation of the export wire shapes (17.16
accounts); NOWPayments payout exports (H2, decision 8).

## 3. Architecture sketch

- `domain.PaymentExporter` (optional SPI v1):
  `ExportPayments(ctx, ExportRequest{From, To, Limit, Cursor}) (ExportPage{Payments []PaymentStatus, NextCursor}, error)`.
  The export reuses `PaymentStatus` (gateway id, our payment/order id, canonical
  state, raw status, received amount, confirmations, txid, network, event time).
  An empty `State` with a non-empty `RawStatus` means "the provider word is not
  mapped": the row is exported with the raw word for the reconciliation alert,
  never fabricated into a canonical state and never a page error (COMMON-3).
- Cursor is opaque to consumers: `domain.EncodeExportCursor(offset)` /
  `domain.DecodeExportCursor(cursor)` (base64url of `x1:<offset>`); an empty
  cursor means offset 0; an invalid cursor is `domain.ErrInvalidGatewayRequest`
  (`InvalidArgument`). Page-based providers (NOWPayments, CoinGate) derive
  `page = offset/limit + 1` and reject a cursor whose offset is not a multiple
  of the request limit; offset-based providers (BitPay, BTCPay) pass the offset
  as `offset`/`skip`. `NextCursor` is set exactly when the page is full
  (`len == limit`), so a short page terminates the walk.
- `gateway.Limited` forwards `ExportPayments` under the `status` bucket (a
  read-only provider call, no new limiter bucket); `gateway.Resilient`
  forwards it with a dedicated attempt timeout (`attemptTimeoutExport`, 3 s,
  deliberately well below the 10 s RPC deadline of CBL-003 so a hung provider
  cannot burn the caller budget in one attempt and the shared bulkhead slot
  is released quickly) through the shared retry/breaker/bulkhead executor.
  Missing capability on a bare implementation is `ErrRejected` (the Lookup
  precedent).
- `transport.ConnectorServer.ListGatewayPayments`: provider required and
  resolved from the wired map (normalized id, `NotFound` when absent); `from`
  and `to` required (`from < to`, window bounded by `maxExportWindow` = 31 d,
  `to` no further than `now + exportFutureSkew` = 5 m); `limit` defaults to
  `defaultExportLimit` = 100 and is bounded by `maxExportLimit` = 500; cursor
  passed through. Rows are serialized with the existing `canonicalStatus`; a
  row with the unmapped-status shape (empty state, non-empty raw) becomes
  `UNSPECIFIED` with `raw_status`; any other state outside the allowlist is an
  adapter contract violation (`Internal`); more rows than the limit is
  `Internal`.
- Adapters implement `ExportPayments` with fail-soft row mapping
  (`exportStatusOf`): an unknown status word or an unparsable amount yields a
  row with the raw word / zero amount instead of failing the page; transport
  errors keep the standard classification (`request` helper: 404 `notFound`,
  429/5xx `ErrTemporary`, other 4xx `ErrRejected`).

Provider wire shapes (provisional until the 17.16 sandbox runs, the
KI-connector-12 pattern):

| Provider | Endpoint | Window | Paging |
| :-- | :-- | :-- | :-- |
| nowpayments | `GET /v1/payment/` | `dateFrom`/`dateTo` RFC3339 | `page`, `limit` |
| coingate | `GET /v2/orders` | `from`/`to` RFC3339 | `page`, `per_page` |
| bitpay | `GET /invoices` | `dateStart`/`dateEnd` (date) | `limit`, `offset` |
| btcpay | store invoices `GET .../invoices` | `startDate`/`endDate` (unix) | `take`, `skip` |

## 4. Functional requirements

FR-1. `ListGatewayPayments` returns one page of canonical provider records for
the requested provider and window, with a stable `next_cursor` contract
(empty on the last page, opaque otherwise).

FR-2. Records that cannot be canonically mapped are exported with the raw
provider word (`status = UNSPECIFIED` on the wire) instead of failing the page
or fabricating a state.

FR-3. Invalid input (unknown provider, missing/ill-ordered/oversized window,
limit over the bound, malformed cursor, cursor/limit mismatch) fails with
`InvalidArgument`/`NotFound` and never reaches the provider.

FR-4. A temporary provider failure is retryable (`Unavailable`), a provider
business rejection is `FailedPrecondition`; the breaker counts only temporary
failures (the shared executor).

FR-5. The export is throttled by the manifest limiter (status bucket) and
carries the resilience stack (retries, breaker, bulkhead, attempt timeout).

## 5. Contracts

`ListGatewayPaymentsRequest{provider, from, to, limit, cursor}` and
`ListGatewayPaymentsResponse{payments, next_cursor}` are frozen at CF-1; this
increment implements them, no contract change.

## 6. Data model and migrations

None: the export is stateless (the reconciler persists cursors on its side).

## 7. State machines and rules

The export reports provider state at the moment of the read. The canonical
state table per adapter is the existing `paymentStates`/`orderStates`/
`invoiceStates` mapping; the export adds no new canonical values.

## 8. Edge cases

E-1. Empty page below the limit -> no `next_cursor`.
E-2. A full last page still carries a cursor; the following page is empty.
E-3. Unknown provider word in one row -> row exported with the raw word, page
succeeds, the reconciler raises `status_mismatch`.
E-4. Malformed amount evidence in one row -> the amount stays absent on the
wire (empty asset, `received_amount` unset): "no evidence" and a genuine zero
(with asset and scale) are distinct for the reconciler.
E-5. Cursor offset beyond the result set -> empty page (providers clamp). A
provider that clamps the page size below the requested limit silently shortens
the walk (the short-page termination rule); the reconciler dedupes by
`(provider, gateway_id)` and windows absorb the gap (KI-connector-13).
E-6. Cursor with a non-multiple offset for a page-based provider ->
`InvalidArgument` before any provider call.
E-7. Provider 404 on a window -> `NotFound` (a misconfigured export call).
E-8. Row count above the requested limit (adapter bug) -> sanitized `Internal`.

## 9. Non-functional requirements

- NFR-1. Coverage stays at least 80% (unit+integration).
- NFR-2. No new dependencies.
- NFR-3. English only, no em dash, godoc on every exported entity.
- NFR-4. No new tables, no migrations.

## 10. Test plan

### 10.1 Unit matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| X-01 | cursor round-trip | offsets 0, 100, 12345 | decode(encode(n)) == n |
| X-02 | invalid cursor | "", "x1:-1", "%%%", "x9:5", raw base64 of "5" | ErrInvalidGatewayRequest (except "" -> 0) |
| X-03 | transport happy page | two rows + next cursor | proto fields incl. raw, money, ts; cursor passed to exporter |
| X-04 | transport unmapped row | empty state + raw word | status UNSPECIFIED, raw kept |
| X-05 | transport unknown provider | provider "nope" | NotFound, exporter not called |
| X-06 | transport window validation | missing from/to; from >= to; window > 31 d; to far future | InvalidArgument |
| X-07 | transport limit | 0 -> default 100; 501 -> InvalidArgument | exporter sees 100 / call rejected |
| X-08 | transport malformed cursor | cursor "%%%" | InvalidArgument (adapter wraps decode) |
| X-09 | transport no capability | bare gateway without PaymentExporter | FailedPrecondition fixed message |
| X-10 | transport adapter violation | state "weird" (mapped path) non-empty | Internal, sanitized |
| X-11 | transport page over limit | 3 rows with limit 2 | Internal |
| X-12 | transport error mapping | ErrTemporary; ErrGatewayNotFound | Unavailable; NotFound |
| X-13 | resilient forwards | fake inner with export | delegates, provider preserved |
| X-14 | resilient missing capability | inner without exporter | ErrRejected |
| X-15 | limited forwards | limiter allows | delegates via status bucket key |
| X-16 | limited missing capability | inner without exporter | ErrRejected |
| X-17 | nowpayments export | window + page query, envelope rows | canonical rows, order id mapping, next cursor on full page |
| X-18 | nowpayments fail-soft | unknown status + broken amount row | row with raw word/zero amount, no error |
| X-19 | coingate export | page/per_page/from/to query, array or `orders` envelope | rows, order id, cursor |
| X-20 | coingate soft + errors | unknown status; 429 | soft row; ErrTemporary |
| X-21 | bitpay export | signed GET, date window, limit/offset | rows from facade envelope, order id, cursor |
| X-22 | bitpay soft | unknown status row | soft row |
| X-23 | btcpay export | store path, take/skip, unix window | rows from array, order id from metadata, cursor |
| X-24 | btcpay soft + errors | unknown status; 500 | soft row; ErrTemporary |
| X-25 | cursor alignment | page-based provider with offset 3, limit 2 | InvalidArgument, no provider call |

### 10.2 Integration matrix

None (stateless export; the wrapped-adapter path is covered by unit rows).

### 10.3 E2E scenario description

`E2E_Connector_Export` (runs with the 17.16 sandbox harness and the mock
profiles meanwhile): start the connector against the mock-gateway profile of a
provider, call `ListGatewayPayments` through grpcurl (bufconn in tests) twice
with the returned cursor, assert the second call consumes the cursor, the rows
carry `payment_id`, canonical status and `raw_status`, and an unknown-status
fixture row arrives as `UNSPECIFIED`; artifacts per E2E.md. The local
testcontainers scenario is not needed (no Kafka/PG state); the adapter and
transport unit rows plus the existing cross-service profiles cover the wire.

### 10.4 Load

Unchanged; the export is a background path (the reconciler pacing owns it).

## 11. Reviews

Red run (18.10.2026, before the implementation): `go test` over the new test
files failed as expected: `undefined: domain.EncodeExportCursor`,
`undefined: domain.DecodeExportCursor`, `undefined: domain.ExportRequest` and
`adapter.ExportPayments undefined` for the four providers (build failures
recorded in the session evidence file `conn-export-red.txt`).

Four reviewers ran on the staged diff (18.10.2026): no-context (16 findings),
architecture (10), with-context (7, one blocking), security (7, none blocking).
Closure:

- Blocking (with-context): `AGENTS.md` and `test/e2e/E2E.md` were not updated.
  Closed: the contracts line, the SPI v1 interface list, `T-CONNECTOR-16` and
  the new `T-CONNECTOR-21`, `KI-connector-13`, the journal row, the export
  runbook block, the `E2E_Connector_Export` scenario row and its section 10.
- The export attempt budget equaled the whole 10 s RPC deadline (flagged by
  three reviewers): `attemptTimeoutExport` is now 3 s with the rationale in
  the constant comment and the spec.
- Missing amount evidence was indistinguishable from a genuine zero (security):
  an empty asset now leaves `received_amount` unset on the wire; a genuine
  zero keeps the asset and the scale; documented in `domain.ExportPage`, the
  spec (E-4) and the runbook.
- The cursor had no length bound (security): the transport rejects a cursor
  over 64 bytes with `InvalidArgument` before any provider call.
- The short-page termination can hide rows when a provider clamps the page
  size: recorded in E-5 and `KI-connector-13`; the reconciler dedup and windows
  absorb it until 17.16 validates the provider behavior.
- Decoder duplication (three reviewers): the list decoders of NOWPayments,
  CoinGate and BitPay now share `providers/wire.DecodeEnvelopeOrArray`, the
  offset providers share `domain.ExportOffsetParams`, and a missing envelope
  key is a decode error instead of a silent empty page.
- Thin test rows were closed: cursor pass-through, empty provider, row without
  status evidence, cursor bound, `ExportPageParams`/`ExportOffsetParams`
  branches, the limiter-denied export, the NOWPayments bare-array and
  malformed-cursor rows, the CoinGate and BitPay shape violations.
- Declined with rationale: a printable bound on `raw_status` (unchanged from
  `GetGatewayPayment`; the word is provider evidence bounded by the 1 MB body
  read), the BTCPay `Network` field (the Greenfield invoice carries no
  network; the reconciler matches by `payment_id`), validation of the
  provider-echoed order id (the export is evidence; classification is the
  reconciler's decision), and consulting the admin disabled set (deliberate:
  reconciliation must still see a disabled provider, T-CONNECTOR-21).

Final runs (18.10.2026): unit green, storage/usecase integration green, the
testcontainers e2e (normalizer and payout pipeline with the refund stage)
green, unit+integration coverage 87.7% (no regression; baseline 87.0%),
`go vet` clean, lint 0, the em dash check green. One pre-existing flake
(`TestCreateInvoiceErrorClassification/400_is_rejected` in the nowpayments
suite, an `httptest` connection-reuse race) was observed once and is green on
repeated runs; it is unrelated to this diff.

## 12. Documentation

`AGENTS.md` (contracts line, trade-off entry for the provisional export shapes,
journal), `test/e2e/E2E.md` (scenario), runbook note on the export.

## 13. Open questions

None blocking: the wire shapes are provisional by design (17.16 re-validates).
