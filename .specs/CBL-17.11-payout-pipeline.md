# CBL-17.11. Connector payout pipeline

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.2 (SPI, outbox/inbox, provider invoices), 17.6-17.9 (adapters), CF-2/CF-4 contracts |
| Contract version | none (CF-4 fields already frozen; no `.proto` change) |
| Status | done |

## 1. Context and goal

The payout saga in `cbl-payout` already emits `payout.commands` and consumes
`gateway.payout.events`, but the connector never consumed the commands: task
10.3 (superseded by 17.11) was open with KI-payout-06. This increment builds the
connector payout pipeline for the H1 providers (BTCPay, CoinGate, BitPay; the
NOWPayments batch with JWT/TOTP is H2 per the working plan and stays out of
scope): the command consumer calls the adapter, records the payout mapping and
publishes the submitted event with the provider; a polling worker reads the
provider status and publishes confirmed/failed exactly once. The payout side
propagation is a separate increment in `cbl-payout`.

## 2. Scope

In scope: migration `provider_payouts`, the repository, the command processor,
the payout selector (allowlist + capability match), the status worker, main
wiring, unit/integration/Kafka e2e tests, runbook and docs.

Out of scope: NOWPayments payouts with JWT/TOTP (H2; deferred); provider-side
2FA/approval automation (the operator confirms at the provider, the payout stays
`submitted` and the worker keeps polling); refunds (17.12); reconciler exports
(17.13); real sandbox validation of the wire shapes (17.16); payout-side
provider propagation (`cbl-payout` increment).

## 3. Architecture sketch

- `migrations/00007_provider_payouts.sql`: `provider_payouts` mapping
  (`payout_id` PK, `provider`, `gateway_payout_id`, `state` submitted/confirmed/
  failed, amount/asset/decimals/network, `address_hash`, txid, fee columns,
  reason, timestamps); unique `(provider, gateway_payout_id)`, partial pending
  index on `updated_at`.
- `internal/storage/provider_payouts.go`: the row and the repository
  (`Get`, `InsertSubmittedTx` with ON CONFLICT DO NOTHING, `ListPending`,
  `TouchSubmittedTx`, `ResolveTx` with the `state = 'submitted'` CAS).
- `internal/usecase/payout_processor.go`: consumes `payout.commands` in the
  Guard transaction; validates the untrusted command; skips a replay by
  `payout_id`; selects exactly one enabled provider whose capabilities cover the
  network/asset/amount; calls `CreatePayout`; on success inserts the mapping and
  appends the `submitted` envelope; a business rejection appends `failed`; a
  temporary failure returns an error (retry topics). `PayoutRouter` owns the
  selection (`CONNECTOR_PAYOUT_PROVIDERS`, default `btcpay,coingate,bitpay`).
- `internal/usecase/payout_status_worker.go`: polls pending rows
  (`CONNECTOR_PAYOUT_POLL_INTERVAL`, default 30s; batch 100), calls `GetPayout`
  per provider, touches the row to enforce the cadence, and resolves the first
  terminal status into the mapping plus a `confirmed`/`failed` envelope in one
  transaction (CAS; exactly one publisher).
- Envelopes: topic `gateway.payout.events`, key `payout_id`, payload
  `GatewayPayoutEvent` (provider field 7 filled), headers producer/schema/
  occurred_at/provider plus the incoming traceparent.
- main: a second kit consumer (`cbl-connector-payouts`) on `payout.commands` and
  retry topics, the status worker goroutine; the existing relay publishes the
  new topic.

## 4. Functional requirements

FR-1. A valid command creates exactly one provider payout and publishes one
`submitted` event carrying `gateway_payout_id` and `provider`; a redelivered or
retried command for a recorded `payout_id` never calls the provider again.

FR-2. A provider business rejection (or no capable provider) publishes one
`failed` event with a sanitized reason; the payout saga compensates.

FR-3. Temporary provider failures return an error and go through the retry
topics; nothing is recorded or published.

FR-4. The status worker publishes `confirmed` (txid/fee) or `failed` (reason)
exactly once per submitted payout; a repeated provider answer is a no-op by the
CAS; without a terminal status the row is only touched.

FR-5. Only providers from `CONNECTOR_PAYOUT_PROVIDERS` are selected; an empty
allowlist disables payouts fail-closed. Selection requires a capability row for
the network and asset with the amount within limits.

## 5. Contracts

No `.proto` change: `payout.commands` (`PayoutCommand`) and
`gateway.payout.events` (`GatewayPayoutEvent` with `provider = 7`) are frozen by
CF-2/CF-4.

## 6. Data model and migrations

Migration 00007 `provider_payouts` as in 3; forward only. The mapping is the
second replay barrier behind the inbox: a payout id is processed once, the
provider id is unique per provider. Concurrent deliveries of one payout are
serialized by a `pg_advisory_xact_lock` on the payout id inside the Guard
transaction plus the mapping re-check after the lock, so only one delivery
reaches the provider (review B1). A crash between the provider call and the
commit re-calls the provider: the recovery relies on provider-side dedupe by
the platform payout id used as the wire reference (BTCPay pull-payment name,
CoinGate `request_id`, BitPay `reference`), documented in T-CONNECTOR-19; a
lookup-based recovery is a candidate for 17.13/17.16.

## 7. State machines and rules

The connector mapping has three states (`submitted` -> `confirmed` | `failed`);
only the worker resolves, only from `submitted`, with CAS in the same
transaction as the outbox event.

## 8. Edge cases

E-1. Command replay after submitted: acknowledged, no provider call.
E-2. No capable provider / unsupported network: `failed` event, compensation.
E-3. Provider rejection: `failed` event; the raw provider text stays in the log.
E-4. Temporary failure: error, retry topics; no mapping.
E-5. Status read returns submitted/unknown: touch only, poll again next interval.
E-6. GetPayout returns not found: resolved `failed` (the submitted payout
    disappeared at the provider).
E-7. Concurrent workers: the CAS allows one resolution and one event.
E-8. Amount/asset/decimals/network/address/idempotency key bounds are validated;
    invalid commands go to the DLQ.
E-9. Concurrent deliveries of one payout (different event ids): the advisory
    lock plus the mapping re-check serialize them; one provider call, one event,
    the loser acknowledges (B1).
E-10. A provider verdict with unusable parts does not wedge the payout: the
    reason is rune-safe sanitized (control runes and invalid UTF-8 dropped), an
    unusable txid or fee is skipped with a WARN, and the verdict still resolves.

## 9. Non-functional requirements

- NFR-1. Coverage stays at least 80% unit+integration.
- NFR-2. No new dependencies, no contract change.
- NFR-3. Money stays int64 minor units; no float.
- NFR-4. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| P-01 | submitted | valid command, capable provider | mapping inserted, submitted envelope with provider |
| P-02 | replay | payout already recorded | ok, no provider call, no new event |
| P-03 | rejection | adapter `ErrRejected` | failed envelope, sanitized reason, no mapping |
| P-04 | unsupported | no capable provider | failed envelope |
| P-05 | temporary | adapter `ErrTemporary` | error (retry), no mapping |
| P-06 | invalid | bad id/amount/address/key | invalid command error |
| P-07 | worker resolve | submitted row, provider confirmed | CAS resolves, confirmed envelope once |
| P-08 | worker no-op | provider still submitted / repeated confirm | touch or skip, no duplicate event |

### 10.2. Integration matrix (PostgreSQL)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| I-01 | mapping replay | two inserts of one payout id | second returns false |
| I-02 | pending list | submitted rows with different updated_at | only stale rows returned |
| I-03 | resolve CAS | two resolves | one wins, the second returns false |

### 10.3. Smoke set

`make test`, `make test-integration`, `make test-e2e`, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Connector_PayoutPipeline | testcontainers Kafka/PostgreSQL: a `payout.commands` record produces a `gateway.payout.events` submitted event with the provider; the status worker with a stub confirmed answer produces exactly one confirmed event; a replay produces nothing | `go test -tags e2e ./test/e2e/ -run Payout` |

### 10.5. Load plan

Not applicable (payout volume is far below the payment path; the worker is
bounded by the batch size and the interval).

### 10.6. Test-first evidence

Process fact recorded honestly: the implementation and the tests of this
increment were written in one pass, so the red-first rule was not followed for
it (the unit matrix was executed green only after the implementation, and the
Kafka e2e then caught a helper defect in the poll loop that was fixed). The
review round must account for this deviation; the next increments restore the
red run before the implementation.

Final batteries: unit green (`go test ./...`), the full battery with
integration (`go test -tags integration -coverpkg=./internal/... ./...`) green,
total coverage 86.9% (no regression against 86.6%); the storage integration
suite covers the replay barrier, the pending selection and the resolution CAS;
`go test -tags e2e ./test/e2e/ -run TestE2EPayoutPipeline` green (submitted
event with the provider, one confirmed verdict, a replay without a provider
call or an event; testcontainers Kafka and PostgreSQL); `go vet` and lint
clean; the em dash check passes.

Review rounds (18.10.2026, four reviewers): the first round produced one
blocking finding (B1: the provider call before the replay barrier allowed a
double send under concurrent deliveries) and several major/minor ones; all
were closed on the staged diff: the advisory transaction lock plus the
mapping re-check before the provider call (P-09 asserts the lock order), the
rune-safe sanitization of provider reasons and txid/fee validation that cannot
wedge the resolution (E-10), touching parked rows so they cannot starve the
batch, `os.LookupEnv` semantics for `CONNECTOR_PAYOUT_PROVIDERS` (set-empty
disables) and `CONNECTOR_PAYOUT_POLL_INTERVAL` (explicit zero disables, clamp
to 1s), the capability scale check before the amount bounds with min/max unit
cases, the corrected runbook item about failed events with and without a
mapping row, and the Makefile/E2E.md scenario descriptions. The closure round
confirmed the findings closed with no blockers and three minor observations,
all now closed: a real-PostgreSQL serialization test of the payout advisory
lock (`TestIntegrationProviderPayoutsLockSerializes`), unit coverage of the
sanitization branches (control runes, invalid txid, invalid fee,
`TestPayoutStatusWorkerSanitizesVerdictFields`) and the exact wording of the
disabled-polling log and of a negative interval (journal).

## 11. Work plan and commits

Single commit: spec, migration, storage, processor, worker, main wiring, tests,
docs. The `cbl-payout` provider propagation follows in its repository.
