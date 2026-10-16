# CBL-3.3. Resilience: timeouts, retries with backoff, circuit breaker, bulkhead

| Field | Value |
| :-- | :-- |
| Epic | E3, Connector |
| Repository | `cbl-connector` |
| Size | M |
| Wave and parallel group | W1-CONNECTOR |
| Depends on | 3.1 |
| Contract version | `cbl-protos@cf-1` (unchanged) |
| Status | done, review closed |

## 1. Context and goal

The external gateway is the only uncontrolled segment of the money loop (`CBL-000`,
T-01): Payment calls Connector with a 5 s deadline, and Connector calls the provider. Task 3.3
wraps the `domain.PaymentGateway` port in a resilience decorator: per-attempt
timeouts, retries only for safe calls with exponential backoff and jitter,
a per-provider circuit breaker and bulkhead concurrency limits. The real provider
adapter (3.2) remains behind KI-01; the decorator is wired by the router in 3.4.

## 2. Boundaries

In scope: the `internal/gateway` package with the `Resilient` decorator (implements
`domain.PaymentGateway`), provider error classification (temporary/permanent),
configuration from env, unit tests with deterministic time.
Out of scope: provider adapters and the HTTP client (3.2), the router and Redis configuration (3.4),
adapter contract tests (3.7), Prometheus metrics (0.7), the shared Redis breaker
state (ADR-3.3, T-CONNECTOR-06).

## 3. Functional requirements

FR-1. The decorator wraps provider calls in the order: bulkhead -> retry -> circuit
breaker -> per-attempt timeout -> adapter. `VerifyWebhook` and `Capabilities`
are passed through directly (local operations without the network; the signature is verified
against the raw body).

FR-2. Attempt timeouts follow the `CBL-003` budget: `CreateInvoice` 2 s, `GetPayment` 1 s,
`CreatePayout` 2 s, `GetPayout` 1 s. `Capabilities` has no context in the port and
is passed through directly (the 200 ms from `CBL-003` is the budget of the adapter
`GetCapabilities` of task 3.2). The budget is honestly distributed as follows: on timeout
failures, two full attempts are guaranteed to fit into the RPC deadline, and the
third runs on the remaining budget of the caller; when the deadline is exhausted,
a context error (`context.DeadlineExceeded`) is returned, which the transport maps to
`UNAVAILABLE`. An attempt timeout is a temporary error; the caller's common context is
always respected (cancellation and deadline stop retries).

FR-3. Retries: only for temporary errors (`domain.ErrTemporary`, attempt timeout,
adapter network failures) and only for idempotent calls (mutating methods carry
`idempotency_key`; an empty key is a fail-fast `ErrInvalidGatewayRequest` without contacting
the provider, so that a retry does not create a second invoice or payout). Permanent errors
(`domain.ErrRejected`, validation) and an open breaker are not retried; unknown errors
are treated as permanent (fail-fast). Parameters: up to 3 attempts, base 100 ms, multiplier 1.5
(fixed explicitly), cap 1 s, jitter 50% (`cenkalti/backoff/v4`, `MaxElapsedTime=0`,
stop by attempt count). Exhausted attempts - `ErrProviderUnavailable` with the cause
preserved.

FR-4. Circuit breaker (`sony/gobreaker`, in-process, per provider): opens after 5
consecutive provider failures, open time 30 s, in half-open 1 trial call is allowed;
in the open state calls are rejected instantly (`ErrProviderUnavailable`) without contacting
the provider; a success in half-open closes the breaker, a failed trial opens it again.
Only temporary failures and attempt timeouts count as provider failures; business rejections
(`ErrRejected`, `ErrInvalidRoute`, `ErrUnsupportedNetwork`, `ErrInvalidGatewayRequest`,
`ErrUnknownState`) and caller cancellation are not counted - a stream of rejected addresses
must not open the breaker for all merchants; unknown errors count as failures
(conservatively). State transitions are logged (warn).

FR-5. Bulkhead: no more than 32 concurrent logical provider calls (the slot is held for
all attempts of one operation); when exhausted - an instant rejection `ErrProviderOverloaded`
without waiting. The slot is always released, including on an adapter panic (the panic is
propagated outward; the port contract does not intercept it).

FR-6. Error classification: adapters (3.2/3.7) must wrap temporary failures with
`domain.ErrTemporary` and business-level rejections with `domain.ErrRejected`; unknown errors in
the decorator are treated as permanent and are not retried. Adapters must sanitize error
text (no URLs with keys, no provider response headers or bodies): errors end up in retry
logs. Observation: every retry is logged (zap warn: operation, attempt, delay,
cause), breaker transitions - warn.

FR-7. Configuration: defaults are sensible without env; overrides via
`CONNECTOR_RETRY_MAX_ATTEMPTS`, `CONNECTOR_RETRY_BASE_DELAY`, `CONNECTOR_RETRY_MAX_DELAY`,
`CONNECTOR_BREAKER_FAILURE_THRESHOLD`, `CONNECTOR_BREAKER_TIMEOUT`,
`CONNECTOR_BREAKER_HALF_OPEN_REQUESTS`, `CONNECTOR_BULKHEAD_MAX_CONCURRENT`,
`CONNECTOR_PROVIDER_ATTEMPT_TIMEOUT` (overrides the timeouts of FR-2; above the RPC budget
is the operator's responsibility). Several layers of protection: invalid values are ignored;
after parsing, the configuration is normalized - zeros are replaced with defaults,
values are clamped from above (`MaxAttempts` 10, delay 30 s, breaker threshold 100,
breaker timeout 1 h, half-open 16, bulkhead 1024, attempt timeout 30 s; base not above the
delay cap), so that "valid but insane" env values do not disable protection.

## 4. Contracts

External contracts do not change: the decorator implements the existing port
`domain.PaymentGateway` (CF-1), RPC deadlines - `CBL-003` (CreateInvoice 5 s,
GetGatewayPayment 3 s, GetCapabilities 200 ms).

Classification contract for adapters (3.2/3.7):

| Provider response | Classification | Decorator behavior |
| :-- | :-- | :-- |
| network failure, timeout, 408/429, 5xx | `domain.ErrTemporary` (`%w`, the cause is preserved) | retry per FR-3 |
| business rejection 4xx (address rejected, limit) | `domain.ErrRejected` | fail-fast, the breaker does not open |
| 401/403 and other unknown | without wrapping | fail-fast, the breaker counts a failure |

gRPC mapping (transport 3.4): `ErrProviderUnavailable` -> `UNAVAILABLE`,
`ErrProviderOverloaded` -> `RESOURCE_EXHAUSTED`, `ErrRejected` -> `FAILED_PRECONDITION`,
`ErrTemporary` (after attempts are exhausted it is already wrapped into `ErrProviderUnavailable`) ->
`UNAVAILABLE`, caller deadline -> `DEADLINE_EXCEEDED`.

## 5. Data model and migrations

Not applicable: the schema does not change. Breaker state is in-process (ADR-3.3);
the Redis key `cb:{provider}` from `CBL-003` (section 7) is not used until a shared
state requirement appears (T-CONNECTOR-06, KI-connector-07).

## 6. State machines

Circuit breaker: closed -> open (consecutive failure threshold) -> half-open (by timeout)
-> closed (successful trial) | open (failed trial). The states are standard gobreaker,
transitions are not extended.

## 7. Edge cases

Success on the first attempt; a temporary error then success; a permanent error without retries;
an unknown error fail-fast; all attempts temporary (exhaustion -> `ErrProviderUnavailable`
with the cause); an attempt timeout is retried; caller cancellation and deadline stop retries;
the breaker opens and rejects without calling the provider; business rejections and cancellation
do not open the breaker; half-open recovers; half-open fails again (the breaker opens again);
the bulkhead is full (the second call is rejected instantly); the bulkhead slot is released
after adapter errors and panic; an empty `idempotency_key` on mutating calls - fail-fast without
contacting the provider; an empty provider name; zero jitter (tests) and full jitter; zero and
extreme config (FR-7 clamps).

## 8. Non-functional requirements

Decorator overhead on a successful call is a few microseconds (one select and wrappers);
memory - O(1). Deterministic unit tests: the backoff clock is injected, breaker tests
use short timeouts; sleeps in tests are limited to tens of milliseconds. Package coverage
not below 85% (the critical path of the money loop).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | `internal/gateway`: all edge cases of section 7 on a fake provider; deterministic retry delays (recording timer, fixed multiplier 1.5); business rejections do not open the breaker; half-open rejection; slot on panic; empty idempotency key; configuration clamps; breaker transition log (observer); the caller deadline limits attempts; configuration from env and defaults |
| Integration | not required at this layer: the decorator has no external dependencies; the adapter e2e loop appears in 3.2 (mock-gateway scenarios `fail_next`/`latency_ms`) and 3.7 |
| Smoke | `go build ./...`; compose smoke runs follow the `test/e2e/E2E.md` instruction (scenario E2E_Connector_Smoke, compose stand) without changes (the decorator is wired in 3.4) |
| Load | not extended (3.3 does not change the throughput of the stand) |

## 10. Work plan and commit

Commit `CBL-3.3: provider call resilience`; the spec is part of the commit; AGENTS
(trade-offs T-CONNECTOR-06/07, journal) and `CBL-004` are updated.

## 11. Open questions and decisions

- ADR-3.3: circuit breaker in-process (gobreaker per provider), the shared Redis state
  `cb:{provider}` is not implemented until a multi-instance consistency requirement appears
  (T-CONNECTOR-06, KI-connector-07): the key is reserved and is not a source of truth;
  the cost of in-process is that with I replicas the actual open threshold grows proportionally and
  half-open trials multiply, which is accepted until the consolidation task.
- ADR: only temporary failures and attempt timeouts count as breaker failures; business rejections
  and caller cancellation do not open the breaker (otherwise a stream of rejected addresses or caller
  bugs blocks a healthy provider for all merchants); unknown errors are
  conservatively failures.
- ADR: retries only for classified temporary errors; an empty `idempotency_key`
  on mutating calls is a fail-fast: the decorator is the last control point before a retry
  that could otherwise create a second invoice or a double payout.
- ADR: the bulkhead holds the slot for the whole operation (including backoff pauses): the
  per-provider limit is more important than the throughput of a single request; rejection is
  instant, without a wait queue (otherwise caller timeouts pile up). An adapter panic is
  propagated outward, the slot is released via defer.
- ADR: the configuration is normalized and clamped from above (FR-7): "valid but insane"
  env values must not disable provider protection.
- Open question: adapter e2e with mock-gateway (`fail_next`, `latency_ms` from 3.6) and
  contract tests - tasks 3.2 and 3.7 (blocked by KI-01).
- Open question: one `Resilient` instance per provider is created by router 3.4 when
  building the routing table (the dependency of 3.4 on 3.3 is fixed in `CBL-004`).

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). Blocker: business rejections and caller cancellation
opened the breaker for all merchants - closed by the failure predicate. Major: the attempt
budget, wiring in 3.4, step 7 loose ends, the missing open log - closed; go.mod brought to
tidy.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | FR-6: no warn on open; no tests for half-open rejection, slot on panic, empty name, full jitter; dead Capabilities constant; go.mod not tidy; the breaker counts business rejections; no config normalization | `OnStateChange` + warn + observer test; tests added (half-open rejection, panic+slot, default jitter, empty key, clamps, caller deadline); the constant removed, FR-2 clarified; `go mod tidy`; the failure predicate + the test "5 ErrRejected do not open"; `normalizeConfig` with clamps |
| Architecture | major: the breaker failure predicate; major: honest budget wording (2 full attempts, the 3rd on the remainder); major: the dependency of 3.4 on 3.3 and the singleton are not fixed; the `cb:{provider}` discrepancy; the classification contract is incomplete; dead constant; tidy; release masks errors | predicate + tests; FR-2 rewritten (caller deadline -> `DeadlineExceeded`); `CBL-004` 3.4 depends on 3.3 and fixes the singleton; CBL-003/CBL-000 mark the key as reserved (T-CONNECTOR-06, KI-connector-07); in §4 the classification table and the gRPC mapping; the constant removed; tidy; `release` without default |
| With-context | FR-6 log; breaker predicate; go.mod indirect; AGENTS/CBL-004 loose ends; dead constant; test gaps; the multiplier 1.5 not fixed; env junk not covered enough | log + test; predicate + test; tidy; AGENTS updated by this commit (T-CONNECTOR-06/07, KI-07, journal, env); the constant removed; tests extended (including payout methods and normalization); the multiplier asserted; env junk extended |
| Security | BLOCKER: cross-tenant DoS via opening the breaker with business rejections; major: no fail-fast on an empty `idempotency_key` (double payout risk); major: no config upper bounds; major: no open warn; minor: panic and the slot (confirmed - the slot is released), error sanitization in logs, tests, tidy, New without normalization | the failure predicate (a failure = availability only) + test; fail-fast `ErrInvalidGatewayRequest` before calling the provider + CreateInvoice/CreatePayout tests; `normalizeConfig` (clamps) + test; `OnStateChange` warn + test; panic test; the error sanitization requirement in FR-6; tidy; normalization in `New` |
