# CBL-3.5. Webhook normalizer into canonical events

| Field | Value |
| :-- | :-- |
| Epic | E3, Gateway Connector |
| Repository | `cbl-connector` |
| Size | M |
| Wave and parallel group | W1-CONNECTOR |
| Depends on | 3.1, 1.3 |
| Contract version | `cbl-protos@cf-1` (`billing/events/v1`) |
| Status | done (review closed, commit CBL-3.5) |

## 1. Context and goal

The asynchronous payment intake loop (`CBL-000`, section 4): Webhook Ingress publishes the
raw webhook to `gateway.webhooks.raw`, Connector normalizes it into `gateway.events.canonical`
for Payment (4.3) and Reconciler (9.1). The task builds the normalization pipeline: an
idempotent consumer (inbox 1.3), a provider decoder port, publication via outbox. Real
provider decoders will arrive with the adapters (3.2, 10.3); until the provider is selected
(KI-01) a universal sandbox decoder for the mock gateway works (3.6).

## 2. Boundaries

In scope: consumer of `gateway.webhooks.raw` (+ retry topics), the `WebhookDecoder` port and
registry, sandbox JSON decoder, canonical event validation, publication via outbox,
outbox/inbox migrations, wiring in main, an end-to-end test on real Kafka and PostgreSQL.

Out of scope: real provider adapters (3.2), router (3.4), mock-gateway as a service
(3.6, implements the webhook format), payout normalization (10.3), Prometheus metrics (0.7).

## 3. Functional requirements

FR-1. Consumer: the topic `gateway.webhooks.raw` and the redelivery topics
`.retry.5s/.retry.1m/.retry.10m` (kit Router), consumer group `KAFKA_CONSUMER_GROUP`
(default `cbl-connector-webhooks`), manual offset commit after successful processing
(`kafkaconsumer`), deduplication by `(consumer, event_id)` via inbox. A processing error
moves the message to a retry topic, and after attempts are exhausted - to `.dlq` with the
reason in the `x-error` header (kit Processor behavior). A message without `event_id` goes
to DLQ.

FR-2. Normalization handler (`inbox.Handler`, executed in the guard transaction):
- rejects an envelope larger than 512 KB and a webhook body larger than 256 KB before
  parsing: untrusted input must not inflate retry/DLQ messages and block the partition
  (poison pill);
- parses `GatewayWebhookRaw` from the payload (parse error - `ErrInvalidWebhook`);
- checks `signature_verified`: false - `ErrUnsignedWebhook` (to DLQ after retries);
  the boundary with Webhook Ingress is fixed in T-CONNECTOR-01: Ingress verifies the
  signature, Connector trusts the flag;
- looks up the decoder by `provider` in the registry; unknown provider - `ErrUnknownProvider`;
- `decoder.Decode` returns `domain.CanonicalEvent`; the event is validated:
  `payment_id` - UUID, `gateway_id` and `provider` - non-empty printable ASCII up to 128/64,
  a known canonical type, non-zero `occurred_at`, `txid` up to 256 (empty allowed),
  `network` up to 64 from the code charset (empty allowed), `received_amount` with an asset
  of 2 - 16 charset characters, decimals no more than 18, a non-negative amount; errors do
  not interpolate untrusted values (the error text and `x-error` stay bounded);
- builds `CanonicalPaymentEvent` (`billing/events/v1`) and the outbox envelope:
  topic `gateway.events.canonical`, key `payment_id`, event_type `gateway.payment.<type>`,
  producer headers `cbl-connector`, schema_version `v1`, `provider`, `gateway_id` and
  passthrough of `traceparent` from the incoming envelope only with a valid W3C format;
  `occurred_at` is the event time at the provider;
- writes the envelope to outbox in the same transaction as the inbox record: the event is
  not published without the processing mark and vice versa.

FR-3. Decoder port and registry:

```go
type WebhookDecoder interface {
    Provider() string
    Decode(ctx context.Context, raw *eventsv1.GatewayWebhookRaw) (domain.CanonicalEvent, error)
}
```

The registry is built from the list of decoders, a duplicate provider is a configuration
error; `Decoder(provider)` returns the decoder and a presence flag. The port covers payment
webhooks: adapters 3.2 implement it, for payout events (10.3) a separate port/pipeline will
appear; the registry does not depend on specific providers.

FR-4. Sandbox decoder (until KI-01): the webhook body is the JSON of the contract:

```json
{
  "payment_id": "01a138...", "gateway_id": "inv-1",
  "status": "confirmed",
  "received_amount": {"minor_units": 100000, "asset": "USDT", "decimals": 2},
  "confirmations": 3, "txid": "0x...", "network": "trc20",
  "event_ts": "2026-10-14T12:00:00Z"
}
```

`status` is translated via `domain.CanonicalState` (aliases of provider strings 3.1);
an empty status or one longer than 32 characters - `ErrInvalidWebhook`, an unknown non-empty
one - `ErrUnknownState`; `payment_id` must be a UUID; missing required fields, a negative
amount, `event_ts` longer than 64 characters or unparsable - `ErrInvalidWebhook`
(the error text does not contain the input value). Providers are enabled by the env
`CONNECTOR_SANDBOX_PROVIDERS` (default `mock`, an empty list refuses startup);
the format is fixed and used by mock-gateway (3.6) as the sandbox provider contract.

FR-5. Domain errors: `ErrUnsignedWebhook`, `ErrUnknownProvider`, `ErrInvalidWebhook`
(complement 3.1); mapping to gRPC is not required: the pipeline is asynchronous, the error
class determines retry/DLQ via inbox.

FR-6. Wiring (main): `PG_DSN` is mandatory (inbox/outbox), migrations via `storagex`;
with a non-empty `KAFKA_BROKERS` the outbox relay (kit) and the normalization consumer
start; without Kafka the service works without the pipeline (WARN), as in 5.1. Shutdown on
SIGTERM: context cancellation, closing the consumer and the publisher.

FR-7. E2E: the end-to-end scenario (raw webhook to Kafka -> canonical to Kafka,
deduplication, DLQ for unsigned) runs on testcontainers with real Kafka and PostgreSQL
(build tag `e2e`, precedent CBL-5.1 section 9); compose smoke (run instruction
`test/e2e/E2E.md`, scenario E2E_Connector_Smoke, compose stand): migrations applied,
healthz ok, consumer started; topics are created by the one-shot `kafka-init` service in
compose (auto-create is disabled).

## 4. Contracts

- Kafka: `gateway.webhooks.raw` (input, key `provider+gateway_id`, payload
  `GatewayWebhookRaw`), `gateway.events.canonical` (output, key `payment_id`, payload
  `CanonicalPaymentEvent`); envelope headers from `CBL-003` (section 6); kit retry/DLQ
  suffixes. `cbl-protos@cf-1` unchanged.
- PostgreSQL `connector`: tables `outbox`, `inbox` (reference DDL from `cbl-kit`).
- gRPC/HTTP: unchanged (ConnectorService - 3.2/3.4).

## 5. Data model and migrations

`migrations/00001_outbox.sql`, `migrations/00002_inbox.sql` - copies of the reference DDL
from `cbl-kit/migrations` (`CBL-003`, section 7). The schema of adapters and routes - 3.4.

## 6. State machines and rules

Payment drives the payment states (4.3); the normalizer does not filter repeated statuses
and does not reorder events: it passes through the provider `gateway_event_ts`, while order
control (a watermark by `gateway_event_ts` per payment) is Payment's responsibility
(T-CONNECTOR-05, the requirement is mirrored in spec 4.3). Deduplication of redelivery of
one raw event is done by inbox by `event_id` (a deterministic UUIDv5 from 5.1), so one raw
event yields exactly one canonical envelope.

## 7. Edge cases

A duplicate raw event (the same `event_id`) - skipped without publication; an unsigned
webhook - DLQ; an unknown provider, an unknown status, broken protobuf/JSON, an empty or
non-UUID `payment_id` - retry, then DLQ; a poison pill (giant `status`/`event_ts`/body, an
envelope larger than 512 KB) is rejected with a short error before parsing, the retry
message remains deliverable and goes to DLQ, the partition is not blocked; `event_ts` in
the future (accepted, Payment decides the order); an empty `txid` at detected; zero
confirmations; repeated publication by the relay (at-least-once, Payment deduplicates);
assets and amounts outside the limits are rejected by validation (asset 2 - 16, decimals
up to 18, minor units non-negative).

## 8. Non-functional requirements

- At-least-once without loss: offsets are committed after processing; publication only via
  outbox (no dual-write).
- Processing one event makes no network calls and fits into single-digit milliseconds
  (no provider calls); budget and lag - 14.1.
- Logs do not contain raw webhook bodies and secrets; provider/gateway_id/payment_id in
  logs are allowed.
- Unit+integration coverage is at least 80%; money scenarios - on real PostgreSQL and
  Kafka (testcontainers).

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Smoke | `go build ./...`; compose: healthz, migrations, consumer start (run instruction `test/e2e/E2E.md`, scenario E2E_Connector_Smoke, compose stand) |
| Unit | registry (duplicate, lookup, absence), sandbox decoder (valid JSON, status aliases, broken JSON/ts, empty payment_id, negative amount), event validation |
| Integration | testcontainers PostgreSQL: handler via inbox.Processor: canonical envelope in outbox, duplicate is not published, unsigned/unknown/broken - error for retry |
| E2E | testcontainers Kafka+PostgreSQL (tag `e2e`): raw -> canonical, deduplication, DLQ of unsigned |
| Load | 14.1 |

## 10. Work plan and commit

1. Spec, tests (unit + integration + e2e), then implementation: domain errors, registry,
   normalizer, sandbox decoder, migrations, wiring, compose, smoke script.
2. Four review passes, AGENTS.md, linters, final runs.
3. Commit `CBL-3.5: webhook normalizer into canonical events`; the spec in the same commit.

## 11. Open questions and decisions

- ADR: until the provider is selected (KI-01) the registry is populated with the sandbox
  decoder; real decoders are part of adapters 3.2, the pipeline does not depend on them.
- ADR: an event without `signature_verified=true` is not normalized: an unsigned webhook
  must not trigger money transitions; DLQ keeps the message for analysis.
- ADR: untrusted input is bounded before parsing (envelope 512 KB, body 256 KB, field
  lengths and charset, UUID payment_id): a poison pill does not block the partition,
  x-error and logs are bounded; kit hardening (x-error truncation, parking, retry delays) -
  KI-connector-05.
- ADR: the envelope `producer` is not checked (KI-connector-06): the trust boundary is the
  ACL for writing to `gateway.webhooks.raw` in task 0.5; the risk is recorded.
- Open question: the sandbox webhook format is fixed here; 3.6 implements sending,
  discrepancies are allowed only via a spec update.
- Open question: real-provider mapping gateway_id -> payment_id in the adapter (3.2) may
  differ from sandbox (payment_id in the body); the port contract allows this.
- Open question: Schema Registry is not connected (platform backlog 0.5): events are
  published as plain protobuf, as in 5.1; alignment with Payment (4.3) - W2 wave input.
- Accepted with a limitation: with several replicas each runs its own relay; the order of
  events of one payment may be broken, Payment deduplicates and controls the order
  (T-CONNECTOR-05); one relay per outbox - a 14.2 candidate.

## 12. Review (CBL-001, step 6)

Four independent passes (14.10.2026). The security blocker (a poison pill with giant
`status`/`event_ts` blocked the partition with endless redelivery) is closed with untrusted
input limits and error sanitization; e2e is extended with a poison-pill scenario. All
majors and minors are closed or recorded.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| Without context | no asset length limit; deduplication was tested only with a negative expectation; future event_ts/empty txid/broken protobuf are not covered; smoke without a positive migration check; the journal was ahead of the review; an empty provider list; a nil registry | field limits and charset in Validate; e2e asserts the outbox/inbox count in the DB; cases added (empty status, long fields, poison); smoke checks `successfully migrated`; statuses synchronized; an empty list refuses startup; the registry requirement is documented |
| Architecture | occurred_at monotonicity is not implemented; retry without delay dumps the backlog into DLQ; os.Exit and graceful shutdown; readyz always 200; a payout port in the decoder; several relays and ordering; a missing event_type breaks retry; Schema Registry | T-CONNECTOR-05: ordering is Payment's responsibility (4.3); T-CONNECTOR-04 reformulated honestly, delays/parking - 14.2; exit/shutdown/readyz - platform tasks 0.7/14.4; FR-3 is limited to payment webhooks, payout port - 10.3; the relay limitation is recorded; kit findings in KI-connector-05; SR - 0.5 |
| With context | the trade-offs header did not match the rows; KI-connector-04 is inaccurate; a duplicate package comment; `%v` instead of `%w`; empty status = ErrUnknownState; the global compose did not wait for kafka-init | the table is reduced to 5 columns; KI refined; the comment removed; `%w`; empty status - ErrInvalidWebhook; the global compose waits for kafka-init |
| Security (BLOCK lifted) | the poison pill blocks the partition (giant fields in x-error); no field/format limits for payment_id; Kafka PLAINTEXT without ACL trusts signature_verified; traceparent without validation; DLQ retention | envelope/body/field limits, UUID payment_id, short errors without values; the e2e poison-pill scenario confirms unblocking; KI-connector-06 (ACL/SASL - 0.5); W3C traceparent validation; KI-connector-04 (retention) |
