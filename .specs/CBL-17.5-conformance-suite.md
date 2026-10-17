# CBL-17.5 (cbl-connector part). Adapter conformance suite

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-connector` (this spec) and `cbl-mock-gateway` (companion spec `.specs/CBL-17.5-provider-profiles.md`, the profile wire contract) |
| Size | M |
| Wave and parallel group | W5-PROVIDER |
| Depends on | CBL-17.2 (SPI v1, manifests, provider_invoices), CBL-17.3 (kit money and webhooksig), CBL-17.5 mock part (profiles) |
| Contract version | none (testkit; no `.proto` change) |
| Status | done |

## 1. Context and goal

Original task 3.7 required a contract test suite that any new adapter must pass.
The plan decision 21 and 14.5.8 make the suite a functional extensibility
guarantee: the manifest schema and SPI v1 are versioned (17.2) and every adapter,
including future ones, must be proven against the same suite before onboarding
(steps 2 - 3 and 8 of the checklist 14.4). This task delivers the suite as an
internal testkit plus provider fixtures, proves the suite's efficacy with
meta-tests, and fixes the one missing contract detail it needs: a stable "gateway
payment not found" error class referenced by the `LookupGateway` docstring.

The suite is provider-agnostic: it drives an adapter through its manifest and
fixtures; per-provider data lives only in fixtures, mirroring the mock-gateway
profiles (companion spec section 3.2).

## 2. Scope

In scope: `internal/conformance` testkit (suite, wire mock, fixtures for the five
profiles, signature vectors, strategy builder), the `domain.ErrGatewayNotFound`
class and its transport mapping, meta-tests with compliant and violating test
adapters, and the adapter onboarding note in `AGENTS.md`.

Out of scope: the real adapters (17.6 - 17.9, they consume the suite); the
mock-gateway profiles (companion spec); CI enforcement of "the suite is mandatory"
(17.22 onboarding kit); sandbox runs (17.16).

## 3. Architecture sketch

### 3.1. Packages and layers

`internal/conformance` is a testkit package (Go code without runtime wiring; it is
not imported by `cmd` or other runtime packages). It depends on `internal/domain`,
`cbl-kit/money` and `cbl-kit/webhooksig` only. Adapter packages under
`internal/providers/<provider>` (17.6 - 17.9) will call it from `_test.go` files;
the same-module internal visibility is intentional.

```go
// internal/conformance/conformance.go
// Harness describes an adapter under test.
type Harness struct {
    // Provider is the manifest id; the fixtures must exist for it.
    Provider string
    // New builds the adapter against the wire base URL.
    New func(t *testing.T, baseURL string) domain.PaymentGateway
    // Manifest is the provider manifest the adapter claims to implement.
    Manifest domain.ProviderManifest
}

// CheckResult describes one check outcome; Skipped carries a recorded reason.
type CheckResult struct {
    ID      string
    Skipped bool
    Err     error
}

// Report describes the suite outcome.
type Report struct { Results []CheckResult }

// Run executes the full suite and fails the test on any violation.
func Run(t *testing.T, h Harness) Report

// RunChecks executes the suite without failing the test; the entry point of
// the suite meta-tests. Checks are plain closures whose errors are collected
// into the report; skip reasons (the none strategy, an absent lookup
// interface) are recorded in Skipped results.
func RunChecks(t *testing.T, h Harness) Report
```

```go
// internal/conformance/wire.go
// Wire is the in-test provider emulator built from fixtures: it serves the
// provider routes, records requests and injects failures. The template engine
// understands {{request.<dotted.path>}}; an unresolved placeholder answers 400
// (fixture bug, E-3).
type Wire struct { /* httptest server, route table, request log, failNext */ }
func NewWire(t *testing.T, fixtures ProfileFixtures) *Wire
func (w *Wire) URL() string
func (w *Wire) Close()
func (w *Wire) Requests() []RecordedRequest
func (w *Wire) SetPaymentStatus(raw string) error
func (w *Wire) SetPaymentUnknown()
func (w *Wire) SetLookupFound(found bool)
func (w *Wire) FailNext(status, retryAfterS int) // 429 gets Retry-After
```

```go
// internal/conformance/profiles.go
// StatusCase maps one documented raw status to the expected canonical state.
type StatusCase struct {
    Raw      string
    Expected domain.State
}

// ProfileFixtures describes one provider profile for the suite: route strings
// ("METHOD /path" with {gateway_id}/{order} wildcards), response templates,
// per-status payment payloads, payout and lookup routes, the signing secret and
// the webhook vectors (valid, tampered, missing).
type ProfileFixtures struct {
    Provider         string
    CreatePath       string
    CreateStatus     int
    CreateTemplate   []byte // {{request.<path>}} echoes; unresolved -> 400
    CreateGatewayID  string
    OrderField       string
    PaymentPath      string
    PaymentGatewayID string
    PaymentByStatus  map[string][]byte
    PaymentUnknown   []byte
    LookupPath       string // optional
    PayoutPath       string // optional
    PayoutTemplate   []byte
    PayoutGatewayID  string
    Secret           []byte
    WebhookValid     []byte
    WebhookHeaders   map[string]string
    WebhookTampered  []byte
    TamperedHeaders  map[string]string
    WebhookMissing   []byte // token echo: the body without the token field
    ExpectedEvent    ExpectedEvent
    Statuses         []StatusCase
}

// Fixtures returns the fixture set of a provider.
func Fixtures(provider string) (ProfileFixtures, bool)
```

```go
// internal/conformance/strategy.go
// StrategyFromManifest builds the kit verification strategy declared by the
// manifest; the suite is its first consumer, the defense-in-depth re-check
// (T-CONNECTOR-01) reuses it later.
func StrategyFromManifest(m domain.ProviderManifest) (webhooksig.Strategy, error)

// SignBody signs a webhook body the way the provider does; token echo bodies
// embed the token and have no signable form.
func SignBody(m domain.ProviderManifest, secret, body []byte) (header, value string, err error)
```

Suite checks (subtests CONF-1..CONF-10):

- CONF-1 identity: `Capabilities().Provider` equals the manifest id; the adapter
  for another provider must not masquerade.
- CONF-2 capabilities sanity: at least one network; min > 0; max == 0 or max >=
  min; decimals <= `money.MaxDecimals`.
- CONF-3 create invoice echo: the adapter passes the order reference from the
  request to the wire (`OrderField` recorded), returns a non-empty gateway id
  equal to the wire response (`GatewayInCreate`), echoes network, amount and
  expiry within the request horizon.
- CONF-4 status mapping: for every `StatusCase` the wire get-route answers the
  per-status fixture; `GetPayment` returns the expected canonical state,
  `RawStatus` equal to the raw string, and the gateway id unchanged.
- CONF-5 unknown status: the wire answers `UnknownStatus`; `GetPayment` returns
  an error wrapping `domain.ErrUnknownState` (no fabricated canonical state).
- CONF-6 webhook verification: the valid fixture passes `VerifyWebhook` and
  yields the expected canonical event fields (gateway id, raw status, canonical
  type); the tampered fixture is rejected; the missing-signature probe (no
  headers, `WebhookMissing`: for token echo the body without the token field)
  is rejected; strategies of `none` manifests skip the check with a recorded
  reason.
- CONF-7 operations consistency: manifest operations containing `refund` require
  `domain.Refunder` (asserted); containing `payout` requires a successful
  `CreatePayout` round trip; manifests without a payout operation expect
  `CreatePayout` to fail with `domain.ErrRejected` (feature not offered), not to
  panic or to call the wire.
- CONF-8 lookup semantics: if the adapter implements `domain.LookupGateway`, an
  unknown order must return an error wrapping `domain.ErrGatewayNotFound`; the
  known order returns the fixture status.
- CONF-9 idempotent create: two identical `CreateInvoice` calls return the same
  gateway id and the wire records two requests with the same order reference
  (the provider-side replay is the fixture behavior; the adapter must not mutate
  the reference between calls).
- CONF-10 error classification: with `Wire.FailNext(503, 30)` the adapter returns
  an error wrapping `domain.ErrTemporary` (and must not wrap `ErrRejected`); a
  wire 400 business rejection without a retryable class maps to
  `domain.ErrRejected` (`FailNext(400, 0)` with a rejection fixture).

### 3.2. Fixture data (mirror of the mock profiles)

Fixtures mirror the wire shapes of the companion spec section 3.2 for
`nowpayments`, `coingate`, `bitpay`, `btcpay` and `mock`. Status lists:

- nowpayments: `waiting` -> detected, `confirming` -> detected, `confirmed` ->
  detected, `sending` -> detected, `partially_paid` -> underpaid, `finished` ->
  confirmed, `failed` -> failed, `refunded` -> failed, `expired` -> expired.
- coingate: `new` -> detected, `pending` -> detected, `confirming` -> detected,
  `paid` -> confirmed, `invalid` -> failed, `expired` -> expired, `canceled` ->
  failed, `refunded` -> failed.
- bitpay: `new` -> detected, `paid` -> detected, `confirmed` -> detected,
  `complete` -> confirmed, `expired` -> expired, `invalid` -> failed.
- btcpay: `New` -> detected, `Processing` -> detected, `Settled` -> confirmed,
  `Expired` -> expired, `Invalid` -> failed.
- mock: the canonical aliases of CBL-3.5 (detected/confirmed/failed/expired/
  underpaid/overpaid).

Note: `underpaid`/`overpaid` for CoinGate/BitPay/BTCPay arrive as their
documented statuses with amount evidence (`invalid`/`paid`+`amountPaid`), so the
fixture mapping above is the adapter-visible contract; the payment-domain rules
come from 17.10.

Webhook fixtures per provider reuse the mock recipes: canonical JSON HMAC-SHA512
(NOWPayments), token echo (CoinGate), raw HMAC-SHA256 base64 (BitPay), raw
HMAC-SHA256 hex with the `sha256=` prefix (BTCPay). The payment payloads mirror
the mock profiles field for field; the payout and order-lookup routes are
fixture-side shapes owned by the adapter tasks (17.6 - 17.9) and re-validated
against live APIs by 17.16. The `mock` fixture set covers the sandbox wire
shapes (create, payment, statuses) for documentation; the suite is not run for
it (no adapter implements the sandbox wire; the sandbox decoder path is covered
by the connector e2e), so E-1 records the skip rather than asserting a legacy
hmac vector.

### 3.3. Core changes

`domain.ErrGatewayNotFound` is added (the `LookupGateway` docstring already refers
to "ErrNotFound"): returned when the provider has no invoice for the order (404
responses of real providers). The transport mapper maps it to `codes.NotFound`.

The manifest schema v1 gains two additive fields needed to build the declared
verification strategy without provider branches (mirror of the ingress
configuration of 17.4): `verify_encoding` (`hex` default, `base64` of BitPay) and
`verify_prefix` (optional, `sha256=` of BTCPay; raw_hmac only, bounded). The
built-in BitPay and BTCPay manifests set them; validation covers unknown
encodings and misplaced prefixes. Adding the fields now is a core change (core
tasks own the core); provider PRs still do not touch it (14.5.8).

### 3.4. Suite efficacy (meta-tests)

`conformance_test.go` proves the suite detects violations using a small generic
test adapter over the `nowpayments` fixtures (JSON HTTP client, no provider
branches in the suite itself) and mutants:

- M-1 (compliant) passes every check.
- M-2 create returns an empty gateway id -> CONF-3 fails, others pass.
- M-3 unknown status mapped to detected -> CONF-5 fails.
- M-4 valid signature accepted but tampered accepted too -> CONF-6 fails.
- M-5 manifest declares `refund`, adapter does not implement `Refunder` ->
  CONF-7 fails.
- M-6 lookup returns a nil error for an unknown order -> CONF-8 fails.
- M-7 503 mapped to `ErrRejected` -> CONF-10 fails.
- M-8 second create mutates the order reference -> CONF-9 fails.

Each mutant test asserts that exactly the named check fails, so the suite cannot
silently pass everything.

### 3.5. Data flows

Adapter test (17.6 - 17.9): `conformance.Run(t, Harness{Provider, Manifest, New})`
-> the suite starts `Wire` from fixtures -> builds the adapter with `Wire.URL()`
-> runs CONF-1..CONF-10 -> the wire records every request for the assertions.

Fixture signature tests: `Fixtures(provider)` -> `SignBody(manifest, secret,
body)` (or the precomputed headers) -> the built strategy verifies the valid
vector and rejects the tampered one.

### 3.6. Decisions and rejected alternatives

- D1. The suite lives in `internal/conformance` instead of `_test` packages: it
  is shared code across adapter test packages; internal visibility keeps it out
  of the public API.
- D2. Fixtures mirror (not import) the mock-gateway profiles: cross-repository
  code reuse is impossible. Meta-tests and signature/shape tests are
  self-referential and cannot detect cross-repository drift; that duty stays
  with the mock side e2e (the mock generates the shapes) and with the 17.16
  sandbox runs against live APIs (T-CONF-01).
- D3. The wire mock is data-driven (route strings and templates in fixtures, one
  generic server): no provider branches in the suite (14.5.2 mirror).
- D4. `ErrGatewayNotFound` and the additive manifest fields `verify_encoding`/
  `verify_prefix` are core changes owned by this core task; the mapper gains a
  NotFound row. The manifest fields have no runtime consumer yet: the
  defense-in-depth re-check (T-CONNECTOR-01) will consume `StrategyFromManifest`
  when it lands, staying within the same core mapping.
- Rejected: gRPC-level conformance (the SPI is Go interfaces; transport mapping
  is covered by 17.2 tests); recording real provider responses in fixtures now
  (sandbox runs 17.16 record them later; today's fixtures document shapes).

## 4. Functional requirements

FR-1. `conformance.Run` executes CONF-1..CONF-10 against any `Harness` and
reports every failure with the offending fixture and check id.

FR-2. `Fixtures(provider)` exists for the five profiles and mirrors the wire
shapes and status tables of the companion spec.

FR-3. `StrategyFromManifest` and `SignBody` map the manifest strategy fields to
the kit `webhooksig` strategies: `raw_hmac` (encoding and prefix from the
manifest extension fields), `canonical_json_hmac`, `token_echo`, `none`.

FR-4. `domain.ErrGatewayNotFound` exists and maps to `codes.NotFound`; the
`LookupGateway` docstring references it.

FR-5. Meta-tests prove suite efficacy per section 3.4; the compliant adapter
passes.

FR-6. Adapter onboarding documentation: the connector `AGENTS.md` states that the
conformance suite is mandatory for every adapter (including future ones) and how
to run it; the suite is referenced by 17.22 and the checklist 14.4.

## 5. Contracts

No `.proto`/Kafka changes. The manifest schema is extended additively
(`verify_encoding`, `verify_prefix`, section 3.3); `StrategyFromManifest` reads
them. The gRPC mappings stay unchanged, plus the new
`ErrGatewayNotFound -> NotFound` row.

## 6. Data model and migrations

None.

## 7. State machines and rules

None (testkit).

## 8. Edge cases

E-1. Manifest with `verify_strategy: none` (the mock profile): CONF-6 is skipped
with a recorded reason; the sandbox wire shapes stay a documentation fixture
(E-1 note in 3.2).

E-2. Adapter that implements no optional interfaces: CONF-7 requires only what
the manifest declares; CONF-8 is skipped with a recorded reason.

E-3. Wire template with an absent `{{request.<path>}}` value: the wire answers
400 and the check fails with a clear message (fixture bug, not adapter bug).
The template engine deliberately supports only the request echo; `{{seq}}` and
`{{uuid}}` were dropped from the sketch as unused by the fixture catalogue.

E-4. `FailNext(400, 0)` rejects the next business call; the fixture body must be
a provider-shaped rejection for the adapter to classify it as `ErrRejected`.

E-5. An adapter returning a canonical state for `UnknownStatus` fails CONF-5
even when it also returns an error (no silent fabrication is tolerated: the
error must wrap `ErrUnknownState`).

## 9. Non-functional requirements

- NFR-1: the suite runs in-process (httptest), no Docker, no network, under a few
  seconds for the fixture set.
- NFR-2: coverage: the `conformance` package is covered by meta-tests; the
  repository coverage stays at least 80%.
- NFR-3: no new dependencies (stdlib, testify, kit).
- NFR-4: English only, no em dash; godoc docstrings for every exported entity.

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| CU-01 | fixtures exist | Fixtures for the five providers | ok, shape fields non-empty |
| CU-02 | status coverage | each provider's Statuses | unique raw values, expected states in the canonical set |
| CU-03 | signature vectors | SignBody + StrategyFromManifest per provider | valid verifies; tampered fails |
| CU-04 | strategy mapping | manifests of BuiltinManifests | strategy per manifest, `none` -> error/skip |
| CU-05 | template rendering | route with `{{request.order_id}}`, `{{seq}}`, `{{uuid}}` | rendered response, missing path -> 400 |
| CU-06 | wire recording | two create calls | two recorded requests in order |
| CU-07 | FailNext | 503 then normal | first fails, second succeeds |
| CU-08 | ErrGatewayNotFound mapping | mapper | codes.NotFound |

### 10.2. Meta-test matrix (suite behavior)

| ID | Mutant | Expected |
| :-- | :-- | :-- |
| CM-1 | compliant adapter | all CONF checks pass |
| CM-2 | empty gateway id | CONF-3 fails, CONF-1/2 pass |
| CM-3 | unknown -> detected | CONF-5 fails |
| CM-4 | tampered accepted | CONF-6 fails |
| CM-5 | missing Refunder | CONF-7 fails |
| CM-6 | nil error on unknown order | CONF-8 fails |
| CM-7 | 503 as ErrRejected | CONF-10 fails |
| CM-8 | mutated order reference | CONF-9 fails |

### 10.3. Smoke set

`make build`, `make test`, `make test-integration` (existing), `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Connector_Smoke (existing) | compose stand smoke unchanged; a regression run is part of this task | `test/e2e/E2E.md` |
| E2E_Connector_Normalizer (existing, tag `e2e`) | normalizer end-to-end on testcontainers; a regression run proves the domain addition (ErrGatewayNotFound) does not affect the pipeline | `make test-e2e` |
| ConformanceDogfood (in-process, part of `make test`) | the suite runs against the template adapter on every unit run (meta-tests), so suite regressions fail CI | `go test ./internal/conformance/...` |

There is no new compose scenario: the suite is an in-process testkit; the
end-to-end provider coverage arrives with the real adapters (17.6 - 17.9) and the
sandbox harness (17.16).

### 10.5. Load plan

Not applicable (no runtime change; the domain error class adds no work to the
money path).

### 10.6. Test-first evidence

Red run recorded before implementation (repository `cbl-connector`,
17.10.2026): `go test ./...` failed with
`internal/conformance: no non-test Go files` and
`undefined: domain.ErrGatewayNotFound`; the existing suite stayed green
(`domain`, `gateway`, `usecase` ok). Review closure round: findings of the four
reviewers closed (CONF-6 token-echo missing-signature fixture and focused stub
test, template 400 on unresolved placeholders, strengthened CONF-3/5/7/10
assertions, manifest encoding gate, mutant exclusivity, documentation and spec
alignment).

## 11. Work plan and commit

Single commit in `cbl-connector`: spec, `internal/conformance` (suite, wire,
fixtures, strategy), `domain.ErrGatewayNotFound` and mapper, meta-tests,
`AGENTS.md`, `README.md`. The umbrella `CBL-004` status row is updated in the
umbrella commit after both repositories land.
