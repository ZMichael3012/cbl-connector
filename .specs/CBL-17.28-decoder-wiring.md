# CBL-17.28 (cbl-connector). Decoder wiring: allow fetch-mandatory-only deployments

Status: DRAFT (scheduled, not started). The full architecture sketch and the test
matrices are completed at task start per CBL-001.

## 1. Context

`cmd/connector/main.go` builds the webhook decoder registry from
`CONNECTOR_SANDBOX_PROVIDERS` (default `mock`) and refuses to start when the list
is empty:

```
if len(decoders) == 0 {
    log.Error("CONNECTOR_SANDBOX_PROVIDERS is empty: no webhook decoders configured")
    return
}
```

The normalizer uses a decoder only for the non-fetch-mandatory path
(`internal/usecase/normalizer.go`: `fetch_required` or `fetch_mandatory` ->
`fetchCanonical`, otherwise `decodeCanonical`, which requires a decoder). Every
real provider manifest (`coingate`, `nowpayments`, `bitpay`, `btcpay`) has
`FetchMandatory: true`; only `mock` is a sandbox and uses the decoder. So a
production deployment with real providers needs no decoder, yet the startup guard
rejects an empty set. The default `mock` masks the guard, and
`NewSandboxDecoder` accepts any provider name (for example `coingate`), which
suggests a decoder that is never used.

## 2. Goal

Make the startup decision match the actual runtime need, without weakening the
fail-closed behavior of the decode path:

- an empty decoder set is allowed when no enabled provider needs the decode path;
- a non-fetch-mandatory delivery without a decoder still fails closed (retry/DLQ)
  with an explicit error;
- the sandbox decoder names are constrained to the known sandbox set;
- the env/AGENTS documentation states the real meaning of the variable.

## 3. Scope

In scope: `cmd/connector/main.go` startup validation, the decoder registry
construction (optionally extracted into a testable decision helper),
`internal/usecase/sandbox_decoder.go` name validation, the unit tests of the
startup matrix and the normalizer path, the AGENTS/env docs, the stand e2e for
both configurations.

Out of scope: provider adapters implementing `WebhookDecoder` (not needed while
`fetch_mandatory` holds), the mock pipeline contract, contract changes (no
`cbl-protos`).

## 4. Proposed design (to confirm at task start)

1. Compute the set of enabled providers (the `CONNECTOR_PROVIDERS` allowlist and
   the configured base URLs, the same predicate as the gateway construction).
2. A decoder requirement exists only for an enabled provider whose manifest has
   `FetchMandatory == false` (today only `mock`).
3. If no decoder is configured and a requirement exists -> startup failure with
   the provider names; if no requirement exists -> start with an empty registry.
4. `NewSandboxDecoder` rejects names outside the sandbox catalogue (today
   `mock`; a new sandbox profile is an explicit additive change).
5. The runner logs the decoder set and the fetch-mandatory providers at startup
   for operability.
6. Keep the default `CONNECTOR_SANDBOX_PROVIDERS=mock` for the stands; document
   that production sets it to an empty value (or omits the mock) when only
   real providers are enabled.

## 5. Acceptance criteria

- With all enabled providers fetch-mandatory and `CONNECTOR_SANDBOX_PROVIDERS=""`,
  the service starts and the mock profile is unavailable (no decoder).
- With `CONNECTOR_SANDBOX_PROVIDERS=mock` the service starts and the mock webhook
  path behaves exactly as today.
- A non-fetch-mandatory delivery for a provider without a decoder returns the
  explicit unknown-provider error; the inbox moves it to the retry topics/DLQ; a
  unit test pins it.
- An unknown sandbox name fails the startup with a clear message.
- AGENTS, `.env.example`/compose comments and the runbook describe the variable
  correctly (sandbox/non-fetch-mandatory decoders, not a production requirement).
- 4 review passes, unit+integration coverage at least 80%, the service e2e for
  both configurations, the global stand unaffected.

## 6. Test plan outline (full matrix at task start)

- Unit: the startup decision matrix (allowlist/base URL/enabled provider x
  `FetchMandatory` x decoder set); `NewSandboxDecoder` name validation; the
  normalizer unknown-provider error.
- Integration: main-level wiring cannot be unit-tested directly; extract the
  decision helper and test it; keep the normalizer integration unchanged.
- E2E: the service/global stand with `CONNECTOR_PROVIDERS=btcpay` and an empty
  decoder list (starts, invoices work, a mock webhook is not expected); the mock
  stand with the default list (GE webhook path green).

## 7. Dependencies and references

- Depends on: 17.2 (manifests, fetch-to-verify), 17.9 (BTCPay), both done.
- References: `cmd/connector/main.go` (the guard), `internal/usecase/normalizer.go`
  (`Handle`, `decodeCanonical`), `internal/usecase/decoder.go`,
  `internal/usecase/sandbox_decoder.go`, `internal/domain/manifest.go`.
- Trade-off candidate (T-CONNECTOR-30): "startup validates only the enabled
  providers' decoder needs" with the pros/cons (a misconfigured non-enabled
  provider is caught at delivery time, not startup) to be recorded in the spec
  and AGENTS.
