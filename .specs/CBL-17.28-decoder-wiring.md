# CBL-17.28 - Decoder wiring: allow fetch-mandatory-only deployments (cbl-connector)

Task: CBL-17.28 in `CBL-004` (E17, W5-SCALE), depends on 17.2 and 17.9 (done).
Repository: `cbl-connector`. Status: in progress.

## 1. Problem and scope

`cmd/connector/main.go` refuses to start when `CONNECTOR_SANDBOX_PROVIDERS` yields no
decoder, although the normalizer needs a decoder only for the non-fetch-mandatory path
(`fetch_required` deliveries and providers without `fetch_mandatory`). Every real provider
manifest is fetch-mandatory, so a production deployment with real providers is rejected by
the guard while the default `mock` masks it. `NewSandboxDecoder` also accepts any name
(for example `coingate`), suggesting a provider decoder that would never be correct.

In scope: the startup decision, the empty-list semantics (set-but-empty disables the
sandbox decoder), the sandbox name catalogue, the normalizer fail-closed coverage, the
compose/.env/AGENTS documentation and the stand e2e in both configurations. Out of scope:
provider adapters implementing `WebhookDecoder`, the mock pipeline contract and contract
changes.

## 2. Architecture

- `usecase.MissingDecoders(manifests, configured, decoders) []string`: the single startup
  decision. It returns the configured providers that need a decoder: non-sandbox manifests
  with `FetchMandatory == false` and no registered decoder. The result is sorted, so the
  startup log is deterministic. Sandbox providers are excluded: without a decoder their
  deliveries fail closed at runtime, which is the existing normalizer behavior.
- `main`: builds the sandbox decoders from the variable with `os.LookupEnv` semantics
  (`unset` -> `mock`, `set-but-empty` -> no decoders), computes the missing set against the
  **configured gateways** (the same predicate as the runtime), fails startup with the
  provider names when the set is non-empty, and otherwise builds the registry (an empty
  registry is valid).
- `usecase.NewSandboxDecoder`: accepts only the closed sandbox catalogue (today `mock`);
  an unknown or real provider name is a startup error.
- The normalizer is unchanged: a non-fetch-mandatory delivery without a decoder returns
  `ErrUnknownProvider`, the inbox retries and the message reaches the DLQ.

Trade-off T-CONNECTOR-33: the startup validates only configured providers; a provider
added to the allowlist without a base URL is not checked until it is configured (it cannot
deliver webhooks anyway). The fail-closed runtime error covers the decode path.

## 3. Test plan

| ID | Case | Expectation |
| :-- | :-- | :-- |
| D1 | btcpay configured, no decoders | MissingDecoders is empty, startup allowed |
| D2 | a non-fetch-mandatory provider configured, no decoder | the provider is reported |
| D3 | same provider with its decoder | empty |
| D4 | the non-fetch-mandatory provider not configured | empty (no requirement) |
| D5 | a sandbox provider configured, no decoder | empty (runtime fail-closed) |
| D6 | two missing providers | sorted, deterministic |
| D7 | `NewSandboxDecoder("mock")` / `"Mock"` | accepted |
| D8 | `NewSandboxDecoder("coingate")` / `"unknown"` | error |
| D9 | empty decoder registry + a non-fetch-mandatory delivery | `ErrUnknownProvider` (inbox retry/DLQ) |
| D10 | `CONNECTOR_SANDBOX_PROVIDERS` unset vs set-but-empty | mock decoder vs zero decoders |

Integration: the normalizer registry path is unit/integration covered (D9); no new
testcontainers case.
E2E: (a) `CONNECTOR_PROVIDERS=btcpay`, `CONNECTOR_SANDBOX_PROVIDERS=` empty: the service
starts, logs zero decoders, CreateInvoice/GetGatewayPayment work; (b) the default stand:
the mock decoder is configured and the smoke passes. Both on the compose stand with
artifacts under `test/e2e/.runs/<mark>/`.

## 4. Red evidence

Recorded 20.10.2026: `go test ./internal/usecase` failed with `undefined:
usecase.MissingDecoders`; the sandbox constructor accepted `coingate`, `btcpay`,
`nowpayments` and `bitpay` before the closed catalogue (the new D8 test failed); the
old startup guard rejected an empty decoder list by construction (the e2e stand A was
not startable before the fix).

## 5. Results and review closure (20.10.2026)

- D1 - D10 green; empty registry through the normalizer returns
  `ErrUnknownProvider` (D9), which the inbox routes to retry/DLQ.
- Coverage 82.3% (`-coverpkg=./internal/...`), golangci-lint 0, go vet clean, em dash
  check green.
- E2E stand A `20261020t084425z` PASS: `CONNECTOR_PROVIDERS=btcpay`, empty decoder list
  (compose `--env-file`, since cmd removes an empty `set` variable): decoders=0, no
  startup error, invoice PENDING/New.
- E2E stand B `20261020t085032z` PASS: default configuration, decoders=1, mock-gateway
  healthy; the mock decode path is unchanged and covered by the normalizer suites.
- Review pass 1 (no-context): the semantics of unset vs set-but-empty are explicit in
  the env, compose and AGENTS; the decision matrix covers fetch-mandatory, sandbox and
  non-configured providers.
- Review pass 2 (architecture): the decision lives in `usecase.MissingDecoders` (one
  implementation, unit-tested) and uses the configured gateways as the predicate; no
  startup logic is duplicated in main.
- Review pass 3 (with-context, blocking): the default stand must keep the mock decoder.
  Closed: the compose default is `${CONNECTOR_SANDBOX_PROVIDERS-mock}` (unset -> mock),
  and both stands were run; the binary log line is stable for e2e greps.
- Review pass 4 (security): closing the sandbox catalogue prevents a real provider name
  from being decoded with the sandbox JSON format (which would canonicalize untrusted
  unsigned fields); a delivery without a decoder still fails closed; no secrets are
  logged.
