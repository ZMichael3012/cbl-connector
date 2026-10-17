# E2E cbl-connector: compose stand smoke (migrations and the normalizer pipeline)

Run instruction for the local e2e scenario of the service. A run is performed by an
agent platform-independently: docker and a command shell. There are no PowerShell run
scripts (customer decision of 16.10.2026, `CBL-002` section 7).

Repository: `cbl-connector`. Stand: `deploy/docker-compose.yml` (postgres, kafka,
schema-registry, connector and mock-gateway in the `app` profile). The mock-gateway
service is built from the sibling repository `../cbl-mock-gateway` (both repositories
must be checked out side by side, the Dockerfile expects the `services/` build
context); it has no compose healthcheck (KI-08), so the scenarios wait for its
`/healthz` explicitly. The stand smoke checks that the service starts, migrations
apply and the webhook normalization pipeline is running; the normalizer end-to-end
scenario with poison-pill cases runs separately on testcontainers (`make test-e2e`,
`test/e2e/normalizer_kafka_test.go`).

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_Connector_Smoke` | compose stand up, goose migrations applied (success log line, no failure line), `webhook normalizer started`, healthz/readyz, the admin contour answers `GetRoutingRules` (revision 0) | CBL-3.5 pipeline start, administrative endpoints, `ConnectorAdminService` (CBL-12.2) |
| `E2E_Connector_BTCPayProfile` | compose stand with the mock-gateway `btcpay` profile: `ConnectorService/CreateInvoice` answers a 32-hex invoice id, provider `btcpay` and a checkout link; `GetGatewayPayment` answers the detected state with raw `New`; after `POST /api/v1/simulate provider=btcpay status=confirmed` on the mock the read answers the confirmed state with raw `Settled` | the BTCPay adapter against the CBL-17.5 profile wire contract (cross-service) |

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz` (uppercase is rejected by compose project
  naming rules).
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- Compose project: `cbl-connector-e2e-<mark>` (the compose file declares
  `name: cbl-connector`; the `-p` flag overrides it); the connector container is
  `cbl-connector-e2e-<mark>-connector-1`.
- Host ports 5432, 9092, 8081, 8050, 9050, 9250 and 8120 (mock-gateway) are
  published; do not run two connector stands in parallel and stop other connector
  stands first.
- All commands run from the repository root.

## 3. Common run order

1. `<MARK>` = current UTC time in lowercase; create `test/e2e/.runs/<MARK>/`.
2. Stop the previous stand: `docker compose -p cbl-connector-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
   (output to `compose-down.log`; the first run has nothing to stop).
3. Start the stand: `docker compose -p cbl-connector-e2e-<MARK> -f deploy/docker-compose.yml --profile app up -d --build --wait`
   (`compose-up.log`); `docker compose ps` into `compose-ps.txt`.
4. Wait for `GET http://localhost:8050/healthz` = 200 (up to 90 s, 2 s interval) and
   `GET http://localhost:8050/readyz` = 200 (curl.exe; redirect native command output to
   files: goose and zap write to stderr).
5. Run the scenario checks (section 4), recording every OK/FAIL line in
   `smoke-checks.txt` and transcripts in `smoke.log`.
6. Capture logs: `docker compose -p ... logs --no-color > compose-logs.txt` and
   `docker compose -p ... logs --no-color connector > connector-logs.txt`.
7. `summary.md` by the `.additionals/summary.template.md` template; stop the stand:
   `down -v` (append to `compose-down.log`).

## 4. E2E_Connector_Smoke

1. Common run order, steps 1-4.
2. Liveness/readiness: `healthz` = 200, `readyz` = 200.
3. Migrations: `connector-logs.txt` contains
   `successfully migrated database to version` and does not contain
   `apply migrations failed`.
4. Pipeline: `connector-logs.txt` contains `webhook normalizer started`.
5. Admin contour (CBL-12.2): the gRPC server answers the routing read -
   `docker run --rm --network <project>_cbl-net -v <repo>/../cbl-protos/proto:/p fullstorydev/grpcurl:v1.9.1 -plaintext -import-path /p -proto billing/connector/v1/connector.proto -d {} connector:9050 billing.connector.v1.ConnectorAdminService/GetRoutingRules`
   -> a fresh configuration renders as `{}` (protojson omits zero values: revision 0
   and an empty item list); the check parses the JSON and treats the absent revision
   as 0.
6. Checks go to `smoke-checks.txt`.

## 5. E2E_Connector_BTCPayProfile

1. Common run order, steps 1-4; additionally wait for
   `GET http://localhost:8120/healthz` = 200 (the mock-gateway service of the
   stand).
2. Create an invoice through the money path: pipe the body
   `{"payment_id":"<fresh UUID>","idempotency_key":"btcpay-e2e-1","amount":{"minor_units":100000,"asset":"BTC","decimals":8},"asset":"BTC","network":"btc","expires_at":"<now+30m RFC3339>"}`
   into
   `docker run --rm -i --network <project>_cbl-net -v <repo>/../cbl-protos/proto:/p fullstorydev/grpcurl:v1.9.1 -plaintext -import-path /p -proto billing/connector/v1/connector.proto -d @ connector:9050 billing.connector.v1.ConnectorService/CreateInvoice`;
   the answer must carry a 32-character lowercase hex `gatewayInvoiceId`,
   `provider` = `btcpay` and a non-empty `checkoutUrl`.
3. Read back: `GetGatewayPayment` with `{"provider":"btcpay","gatewayId":"<id>"}`;
   the answer must carry the canonical detected state (`DETECTED`) and
   `rawStatus` = `New`.
4. Advance the mock invoice:
   `curl.exe -X POST http://localhost:8120/api/v1/simulate -H "Content-Type: application/json" -d @<file>`
   with `{"gateway_id":"<id>","provider":"btcpay","status":"confirmed"}`; the
   response carries a delivery (a network error to the stand webhook target is
   acceptable).
5. Read again: `GetGatewayPayment` must answer the canonical confirmed state
   (`CONFIRMED`) with `rawStatus` = `Settled`.
6. Checks go to `smoke-checks.txt`.

## 6. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
  key values, artifact list, time);
- `compose-ps.txt`, `compose-up.log`, `compose-down.log`, `compose-logs.txt`,
  `connector-logs.txt`;
- `smoke.log` (curl transcripts) and `smoke-checks.txt`;
- on failure: container states and the connector log fragments around the failure.

## 7. Auxiliary files (.additionals)

- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

## 8. Cleanup

`docker compose -p cbl-connector-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
at the end; `.runs/` is not committed; a repeated run must start from a clean stand.
