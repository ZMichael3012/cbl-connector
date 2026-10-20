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
| `E2E_Connector_Smoke` | compose stand up, goose migrations applied (success log line, no failure line), `webhook normalizer started`, healthz/readyz, the admin contour answers `GetRoutingRules` (revision 0); with only fetch-mandatory providers and an empty decoder list the service starts with `webhook decoders configured decoders=0` (CBL-17.28) | CBL-3.5 pipeline start, administrative endpoints, `ConnectorAdminService` (CBL-12.2) |
| `E2E_Connector_BTCPayProfile` | compose stand with the mock-gateway `btcpay` profile: `ConnectorService/CreateInvoice` answers a 32-hex invoice id, provider `btcpay` and a checkout link; `GetGatewayPayment` answers the detected state with raw `New`; after `POST /api/v1/simulate provider=btcpay status=confirmed` on the mock the read answers the confirmed state with raw `Settled` | the BTCPay adapter against the CBL-17.5 profile wire contract (cross-service) |
| `E2E_Connector_BTCPayRegtest` | local BTCPay regtest stand: programmatic setup (admin, API key, store, webhook, watch-only wallet), regtest funding; an invoice created through `ConnectorService` is paid from the node, `GetGatewayPayment` settles to `CONFIRMED`/`Settled`; the provider order lookup finds the invoice; deliveries succeed and `BTCPay-Sig` verifies offline over the raw delivery body; the refund wire shape (pull payment with string method ids plus a payout `AwaitingApproval`) is accepted | the BTCPay adapter against the real Greenfield API (BTCPay Server 2.0.6) |
| `E2E_Connector_CoinGateProfile` | compose stand with the mock-gateway `coingate` profile selected through `CONNECTOR_PROVIDERS=coingate`: `CreateInvoice` answers a numeric order id, provider `coingate` and a payment address; `GetGatewayPayment` answers detected/`new`; the mock simulate advances the order; the read answers confirmed/`paid` | the CoinGate adapter against the CBL-17.5 profile wire contract (cross-service) |
| `E2E_Connector_NowPaymentsProfile` | compose stand with the mock-gateway `nowpayments` profile selected: `CreateInvoice` answers the invoice id, provider `nowpayments` and a checkout URL; the read answers detected/`waiting`; the mock simulate advances; the read answers confirmed/`finished` | the NOWPayments adapter against the CBL-17.5 profile wire contract (cross-service) |
| `E2E_Connector_BitPayProfile` | compose stand with the mock-gateway `bitpay` profile selected: `CreateInvoice` (ECDSA-signed) answers the invoice id, provider `bitpay` and a checkout URL; the read answers detected/`new`; the mock simulate advances; the read answers confirmed/`complete` | the BitPay adapter against the CBL-17.5 profile wire contract (cross-service) |
| `E2E_Connector_PayoutPipeline` | testcontainers Kafka/PostgreSQL: a `payout.commands` command produces a `gateway.payout.events` submitted event with the provider and the provider payout id; the status worker resolves a confirmed verdict exactly once; a replay (new event id) calls no provider and publishes nothing; a refund command (cf-4.2 marker) goes through `CreateRefund` of the original provider and is resolved through `GetRefund`, with the refund marker recorded in the mapping | the payout and refund pipelines (CBL-17.11, CBL-17.12) |
| `E2E_Connector_Export` | planned (17.16 harness / mock list endpoints): `ListGatewayPayments` twice per provider continuing an opaque cursor, rows carry `payment_id`, canonical status and `raw_status`, a fixture unknown-status row arrives as `UNSPECIFIED`, a window with no operations answers an empty page; locally the wire shapes are pinned by the adapter export unit suites (KI-connector-13) | the CBL-17.13 export (`ListGatewayPayments`, frozen at CF-1) |

The consumer and payout-pipeline scenarios run on testcontainers (`make test-e2e`:
`test/e2e/normalizer_kafka_test.go`, `test/e2e/payout_pipeline_kafka_test.go`);
the compose stand covers the smoke and provider-profile scenarios.

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
   the answer must carry the canonical pending state (`PENDING`) and
   `rawStatus` = `New` (a pre-deposit status, KI-connector-14).
4. Advance the mock invoice:
   `curl.exe -X POST http://localhost:8120/api/v1/simulate -H "Content-Type: application/json" -d @<file>`
   with `{"gateway_id":"<id>","provider":"btcpay","status":"confirmed"}`; the
   response carries a delivery (a network error to the stand webhook target is
   acceptable).
5. Read again: `GetGatewayPayment` must answer the canonical confirmed state
   (`CONFIRMED`) with `rawStatus` = `Settled`.
6. Checks go to `smoke-checks.txt`.

## 6. E2E_Connector_CoinGateProfile

1. Common run order, steps 1-4; the stand includes the mock-gateway service and
   the connector must be started with the CoinGate provider selected:
   `CONNECTOR_PROVIDERS=coingate`, `CONNECTOR_COINGATE_BASE_URL=http://mock-gateway:8120`,
   `CONNECTOR_COINGATE_API_TOKEN=mock-token`,
   `CONNECTOR_COINGATE_IPN_TOKEN=mock-coingate-token`,
   `CONNECTOR_COINGATE_CAPABILITIES=[{"network":"usdt_trc20","asset":"USDT","min_minor":1000000,"decimals":6,"confirmations":2}]`
   (export these before `docker compose up`; the allowlist disables the
   defaulted BTCPay adapter).
2. Create an invoice through the money path:
   `{"payment_id":"<fresh UUID>","idempotency_key":"coingate-e2e-1","amount":{"minor_units":12340000,"asset":"USDT","decimals":6},"asset":"USDT","network":"USDT_TRC20","expires_at":"<now+30m RFC3339>"}`;
   the answer must carry a numeric `gatewayInvoiceId`, `provider` = `coingate`
   and a non-empty `address` (the payment address).
3. Read back: `GetGatewayPayment` with `{"provider":"coingate","gatewayId":"<id>"}`
   must answer the canonical pending state (`PENDING`) and `rawStatus` = `new`
   (a pre-deposit status, KI-connector-14).
4. Advance the mock order: `POST http://localhost:8120/api/v1/simulate` with
   `{"gateway_id":"<id>","provider":"coingate","status":"confirmed"}`.
5. Read again: the canonical confirmed state (`CONFIRMED`) with `rawStatus` = `paid`.
6. Checks go to `smoke-checks.txt`.

## 7. E2E_Connector_NowPaymentsProfile

1. Common run order, steps 1-4; start the connector with the NOWPayments
   provider selected: `CONNECTOR_PROVIDERS=nowpayments`,
   `CONNECTOR_NOWPAYMENTS_BASE_URL=http://mock-gateway:8120`,
   `CONNECTOR_NOWPAYMENTS_API_KEY=mock-token`,
   `CONNECTOR_NOWPAYMENTS_IPN_SECRET=mock-nowpayments-secret`,
   `CONNECTOR_NOWPAYMENTS_CAPABILITIES=[{"network":"usdttrc20","asset":"USDT","min_minor":1000000,"decimals":6,"confirmations":2}]`.
2. Create an invoice through the money path:
   `{"payment_id":"<fresh UUID>","idempotency_key":"nowpayments-e2e-1","amount":{"minor_units":12340000,"asset":"USDT","decimals":6},"asset":"USDT","network":"usdttrc20","expires_at":"<now+30m RFC3339>"}`;
   the answer must carry the mock invoice id, `provider` = `nowpayments` and a
   non-empty `checkoutUrl`.
3. Read back: `GetGatewayPayment` with
   `{"provider":"nowpayments","gatewayId":"<id>"}` must answer the canonical
   pending state (`PENDING`) and `rawStatus` = `waiting`
   (a pre-deposit status, KI-connector-14).
4. Advance the mock order: `POST http://localhost:8120/api/v1/simulate` with
   `{"gateway_id":"<id>","provider":"nowpayments","status":"confirmed"}`.
5. Read again: the canonical confirmed state (`CONFIRMED`) with
   `rawStatus` = `finished`.
6. Checks go to `smoke-checks.txt`.

## 8. E2E_Connector_BitPayProfile

1. Common run order, steps 1-4; start the connector with the BitPay provider
   selected: `CONNECTOR_PROVIDERS=bitpay`,
   `CONNECTOR_BITPAY_BASE_URL=http://mock-gateway:8120`,
   `CONNECTOR_BITPAY_API_TOKEN=mock-token`,
   `CONNECTOR_BITPAY_PRIVATE_KEY_PEM=<dev PEM EC PRIVATE KEY>` (the stand-only
   dev key; production injects the secret from Vault),
   `CONNECTOR_BITPAY_WEBHOOK_SECRET=mock-bitpay-secret`,
   `CONNECTOR_BITPAY_CAPABILITIES=[{"network":"usdt_trc20","asset":"USDT","min_minor":1000000,"decimals":6,"confirmations":2}]`.
   The dev key must be generated outside the repository and never recorded in
   the run artifacts or logs; `.gitignore` blocks `*.pem`/`*.key`, and only the
   public identity fingerprint may be noted anywhere.
2. Create an invoice through the money path:
   `{"payment_id":"<fresh UUID>","idempotency_key":"bitpay-e2e-1","amount":{"minor_units":12340000,"asset":"USDT","decimals":6},"asset":"USDT","network":"usdt_trc20","expires_at":"<now+30m RFC3339>"}`;
   the answer must carry the mock invoice id, `provider` = `bitpay` and a
   non-empty `checkoutUrl`.
3. Read back: `GetGatewayPayment` with `{"provider":"bitpay","gatewayId":"<id>"}`
   must answer the canonical pending state (`PENDING`) and `rawStatus` = `new`
   (a pre-deposit status, KI-connector-14).
4. Advance the mock invoice: `POST http://localhost:8120/api/v1/simulate` with
   `{"gateway_id":"<id>","provider":"bitpay","status":"confirmed"}`.
5. Read again: the canonical confirmed state (`CONFIRMED`) with
   `rawStatus` = `complete`.
6. Checks go to `smoke-checks.txt`.

## 9. E2E_Connector_BTCPayRegtest

Stand: `test/e2e/btcpay-stand/docker-compose.yml` (bitcoind regtest, NBXplorer
2.5.14, BTCPay Server 2.0.6, postgres, the sink logger, the connector built from
this repository; the sibling repositories `../cbl-kit` and `../cbl-protos` must
be checked out, the build context is the `services/` directory). Prerequisites:
the `btcpayserver/btcpayserver:2.0.6`, `nicolasdorier/nbxplorer:2.5.14`,
`ruimarinho/bitcoin-core:24.0.1`, `postgres:16-alpine` and `python:3.12-alpine`
images (about 1.5 GiB), roughly 5 minutes of runtime. Run the stand under a
per-mark project name (`-p cbl-btcpay-e2e-<mark>`), which also decides the
network name (`<project>_btcpay-net`); only port 49392 is published, and the
connector is reached in-network with
`docker run --rm -i --network <project>_btcpay-net -v <protos>:/p fullstorydev/grpcurl:v1.9.1 -plaintext -import-path /p -proto billing/connector/v1/connector.proto ... connector:9050 <method>`.
The stand connector runs without Kafka and Redis: the webhook normalizer is
disabled there (its pathway is covered by the testcontainers e2e), and the
scenario exercises the money path (create/read), the provider delivery log and
the refund wire shapes.

1. Start the stand fresh (`down -v` then `up -d bitcoind nbxplorer postgres
   btcpay sink`), wait for `GET http://localhost:49392/api/v1/health` = 200.
2. Programmatic setup through the Greenfield API: create the first admin user,
   create an API key (Basic auth) with store settings, invoice, pull payment and
   payout permissions, create the store, register the webhook
   `http://sink/btcpay` with the secret `e2e-btcpay-wh-secret`, generate the
   watch-only store wallet, then fund the node (`createwallet`, 101 blocks).
3. Start the connector profile with the stand credentials in
   `BTC_STAND_STORE_ID`/`BTC_STAND_API_KEY` and wait for the admin gRPC to answer.
4. Create an invoice through `ConnectorService/CreateInvoice` (100000 sats,
   order id `<fresh UUID>`); the answer carries the provider invoice id and the
   checkout link. Before paying, `GetGatewayPayment` must answer `PENDING` with
   raw `New` (the read-only pre-deposit state of increment A, T-CONNECTOR-22).
5. Read the invoice payment address (`GET .../invoices/{id}/payment-methods`), pay
   it from the node (`sendtoaddress`), mine two blocks, then poll
   `GetGatewayPayment` until `CONFIRMED` with `rawStatus` `Settled`.
6. Verify the provider order lookup (`GET .../invoices?orderId=<uuid>`).
7. Verify the webhook: deliveries exist with `HttpSuccess`; the sink log records
   `BTCPay-Sig`; recompute the HMAC-SHA256 over the delivery body and compare
   with the header and the body hash (offline verification).
8. Verify the refund wire shape: create a pull payment
   (`payoutMethods:["BTC"]`) and a payout against it (`payoutMethodId`
   `BTC-CHAIN`); the payout answers `AwaitingApproval` (the approval flow is
   17.11).
9. Artifacts as in section 11 plus the setup and webhook transcripts; stop the
   stand with `down -v`.

## 10. E2E_Connector_Export (planned, 17.16)

The export scenario needs either the 17.16 sandbox harness or mock-gateway list
endpoints for the provider profiles; until then the adapter wire shapes are
pinned by the export unit suites (`internal/providers/*/export_test.go`) and the
transport rows. The procedure to run once the harness lands:

1. Common run order, steps 1-4 with a provider profile selected.
2. `ListGatewayPayments` (grpcurl:
   `-d '{"provider":"<id>","from":"<RFC3339>","to":"<RFC3339>","limit":2}'`)
   -> a page of rows with `payment_id`, `status`, `raw_status` and, on a full
   page, `next_cursor`.
3. Repeat with the returned `next_cursor` -> the walk continues (or answers an
   empty page), proving cursor continuity end to end.
4. Assert a fixture unknown-status row arrives as `UNSPECIFIED` with the raw
   word, and a window without operations answers an empty page.
5. Artifacts as in section 11; stop the stand with `down -v`.

## 11. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
  key values, artifact list, time);
- `compose-ps.txt`, `compose-up.log`, `compose-down.log`, `compose-logs.txt`,
  `connector-logs.txt`;
- `smoke.log` (curl transcripts) and `smoke-checks.txt`;
- on failure: container states and the connector log fragments around the failure.

## 12. Auxiliary files (.additionals)

- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

## 13. Cleanup

`docker compose -p cbl-connector-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
at the end; `.runs/` is not committed; a repeated run must start from a clean stand.
