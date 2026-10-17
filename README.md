# cbl-connector

Abstraction of external crypto gateways: adapters, routing, resilience, webhook
normalization, the administrative routing contour.

Service role and contracts: `AGENTS.md`. Global platform rules: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

E2E: the smoke run follows `test/e2e/E2E.md` (scenarios `E2E_Connector_Smoke`,
`E2E_Connector_BTCPayProfile`, `E2E_Connector_CoinGateProfile`,
`E2E_Connector_NowPaymentsProfile`, `E2E_Connector_BitPayProfile` and the local
BTCPay regtest stand `E2E_Connector_BTCPayRegtest`; artifacts in
`test/e2e/.runs/<UTC mark>/`).

## BitPay adapter (17.8)

```
CONNECTOR_PROVIDERS=bitpay                  # optional CSV allowlist of wired adapters
CONNECTOR_BITPAY_BASE_URL=https://bitpay.com
CONNECTOR_BITPAY_API_TOKEN=...              # optional pairing token
CONNECTOR_BITPAY_PRIVATE_KEY_PEM=<PEM>      # secp256k1 request-signing key (SEC1 or PKCS8)
CONNECTOR_BITPAY_WEBHOOK_SECRET=...         # raw HMAC-SHA256 in base64 (x-signature)
CONNECTOR_BITPAY_WEBHOOK_URL=https://...    # optional notificationURL
CONNECTOR_BITPAY_CAPABILITIES=[{"network":"usdt_trc20","asset":"USDT","min_minor":1000000,"decimals":6,"confirmations":2}]
```

Invoices create/get, order lookup, raw-HMAC webhook verification and the
provisional payout/refund shapes (17.11/17.12 own the real flows). Every request
is ECDSA secp256k1-signed (`X-Identity`/`X-Signature` over url plus body). The
adapter passes the conformance suite and the compose scenario
`E2E_Connector_BitPayProfile`.

## BTCPay adapter (17.9)

```
CONNECTOR_BTCPAY_BASE_URL=https://btcpay.example          # unset keeps the runtime fail-closed
CONNECTOR_BTCPAY_STORE_ID=...
CONNECTOR_BTCPAY_API_TOKEN=...
CONNECTOR_BTCPAY_WEBHOOK_SECRET=...
CONNECTOR_BTCPAY_CAPABILITIES=[{"network":"btc","asset":"BTC","min_minor":10000,"decimals":8,"confirmations":2}]
```

The adapter implements SPI v1 (hosted invoices, order lookup, `BTCPay-Sig` webhook
verification, pull payments for payouts/refunds) and passes the conformance suite
(`internal/providers/btcpay/conformance_test.go`). The compose stand points it at the
mock-gateway `btcpay` profile for the cross-service e2e.

## NOWPayments adapter (17.6)

```
CONNECTOR_PROVIDERS=nowpayments             # optional CSV allowlist of wired adapters
CONNECTOR_NOWPAYMENTS_BASE_URL=https://api.nowpayments.io
CONNECTOR_NOWPAYMENTS_API_KEY=...
CONNECTOR_NOWPAYMENTS_IPN_SECRET=...        # canonical JSON HMAC-SHA512 (x-nowpayments-sig)
CONNECTOR_NOWPAYMENTS_CAPABILITIES=[{"network":"usdttrc20","asset":"USDT","min_minor":1000000,"decimals":6,"confirmations":2}]
```

Hosted invoices, payment reads and the envelope lookup, IPN verification and the
provisional payout shapes (17.11). Refunds are manual (decision 8). The adapter
passes the conformance suite and the compose scenario
`E2E_Connector_NowPaymentsProfile`.

## CoinGate adapter (17.7)

```
CONNECTOR_PROVIDERS=coingate               # optional CSV allowlist of wired adapters
CONNECTOR_COINGATE_BASE_URL=https://api.coingate.com
CONNECTOR_COINGATE_API_TOKEN=...
CONNECTOR_COINGATE_IPN_TOKEN=...           # token echo secret of the callbacks
CONNECTOR_COINGATE_CAPABILITIES=[{"network":"usdt_trc20","asset":"USDT","min_minor":1000000,"decimals":6,"confirmations":2}]
```

Orders create/get, order lookup, token-echo verification and the provisional
payout/refund shapes (17.11/17.12 own the real flows). The adapter passes the
conformance suite and the compose scenario `E2E_Connector_CoinGateProfile`.

## Adapter conformance (17.5)

Every adapter (including future ones) passes the shared contract suite from
`internal/conformance` before onboarding (plan 14.4/14.5.8):

```go
func TestConformance(t *testing.T) {
    conformance.Run(t, conformance.Harness{
        Provider: "nowpayments",
        Manifest: manifest,
        New: func(t *testing.T, baseURL string) domain.PaymentGateway {
            return adapter.New(baseURL, secrets)
        },
    })
}
```

The suite drives the adapter against provider wire fixtures through `CONF-1..CONF-10`
(identity, capabilities, create echo, status mapping, unknown status, webhook
verification, operations, lookup, idempotent create, error classification) and reports
every violation with the offending fixture.

## Admin contour (12.2)

`billing.connector.v1.ConnectorAdminService` on the gRPC port (:9050) serves the
back-office: `GetRoutingRules`/`PutRoutingRules` with the revision-based optimistic
concurrency (the configuration revision lives in `routing_config`) and
`DisableProvider` (the disabled set lives in `disabled_providers`; the route source
filters those providers out, so the router never selects them). The back-office
enforces RBAC and audit; port 9050 must not leave the trusted zone (KI-connector-03).

## Commands

```powershell
make build        # build
make test         # mocked unit tests
make vet          # go vet
make lint         # golangci-lint in Docker
make up           # dependencies + service in docker-compose
make down
```
