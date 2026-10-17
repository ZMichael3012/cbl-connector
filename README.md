# cbl-connector

Abstraction of external crypto gateways: adapters, routing, resilience, webhook
normalization, the administrative routing contour.

Service role and contracts: `AGENTS.md`. Global platform rules: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

E2E: the smoke run follows `test/e2e/E2E.md` (scenario `E2E_Connector_Smoke`,
including the `ConnectorAdminService` read); artifacts in `test/e2e/.runs/<UTC mark>/`.

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
