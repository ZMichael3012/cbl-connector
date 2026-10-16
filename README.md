# cbl-connector

Abstraction of external crypto gateways: adapters, routing, resilience, webhook normalization.

Service role and contracts: `AGENTS.md`. Global platform rules: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

## Commands

```powershell
make build        # build
make test         # mocked unit tests
make vet          # go vet
make lint         # golangci-lint in Docker
make up           # dependencies + service in docker-compose
make down
```
