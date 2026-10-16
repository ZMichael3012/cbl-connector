# E2E auxiliary files (cbl-connector)

Purpose: support runs per `test/e2e/E2E.md` and analysis of their artifacts.
There are no PowerShell run scripts here (convention `CBL-002` section 7).

| File/directory | Purpose |
| :-- | :-- |
| `summary.template.md` | The `summary.md` run report template |

The smoke scenario uses only standard tools (docker compose, curl) and
`test/e2e/normalizer_kafka_test.go` for the separate testcontainers check. New auxiliary
files are added here and recorded in this table.
