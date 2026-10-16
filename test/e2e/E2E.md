# E2E cbl-connector: compose stand smoke (migrations and the normalizer pipeline)

Run instruction for the local e2e scenario of the service. A run is performed by an
agent platform-independently: docker and a command shell. There are no PowerShell run
scripts (customer decision of 16.10.2026, `CBL-002` section 7).

Repository: `cbl-connector`. Stand: `deploy/docker-compose.yml` (postgres, kafka,
schema-registry, connector in the `app` profile). The stand smoke checks that the
service starts, migrations apply and the webhook normalization pipeline is running; the
normalizer end-to-end scenario with poison-pill cases runs separately on testcontainers
(`make test-e2e`, `test/e2e/normalizer_kafka_test.go`).

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_Connector_Smoke` | compose stand up, goose migrations applied (success log line, no failure line), `webhook normalizer started`, healthz/readyz | CBL-3.5 pipeline start, administrative endpoints |

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz` (uppercase is rejected by compose project
  naming rules).
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- Compose project: `cbl-connector-e2e-<mark>` (the compose file declares
  `name: cbl-connector`; the `-p` flag overrides it); the connector container is
  `cbl-connector-e2e-<mark>-connector-1`.
- Host ports 5432, 9092, 8081, 8050, 9050, 9250 are published; do not run two connector
  stands in parallel and stop other connector stands first.
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
5. Checks go to `smoke-checks.txt`.

## 5. Mandatory run artifacts

- `summary.md` (template `.additionals/summary.template.md`; PASS/FAIL verdict, steps,
  key values, artifact list, time);
- `compose-ps.txt`, `compose-up.log`, `compose-down.log`, `compose-logs.txt`,
  `connector-logs.txt`;
- `smoke.log` (curl transcripts) and `smoke-checks.txt`;
- on failure: container states and the connector log fragments around the failure.

## 6. Auxiliary files (.additionals)

- `summary.template.md` - the `summary.md` template;
- `inventory.md` - the list of auxiliary files and their purpose.

## 7. Cleanup

`docker compose -p cbl-connector-e2e-<MARK> -f deploy/docker-compose.yml --profile app down -v`
at the end; `.runs/` is not committed; a repeated run must start from a clean stand.
