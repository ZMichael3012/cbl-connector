# Smoke CBL-3.5: compose-стенд connector поднимается, миграции применяются,
# конвейер нормализации запущен. Запуск: powershell -NoProfile -File test/e2e/smoke.ps1
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ComposeFile = Join-Path $RepoRoot "deploy\docker-compose.yml"

Push-Location $RepoRoot
try {
    docker compose -f $ComposeFile --profile app up -d --build
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

    $ready = $false
    foreach ($attempt in 1..45) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 http://localhost:8050/healthz
            if ($response.StatusCode -eq 200) { $ready = $true; break }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $ready) { throw "connector healthz is not ready" }

    # Логи читаются через cmd: PowerShell при Stop-политике превращает stderr
    # нативных команд (zap и goose пишут в stderr) в исключения NativeCommandError.
    $logs = cmd /c "docker logs cbl-connector-connector-1 2>&1"
    if ($LASTEXITCODE -ne 0) { throw "docker logs failed" }
    $logsText = ($logs | Out-String)
    if ($logsText -notmatch "successfully migrated database to version") { throw "migrations did not apply" }
    if ($logsText -notmatch "webhook normalizer started") { throw "webhook normalizer did not start" }
    if ($logsText -match "apply migrations failed") { throw "migrations failed" }

    Write-Host "OK: healthz ok, migrations applied, webhook normalizer started"
} finally {
    docker compose -f $ComposeFile --profile app down -v | Out-Null
    Pop-Location
}
