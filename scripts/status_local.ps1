# Status: ports, health, Master inventory, PIDs.
#   .\scripts\status_local.ps1

$ErrorActionPreference = "Continue"
. "$PSScriptRoot\_common.ps1"
Set-Location $script:RepoRoot

Write-Host "=== PIDs ===" -ForegroundColor Cyan
$map = Read-Pids
if ($map.Count -eq 0) {
    Write-Host "(no scripts/run/pids.json - maybe start_local_p0 window mode)"
} else {
    foreach ($name in $map.Keys) {
        $procId = [int]$map[$name].Pid
        $alive = $false
        try { $alive = $null -ne (Get-Process -Id $procId -ErrorAction SilentlyContinue) } catch {}
        $state = if ($alive) { "RUNNING" } else { "DEAD" }
        Write-Host ("  {0,-10} pid={1,-8} {2}" -f $name, $procId, $state)
    }
}

Write-Host "`n=== Health ===" -ForegroundColor Cyan
foreach ($u in @(
    "http://127.0.0.1:9200/healthz",
    "http://127.0.0.1:9200/ready",
    "http://127.0.0.1:8080/healthz",
    "http://127.0.0.1:8080/ready"
)) {
    try {
        $r = Invoke-WebRequest -Uri $u -UseBasicParsing -TimeoutSec 2
        Write-Host ("  {0} -> {1}" -f $u, $r.StatusCode) -ForegroundColor Green
    } catch {
        Write-Host ("  {0} -> DOWN" -f $u) -ForegroundColor Red
    }
}

Write-Host "`n=== Master /v1/nodes ===" -ForegroundColor Cyan
try {
    curl.exe -s http://127.0.0.1:9200/v1/nodes
    Write-Host ""
} catch {
    Write-Host "master unreachable" -ForegroundColor Red
}
