# Start local topology (headless, PID-tracked): master + http2gw + logic-1 + logic-2
#   .\scripts\start_local.ps1
# Window mode (debug): .\scripts\start_local_p0.ps1

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"
Set-Location $script:RepoRoot
Ensure-RunDir

$existing = Read-Pids
if ($existing.Count -gt 0) {
    Write-Host "PID file exists — stop first: .\scripts\stop_local.ps1" -ForegroundColor Yellow
}

$map = @{}
foreach ($n in $script:NodeDefs) {
    $out = Join-Path $script:LogDir "$($n.Name).out.log"
    $err = Join-Path $script:LogDir "$($n.Name).err.log"
    Write-Host "START $($n.Name)" -ForegroundColor Cyan
    $p = Start-Process -FilePath "go" -ArgumentList $n.Cmd `
        -WorkingDirectory $script:RepoRoot `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -PassThru -NoNewWindow
    $map[$n.Name] = @{
        Pid       = $p.Id
        StartedAt = (Get-Date).ToString("o")
        OutLog    = $out
        ErrLog    = $err
    }
    Start-Sleep -Seconds 1
}
Write-Pids $map

Write-Host "Waiting health..." -ForegroundColor Yellow
$okMaster = Wait-HttpOk "http://127.0.0.1:9200/healthz" 40
$okGw = Wait-HttpOk "http://127.0.0.1:8080/healthz" 40
if (-not $okMaster -or -not $okGw) {
    Write-Host "Health check incomplete (master=$okMaster gw=$okGw). See scripts/run/logs/" -ForegroundColor Red
    exit 1
}

Write-Host "Topology UP." -ForegroundColor Green
Write-Host "  Master nodes: curl.exe -s http://127.0.0.1:9200/v1/nodes"
Write-Host "  Status:       .\scripts\status_local.ps1"
Write-Host "  Load:         .\scripts\load_local.ps1"
Write-Host "  Stop:         .\scripts\stop_local.ps1"
