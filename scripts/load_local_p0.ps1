# P0 load: 2x Http2 client + 4x Performance (nhieu process cmd/client).
# Chay tu root repo (sau khi start_local_p0 + Logic da REGISTER):
#   .\scripts\load_local_p0.ps1
#   .\scripts\load_local_p0.ps1 -Duration 60s -ClientQPS 20 -PerfQPS 50
#
# Moi process dung -unique-session de consistent_hash trai 2 Logic.
# Ket qua stdout/stderr: .\scripts\load-logs\

[CmdletBinding()]
param(
    [string]$Addr = "http://127.0.0.1:8080",
    [string]$Duration = "30s",
    [int]$ClientC = 4,
    [double]$ClientQPS = 20,
    [int]$PerfC = 8,
    [double]$PerfQPS = 40,
    [uint32]$MessageId = 1001,
    [string]$Body = "hello"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "go.mod"))) {
    $Root = $PSScriptRoot
}
Set-Location $Root

$LogDir = Join-Path $Root "scripts\load-logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"

function Start-ClientJob {
    param(
        [string]$Name,
        [int]$C,
        [double]$QPS,
        [string]$SessionPrefix
    )
    $out = Join-Path $LogDir "${stamp}_${Name}.out.log"
    $err = Join-Path $LogDir "${stamp}_${Name}.err.log"
    $args = @(
        "run", "./cmd/client",
        "-addr", $Addr,
        "-d", $Duration,
        "-c", "$C",
        "-qps", "$QPS",
        "-message-id", "$MessageId",
        "-session-id", $SessionPrefix,
        "-unique-session",
        "-body", $Body
    )
    Write-Host "START $Name  c=$C qps=$QPS  -> $out" -ForegroundColor Cyan
    $p = Start-Process -FilePath "go" -ArgumentList $args `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -PassThru -NoNewWindow
    return [pscustomobject]@{ Name = $Name; Process = $p; Out = $out; Err = $err }
}

Write-Host "P0 load: 2 Http2-client + 4 Performance  duration=$Duration" -ForegroundColor Green
Write-Host "Addr=$Addr  logs=$LogDir" -ForegroundColor DarkGray

$jobs = @()
# 2 node Http2 client (tai on dinh)
$jobs += Start-ClientJob -Name "http2client-1" -C $ClientC -QPS $ClientQPS -SessionPrefix "cli1"
$jobs += Start-ClientJob -Name "http2client-2" -C $ClientC -QPS $ClientQPS -SessionPrefix "cli2"
# 4 node Performance
$jobs += Start-ClientJob -Name "perf-1" -C $PerfC -QPS $PerfQPS -SessionPrefix "perf1"
$jobs += Start-ClientJob -Name "perf-2" -C $PerfC -QPS $PerfQPS -SessionPrefix "perf2"
$jobs += Start-ClientJob -Name "perf-3" -C $PerfC -QPS $PerfQPS -SessionPrefix "perf3"
$jobs += Start-ClientJob -Name "perf-4" -C $PerfC -QPS $PerfQPS -SessionPrefix "perf4"

Write-Host "Dang cho $($jobs.Count) process..." -ForegroundColor Yellow
$jobs.Process | Wait-Process

$fail = 0
foreach ($j in $jobs) {
    $code = $j.Process.ExitCode
    if ($null -eq $code) { $code = -1 }
    $color = if ($code -eq 0) { "Green" } else { "Red"; $fail++ }
    Write-Host ("DONE {0} exit={1}  {2}" -f $j.Name, $code, $j.Out) -ForegroundColor $color
    if (Test-Path $j.Out) {
        Get-Content $j.Out -Tail 12 | ForEach-Object { Write-Host "  $_" }
    }
    if ($code -ne 0 -and (Test-Path $j.Err)) {
        Get-Content $j.Err -Tail 5 | ForEach-Object { Write-Host "  ERR $_" -ForegroundColor DarkRed }
    }
}

Write-Host ""
Write-Host "Snapshot metrics:" -ForegroundColor Cyan
try {
    curl.exe -s "$Addr/metrics.json"
    Write-Host ""
} catch {
    Write-Host "Khong lay duoc metrics.json (GW chua chay?)" -ForegroundColor Red
}

if ($fail -gt 0) {
    Write-Host "P0 load: $fail process fail (xem log)." -ForegroundColor Red
    exit 1
}
Write-Host "P0 load: tat ca process exit 0." -ForegroundColor Green
