# Phase 8 load: 2x Http2-client + 4x Performance via configs/local identity YAML.
# Chay sau khi topology san sang (start_local_p0 hoac docker-compose.local):
#   .\scripts\load_local.ps1
#   .\scripts\load_local.ps1 -Duration 60s
#
# Moi process: go run ./cmd/client -config configs/local/<name>.dev.yaml
# (register/HB Master neu master_url trong YAML).
# Log: scripts/load-logs/

[CmdletBinding()]
param(
    [string]$Duration = ""
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

$configs = @(
    "configs/local/http2client-1.dev.yaml",
    "configs/local/http2client-2.dev.yaml",
    "configs/local/perf-1.dev.yaml",
    "configs/local/perf-2.dev.yaml",
    "configs/local/perf-3.dev.yaml",
    "configs/local/perf-4.dev.yaml"
)

function Start-ConfigClient {
    param([string]$ConfigPath)
    $name = [System.IO.Path]::GetFileNameWithoutExtension($ConfigPath)
    $out = Join-Path $LogDir "${stamp}_${name}.out.log"
    $err = Join-Path $LogDir "${stamp}_${name}.err.log"
    $args = @("run", "./cmd/client", "-config", $ConfigPath)
    if ($Duration -ne "") {
        $args += @("-d", $Duration)
    }
    Write-Host "START $name -> $out" -ForegroundColor Cyan
    $p = Start-Process -FilePath "go" -ArgumentList $args `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -PassThru -NoNewWindow
    return [pscustomobject]@{ Name = $name; Process = $p; Out = $out; Err = $err }
}

Write-Host "Phase 8 load: 2 http2client + 4 performance (configs/local)" -ForegroundColor Green
$jobs = @()
foreach ($c in $configs) {
    if (-not (Test-Path $c)) {
        Write-Host "MISSING $c" -ForegroundColor Red
        exit 2
    }
    $jobs += Start-ConfigClient -ConfigPath $c
}

Write-Host "Dang cho $($jobs.Count) process..." -ForegroundColor Yellow
$jobs.Process | Wait-Process

$fail = 0
foreach ($j in $jobs) {
    $code = $j.Process.ExitCode
    if ($null -eq $code) { $code = -1 }
    $color = if ($code -eq 0) { "Green" } else { "Red"; $script:fail++ }
    Write-Host ("DONE {0} exit={1}" -f $j.Name, $code) -ForegroundColor $color
    if (Test-Path $j.Out) {
        Get-Content $j.Out -Tail 10 | ForEach-Object { Write-Host "  $_" }
    }
}

Write-Host ""
Write-Host "Master nodes:" -ForegroundColor Cyan
try { curl.exe -s http://127.0.0.1:9200/v1/nodes } catch { Write-Host "master unreachable" }
Write-Host ""
Write-Host "GW metrics.json:" -ForegroundColor Cyan
try { curl.exe -s http://127.0.0.1:8080/metrics.json } catch { Write-Host "gw unreachable" }
Write-Host ""

if ($fail -gt 0) {
    Write-Host "load_local: $fail process fail" -ForegroundColor Red
    exit 1
}
Write-Host "load_local: all exit 0" -ForegroundColor Green
