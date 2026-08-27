# P0/local: Master + 1x http2gw + 2x Logic (go run, cua so rieng).
# Chay tu root repo:  .\scripts\start_local_p0.ps1
#
# Topology:
#   master   HTTP 9200
#   http2gw  TCP 8080 / UDP 9000  (configs/local)
#   logic-1  UDP 9100  node_id=2
#   logic-2  UDP 9101  node_id=3

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "go.mod"))) {
    $Root = $PSScriptRoot
}
Set-Location $Root

function Start-GoWindow {
    param(
        [string]$Title,
        [string]$Command
    )
    $ps = @"
`$Host.UI.RawUI.WindowTitle = '$Title'
Set-Location '$Root'
Write-Host '=== $Title ===' -ForegroundColor Cyan
Write-Host '$Command' -ForegroundColor DarkGray
$Command
Write-Host ''
Write-Host 'Process exited. Press Enter to close.' -ForegroundColor Yellow
Read-Host
"@
    Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $ps) | Out-Null
}

Write-Host "Start: master + http2gw + logic-1 + logic-2 (repo: $Root)" -ForegroundColor Green

Start-GoWindow -Title "master" -Command "go run ./cmd/master -config configs/local/master.dev.yaml -debug=false"
Start-Sleep -Seconds 2
Start-GoWindow -Title "http2gw" -Command "go run ./cmd/http2gw -config configs/local/http2gw.dev.yaml -debug=false"
Start-Sleep -Seconds 2
Start-GoWindow -Title "logic-1" -Command "go run ./cmd/logic -config configs/local/logic-1.dev.yaml -debug=false"
Start-Sleep -Seconds 1
Start-GoWindow -Title "logic-2" -Command "go run ./cmd/logic -config configs/local/logic-2.dev.yaml -debug=false"

Write-Host ""
Write-Host "Da mo 4 cua so. Kiem tra Master inventory:" -ForegroundColor Green
Write-Host "  curl.exe -s http://127.0.0.1:9200/v1/nodes"
Write-Host "Smoke:"
Write-Host "  go run ./cmd/client -n 1 -unique-session -body hello"
Write-Host "Load (co identity):"
Write-Host "  go run ./cmd/client -config configs/local/perf-1.dev.yaml"
Write-Host "  .\scripts\load_local_p0.ps1"
