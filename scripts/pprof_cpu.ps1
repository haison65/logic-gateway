# Capture CPU pprof from http2gw (P0). Prefer curl.exe -o (avoid PowerShell UTF-16 redirect).
param(
    [string]$Addr = "http://127.0.0.1:6060",
    [int]$Seconds = 30,
    [string]$Out = "cpu.pb.gz"
)

$ErrorActionPreference = "Stop"
$url = "$Addr/debug/pprof/profile?seconds=$Seconds"
Write-Host "Capturing CPU profile ${Seconds}s -> $Out"
Write-Host "  $url"
curl.exe -sS -o $Out $url
Write-Host "Done. Open with: go tool pprof -http=:8081 $Out"
Write-Host "Also useful: $Addr/debug/pprof/heap  block  mutex  allocs"
