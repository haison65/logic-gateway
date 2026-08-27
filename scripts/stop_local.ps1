# Stop all PID-tracked local nodes.
#   .\scripts\stop_local.ps1

$ErrorActionPreference = "Continue"
. "$PSScriptRoot\_common.ps1"
Set-Location $script:RepoRoot

$map = Read-Pids
if ($map.Count -eq 0) {
    Write-Host "No pids.json — nothing to stop (or use kill by name)." -ForegroundColor Yellow
    exit 0
}

foreach ($name in @($map.Keys)) {
    $procId = [int]$map[$name].Pid
    Write-Host "STOP $name pid=$procId" -ForegroundColor Cyan
    Stop-Tree $procId
}
Remove-Item -Force -ErrorAction SilentlyContinue $script:PidFile
Write-Host "Stopped." -ForegroundColor Green
