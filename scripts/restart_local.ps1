# Restart local topology.
#   .\scripts\restart_local.ps1

$ErrorActionPreference = "Stop"
& "$PSScriptRoot\stop_local.ps1"
Start-Sleep -Seconds 1
& "$PSScriptRoot\start_local.ps1"
