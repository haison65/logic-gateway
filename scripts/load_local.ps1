# Chay tai (wrapper). Chinh: configs/local/load.request.yaml
#   .\scripts\load_local.ps1
#   .\scripts\load_local.ps1 -Clients 2 -ClientCPUs 1 -Duration 60s

[CmdletBinding()]
param(
    [string]$Duration = "",
    [int]$Clients = 0,
    [double]$ClientCPUs = 0
)

$ErrorActionPreference = "Stop"
& "$PSScriptRoot\start_load.ps1" -Duration $Duration -Clients $Clients -ClientCPUs $ClientCPUs
exit $LASTEXITCODE
