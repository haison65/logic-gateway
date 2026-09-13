# Load: .\scripts\start.ps1  [-Clients N] [-ClientCPUs N] [-Duration 60s]
# Config: configs/local/load.request.yaml

[CmdletBinding()]
param(
    [string]$Duration = "",
    [int]$Clients = 0,
    [double]$ClientCPUs = 0
)

$ErrorActionPreference = "Stop"
$loadArgs = @{}
if ($Duration -ne "") { $loadArgs["Duration"] = $Duration }
if ($Clients -gt 0) { $loadArgs["Clients"] = $Clients }
if ($ClientCPUs -gt 0) { $loadArgs["ClientCPUs"] = $ClientCPUs }
& "$PSScriptRoot\start_load.ps1" @loadArgs
exit $LASTEXITCODE
