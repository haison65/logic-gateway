# Kill one tracked node by name (master|http2gw|logic-1|logic-2|logic-3).
#   .\scripts\kill_node.ps1 -Name logic-1
# Useful for failover drills: kill logic-1 then send traffic.

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("master", "http2gw", "logic-1", "logic-2", "logic-3")]
    [string]$Name
)

$ErrorActionPreference = "Stop"
. "$PSScriptRoot\_common.ps1"
Set-Location $script:RepoRoot

$map = Read-Pids
if (-not $map.ContainsKey($Name)) {
    Write-Host "Node '$Name' not in pids.json. Start with .\scripts\start_local.ps1" -ForegroundColor Red
    exit 2
}
$procId = [int]$map[$Name].Pid
Write-Host "KILL $Name pid=$procId" -ForegroundColor Yellow
Stop-Tree $procId
$map.Remove($Name)
Write-Pids $map
Write-Host "Done. Check: .\scripts\status_local.ps1" -ForegroundColor Green
