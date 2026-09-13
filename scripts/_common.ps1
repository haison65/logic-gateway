# Shared helpers for local topology scripts.
$script:RepoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $script:RepoRoot "go.mod"))) {
    $script:RepoRoot = $PSScriptRoot
}

$script:RunDir = Join-Path $script:RepoRoot "scripts\run"
$script:PidFile = Join-Path $script:RunDir "pids.json"
$script:LogDir = Join-Path $script:RunDir "logs"

$script:NodeDefs = @(
    @{ Name = "master";  Cmd = @("run", "./cmd/master", "-config", "configs/local/master.dev.yaml", "-debug=false"); Port = 9200 },
    @{ Name = "http2gw"; Cmd = @("run", "./cmd/http2gw", "-config", "configs/local/http2gw.dev.yaml", "-debug=false"); Port = 8080 },
    @{ Name = "logic-1"; Cmd = @("run", "./cmd/logic", "-config", "configs/local/logic-1.dev.yaml", "-debug=false"); Port = 9100 },
    @{ Name = "logic-2"; Cmd = @("run", "./cmd/logic", "-config", "configs/local/logic-2.dev.yaml", "-debug=false"); Port = 9101 },
    @{ Name = "logic-3"; Cmd = @("run", "./cmd/logic", "-config", "configs/local/logic-3.dev.yaml", "-debug=false"); Port = 9102 }
)

function Ensure-RunDir {
    New-Item -ItemType Directory -Force -Path $script:RunDir | Out-Null
    New-Item -ItemType Directory -Force -Path $script:LogDir | Out-Null
}

function Read-Pids {
    if (-not (Test-Path $script:PidFile)) {
        return @{}
    }
    $raw = Get-Content $script:PidFile -Raw
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return @{}
    }
    $obj = $raw | ConvertFrom-Json
    $map = @{}
    foreach ($p in $obj.PSObject.Properties) {
        $map[$p.Name] = @{
            Pid       = [int]$p.Value.Pid
            StartedAt = [string]$p.Value.StartedAt
            OutLog    = [string]$p.Value.OutLog
            ErrLog    = [string]$p.Value.ErrLog
        }
    }
    return $map
}

function Write-Pids([hashtable]$map) {
    Ensure-RunDir
    ($map | ConvertTo-Json -Depth 4) | Set-Content -Path $script:PidFile -Encoding UTF8
}

function Stop-Tree([int]$ProcessId) {
    if ($ProcessId -le 0) { return }
    try {
        # Kill process tree (go run + child binary)
        & taskkill.exe /PID $ProcessId /T /F 2>$null | Out-Null
    } catch {}
}

function Wait-HttpOk([string]$Url, [int]$TimeoutSec = 30) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 300) {
                return $true
            }
        } catch {}
        Start-Sleep -Milliseconds 300
    }
    return $false
}
