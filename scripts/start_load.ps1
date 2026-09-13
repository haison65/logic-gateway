# Load N HTTP2 clients -> HTTP2GW -> 3 Logic.
# Config: configs/local/load.request.yaml
# Usage:  .\scripts\start.ps1 [-Clients N] [-ClientCPUs N] [-Duration 60s]

[CmdletBinding()]
param(
    [string]$Duration = "",
    [int]$Clients = 0,
    [double]$ClientCPUs = 0
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "go.mod"))) {
    $Root = $PSScriptRoot
}
Set-Location $Root

$ComposeFile = Join-Path $Root "docker-compose.local.yml"
$RequestFile = Join-Path $Root "configs\local\load.request.yaml"
$MaxClients = 6
$DefaultClients = 3
$DefaultClientCPUs = 2.0

if (-not (Test-Path $RequestFile)) {
    Write-Host "MISSING $RequestFile" -ForegroundColor Red
    exit 2
}

function Read-LoadRequest([string]$Path) {
    $raw = Get-Content -Path $Path -Raw
    if ($raw.Length -gt 0 -and [int][char]$raw[0] -eq 0xFEFF) {
        $raw = $raw.Substring(1)
    }
    $cfg = @{
        client_count   = "$DefaultClients"
        client_cpus    = "$DefaultClientCPUs"
        message_id     = "1001"
        unique_session = "true"
        body           = "hello"
        body_size      = "0"
        timeout        = "10s"
        concurrency    = "1"
        qps            = "0"
        duration       = "30s"
        n              = "0"
        verbose        = "false"
        target         = ""
        session_id     = ""
        master_url     = ""
        fail_log_path  = ""
        fail_log_max   = "100000"
    }
    foreach ($line in ($raw -split "`n")) {
        $t = $line.Trim()
        if ($t -eq "" -or $t.StartsWith("#")) { continue }
        $i = $t.IndexOf(":")
        if ($i -lt 1) { continue }
        $k = $t.Substring(0, $i).Trim()
        $v = $t.Substring($i + 1).Trim()
        if ($v.StartsWith('"') -and $v.EndsWith('"') -and $v.Length -ge 2) {
            $v = $v.Substring(1, $v.Length - 2)
        } elseif ($v.StartsWith("'") -and $v.EndsWith("'") -and $v.Length -ge 2) {
            $v = $v.Substring(1, $v.Length - 2)
        }
        $hash = $v.IndexOf("#")
        if ($hash -ge 0) { $v = $v.Substring(0, $hash).Trim() }
        if ($cfg.ContainsKey($k)) {
            $cfg[$k] = $v
        }
    }
    $size = 0
    [void][int]::TryParse([string]$cfg["body_size"], [ref]$size)
    if ($size -gt 0) {
        $cfg["body"] = "x" * $size
    }
    return $cfg
}

$req = Read-LoadRequest $RequestFile
if ($Duration -ne "") { $req["duration"] = $Duration }

$flagClients = $Clients
$flagClientCPUs = $ClientCPUs

$numClients = $DefaultClients
$fileCount = 0
if ([int]::TryParse(([string]$req["client_count"]).Trim(), [ref]$fileCount) -and $fileCount -gt 0) {
    $numClients = $fileCount
}
if ($flagClients -gt 0) { $numClients = $flagClients }

$cpuPerClient = $DefaultClientCPUs
$fileCPUs = 0.0
if ([double]::TryParse(([string]$req["client_cpus"]).Trim(), [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$fileCPUs) -and $fileCPUs -gt 0) {
    $cpuPerClient = $fileCPUs
}
if ($flagClientCPUs -gt 0) { $cpuPerClient = $flagClientCPUs }

if ($numClients -lt 1 -or $numClients -gt $MaxClients) {
    Write-Host "client_count=$numClients invalid (1..$MaxClients)" -ForegroundColor Red
    exit 2
}
if ($cpuPerClient -le 0) {
    Write-Host "client_cpus must be > 0" -ForegroundColor Red
    exit 2
}

$gomax = [Math]::Max(1, [int][Math]::Ceiling($cpuPerClient))

function Test-YamlBool([string]$v) {
    $x = $v.ToLower()
    return ($x -eq "true" -or $x -eq "1" -or $x -eq "yes")
}

# Docker writes progress to stderr; with $ErrorActionPreference=Stop that becomes a terminating error.
function Invoke-DockerQuiet([string[]]$DockerArgs) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & docker @DockerArgs *> $null
        return $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $prev
    }
}

$uniqFlag = if (Test-YamlBool ([string]$req["unique_session"])) { "true" } else { "false" }

function New-ClientArgs([int]$Index) {
    $list = [System.Collections.Generic.List[string]]::new()
    $target = [string]$req["target"]
    if ($target.Trim() -eq "") {
        $target = "http://http2gw:8080"
    }
    $list.AddRange([string[]]@(
        "-config", "/configs/client.yaml",
        "-addr", $target.Trim(),
        "-message-id", [string]$req["message_id"],
        "-timeout", [string]$req["timeout"],
        "-c", [string]$req["concurrency"],
        "-qps", [string]$req["qps"],
        "-d", [string]$req["duration"],
        "-n", [string]$req["n"],
        "-body", [string]$req["body"],
        "-unique-session=$uniqFlag"
    ))
    if (Test-YamlBool ([string]$req["verbose"])) {
        $list.Add("-v")
    }
    $sess = [string]$req["session_id"]
    if ($sess.Trim() -ne "") {
        $list.Add("-session-id")
        $list.Add(("{0}-{1}" -f $sess.Trim(), $Index))
    }
    $master = [string]$req["master_url"]
    if ($master.Trim() -eq "") {
        $master = "http://master:9200"
    }
    $list.Add("-master-url")
    $list.Add($master.Trim())
    $list.Add("-fail-log")
    $list.Add("/var/log/logic-gateway/client-$Index-fails.jsonl")
    $failMax = [string]$req["fail_log_max"]
    if ($failMax.Trim() -eq "") { $failMax = "100000" }
    $list.Add("-fail-log-max")
    $list.Add($failMax)
    $list.Add("-summary-log")
    $list.Add("/var/log/run-logs/${stamp}_client-$Index.summary.json")
    $list.Add("-client-name")
    $list.Add("client-$Index")
    $list.Add("-client-cpus")
    $list.Add($cpuPerClient.ToString([Globalization.CultureInfo]::InvariantCulture))
    return $list.ToArray()
}

function Start-DockerComposeRun([string[]]$ArgumentList, [string]$LogPath, [string]$Prefix) {
    $quoted = foreach ($a in $ArgumentList) {
        if ($null -eq $a) { '""'; continue }
        $s = [string]$a
        if ($s -match '[\s"]') {
            '"' + ($s.Replace('"', '\"')) + '"'
        } else {
            $s
        }
    }
    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = "docker"
    $psi.WorkingDirectory = $Root
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true
    $psi.Arguments = ($quoted -join " ")

    $p = [System.Diagnostics.Process]::new()
    $p.StartInfo = $psi
    $p.EnableRaisingEvents = $true

    $utf8 = [System.Text.UTF8Encoding]::new($false)
    $state = [hashtable]::Synchronized(@{
        Writer = [System.IO.StreamWriter]::new($LogPath, $false, $utf8)
        Prefix = $Prefix
        Lock   = New-Object object
    })
    $state.Writer.AutoFlush = $true

    $action = {
        if ($null -eq $EventArgs.Data) { return }
        $st = $Event.MessageData
        $raw = [string]$EventArgs.Data
        $line = "[{0}] {1}" -f $st.Prefix, $raw
        [System.Threading.Monitor]::Enter($st.Lock)
        try {
            $st.Writer.WriteLine($raw)
            [Console]::Out.WriteLine($line)
        } finally {
            [System.Threading.Monitor]::Exit($st.Lock)
        }
    }

    $outEvent = Register-ObjectEvent -InputObject $p -EventName OutputDataReceived -Action $action -MessageData $state
    $errEvent = Register-ObjectEvent -InputObject $p -EventName ErrorDataReceived -Action $action -MessageData $state
    if (-not $p.Start()) { return $null }
    $p.BeginOutputReadLine()
    $p.BeginErrorReadLine()

    return [pscustomobject]@{
        Process  = $p
        LogPath  = $LogPath
        OutEvent = $outEvent
        ErrEvent = $errEvent
        State    = $state
    }
}

$clientNames = [System.Collections.Generic.List[string]]::new()
for ($n = 1; $n -le $numClients; $n++) {
    [void]$clientNames.Add("client-$n")
}

$concurrencyPer = 0
$qpsPer = 0.0
[void][int]::TryParse([string]$req["concurrency"], [ref]$concurrencyPer)
[void][double]::TryParse([string]$req["qps"], [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref]$qpsPer)

Write-Host ("Load: {0}x client @{1}CPU | c={2} qps={3} d={4} | total qps~{5}" -f `
    $numClients, $cpuPerClient, $req["concurrency"], $req["qps"], $req["duration"], ($qpsPer * $numClients)) -ForegroundColor Green

$FailLogDir = Join-Path $Root "scripts\fail-logs"
$RunLogDir = Join-Path $Root "scripts\run-logs"
New-Item -ItemType Directory -Force -Path $FailLogDir | Out-Null
New-Item -ItemType Directory -Force -Path $RunLogDir | Out-Null

& "$PSScriptRoot\set_docker_udp_buffers.ps1" -Quiet
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARN set_docker_udp_buffers failed" -ForegroundColor Yellow
}

Write-Host "Stack up..." -ForegroundColor DarkGray
$code = Invoke-DockerQuiet @(
    "compose", "-f", $ComposeFile, "up", "-d", "--build",
    "master", "http2gw", "logic-1", "logic-2", "logic-3", "prometheus"
)
if ($code -ne 0) {
    Write-Host "docker compose up failed" -ForegroundColor Red
    exit 1
}

$code = Invoke-DockerQuiet @(
    "compose", "-f", $ComposeFile, "restart",
    "http2gw", "logic-1", "logic-2", "logic-3"
)
if ($code -ne 0) {
    Write-Host "docker compose restart failed" -ForegroundColor Red
    exit 1
}

$deadline = (Get-Date).AddSeconds(40)
$ready = $false
while ((Get-Date) -lt $deadline) {
    try {
        $r = Invoke-WebRequest -Uri "http://127.0.0.1:8080/healthz" -UseBasicParsing -TimeoutSec 2
        if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 300) {
            $ready = $true
            break
        }
    } catch {}
    Start-Sleep -Milliseconds 400
}
if (-not $ready) {
    Write-Host "http2gw healthz not ready" -ForegroundColor Red
    exit 1
}

$prevEA = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$bufLine = docker logs logic-gateway-http2gw-1 2>&1 | Select-String -Pattern "udp socket buffers" | Select-Object -Last 1
$ErrorActionPreference = $prevEA
if ($bufLine) {
    $rcv = 0L
    $lineText = [string]$bufLine.Line
    if ($lineText -match "actual_rcvbuf_after_force.:(\d+)") {
        $rcv = [long]$Matches[1]
    }
    elseif ($lineText -match "so_rcvbuf.:(\d+)") {
        $rcv = [long]$Matches[1]
    }
    if ($rcv -gt 0 -and $rcv -lt 1048576) {
        Write-Host ("WARN rcvbuf={0} < 1MiB" -f $rcv) -ForegroundColor Yellow
    }
}

Start-Sleep -Seconds 3

$code = Invoke-DockerQuiet @("compose", "-f", $ComposeFile, "--profile", "load", "build", "client-1")
if ($code -ne 0) {
    Write-Host "build client failed" -ForegroundColor Red
    exit 1
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$jobs = [System.Collections.Generic.List[object]]::new()
$i = 0
$cpusArg = $cpuPerClient.ToString([Globalization.CultureInfo]::InvariantCulture)

$overridePath = Join-Path $RunLogDir ("{0}_client-cpus.override.yml" -f $stamp)
$clientCpuset = @{
    "client-1" = "9-10"
    "client-2" = "11-12"
    "client-3" = "13-14"
    "client-4" = "15-16"
    "client-5" = "17-18"
    "client-6" = "9-10"
}
$ov = [System.Collections.Generic.List[string]]::new()
$ov.Add("services:")
foreach ($name in $clientNames) {
    $set = $clientCpuset[$name]
    if (-not $set) { $set = "9-10" }
    $ov.Add("  ${name}:")
    $ov.Add("    cpus: $cpusArg")
    $ov.Add("    cpuset: `"$set`"")
    $ov.Add("    environment:")
    $ov.Add("      GOMAXPROCS: `"$gomax`"")
}
[System.IO.File]::WriteAllLines($overridePath, $ov.ToArray())

foreach ($name in $clientNames) {
    $i++
    $runList = [System.Collections.Generic.List[string]]::new()
    $runList.AddRange([string[]]@(
        "compose",
        "-f", $ComposeFile,
        "-f", $overridePath,
        "--profile", "load",
        "run", "--rm", "--no-deps",
        "-e", "GOMAXPROCS=$gomax",
        "--name", "logic-gateway-$name-load-$stamp",
        $name
    ))
    $runList.AddRange([string[]](New-ClientArgs -Index $i))
    $clientLog = Join-Path $RunLogDir ("{0}_client-{1}.console.log" -f $stamp, $i)
    Write-Host ("START {0}" -f $name) -ForegroundColor Cyan
    $started = Start-DockerComposeRun -ArgumentList $runList.ToArray() -LogPath $clientLog -Prefix $name
    if ($null -eq $started -or $null -eq $started.Process) {
        Write-Host ("START {0} failed" -f $name) -ForegroundColor Red
        exit 1
    }
    [void]$jobs.Add([pscustomobject]@{
        Name     = $name
        Process  = $started.Process
        Index    = $i
        LogPath  = $clientLog
        OutEvent = $started.OutEvent
        ErrEvent = $started.ErrEvent
        State    = $started.State
    })
}

foreach ($j in $jobs) {
    $j.Process.WaitForExit()
}
Start-Sleep -Milliseconds 400
foreach ($j in $jobs) {
    if ($null -ne $j.OutEvent) {
        Unregister-Event -SourceIdentifier $j.OutEvent.Name -ErrorAction SilentlyContinue
        Remove-Job $j.OutEvent -Force -ErrorAction SilentlyContinue
    }
    if ($null -ne $j.ErrEvent) {
        Unregister-Event -SourceIdentifier $j.ErrEvent.Name -ErrorAction SilentlyContinue
        Remove-Job $j.ErrEvent -Force -ErrorAction SilentlyContinue
    }
    if ($null -ne $j.State -and $null -ne $j.State.Writer) {
        $j.State.Writer.Flush()
        $j.State.Writer.Dispose()
    }
}

$fail = 0
foreach ($j in $jobs) {
    $code = $j.Process.ExitCode
    if ($null -eq $code) { $code = -1 }
    $color = "Green"
    if ($code -ne 0) { $color = "Red"; $fail++ }
    Write-Host ("DONE {0} exit={1}" -f $j.Name, $code) -ForegroundColor $color
}

$aggPath = Join-Path $RunLogDir ("{0}_aggregate.json" -f $stamp)
$clientSummaries = @()
$totalReq = 0; $totalOk = 0; $totalFail = 0
$maxElapsed = 0.0
foreach ($j in $jobs) {
    $sumFile = Join-Path $RunLogDir ("{0}_client-{1}.summary.json" -f $stamp, $j.Index)
    if (-not (Test-Path $sumFile)) {
        Write-Host ("MISSING summary: client-{0}" -f $j.Index) -ForegroundColor DarkYellow
        continue
    }
    $obj = Get-Content -Raw $sumFile | ConvertFrom-Json
    $clientSummaries += $obj
    $totalReq += [int]$obj.requests
    $totalOk += [int]$obj.ok
    $totalFail += [int]$obj.fail
    if ([double]$obj.elapsed_sec -gt $maxElapsed) { $maxElapsed = [double]$obj.elapsed_sec }
    Write-Host ("  {0}: req={1} ok={2} fail={3} rps={4:N1} success={5:N2}%" -f `
        $obj.client, $obj.requests, $obj.ok, $obj.fail, $obj.rps, $obj.success_rate_pct) -ForegroundColor Cyan
}
$aggRps = 0.0
$aggSuccess = 0.0
if ($maxElapsed -gt 0 -and $totalReq -gt 0) { $aggRps = $totalReq / $maxElapsed }
if ($totalReq -gt 0) { $aggSuccess = 100.0 * $totalOk / $totalReq }
$aggregate = [ordered]@{
    stamp            = $stamp
    client_count     = $numClients
    client_cpus      = $cpuPerClient
    requests         = $totalReq
    ok               = $totalOk
    fail             = $totalFail
    rps              = [math]::Round($aggRps, 3)
    success_rate_pct = [math]::Round($aggSuccess, 4)
    elapsed_sec_max  = [math]::Round($maxElapsed, 3)
    clients          = $clientSummaries
}
($aggregate | ConvertTo-Json -Depth 8) | Set-Content -Path $aggPath -Encoding UTF8

Write-Host ("Aggregate: req={0} ok={1} fail={2} rps~={3:N1} success={4:N2}%" -f `
    $totalReq, $totalOk, $totalFail, $aggRps, $aggSuccess) -ForegroundColor Green
Write-Host $aggPath -ForegroundColor DarkCyan

if ($fail -gt 0) {
    Write-Host "FAIL: $fail client" -ForegroundColor Red
    exit 1
}
Write-Host "OK" -ForegroundColor Green
