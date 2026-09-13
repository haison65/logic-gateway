# Optional: raise Docker VM rmem_max/wmem_max when the kernel exposes them.
# WSL2/Docker Desktop often has no net.core.rmem_max — SO_RCVBUFFORCE + NET_ADMIN covers that.

[CmdletBinding()]
param(
    [long]$RmemMax = 67108864,
    [long]$WmemMax = 67108864,
    [switch]$Quiet
)

$ErrorActionPreference = "Continue"

function Write-Info([string]$Msg, [string]$Color = "Gray") {
    if (-not $Quiet) { Write-Host $Msg -ForegroundColor $Color }
}

function Test-RmemMaxExists {
    $out = & docker run --rm --privileged --pid=host justincormack/nsenter1 `
        /bin/sh -c "test -e /proc/sys/net/core/rmem_max && echo yes || echo no" 2>&1
    return (("$out").Trim() -eq "yes")
}

function Set-ViaNsenter([string]$Name, [long]$Value) {
    $out = & docker run --rm --privileged --pid=host justincormack/nsenter1 `
        /sbin/sysctl -w "$Name=$Value" 2>&1
    return @{ Ok = ($LASTEXITCODE -eq 0); Out = ("$out").Trim() }
}

function Get-ViaNsenter([string]$Name) {
    $out = & docker run --rm --privileged --pid=host justincormack/nsenter1 /sbin/sysctl -n $Name 2>&1
    if ($LASTEXITCODE -ne 0) { return $null }
    $n = 0L
    if ([long]::TryParse(("$out").Trim(), [ref]$n)) { return $n }
    return $null
}

$null = & docker image inspect justincormack/nsenter1 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Info "Pulling justincormack/nsenter1 ..." "DarkGray"
    & docker pull justincormack/nsenter1 2>&1 | Out-Host
}

if (-not (Test-RmemMaxExists)) {
    Write-Info "UDP buffers: no net.core.rmem_max (using SO_RCVBUFFORCE)" "DarkGray"
    exit 0
}

$r1 = Set-ViaNsenter "net.core.rmem_max" $RmemMax
$r2 = Set-ViaNsenter "net.core.wmem_max" $WmemMax
if (-not $r1.Ok -or -not $r2.Ok) {
    Write-Host "WARN sysctl failed; fallback SO_RCVBUFFORCE" -ForegroundColor Yellow
    exit 0
}

$gotR = Get-ViaNsenter "net.core.rmem_max"
$gotW = Get-ViaNsenter "net.core.wmem_max"
Write-Info ("UDP buffers: rmem_max={0} wmem_max={1}" -f $gotR, $gotW) "Green"
exit 0
