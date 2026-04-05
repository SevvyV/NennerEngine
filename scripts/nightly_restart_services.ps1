# nightly_restart_services.ps1 -- Restart all NSSM services
# Scheduled via Task Scheduler at 3:00 AM ET, daily
# Failsafe: clears dead threads, memory leaks, accumulated state

$ErrorActionPreference = "Continue"

$logDir  = "E:\Workspace\NennerEngine\logs"
$logFile = Join-Path $logDir "nightly_restart.log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $msg" | Tee-Object -FilePath $logFile -Append
}

$services = @(
    "NennerEngineMonitor",
    "NennerEngineAPI",
    "NennerEngineDashboard",
    "ServiceBot"
)

Log "=== Nightly service restart begin ==="

foreach ($svc in $services) {
    $status = (Get-Service -Name $svc -ErrorAction SilentlyContinue).Status
    if ($null -eq $status) {
        Log "SKIP  $svc -- service not found"
        continue
    }

    Log "RESTART  $svc (was $status)"
    Restart-Service -Name $svc -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 5

    $newStatus = (Get-Service -Name $svc).Status
    if ($newStatus -eq "Running") {
        Log "OK    $svc is Running"
    } else {
        Log "WARN  $svc is $newStatus after restart"
    }
}

Log "=== Nightly service restart complete ==="
