# morning_startup.ps1 -- Pre-market startup
# Scheduled via Task Scheduler at 8:00 AM ET, weekdays only
#
# T1 (LSEG ONE) and DataCenter Excel removed 2026-03-23 —
# DataBento equity stream replaced T1 as the primary spot price source.
# FischerDaily monitor and NennerEngine are separate scheduled tasks.

$ErrorActionPreference = "Continue"

$logDir  = "E:\Workspace\NennerEngine\logs"
$logFile = Join-Path $logDir "morning_startup.log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $msg" | Tee-Object -FilePath $logFile -Append
}

Log "=== Morning startup begin ==="
Log "No processes to launch -- FischerDaily and NennerEngine are NSSM services."
Log "=== Morning startup complete ==="
