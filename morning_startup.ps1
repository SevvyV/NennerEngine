# morning_startup.ps1 -- Pre-market startup
# Scheduled via Task Scheduler at 8:00 AM ET, weekdays only
# Launches: T1 (LSEG ONE) with auto-login and RTD health check
# DataCenter, FischerDaily monitor, and NennerDashboard are separate scheduled tasks

$ErrorActionPreference = "Continue"

$logDir  = "E:\Workspace\NennerEngine\logs"
$logFile = Join-Path $logDir "morning_startup.log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $msg" | Tee-Object -FilePath $logFile -Append
}

Log "=== Morning startup begin ==="

# --- 1. LSEG ONE (Thomson One) — always restart fresh ---
$t1Exe  = "C:\Users\sevag\AppData\Local\LSEG Wealth\Install\LSEG ONE-7.1.3.0\LSEG ONE.exe"
$t1Proc = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue

if ($t1Proc) {
    Log "T1: Running (PID $($t1Proc.Id)) -- closing for fresh restart..."
    $t1Proc | Stop-Process -Force
    Start-Sleep -Seconds 5

    # Verify it's gone
    $t1Still = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue
    if ($t1Still) {
        Log "T1: Still alive after kill -- waiting 10s..."
        Start-Sleep -Seconds 10
    }
    Log "T1: Closed."
} else {
    Log "T1: Not running."
}

Log "T1: Launching fresh..."
Start-Process -FilePath $t1Exe

# Wait for the login window to appear
Log "T1: Waiting for login window..."
$wsh = New-Object -ComObject WScript.Shell
$loginReady = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 2
    $activated = $wsh.AppActivate("LSEG")
    if ($activated) { $loginReady = $true; break }
}

if ($loginReady) {
    Log "T1: Login window detected -- waiting for login form to fully render..."
    Start-Sleep -Seconds 15

    Log "T1: Sending credentials..."
    $wsh.SendKeys("sevagv@gmail.com")
    Start-Sleep -Seconds 1

    $wsh.SendKeys("{TAB}")
    Start-Sleep -Seconds 2

    $wsh.SendKeys("{ENTER}")

    Log "T1: Sign-in keys sent. Waiting 5 min for early health check..."
} else {
    Log "T1: WARNING -- login window not detected after 60s. Sign in manually."
}

# Early health check at ~5 min — catch cold-start crashes quickly
Start-Sleep -Seconds 300

$t1Early = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue
if ($t1Early) {
    Log "T1: Early check passed (PID $($t1Early.Id)). Waiting 10 more min..."
    Start-Sleep -Seconds 600
} else {
    Log "T1: Crashed after launch — relaunching and re-sending credentials..."
    Start-Process -FilePath $t1Exe

    # Wait for login window again
    $loginReady2 = $false
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 2
        $activated2 = $wsh.AppActivate("LSEG")
        if ($activated2) { $loginReady2 = $true; break }
    }

    if ($loginReady2) {
        Log "T1: Login window detected on relaunch -- waiting for form to render..."
        Start-Sleep -Seconds 15
        Log "T1: Sending credentials (attempt 2)..."
        $wsh.SendKeys("sevagv@gmail.com")
        Start-Sleep -Seconds 1
        $wsh.SendKeys("{TAB}")
        Start-Sleep -Seconds 2
        $wsh.SendKeys("{ENTER}")
        Log "T1: Sign-in keys sent (attempt 2). Waiting 10 min..."
    } else {
        Log "T1: WARNING -- login window not detected on relaunch. Sign in manually."
    }

    Start-Sleep -Seconds 600
}

# Final verification
$t1Check = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue
if ($t1Check) {
    Log "T1: Confirmed running (PID $($t1Check.Id))"
} else {
    Log "T1: FAILED -- not running after both attempts. Sign in manually."
}

# DataCenter, FischerDaily monitor, and NennerDashboard are now separate
# scheduled tasks at 8:15, 8:25, and 8:30 AM ET respectively.

Log "=== Morning startup complete ==="
