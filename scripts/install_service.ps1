# install_service.ps1 -- Register NennerEngine Dashboard as an NSSM service
# and create a heartbeat watchdog scheduled task.
#
# Run as Administrator:
#   powershell -ExecutionPolicy Bypass -File scripts\install_service.ps1
#
# To uninstall:
#   nssm remove NennerEngineDashboard confirm
#   Unregister-ScheduledTask -TaskName "NennerEngine Heartbeat Watchdog" -Confirm:$false
#
# NOTE: NSSM runs services in Session 0 (isolated from the desktop).
# xlwings/COM features (T1 real-time prices, position reading, signal export)
# will NOT work in service mode. The dashboard handles this gracefully:
#   - T1 prices fall back to cached DB prices (yFinance daily OHLC)
#   - Position cards show empty
#   - Signal export to Excel is skipped
# The web UI, AlertMonitor, and EmailScheduler all work normally.

$ErrorActionPreference = "Stop"

$serviceName  = "NennerEngineDashboard"
$pythonExe    = "E:\Workspace\NennerEngine\.venv\Scripts\python.exe"
$serviceArgs  = "dashboard.py --db E:\Workspace\DataCenter\nenner_signals.db"
$projectDir   = "E:\Workspace\NennerEngine"
$logDir       = "E:\Workspace\NennerEngine\logs"

# Ensure log directory exists
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

# ── 1. Remove old Task Scheduler task if it exists ──
$oldTask = Get-ScheduledTask -TaskName "NennerEngine" -ErrorAction SilentlyContinue
if ($oldTask) {
    Write-Host "Removing old Task Scheduler task 'NennerEngine'..."
    Unregister-ScheduledTask -TaskName "NennerEngine" -Confirm:$false
    Write-Host "  Removed."
}

# ── 2. Kill any running dashboard process ──
# The old Task Scheduler may have left dashboard.py running.
# Find Python processes whose command line includes "dashboard.py" in NennerEngine.
$stale = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" |
    Where-Object { $_.CommandLine -match 'NennerEngine.*dashboard' -or $_.CommandLine -match 'launch_dashboard' }
if ($stale) {
    foreach ($p in $stale) {
        Write-Host "  Killing stale dashboard process PID $($p.ProcessId)..."
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 3
}

# ── 3. Install NSSM service ──
Write-Host "Installing NSSM service '$serviceName'..."

# Remove existing service if reinstalling
$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "  Stopping existing service..."
    nssm stop $serviceName 2>$null
    Start-Sleep -Seconds 3
    nssm remove $serviceName confirm
}

nssm install $serviceName $pythonExe $serviceArgs
nssm set $serviceName AppDirectory $projectDir
nssm set $serviceName DisplayName "NennerEngine Dashboard"
nssm set $serviceName Description "Nenner Signal Engine -- Dash web UI + alert monitor + email scheduler"

# Restart on failure: wait 10s, then restart.
nssm set $serviceName AppExit Default Restart
nssm set $serviceName AppRestartDelay 10000

# Stdout/stderr → log files
nssm set $serviceName AppStdout "$logDir\dashboard_stdout.log"
nssm set $serviceName AppStderr "$logDir\dashboard_stderr.log"
nssm set $serviceName AppStdoutCreationDisposition 4
nssm set $serviceName AppStderrCreationDisposition 4
nssm set $serviceName AppRotateFiles 1
nssm set $serviceName AppRotateSeconds 86400
nssm set $serviceName AppRotateBytes 10485760

# Start automatically on boot
nssm set $serviceName Start SERVICE_AUTO_START

# Environment: inherit .env secrets + set PYTHONUNBUFFERED for real-time logging
nssm set $serviceName AppEnvironmentExtra "PYTHONUNBUFFERED=1"

# Run as LocalSystem (same as FischerDailyMonitor -- Key Vault access via
# DefaultAzureCredential works with environment variables or managed identity)

Write-Host "  NSSM service installed."

# ── 4. Start the service ──
Write-Host "Starting service..."
nssm start $serviceName
Start-Sleep -Seconds 5

# Verify it's running
$svcStatus = nssm status $serviceName
if ($svcStatus -eq "SERVICE_RUNNING") {
    Write-Host "  Service started successfully." -ForegroundColor Green
} else {
    Write-Host "  WARNING: Service status is '$svcStatus'. Check logs:" -ForegroundColor Yellow
    Write-Host "    Get-Content $logDir\dashboard_stderr.log -Tail 30"
}

# ── 5. Verify port 8050 is listening ──
Start-Sleep -Seconds 5
$portCheck = Get-NetTCPConnection -LocalPort 8050 -ErrorAction SilentlyContinue
if ($portCheck) {
    Write-Host "  Dashboard responding on port 8050." -ForegroundColor Green
} else {
    Write-Host "  WARNING: Port 8050 not yet listening. May need a few more seconds to start." -ForegroundColor Yellow
}

# ── 6. Create heartbeat watchdog scheduled task ──
$watchdogTaskName = "NennerEngine Heartbeat Watchdog"
$watchdogPS1      = "E:\Workspace\NennerEngine\scripts\watchdog_heartbeat.ps1"

$existingWatchdog = Get-ScheduledTask -TaskName $watchdogTaskName -ErrorAction SilentlyContinue
if ($existingWatchdog) {
    Unregister-ScheduledTask -TaskName $watchdogTaskName -Confirm:$false
}

Write-Host "Creating heartbeat watchdog task '$watchdogTaskName'..."

$action  = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$watchdogPS1`""

$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 5) `
    -Once -At (Get-Date).Date

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 2)

Register-ScheduledTask `
    -TaskName $watchdogTaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "External heartbeat monitor for NennerEngine -- Telegram alert when dashboard is down" `
    -RunLevel Highest

Write-Host "  Watchdog task created -- runs every 5 min."

# ── Done ──
Write-Host ""
Write-Host "=== Installation complete ===" -ForegroundColor Green
Write-Host "  Service:  $serviceName -- NSSM, auto-restart on failure, auto-start on boot"
Write-Host "  Watchdog: $watchdogTaskName -- Task Scheduler, every 5 min"
Write-Host "  Web UI:   http://127.0.0.1:8050"
Write-Host "  Logs:     $logDir\dashboard_stdout.log"
Write-Host ""
Write-Host "xlwings note:" -ForegroundColor Yellow
Write-Host "  T1 real-time prices, position reading, and signal export are unavailable"
Write-Host "  in service mode -- Session 0 has no desktop/COM access. The dashboard"
Write-Host "  falls back to cached DB prices. To use T1 prices, open the dashboard"
Write-Host "  manually via launch_dashboard.pyw in your interactive session."
Write-Host ""
Write-Host "Useful commands:"
Write-Host "  nssm status $serviceName       # Check service status"
Write-Host "  nssm restart $serviceName      # Manual restart"
Write-Host "  nssm edit $serviceName          # Open NSSM GUI editor"
Write-Host "  Get-Content $logDir\dashboard_stderr.log -Tail 50  # View recent logs"
