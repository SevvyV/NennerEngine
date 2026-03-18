# watchdog_heartbeat.ps1 -- External health monitor for NennerEngine Dashboard
# Runs via Task Scheduler every 5 minutes. Checks the /health endpoint which
# reports thread status. Alerts via Telegram if dashboard is down or threads
# are dead.
#
# Install: see install_service.ps1

$port         = 8050
$healthUrl    = "http://127.0.0.1:${port}/health"
$pythonExe    = "E:\Workspace\NennerEngine\.venv\Scripts\python.exe"
$projectDir   = "E:\Workspace\NennerEngine"
$timeout      = 10

# --- Check /health endpoint ---
try {
    $response = Invoke-WebRequest -Uri $healthUrl -TimeoutSec $timeout -UseBasicParsing -ErrorAction Stop
    if ($response.StatusCode -eq 200) {
        # All threads healthy -- exit silently
        exit 0
    }
    # 503 = dashboard up but threads dead
    $body = $response.Content | ConvertFrom-Json
    $deadThreads = ($body.threads.PSObject.Properties | Where-Object { $_.Value -eq $false } | ForEach-Object { $_.Name }) -join ", "
    $reason = "Dashboard running but threads dead: $deadThreads"
} catch [System.Net.WebException] {
    # HTTP error -- check if 503
    $webResponse = $_.Exception.Response
    if ($webResponse -and $webResponse.StatusCode.value__ -eq 503) {
        try {
            $reader = New-Object System.IO.StreamReader($webResponse.GetResponseStream())
            $body = $reader.ReadToEnd() | ConvertFrom-Json
            $reader.Close()
            $deadThreads = ($body.threads.PSObject.Properties | Where-Object { $_.Value -eq $false } | ForEach-Object { $_.Name }) -join ", "
            $reason = "Dashboard running but threads dead: $deadThreads"
        } catch {
            $reason = "Dashboard returned HTTP 503 -- threads unhealthy"
        }
    } else {
        # Connection refused or other network error
        $svcStatus = nssm status NennerEngineDashboard 2>&1
        if ($svcStatus -eq "SERVICE_RUNNING") {
            $reason = "Dashboard service running but port $port not responding"
        } elseif ($svcStatus -eq "SERVICE_STOPPED") {
            $reason = "Dashboard service is STOPPED"
        } elseif ($svcStatus -eq "SERVICE_PAUSED") {
            $reason = "Dashboard service is PAUSED"
        } else {
            $reason = "Dashboard service status: $svcStatus"
        }
    }
} catch {
    $svcStatus = nssm status NennerEngineDashboard 2>&1
    $reason = "Health check failed -- service status: $svcStatus"
}

# --- Send Telegram alert via Python ---
$alertScript = @"
import sys
sys.path.insert(0, r'$projectDir')
from nenner_engine.alert_dispatch import send_telegram, get_telegram_config
token, chat_id = get_telegram_config()
if token and chat_id:
    from datetime import datetime
    ts = datetime.now().strftime('%I:%M %p')
    send_telegram(
        f'\U0001f6a8 WATCHDOG: NennerEngine dashboard DOWN at {ts} ET\n\n$reason\n\nNSSM should auto-restart the service. If this repeats, check logs.',
        token, chat_id,
    )
    print('Alert sent')
else:
    print('No Telegram config')
"@

& $pythonExe -c $alertScript
