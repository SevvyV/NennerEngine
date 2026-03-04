# install_morning_task.ps1 — Register the morning startup in Task Scheduler
# Run this ONCE as Administrator to install the scheduled task

$taskName = "NennerEngine Morning Startup"
$script   = "E:\Workspace\NennerEngine\morning_startup.ps1"

# Remove existing task if present
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

# Trigger: 6:00 AM weekdays only
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "06:00"

# Action: run PowerShell with the startup script
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$script`"" `
    -WorkingDirectory "E:\Workspace\NennerEngine"

# Settings: run whether logged in or not, wake the computer
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -StartWhenAvailable

# Register under current user — will prompt for password
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

Register-ScheduledTask `
    -TaskName $taskName `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -Principal $principal `
    -Description "Launches T1, DataCenter, and dashboard.py at 6:00 AM weekdays for pre-market prep"

Write-Host ""
Write-Host "Task '$taskName' registered successfully." -ForegroundColor Green
Write-Host ""
Write-Host "To test immediately:  schtasks /run /tn `"$taskName`""
