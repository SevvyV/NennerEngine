# install_scheduled_task.ps1
# ============================
# Creates a Windows Scheduled Task to auto-launch the NennerEngine dashboard
# at 6:50 AM ET every weekday (Mon-Fri), ensuring the 7:00 AM stock report
# is never missed.
#
# Run as Administrator:
#   Right-click PowerShell -> "Run as administrator"
#   cd E:\Workspace\NennerEngine
#   .\install_scheduled_task.ps1

$TaskName = "NennerDashboard"
$PythonW = "C:\Users\sevag\AppData\Local\Python\pythoncore-3.14-64\pythonw.exe"
$Script = "E:\Workspace\NennerEngine\launch_dashboard.pyw"
$WorkDir = "E:\Workspace\NennerEngine"

# Remove existing task if present
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# Action: launch dashboard via pythonw (no console window)
$Action = New-ScheduledTaskAction `
    -Execute $PythonW `
    -Argument "`"$Script`"" `
    -WorkingDirectory $WorkDir

# Trigger: 6:50 AM every weekday (Mon-Fri)
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "06:50"

# Settings: allow start if missed (e.g. PC was off), don't stop on idle,
# allow running on battery, don't start a new instance if already running
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 14)

# Principal: run as current user, only when logged on (no stored password needed)
$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

# Register the task
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description "Auto-launches NennerEngine dashboard at 6:50 AM ET weekdays (before 7:00 AM stock report)"

Write-Host ""
Write-Host "Scheduled task '$TaskName' created successfully!" -ForegroundColor Green
Write-Host "  Schedule: Mon-Fri at 6:50 AM"
Write-Host "  Action:   $PythonW `"$Script`""
Write-Host "  StartWhenAvailable: Yes (catches up if PC was off)"
Write-Host ""
Write-Host "Verify with: Get-ScheduledTask -TaskName '$TaskName' | Format-List"
