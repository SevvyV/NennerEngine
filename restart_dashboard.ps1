# Kill existing dashboard and relaunch
Get-Process pythonw -ErrorAction SilentlyContinue | Stop-Process -Force
Write-Host "Killed old dashboard"
Start-Sleep -Seconds 2

Start-Process pythonw.exe -ArgumentList "E:\Workspace\NennerEngine\launch_dashboard.pyw"
Write-Host "Dashboard restarted - http://127.0.0.1:8050/"
