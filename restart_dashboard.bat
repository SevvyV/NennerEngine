@echo off
echo Killing old dashboard...
taskkill /F /IM pythonw.exe >nul 2>&1
timeout /t 2 /nobreak >nul
echo Starting dashboard...
start "" pythonw.exe "E:\Workspace\NennerEngine\launch_dashboard.pyw"
echo Dashboard restarted - http://127.0.0.1:8050/
timeout /t 3 /nobreak >nul
