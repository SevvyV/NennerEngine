@echo off
REM Launch Claude Code in the NennerEngine trading desk environment
REM Double-click this file or pin it to taskbar

cd /d E:\Workspace\NennerEngine
echo.
echo  ==========================================
echo   NennerEngine Trading Desk - Claude Code
echo  ==========================================
echo.
echo  CLAUDE.md loaded with risk parameters
echo  8 skills available: /signal-check, /position-size,
echo    /morning-scan, /risk-report, /stat-confirm,
echo    /trade-journal, /backtest-pattern, /portfolio-construct
echo  45 knowledge rules in Stanley's brain
echo  Risk hooks active (auto-verification)
echo.
echo  Starting Claude Code...
echo.

REM Use the PATH-resolved claude (standalone CLI at %USERPROFILE%\.local\bin)
where claude >nul 2>nul
if %errorlevel%==0 (
    claude
    goto :eof
)

echo  ERROR: Could not find claude in PATH
echo  Expected: %USERPROFILE%\.local\bin\claude
echo  Please reinstall Claude Code.
pause
