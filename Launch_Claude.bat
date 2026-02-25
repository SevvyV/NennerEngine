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

REM Check multiple known locations for claude.exe
set "CLAUDE_EXE="

REM Location 1: E:\Claude\claude-code (custom install path)
for /d %%d in ("E:\Claude\claude-code\*") do (
    if exist "%%d\claude.exe" set "CLAUDE_EXE=%%d\claude.exe"
)

REM Location 2: %APPDATA%\Claude\claude-code (default install path)
if not defined CLAUDE_EXE (
    for /d %%d in ("%APPDATA%\Claude\claude-code\*") do (
        if exist "%%d\claude.exe" set "CLAUDE_EXE=%%d\claude.exe"
    )
)

if defined CLAUDE_EXE (
    "%CLAUDE_EXE%"
    goto :eof
)

echo  ERROR: Could not find claude.exe
echo  Searched: E:\Claude\claude-code
echo  Searched: %APPDATA%\Claude\claude-code
echo  Please reinstall Claude Code.
pause
