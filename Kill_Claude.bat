@echo off
REM Kill all Claude Code processes
REM Use after ending a session to clean up

echo.
echo  Stopping all Claude Code processes...
echo.

taskkill /F /IM claude.exe /T 2>nul
taskkill /F /IM claude-agent.exe /T 2>nul

if %errorlevel%==128 (
    echo  No Claude processes were running.
) else (
    echo  All Claude processes terminated.
)

echo.
timeout /t 3 >nul
