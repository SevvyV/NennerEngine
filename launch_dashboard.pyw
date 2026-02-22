"""
NennerEngine Dashboard + Alert Monitor Launcher
Starts the Dash server, opens the browser, AND starts the Telegram
alert monitor in the background.
Uses .pyw extension so no console window flashes on double-click.

Safe to double-click multiple times -- kills stale instances first.
"""
import subprocess
import webbrowser
import time
import sys
import os
import shutil
import socket

# Run from the NennerEngine project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = r"E:\AI_Workspace\NennerEngine\nenner_signals.db"
LOG_PATH = r"E:\AI_Workspace\NennerEngine\nenner_engine.log"
PID_PATH = r"E:\AI_Workspace\NennerEngine\.launcher_pids"
PORT = 8050

# pythonw.exe cannot run console apps properly, so we need to find
# the real python.exe even when launched via .pyw
python_exe = sys.executable
if python_exe.lower().endswith("pythonw.exe"):
    python_exe = python_exe[:-5] + ".exe"  # pythonw.exe -> python.exe
if not os.path.exists(python_exe):
    python_exe = shutil.which("python") or sys.executable


def kill_stale():
    """Kill any previous dashboard/monitor processes we launched."""
    # Kill by saved PIDs
    if os.path.exists(PID_PATH):
        try:
            with open(PID_PATH) as f:
                for line in f:
                    pid = int(line.strip())
                    try:
                        os.kill(pid, 9)
                    except (OSError, ProcessLookupError):
                        pass
        except Exception:
            pass
        os.remove(PID_PATH)

    # Also free port 8050 if still in use
    try:
        subprocess.run(
            ["powershell", "-Command",
             f"Get-NetTCPConnection -LocalPort {PORT} -ErrorAction SilentlyContinue "
             "| ForEach-Object { Stop-Process -Id $_.OwningProcess -Force "
             "-ErrorAction SilentlyContinue }"],
            timeout=5, capture_output=True,
        )
    except Exception:
        pass
    time.sleep(1)


def save_pids(*pids):
    """Save child PIDs so we can kill them on next launch."""
    with open(PID_PATH, "w") as f:
        for pid in pids:
            f.write(f"{pid}\n")


# Clean up any stale instances from previous launches
kill_stale()

# Log file for debugging
log_file = open(LOG_PATH, "a", encoding="utf-8")

# 1. Start the dashboard server in the background
dashboard_proc = subprocess.Popen(
    [python_exe, "dashboard.py"],
    stdout=log_file,
    stderr=log_file,
)

# 2. Start the alert monitor in the background (Telegram only, no toast)
monitor_proc = subprocess.Popen(
    [
        python_exe, "-m", "nenner_engine",
        "--monitor",
        "--interval", "60",
        "--db", DB_PATH,
    ],
    stdout=log_file,
    stderr=log_file,
)

# Save PIDs for cleanup on next launch
save_pids(dashboard_proc.pid, monitor_proc.pid)

# Give the server a moment to start, then open browser
time.sleep(3)
webbrowser.open(f"http://127.0.0.1:{PORT}")

# Keep running until the dashboard exits, then clean up the monitor
dashboard_proc.wait()
monitor_proc.terminate()
log_file.close()

# Clean up PID file
try:
    os.remove(PID_PATH)
except Exception:
    pass
