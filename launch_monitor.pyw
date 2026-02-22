"""
NennerEngine Alert Monitor Launcher
Starts the alert monitor daemon with Telegram notifications.
Uses .pyw extension so no console window flashes on double-click.

To see live output, run from terminal instead:
    python -m nenner_engine --monitor --db E:\AI_Workspace\NennerEngine\nenner_signals.db
"""
import subprocess
import sys
import os

# Run from the NennerEngine project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Start the alert monitor
# Uses python.exe (not pythonw.exe) via Popen so we can keep a console
# for log output, but the .pyw launcher itself won't flash a window.
proc = subprocess.Popen(
    [
        sys.executable,
        "-m", "nenner_engine",
        "--monitor",
        "--interval", "60",
        "--db", r"E:\AI_Workspace\NennerEngine\nenner_signals.db",
    ],
    creationflags=subprocess.CREATE_NEW_CONSOLE,
)

# Keep running until the monitor exits
proc.wait()
