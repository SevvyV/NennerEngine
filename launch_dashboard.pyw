"""
NennerEngine Dashboard Launcher
Starts the Dash server and opens the browser automatically.
Uses .pyw extension so no console window flashes on double-click.
"""
import subprocess
import webbrowser
import time
import sys
import os

# Run from the NennerEngine project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Start the dashboard server in the background
proc = subprocess.Popen(
    [sys.executable, "dashboard.py"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)

# Give the server a moment to start, then open browser
time.sleep(2)
webbrowser.open("http://127.0.0.1:8050")

# Keep running until the server exits
proc.wait()
