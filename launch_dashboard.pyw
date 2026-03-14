"""
NennerEngine Dashboard + Alert Monitor Launcher
Starts the Dash server, opens the browser, AND starts the Telegram
alert monitor in the background.
Uses .pyw extension so no console window flashes on double-click.

Process management:
- Named Mutex prevents duplicate launcher instances
- Job Object auto-kills child processes if the launcher dies
"""
import ctypes
import ctypes.wintypes
import subprocess
import webbrowser
import time
import sys
import os
import shutil

# Run from the NennerEngine project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = r"E:\Workspace\DataCenter\nenner_signals.db"
LOG_PATH = r"E:\Workspace\NennerEngine\nenner_engine.log"
PORT = 8050
MUTEX_NAME = "NennerEngine_Dashboard_Launcher"

# --- Win32 constants ---
ERROR_ALREADY_EXISTS = 183
SYNCHRONIZE = 0x00100000
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
JobObjectExtendedLimitInformation = 9

kernel32 = ctypes.windll.kernel32


class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.wintypes.LARGE_INTEGER),
        ("PerJobUserTimeLimit", ctypes.wintypes.LARGE_INTEGER),
        ("LimitFlags", ctypes.wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", ctypes.wintypes.DWORD),
        ("Affinity", ctypes.POINTER(ctypes.c_ulong)),
        ("PriorityClass", ctypes.wintypes.DWORD),
        ("SchedulingClass", ctypes.wintypes.DWORD),
    ]


class IO_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount", ctypes.c_ulonglong),
        ("WriteTransferCount", ctypes.c_ulonglong),
        ("OtherTransferCount", ctypes.c_ulonglong),
    ]


class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
        ("IoInfo", IO_COUNTERS),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


def acquire_mutex(name):
    """Try to acquire a named mutex. Returns handle if acquired, None if
    another instance already holds it."""
    handle = kernel32.CreateMutexW(None, True, name)
    if not handle:
        return None
    if kernel32.GetLastError() == ERROR_ALREADY_EXISTS:
        kernel32.CloseHandle(handle)
        return None
    return handle


def create_job_object():
    """Create a Job Object that kills all assigned processes when closed."""
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        return None

    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

    kernel32.SetInformationJobObject(
        job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    return job


def assign_to_job(job, process):
    """Assign a subprocess to a Job Object."""
    handle = kernel32.OpenProcess(SYNCHRONIZE | 0x100, False, process.pid)
    if handle:
        kernel32.AssignProcessToJobObject(job, handle)
        kernel32.CloseHandle(handle)


# --- Singleton check ---
mutex = acquire_mutex(MUTEX_NAME)
if mutex is None:
    # Another instance is already running — just open the browser and exit
    webbrowser.open(f"http://127.0.0.1:{PORT}")
    sys.exit(0)

# --- Job Object for automatic child cleanup ---
job = create_job_object()

# pythonw.exe cannot run console apps properly, so we need to find
# the real python.exe even when launched via .pyw
python_exe = sys.executable
if python_exe.lower().endswith("pythonw.exe"):
    python_exe = python_exe[:-5] + ".exe"  # pythonw.exe -> python.exe
if not os.path.exists(python_exe):
    python_exe = shutil.which("python") or sys.executable

# Log file for debugging
log_file = open(LOG_PATH, "a", encoding="utf-8")

try:
    # 1. Start the dashboard server
    dashboard_proc = subprocess.Popen(
        [python_exe, "dashboard.py"],
        stdout=log_file,
        stderr=log_file,
    )

    # 2. Start the alert monitor (Telegram only, no toast)
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

    # Assign children to Job Object — if this launcher dies, they die too
    if job:
        assign_to_job(job, dashboard_proc)
        assign_to_job(job, monitor_proc)

    # Give the server a moment to start, then open browser
    time.sleep(3)
    webbrowser.open(f"http://127.0.0.1:{PORT}")

    # Block until the dashboard exits, then clean up the monitor
    dashboard_proc.wait()
    monitor_proc.terminate()

finally:
    log_file.close()
    if job:
        kernel32.CloseHandle(job)
    kernel32.ReleaseMutex(mutex)
    kernel32.CloseHandle(mutex)
