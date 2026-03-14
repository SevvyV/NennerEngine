"""
NennerEngine Alert Monitor Launcher
Starts the alert monitor daemon with Telegram notifications.
Uses .pyw extension so no console window flashes on double-click.

Process management:
- Named Mutex prevents duplicate monitor instances
- Job Object auto-kills the monitor if the launcher dies

To see live output, run from terminal instead:
    python -m nenner_engine --monitor --db E:\Workspace\DataCenter\nenner_signals.db
"""
import ctypes
import ctypes.wintypes
import subprocess
import sys
import os

# Run from the NennerEngine project root
os.chdir(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = r"E:\Workspace\NennerEngine\nenner_signals.db"
MUTEX_NAME = "NennerEngine_Monitor_Launcher"

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
    # Another instance is already running — exit silently
    sys.exit(0)

# --- Job Object for automatic child cleanup ---
job = create_job_object()

try:
    # Start the alert monitor in a new console window
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m", "nenner_engine",
            "--monitor",
            "--interval", "60",
            "--db", DB_PATH,
        ],
        creationflags=subprocess.CREATE_NEW_CONSOLE,
    )

    # Assign to Job Object — if this launcher dies, the monitor dies too
    if job:
        assign_to_job(job, proc)

    # Block until the monitor exits
    proc.wait()

finally:
    if job:
        kernel32.CloseHandle(job)
    kernel32.ReleaseMutex(mutex)
    kernel32.CloseHandle(mutex)
