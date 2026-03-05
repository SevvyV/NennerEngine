# morning_startup.ps1 -- Pre-market startup for NennerEngine
# Scheduled via Task Scheduler at 6:00 AM ET, weekdays only
# Ensures T1 (LSEG ONE), DataCenter, and dashboard.py are running

$ErrorActionPreference = "Continue"

$logDir  = "E:\Workspace\NennerEngine\logs"
$logFile = Join-Path $logDir "morning_startup.log"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $msg" | Tee-Object -FilePath $logFile -Append
}

Log "=== Morning startup begin ==="

# --- 1. LSEG ONE (Thomson One) — always restart fresh ---
$t1Exe  = "C:\Users\sevag\AppData\Local\LSEG Wealth\Install\LSEG ONE-7.1.3.0\LSEG ONE.exe"
$t1Proc = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue

if ($t1Proc) {
    Log "T1: Running (PID $($t1Proc.Id)) -- closing for fresh restart..."
    $t1Proc | Stop-Process -Force
    Start-Sleep -Seconds 5

    # Verify it's gone
    $t1Still = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue
    if ($t1Still) {
        Log "T1: Still alive after kill -- waiting 10s..."
        Start-Sleep -Seconds 10
    }
    Log "T1: Closed."
} else {
    Log "T1: Not running."
}

Log "T1: Launching fresh..."
Start-Process -FilePath $t1Exe

# Wait for the login window to appear
Log "T1: Waiting for login window..."
$wsh = New-Object -ComObject WScript.Shell
$loginReady = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Seconds 2
    $activated = $wsh.AppActivate("LSEG")
    if ($activated) { $loginReady = $true; break }
}

if ($loginReady) {
    Log "T1: Login window detected -- auto-signing in..."
    Start-Sleep -Seconds 2

    $wsh.SendKeys("sevagv@gmail.com")
    Start-Sleep -Seconds 1

    $wsh.SendKeys("{TAB}")
    Start-Sleep -Seconds 2

    $wsh.SendKeys("{ENTER}")

    Log "T1: Sign-in keys sent. Waiting 15 min for T1 to fully connect..."
} else {
    Log "T1: WARNING -- login window not detected after 60s. Sign in manually."
}

# Wait 15 minutes for T1 to fully initialize and RTD server to connect
Start-Sleep -Seconds 900

# Verify T1 is actually running before opening spreadsheets
$t1Check = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue
if ($t1Check) {
    Log "T1: Confirmed running (PID $($t1Check.Id))"
} else {
    Log "T1: WARNING -- process not found after 15 min wait. Attempting relaunch..."
    Start-Process -FilePath $t1Exe
    Start-Sleep -Seconds 60

    $t1Check2 = Get-Process -Name "LSEG ONE" -ErrorAction SilentlyContinue
    if ($t1Check2) {
        Log "T1: Relaunch successful (PID $($t1Check2.Id)). Sign in manually."
    } else {
        Log "T1: FAILED -- could not start T1. Continuing with spreadsheets anyway."
    }
}

# --- 2. Excel Workbooks (fresh restart for clean RTD) ---
$dcPath = "E:\Workspace\DataCenter\Nenner_DataCenter.xlsm"
$ocPath = "E:\Workspace\DataCenter\OptionChains_Beta.xlsm"

$excelWasRunning = $false
try {
    $xl = [System.Runtime.InteropServices.Marshal]::GetActiveObject("Excel.Application")
    $excelWasRunning = $true
    Log "Excel: Found running instance. Saving open workbooks and closing..."

    # Save any of our workbooks that are open
    foreach ($wb in $xl.Workbooks) {
        if ($wb.Name -eq "Nenner_DataCenter.xlsm" -or $wb.Name -eq "OptionChains_Beta.xlsm") {
            Log "Excel: Saving $($wb.Name)..."
            $wb.Save()
        }
    }

    # Quit Excel gracefully
    $xl.Quit()
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($xl) | Out-Null

    # Wait for Excel process to fully exit
    Log "Excel: Waiting for process to exit..."
    for ($i = 0; $i -lt 15; $i++) {
        $excelProc = Get-Process -Name "EXCEL" -ErrorAction SilentlyContinue
        if (-not $excelProc) { break }
        Start-Sleep -Seconds 2
    }

    # If still alive after 30s, force kill
    $excelProc = Get-Process -Name "EXCEL" -ErrorAction SilentlyContinue
    if ($excelProc) {
        Log "Excel: Still running after 30s -- force killing..."
        $excelProc | Stop-Process -Force
        Start-Sleep -Seconds 3
    }

    Log "Excel: Closed. Restarting fresh..."
} catch {
    # No running Excel instance -- clean slate
    Log "Excel: Not running."
}

# Open both workbooks fresh
Log "Excel: Opening Nenner_DataCenter.xlsm..."
Start-Process -FilePath $dcPath
Start-Sleep -Seconds 15

Log "Excel: Opening OptionChains.xlsm..."
Start-Process -FilePath $ocPath
Start-Sleep -Seconds 15

Log "Excel: Both workbooks launched. Waiting 30s for workbooks to fully load..."
Start-Sleep -Seconds 30
Log "Excel: RTD should be populating now."

# --- 3. RTD Health Check ---
# After T1 and DataCenter are up, verify RTD is actually feeding prices.
# Reads a known cell (Equities_RT!B5 = first stock BID). If it's empty,
# an error string, or zero, RTD is dead -- reset and retry.
Log "RTD: Checking health..."
$rtdHealthy = $false
try {
    $xl = [System.Runtime.InteropServices.Marshal]::GetActiveObject("Excel.Application")
    $dc = $null
    foreach ($wb in $xl.Workbooks) {
        if ($wb.Name -eq "Nenner_DataCenter.xlsm") { $dc = $wb; break }
    }
    if ($dc) {
        $sheet = $dc.Sheets.Item("Equities_RT")
        $cellVal = $sheet.Range("B5").Value2
        if ($cellVal -ne $null -and $cellVal -is [double] -and $cellVal -gt 0) {
            Log "RTD: Healthy -- Equities_RT!B5 = $cellVal"
            $rtdHealthy = $true
        } else {
            Log "RTD: Dead or stale -- Equities_RT!B5 = '$cellVal'. Resetting..."
        }
    } else {
        Log "RTD: DataCenter workbook not found in Excel."
    }

    if (-not $rtdHealthy -and $dc) {
        # Reset all RTD connections and force recalc
        try {
            $xl.RTD.ResetAll()
            Log "RTD: ResetAll() called. Waiting 30s for reconnect..."
            Start-Sleep -Seconds 30
            $xl.CalculateFull()
            Start-Sleep -Seconds 10

            # Re-check
            $cellVal2 = $sheet.Range("B5").Value2
            if ($cellVal2 -ne $null -and $cellVal2 -is [double] -and $cellVal2 -gt 0) {
                Log "RTD: Recovered after reset -- Equities_RT!B5 = $cellVal2"
                $rtdHealthy = $true
            } else {
                Log "RTD: Still dead after reset -- Equities_RT!B5 = '$cellVal2'. Manual intervention needed."
            }
        } catch {
            Log "RTD: ResetAll() failed -- $($_.Exception.Message)"
        }
    }

    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($xl) | Out-Null
} catch {
    Log "RTD: Could not connect to Excel -- $($_.Exception.Message)"
}

# --- 4. Dashboard / Scheduler ---
$dashProc = Get-Process -Name "python*" -ErrorAction SilentlyContinue |
    Where-Object {
        try {
            $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine
            $cmdLine -match "dashboard\.py"
        } catch { $false }
    }

if ($dashProc) {
    Log "Dashboard: Already running (PID $($dashProc.Id))"
} else {
    Log "Dashboard: Not running -- launching..."
    Start-Process -FilePath "python" -ArgumentList "E:\Workspace\NennerEngine\dashboard.py" -WorkingDirectory "E:\Workspace\NennerEngine" -WindowStyle Minimized
    Start-Sleep -Seconds 5
    Log "Dashboard: Launched (scheduler will start automatically)"
}

Log "=== Morning startup complete ==="
