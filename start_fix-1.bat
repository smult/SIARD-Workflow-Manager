@echo off
setlocal
cd /d "%~dp0"

REM IKAMR lokal oppstart - endrer ikke original start.bat
REM Bruk standard Python 3.14, ikke free-threaded Python 3.14t.
set "PYTHON=C:\Program Files\Python314\pythonw.exe"

if not exist "%PYTHON%" (
    echo [FEIL] Finner ikke standard Python: %PYTHON%
    pause
    exit /b 1
)

if not exist "%~dp0main.py" (
    echo [FEIL] Finner ikke main.py i %~dp0
    pause
    exit /b 1
)

start "" "%PYTHON%" "%~dp0main.py"
if errorlevel 1 (
    echo [FEIL] Kunne ikke starte SIARD Workflow Manager.
    pause
    exit /b 1
)
endlocal
exit /b 0
