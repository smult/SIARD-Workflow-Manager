@echo off
cd /d "%~dp0"

REM --- Finn Python (pythonw.exe starter GUI uten konsollvindu) ---
where pythonw >nul 2>&1
if errorlevel 1 (
    echo [FEIL] Python ble ikke funnet - pythonw.exe mangler i PATH.
    echo Installer Python 3.10 eller nyere fra https://www.python.org/
    echo og kryss av for "Add python.exe to PATH". Kjor deretter install.bat.
    pause
    exit /b 1
)

REM --- Start GUI frikoblet med pythonw (uten konsollvindu) ---
REM Da henger ikke konsollen/PowerShell mens programmet kjorer, og programmet
REM lukkes ikke selv om konsollen/PowerShell lukkes.
REM Manglende pakker/programmer oppdages av programmet selv (dependencies.py):
REM paakrevde pakker installeres etter bekreftelse for GUI starter, ovrige vises
REM i en oversikt etter oppstart med hva som ikke virker uten dem.
start "" pythonw main.py
