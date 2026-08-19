@echo off
cd /d "%~dp0"

REM --- Sjekk kjore-avhengigheter, installer kun hvis noe mangler ---
python -c "import customtkinter, reportlab" 1>nul 2>nul
if errorlevel 1 (
    echo Installerer avhengigheter første gang - vennligst vent...
    python -m pip install --quiet --disable-pip-version-check customtkinter reportlab tkinterdnd2
)

REM --- Start GUI frikoblet med pythonw (uten konsollvindu) ---
REM Da henger ikke konsollen/PowerShell mens programmet kjorer, og programmet
REM lukkes ikke selv om konsollen/PowerShell lukkes.
start "" pythonw main.py
