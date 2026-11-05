@echo off
chcp 65001 >nul

echo Sjekker Python...
py --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [FEIL] Python launcher ikke funnet
    echo Installer Python fra https://www.python.org/
    echo Husk aa velge "Install launcher" og "Add Python to PATH"
    exit /b
) else (
    echo [OK] Python funnet via py launcher
)

echo.

echo Sjekker LibreOffice...
where soffice >nul 2>&1
if %errorlevel% neq 0 (
    if exist "C:\Program Files\LibreOffice\program\soffice.exe" (
        echo [OK] LibreOffice funnet
    ) else (
        echo [FEIL] LibreOffice ikke funnet
        echo Installer LibreOffice fra https://www.libreoffice.org/
        exit /b
    )
) else (
    echo [OK] LibreOffice funnet i PATH
)

echo.
echo Trykk en tast for aa fortsette
pause >nul

echo Installerer avhengigheter...
py -m pip install -r requirements.txt

echo.
echo Starter SIARD Workflow Manager...
py main.py

pause