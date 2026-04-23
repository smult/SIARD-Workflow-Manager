@echo off
chcp 65001 >nul
echo Sjekker Python...
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [FEIL] Python ikke funnet
    echo Installer Python fra https://www.python.org/
    exit /b
) else (
    echo [OK] Python funnet
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
echo Trykk en tast for å fortsette
pause >nul
echo Installerer avhengigheter...
pip install -r requirements.txt
echo.
echo Starter SIARD Workflow Manager...
python main.py
pause

