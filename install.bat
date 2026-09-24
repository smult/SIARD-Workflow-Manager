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
echo Sjekker Ghostscript (valgfritt, PDF/A-normalisering av e-post)...
where gswin64c >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Ghostscript funnet i PATH
) else (
    if exist "%ProgramFiles%\gs" (
        echo [OK] Ghostscript funnet i %ProgramFiles%\gs
    ) else if exist "%LOCALAPPDATA%\Programs\gs" (
        echo [OK] Ghostscript funnet i %LOCALAPPDATA%\Programs\gs
    ) else if exist "%LOCALAPPDATA%\SIARDManager\ghostscript" (
        echo [OK] Ghostscript funnet i %LOCALAPPDATA%\SIARDManager\ghostscript
    ) else (
        echo [ADVARSEL] Ghostscript ikke funnet - valgfritt.
        echo            Installer fra Innstillinger ^> Ghostscript i programmet.
    )
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

