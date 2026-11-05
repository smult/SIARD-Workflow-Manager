@echo off
setlocal EnableExtensions
cd /d "%~dp0"

echo ============================================================
echo  SIARD Workflow Manager - Generic Decrypt a01 - TEST
echo ============================================================
echo.
echo [1/2] Kjorer enhetstester...
python -X utf8 tests\test_generic_decrypt.py
set "test_rc=%errorlevel%"
if not "%test_rc%"=="0" (
    echo.
    echo [FEIL] Enhetstester feilet. Returkode: %test_rc%
    goto :finish
)

echo.
echo [2/2] Kontrollerer Python-syntaks...
python -m compileall -q gui siard_workflow
set "test_rc=%errorlevel%"
if not "%test_rc%"=="0" (
    echo.
    echo [FEIL] Python-syntakskontroll feilet. Returkode: %test_rc%
    goto :finish
)

echo.
echo [OK] Generic Decrypt a01: Alle tester og syntakskontroll bestatt.

:finish
echo.
echo ============================================================
if "%test_rc%"=="0" (
    echo  SLUTTSTATUS: OK
) else (
    echo  SLUTTSTATUS: FEIL - returkode %test_rc%
)
echo ============================================================
echo.
echo Trykk en tast for aa lukke dette vinduet.
pause
endlocal & exit /b %test_rc%
