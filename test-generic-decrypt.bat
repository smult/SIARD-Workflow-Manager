@echo off
setlocal EnableExtensions
cd /d "%~dp0"
set "rc=0"

echo ============================================================
echo  SIARD Workflow Manager - Generic Decrypt - TEST
echo ============================================================
echo.

REM Check a COMPLETE checkout, not just the files in this delta ZIP.
for %%F in (
    "main.py"
    "siard_workflow\core\base_operation.py"
    "siard_workflow\core\context.py"
    "siard_workflow\core\document_password_rules.py"
    "siard_workflow\core\generic_decrypt_reporting.py"
    "siard_workflow\systemspecific_operations\cosdoc_operation.py"
    "siard_workflow\systemspecific_operations\generic_decrypt_operation.py"
    "siard_workflow\systemspecific_operations\__init__.py"
    "siard_workflow\core\workflow_io.py"
    "siard_workflow\rules\acos_sosial_s1021.json"
    "gui\operations_panel.py"
    "tests\test_generic_decrypt.py"
    "tests\test_generic_decrypt_reporting.py"
) do (
    if not exist "%%~F" (
        echo [FEIL] Mangler %%~F i %CD%
        set "rc=2"
        goto :done
    )
)

where python >nul 2>&1
if errorlevel 1 (
    echo [FEIL] Fant ikke kommandoen python.
    set "rc=3"
    goto :done
)

echo Python:
python --version

echo.
echo [1/4] Kontrollerer operasjon, profilregistrering og GUI...
python -X utf8 -c "from siard_workflow.systemspecific_operations.generic_decrypt_operation import GenericDocumentDecryptOperation as Op; from siard_workflow.core.workflow_io import _op_map; from gui.operations_panel import OP_DEFS; assert Op.operation_id == 'generic_document_decrypt' and Op.requires_unpack and Op.modifies_content; assert _op_map().get(Op.operation_id) is Op; assert any(x['cls'] is Op for x in OP_DEFS); print('[OK] Operasjon, profil og GUI er koblet sammen')"
if errorlevel 1 (
    echo [FEIL] Integrasjonssjekken feilet.
    set "rc=1"
    goto :done
)

echo.
echo [2/4] Kontrollerer PDF-avhengighet...
python -X utf8 -c "import reportlab; print('[OK] ReportLab', reportlab.Version)"
if errorlevel 1 (
    echo [FEIL] ReportLab mangler - PDF-rapportering kan ikke testes.
    set "rc=1"
    goto :done
)

echo.
echo [3/4] Kjorer enhetstester (inkludert CSV, feilrapport og PDF)...
python -X utf8 -m unittest discover -s tests -p "test_generic_decrypt*.py" -v
if errorlevel 1 (
    echo [FEIL] Enhetstestene feilet.
    set "rc=1"
    goto :done
)

echo.
echo [4/4] Kontrollerer syntaks i LEVERTE filer...
python -X utf8 -m py_compile "siard_workflow\core\document_password_rules.py" "siard_workflow\core\generic_decrypt_reporting.py" "siard_workflow\systemspecific_operations\generic_decrypt_operation.py" "siard_workflow\systemspecific_operations\__init__.py" "siard_workflow\core\workflow_io.py" "gui\operations_panel.py" "tests\test_generic_decrypt.py" "tests\test_generic_decrypt_reporting.py"
if errorlevel 1 (
    echo [FEIL] Python-syntakskontroll feilet.
    set "rc=1"
    goto :done
)

echo.
echo [OK] Alle kontroller er bestatt.

:done
echo.
echo ============================================================
if "%rc%"=="0" (
    echo  SLUTTSTATUS: OK
) else (
    echo  SLUTTSTATUS: FEIL - returkode %rc%
)
echo ============================================================
echo.
pause
endlocal & exit /b %rc%
