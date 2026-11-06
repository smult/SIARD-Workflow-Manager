@echo off
setlocal EnableExtensions
cd /d "%~dp0"
set "rc=0"

echo ============================================================
echo  SIARD Workflow Manager - innholdskontroll - TEST
echo ============================================================
echo.

for %%F in (
    "main.py"
    "version.py"
    "gui\operations_panel.py"
    "siard_workflow\core\base_operation.py"
    "siard_workflow\core\context.py"
    "siard_workflow\systemspecific_operations\cosdoc_operation.py"
    "siard_workflow\core\workflow_io.py"
    "siard_workflow\operations\__init__.py"
    "siard_workflow\operations\report_style.py"
    "siard_workflow\operations\content_qa_operation.py"
    "siard_workflow\rules\content_qa\ephorte_template.json"
    "siard_workflow\core\content_qa\__init__.py"
    "siard_workflow\core\content_qa\__main__.py"
    "siard_workflow\core\content_qa\source.py"
    "siard_workflow\core\content_qa\profile.py"
    "siard_workflow\core\content_qa\engine.py"
    "siard_workflow\core\content_qa\discovery.py"
    "tests\test_content_qa_discovery.py"
    "siard_workflow\core\content_qa\reporting.py"
    "tests\test_content_qa.py"
    "tests\test_content_qa_operation.py"
) do (
    if not exist "%%~F" (
        echo [FEIL] Mangler %%~F i %CD%
        set "rc=2"
        goto :done
    )
)

where python >nul 2>&1
if errorlevel 1 (
    echo [FEIL] Kommandoen python er ikke tilgjengelig.
    set "rc=3"
    goto :done
)

echo Python:
python --version

echo.
echo [1/4] Kontroll av QA-operasjon, GUI og workflow-profiler...
python -X utf8 -c "from siard_workflow.operations import ContentQAOperation as Op; from siard_workflow.core.workflow_io import _op_map; from gui.operations_panel import OP_DEFS; assert Op.operation_id == 'content_qa' and not Op.modifies_content and not Op.requires_unpack; assert _op_map()[Op.operation_id] is Op; assert any(x.get('cls') is Op for x in OP_DEFS); print('[OK] QA er registrert i GUI og workflow-profiler')"
if errorlevel 1 (
    echo [FEIL] Kontrollen feilet.
    set "rc=1"
    goto :done
)

echo.
echo [2/4] Kontrollerer PDF-avhengighet...
python -X utf8 -c "import reportlab; print('[OK] ReportLab', reportlab.Version)"
if errorlevel 1 (
    echo [FEIL] Kontrollen feilet.
    set "rc=1"
    goto :done
)

echo.
echo [3/4] Kjorer enhetstester inkl. rapporter...
python -X utf8 -m unittest discover -s tests -p "test_content_qa*.py" -v
if errorlevel 1 (
    echo [FEIL] Kontrollen feilet.
    set "rc=1"
    goto :done
)

echo.
echo [4/4] Kontrollerer syntaks i leverte filer...
python -X utf8 -m py_compile "version.py" "gui\operations_panel.py" "siard_workflow\core\workflow_io.py" "siard_workflow\operations\content_qa_operation.py" "siard_workflow\operations\__init__.py" "siard_workflow\core\content_qa\__init__.py" "siard_workflow\core\content_qa\__main__.py" "siard_workflow\core\content_qa\source.py" "siard_workflow\core\content_qa\profile.py" "siard_workflow\core\content_qa\engine.py" "siard_workflow\core\content_qa\discovery.py" "siard_workflow\core\content_qa\reporting.py" "tests\test_content_qa.py" "tests\test_content_qa_discovery.py" "tests\test_content_qa_operation.py"
if errorlevel 1 (
    echo [FEIL] Kontrollen feilet.
    set "rc=1"
    goto :done
)

echo.
echo [OK] Alle kontroller er bestatt.

:done
echo.
echo ============================================================
if "%rc%"=="0" (
    echo SLUTTSTATUS: OK
) else (
    echo SLUTTSTATUS: FEIL - returkode %rc%
)
echo ============================================================
echo.
pause
endlocal & exit /b %rc%
