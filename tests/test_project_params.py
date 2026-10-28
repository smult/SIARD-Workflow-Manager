"""
Prosjektfila (.siardwf) skal huske operasjonenes verdier — også de som endres
under kjøringen.

Bakgrunn (okt. 2026): DIAS «Submission Agreement» ble nullstilt hver gang
prosjektet ble startet på nytt.
  - Endringer i DIAS-kontrollvinduet (når pakkesteget starter) havnet i
    operasjonen, men prosjektfila lagret kopien av parameterne fra da
    kjøringen startet.
  - Når prosjektfila ble lastet automatisk (samme navn som SIARD-filen),
    fikk panelets steg prosjektets status, men ikke prosjektets verdier —
    neste kjøring skrev panelets verdier tilbake i prosjektfila.

Verifiserer:
  1. verdi endret i kontrollvinduet → lagret i prosjektfila etter steget
  2. automatisk lastet prosjekt → panelets operasjoner får prosjektets verdier
     (duplikate steg matches posisjonelt, steg uten checkpoint er urørt)
  3. Åpne prosjekt → operasjonen bygges med prosjektets verdi

Kjør:  python -X utf8 tests/test_project_params.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_runtime_change_saved(tmp: Path) -> None:
    print("test_runtime_change_saved")
    from gui.app import _sync_checkpoints_to_ops
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.core.project_file import ProjectFile
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    op = DiasPackageOperation(submission_agreement="202X-XX/X", confirm_before_packing=True)
    wf = [op]
    pf = ProjectFile.from_ops(tmp / "u.siard", wf)          # kjøringen starter
    ctx = WorkflowContext(siard_path=tmp / "u.siard", metadata={
        "ask_dias_config_cb": lambda o: {**o.params, "submission_agreement": "2026-14/3"}})
    op._confirm_settings(ctx)                               # kontrollvinduet
    pf.mark_completed(op.operation_id)
    pf.operations = _sync_checkpoints_to_ops(pf.operations, wf)
    path = tmp / "u.siardwf"
    pf.save(path)
    cp = ProjectFile.load(path).operations[0]
    _ok(cp.params["submission_agreement"] == "2026-14/3" and cp.status == "completed",
        "endring i kontrollvinduet lagres i prosjektfila (status beholdt)")


def test_autoload_applies_params(tmp: Path) -> None:
    print("test_autoload_applies_params")
    from gui.app import _apply_checkpoint_params
    from siard_workflow.core.project_file import OpCheckpoint
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    from siard_workflow.operations.pipeline_operations import UnpackSiardOperation
    panel = [UnpackSiardOperation(), DiasPackageOperation(submission_agreement="202X-XX/X"),
             DiasPackageOperation(submission_agreement="202X-XX/X")]
    cps = [OpCheckpoint("dias_package", "DIAS", {"submission_agreement": "2026-14/3",
                                                 "label": "Elevarkiv"}),
           OpCheckpoint("dias_package", "DIAS", {"submission_agreement": "2026-15/1"})]
    n = _apply_checkpoint_params(panel, cps)
    _ok(n == 2 and panel[1].params["submission_agreement"] == "2026-14/3"
        and panel[1].params["label"] == "Elevarkiv"
        and panel[2].params["submission_agreement"] == "2026-15/1",
        "prosjektets verdier brukes, duplikate steg matches posisjonelt")
    _ok("output_dir" in panel[1].params, "øvrige parametere beholdes")


def test_open_project(tmp: Path) -> None:
    print("test_open_project")
    from siard_workflow.core.project_file import ProjectFile
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    pf = ProjectFile.from_ops(tmp / "v.siard",
                              [DiasPackageOperation(submission_agreement="2026-14/3")])
    pf.save(tmp / "v.siardwf")
    cp = ProjectFile.load(tmp / "v.siardwf").operations[0]
    _ok(DiasPackageOperation(**cp.params).params["submission_agreement"] == "2026-14/3",
        "Åpne prosjekt: operasjonen får prosjektets Submission Agreement")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="prosjekt_") as td:
        tmp = Path(td)
        test_runtime_change_saved(tmp)
        test_autoload_applies_params(tmp)
        test_open_project(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
