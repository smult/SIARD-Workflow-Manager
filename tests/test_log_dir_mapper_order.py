"""
Logg-mappe fra Innstillinger og plassering av SIARDMapper.

  1. resolve_log_dir: «Logg-mappe» (config log_dir) brukes når den er satt;
     tom = mappa SIARD-fila ligger i (eller oppgitt reserve).
  2. Legges SIARDMapper til en arbeidsflyt med BLOB-konvertering, foreslås
     plassering før BLOB-konverteringen — «Ja» setter den der, «Nei» sist.

Kjør:  python -X utf8 tests/test_log_dir_mapper_order.py
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_resolve_log_dir() -> None:
    print("test_resolve_log_dir")
    import settings
    from siard_workflow.core.file_logger import resolve_log_dir
    _ok(settings._CONFIG_DEFAULTS.get("log_dir") == "", "standard: tom logg-mappe")
    siard = Path(r"G:\arkiv\1504\uttrekk.siard")
    orig = settings.get_config
    try:
        settings.get_config = lambda k, d=None: ""
        _ok(resolve_log_dir(siard) == siard.parent, "tom → mappa SIARD-fila ligger i")
        _ok(resolve_log_dir(siard, siard.parent / "siard_logs") == siard.parent / "siard_logs",
            "tom → oppgitt reserve (batch: siard_logs)")
        settings.get_config = lambda k, d=None: r"  E:\logger  " if k == "log_dir" else d
        _ok(resolve_log_dir(siard) == Path(r"E:\logger"), "satt → valgt logg-mappe")
    finally:
        settings.get_config = orig


class _Panel:
    def __init__(self, ops):
        self.ops = list(ops)

    def get_operations(self):
        return list(self.ops)

    def add_operation(self, op, silent=False):
        self.ops.append(op)

    def insert_operation_before(self, op, before_id, silent=False):
        i = next((i for i, o in enumerate(self.ops) if o.operation_id == before_id),
                 len(self.ops))
        self.ops.insert(i, op)


def test_mapper_placement() -> None:
    print("test_mapper_placement")
    import gui.app as A
    from siard_workflow.operations import SiardMapperOperation
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    from siard_workflow.operations.pipeline_operations import (
        RepackSiardOperation, UnpackSiardOperation)

    answers = {"svar": "ja"}
    shown = []

    class _Dlg:
        def __init__(self, parent, message, **kw):
            shown.append((message, kw))
            self.result = answers["svar"]

        def title(self, _t):
            pass

    orig = A._PipelineSuggestionDialog
    A._PipelineSuggestionDialog = _Dlg
    try:
        def _fake(ops):
            f = SimpleNamespace(workflow=SimpleNamespace(add=lambda op: None),
                                workflow_panel=_Panel(ops))
            f._log = lambda m, lvl="info": None
            f.wait_window = lambda d: None
            f._place_before_blob_convert = A.App._place_before_blob_convert.__get__(f)
            return f

        base = [UnpackSiardOperation(), BlobConvertOperation(), RepackSiardOperation()]
        f = _fake(base)
        A.App._add_operation(f, SiardMapperOperation())
        ids = [o.operation_id for o in f.workflow_panel.ops]
        _ok(ids == ["unpack_siard", "siardmapper", "blob_convert", "repack_siard"],
            f"«Ja»: SIARDMapper før BLOB-konvertering {ids}")
        _ok(shown and "Før BLOB-konvertering (anbefalt)" == shown[-1][1].get("yes_text"),
            "forslaget har «før BLOB-konvertering» som anbefalt valg")

        answers["svar"] = "nei"
        f = _fake(base)
        A.App._add_operation(f, SiardMapperOperation())
        _ok([o.operation_id for o in f.workflow_panel.ops][-1] == "siardmapper",
            "«Nei»: SIARDMapper sist")

        shown.clear()
        f = _fake([UnpackSiardOperation(), RepackSiardOperation()])
        A.App._add_operation(f, SiardMapperOperation())
        _ok(not shown and f.workflow_panel.ops[-1].operation_id == "siardmapper",
            "uten BLOB-konvertering: ingen spørsmål, legges sist")
    finally:
        A._PipelineSuggestionDialog = orig


def main() -> None:
    test_resolve_log_dir()
    test_mapper_placement()
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
