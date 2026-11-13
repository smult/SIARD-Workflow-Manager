"""
DIAS-pakking uten periode (nov. 2026).

  1. DIAS kan legges til uten fra–til: plassholderen «ÅÅÅÅ-MM-DD» regnes som
     ikke fylt ut (dialogen godtar den, feltet blir tomt).
  2. DIAS-steget uten periode, og uttrekket oppgir ikke datospenn: brukeren
     spørres om «Estimer datospenn» skal kjøres nå. Ja → estimatet foreslås i
     DIAS-vinduet (vises selv om bryteren er av), metadata.xml endres ikke.
  3. «Estimer datospenn» kjørt tidligere i kjøringen (ikke godkjent for
     metadata.xml) → estimatet foreslås uten nytt spørsmål.
  4. Nei → perioden må fylles ut i vinduet; tom periode → steget feiler.

Kjør:  python -X utf8 tests/test_dias_period.py
"""
from __future__ import annotations

import importlib.util
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_spec = importlib.util.spec_from_file_location("_tds", Path(__file__).with_name("test_date_span.py"))
_tds = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_tds)


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _siard(tmp: Path) -> Path:
    root = _tds._make(tmp / "uten_spenn", "")
    return _tds._zip(root, tmp / "uten_spenn.siard")


def test_is_period_date() -> None:
    print("test_is_period_date")
    from siard_workflow.operations.dias_package_operation import is_period_date as f
    _ok(f("2014-01-01") and not f("ÅÅÅÅ-MM-DD") and not f("") and not f(None)
        and not f("20140101") and not f("2014-13-01"),
        "bare gyldig ÅÅÅÅ-MM-DD regnes som fylt ut")


def test_estimate_on_request(tmp: Path) -> None:
    print("test_estimate_on_request")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    siard = _siard(tmp)
    before = siard.read_bytes()
    asked: list = []
    shown: dict = {}

    def ask_estimate(op):
        asked.append(op)
        return True

    def ask_config(op):
        shown["period"] = (op.params["period_start"], op.params["period_end"])
        shown["note"] = op.period_note
        return None                                    # avbryt — ikke pakk i testen
    ctx = WorkflowContext(siard_path=siard, metadata={
        "ask_dias_date_span_cb": ask_estimate, "ask_dias_config_cb": ask_config})
    op = DiasPackageOperation(period_start="ÅÅÅÅ-MM-DD", period_end="ÅÅÅÅ-MM-DD",
                              confirm_before_packing=False)
    r = op.run(ctx)
    _ok(len(asked) == 1, "spør om «Estimer datospenn» skal kjøres når perioden mangler")
    _ok("period" in shown, "DIAS-vinduet vises selv om bryteren er av (periode foreslått)")
    s, e = shown["period"]
    _ok(s.startswith("2010-") and e.startswith("2015-"),
        f"estimatet foreslås som periode: {s} – {e}")
    _ok("Estimer datospenn" in shown["note"], f"vinduet får forklaring: «{shown['note']}»")
    _ok(siard.read_bytes() == before and not list(tmp.glob("*_datospenn*.siard")),
        "uttrekket endres ikke (ingen skriving til metadata.xml, ingen kopi)")
    _ok(not r.success and "avbrutt" in r.message, "avbrutt i vinduet → steget stopper")


def test_prior_estimate(tmp: Path) -> None:
    print("test_prior_estimate")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    siard = tmp / "uten_spenn.siard"
    asked: list = []
    shown: dict = {}
    ctx = WorkflowContext(siard_path=siard, metadata={
        "ask_dias_date_span_cb": lambda op: asked.append(op) or True,
        "ask_dias_config_cb": lambda op: shown.update(p=(op.params["period_start"],
                                                         op.params["period_end"])) or None})
    ctx.set_result("date_span", {"found": True, "source": "estimat", "accepted": False,
                                 "from_date": "2011-03-04", "to_date": "2014",
                                 "reliable": True})
    DiasPackageOperation(period_start="", period_end="").run(ctx)
    _ok(not asked, "estimat fra tidligere i kjøringen → ikke spurt på nytt")
    _ok(shown.get("p") == ("2011-03-04", "2014-12-31"),
        f"estimatet foreslås (årstall → hele år): {shown.get('p')}")


def test_declined(tmp: Path) -> None:
    print("test_declined")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    siard = tmp / "uten_spenn.siard"
    shown: dict = {}

    def ask_config(op):
        shown["period"] = (op.params["period_start"], op.params["period_end"])
        return dict(op.params)                      # «Fortsett» uten å fylle inn
    ctx = WorkflowContext(siard_path=siard, metadata={
        "ask_dias_date_span_cb": lambda op: False, "ask_dias_config_cb": ask_config})
    r = DiasPackageOperation(period_start="ÅÅÅÅ-MM-DD", period_end="",
                             confirm_before_packing=False).run(ctx)
    _ok(shown.get("period") == ("", ""),
        "Nei → vinduet vises med tom periode (plassholderen er fjernet)")
    _ok(not r.success and "period_start" in r.message,
        f"tom periode etter vinduet → steget feiler: {r.message}")


def test_dialog_without_period(tmp: Path) -> None:
    print("test_dialog_without_period")
    import customtkinter as ctk
    import gui.dias_dialog as DD
    from gui.operations_panel import OP_DEFS
    op_def = next(d for d in OP_DEFS if d.get("custom_dialog") == "DiasParamDialog")
    live = {**op_def, "params": [
        {**p, "default": "ÅÅÅÅ-MM-DD" if p["key"].startswith("period_") else p["default"]}
        for p in op_def["params"]]}
    saved = DD.save_remembered
    DD.save_remembered = lambda f, v: {}             # ikke skriv brukerens innstillinger
    root = ctk.CTk()
    root.geometry("300x200+0+0")
    got: list = []
    try:
        d = DD.DiasParamDialog(root, live, on_confirm=got.append)
        root.update()
        _ok(d._vars["period_start"][0].get() == "" and d._vars["period_end"][0].get() == "",
            "plassholderen vises ikke som verdi")
        _ok("Estimer datospenn" in d._period_hint_lbl.cget("text"),
            "forklaring: tom periode hentes ved pakking")
        d._vars["period_end"][0].set("ÅÅÅÅ-MM-DD")      # skrevet inn igjen
        d._confirm()
        _ok(got and got[-1].params["period_start"] == ""
            and got[-1].params["period_end"] == "",
            "DIAS kan legges til uten periode")

        # Kjøremodus med foreslått periode: forklaringen vises
        live2 = {**op_def, "params": [
            {**p, "default": {"period_start": "2010-10-01",
                              "period_end": "2015-12-31"}.get(p["key"], p["default"])}
            for p in op_def["params"]],
            "initial_extra_files": "[]",
            "period_note": "Periode estimert av «Estimer datospenn» — kontroller før pakking"}
        d2 = DD.DiasParamDialog(root, live2, on_confirm=lambda o: None, runtime=True)
        root.update()
        _ok("kontroller" in d2._period_hint_lbl.cget("text"),
            "kjøremodus: forslaget er merket i vinduet")
        d2.destroy()
    finally:
        DD.save_remembered = saved
        root.destroy()


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="dias_period_"))
    try:
        test_is_period_date()
        test_estimate_on_request(tmp)
        test_prior_estimate(tmp)
        test_declined(tmp)
        test_dialog_without_period(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
