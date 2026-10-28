"""
DIAS-pakking: konfigurasjonsvinduet vises når pakkesteget starter.

Verifiserer:
  1. bryteren confirm_before_packing (standard på): vinduet spørres; endringer
     brukes og logges; «Avbryt pakking» stopper steget; bryter av eller uten
     GUI → lagrede innstillinger brukes uten spørsmål
  2. kontrollvinduet (App._ask_dias_config + DiasParamDialog runtime=True):
     egen tittel og knapper, perioden fra operasjonen (metadata.xml/estimat)
     beholdes, tidligere valgte ekstra filer er med, Fortsett/Avbryt gir
     parametere/None

Kjør:  python -X utf8 tests/test_dias_confirm.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


class _Log:
    def __init__(self):
        self.lines: list = []

    def log(self, msg, lvl="info"):
        self.lines.append(msg)


def test_operation(tmp: Path) -> None:
    print("test_operation")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    _ok(DiasPackageOperation.default_params.get("confirm_before_packing") is True,
        "bryteren er på som standard")
    log = _Log()
    asked: list = []
    ctx = WorkflowContext(siard_path=tmp / "x.siard",
                          metadata={"file_logger": log,
                                    "ask_dias_config_cb": lambda op: (asked.append(op),
                                                                      {**op.params,
                                                                       "period_start": "2014-01-01"})[1]})
    op = DiasPackageOperation(period_start="2014-10-01", period_end="2020-06-19",
                              confirm_before_packing=True)
    _ok(op._confirm_settings(ctx) and asked and op.params["period_start"] == "2014-01-01"
        and any("period_start: «2014-10-01» → «2014-01-01»" in m for m in log.lines),
        "vinduet spørres når steget starter; endringen brukes og logges")
    ctx.metadata["ask_dias_config_cb"] = lambda op: None
    _ok(op._confirm_settings(ctx) is False, "«Avbryt pakking» → steget stoppes")
    asked.clear()
    ctx.metadata["ask_dias_config_cb"] = lambda op: (asked.append(op), None)[1]
    op2 = DiasPackageOperation(confirm_before_packing=False)
    _ok(op2._confirm_settings(ctx) is True and not asked,
        "bryter av → ingen spørsmål, lagrede innstillinger brukes")
    del ctx.metadata["ask_dias_config_cb"]
    _ok(op._confirm_settings(ctx) is True, "uten GUI (ingen callback) → pakker uten spørsmål")

    # run(): avbrudd i vinduet stopper før pakkingen
    siard = tmp / "uttrekk.siard"
    with zipfile.ZipFile(siard, "w") as zf:
        zf.writestr("header/metadata.xml",
                    "<siardArchive><dataOriginTimespan>01.10.2014 - 19.06.2020"
                    "</dataOriginTimespan></siardArchive>")
    seen: dict = {}

    def cancel(op):
        seen["period"] = (op.params["period_start"], op.params["period_end"])
        return None
    ctx2 = WorkflowContext(siard_path=siard, metadata={"file_logger": log,
                                                        "ask_dias_config_cb": cancel})
    r = DiasPackageOperation(period_start="1999-01-01", period_end="1999-12-31").run(ctx2)
    _ok(not r.success and "avbrutt" in r.message
        and seen.get("period") == ("2014-10-01", "2020-06-19"),
        "vinduet får perioden fra metadata.xml (ikke gamle verdier); avbrudd → steget feiler")


def test_dialog(tmp: Path) -> None:
    print("test_dialog")
    import customtkinter as ctk
    import gui.operations_panel as OP
    from gui.app import App
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    import settings
    saved_save = settings.save_op_params
    import gui.dias_dialog as DD
    DD.save_op_params = lambda *a, **k: None          # ikke skriv brukerens innstillinger
    # Inndatafila oppgir et ANNET spenn — kjøremodus skal ikke overstyre operasjonens
    other = tmp / "annen.siard"
    with zipfile.ZipFile(other, "w") as zf:
        zf.writestr("header/metadata.xml", "<siardArchive><dataOriginTimespan>2000 - 2001"
                                           "</dataOriginTimespan></siardArchive>")
    OP._current_siard_path = other
    extra = tmp / "vedlegg_avtale.pdf"
    extra.write_bytes(b"%PDF-1.4")
    op = DiasPackageOperation(period_start="2014-10-01", period_end="2020-06-19",
                              label="Testpakke", confirm_before_packing=True,
                              extra_files=json.dumps([{
                                  "src": str(extra),
                                  "dest": "administrative_metadata/vedlegg_avtale.pdf"}]))

    class _WP:
        def get_operations(self):
            return [type("O", (), {"operation_id": i})() for i in
                    ("unpack_siard", "repack_siard", "dias_package")]
    root = ctk.CTk()
    root.geometry("1200x800+0+0")
    root.workflow_panel = _WP()
    root._log = lambda m, lvl="info": None
    results: list = []
    seen: dict = {}

    def drive(answer):
        def click():
            tops = [w for w in root.winfo_children()
                    if isinstance(w, ctk.CTkToplevel) and w.winfo_exists()]
            if not tops:
                root.after(150, click)
                return
            d = tops[0]
            seen["title"] = d.title()
            seen["period"] = (d._vars["period_start"][0].get(), d._vars["period_end"][0].get())
            seen["files"] = [e["src"] for e in d._file_entries.values()]
            seen["buttons"] = _buttons(d)
            if answer == "fortsett":
                d._confirm()
            else:
                d._cancel()
        root.after(1000, click)

    def run_once(answer):
        th = threading.Thread(target=lambda: results.append(App._ask_dias_config.__get__(root)(op)))
        th.start()
        drive(answer)

        def poll():
            if th.is_alive():
                root.after(100, poll)
            else:
                root.quit()
        root.after(100, poll)
        root.mainloop()
    try:
        run_once("fortsett")
        _ok(seen["title"] == "DIAS-pakking: kontroller innstillingene før pakking"
            and "Fortsett pakking" in seen["buttons"] and "Avbryt pakking" in seen["buttons"],
            f"kjøremodus: egen tittel og knapper {seen['buttons']}")
        _ok(seen["period"] == ("2014-10-01", "2020-06-19"),
            f"perioden fra operasjonen beholdes (ikke inndatafilas 2000–2001): {seen['period']}")
        _ok(str(extra) in seen["files"], "tidligere valgt ekstra fil er med i treet")
        p = results[-1]
        _ok(isinstance(p, dict) and p["period_start"] == "2014-10-01" and p["label"] == "Testpakke"
            and str(extra) in [e["src"] for e in json.loads(p["extra_files"])]
            and p.get("confirm_before_packing") in (True, 1),
            "«Fortsett pakking» → parameterne (med ekstra fil og bryter) returneres")
        run_once("avbryt")
        _ok(results[-1] is None, "«Avbryt pakking» → None")
    finally:
        DD.save_op_params = saved_save
        OP._current_siard_path = None
        root.destroy()


def _buttons(widget) -> list:
    import customtkinter as ctk
    out = []
    for w in widget.winfo_children():
        if isinstance(w, ctk.CTkButton):
            out.append(w.cget("text"))
        out.extend(_buttons(w))
    return out


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="diaskontroll_") as td:
        tmp = Path(td)
        test_operation(tmp)
        test_dialog(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
