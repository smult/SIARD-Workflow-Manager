"""
DIAS-pakking: «Husk»-avkrysning per felt (nov. 2026).

Bakgrunn: alle DIAS-feltene ble lagret i op_params etter hver bekreftelse og
fulgte dermed med til neste uttrekk. Nå huskes bare felt krysset av for «Husk»
— i programmets innstillinger (config.json), uavhengig av uttrekk og system.

  1. Første gang (ingen valg lagret): standardfeltene hentes fra de gamle
     op_params, resten (Submission Agreement, tittel …) huskes ikke.
  2. save_remembered → nye DIAS-steg får bare avkryssede felt; eksplisitte
     parametre (prosjektfila) går foran.
  3. Dialogen: «Husk»-avkrysning per felt, huskede verdier fylles inn i nye
     steg (ikke i et eksisterende steg), bekreftelse lagrer valgene.

Kjør:  python -X utf8 tests/test_dias_remember.py
"""
from __future__ import annotations

import json
import shutil
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


def test_core(tmp: Path) -> None:
    print("test_core")
    import settings
    from siard_workflow.core import dias_remember as R
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation
    defaults = DiasPackageOperation.default_params

    # 1. Gamle op_params, ingen valg ennå
    settings.save_settings({"op_params": {"dias_package": {
        "creator": "Arkivsenter X", "label": "Forrige uttrekk", "preserver": "KDRS-test"}}})
    _ok(R.remembered_fields() == list(R.DEFAULT_FIELDS), "standardutvalg før brukeren har valgt")
    _ok(R.remembered_values() == {"creator": "Arkivsenter X", "preserver": "KDRS-test"},
        f"standardfeltene hentes fra gamle op_params ({R.remembered_values()})")
    op = DiasPackageOperation()
    _ok(op.params["creator"] == "Arkivsenter X" and op.params["label"] == defaults["label"],
        "nytt steg: Skaper huskes, pakketittel fra forrige uttrekk gjør det ikke")

    # 2. Brukeren krysser av tittel, ikke Skaper
    stored = R.save_remembered(["label", "confirm_before_packing"],
                               {"label": "Ny tittel", "creator": "Y",
                                "confirm_before_packing": False})
    _ok(stored == {"label": "Ny tittel", "confirm_before_packing": False},
        "bare avkryssede felt lagres")
    cfg = json.loads(Path(settings._CONFIG_FILE).read_text("utf-8"))
    _ok(cfg["dias_remember_fields"] == ["label", "confirm_before_packing"]
        and cfg["dias_remembered_values"]["label"] == "Ny tittel",
        "lagret i programmets innstillinger (config.json)")
    op = DiasPackageOperation()
    _ok(op.params["label"] == "Ny tittel" and op.params["creator"] == defaults["creator"]
        and op.params["confirm_before_packing"] is False,
        "nytt steg: huskede felt fylles inn, Skaper (ikke avkrysset) tilbake til standard")
    op = DiasPackageOperation(label="Prosjektets tittel")
    _ok(op.params["label"] == "Prosjektets tittel",
        "eksplisitte parametre (prosjektfila) går foran huskede verdier")
    R.save_remembered([], {"label": "x"})
    _ok(R.remembered_values() == {} and DiasPackageOperation().params["label"] == defaults["label"],
        "ingen avkrysset → ingenting huskes")


def test_dialog(tmp: Path) -> None:
    print("test_dialog")
    import customtkinter as ctk
    import settings
    from gui.operations_panel import OP_DEFS
    from gui.dias_dialog import DiasParamDialog
    from siard_workflow.core import dias_remember as R
    op_def = next(d for d in OP_DEFS if d.get("custom_dialog") == "DiasParamDialog")
    R.save_remembered(["label", "preserver"], {"label": "Husket tittel", "preserver": "KDRS"})

    root = ctk.CTk()
    root.geometry("300x200+0+0")
    got: list = []
    saved: list = []
    try:
        # Nytt steg (fra operasjonspanelet)
        d = DiasParamDialog(root, {**op_def, "apply_remembered": True},
                            on_confirm=got.append,
                            on_saved=lambda *a, **k: saved.append(a))
        root.update()
        _ok(set(d._remember) == {p["key"] for p in op_def["params"]},
            "«Husk»-avkrysning for hvert felt")
        _ok(d._remember["label"].get() and d._remember["preserver"].get()
            and not d._remember["submission_agreement"].get(),
            "avkrysningen viser lagrede valg")
        _ok(d._vars["label"][0].get() == "Husket tittel",
            "nytt steg: huskede verdier er fylt inn")
        d._vars["period_start"][0].set("2001-01-01")     # plassholderen er ugyldig
        d._vars["period_end"][0].set("2010-12-31")
        d._vars["submission_agreement"][0].set("2026-99/1")
        d._remember["submission_agreement"].set(True)
        d._remember["preserver"].set(False)
        d._confirm()
        _ok(got and got[-1].params["submission_agreement"] == "2026-99/1",
            "steget får verdiene fra skjemaet")
        _ok(R.remembered_fields() == ["submission_agreement", "label"]
            and R.remembered_values() == {"submission_agreement": "2026-99/1",
                                          "label": "Husket tittel"},
            f"bekreftelse lagrer avkryssede felt ({R.remembered_values()})")
        _ok(saved and saved[-1][2] == settings._CONFIG_FILE,
            "loggen får stien til programmets innstillinger")

        # Eksisterende steg (prosjektets verdier) — huskede verdier overstyrer ikke
        live = {**op_def, "params": [{**p, "default": ("Prosjektets tittel" if p["key"] == "label"
                                                       else p["default"])}
                                     for p in op_def["params"]],
                "initial_extra_files": "[]"}
        d2 = DiasParamDialog(root, live, on_confirm=lambda o: None)
        root.update()
        _ok(d2._vars["label"][0].get() == "Prosjektets tittel",
            "eksisterende steg: verdiene for uttrekket beholdes")
        d2._cancel()
    finally:
        root.destroy()


def main() -> None:
    import settings
    tmp = Path(tempfile.mkdtemp(prefix="dias_remember_"))
    orig = (settings._CONFIG_FILE, settings._SETTINGS_FILE)
    settings._CONFIG_FILE = tmp / "config.json"       # ikke brukerens innstillinger
    settings._SETTINGS_FILE = tmp / "settings.json"
    try:
        test_core(tmp)
        test_dialog(tmp)
    finally:
        settings._CONFIG_FILE, settings._SETTINGS_FILE = orig
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
