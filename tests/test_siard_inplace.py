"""
«Berik SIARD» uten utpakking: valgfritt å skrive metadata.xml direkte i
SIARD-fila (nov. 2026).

Bakgrunn: uten «Pakk ut SIARD» må resultatet skrives som en ny SIARD-fil —
en full kopi (169 GB i brukerens tilfelle). Som valg kan ny metadata.xml
skrives rett inn i den eksisterende fila på sekunder. Da endres originalen,
så valget har advarsel, bekreftelse ved kjøring og en angrefil som legger
fila tilbake hvis skrivingen avbrytes.

Verifiserer:
  1. direkte endring: bare slutten av fila endres, arkivet er gyldig, alt
     annet innhold er urørt (også ZIP64 og arkiv med datadeskriptor)
  2. feil underveis → fila er byte for byte som før
  3. avbrutt prosess → angrefila ligger igjen, og neste bruk gjenoppretter
  4. skadet angrefil → klar feil, ingenting overskrives
  5. operasjonen: standard er ny fil; «direkte» endrer originalen og sier
     fra; avslått bekreftelse → ny fil
  6. parametervinduet: valget med advarsel og bekreftelse

Kjør:  python -X utf8 tests/test_siard_inplace.py
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import threading
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(Path(__file__).resolve().parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from test_siard_rewrite import _TABLES, _make_siard, _ok  # noqa: E402

_META = "header/metadata.xml"
_NEW = ("<siardArchive>" + "ny og lengre metadata " * 400 + "</siardArchive>").encode()


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_in_place(tmp: Path) -> None:
    print("\n[1] Direkte endring av metadata.xml")
    from siard_workflow.core import zip_progress as zp
    for label, kw, limit in (("vanlig arkiv", {}, None),
                             ("arkiv med datadeskriptor", {"streamed": True}, None),
                             ("ZIP64", {}, 2000)):
        print(f"  — {label}")
        p = tmp / f"direkte_{label[:6].strip()}.siard"
        orig_limit = zipfile.ZIP64_LIMIT
        if limit:
            zipfile.ZIP64_LIMIT = limit
        try:
            content = _make_siard(p, lob_mb=1, **kw)
            with zipfile.ZipFile(p) as z:
                start_dir = z.start_dir
                names = z.namelist()
            before = p.read_bytes()
            stats = zp.update_member_in_place(p, _META, _NEW)
        finally:
            zipfile.ZIP64_LIMIT = orig_limit
        after = p.read_bytes()
        _ok(after[:start_dir] == before[:start_dir],
            f"alt foran innholdslista er urørt ({start_dir} byte) — bare slutten er skrevet")
        with zipfile.ZipFile(p) as z:
            _ok(z.testzip() is None and len(z.namelist()) == len(names),
                f"gyldig arkiv med like mange oppføringer ({len(names)})")
            _ok(z.read(_META) == _NEW, "metadata.xml er den nye")
            _ok(all(z.read(n) == content[n] for n in names if n != _META),
                "alt annet innhold er uendret")
            _ok(z.namelist().count(_META) == 1, "metadata.xml står én gang i innholdslista")
        _ok(not zp.undo_path(p).exists(), "angrefila er slettet etter vellykket skriving")
        _ok(stats["size_after"] > stats["size_before"] and stats["entries"] == len(names),
            f"fila vokste med den nye oppføringen ({stats['size_before']} → {stats['size_after']} byte)")


def test_failure_rolls_back(tmp: Path) -> None:
    print("\n[2] Feil underveis → fila er som før")
    from siard_workflow.core import zip_progress as zp
    p = tmp / "feil.siard"
    _make_siard(p, lob_mb=1)
    sha0 = _sha(p)
    orig = zipfile.ZipFile.writestr

    def broken(self, zinfo, data, *a, **k):
        orig(self, zinfo, data[: len(data) // 2], *a, **k)     # halvskrevet …
        raise OSError("disken forsvant")

    zipfile.ZipFile.writestr = broken
    lines: list = []
    try:
        zp.update_member_in_place(p, _META, _NEW, w=lambda m, l="info": lines.append(m))
        raise AssertionError("skulle feilet")
    except OSError as exc:
        _ok("disken forsvant" in str(exc), "feilen meldes videre")
    finally:
        zipfile.ZipFile.writestr = orig
    _ok(_sha(p) == sha0, "SIARD-fila er byte for byte som før")
    _ok(not zp.undo_path(p).exists() and any("gjenopprettet" in m for m in lines),
        "gjenopprettet fra angrefila, som så er slettet — og loggen sier det")

    print("\n[3] Avbrutt prosess → gjenopprettes ved neste bruk")
    real_restore = zp.restore_interrupted_update
    zipfile.ZipFile.writestr = broken
    zp.restore_interrupted_update = lambda *a, **k: False       # «strømmen gikk»
    try:
        try:
            zp.update_member_in_place(p, _META, _NEW)
        except OSError:
            pass
    finally:
        zipfile.ZipFile.writestr = orig
        zp.restore_interrupted_update = real_restore
    _ok(zp.undo_path(p).exists() and _sha(p) != sha0,
        "(fila er halvskrevet, og angrefila ligger ved siden av)")
    _ok(zp.restore_interrupted_update(p) is True and _sha(p) == sha0
        and not zp.undo_path(p).exists(),
        "gjenoppretting → byte for byte som originalen")
    _ok(zp.restore_interrupted_update(p) is False, "uten angrefil skjer ingenting")

    print("\n[4] Skadet angrefil")
    undo = zp.undo_path(p)
    undo.write_bytes(json.dumps({"size": 10, "start_dir": 5, "tail_len": 99,
                                 "sha256": "x", "member": _META}).encode() + b"\nkort")
    try:
        zp.update_member_in_place(p, _META, _NEW)
        raise AssertionError("skulle feilet")
    except zipfile.BadZipFile as exc:
        _ok("skadet" in str(exc), "klar feilmelding")
    _ok(_sha(p) == sha0, "SIARD-fila er ikke rørt")
    undo.unlink()


def _template(d: Path) -> Path:
    tmpl = d / "mal.json"
    tmpl.write_text(json.dumps({"tables": [
        {"name": n, "description": f"Beskrivelse av {n}", "columns": [
            {"name": "ID", "description": "Løpenummer"},
            {"name": "NAVN", "description": "Navn"}]} for n, _f in _TABLES]}),
        encoding="utf-8")
    return tmpl


def test_operation(tmp: Path) -> None:
    print("\n[5] «Berik SIARD» uten utpakking")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.siardmapper_operation import SiardMapperOperation
    d = tmp / "op"
    d.mkdir()
    siard = d / "Uttrekk.siard"
    content = _make_siard(siard, lob_mb=2)
    sha0 = _sha(siard)
    tmpl = _template(d)
    out = d / "Uttrekk_beriket.siard"

    def run(mode, confirm=None):
        logs: list = []
        meta = {"progress_cb": lambda kind, **kw: logs.append((kw.get("level"), kw.get("msg", "")))
                if kind == "log" else None, "stop_event": threading.Event()}
        if confirm is not None:
            meta["confirm_inplace_cb"] = confirm
        op = SiardMapperOperation(json_template=str(tmpl))
        if mode:
            op.params["standalone_write"] = mode
        return op, op.run(WorkflowContext(siard_path=siard, metadata=meta)), logs

    op, r, logs = run(None)
    _ok(op.params["standalone_write"] == "ny_fil" and not op.writes_in_place(),
        "standard er ny fil")
    _ok(r.success and out.exists() and _sha(siard) == sha0 and not r.data.get("in_place"),
        "ny fil skrevet, originalen er urørt")
    out.unlink()

    asked: list = []
    op, r, logs = run("direkte", confirm=lambda name: asked.append(name) or False)
    _ok(asked == ["Uttrekk.siard"] and r.success and out.exists() and _sha(siard) == sha0,
        "«direkte» + avslått ved kjøring → ny fil, originalen er urørt")
    _ok(any("valgt bort ved kjøring" in m for _l, m in logs), "og loggen sier det")
    out.unlink()

    op, r, logs = run("direkte", confirm=lambda name: True)
    _ok(r.success and r.data.get("in_place") is True
        and r.data.get("output_path") == str(siard) and not out.exists(),
        f"«direkte» + bekreftet → ingen ny fil ({r.message})")
    with zipfile.ZipFile(siard) as z:
        meta_xml = z.read(_META).decode()
        _ok(z.testzip() is None and "Beskrivelse av KLIENT" in meta_xml,
            "original-SIARD-fila har fått beskrivelsene og er gyldig")
        _ok(all(z.read(n) == content[n] for n in content if n != _META),
            "alt annet innhold er uendret")
    _ok(any(lvl == "warn" and "DIREKTE" in m for lvl, m in logs),
        "advarsel i loggen om at originalfila endres")
    _ok(any(lvl == "warn" and "SHA-256" in m for lvl, m in logs),
        "og om at tidligere sjekksummer ikke gjelder lenger")
    _ok("skrevet direkte i SIARD-fila" in r.message,
        "resultatteksten (og PREMIS-hendelsen) sier at det ble skrevet direkte")

    # Uten GUI (ingen bekreftelse tilgjengelig) følges innstillingen
    _make_siard(siard, lob_mb=1)
    op, r, logs = run("direkte")
    _ok(r.success and r.data.get("in_place") is True, "uten GUI følges innstillingen")


def test_param_dialog() -> None:
    print("\n[6] Parametervinduet")
    import customtkinter as ctk
    import gui.siardmapper_param_dialog as P
    from gui.operations_panel import OP_DEFS
    from siard_workflow.operations.siardmapper_operation import SiardMapperOperation
    op_def = next(d for d in OP_DEFS if d.get("cls") is SiardMapperOperation)
    par = next(p for p in op_def["params"] if p["key"] == "standalone_write")
    _ok(par["default"] == "ny_fil" and par["choices"] == ["ny_fil", "direkte"],
        "parameteren følger med arbeidsflyt/prosjekt, standard «ny_fil»")

    P.SiardMapperParamDialog._prompt_template_dir = lambda self: None
    P.SiardMapperParamDialog._refresh_async = lambda self: None
    asked: list = []
    answers = [False, True]
    orig_ask = P.messagebox.askokcancel
    P.messagebox.askokcancel = lambda title, text, **kw: (asked.append(text), answers.pop(0))[1]
    root = ctk.CTk()
    root.withdraw()
    got: list = []
    try:
        dlg = P.SiardMapperParamDialog(root, op_def, on_confirm=got.append)
        root.update()
        _ok(dlg._write_var.get() == "ny_fil" and not dlg._write_warn.winfo_manager(),
            "standard: «Skriv ny fil», ingen advarsel vist")
        dlg._json_path_var.set("C:/maler/mal.json")
        dlg._write_var.set("direkte")
        dlg._on_write_mode()
        root.update()
        _ok(dlg._write_warn.winfo_manager() == "grid"
            and "ADVARSEL" in dlg._write_warn.cget("text"),
            "«Skriv direkte» valgt → advarselen vises i vinduet")
        dlg._ok()
        _ok(len(asked) == 1 and "original-SIARD-fila endres" in asked[0] and not got
            and dlg.winfo_exists(),
            "OK → bekreftelse med advarsel; «Avbryt» der → ingenting legges til")
        dlg._ok()
        _ok(len(got) == 1 and got[0].params["standalone_write"] == "direkte"
            and got[0].writes_in_place(),
            "bekreftet → steget legges til med «direkte»")

        # Eksisterende steg åpnes igjen med valget sitt
        live = {**op_def, "params": [
            {**p, "default": got[0].params.get(p["key"], p["default"])}
            for p in op_def["params"]]}
        dlg2 = P.SiardMapperParamDialog(root, live, on_confirm=got.append)
        root.update()
        _ok(dlg2._write_var.get() == "direkte" and dlg2._write_warn.winfo_manager() == "grid",
            "steget åpnet på nytt viser valget og advarselen")
        dlg2._write_var.set("ny_fil")
        dlg2._on_write_mode()
        dlg2._ok()
        _ok(len(asked) == 2 and got[-1].params["standalone_write"] == "ny_fil",
            "tilbake til «ny fil» → ingen bekreftelse trengs")
    finally:
        P.messagebox.askokcancel = orig_ask
        root.destroy()


def main() -> int:
    threading.Timer(180, lambda: os._exit(3)).start()
    with tempfile.TemporaryDirectory(prefix="siardinplace_") as td:
        tmp = Path(td)
        test_in_place(tmp)
        test_failure_rolls_back(tmp)
        test_operation(tmp)
    test_param_dialog()
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    except BaseException:
        import traceback
        traceback.print_exc()
        rc = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)
