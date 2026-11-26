"""
SIARD-header-panelet utenom kjøring (nov. 2026).

Bakgrunn — ønsker fra bruker:
  - headeren skal vises så snart en SIARD-fil er lagt til / valgt i fillista
    (før ble den først vist når kjøringen startet)
  - uten «Pakk ut SIARD» + «Pakk sammen SIARD» i arbeidsflyten skal knappen
    være «Lagre endringer», og skrive metadata.xml direkte tilbake i fila
  - «Bekreft endringer» skal bare finnes når arbeidsflyten pakker ut og sammen
  - ubekreftede endringer stopper kjøringen før «Pakk sammen SIARD» med
    beskjed om at de må bekreftes for å bli med (se test_siard_header.py)

Verifiserer:
  1. kjernen: header-endringer skrives direkte i SIARD-fila; ugyldig verdi
     og «ingen endring» lar fila være urørt
  2. panelet: «Lagre endringer» / «Bekreft endringer» etter modus
  3. programmet: header vises ved filvalg, knappen følger arbeidsflyten,
     lagring direkte i fila med bekreftelse og PREMIS-hendelse, endringer
     gjort før kjøring beholdes når kjøringen starter

Kjør:  python -X utf8 tests/test_header_idle_save.py
"""
from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(Path(__file__).resolve().parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from test_siard_header import META, _ok  # noqa: E402


def _siard(path: Path) -> Path:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("header/", b"")
        zf.writestr("header/metadata.xml", META)
        zf.writestr("content/schema0/table0/table0.xml", "<table><row><c1>1</c1></row></table>")
        zf.writestr("content/schema0/table0/lob1/record1.bin", os.urandom(5000))
    return path


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_core(tmp: Path) -> None:
    print("\n[1] Header-endringer direkte i SIARD-fila")
    from siard_workflow.core import siard_header as H
    from siard_workflow.core.zip_progress import undo_path
    p = _siard(tmp / "kjerne.siard")
    with zipfile.ZipFile(p) as z:
        table = z.read("content/schema0/table0/table0.xml")
        lob = z.read("content/schema0/table0/lob1/record1.bin")
    changes = H.save_header_in_place(p, {"dataOwner": "Arkivsenter sør", "archiver": "Kari"})
    info = H.read_header(p)
    _ok(info["values"]["dataOwner"] == "Arkivsenter sør" and info["values"]["archiver"] == "Kari"
        and len(changes) == 2, f"feltene er skrevet: {'; '.join(changes)}")
    with zipfile.ZipFile(p) as z:
        _ok(z.testzip() is None and z.namelist().count("header/metadata.xml") == 1,
            "SIARD-fila er gyldig")
        _ok(z.read("content/schema0/table0/table0.xml") == table
            and z.read("content/schema0/table0/lob1/record1.bin") == lob
            and "<description>tabellbeskrivelse</description>" in z.read(
                "header/metadata.xml").decode(),
            "tabelldata, LOB-filer og resten av metadata.xml er uendret")
    _ok(not undo_path(p).exists(), "ingen angrefil igjen")

    sha = _sha(p)
    changes = H.save_header_in_place(p, {"archivalDate": "1.5.2020"})
    _ok(_sha(p) == sha and changes and changes[0].startswith("IKKE"),
        f"ugyldig verdi skrives ikke, fila er urørt ({changes[0]})")
    _ok(H.save_header_in_place(p, {"dataOwner": "Arkivsenter sør"}) == [] and _sha(p) == sha,
        "samme verdi som i fila → ingenting skrives")


def test_panel_modes() -> None:
    print("\n[2] Panelet: «Lagre endringer» / «Bekreft endringer»")
    import customtkinter as ctk
    from gui.header_panel import HeaderDock
    from siard_workflow.core import siard_header as H
    root = ctk.CTk()
    root.withdraw()
    try:
        dock = HeaderDock(root, persist=False)
        p = dock.panel
        btn = lambda: (p._confirm_btn.cget("text"), str(p._confirm_btn.cget("state")))
        dock.load(H.read_header_bytes(META.encode()), "x.siard", True, "")
        root.update()
        _ok(p.mode == "confirm" and btn() == ("Bekreft endringer", "disabled"),
            "standard: «Bekreft endringer» (ingen endringer → ikke aktiv)")
        p.set_mode("save")
        _ok(btn() == ("Lagre endringer", "disabled"), "modus «lagre»: knappen heter «Lagre endringer»")
        p.set_value("dataOwner", "Ny eier")
        _ok(btn() == ("Lagre endringer (1)", "normal")
            and p._marks["dataOwner"].cget("text") == "● ikke lagret",
            "endring → «Lagre endringer (1)» er aktiv, feltet er merket «ikke lagret»")
        saved: list = []
        confirmed: list = []
        p.on_save, p.on_confirm = saved.append, confirmed.append
        p._confirm_btn.invoke()
        _ok(saved == [{"dataOwner": "Ny eier"}] and not confirmed
            and p.unconfirmed() == {"dataOwner": "Ny eier"} and p.take_confirmed() == {},
            "klikk → endringene gis til lagring (ikke «bekreftet»); de står til fila er skrevet")
        p.set_busy(True, "Lagrer …")
        _ok(btn()[1] == "disabled" and str(p._undo_btn.cget("state")) == "disabled"
            and p._note.cget("text") == "Lagrer …", "låst mens lagringen pågår")
        p.set_busy(False)
        _ok(btn() == ("Lagre endringer (1)", "normal"), "og åpen igjen etterpå")

        p.set_mode("confirm")
        _ok(btn() == ("Bekreft endringer (1)", "normal")
            and p._marks["dataOwner"].cget("text") == "● ikke bekreftet",
            "modus «bekreft»: samme endring, nå «Bekreft endringer (1)»")
        p._confirm_btn.invoke()
        _ok(confirmed == [{"dataOwner": "Ny eier"}] and len(saved) == 1
            and p._confirmed == {"dataOwner": "Ny eier"}, "klikk → bekreftet (ikke lagret til fil)")
        p.set_mode("save")
        _ok(p.unconfirmed() == {"dataOwner": "Ny eier"} and p._confirmed == {}
            and btn() == ("Lagre endringer (1)", "normal"),
            "tilbake til «lagre»: det bekreftede er ikke skrevet noe sted → må lagres")
    finally:
        root.destroy()


def _pump(app, sec: float, until=None) -> bool:
    end = time.time() + sec
    while time.time() < end:
        app.update()
        if until is not None and until():
            return True
        time.sleep(0.02)
    return until is None


def test_app(tmp: Path) -> None:
    print("\n[3] Programmet")
    import settings
    from tkinter import messagebox
    from gui.app import App
    from siard_workflow.core import siard_header as H
    from siard_workflow.operations.pipeline_operations import (
        RepackSiardOperation, UnpackSiardOperation)

    d = tmp / "app"
    d.mkdir()
    siard = _siard(d / "Uttrekk.siard")
    other = _siard(d / "Annet.siard")

    orig_get = settings.get_config
    settings.get_config = lambda k, default=None: "" if k == "log_dir" else orig_get(k, default)
    orig_ask, orig_yesno = messagebox.askokcancel, messagebox.askyesno
    asked: list = []
    answer = {"v": False}
    messagebox.askokcancel = lambda title, text, **kw: (asked.append((title, text)), answer["v"])[1]

    sys.argv = ["app"]
    App._startup_dependency_check = lambda self: None
    app = App()
    _pump(app, 0.8)
    panel = app.header_dock.panel
    btn = lambda: (panel._confirm_btn.cget("text"), str(panel._confirm_btn.cget("state")))
    try:
        _ok(not app.header_dock.winfo_ismapped(), "(ingen fil valgt → panelet er skjult)")
        app._queue_push(siard)
        _ok(_pump(app, 10, lambda: panel.is_editable()), "fil lagt til → headeren leses og vises")
        _ok(app.header_dock.winfo_ismapped() and panel.get_value("dbname") == "KRS"
            and panel.get_value("dataOwner") == "Kommune"
            and "Uttrekk.siard" in panel._source_lbl.cget("text"),
            "panelet viser feltene fra metadata.xml i den valgte fila")
        _ok(panel.mode == "save" and btn()[0] == "Lagre endringer"
            and "direkte tilbake i SIARD-fila" in panel._note.cget("text"),
            "arbeidsflyt uten utpakking/sammenpakking → «Lagre endringer»")

        app.workflow_panel.add_operation(UnpackSiardOperation())
        _pump(app, 0.2)
        _ok(panel.mode == "save", "bare «Pakk ut SIARD» lagt til → fortsatt «Lagre endringer»")
        app.workflow_panel.add_operation(RepackSiardOperation())
        _pump(app, 0.2)
        _ok(panel.mode == "confirm" and btn()[0] == "Bekreft endringer"
            and "Bekreft endringer" in panel._note.cget("text"),
            "både utpakking og sammenpakking → «Bekreft endringer»")

        # Endring bekreftet FØR kjøring beholdes når kjøringen starter på samme fil
        panel.set_value("dataOwner", "Bekreftet før kjøring")
        panel.confirm()
        panel.set_value("archiver", "Ubekreftet før kjøring")
        info = H.read_header(siard)
        app._log_queue.put(("header_show", True, info, siard.name, True, "kjører", str(siard)))
        _pump(app, 0.4)
        _ok(panel._confirmed == {"dataOwner": "Bekreftet før kjøring"}
            and panel.unconfirmed() == {"archiver": "Ubekreftet før kjøring"}
            and panel.get_value("dataOwner") == "Bekreftet før kjøring",
            "kjøringen starter på samme fil → det som er skrevet og bekreftet, står")
        app._log_queue.put(("header_show", True, H.read_header(other), other.name, True,
                            "kjører", str(other)))
        _pump(app, 0.4)
        _ok(panel._confirmed == {} and panel.unconfirmed() == {}
            and "Annet.siard" in panel._source_lbl.cget("text"),
            "neste fil i køen → panelet lastes på nytt for den fila")

        # Tilbake til valgt fil, uten utpakking/sammenpakking → lagre direkte
        app._clear_workflow()
        app._set_active_file(siard)
        _ok(_pump(app, 10, lambda: panel.is_editable()) and panel.mode == "save",
            "arbeidsflyten tømt → «Lagre endringer» igjen")
        sha0 = hashlib.sha256(siard.read_bytes()).hexdigest()
        panel.set_value("dataOwner", "Arkivsenter sør")
        panel.set_value("description", "Ny beskrivelse")
        _ok(btn() == ("Lagre endringer (2)", "normal"), "to endringer → «Lagre endringer (2)»")
        panel._confirm_btn.invoke()                      # svarer «Avbryt» i advarselen
        _pump(app, 0.3)
        _ok(len(asked) == 1 and "Originalfila endres" in asked[0][1]
            and "Arkivsenter sør" in asked[0][1]
            and hashlib.sha256(siard.read_bytes()).hexdigest() == sha0
            and panel.unconfirmed(),
            "«Lagre endringer» → advarsel med endringene; «Avbryt» → fila er urørt")
        answer["v"] = True
        panel._confirm_btn.invoke()
        _ok(_pump(app, 15, lambda: not app._hdr_saving and panel.is_editable()
                  and not panel.unconfirmed()), "bekreftet → lagret og lastet på nytt")
        got = H.read_header(siard)["values"]
        _ok(got["dataOwner"] == "Arkivsenter sør" and got["description"] == "Ny beskrivelse",
            "SIARD-fila har de nye verdiene")
        with zipfile.ZipFile(siard) as z:
            _ok(z.testzip() is None, "og er et gyldig arkiv")
        _ok(panel.get_value("dataOwner") == "Arkivsenter sør"
            and btn() == ("Lagre endringer", "disabled")
            and "Lagret i SIARD-fila kl." in panel._note.cget("text"),
            "panelet viser verdiene fra fila; ingen ulagrede endringer")
        premis = d / "Uttrekk_premis.xml"
        _ok(premis.exists() and "skrevet direkte i SIARD-fila" in premis.read_text("utf-8")
            and "Arkivsenter sør" in premis.read_text("utf-8"),
            "PREMIS-hendelse er ført for endringen")
        logged = " | ".join(m for _l, m in app._log_entries)
        _ok("lagret direkte i Uttrekk.siard" in logged and "SHA-256" in logged,
            "kjøreloggen sier hva som er gjort, og at sjekksummer ikke gjelder lenger")

        # Ulagrede endringer når kjøringen startes uten utpakking/sammenpakking
        panel.set_value("archiver", "Ikke lagret")
        from siard_workflow.operations import SHA256Operation
        app.workflow_panel.add_operation(SHA256Operation())
        started: list = []
        yn: list = []
        messagebox.askyesno = lambda title, text, **kw: (yn.append((title, text)), False)[1]
        app._confirm_run_plan = lambda ops: started.append(1)     # nås bare hvis vi går videre
        app._run_workflow()
        _ok(len(yn) == 1 and "ikke lagret" in yn[0][0] and not started
            and panel.unconfirmed() == {"archiver": "Ikke lagret"},
            "kjøring startet med ulagrede endringer → spørsmål; «Nei» → ingenting startes")
    finally:
        messagebox.askokcancel, messagebox.askyesno = orig_ask, orig_yesno
        settings.get_config = orig_get
        try:
            app.destroy()
        except Exception:
            pass


def main() -> int:
    threading.Timer(180, lambda: os._exit(3)).start()
    with tempfile.TemporaryDirectory(prefix="hdridle_") as td:
        tmp = Path(td)
        test_core(tmp)
        test_panel_modes()
        test_app(tmp)
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
