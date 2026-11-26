"""
Valg av SIARD-fil skal ikke henge programmet (nov. 2026).

Bakgrunn: da en stor SIARD-fil ble valgt, tok det veldig lang tid før
brukergrensesnittet svarte.
  - Første gang en fil ble valgt, ble beste temp-disk funnet i hovedtråden
    (PowerShell-spørring mot Windows — ca. 11 s på brukerens maskin).
  - Headeren og forslagslista ble lest ved å åpne arkivet med zipfile, som
    bygger ett Python-objekt per fil. I en bakgrunnstråd holder det igjen
    brukergrensesnittet (målt: 13 mot 890 hendelser i sekundet).
  - Header-panelet viste tomme felt mens det leste, og ledeteksten nederst
    ble kuttet i høyre kant (brutt etter skjermpiksler, ikke skalert bredde).

Verifiserer:
  1. rask lesing av én oppføring: riktig innhold i alle arkivformer, None når
     den ikke kan brukes (da overtar zipfile)
  2. headeren og forslagsfeltene leses uten zipfile
  3. panelet: ventevisning uten felt mens fila leses; ledeteksten brytes
     etter panelets bredde
  4. programmet: filvalg returnerer straks, temp-disk i bakgrunnen, og
     arbeidsflyten kan settes sammen mens det lastes

Kjør:  python -X utf8 tests/test_select_responsive.py
"""
from __future__ import annotations

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

from test_siard_header import META, _ok       # noqa: E402
from test_siard_rewrite import _NoSeek        # noqa: E402

_NAMES = ("header/metadata.xml", "metadata.xml")


def _zip(path: Path, entries: list, *, streamed: bool = False,
         ctype: int = zipfile.ZIP_DEFLATED) -> Path:
    target = _NoSeek(path) if streamed else path
    with zipfile.ZipFile(target, "w", ctype, allowZip64=True) as z:
        for name, data in entries:
            z.writestr(name, data)
    if streamed:
        target.close()
    return path


def test_fast_read(tmp: Path) -> None:
    print("\n[1] Rask lesing av én oppføring")
    from siard_workflow.core.zip_progress import read_member_fast
    meta = META.encode()
    filler = [(f"content/schema0/table0/lob1/record{i}.bin", os.urandom(300)) for i in range(40)]

    for label, entries, kw in (
            ("først i arkivet", [("header/metadata.xml", meta)] + filler, {}),
            ("sist i arkivet", filler + [("header/metadata.xml", meta)], {}),
            ("lagret ukomprimert", filler + [("header/metadata.xml", meta)],
             {"ctype": zipfile.ZIP_STORED}),
            ("arkiv med datadeskriptor", filler + [("header/metadata.xml", meta)],
             {"streamed": True})):
        p = _zip(tmp / f"f_{label[:5].strip()}_{len(kw)}.siard", entries, **kw)
        _ok(read_member_fast(p, _NAMES) == ("header/metadata.xml", meta), f"{label}: riktig innhold")

    p = _zip(tmp / "rot.siard", filler + [("metadata.xml", meta)])
    _ok(read_member_fast(p, _NAMES) == ("metadata.xml", meta),
        "metadata.xml i rota brukes når header/metadata.xml ikke finnes")
    p = _zip(tmp / "begge.siard", [("metadata.xml", b"<rot/>")] + filler
             + [("header/metadata.xml", meta)])
    _ok(read_member_fast(p, _NAMES)[0] == "header/metadata.xml",
        "header/metadata.xml har forrang, uansett rekkefølge i arkivet")
    p = _zip(tmp / "lignende.siard", [("xheader/metadata.xml", b"feil"),
                                      ("header/metadata.xml.bak", b"feil")] + filler)
    _ok(read_member_fast(p, _NAMES) is None,
        "lignende navn (lengre foran/bak) regnes ikke som treff")

    orig = zipfile.ZIP64_LIMIT
    zipfile.ZIP64_LIMIT = 3000                   # posisjoner «over 4 GB» → ZIP64-tillegg
    try:
        p = _zip(tmp / "z64.siard", filler + [("header/metadata.xml", meta)])
    finally:
        zipfile.ZIP64_LIMIT = orig
    _ok(read_member_fast(p, _NAMES) == ("header/metadata.xml", meta),
        "ZIP64-posisjon i innholdslista (filer over 4 GB foran)")
    p = tmp / "mange.siard"
    with zipfile.ZipFile(p, "w", zipfile.ZIP_STORED, allowZip64=True) as z:
        for i in range(66_000):                  # > 65 535 oppføringer → ZIP64-sluttpost
            z.writestr(f"c/r{i}", b"")
        z.writestr("header/metadata.xml", meta)
    t0 = time.perf_counter()
    got = read_member_fast(p, _NAMES)
    dt = time.perf_counter() - t0
    _ok(got == ("header/metadata.xml", meta),
        f"ZIP64-sluttpost (66 000 filer) — lest på {dt * 1000:.0f} ms")

    _ok(read_member_fast(tmp / "finnes_ikke.siard", _NAMES) is None, "manglende fil → None")
    bad = tmp / "ikke_zip.siard"
    bad.write_bytes(b"ikke et arkiv" * 100)
    _ok(read_member_fast(bad, _NAMES) is None, "ikke et zip-arkiv → None")
    p = _zip(tmp / "skadet.siard", filler + [("header/metadata.xml", meta)],
             ctype=zipfile.ZIP_STORED)
    raw = bytearray(p.read_bytes())
    at = raw.find(b"<dbname>")
    raw[at + 8] ^= 0xFF                          # én byte endret i innholdet
    p.write_bytes(bytes(raw))
    _ok(read_member_fast(p, _NAMES) is None, "feil sjekksum → None (ikke skadet innhold)")


def test_no_zipfile(tmp: Path) -> None:
    print("\n[2] Header og forslagsfelt leses uten zipfile")
    from siard_workflow.core import siard_header as H
    import gui.operations_panel as op
    p = _zip(tmp / "uten.siard", [("content/a.bin", b"x" * 100),
                                  ("header/metadata.xml", META.encode())])
    opened: list = []
    real = zipfile.ZipFile

    class _Counting(real):
        def __init__(self, file, mode="r", *a, **k):
            opened.append(str(file))
            super().__init__(file, mode, *a, **k)

    zipfile.ZipFile = _Counting
    try:
        info = H.read_header(p)
        _ok(info["values"]["dbname"] == "KRS" and not opened,
            "read_header: innholdslista bygges ikke")
        got = op._read_siard_meta(p)
        _ok(got.get("databaseuser") is None and not opened and isinstance(got, dict),
            "forslagsfeltene: innholdslista bygges ikke")
        # Arkiv med data foran (selvutpakkende o.l.) håndteres ikke av den
        # raske veien — da overtar zipfile, og resultatet er det samme
        pre = tmp / "foran.siard"
        pre.write_bytes(b"X" * 5000 + p.read_bytes())
        info2 = H.read_header(pre)
        _ok(info2 is not None and info2["values"]["dbname"] == "KRS" and opened,
            "uvanlig arkiv → zipfile overtar, samme resultat")
    finally:
        zipfile.ZipFile = real


def test_panel() -> None:
    print("\n[3] Header-panelet: ventevisning og ledetekst")
    import customtkinter as ctk
    from gui.header_panel import HeaderDock
    from siard_workflow.core import siard_header as H
    root = ctk.CTk()
    root.geometry("1200x700+20+20")
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(0, weight=1)
    ctk.CTkFrame(root).grid(row=0, column=0, sticky="nsew")
    try:
        dock = HeaderDock(root, persist=False)
        dock.grid(row=0, column=1, sticky="ns")
        p = dock.panel
        dock.show_loading("Uttrekk.siard", "Leser header fra SIARD-fila …")
        root.update()
        _ok(p.is_loading() and p._wait.winfo_ismapped() and not p._body.winfo_viewable()
            and not p._confirm_btn.winfo_ismapped(),
            "mens fila leses: «arbeider»-felt, ingen tomme felt og ingen knapp")
        _ok("Leser header" in p._wait_lbl.cget("text")
            and p._source_lbl.cget("text") == "Uttrekk.siard",
            "filnavnet og hva som skjer, står i panelet")
        _ok(not p.is_editable() and str(p._undo_btn.cget("state")) == "disabled",
            "ingenting kan endres før innholdet er der")
        dock.load(H.read_header_bytes(META.encode()), "Uttrekk.siard", True, "Kort ledetekst.")
        root.update()
        _ok(not p.is_loading() and p._body.winfo_viewable() and not p._wait.winfo_ismapped()
            and p._confirm_btn.winfo_ismapped() and p.get_value("dbname") == "KRS",
            "innholdet er lest → feltene vises med verdier")

        long_note = ("«Lagre endringer» skriver metadata.xml direkte tilbake i SIARD-fila "
                     "(originalfila endres). Legg til «Pakk ut SIARD» og «Pakk sammen "
                     "SIARD» for å få endringene inn i en ny fil i stedet.")
        p.set_note(long_note)
        scale = float(ctk.ScalingTracker.get_widget_scaling(p))
        for w in (440, 300):
            dock.set_width(w)
            root.update()
            root.update()
            wrap = int(p._note.cget("wraplength"))
            width = p.winfo_width()                       # skjermpiksler
            _ok(wrap * scale <= width - 20 * scale and wrap >= 140,
                f"bredde {width} px (skalering {scale:g}): teksten brytes ved "
                f"{wrap * scale:.0f} px — innenfor panelet")
            lbl = p._note._label
            _ok(lbl.winfo_reqwidth() <= width and lbl.winfo_reqheight() > 2 * lbl.winfo_reqheight() // 3
                and lbl.winfo_height() >= lbl.winfo_reqheight() - 2,
                f"hele ledeteksten får plass ({lbl.winfo_reqheight()} px høy, "
                f"{lbl.winfo_height()} px tildelt)")
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
    print("\n[4] Programmet: filvalg henger ikke")
    import settings
    from gui.app import App
    from siard_workflow.operations.pipeline_operations import (
        RepackSiardOperation, UnpackSiardOperation)

    siard = _zip(tmp / "Uttrekk.siard", [("header/metadata.xml", META.encode()),
                                         ("content/schema0/table0/table0.xml", b"<table/>")])
    orig_set = settings.set_config
    saved: list = []
    settings.set_config = lambda k, v: saved.append((k, v))        # ikke rør config.json
    orig_cands = App._temp_candidates
    release = threading.Event()

    def slow_candidates(siard_path):
        release.wait(20)                                  # «PowerShell bruker 11 sekunder»
        return Path("T:/temp"), [{"path": Path("T:/temp"), "label": "T: (syntetisk NVMe)"}]

    App._temp_candidates = staticmethod(slow_candidates)
    sys.argv = ["app"]
    App._startup_dependency_check = lambda self: None
    app = App()
    _pump(app, 0.6)
    app._global_temp_dir = None
    panel = app.header_dock.panel
    try:
        t0 = time.perf_counter()
        app._queue_push(siard)
        dt = time.perf_counter() - t0
        _ok(dt < 0.6, f"filvalget returnerer straks ({dt * 1000:.0f} ms) selv om "
                      f"temp-disk-valget ikke er ferdig")
        _ok(siard in app.siard_queue and app.siard_path == siard,
            "fila står i lista og er aktiv")
        _ok("finner beste disk" in app.temp_label.cget("text") and app._global_temp_dir is None,
            "temp-linja viser at beste disk letes opp i bakgrunnen")
        _ok(panel.is_loading() and not panel._body.winfo_viewable(),
            "header-panelet viser «arbeider»-feltet, ikke tomme felt")

        # Brukeren setter sammen arbeidsflyten mens det lastes
        app.workflow_panel.add_operation(UnpackSiardOperation())
        app.workflow_panel.add_operation(RepackSiardOperation())
        _ok([o.operation_id for o in app.workflow_panel.get_operations()]
            == ["unpack_siard", "repack_siard"],
            "arbeidsflyten kan settes sammen mens det lastes")
        _ok(_pump(app, 10, lambda: panel.is_editable()) and panel.get_value("dbname") == "KRS",
            "headeren kommer når den er lest")
        _ok(panel.mode == "confirm" and "Bekreft endringer" in panel._confirm_btn.cget("text"),
            "knappen følger arbeidsflyten som ble satt sammen i mellomtiden")
        _ok(app._global_temp_dir is None, "(temp-disk-valget pågår fortsatt)")

        release.set()
        _ok(_pump(app, 10, lambda: app._global_temp_dir is not None)
            and app._global_temp_dir == Path("T:/temp")
            and "T:" in app.temp_label.cget("text"),
            "temp-disken tas i bruk når valget er ferdig")
        _ok(("global_temp_dir", str(Path("T:/temp"))) in saved
            and any("Temp-mappe (auto)" in m for _l, m in app._log_entries),
            "valget lagres og logges som før")

        # Valgt av brukeren i mellomtiden → det automatiske valget overstyrer ikke
        app._global_temp_dir = Path("U:/eget")
        app._apply_auto_temp(siard, Path("T:/temp"), [])
        _ok(app._global_temp_dir == Path("U:/eget"),
            "temp-mappe valgt av brukeren i mellomtiden overstyres ikke")
    finally:
        release.set()
        App._temp_candidates = orig_cands
        settings.set_config = orig_set
        try:
            app.destroy()
        except Exception:
            pass


def main() -> int:
    threading.Timer(180, lambda: os._exit(3)).start()
    with tempfile.TemporaryDirectory(prefix="selresp_") as td:
        tmp = Path(td)
        test_fast_read(tmp)
        test_no_zipfile(tmp)
        test_panel()
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
