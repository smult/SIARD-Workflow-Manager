"""
SIARD-headeren (siard_workflow/core/siard_header.py) og header-panelet.

Verifiserer:
  1. Lesing: bare header-delen (ikke <description> i tabeller), prefiks,
     tomt element, messageDigest med underelementer, versjon, .siard og mappe
  2. Skriving: endret felt, æøå/&/< escapes, CRLF bevares, resten byte-likt,
     tomt valgfritt felt fjernes, manglende felt settes inn på skjemaplass,
     estimat-kommentaren foran dataOriginTimespan fjernes ved manuell verdi
  3. Validering: påkrevde felt, archivalDate som xs:date — ugyldig skrives ikke
  4. Header-panelet: endring → ventende, ugyldig → ikke ventende, oppdatering
     fra fil overskriver ikke et felt som er under redigering, minimer/utvid

Kjør:  python -X utf8 tests/test_siard_header.py
"""
from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core import siard_header as H  # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


META = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">\n'
    '  <dbname>KRS</dbname>\n'
    '  <description>Gammel &amp; god</description>\n'
    '  <dataOwner>Kommune</dataOwner>\n'
    '  <!-- dataOriginTimespan er estimert av SIARD Workflow Manager 2026-10-14 ut fra x -->\n'
    '  <dataOriginTimespan>2001-01-01 - 2010-12-31</dataOriginTimespan>\n'
    '  <lobFolder>content</lobFolder>\n'
    '  <producerApplication/>\n'
    '  <archivalDate>2020-05-01</archivalDate>\n'
    '  <messageDigest><digestType>MD5</digestType><digest>ABC</digest></messageDigest>\n'
    '  <databaseUser>sa</databaseUser>\n'
    '  <schemas><schema><name>s</name><description>tabellbeskrivelse</description>'
    '<tables><table><name>t</name><description>T</description></table></tables>'
    '</schema></schemas>\n'
    '</siardArchive>\n')


def test_read() -> None:
    print("test_read")
    h = H.read_header_bytes(META.encode())
    v = h["values"]
    _ok(v["dbname"] == "KRS" and v["description"] == "Gammel & god",
        "felt leses og avescapes")
    _ok(v["archiver"] == "" and "archiver" not in h["present"], "manglende felt er tomt")
    _ok(v["producerApplication"] == "" and "producerApplication" in h["present"],
        "tomt element <x/> leses")
    _ok(v["messageDigest"] == "MD5 ABC", "messageDigest med underelementer")
    _ok(h["version"] == "2.1", "versjon fra rot-elementet")
    pre = META.replace("<dbname>", "<s:dbname>").replace("</dbname>", "</s:dbname>")
    _ok(H.read_header_bytes(pre.encode())["values"]["dbname"] == "KRS",
        "prefiks i elementnavn")
    nodesc = META.replace("  <description>Gammel &amp; god</description>\n", "")
    _ok(H.read_header_bytes(nodesc.encode())["values"]["description"] == "",
        "<description> i schema/tabell treffes ikke")


def test_write() -> None:
    print("test_write")
    raw = META.replace("\n", "\r\n").encode()
    new, ch = H.apply_header(raw, {"dbname": "KRS", "dataOwner": "Ny eier <AS> & co",
                                   "description": "",
                                   "archiver": "Kari Øvrebø",
                                   "producerApplication": "SIARD Suite",
                                   "dataOriginTimespan": "2001-01-01 - 2012-12-31"})
    t = new.decode()
    _ok("<dataOwner>Ny eier &lt;AS&gt; &amp; co</dataOwner>" in t, "verdi escapes")
    _ok("<description>" not in t.split("<schemas>")[0], "tomt valgfritt felt fjernes")
    _ok("tabellbeskrivelse" in t, "description i schema står urørt")
    _ok("<dbname>KRS</dbname>\r\n  <archiver>Kari Øvrebø</archiver>\r\n" in t,
        "manglende felt settes inn på skjemaplass (etter forrige felt), CRLF bevart")
    _ok("<producerApplication>SIARD Suite</producerApplication>" in t, "<x/> fylles")
    _ok("estimert av SIARD" not in t
        and "  <dataOriginTimespan>2001-01-01 - 2012-12-31</dataOriginTimespan>\r\n" in t,
        "estimat-kommentaren fjernes ved manuell verdi")
    _ok("\n" not in t.replace("\r\n", ""), "ingen nakne LF")
    _ok(t.split("<schemas>")[1] == raw.decode().split("<schemas>")[1], "schema-delen byte-lik")
    _ok(len([c for c in ch if not c.startswith("IKKE")]) == 5,
        f"fem endringer rapportert ({ch})")
    same, ch2 = H.apply_header(raw, {"dbname": "KRS", "archiver": ""})
    _ok(same is raw and not ch2, "ingen endring → samme bytes")


def test_validate() -> None:
    print("test_validate")
    _ok(H.validate("dbname", " ") is not None, "påkrevd dbname kan ikke være tomt")
    _ok(H.validate("archivalDate", "2020-13-01") is not None, "ugyldig måned")
    _ok(H.validate("archivalDate", "01.05.2020") is not None, "norsk datoformat avvises")
    _ok(H.validate("archivalDate", "2020-05-01Z") is None, "xs:date med sone godtas")
    _ok(H.validate("archiver", "") is None, "valgfritt felt kan være tomt")
    new, ch = H.apply_header(META.encode(), {"archivalDate": "1.5.2020", "dbname": ""})
    _ok(new == META.encode() and len(ch) == 2 and all(c.startswith("IKKE") for c in ch),
        "ugyldige verdier skrives ikke")


def test_sources(tmp: Path) -> None:
    print("test_sources")
    d = tmp / "x"
    (d / "header").mkdir(parents=True)
    (d / "header/metadata.xml").write_bytes(META.encode())
    _ok(H.read_header(d)["values"]["dbname"] == "KRS", "utpakket mappe")
    z = tmp / "x.siard"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("header/metadata.xml", META)
    _ok(H.read_header(z)["values"]["dataOwner"] == "Kommune", ".siard-fil")
    ch = H.apply_header_file(d / "header/metadata.xml", {"dbname": "Nytt"})
    _ok(ch and H.read_header(d)["values"]["dbname"] == "Nytt", "apply_header_file skriver")
    _ok(H.read_header(tmp / "finnes_ikke.siard") is None, "manglende fil → None")


def test_panel() -> None:
    print("test_panel")
    import customtkinter as ctk
    from gui.header_panel import HeaderDock
    root = ctk.CTk()
    root.geometry("1200x700")
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(0, weight=1)
    ctk.CTkFrame(root).grid(row=0, column=0, sticky="nsew")
    dock = HeaderDock(root, persist=False)
    dock.grid(row=0, column=1, sticky="ns")
    p = dock.panel
    info = H.read_header_bytes(META.encode())
    dock.load(info, "x.siard", editable=True, note="")
    root.update()
    _ok(p.get_value("dbname") == "KRS", "panelet viser verdiene")
    p.set_value("dataOwner", "Ny eier")
    p.set_value("archivalDate", "1.5.2020")
    _ok(p.unconfirmed() == {"dataOwner": "Ny eier"} and p.take_confirmed() == {},
        "endring er ubekreftet og skrives ikke automatisk; ugyldig dato ikke med")
    _ok("archivalDate" in p.invalid_fields(), "ugyldig felt meldes")
    _ok(p._confirm_btn.cget("state") == "normal"
        and "(1)" in p._confirm_btn.cget("text"), "«Bekreft endringer (1)» er aktiv")
    seen: list = []
    p.on_confirm = seen.append
    p.confirm()
    _ok(seen == [{"dataOwner": "Ny eier"}] and p.unconfirmed() == {}
        and p._confirm_btn.cget("state") == "disabled", "bekreftet; knappen inaktiv")
    p.set_value("dbname", "Skrives")
    info2 = H.read_header_bytes(META.replace("<databaseUser>sa", "<databaseUser>dba")
                                .encode())
    dock.refresh(info2, editable=True, note="")
    root.update()
    _ok(p.get_value("dbname") == "Skrives" and p.get_value("dataOwner") == "Ny eier",
        "ubekreftet og bekreftet (ikke skrevet) felt overskrives ikke")
    _ok(p.get_value("databaseUser") == "dba", "andre felt oppdateres fra fila")
    _ok(p.take_confirmed() == {"dataOwner": "Ny eier"}
        and p.unconfirmed() == {"dbname": "Skrives"}, "bekreftet/ubekreftet holdes adskilt")
    p.set_value("dataOwner", "Ny eier 2")
    p.confirm()
    p.set_value("dataOwner", "Ny eier 3")
    _ok("dataOwner" not in p.take_confirmed()
        and p.unconfirmed().get("dataOwner") == "Ny eier 3",
        "endret etter bekreftelse → må bekreftes på nytt")
    p.undo()
    _ok(p.unconfirmed() == {} and p.get_value("dataOwner") == "Kommune", "Angre")

    class _Ev:
        def __init__(self, x):
            self.x_root = x
    sc = ctk.ScalingTracker.get_widget_scaling(p)
    w0 = dock._width
    dock._drag_start(_Ev(1000))
    dock._drag(_Ev(900))
    root.update()
    _ok(not p.winfo_ismapped() and dock._ghost.winfo_ismapped(),
        "under draing er panelinnholdet skjult")
    _ok(dock._width == w0, "panelet får ikke ny bredde før knappen slippes")
    dock._drag(_Ev(880))
    root.after(60)
    root.update()
    dock._drag_end(_Ev(880))
    root.update()
    _ok(p.winfo_ismapped() and not dock._ghost.winfo_ismapped()
        and abs(dock._width - (w0 + 120 / sc)) <= 1, f"ny bredde etter slipp ({dock._width})")
    dock.set_collapsed(True)
    root.update()
    _ok(not p.winfo_ismapped() and dock.strip.winfo_ismapped(), "minimert til stripe")
    dock.set_collapsed(False)
    root.update()
    _ok(p.winfo_ismapped(), "utvidet igjen")
    dock.set_width(500)
    root.update()
    sc = ctk.ScalingTracker.get_widget_scaling(p)
    _ok(abs(p.winfo_width() - 500 * sc) < 30, f"bredden kan endres ({p.winfo_width()})")
    dock.refresh(info2, editable=False, note="Pakket")
    root.update()
    _ok(not p.is_editable(), "skrivebeskyttet etter sammenpakking")
    root.destroy()


def test_run_chain(tmp: Path) -> None:
    """Kjøretrådens hjelpere (App._header_*) mot ekte Pakk ut / Pakk sammen."""
    print("test_run_chain")
    import queue
    import threading
    import customtkinter as ctk
    from gui.app import App
    from gui.header_panel import HeaderDock
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.core.premis_logger import PremisProvenanceLogger
    from siard_workflow.operations.pipeline_operations import (
        RepackSiardOperation, UnpackSiardOperation)

    siard = tmp / "uttrekk.siard"
    with zipfile.ZipFile(siard, "w") as zf:
        zf.writestr("header/", b"")
        zf.writestr("header/metadata.xml", META)
        zf.writestr("content/schema0/table0/table0.xml", "<table/>")

    root = ctk.CTk()
    fake = type("F", (), {})()
    fake._log_queue = queue.Queue()
    fake.header_dock = HeaderDock(root, persist=False)
    fake._header_meta = App._header_meta                    # staticmethod
    for name in ("_header_publish", "_header_apply_pending"):
        setattr(fake, name, getattr(App, name).__get__(fake))

    def shown():
        items = []
        while not fake._log_queue.empty():
            it = fake._log_queue.get()
            if it[0] == "header_show":
                items.append(it)
        return items

    ctx = WorkflowContext(siard_path=siard, metadata={
        "stop_event": threading.Event(), "pause_event": threading.Event(),
        "temp_dir": str(tmp)})
    hdr = {"can_edit": True, "repacked": False}
    fake._header_publish(ctx, hdr, first=True)
    it = shown()[-1]
    _ok(it[1] is True and it[2]["values"]["dbname"] == "KRS" and it[4] is True
        and "etter utpakkingen" in it[5], "før utpakking: header fra .siard, redigerbar")
    panel = fake.header_dock.panel
    fake.header_dock.load(it[2], it[3], it[4], it[5])
    panel.set_value("dataOwner", "Arkivverket test")
    panel.confirm()
    _ok(fake._header_apply_pending(ctx, None, None) is False
        and panel._confirmed == {"dataOwner": "Arkivverket test"},
        "ikke utpakket ennå → den bekreftede endringen venter")

    r = UnpackSiardOperation().run(ctx)
    ctx.set_result("unpack_siard", r.data)
    _ok(r.success and ctx.extracted_path, "pakket ut")
    meta_p = ctx.extracted_path / "header/metadata.xml"
    premis = PremisProvenanceLogger(tmp, siard)
    panel.set_value("dbname", "Ubekreftet")
    _ok(fake._header_apply_pending(ctx, None, premis), "bekreftet endring skrives før steget")
    meta = meta_p.read_text("utf-8")
    _ok("<dataOwner>Arkivverket test</dataOwner>" in meta and "Ubekreftet" not in meta,
        "bare den bekreftede endringen er skrevet")
    _ok(premis.has_events(), "PREMIS-hendelse for manuell header-endring")

    # Før «Pakk sammen»: ubekreftede endringer → spørsmål (vindu + ntfy via _ask_yes_no)
    asked: list = []
    answer = {"v": False}
    fake._ask_yes_no = lambda title, text, yes, no: (asked.append((title, text, yes, no)),
                                                    answer["v"])[1]
    fake._ask_header_unconfirmed = App._ask_header_unconfirmed.__get__(fake)
    fake._header_apply_pending(ctx, None, None, before_repack=True)
    _ok(len(asked) == 1 and "Lagre endringer i SIARD-header ved pakking av ny siard?"
        in asked[0][1] and "«Ubekreftet»" in asked[0][1],
        "spørsmålet stilles med endringene listet")
    _ok("Ubekreftet" not in meta_p.read_text("utf-8") and panel.unconfirmed() == {},
        "«Nei» → ubekreftet endring forkastes")
    panel.set_value("archiver", "Kari")
    answer["v"] = True
    _ok(fake._header_apply_pending(ctx, None, None, before_repack=True)
        and "<archiver>Kari</archiver>" in meta_p.read_text("utf-8"),
        "«Ja» → ubekreftet endring skrives før sammenpakking")
    asked.clear()
    fake._header_apply_pending(ctx, None, None, before_repack=True)
    _ok(not asked, "ingen ubekreftede endringer → ingen spørsmål")

    r = RepackSiardOperation(keep_temp=False).run(ctx)
    _ok(r.success, f"pakket sammen ({r.message})")
    ctx.chain_siard(True, r.data)
    hdr["repacked"] = True
    fake._header_publish(ctx, hdr)
    it = shown()[-1]
    _ok(it[2]["values"]["dataOwner"] == "Arkivverket test" and it[4] is False
        and "Pakket inn" in it[5], "ny SIARD har headeren fra panelet; skrivebeskyttet")
    with zipfile.ZipFile(ctx.siard_path) as z:
        _ok("<archiver>Kari</archiver>" in z.read("header/metadata.xml").decode(),
            "header/metadata.xml i ny .siard")
    root.destroy()


def main() -> int:
    test_read()
    test_write()
    test_validate()
    with tempfile.TemporaryDirectory(prefix="hdr_") as td:
        test_sources(Path(td))
    test_panel()
    with tempfile.TemporaryDirectory(prefix="hdr_run_") as td:
        test_run_chain(Path(td))
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
