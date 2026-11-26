"""
Datavisningen i SIARDMapper-vinduet («berik metadata») — nov. 2026.

Bakgrunn:
  - Ctrl +/− (og A−/A+) endret ikke skriften i selve tabellene, bare i
    elementene rundt
  - antall rader ble lest på nytt for hvert tastetrykk, og maks var 100:
    «199» ga 19 rader, «115» ga 11
  - det gikk ikke an å se annet enn de første radene i en tabell
  - like rader fylte visningen

Verifiserer:
  1. radleseren: sider forfra og bakfra, også når slutt-taggen deles mellom
     to leseblokker
  2. «Unike»: rader med ulik verdi, forfra og bakfra, med grense for letingen
  3. uvanlige filer (CDATA, navneromsprefiks, tom tabell), SIARD-zip som
     kilde, LOB-referanser, flere skjemaer med samme tabellmappe
  4. vinduet: antall tas i bruk ved Enter / når feltet forlates, inntil
     1000 rader, Start / Forrige / Neste / Slutt, «Unike»
  5. skriftstørrelsen gjelder også tabellene

Kjør:  python -X utf8 tests/test_mapper_data_view.py
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
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_NS = "http://www.admin.ch/xmlns/siard/2.1/schema0/table0.xsd"


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _write_table(path: Path, rows, prefix: str = "") -> None:
    """rows: liste av lister med ferdig XML-innhold per celle."""
    p = f"{prefix}:" if prefix else ""
    xmlns = f'xmlns:{prefix}="{_NS}"' if prefix else f'xmlns="{_NS}"'
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(f'<{p}table {xmlns} version="2.1">')
        for cells in rows:
            f.write(f"<{p}row>")
            for i, c in enumerate(cells, 1):
                f.write(c if c.startswith("<") else f"<{p}c{i}>{c}</{p}c{i}>")
            f.write(f"</{p}row>\n")
        f.write(f"</{p}table>\n")


def _klient_rows(n: int):
    # c1 = løpenummer, c2 = poststed (60 like etter hverandre), c3 = alltid «J»
    return [[str(i + 1), f"Poststed {i // 60}", "J"] for i in range(n)]


def test_reader(tmp: Path) -> None:
    import siard_workflow.core.table_rows as tr
    from siard_workflow.core.table_rows import TableRows

    print("\n[1] Radleseren: sider forfra og bakfra")
    p = tmp / "content" / "schema0" / "table0" / "table0.xml"
    rows = _klient_rows(450)
    _write_table(p, rows)

    def reader():
        return TableRows(lambda: open(p, "rb"), 3)

    r = reader()
    pg = r.page_forward(0, 10)
    _ok(pg.rows == rows[:10] and pg.numbers == list(range(10)),
        "første 10 rader")
    _ok(pg.at_start and not pg.at_end and pg.total is None,
        "på starten; antall rader er ikke kjent før slutten er lest")
    pg = r.page_forward(pg.scan_to, 10)
    _ok(pg.rows == rows[10:20] and pg.scan_from == 10 and pg.scan_to == 20,
        "neste 10 fortsetter der forrige side sluttet")
    pg = r.page_backward(None, 10)
    _ok(pg.rows == rows[440:] and pg.total == 450 and pg.at_end,
        "slutt: de 10 siste radene, og antallet er nå kjent (450)")
    pg = r.page_backward(pg.scan_from, 10)
    _ok(pg.rows == rows[430:440], "forrige 10 fra slutten")
    pg = r.page_backward(4, 10)
    _ok(pg.rows == rows[:4] and pg.at_start,
        "forrige nær starten gir radene som er igjen")
    pg = r.page_forward(445, 10)
    _ok(pg.rows == rows[445:] and pg.at_end, "siste side kan være kortere")
    pg = r.page_forward(9999, 10)
    _ok(pg.rows == rows[440:], "forbi slutten → siste side")
    pg = reader().page_forward(440, 10)
    _ok(pg.at_end and pg.total == 450,
        "en side som slutter på siste rad, vet at den er på slutten")
    pg = reader().page_forward(0, 1000)
    _ok(len(pg.rows) == 450 and pg.at_end, "1000 rader fra en tabell med 450")

    # Slutt-taggen deles mellom leseblokkene (bittesmå blokker)
    old = (tr._SKIP_CHUNK, tr._PARSE_CHUNK)
    try:
        for size in (7, 13, 64):
            tr._SKIP_CHUNK, tr._PARSE_CHUNK = size, 5
            r = reader()
            a = r.page_forward(123, 7).rows
            b = r.page_backward(None, 9).rows
            c = r.page_backward(301, 4).rows
            _ok(a == rows[123:130] and b == rows[441:] and c == rows[297:301]
                and r.total == 450,
                f"riktige rader med leseblokker på {size} byte")
    finally:
        tr._SKIP_CHUNK, tr._PARSE_CHUNK = old

    print("\n[2] «Unike»: rader med ulik verdi")
    r = reader()
    by_col2 = lambda cells: cells[1]
    pg = r.page_forward(0, 5, key=by_col2)
    _ok([c[1] for c in pg.rows] == [f"Poststed {i}" for i in range(5)]
        and pg.numbers == [0, 60, 120, 180, 240],
        "5 unike poststeder: rad 1, 61, 121, 181, 241 (ikke 5 like)")
    _ok(pg.scan_from == 0 and pg.scan_to == 241, "letingen stoppet på rad 241")
    pg2 = r.page_forward(pg.scan_to, 5, key=by_col2)
    _ok([c[1] for c in pg2.rows] == ["Poststed 4", "Poststed 5",
                                     "Poststed 6", "Poststed 7"] and pg2.at_end,
        "neste side leter videre til slutten av tabellen")
    pg = r.page_forward(0, 5, key=lambda cells: cells[2])
    _ok(len(pg.rows) == 1 and pg.scan_to == 450 and not pg.capped,
        "bare én verdi i kolonnen → én rad, hele tabellen gjennomsøkt")
    pg = r.page_forward(0, 5, key=lambda cells: cells[2], max_scan=100)
    _ok(len(pg.rows) == 1 and pg.capped and pg.scan_to == 100,
        "letingen stopper ved grensen og sier fra")
    pg = r.page_forward(0, 5, key=tuple)
    _ok(pg.rows == rows[:5], "hele raden: alle rader er ulike (løpenummer)")
    pg = r.page_backward(None, 3, key=by_col2)
    _ok([c[1] for c in pg.rows] == ["Poststed 5", "Poststed 6", "Poststed 7"]
        and pg.numbers == [359, 419, 449] and pg.scan_from == 359,
        "unike bakfra: siste rad for hver av de 3 siste verdiene")
    pg = r.page_backward(pg.scan_from, 3, key=by_col2)
    _ok([c[1] for c in pg.rows] == ["Poststed 3", "Poststed 4", "Poststed 5"],
        "forrige unike fortsetter bakover derfra")

    print("\n[3] Uvanlige filer og andre kilder")
    # CDATA med </row> som tekst: byte-telling er utrygg → vanlig strømtolking
    cd = tmp / "cdata" / "content" / "schema0" / "table0" / "table0.xml"
    cd_rows = [[str(i), f"tekst {i}"] for i in range(40)]
    raw = [list(x) for x in cd_rows]
    raw[3][1] = "<c2><![CDATA[a </row> b]]></c2>"
    cd_rows[3][1] = "a </row> b"
    _write_table(cd, raw)
    r = TableRows(lambda: open(cd, "rb"), 2)
    _ok(r.page_forward(0, 5).rows == cd_rows[:5], "CDATA-rad leses riktig")
    _ok(r.page_forward(10, 5).rows == cd_rows[10:15]
        and r.page_backward(None, 5).rows == cd_rows[35:] and r.total == 40,
        "radnummer og antall stemmer selv om </row> står som tekst")
    _ok(r._fast is False, "(fila leses med strømtolking)")

    pre = tmp / "prefiks" / "table0.xml"
    _write_table(pre, _klient_rows(30), prefix="s")
    r = TableRows(lambda: open(pre, "rb"), 3)
    _ok(r.page_forward(20, 5).rows == _klient_rows(30)[20:25]
        and r.page_backward(None, 2).numbers == [28, 29],
        "navneromsprefiks (<s:row>)")

    empty = tmp / "tom" / "table0.xml"
    _write_table(empty, [])
    r = TableRows(lambda: open(empty, "rb"), 3)
    pg = r.page_forward(0, 5)
    _ok(pg.rows == [] and pg.total == 0 and pg.at_end and pg.at_start,
        "tom tabell")
    _ok(r.page_backward(None, 5).rows == [], "tom tabell: «Slutt»")

    lob = tmp / "lob" / "table0.xml"
    _write_table(lob, [["1", '<c2 file="content/schema0/table0/lob2/record1.pdf" '
                             'length="812"/>'],
                       ["2", "<c2><a1>rød</a1><a2>blå</a2></c2>"]])
    pg = TableRows(lambda: open(lob, "rb"), 2).page_forward(0, 5)
    _ok(pg.rows[0][1] == "→ content/schema0/table0/lob2/record1.pdf",
        "LOB-celle uten tekst viser filreferansen")
    _ok(pg.rows[1][1] == "rød | blå", "ARRAY-celle viser verdiene")

    # Kilde: utpakket mappe og SIARD-zip, to skjemaer med samme tabellmappe
    from siard_workflow.core.table_rows import (
        SiardZip, find_extracted_table, table_opener)
    ext = tmp / "utpakket"
    _write_table(ext / "content" / "schema0" / "table0" / "table0.xml",
                 [["A", "skjema 0", ""]])
    _write_table(ext / "content" / "schema1" / "table0" / "table0.xml",
                 [["B", "skjema 1", ""]])
    (ext / "content" / "schema1" / "table0" / "lob2").mkdir()
    (ext / "content" / "schema1" / "table0" / "lob2" / "record1.xml").write_text("<x/>")
    _ok(find_extracted_table(ext, "schema1", "table0").parent.parent.name == "schema1",
        "riktig skjemamappe når to skjemaer har table0")
    _ok(find_extracted_table(ext, "", "table0").name == "table0.xml",
        "uten skjemamappe: første tabellfil (ikke en LOB-fil)")
    _ok(find_extracted_table(ext, "schema0", "table9") is None,
        "ukjent tabellmappe → ingen fil")

    siard = tmp / "arkiv.siard"
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("header/metadata.xml", "<siardArchive/>")
        for sch in ("schema0", "schema1"):
            zf.write(ext / "content" / sch / "table0" / "table0.xml",
                     f"content/{sch}/table0/table0.xml")
        zf.write(p, "content/schema0/table7/table7.xml")
    sz = SiardZip(siard)
    try:
        r = TableRows(table_opener("table0", "schema1", None, sz), 3)
        _ok(r.page_forward(0, 5).rows == [["B", "skjema 1", ""]],
            "rett fra SIARD-fila når den ikke er pakket ut")
        r = TableRows(table_opener("table7", "schema0", None, sz), 3)
        _ok(r.page_backward(None, 3).rows == rows[447:]
            and r.page_forward(200, 2).rows == rows[200:202],
            "blaing virker også rett fra SIARD-fila")
        try:
            TableRows(table_opener("table99", "schema0", None, sz), 3).page_forward(0, 5)
            found = True
        except FileNotFoundError:
            found = False
        _ok(not found, "manglende tabell gir tydelig feil, ikke tomme rader")
    finally:
        sz.close()


# ── Vinduet ───────────────────────────────────────────────────────────────────

def _pump(root, n: int = 6) -> None:
    for _ in range(n):
        root.update()
        time.sleep(0.02)


def _wait(root, dlg, timeout: float = 20.0) -> None:
    """Vent til datavisningen er ferdig lest."""
    t0 = time.monotonic()
    _pump(root, 2)
    while dlg._load_waiting is not None:
        if time.monotonic() - t0 > timeout:
            raise AssertionError("datavisningen ble ikke ferdig")
        _pump(root, 1)
    _pump(root, 2)


def _shown(dlg):
    tree = dlg._data_tree
    return [tree.item(i, "values") for i in tree.get_children()]


def test_dialog(tmp: Path) -> None:
    import customtkinter as ctk
    import settings
    from tkinter import ttk
    from gui.styles import FontRegistry
    from gui.siardmapper_dialog import SiardMapperDialog, _ColItem, _TblItem

    ext = tmp / "gui"
    n_rows = 450
    rows = _klient_rows(n_rows)
    _write_table(ext / "content" / "schema0" / "table0" / "table0.xml", rows)
    _write_table(ext / "content" / "schema0" / "table1" / "table1.xml",
                 [[str(i), f"Sak {i}"] for i in range(3)])

    saved: list = []
    orig_save, orig_get = settings.save_config, settings.get_config
    settings.save_config = lambda d: saved.append(dict(d))     # ikke rør config.json
    defaults = {"siardmapper_show_data": True, "siardmapper_rows": 5,
                "siardmapper_unique": False}
    settings.get_config = lambda k, d=None: defaults.get(k, orig_get(k, d))
    start_offset = FontRegistry.current_offset()
    FontRegistry.scale(-start_offset)

    root = ctk.CTk()
    root.geometry("300x200")
    klient = _TblItem("KLIENT", "klient", "", "table0", True, [
        _ColItem("KLI_ID", "kli_id", "", 0),
        _ColItem("POSTSTED", "poststed", "", 1),
        _ColItem("AKTIV", "aktiv", "", 2),
    ], schema_folder="schema0", n_rows=n_rows)
    sak = _TblItem("SAK", "sak", "", "table1", True, [
        _ColItem("SAK_ID", "sak_id", "", 0),
        _ColItem("TITTEL", "tittel", "", 1),
    ], schema_folder="schema0", n_rows=3)
    dlg = SiardMapperDialog(root, [klient, sak], None, ext)
    _pump(root, 15)
    nav = dlg._nav_btns
    hdr = lambda: dlg._data_hdr.cget("text")
    state = lambda k: str(nav[k].cget("state"))
    try:
        print("\n[4] Vinduet: antall rader, blaing og «Unike»")
        dlg._on_row_selected("T:klient")
        _wait(root, dlg)
        got = _shown(dlg)
        _ok(len(got) == 5 and got[0] == ("1", "1", "Poststed 0", "J"),
            "5 rader vises, med radnummer i første kolonne")
        _ok("rad 1–5 av 450" in hdr(), f"overskrift: «{hdr().strip()}»")
        _ok(state("start") == "disabled" and state("prev") == "disabled"
            and state("next") == "normal" and state("end") == "normal",
            "på starten: Start/Forrige av, Neste/Slutt på")
        _ok(nav["next"].cget("text") == "Neste 5 ▶"
            and nav["prev"].cget("text") == "◀ Forrige 5",
            "knappene viser antallet")

        # Skriving i feltet leser ikke på nytt før Enter
        gen = dlg._load_gen
        for typed in ("1", "19", "199"):
            dlg._rows_var.set(typed)
            _pump(root, 2)
        _ok(dlg._load_gen == gen and len(_shown(dlg)) == 5,
            "skriving i feltet («1», «19», «199») leser ikke data på nytt")
        _ok(bool(dlg._rows_entry.bind("<Return>"))
            and bool(dlg._rows_entry.bind("<FocusOut>")),
            "feltet reagerer på Enter og på at det forlates")
        dlg._on_rows_enter()
        _wait(root, dlg)
        _ok(len(_shown(dlg)) == 199 and "rad 1–199 av 450" in hdr(),
            "Enter → 199 rader (før: 19)")
        _ok(saved and saved[-1].get("siardmapper_rows") == 199,
            "antallet lagres i innstillingene")

        dlg._rows_var.set("115")
        dlg._rows_entry.event_generate("<FocusOut>")
        _wait(root, dlg)
        _ok(len(_shown(dlg)) == 115, "feltet forlates → 115 rader (før: 11)")

        dlg._rows_var.set("5000")
        dlg._on_rows_enter()
        _wait(root, dlg)
        _ok(dlg._rows_var.get() == "1000" and len(_shown(dlg)) == 450,
            "for høyt tall settes til 1000 (her: alle 450 radene)")
        _ok(state("next") == "disabled" and state("end") == "disabled",
            "hele tabellen vises: Neste/Slutt av")
        dlg._rows_var.set("abc")
        dlg._on_rows_enter()
        _ok(dlg._rows_var.get() == "1000", "ugyldig tekst → forrige verdi beholdes")

        dlg._rows_var.set("100")
        dlg._on_rows_enter()
        _wait(root, dlg)
        dlg._nav("next")
        _wait(root, dlg)
        got = _shown(dlg)
        _ok(got[0][1] == "101" and got[-1][1] == "200"
            and "rad 101–200 av 450" in hdr(), "Neste 100 → rad 101–200")
        _ok(all(state(k) == "normal" for k in nav), "midt i tabellen: alle knapper på")
        dlg._nav("end")
        _wait(root, dlg)
        got = _shown(dlg)
        _ok(got[0][1] == "351" and got[-1][1] == "450" and state("next") == "disabled",
            "Slutt → de 100 siste radene (351–450)")
        dlg._nav("prev")
        _wait(root, dlg)
        _ok(_shown(dlg)[0][1] == "251" and _shown(dlg)[-1][1] == "350",
            "Forrige 100 → rad 251–350")
        dlg._nav("start")
        _wait(root, dlg)
        _ok(_shown(dlg)[0][1] == "1" and state("prev") == "disabled", "Start → rad 1")

        # Antallet endres i feltet og brukeren trykker rett på «Neste»
        dlg._rows_var.set("10")
        dlg._nav("next")
        _wait(root, dlg)
        _ok(len(_shown(dlg)) == 10 and _shown(dlg)[0][1] == "101"
            and nav["next"].cget("text") == "Neste 10 ▶",
            "«Neste» tar i bruk antallet som står i feltet")

        # Bytte kolonne i samme tabell beholder plassen; annen tabell starter forfra
        dlg._on_row_selected("C:klient:poststed")
        _wait(root, dlg)
        _ok(_shown(dlg)[0][1] == "101" and "KLIENT.POSTSTED" in hdr(),
            "valg av kolonne i samme tabell beholder plassen")
        dlg._on_row_selected("T:sak")
        _wait(root, dlg)
        _ok(len(_shown(dlg)) == 3 and "rad 1–3 av 3" in hdr()
            and state("next") == "disabled", "annen tabell vises fra starten")
        dlg._on_row_selected("C:klient:poststed")
        _wait(root, dlg)
        _ok(_shown(dlg)[0][1] == "101", "tilbake i tabellen: samme sted som sist")

        # «Unike»
        dlg._nav("start")
        _wait(root, dlg)
        _ok(len({v[2] for v in _shown(dlg)}) == 1,
            "uten «Unike»: de 10 første radene har samme poststed")
        dlg._unique_var.set(True)
        dlg._on_unique_toggle()
        _wait(root, dlg)
        got = _shown(dlg)
        _ok([v[2] for v in got] == [f"Poststed {i}" for i in range(8)],
            "«Unike» på kolonnen: 8 ulike poststeder (alle som finnes)")
        _ok(got[1][0] == "61" and "8 unike verdier i POSTSTED blant rad 1–450" in hdr(),
            f"radnummer og overskrift: «{hdr().strip()}»")
        _ok(saved[-1].get("siardmapper_unique") is True, "valget lagres")
        dlg._on_row_selected("C:klient:aktiv")
        _wait(root, dlg)
        _ok(len(_shown(dlg)) == 1 and "1 unik verdi i AKTIV" in hdr(),
            "kolonne med én verdi → én rad")
        dlg._on_row_selected("T:klient")
        _wait(root, dlg)
        _ok(len(_shown(dlg)) == 10 and "10 unike rader blant rad 1–10" in hdr(),
            "tabell valgt: hele raden sammenlignes")
        dlg._unique_var.set(False)
        dlg._on_unique_toggle()
        _wait(root, dlg)

        # «Vis data» av → ingen lesing
        dlg._show_data_var.set(False)
        dlg._on_show_data_toggle()
        gen = dlg._load_gen
        dlg._on_row_selected("T:sak")
        _pump(root, 3)
        _ok(dlg._load_gen == gen, "«Vis data» av → ingen lesing av rader")
        dlg._show_data_var.set(True)
        dlg._on_show_data_toggle()
        _wait(root, dlg)
        _ok(len(_shown(dlg)) == 3, "«Vis data» på igjen → radene hentes")

        print("\n[5] Skriftstørrelsen gjelder også tabellene")
        dlg._on_row_selected("T:klient")
        _wait(root, dlg)
        style = ttk.Style()
        tree, data = dlg._tree, dlg._data_tree
        size0 = dlg._f_cell.cget("size")
        h0 = int(style.lookup("SMap.Treeview", "rowheight"))
        d0 = int(style.lookup("Data.Treeview", "rowheight"))
        box0 = tree.bbox("T:klient")[3]
        dbox0 = data.bbox(data.get_children()[0])[3]
        FontRegistry.scale(+3)                       # samme vei som Ctrl + / A+
        _pump(root, 6)
        _ok(dlg._f_cell.cget("size") == size0 + 3
            and str(style.lookup("SMap.Treeview", "font")) == str(dlg._f_cell)
            and str(style.lookup("Data.Treeview", "font")) == str(dlg._f_cell),
            f"skriften i begge tabellene: {size0} → {size0 + 3}")
        _ok(tree.bbox("T:klient")[3] > box0
            and data.bbox(data.get_children()[0])[3] > dbox0,
            f"radene blir høyere (metadatatabellen {box0} → "
            f"{tree.bbox('T:klient')[3]} px)")
        _ok(dlg._f_head.cget("size") == dlg._f_ctrl.cget("size") > 10,
            "kolonneoverskrifter og kontrollraden følger med")
        _ok(str(dlg._editor._entry.cget("font")) == str(dlg._f_cell),
            "redigeringsfeltet bruker samme skrift")
        FontRegistry.scale(-5)
        _pump(root, 6)
        _ok(int(style.lookup("SMap.Treeview", "rowheight")) < h0 + 3
            and dlg._f_cell.cget("size") == max(10, size0 - 2),
            "mindre skrift → lavere rader")
        FontRegistry.scale(+2)
        _pump(root, 4)
        _ok(int(style.lookup("SMap.Treeview", "rowheight")) == h0
            and int(style.lookup("Data.Treeview", "rowheight")) == d0,
            "tilbake til standard → samme radhøyde som før")
    finally:
        settings.save_config, settings.get_config = orig_save, orig_get
        FontRegistry.scale(start_offset - FontRegistry.current_offset())
        try:
            dlg.grab_release()
        except Exception:
            pass
        dlg.destroy()
        _pump(root, 3)
        root.destroy()


def main() -> int:
    threading.Timer(240, lambda: os._exit(3)).start()
    with tempfile.TemporaryDirectory(prefix="mapperdata_") as td:
        test_reader(Path(td))
        test_dialog(Path(td))
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
