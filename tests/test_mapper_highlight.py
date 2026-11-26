"""
SIARDMapper («berik metadata»): kolonnen som beskrives, utheves i
datavisningen og rulles inn i synsfeltet — nov. 2026.

Bakgrunn: når man klikket på en kolonne i metadatatabellen, sto dataene fra
kolonnen i datavisningen under uten å skille seg ut, og i tabeller med mange
kolonner kunne kolonnen ligge utenfor det som vistes. Kolonnene ble dessuten
klemt ned til 50 px når tabellen hadde mange kolonner.

Verifiserer:
  1. valgt kolonne får uthevet bakgrunn i alle synlige rader, med samme
     tekst som cellene; valgt rad har egen farge; tabell valgt → ingen utheving
  2. uthevingen følger rulling, kolonnebredde og skriftstørrelse
  3. kolonnen rulles inn i synsfeltet (med nabokolonnen), men ikke ved blaing
  4. kolonnebredden følger innholdet, og kolonnen som beskrives kan leses
  5. «Vis data» av → ingen utheving

Kjør:  python -X utf8 tests/test_mapper_highlight.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(Path(__file__).resolve().parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from test_mapper_data_view import _ok, _pump, _wait, _write_table  # noqa: E402

_NAMES = ["SAK_ID", "TITTEL", "STATUS", "MERKNAD"] + [f"EKSTRA_{k:02d}" for k in range(36)]


def _rows(n: int) -> list:
    return [[str(i + 1), f"Eksempelsak nummer {i + 1}", ("Åpen", "Avsluttet")[i % 2],
             f"En lengre merknad til sak {i + 1} som trenger plass"]
            + [f"verdi {k}-{i}" for k in range(36)] for i in range(n)]


def _canvas(dlg) -> "tuple | None":
    """(x, y, bredde, høyde) for uthevingen, eller None når den er skjult."""
    cv = dlg._hl_canvas
    if not cv.winfo_ismapped():
        return None
    return cv.winfo_x(), cv.winfo_y(), cv.winfo_width(), cv.winfo_height()


def _texts(dlg) -> list:
    cv = dlg._hl_canvas
    return [cv.itemcget(i, "text") for i in cv.find_all() if cv.type(i) == "text"]


def _fills(dlg) -> list:
    cv = dlg._hl_canvas
    return [cv.itemcget(i, "fill") for i in cv.find_all() if cv.type(i) == "rectangle"]


def _col_visible(dlg, cid: str) -> bool:
    tree = dlg._data_tree
    ws = [int(tree.column(c, "width")) for c in dlg._data_cols]
    i = dlg._data_cols.index(cid)
    f0, f1 = tree.xview()
    return sum(ws[:i]) >= f0 * sum(ws) - 1 and sum(ws[:i + 1]) <= f1 * sum(ws) + 1


def main() -> int:
    threading.Timer(240, lambda: os._exit(3)).start()
    import customtkinter as ctk
    import settings
    from gui.styles import COLORS, FontRegistry
    from gui.siardmapper_dialog import SiardMapperDialog, _ColItem, _TblItem

    tmp = Path(tempfile.mkdtemp(prefix="mapperhl_"))
    _write_table(tmp / "content" / "schema0" / "table0" / "table0.xml", _rows(120))
    _write_table(tmp / "content" / "schema0" / "table1" / "table1.xml",
                 [[str(i), f"Klient {i}"] for i in range(4)])

    orig_save, orig_get = settings.save_config, settings.get_config
    settings.save_config = lambda d: None                     # ikke rør config.json
    defaults = {"siardmapper_show_data": True, "siardmapper_rows": 40,
                "siardmapper_unique": False}
    settings.get_config = lambda k, d=None: defaults.get(k, orig_get(k, d))
    start_offset = FontRegistry.current_offset()
    FontRegistry.scale(-start_offset)

    root = ctk.CTk()
    root.geometry("300x200")
    sak = _TblItem("SAK", "sak", "", "table0", True,
                   [_ColItem(n, n.lower(), "", i) for i, n in enumerate(_NAMES)],
                   schema_folder="schema0", n_rows=120)
    klient = _TblItem("KLIENT", "klient", "", "table1", True, [
        _ColItem("KLI_ID", "kli_id", "", 0), _ColItem("NAVN", "navn", "", 1)],
        schema_folder="schema0", n_rows=4)
    dlg = SiardMapperDialog(root, [sak, klient], None, tmp)
    _pump(root, 15)
    dlg.state("normal")
    dlg.geometry("900x640+30+30")       # smalt vindu → datavisningen må rulles
    _pump(root, 15)
    tree = dlg._data_tree

    def select(iid: str) -> None:
        dlg._on_row_selected(iid)
        _wait(root, dlg)
        _pump(root, 8)

    try:
        print("\n[1] Valgt kolonne utheves")
        select("T:sak")
        _ok(_canvas(dlg) is None, "tabell valgt → ingen utheving")
        select("C:sak:tittel")
        box = _canvas(dlg)
        kids = tree.get_children()
        bb = tree.bbox(kids[0], "c1")
        _ok(box is not None and box[:3] == (bb[0], bb[1], bb[2]),
            f"uthevingen ligger nøyaktig over kolonnens celler (x={box[0]}, bredde={box[2]})")
        visible = [k for k in kids if tree.bbox(k, "c1")]
        _ok(len(dlg._hl_rows) == len(visible) > 1
            and box[3] == sum(tree.bbox(k, "c1")[3] for k in visible),
            f"alle {len(visible)} synlige rader er uthevet")
        _ok(_texts(dlg) == [tree.set(k, "c1") for k in visible]
            and _texts(dlg)[0] == "Eksempelsak nummer 1",
            "teksten i uthevingen er cellenes egen tekst")
        _ok(set(_fills(dlg)) == {COLORS["hl_bg"]}, "med uthevingsfargen som bakgrunn")
        _ok(tree.heading("c1", "text") == "▶ TITTEL" and tree.heading("c2", "text") == "STATUS",
            "kolonneoverskriften er merket")
        tree.selection_set(kids[1])
        _pump(root, 6)
        _ok(_fills(dlg)[1] == COLORS["hl_sel_bg"] and _fills(dlg)[0] == COLORS["hl_bg"],
            "valgt rad har egen farge i uthevingen")
        tree.selection_set(())
        ev = type("E", (), {"y": dlg._hl_rows[2][1] + 3})()
        dlg._hl_click(ev)
        _ok(tree.selection() == (kids[2],), "klikk i den uthevede kolonnen velger raden")

        print("\n[2] Uthevingen følger rulling, kolonnebredde og skrift")
        tree.yview_scroll(7, "units")
        _pump(root, 6)
        _ok(_texts(dlg)[0] == "Eksempelsak nummer 8", "rulling ned → nye rader er uthevet")
        tree.yview_moveto(0)
        w0 = _canvas(dlg)[2]
        tree.column("c1", width=w0 + 60)                 # brukeren drar kolonnekanten
        tree.event_generate("<ButtonRelease-1>")
        _pump(root, 8)
        _ok(_canvas(dlg)[2] == w0 + 60, f"bredere kolonne → bredere utheving ({w0} → {w0 + 60})")
        h0 = dlg._hl_rows[0][2]
        FontRegistry.scale(+4)
        _pump(root, 10)
        _ok(dlg._hl_rows[0][2] > h0 and _canvas(dlg)[1] == tree.bbox(tree.get_children()[0], "c1")[1],
            f"større skrift → høyere rader i uthevingen ({h0} → {dlg._hl_rows[0][2]})")
        FontRegistry.scale(-4)
        _pump(root, 8)

        print("\n[3] Kolonnen rulles inn i synsfeltet")
        _ok(tree.xview()[1] < 1.0, "(tabellen er bredere enn vinduet)")
        _ok(not _col_visible(dlg, "c30"), "(EKSTRA_26 ligger utenfor til høyre)")
        select("C:sak:ekstra_26")
        _ok(_col_visible(dlg, "c30") and _canvas(dlg) is not None
            and 0 <= _canvas(dlg)[0] < tree.winfo_width(),
            "kolonne langt til høyre → rullet inn og uthevet")
        _ok(_col_visible(dlg, "c31"), "nabokolonnen til høyre er også synlig")
        select("C:sak:status")
        _ok(_col_visible(dlg, "c2") and _col_visible(dlg, "c1"),
            "kolonne til venstre → rullet tilbake, med nabokolonnen til venstre")
        select("C:sak:sak_id")
        _ok(tree.xview()[0] == 0.0, "første kolonne → helt til venstre (radnummeret synlig)")
        x_before = tree.xview()
        select("C:sak:tittel")
        _ok(tree.xview() == x_before, "kolonne som alt er synlig → ingen rulling")
        tree.xview_moveto(0.5)                           # brukeren ruller selv
        _pump(root, 6)
        moved = tree.xview()
        dlg._nav("next")
        _wait(root, dlg)
        _pump(root, 6)
        _ok(tree.xview() == moved, "blaing til neste side flytter ikke visningen sidelengs")

        print("\n[4] Kolonnebredden følger innholdet")
        ch = dlg._f_cell.measure("0")
        ws = {c: int(tree.column(c, "width")) for c in dlg._data_cols}
        _ok(ws["c0"] < ws["c2"] < ws["c3"],
            f"korte kolonner er smale, lange brede (SAK_ID {ws['c0']}, STATUS {ws['c2']}, "
            f"MERKNAD {ws['c3']} px)")
        _ok(ws["c3"] <= ch * 28 + 12 and min(ws.values()) > 50,
            "med øvre grense, og ingen klemt ned til 50 px")
        select("C:sak:merknad")
        need = ch * 45 + 12
        full = ch * (len(_rows(1)[0][3]) + 3) + 12
        _ok(int(tree.column("c3", "width")) >= min(need, full) - ch * 3,
            f"kolonnen som beskrives blir bred nok til å leses "
            f"({ws['c3']} → {int(tree.column('c3', 'width'))} px)")
        select("C:klient:navn")
        _ok(dlg._data_cols == ["n", "c0", "c1"] and _canvas(dlg) is not None
            and _texts(dlg)[0] == "Klient 0",
            "annen tabell → egne bredder, og uthevingen gjelder den nye kolonnen")
        _ok(tree.xview() == (0.0, 1.0), "smal tabell fyller bredden uten rulling")

        print("\n[5] «Vis data» av")
        dlg._show_data_var.set(False)
        dlg._on_show_data_toggle()
        _pump(root, 6)
        _ok(dlg._hl_column() is None and _canvas(dlg) is None, "ingen utheving når data er skjult")
        dlg._show_data_var.set(True)
        dlg._on_show_data_toggle()
        _wait(root, dlg)
        _pump(root, 8)
        _ok(_canvas(dlg) is not None, "og tilbake når data vises igjen")
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
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
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
