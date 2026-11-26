"""
Faseradene i fremdriftspanelet under BLOB-konvertering.

Bakgrunn (nov. 2026): i en arbeidsflyt viste rad 4 «Konverterer filer
1,000/1,000» fra start til slutt. Tallet kom fra «Pakk ut SIARD», som melder
fremdrift i promille av bytes uten å starte en fase — panelet skrev det til
raden forrige BLOB-kjøring sluttet på. Rad 3 dekket dessuten både deteksjon og
konvertering, og rad 5–6 ble aldri brukt.

Verifiserer:
  1. fremdrift fra et steg uten faser tegnes aldri i faseradene, heller ikke
     etter en tidligere BLOB-kjøring i samme økt
  2. hvert steg starter med tomme rader og standardtekster
  3. faseplan: egne tekster og bare så mange rader som flyten har
  4. konverteringen (omdøping + LibreOffice) vises i rad 4, deteksjonen i rad 3
  5. BLOB-steget sender fase 3 og 4 med kallerens antall faser

Kjør:  python -X utf8 tests/test_progress_phases.py
"""
from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _row(panel, i: int) -> dict:
    return {"state": panel._phase_state[i],
            "pct": panel._phase_canvases[i]._pct,
            "text": panel._phase_pcts[i].cget("text"),
            "label": panel._phase_lbls[i].cget("text"),
            "visible": bool(panel._phase_rows[i][2].winfo_manager())}


def test_panel() -> None:
    import customtkinter as ctk
    from gui.progress_panel import ProgressPanel, _PHASE_LABELS

    print("\n[1] Steg uten faser tegner ikke i faseradene")
    root = ctk.CTk()
    root.withdraw()
    try:
        panel = ProgressPanel(root)
        panel.grid(row=0, column=0)

        # Forrige BLOB-kjøring i samme økt sluttet i fase 4
        panel.show_simple("BLOB Konverter til PDF/A")
        for n in (1, 2, 3, 4):
            panel.set_phase(n, 4, f"fase {n}")
        panel.phase_done()
        panel.reset()                                  # kø ferdig

        # Ny kjøring: «Pakk ut SIARD» melder promille av bytes, uten fase
        panel.show_simple("Pakk ut SIARD")
        panel.set_phase_progress(1000, 1000, text="Pakker ut: 40/40 filer")
        for i in range(len(_PHASE_LABELS)):
            r = _row(panel, i)
            _ok(r["state"] == "venter" and r["pct"] == 0.0 and r["text"] == "",
                f"rad {i + 1} er urørt etter fremdrift uten fase")

        # Samme uten reset() imellom (stegene følger rett etter hverandre)
        panel.show_simple("Standardiser filendelser")
        panel.set_phase(4, 4, "Pakker ny SIARD")
        panel.set_phase_progress(7, 9)
        _ok(_row(panel, 3)["text"] == "7/9", "en startet fase får fremdrift")
        panel.phase_done()
        panel.set_phase_progress(9, 9)
        _ok(_row(panel, 3)["text"] == "✓",
            "fremdrift etter at fasen er ferdig tegner ikke raden på nytt")

        print("\n[2] Hvert steg starter med tomme rader")
        panel.show_simple("Pakk ut SIARD")
        panel.set_phase_progress(500, 1000, text="Pakker ut: 20/40 filer")
        _ok(all(_row(panel, i)["state"] == "venter"
                and _row(panel, i)["text"] == ""
                for i in range(len(_PHASE_LABELS))),
            "radene fra forrige steg er nullstilt")
        _ok([_row(panel, i)["label"] for i in range(len(_PHASE_LABELS))]
            == list(_PHASE_LABELS), "standardtekstene er tilbake")
        _ok(panel._current_phase == 0, "ingen fase er aktiv")

        print("\n[3] Faseplan for arbeidsflyt (5 faser)")
        plan = ["Skanner utpakket filsystem", "Inline-ekstraksjon",
                "Detekterer filer", "Konverterer filer", "Oppdaterer tableX.xml"]
        panel.show_simple("BLOB Konverter til PDF/A")
        panel.set_phase_plan(plan)
        root.update_idletasks()
        _ok([_row(panel, i)["label"] for i in range(5)] == plan,
            "radene har flytens egne tekster før fasene starter")
        _ok(all(_row(panel, i)["visible"] for i in range(5))
            and not _row(panel, 5)["visible"], "rad 6 er skjult, rad 1–5 vises")

        print("\n[4] Deteksjon i rad 3, konvertering i rad 4")
        panel.set_phase(1, 5, plan[0])
        panel.set_phase(2, 5, "Inline-ekstraksjon hoppet over")
        panel.set_phase(3, 5, "Detekterer 10 234 filer")
        panel.init_run(10234)
        _ok(_row(panel, 0)["state"] == "ferdig" and _row(panel, 2)["state"] == "aktiv",
            "init_run nullstiller ikke fasene som alt er i gang")
        panel.set_phase_progress(1428, 10234, text="Identifiserer filer: 1 428/10 234")
        r3, r4 = _row(panel, 2), _row(panel, 3)
        _ok(r3["text"] == "14%" and abs(r3["pct"] - 1428 / 10234) < 1e-6,
            "deteksjonen vises i rad 3")
        _ok(r4["state"] == "venter" and r4["pct"] == 0.0 and r4["text"] == ""
            and r4["label"] == "Konverterer filer",
            "rad 4 venter og er tom mens deteksjonen pågår")

        panel.set_phase(4, 5, "Konverterer filer")
        _ok(_row(panel, 2)["state"] == "ferdig", "deteksjonen blir ferdig (grønn)")
        stats = {"converted": 0, "kept": 2000, "failed": 0}
        panel.update_stats_and_bar(stats, 2000)        # omdøping (Steg C)
        _ok(_row(panel, 3)["text"] == "20%" and _row(panel, 2)["text"] == "✓",
            "omdøping teller i rad 4, ikke i rad 3")
        stats = {"converted": 3117, "kept": 2000, "failed": 0}
        panel.file_done(0, "record1.doc", "pdf", True, "", stats)   # LibreOffice
        _ok(_row(panel, 3)["text"] == "50%",
            "LibreOffice-konverteringen fortsetter i samme rad")

        panel.set_phase(5, 5, plan[4])
        _ok(_row(panel, 3)["state"] == "ferdig" and _row(panel, 4)["state"] == "aktiv",
            "rad 5 er «Oppdaterer tableX.xml»")

        print("\n[5] 6-fase-flyten (BLOB alene) viser alle radene igjen")
        panel.show_simple("BLOB Konverter til PDF/A")
        panel.set_phase(1, 6, "Skanner SIARD-arkiv")
        root.update_idletasks()
        _ok(all(_row(panel, i)["visible"] for i in range(6)), "alle seks rader vises")
        _ok(_row(panel, 4)["label"] == "Oppdaterer XML", "standardtekst i rad 5")
    finally:
        root.destroy()


def test_blob_phase_events(tmp: Path) -> None:
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation

    print("\n[6] BLOB-steget sender fase 3 og 4 med kallerens antall faser")
    lob = tmp / "content" / "schema0" / "table0" / "lob3"
    lob.mkdir(parents=True)
    rels = []
    for i in range(3):
        p = lob / f"record{i}.txt"
        p.write_text(f"Notat {i}: bare vanlig tekst.\n", encoding="utf-8")
        rels.append(p.relative_to(tmp).as_posix())

    for total in (5, 6):
        events: list = []
        op = BlobConvertOperation()
        op.params["max_workers"] = 1
        stats = {"detected": 0, "converted": 0, "kept": 0, "failed": 0,
                 "xml_updated": 0, "inline_extracted": 0, "missing_blob_refs": 0}
        op._convert_all(
            rels, tmp, stats, lambda m, lvl="info": None,
            lambda ev, **kw: events.append((ev, kw)),
            "soffice", threading.Event(), threading.Event(),
            csv_log=None, xml_type_hints=None, err_log=None,
            conversion_registry={}, creg_lock=threading.Lock(),
            total_phases=total)
        phases = [(kw["phase"], kw["total_phases"], kw["label"])
                  for ev, kw in events if ev == "phase"]
        _ok([p[:2] for p in phases] == [(3, total), (4, total)],
            f"fase 3 og 4 av {total}: {[p[2] for p in phases]}")
        first_prog = next((i for i, (ev, _) in enumerate(events)
                           if ev == "phase_progress"), None)
        i3 = next(i for i, (ev, kw) in enumerate(events)
                  if ev == "phase" and kw["phase"] == 3)
        i4 = next(i for i, (ev, kw) in enumerate(events)
                  if ev == "phase" and kw["phase"] == 4)
        _ok(first_prog is not None and i3 < first_prog < i4,
            "deteksjonens fremdrift kommer mellom fase 3 og fase 4")


def main() -> int:
    test_panel()
    with tempfile.TemporaryDirectory(prefix="faser_") as td:
        test_blob_phase_events(Path(td))
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
