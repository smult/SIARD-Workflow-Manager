"""
Oversikt over filer som ikke ble konvertert (BLOB-konvertering):

  * alle feil registreres med kategori: konverteringsfeil, tidsavbrudd, skadet,
    tom, passordbeskyttet, mangler, oppgradering
  * feilloggen (.log) har kategori-kolonne og oppsummering; komplett liste i
    _konvertering_feil.csv med tabell-/kolonnenavn fra metadata.xml
  * opptelling + tabell i kjørerapporten (PDF)
  * kjøreloggen får opptelling per årsak

Del 1 (kategorisering, filer, plassering) kjører uten LibreOffice. Del 2 kjører
en ekte BLOB-konvertering med skadede, tomme og manglende filer (krever
LibreOffice) og lager kjørerapport.

Kjør:  python -X utf8 tests/test_failure_report.py
"""
from __future__ import annotations

import csv
import io
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


_META = ('<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd">'
         '<lobFolder>content</lobFolder><schemas><schema><name>arkiv</name><folder>schema0</folder>'
         '<tables><table><name>dokumenter</name><folder>table0</folder><columns>'
         '<column><name>id</name><type>INTEGER</type></column>'
         '<column><name>fil</name><lobFolder>schema0/table0/lob2</lobFolder><type>BLOB</type></column>'
         '</columns><rows>5</rows></table></tables></schema></schemas></siardArchive>')


def _make_siard(root: Path, files: dict, refs: list) -> Path:
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(_META, "utf-8")
    lob = root / "content/schema0/table0/lob2"
    lob.mkdir(parents=True)
    for name, data in files.items():
        (lob / name).write_bytes(data)
    rows = "".join(f'<row><c1>{i}</c1><c2 file="{r}"/></row>\n' for i, r in enumerate(refs, 1))
    (root / "content/schema0/table0/table0.xml").write_text(f"<table>\n{rows}</table>\n", "utf-8")
    return lob


def test_logger(tmp: Path) -> None:
    print("test_logger")
    from siard_workflow.core.blob_csv_logger import ConversionErrorLogger as L
    _ok(L.classify("tidsavbrudd etter 300 s") == "tidsavbrudd", "tidsavbrudd gjenkjennes")
    _ok(L.classify("Error: source file could not be loaded. LibreOffice kunne ikke åpne filen")
        == "skadet", "«could not be loaded» → skadet")
    _ok(L.classify("OSError: not an OLE2 structured storage file") == "skadet", "ugyldig OLE2 → skadet")
    _ok(L.classify("Ingen PDF produsert") == "konverteringsfeil", "øvrige → konverteringsfeil")

    root = tmp / "u"
    _make_siard(root, {}, [])
    log = L(tmp / "logg", "Test")
    log.__enter__()
    log.attach_siard(root)
    log.write("content/schema0/table0/lob2/record1.bin", "doc", "Ingen PDF produsert")
    log.write("content/schema0/table0/lob2/record1.bin", "doc", "Ingen PDF produsert")   # duplikat
    log.write("content/schema0/table0/lob2/record2.bin", "docx", "x", "passordbeskyttet")
    log.write("content/schema0/table0/table0.xml::record9.bin", "bin", "finnes ikke", "mangler")
    log.__exit__(None, None, None)
    sm = log.summary()
    _ok(sm["total"] == 3 and sm["counts"] == {"konverteringsfeil": 1, "passordbeskyttet": 1,
                                                "mangler": 1}, f"opptelling uten duplikater ({sm['counts']})")
    _ok(sm["entries"][0]["plassering"] == "arkiv.dokumenter.fil",
        f"plassering = schema.tabell.kolonne ({sm['entries'][0]['plassering']})")
    _ok(sm["entries"][2]["plassering"] == "arkiv.dokumenter",
        "manglende referanse: tabell fra tableX.xml-stien")
    text = Path(sm["log_path"]).read_text("utf-8")
    _ok("OPPSUMMERING" in text and "passordbeskyttet" in text, "feilloggen har kategori og oppsummering")
    with open(sm["csv_path"], encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f, delimiter=";"))
    _ok(rows[0] == L.CSV_HEADER and len(rows) == 4 and rows[3][3].startswith("Manglende fil"),
        "komplett CSV-liste med kategori i klartekst")
    empty = L(tmp / "logg2", "Tom")
    empty.__enter__(); empty.__exit__(None, None, None)
    _ok(empty.log_path is None and empty.csv_path is None, "ingen feil → ingen filer")
    mem = L(None)
    mem.__enter__(); mem.write("a/b.doc", "doc", "Ingen PDF produsert"); mem.__exit__(None, None, None)
    _ok(mem.summary()["total"] == 1, "uten loggmappe: registeret holdes i minnet")


def test_blob_and_report(tmp: Path) -> None:
    print("test_blob_and_report")
    import settings
    settings.set_config = lambda k, v: None
    from siard_workflow.core.libreoffice import find_libreoffice
    lo = find_libreoffice(settings.get_config("lo_executable", "") or "soffice")
    if not lo:
        print("  (LibreOffice ikke funnet — del 2 hoppet over)")
        return
    import docx
    b = io.BytesIO(); d = docx.Document(); d.add_paragraph("OK-dokument"); d.save(b)
    ole_garbage = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + bytes(range(256)) * 40
    zip_garbage = b"PK\x03\x04" + b"\x00\xff" * 500
    files = {"record1.bin": b.getvalue(), "record2.bin": ole_garbage, "record3.bin": b"",
             "record4.bin": zip_garbage}
    root = tmp / "kjoring"
    _make_siard(root, files, ["record1.bin", "record2.bin", "record3.bin", "record4.bin",
                              "record5.bin"])          # record5 finnes ikke
    logs: list = []
    logdir = tmp / "kjorelogg"
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    ctx = WorkflowContext(siard_path=tmp / "Test.siard", metadata={
        "file_logger": None, "log_dir": logdir,
        "progress_cb": lambda ev, **k: logs.append(k.get("msg", "")) if ev == "log" else None,
        "stop_event": threading.Event(), "pause_event": threading.Event()})
    ctx.extracted_path = root
    op = BlobConvertOperation()
    op.params["libreoffice_bin"] = lo
    r = op.run(ctx)
    _ok(r.success, f"BLOB-konvertering kjørt ({r.message})")
    fs = r.data.get("failure_summary") or {}
    print("    opptelling:", fs.get("counts"))
    for e in fs.get("entries", []):
        print(f"      {e['kategori']:<17} {e['fil'].split('/')[-1]:<34} {e['arsak'][:70]}")
    c = fs.get("counts", {})
    _ok(c.get("tom") == 1, "tom fil telles")
    _ok(c.get("mangler") == 1, "manglende fil telles")
    _ok(c.get("skadet", 0) + c.get("konverteringsfeil", 0) >= 2,
        "skadet DOC og skadet ZIP telles")
    _ok(any(e["plassering"] == "arkiv.dokumenter.fil" for e in fs["entries"]),
        "tabell/kolonne-navn i lista")
    text = "\n".join(logs)
    _ok("IKKE KONVERTERT / PROBLEMER" in text and "Tom fil (0 byte)" in text,
        "kjøreloggen har opptelling per årsak")
    _ok(fs.get("csv_path") and Path(fs["csv_path"]).exists(), "komplett liste (CSV) skrevet")

    # Kjørerapport
    from siard_workflow.operations.workflow_report_operation import WorkflowReportOperation
    (tmp / "Test.siard").write_bytes(b"x")
    ctx.metadata["step_results"] = [{"id": "blob_convert", "label": "BLOB-konvertering",
                                     "success": True, "elapsed": 3.0, "message": r.message,
                                     "category": "Innhold"}]
    ctx.results["blob_convert"] = r.data
    rr = WorkflowReportOperation().run(ctx)
    _ok(rr.success, f"kjørerapport laget ({rr.message})")
    import fitz
    with fitz.open(rr.data["report_path"]) as doc:
        pdf_text = "\n".join(p.get_text() for p in doc)
    _ok("Filer som ikke ble konvertert" in pdf_text and "Tom fil (0 byte)" in pdf_text
        and "Manglende fil" in pdf_text and "record5.bin" in pdf_text,
        "rapporten har opptelling og filliste")
    _ok("arkiv.dokumenter.fil" in pdf_text, "rapporten viser tabell/kolonne")


def test_ole2_far_directory(tmp: Path) -> None:
    """Word-fil der OLE2-katalogen ligger etter de første 64 kB og teksten
    inneholder «Impress»: ble identifisert som PowerPoint og deretter avvist
    som skadet (ekte tilfelle: Plania/Visma Unique, 2026-10-06)."""
    print("test_ole2_far_directory")
    sys.path.insert(0, str(_ROOT / "tests"))
    from test_msg_to_pdf import _write_ole
    from siard_workflow.core.identifiers.magic_bytes import MagicIdentifier, _detect
    from siard_workflow.core.file_integrity import check_structure, ole2_office_type
    word_clsid = bytes.fromhex("0609020000000000c000000000000046")
    text = "Referat. Kurs i Impress for ansatte. ".encode("cp1252")  # 8-bit tekst, som i Word 97
    # «1Table» skrives først → teksten havner innen de første 4 kB, som i den
    # ekte filen; WordDocument gjør filen > 64 kB så katalogen havner utenfor
    data = _write_ole({"WordDocument": b"\x00" * 80000, "1Table": text + b"t" * 5000},
                      root_clsid=word_clsid)
    f = tmp / "record132851.bin"
    f.write_bytes(data)
    head = data[:65536]
    _ok(_detect(head)[0] == "ppt", "kun 64 kB: katalogen er utenfor vinduet → feiltolket som PPT")
    _ok(MagicIdentifier().identify(data=head, path=f)[0] == "doc",
        "med filsti: hele OLE2-katalogen leses → Word")
    _ok(ole2_office_type(f)[0] == "doc" and check_structure(f, "doc") is None,
        "strukturkontroll godtar Word-filen")
    _ok(check_structure(f, "ppt") is not None, "som PowerPoint ville den blitt avvist (gammel feil)")


def test_type_hint_text(tmp: Path) -> None:
    """Typekolonnen sier «DOC», men filen er en SQL-spørring (lesbar tekst):
    ble behandlet som Word og avvist som skadet (ekte tilfelle 2026-10-09).
    Tekst skal beholdes som tekst; ukjent binærinnhold merket DOC som ikke er
    Word, skal fortsatt avvises — med typekolonnen nevnt i årsaken."""
    print("test_type_hint_text")
    import settings
    settings.set_config = lambda k, v: None
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    root = tmp / "hint"
    sql = ("SELECT cast(ve.type as nvarchar) + '-' + cast(ve.vedtaksnr as nvarchar) "
           "as vedtaksnummer, rP.FNavn, rP.ENavn FROM rVedtak ve, rPerson rP "
           "WHERE ve.personid = rP.personid ORDER BY ve.Aar").encode("utf-8")
    lob = _make_siard(root, {"record5.txt": sql, "record6.bin": bytes(range(1, 255)) * 8}, [])
    (root / "content/schema0/table0/table0.xml").write_text(
        '<table>\n'
        '<row><c1>5</c1><c2 file="record5.txt"/><c3>DOC</c3></row>\n'
        '<row><c1>6</c1><c2 file="record6.bin"/><c3>DOC</c3></row>\n'
        '</table>\n', "utf-8")
    logs: list = []
    ctx = WorkflowContext(siard_path=tmp / "Hint.siard", metadata={
        "file_logger": None,
        "progress_cb": lambda ev, **k: logs.append(k.get("msg", "")) if ev == "log" else None,
        "stop_event": threading.Event(), "pause_event": threading.Event()})
    ctx.extracted_path = root
    op = BlobConvertOperation()
    op.params["extract_inline"] = True     # standard i GUI-et; bygger typekolonne-hint
    r = op.run(ctx)
    fs = r.data.get("failure_summary") or {}
    bad = {e["fil"].split("/")[-1]: e for e in fs.get("entries", [])}
    _ok(r.success and not any(k.startswith("record5") for k in bad),
        f"SQL-tekst merket DOC regnes ikke som skadet ({sorted(bad)})")
    _ok(any(p.name.startswith("record5") and p.read_bytes() == sql for p in lob.iterdir()),
        "SQL-teksten beholdes uendret")
    _ok("Typekolonne overstyrt av innhold" in "\n".join(logs) and "1 merket DOC" in "\n".join(logs),
        "loggen sier at typekolonnen ble overstyrt")
    e6 = next((e for k, e in bad.items() if k.startswith("record6")), None)
    _ok(e6 is not None and e6["kategori"] == "skadet" and "Typekolonnen sier DOC" in e6["arsak"],
        f"ukjent binærfil merket DOC: skadet, årsak nevner typekolonnen ({e6 and e6['arsak'][:70]})")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="feilrapport_") as td:
        test_logger(Path(td))
        test_ole2_far_directory(Path(td))
        test_type_hint_text(Path(td))
        test_blob_and_report(Path(td))
    print("\nAlle tester OK")
    import os
    os._exit(0)


if __name__ == "__main__":
    main()
