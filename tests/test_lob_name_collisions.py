"""
Nye LOB-filer må få LEDIGE filnavn i lob-mappa.

Bakgrunn (HK-Data PPT, okt. 2026): inline-dokumenter i table32.c2 ble skrevet
ut som record1.rtf, record2.rtf … i lob2/, der originalen allerede hadde
record11.txt osv. for ANDRE rader. Begge ble konvertert til record11.bin —
det ene dokumentet overskrev det andre; 5 623 rader pekte på feil dokument.

Verifiserer:
  1. LobNameAllocator: ledig stamme brukes, ellers neste ledige nummer over
     det høyeste i mappa (samme prefiks), også rec7 ↔ record7
  2. BLOB-konverteringens inline-utskriving: nye filer kolliderer ikke med
     filer fra originalen; eksisterende filer og referanser er urørt
  3. HEX Inline Extract (pipeline): samme regel
  4. konverteringen flytter aldri en ferdig fil over en annen LOB (_target_taken)
  5. etterkontrollen av referanser finner LOB-ene i lob<N>/ når kolonnen ikke
     har lobFolder (ga 39 254 falske advarsler i HK-Data-kjøringen)

Kjør:  python -X utf8 tests/test_lob_name_collisions.py
"""
from __future__ import annotations

import re
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


def test_allocator() -> None:
    print("test_allocator")
    from siard_workflow.core.lob_naming import LobNameAllocator
    a = LobNameAllocator(lambda d: ["record11.txt", "record40.txt", "rec7.bin"]
                         if d == "lob2" else [])
    got = [a.take("lob2", s) for s in ("record1", "record11", "record7", "record2", "record11")]
    _ok(got == ["record1", "record41", "record42", "record2", "record43"] and a.collisions == 3,
        f"ledig navn beholdes, kolliderende får neste over høyeste (rec7 ↔ record7): {got}")
    b = LobNameAllocator(lambda d: ["LOB0003.bin"])
    _ok((b.take("x", "LOB0003"), b.take("x", "LOB0004")) == ("LOB0004", "LOB0005"),
        "nullutfylt prefiks beholdes (LOB0003 → LOB0004)")


def _cfg(**kw):
    import settings
    orig = settings.get_config
    settings.get_config = lambda k, d=None: kw.get(k, orig(k, d))
    return lambda: setattr(settings, "get_config", orig)


def test_blob_inline(tmp: Path) -> None:
    print("test_blob_inline")
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    restore = _cfg(dbptk_lob_names=True)
    try:
        root = tmp / "blob"
        lob = root / "content/schema0/table0/lob2"
        lob.mkdir(parents=True)
        (lob / "record1.txt").write_bytes(b"ORIGINAL rad 1")
        (lob / "record2.txt").write_bytes(b"ORIGINAL rad 2")
        rows = (
            '<row><c1>1</c1><c2 file="content/schema0/table0/lob2/record1.txt" length="14"/></row>\n'
            '<row><c1>2</c1><c2 file="content/schema0/table0/lob2/record2.txt" length="14"/></row>\n'
            '<row><c1>3</c1><c2>Inline dokument for rad tre, ganske langt innhold her.</c2></row>\n'
            '<row><c1>4</c1><c2>Inline dokument for rad fire, også med litt innhold.</c2></row>\n')
        xml = ('<?xml version="1.0" encoding="utf-8"?>\n<table xmlns="http://www.bar.admin.ch/'
               'xmlns/siard/2/table.xsd">\n' + rows + "</table>\n").encode("utf-8")
        op = BlobConvertOperation()
        logs: list = []
        stats: dict = {"inline_extracted": 0}
        patched, new_files, n = op._extract_inline(
            xml, "content/schema0/table0/table0.xml", "schema0/table0", stats,
            lambda m, lvl="info": logs.append(m),
            lob_cols={"schema0/table0": {2: "content/schema0/table0/lob2"}},
            xml_path=None, extract_dir=root)
        stems = [Path(k).name.split(".")[0] for k in new_files]
        _ok(n == 2 and set(stems).isdisjoint({"record1", "record2"}),
            f"inline-filene kolliderer ikke med originalens record1/record2: {sorted(new_files)}")
        _ok((lob / "record1.txt").read_bytes() == b"ORIGINAL rad 1"
            and b'file="content/schema0/table0/lob2/record1.txt"' in patched,
            "originalens filer og referanser er urørt")
        refs = re.findall(rb'<c2 [^>]*file="([^"]+)"', patched)
        _ok(len(refs) == 4 and len({r.rsplit(b"/", 1)[-1].split(b".")[0] for r in refs}) == 4,
            f"fire rader → fire ulike filstammer: {[r.decode() for r in refs]}")
        _ok(stats.get("inline_name_collisions") == 2 and any("finnes fra før" in m for m in logs),
            "kollisjonene telles og logges")
    finally:
        restore()


def test_hex_extract(tmp: Path) -> None:
    print("test_hex_extract")
    from siard_workflow.operations.hex_extract_operation import _process_table_fs
    restore = _cfg(dbptk_lob_names=True)
    try:
        root = tmp / "hex"
        t = root / "content/schema0/table0"
        (t / "lob2").mkdir(parents=True)
        (t / "lob2/record2.txt").write_bytes(b"ORIGINAL rad 5")
        long_hex = ("tekst " * 60).encode().hex()
        (t / "table0.xml").write_text(
            '<?xml version="1.0" encoding="utf-8"?>\n<table>\n'
            f'<row><c1>1</c1><c2>{long_hex}</c2></row>\n'
            f'<row><c1>2</c1><c2>{long_hex}</c2></row>\n'
            '<row><c1>3</c1><c2 file="record2.txt" length="14"/></row>\n</table>\n', "utf-8")
        info = {"schema_folder": "schema0", "folder": "table0", "name": "t",
                "clob_columns": [2], "blob_columns": [], "column_types": {2: "CLOB"},
                "column_names": {2: "notat"}}
        _process_table_fs(root, info, lambda m, lvl="info": None, {}, min_text_length=200)
        files = sorted(p.name for p in (t / "lob2").iterdir())
        xml = (t / "table0.xml").read_text("utf-8")
        _ok((t / "lob2/record2.txt").read_bytes() == b"ORIGINAL rad 5"
            and files == ["record1.txt", "record2.txt", "record3.txt"],
            f"rad 2 sin hex-tekst fikk record3.txt, ikke originalens record2.txt: {files}")
        _ok('file="record1.txt"' in xml and 'file="record3.txt"' in xml
            and xml.count('file="record2.txt"') == 1, "referansene peker på hver sin fil")
    finally:
        restore()


def test_target_taken(tmp: Path) -> None:
    print("test_target_taken")
    from siard_workflow.operations.blob_convert_operation import _target_taken
    a, b = tmp / "record11.txt", tmp / "record11.bin"
    a.write_bytes(b"a")
    _ok(not _target_taken(b, a), "målet finnes ikke → ledig")
    b.write_bytes(b"annen LOB")
    _ok(_target_taken(b, a), "målet er en annen LOB → konvertering stoppes (ingen overskriving)")
    _ok(not _target_taken(a, a), "målet er kildefila selv (record11.bin → record11.bin) → tillatt")


def test_check_without_lobfolder(tmp: Path) -> None:
    """Etterkontrollen ga 39 254 falske «finnes ikke» når kolonnen manglet lobFolder."""
    print("test_check_without_lobfolder")
    from siard_workflow.core.lob_column_types import repair_dangling_lob_refs
    root = tmp / "kontroll"
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(
        '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/'
        'metadata.xsd"><lobFolder>content</lobFolder><schemas><schema><name>s</name>'
        '<folder>schema0</folder><tables><table><name>t</name><folder>table32</folder>'
        '<columns><column><name>id</name><type>INTEGER</type></column>'
        '<column><name>DOC</name><type>BINARY LARGE OBJECT</type></column></columns>'
        '</table></tables></schema></schemas></siardArchive>', "utf-8")
    t = root / "content/schema0/table32"
    (t / "lob2").mkdir(parents=True)
    for i in (1, 2):
        (t / f"lob2/record{i}.bin").write_bytes(b"x")
    (t / "table32.xml").write_text(
        '<table><row><c1>1</c1><c2 file="record1.bin"/></row>'
        '<row><c1>2</c1><c2 file="record2.bin"/></row>'
        '<row><c1>3</c1><c2 file="record3.bin"/></row></table>', "utf-8")
    warns: list = []
    r = repair_dangling_lob_refs(root, lambda m, lvl="info": warns.append(m))
    _ok(r["unresolved"] == 1 and len(warns) == 1 and "record3.bin" in warns[0],
        "uten kolonne-lobFolder slås det opp i lob2/ — bare den reelt manglende record3 meldes")
    (t / "table32.xml").write_text(
        '<table><row><c1>1</c1><c2 file="record1.bin"/></row>'
        '<row><c1>2</c1><c2 file="content/schema0/table32/lob2/record1.bin"/></row>'
        '<row><c1>3</c1><c2 file="record2.bin"/></row></table>', "utf-8")
    warns.clear()
    r = repair_dangling_lob_refs(root, lambda m, lvl="info": warns.append(m))
    _ok(r["shared_files"] == 1 and any("brukes av flere rader" in m and "record1.bin × 2" in m
                                       for m in warns),
        "to rader (filnavn og full sti) som peker på samme fil → advarsel om navnekollisjon")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lobnavn_") as td:
        tmp = Path(td)
        test_allocator()
        test_blob_inline(tmp)
        test_hex_extract(tmp)
        test_target_taken(tmp)
        test_check_without_lobfolder(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
