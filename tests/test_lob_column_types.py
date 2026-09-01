"""
Tester for lob_column_types — konsistens mellom LOB-kolonnens deklarerte type
og LOB-filenes endelse.

Bakgrunn (verifisert mot DBPTK-kildekoden og en ekte feilende innlasting):
DBPTK velger BLOB- eller CLOB-gren ved import ut fra FILENDELSEN
(SIARD20ContentImportStrategy: "bin" → BLOB, "txt" → CLOB), men i viseren ut
fra KOLONNETYPEN i metadata.xml (ToolkitStructure2ViewerStructure.getCell).
En CLOB-kolonne med .bin-filer gir derfor:

    java.lang.NullPointerException: entry
        at java.util.zip.ZipFile.getInputStream(...)
        at ToolkitStructure2ViewerStructure.getCLOBValue(...)

Dette oppstår når BLOB-konverteringen gjør RTF/WPT i en MEMO-kolonne om til
PDF/A og filendelsen blir .bin, mens <type>CLOB</type> blir stående.

Kjør:  python -X utf8 tests/test_lob_column_types.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.lob_column_types import (
    BINARY_LOB_TYPE,
    is_character_lob, is_lob_type,
    scan_table_lob_extensions, set_column_type,
    find_inconsistent_lob_columns, reconcile_lob_column_types,
)


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_META = '''<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
\t<dbname>WIS</dbname>
\t<schemas>
\t\t<schema>
\t\t\t<name>s1</name>
\t\t\t<folder>schema0</folder>
\t\t\t<tables>
\t\t\t\t<table>
\t\t\t\t\t<name>Fag</name>
\t\t\t\t\t<folder>table14</folder>
\t\t\t\t\t<columns>
\t\t\t\t\t\t<column><name>Fagkode</name><type>VARCHAR(15)</type><typeOriginal>CHARACTER(15)</typeOriginal></column>
\t\t\t\t\t\t<column><name>KortformSamiskNord</name><type>CLOB</type><typeOriginal>MEMO</typeOriginal><lobFolder>schema0/table14/lob2</lobFolder></column>
\t\t\t\t\t\t<column><name>Merknad</name><type>CLOB</type><typeOriginal>MEMO</typeOriginal><lobFolder>schema0/table14/lob3</lobFolder></column>
\t\t\t\t\t\t<column><name>Innledning</name><type>CLOB</type><typeOriginal>MEMO</typeOriginal><lobFolder>schema0/table14/lob4</lobFolder></column>
\t\t\t\t\t</columns>
\t\t\t\t\t<rows>3</rows>
\t\t\t\t</table>
\t\t\t</tables>
\t\t</schema>
\t</schemas>
\t<users><user><name>x</name></user></users>
</siardArchive>
'''

# c2 = fullt konvertert (.bin), c3 = uberørt tekst (.txt), c4 = BLANDET
_TABLE = '''<?xml version="1.0" encoding="UTF-8"?>
<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd" version="2.1">
\t<row>
\t\t<c1>ELR001</c1>
\t\t<c2 file="xrec1.bin" length="100"/> <!-- Filinnhold konvertert : .rtf til .pdf - Filendelse endret fra .txt til .bin -->
\t\t<c3 file="xrec1.txt" length="20"/>
\t\t<c4 file="xrec1.bin" length="90"/>
\t</row>
\t<row>
\t\t<c1>ELR002</c1>
\t\t<c2 file="xrec2.bin" length="120"/>
\t\t<c3 file="xrec2.txt" length="25"/>
\t\t<c4 file="xrec2.txt" length="30"/>
\t</row>
</table>
'''


def _build(tmp: Path) -> Path:
    root = tmp / "extracted"
    (root / "header").mkdir(parents=True)
    (root / "header" / "metadata.xml").write_text(_META, encoding="utf-8")
    tdir = root / "content" / "schema0" / "table14"
    tdir.mkdir(parents=True)
    (tdir / "table14.xml").write_text(_TABLE, encoding="utf-8")
    for lob, files in (("lob2", ["xrec1.bin", "xrec2.bin"]),
                       ("lob3", ["xrec1.txt", "xrec2.txt"]),
                       ("lob4", ["xrec1.bin", "xrec2.txt"])):
        d = tdir / lob
        d.mkdir()
        for f in files:
            (d / f).write_bytes(b"%PDF-1.4 x" if f.endswith(".bin") else b"tekst")
    return root


def test_type_predicates() -> None:
    print("test_type_predicates")
    for t in ("CLOB", "clob", "CHARACTER LARGE OBJECT", "NCLOB",
              "CLOB(1048576)", "NATIONAL CHARACTER LARGE OBJECT"):
        _ok(is_character_lob(t), f"{t!r} er tegn-LOB")
    for t in ("BLOB", "BINARY LARGE OBJECT", "VARCHAR(50)", "INT", ""):
        _ok(not is_character_lob(t), f"{t!r} er ikke tegn-LOB")
    _ok(is_lob_type("BLOB") and is_lob_type("CLOB"), "begge LOB-former gjenkjent")
    _ok(not is_lob_type("VARCHAR(50)"), "VARCHAR er ikke LOB")


def test_scan_extensions() -> None:
    print("test_scan_extensions")
    with tempfile.TemporaryDirectory() as td:
        root = _build(Path(td))
        per_col = scan_table_lob_extensions(
            root / "content" / "schema0" / "table14" / "table14.xml")
        _ok(per_col[2] == {"bin"}, "c2: kun .bin")
        _ok(per_col[3] == {"txt"}, "c3: kun .txt")
        _ok(per_col[4] == {"bin", "txt"}, "c4: blandet")
        _ok(1 not in per_col, "c1 uten file= er ikke med")


def test_set_column_type() -> None:
    print("test_set_column_type")
    meta = _META.encode("utf-8")
    out, ok = set_column_type(meta, "schema0", "table14", 2, BINARY_LOB_TYPE)
    _ok(ok, "kolonne 2 funnet og endret")
    txt = out.decode("utf-8")
    _ok("<name>KortformSamiskNord</name><type>BINARY LARGE OBJECT</type>" in txt,
        "riktig kolonne fikk ny type")
    _ok("<typeOriginal>MEMO</typeOriginal>" in txt, "typeOriginal bevart")
    _ok(txt.count("<type>CLOB</type>") == 2, "de to andre CLOB-ene urørt")
    _ok("<type>VARCHAR(15)</type>" in txt, "ikke-LOB-kolonne urørt")

    # Ukjent schema/tabell → ingen endring
    _, ok2 = set_column_type(meta, "schemaX", "table14", 2, BINARY_LOB_TYPE)
    _ok(not ok2, "ukjent schema gir ingen endring")


def test_find_inconsistent() -> None:
    print("test_find_inconsistent")
    with tempfile.TemporaryDirectory() as td:
        root = _build(Path(td))
        found = find_inconsistent_lob_columns(root)
        idx = sorted(c["index"] for c in found)
        _ok(idx == [2, 4], f"c2 (kun .bin) og c4 (blandet) flagget, fikk {idx}")
        _ok(all(c["type"] == "CLOB" for c in found), "typen rapportert")
        c4 = [c for c in found if c["index"] == 4][0]
        _ok(c4["extensions"] == ["bin", "txt"], "blandet kolonne rapportert")


def test_reconcile() -> None:
    print("test_reconcile (ende-til-ende)")
    with tempfile.TemporaryDirectory() as td:
        root = _build(Path(td))
        tdir = root / "content" / "schema0" / "table14"
        stats = reconcile_lob_column_types(root)

        _ok(stats["columns_retyped"] == 2, "to kolonner omtypet")
        _ok(stats["refs_rewritten"] == 1, "én .txt-referanse i blandet kolonne")
        _ok(stats["files_renamed"] == 1, "tilhørende fil omdøpt")

        meta = (root / "header" / "metadata.xml").read_text(encoding="utf-8")
        _ok(meta.count(f"<type>{BINARY_LOB_TYPE}</type>") == 2,
            "c2 og c4 er nå BINARY LARGE OBJECT")
        _ok("<name>Merknad</name><type>CLOB</type>" in meta,
            "c3 (ren tekst) er fortsatt CLOB")
        _ok(meta.count("<typeOriginal>MEMO</typeOriginal>") == 3,
            "alle typeOriginal bevart")

        table = (tdir / "table14.xml").read_text(encoding="utf-8")
        _ok('<c4 file="xrec2.bin"' in table, "blandet kolonne: ref → .bin")
        _ok('<c3 file="xrec2.txt"' in table, "ren tekstkolonne urørt")
        _ok((tdir / "lob4" / "xrec2.bin").exists(), "fil omdøpt på disk")
        _ok(not (tdir / "lob4" / "xrec2.txt").exists(), "gammelt navn borte")
        _ok((tdir / "lob4" / "xrec2.bin").read_bytes() == b"tekst",
            "innhold uendret av omdøpingen")
        _ok((tdir / "lob3" / "xrec2.txt").exists(), "urørt kolonnes filer beholdt")

        # XML-kommentaren fra konverteringen skal overleve
        _ok("Filendelse endret fra .txt til .bin" in table, "kommentar bevart")

        # Idempotens: ingen gjenværende inkonsistens
        _ok(find_inconsistent_lob_columns(root) == [], "ingen inkonsistens igjen")
        again = reconcile_lob_column_types(root)
        _ok(again["columns_retyped"] == 0, "andre kjøring er no-op")


def test_clean_archive_untouched() -> None:
    print("test_clean_archive_untouched")
    with tempfile.TemporaryDirectory() as td:
        root = _build(Path(td))
        tdir = root / "content" / "schema0" / "table14"
        # Gjør arkivet til rent tekstarkiv — BÅDE referanser OG filer må være
        # .txt, ellers slår reparasjonen av hengende referanser inn.
        tx = tdir / "table14.xml"
        tx.write_text(tx.read_text(encoding="utf-8").replace(".bin", ".txt"),
                      encoding="utf-8")
        for lob in ("lob2", "lob4"):
            for f in (tdir / lob).iterdir():
                if f.suffix == ".bin":
                    f.rename(f.with_suffix(".txt"))
        before = (root / "header" / "metadata.xml").read_bytes()
        stats = reconcile_lob_column_types(root)
        _ok(stats["columns_retyped"] == 0, "rent tekstarkiv: ingen omtyping")
        _ok(stats["refs_repaired"] == 0, "ingen hengende referanser")
        _ok((root / "header" / "metadata.xml").read_bytes() == before,
            "metadata.xml ikke skrevet")


def test_dangling_ref_repair() -> None:
    """
    BLOB-konverteringen kan patche file= fra .txt til .bin uten at
    konverteringen faktisk ble utført. Referansen henger da i løse luften og
    gir NØYAKTIG samme NullPointerException i DBPTK som typemismatchen.
    """
    print("test_dangling_ref_repair")
    with tempfile.TemporaryDirectory() as td:
        root = _build(Path(td))
        tdir = root / "content" / "schema0" / "table14"
        # c3 er en ren tekstkolonne: patch referansen til .bin uten å røre fila
        tx = tdir / "table14.xml"
        tx.write_text(
            tx.read_text(encoding="utf-8")
              .replace('<c3 file="xrec1.txt"', '<c3 file="xrec1.bin"'),
            encoding="utf-8")
        _ok(not (tdir / "lob3" / "xrec1.bin").exists(), "referansen henger")

        stats = reconcile_lob_column_types(root)
        _ok(stats["refs_repaired"] == 1, "én hengende referanse reparert")

        table = (tdir / "table14.xml").read_text(encoding="utf-8")
        _ok('<c3 file="xrec1.txt"' in table,
            "referansen pekt tilbake på filen som faktisk finnes")
        meta = (root / "header" / "metadata.xml").read_text(encoding="utf-8")
        _ok("<name>Merknad</name><type>CLOB</type>" in meta,
            "kolonnen forblir CLOB — den var aldri konvertert")

        # Ingen refs peker lenger på noe som ikke finnes
        import re as _re
        for m in _re.finditer(r'<c(\d+) file="([^"]+)"', table):
            idx, ref = int(m.group(1)), m.group(2)
            lob = {2: "lob2", 3: "lob3", 4: "lob4"}[idx]
            _ok((tdir / lob / ref).exists(), f"c{idx} → {ref} finnes")


def test_ambiguous_dangling_ref_left_alone() -> None:
    print("test_ambiguous_dangling_ref_left_alone")
    with tempfile.TemporaryDirectory() as td:
        root = _build(Path(td))
        tdir = root / "content" / "schema0" / "table14"
        tx = tdir / "table14.xml"
        tx.write_text(
            tx.read_text(encoding="utf-8")
              .replace('<c3 file="xrec1.txt"', '<c3 file="xrec1.bin"'),
            encoding="utf-8")
        # To kandidater med samme stamme → tvetydig, skal IKKE gjettes
        (tdir / "lob3" / "xrec1.rtf").write_bytes(b"rtf")
        stats = reconcile_lob_column_types(root)
        _ok(stats["refs_repaired"] == 0, "tvetydig referanse ikke rettet")
        _ok(stats["unresolved_refs"] == 1, "rapportert som uløst")


def main() -> None:
    test_type_predicates()
    test_scan_extensions()
    test_set_column_type()
    test_find_inconsistent()
    test_reconcile()
    test_clean_archive_untouched()
    test_dangling_ref_repair()
    test_ambiguous_dangling_ref_left_alone()
    print("\nAlle LOB-kolonnetype-tester bestått ✓")


if __name__ == "__main__":
    main()
