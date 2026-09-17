"""
Tester for DBPTK-validator-kompatibilitet av LOB-filer.

Bakgrunn (verifisert mot keeps/dbptk-developer og CGM_HS_MAN_konvertert.siard):
  P_4.2-3      LOB-filer må hete record[0-9]+.* (recN/xrecN feiler)
  A_M_5.6-1-2  validatoren limer "content/" + lobFolder + file uten '/',
               så bare full content/-sti i file= passerer
  T_6.0-2      hex_extract skrev dekodet TEKST inline i BLOB-celler
               (xs:hexBinary) → XSD-brudd. Løsning: BLOB-kolonner der ALT
               inline hex-innhold er tekst omtypes til CLOB (metadata.xml
               <type> + tableX.xsd blobType→clobType) og dekodes; binære
               og blandede kolonner beholdes som BLOB.

Verifiserer:
  1. lob_naming-hjelperne mapper recN/xrecN/LOBnnnn → recordN, ellers None.
  2. hex_extract: binær BLOB-kolonne — under terskel beholdes som hex, over
     terskel → .bin; ren tekst-BLOB-kolonne omtypes til CLOB og dekodes
     (inline / .txt, cp1252 → UTF-8); blandet kolonne beholdes som BLOB;
     CLOB som før; nye filer heter recordN (xrecN når innstillingen er av).
     Både pipeline-modus (filsystem) og standalone zip-modus.
  3. Standardiser filendelser: omdøper til recordN, bevarer endelse, hopper
     over kollisjoner, patcher file= uten kommentar for rene navnebytter.
  4. lobfolder_fix full_file_paths: pipeline- og standalone-modus skriver
     full content/-sti, lar eksterne/ugjenfinnbare referanser stå.

Kjør:  python -X utf8 tests/test_dbptk_lob_names.py
"""
from __future__ import annotations

import re
import sys
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import settings
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.lob_naming import (
    dbptk_stem, dbptk_file_name, is_dbptk_lob_name, lob_file_stem,
)
from siard_workflow.operations.hex_extract_operation import (
    HexExtractOperation, _patch_table_xsd_bytes, _decode_as_text,
)
from siard_workflow.operations.standardize_ext_operation import (
    StandardizeExtOperation, count_non_standard_lob_files,
)
from siard_workflow.operations.lobfolder_fix_operation import (
    LobFolderFixOperation, _resolve_full_path,
)


# ── Hjelpere ──────────────────────────────────────────────────────────────────

class _CfgPatch:
    """Midlertidig overstyring av config-nøkler i settings.get_config."""
    def __init__(self, **overrides):
        self.overrides = overrides
        self._orig = None

    def __enter__(self):
        self._orig = settings.get_config
        ov = self.overrides

        def _patched(key, default=None):
            if key in ov:
                return ov[key]
            return self._orig(key, default)
        settings.get_config = _patched
        return self

    def __exit__(self, *a):
        settings.get_config = self._orig


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_NS_META = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
_NS_TAB  = "http://www.bar.admin.ch/xmlns/siard/2/table.xsd"


def _metadata(columns: list[tuple[str, str, str | None]],
              db_lobfolder: str | None = "content") -> bytes:
    """metadata.xml for schema0/table0 med gitte (navn, type, lobFolder)."""
    cols = "".join(
        f"<column><name>{n}</name>"
        + (f"<lobFolder>{lf}</lobFolder>" if lf else "")
        + f"<type>{t}</type><nullable>true</nullable></column>"
        for n, t, lf in columns)
    dblf = f"<lobFolder>{db_lobfolder}</lobFolder>" if db_lobfolder else ""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<siardArchive xmlns="{_NS_META}" version="2.1">'
        f'<dbname>t</dbname><dataOriginTimespan>2024</dataOriginTimespan>'
        f'{dblf}'
        f'<schemas><schema><name>s</name><folder>schema0</folder><tables>'
        f'<table><name>tab</name><folder>table0</folder><columns>{cols}</columns>'
        f'<rows>2</rows></table></tables></schema></schemas></siardArchive>\n'
    ).encode("utf-8")


def _table_xml(rows: list[str]) -> bytes:
    body = "\n".join(f"<row>{r}</row>" for r in rows)
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<table xmlns="{_NS_TAB}" version="2.1">\n{body}\n</table>\n'
            ).encode("utf-8")


def _write(root: Path, rel: str, data: bytes) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def _null_log():
    return {"file_logger": None, "progress_cb": None}


# ── 1. Navnehjelpere ──────────────────────────────────────────────────────────

def test_naming_helpers() -> None:
    print("test_naming_helpers")
    _ok(dbptk_stem("rec5") == "record5",      "rec5 → record5")
    _ok(dbptk_stem("xrec5") == "record5",     "xrec5 → record5")
    _ok(dbptk_stem("LOB0005") == "record5",   "LOB0005 → record5")
    _ok(dbptk_stem("record0005") == "record5", "record0005 → record5 (nullpadding)")
    _ok(dbptk_stem("record5") is None,        "record5 uendret")
    _ok(dbptk_stem("dokument_5") is None,     "ukjent stamme røres ikke")
    _ok(dbptk_file_name("rec7.doc.pdf") == "record7.doc.pdf",
        "alle endelser bevares")
    _ok(is_dbptk_lob_name("record12.bin") and not is_dbptk_lob_name("rec12.bin"),
        "is_dbptk_lob_name matcher validatorens regex")
    with _CfgPatch(dbptk_lob_names=True):
        _ok(lob_file_stem(3, "xrec") == "record3", "innstilling på → record3")
    with _CfgPatch(dbptk_lob_names=False):
        _ok(lob_file_stem(3, "xrec") == "xrec3", "innstilling av → xrec3 (historisk)")


# ── 2. hex_extract: BLOB-guard, omtyping og navn ─────────────────────────────

_NS_XS = "http://www.w3.org/2001/XMLSchema"


def _table_xsd(col_types: list[str], with_clob_def: bool = True) -> bytes:
    """tableX.xsd i SIARD Suite/DBPTK-stil for kolonnene c1..cN."""
    els = "".join(
        f'      <xs:element minOccurs="0" name="c{i}" type="{t}"/>\n'
        for i, t in enumerate(col_types, start=1))

    def _lobdef(name: str, base: str) -> str:
        return (f'  <xs:complexType name="{name}">\n'
                f'    <xs:simpleContent>\n'
                f'      <xs:extension base="xs:{base}">\n'
                f'        <xs:attribute name="file" type="xs:anyURI" use="optional"/>\n'
                f'        <xs:attribute name="length" type="xs:integer" use="optional"/>\n'
                f'        <xs:attribute name="digestType" type="xs:string" use="optional"/>\n'
                f'        <xs:attribute name="digest" type="xs:string" use="optional"/>\n'
                f'      </xs:extension>\n'
                f'    </xs:simpleContent>\n'
                f'  </xs:complexType>\n')
    defs = (_lobdef("clobType", "string") if with_clob_def else "") + _lobdef("blobType", "hexBinary")
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<xs:schema xmlns="{_NS_TAB}" xmlns:xs="{_NS_XS}" targetNamespace="{_NS_TAB}"\n'
        f'  elementFormDefault="qualified" attributeFormDefault="unqualified">\n'
        f'  <xs:element name="table">\n    <xs:complexType>\n      <xs:sequence>\n'
        f'        <xs:element maxOccurs="unbounded" minOccurs="0" name="row" type="recordType"/>\n'
        f'      </xs:sequence>\n    </xs:complexType>\n  </xs:element>\n'
        f'  <xs:complexType name="recordType">\n    <xs:sequence>\n{els}'
        f'    </xs:sequence>\n  </xs:complexType>\n{defs}</xs:schema>\n'
    ).encode("utf-8")


# Testdata for hex_extract
_SHORT_TXT  = "hei".encode("utf-8").hex()                       # CLOB, < 200
_LONG_TXT   = ("tekst " * 60).encode("utf-8").hex()             # CLOB, > 200
_SHORT_BIN  = bytes([0x2A, 0x00, 0x01, 0x42, 0x5A, 0x68]).hex() # binær, < 200
_LONG_BIN   = bytes(range(256)) + b"\x00" * 60                  # binær, > 200
_MEMO_SHORT = "Blåbær og rømme".encode("cp1252").hex()          # cp1252-tekst, < 200
_MEMO_LONG  = ("Journalnotat: pasienten møtte til avtalt tid. " * 6).encode("utf-8").hex()


def _build_hex_case(root: Path, with_clob_def: bool = True) -> None:
    """
    c2 note  CLOB  — kort/lang UTF-8-tekst (som før)
    c3 doc   BLOB  — kort/lang binært      → beholdes BLOB
    c4 memo  BLOB  — kort cp1252 + lang UTF-8 tekst → omtypes CLOB
    c5 mixed BLOB  — tekst OG binært       → beholdes BLOB (blandet)
    """
    _write(root, "header/metadata.xml",
           _metadata([("id",    "INTEGER", None),
                      ("note",  "CLOB",    "schema0/table0/lob2"),
                      ("doc",   "BLOB",    "schema0/table0/lob3"),
                      ("memo",  "BINARY LARGE OBJECT", "schema0/table0/lob4"),
                      ("mixed", "BLOB",    "schema0/table0/lob5")]))
    _write(root, "content/schema0/table0/table0.xsd",
           _table_xsd(["xs:integer", "clobType", "blobType", "blobType", "blobType"],
                      with_clob_def=with_clob_def))
    _write(root, "content/schema0/table0/table0.xml", _table_xml([
        f"<c1>1</c1><c2>{_SHORT_TXT}</c2><c3>{_SHORT_BIN}</c3>"
        f"<c4>{_MEMO_SHORT}</c4><c5>{_MEMO_SHORT}</c5>",
        f"<c1>2</c1><c2>{_LONG_TXT}</c2><c3>{_LONG_BIN.hex()}</c3>"
        f"<c4>{_MEMO_LONG}</c4><c5>{_LONG_BIN.hex()}</c5>",
    ]))


def _check_hex_result(root: Path, res, xml: str, meta: str, xsd: str) -> None:
    """Felles sjekker for pipeline- og zip-modus."""
    # CLOB som før
    _ok("<c2>hei</c2>" in xml, "kort CLOB dekodet inline til tekst (som før)")
    _ok('<c2 file="record2.txt"' in xml, "lang CLOB → record2.txt")

    # Binær BLOB-kolonne beholdes
    _ok(f"<c3>{_SHORT_BIN}</c3>" in xml,
        "kort binær BLOB beholdt som hex — ikke tekst (T_6.0-2)")
    _ok('<c3 file="record2.bin"' in xml, "lang binær BLOB → record2.bin (aldri .txt)")

    # Ren tekst-BLOB omtypes og dekodes
    _ok("<c4>Blåbær og rømme</c4>" in xml,
        "kort cp1252-tekst i BLOB-kolonne dekodet inline")
    _ok('<c4 file="record2.txt"' in xml, "lang tekst i BLOB-kolonne → record2.txt")
    _ok(f"<c4>{_MEMO_SHORT}</c4>" not in xml and _MEMO_LONG not in xml,
        "ingen rå hex igjen i den omtypede kolonnen")
    _ok(re.search(r"<name>memo</name>.*?<type>CLOB</type>", meta, re.S) is not None,
        "metadata.xml: memo BINARY LARGE OBJECT → CLOB")
    _ok(re.search(r"<name>doc</name>.*?<type>BLOB</type>", meta, re.S) is not None
        and re.search(r"<name>mixed</name>.*?<type>BLOB</type>", meta, re.S) is not None,
        "metadata.xml: doc og mixed forblir BLOB")
    _ok('name="c4" type="clobType"' in xsd, "tableX.xsd: c4 blobType → clobType")
    _ok('name="c3" type="blobType"' in xsd and 'name="c5" type="blobType"' in xsd,
        "tableX.xsd: c3 og c5 uendret blobType")
    _ok(xsd.count('name="clobType"') == 1 and xsd.count('name="blobType"') == 1,
        "tableX.xsd: typedefinisjonene ikke duplisert")

    # Blandet kolonne beholdes som BLOB
    _ok(f"<c5>{_MEMO_SHORT}</c5>" in xml,
        "blandet BLOB-kolonne: kort tekstfelt beholdt som hex")
    _ok('<c5 file="record2.bin"' in xml, "blandet BLOB-kolonne: langt felt → .bin")

    _ok(res.data.get("hex_blob_kept_inline") == 2,
        f"statistikk: 2 BLOB-felt beholdt som hex (fikk {res.data.get('hex_blob_kept_inline')})")
    _ok(res.data.get("columns_retyped") == 1, "statistikk: 1 kolonne omtypet")
    _ok(res.data.get("retyped_columns") == ["schema0/table0.memo (c4): BINARY LARGE OBJECT → CLOB"],
        f"retyped_columns-detalj for PREMIS: {res.data.get('retyped_columns')}")
    detail = HexExtractOperation().premis_detail(res, None)
    _ok("omtypet til tegn-LOB" in detail and "memo" in detail,
        "premis_detail nevner omtypingen")


def test_hex_extract_blob_guard(tmp: Path) -> None:
    print("test_hex_extract_blob_guard")
    root = tmp / "hex_on"
    _build_hex_case(root)
    ctx = WorkflowContext(siard_path=tmp / "dummy.siard", metadata=_null_log())
    ctx.extracted_path = root
    with _CfgPatch(dbptk_lob_names=True):
        res = HexExtractOperation().run(ctx)
    _ok(res.success, "hex_extract pipeline vellykket")

    xml  = (root / "content/schema0/table0/table0.xml").read_text("utf-8")
    meta = (root / "header/metadata.xml").read_text("utf-8")
    xsd  = (root / "content/schema0/table0/table0.xsd").read_text("utf-8")
    _check_hex_result(root, res, xml, meta, xsd)
    _ok((root / "content/schema0/table0/lob2/record2.txt").is_file()
        and (root / "content/schema0/table0/lob3/record2.bin").is_file()
        and (root / "content/schema0/table0/lob4/record2.txt").is_file()
        and (root / "content/schema0/table0/lob5/record2.bin").is_file(),
        "filene finnes i lobN/")
    _ok((root / "content/schema0/table0/lob3/record2.bin").read_bytes() == _LONG_BIN,
        "BLOB-innhold byte-eksakt")
    _ok((root / "content/schema0/table0/lob4/record2.txt").read_bytes()
        == bytes.fromhex(_MEMO_LONG), "tekst-LOB skrevet som UTF-8")

    # Idempotent: andre kjøring finner ingen hex og endrer ingenting
    res_b = HexExtractOperation().run(ctx)
    _ok(res_b.success and not res_b.data.get("columns_retyped")
        and res_b.data.get("hex_exported") == 0,
        "andre kjøring: ingenting å omtype eller eksportere")

    # Innstilling av → historisk xrecN
    root2 = tmp / "hex_off"
    _build_hex_case(root2)
    ctx2 = WorkflowContext(siard_path=tmp / "dummy.siard", metadata=_null_log())
    ctx2.extracted_path = root2
    with _CfgPatch(dbptk_lob_names=False):
        res2 = HexExtractOperation().run(ctx2)
    xml2 = (root2 / "content/schema0/table0/table0.xml").read_text("utf-8")
    _ok(res2.success and '<c2 file="xrec2.txt"' in xml2,
        "innstilling av → xrec2.txt (historisk navn)")
    _ok('<c3 file="xrec2.bin"' in xml2, "BLOB-guard gjelder uavhengig av navneinnstilling")
    _ok('<c4 file="xrec2.txt"' in xml2, "omtyping gjelder uavhengig av navneinnstilling")


def test_hex_extract_zip_mode(tmp: Path) -> None:
    print("test_hex_extract_zip_mode")
    root = tmp / "hex_zip_src"
    _build_hex_case(root, with_clob_def=False)   # XSD uten clobType-definisjon
    siard = tmp / "hex_in.siard"
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(root.rglob("*")):
            if p.is_file():
                zf.write(p, str(p.relative_to(root)).replace("\\", "/"))
    ctx = WorkflowContext(siard_path=siard, metadata=_null_log())
    with _CfgPatch(dbptk_lob_names=True):
        res = HexExtractOperation().run(ctx)
    _ok(res.success, "hex_extract zip-modus vellykket")
    out = Path(res.data["output_path"])
    _ok(out.name == "hex_in_hex_extracted.siard" and out.is_file(),
        "skrevet <original>_hex_extracted.siard")
    with zipfile.ZipFile(out) as zf:
        _ok(zf.testzip() is None, "zip integritet ok")
        names = set(zf.namelist())
        xml  = zf.read("content/schema0/table0/table0.xml").decode("utf-8")
        meta = zf.read("header/metadata.xml").decode("utf-8")
        xsd  = zf.read("content/schema0/table0/table0.xsd").decode("utf-8")
        lob4 = zf.read("content/schema0/table0/lob4/record2.txt")
    _check_hex_result(root, res, xml, meta, xsd)
    _ok({"content/schema0/table0/lob2/record2.txt",
         "content/schema0/table0/lob3/record2.bin",
         "content/schema0/table0/lob4/record2.txt",
         "content/schema0/table0/lob5/record2.bin"} <= names, "LOB-filer i zip")
    _ok(lob4 == bytes.fromhex(_MEMO_LONG), "tekst-LOB i zip skrevet som UTF-8")
    _ok('<xs:extension base="xs:string">' in xsd,
        "manglende clobType-definisjon avledet fra blobType (hexBinary → string)")
    _ok(xsd.index('name="clobType"') < xsd.index('name="blobType"'),
        "avledet clobType satt inn foran blobType")


def test_hex_helpers() -> None:
    print("test_hex_helpers")
    _ok(_decode_as_text("Blåbær".encode("cp1252")) == "Blåbær", "cp1252-tekst gjenkjennes")
    _ok(_decode_as_text("Blåbær".encode("utf-8")) == "Blåbær", "UTF-8-tekst gjenkjennes")
    _ok(_decode_as_text(b"%PDF-1.4\n\x00\x01") is None, "kontrolltegn → binært")
    _ok(_decode_as_text(b"abc\x81def") is None, "udefinert cp1252-byte → binært")
    _ok(_decode_as_text(b"") is None, "tomt → ikke tekst")
    _ok(_decode_as_text(b"linje1\r\nlinje2\tx") == "linje1\r\nlinje2\tx",
        "CR/LF/TAB er lovlig tekst")

    # XSD-patch: attributtrekkefølge og prefiks varierer
    xsd = (b'<xsd:element type="blobType" name="c7" minOccurs="0"/>\n'
           b'<xsd:element name="c8" type="blobType"/>\n'
           b'<xsd:element name="c17" type="blobType"/>\n'
           b'<xsd:complexType name="clobType"/><xsd:complexType name="blobType"/>')
    out, n = _patch_table_xsd_bytes(xsd, {7, 17})
    _ok(n == 2, f"2 elementer skrevet om (fikk {n})")
    _ok(b'type="clobType" name="c7"' in out and b'name="c17" type="clobType"' in out,
        "c7 og c17 → clobType uavhengig av attributtrekkefølge")
    _ok(b'name="c8" type="blobType"' in out, "c8 urørt")
    _ok(out.count(b'name="clobType"') == 1, "eksisterende clobType-definisjon ikke duplisert")


# ── 3. Standardiser filendelser: navn → recordN ───────────────────────────────

def _build_std_case(root: Path) -> None:
    lob = root / "content/schema0/table0/lob2"
    lob.mkdir(parents=True)
    for name in ("rec1.txt", "xrec2.bin", "LOB0003.doc.pdf", "record4.bin",
                 "rec5.txt", "record5.txt", "weird.bin"):
        (lob / name).write_bytes(b"x")
    _write(root, "header/metadata.xml",
           _metadata([("id", "INTEGER", None), ("f", "BLOB", "schema0/table0/lob2")]))
    _write(root, "content/schema0/table0/table0.xml", _table_xml([
        '<c1>1</c1><c2 file="rec1.txt"/>',
        '<c1>2</c1><c2 file="xrec2.bin"/>',
        '<c1>3</c1><c2 file="LOB0003.doc.pdf"/>',
        '<c1>4</c1><c2 file="record4.bin"/>',
        '<c1>5</c1><c2 file="rec5.txt"/>',
        '<c1>6</c1><c2 file="weird.bin"/>',
    ]))


def test_standardize_names(tmp: Path) -> None:
    print("test_standardize_names")
    root = tmp / "std_on"
    _build_std_case(root)
    n_ext, n_name = count_non_standard_lob_files(root)
    _ok(n_ext == 1 and n_name == 4,
        f"telling: 1 ikke-standard endelse, 4 omdøpbare navn (fikk {n_ext},{n_name})")

    ctx = WorkflowContext(siard_path=tmp / "dummy.siard", metadata=_null_log())
    ctx.extracted_path = root
    with _CfgPatch(dbptk_lob_names=True, standardize_bin_ext=True):
        res = StandardizeExtOperation().run(ctx)
    _ok(res.success, "standardize pipeline vellykket")
    lob = root / "content/schema0/table0/lob2"
    names = sorted(p.name for p in lob.iterdir())
    _ok(names == sorted(["record1.txt", "record2.bin", "record3.bin", "record4.bin",
                         "rec5.txt", "record5.txt", "weird.bin"]),
        f"filnavn etter omdøping: {names}")
    _ok(res.data["renamed_names"] == 3 and res.data["renamed"] == 1
        and res.data["name_collisions"] == 1,
        "statistikk: 3 navnebytter, 1 endelse, 1 kollisjon")

    xml = (root / "content/schema0/table0/table0.xml").read_text("utf-8")
    _ok('file="record1.txt"' in xml and 'file="record2.bin"' in xml
        and 'file="record3.bin"' in xml, "file= patchet for alle omdøpte")
    _ok('file="rec5.txt"' in xml, "kollisjon: referanse urørt")
    _ok(xml.count("<!--") == 1 and "Filendelse endret fra .doc.pdf" in xml,
        "kun endelsesbytte får XML-kommentar")
    _ok(re.search(r'file="record1.txt"[^\n]*<!--', xml) is None,
        "rent navnebytte får ingen kommentar")

    # Navneinnstilling av: kun endelse standardiseres
    root2 = tmp / "std_off"
    _build_std_case(root2)
    ctx2 = WorkflowContext(siard_path=tmp / "dummy.siard", metadata=_null_log())
    ctx2.extracted_path = root2
    with _CfgPatch(dbptk_lob_names=False, standardize_bin_ext=True):
        res2 = StandardizeExtOperation().run(ctx2)
    names2 = sorted(p.name for p in (root2 / "content/schema0/table0/lob2").iterdir())
    _ok(res2.success and "rec1.txt" in names2 and "LOB0003.bin" in names2,
        "innstilling av: rec1.txt beholdt, kun endelse → LOB0003.bin")

    # Begge av → no-op
    with _CfgPatch(dbptk_lob_names=False, standardize_bin_ext=False):
        res3 = StandardizeExtOperation().run(ctx2)
    _ok(res3.success and not res3.data, "begge innstillinger av → ingen endringer")


# ── 4. lobfolder_fix: full_file_paths ─────────────────────────────────────────

def _build_lobfix_case(root: Path) -> None:
    _write(root, "header/metadata.xml",
           _metadata([("id",   "INTEGER", None),
                      ("a",    "CLOB",    "schema0/table0/lob2/"),   # SCFC + trailing '/'
                      ("b",    "BLOB",    "lob3"),                   # SIARD Suite-stil
                      ("c",    "CLOB",    None)],                    # ingen lobFolder
                     db_lobfolder=None))                             # mangler db-nivå
    for rel in ("content/schema0/table0/lob2/record1.txt",
                "content/schema0/table0/lob3/record1.bin",
                "content/schema0/table0/lob4/record1.txt",
                "content/schema0/table0/lob2/record2.txt"):
        _write(root, rel, b"x")
    _write(root, "content/schema0/table0/table0.xml", _table_xml([
        '<c1>1</c1><c2 file="record1.txt"/><c3 file="record1.bin"/><c4 file="record1.txt"/>',
        '<c1>2</c1><c2 file="lob2/record2.txt"/><c3 file="../ext/record2.bin"/>'
        '<c4 file="record9.txt"/>',
        '<c1>3</c1><c2 file="content/schema0/table0/lob2/record1.txt"/>',
    ]))


def test_resolve_helper() -> None:
    print("test_resolve_helper")
    files = {"content/schema0/table0/lob2/record1.txt",
             "content/schema0/table0/lob3/record1.bin"}
    ex = files.__contains__
    _ok(_resolve_full_path("schema0", "table0", 2, "record1.txt",
                           "schema0/table0/lob2", ex)
        == "content/schema0/table0/lob2/record1.txt", "SCFC-lobFolder → full sti")
    _ok(_resolve_full_path("schema0", "table0", 3, "record1.bin", "lob3", ex)
        == "content/schema0/table0/lob3/record1.bin", "Suite-lobFolder → full sti")
    _ok(_resolve_full_path("schema0", "table0", 2, "lob2/record1.txt",
                           "schema0/table0/lob2", ex)
        == "content/schema0/table0/lob2/record1.txt", "relativ mappedel håndteres")
    _ok(_resolve_full_path("schema0", "table0", 2, "content/x/record1.txt",
                           None, ex) is None, "allerede full sti → urørt")
    _ok(_resolve_full_path("schema0", "table0", 2, "../ext/record1.txt",
                           None, ex) is None, "ekstern (..) → urørt")
    _ok(_resolve_full_path("schema0", "table0", 2, "finnes_ikke.txt",
                           "schema0/table0/lob2", ex) is None, "ugjenfinnbar → None")


def test_lobfix_full_paths_pipeline(tmp: Path) -> None:
    print("test_lobfix_full_paths_pipeline")
    root = tmp / "lobfix_pipe"
    _build_lobfix_case(root)
    xml_before = (root / "content/schema0/table0/table0.xml").read_bytes()

    # Default (opsjon av): kun metadata endres
    ctx = WorkflowContext(siard_path=tmp / "dummy.siard", metadata=_null_log())
    ctx.extracted_path = root
    res = LobFolderFixOperation().run(ctx)
    _ok(res.success and res.data["fixes"] == 2 and res.data.get("file_paths", 0) == 0,
        "opsjon av: 2 metadata-fikser, ingen file=-endringer")
    _ok((root / "content/schema0/table0/table0.xml").read_bytes() == xml_before,
        "tableX.xml urørt uten opsjon")
    meta = (root / "header/metadata.xml").read_text("utf-8")
    _ok("<lobFolder>content</lobFolder>" in meta
        and "<lobFolder>schema0/table0/lob2</lobFolder>" in meta,
        "metadata: db-nivå lagt til, trailing '/' fjernet")

    # Opsjon på (metadata nå allerede ok → kun file=)
    op = LobFolderFixOperation()
    op.params["full_file_paths"] = True
    res2 = op.run(ctx)
    _ok(res2.success and res2.data["file_paths"] == 4,
        f"4 file=-referanser skrevet om (fikk {res2.data.get('file_paths')})")
    _ok(res2.data.get("file_unresolved") == 1, "1 ugjenfinnbar referanse (record9.txt)")
    xml = (root / "content/schema0/table0/table0.xml").read_text("utf-8")
    _ok('<c2 file="content/schema0/table0/lob2/record1.txt"/>' in xml, "c2 SCFC-lobFolder")
    _ok('<c3 file="content/schema0/table0/lob3/record1.bin"/>' in xml, "c3 Suite-lobFolder")
    _ok('<c4 file="content/schema0/table0/lob4/record1.txt"/>' in xml, "c4 uten lobFolder → lobN")
    _ok('<c2 file="content/schema0/table0/lob2/record2.txt"/>' in xml, "relativ mappedel løst")
    _ok('<c3 file="../ext/record2.bin"/>' in xml, "ekstern referanse urørt")
    _ok('<c4 file="record9.txt"/>' in xml, "ugjenfinnbar referanse urørt")
    _ok(xml.count('file="content/schema0/table0/lob2/record1.txt"') == 2,
        "allerede full sti bevart")

    # Idempotent
    res3 = op.run(ctx)
    _ok(res3.success and res3.data["file_paths"] == 0 and res3.data["fixes"] == 0,
        "andre kjøring: ingenting å gjøre")


def test_lobfix_full_paths_standalone(tmp: Path) -> None:
    print("test_lobfix_full_paths_standalone")
    root = tmp / "lobfix_src"
    _build_lobfix_case(root)
    siard = tmp / "in.siard"
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(root.rglob("*")):
            if p.is_file():
                zf.write(p, str(p.relative_to(root)).replace("\\", "/"))
    ctx = WorkflowContext(siard_path=siard, metadata=_null_log())
    op = LobFolderFixOperation()
    op.params["full_file_paths"] = True
    res = op.run(ctx)
    _ok(res.success and res.data["file_paths"] == 4 and res.data["fixes"] == 2,
        "standalone: metadata + 4 file=-referanser")
    out = Path(res.data["output_path"])
    _ok(out.name == "in_lobfix.siard" and out.is_file(), "skrevet <original>_lobfix.siard")
    with zipfile.ZipFile(out) as zf:
        names = set(zf.namelist())
        xml = zf.read("content/schema0/table0/table0.xml").decode("utf-8")
        meta = zf.read("header/metadata.xml").decode("utf-8")
        _ok(zf.testzip() is None, "zip integritet ok")
    _ok(names == {str(p.relative_to(root)).replace("\\", "/")
                  for p in root.rglob("*") if p.is_file()}, "alle entries bevart")
    _ok('<c3 file="content/schema0/table0/lob3/record1.bin"/>' in xml
        and '<c3 file="../ext/record2.bin"/>' in xml, "file= patchet i zip")
    _ok("<lobFolder>content</lobFolder>" in meta, "metadata fikset i zip")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    import tempfile
    with tempfile.TemporaryDirectory(prefix="dbptk_names_") as td:
        tmp = Path(td)
        test_naming_helpers()
        test_hex_helpers()
        test_hex_extract_blob_guard(tmp)
        test_hex_extract_zip_mode(tmp)
        test_standardize_names(tmp)
        test_resolve_helper()
        test_lobfix_full_paths_pipeline(tmp)
        test_lobfix_full_paths_standalone(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
