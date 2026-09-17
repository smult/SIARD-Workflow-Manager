"""
Tester for kolonnetype-konsistens metadata.xml ↔ tableX.xsd (DBPTK P_4.3-3).

Bakgrunn (verifisert mot keeps/dbptk-developer master 2026-09-15,
MetadataAndTableDataValidator.populateSQL2008Types):
  FLOAT / FLOAT(p) -> xs:double (uansett p), REAL -> xs:float,
  DOUBLE PRECISION -> xs:double, INTEGER-familien -> xs:integer,
  DECIMAL/NUMERIC -> xs:decimal, VARCHAR -> clobType eller xs:string.

Verifiserer:
  1. Typemappingen og hvilke avvik som regnes som tapsfrie.
  2. compare_column_types på syntetisk metadata + XSD: rettbare, ikke
     rettbare, ukjent SQL-type, manglende element/XSD.
  3. XsdTypeFixOperation i pipeline-modus (XSD patchet, metadata urørt,
     idempotent) og standalone (ny zip, øvrige entries byte-identiske).
  4. XMLValidationOperation rapporterer avvikene og er grønn etter retting.
  5. scan_xsd_type_issues for preflight.
  6. T_6.3-1: datofasetter på dateType/dateTimeType — SCFC-XSD (ekte eksempel,
     table116.xsd) rettes tapsfritt, pattern/innrykk bevares, idempotent;
     manglende fasett/definisjon legges inn; feil base og innsnevring rettes ikke.

Kjør:  python -X utf8 tests/test_xsd_column_types.py
"""
from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.column_xsd_types import (
    expected_xsd_types, lossless_fix, compare_column_types, check_dir,
    patch_xsd_column_types, scan_xsd_type_issues, format_finding,
    check_date_facets, patch_date_facets, rule_code, DATE_FACETS,
)
from siard_workflow.operations.xsd_type_fix_operation import XsdTypeFixOperation
from siard_workflow.operations.standard_operations import XMLValidationOperation


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_NS_META = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
_NS_TAB  = "http://www.bar.admin.ch/xmlns/siard/2/table.xsd"
_NS_XS   = "http://www.w3.org/2001/XMLSchema"


def _metadata(tables: dict[str, list[tuple[str, str]]]) -> bytes:
    """metadata.xml med schema0 og tabeller {folder: [(navn, type)]}."""
    tbls = ""
    for folder, cols in tables.items():
        c = "".join(f"<column><name>{n}</name><type>{t}</type>"
                    f"<nullable>true</nullable></column>" for n, t in cols)
        tbls += (f"<table><name>{folder}</name><folder>{folder}</folder>"
                 f"<columns>{c}</columns><rows>1</rows></table>")
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<siardArchive xmlns="{_NS_META}" version="2.1"><dbname>t</dbname>'
            f'<schemas><schema><name>s</name><folder>schema0</folder>'
            f'<tables>{tbls}</tables></schema></schemas></siardArchive>\n'
            ).encode("utf-8")


def _xsd(col_types: list[str], prefix: str = "xs") -> bytes:
    els = "".join(f'      <{prefix}:element minOccurs="0" name="c{i}" type="{t}"/>\n'
                  for i, t in enumerate(col_types, start=1))
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<{prefix}:schema xmlns="{_NS_TAB}" xmlns:{prefix}="{_NS_XS}" '
            f'targetNamespace="{_NS_TAB}" elementFormDefault="qualified">\n'
            f'  <{prefix}:element name="table"><{prefix}:complexType><{prefix}:sequence>\n'
            f'    <{prefix}:element maxOccurs="unbounded" minOccurs="0" name="row" type="recordType"/>\n'
            f'  </{prefix}:sequence></{prefix}:complexType></{prefix}:element>\n'
            f'  <{prefix}:complexType name="recordType">\n    <{prefix}:sequence>\n{els}'
            f'    </{prefix}:sequence>\n  </{prefix}:complexType>\n'
            f'  <!-- kommentar som skal bevares -->\n</{prefix}:schema>\n'
            ).encode("utf-8")


_TABLE_XML = (f'<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="{_NS_TAB}" '
              f'version="2.1">\n<row><c1>1</c1><c2>1.5</c2></row>\n</table>\n'
              ).encode("utf-8")


def _write(root: Path, rel: str, data: bytes) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def _null_log():
    return {"file_logger": None, "progress_cb": None}


# ── 1. Mapping ────────────────────────────────────────────────────────────────

def test_mapping() -> None:
    print("test_mapping")
    _ok(expected_xsd_types("FLOAT(53)") == ("xs:double",),  "FLOAT(53) → xs:double")
    _ok(expected_xsd_types("FLOAT(24)") == ("xs:double",),  "FLOAT(24) → xs:double (ingen presisjonsgrense)")
    _ok(expected_xsd_types("FLOAT") == ("xs:double",),      "FLOAT → xs:double")
    _ok(expected_xsd_types("float ( 53 )") == ("xs:double",), "case/mellomrom normaliseres")
    _ok(expected_xsd_types("REAL") == ("xs:float",),        "REAL → xs:float")
    _ok(expected_xsd_types("DOUBLE PRECISION") == ("xs:double",), "DOUBLE PRECISION → xs:double")
    _ok(expected_xsd_types("DECIMAL(10,2)") == ("xs:decimal",), "DECIMAL(10,2) → xs:decimal")
    _ok(expected_xsd_types("NUMERIC(18)") == ("xs:decimal",), "NUMERIC(18) → xs:decimal")
    _ok(expected_xsd_types("BIGINT") == ("xs:integer",),    "BIGINT → xs:integer")
    _ok(expected_xsd_types("VARCHAR(50)") == ("clobType", "xs:string"), "VARCHAR(50) → clobType|xs:string")
    _ok(expected_xsd_types("CHARACTER LARGE OBJECT(2G)") == ("clobType",), "CLOB(2G) langform")
    _ok(expected_xsd_types("TIMESTAMP(0)") == ("dateTimeType",), "TIMESTAMP(0) → dateTimeType")
    _ok(expected_xsd_types("INTERVAL DAY TO SECOND") == ("xs:duration",), "INTERVAL → xs:duration")
    _ok(expected_xsd_types("MEMO") is None,                 "ukjent type → None")
    _ok(expected_xsd_types("DOUBLE") is None,               "DOUBLE (uten PRECISION) godtas ikke av DBPTK")

    _ok(lossless_fix("xs:float", ("xs:double",)) == "xs:double",  "xs:float → xs:double rettbar")
    _ok(lossless_fix("xs:int", ("xs:integer",)) == "xs:integer",  "xs:int → xs:integer rettbar")
    _ok(lossless_fix("xs:integer", ("xs:decimal",)) == "xs:decimal", "xs:integer → xs:decimal rettbar")
    _ok(lossless_fix("xs:long", ("xs:decimal",)) == "xs:decimal",  "xs:long → xs:decimal rettbar")
    _ok(lossless_fix("xs:double", ("xs:float",)) is None,          "xs:double → xs:float IKKE rettbar (innsnevring)")
    _ok(lossless_fix("xs:decimal", ("xs:double",)) is None,        "xs:decimal → xs:double IKKE rettbar")
    _ok(lossless_fix("xs:string", ("xs:decimal",)) is None,        "xs:string → xs:decimal IKKE rettbar")
    _ok(lossless_fix("clobType", ("blobType",)) is None,           "clobType → blobType IKKE rettbar")


# ── 2. Sammenligning ──────────────────────────────────────────────────────────

def _build_case(root: Path) -> None:
    """
    table0: c1 INTEGER/xs:integer ok, c2 FLOAT(53)/xs:float (rettbar),
            c3 SMALLINT/xs:short (rettbar), c4 DECIMAL(10,2)/xs:string (manuell),
            c5 REAL/xs:float ok, c6 VARCHAR(20)/xs:string ok, c7 MEMO/xs:string (ukjent)
    table1: c1 NUMERIC(5)/xs:int (rettbar → xs:decimal), c2 DATE (mangler i XSD)
    table2: uten XSD
    """
    _write(root, "header/metadata.xml", _metadata({
        "table0": [("id", "INTEGER"), ("pris", "FLOAT(53)"), ("ant", "SMALLINT"),
                   ("bel", "DECIMAL(10,2)"), ("r", "REAL"), ("navn", "VARCHAR(20)"),
                   ("notat", "MEMO")],
        "table1": [("n", "NUMERIC(5)"), ("d", "DATE")],
        "table2": [("x", "INTEGER")],
    }))
    _write(root, "content/schema0/table0/table0.xsd",
           _xsd(["xs:integer", "xs:float", "xs:short", "xs:string", "xs:float",
                 "xs:string", "xs:string"]))
    _write(root, "content/schema0/table0/table0.xml", _TABLE_XML)
    _write(root, "content/schema0/table1/table1.xsd", _xsd(["xs:int"], prefix="xsd"))
    _write(root, "content/schema0/table1/table1.xml", _TABLE_XML)
    _write(root, "content/schema0/table2/table2.xml", _TABLE_XML)


def test_compare(tmp: Path) -> None:
    print("test_compare")
    root = tmp / "cmp"
    _build_case(root)
    findings = check_dir(root)
    by = {(f["table"], f["col_index"]): f for f in findings}
    _ok(len(findings) == 7, f"7 funn (fikk {len(findings)}): "
        + "; ".join(format_finding(f) for f in findings))
    f = by[("table0", 2)]
    _ok(f["kind"] == "mismatch" and f["fix_to"] == "xs:double",
        "table0.c2 FLOAT(53)/xs:float → rettes til xs:double")
    _ok(by[("table0", 3)]["fix_to"] == "xs:integer", "table0.c3 SMALLINT/xs:short → xs:integer")
    f = by[("table0", 4)]
    _ok(f["kind"] == "mismatch" and f["fix_to"] is None,
        "table0.c4 DECIMAL/xs:string → manuell")
    _ok(by[("table0", 7)]["kind"] == "unknown_sql_type", "table0.c7 MEMO → ukjent SQL-type")
    _ok(("table0", 1) not in by and ("table0", 5) not in by and ("table0", 6) not in by,
        "konsistente kolonner gir ingen funn (INTEGER, REAL, VARCHAR/xs:string)")
    _ok(by[("table1", 1)]["fix_to"] == "xs:decimal", "table1.c1 NUMERIC/xs:int → xs:decimal (xsd:-prefiks)")
    _ok(by[("table1", 2)]["kind"] == "missing_element", "table1.c2 DATE mangler i recordType")
    _ok(by[("table2", 0)]["kind"] == "missing_xsd", "table2 uten XSD rapporteres")
    _ok("FLOAT(53) forventer xs:double, XSD har xs:float → rettes til xs:double"
        in format_finding(by[("table0", 2)]), "format_finding i DBPTK-stil")

    # Ren XSD-patch: attributtrekkefølge, kommentar bevart, uendret type teller ikke
    xsd = (b'<xs:element type="xs:float" name="c2"/>\n<xs:element name="c3" type="xs:short"/>'
           b'\n<!-- k -->\n<xs:element name="c4" type="xs:string"/>')
    out, n = patch_xsd_column_types(xsd, {2: "xs:double", 3: "xs:integer", 4: "xs:string"})
    _ok(n == 2 and b'type="xs:double" name="c2"' in out and b'name="c3" type="xs:integer"' in out
        and b"<!-- k -->" in out, "patch: 2 endret, rekkefølge og kommentar bevart")


# ── 3. Operasjonen ────────────────────────────────────────────────────────────

def test_operation_pipeline(tmp: Path) -> None:
    print("test_operation_pipeline")
    root = tmp / "pipe"
    _build_case(root)
    meta_before = (root / "header/metadata.xml").read_bytes()
    ctx = WorkflowContext(siard_path=tmp / "dummy.siard", metadata=_null_log())
    ctx.extracted_path = root
    op = XsdTypeFixOperation()
    res = op.run(ctx)
    _ok(res.success and res.data["fixes"] == 3,
        f"3 rettinger (fikk {res.data.get('fixes')}): {res.data.get('changes')}")
    _ok(res.data["unfixed"] == 4, f"4 avvik til manuell vurdering (fikk {res.data.get('unfixed')})")
    _ok(op.produces_siard is False, "pipeline: ingen ny SIARD")
    xsd0 = (root / "content/schema0/table0/table0.xsd").read_text("utf-8")
    _ok('name="c2" type="xs:double"' in xsd0 and 'name="c3" type="xs:integer"' in xsd0,
        "table0.xsd: c2 → xs:double, c3 → xs:integer")
    _ok('name="c4" type="xs:string"' in xsd0 and 'name="c5" type="xs:float"' in xsd0,
        "table0.xsd: c4 (manuell) og c5 (REAL) urørt")
    _ok("<!-- kommentar som skal bevares -->" in xsd0, "XSD-kommentar bevart")
    xsd1 = (root / "content/schema0/table1/table1.xsd").read_text("utf-8")
    _ok('name="c1" type="xs:decimal"' in xsd1, "table1.xsd (xsd:-prefiks): c1 → xs:decimal")
    _ok((root / "header/metadata.xml").read_bytes() == meta_before, "metadata.xml urørt")
    _ok("P_4.3-3" in op.premis_detail(res, ctx) and "FLOAT(53)" in op.premis_detail(res, ctx),
        "premis_detail beskriver rettingen")
    _ok(op.premis_should_record(res, ctx), "PREMIS-event registreres når noe ble rettet")

    res2 = XsdTypeFixOperation().run(ctx)
    _ok(res2.success and res2.data["fixes"] == 0 and res2.data["unfixed"] == 4,
        "idempotent: andre kjøring retter ingenting, manuelle avvik står")
    _ok(not XsdTypeFixOperation().premis_should_record(res2, ctx),
        "ingen PREMIS-event uten endringer")


def test_operation_standalone(tmp: Path) -> None:
    print("test_operation_standalone")
    root = tmp / "zsrc"
    _build_case(root)
    siard = tmp / "in.siard"
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(root.rglob("*")):
            if p.is_file():
                zf.write(p, str(p.relative_to(root)).replace("\\", "/"))
    ctx = WorkflowContext(siard_path=siard, metadata=_null_log())
    res = XsdTypeFixOperation().run(ctx)
    _ok(res.success and res.data["fixes"] == 3, "standalone: 3 rettinger")
    out = Path(res.data["output_path"])
    _ok(out.name == "in_xsdfix.siard" and out.is_file(), "skrevet <original>_xsdfix.siard")
    _ok(ctx.siard_path == out, "ctx.siard_path peker på ny fil")
    with zipfile.ZipFile(siard) as zin, zipfile.ZipFile(out) as zout:
        _ok(zout.testzip() is None, "zip integritet ok")
        _ok(zin.namelist() == zout.namelist(), "samme entries i samme rekkefølge")
        for name in zin.namelist():
            if name.endswith(("table0.xsd", "table1.xsd")):
                continue
            assert zin.read(name) == zout.read(name), name
        _ok(True, "alle andre entries byte-identiske")
        xsd0 = zout.read("content/schema0/table0/table0.xsd").decode("utf-8")
        _ok('name="c2" type="xs:double"' in xsd0, "table0.xsd patchet i zip")

    # Ingen rettbare avvik → ingen ny fil
    root2 = tmp / "clean"
    _write(root2, "header/metadata.xml", _metadata({"table0": [("id", "INTEGER")]}))
    _write(root2, "content/schema0/table0/table0.xsd", _xsd(["xs:integer"]))
    siard2 = tmp / "clean.siard"
    with zipfile.ZipFile(siard2, "w") as zf:
        for p in sorted(root2.rglob("*")):
            if p.is_file():
                zf.write(p, str(p.relative_to(root2)).replace("\\", "/"))
    ctx2 = WorkflowContext(siard_path=siard2, metadata=_null_log())
    res2 = XsdTypeFixOperation().run(ctx2)
    _ok(res2.success and res2.data["fixes"] == 0 and "output_path" not in res2.data
        and not (tmp / "clean_xsdfix.siard").exists(),
        "konsistent arkiv: ingen ny fil skrives")


# ── 4. XML-validering + 5. preflight-skann ────────────────────────────────────

def test_xml_validation_and_scan(tmp: Path) -> None:
    print("test_xml_validation_and_scan")
    siard = tmp / "in.siard"           # fra test_operation_standalone
    fixed = tmp / "in_xsdfix.siard"
    found = scan_xsd_type_issues(siard)
    _ok(len(found) == 7 and sum(1 for f in found if f.get("fix_to")) == 3,
        "scan_xsd_type_issues: 7 funn, 3 rettbare")
    _ok(scan_xsd_type_issues(tmp / "finnes_ikke.siard") == [], "lesefeil → tom liste")

    ctx = WorkflowContext(siard_path=siard, metadata=_null_log())
    res = XMLValidationOperation().run(ctx)
    p433 = [e for e in res.data["errors"] if e.startswith("P_4.3-3")]
    _ok(not res.success and len(p433) == 7, f"XML-validering: 7 P_4.3-3-feil (fikk {len(p433)})")
    _ok(sum("rettes av «Rett tableX.xsd»" in e for e in p433) == 3,
        "3 av dem merket som rettbare")

    ctx2 = WorkflowContext(siard_path=fixed, metadata=_null_log())
    res2 = XMLValidationOperation().run(ctx2)
    p433b = [e for e in res2.data["errors"] if e.startswith("P_4.3-3")]
    _ok(len(p433b) == 4 and not any("rettes av" in e for e in p433b),
        "etter retting: kun de 4 manuelle avvikene står igjen")

    op = XMLValidationOperation()
    op.params["check_column_types"] = False
    res3 = op.run(ctx)
    _ok(not any(e.startswith("P_4.3-3") for e in res3.data["errors"]),
        "sjekken kan slås av")


# ── 6. T_6.3-1: datofasetter ──────────────────────────────────────────────────

# Ekte SCFC-generert tableX.xsd (table116.xsd, forkortet) — tabulator-innrykk
_SCFC_XSD = b"""<?xml version="1.0" encoding="utf-8" standalone="no"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd" targetNamespace="http://www.bar.admin.ch/xmlns/siard/2/table.xsd" elementFormDefault="qualified" attributeFormDefault="unqualified" version="2.1">
\t<!-- root element is the table element -->
\t<xs:element name="table">
\t\t<xs:complexType>
\t\t\t<xs:sequence>
\t\t\t\t<xs:element maxOccurs="unbounded" minOccurs="0" name="row" type="recordType" />
\t\t\t</xs:sequence>
\t\t\t<xs:attribute name="version" type="versionType" use="required" />
\t\t</xs:complexType>
\t</xs:element>
\t<xs:simpleType name="versionType">
\t\t<xs:restriction base="xs:string">
\t\t\t<xs:whiteSpace value="collapse" />
\t\t\t<xs:enumeration value="2.1" />
\t\t</xs:restriction>
\t</xs:simpleType>
\t<xs:complexType name="recordType">
\t\t<xs:annotation />
\t\t<xs:sequence>
\t\t\t<xs:element name="c1" type="xs:string" />
\t\t\t<xs:element name="c2" type="dateType" />
\t\t\t<xs:element name="c5" type="dateTimeType" />
\t\t\t<xs:element name="c9" type="dateTimeType" />
\t\t</xs:sequence>
\t</xs:complexType>
\t<!-- date type between 0001 and 9999 restricted to UTC -->
\t<xs:simpleType name="dateType">
\t\t<xs:restriction base="xs:date">
\t\t\t<xs:minInclusive value="0001-01-01Z" />
\t\t\t<xs:maxExclusive value="9999-12-31Z" />
\t\t\t<xs:pattern value="\\d{4}-\\d{2}-\\d{2}Z?" />
\t\t</xs:restriction>
\t</xs:simpleType>
\t<xs:simpleType name="timeType">
\t\t<xs:restriction base="xs:time">
\t\t\t<xs:pattern value="\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z?" />
\t\t</xs:restriction>
\t</xs:simpleType>
\t<!-- dateTime type between 0001 and 9999 restricted to UTC -->
\t<xs:simpleType name="dateTimeType">
\t\t<xs:restriction base="xs:dateTime">
\t\t\t<xs:minInclusive value="0001-01-01T00:00:00.000000000Z" />
\t\t\t<xs:maxExclusive value="9999-12-31T23:59:59.9999999Z" />
\t\t\t<xs:pattern value="\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z?" />
\t\t</xs:restriction>
\t</xs:simpleType>
\t<!--Create time: 16.02.2024 07:20:44-->
</xs:schema>
"""


def _xsd_facets(xsd: bytes, tname: str) -> dict[str, str | None]:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xsd)
    ns = {"xs": _NS_XS}
    out = {}
    for facet in ("minInclusive", "maxExclusive", "pattern"):
        el = root.find(f"xs:simpleType[@name='{tname}']/xs:restriction/xs:{facet}", ns)
        out[facet] = el.get("value") if el is not None else None
    return out


def test_date_facets(tmp: Path) -> None:
    print("test_date_facets")
    findings = check_date_facets(_SCFC_XSD)
    _ok(len(findings) == 2 and all(f["kind"] == "date_facet" and f["facet"] == "maxExclusive"
                                  for f in findings),
        "SCFC-XSD: nøyaktig maxExclusive på dateType og dateTimeType avviker")
    by = {f["type_name"]: f for f in findings}
    _ok(by["dateType"]["found"] == "9999-12-31Z" and by["dateType"]["fix_to"] == "10000-01-01Z",
        "dateType 9999-12-31Z → 10000-01-01Z (tapsfri utvidelse)")
    _ok(by["dateTimeType"]["fix_to"] == "10000-01-01T00:00:00.000000000Z",
        "dateTimeType → 10000-01-01T00:00:00.000000000Z")
    _ok(rule_code(by["dateType"]) == "T_6.3-1", "regelkode T_6.3-1")
    _ok("9999-12-31Z" in format_finding({**by["dateType"], "schema": "schema0", "table": "table116"})
        and "rettes til" in format_finding({**by["dateType"], "schema": "schema0", "table": "table116"}),
        "format_finding for datofasett")

    patched, n = patch_date_facets(_SCFC_XSD, findings)
    _ok(n == 2, f"2 fasetter skrevet (fikk {n})")
    d, dt = _xsd_facets(patched, "dateType"), _xsd_facets(patched, "dateTimeType")
    _ok(d["maxExclusive"] == "10000-01-01Z" and d["minInclusive"] == "0001-01-01Z",
        "dateType: maxExclusive rettet, minInclusive urørt")
    _ok(dt["maxExclusive"] == "10000-01-01T00:00:00.000000000Z"
        and dt["minInclusive"] == "0001-01-01T00:00:00.000000000Z", "dateTimeType rettet")
    _ok(d["pattern"] == r"\d{4}-\d{2}-\d{2}Z?" and dt["pattern"] is not None,
        "xs:pattern bevart")
    _ok(b'<xs:maxExclusive value="10000-01-01Z" />' in patched
        and patched.count(b"\t") == _SCFC_XSD.count(b"\t")
        and b"<!--Create time: 16.02.2024 07:20:44-->" in patched,
        "byte-nivå: mellomrom før '/>', tab-innrykk og kommentarer bevart")
    _ok(_xsd_facets(patched, "timeType")["pattern"] is not None
        and b'<xs:enumeration value="2.1" />' in patched, "timeType/versionType urørt")
    _ok(check_date_facets(patched) == [], "idempotent: ingen funn etter retting")

    # Manglende fasett → legges inn; manglende definisjon (brukt) → kanonisk def
    bare = (_SCFC_XSD
            .replace(b'\t\t\t<xs:maxExclusive value="9999-12-31Z" />\n', b"")
            .replace(b'\t\t\t<xs:minInclusive value="0001-01-01T00:00:00.000000000Z" />\n', b""))
    f2 = check_date_facets(bare)
    _ok(len(f2) == 3 and all(f["fix_to"] for f in f2), "manglende fasetter rapporteres som rettbare")
    p2, n2 = patch_date_facets(bare, f2)
    _ok(n2 == 3 and _xsd_facets(p2, "dateType")["maxExclusive"] == "10000-01-01Z"
        and _xsd_facets(p2, "dateTimeType")["minInclusive"] == "0001-01-01T00:00:00.000000000Z",
        "manglende fasetter satt inn")
    _ok(check_date_facets(p2) == [], "idempotent etter innsetting")

    import re as _re
    no_def = _re.sub(rb"\t<!-- dateTime type[^\n]*\n\t<xs:simpleType name=\"dateTimeType\">.*?</xs:simpleType>\n",
                     b"", _SCFC_XSD, flags=_re.DOTALL)
    f3 = [f for f in check_date_facets(no_def) if f["type_name"] == "dateTimeType"]
    _ok(len(f3) == 1 and f3[0]["kind"] == "date_missing", "brukt type uten definisjon → date_missing")
    p3, n3 = patch_date_facets(no_def, f3)
    fx = _xsd_facets(p3, "dateTimeType")
    _ok(n3 == 1 and fx["minInclusive"] == DATE_FACETS["dateTimeType"][1]
        and fx["maxExclusive"] == DATE_FACETS["dateTimeType"][2] and fx["pattern"],
        "kanonisk dateTimeType lagt inn med fasetter og pattern")
    _ok([f for f in check_date_facets(p3) if f["type_name"] == "dateTimeType"] == [],
        "idempotent etter innsetting av definisjon")

    # Ikke rettbart: feil base, og maxExclusive som ville innsnevre
    bad_base = _SCFC_XSD.replace(b'<xs:restriction base="xs:date">', b'<xs:restriction base="xs:string">')
    fb = [f for f in check_date_facets(bad_base) if f["type_name"] == "dateType"]
    _ok(len(fb) == 1 and fb[0]["kind"] == "date_base" and fb[0]["fix_to"] is None,
        "feil base rapporteres, rettes ikke")
    wide = _SCFC_XSD.replace(b'value="9999-12-31Z"', b'value="12000-01-01Z"')
    fw = [f for f in check_date_facets(wide) if f["type_name"] == "dateType"]
    _ok(len(fw) == 1 and fw[0]["fix_to"] is None, "maxExclusive år 12000 → innsnevring, rettes ikke")
    neg = _SCFC_XSD.replace(b'value="0001-01-01Z"', b'value="-0500-01-01Z"')
    fn = [f for f in check_date_facets(neg) if f["type_name"] == "dateType" and f["facet"] == "minInclusive"]
    _ok(len(fn) == 1 and fn[0]["fix_to"] is None, "negativt år i minInclusive → innsnevring, rettes ikke")

    # Ende-til-ende: operasjon + XML-validering på SCFC-XSD
    root = tmp / "dates"
    _write(root, "header/metadata.xml", _metadata({"table116": [
        ("a", "VARCHAR(10)"), ("d", "DATE"), ("x", "INTEGER"), ("y", "INTEGER"),
        ("t1", "TIMESTAMP"), ("z", "INTEGER"), ("w", "INTEGER"), ("v", "INTEGER"), ("t2", "TIMESTAMP")]}))
    _write(root, "content/schema0/table116/table116.xsd", _SCFC_XSD)
    _write(root, "content/schema0/table116/table116.xml", _TABLE_XML)
    ctx = WorkflowContext(siard_path=tmp / "dummy.siard", metadata=_null_log())
    ctx.extracted_path = root
    op = XsdTypeFixOperation()
    res = op.run(ctx)
    date_fixes = [c for c in res.data["changes"] if "maxExclusive" in c]
    _ok(res.success and len(date_fixes) == 2, f"operasjon: 2 datofasetter rettet ({res.data['changes']})")
    _ok(check_date_facets((root / "content/schema0/table116/table116.xsd").read_bytes()) == [],
        "XSD på disk rettet")
    _ok("T_6.3-1" in op.premis_detail(res, ctx), "premis_detail nevner T_6.3-1")

    siard = tmp / "dates.siard"
    root2 = tmp / "dates_src"
    _write(root2, "header/metadata.xml", (root / "header/metadata.xml").read_bytes())
    _write(root2, "content/schema0/table116/table116.xsd", _SCFC_XSD)
    _write(root2, "content/schema0/table116/table116.xml", _TABLE_XML)
    with zipfile.ZipFile(siard, "w") as zf:
        for p in sorted(root2.rglob("*")):
            if p.is_file():
                zf.write(p, str(p.relative_to(root2)).replace("\\", "/"))
    resv = XMLValidationOperation().run(WorkflowContext(siard_path=siard, metadata=_null_log()))
    t63 = [e for e in resv.data["errors"] if e.startswith("T_6.3-1")]
    _ok(len(t63) == 2 and all("rettes av «Rett tableX.xsd»" in e for e in t63),
        "XML-validering: 2 T_6.3-1-feil, begge merket rettbare")
    ress = XsdTypeFixOperation().run(WorkflowContext(siard_path=siard, metadata=_null_log()))
    _ok(ress.success and ress.data["fixes"] == 2, "standalone zip: 2 rettinger")
    resv2 = XMLValidationOperation().run(
        WorkflowContext(siard_path=Path(ress.data["output_path"]), metadata=_null_log()))
    _ok(not any(e.startswith("T_6.3-1") for e in resv2.data["errors"]),
        "etter retting: ingen T_6.3-1-feil")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    with tempfile.TemporaryDirectory(prefix="xsd_types_") as td:
        tmp = Path(td)
        test_mapping()
        test_compare(tmp)
        test_operation_pipeline(tmp)
        test_operation_standalone(tmp)
        test_xml_validation_and_scan(tmp)
        test_date_facets(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
