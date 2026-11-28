"""
Tidspunkt uten tidssone helt inntil fasettgrensen (DBPTK T_6.0-2).

Bakgrunn (nov. 2026, DBPTK-validering av behandlet CosDoc-uttrekk): uttrekket
hadde «uendelig»-markører som 9999-12-31T23:59:59.9999999 uten tidssone
(SQL Servers systemversjonerte tabeller, ValidTo/EndTime). XML Schema kan ikke
avgjøre et tidspunkt uten tidssone mot grensen 10000-01-01T00:00:00Z (±14 t),
og validatoren feilet med cvc-maxExclusive-valid. «Rett tableX.xsd» merker nå
akkurat disse verdiene som UTC med «Z».

Verifiserer:
  1. grensene: hvilke tidspunkt er ubestemte (øvre fra 10:00, nedre til 14:00)
  2. oppdelingen i biter deler aldri en celle — samme resultat for alle
     blokkstørrelser, også med svært lange celler
  3. bare kolonner XSD-en typer som tidspunkt røres; andre verdier står
  4. pipeline: tableX.xml validerer mot XSD-en etterpå (lxml — som ikke
     fanger selve feilen; den er kontrollert med DBPTK-validatoren), fila uten
     treff skrives ikke på nytt, PREMIS-tekst, ny kjøring gjør ingenting
  5. standalone (zip → zip)
  6. parameteren av → ingenting endres; stopp → originalen står urørt

Alle data er syntetiske.

Kjør:  python -X utf8 tests/test_date_bounds.py
"""
from __future__ import annotations

import io
import os
import random
import re
import sys
import tempfile
import threading
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core import date_bounds as db                       # noqa: E402
from siard_workflow.core.context import WorkflowContext                 # noqa: E402
from siard_workflow.operations.xsd_type_fix_operation import XsdTypeFixOperation  # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


MNS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
TNS = "http://www.bar.admin.ch/xmlns/siard/2/table.xsd"

MAXV = "9999-12-31T23:59:59.9999999"


def _metadata(tables: "dict[str, list[tuple[str, str]]]") -> bytes:
    tbls = ""
    for folder, cols in tables.items():
        c = "".join(f"<column><name>{n}</name><type>{t}</type>"
                    f"<nullable>true</nullable></column>" for n, t in cols)
        tbls += (f"<table><name>T_{folder}</name><folder>{folder}</folder>"
                 f"<columns>{c}</columns><rows>1</rows></table>")
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<siardArchive xmlns="{MNS}" version="2.1"><dbname>t</dbname>'
            f'<schemas><schema><name>s</name><folder>schema0</folder>'
            f'<tables>{tbls}</tables></schema></schemas></siardArchive>\n').encode()


def _xsd(col_types: "list[str]") -> bytes:
    els = "".join(f'      <xs:element minOccurs="0" name="c{i}" type="{t}"/>\n'
                  for i, t in enumerate(col_types, start=1))
    return (f'''<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns="{TNS}" xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="{TNS}" elementFormDefault="qualified">
  <xs:element name="table"><xs:complexType><xs:sequence>
    <xs:element maxOccurs="unbounded" minOccurs="0" name="row" type="recordType"/>
  </xs:sequence></xs:complexType></xs:element>
  <xs:complexType name="recordType">
    <xs:sequence>
{els}    </xs:sequence>
  </xs:complexType>
  <xs:simpleType name="dateType">
    <xs:restriction base="xs:date">
      <xs:minInclusive value="0001-01-01Z"/>
      <xs:maxExclusive value="10000-01-01Z"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="dateTimeType">
    <xs:restriction base="xs:dateTime">
      <xs:minInclusive value="0001-01-01T00:00:00.000000000Z"/>
      <xs:maxExclusive value="10000-01-01T00:00:00.000000000Z"/>
      <xs:pattern value="\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}}(\\.\\d+)?Z?"/>
    </xs:restriction>
  </xs:simpleType>
</xs:schema>
''').encode()


def _table_xml(rows: "list[list[str]]") -> bytes:
    body = ""
    for r in rows:
        cells = "".join(f"<c{i}>{v}</c{i}>" for i, v in enumerate(r, 1) if v is not None)
        body += f"<row>{cells}</row>\n"
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="{TNS}">\n'
            f'{body}</table>\n').encode()


# table0: c1 heltall, c2 tekst, c3 tidspunkt (dateTimeType), c4 dato, c5 tidspunkt (xs:dateTime)
T0_TYPES = ["xs:integer", "xs:string", "dateTimeType", "dateType", "xs:dateTime"]
T0_ROWS = [
    ["1", "vanlig", "2020-05-17T12:00:00", "2020-05-17", "2021-01-01T00:00:00"],
    ["2", MAXV, MAXV, "9999-12-31", "9999-12-31T23:59:59"],          # c2 er TEKST
    ["3", "x", "9999-12-31T00:00:00.0000000", "9999-12-31", None],   # avgjørbar — står
    ["4", "x", "9999-12-31T23:59:59.9970000", None, "0001-01-01T00:00:00"],
    ["5", "x", "9999-12-31T23:59:59Z", None, "9999-12-31T23:59:59+01:00"],  # har tidssone
    ["6", "x", "0001-01-01T14:00:00.5", None, "0001-01-01T13:59:59"],
]
# table1: tidspunktkolonne uten grenseverdier → fila skal ikke skrives på nytt
T1_TYPES = ["xs:integer", "dateTimeType"]
T1_ROWS = [["1", "2001-02-03T04:05:06"], ["2", "9999-12-31T09:59:59.999"]]
# table2: ingen tidspunktkolonner, men en tekstkolonne med «grenseverdi»
T2_TYPES = ["xs:integer", "xs:string"]
T2_ROWS = [["1", MAXV]]


def _build(root: Path) -> None:
    def wr(rel: str, data: bytes) -> None:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    wr("header/metadata.xml", _metadata({
        "table0": [("ID", "INTEGER"), ("TEKST", "VARCHAR(50)"), ("EndTime", "TIMESTAMP"),
                   ("Dato", "DATE"), ("TilDato", "TIMESTAMP")],
        "table1": [("ID", "INTEGER"), ("Tid", "TIMESTAMP")],
        "table2": [("ID", "INTEGER"), ("TEKST", "VARCHAR(50)")],
    }))
    for name, types, rows in (("table0", T0_TYPES, T0_ROWS), ("table1", T1_TYPES, T1_ROWS),
                              ("table2", T2_TYPES, T2_ROWS)):
        wr(f"content/schema0/{name}/{name}.xsd", _xsd(types))
        wr(f"content/schema0/{name}/{name}.xml", _table_xml(rows))


def _validate(root: Path, table: str) -> "str | None":
    """None = gyldig; ellers første feil. (lxml)"""
    from lxml import etree
    d = root / "content" / "schema0" / table
    schema = etree.XMLSchema(etree.parse(str(d / f"{table}.xsd")))
    doc = etree.parse(str(d / f"{table}.xml"))
    return None if schema.validate(doc) else str(schema.error_log.last_error)


class _Log:
    def __init__(self):
        self.lines = []

    def log(self, msg, lvl="info"):
        self.lines.append((lvl, msg))


def _ctx(root: "Path | None", siard: Path, log=None, stop=None) -> WorkflowContext:
    ctx = WorkflowContext(siard_path=siard, metadata={
        "file_logger": log, "progress_cb": None,
        "stop_event": stop or threading.Event()})
    if root is not None:
        ctx.extracted_path = root
    return ctx


# ── 1 ────────────────────────────────────────────────────────────────────────

def test_bounds() -> None:
    print("\n1. Hvilke tidspunkt er ubestemte mot grensen")
    yes = [MAXV, "9999-12-31T23:59:59", "9999-12-31T10:00:00", "9999-12-31T10:00:00.0",
           "0001-01-01T00:00:00", "0001-01-01T13:59:59.999", "0001-01-01T14:00:00",
           "0001-01-01T14:00:00.000"]
    no = ["9999-12-31T09:59:59.9999999", "9999-12-31T00:00:00", "0001-01-01T14:00:00.001",
          "0001-01-01T14:00:01", "0001-01-02T00:00:00", "9999-12-30T23:59:59",
          "9999-12-31T23:59:59Z", "9999-12-31T23:59:59+02:00", "2020-01-01T00:00:00",
          "9999-12-31", "tekst"]
    for v in yes:
        _ok(db.is_indeterminate(v), f"ubestemt: {v}")
    for v in no:
        _ok(not db.is_indeterminate(v), f"ikke ubestemt: {v}")


# ── 2 ────────────────────────────────────────────────────────────────────────

def test_segments() -> None:
    print("\n2. Oppdeling i biter deler aldri en celle")
    rnd = random.Random(7)
    rows = []
    for i in range(60):
        long_text = "y" * rnd.choice([0, 3, 40, 700, 5000])
        rows.append([str(i), long_text + ("&lt;c3&gt;" if i % 7 == 0 else ""),
                     rnd.choice([MAXV, "2020-01-01T00:00:00", "9999-12-31T23:59:59",
                                 "0001-01-01T00:00:00", "9999-12-31T01:00:00"]),
                     None, rnd.choice([MAXV, "1999-12-31T23:59:59"])])
    data = _table_xml(rows) + b"<!-- " + b"k" * 900 + b" -->"
    cols = {3, 5}

    def run(chunk: int) -> "tuple[bytes, dict]":
        src, out = io.BytesIO(data), io.BytesIO()
        old = db.CHUNK
        db.CHUNK = chunk
        try:
            counts = db.fix_stream(src.read, out.write, cols)
        finally:
            db.CHUNK = old
        return out.getvalue(), counts

    ref, ref_counts = run(1 << 20)
    n_expected = sum(1 for r in rows for ci in (3, 5)
                     if r[ci - 1] and db.is_indeterminate(r[ci - 1]))
    _ok(sum(ref_counts.values()) == n_expected and n_expected > 20,
        f"alle {n_expected} grenseverdier funnet i én blokk")
    _ok(ref.replace(b"Z</c", b"</c") == data, "bare «Z» er lagt til — ellers byte-identisk")
    _ok(b"".join(db.segments(io.BytesIO(data).read, 13)) == data,
        "bitene satt sammen gir originalen")
    bad = [c for c in list(range(1, 140)) + [255, 256, 511, 512, 513, 1000, 4096]
           if run(c) != (ref, ref_counts)]
    _ok(not bad, f"samme resultat for alle blokkstørrelser (avvik: {bad[:5]})")


# ── 3 + 4 ────────────────────────────────────────────────────────────────────

def test_pipeline() -> None:
    print("\n3–4. Pipeline: bare tidspunktkolonner, gyldig mot XSD etterpå")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "ut"
        _build(root)
        t0 = root / "content/schema0/table0/table0.xml"
        t1 = root / "content/schema0/table1/table1.xml"
        t2 = root / "content/schema0/table2/table2.xml"
        before = {p: p.read_bytes() for p in (t0, t1, t2)}
        # NB: lxml (libxml2) godtar ubestemte sammenligninger og kan derfor ikke
        # vise feilen FØR rettingen — det gjør bare Xerces, som DBPTK bruker.
        # Før/etter er kontrollert med DBPTK-validatoren 4.4.0 (nov. 2026).
        mtime1 = t1.stat().st_mtime_ns

        log = _Log()
        op = XsdTypeFixOperation()
        res = op.run(_ctx(root, Path(td) / "x.siard", log))
        _ok(res.success, f"operasjonen fullfører ({res.message})")
        after = t0.read_bytes()

        def cell(row: int, col: int) -> str:
            m = re.findall(rb"<row>(.*?)</row>", after, re.S)[row - 1]
            c = re.search(rb"<c%d>(.*?)</c%d>" % (col, col), m)
            return c.group(1).decode() if c else ""

        _ok(cell(2, 3) == MAXV + "Z" and cell(4, 3) == "9999-12-31T23:59:59.9970000Z",
            "tidspunkt ved øvre grense har fått «Z»")
        _ok(cell(2, 5) == "9999-12-31T23:59:59Z", "xs:dateTime-kolonne behandles også")
        _ok(cell(4, 5) == "0001-01-01T00:00:00Z" and cell(6, 5) == "0001-01-01T13:59:59Z",
            "tidspunkt ved nedre grense har fått «Z»")
        _ok(cell(2, 2) == MAXV, "TEKST-kolonne med samme tegn er urørt")
        _ok(cell(3, 3) == "9999-12-31T00:00:00.0000000" and cell(6, 3) == "0001-01-01T14:00:00.5",
            "avgjørbare tidspunkt på grensedagene er urørt")
        _ok(cell(5, 3) == "9999-12-31T23:59:59Z" and cell(5, 5) == "9999-12-31T23:59:59+01:00",
            "tidspunkt som alt har tidssone er urørt")
        _ok(cell(1, 3) == "2020-05-17T12:00:00" and cell(2, 4) == "9999-12-31",
            "vanlige tidspunkt og datoer er urørt")
        _ok(after.replace(b"Z</c", b"</c") == before[t0].replace(b"Z</c", b"</c"),
            "ingen andre byte er endret i table0.xml")
        _ok(_validate(root, "table0") is None,
            "etter: table0 er gyldig mot XSD-en (mønsteret tillater «Z»)")
        _ok(t1.read_bytes() == before[t1] and t1.stat().st_mtime_ns == mtime1,
            "table1 (ingen grenseverdier) er ikke skrevet på nytt")
        _ok(t2.read_bytes() == before[t2], "table2 (ingen tidspunktkolonner) er urørt")
        _ok(not list(root.rglob("*.utc_tmp")), "ingen midlertidige filer ligger igjen")

        d = res.data
        _ok(d.get("date_values_marked") == 5 and d.get("fixes") == 0,
            "resultatet: 5 tidspunkt merket, ingen XSD-rettinger")
        _ok(d.get("date_value_columns") == ["T_table0.EndTime (2)", "T_table0.TilDato (3)"],
            f"kolonnene oppgis: {d.get('date_value_columns')}")
        _ok(op.premis_should_record(res, None), "PREMIS-hendelse føres")
        detail = op.premis_detail(res, None)
        _ok("5 tidspunkt uten tidssone" in detail and "T_6.0-2" in detail
            and "sifrene er uendret" in detail and "T_table0.EndTime (2)" in detail,
            f"PREMIS-teksten forklarer endringen: {detail[:90]} …")
        _ok(any("leser 2 tabell(er) med tidspunktkolonner" in m for _l, m in log.lines),
            "loggen sier hva som leses")
        _ok(any("schema0/table0 T_table0.EndTime (c3): 2 verdi(er)" in m
                and "→ «" in m for _l, m in log.lines),
            "loggen lister kolonnene med eksempel")

        res2 = XsdTypeFixOperation().run(_ctx(root, Path(td) / "x.siard"))
        _ok(res2.success and res2.data.get("date_values_marked") == 0
            and t0.read_bytes() == after, "ny kjøring gjør ingenting")
        _ok(not XsdTypeFixOperation().premis_should_record(res2, None),
            "ny kjøring føres ikke i PREMIS")


# ── 5 ────────────────────────────────────────────────────────────────────────

def test_zip() -> None:
    print("\n5. Standalone (zip → zip)")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "ut"
        _build(root)
        siard = Path(td) / "kilde.siard"
        with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as z:
            for p in sorted(root.rglob("*")):
                if p.is_file():
                    z.write(p, p.relative_to(root).as_posix())
        src_bytes = siard.read_bytes()
        ctx = _ctx(None, siard)
        res = XsdTypeFixOperation().run(ctx)
        _ok(res.success and res.data.get("date_values_marked") == 5,
            f"5 tidspunkt merket ({res.message})")
        dst = Path(td) / "kilde_xsdfix.siard"
        _ok(dst.exists() and ctx.siard_path == dst, "ny fil kilde_xsdfix.siard er skrevet")
        _ok(siard.read_bytes() == src_bytes, "kildefila er urørt")
        with zipfile.ZipFile(dst) as z, zipfile.ZipFile(siard) as z0:
            _ok(z.testzip() is None and z.namelist() == z0.namelist(),
                "arkivet er helt, med samme oppføringer i samme rekkefølge")
            t0 = z.read("content/schema0/table0/table0.xml")
            _ok(t0.count(b"Z</c") == 5 + 1 and (MAXV + "Z").encode() in t0,
                "table0.xml har «Z» på grenseverdiene")
            _ok(t0.replace(b"Z</c", b"</c")
                == z0.read("content/schema0/table0/table0.xml").replace(b"Z</c", b"</c"),
                "ellers byte-identisk med kilden")
            for n in z0.namelist():
                if not n.endswith("table0/table0.xml"):
                    assert z.read(n) == z0.read(n), n
            _ok(True, "alle andre oppføringer er byte-identiske")

        # Ingen funn → ingen ny fil
        res2 = XsdTypeFixOperation().run(_ctx(None, dst))
        _ok(res2.success and not (Path(td) / "kilde_xsdfix_xsdfix.siard").exists(),
            "fil uten funn gir ingen ny SIARD")


# ── 6 ────────────────────────────────────────────────────────────────────────

def test_off_and_stop() -> None:
    print("\n6. Parameter av, og stopp underveis")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "ut"
        _build(root)
        t0 = root / "content/schema0/table0/table0.xml"
        before = t0.read_bytes()
        res = XsdTypeFixOperation(mark_boundary_datetimes=False).run(
            _ctx(root, Path(td) / "x.siard"))
        _ok(res.success and t0.read_bytes() == before
            and res.data.get("date_values_marked") == 0,
            "parameteren av: tableX.xml er urørt")

        stop = threading.Event()
        stop.set()
        res = XsdTypeFixOperation().run(_ctx(root, Path(td) / "x.siard", stop=stop))
        _ok(not res.success and "Stoppet" in res.message, "stopp gir tydelig melding")
        _ok(t0.read_bytes() == before and not list(root.rglob("*.utc_tmp")),
            "originalen står urørt, ingen midlertidig fil igjen")

        # Stopp midt i omskrivingen av en fil
        calls = [0]

        def stop_later() -> bool:
            calls[0] += 1
            return calls[0] > 1
        old = db.CHUNK
        db.CHUNK = 64
        try:
            try:
                db.fix_table_xml(t0, {3, 5}, None, stop_later)
                raised = False
            except db.Stopped:
                raised = True
        finally:
            db.CHUNK = old
        _ok(raised and t0.read_bytes() == before and not list(root.rglob("*.utc_tmp")),
            "stopp midt i en fil: originalen står, midlertidig fil er fjernet")


def main() -> int:
    test_bounds()
    test_segments()
    test_pipeline()
    test_zip()
    test_off_and_stop()
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
