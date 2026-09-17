"""siard_workflow/core/column_xsd_types.py

Konsistens mellom kolonnetypen i header/metadata.xml og XSD-typen i
content/schemaN/tableM/tableM.xsd (DBPTK-validatorens regel P_4.3-3).

Bakgrunn
========
DBPTK-validatoren (``MetadataAndTableDataValidator``, verifisert mot
keeps/dbptk-developer master 2026-09-15) krever at ``type``-attributtet på
``/xs:schema/xs:complexType[@name='recordType']/xs:sequence/xs:element``
i tableM.xsd stemmer med SQL:2008-typen i metadata.xml, etter tabellen
``populateSQL2008Types()``. Sentrale regler::

    FLOAT / FLOAT(p)   -> xs:double     (uansett p — ingen presisjonsgrense)
    DOUBLE PRECISION   -> xs:double
    REAL               -> xs:float
    INTEGER/INT/SMALLINT/BIGINT -> xs:integer
    DECIMAL/NUMERIC    -> xs:decimal

Typiske uttrekk (SCFC, og DBPTKs egen eksport, som mapper ``FLOAT(p)`` til
xs:float i strid med sin egen validator) deklarerer ``FLOAT(53)`` i metadata
mens XSD-en sier ``xs:float``. Validatoren feiler da med::

    For 'FLOAT(53)' type expected '[xs:double]' but found 'xs:float' in c5 at ...

Hva som rettes
==============
Kun TAPSFRIE utvidelser der alle verdier i tableM.xml forblir gyldige under
den nye typen (verdiene endres ikke):

    xs:float                          -> xs:double
    xs:int/xs:long/xs:short/xs:byte/
    xs:unsigned*/xs:*Integer          -> xs:integer   (eller xs:decimal)
    xs:integer                        -> xs:decimal

Andre avvik (f.eks. xs:string der xs:decimal forventes, clobType mot
blobType) RAPPORTERES bare — dataene er ikke nødvendigvis gyldige under den
forventede typen, og en endring der krever manuell vurdering.

metadata.xml røres ikke: ``FLOAT(53)`` er den korrekte kildetypen (dobbel
presisjon i SQL Server/Sybase); det er XSD-en som beskriver den feil.

Dato-/tidsstempelfasetter (T_6.3-1)
===================================
``DateAndTimestampDataValidator`` krever at ``dateType`` og ``dateTimeType``
i tableM.xsd har NØYAKTIG disse fasettene (sammenlignet tegn for tegn)::

    dateType:      minInclusive 0001-01-01Z
                   maxExclusive 10000-01-01Z
    dateTimeType:  minInclusive 0001-01-01T00:00:00.000000000Z
                   maxExclusive 10000-01-01T00:00:00.000000000Z

SCFC skriver ``maxExclusive="9999-12-31Z"`` / ``…T23:59:59.9999999Z`` og
feiler med «restriction not enforced». Rettingen UTVIDER grensen (siste dag i
år 9999 blir gyldig), og er tapsfri. Andre avvik regnes som tapsfrie når de
ikke innsnevrer intervallet år 0001–9999 (SQL:2008 tillater ikke datoer
utenfor). Mangler definisjonen mens kolonner bruker typen, legges DBPTKs
kanoniske definisjon inn. ``xs:pattern`` og øvrig innhold røres ikke.
Merk: validatoren sjekker bare tabeller med MER ENN ÉN kolonne av typen —
vi retter uansett, slik at definisjonen er lik i alle tabeller.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path
from typing import Callable
import xml.etree.ElementTree as ET


# ── DBPTK populateSQL2008Types() — portert regex for regex ───────────────────
# (nøkkel, tillatte XSD-typer). Java bruker String.matches (hele strengen),
# så mønstrene er forankret med ^…$ som i kilden.
_N   = r"\s*\(\s*[1-9]\d*\s*\)"                    # (n)
_NKM = r"\s*\(\s*[1-9]\d*(\s*(K|M|G))?\s*\)"        # (n[K|M|G])
_NS  = r"\s*\(\s*[1-9]\d*\s*(,\s*\d+\s*)?\)"        # (p[,s])
_TS  = r"\s*\(\s*(0|([1-9]\d*))\s*\)"               # (0|n)

_SQL2008_TO_XSD: list[tuple[re.Pattern, tuple[str, ...]]] = [(re.compile(p, re.IGNORECASE), t) for p, t in [
    (r"^BIGINT$",                                              ("xs:integer",)),
    (r"^BINARY\s+LARGE\s+OBJECT(" + _NKM + r")?$",             ("blobType",)),
    (r"^BLOB(" + _NKM + r")?$",                                ("blobType",)),
    (r"^BLOB",                                                 ("blobType",)),
    (r"^(?:BINARY\s+VARYING|VARBINARY)(" + _N + r")?$",        ("clobType", "xs:hexBinary")),
    (r"^BINARY\s*(" + _N + r")?$",                             ("blobType", "xs:hexBinary")),
    (r"^VARBINARY(" + _N + r")?$",                             ("blobType", "xs:hexBinary")),
    (r"^BOOLEAN$",                                             ("xs:boolean",)),
    (r"^CHARACTER\s+LARGE\s+OBJECT(" + _NKM + r")?$",          ("clobType",)),
    (r"^CLOB(" + _NKM + r")?$",                                ("clobType",)),
    (r"^CHARACTER\s+VARYING(" + _N + r")?$",                   ("clobType", "xs:string")),
    (r"^CHAR\s+VARYING(" + _N + r")?$",                        ("clobType", "xs:string")),
    (r"^VARCHAR(" + _N + r")?$",                               ("clobType", "xs:string")),
    (r"^CHARACTER(" + _N + r")?$",                             ("clobType", "xs:string")),
    (r"^CHAR(" + _N + r")?$",                                  ("clobType", "xs:string")),
    (r"^DATE$",                                                ("dateType",)),
    (r"^DECIMAL(" + _NS + r")?$",                              ("xs:decimal",)),
    (r"^DEC(" + _NS + r")?$",                                  ("xs:decimal",)),
    (r"^DOUBLE PRECISION$",                                    ("xs:double",)),
    (r"^FLOAT(" + _N + r")?$",                                 ("xs:double",)),
    (r"^INTEGER$",                                             ("xs:integer",)),
    (r"^INT$",                                                 ("xs:integer",)),
    (r"^INTERVAL\s+(((YEAR|MONTH|DAY|HOUR|MINUTE)(" + _N + r")?"
     r"(\s+TO\s+(MONTH|DAY|HOUR|MINUTE|SECOND)(" + _N + r")?)?)|"
     r"(SECOND(" + _NS + r")?))$",                             ("xs:duration",)),
    (r"^NATIONAL\s+CHARACTER\s+LARGE\s+OBJECT(" + _NKM + r")?$", ("clobType", "xs:string")),
    (r"^NCHAR\s+LARGE\s+OBJECT(" + _NKM + r")?$",              ("clobType", "xs:string")),
    (r"^NCLOB(" + _NKM + r")?$",                               ("clobType", "xs:string")),
    (r"^NATIONAL\s+CHARACTER\s+VARYING(" + _N + r")?$",        ("clobType", "xs:string")),
    (r"^NATIONAL\s+CHAR\s+VARYING(" + _N + r")?$",             ("clobType", "xs:string")),
    (r"^NCHAR\s+VARYING(" + _N + r")?$",                       ("clobType", "xs:string")),
    (r"^NATIONAL\s+CHARACTER(" + _N + r")?$",                  ("clobType", "xs:string")),
    (r"^NATIONAL\s+CHAR(" + _N + r")?$",                       ("clobType", "xs:string")),
    (r"^NCHAR(" + _N + r")?$",                                 ("clobType", "xs:string")),
    (r"^NUMERIC(" + _NS + r")?$",                              ("xs:decimal",)),
    (r"^REAL$",                                                ("xs:float",)),
    (r"^SMALLINT$",                                            ("xs:integer",)),
    (r"^TIME(" + _N + r")?$",                                  ("timeType",)),
    (r"^TIME\s+WITH\s+TIME\s+ZONE(" + _N + r")?$",             ("timeType",)),
    (r"^TIMESTAMP(" + _TS + r")?$",                            ("dateTimeType",)),
    (r"^TIMESTAMP\s+WITH\s+TIME\s+ZONE(" + _TS + r")?$",       ("dateTimeType",)),
    (r"^XML$",                                                 ("clobType",)),
]]

# XSD-typer hvis verdirom er en delmengde av xs:integer (og dermed xs:decimal)
_INTEGER_FAMILY = frozenset((
    "xs:integer", "xs:int", "xs:long", "xs:short", "xs:byte",
    "xs:unsignedLong", "xs:unsignedInt", "xs:unsignedShort", "xs:unsignedByte",
    "xs:nonNegativeInteger", "xs:positiveInteger",
    "xs:negativeInteger", "xs:nonPositiveInteger",
))

_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")


def normalize_sql_type(sql_type: str) -> str:
    """Trim og slå sammen mellomrom: ' float ( 53 )' -> 'float ( 53 )'."""
    return re.sub(r"\s+", " ", (sql_type or "").strip())


def expected_xsd_types(sql_type: str) -> tuple[str, ...] | None:
    """
    XSD-typene DBPTK godtar for en SQL:2008-type, eller None hvis typen ikke
    matcher noe mønster (DBPTK rapporterer da også avvik).
    """
    t = normalize_sql_type(sql_type)
    for pat, allowed in _SQL2008_TO_XSD:
        if pat.match(t):
            return allowed
    return None


def lossless_fix(found: str, expected: tuple[str, ...]) -> str | None:
    """
    Ny XSD-type hvis avviket kan rettes uten at noen verdi blir ugyldig,
    ellers None.
    """
    if found == "xs:float" and "xs:double" in expected:
        return "xs:double"
    if found in _INTEGER_FAMILY:
        if "xs:integer" in expected and found != "xs:integer":
            return "xs:integer"
        if "xs:decimal" in expected:
            return "xs:decimal"
    return None


# ── Parsing ──────────────────────────────────────────────────────────────────

def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _child(el: ET.Element, name: str) -> ET.Element | None:
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _text(el: ET.Element | None, name: str) -> str:
    c = _child(el, name) if el is not None else None
    return (c.text or "").strip() if c is not None and c.text else ""


def tables_from_metadata(meta_bytes: bytes) -> list[dict]:
    """
    [{schema_folder, table_folder, table_name,
      columns: [(idx, name, sql_type)]}] — kolonner med <typeName> (UDT) i
    stedet for <type> tas med med tom sql_type og hoppes over i sjekken.
    """
    root = ET.parse(io.BytesIO(meta_bytes)).getroot()
    out: list[dict] = []
    schemas = _child(root, "schemas")
    if schemas is None:
        return out
    for si, schema in enumerate(c for c in schemas if _local(c.tag) == "schema"):
        sfolder = _text(schema, "folder") or f"schema{si}"
        tables = _child(schema, "tables")
        if tables is None:
            continue
        for table in (c for c in tables if _local(c.tag) == "table"):
            tfolder = _text(table, "folder")
            cols_el = _child(table, "columns")
            if not tfolder or cols_el is None:
                continue
            cols: list[tuple[int, str, str]] = []
            for idx, col in enumerate(
                    (c for c in cols_el if _local(c.tag) == "column"), start=1):
                cols.append((idx, _text(col, "name") or f"c{idx}",
                             _text(col, "type")))
            out.append({"schema_folder": sfolder, "table_folder": tfolder,
                        "table_name": _text(table, "name") or tfolder,
                        "columns": cols})
    return out


def xsd_column_types(xsd_bytes: bytes) -> dict[int, str]:
    """{kolonneindeks: type} fra recordType-sekvensen i en tableM.xsd."""
    root = ET.parse(io.BytesIO(xsd_bytes)).getroot()
    types: dict[int, str] = {}
    for ct in root:
        if _local(ct.tag) != "complexType" or ct.get("name") != "recordType":
            continue
        for seq in ct:
            if _local(seq.tag) != "sequence":
                continue
            for el in seq:
                if _local(el.tag) != "element":
                    continue
                m = re.fullmatch(r"c([0-9]+)", el.get("name") or "")
                t = el.get("type")
                if m and t:
                    types[int(m.group(1))] = t
    return types


def xsd_arc_path(schema_folder: str, table_folder: str) -> str:
    return f"content/{schema_folder}/{table_folder}/{table_folder}.xsd"


# ── Sammenligning ────────────────────────────────────────────────────────────

def compare_column_types(
        meta_bytes: bytes,
        read_xsd: Callable[[str, str], bytes | None]) -> list[dict]:
    """
    Sammenlign alle kolonner i metadata.xml med tableM.xsd.

    read_xsd(schema_folder, table_folder) -> bytes | None.

    Returnerer funn: {kind, schema, table, table_name, col_index, col_name,
    sql_type, found, expected, fix_to}. kind er 'mismatch', 'unknown_sql_type',
    'missing_element' eller 'missing_xsd'. fix_to er ny XSD-type når avviket
    kan rettes tapsfritt, ellers None.
    """
    findings: list[dict] = []
    for tbl in tables_from_metadata(meta_bytes):
        base = {"schema": tbl["schema_folder"], "table": tbl["table_folder"],
                "table_name": tbl["table_name"]}
        xsd = read_xsd(tbl["schema_folder"], tbl["table_folder"])
        if xsd is None:
            findings.append({**base, "kind": "missing_xsd", "col_index": 0,
                             "col_name": "", "sql_type": "", "found": None,
                             "expected": (), "fix_to": None})
            continue
        try:
            xsd_types = xsd_column_types(xsd)
        except ET.ParseError as exc:
            findings.append({**base, "kind": "missing_xsd", "col_index": 0,
                             "col_name": "", "sql_type": f"ugyldig XSD: {exc}",
                             "found": None, "expected": (), "fix_to": None})
            continue
        for df in check_date_facets(xsd):
            findings.append({**base, "col_index": 0, "col_name": df["type_name"],
                             "sql_type": "", **df})
        for idx, name, sql_type in tbl["columns"]:
            if not sql_type:
                continue                       # <typeName> (UDT) — P_4.3-6, ikke vår sak
            col = {**base, "col_index": idx, "col_name": name,
                   "sql_type": normalize_sql_type(sql_type)}
            expected = expected_xsd_types(sql_type)
            found = xsd_types.get(idx)
            if found is None:
                findings.append({**col, "kind": "missing_element",
                                 "found": None, "expected": expected or (),
                                 "fix_to": None})
            elif expected is None:
                findings.append({**col, "kind": "unknown_sql_type",
                                 "found": found, "expected": (), "fix_to": None})
            elif found not in expected:
                findings.append({**col, "kind": "mismatch", "found": found,
                                 "expected": expected,
                                 "fix_to": lossless_fix(found, expected)})
    return findings


def format_finding(f: dict) -> str:
    """Én linje per funn, i stil med DBPTKs egen feilmelding."""
    where = f"{f['schema']}/{f['table']}"
    if f["kind"] == "date_missing":
        return (f"{where}: kolonner bruker {f['type_name']}, men definisjonen "
                f"mangler → legges inn (T_6.3-1)")
    if f["kind"] == "date_base":
        return (f"{where}: {f['type_name']} har base {f['found']}, forventet "
                f"{f['expected']} — rettes ikke automatisk")
    if f["kind"] == "date_facet":
        found = f"«{f['found']}»" if f["found"] else "mangler"
        tail = (f" → rettes til «{f['expected']}»" if f.get("fix_to")
                else f" (forventet «{f['expected']}») — rettes ikke automatisk")
        return f"{where}: {f['type_name']} {f['facet']} {found}{tail}"
    if f["kind"] == "missing_xsd":
        extra = f" ({f['sql_type']})" if f.get("sql_type") else ""
        return f"{where}: tableX.xsd mangler{extra}"
    where += f".{f['col_name']} (c{f['col_index']})"
    if f["kind"] == "missing_element":
        return f"{where}: {f['sql_type']} — ingen c{f['col_index']} i recordType"
    if f["kind"] == "unknown_sql_type":
        return (f"{where}: «{f['sql_type']}» er ikke en gjenkjent SQL:2008-type "
                f"(XSD: {f['found']})")
    exp = "/".join(f["expected"])
    tail = (f" → rettes til {f['fix_to']}" if f.get("fix_to")
            else " — rettes ikke automatisk")
    return f"{where}: {f['sql_type']} forventer {exp}, XSD har {f['found']}{tail}"


# ── Retting av tableM.xsd ────────────────────────────────────────────────────

_XSD_ELEMENT_RE   = re.compile(rb"<(?:[A-Za-z0-9_]+:)?element\b[^>]*>")
_XSD_COL_NAME_RE  = re.compile(rb'\bname\s*=\s*"c([0-9]+)"')
_XSD_TYPE_ATTR_RE = re.compile(rb'(\btype\s*=\s*")([^"]*)(")')
_XSD_CLOB_DEF_RE  = re.compile(rb'\bname\s*=\s*"clobType"')
_XSD_BLOB_DEF_RE  = re.compile(
    rb'<(?:[A-Za-z0-9_]+:)?complexType\b[^>]*\bname\s*=\s*"blobType"[^>]*>'
    rb'.*?</(?:[A-Za-z0-9_]+:)?complexType\s*>', re.DOTALL)


def patch_xsd_column_types(xsd: bytes, changes: dict[int, str]) -> tuple[bytes, int]:
    """
    Sett type="…" for <xs:element name="cN"> i en tableM.xsd etter
    changes = {N: ny_type}. Byte-nivå: bevarer formatering, kommentarer og
    namespace-prefiks. Blir en kolonne clobType uten at XSD-en definerer
    clobType, avledes definisjonen fra blobType (xs:hexBinary → xs:string).
    Returnerer (nye_bytes, antall_endret).
    """
    n = 0

    def _sub(m: re.Match) -> bytes:
        nonlocal n
        tag = m.group(0)
        nm  = _XSD_COL_NAME_RE.search(tag)
        if not nm:
            return tag
        new_type = changes.get(int(nm.group(1)))
        if new_type is None:
            return tag
        new_b = new_type.encode("utf-8")

        def _t(tm: re.Match) -> bytes:
            nonlocal n
            if tm.group(2) == new_b:
                return tm.group(0)
            n += 1
            return tm.group(1) + new_b + tm.group(3)
        return _XSD_TYPE_ATTR_RE.sub(_t, tag, count=1)

    out = _XSD_ELEMENT_RE.sub(_sub, xsd)
    if (n and "clobType" in changes.values()
            and not _XSD_CLOB_DEF_RE.search(out)):
        bm = _XSD_BLOB_DEF_RE.search(out)
        if bm:
            clob_def = (bm.group(0)
                        .replace(b'"blobType"', b'"clobType"')
                        .replace(b"hexBinary", b"string"))
            out = out[:bm.start()] + clob_def + b"\n  " + out[bm.start():]
    return out, n


def fixes_by_xsd(findings: list[dict]) -> dict[tuple[str, str], dict[int, str]]:
    """{(schema_folder, table_folder): {kolonneindeks: ny_type}} for rettbare kolonnefunn."""
    out: dict[tuple[str, str], dict[int, str]] = {}
    for f in findings:
        if f.get("fix_to") and f["kind"] == "mismatch":
            out.setdefault((f["schema"], f["table"]), {})[f["col_index"]] = f["fix_to"]
    return out


# ── Dato-/tidsstempelfasetter (T_6.3-1) ──────────────────────────────────────

# typenavn -> (base, minInclusive, maxExclusive) slik DBPTK krever dem
DATE_FACETS: dict[str, tuple[str, str, str]] = {
    "dateType":     ("xs:date",     "0001-01-01Z",
                                    "10000-01-01Z"),
    "dateTimeType": ("xs:dateTime", "0001-01-01T00:00:00.000000000Z",
                                    "10000-01-01T00:00:00.000000000Z"),
}
# xs:pattern DBPTK selv skriver — brukes kun når hele definisjonen mangler
_DATE_PATTERNS = {
    "dateType":     r"\d{4}-\d{2}-\d{2}Z?",
    "dateTimeType": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d*)Z?",
}

_XSD_SIMPLETYPE_RE = re.compile(
    rb'<(?P<p>[A-Za-z0-9_]+:)?simpleType\b[^>]*\bname\s*=\s*"(?P<name>[^"]+)"[^>]*>'
    rb'.*?</(?:[A-Za-z0-9_]+:)?simpleType\s*>', re.DOTALL)
_XSD_RESTRICTION_OPEN_RE = re.compile(
    rb'<(?P<p>[A-Za-z0-9_]+:)?restriction\b[^>]*>')
_XSD_FACET_RE = {
    facet: re.compile(rb'(<(?:[A-Za-z0-9_]+:)?' + facet.encode()
                      + rb'\b[^>]*\bvalue\s*=\s*")([^"]*)(")')
    for facet in ("minInclusive", "maxExclusive")
}
_YEAR_RE = re.compile(r"^(-?)(\d{4,})-")


def _facet_change_is_lossless(facet: str, found: str | None) -> bool:
    """
    Sann når endring til DBPTK-verdien ikke gjør noen dato i år 0001–9999
    ugyldig: manglende fasett (SQL:2008 tillater uansett ikke år utenfor),
    minInclusive i år ≥ 0001, maxExclusive i år ≤ 9999 eller lik kravet.
    """
    if not found:
        return True
    m = _YEAR_RE.match(found.strip())
    if not m:
        return True                      # uparselig verdi — kravet er strengere definert
    neg, year = m.group(1) == "-", int(m.group(2))
    if facet == "minInclusive":
        return not neg and year >= 1
    return not neg and year <= 9999      # maxExclusive; ≥ 10000 ville innsnevre


def check_date_facets(xsd_bytes: bytes) -> list[dict]:
    """
    Funn for dateType/dateTimeType i en tableM.xsd (T_6.3-1):
      kind 'date_facet'   — fasett mangler/avviker: facet, found, expected, fix_to
      kind 'date_base'    — restriction har feil base (rapporteres, rettes ikke)
      kind 'date_missing' — kolonner bruker typen men definisjonen mangler
    """
    used = set(xsd_column_types(xsd_bytes).values())
    defs: dict[str, bytes] = {}
    for m in _XSD_SIMPLETYPE_RE.finditer(xsd_bytes):
        defs[m.group("name").decode("utf-8", "replace")] = m.group(0)

    findings: list[dict] = []
    for tname, (base, exp_min, exp_max) in DATE_FACETS.items():
        block = defs.get(tname)
        if block is None:
            if tname in used:
                findings.append({"kind": "date_missing", "type_name": tname,
                                 "facet": "", "found": None, "expected": "",
                                 "fix_to": "insert"})
            continue
        ro = _XSD_RESTRICTION_OPEN_RE.search(block)
        base_m = re.search(rb'\bbase\s*=\s*"([^"]*)"', ro.group(0)) if ro else None
        found_base = base_m.group(1).decode("utf-8", "replace") if base_m else None
        if found_base != base:
            findings.append({"kind": "date_base", "type_name": tname, "facet": "base",
                             "found": found_base, "expected": base, "fix_to": None})
            continue
        for facet, expected in (("minInclusive", exp_min), ("maxExclusive", exp_max)):
            fm = _XSD_FACET_RE[facet].search(block)
            found = fm.group(2).decode("utf-8", "replace") if fm else None
            if found == expected:
                continue
            findings.append({
                "kind": "date_facet", "type_name": tname, "facet": facet,
                "found": found, "expected": expected,
                "fix_to": expected if _facet_change_is_lossless(facet, found) else None,
            })
    return findings


def _canonical_date_simpletype(tname: str, prefix: bytes, indent: bytes) -> bytes:
    base, mn, mx = DATE_FACETS[tname]
    p = prefix
    i1, i2 = indent, indent * 2
    return (indent + b"<" + p + b'simpleType name="' + tname.encode() + b'">\n'
            + i1 + indent + b"<" + p + b'restriction base="' + base.encode() + b'">\n'
            + i2 + indent + b"<" + p + b'minInclusive value="' + mn.encode() + b'"/>\n'
            + i2 + indent + b"<" + p + b'maxExclusive value="' + mx.encode() + b'"/>\n'
            + i2 + indent + b"<" + p + b'pattern value="' + _DATE_PATTERNS[tname].encode() + b'"/>\n'
            + i1 + indent + b"</" + p + b"restriction>\n"
            + indent + b"</" + p + b"simpleType>\n")


def patch_date_facets(xsd_bytes: bytes, findings: list[dict]) -> tuple[bytes, int]:
    """
    Skriv DBPTK-verdiene for rettbare 'date_facet'-funn og legg inn manglende
    definisjoner ('date_missing'). Byte-nivå; xs:pattern og øvrig innhold
    røres ikke. Returnerer (nye_bytes, antall_endringer).
    """
    n = 0
    out = xsd_bytes
    by_type: dict[str, list[dict]] = {}
    for f in findings:
        if f.get("fix_to") and f["kind"] in ("date_facet", "date_missing"):
            by_type.setdefault(f["type_name"], []).append(f)
    if not by_type:
        return out, 0

    # Prefiks/innrykk hentes fra dokumentet selv
    any_st = _XSD_SIMPLETYPE_RE.search(out)
    prefix = any_st.group("p") or b"" if any_st else b"xs:"
    lm = re.search(rb"\n([ \t]+)<" + re.escape(prefix) + rb"(?:simpleType|complexType)\b", out)
    indent = lm.group(1) if lm else b"  "

    for tname, tfindings in by_type.items():
        if any(f["kind"] == "date_missing" for f in tfindings):
            close = re.search(rb"</(?:[A-Za-z0-9_]+:)?schema\s*>", out)
            if close:
                out = (out[:close.start()]
                       + _canonical_date_simpletype(tname, prefix, indent)
                       + out[close.start():])
                n += 1
            continue

        def _fix_block(m: re.Match, tf=tfindings) -> bytes:
            nonlocal n
            block = m.group(0)
            ro = _XSD_RESTRICTION_OPEN_RE.search(block)
            if not ro:
                return block
            p = ro.group("p") or b""
            # innrykk for fasetter = innrykk til første eksisterende fasett/barn
            im = re.search(rb"\n([ \t]*)<" + re.escape(p) + rb"(?:minInclusive|maxExclusive|pattern|enumeration)\b",
                           block)
            f_indent = im.group(1) if im else indent * 2
            for f in tf:
                facet, value = f["facet"], f["fix_to"].encode()
                fre = _XSD_FACET_RE[facet]
                if fre.search(block):
                    block, k = fre.subn(lambda fm: fm.group(1) + value + fm.group(3), block, count=1)
                    n += k
                else:
                    ins = b"\n" + f_indent + b"<" + p + facet.encode() + b' value="' + value + b'"/>'
                    ro2 = _XSD_RESTRICTION_OPEN_RE.search(block)
                    block = block[:ro2.end()] + ins + block[ro2.end():]
                    n += 1
            return block

        def _sub(m: re.Match, tn=tname, fb=_fix_block) -> bytes:
            return fb(m) if m.group("name") == tn.encode() else m.group(0)
        out = _XSD_SIMPLETYPE_RE.sub(_sub, out)
    return out, n


def date_fixes_by_xsd(findings: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """{(schema_folder, table_folder): [rettbare datofunn]}."""
    out: dict[tuple[str, str], list[dict]] = {}
    for f in findings:
        if f.get("fix_to") and f["kind"] in ("date_facet", "date_missing"):
            out.setdefault((f["schema"], f["table"]), []).append(f)
    return out


def rule_code(f: dict) -> str:
    """DBPTK-regelkode for et funn."""
    return "T_6.3-1" if f["kind"].startswith("date_") else "P_4.3-3"


# ── Innganger: utpakket mappe og zip ─────────────────────────────────────────

def _find_meta_in_dir(extract_dir: Path) -> Path | None:
    for rel in _METADATA_PATHS:
        p = extract_dir / rel
        if p.exists():
            return p
    return None


def check_dir(extract_dir: Path) -> list[dict]:
    """Sammenlign i en utpakket SIARD-mappe."""
    extract_dir = Path(extract_dir)
    meta = _find_meta_in_dir(extract_dir)
    if meta is None:
        return []

    def _read(sf: str, tf: str) -> bytes | None:
        p = extract_dir / xsd_arc_path(sf, tf)
        return p.read_bytes() if p.exists() else None

    return compare_column_types(meta.read_bytes(), _read)


def apply_xsd_fixes(xsd: bytes, key: tuple[str, str],
                    findings: list[dict]) -> tuple[bytes, list[str]]:
    """
    Utfør alle rettbare funn for én tableM.xsd (kolonnetyper + datofasetter).
    Returnerer (nye_bytes, endringstekster).
    """
    cols  = fixes_by_xsd(findings).get(key, {})
    dates = date_fixes_by_xsd(findings).get(key, [])
    changes: list[str] = []
    if cols:
        xsd, n = patch_xsd_column_types(xsd, cols)
        if n:
            changes.extend(format_finding(f) for f in findings
                           if f["kind"] == "mismatch" and f.get("fix_to")
                           and (f["schema"], f["table"]) == key)
    if dates:
        xsd, n = patch_date_facets(xsd, dates)
        if n:
            changes.extend(format_finding(f) for f in dates)
    return xsd, changes


def xsds_with_fixes(findings: list[dict]) -> set[tuple[str, str]]:
    return set(fixes_by_xsd(findings)) | set(date_fixes_by_xsd(findings))


def fix_dir(extract_dir: Path, findings: list[dict]) -> list[str]:
    """Skriv rettbare funn til tableM.xsd på disk. Returnerer endringstekster."""
    extract_dir = Path(extract_dir)
    changes: list[str] = []
    for key in sorted(xsds_with_fixes(findings)):
        p = extract_dir / xsd_arc_path(*key)
        if not p.exists():
            continue
        data, ch = apply_xsd_fixes(p.read_bytes(), key, findings)
        if ch:
            p.write_bytes(data)
            changes.extend(ch)
    return changes


def check_zip(zf: zipfile.ZipFile) -> list[dict]:
    """Sammenlign i en åpen SIARD-zip."""
    name_lower = {n.lower(): n for n in zf.namelist()}
    meta_entry = next((name_lower[c] for c in _METADATA_PATHS if c in name_lower), None)
    if meta_entry is None:
        return []

    def _read(sf: str, tf: str) -> bytes | None:
        entry = name_lower.get(xsd_arc_path(sf, tf).lower())
        return zf.read(entry) if entry else None

    return compare_column_types(zf.read(meta_entry), _read)


def scan_xsd_type_issues(siard_path: Path) -> list[dict]:
    """
    Preflight-skann av en SIARD-fil. Returnerer funnene (tom liste ved
    lesefeil). Kalleren avgjør ut fra fix_to hvilke som kan rettes.
    """
    try:
        with zipfile.ZipFile(siard_path, "r") as zf:
            return check_zip(zf)
    except Exception:
        return []
