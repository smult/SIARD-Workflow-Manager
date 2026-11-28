"""
Tidspunkt uten tidssone helt inntil fasettgrensen (DBPTK T_6.0-2).

SIARD krever at ``dateTimeType`` i tableX.xsd har fasettene
``minInclusive 0001-01-01T00:00:00.000000000Z`` og
``maxExclusive 10000-01-01T00:00:00.000000000Z`` (T_6.3-1). XML Schema
sammenligner et tidspunkt UTEN tidssone med en grense MED tidssone som
±14 timer. Et tidspunkt uten tidssone fra og med ``9999-12-31T10:00:00`` kan
derfor ikke avgjøres mot øvre grense, og validatoren feiler med::

    cvc-maxExclusive-valid: Value '9999-12-31T23:59:59.9999999' is not
    facet-valid with respect to maxExclusive '10000-01-01T00:00:00Z'

Tilsvarende til og med ``0001-01-01T14:00:00`` mot nedre grense.

Slike verdier er nesten alltid «uendelig»-markører: sluttidspunktet i SQL
Servers systemversjonerte tabeller (ValidTo/EndTime, som alltid er UTC) eller
åpne sluttdatoer. Rettingen legger til ``Z`` på akkurat disse verdiene.
Sifrene endres ikke; verdien merkes som UTC. Alle andre tidspunkt står urørt.

Bare kolonner som tableX.xsd typer som tidspunkt (``dateTimeType`` /
``xs:dateTime``) behandles — en tekstkolonne med samme tegn røres aldri.
"""
from __future__ import annotations

import os
import re
import zipfile
from pathlib import Path
from typing import Callable, Iterator, Optional

from .column_xsd_types import (
    _find_meta_in_dir, _METADATA_PATHS, tables_from_metadata, xsd_arc_path,
    xsd_column_types,
)

CHUNK = 4 << 20
# En tidspunktcelle er godt under dette; brukes for å avgjøre at en lang
# celle ikke kan være en kandidat
_MAX_CELL = 512

_NEEDLES = (b"9999-12-31T", b"0001-01-01T")
_CELL_RE = re.compile(
    rb"(<c(\d+)>\s*)"
    rb"((9999-12-31|0001-01-01)T(\d\d):(\d\d):(\d\d)(?:\.(\d+))?)"
    rb"(\s*</c\2>)"
)
_DATETIME_TYPES = ("dateTimeType", "dateTime")


class Stopped(Exception):
    """Operatøren stoppet kjøringen."""


def is_indeterminate(value: str) -> bool:
    """
    Sann når et tidspunkt uten tidssone ikke kan avgjøres mot fasettgrensene
    (år 0001–9999) og derfor feiler XSD-valideringen.
    """
    m = re.fullmatch(
        r"(9999-12-31|0001-01-01)T(\d\d):(\d\d):(\d\d)(?:\.(\d+))?", value.strip())
    if not m:
        return False
    return _near_bound(m.group(1), int(m.group(2)), int(m.group(3)),
                       int(m.group(4)), m.group(5) or "")


def _near_bound(day, hh: int, mm: int, ss: int, frac) -> bool:
    if day in ("9999-12-31", b"9999-12-31"):
        # verdi + 14 t må være før 10000-01-01T00:00:00Z
        return (hh, mm, ss) >= (10, 0, 0)
    # verdi − 14 t må være etter 0001-01-01T00:00:00Z
    if (hh, mm, ss) < (14, 0, 0):
        return True
    return (hh, mm, ss) == (14, 0, 0) and not (frac and int(frac))


def datetime_columns(xsd_bytes: bytes) -> "set[int]":
    """Kolonneindekser (1-basert) som tableX.xsd typer som tidspunkt."""
    try:
        types = xsd_column_types(xsd_bytes)
    except Exception:
        return set()
    return {i for i, t in types.items()
            if t.rsplit(":", 1)[-1] in _DATETIME_TYPES}


def segments(read: Callable[[int], bytes], chunk: int = 0) -> Iterator[bytes]:
    """
    Del en tableX.xml i biter som aldri deler en celle ``<cN>…</cN>`` vi kan
    komme til å endre: det kuttes bare rett foran en ``<c``-start.
    Minnebruken er én blokk pluss den lengste cella.
    """
    chunk = chunk or CHUNK
    carry = b""
    while True:
        data = read(chunk)
        if not data:
            if carry:
                yield carry
            return
        buf = carry + data if carry else data
        p = buf.rfind(b"<c")
        if p > 0:
            yield buf[:p]
            carry = buf[p:]
            continue
        if p == 0 and len(buf) <= _MAX_CELL:
            carry = buf                 # for kort til å avgjøre — les mer
            continue
        # Ingen ny cellestart: enten midt i en lang celle (aldri en kandidat)
        # eller en hel kort celle fulgt av annet innhold. Alt kan slippes,
        # unntatt en avsluttende «<» som kan være starten på «<c».
        cut = len(buf) - 1 if buf.endswith(b"<") else len(buf)
        if cut:
            yield buf[:cut]
        carry = buf[cut:]


def _process(read, write, cols: "set[int]", counts: dict, examples: dict,
             on_bytes=None, stop=None) -> int:
    """Gå gjennom én tableX.xml. write=None: bare tell. Returnerer antall treff."""
    hits = 0

    def _sub(m: "re.Match") -> bytes:
        nonlocal hits
        ci = int(m.group(2))
        if ci not in cols or not _near_bound(
                m.group(4), int(m.group(5)), int(m.group(6)), int(m.group(7)),
                m.group(8) or b""):
            return m.group(0)
        hits += 1
        counts[ci] = counts.get(ci, 0) + 1
        examples.setdefault(ci, m.group(3).decode("ascii"))
        return m.group(1) + m.group(3) + b"Z" + m.group(9)

    for seg in segments(read):
        if stop is not None and stop():
            raise Stopped()
        if _NEEDLES[0] in seg or _NEEDLES[1] in seg:
            out = _CELL_RE.sub(_sub, seg)
        else:
            out = seg
        if write is not None:
            write(out)
        if on_bytes is not None:
            on_bytes(len(seg))
    return hits


def scan_table_xml(path: Path, cols: "set[int]", on_bytes=None,
                   stop=None) -> "tuple[dict, dict]":
    """({kolonne: antall}, {kolonne: eksempelverdi}) — fila endres ikke."""
    counts: dict = {}
    examples: dict = {}
    with open(path, "rb") as f:
        _process(f.read, None, cols, counts, examples, on_bytes, stop)
    return counts, examples


def fix_table_xml(path: Path, cols: "set[int]", on_bytes=None,
                  stop=None) -> "dict[int, int]":
    """
    Legg til ``Z`` på grenseverdiene i fila. Skrives til en midlertidig fil
    som byttes inn til slutt — ved feil eller stopp står originalen urørt.
    """
    path = Path(path)
    tmp = path.with_name(path.name + ".utc_tmp")
    counts: dict = {}
    try:
        with open(path, "rb") as src, open(tmp, "wb") as dst:
            _process(src.read, dst.write, cols, counts, {}, on_bytes, stop)
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    return counts


def fix_stream(read, write, cols: "set[int]", stop=None) -> "dict[int, int]":
    """Som fix_table_xml, men fra/til åpne strømmer (zip-oppføringer)."""
    counts: dict = {}
    _process(read, write, cols, counts, {}, None, stop)
    return counts


# ── Innganger: utpakket mappe og zip ─────────────────────────────────────────

def _table_xml_arc(schema_folder: str, table_folder: str) -> str:
    return f"content/{schema_folder}/{table_folder}/{table_folder}.xml"


def _finding(t: dict, ci: int, n: int, example: str) -> dict:
    name = next((c[1] for c in t["columns"] if c[0] == ci), f"c{ci}")
    return {"schema": t["schema_folder"], "table": t["table_folder"],
            "table_name": t["table_name"], "col_index": ci, "col_name": name,
            "count": n, "example": example}


def candidate_tables_dir(extract_dir: Path) -> "list[tuple[dict, Path, set, int]]":
    """[(tabell, tableX.xml, tidspunktkolonner, størrelse)] i en utpakket mappe."""
    extract_dir = Path(extract_dir)
    meta = _find_meta_in_dir(extract_dir)
    if meta is None:
        return []
    out = []
    for t in tables_from_metadata(meta.read_bytes()):
        sf, tf = t["schema_folder"], t["table_folder"]
        xsd = extract_dir / xsd_arc_path(sf, tf)
        xml = extract_dir / _table_xml_arc(sf, tf)
        if not xsd.exists() or not xml.exists():
            continue
        cols = datetime_columns(xsd.read_bytes())
        if cols:
            out.append((t, xml, cols, xml.stat().st_size))
    return out


def fix_dir(extract_dir: Path, on_bytes=None, stop=None,
            dry_run: bool = False, on_file=None) -> "list[dict]":
    """
    Finn (og, uten dry_run, rett) grenseverdier i en utpakket SIARD-mappe.
    Bare filer med treff skrives på nytt. Returnerer ett funn per kolonne.
    """
    findings: list[dict] = []
    for t, xml, cols, _size in candidate_tables_dir(extract_dir):
        counts, examples = scan_table_xml(xml, cols, on_bytes, stop)
        if on_file is not None:
            on_file()
        if not counts:
            continue
        if not dry_run:
            fix_table_xml(xml, cols, None, stop)
        for ci in sorted(counts):
            findings.append(_finding(t, ci, counts[ci], examples.get(ci, "")))
    return findings


def candidate_tables_zip(zf: zipfile.ZipFile) -> "list[tuple[dict, str, set, int]]":
    """[(tabell, oppføringsnavn for tableX.xml, tidspunktkolonner, størrelse)]."""
    name_lower = {n.lower(): n for n in zf.namelist()}
    meta_entry = next((name_lower[c] for c in _METADATA_PATHS if c in name_lower), None)
    if meta_entry is None:
        return []
    out = []
    for t in tables_from_metadata(zf.read(meta_entry)):
        sf, tf = t["schema_folder"], t["table_folder"]
        xsd = name_lower.get(xsd_arc_path(sf, tf).lower())
        xml = name_lower.get(_table_xml_arc(sf, tf).lower())
        if not xsd or not xml:
            continue
        cols = datetime_columns(zf.read(xsd))
        if cols:
            out.append((t, xml, cols, zf.getinfo(xml).file_size))
    return out


def scan_zip(zf: zipfile.ZipFile, on_bytes=None, stop=None,
             on_file=None) -> "tuple[list[dict], dict[str, set]]":
    """(funn per kolonne, {oppføringsnavn: tidspunktkolonner} for filer med treff)."""
    findings: list[dict] = []
    entries: dict = {}
    for t, entry, cols, _size in candidate_tables_zip(zf):
        counts: dict = {}
        examples: dict = {}
        with zf.open(entry) as f:
            _process(f.read, None, cols, counts, examples, on_bytes, stop)
        if on_file is not None:
            on_file()
        if not counts:
            continue
        entries[entry] = cols
        for ci in sorted(counts):
            findings.append(_finding(t, ci, counts[ci], examples.get(ci, "")))
    return findings, entries


def format_finding(f: dict) -> str:
    from .fmt import num
    return (f"{f['schema']}/{f['table']} {f['table_name']}.{f['col_name']} "
            f"(c{f['col_index']}): {num(f['count'])} verdi(er), f.eks. "
            f"«{f['example']}» → «{f['example']}Z»")


def total(findings: "Optional[list[dict]]") -> int:
    return sum(f["count"] for f in findings or [])
