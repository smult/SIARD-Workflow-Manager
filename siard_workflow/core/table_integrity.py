"""siard_workflow/core/table_integrity.py

Er tabellfilene hele?

En tableX.xml som er kuttet — avbrutt eksport hos produsenten, full disk under
utpakking, eller et avbrudd mens fila ble skrevet — mangler avsluttende
</table>. Da mangler det som regel rader, og steg som leser fila rad for rad
stopper med «no element found». Her kontrolleres bare slutten av hver fil:

  * utpakket mappe: de siste 64 kB leses (millisekunder per tabell)
  * SIARD-fil (zip): ukomprimerte oppføringer leses bakfra; komprimerte må
    pakkes ut i minnet fra start til slutt — det tar tid for store tabeller,
    så det gjøres i flere tråder og med fremdrift. CRC kontrolleres samtidig.

Programmet bruker mappe-kontrollen: rett etter «Pakk ut SIARD», og i
forhåndskontrollen når en kjøring gjenopptas i temp-mappa. check_zip brukes
ikke av arbeidsflyten (å lese SIARD-fila på forhånd ble valgt bort som
unødvendig ved ny kjøring), men kan kalles for å kontrollere en fil direkte.

Ingenting skrives.
"""
from __future__ import annotations

import concurrent.futures
import io
import os
import re
import struct
import xml.etree.ElementTree as ET
import zipfile
import zlib
from pathlib import Path
from typing import Callable, Optional

TAIL = 64 << 10                  # så mye av slutten leses
_READ = 1 << 20                  # komprimerte byte per lesing (zip)
_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")

_TRAIL = rb"\s*(?:(?:<!--.*?-->|<\?.*?\?>)\s*)*\Z"
_END_RE = re.compile(rb"</(?:[\w.\-]+:)?table\s*>" + _TRAIL, re.S)
_EMPTY_RE = re.compile(rb"<(?:[\w.\-]+:)?table\b[^<>]*/>" + _TRAIL, re.S)

CUT, EMPTY, MISSING, DAMAGED = "cut", "empty", "missing", "damaged"
_KIND_TEXT = {
    CUT:     "slutter uten </table> — fila er avkuttet eller ufullstendig",
    EMPTY:   "fila er tom (0 byte)",
    MISSING: "tabellfila mangler",
    DAMAGED: "kan ikke leses",
}


class Stopped(Exception):
    """Kontrollen ble avbrutt."""


# ── Slutten av en fil ────────────────────────────────────────────────────────

def tail_complete(tail: bytes) -> bool:
    """True når byte-biten (slutten av en tableX.xml) ender med </table>."""
    if b"\x00" in tail and tail.count(b"\x00") * 3 >= len(tail):
        tail = tail.replace(b"\x00", b"")        # UTF-16: taggene er ASCII
    return bool(_END_RE.search(tail) or _EMPTY_RE.search(tail))


def read_tail(path, n: int = TAIL) -> bytes:
    with open(path, "rb") as f:
        size = f.seek(0, os.SEEK_END)
        f.seek(max(0, size - n))
        return f.read(n)


def file_complete(path) -> bool:
    """True når tabellfila på disk ender med </table>."""
    return tail_complete(read_tail(path))


# ── Tabellene i metadata.xml ─────────────────────────────────────────────────

def _local(tag) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _child(el, name: str):
    return next((c for c in el if _local(c.tag) == name), None)


def _text(el, name: str) -> str:
    c = _child(el, name)
    return (c.text or "").strip() if c is not None else ""


def tables_in_metadata(meta_bytes: bytes) -> "list[dict]":
    """[{schema_folder, table_folder, table_name, rows}] — rows er None når
    <rows> mangler eller ikke er et tall."""
    root = ET.parse(io.BytesIO(meta_bytes)).getroot()
    out: list = []
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
            if not tfolder:
                continue
            try:
                rows = int(_text(table, "rows"))
            except ValueError:
                rows = None
            out.append({"schema_folder": sfolder, "table_folder": tfolder,
                        "table_name": _text(table, "name") or tfolder, "rows": rows})
    return out


def _arc(t: dict) -> str:
    return f"content/{t['schema_folder']}/{t['table_folder']}/{t['table_folder']}.xml"


def _finding(t: dict, kind: str, size: "int | None" = None, detail: str = "") -> dict:
    return {"schema": t["schema_folder"], "table": t["table_folder"],
            "table_name": t["table_name"], "rows": t["rows"], "kind": kind,
            "size": size, "detail": detail, "path": _arc(t)}


def format_finding(f: dict) -> str:
    """Én linje per funn: «schema0/table37 HKJOURDOC: slutter uten </table> …»."""
    from .fmt import num, size as _size
    text = _KIND_TEXT.get(f["kind"], f["kind"])
    if f.get("detail"):
        text += f" ({f['detail']})"
    extra = []
    if f.get("size"):
        extra.append(_size(f["size"]))
    if f.get("rows") is not None:
        extra.append(f"metadata.xml oppgir {num(f['rows'])} rad(er)")
    return (f"{f['schema']}/{f['table']} {f['table_name']}: {text}"
            + (f" [{'; '.join(extra)}]" if extra else ""))


# ── Utpakket mappe ───────────────────────────────────────────────────────────

def check_dir(extract_dir) -> dict:
    """
    Kontroller tabellfilene i en utpakket SIARD-mappe.
    Returnerer {"findings": [...], "checked": antall filer lest, "unchecked": 0}.
    En tabell uten fil er et funn bare når metadata.xml oppgir rader.
    """
    extract_dir = Path(extract_dir)
    meta = next((extract_dir / p for p in _METADATA_PATHS
                 if (extract_dir / p).is_file()), None)
    out = {"findings": [], "checked": 0, "unchecked": 0, "bytes": 0}
    if meta is None:
        return out
    for t in tables_in_metadata(meta.read_bytes()):
        xml = extract_dir / _arc(t)
        try:
            size = xml.stat().st_size
        except OSError:
            if (t["rows"] or 0) > 0:
                out["findings"].append(_finding(t, MISSING))
            continue
        out["checked"] += 1
        out["bytes"] += size
        if size == 0:
            out["findings"].append(_finding(t, EMPTY, 0))
            continue
        try:
            ok = file_complete(xml)
        except OSError as exc:
            out["findings"].append(_finding(t, DAMAGED, size, str(exc)))
            continue
        if not ok:
            out["findings"].append(_finding(t, CUT, size))
    return out


# ── SIARD-fil (zip) ──────────────────────────────────────────────────────────

def _entry_tail(fh, offset: int, csize: int, size: int, method: int, crc: int,
                add, stop) -> "tuple[str | None, str]":
    """(funn-type eller None, detalj) for én oppføring, lest rett fra fila."""
    fh.seek(offset)
    hdr = fh.read(30)
    if len(hdr) < 30 or hdr[:4] != b"PK\x03\x04":
        return DAMAGED, "ugyldig lokal filheader"
    n, m = struct.unpack_from("<HH", hdr, 26)
    start = offset + 30 + n + m
    if size == 0:
        return EMPTY, ""
    if method == zipfile.ZIP_STORED:
        fh.seek(start + max(0, csize - TAIL))
        tail = fh.read(min(TAIL, csize))
        add(csize)
        return (None, "") if tail_complete(tail) else (CUT, "")
    fh.seek(start)
    dec = zlib.decompressobj(-15)
    left, run_crc, tail = csize, 0, b""
    try:
        while left > 0:
            if stop and stop():
                raise Stopped()
            raw = fh.read(min(_READ, left))
            if not raw:
                return DAMAGED, "arkivet sluttet for tidlig"
            left -= len(raw)
            data = dec.decompress(raw)
            run_crc = zlib.crc32(data, run_crc)
            if data:
                tail = data[-TAIL:] if len(data) >= TAIL else (tail + data)[-TAIL:]
            add(len(raw))
        data = dec.flush()
        run_crc = zlib.crc32(data, run_crc)
        if data:
            tail = (tail + data)[-TAIL:]
    except zlib.error as exc:
        return DAMAGED, f"komprimerte data er skadet: {exc}"
    if run_crc & 0xFFFFFFFF != crc & 0xFFFFFFFF:
        return DAMAGED, "feil CRC-32"
    return (None, "") if tail_complete(tail) else (CUT, "")


def check_zip(zip_path, progress_factory: "Optional[Callable]" = None,
              stop: "Optional[Callable[[], bool]]" = None, workers: int = 4) -> dict:
    """
    Kontroller tabellfilene i en SIARD-fil uten å pakke den ut.
    progress_factory(n_filer, byte_totalt) gir et objekt med .add(nbytes, files=)
    og .finish() (f.eks. ByteProgress); byte telles komprimert (det som leses).
    Returnerer {"findings", "checked", "unchecked", "bytes"}; «unchecked» er
    oppføringer med kryptering eller en komprimeringsmetode som ikke støttes.
    """
    out = {"findings": [], "checked": 0, "unchecked": 0, "bytes": 0}
    jobs: list = []
    with zipfile.ZipFile(zip_path, "r", allowZip64=True) as zf:
        info = zf.NameToInfo
        meta_name = next((p for p in _METADATA_PATHS if p in info), None)
        if meta_name is None:
            lower = {n.lower(): n for n in info}
            meta_name = next((lower[p] for p in _METADATA_PATHS if p in lower), None)
        else:
            lower = None
        if meta_name is None:
            return out
        for t in tables_in_metadata(zf.read(meta_name)):
            arc = _arc(t)
            zi = info.get(arc)
            if zi is None:
                if lower is None:
                    lower = {n.lower(): n for n in info}
                zi = info.get(lower.get(arc.lower(), ""))
            if zi is None:
                if (t["rows"] or 0) > 0:
                    out["findings"].append(_finding(t, MISSING))
                continue
            if zi.flag_bits & 0x1 or zi.compress_type not in (zipfile.ZIP_STORED,
                                                              zipfile.ZIP_DEFLATED):
                out["unchecked"] += 1
                continue
            jobs.append((t, zi.header_offset, zi.compress_size, zi.file_size,
                         zi.compress_type, zi.CRC))
    total = sum(j[2] for j in jobs)
    out["bytes"] = total
    prog = progress_factory(len(jobs), total) if progress_factory else None

    def add(nbytes: int) -> None:
        if prog is not None:
            prog.add(nbytes)

    def run(job) -> "dict | None":
        t, offset, csize, size, method, crc = job
        if stop and stop():
            raise Stopped()
        try:
            with open(zip_path, "rb") as fh:
                kind, detail = _entry_tail(fh, offset, csize, size, method, crc, add, stop)
        except OSError as exc:
            kind, detail = DAMAGED, str(exc)
        if prog is not None:
            prog.add(files=1)
        return _finding(t, kind, size, detail) if kind else None

    # Store filer først, så trådene får jevnt med arbeid
    jobs.sort(key=lambda j: -j[2])
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers),
                                               thread_name_prefix="tabellsjekk") as pool:
        for f in pool.map(run, jobs):
            out["checked"] += 1
            if f:
                out["findings"].append(f)
    if prog is not None:
        prog.finish()
    out["findings"].sort(key=lambda f: (f["schema"], f["table"]))
    return out


# ── Tekst til logg og dialog ─────────────────────────────────────────────────

def summary_lines(findings: "list[dict]", limit: int = 12) -> "list[str]":
    lines = [f"  • {format_finding(f)}" for f in findings[:limit]]
    if len(findings) > limit:
        lines.append(f"  … og {len(findings) - limit} til")
    return lines
