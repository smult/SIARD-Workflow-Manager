"""siard_workflow/core/siard_header.py

SIARD-headeren: feltene øverst i header/metadata.xml (<dbname>, <description>,
<archiver>, <dataOwner>, <dataOriginTimespan>, <archivalDate> …) — det som
står før <schemas>.

Lesing og skriving er tekstbasert og byte-bevarende (samme mønster som
metadata_quality_operation og date_span): bare de feltene som endres skrives
om, resten av fila (navnerom, prefiks, innrykk, kommentarer, schema-delen)
står urørt. Søket avgrenses til header-delen, slik at <description> i en
tabell eller kolonne aldri treffes.

  read_header(kilde)            -> dict | None   (mappe, metadata.xml eller .siard)
  read_header_bytes(bytes)      -> dict
  validate(tag, verdi)          -> feiltekst | None
  apply_header(bytes, verdier)  -> (nye bytes, endringer)
  apply_header_file(sti, verdier) -> endringer   (skriver bare ved endring)

Typer i SIARD 2.1/2.2-skjemaet (metadata.xsd): dbname, dataOwner og
dataOriginTimespan er mandatoryString (minst ett tegn); archivalDate er
xs:date (påkrevd); de andre er valgfrie xs:string. lobFolder (anyURI) og
messageDigest vises, men endres ikke her.
"""
from __future__ import annotations

import datetime as _dt
import os
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape as _xml_escape

_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")


@dataclass(frozen=True)
class HeaderField:
    tag: str
    label: str
    required: bool = False
    editable: bool = True
    multiline: bool = False
    hint: str = ""


# Rekkefølgen er skjemaets (xs:sequence) — brukes når et valgfritt felt som
# mangler skal settes inn. SIARD 1.0 har samme rekkefølge, men uten lobFolder.
FIELDS: tuple[HeaderField, ...] = (
    HeaderField("dbname", "Databasenavn", required=True),
    HeaderField("description", "Beskrivelse", multiline=True),
    HeaderField("archiver", "Arkivar"),
    HeaderField("archiverContact", "Arkivarens kontaktinfo"),
    HeaderField("dataOwner", "Dataeier", required=True),
    HeaderField("dataOriginTimespan", "Tidsrom for dataene", required=True,
                hint="f.eks. 2001-01-01 - 2019-12-31"),
    HeaderField("lobFolder", "LOB-mappe", editable=False),
    HeaderField("producerApplication", "Produsert av (program)"),
    HeaderField("archivalDate", "Arkiveringsdato", required=True,
                hint="ÅÅÅÅ-MM-DD"),
    HeaderField("messageDigest", "Sjekksum (messageDigest)", editable=False),
    HeaderField("clientMachine", "Klientmaskin"),
    HeaderField("databaseProduct", "Databaseprodukt"),
    HeaderField("connection", "Tilkobling"),
    HeaderField("databaseUser", "Databasebruker"),
)
FIELD_BY_TAG = {f.tag: f for f in FIELDS}
EDITABLE_TAGS = tuple(f.tag for f in FIELDS if f.editable)

_RX_ROOT = re.compile(r"<(?:[\w.-]+:)?siardArchive\b[^>]*>", re.S)
_RX_VERSION = re.compile(r"""\bversion\s*=\s*["']([^"']*)["']""")
# Header-delen slutter der schema-delen (eller rot-elementet) begynner
_RX_REGION_END = re.compile(
    r"<(?:[\w.-]+:)?(?:schemas|users|roles|privileges)\b|</(?:[\w.-]+:)?siardArchive\b")
_RX_CHILD_TAG = re.compile(r"<[^>]*>")
# Kommentaren date_span skriver foran et estimert spenn
_ESTIMATE_COMMENT = re.compile(
    r"[ \t]*<!--(?:(?!-->).)*er estimert av SIARD Workflow Manager(?:(?!-->).)*-->"
    r"[ \t]*\r?\n?", re.S)

_ELEM_CACHE: dict[str, re.Pattern] = {}


def _elem_re(tag: str) -> re.Pattern:
    """(innrykk, prefiks, verdi) for <tag>…</tag> eller <tag/>."""
    rx = _ELEM_CACHE.get(tag)
    if rx is None:
        rx = re.compile(
            r"(?P<indent>[ \t]*)<(?P<p>(?:[\w.-]+:)?)" + tag + r"(?:\s[^>]*?)?"
            r"(?:/>|>(?P<val>.*?)</(?P=p)" + tag + r"\s*>)", re.S)
        _ELEM_CACHE[tag] = rx
    return rx


def _decode(raw: bytes) -> tuple[str, str]:
    try:
        return raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return raw.decode("latin-1"), "latin-1"


def _unescape(s: str) -> str:
    def _ref(m: re.Match) -> str:
        try:
            return chr(int(m[2], 16) if m[1] else int(m[2]))
        except (ValueError, OverflowError):
            return m[0]
    s = re.sub(r"&#(x?)([0-9a-fA-F]+);", _ref, s)
    return (s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
             .replace("&apos;", "'").replace("&amp;", "&"))


def _region(text: str) -> tuple[int, int, "re.Match | None"]:
    root = _RX_ROOT.search(text)
    start = root.end() if root else 0
    end_m = _RX_REGION_END.search(text, start)
    return start, (end_m.start() if end_m else len(text)), root


def _find(text: str, tag: str, start: int, end: int) -> "list[re.Match]":
    return list(_elem_re(tag).finditer(text, start, end))


def _value_of(m: re.Match) -> str:
    raw = m["val"] or ""
    if "<" in raw:                       # messageDigest (SIARD 2) har underelementer
        parts = [p.strip() for p in _RX_CHILD_TAG.split(raw)]
        return " ".join(_unescape(p) for p in parts if p)
    return _unescape(raw).strip()


def read_header_bytes(meta: bytes) -> dict:
    """
    {"values": {tag: tekst}, "present": {tag, …}, "version": "2.1"}.
    Felt som mangler i fila har tom verdi og står ikke i "present".
    """
    text, _codec = _decode(meta)
    start, end, root = _region(text)
    values: dict[str, str] = {}
    present: set[str] = set()
    for f in FIELDS:
        found = _find(text, f.tag, start, end)
        if found:
            present.add(f.tag)
            values[f.tag] = "; ".join(_value_of(m) for m in found) \
                if f.tag == "messageDigest" else _value_of(found[0])
        else:
            values[f.tag] = ""
    vm = _RX_VERSION.search(root.group(0)) if root else None
    return {"values": values, "present": present, "version": vm[1] if vm else ""}


def metadata_path(folder: Path) -> "Path | None":
    """header/metadata.xml i en utpakket SIARD-mappe (None når den mangler)."""
    for rel in _METADATA_PATHS:
        p = Path(folder) / rel
        if p.is_file():
            return p
    return None


def read_header(source: "Path | str") -> "dict | None":
    """Header fra en utpakket mappe, en metadata.xml eller en .siard-fil."""
    source = Path(source)
    try:
        if source.is_dir():
            p = metadata_path(source)
            return read_header_bytes(p.read_bytes()) if p else None
        if source.suffix.lower() == ".xml":
            return read_header_bytes(source.read_bytes())
        with zipfile.ZipFile(source) as z:
            names = set(z.namelist())
            for rel in _METADATA_PATHS:
                if rel in names:
                    return read_header_bytes(z.read(rel))
    except Exception:
        return None
    return None


_RX_XS_DATE = re.compile(r"^-?(\d{4,})-(\d{2})-(\d{2})(Z|[+-]\d{2}:\d{2})?$")


def validate(tag: str, value: str) -> "str | None":
    """Feiltekst (norsk) når verdien ikke er gyldig etter SIARD-skjemaet."""
    f = FIELD_BY_TAG.get(tag)
    if f is None or not f.editable:
        return None
    v = (value or "").strip()
    if f.required and not v:
        return "Påkrevd i SIARD — kan ikke være tomt"
    if tag == "archivalDate" and v:
        m = _RX_XS_DATE.match(v)
        if not m:
            return "Må være en dato på formen ÅÅÅÅ-MM-DD"
        try:
            _dt.date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return "Ugyldig dato"
    if "\x00" in v or re.search(r"[\x01-\x08\x0b\x0c\x0e-\x1f]", v):
        return "Inneholder kontrolltegn som ikke er tillatt i XML"
    return None


def _norm(v: str) -> str:
    return (v or "").replace("\r\n", "\n").strip()


def apply_header(meta: bytes, values: dict) -> "tuple[bytes, list[str]]":
    """
    Skriv nye verdier for header-feltene. Bare endrede, redigerbare og gyldige
    felt skrives; et tomt valgfritt felt fjernes, et manglende settes inn på
    skjemaets plass. Returnerer (bytes, endringer); bytes er uendret når ingen
    felt endres. Ugyldige verdier hoppes over (endringen nevner det).
    """
    text, codec = _decode(meta)
    nl = "\r\n" if "\r\n" in text else "\n"
    changes: list[str] = []
    for f in FIELDS:
        if not f.editable or f.tag not in values:
            continue
        new = _norm(values[f.tag])
        start, end, _root = _region(text)
        found = _find(text, f.tag, start, end)
        old = _value_of(found[0]) if found else ""
        if new == _norm(old):
            continue
        err = validate(f.tag, new)
        if err:
            changes.append(f"IKKE SKREVET <{f.tag}> «{new}»: {err}")
            continue
        esc = _xml_escape(new).replace("\n", nl)
        if found:
            m = found[0]
            if not new:
                # Tomt valgfritt felt → fjern elementet (med linjeskiftet etter)
                cut_end = m.end()
                tail = re.match(r"[ \t]*\r?\n", text[cut_end:])
                if tail:
                    cut_end += tail.end()
                text = text[:m.start()] + text[cut_end:]
                changes.append(f"<{f.tag}> fjernet (var «{old}»)")
                continue
            p = m["p"]
            elem = f"{m['indent']}<{p}{f.tag}>{esc}</{p}{f.tag}>"
            begin = m.start()
            if f.tag == "dataOriginTimespan":
                # Estimat-kommentaren (date_span) gjelder ikke en manuell verdi
                before = text[:begin]
                cm = None
                for cm in _ESTIMATE_COMMENT.finditer(before, max(0, begin - 2000)):
                    pass
                if cm is not None and cm.end() == begin:
                    begin = cm.start()
            text = text[:begin] + elem + text[m.end():]
            changes.append(f"<{f.tag}>: «{old}» → «{new}»")
            continue
        if not new:
            continue
        # Mangler: sett inn etter nærmeste foregående felt (ellers før neste)
        idx = FIELDS.index(f)
        anchor = None
        for prev in reversed(FIELDS[:idx]):
            hits = _find(text, prev.tag, start, end)
            if hits:
                anchor = ("after", hits[-1])
                break
        if anchor is None:
            for nxt in FIELDS[idx + 1:]:
                hits = _find(text, nxt.tag, start, end)
                if hits:
                    anchor = ("before", hits[0])
                    break
        if anchor is None:
            changes.append(f"IKKE SKREVET <{f.tag}>: fant ikke plass i header-delen")
            continue
        where, m = anchor
        p, ind = m["p"], m["indent"]
        elem = f"{ind}<{p}{f.tag}>{esc}</{p}{f.tag}>"
        if where == "after":
            text = text[:m.end()] + nl + elem + text[m.end():]
        else:
            text = text[:m.start()] + elem + nl + text[m.start():]
        changes.append(f"<{f.tag}> lagt til: «{new}»")
    if not any(not c.startswith("IKKE") for c in changes):
        return meta, changes
    return text.encode(codec, errors="xmlcharrefreplace"), changes


def apply_header_file(path: "Path | str", values: dict) -> "list[str]":
    """apply_header() på en metadata.xml på disk (skriver via temp-fil)."""
    path = Path(path)
    raw = path.read_bytes()
    new, changes = apply_header(raw, values)
    if new != raw:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(new)
        os.replace(tmp, path)
    return changes
