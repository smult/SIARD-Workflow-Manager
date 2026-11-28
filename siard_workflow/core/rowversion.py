"""
siard_workflow/core/rowversion.py

Radversjonstellere lagret som BLOB.

SQL Server og Sybase har en kolonnetype som heter TIMESTAMP (i nyere SQL
Server også ROWVERSION). Den er IKKE et tidspunkt, men en teller på 8 byte
som databasen øker hver gang raden endres. Uttrekksverktøy (bl.a. Full
Convert) beskriver kolonnen slik i metadata.xml:

    <type>BLOB</type>
    <typeOriginal>TIMESTAMP</typeOriginal>

med verdien inline som hex i tableX.xml (<c7>000000000041C3A8</c7>).

Fordi kolonnen er en BLOB, dekodet «HEX Inline Extract» hver verdi til en egen
.bin-fil på 8 byte — én fil per rad i hver tabell med en slik kolonne. Filene
gikk videre til Siegfried og BLOB-konverteringen som uidentifiserte.

Løsningen er å si hva kolonnen er: en kort binærverdi. <type> settes til
BINARY(8) (VARBINARY(8) hvis noen verdier er kortere), kolonnens <lobFolder>
fjernes, og tableX.xsd får xs:hexBinary. Verdiene blir stående inline, uendret.
<typeOriginal> bevares. Endringen er tapsfri.

Regelen er bevisst smal: <typeOriginal> må være TIMESTAMP/ROWVERSION, og
innholdet må bekrefte det (bare hex, ingen verdi over 8 byte). Ekte tidspunkt
fra andre databaser har <type>TIMESTAMP</type>, ikke BLOB, og berøres ikke.
"""
from __future__ import annotations

import re
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

from siard_workflow.core.lob_column_types import (
    _COLUMN_BLOCK_RE, _SCHEMA_BLOCK_RE, _TABLE_BLOCK_RE, _folder_of,
    is_character_lob, is_lob_type, set_column_type,
)

ROWVERSION_BYTES = 8
_ORIGINAL_TYPES = frozenset(("TIMESTAMP", "ROWVERSION"))
_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")

# <lobFolder>…</lobFolder> eller <lobFolder/> på egen linje (med innrykk og linjeskift)
_LOB_FOLDER_LINE_RE = re.compile(
    rb"[ \t]*<(?P<p>(?:[A-Za-z0-9_-]+:)?)lobFolder(?:\s[^>]*)?"
    rb"(?:/>|>.*?</(?P=p)lobFolder\s*>)[ \t]*(?:\r?\n)?",
    re.DOTALL)


def is_rowversion_original(type_original: str) -> bool:
    """<typeOriginal> er SQL Server/Sybase TIMESTAMP eller ROWVERSION."""
    t = (type_original or "").strip().strip('"[]`').upper()
    return t.split("(", 1)[0].strip() in _ORIGINAL_TYPES


def is_candidate(sql_type: str, type_original: str) -> bool:
    """Binær LOB-kolonne som opprinnelig var en radversjonsteller."""
    return (is_lob_type(sql_type) and not is_character_lob(sql_type)
            and is_rowversion_original(type_original))


def binary_type_for(min_len: "int | None", max_len: int) -> str:
    """
    SIARD-typen kolonnen får: BINARY(8) når alle verdier er 8 byte (eller
    kolonnen er tom), VARBINARY(8) når noen er kortere.
    """
    if min_len is None or (min_len == max_len == ROWVERSION_BYTES):
        return f"BINARY({ROWVERSION_BYTES})"
    return f"VARBINARY({ROWVERSION_BYTES})"


def verdict(st: dict) -> "tuple[bool, str]":
    """
    Bekrefter innholdet at kolonnen er en radversjonsteller?
    st: {"hex", "nonhex", "file", "file_bad", "max_len", "min_len"} fra skanningen.
    Returnerer (ja/nei, begrunnelse når nei).
    """
    if st.get("nonhex"):
        return False, f"{st['nonhex']} verdi(er) er ikke hex"
    if st.get("max_len", 0) > ROWVERSION_BYTES:
        return False, (f"verdier over {ROWVERSION_BYTES} byte finnes "
                       f"(største {st['max_len']} byte)")
    if st.get("file_bad"):
        return False, (f"{st['file_bad']} filreferanse(r) peker på filer som "
                       f"mangler eller er over {ROWVERSION_BYTES} byte")
    return True, ""


def remove_column_lob_folder(meta: bytes, schema_folder: str, table_folder: str,
                             col_index: int) -> "tuple[bytes, bool]":
    """
    Fjern <lobFolder> fra kolonne `col_index` (1-basert) i angitt schema/tabell.
    Byte-nivå, som set_column_type: bevarer formatering og namespaces.
    """
    sfolder_b = schema_folder.encode("utf-8")
    tfolder_b = table_folder.encode("utf-8")
    hit = [False]

    def _in_schema(sm: re.Match) -> bytes:
        if _folder_of(sm.group(2)) != sfolder_b:
            return sm.group(0)

        def _in_table(tm: re.Match) -> bytes:
            if _folder_of(tm.group(2)) != tfolder_b:
                return tm.group(0)
            counter = [0]

            def _in_col(cm: re.Match) -> bytes:
                counter[0] += 1
                if counter[0] != col_index:
                    return cm.group(0)
                inner, n = _LOB_FOLDER_LINE_RE.subn(b"", cm.group(2), count=1)
                if n:
                    hit[0] = True
                return cm.group(1) + inner + cm.group(3)

            return (tm.group(1)
                    + _COLUMN_BLOCK_RE.sub(_in_col, tm.group(2))
                    + tm.group(3))

        return (sm.group(1)
                + _TABLE_BLOCK_RE.sub(_in_table, sm.group(2))
                + sm.group(3))

    return _SCHEMA_BLOCK_RE.sub(_in_schema, meta), hit[0]


def retype_in_metadata(meta: bytes, schema_folder: str, table_folder: str,
                       col_index: int, new_type: str) -> "tuple[bytes, bool]":
    """Sett <type> og fjern kolonnens <lobFolder>. (<typeOriginal> bevares.)"""
    meta, ok = set_column_type(meta, schema_folder, table_folder, col_index, new_type)
    if ok:
        meta, _removed = remove_column_lob_folder(meta, schema_folder,
                                                  table_folder, col_index)
    return meta, ok


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def find_rowversion_columns(meta_bytes: bytes) -> "list[dict]":
    """
    Binære LOB-kolonner med <typeOriginal> TIMESTAMP/ROWVERSION i metadata.xml:
    [{schema_folder, folder, table, col_index, col_name, type, type_original, rows}].
    (Bare metadata — innholdet kontrolleres når «HEX Inline Extract» kjører.)
    """
    try:
        root = ET.fromstring(meta_bytes)
    except ET.ParseError:
        return []

    def _txt(node, name: str) -> str:
        for ch in node:
            if _local(ch.tag) == name:
                return (ch.text or "").strip()
        return ""

    def _kids(node, name: str):
        return [ch for ch in node if _local(ch.tag) == name]

    found: list = []
    for schemas in _kids(root, "schemas"):
        for si, schema in enumerate(_kids(schemas, "schema")):
            sfolder = _txt(schema, "folder") or f"schema{si}"
            for tables in _kids(schema, "tables"):
                for table in _kids(tables, "table"):
                    try:
                        rows = int(_txt(table, "rows") or 0)
                    except ValueError:
                        rows = 0
                    for columns in _kids(table, "columns"):
                        for ci, col in enumerate(_kids(columns, "column"), 1):
                            typ, orig = _txt(col, "type"), _txt(col, "typeOriginal")
                            if is_candidate(typ, orig):
                                found.append({
                                    "schema_folder": sfolder,
                                    "folder": _txt(table, "folder"),
                                    "table": _txt(table, "name"),
                                    "col_index": ci,
                                    "col_name": _txt(col, "name") or f"c{ci}",
                                    "type": typ, "type_original": orig,
                                    "rows": rows})
    return found


def read_metadata_bytes(source: Path) -> "bytes | None":
    """metadata.xml fra en utpakket mappe eller en .siard-fil (None ved feil)."""
    source = Path(source)
    try:
        if source.is_dir():
            for rel in _METADATA_PATHS:
                p = source / rel
                if p.exists():
                    return p.read_bytes()
            return None
        from siard_workflow.core.zip_progress import read_member_fast
        hit = read_member_fast(source, _METADATA_PATHS)
        if hit is not None:
            return hit[1]
        with zipfile.ZipFile(source) as zf:
            names = {n.lower(): n for n in zf.namelist()}
            for rel in _METADATA_PATHS:
                if rel in names:
                    return zf.read(names[rel])
    except Exception:
        return None
    return None


def summarize(cols: "list[dict]") -> str:
    """
    «3 kolonne(r) i 3 tabell(er), 1 234 567 verdier» — til preflight og logg.
    Verdier = rader per kolonne lagt sammen: så mange filer ville det blitt.
    """
    from siard_workflow.core.fmt import num
    tables = {(c["schema_folder"], c["folder"]) for c in cols}
    values = sum(c.get("rows", 0) for c in cols)
    return (f"{num(len(cols))} kolonne(r) i {num(len(tables))} tabell(er)"
            + (f", {num(values)} verdier" if values else ""))
