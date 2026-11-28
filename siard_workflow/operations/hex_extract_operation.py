"""
siard_workflow/operations/hex_extract_operation.py

HexExtractOperation — identifiserer og eksporterer inline HEX-kodet CLOB-tekst
fra tableX.xml-filer i SIARD-arkiver til eksterne .txt-filer.

Logikk basert på referansescript (SIARD-Hex-convert.py):
  1. Les metadata.xml og finn tabeller med CLOB-kolonner.
  2. For hver tabell: stream tableX.xml med iterparse.
  3. For hver rad: HEX-dekod → UTF-8-tekst → skriv record{N}.txt
     i content/{schema}/{folder}/lob{col_index}/, patch <cN> med attributter.
     (Filnavn: recordN når dbptk_lob_names er på, ellers historisk xrecN.)

BLOB-kolonner (xs:hexBinary i tabell-XSD) forhåndsskannes per kolonne:
  * Dekoder ALLE inline hex-felt til tekst (UTF-8 eller cp1252) og kolonnen
    har ingen eksterne file=-referanser, inneholder kolonnen i realiteten
    tekst. Den behandles da som CLOB (kort tekst inline, lang tekst → .txt)
    og OMTYPES: <type> i metadata.xml → CLOB/NCLOB, og kolonnens
    type="blobType" i tableX.xsd → "clobType". Uten omtypingen ville
    dekodet tekst i en xs:hexBinary-celle gi XSD-brudd (DBPTK T_6.0-2).
  * Er kolonnen en radversjonsteller (<typeOriginal> TIMESTAMP/ROWVERSION fra
    SQL Server/Sybase — 8 byte, ikke et tidspunkt) og innholdet bekrefter det,
    OMTYPES den til BINARY(8)/VARBINARY(8): verdiene blir stående inline som
    hex, <lobFolder> fjernes og tableX.xsd får xs:hexBinary. Ingen filer lages
    (før: én .bin-fil på 8 byte per rad). Se core/rowversion.py.
  * Ellers (binært eller blandet innhold) beholdes kolonnen som BLOB, og ALLE
    hex-felt dekodes til .bin-filer uansett størrelse. Terskelen gjelder bare
    tekstkolonner: en BLOB-celle kan bare holde hex eller en filreferanse —
    dekodet tekst inline bryter xs:hexBinary (DBPTK T_6.0-2), og binærdata kan
    bare dekodes til fil.
  4. Skriv ny SIARD: kopier alt unntatt behandlede tableX.xml direkte,
     erstatt disse med patchet versjon.
"""
from __future__ import annotations
from siard_workflow.core.fmt import num as _fnum

import hashlib
import io
import os
import shutil
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from siard_workflow.core.atomic_write import replace_file
from siard_workflow.core.base_operation import BaseOperation
from siard_workflow.core.column_xsd_types import (
    patch_xsd_column_types, xsd_column_types,
)
from siard_workflow.core import rowversion as _rv
from siard_workflow.core.lob_column_types import (
    is_lob_type as _std_is_lob_type, is_character_lob, set_column_type,
    ref_file_path, resolve_lob_dir,
)
from siard_workflow.core.lob_naming import LobNameAllocator, lob_file_stem
from siard_workflow.core.siard_format import (
    detect_siard_version, siard_version_transform,
    get_target_siard_version, is_siard_xml,
    detect_folder_siard_version, rewrite_siardversion_path,
    extract_table_non_row_content,
)


# ── Hjelpefunksjoner ──────────────────────────────────────────────────────────

def _extract_xml_preamble(xml_bytes: bytes) -> bytes:
    """
    Returner alt fra starten av filen opp til og med avslutnings->
    for <table...>-åpningstaggen.

    Bevarer XML-deklarasjon, kommentarer og alle attributter/namespace-
    deklarasjoner i <table>-taggen slik de var i originalfilen.
    """
    idx = xml_bytes.find(b"<table")
    if idx == -1:
        return b'<?xml version="1.0" encoding="utf-8"?>\n<table>'
    end = xml_bytes.find(b">", idx)
    if end == -1:
        return b'<?xml version="1.0" encoding="utf-8"?>\n<table>'
    return xml_bytes[:end + 1]

def _strip_ns(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def _strip_ns_recursively(elem: ET.Element) -> None:
    elem.tag = _strip_ns(elem.tag)
    for child in elem:
        _strip_ns_recursively(child)


def _is_hex_string(s: str) -> bool:
    s = s.strip()
    if len(s) % 2 != 0 or len(s) < 2:
        return False
    try:
        bytes.fromhex(s)
        return True
    except Exception:
        return False


# Ikke-standard typenavn som forekommer i eldre uttrekk. Standardformene
# (CLOB/NCLOB/BLOB, «CHARACTER LARGE OBJECT», «BINARY LARGE OBJECT», med
# eller uten lengdeangivelse) gjenkjennes av lob_column_types.
_EXTRA_BINARY_LOB = {"NBLOB"}


def _base_type(type_str: str) -> str:
    """Typenavn uten lengdeangivelse: 'BLOB(1048576)' -> 'BLOB'."""
    return (type_str or "").strip().upper().split("(", 1)[0].strip()


def _is_lob_type(type_str: str) -> bool:
    return _std_is_lob_type(type_str) or _base_type(type_str) in _EXTRA_BINARY_LOB


def _is_blob_type(type_str: str) -> bool:
    """Binær LOB (xs:hexBinary i tabell-XSD)."""
    return _is_lob_type(type_str) and not is_character_lob(type_str)


def _character_type_for(old_type: str) -> str:
    """Tegn-LOB-typen en binær LOB-kolonne omtypes til: NBLOB → NCLOB, ellers CLOB."""
    return "NCLOB" if _base_type(old_type).startswith("N") else "CLOB"


def _hex_decode(text: str) -> tuple[bytes | None, str]:
    """
    Dekod hex-streng til bytes.
    Returnerer (decoded_bytes, ext) der ext er 'txt' hvis gyldig UTF-8, ellers 'bin'.
    Returnerer (None, '') hvis ikke gyldig hex.
    """
    if not _is_hex_string(text):
        return None, ""
    raw = bytes.fromhex(text.strip())
    try:
        raw.decode("utf-8")
        return raw, "txt"
    except UnicodeDecodeError:
        return raw, "bin"


def _md5_upper(data: bytes) -> str:
    return hashlib.md5(data).hexdigest().upper()


# XML 1.0 tillater ikke kontrolltegn 0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F
# (unntak: 0x09 tab, 0x0A LF, 0x0D CR).
import re as _re
_XML_INVALID_CTRL = _re.compile(
    r"[\x00-\x08\x0B\x0C\x0E-\x1F]"
)


def _hex_to_text(decoded: bytes) -> str | None:
    """
    Dekod bytes til tekst for inline-bruk i tableX.xml.
    Prøver UTF-8 først, så cp1252 (Windows-1252) som dekker norsk/vesteuropeisk.
    Filtrer XML-ulovlige kontrolltegn med replacement char.
    Returnerer None hvis dekoding feiler helt.
    """
    try:
        text = decoded.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = decoded.decode("cp1252", errors="replace")
        except Exception:
            return None
    # Erstatt XML-ulovlige kontrolltegn (sjelden, men kan finnes i CLOB)
    return _XML_INVALID_CTRL.sub("�", text)


# Tegn som gjør at hex-dekodede bytes IKKE regnes som tekst: XML-ulovlige
# kontrolltegn samt DEL og C1-kontrolltegn (U+007F–U+009F).
_TEXT_REJECT = _re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]")


def _decode_as_text(decoded: bytes) -> str | None:
    """
    Streng tekstgjenkjenning for hex-dekodede bytes.

    Gyldig UTF-8, ellers gyldig cp1252 (strict — 0x81/0x8D/0x8F/0x90/0x9D er
    udefinert), og uten kontrolltegn. Returnerer None for binært innhold.
    Brukes både til å avgjøre om en BLOB-kolonne egentlig inneholder tekst,
    og til å transkode cp1252-tekst til UTF-8 ved eksport til .txt.
    """
    if not decoded:
        return None
    text = None
    for enc in ("utf-8", "cp1252"):
        try:
            text = decoded.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None or _TEXT_REJECT.search(text):
        return None
    return text


def _scan_blob_columns(xml_src_bytes: bytes, table_info: dict,
                       blob_columns: set[int], w,
                       file_size=None) -> "tuple[set[int], dict[int, str]]":
    """
    Forhåndsskann BLOB-kolonnene i en tableX.xml. Returnerer
    (tekstkolonner, radversjonskolonner {kolonne: ny type}).

    Tekstkolonner inneholder i realiteten tekst: minst ett inline hex-felt,
    ALLE hex-felt dekoder til tekst (UTF-8/cp1252), og ingen eksterne
    file=-referanser. Disse omtypes BLOB → CLOB og behandles som
    tekstkolonner. Blandede kolonner (tekst OG binært) kan ikke beskrives av
    én kolonnetype og beholdes som BLOB — hex er der gyldig
    xs:hexBinary-innhold.

    Radversjonskolonner har <typeOriginal> TIMESTAMP/ROWVERSION og bare
    hex-verdier på høyst 8 byte. De omtypes til BINARY(8)/VARBINARY(8) og
    blir stående inline (core/rowversion.py). file_size(kolonne, referanse)
    → størrelse | None brukes for verdier som alt ligger i filer; uten den
    (ZIP-modus) omtypes ikke kolonner med filreferanser.
    """
    if not blob_columns:
        return set(), {}
    tag_to_col = {f"c{c}": c for c in blob_columns}
    st = {c: {"hex": 0, "text": 0, "bin": 0, "file": 0, "nonhex": 0,
              "file_bad": 0, "file_unread": 0, "max_len": 0, "min_len": None}
          for c in blob_columns}
    origs = table_info.get("column_orig_types", {})
    cand = {c for c in blob_columns if _rv.is_rowversion_original(origs.get(c, ""))}

    def _seen_len(s: dict, n: int) -> None:
        s["max_len"] = max(s["max_len"], n)
        s["min_len"] = n if s["min_len"] is None else min(s["min_len"], n)

    for _event, elem in ET.iterparse(io.BytesIO(xml_src_bytes), events=("end",)):
        if _strip_ns(elem.tag) != "row":
            continue
        for child in elem:
            col = tag_to_col.get(_strip_ns(child.tag).lower())
            if col is None:
                continue
            s = st[col]
            ref = child.get("file") or child.get("fileName")
            if ref:
                s["file"] += 1
                if col in cand:
                    if file_size is None:
                        s["file_unread"] += 1
                    else:
                        size = file_size(col, ref)
                        if size is None or size > _rv.ROWVERSION_BYTES:
                            s["file_bad"] += 1
                        else:
                            _seen_len(s, size)
                continue
            if not child.text or not child.text.strip():
                continue
            raw = child.text.strip()
            if not _is_hex_string(raw):
                s["nonhex"] += 1
                continue
            s["hex"] += 1
            if col in cand:
                _seen_len(s, len(raw) // 2)
            if _decode_as_text(bytes.fromhex(raw)) is not None:
                s["text"] += 1
            else:
                s["bin"] += 1
        elem.clear()

    names   = table_info.get("column_names", {})
    types   = table_info.get("column_types", {})
    retype: set[int] = set()
    rowver: dict[int, str] = {}
    for col in sorted(blob_columns):
        s     = st[col]
        label = f"c{col} ({names.get(col, '?')}, {types.get(col, 'BLOB')})"
        if col in cand:
            orig = origs.get(col, "").strip()
            ok, why = _rv.verdict(s)
            if ok and s["file_unread"]:
                ok, why = False, (f"{s['file_unread']} verdi(er) ligger i filer — de "
                                  f"leses bare inn igjen når steget kjører etter "
                                  f"«Pakk ut SIARD»")
            if ok:
                new_type = _rv.binary_type_for(s["min_len"], s["max_len"])
                rowver[col] = new_type
                n_vals = s["hex"] + s["file"]
                w(f"    {label}: radversjonsteller (opprinnelig {orig}), "
                  f"{_fnum(n_vals)} verdi(er) → omtypes til {new_type} og blir "
                  f"stående inline — ingen filer"
                  + (f"; {_fnum(s['file'])} småfil(er) leses inn igjen"
                     if s["file"] else ""), "info")
                continue
            w(f"    {label}: opprinnelig {orig}, men omtypes ikke til kort "
              f"binærtype — {why}", "warn")
        if s["hex"] == 0:
            continue   # ingen inline hex — ingenting å avgjøre
        if s["bin"] == 0 and s["file"] == 0:
            retype.add(col)
            w(f"    {label}: {s['hex']} hex-felt, alle tekst → omtypes til "
              f"{_character_type_for(types.get(col, 'BLOB'))} og dekodes", "info")
        elif s["text"] and (s["bin"] or s["file"]):
            w(f"    {label}: blandet innhold ({s['text']} tekst, {s['bin']} binær, "
              f"{s['file']} filreferanser) → beholdes BLOB; alle {s['hex']} hex-felt "
              f"dekodes til .bin-filer uansett størrelse", "warn")
        else:
            w(f"    {label}: {s['hex']} hex-felt, binært → beholdes BLOB; dekodes til "
              f".bin-filer uansett størrelse", "info")
    return retype, rowver


def _retype_entry(table_info: dict, col_index: int, new_type: str = "") -> dict:
    """new_type gitt → radversjonskolonne (kort binærtype), ellers tekst → tegn-LOB."""
    old = table_info.get("column_types", {}).get(col_index, "BLOB")
    return {
        "schema_folder": table_info["schema_folder"],
        "folder":        table_info["folder"],
        "table":         table_info["name"],
        "col_index":     col_index,
        "col_name":      table_info.get("column_names", {}).get(col_index, f"c{col_index}"),
        "old_type":      old,
        "new_type":      new_type or _character_type_for(old),
        "kind":          "rowversion" if new_type else "text",
    }


def _is_rowversion(r: dict) -> bool:
    return r.get("kind") == "rowversion"


def _xsd_type_for(r: dict) -> str:
    """Kolonnens type i tableX.xsd etter omtypingen."""
    return "xs:hexBinary" if _is_rowversion(r) else "clobType"


_LOG_ROWVERSION = 10    # radversjonskolonner som listes enkeltvis (kan være hundrevis)


def _log_retypes(retypes: list[dict], w) -> None:
    text = [r for r in retypes if not _is_rowversion(r)]
    rowv = [r for r in retypes if _is_rowversion(r)]
    if text:
        w(f"  Omtyper {len(text)} BLOB-kolonne(r) til tegn-LOB:", "step")
        for d in _retype_details(text):
            w(f"    {d}", "ok")
    if rowv:
        w(f"  Omtyper {len(rowv)} radversjonskolonne(r) (TIMESTAMP lagret som BLOB) "
          f"til kort binærtype — verdiene er uendret og står inline:", "step")
        for d in _retype_details(rowv[:_LOG_ROWVERSION]):
            w(f"    {d}", "ok")
        if len(rowv) > _LOG_ROWVERSION:
            w(f"    … og {len(rowv) - _LOG_ROWVERSION} kolonne(r) til", "info")


def _keep_rowversion_inline(elem, col_index: int, stats: dict, read_ref) -> None:
    """
    Radversjonskolonne: verdien blir stående inline som hex. En verdi som alt
    ligger i en fil (tidligere kjøring / uttrekksverktøyet) leses inn igjen.
    """
    tag = f"c{col_index}"
    for child in elem:
        if _strip_ns(child.tag).lower() != tag:
            continue
        ref = child.get("file") or child.get("fileName")
        if ref:
            data = read_ref(col_index, ref)
            child.attrib.clear()
            child.text = data.hex().upper()
            stats["rowversion_files_inlined"] = (
                stats.get("rowversion_files_inlined", 0) + 1)
        elif not (child.text and child.text.strip()):
            continue
        stats["rowversion_values"] = stats.get("rowversion_values", 0) + 1


def _retype_details(retypes: list[dict]) -> list[str]:
    return [f"{r['schema_folder']}/{r['folder']}.{r['col_name']} (c{r['col_index']}): "
            f"{r['old_type']} → {r['new_type']}" for r in retypes]


def _apply_retypes_meta(meta: bytes, retypes: list[dict], w) -> tuple[bytes, int]:
    """Sett <type> for omtypede kolonner i metadata.xml (<typeOriginal> bevares)."""
    n = 0
    for r in retypes:
        _set = _rv.retype_in_metadata if _is_rowversion(r) else set_column_type
        meta, ok = _set(meta, r["schema_folder"], r["folder"],
                        r["col_index"], r["new_type"])
        if ok:
            n += 1
        else:
            w(f"    [ADVARSEL] fant ikke <type> for {r['schema_folder']}/"
              f"{r['folder']} c{r['col_index']} i metadata.xml", "warn")
    return meta, n


def _patch_table_xsd_bytes(xsd: bytes, col_indices: set[int]) -> tuple[bytes, int]:
    """
    Skriv om type="blobType" → "clobType" for <xs:element name="cN"> i en
    tableX.xsd (felles implementasjon i core/column_xsd_types; avleder
    clobType-definisjonen fra blobType hvis den mangler).
    """
    return patch_xsd_column_types(xsd, _xsd_targets(xsd, col_indices))


_XSD_NS_PREFIX_RE = _re.compile(
    rb'xmlns:([A-Za-z0-9_]+)\s*=\s*"http://www\.w3\.org/2001/XMLSchema"')


def _xsd_targets(xsd: bytes, col_indices) -> dict:
    """
    {kolonne: ny XSD-type}. Et sett betyr clobType for alle. Innebygde typer
    («xs:hexBinary») får XSD-ens eget prefiks for XML Schema (xs, xsd …) —
    et prefiks som ikke er deklarert i fila ville gjort XSD-en ugyldig.
    """
    if not isinstance(col_indices, dict):
        return {c: "clobType" for c in col_indices}
    m = _XSD_NS_PREFIX_RE.search(xsd)
    prefix = m.group(1).decode("ascii") if m else "xs"
    return {c: (f"{prefix}:{t[3:]}" if t.startswith("xs:") else t)
            for c, t in col_indices.items()}


def _warn_xsd_not_patched(xsd: bytes, col_indices, name: str, w) -> None:
    """Advar om kolonner som ikke har fått ønsket type i XSD-en (finnes ikke der)."""
    want = _xsd_targets(xsd, col_indices)
    try:
        have = xsd_column_types(xsd)
    except Exception:
        have = {}
    bad = sorted(c for c, t in want.items() if have.get(c) != t)
    if bad:
        w(f"    [ADVARSEL] {name}: fant ikke "
          + ", ".join(f"c{c}" for c in bad)
          + " i XSD-en — kolonnetypen der er ikke oppdatert", "warn")


def _apply_retypes_fs(extract_dir: Path, retypes: list[dict],
                      dry_run: bool, w) -> None:
    """Pipeline-modus: patch metadata.xml og berørte tableX.xsd på disk."""
    if not retypes:
        return
    _log_retypes(retypes, w)
    if dry_run:
        return

    meta_path = extract_dir / "header" / "metadata.xml"
    meta, n = _apply_retypes_meta(meta_path.read_bytes(), retypes, w)
    if n:
        meta_path.write_bytes(meta)

    per_xsd: dict[Path, dict[int, str]] = {}
    for r in retypes:
        xsd = (extract_dir / "content" / r["schema_folder"] / r["folder"]
               / f"{r['folder']}.xsd")
        per_xsd.setdefault(xsd, {})[r["col_index"]] = _xsd_type_for(r)
    for xsd_path, cols in per_xsd.items():
        if not xsd_path.exists():
            w(f"    [ADVARSEL] {xsd_path.name} finnes ikke — XSD ikke oppdatert", "warn")
            continue
        data, k = _patch_table_xsd_bytes(xsd_path.read_bytes(), cols)
        if k:
            xsd_path.write_bytes(data)
        _warn_xsd_not_patched(data, cols, xsd_path.name, w)


def _parse_clob_tables_from_xml(xml_bytes: bytes) -> list[dict]:
    """
    Felles hjelpefunksjon: parser metadata.xml-bytes og returnerer
    liste av tabeller med CLOB-kolonner.
    """
    ns = {"ns": "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"}
    root = ET.parse(io.BytesIO(xml_bytes)).getroot()

    _db_lob_el = root.find("ns:lobFolder", ns)
    db_lob = (_db_lob_el.text or "").strip() if _db_lob_el is not None else ""

    tables = []
    schema_idx = 0
    for schema in root.findall("ns:schemas/ns:schema", ns):
        folder_el = schema.find("ns:folder", ns)
        schema_folder = folder_el.text if folder_el is not None else f"schema{schema_idx}"
        schema_idx += 1

        for table in schema.findall("ns:tables/ns:table", ns):
            name_el   = table.find("ns:name",    ns)
            tbl_fld   = table.find("ns:folder",  ns)
            cols_el   = table.find("ns:columns", ns)

            if name_el is None or tbl_fld is None or cols_el is None:
                continue

            clob_cols = []
            blob_cols = []
            col_types: dict[int, str] = {}
            col_names: dict[int, str] = {}
            col_origs: dict[int, str] = {}
            col_lobf:  dict[int, str] = {}
            col_idx   = 0
            for col in cols_el.findall("ns:column", ns):
                col_idx += 1
                type_el = col.find("ns:type", ns)
                cname_el = col.find("ns:name", ns)
                type_str = (type_el.text or "") if type_el is not None else ""
                if _is_lob_type(type_str):
                    clob_cols.append(col_idx)
                    col_types[col_idx] = type_str.strip()
                    col_names[col_idx] = ((cname_el.text or "").strip()
                                          if cname_el is not None else f"c{col_idx}")
                    _orig_el = col.find("ns:typeOriginal", ns)
                    _lobf_el = col.find("ns:lobFolder", ns)
                    col_origs[col_idx] = ((_orig_el.text or "").strip()
                                          if _orig_el is not None else "")
                    col_lobf[col_idx] = ((_lobf_el.text or "").strip()
                                         if _lobf_el is not None else "")
                    if _is_blob_type(type_str):
                        blob_cols.append(col_idx)

            if clob_cols:
                tables.append({
                    "name":          name_el.text,
                    "folder":        tbl_fld.text,
                    "schema_folder": schema_folder,
                    "clob_columns":  clob_cols,
                    "blob_columns":  blob_cols,
                    "column_types":  col_types,
                    "column_names":  col_names,
                    "column_orig_types":  col_origs,
                    "column_lob_folders": col_lobf,
                    "db_lob_folder":      db_lob,
                })
    return tables


def _find_clob_tables(zf: zipfile.ZipFile) -> list[dict]:
    """
    Les metadata.xml fra ZIP og returner liste av tabeller med CLOB-kolonner.
    Støtter alle skjemaer (schema0, schema1 ...).
    """
    metadata_path = next(
        (n for n in zf.namelist() if n.lower().endswith("header/metadata.xml")),
        None,
    )
    if not metadata_path:
        raise FileNotFoundError("metadata.xml ikke funnet i SIARD-arkivet")

    with zf.open(metadata_path) as f:
        xml_bytes = f.read()

    return _parse_clob_tables_from_xml(xml_bytes)


def _count_lob_files(extract_dir: Path, w=None, label: str = "") -> int:
    """
    Antall filer i lob-mapper (mappenavn med «lob») under extract_dir.

    os.walk henter filnavnene fra mappelistingen — ingen stat per fil. Med
    1,6 millioner LOB-filer tok rglob + is_file 27 minutter uten loggmeldinger;
    nå logges fremdrift hvert minutt.
    """
    import time as _time
    t0 = last = _time.monotonic()
    n = 0
    for dirpath, _dirs, files in os.walk(extract_dir):
        if "lob" in os.path.basename(dirpath).lower():
            n += len(files)
        now = _time.monotonic()
        if w and now - last >= 60:
            last = now
            w(f"    {label}: {_fnum(n)} LOB-filer telt så langt "
              f"({int(now - t0) // 60} min) …", "info")
    return n


def _find_clob_tables_fs(extract_dir: Path) -> list[dict]:
    """
    Les metadata.xml fra filsystemet (utpakket SIARD) og returner
    liste av tabeller med CLOB-kolonner.
    """
    # Prøv direkte sti først (uten rglob for Windows-kompatibilitet)
    metadata_path = extract_dir / "header" / "metadata.xml"
    if not metadata_path.exists():
        raise FileNotFoundError(
            f"metadata.xml ikke funnet i utpakket SIARD: {metadata_path}")
    return _parse_clob_tables_from_xml(metadata_path.read_bytes())


# Antall eksporterte felt per tabell som listes enkeltvis i loggen
_LOG_FILES = 3      # felt som listes enkeltvis per tabell (eksempler)


def _table_parse_error(xml_path: Path, table_name: str, exc) -> str:
    """Forståelig feiltekst når en tabellfil ikke lar seg lese som XML."""
    from siard_workflow.core.table_integrity import file_complete
    try:
        cut = not file_complete(xml_path)
    except OSError:
        cut = False
    if cut:
        return (f"Tabellfila {xml_path.name} ({table_name}) er ufullstendig: den slutter "
                f"uten </table> ({exc}). Rader mangler trolig. Fila er ikke endret. "
                f"Pakk ut SIARD-fila på nytt; er tabellfila ufullstendig også i "
                f"SIARD-fila, må uttrekket lages på nytt.")
    return (f"Tabellfila {xml_path.name} ({table_name}) er ikke gyldig XML: {exc}. "
            f"Fila er ikke endret.")


def _process_table_fs(
    extract_dir:     Path,
    table_info:      dict,
    w,
    stats:           dict,
    dry_run:         bool = False,
    min_text_length: int  = 200,
    retypes:         list | None = None,
) -> int:
    """
    Filesystem-variant av _process_table.
    Leser tableX.xml fra extract_dir, dekoder HEX CLOB-felt, skriver
    record{N}.txt til lob{N}/ og oppdaterer XML-filen på disk.
    BLOB-kolonner som viser seg å inneholde bare tekst legges til i
    `retypes` (omtypes av kalleren) og behandles som CLOB.
    Returnerer antall LOB-filer skrevet.
    """
    schema_folder = table_info["schema_folder"]
    folder        = table_info["folder"]
    clob_columns  = table_info["clob_columns"]
    blob_columns  = set(table_info.get("blob_columns", ()))
    table_name    = table_info["name"]

    xml_path = extract_dir / "content" / schema_folder / folder / f"{folder}.xml"

    # Radversjonskolonner: verdier som alt ligger i filer leses inn igjen
    table_dir = xml_path.parent
    _rv_dirs: dict = {}
    rv_delete: list = []         # filer som er lest inn i tabellfila (slettes etterpå)

    def _rv_lob_dir(col: int) -> Path:
        if col not in _rv_dirs:
            _rv_dirs[col] = resolve_lob_dir(
                extract_dir, table_info.get("db_lob_folder", ""),
                table_info.get("column_lob_folders", {}).get(col, ""),
                table_dir, col)
        return _rv_dirs[col]

    def _rv_size(col: int, ref: str) -> "int | None":
        try:
            return os.path.getsize(ref_file_path(extract_dir, _rv_lob_dir(col), ref))
        except OSError:
            return None

    def _rv_read(col: int, ref: str) -> bytes:
        p = ref_file_path(extract_dir, _rv_lob_dir(col), ref)
        data = p.read_bytes()
        rv_delete.append(p)
        return data
    w(f"  Prosesserer {table_name} ({folder}) — LOB-kolonner: "
      + ", ".join(f"c{c}" for c in clob_columns), "info")

    if not xml_path.exists():
        w(f"  [ADVARSEL] {xml_path} ikke funnet", "warn")
        return 0

    lob_written = 0
    row_counter = 0
    # Ledige filnavn: lob-mappa kan ha recordN-filer fra originalen (andre rader)
    alloc = LobNameAllocator(
        lambda d: [e.name for e in os.scandir(d) if e.is_file()] if os.path.isdir(d) else [])

    # Den nye tabellfila skrives ved siden av den gamle og byttes inn i ett
    # trinn til slutt — et avbrudd kan da aldri etterlate en halv tabellfil
    tmp_path = xml_path.with_name(xml_path.name + ".tmp_hex")

    try:
        # Les kilde-bytes én gang for header, pre/post-innhold og iterparse
        xml_src_bytes = xml_path.read_bytes()
        preamble = _extract_xml_preamble(xml_src_bytes)
        pre_content, post_content = extract_table_non_row_content(xml_src_bytes)

        # BLOB-kolonner med bare tekst → behandles som CLOB og omtypes.
        # Radversjonstellere → kort binærtype, verdiene blir stående inline.
        retype_cols, rowver = _scan_blob_columns(
            xml_src_bytes, table_info, blob_columns, w, _rv_size)
        blob_columns -= retype_cols
        if retypes is not None:
            retypes.extend(_retype_entry(table_info, c) for c in sorted(retype_cols))
            retypes.extend(_retype_entry(table_info, c, t)
                           for c, t in sorted(rowver.items()))

        with open(tmp_path, "wb") as out:
            out.write(preamble + b"\n")
            out.write(pre_content)   # bevarer <!--Row count: N--> o.l.

            context = ET.iterparse(io.BytesIO(xml_src_bytes), events=("start", "end"))
            for event, elem in context:
                if _strip_ns(elem.tag) != "row" or event != "end":
                    continue

                row_counter += 1

                for col_index in clob_columns:
                    if col_index in rowver:
                        _keep_rowversion_inline(elem, col_index, stats, _rv_read)
                        continue
                    target_tag = f"c{col_index}"
                    is_blob    = col_index in blob_columns
                    lob_dir    = (extract_dir / "content" / schema_folder
                                  / folder / f"lob{col_index}")

                    for child in elem:
                        if _strip_ns(child.tag).lower() != target_tag:
                            continue
                        if child.get("file") or child.get("fileName"):
                            continue
                        if not child.text or not child.text.strip():
                            continue

                        raw = child.text.strip()
                        decoded, ext = _hex_decode(raw)
                        if decoded is None:
                            continue
                        data_bytes = decoded
                        if is_blob:
                            ext = "bin"   # binær BLOB-kolonne → alltid .bin
                        elif ext != "txt":
                            # Ikke UTF-8: cp1252-tekst transkodes til UTF-8
                            # .txt (tegn-LOB skal ikke få .bin-filer); ellers
                            # binært innhold i tekstkolonne → .bin.
                            _t = _decode_as_text(decoded)
                            if _t is not None:
                                ext, data_bytes = "txt", _t.encode("utf-8")

                        try:
                            # Tekstfelt under minimum dekodet lengde — ikke eksporter
                            # til ekstern fil, men dekod hex-strengen INLINE slik
                            # at ferdig SIARD ikke inneholder rå hex.
                            if len(decoded) < min_text_length and not is_blob:
                                stats["hex_skipped"] = stats.get("hex_skipped", 0) + 1
                                _txt = _hex_to_text(decoded)
                                if _txt is not None:
                                    child.text = _txt
                                    stats["hex_inline_decoded"] = (
                                        stats.get("hex_inline_decoded", 0) + 1)
                                continue
                            if is_blob and len(decoded) < min_text_length:
                                # BLOB-celle: bare hex eller filreferanse er
                                # gyldig — dekodes derfor til fil uansett størrelse
                                stats["hex_blob_small_exported"] = (
                                    stats.get("hex_blob_small_exported", 0) + 1)

                            filename   = (f"{alloc.take(str(lob_dir), lob_file_stem(row_counter, 'xrec'))}"
                                          f".{ext}")
                            length     = len(data_bytes)
                            digest     = _md5_upper(data_bytes)

                            if not dry_run:
                                lob_dir.mkdir(parents=True, exist_ok=True)
                                (lob_dir / filename).write_bytes(data_bytes)

                            lob_written += 1
                            stats["hex_exported"] = stats.get("hex_exported", 0) + 1

                            child.text = None
                            child.attrib.clear()
                            child.set("file",       filename)
                            child.set("length",     str(length))
                            child.set("digestType", "MD5")
                            child.set("digest",     digest)

                            if lob_written <= _LOG_FILES:
                                w(f"    rad {row_counter}/{target_tag} → "
                                  f"lob{col_index}/{filename} ({_fnum(length)} bytes)", "info")

                        except Exception as exc:
                            w(f"  [FEIL] LOB rad {row_counter}/{target_tag}: {exc}",
                              "feil")

                _strip_ns_recursively(elem)
                out.write(ET.tostring(elem, encoding="utf-8"))
                elem.clear()

            out.write(post_content)  # bevarer <!--Finished at: ...-> o.l.
            out.write(b"</table>")

        if not dry_run:
            replace_file(tmp_path, xml_path)
            stats["tables_patched"] = stats.get("tables_patched", 0) + 1
            # Radversjonsverdier som lå i filer står nå inline i tabellfila —
            # filene (og en tom lob-mappe) er overflødige
            for _p in rv_delete:
                try:
                    _p.unlink()
                except OSError:
                    pass
            for _col in rowver:
                _d = _rv_lob_dir(_col)
                if _d != table_dir:
                    try:
                        os.rmdir(_d)          # bare hvis mappa er tom
                    except OSError:
                        pass

    except ET.ParseError as exc:
        raise RuntimeError(_table_parse_error(xml_path, table_name, exc)) from exc
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

    if lob_written > _LOG_FILES:
        w(f"    … og {_fnum(lob_written - _LOG_FILES)} felt til (ikke listet enkeltvis)", "info")
    if lob_written:
        w(f"  {table_name}: {lob_written} felt eksportert ({row_counter} rader)", "ok")
    else:
        w(f"    ingen HEX-felt ({row_counter} rader)", "info")
    return lob_written




def _process_table(
    zin:           zipfile.ZipFile,
    zout:          zipfile.ZipFile | None,
    table_info:    dict,
    written_files: set,
    w,
    stats:         dict,
    dry_run:       bool = False,
    min_text_length: int = 200,
    retypes:       list | None = None,
) -> int:
    """
    Stream tableX.xml, dekod HEX CLOB-felt, skriv record{N}.txt,
    patch XML og skriv til zout.  Returnerer antall LOB-filer skrevet.
    Tekstfelt kortere enn min_text_length tegn (etter dekoding) eksporteres
    ikke til ekstern fil, men hex-strengen dekodes INLINE slik at ferdig
    SIARD ikke inneholder rå hex. BLOB-kolonner med bare tekst legges til
    i `retypes` og behandles som CLOB; i øvrige BLOB-kolonner dekodes alle
    hex-felt til .bin-filer uansett størrelse.
    """
    schema_folder = table_info["schema_folder"]
    folder        = table_info["folder"]
    clob_columns  = table_info["clob_columns"]
    blob_columns  = set(table_info.get("blob_columns", ()))
    table_name    = table_info["name"]
    xml_arc_path  = f"content/{schema_folder}/{folder}/{folder}.xml"
    # Radversjonsverdier i filer leses bare inn igjen i pipeline-modus
    _rv_size = _rv_read = None

    w(f"  Prosesserer {table_name} ({folder}) — LOB-kolonner: "
      + ", ".join(f"c{c}" for c in clob_columns), "info")

    if xml_arc_path not in set(zin.namelist()):
        w(f"    [ADVARSEL] {xml_arc_path} ikke funnet", "warn")
        return 0

    lob_written = 0
    row_counter = 0
    # Ledige filnavn: lob-mappa kan ha recordN-filer fra originalen (andre rader)
    _all_names = zin.namelist()

    def _zip_names(d: str) -> list:
        pre = d if d.endswith("/") else d + "/"
        return [n[len(pre):] for n in _all_names
                if n.startswith(pre) and "/" not in n[len(pre):]]
    alloc = LobNameAllocator(_zip_names)

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".xml")
    os.close(tmp_fd)

    try:
        # Les ZIP-entry til bytes én gang: header, pre/post-innhold og iterparse
        with zin.open(xml_arc_path) as f_src:
            xml_src_bytes = f_src.read()
        preamble = _extract_xml_preamble(xml_src_bytes)
        pre_content, post_content = extract_table_non_row_content(xml_src_bytes)

        # BLOB-kolonner med bare tekst → behandles som CLOB og omtypes.
        # Radversjonstellere → kort binærtype, verdiene blir stående inline.
        retype_cols, rowver = _scan_blob_columns(
            xml_src_bytes, table_info, blob_columns, w, _rv_size)
        blob_columns -= retype_cols
        if retypes is not None:
            retypes.extend(_retype_entry(table_info, c) for c in sorted(retype_cols))
            retypes.extend(_retype_entry(table_info, c, t)
                           for c, t in sorted(rowver.items()))

        with open(tmp_path, "wb") as out:
            out.write(preamble + b"\n")
            out.write(pre_content)   # bevarer <!--Row count: N--> o.l.

            context = ET.iterparse(io.BytesIO(xml_src_bytes), events=("start", "end"))

            for event, elem in context:
                if _strip_ns(elem.tag) != "row" or event != "end":
                    continue

                row_counter += 1

                for col_index in clob_columns:
                    if col_index in rowver:
                        _keep_rowversion_inline(elem, col_index, stats, _rv_read)
                        continue
                    target_tag = f"c{col_index}"
                    is_blob    = col_index in blob_columns
                    lob_folder = (f"content/{schema_folder}/{folder}"
                                  f"/lob{col_index}/")

                    for child in elem:
                        if _strip_ns(child.tag).lower() != target_tag:
                            continue
                        if child.get("file") or child.get("fileName"):
                            continue
                        if not child.text or not child.text.strip():
                            continue

                        raw = child.text.strip()
                        decoded, ext = _hex_decode(raw)
                        if decoded is None:
                            continue
                        data_bytes = decoded
                        if is_blob:
                            ext = "bin"   # binær BLOB-kolonne → alltid .bin
                        elif ext != "txt":
                            # Ikke UTF-8: cp1252-tekst transkodes til UTF-8
                            # .txt (tegn-LOB skal ikke få .bin-filer); ellers
                            # binært innhold i tekstkolonne → .bin.
                            _t = _decode_as_text(decoded)
                            if _t is not None:
                                ext, data_bytes = "txt", _t.encode("utf-8")

                        try:
                            # Tekstfelt under minimum dekodet lengde — ikke eksporter
                            # til ekstern fil, men dekod hex-strengen INLINE slik
                            # at ferdig SIARD ikke inneholder rå hex.
                            if len(decoded) < min_text_length and not is_blob:
                                stats["hex_skipped"] = stats.get("hex_skipped", 0) + 1
                                _txt = _hex_to_text(decoded)
                                if _txt is not None:
                                    child.text = _txt
                                    stats["hex_inline_decoded"] = (
                                        stats.get("hex_inline_decoded", 0) + 1)
                                continue
                            if is_blob and len(decoded) < min_text_length:
                                # BLOB-celle: bare hex eller filreferanse er
                                # gyldig — dekodes derfor til fil uansett størrelse
                                stats["hex_blob_small_exported"] = (
                                    stats.get("hex_blob_small_exported", 0) + 1)
                            filename   = (f"{alloc.take(lob_folder, lob_file_stem(row_counter, 'xrec'))}"
                                          f".{ext}")
                            zip_path   = lob_folder + filename
                            length     = len(data_bytes)
                            digest     = _md5_upper(data_bytes)

                            if not dry_run and zout is not None:
                                zout.writestr(zip_path, data_bytes)
                                written_files.add(zip_path)

                            lob_written += 1
                            stats["hex_exported"] = stats.get("hex_exported", 0) + 1

                            child.text = None
                            child.attrib.clear()
                            child.set("file",       filename)
                            child.set("length",     str(length))
                            child.set("digestType", "MD5")
                            child.set("digest",     digest)

                            if lob_written <= _LOG_FILES:
                                w(f"    rad {row_counter}/{target_tag} → "
                                  f"{zip_path} ({_fnum(length)} bytes)", "info")

                        except Exception as exc:
                            w(f"    [FEIL] CLOB rad {row_counter}/{target_tag}:"
                              f" {exc}", "feil")

                _strip_ns_recursively(elem)
                out.write(ET.tostring(elem, encoding="utf-8"))
                elem.clear()

            out.write(post_content)  # bevarer <!--Finished at: ...-> o.l.
            out.write(b"</table>")

        if not dry_run and zout is not None:
            with open(tmp_path, "rb") as f:
                zout.writestr(xml_arc_path, f.read())
            written_files.add(xml_arc_path)
            stats["tables_patched"] = stats.get("tables_patched", 0) + 1

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    if lob_written > _LOG_FILES:
        w(f"    … og {_fnum(lob_written - _LOG_FILES)} felt til (ikke listet enkeltvis)", "info")
    if lob_written:
        w(f"  {table_name}: {lob_written} felt eksportert ({row_counter} rader)", "ok")
    else:
        w(f"    ingen HEX-felt ({row_counter} rader)", "info")
    return lob_written


# ── Operasjon ─────────────────────────────────────────────────────────────────

_STAT_LABELS = {
    "hex_exported":         "HEX-felt eksportert",
    "hex_skipped":          "HEX-felt under terskel",
    "hex_inline_decoded":   "HEX-felt dekodet inline",
    "hex_blob_small_exported": "BLOB-felt under terskel → fil",
    "columns_retyped":      "BLOB-kolonner omtypet",
    "rowversion_columns":   "Radversjonskolonner → BINARY",
    "rowversion_values":    "Radversjonsverdier (inline)",
    "rowversion_files_inlined": "Småfiler lest inn igjen",
    "tables_patched":       "Tabeller patchet",
    "lob_before":           "LOB-filer (før)",
    "lob_after":            "LOB-filer (etter)",
    "lob_diff":             "LOB-filer (endring)",
}


class HexExtractOperation(BaseOperation):
    """
    Dekoder inline HEX CLOB-tekst i tableX.xml og eksporterer til .txt-filer.
    Kjøres FØR BlobConvertOperation.
    """

    operation_id    = "hex_extract"
    label           = "HEX Inline Extract"
    category        = "Innhold"
    status          = 2
    produces_siard  = True
    requires_unpack = True
    modifies_content = True
    premis_event_type  = "Migration"
    premis_event_label = "hex-ekstraksjon"

    default_params = {
        "dry_run":         False,
        "temp_dir":        "",
        "min_text_length": 200,    # tekst kortere enn dette dekodes inline (ikke fil);
                                   # BLOB-felt (binært/blandet) dekodes alltid til fil
    }

    @property
    def description(self) -> str:
        return ("Dekoder all inline HEX i LOB-kolonner: kort tekst inline, lang "
                "tekst til .txt-filer. BLOB-kolonner med bare tekst omtypes til "
                "CLOB; i binære/blandede BLOB-kolonner dekodes alle felt til "
                ".bin-filer uansett størrelse. Radversjonstellere (TIMESTAMP/"
                "ROWVERSION lagret som BLOB) omtypes til BINARY(8) og blir "
                "stående inline. Kjøres før BLOB Konverter.")

    def premis_detail(self, result, ctx) -> str:
        """Ta med BLOB-kolonner som ble omtypet til tegn-LOB.

        Dekoding av hex-tekst i en xs:hexBinary-kolonne endrer kolonnens
        faktiske innhold fra binærdata til tegndata; typeendringen er en
        direkte følge av dekodingen og hører hjemme i samme event.
        """
        data  = result.data or {}
        parts = []
        if data.get("hex_exported"):
            parts.append(f"{data['hex_exported']} HEX-felt eksportert til LOB-filer")
        if data.get("hex_inline_decoded"):
            parts.append(f"{data['hex_inline_decoded']} HEX-felt dekodet inline")
        details = data.get("retyped_columns") or []
        if details:
            parts.append(
                f"{len(details)} BLOB-kolonne(r) omtypet til tegn-LOB fordi "
                f"alt innhold er tekst (<typeOriginal> bevart): "
                + "; ".join(details))
        rowv = data.get("rowversion_columns_retyped") or []
        if rowv:
            shown = rowv[:20]
            parts.append(
                f"{len(rowv)} radversjonskolonne(r) (TIMESTAMP/ROWVERSION fra "
                f"kildebasen, lagret som BLOB) omtypet til kort binærtype; "
                f"verdiene er uendret og står inline, <lobFolder> fjernet, "
                f"<typeOriginal> bevart: " + "; ".join(shown)
                + (f" … og {len(rowv) - len(shown)} til" if len(rowv) > len(shown) else ""))
            if data.get("rowversion_files_inlined"):
                parts.append(f"{data['rowversion_files_inlined']} radversjonsverdi(er) "
                             f"som lå i filer er lest inn igjen i tabellfila")
        return "; ".join(parts) or (result.message or "hex-ekstraksjon utført")

    def _note_retypes(self, retypes: list, stats: dict) -> list:
        """Før statistikk; returnerer beskrivelsene av tekst-omtypingene."""
        text = [r for r in retypes if not _is_rowversion(r)]
        rowv = [r for r in retypes if _is_rowversion(r)]
        if text:
            stats["columns_retyped"] = len(text)
        if rowv:
            stats["rowversion_columns"] = len(rowv)
        self._rowversion_details = _retype_details(rowv)
        return _retype_details(text)

    def _result_data(self, stats: dict, retyped: list, **extra) -> dict:
        return {**stats, **extra, "retyped_columns": retyped,
                "rowversion_columns_retyped": list(self._rowversion_details)}

    @staticmethod
    def _rowversion_note(stats: dict) -> str:
        n = stats.get("rowversion_columns", 0)
        return f", {n} radversjonskolonne(r) omtypet til kort binærtype" if n else ""

    def run(self, ctx) -> object:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg, lvl="info"):
            if log: log.log(msg, lvl)
            if pcb: pcb("log", msg=msg, level=lvl)

        def progress(event, **kw):
            if pcb: pcb(event, **kw)

        w("=" * 56)
        w("  HEX INLINE EXTRACT", "step")
        w("=" * 56)

        stats: dict = {"hex_exported": 0, "tables_patched": 0, "hex_skipped": 0}
        self._rowversion_details: list = []

        # ── Pipeline-modus: jobb direkte på utpakket filsystem ───────────────
        extract_dir = getattr(ctx, "extracted_path", None)
        if extract_dir and extract_dir.is_dir():
            w(f"  Pipeline-modus: bruker utpakket mappe {extract_dir}", "info")
            try:
                retyped = self._process_filesystem(extract_dir, stats, w, progress) or []
            except Exception as exc:
                import traceback as _tb
                w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
                progress("finish", stats=stats)
                return self._fail(str(exc), stats)

            w("  OPPSUMMERING:", "step")
            for k, v in stats.items():
                w(f"    {_STAT_LABELS.get(k, k):<28} {v}", "info")
            w("=" * 56)
            progress("finish", stats=stats)
            # Ingen ny SIARD — RepackSiard pakker sammen til slutt
            self.produces_siard = False
            return self._ok(
                self._result_data(stats, retyped),
                f"{stats['hex_exported']} HEX-felt eksportert (pipeline-modus)"
                + self._rowversion_note(stats))

        # ── Normal ZIP-modus ─────────────────────────────────────────────────
        self.produces_siard = True
        src_path = ctx.siard_path
        suffix   = "_hex_extracted"
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        c = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{c}" + src_path.suffix)
            c += 1

        try:
            retyped = self._process(ctx, src_path, dst_path, stats, w, progress) or []
        except Exception as exc:
            import traceback as _tb
            w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
            progress("finish", stats=stats)
            return self._fail(str(exc), stats)

        w("  OPPSUMMERING:", "step")
        for k, v in stats.items():
            w(f"    {_STAT_LABELS.get(k, k):<28} {v}", "info")
        if not self.params.get("dry_run"):
            w(f"    Ny SIARD: {dst_path}", "ok")
        w("=" * 56)
        progress("finish", stats=stats)
        return self._ok(
            self._result_data(stats, retyped, output_path=str(dst_path)),
            f"{stats['hex_exported']} HEX-felt eksportert, "
            f"{stats['tables_patched']} tabeller patchet"
            + self._rowversion_note(stats))

    def _process_filesystem(self, extract_dir: Path,
                            stats: dict, w, progress) -> list[str]:
        """
        Pipeline-modus: prosesser HEX CLOB direkte på utpakket filsystem.
        Endrer tableX.xml, LOB-filer, metadata.xml og tableX.xsd in-place.
        Returnerer beskrivelser av omtypede kolonner (for PREMIS).
        """
        dry_run         = bool(self.params.get("dry_run", False))
        min_text_length = max(0, int(self.params.get("min_text_length", 200)))
        PHASES = 3

        def phase(n, label):
            progress("phase", phase=n, total_phases=PHASES, label=label)

        phase(1, "Leser metadata — finner CLOB-tabeller")
        try:
            tables = _find_clob_tables_fs(extract_dir)
        except Exception as exc:
            raise RuntimeError(f"Kan ikke lese metadata: {exc}") from exc

        if not tables:
            w("  Ingen LOB-tabeller funnet.", "info")
            for _ in range(PHASES):
                progress("phase_done")
            return []

        # Tabellene listes én gang — med navn — når de behandles
        w(f"  Fant {len(tables)} tabell(er) med LOB-kolonner", "info")
        progress("phase_done")

        phase(2, "Prosesserer tabeller (filesystem)")
        w("  Teller LOB-filer før uttrekket (for kontroll etterpå) …", "info")
        lob_before = _count_lob_files(extract_dir, w, "Før")
        retypes: list[dict] = []
        for table_info in tables:
            _process_table_fs(extract_dir, table_info, w, stats,
                              dry_run=dry_run,
                              min_text_length=min_text_length,
                              retypes=retypes)
        # Omtyp BLOB-kolonner som bare inneholdt tekst, og radversjonstellere
        # (metadata.xml + .xsd)
        _apply_retypes_fs(extract_dir, retypes, dry_run, w)
        progress("phase_done")

        phase(3, "Validering")
        lob_after = _count_lob_files(extract_dir, w, "Etter")
        stats["lob_before"] = lob_before
        stats["lob_after"]  = lob_after
        stats["lob_diff"]   = lob_after - lob_before
        # Radversjonsverdier som lå i småfiler er lest inn igjen — de filene
        # skal være borte
        inlined = stats.get("rowversion_files_inlined", 0) if not dry_run else 0
        if lob_after >= lob_before - inlined:
            w(f"  LOB-validering OK: {lob_before} → {lob_after} "
              f"({lob_after - lob_before:+d})"
              + (f" — {inlined} radversjonsverdi(er) lest inn igjen fra småfiler"
                 if inlined else ""), "ok")
        else:
            w(f"  [ADVARSEL] LOB-antall gikk ned: {lob_before} → {lob_after}",
              "warn")
        progress("phase_done")
        return self._note_retypes(retypes, stats)

    def _process(self, ctx, src_path: Path, dst_path: Path,
                 stats: dict, w, progress) -> list[str]:

        dry_run          = bool(self.params.get("dry_run", False))
        min_text_length  = max(0, int(self.params.get("min_text_length", 200)))
        # Temp-mappe: global fra ctx, ellers self.params
        td = ""
        if hasattr(ctx, "metadata"):
            td = ctx.metadata.get("temp_dir", "")
        if not td:
            td = self.params.get("temp_dir", "").strip()
        PHASES  = 3

        def phase(n, label):
            progress("phase", phase=n, total_phases=PHASES, label=label)

        # ── Fase 1: Finn CLOB-tabeller ────────────────────────────────────────
        phase(1, "Leser metadata — finner CLOB-tabeller")
        try:
            zin = zipfile.ZipFile(src_path, "r", allowZip64=True)
        except Exception as exc:
            raise RuntimeError(f"Kan ikke åpne SIARD: {exc}") from exc

        with zin:
            # ── Versjondeteksjon ──────────────────────────────────────────────
            _meta_name = next(
                (n for n in zin.namelist()
                 if n.lower().endswith("header/metadata.xml")), None)
            src_version = "2.1"
            if _meta_name:
                try:
                    src_version = detect_siard_version(zin.read(_meta_name))
                except Exception:
                    pass
            target_version = get_target_siard_version()

            # Finn faktisk mappeversjon i header/siardversion/<x.y>/ direkte
            # fra ZIP-listen — uavhengig av XML-namespace-deteksjon.
            _folder_version = detect_folder_siard_version(
                zin.namelist(), src_version)
            if _folder_version != target_version:
                w(f"  Versjonsmarkør header/siardversion/: "
                  f"{_folder_version} → {target_version}", "info")

            w(f"  Kilde SIARD: {src_version}  →  "
              f"Mål SIARD: {target_version}", "info")

            try:
                tables = _find_clob_tables(zin)
            except FileNotFoundError as exc:
                raise RuntimeError(str(exc)) from exc

            if not tables:
                w("  Ingen LOB-tabeller funnet.", "info")
                for _ in range(1, PHASES + 1):
                    progress("phase_done")
                return []

            w(f"  Fant {len(tables)} tabell(er) med LOB-kolonner", "info")

            # Sett med tableX.xml som behandles av _process_table
            table_xml_paths: set[str] = {
                f"content/{t['schema_folder']}/{t['folder']}/{t['folder']}.xml"
                for t in tables
            }

            progress("phase_done")

            # ── Fase 2: Kopier + behandle ─────────────────────────────────────
            phase(2, "Prosesserer tabeller og skriver SIARD")

            written_files: set[str] = set()
            lob_before = sum(1 for f in zin.namelist() if "/lob" in f.lower())
            all_items  = zin.infolist()
            n_total    = len(all_items)
            REPORT     = max(1, n_total // 20)

            if dry_run:
                w("  Dry-run: skanner uten å skrive filer.", "info")
                zout = None
            else:
                zout = zipfile.ZipFile(dst_path, "w",
                                       zipfile.ZIP_DEFLATED,
                                       allowZip64=True)
            try:
                # Kopier alt unntatt behandlede tableX.xml.
                # Rename header/siardversion/<kilde>/ → header/siardversion/<mål>/
                # basert på faktisk mappenavn (ikke XML-namespace-innhold).
                def _ver_path_hex(name: str) -> str:
                    return rewrite_siardversion_path(name, target_version)

                # Behandle LOB-tabellene FØRST: avgjør hvilke BLOB-kolonner
                # som omtypes, slik at metadata.xml og tableX.xsd kan
                # patches når de kopieres under.
                retypes: list[dict] = []
                for table_info in tables:
                    _process_table(zin, zout, table_info,
                                   written_files, w, stats, dry_run,
                                   min_text_length=min_text_length,
                                   retypes=retypes)
                _log_retypes(retypes, w)
                xsd_patch: dict[str, dict[int, str]] = {}
                for r in retypes:
                    xsd_patch.setdefault(
                        f"content/{r['schema_folder']}/{r['folder']}/{r['folder']}.xsd",
                        {})[r["col_index"]] = _xsd_type_for(r)

                n_transformed = 0
                if not dry_run:
                    for n_done, item in enumerate(all_items, 1):
                        if item.filename in table_xml_paths:
                            pass  # skrives av _process_table
                        else:
                            try:
                                data = zin.read(item.filename)
                                if is_siard_xml(item.filename):
                                    data = siard_version_transform(
                                        data, target_version)
                                    n_transformed += 1
                                if retypes and item.filename == _meta_name:
                                    data, _n = _apply_retypes_meta(data, retypes, w)
                                elif item.filename in xsd_patch:
                                    cols = xsd_patch[item.filename]
                                    data, _k = _patch_table_xsd_bytes(data, cols)
                                    _warn_xsd_not_patched(data, cols,
                                                          item.filename, w)
                                ct   = (zipfile.ZIP_STORED
                                        if item.filename.lower().endswith(".bin")
                                        else zipfile.ZIP_DEFLATED)
                                out_name = _ver_path_hex(item.filename)
                                zout.writestr(out_name, data,
                                              compress_type=ct)
                                written_files.add(out_name)
                            except Exception as exc:
                                w(f"    [FEIL] Kopiering {item.filename}: "
                                  f"{exc}", "feil")
                        if n_done % REPORT == 0 or n_done == n_total:
                            progress("phase_progress",
                                     done=n_done, total=n_total)
                    if n_transformed:
                        w(f"  SIARD-versjon: {n_transformed} XML-filer "
                          f"transformert til versjon {target_version}", "info")
                    for xsd_name in xsd_patch:
                        if xsd_name not in written_files:
                            w(f"    [ADVARSEL] {xsd_name} finnes ikke i arkivet "
                              f"— XSD ikke oppdatert", "warn")

            finally:
                if zout is not None:
                    zout.close()

            progress("phase_done")

            # ── Fase 3: Validering ────────────────────────────────────────────
            phase(3, "Validering")

            if not dry_run and dst_path.exists():
                with zipfile.ZipFile(dst_path, "r") as zcheck:
                    lob_after = sum(
                        1 for f in zcheck.namelist() if "/lob" in f.lower())
                stats["lob_before"] = lob_before
                stats["lob_after"]  = lob_after
                stats["lob_diff"]   = lob_after - lob_before

                if lob_after >= lob_before:
                    w(f"  LOB-validering OK: {lob_before} → {lob_after} "
                      f"(+{lob_after - lob_before})", "ok")
                else:
                    w(f"  [ADVARSEL] LOB-antall gikk ned: "
                      f"{lob_before} → {lob_after}", "warn")

            progress("phase_done")
            return self._note_retypes(retypes, stats)
