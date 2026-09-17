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
  * Ellers (binært eller blandet innhold) beholdes kolonnen som BLOB: felt
    over terskelen eksporteres som .bin, felt under beholdes som hex (som er
    gyldig xs:hexBinary-innhold).
  4. Skriv ny SIARD: kopier alt unntatt behandlede tableX.xml direkte,
     erstatt disse med patchet versjon.
"""
from __future__ import annotations

import hashlib
import io
import os
import shutil
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation
from siard_workflow.core.column_xsd_types import patch_xsd_column_types
from siard_workflow.core.lob_column_types import (
    is_lob_type as _std_is_lob_type, is_character_lob, set_column_type,
)
from siard_workflow.core.lob_naming import lob_file_stem
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
                       blob_columns: set[int], w) -> set[int]:
    """
    Forhåndsskann BLOB-kolonnene i en tableX.xml og returner de som i
    realiteten inneholder tekst: minst ett inline hex-felt, ALLE hex-felt
    dekoder til tekst (UTF-8/cp1252), og ingen eksterne file=-referanser.

    Disse omtypes BLOB → CLOB og behandles som tekstkolonner. Blandede
    kolonner (tekst OG binært) kan ikke beskrives av én kolonnetype og
    beholdes som BLOB — hex er der gyldig xs:hexBinary-innhold.
    """
    if not blob_columns:
        return set()
    tag_to_col = {f"c{c}": c for c in blob_columns}
    st = {c: {"hex": 0, "text": 0, "bin": 0, "file": 0} for c in blob_columns}

    for _event, elem in ET.iterparse(io.BytesIO(xml_src_bytes), events=("end",)):
        if _strip_ns(elem.tag) != "row":
            continue
        for child in elem:
            col = tag_to_col.get(_strip_ns(child.tag).lower())
            if col is None:
                continue
            s = st[col]
            if child.get("file") or child.get("fileName"):
                s["file"] += 1
                continue
            if not child.text or not child.text.strip():
                continue
            raw = child.text.strip()
            if not _is_hex_string(raw):
                continue
            s["hex"] += 1
            if _decode_as_text(bytes.fromhex(raw)) is not None:
                s["text"] += 1
            else:
                s["bin"] += 1
        elem.clear()

    names   = table_info.get("column_names", {})
    types   = table_info.get("column_types", {})
    retype: set[int] = set()
    for col in sorted(blob_columns):
        s     = st[col]
        label = f"c{col} ({names.get(col, '?')}, {types.get(col, 'BLOB')})"
        if s["hex"] == 0:
            continue   # ingen inline hex — ingenting å avgjøre
        if s["bin"] == 0 and s["file"] == 0:
            retype.add(col)
            w(f"    {label}: {s['hex']} hex-felt, alle tekst → omtypes til "
              f"{_character_type_for(types.get(col, 'BLOB'))} og dekodes", "info")
        elif s["text"] and (s["bin"] or s["file"]):
            w(f"    {label}: blandet innhold ({s['text']} tekst, {s['bin']} binær, "
              f"{s['file']} filreferanser) → beholdes BLOB; tekstfelt under "
              f"terskel beholdes som hex", "warn")
        else:
            w(f"    {label}: {s['hex']} hex-felt, binært → beholdes BLOB", "info")
    return retype


def _retype_entry(table_info: dict, col_index: int) -> dict:
    old = table_info.get("column_types", {}).get(col_index, "BLOB")
    return {
        "schema_folder": table_info["schema_folder"],
        "folder":        table_info["folder"],
        "table":         table_info["name"],
        "col_index":     col_index,
        "col_name":      table_info.get("column_names", {}).get(col_index, f"c{col_index}"),
        "old_type":      old,
        "new_type":      _character_type_for(old),
    }


def _retype_details(retypes: list[dict]) -> list[str]:
    return [f"{r['schema_folder']}/{r['folder']}.{r['col_name']} (c{r['col_index']}): "
            f"{r['old_type']} → {r['new_type']}" for r in retypes]


def _apply_retypes_meta(meta: bytes, retypes: list[dict], w) -> tuple[bytes, int]:
    """Sett <type> for omtypede kolonner i metadata.xml (<typeOriginal> bevares)."""
    n = 0
    for r in retypes:
        meta, ok = set_column_type(meta, r["schema_folder"], r["folder"],
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
    return patch_xsd_column_types(xsd, {c: "clobType" for c in col_indices})


def _apply_retypes_fs(extract_dir: Path, retypes: list[dict],
                      dry_run: bool, w) -> None:
    """Pipeline-modus: patch metadata.xml og berørte tableX.xsd på disk."""
    if not retypes:
        return
    w(f"  Omtyper {len(retypes)} BLOB-kolonne(r) til tegn-LOB:", "step")
    for d in _retype_details(retypes):
        w(f"    {d}", "ok")
    if dry_run:
        return

    meta_path = extract_dir / "header" / "metadata.xml"
    meta, n = _apply_retypes_meta(meta_path.read_bytes(), retypes, w)
    if n:
        meta_path.write_bytes(meta)

    per_xsd: dict[Path, set[int]] = {}
    for r in retypes:
        xsd = (extract_dir / "content" / r["schema_folder"] / r["folder"]
               / f"{r['folder']}.xsd")
        per_xsd.setdefault(xsd, set()).add(r["col_index"])
    for xsd_path, cols in per_xsd.items():
        if not xsd_path.exists():
            w(f"    [ADVARSEL] {xsd_path.name} finnes ikke — XSD ikke oppdatert", "warn")
            continue
        data, k = _patch_table_xsd_bytes(xsd_path.read_bytes(), cols)
        if k:
            xsd_path.write_bytes(data)
        if k != len(cols):
            w(f"    [ADVARSEL] {xsd_path.name}: {k}/{len(cols)} kolonne(r) "
              f"hadde type=\"blobType\" å skrive om", "warn")


def _parse_clob_tables_from_xml(xml_bytes: bytes) -> list[dict]:
    """
    Felles hjelpefunksjon: parser metadata.xml-bytes og returnerer
    liste av tabeller med CLOB-kolonner.
    """
    ns = {"ns": "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"}
    root = ET.parse(io.BytesIO(xml_bytes)).getroot()

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
    w(f"  Prosesserer {table_name} — CLOB-kol: {clob_columns}", "info")

    if not xml_path.exists():
        w(f"  [ADVARSEL] {xml_path} ikke funnet", "warn")
        return 0

    lob_written = 0
    row_counter = 0

    tmp_fd, tmp_path_str = tempfile.mkstemp(suffix=".xml")
    os.close(tmp_fd)
    tmp_path = Path(tmp_path_str)

    try:
        # Les kilde-bytes én gang for header, pre/post-innhold og iterparse
        xml_src_bytes = xml_path.read_bytes()
        preamble = _extract_xml_preamble(xml_src_bytes)
        pre_content, post_content = extract_table_non_row_content(xml_src_bytes)

        # BLOB-kolonner med bare tekst → behandles som CLOB og omtypes
        retype_cols = _scan_blob_columns(xml_src_bytes, table_info, blob_columns, w)
        blob_columns -= retype_cols
        if retypes is not None:
            retypes.extend(_retype_entry(table_info, c) for c in sorted(retype_cols))

        with open(tmp_path, "wb") as out:
            out.write(preamble + b"\n")
            out.write(pre_content)   # bevarer <!--Row count: N--> o.l.

            context = ET.iterparse(io.BytesIO(xml_src_bytes), events=("start", "end"))
            for event, elem in context:
                if _strip_ns(elem.tag) != "row" or event != "end":
                    continue

                row_counter += 1

                for col_index in clob_columns:
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
                            # Felt under minimum dekodet lengde — ikke eksporter
                            # til ekstern fil, men dekod hex-strengen INLINE slik
                            # at ferdig SIARD ikke inneholder rå hex.
                            if len(decoded) < min_text_length:
                                stats["hex_skipped"] = stats.get("hex_skipped", 0) + 1
                                if is_blob:
                                    # Binær BLOB-kolonne: hex ER gyldig innhold.
                                    # Tekst her gir XSD-brudd (DBPTK T_6.0-2).
                                    stats["hex_blob_kept_inline"] = (
                                        stats.get("hex_blob_kept_inline", 0) + 1)
                                    continue
                                _txt = _hex_to_text(decoded)
                                if _txt is not None:
                                    child.text = _txt
                                    stats["hex_inline_decoded"] = (
                                        stats.get("hex_inline_decoded", 0) + 1)
                                continue

                            filename   = f"{lob_file_stem(row_counter, 'xrec')}.{ext}"
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

                            w(f"    rad {row_counter}/{target_tag} → "
                              f"lob{col_index}/{filename} ({length:,} bytes)", "info")

                        except Exception as exc:
                            w(f"  [FEIL] LOB rad {row_counter}/{target_tag}: {exc}",
                              "feil")

                _strip_ns_recursively(elem)
                out.write(ET.tostring(elem, encoding="utf-8"))
                elem.clear()

            out.write(post_content)  # bevarer <!--Finished at: ...-> o.l.
            out.write(b"</table>")

        if not dry_run:
            shutil.copy2(tmp_path, xml_path)
            stats["tables_patched"] = stats.get("tables_patched", 0) + 1

    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

    w(f"  {table_name}: {lob_written} felt eksportert ({row_counter} rader)",
      "ok" if lob_written else "info")
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
    Felt kortere enn min_text_length tegn (etter dekoding) eksporteres
    ikke til ekstern fil, men hex-strengen dekodes INLINE slik at ferdig
    SIARD ikke inneholder rå hex. BLOB-kolonner med bare tekst legges til
    i `retypes` og behandles som CLOB; øvrige BLOB-felt under terskelen
    beholdes som hex (gyldig xs:hexBinary).
    """
    schema_folder = table_info["schema_folder"]
    folder        = table_info["folder"]
    clob_columns  = table_info["clob_columns"]
    blob_columns  = set(table_info.get("blob_columns", ()))
    table_name    = table_info["name"]
    xml_arc_path  = f"content/{schema_folder}/{folder}/{folder}.xml"

    w(f"  Prosesserer {table_name} — CLOB-kol: {clob_columns}", "info")

    if xml_arc_path not in set(zin.namelist()):
        w(f"    [ADVARSEL] {xml_arc_path} ikke funnet", "warn")
        return 0

    lob_written = 0
    row_counter = 0

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".xml")
    os.close(tmp_fd)

    try:
        # Les ZIP-entry til bytes én gang: header, pre/post-innhold og iterparse
        with zin.open(xml_arc_path) as f_src:
            xml_src_bytes = f_src.read()
        preamble = _extract_xml_preamble(xml_src_bytes)
        pre_content, post_content = extract_table_non_row_content(xml_src_bytes)

        # BLOB-kolonner med bare tekst → behandles som CLOB og omtypes
        retype_cols = _scan_blob_columns(xml_src_bytes, table_info, blob_columns, w)
        blob_columns -= retype_cols
        if retypes is not None:
            retypes.extend(_retype_entry(table_info, c) for c in sorted(retype_cols))

        with open(tmp_path, "wb") as out:
            out.write(preamble + b"\n")
            out.write(pre_content)   # bevarer <!--Row count: N--> o.l.

            context = ET.iterparse(io.BytesIO(xml_src_bytes), events=("start", "end"))

            for event, elem in context:
                if _strip_ns(elem.tag) != "row" or event != "end":
                    continue

                row_counter += 1

                for col_index in clob_columns:
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
                            # Felt under minimum dekodet lengde — ikke eksporter
                            # til ekstern fil, men dekod hex-strengen INLINE slik
                            # at ferdig SIARD ikke inneholder rå hex.
                            if len(decoded) < min_text_length:
                                stats["hex_skipped"] = stats.get("hex_skipped", 0) + 1
                                if is_blob:
                                    # Binær BLOB-kolonne: hex ER gyldig innhold.
                                    # Tekst her gir XSD-brudd (DBPTK T_6.0-2).
                                    stats["hex_blob_kept_inline"] = (
                                        stats.get("hex_blob_kept_inline", 0) + 1)
                                    continue
                                _txt = _hex_to_text(decoded)
                                if _txt is not None:
                                    child.text = _txt
                                    stats["hex_inline_decoded"] = (
                                        stats.get("hex_inline_decoded", 0) + 1)
                                continue
                            filename   = f"{lob_file_stem(row_counter, 'xrec')}.{ext}"
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

                            w(f"    rad {row_counter}/{target_tag} → "
                              f"{zip_path} ({length:,} bytes)", "info")

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

    w(f"  {table_name}: {lob_written} felt eksportert "
      f"({row_counter} rader)", "ok" if lob_written else "info")
    return lob_written


# ── Operasjon ─────────────────────────────────────────────────────────────────

_STAT_LABELS = {
    "hex_exported":         "HEX-felt eksportert",
    "hex_skipped":          "HEX-felt under terskel",
    "hex_inline_decoded":   "HEX-felt dekodet inline",
    "hex_blob_kept_inline": "BLOB-felt beholdt som hex",
    "columns_retyped":      "BLOB-kolonner omtypet",
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
        "min_text_length": 200,    # tekst kortere enn dette dekodes inline (ikke fil)
    }

    @property
    def description(self) -> str:
        return ("Dekoder inline HEX CLOB-tekst i tableX.xml og eksporterer "
                "til eksterne .txt-filer. BLOB-kolonner med bare tekst "
                "omtypes til CLOB. Kjøres før BLOB Konverter.")

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
        return "; ".join(parts) or (result.message or "hex-ekstraksjon utført")

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
                {**stats, "retyped_columns": retyped},
                f"{stats['hex_exported']} HEX-felt eksportert (pipeline-modus)")

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
            {**stats, "output_path": str(dst_path), "retyped_columns": retyped},
            f"{stats['hex_exported']} HEX-felt eksportert, "
            f"{stats['tables_patched']} tabeller patchet")

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

        w(f"  Fant {len(tables)} tabell(er) med LOB-kolonner:", "info")
        for t in tables:
            w(f"    • {t['schema_folder']}/{t['folder']} "
              f"— kol {t['clob_columns']}", "info")
        progress("phase_done")

        phase(2, "Prosesserer tabeller (filesystem)")
        lob_before = sum(
            1 for f in extract_dir.rglob("*")
            if f.is_file() and "lob" in f.parent.name.lower())
        retypes: list[dict] = []
        for table_info in tables:
            _process_table_fs(extract_dir, table_info, w, stats,
                              dry_run=dry_run,
                              min_text_length=min_text_length,
                              retypes=retypes)
        # Omtyp BLOB-kolonner som bare inneholdt tekst (metadata.xml + .xsd)
        _apply_retypes_fs(extract_dir, retypes, dry_run, w)
        if retypes:
            stats["columns_retyped"] = len(retypes)
        progress("phase_done")

        phase(3, "Validering")
        lob_after = sum(
            1 for f in extract_dir.rglob("*")
            if f.is_file() and "lob" in f.parent.name.lower())
        stats["lob_before"] = lob_before
        stats["lob_after"]  = lob_after
        stats["lob_diff"]   = lob_after - lob_before
        if lob_after >= lob_before:
            w(f"  LOB-validering OK: {lob_before} → {lob_after} "
              f"(+{lob_after - lob_before})", "ok")
        else:
            w(f"  [ADVARSEL] LOB-antall gikk ned: {lob_before} → {lob_after}",
              "warn")
        progress("phase_done")
        return _retype_details(retypes)

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

            w(f"  Fant {len(tables)} tabell(er) med LOB-kolonner:", "info")
            for t in tables:
                w(f"    • {t['schema_folder']}/{t['folder']} "
                  f"— kol {t['clob_columns']}", "info")

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
                if retypes:
                    stats["columns_retyped"] = len(retypes)
                    w(f"  Omtyper {len(retypes)} BLOB-kolonne(r) til tegn-LOB:",
                      "step")
                    for d in _retype_details(retypes):
                        w(f"    {d}", "ok")
                xsd_patch: dict[str, set[int]] = {}
                for r in retypes:
                    xsd_patch.setdefault(
                        f"content/{r['schema_folder']}/{r['folder']}/{r['folder']}.xsd",
                        set()).add(r["col_index"])

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
                                    if _k != len(cols):
                                        w(f"    [ADVARSEL] {item.filename}: "
                                          f"{_k}/{len(cols)} kolonne(r) hadde "
                                          f"type=\"blobType\" å skrive om", "warn")
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
            return _retype_details(retypes)
