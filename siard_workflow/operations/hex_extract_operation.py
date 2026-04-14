"""
siard_workflow/operations/hex_extract_operation.py

HexExtractOperation — identifiserer og eksporterer inline HEX-kodet CLOB-tekst
fra tableX.xml-filer i SIARD-arkiver til eksterne .txt-filer.

Logikk basert på referansescript (SIARD-Hex-convert.py):
  1. Les metadata.xml og finn tabeller med CLOB-kolonner.
  2. For hver tabell: stream tableX.xml med iterparse.
  3. For hver rad: HEX-dekod → UTF-8-tekst → skriv xrec{N}.txt
     i content/{schema}/{folder}/lob{col_index}/, patch <cN> med attributter.
  4. Skriv ny SIARD: kopier alt unntatt behandlede tableX.xml direkte,
     erstatt disse med patchet versjon.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation


# ── Hjelpefunksjoner ──────────────────────────────────────────────────────────

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


def _hex_to_text(text: str) -> str:
    """Dekod hex-streng til UTF-8-tekst. Returnerer original ved feil."""
    if _is_hex_string(text):
        try:
            return bytes.fromhex(text.strip()).decode("utf-8")
        except Exception:
            return text
    return text


def _md5_upper(data: bytes) -> str:
    return hashlib.md5(data).hexdigest().upper()


def _find_clob_tables(zf: zipfile.ZipFile) -> list[dict]:
    """
    Les metadata.xml og returner liste av tabeller med CLOB-kolonner.
    Støtter alle skjemaer (schema0, schema1 ...).
    """
    ns = {"ns": "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"}

    metadata_path = next(
        (n for n in zf.namelist() if n.lower().endswith("header/metadata.xml")),
        None,
    )
    if not metadata_path:
        raise FileNotFoundError("metadata.xml ikke funnet i SIARD-arkivet")

    tables = []
    with zf.open(metadata_path) as f:
        tree = ET.parse(f)
        root = tree.getroot()

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
            col_idx   = 0
            for col in cols_el.findall("ns:column", ns):
                col_idx += 1
                type_el = col.find("ns:type", ns)
                if type_el is not None and "CLOB" in type_el.text.upper():
                    clob_cols.append(col_idx)

            if clob_cols:
                tables.append({
                    "name":          name_el.text,
                    "folder":        tbl_fld.text,
                    "schema_folder": schema_folder,
                    "clob_columns":  clob_cols,
                })

    return tables


def _process_table(
    zin:           zipfile.ZipFile,
    zout:          zipfile.ZipFile | None,
    table_info:    dict,
    written_files: set,
    w,
    stats:         dict,
    dry_run:       bool = False,
    min_text_length: int = 30,
) -> int:
    """
    Stream tableX.xml, dekod HEX CLOB-felt, skriv xrec{N}.txt,
    patch XML og skriv til zout.  Returnerer antall LOB-filer skrevet.
    Felt kortere enn min_text_length tegn (etter dekoding) hoppes over.
    """
    schema_folder = table_info["schema_folder"]
    folder        = table_info["folder"]
    clob_columns  = table_info["clob_columns"]
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
        with zin.open(xml_arc_path) as f_in, \
             open(tmp_path, "wb") as out:

            out.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
            out.write(b"<table>\n")

            context = ET.iterparse(f_in, events=("start", "end"))

            for event, elem in context:
                if _strip_ns(elem.tag) != "row" or event != "end":
                    continue

                row_counter += 1

                for col_index in clob_columns:
                    target_tag = f"c{col_index}"
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
                        if not _is_hex_string(raw):
                            continue

                        try:
                            text_value = _hex_to_text(raw)
                            # Hopp over felt som er kortere enn minimumslengde
                            if len(text_value) < min_text_length:
                                stats["hex_skipped"] = stats.get("hex_skipped", 0) + 1
                                continue
                            filename   = f"xrec{row_counter}.txt"
                            zip_path   = lob_folder + filename
                            data_bytes = text_value.encode("utf-8")
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

class HexExtractOperation(BaseOperation):
    """
    Dekoder inline HEX CLOB-tekst i tableX.xml og eksporterer til .txt-filer.
    Kjøres FØR BlobConvertOperation.
    """

    operation_id = "hex_extract"
    label        = "HEX Inline Extract"
    category     = "Konvertering"

    default_params = {
        "dry_run":         False,
        "temp_dir":        "",
        "min_text_length": 30,    # tekst kortere enn dette eksporteres ikke
    }

    @property
    def description(self) -> str:
        return ("Dekoder inline HEX CLOB-tekst i tableX.xml og eksporterer "
                "til eksterne .txt-filer. Kjøres før BLOB Konverter.")

    def run(self, ctx) -> object:
        import threading as _th

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

        src_path = ctx.siard_path
        suffix   = "_hex_extracted"
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        c = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{c}" + src_path.suffix)
            c += 1

        stats: dict = {"hex_exported": 0, "tables_patched": 0, "hex_skipped": 0}
        try:
            self._process(ctx, src_path, dst_path, stats, w, progress)
        except Exception as exc:
            import traceback as _tb
            w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
            progress("finish", stats=stats)
            return self._fail(str(exc), stats)

        w("  OPPSUMMERING:", "step")
        STAT_LABELS_HEX = {
            "hex_exported":  "HEX-felt eksportert",
            "hex_skipped":   "HEX-felt hoppet over",
            "tables_patched":"Tabeller patchet",
            "lob_before":    "LOB-filer (før)",
            "lob_after":     "LOB-filer (etter)",
            "lob_diff":      "LOB-filer (endring)",
        }
        for k, v in stats.items():
            label = STAT_LABELS_HEX.get(k, k)
            w(f"    {label:<28} {v}", "info")
        if not self.params.get("dry_run"):
            w(f"    Ny SIARD: {dst_path}", "ok")
        w("=" * 56)
        progress("finish", stats=stats)
        return self._ok(
            {**stats, "output_path": str(dst_path)},
            f"{stats['hex_exported']} HEX-felt eksportert, "
            f"{stats['tables_patched']} tabeller patchet")

    def _process(self, ctx, src_path: Path, dst_path: Path,
                 stats: dict, w, progress) -> None:

        dry_run          = bool(self.params.get("dry_run", False))
        min_text_length  = max(0, int(self.params.get("min_text_length", 30)))
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
            try:
                tables = _find_clob_tables(zin)
            except FileNotFoundError as exc:
                raise RuntimeError(str(exc)) from exc

            if not tables:
                w("  Ingen CLOB-tabeller funnet.", "info")
                for n in range(1, PHASES + 1):
                    progress("phase_done")
                return

            w(f"  Fant {len(tables)} tabell(er) med CLOB-kolonner:", "info")
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
                # Kopier alt unntatt behandlede tableX.xml
                if not dry_run:
                    for n_done, item in enumerate(all_items, 1):
                        if item.filename in table_xml_paths:
                            pass  # skrives av _process_table
                        else:
                            try:
                                data = zin.read(item.filename)
                                ct   = (zipfile.ZIP_STORED
                                        if item.filename.lower().endswith(".bin")
                                        else zipfile.ZIP_DEFLATED)
                                zout.writestr(item.filename, data,
                                              compress_type=ct)
                                written_files.add(item.filename)
                            except Exception as exc:
                                w(f"    [FEIL] Kopiering {item.filename}: "
                                  f"{exc}", "feil")
                        if n_done % REPORT == 0 or n_done == n_total:
                            progress("phase_progress",
                                     done=n_done, total=n_total)

                # Behandle CLOB-tabellene
                for table_info in tables:
                    _process_table(zin, zout, table_info,
                                   written_files, w, stats, dry_run,
                                   min_text_length=min_text_length)

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
