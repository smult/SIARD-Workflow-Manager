"""
Diverse operasjoner:
    - XMLValidationOperation
    - MetadataExtractOperation
    - VirusScanOperation
    - ConditionalOperation  (kjøres kun hvis et gitt flag er True)
"""

from __future__ import annotations
import re
import subprocess
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


def _open_file(path: Path) -> None:
    """Åpner en fil med standard systemprogram (plattformuavhengig)."""
    import os
    import platform
    try:
        _sys = platform.system()
        if _sys == "Windows":
            os.startfile(str(path))
        elif _sys == "Darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
class XMLValidationOperation(BaseOperation):
    """
    Validerer metadata.xml i SIARD-filen mot forventet struktur.
    Logger alle funn (feil, advarsler, OK-meldinger) detaljert.
    """

    operation_id = "xml_validation"
    label        = "XML-validering"
    description  = "Validerer metadata.xml og tabellskjemaer. Logger alle funn."
    category     = "Validering"
    status       = 2
    default_params = {
        "required_elements": ["dbName", "databaseProduct", "tables"],
        "check_table_xsd":   True,
        "check_table_xml":   True,   # parse hver tableX.xml for velformethet
    }

    _METADATA_PATHS = ["header/metadata.xml", "metadata.xml"]

    def run(self, ctx: WorkflowContext) -> OperationResult:
        errors:   list[str] = []
        warnings: list[str] = []
        ok_msgs:  list[str] = []

        try:
            with zipfile.ZipFile(ctx.siard_path, "r") as zf:
                namelist        = zf.namelist()
                namelist_lower  = {n.lower(): n for n in namelist}
                namelist_set    = set(namelist)

                # ── 1. Finn og les metadata.xml ──────────────────────────────
                metadata_entry = None
                for candidate in self._METADATA_PATHS:
                    if candidate in namelist_lower:
                        metadata_entry = namelist_lower[candidate]
                        break

                if not metadata_entry:
                    self._log_result(ctx, errors, warnings, ok_msgs)
                    return self._fail("metadata.xml ikke funnet i SIARD-arkivet")

                with zf.open(metadata_entry) as f:
                    try:
                        tree = ET.parse(f)
                        root = tree.getroot()
                        ok_msgs.append(f"metadata.xml: velformet XML")
                    except ET.ParseError as e:
                        errors.append(f"metadata.xml: ugyldig XML — {e}")
                        self._log_result(ctx, errors, warnings, ok_msgs)
                        return self._fail(f"metadata.xml er ugyldig XML: {e}",
                                          data={"errors": errors, "warnings": warnings})

                # ── 2. Sjekk paakrevde elementer ─────────────────────────────
                def local(tag: str) -> str:
                    return re.sub(r"\{[^}]+\}", "", tag)

                found = {local(el.tag) for el in root.iter()}
                for req in self.params["required_elements"]:
                    if req in found:
                        ok_msgs.append(f"Paakredd element <{req}>: funnet")
                    else:
                        errors.append(f"Mangler paakredd element: <{req}>")

                # ── 3. Hent skjema- og tabellstruktur fra metadata ────────────
                schemas_in_meta: list[str] = []
                tables_in_meta:  list[str] = []
                for el in root.iter():
                    tag = local(el.tag)
                    if tag == "schema":
                        for child in el:
                            if local(child.tag) in ("name", "n"):
                                schemas_in_meta.append(child.text or "?")
                                break
                    elif tag == "table":
                        for child in el:
                            if local(child.tag) in ("name", "n"):
                                tables_in_meta.append(child.text or "?")
                                break

                if schemas_in_meta:
                    ok_msgs.append(f"Skjemaer i metadata: {', '.join(schemas_in_meta)}")
                else:
                    warnings.append("Ingen <schema>-elementer funnet i metadata.xml")

                if tables_in_meta:
                    ok_msgs.append(f"Tabeller i metadata: {len(tables_in_meta)} stk")
                else:
                    warnings.append("Ingen <table>-elementer funnet i metadata.xml")

                # ── 4. Sjekk at tableX.xsd finnes ────────────────────────────
                if self.params["check_table_xsd"]:
                    xml_files = [n for n in namelist
                                 if n.lower().endswith(".xml") and "/table" in n.lower()
                                 and "metadata" not in n.lower()]
                    missing_xsd = 0
                    for xml_path in xml_files:
                        xsd_path = xml_path[:-4] + ".xsd"
                        if xsd_path in namelist_set:
                            ok_msgs.append(f"XSD funnet for: {xml_path}")
                        else:
                            warnings.append(f"Mangler XSD for: {xml_path}")
                            missing_xsd += 1
                    if not xml_files:
                        warnings.append("Ingen tableX.xml-filer funnet i Content/")

                # ── 5. Velformethet av tableX.xml ────────────────────────────
                if self.params["check_table_xml"]:
                    table_xmls = [n for n in namelist
                                  if n.lower().endswith(".xml") and "/table" in n.lower()
                                  and "metadata" not in n.lower()]
                    for txml in table_xmls:
                        with zf.open(txml) as tf:
                            try:
                                ET.parse(tf)
                                ok_msgs.append(f"Velformet XML: {txml}")
                            except ET.ParseError as e:
                                errors.append(f"Ugyldig XML i {txml}: {e}")

        except zipfile.BadZipFile as e:
            return self._fail(f"Ugyldig ZIP/SIARD: {e}")

        self._log_result(ctx, errors, warnings, ok_msgs)

        if errors:
            return self._fail(
                f"{len(errors)} feil, {len(warnings)} advarsel(er)",
                data={"errors": errors, "warnings": warnings, "ok": ok_msgs},
            )
        msg = f"OK — {len(ok_msgs)} sjekker bestatt"
        if warnings:
            msg = f"OK med {len(warnings)} advarsel(er) — {len(ok_msgs)} sjekker bestatt"
        return self._ok(
            data={"errors": [], "warnings": warnings, "ok": ok_msgs},
            message=msg,
        )

    def _log_result(self, ctx: WorkflowContext,
                    errors: list, warnings: list, ok_msgs: list) -> None:
        """
        Logger kun ved feil eller advarsler.
        Ved alt-OK skrives en enkelt OK-linje.
        """
        logger = ctx.metadata.get("file_logger")
        if not logger:
            return
        w = logger.log

        if not errors and not warnings:
            w(f"  XML-validering: OK ({len(ok_msgs)} sjekker bestatt)", "ok")
            return

        # Feil og/eller advarsler funnet — skriv full rapport
        w("", "info")
        w("=" * 56, "info")
        w("  XML-VALIDERING — FUNN", "step")
        w("=" * 56, "info")

        if errors:
            w(f"  FEIL ({len(errors)}):", "feil")
            for e in errors:
                w(f"    [X] {e}", "feil")
        if warnings:
            w(f"  ADVARSLER ({len(warnings)}):", "warn")
            for wn in warnings:
                w(f"    [!] {wn}", "warn")

        overall = "FEIL" if errors else "ADVARSEL"
        w("", "info")
        w(f"  Resultat: {overall}  ({len(ok_msgs)} sjekker bestatt)", "feil" if errors else "warn")
        w("=" * 56, "info")
        w("", "info")



# ─────────────────────────────────────────────────────────────────────────────
_LOB_TYPE_RE = re.compile(
    r"\b(blob|clob|nclob|binary large object|character large object"
    r"|national character large object)\b",
    re.IGNORECASE,
)


class MetadataExtractOperation(BaseOperation):
    """
    Henter ut komplett metadata fra SIARD-filens metadata.xml og genererer
    en visuell PDF-rapport med tabelloversikt, ER-diagram og kolonnedetaljer.
    """

    operation_id = "metadata_extract"
    label = "Metadata-uttrekk"
    description = (
        "Henter komplett metadata fra SIARD-filen og genererer en PDF-rapport "
        "med tabelloversikt, statistikk, ER-diagram og kolonnedetaljer."
    )
    category = "Metadata"
    status = 2
    default_params = {
        "generate_pdf":       True,
        "generate_er_diagram": True,
        "pdf_suffix":         "_metadata_rapport",
    }

    _NS = re.compile(r"\{[^}]+\}")

    def _tag(self, el: ET.Element) -> str:
        return self._NS.sub("", el.tag)

    def _text(self, el: ET.Element, tag: str) -> str | None:
        """Hent tekstinnhold av første barn med gitt lokal tag-navn."""
        for child in el:
            if self._tag(child) == tag:
                return (child.text or "").strip() or None
        return None

    def _extract_all(self, siard_path: Path) -> dict:
        """
        Henter ut komplett metadata fra metadata.xml i SIARD-arkivet.

        Returnerer en dict med følgende nøkler:
            db_name, db_product, db_origin, connection, db_user,
            archival_date, producer_app, data_start, data_end,
            description, siard_version, message_digest, message_digest_algo,
            file_size, zip_entry_count, lob_file_count, content_extensions,
            schema_count, table_count, row_count, lob_table_count,
            schemas: [
                {
                    name, description,
                    tables: [
                        {
                            name, description, rows, has_lob, lob_col_count,
                            columns: [
                                {pos, name, type, type_original, nullable,
                                 is_lob, mime_type, description}
                            ],
                            primary_key: {name, columns: [...]},
                            foreign_keys: [
                                {name, ref_schema, ref_table,
                                 references: [{column, referenced}]}
                            ],
                            unique_keys: [{name, columns: [...]}],
                        }
                    ]
                }
            ]
        """
        meta: dict = {
            "db_name": None, "db_product": None, "db_origin": None,
            "connection": None, "db_user": None, "archival_date": None,
            "producer_app": None, "data_start": None, "data_end": None,
            "description": None, "siard_version": None,
            "message_digest": None, "message_digest_algo": None,
            "file_size": None, "zip_entry_count": 0, "lob_file_count": 0,
            "content_extensions": [],
            "schema_count": 0, "table_count": 0, "row_count": 0,
            "lob_table_count": 0, "schemas": [],
        }

        # Filstatistikk
        try:
            meta["file_size"] = siard_path.stat().st_size
        except OSError:
            pass

        # Regex: content/{schema_folder}/{table_folder}/lob*/filnavn
        # Fanger faktiske mappenavn (ikke antatt schema1/table1-indeksering)
        _lob_path_re = re.compile(
            r"^content/([^/]+)/([^/]+)/lob[^/]*/[^/]+$",
            re.IGNORECASE,
        )

        with zipfile.ZipFile(siard_path, "r") as zf:
            namelist = zf.namelist()
            meta["zip_entry_count"] = len(namelist)

            # Tell LOB-filer totalt og per (schema_folder, table_folder)
            lob_extensions: set[str] = set()
            # lob_counts_by_folder[(schema_folder_lower, table_folder_lower)] = antall filer
            lob_counts_by_folder: dict[tuple[str, str], int] = {}
            for n in namelist:
                if n.endswith("/"):   # hopp over mappe-oppføringer
                    continue
                nl = n.lower()
                lob_m = _lob_path_re.match(nl)
                if lob_m:
                    meta["lob_file_count"] += 1
                    key = (lob_m.group(1), lob_m.group(2))
                    lob_counts_by_folder[key] = lob_counts_by_folder.get(key, 0) + 1
                    ext = PurePosixPath(nl).suffix.lstrip(".")
                    if ext:
                        lob_extensions.add(ext)
            meta["content_extensions"] = sorted(lob_extensions)

            # Les metadata.xml
            names_lower = {n.lower(): n for n in namelist}
            entry = (names_lower.get("header/metadata.xml")
                     or names_lower.get("metadata.xml"))
            if not entry:
                raise FileNotFoundError("metadata.xml ikke funnet i SIARD-arkivet")

            with zf.open(entry) as f:
                root = ET.parse(f).getroot()

        # ── Rot-nivå-attributter ──────────────────────────────────────────
        # SIARD 2.x: versjon ligger som attributt på rot-elementet
        for attr_name, attr_val in root.attrib.items():
            local = self._NS.sub("", attr_name).lower()
            if local == "version" and attr_val.strip():
                meta["siard_version"] = attr_val.strip()

        for child in root:
            tag  = self._tag(child)
            tagl = tag.lower()
            txt  = (child.text or "").strip() or None
            if   tagl == "dbname":              meta["db_name"]         = txt
            elif tagl == "databaseproduct":     meta["db_product"]      = txt
            elif tagl == "databaseorigin":      meta["db_origin"]       = txt
            elif tagl == "dataorigintimespan":  meta["data_origin_time_span"] = txt
            elif tagl == "connection":          meta["connection"]      = txt
            elif tagl == "databaseuser":        meta["db_user"]         = txt
            elif tagl == "archivaldate":        meta["archival_date"]   = txt
            elif tagl == "producerapplication": meta["producer_app"]    = txt
            elif tagl == "datastart":           meta["data_start"]      = txt
            elif tagl == "dataend":             meta["data_end"]        = txt
            elif tagl == "description":         meta["description"]     = txt
            elif tagl == "version":             meta["siard_version"]   = txt  # SIARD 1.x fallback
            elif tagl == "messagedigest":
                meta["message_digest"]      = txt
                algo = child.get("algorithm") or child.get("digestType")
                if algo:
                    meta["message_digest_algo"] = algo.strip()

        # ── Skjema- og tabellstruktur ─────────────────────────────────────
        def _parse_columns(tbl_el) -> list[dict]:
            # I SIARD 2.x er kolonner pakket inn i <columns>-elementet
            cols = []
            for wrapper in tbl_el:
                if self._tag(wrapper) != "columns":
                    continue
                for col_el in wrapper:
                    if self._tag(col_el) != "column":
                        continue
                    pos_txt = self._text(col_el, "columnId") or self._text(col_el, "pos")
                    try:
                        pos = int(pos_txt) if pos_txt else len(cols) + 1
                    except ValueError:
                        pos = len(cols) + 1

                    col_type  = self._text(col_el, "type") or ""
                    col_torig = self._text(col_el, "typeOriginal") or ""
                    nullable_txt = (self._text(col_el, "nullable") or "true").lower()
                    nullable = nullable_txt not in ("false", "0", "no")
                    mime = self._text(col_el, "mimeType") or ""
                    is_lob = bool(_LOB_TYPE_RE.search(col_type) or
                                  _LOB_TYPE_RE.search(col_torig) or
                                  mime)
                    cols.append({
                        "pos":           pos,
                        "name":          self._text(col_el, "name") or "",
                        "type":          col_type,
                        "type_original": col_torig,
                        "nullable":      nullable,
                        "is_lob":        is_lob,
                        "mime_type":     mime,
                        "description":   self._text(col_el, "description") or "",
                    })
            return cols

        def _parse_primary_key(tbl_el) -> dict | None:
            # <primaryKey> er direkte barn av <table>
            for child in tbl_el:
                if self._tag(child) != "primaryKey":
                    continue
                pk_cols = [
                    c.text.strip()
                    for c in child
                    if self._tag(c) == "column" and c.text
                ]
                return {
                    "name":    self._text(child, "name") or "",
                    "columns": pk_cols,
                }
            return None

        def _parse_foreign_keys(tbl_el) -> list[dict]:
            # I SIARD 2.x er fremmednøkler pakket inn i <foreignKeys>-elementet
            fks = []
            for wrapper in tbl_el:
                if self._tag(wrapper) != "foreignKeys":
                    continue
                for fk_el in wrapper:
                    if self._tag(fk_el) != "foreignKey":
                        continue
                    refs = []
                    ref_schema = ref_table = ""
                    for sub in fk_el:
                        st = self._tag(sub)
                        if st == "referencedSchema":
                            ref_schema = (sub.text or "").strip()
                        elif st == "referencedTable":
                            ref_table = (sub.text or "").strip()
                        elif st == "reference":
                            col     = self._text(sub, "column") or ""
                            ref_col = self._text(sub, "referenced") or ""
                            refs.append({"column": col, "referenced": ref_col})
                    fks.append({
                        "name":       self._text(fk_el, "name") or "",
                        "ref_schema": ref_schema,
                        "ref_table":  ref_table,
                        "references": refs,
                    })
            return fks

        def _parse_unique_keys(tbl_el) -> list[dict]:
            # I SIARD 2.x er unike nøkler pakket inn i <uniqueKeys>-elementet
            uks = []
            for wrapper in tbl_el:
                if self._tag(wrapper) != "uniqueKeys":
                    continue
                for uk_el in wrapper:
                    if self._tag(uk_el) != "uniqueKey":
                        continue
                    uk_cols = [
                        c.text.strip()
                        for c in uk_el
                        if self._tag(c) == "column" and c.text
                    ]
                    uks.append({
                        "name":    self._text(uk_el, "name") or "",
                        "columns": uk_cols,
                    })
            return uks

        def _parse_table(tbl_el) -> dict:
            cols        = _parse_columns(tbl_el)
            has_lob     = any(c["is_lob"] for c in cols)
            lob_col_cnt = sum(1 for c in cols if c["is_lob"])
            rows_txt    = self._text(tbl_el, "rows")
            try:
                rows = int(rows_txt) if rows_txt else 0
            except ValueError:
                rows = 0

            return {
                "name":         self._text(tbl_el, "name") or "",
                "description":  self._text(tbl_el, "description") or "",
                "rows":         rows,
                "has_lob":      has_lob,
                "lob_col_count": lob_col_cnt,
                "columns":      cols,
                "primary_key":  _parse_primary_key(tbl_el),
                "foreign_keys": _parse_foreign_keys(tbl_el),
                "unique_keys":  _parse_unique_keys(tbl_el),
            }

        # Iterer over skjemaer — bruker <folder>-elementet fra metadata.xml
        # for å matche mot faktiske mappenavn i ZIP-arkivet
        schema_idx = 0
        for el in root.iter():
            if self._tag(el) != "schema":
                continue
            schema_idx += 1
            schema_name   = self._text(el, "name")   or "ukjent"
            schema_folder = (self._text(el, "folder") or f"schema{schema_idx}").lower()
            schema_desc   = self._text(el, "description") or ""
            tables = []
            table_idx = 0
            for child in el:
                if self._tag(child) == "tables":
                    for tbl_el in child:
                        if self._tag(tbl_el) == "table":
                            table_idx += 1
                            table_folder = (
                                self._text(tbl_el, "folder") or f"table{table_idx}"
                            )
                            tbl = _parse_table(tbl_el)
                            tbl["folder"] = table_folder
                            tbl["lob_file_count"] = lob_counts_by_folder.get(
                                (schema_folder, table_folder.lower()), 0
                            )
                            tables.append(tbl)
                            meta["table_count"] += 1
                            meta["row_count"] += tbl["rows"]
                            if tbl["has_lob"] or tbl["lob_file_count"] > 0:
                                meta["lob_table_count"] += 1
            meta["schemas"].append({
                "name":        schema_name,
                "description": schema_desc,
                "tables":      tables,
            })

        meta["schema_count"] = len(meta["schemas"])
        return meta

    def run(self, ctx: WorkflowContext) -> OperationResult:
        try:
            meta = self._extract_all(ctx.siard_path)
        except (zipfile.BadZipFile, OSError) as e:
            return self._fail(str(e))
        except ET.ParseError as e:
            return self._fail(f"XML-parsefeil: {e}")
        except FileNotFoundError as e:
            return self._fail(str(e))

        ctx.set_result("metadata", meta)

        result_data = dict(meta)

        # ── PDF-generering ────────────────────────────────────────────────
        if self.params.get("generate_pdf", True):
            suffix = self.params.get("pdf_suffix", "_metadata_rapport")
            pdf_path = ctx.siard_path.parent / (ctx.siard_path.stem + suffix + ".pdf")
            try:
                from siard_workflow.operations.metadata_pdf import generate_metadata_pdf
                pdf_opts = {
                    "generate_er_diagram": self.params.get("generate_er_diagram", True),
                }
                generate_metadata_pdf(meta, ctx.siard_path, pdf_path, options=pdf_opts)
                result_data["pdf_path"] = str(pdf_path)
                _open_file(pdf_path)
            except ImportError:
                result_data["pdf_warning"] = "reportlab ikke installert — PDF ikke generert"
            except Exception as exc:
                result_data["pdf_warning"] = f"PDF-generering feilet: {exc}"

        msg = (
            f"{meta.get('db_name') or '?'} | "
            f"{meta['table_count']} tabeller | "
            f"{meta['row_count']:,} rader"
        )
        if "pdf_path" in result_data:
            msg += f" | PDF: {Path(result_data['pdf_path']).name}"

        return self._ok(data=result_data, message=msg)



# ─────────────────────────────────────────────────────────────────────────────
class ConditionalOperation(BaseOperation):
    """
    Wrapper som gjør en annen operasjon betinget.
    Kjøres kun hvis et navngitt kontekstflagg er True (eller False).

    Eks – kjør kun hvis has_blobs er True:
        ConditionalOperation(
            operation=VirusScanOperation(),
            flag="has_blobs",
            run_when=True,
        )
    """

    operation_id = "conditional"
    label = "Betinget operasjon"
    category = "Kontroll"
    status = 0
    default_params = {
        "flag": "",         # kontekstflagg å sjekke
        "run_when": True,   # kjør når flagget er denne verdien
    }

    def __init__(self, operation: BaseOperation, flag: str, run_when: bool = True):
        self._inner = operation
        # Overstyr label og id for leselig output
        self.operation_id = f"if_{flag}_{operation.operation_id}"
        self.label = f"[IF {flag}={run_when}] {operation.label}"
        self.category = operation.category
        super().__init__(flag=flag, run_when=run_when)

    def should_run(self, ctx: WorkflowContext) -> bool:
        flag_value = ctx.get_flag(self.params["flag"])
        return flag_value == self.params["run_when"]

    def run(self, ctx: WorkflowContext) -> OperationResult:
        result = self._inner.run(ctx)
        # Viderefør operation_id fra inner slik at konteksten skrives riktig
        ctx.set_result(self._inner.operation_id, result.data)
        return result
