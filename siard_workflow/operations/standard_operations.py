"""
Diverse operasjoner:
    - XMLValidationOperation
    - MetadataExtractOperation
    - VirusScanOperation
    - ConditionalOperation  (kjøres kun hvis et gitt flag er True)
"""

from __future__ import annotations
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


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
class MetadataExtractOperation(BaseOperation):
    """
    Henter ut nøkkelmetadata fra SIARD-filens metadata.xml:
    databasenavn, DBMS, antall skjemaer, tabeller og estimert radeantall.
    """

    operation_id = "metadata_extract"
    label = "Metadata-uttrekk"
    description = "Henter metadata (DB-navn, DBMS, tabeller, rader) fra SIARD-filen."
    category = "Metadata"
    default_params = {}

    _NS = re.compile(r"\{[^}]+\}")

    def _tag(self, el: ET.Element) -> str:
        return self._NS.sub("", el.tag)

    def run(self, ctx: WorkflowContext) -> OperationResult:
        meta: dict = {
            "db_name": None,
            "db_product": None,
            "schemas": [],
            "table_count": 0,
            "row_count": 0,
        }

        try:
            with zipfile.ZipFile(ctx.siard_path, "r") as zf:
                names_lower = {n.lower(): n for n in zf.namelist()}
                entry = names_lower.get("header/metadata.xml") or names_lower.get("metadata.xml")
                if not entry:
                    return self._fail("metadata.xml ikke funnet")

                with zf.open(entry) as f:
                    root = ET.parse(f).getroot()

        except (zipfile.BadZipFile, OSError) as e:
            return self._fail(str(e))
        except ET.ParseError as e:
            return self._fail(f"XML-parsefeil: {e}")

        # Hent nøkkelverdier (namespace-uavhengig)
        for el in root.iter():
            tag = self._tag(el)
            if tag == "dbName" and not meta["db_name"]:
                meta["db_name"] = el.text
            elif tag == "databaseProduct" and not meta["db_product"]:
                meta["db_product"] = el.text
            elif tag == "schema":
                schema_name = None
                for child in el:
                    if self._tag(child) == "name":
                        schema_name = child.text
                        break
                if schema_name:
                    meta["schemas"].append(schema_name)
            elif tag == "table":
                meta["table_count"] += 1
                for child in el:
                    if self._tag(child) == "rows":
                        try:
                            meta["row_count"] += int(child.text or 0)
                        except ValueError:
                            pass

        ctx.set_result("metadata", meta)

        msg = (
            f"{meta['db_name'] or '?'} | "
            f"{meta['table_count']} tabeller | "
            f"{meta['row_count']:,} rader"
        )
        return self._ok(data=meta, message=msg)



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
