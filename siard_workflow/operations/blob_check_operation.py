"""
BlobCheckOperation
------------------
Workflow-IF: Sjekker og teller CLOB/BLOB-innhold i SIARD-uttrekket.

Opptelling per tableX:
  - CLOB inline    : <clob>-element i tableX.xml der verdien er tekst direkte i XML-en
  - CLOB ekstern   : <clob>-element med fil-referanse (file="...") eller tom verdi + ekstern fil
  - BLOB i lob-dir : filer i lob-mapper (ikke .xml/.xsd), gruppert etter filtype

Setter ctx-flagg:
  has_blobs (bool)  — True hvis noen form for BLOB/CLOB funnet
  has_lob_files     — True hvis lob-mappe-filer finnes

Resultater lagres i ctx under nokkel 'blob_check'.
"""

from __future__ import annotations
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import PurePosixPath

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext

_EXPECTED_SUFFIXES = {".xml", ".xsd"}
_NS_RE = re.compile(r"\{[^}]+\}")


def _local(tag: str) -> str:
    return _NS_RE.sub("", tag)


def _is_lob_dir(parts):
    """Returner True hvis stien er content/schemaX/tableX/lobDir/fil."""
    return len(parts) >= 5


class BlobCheckOperation(BaseOperation):
    """
    Teller og klassifiserer CLOB/BLOB-innhold i SIARD-uttrekket.
    Setter flagget 'has_blobs' i konteksten.
    """

    operation_id = "blob_check"
    label        = "BLOB/CLOB Kontroll"
    description  = (
        "Teller inline CLOB, ekstern CLOB og BLOB-filer per tabell. "
        "Eksporterer opptelling til logg."
    )
    category     = "Innhold"
    status       = 0
    default_params = {
        "content_prefix": "content/",
        "schema_pattern": r"^schema\d+$",
        "table_pattern":  r"^table\d+$",
        "clob_tags":      ["clob", "clobValue", "blobValue", "blob"],
    }

    def _compile(self):
        self._schema_re = re.compile(self.params["schema_pattern"], re.IGNORECASE)
        self._table_re  = re.compile(self.params["table_pattern"],  re.IGNORECASE)
        self._clob_tags = set(t.lower() for t in self.params["clob_tags"])

    # ── Hovedlogikk ───────────────────────────────────────────────────────────

    def run(self, ctx: WorkflowContext) -> OperationResult:
        self._compile()
        path = ctx.siard_path

        try:
            with zipfile.ZipFile(path, "r") as zf:
                namelist = zf.namelist()
                result   = self._analyse(zf, namelist)
        except zipfile.BadZipFile as e:
            return self._fail(f"Ugyldig ZIP/SIARD: {e}")
        except OSError as e:
            return self._fail(f"Kunne ikke apne filen: {e}")

        has_blobs = (
            result["total_clob_inline"] > 0
            or result["total_clob_extern"] > 0
            or result["total_blob_files"] > 0
        )

        ctx.set_flag("has_blobs",     has_blobs)
        ctx.set_flag("has_lob_files", result["total_blob_files"] > 0)
        ctx.set_result("blob_check", result)

        # Skriv opptelling til logg via WorkflowFileLogger hvis aktiv
        self._write_log(ctx, result)

        if has_blobs:
            parts = []
            if result["total_clob_inline"]  > 0: parts.append(f"{result['total_clob_inline']} CLOB inline")
            if result["total_clob_extern"]  > 0: parts.append(f"{result['total_clob_extern']} CLOB ekstern")
            if result["total_blob_files"]   > 0: parts.append(f"{result['total_blob_files']} BLOB-filer")
            msg = "TRUE  " + ", ".join(parts)
        else:
            msg = "FALSE  ingen BLOB/CLOB funnet"

        return self._ok(data=result, message=msg)

    # ── Analyse ───────────────────────────────────────────────────────────────

    def _analyse(self, zf: zipfile.ZipFile, namelist: list[str]) -> dict:
        prefix = self.params["content_prefix"].strip("/").lower()

        # --- Sorter ZIP-oppforinger i kategorier ---
        table_xml_map: dict[str, str] = {}      # "schema0/table1" -> zip-sti til tableX.xml
        lob_files:     dict[str, list[str]] = defaultdict(list)  # "schema0/table1" -> [filnavn, ...]

        for name in namelist:
            parts = PurePosixPath(name).parts
            if len(parts) < 4:
                continue
            content_dir = parts[0].lower()
            if content_dir != prefix:
                continue
            schema_dir, table_dir = parts[1], parts[2]
            if not self._schema_re.match(schema_dir):
                continue
            if not self._table_re.match(table_dir):
                continue

            table_key = f"{schema_dir}/{table_dir}"
            rest = parts[3:]

            if len(rest) == 1:
                fname = rest[0]
                if PurePosixPath(fname).suffix.lower() == ".xml":
                    table_xml_map[table_key] = name
                # .xsd ignoreres
            else:
                # Fil i sub-mappe (lob-mappe)
                lob_files[table_key].append("/".join(rest))

        # --- Tell CLOB i tableX.xml ---
        per_table: dict[str, dict] = {}

        for table_key, xml_path in table_xml_map.items():
            counts = self._count_clobs_in_xml(zf, xml_path)
            blob_list = lob_files.get(table_key, [])
            blob_by_ext = self._count_blobs_by_ext(blob_list)
            per_table[table_key] = {
                "clob_inline":  counts["inline"],
                "clob_extern":  counts["extern"],
                "blob_files":   len(blob_list),
                "blob_by_ext":  blob_by_ext,
                "blob_filelist": blob_list,
            }

        # Tabeller med lob-filer men ingen tableX.xml (sjeldent)
        for table_key, blob_list in lob_files.items():
            if table_key not in per_table:
                blob_by_ext = self._count_blobs_by_ext(blob_list)
                per_table[table_key] = {
                    "clob_inline":  0,
                    "clob_extern":  0,
                    "blob_files":   len(blob_list),
                    "blob_by_ext":  blob_by_ext,
                    "blob_filelist": blob_list,
                }

        total_ci = sum(t["clob_inline"] for t in per_table.values())
        total_ce = sum(t["clob_extern"] for t in per_table.values())
        total_bf = sum(t["blob_files"]  for t in per_table.values())

        return {
            "per_table":          per_table,
            "total_clob_inline":  total_ci,
            "total_clob_extern":  total_ce,
            "total_blob_files":   total_bf,
            "tables_with_blobs":  [k for k, v in per_table.items()
                                   if v["clob_inline"] or v["clob_extern"] or v["blob_files"]],
            "all_tables":         sorted(per_table.keys()),
        }

    def _count_clobs_in_xml(self, zf: zipfile.ZipFile, xml_path: str) -> dict:
        """
        Les tableX.xml og tell:
          inline  : clob-element med tekst direkte (ingen @file-attributt, tekst finnes)
          extern  : clob-element med @file-attributt ELLER tomt element + tilsvarende fil
        """
        inline = 0
        extern = 0
        try:
            with zf.open(xml_path) as f:
                # Iterativ parsing for store filer
                context = ET.iterparse(f, events=("start",))
                for _, elem in context:
                    tag = _local(elem.tag).lower()
                    if tag not in self._clob_tags:
                        elem.clear()
                        continue
                    file_attr = elem.get("file") or elem.get("fileName") or elem.get("href")
                    if file_attr:
                        extern += 1
                    elif elem.text and elem.text.strip():
                        inline += 1
                    else:
                        # Tomt element — regnes som ekstern referanse
                        extern += 1
                    elem.clear()
        except ET.ParseError:
            pass  # XML-feil haandteres av XMLValidationOperation
        except Exception:
            pass

        return {"inline": inline, "extern": extern}

    def _count_blobs_by_ext(self, filelist: list[str]) -> dict[str, int]:
        by_ext: dict[str, int] = defaultdict(int)
        for f in filelist:
            ext = PurePosixPath(f).suffix.lower() or "(ingen)"
            by_ext[ext] += 1
        return dict(by_ext)

    # ── Loggutskrift ──────────────────────────────────────────────────────────

    def _write_log(self, ctx: WorkflowContext, result: dict) -> None:
        """
        Skriv detaljert opptelling til WorkflowFileLogger hvis den er aktiv i ctx.
        Faller stille tilbake hvis ingen logger er registrert.
        """
        logger = ctx.metadata.get("file_logger")
        if not logger:
            return

        w = logger.log
        w("", "info")
        w("=" * 56, "info")
        w("  BLOB/CLOB OPPTELLING", "step")
        w("=" * 56, "info")
        w(f"  Totalt  CLOB inline : {result['total_clob_inline']}", "info")
        w(f"  Totalt  CLOB ekstern: {result['total_clob_extern']}", "info")
        w(f"  Totalt  BLOB-filer  : {result['total_blob_files']}", "info")
        w("", "info")
        w("  Per tabell:", "info")
        w(f"  {'Tabell':<30} {'CI':>6} {'CE':>6} {'BF':>6}  Blob-filtyper", "info")
        w("  " + "-" * 70, "info")

        for tkey in sorted(result["per_table"]):
            t = result["per_table"][tkey]
            ci = t["clob_inline"]
            ce = t["clob_extern"]
            bf = t["blob_files"]
            ext_str = "  ".join(f"{e}:{n}" for e, n in sorted(t["blob_by_ext"].items())) or "-"
            flag = " *" if (ci or ce or bf) else ""
            w(f"  {tkey:<30} {ci:>6} {ce:>6} {bf:>6}  {ext_str}{flag}", "ok" if (ci or ce or bf) else "info")

        w("", "info")
        w("  CI=CLOB inline  CE=CLOB ekstern  BF=BLOB-filer", "info")
        w("=" * 56, "info")
        w("", "info")
