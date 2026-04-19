"""
siard_workflow/operations/pipeline_operations.py

UnpackSiardOperation  —  Pakker ut SIARD-arkivet til temp-mappe én gang
RepackSiardOperation  —  Pakker ferdig temp-mappe tilbake til ny .siard-fil

Disse to operasjonene danner rammen rundt en pipeline-arbeidsflyt der SIARD
kun pakkes ut og inn én gang, mens alle mellomliggende operasjoner (Virusskan,
HexExtract, BlobKonverter) jobber direkte på filsystemet.

Anbefalt rekkefølge:
    SHA256 → Pakk ut SIARD → Virusskan → HEX Inline Extract
           → BLOB Konverter → Pakk sammen SIARD
"""
from __future__ import annotations

import datetime
import shutil
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.siard_format import (
    detect_siard_version, siard_version_transform,
    get_target_siard_version, is_siard_xml,
)


# ─────────────────────────────────────────────────────────────────────────────

class UnpackSiardOperation(BaseOperation):
    """
    Pakker ut SIARD-arkivet til en midlertidig mappe.

    Setter ctx.extracted_path og lagrer den originale ZIP-navnlisten i
    ctx.results["unpack_siard"]["original_namelist"] slik at
    RepackSiardOperation kan gjenopprette kataloginnganger i ZIP-en.
    """

    operation_id   = "unpack_siard"
    label          = "Pakk ut SIARD"
    description    = (
        "Pakker ut SIARD-arkivet til en midlertidig mappe for pipeline-behandling. "
        "Bruk 'Pakk sammen SIARD' som siste operasjon for å lage ferdig arkiv."
    )
    category       = "Pipeline"
    status         = 2
    produces_siard = False
    requires_unpack = False  # er selve utpakkeren

    default_params: dict = {}

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log: log.log(msg, lvl)
            if pcb: pcb("log", msg=msg, level=lvl)

        # Hopp over hvis allerede pakket ut (f.eks. ved gjenkjøring)
        if ctx.extracted_path and ctx.extracted_path.is_dir():
            w(f"  SIARD allerede utpakket: {ctx.extracted_path}", "info")
            return self._ok(
                {"extracted_path": str(ctx.extracted_path)},
                "Allerede utpakket — hopper over")

        siard_path = ctx.siard_path
        if not siard_path or not siard_path.exists():
            return self._fail(f"SIARD-fil ikke funnet: {siard_path}")

        # Velg temp-rot
        td = ctx.metadata.get("temp_dir", "").strip()
        temp_root = Path(td) if td and Path(td).is_dir() else None

        try:
            tmp = Path(tempfile.mkdtemp(prefix="siard_pipeline_", dir=temp_root))
        except Exception as exc:
            return self._fail(f"Kunne ikke opprette temp-mappe: {exc}")

        w(f"  Pakker ut {siard_path.name} → {tmp} ...", "info")
        t0 = datetime.datetime.now()

        try:
            with zipfile.ZipFile(siard_path, "r", allowZip64=True) as zf:
                original_namelist: list[str] = zf.namelist()
                n_total = len(original_namelist)
                n_done = 0
                REPORT = max(1, n_total // 40)

                for name in original_namelist:
                    try:
                        zf.extract(name, tmp)
                    except Exception as exc:
                        w(f"  [ADVARSEL] Kunne ikke pakke ut {name}: {exc}", "warn")
                    n_done += 1
                    if n_done % REPORT == 0 or n_done == n_total:
                        if pcb:
                            pcb("phase_progress", done=n_done, total=n_total)

        except Exception as exc:
            shutil.rmtree(tmp, ignore_errors=True)
            return self._fail(f"Kunne ikke åpne SIARD: {exc}")

        elapsed = (datetime.datetime.now() - t0).total_seconds()
        w(f"  Pakket ut {n_done:,} filer på {elapsed:.1f}s", "ok")

        ctx.extracted_path = tmp
        data = {
            "extracted_path":    str(tmp),
            "original_namelist": original_namelist,
            "files_extracted":   n_done,
        }
        return self._ok(data, f"{n_done:,} filer pakket ut til {tmp}")


# ─────────────────────────────────────────────────────────────────────────────

class RepackSiardOperation(BaseOperation):
    """
    Pakker den midlertidige utpakkede mappen tilbake til en ny .siard-fil
    og rydder opp temp-mappen.

    Fordi operasjonen setter output_path i result.data, vil WorkflowContext
    automatisk oppdatere ctx.siard_path til den nye filen.
    """

    operation_id   = "repack_siard"
    label          = "Pakk sammen SIARD"
    description    = (
        "Pakker den utpakkede SIARD-strukturen til en ny .siard-fil og rydder "
        "temp-mappen. Bruk alltid etter 'Pakk ut SIARD' i pipeline-arbeidsflyten."
    )
    category       = "Pipeline"
    status         = 2
    produces_siard = True
    requires_unpack = False  # er selve sammenpackeren

    default_params = {
        "output_suffix": "_konvertert",
        "keep_temp":     False,
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log: log.log(msg, lvl)
            if pcb: pcb("log", msg=msg, level=lvl)

        extract_dir = ctx.extracted_path
        if not extract_dir or not extract_dir.is_dir():
            return self._fail(
                "Ingen utpakket SIARD-mappe funnet. "
                "Legg til 'Pakk ut SIARD' som første operasjon i workflowen.")

        # Hent original namelist (for å bevare ZIP-kataloginnganger)
        unpack_data   = ctx.get_result("unpack_siard") or {}
        orig_namelist: list[str] = unpack_data.get("original_namelist", [])

        # Bestem destinasjonsfil
        suffix   = (self.params.get("output_suffix") or "_konvertert").strip()
        src_path = ctx.siard_path
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        counter  = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{counter}" + src_path.suffix)
            counter += 1

        w(f"  Pakker sammen → {dst_path.name} ...", "info")

        # Versjonhåndtering
        target_version = get_target_siard_version()
        src_version    = "2.1"
        metadata_xml   = extract_dir / "header" / "metadata.xml"
        if metadata_xml.exists():
            try:
                src_version = detect_siard_version(metadata_xml.read_bytes())
            except Exception:
                pass
        w(f"  SIARD-versjon: {src_version} → {target_version}", "info")

        # Kataloginnganger fra original ZIP
        orig_dir_entries = sorted(
            n for n in orig_namelist if n.endswith("/"))

        def _ver_path(name: str) -> str:
            if (src_version and src_version != target_version
                    and src_version in name
                    and name.startswith("header/")):
                return name.replace(src_version, target_version)
            return name

        all_files = sorted(f for f in extract_dir.rglob("*") if f.is_file())
        n_total   = len(all_files)
        n_written = 0
        t0        = datetime.datetime.now()
        REPORT    = max(1, n_total // 40)

        try:
            with zipfile.ZipFile(dst_path, "w", zipfile.ZIP_DEFLATED,
                                 allowZip64=True) as zf:

                # 1. Kataloginnganger fra original (f.eks. header/siardversion/2.1/)
                for dir_entry in orig_dir_entries:
                    dir_entry_out = _ver_path(dir_entry)
                    zf.writestr(zipfile.ZipInfo(dir_entry_out), b"")

                # 2. Alle filer fra filsystemet
                for file_path in all_files:
                    arc_name = str(
                        file_path.relative_to(extract_dir)).replace("\\", "/")
                    arc_name = _ver_path(arc_name)

                    # SIARD XML-filer: transformer versjon
                    if is_siard_xml(arc_name):
                        try:
                            data = siard_version_transform(
                                file_path.read_bytes(), target_version)
                        except Exception:
                            data = file_path.read_bytes()
                    else:
                        data = file_path.read_bytes()

                    compress = (zipfile.ZIP_STORED
                                if arc_name.lower().endswith(".bin")
                                else zipfile.ZIP_DEFLATED)
                    zf.writestr(arc_name, data, compress_type=compress)
                    n_written += 1

                    if n_written % REPORT == 0 or n_written == n_total:
                        if pcb:
                            pcb("phase_progress", done=n_written, total=n_total)

        except Exception as exc:
            return self._fail(f"Kunne ikke pakke sammen SIARD: {exc}")

        elapsed = (datetime.datetime.now() - t0).total_seconds()
        size_mb = dst_path.stat().st_size / 1_048_576
        w(f"  Skrevet {n_written:,} filer ({size_mb:.1f} MB) på {elapsed:.1f}s",
          "ok")
        w(f"  Ny SIARD: {dst_path}", "ok")

        # Rydd temp
        keep = bool(self.params.get("keep_temp", False))
        if keep:
            w(f"  Temp-mappe beholdt: {extract_dir}", "info")
        else:
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
                if not extract_dir.exists():
                    w(f"  Temp-mappe ryddet: {extract_dir}", "info")
                else:
                    w(f"  Advarsel: temp-mappe kunne ikke slettes: {extract_dir}",
                      "warn")
            except Exception as exc:
                w(f"  Advarsel: opprydding feilet: {exc}", "warn")

        ctx.extracted_path = None

        data = {
            "output_path":   str(dst_path),
            "files_written": n_written,
            "size_mb":       round(size_mb, 2),
        }
        return self._ok(data, f"{n_written:,} filer → {dst_path.name}")
