"""
siard_workflow/operations/depot_reports_operation.py
----------------------------------------------------
DepotReportsOperation — genererer «depotrapportene»: et sett PDF-rapporter
analogt med rapportene et arkivdepot lager ved mottak av en SIARD-deponering.

Operasjonen inspiserer hvilke operasjoner som faktisk ble kjørt i workflowen og
lager de mest naturlige rapportene basert på dette:

  • Arkivinformasjon        — krever metadata-uttrekk (metadata_extract)
  • Konverteringsrapport    — krever BLOB-konvertering (blob_convert)
  • Filorganisering         — krever BLOB-konvertering med fil-logg
  • Behandlingssammendrag   — alltid (oppsummering + godkjenning)

Stilen følger «dagens rapport» (report_style.py / workflow_report). Innholdet
er bygget på målepunktene fra det eksterne rapportsettet i extras/.

Legg denne operasjonen sist i workflowen for best resultat.
"""
from __future__ import annotations

import datetime
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


class DepotReportsOperation(BaseOperation):
    """Genererer et sett depot-PDF-rapporter basert på utførte operasjoner."""

    operation_id    = "depot_reports"
    label           = "Depotrapporter (PDF)"
    description      = (
        "Genererer et sett PDF-rapporter etter mønster fra et arkivdepot: "
        "Arkivinformasjon, Konverteringsrapport, Filorganisering og et "
        "Behandlingssammendrag. Rapportene som lages velges automatisk ut fra "
        "hvilke operasjoner som ble kjørt. Legg operasjonen sist i workflowen."
    )
    category        = "Rapport"
    status          = 2
    produces_siard  = False
    requires_unpack = False

    default_params = {
        "report_subdir":             "depotrapporter",
        "include_archive_info":      True,
        "include_conversion":        True,
        "include_file_organization": True,
        "include_summary":           True,
        "open_folder":               True,
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        siard_path = ctx.siard_path
        if not siard_path:
            return self._fail("Ingen SIARD-fil i kontekst — kan ikke lage rapporter.")

        p = self.params
        now = datetime.datetime.now()
        ts  = now.strftime("%Y%m%d_%H%M%S")

        # ── Samle data fra konteksten ─────────────────────────────────────────
        meta = (ctx.get_result("metadata")
                or ctx.results.get("metadata_extract")
                or {})
        if not isinstance(meta, dict):
            meta = {}
        blob = ctx.results.get("blob_convert") or {}
        if not isinstance(blob, dict):
            blob = {}
        step_results = [s for s in ctx.metadata.get("step_results", [])
                        if s.get("id") != self.operation_id]

        # Hvis metadata mangler, prøv å lese den ut direkte (best-effort).
        if not meta.get("schemas") and p.get("include_archive_info", True):
            meta = self._extract_metadata_fallback(siard_path) or meta

        arc_name = (meta.get("db_name") or siard_path.stem).strip() or siard_path.stem

        # ── Finn konverterings-logger (CSV + feillogg) ────────────────────────
        log_dir = ctx.metadata.get("log_dir") or str(siard_path.parent)
        csv_rows, errors = [], []
        if blob:
            csv_path = self._latest(log_dir, "*_blob_konvertering.csv", siard_path.stem)
            err_path = self._latest(log_dir, "*_konvertering_feil.log", siard_path.stem)
            from siard_workflow.operations.depot_reports import (
                _read_conversion_csv, _read_error_log,
            )
            if csv_path:
                csv_rows = _read_conversion_csv(csv_path)
            if err_path:
                errors = _read_error_log(err_path)

        # ── Utdatamappe ──────────────────────────────────────────────────────
        subdir = (p.get("report_subdir") or "").strip()
        out_dir = (siard_path.parent / subdir) if subdir else siard_path.parent
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            return self._fail(f"Kunne ikke opprette rapportmappe: {exc}")

        stem = siard_path.stem
        produced: list[str] = []
        skipped: list[str] = []

        try:
            from siard_workflow.operations import depot_reports as dr
        except ImportError:
            return self._fail(
                "reportlab er ikke installert — kan ikke generere rapporter. "
                "Installer med: pip install reportlab")

        # ── Arkivinformasjon ──────────────────────────────────────────────────
        if p.get("include_archive_info", True):
            if meta.get("table_count") or meta.get("schemas"):
                path = out_dir / f"{stem}_arkivinformasjon_{ts}.pdf"
                self._safe(produced, skipped, "Arkivinformasjon", path,
                           lambda: dr.build_archive_info_report(
                               meta, siard_path, path, generated=now))
            else:
                skipped.append("Arkivinformasjon (ingen metadata — kjør "
                               "Metadata-uttrekk først)")

        # ── Konverteringsrapport ─────────────────────────────────────────────
        if p.get("include_conversion", True):
            if blob:
                path = out_dir / f"{stem}_konverteringsrapport_{ts}.pdf"
                self._safe(produced, skipped, "Konverteringsrapport", path,
                           lambda: dr.build_conversion_report(
                               blob, csv_rows, errors, siard_path, path,
                               arc_name, generated=now))
            else:
                skipped.append("Konverteringsrapport (ingen BLOB-konvertering "
                               "ble kjørt)")

        # ── Filorganisering ──────────────────────────────────────────────────
        if p.get("include_file_organization", True):
            if csv_rows:
                path = out_dir / f"{stem}_filorganisering_{ts}.pdf"
                self._safe(produced, skipped, "Filorganisering", path,
                           lambda: dr.build_file_organization_report(
                               csv_rows, siard_path, path, arc_name,
                               meta=meta, generated=now))
            elif blob:
                skipped.append("Filorganisering (fant ingen konverterings-CSV "
                               "med fil-detaljer)")
            else:
                skipped.append("Filorganisering (ingen BLOB-konvertering ble kjørt)")

        # ── Behandlingssammendrag ────────────────────────────────────────────
        if p.get("include_summary", True):
            path = out_dir / f"{stem}_behandlingssammendrag_{ts}.pdf"
            self._safe(produced, skipped, "Behandlingssammendrag", path,
                       lambda: dr.build_processing_summary_report(
                           step_results, dict(ctx.results), meta, blob,
                           siard_path, path, arc_name, generated=now))

        if not produced:
            return self._fail(
                "Ingen rapporter ble generert. "
                + ("; ".join(skipped) if skipped else ""))

        # Åpne mappen i utforsker (best-effort)
        if p.get("open_folder", True):
            self._open_folder(out_dir)

        msg = f"{len(produced)} rapport(er) lagret i {out_dir.name}/"
        if skipped:
            msg += f"  (utelatt: {len(skipped)})"
        return self._ok(
            data={
                "report_dir":   str(out_dir),
                "report_paths": produced,
                "skipped":      skipped,
                "count":        len(produced),
            },
            message=msg)

    # ── Hjelpere ──────────────────────────────────────────────────────────────

    @staticmethod
    def _safe(produced: list, skipped: list, name: str, path: Path, fn) -> None:
        """Kjør en rapportbygger; logg suksess/feil uten å stoppe resten."""
        try:
            fn()
            produced.append(str(path))
        except Exception as exc:  # noqa: BLE001 — én rapportfeil skal ikke felle resten
            skipped.append(f"{name} (feil: {exc})")

    @staticmethod
    def _latest(log_dir, pattern: str, stem_hint: str = "") -> Path | None:
        """Nyeste fil i log_dir som matcher pattern. Foretrekk filer som
        starter med stem_hint, fall tilbake til generelt nyeste."""
        try:
            d = Path(log_dir)
            if not d.is_dir():
                return None
            cands = list(d.glob(pattern))
            if not cands:
                return None
            if stem_hint:
                pref = [c for c in cands if c.name.startswith(stem_hint)]
                if pref:
                    cands = pref
            return max(cands, key=lambda c: c.stat().st_mtime)
        except Exception:
            return None

    @staticmethod
    def _extract_metadata_fallback(siard_path: Path) -> dict | None:
        """Les metadata direkte hvis metadata_extract ikke ble kjørt."""
        try:
            from siard_workflow.operations.standard_operations import (
                MetadataExtractOperation,
            )
            op = MetadataExtractOperation()
            return op._extract_all(siard_path)
        except Exception:
            return None

    @staticmethod
    def _open_folder(folder: Path) -> None:
        try:
            import os
            import sys
            import subprocess
            if sys.platform.startswith("win"):
                os.startfile(str(folder))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(folder)])
            else:
                subprocess.Popen(["xdg-open", str(folder)])
        except Exception:
            pass
