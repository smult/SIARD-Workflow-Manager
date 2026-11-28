"""siard_workflow/operations/xsd_type_fix_operation.py

XsdTypeFixOperation
-------------------
Retter XSD-kolonnetyper i content/schemaN/tableM/tableM.xsd som ikke stemmer
med SQL:2008-typen i header/metadata.xml (DBPTK-validatorens regel P_4.3-3).

Typisk tilfelle: metadata sier ``FLOAT(53)`` mens XSD-en sier ``xs:float``.
Validatoren krever ``xs:double`` for FLOAT uansett presisjon, og feiler med::

    For 'FLOAT(53)' type expected '[xs:double]' but found 'xs:float' ...

Kun tapsfrie utvidelser rettes (xs:float → xs:double, heltallsvarianter →
xs:integer/xs:decimal); øvrige avvik logges som advarsler og må vurderes
manuelt.

Retter også dato-/tidsstempelfasettene (regel T_6.3-1): ``dateType`` og
``dateTimeType`` må ha ``minInclusive``/``maxExclusive`` med nøyaktig DBPTKs
verdier (år 0001 til og med 9999). SCFC skriver ``maxExclusive="9999-12-31Z"``,
som validatoren avviser med «restriction not enforced».

Tidspunkt ved fasettgrensen (regel T_6.0-2): et tidspunkt UTEN tidssone helt
inntil grensen (typisk «uendelig»-markøren ``9999-12-31T23:59:59``) kan ikke
avgjøres mot fasetten og feiler XSD-valideringen. Slike verdier får ``Z``
(UTC) i tableM.xml; sifrene endres ikke, og ingen andre verdier røres. Se
``siard_workflow/core/date_bounds.py``. Kan slås av med parameteren
``mark_boundary_datetimes``.

metadata.xml røres ikke. Se ``siard_workflow/core/column_xsd_types.py`` for
regelverket.

Pipeline-modus (ctx.extracted_path satt): patcher XSD-filene på disk.
Standalone-modus: skriver <original>_xsdfix.siard med patchede XSD-entries;
alle andre entries kopieres byte for byte.
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from siard_workflow.core import date_bounds
from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.fmt import num as _n
from siard_workflow.core.zip_progress import ByteProgress, fmt_bytes
from siard_workflow.core.column_xsd_types import (
    apply_xsd_fixes, check_dir, check_zip, fix_dir, format_finding, group_lines,
    xsd_arc_path, xsds_with_fixes,
)


class XsdTypeFixOperation(BaseOperation):
    """Retter tapsfrie XSD-typeavvik mellom metadata.xml og tableM.xsd."""

    operation_id   = "xsd_type_fix"
    label          = "Rett tableX.xsd (DBPTK P_4.3-3 / T_6.3-1)"
    description    = (
        "Retter type=\"…\" i tableX.xsd der den ikke stemmer med kolonnetypen i "
        "metadata.xml (P_4.3-3), typisk FLOAT(53) med xs:float → xs:double, og "
        "dato-/tidsstempelfasettene på dateType/dateTimeType (T_6.3-1, "
        "år 0001–9999). Kun tapsfrie endringer; andre avvik rapporteres. "
        "Tidspunkt uten tidssone helt inntil grensen (f.eks. "
        "9999-12-31T23:59:59) merkes som UTC med «Z» (T_6.0-2)."
    )
    category       = "Kompatibilitet"
    status         = 2
    produces_siard = True
    modifies_content = True
    premis_event_type  = "Adjustment"
    premis_event_label = "XSD-typekorreksjon"

    default_params: dict = {
        "output_suffix": "_xsdfix",
        # Tidspunkt uten tidssone inntil fasettgrensen får «Z» (T_6.0-2)
        "mark_boundary_datetimes": True,
    }

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and (
            result.data.get("fixes", 0) > 0
            or result.data.get("date_values_marked", 0) > 0)

    def premis_detail(self, result, ctx) -> str:
        changes = result.data.get("changes") or []
        marked = result.data.get("date_values_marked", 0)
        parts = []
        if changes:
            parts.append(
                f"{len(changes)} retting(er) i tableX.xsd slik DBPTK-validatoren "
                f"krever (P_4.3-3 kolonnetyper / T_6.3-1 datofasetter), "
                f"tapsfritt for verdiene: " + "; ".join(group_lines(changes, 5)))
        if marked:
            cols = result.data.get("date_value_columns") or []
            more = f" og {len(cols) - 5} kolonne(r) til" if len(cols) > 5 else ""
            parts.append(
                f"{_n(marked)} tidspunkt uten tidssone helt inntil "
                f"fasettgrensen (år 0001/9999) merket som UTC med «Z» slik at "
                f"de er gyldige mot tableX.xsd (T_6.0-2); sifrene er uendret: "
                + "; ".join(cols[:5]) + more)
        return "; ".join(parts) or "ingen XSD-typeendringer"

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        w("=" * 56)
        w("  RETT TABLEX.XSD (DBPTK P_4.3-3 / T_6.3-1)", "step")
        w("=" * 56)

        stop_ev = ctx.metadata.get("stop_event")
        stop = (lambda: stop_ev.is_set()) if stop_ev is not None else None
        mark = bool(self.params.get("mark_boundary_datetimes", True))

        # ── Pipeline-modus ────────────────────────────────────────────────────
        if ctx.extracted_path and ctx.extracted_path.is_dir():
            self.produces_siard = False
            root = ctx.extracted_path
            try:
                findings = check_dir(root)
            except Exception as exc:
                return self._fail(f"Kan ikke lese metadata/XSD: {exc}")
            self._log_findings(findings, w)
            changes = fix_dir(root, findings)
            dates: list[dict] = []
            if mark:
                try:
                    dates = self._mark_dir(root, w, pcb, stop)
                except date_bounds.Stopped:
                    return self._fail(
                        "Stoppet av bruker under søk etter tidspunkt ved "
                        "fasettgrensen — tabellfila som var under arbeid er urørt")
                except OSError as exc:
                    return self._fail(f"Kan ikke lese/skrive tableX.xml: {exc}")
            return self._result(findings, changes, w, None, dates)

        # ── Standalone-modus ──────────────────────────────────────────────────
        self.produces_siard = True
        src = ctx.siard_path
        suffix = self.params.get("output_suffix", "_xsdfix")
        dst = src.with_name(src.stem + suffix + src.suffix)

        try:
            with zipfile.ZipFile(src, "r") as zin:
                findings = check_zip(zin)
                self._log_findings(findings, w)
                keys = xsds_with_fixes(findings)
                dates: list[dict] = []
                date_entries: dict = {}
                if mark:
                    dates, date_entries = self._scan_zip(zin, w, pcb, stop)
                if not keys and not date_entries:
                    return self._result(findings, [], w, None)

                name_lower = {n.lower(): n for n in zin.namelist()}
                patch_entries = {
                    name_lower[xsd_arc_path(*key).lower()]: key
                    for key in keys
                    if xsd_arc_path(*key).lower() in name_lower}
                changes: list[str] = []
                with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED,
                                     allowZip64=True) as zout:
                    for item in zin.infolist():
                        key = patch_entries.get(item.filename)
                        if key is not None:
                            data, ch = apply_xsd_fixes(zin.read(item), key, findings)
                            zout.writestr(item, data)
                            changes.extend(ch)
                            continue
                        if item.is_dir():
                            zout.writestr(item, b"")
                            continue
                        cols = date_entries.get(item.filename)
                        with zin.open(item) as s, zout.open(item, "w") as d:
                            if cols:
                                date_bounds.fix_stream(s.read, d.write, cols, stop)
                            else:
                                shutil.copyfileobj(s, d, 1024 * 1024)
        except zipfile.BadZipFile as exc:
            return self._fail(f"Ugyldig ZIP/SIARD: {exc}")
        except date_bounds.Stopped:
            dst.unlink(missing_ok=True)
            return self._fail("Stoppet av bruker — ingen ny SIARD-fil er skrevet")
        except Exception as exc:
            dst.unlink(missing_ok=True)
            return self._fail(f"Feil ved skriving av SIARD: {exc}")

        if not changes and not dates:
            dst.unlink(missing_ok=True)
            return self._result(findings, [], w, None)
        ctx.siard_path = dst
        return self._result(findings, changes, w, dst, dates)

    # ── Hjelpere ─────────────────────────────────────────────────────────────

    @staticmethod
    def _log_findings(findings: list[dict], w) -> None:
        if not findings:
            w("  tableX.xsd: kolonnetyper stemmer med metadata.xml og "
              "datofasettene er som DBPTK krever.", "ok")
            return
        fixable = [f for f in findings if f.get("fix_to")]
        manual  = [f for f in findings if not f.get("fix_to")]
        if fixable:
            # Like rettinger i mange tabeller vises som én linje med antall
            w(f"  {len(fixable)} avvik rettes (tapsfri utvidelse):", "step")
            for line in group_lines(format_finding(f) for f in fixable):
                w(f"    {line}", "info")
        if manual:
            w(f"  {len(manual)} avvik krever manuell vurdering (rettes ikke):", "warn")
            for line in group_lines((format_finding(f) for f in manual), 10):
                w(f"    {line}", "warn")

    # ── Tidspunkt ved fasettgrensen (T_6.0-2) ────────────────────────────────

    _HEAD = "  Tidspunkt ved fasettgrensen (T_6.0-2): "
    # Under dette går søket på sekunder: fremdrift bare i GUI, ikke i loggen
    _LOG_PROGRESS_FROM = 4 << 30

    def _mark_dir(self, root: Path, w, pcb, stop) -> list[dict]:
        """Pipeline: finn og merk grenseverdiene i den utpakkede mappa."""
        cands = date_bounds.candidate_tables_dir(root)
        if not cands:
            w(self._HEAD + "ingen tidspunktkolonner i uttrekket", "info")
            return []
        total = sum(c[3] for c in cands)
        w(self._HEAD + f"leser {_n(len(cands))} tabell(er) med tidspunktkolonner "
          f"({fmt_bytes(total)}) …", "info")
        prog = ByteProgress("Søker etter tidspunkt ved grensen", len(cands),
                            total, pcb, w if total >= self._LOG_PROGRESS_FROM else None)
        dates = date_bounds.fix_dir(
            root, on_bytes=prog.add, stop=stop,
            on_file=lambda: prog.add(0, files=1))
        prog.finish()
        self._log_dates(dates, w)
        return dates

    def _scan_zip(self, zin: zipfile.ZipFile, w, pcb, stop):
        """Standalone: finn grenseverdiene; selve merkingen skjer ved skriving."""
        cands = date_bounds.candidate_tables_zip(zin)
        if not cands:
            w(self._HEAD + "ingen tidspunktkolonner i uttrekket", "info")
            return [], {}
        total = sum(c[3] for c in cands)
        w(self._HEAD + f"leser {_n(len(cands))} tabell(er) med tidspunktkolonner "
          f"({fmt_bytes(total)}) …", "info")
        prog = ByteProgress("Søker etter tidspunkt ved grensen", len(cands),
                            total, pcb, w if total >= self._LOG_PROGRESS_FROM else None)
        dates, entries = date_bounds.scan_zip(
            zin, on_bytes=prog.add, stop=stop,
            on_file=lambda: prog.add(0, files=1))
        prog.finish()
        self._log_dates(dates, w)
        return dates, entries

    def _log_dates(self, dates: list[dict], w) -> None:
        if not dates:
            w(self._HEAD + "ingen tidspunkt uten tidssone ligger inntil grensen",
              "ok")
            return
        w(f"  {_n(date_bounds.total(dates))} tidspunkt uten tidssone ligger helt "
          f"inntil fasettgrensen og er ugyldige mot tableX.xsd (T_6.0-2) — "
          f"merkes som UTC med «Z», sifrene er uendret:", "step")
        for f in dates[:20]:
            w(f"    {date_bounds.format_finding(f)}", "info")
        if len(dates) > 20:
            w(f"    … og {len(dates) - 20} kolonne(r) til", "info")

    def _result(self, findings: list[dict], changes: list[str], w,
                dst: Path | None, dates: "list[dict] | None" = None
                ) -> OperationResult:
        manual = [format_finding(f) for f in findings if not f.get("fix_to")]
        dates = dates or []
        marked = date_bounds.total(dates)
        data = {"fixes": len(changes), "changes": changes,
                "unfixed": len(manual), "unfixed_details": manual,
                "date_values_marked": marked,
                "date_value_columns": [
                    f"{f['table_name']}.{f['col_name']} ({_n(f['count'])})"
                    for f in dates]}
        if dst is not None:
            data["output_path"] = str(dst)
        if not changes and not manual and not marked:
            return self._ok(data, "Ingen XSD-typeavvik funnet")
        parts = []
        if changes:
            parts.append(f"{len(changes)} XSD-retting(er)")
        if marked:
            parts.append(f"{_n(marked)} tidspunkt ved grensen merket som UTC")
        if manual:
            parts.append(f"{len(manual)} avvik krever manuell vurdering")
        msg = ", ".join(parts)
        if dst is not None:
            msg += f" → {dst.name}"
        w(f"  Ferdig: {msg}", "ok" if (changes or marked) else "warn")
        return self._ok(data, msg)
