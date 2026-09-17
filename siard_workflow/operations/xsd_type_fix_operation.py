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

metadata.xml og tableM.xml røres ikke. Se
``siard_workflow/core/column_xsd_types.py`` for regelverket.

Pipeline-modus (ctx.extracted_path satt): patcher XSD-filene på disk.
Standalone-modus: skriver <original>_xsdfix.siard med patchede XSD-entries;
alle andre entries kopieres byte for byte.
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.column_xsd_types import (
    apply_xsd_fixes, check_dir, check_zip, fix_dir, format_finding,
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
        "år 0001–9999). Kun tapsfrie endringer; andre avvik rapporteres."
    )
    category       = "Kompatibilitet"
    status         = 2
    produces_siard = True
    modifies_content = True
    premis_event_type  = "Adjustment"
    premis_event_label = "XSD-typekorreksjon"

    default_params: dict = {
        "output_suffix": "_xsdfix",
    }

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and result.data.get("fixes", 0) > 0

    def premis_detail(self, result, ctx) -> str:
        changes = result.data.get("changes") or []
        if not changes:
            return "ingen XSD-typeendringer"
        return (f"{len(changes)} retting(er) i tableX.xsd slik DBPTK-validatoren "
                f"krever (P_4.3-3 kolonnetyper / T_6.3-1 datofasetter), "
                f"tapsfritt for verdiene: " + "; ".join(changes))

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
            return self._result(findings, changes, w, None)

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
                if not keys:
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
                        with zin.open(item) as s, zout.open(item, "w") as d:
                            shutil.copyfileobj(s, d, 1024 * 1024)
        except zipfile.BadZipFile as exc:
            return self._fail(f"Ugyldig ZIP/SIARD: {exc}")
        except Exception as exc:
            dst.unlink(missing_ok=True)
            return self._fail(f"Feil ved skriving av SIARD: {exc}")

        if not changes:
            dst.unlink(missing_ok=True)
            return self._result(findings, [], w, None)
        ctx.siard_path = dst
        return self._result(findings, changes, w, dst)

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
            w(f"  {len(fixable)} avvik rettes (tapsfri utvidelse):", "step")
            for f in fixable:
                w(f"    {format_finding(f)}", "info")
        if manual:
            w(f"  {len(manual)} avvik krever manuell vurdering (rettes ikke):", "warn")
            for f in manual:
                w(f"    {format_finding(f)}", "warn")

    def _result(self, findings: list[dict], changes: list[str], w,
                dst: Path | None) -> OperationResult:
        manual = [format_finding(f) for f in findings if not f.get("fix_to")]
        data = {"fixes": len(changes), "changes": changes,
                "unfixed": len(manual), "unfixed_details": manual}
        if dst is not None:
            data["output_path"] = str(dst)
        if not changes and not manual:
            return self._ok(data, "Ingen XSD-typeavvik funnet")
        parts = []
        if changes:
            parts.append(f"{len(changes)} XSD-retting(er)")
        if manual:
            parts.append(f"{len(manual)} avvik krever manuell vurdering")
        msg = ", ".join(parts)
        if dst is not None:
            msg += f" → {dst.name}"
        w(f"  Ferdig: {msg}", "ok" if changes else "warn")
        return self._ok(data, msg)
