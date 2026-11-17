"""
siard_workflow/operations/workflow_report_operation.py
------------------------------------------------------
WorkflowReportOperation — Genererer en PDF-sluttrapport for kjøringen.

Rapporten inneholder:
  - Forsideark med SIARD-filnavn, dato/tid og samlet status
  - Oppsummeringstabell over alle kjørte steg (fargekoding: grønn/rød/grå)
  - Detaljseksjoner per operasjon med relevante nøkkeltall
  - Grafiske fremstillinger (kakediagram) for BLOB-data der det finnes

Rapporten lagres i mappen der kilde-SIARD-filen befinner seg.
Legg denne operasjonen sist i workflowen for best resultat.
"""
from __future__ import annotations

import datetime
from pathlib import Path

from siard_workflow.core import fmt
from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext

_n = fmt.num        # antall med mellomrom som tusenskille

# Kategorier i feiloversikten som ikke er feil: fila er beholdt etter regelen
_KEPT_DEFAULT = ("passordbeskyttet", "signert")

# Ledetekster for resultatverdier som vises uten egen detaljseksjon
_RESULT_LABELS = {
    "standardize_ext": {
        "renamed":         "Filer som fikk .bin-endelse",
        "renamed_names":   "Filer omdøpt til recordN",
        "xml_updated":     "Filreferanser oppdatert i XML",
        "name_collisions": "Navnekollisjoner (nytt navn gitt)",
        "rename_errors":   "Feil ved omdøping",
    },
    "xsd_type_fix": {
        "fixes":   "Rettinger i tableX.xsd",
        "unfixed": "Avvik som krever manuell vurdering",
    },
    "lobfolder_fix": {
        "fixes":      "lobFolder-rettinger",
        "file_paths": "Filreferanser skrevet som full sti",
    },
}


def _split_failures(fs: dict) -> "tuple[int, int, set]":
    """(feilet, beholdt etter regelen, nøklene for «beholdt») fra feiloversikten."""
    if not isinstance(fs, dict) or not fs:
        return 0, 0, set(_KEPT_DEFAULT)
    kept_keys = set(fs.get("kept_keys") or _KEPT_DEFAULT)
    counts = fs.get("counts") or {}
    kept = sum(n for k, n in counts.items() if k in kept_keys)
    failed = sum(n for k, n in counts.items() if k not in kept_keys)
    return failed, kept, kept_keys


def key_facts(step_results: list, ctx_results: dict, siard_path: Path) -> list:
    """Nøkkeltall til forsiden som [(ledetekst, verdi)] — bare det som er kjent."""
    def data_of(op_id: str) -> dict:
        for s in step_results:
            if s.get("id") == op_id and isinstance(s.get("data"), dict) and s["data"]:
                return s["data"]
        d = (ctx_results or {}).get(op_id) or {}
        return d if isinstance(d, dict) else {}

    def file_size(path) -> str:
        try:
            p = Path(path)
            return f"  ({fmt.size(p.stat().st_size)})" if p.is_file() else ""
        except (OSError, TypeError, ValueError):
            return ""

    rows: list = []
    sha = data_of("sha256")
    src = sha.get("file")
    if src:
        rows.append(("Kilde:", f"{Path(src).name}{file_size(src)}"))
    if sha.get("sha256"):
        rows.append(("SHA-256 (kilde):", str(sha["sha256"])))
    out_path = data_of("repack_siard").get("output_path") or data_of("blob_convert").get("output_path")
    if out_path and (not src or Path(out_path).name != Path(src).name):
        size_txt = file_size(out_path)
        if not size_txt and data_of("repack_siard").get("size_mb"):
            size_txt = f"  ({fmt.size(float(data_of('repack_siard')['size_mb']) * 1024 * 1024)})"
        rows.append(("Resultat:", f"{Path(out_path).name}{size_txt}"))
    md = data_of("metadata_extract")
    if md.get("db_name"):
        rows.append(("Database:", f"{md['db_name']}"
                     + (f"  —  {md['db_product']}" if md.get("db_product") else "")))
    if md.get("table_count"):
        rows.append(("Innhold:", f"{_n(md['table_count'])} tabeller, "
                                 f"{_n(md.get('row_count', 0))} rader, "
                                 f"{_n(md.get('lob_file_count', 0))} LOB-filer"
                     + (f", SIARD {md['siard_version']}" if md.get("siard_version") else "")))
    span = md.get("data_origin_time_span") or (
        f"{md['data_start']} – {md['data_end']}" if md.get("data_start") else "")
    dias = data_of("dias_package").get("period")
    if dias:
        rows.append(("Periode:", f"{dias} (DIAS-pakken)"
                     + (f"; metadata.xml: {span}" if span else "")))
    elif span:
        rows.append(("Periode:", f"{span} (metadata.xml)"))
    bc = data_of("blob_convert")
    if bc.get("identified") or bc.get("converted"):
        failed, kept_rule, _ = _split_failures(bc.get("failure_summary") or {})
        if not bc.get("failure_summary"):
            failed = int(bc.get("failed", 0) or 0)
        rows.append(("Dokumenter:", f"{_n(bc.get('converted', 0))} konvertert til PDF/A, "
                                    f"{_n(bc.get('kept', 0))} beholdt i originalformat, "
                                    f"{_n(failed)} feilet"))
    try:
        from version import VERSION
        rows.append(("Program:", f"SIARD Workflow Manager v{VERSION}"))
    except Exception:
        pass
    return rows


def follow_up_items(step_results: list, ctx_results: dict) -> "tuple[list, int]":
    """
    Det leseren bør følge opp etter kjøringen, som korte setninger, og antall
    filer som ikke lot seg konvertere. Brukes på forsiden.
    """
    def data_of(step) -> dict:
        d = step.get("data") or (ctx_results or {}).get(step.get("id")) or {}
        return d if isinstance(d, dict) else {}

    items: list = []
    by_id = {s.get("id"): data_of(s) for s in step_results}
    for s in step_results:
        if not s.get("skipped") and s.get("success") is False:
            what = "ble avbrutt" if s.get("interrupted") else "feilet"
            msg = str(s.get("message") or "")[:200]
            items.append(f"Steget «{s.get('label', '')}» {what}" + (f": {msg}" if msg else ""))
    fs = (by_id.get("blob_convert") or {}).get("failure_summary") or {}
    failed, _kept, kept_keys = _split_failures(fs)
    labels, counts = fs.get("labels") or {}, fs.get("counts") or {}
    for key, n in counts.items():
        if key not in kept_keys:
            items.append(f"{_n(n)} fil(er) ble ikke konvertert og er beholdt i "
                         f"originalformat: {labels.get(key, key)}")
    if counts.get("passordbeskyttet"):
        items.append(f"{_n(counts['passordbeskyttet'])} passordbeskyttet(e) fil(er) kunne "
                     f"ikke åpnes og er beholdt uendret")
    if not fs and (by_id.get("blob_convert") or {}).get("failed"):
        failed = int(by_id["blob_convert"]["failed"])
        items.append(f"{_n(failed)} fil(er) ble ikke konvertert")
    xs = by_id.get("xsd_type_fix") or {}
    if xs.get("unfixed"):
        items.append(f"{_n(xs['unfixed'])} avvik i tableX.xsd krever manuell vurdering")
    md = by_id.get("metadata_extract") or {}
    span = md.get("data_origin_time_span")
    if (span and not md.get("data_start") and not md.get("data_end")
            and not md.get("data_origin_time_span_estimated")):
        from siard_workflow.core.date_span import metadata_span
        try:
            ok = metadata_span(str(span)) is not None
        except Exception:
            ok = True
        if not ok:
            items.append(f"Tidsperioden i metadata.xml («{span}») lar seg ikke tolke som "
                         f"datoer — bør rettes eller estimeres")
    return items, failed


# ─────────────────────────────────────────────────────────────────────────────

class WorkflowReportOperation(BaseOperation):
    """Genererer en PDF-sluttrapport etter gjennomført workflow."""

    operation_id    = "workflow_report"
    label           = "Kjørerapport (PDF)"
    description     = (
        "Genererer en PDF-sluttrapport med oversikt over alle utførte steg, "
        "resultater og grafisk fremstilling av nøkkeltall. "
        "Rapporten lagres i mappen der kilde-SIARD-filen befinner seg. "
        "Legg denne operasjonen sist i workflowen."
    )
    category        = "Rapport"
    status          = 2
    produces_siard  = False
    requires_unpack = False

    default_params = {
        "report_suffix":   "_workflow_rapport",
        "include_charts":  True,
        "include_details": True,
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        siard_path = ctx.siard_path
        if not siard_path:
            return self._fail("Ingen SIARD-fil i kontekst — kan ikke lagre rapport.")

        step_results, history = _collect_steps(ctx, self.operation_id, final=False)

        suffix = (self.params.get("report_suffix") or "_workflow_rapport").strip()
        ts     = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = siard_path.parent / f"{siard_path.stem}{suffix}_{ts}.pdf"

        format_counts: dict = dict(ctx.metadata.get("format_counts", {}))
        # Husk valgene — sluttversjonen (write_final_report) bruker de samme
        ctx.metadata["_report_options"] = {
            "include_charts":  bool(self.params.get("include_charts", True)),
            "include_details": bool(self.params.get("include_details", True)),
            "siard_path":      str(siard_path),
        }

        try:
            _generate_pdf(
                pdf_path        = pdf_path,
                siard_path      = siard_path,
                step_results    = step_results,
                ctx_results     = dict(ctx.results),
                format_counts   = format_counts,
                include_charts  = bool(self.params.get("include_charts", True)),
                include_details = bool(self.params.get("include_details", True)),
                history         = history,
                pending_steps   = list(ctx.metadata.get("steps_after_report") or []),
                provenance      = _provenance(ctx),
            )
        except ImportError:
            return self._fail(
                "reportlab er ikke installert — kan ikke generere PDF-rapport. "
                "Installer med: pip install reportlab")
        except Exception as exc:
            return self._fail(f"Kunne ikke generere rapport: {exc}")

        size_kb = pdf_path.stat().st_size / 1024
        if history and history.get("preliminary"):
            return self._ok(
                data={"report_path": str(pdf_path), "size_kb": round(size_kb, 1),
                      "preliminary": True},
                message=f"Foreløpig rapport lagret: {pdf_path.name}  ({size_kb:.0f} KB) "
                        f"— {history['held']} steg er holdt igjen")
        return self._ok(
            data={"report_path": str(pdf_path), "size_kb": round(size_kb, 1)},
            message=f"Rapport lagret: {pdf_path.name}  ({size_kb:.0f} KB)")


def _collect_steps(ctx, report_id: str, final: bool, outcome: str = "pågår"):
    """
    (steg, historikk) til rapporten. Underveis (final=False) er rapportsteget
    selv og stegene etter det ikke med; sluttversjonen har alle.

    Prosjektet er kjørt før: hele prosessen, ikke bare denne kjøringen. Steg
    som ble hoppet over nå, vises med resultatet fra kjøringen der de kjørte;
    steg som er kjørt på nytt, merkes med forrige status.
    """
    step_results = list(ctx.metadata.get("step_results", []))
    if not final:
        # Rapport-operasjonen er ikke ferdig enda
        step_results = [s for s in step_results if s.get("id") != report_id]
    history = None
    pf = ctx.metadata.get("project_file")
    if pf is not None:
        try:
            idx = (len(pf.operations) if final
                   else _report_index(pf, report_id, step_results))
            step_results, history = project_history(
                pf, ctx.metadata.get("run_id"), step_results, report_index=idx,
                current_result=outcome)
        except Exception:
            history = None
    return step_results, history


def _provenance(ctx) -> list:
    """PREMIS-hendelsene så langt (endringer i uttrekket), eldste først."""
    pl = (ctx.metadata or {}).get("premis_logger")
    if pl is None:
        return []
    try:
        return list(pl.all_events())
    except Exception:
        return []


def write_final_report(ctx, pdf_path: Path, outcome: str = "fullført") -> bool:
    """
    Skriv kjørerapporten på nytt etter siste steg — til samme fil — med alle
    stegene (rapporten lages ellers før DIAS-pakking o.l. og mangler dem).
    """
    opts = (ctx.metadata or {}).get("_report_options") or {}
    step_results, history = _collect_steps(ctx, "workflow_report", final=True,
                                           outcome=outcome)
    siard_path = Path(opts.get("siard_path") or ctx.siard_path)
    tmp = pdf_path.with_suffix(".tmp.pdf")
    _generate_pdf(
        pdf_path        = tmp,
        siard_path      = siard_path,
        step_results    = step_results,
        ctx_results     = dict(ctx.results),
        format_counts   = dict(ctx.metadata.get("format_counts", {})),
        include_charts  = bool(opts.get("include_charts", True)),
        include_details = bool(opts.get("include_details", True)),
        history         = history,
        provenance      = _provenance(ctx),
    )
    tmp.replace(pdf_path)
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Historikk fra prosjektfila (.siardwf)
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_run(iso_or_id: "str | None") -> str:
    if not iso_or_id:
        return ""
    for fmt in ("%Y%m%d_%H%M%S",):
        try:
            return datetime.datetime.strptime(str(iso_or_id)[:15], fmt) \
                .strftime("%d.%m.%Y %H:%M")
        except Exception:
            pass
    try:
        return datetime.datetime.fromisoformat(str(iso_or_id)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(iso_or_id)


def _report_index(pf, report_id: str, step_results: list) -> int:
    """Rapportens plass i arbeidsflyten (steg etter den er ikke kjørt ennå)."""
    n_before = len(step_results)
    ids = [cp.operation_id for cp in pf.operations]
    # Rapporten står etter stegene som er registrert i denne kjøringen
    cands = [i for i, oid in enumerate(ids) if oid == report_id]
    for i in cands:
        if i >= n_before:
            return i
    return cands[-1] if cands else len(ids)


_STATUS_TXT = {
    "completed":   "OK",
    "failed":      "FEIL",
    "interrupted": "Avbrutt",
    "held":        "Holdt igjen",
    "skipped":     "Hoppet over",
    "user_skip":   "Hoppet over (valgt)",
    "pending":     "Ikke kjørt",
}


def project_history(pf, run_id, step_results: list, report_index: int,
                    current_result: str = "pågår"):
    """Slå sammen denne kjøringens steg med prosjektets historikk.

    Returnerer (steg, historikk). Hvert steg får:
      run_label  — «denne kjøringen» eller tidspunktet for kjøringen det er fra
      data       — stegets resultat (fra prosjektfila for steg fra før)
      rerun      — kjørt på nytt nå: {"attempt", "prev_status", "prev_message", "prev_when"}
      held / interrupted
    """
    by_index = {}
    for st in step_results:
        if st.get("index") is not None:
            by_index[int(st["index"])] = st
    merged = []
    reruns = []
    for i, cp in enumerate(pf.operations[:report_index]):
        st = dict(by_index.get(i) or {})
        ran_now = bool(st) and not st.get("skipped")
        if ran_now:
            st["run_label"] = "denne kjøringen"
            prev = (cp.attempts or [])[-1] if cp.attempts else None
            # Et tidligere «forsøk» som aldri kjørte (ventet, holdt igjen) er
            # ikke en omkjøring
            if prev and prev.get("status") in ("completed", "failed", "interrupted"):
                info = {"attempt": cp.attempt_no,
                        "prev_status": _STATUS_TXT.get(prev.get("status"), prev.get("status")),
                        "prev_message": prev.get("message") or "",
                        "prev_when": _fmt_run(prev.get("run_id") or prev.get("ended"))}
                st["rerun"] = info
                reruns.append({"label": cp.label, **info,
                               "now": "OK" if st.get("success") else
                               ("Avbrutt" if st.get("interrupted") else "FEIL"),
                               "message": st.get("message", "")})
        else:
            status = cp.status
            st.setdefault("id", cp.operation_id)
            st.setdefault("label", cp.label)
            st.setdefault("category", st.get("category", ""))
            st["index"] = i
            if status == "completed" and cp.run_id != run_id:
                st.update({"success": True, "skipped": False,
                           "elapsed": float(cp.elapsed or 0.0),
                           "message": cp.message or st.get("message", ""),
                           "data": cp.ctx_data or {},
                           "run_label": _fmt_run(cp.run_id or cp.completed_at)
                           or "tidligere kjøring",
                           "carried": True})
            elif status == "held":
                st.update({"success": None, "skipped": True, "held": True,
                           "message": cp.message or "Holdt igjen",
                           "run_label": "denne kjøringen"})
            else:
                st.setdefault("success", None)
                st.setdefault("skipped", True)
                st.setdefault("message", _STATUS_TXT.get(status, status))
                st.setdefault("run_label", "")
        merged.append(st)

    runs = []
    for r in pf.runs:
        steps = r.get("steps_run") or []
        runs.append({
            "when":   _fmt_run(r.get("run_id") or r.get("started")),
            "ended":  _fmt_run(r.get("ended")) if r.get("ended") else "",
            "result": current_result if r.get("run_id") == run_id
                      else (r.get("result") or "—"),
            "steps":  ", ".join(f"{x.get('label')} ({_STATUS_TXT.get(x.get('status'), x.get('status'))})"
                                for x in steps) or "—",
            "log":    Path(r["log_file"]).name if r.get("log_file") else "",
            "current": r.get("run_id") == run_id,
        })
    held = sum(1 for st in merged if st.get("held"))
    history = {
        "runs": runs,
        "reruns": reruns,
        "held": held,
        "preliminary": held > 0,
        "carried": sum(1 for st in merged if st.get("carried")),
    }
    return merged, history


# ─────────────────────────────────────────────────────────────────────────────
# Hjelpefunksjoner
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_elapsed(secs: float) -> str:
    if secs < 0.01:
        return "< 0,01 s"
    if secs < 60:
        return f"{secs:.1f} s".replace(".", ",")
    m, s = divmod(int(secs), 60)
    if m < 60:
        return f"{m} min {s:02d} s"
    h, m = divmod(m, 60)
    return f"{h} t {m:02d} min {s:02d} s"


def _human_size(n) -> str:
    if n is None:
        return "–"
    try:
        n = float(n)
    except (TypeError, ValueError):
        return str(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def _kv_rows(items: list[tuple[str, str]]) -> list[list[str]]:
    return [[k, str(v)] for k, v in items]


# ─────────────────────────────────────────────────────────────────────────────
# PDF-generering
# ─────────────────────────────────────────────────────────────────────────────

def _generate_pdf(
    pdf_path:        Path,
    siard_path:      Path,
    step_results:    list[dict],
    ctx_results:     dict,
    format_counts:   dict | None = None,
    include_charts:  bool = True,
    include_details: bool = True,
    history:         "dict | None" = None,
    pending_steps:   "list | None" = None,
    provenance:      "list | None" = None,
) -> None:
    """Genererer selve PDF-filen. Krever reportlab.

    pending_steps: steg som kjører etter rapporten (den sier fra om dem).
    provenance: PREMIS-hendelsene — endringene som er gjort i uttrekket.

    history: samlet historikk fra prosjektfila (project_history) — da viser
    rapporten hele prosessen på tvers av kjøringer."""

    # ── Lazy-import av reportlab ──────────────────────────────────────────────
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        PageBreak, HRFlowable,
    )

    # ── Fargepalett ───────────────────────────────────────────────────────────
    C_PRIMARY    = colors.HexColor("#1a3a5c")
    C_ACCENT     = colors.HexColor("#2980b9")
    C_SUCCESS    = colors.HexColor("#27ae60")
    C_ERROR      = colors.HexColor("#c0392b")
    C_WARNING    = colors.HexColor("#e67e22")
    C_SKIP       = colors.HexColor("#7f8c8d")
    C_LIGHT_BG   = colors.HexColor("#f4f6f8")
    C_LIGHT_BLUE = colors.HexColor("#eaf3fb")
    C_BORDER     = colors.HexColor("#d5d8dc")
    C_SUCCESS_BG = colors.HexColor("#d5f5e3")
    C_ERROR_BG   = colors.HexColor("#fadbd8")
    C_SKIP_BG    = colors.HexColor("#eaecee")
    C_WHITE      = colors.white
    C_ALT_ROW    = colors.HexColor("#f0f4f8")

    # ── Beregn samlet status ──────────────────────────────────────────────────
    now          = datetime.datetime.now()
    total_steps  = len(step_results)
    ok_steps     = sum(1 for s in step_results if not s.get("skipped") and s.get("success"))
    fail_steps   = sum(1 for s in step_results if not s.get("skipped") and not s.get("success"))
    skip_steps   = sum(1 for s in step_results if s.get("skipped"))
    run_steps    = total_steps - skip_steps
    held_steps   = sum(1 for s in step_results if s.get("held"))
    overall_ok   = (fail_steps == 0 and run_steps > 0)
    preliminary  = bool(held_steps)
    total_elapsed = sum(s.get("elapsed", 0.0) for s in step_results)
    followups, failed_files = follow_up_items(step_results, ctx_results)

    # ── Dokument-oppsett ──────────────────────────────────────────────────────
    W, H = A4
    inner_w = W - 4 * cm  # tekstbredde

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=2 * cm,  bottomMargin=2 * cm,
        title=f"Workflow-rapport: {siard_path.name}",
        author="SIARD Workflow Manager",
        subject="Kontrollrapport for SIARD-uttrekk",
    )

    # ── Tekststiler ───────────────────────────────────────────────────────────
    S = getSampleStyleSheet()

    def _style(name, parent="Normal", **kw):
        return ParagraphStyle(name=name, parent=S[parent], **kw)

    s_body    = _style("SBody",  fontName="Helvetica",       fontSize=10,
                        textColor=colors.HexColor("#333333"), leading=14, spaceAfter=4)
    s_small   = _style("SSmall", fontName="Helvetica",       fontSize=9,
                        textColor=colors.HexColor("#555555"), leading=12, spaceAfter=2)
    s_section = _style("SSec",   fontName="Helvetica-Bold",  fontSize=13,
                        textColor=C_PRIMARY, spaceBefore=14, spaceAfter=6)
    s_sub     = _style("SSub",   fontName="Helvetica-Bold",  fontSize=10,
                        textColor=C_ACCENT, spaceBefore=8, spaceAfter=4)
    s_cell    = _style("SCell",  fontName="Helvetica",       fontSize=9,
                        textColor=colors.HexColor("#222222"), leading=11)

    def _esc_txt(text) -> str:
        from xml.sax.saxutils import escape
        return escape(str(text or ""))

    s_mono    = _style("SMono",  fontName="Courier",         fontSize=9,
                        textColor=C_PRIMARY, leading=12, spaceAfter=2)
    s_caption = _style("SCap",   fontName="Helvetica-Oblique", fontSize=9,
                        textColor=colors.HexColor("#7f8c8d"),
                        alignment=TA_CENTER, spaceAfter=6)

    # ── Hjelpefunksjon: nøkkel-verdi-tabell ──────────────────────────────────
    def _kv_table(rows: list[tuple[str, str]], col_w=None) -> Table:
        col_w = col_w or [4 * cm, inner_w - 4 * cm]
        tbl = Table([[k, v] for k, v in rows], colWidths=col_w)
        tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, -1), C_LIGHT_BLUE),
            ("FONTNAME",      (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTNAME",      (1, 0), (1, -1), "Helvetica"),
            ("FONTSIZE",      (0, 0), (-1, -1), 9),
            ("TEXTCOLOR",     (0, 0), (-1, -1), C_PRIMARY),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
        ]))
        return tbl

    # ── Hjelpefunksjon: seksjonshode for operasjon ────────────────────────────
    def _step_header(idx: int, step: dict) -> Table:
        label   = step.get("label", f"Steg {idx}")
        skipped = step.get("skipped", False)
        success = step.get("success", False)
        elapsed = step.get("elapsed", 0.0)
        if step.get("held"):
            status_txt = "⏸  Holdt igjen"
            sc = C_SKIP
            bg = C_SKIP_BG
        elif skipped:
            status_txt = "Hoppet over"
            sc = C_SKIP
            bg = C_SKIP_BG
        elif success:
            status_txt = "✓  Vellykket"
            sc = C_SUCCESS
            bg = C_SUCCESS_BG
        elif step.get("interrupted"):
            status_txt = "⏹  Avbrutt"
            sc = C_ERROR
            bg = C_ERROR_BG
        else:
            status_txt = "✗  Feil"
            sc = C_ERROR
            bg = C_ERROR_BG

        dur_txt = "" if skipped else f"  ({_fmt_elapsed(elapsed)})"
        if step.get("carried") and step.get("run_label"):
            dur_txt += f" — {step['run_label']}"
        elif step.get("rerun"):
            dur_txt += f" — kjørt på nytt (forsøk {step['rerun'].get('attempt')})"
        tbl = Table(
            [[f"{idx}. {label}", f"{status_txt}{dur_txt}"]],
            colWidths=[inner_w * 0.50, inner_w * 0.50],
        )
        tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), bg),
            ("FONTNAME",      (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTNAME",      (1, 0), (1, -1), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 10),
            ("TEXTCOLOR",     (0, 0), (0, -1), C_PRIMARY),
            ("TEXTCOLOR",     (1, 0), (1, -1), sc),
            ("ALIGN",         (1, 0), (1, -1), "RIGHT"),
            ("TOPPADDING",    (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ("LEFTPADDING",   (0, 0), (0, -1), 10),
            ("RIGHTPADDING",  (1, 0), (1, -1), 10),
            ("BOX",           (0, 0), (-1, -1), 0.5, sc),
        ]))
        return tbl

    # ── Hjelpefunksjon: kakediagram ───────────────────────────────────────────
    def _pie_chart(labels: list[str], values: list[float],
                   title: str, slice_colors: list) -> "object | None":
        total = sum(values)
        if total <= 0:
            return None
        try:
            from reportlab.graphics.shapes import Drawing, String
            from reportlab.graphics.charts.piecharts import Pie
            from reportlab.graphics import renderPDF  # noqa
        except ImportError:
            return None

        # Drawing er høyere enn selve kakediagrammet for å gi plass til etiketter
        # og unngå overlapping med tekst over/under.
        dw, dh = 300, 210
        d = Drawing(dw, dh)

        pie_size = 110
        pie_x    = 20
        pie_y    = 30   # bunn-margin for etiketter under kaken
        title_y  = dh - 14

        # Tittel øverst i tegneflaten
        d.add(String(dw / 2, title_y, title,
                     fontName="Helvetica-Bold", fontSize=9,
                     fillColor=C_PRIMARY, textAnchor="middle"))

        pie = Pie()
        pie.x      = pie_x
        pie.y      = pie_y
        pie.width  = pie_size
        pie.height = pie_size
        pie.data   = values
        # Antall først — små andeler («Feil: 0.00%») sier ellers ingenting
        pie.labels = [f"{l}: {_n(v)} (" + f"{v / total * 100:.1f}".replace(".", ",") + " %)"
                      for l, v in zip(labels, values)]
        pie.simpleLabels        = False
        pie.checkLabelOverlap   = True
        pie.slices.strokeWidth  = 0.5
        pie.slices.strokeColor  = C_WHITE
        pie.slices.labelRadius  = 1.35
        pie.slices.fontSize     = 7.5
        for i, c in enumerate(slice_colors):
            pie.slices[i].fillColor = c
        d.add(pie)
        return d

    # ─────────────────────────────────────────────────────────────────────────
    # Bygg innhold
    # ─────────────────────────────────────────────────────────────────────────
    story = []

    # ══ FORSIDE ═══════════════════════════════════════════════════════════════
    story.append(Spacer(1, 1.0 * cm))

    # Topptittel-boks
    hdr = Table([["KONTROLLRAPPORT"]], colWidths=[inner_w])
    hdr.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), C_PRIMARY),
        ("TEXTCOLOR",     (0, 0), (-1, -1), C_WHITE),
        ("FONTNAME",      (0, 0), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 26),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 22),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 22),
    ]))
    story.append(hdr)

    story.append(Spacer(1, 0.3 * cm))
    story.append(Paragraph(
        "SIARD Workflow Manager  —  Automatisert kontrollrapport",
        _style("cover_sub", fontName="Helvetica", fontSize=11,
               textColor=C_ACCENT, alignment=TA_CENTER)))
    story.append(Spacer(1, 1.5 * cm))

    # Filinfo-tabell
    fi_rows = [
        ["Fil:",      siard_path.name],
        ["Mappe:",    str(siard_path.parent)],
        ["Kjørt:",    now.strftime("%d.%m.%Y  kl. %H:%M:%S")],
        ["Varighet:", _fmt_elapsed(total_elapsed)
                      + ("  (sum for stegene, alle kjøringer)" if history and
                         len(history.get("runs") or []) > 1 else "")],
    ]
    if history and len(history.get("runs") or []) > 1:
        fi_rows.append(["Kjøringer:",
                        f"{len(history['runs'])}  (første {history['runs'][0]['when']})"])
    fi_tbl = Table(fi_rows, colWidths=[3 * cm, inner_w - 3 * cm])
    fi_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (0, -1), C_LIGHT_BLUE),
        ("BACKGROUND",    (1, 0), (1, -1), C_LIGHT_BG),
        ("FONTNAME",      (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME",      (1, 0), (1, -1), "Helvetica"),
        ("FONTSIZE",      (0, 0), (-1, -1), 10),
        ("TEXTCOLOR",     (0, 0), (-1, -1), C_PRIMARY),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
    ]))
    story.append(fi_tbl)

    facts = key_facts(step_results, ctx_results, siard_path)
    if facts:
        story.append(Spacer(1, 0.5 * cm))
        kf_tbl = Table([[k, Paragraph(_esc_txt(v), s_cell)] for k, v in facts],
                       colWidths=[3 * cm, inner_w - 3 * cm])
        kf_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, -1), C_LIGHT_BLUE),
            ("FONTNAME",      (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 9),
            ("TEXTCOLOR",     (0, 0), (-1, -1), C_PRIMARY),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 10),
            ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
        ]))
        story.append(kf_tbl)
    story.append(Spacer(1, 0.8 * cm))

    # Samlet status-boks
    if total_steps > 0:
        stat_txt  = "GODKJENT" if overall_ok else "FEIL OPPDAGET"
        stat_sym  = "✓" if overall_ok else "✗"
        stat_col  = C_SUCCESS if overall_ok else C_ERROR
        stat_bg   = C_SUCCESS_BG if overall_ok else C_ERROR_BG
        if preliminary and overall_ok:
            stat_txt, stat_sym = "FORELØPIG", "⏸"
            stat_col, stat_bg = C_SKIP, C_SKIP_BG
        elif overall_ok and (failed_files or followups):
            # Stegene gikk gjennom, men noe må følges opp
            stat_txt, stat_sym = "GODKJENT MED AVVIK", "!"
            stat_col, stat_bg = C_WARNING, colors.HexColor("#fdebd0")
        sum_txt   = (f"{ok_steps} av {run_steps} steg OK"
                     + (f"  •  {_n(failed_files)} fil(er) ikke konvertert"
                        if failed_files else "")
                     + (f"  •  {held_steps} holdt igjen" if held_steps else "")
                     + (f"  •  {skip_steps - held_steps} hoppet over"
                        if skip_steps - held_steps else "")
                     + (f"  •  {fail_steps} FEIL" if fail_steps else ""))

        st_tbl = Table(
            [[f"{stat_sym}  {stat_txt}", sum_txt]],
            colWidths=[inner_w * 0.44, inner_w * 0.56],
        )
        st_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, -1), stat_bg),
            ("BACKGROUND",    (1, 0), (1, -1), C_LIGHT_BG),
            ("FONTNAME",      (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTNAME",      (1, 0), (1, -1), "Helvetica"),
            ("FONTSIZE",      (0, 0), (0, -1), 15 if len(stat_txt) > 12 else 17),
            ("FONTSIZE",      (1, 0), (1, -1), 11),
            ("TEXTCOLOR",     (0, 0), (0, -1), stat_col),
            ("TEXTCOLOR",     (1, 0), (1, -1), C_PRIMARY),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",    (0, 0), (-1, -1), 16),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 16),
            ("BOX",           (0, 0), (-1, -1), 1.2, stat_col),
            ("LINEAFTER",     (0, 0), (0, -1), 0.5, C_BORDER),
        ]))
        story.append(st_tbl)

    # Det som bør følges opp — kort, på forsiden
    if followups:
        from xml.sax.saxutils import escape as _esc
        story.append(Spacer(1, 0.7 * cm))
        fu_rows = [[Paragraph("<b>Dette bør følges opp</b>", s_body)]]
        for item in followups[:12]:
            fu_rows.append([Paragraph("•  " + _esc(item), s_body)])
        if len(followups) > 12:
            fu_rows.append([Paragraph(f"… og {len(followups) - 12} punkt til "
                                      f"(se detaljene under)", s_small)])
        fu_tbl = Table(fu_rows, colWidths=[inner_w])
        fu_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#fef9e7")),
            ("BOX",           (0, 0), (-1, -1), 0.8, C_WARNING),
            ("LEFTPADDING",   (0, 0), (-1, -1), 10),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        story.append(fu_tbl)

    story.append(PageBreak())

    # ══ OPPSUMMERINGSTABELL ═══════════════════════════════════════════════════
    story.append(Paragraph("Oversikt over kjørte steg", s_section))
    story.append(HRFlowable(width="100%", thickness=1.5, color=C_ACCENT,
                             spaceAfter=6))

    if not step_results:
        story.append(Paragraph(
            "Ingen steg ble registrert. Kontroller at operasjoner "
            "er lagt til i workflowen.", s_body))
    else:
        if history:
            col_w = [0.8 * cm, 5.0 * cm, 2.6 * cm, 2.5 * cm, 2.2 * cm, 3.1 * cm]
            tbl_rows = [["#", "Operasjon", "Kategori", "Status", "Varighet", "Kjøring"]]
        else:
            col_w = [0.9 * cm, 6.2 * cm, 2.9 * cm, 2.8 * cm, 2.4 * cm]
            tbl_rows = [["#", "Operasjon", "Kategori", "Status", "Varighet"]]
        for idx, step in enumerate(step_results, 1):
            skipped = step.get("skipped", False)
            success = step.get("success", False)
            label   = step.get("label", f"Steg {idx}")
            cat     = step.get("category", "")
            elapsed = step.get("elapsed", 0.0)
            if step.get("held"):
                st = "Holdt igjen"
            elif skipped:
                st = "Hoppet over"
            elif success:
                st = "OK" + (f" (forsøk {step['rerun']['attempt']})"
                             if step.get("rerun") else "")
            elif step.get("interrupted"):
                st = "Avbrutt"
            else:
                st = "FEIL"
            # Paragraph i tekstcellene: lange navn brytes i stedet for å
            # skrive over nabocellen
            row = [str(idx), Paragraph(_esc_txt(label), s_cell),
                   Paragraph(_esc_txt(cat), s_cell), st,
                   _fmt_elapsed(elapsed) if not skipped else "—"]
            if history:
                row.append(Paragraph(_esc_txt(step.get("run_label") or "—"), s_cell))
            tbl_rows.append(row)

        sum_tbl = Table(tbl_rows, colWidths=col_w)
        style_cmds = [
            # Topprad
            ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
            ("TEXTCOLOR",     (0, 0), (-1, 0), C_WHITE),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, 0), 9),
            ("ALIGN",         (0, 0), (-1, 0), "CENTER"),
            # Innhold
            ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",      (0, 1), (-1, -1), 9),
            ("ALIGN",         (0, 1), (0, -1), "CENTER"),
            ("ALIGN",         (3, 1), (4, -1), "CENTER"),
            # Gitter
            ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]
        for i, step in enumerate(step_results, 1):
            skipped = step.get("skipped", False)
            success = step.get("success", False)
            if skipped:
                style_cmds += [
                    ("BACKGROUND", (0, i), (-1, i), C_SKIP_BG),
                    ("TEXTCOLOR",  (3, i), (3, i), C_SKIP),
                ]
            elif success:
                style_cmds += [
                    ("BACKGROUND", (0, i), (-1, i), C_SUCCESS_BG),
                    ("TEXTCOLOR",  (3, i), (3, i), C_SUCCESS),
                    ("FONTNAME",   (3, i), (3, i), "Helvetica-Bold"),
                ]
            else:
                style_cmds += [
                    ("BACKGROUND", (0, i), (-1, i), C_ERROR_BG),
                    ("TEXTCOLOR",  (3, i), (3, i), C_ERROR),
                    ("FONTNAME",   (3, i), (3, i), "Helvetica-Bold"),
                ]
        sum_tbl.setStyle(TableStyle(style_cmds))
        story.append(sum_tbl)
        if pending_steps:
            story.append(Spacer(1, 0.2 * cm))
            story.append(Paragraph(
                "<b>Rapporten er laget før disse stegene var kjørt:</b> "
                + _esc_txt(", ".join(pending_steps))
                + ". Den skrives på nytt med alle stegene når kjøringen er ferdig.",
                s_small))

    # ══ KJØRINGER (prosjektet er kjørt flere ganger) ══════════════════════════
    if history and (len(history.get("runs") or []) > 1 or history.get("reruns")):
        story.append(Spacer(1, 0.6 * cm))
        story.append(Paragraph("Kjøringer", s_section))
        story.append(HRFlowable(width="100%", thickness=1.5, color=C_ACCENT,
                                 spaceAfter=6))
        story.append(Paragraph(
            "Prosessen er kjørt i flere omganger. Steg som var fullført, ble "
            "hoppet over i senere kjøringer; tabellen over viser hvert stegs "
            "siste resultat og hvilken kjøring det er fra.", s_body))
        if preliminary:
            story.append(Paragraph(
                f"<b>Foreløpig rapport:</b> {held_steps} steg er holdt igjen fordi "
                f"et steg før dem feilet eller ble avbrutt. Kjør arbeidsflyten "
                f"igjen når feilen er rettet — da kjøres bare det som gjenstår.",
                s_body))
        story.append(Spacer(1, 0.2 * cm))
        r_rows = [["Startet", "Resultat", "Steg kjørt", "Logg"]]
        for r in history.get("runs") or []:
            r_rows.append([
                Paragraph(r["when"] + ("<br/>(denne kjøringen)" if r.get("current") else ""),
                          s_small),
                Paragraph(_esc_txt(r["result"]), s_small),
                Paragraph(r["steps"], s_small),
                Paragraph(r["log"] or "—", s_small),
            ])
        r_tbl = Table(r_rows, colWidths=[3.0 * cm, 2.0 * cm, 6.6 * cm, 3.6 * cm],
                      repeatRows=1)
        r_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
            ("TEXTCOLOR",     (0, 0), (-1, 0), C_WHITE),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(r_tbl)
        if history.get("reruns"):
            story.append(Spacer(1, 0.4 * cm))
            story.append(Paragraph("Kjørt på nytt i denne kjøringen", s_sub))
            rr_rows = [["Steg", "Forrige forsøk", "Nå"]]
            for x in history["reruns"]:
                prev = f"{x['prev_status']} {x['prev_when']}".strip()
                if x.get("prev_message"):
                    prev += f" — {x['prev_message'][:160]}"
                rr_rows.append([Paragraph(x["label"], s_small),
                                Paragraph(prev, s_small),
                                Paragraph(f"{x['now']} (forsøk {x['attempt']})", s_small)])
            rr_tbl = Table(rr_rows, colWidths=[4.0 * cm, 8.0 * cm, 3.2 * cm], repeatRows=1)
            rr_tbl.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
                ("TEXTCOLOR",     (0, 0), (-1, 0), C_WHITE),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
                ("VALIGN",        (0, 0), (-1, -1), "TOP"),
                ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
            ]))
            story.append(rr_tbl)

    # ══ ENDRINGER I UTTREKKET (PREMIS-hendelsene) ═════════════════════════════
    if provenance:
        story.append(Spacer(1, 0.6 * cm))
        story.append(Paragraph("Endringer i uttrekket", s_section))
        story.append(HRFlowable(width="100%", thickness=1.5, color=C_ACCENT,
                                 spaceAfter=6))
        story.append(Paragraph(
            "Hver operasjon som endrer innholdet, er registrert som en "
            "PREMIS-hendelse. Den fullstendige proveniensen ligger i "
            "PREMIS-fila ved siden av uttrekket.", s_body))
        p_rows = [["Tid", "Hendelse", "Beskrivelse"]]
        for ev in provenance[:60]:
            when = str(ev.get("datetime") or "")[:16].replace("T", " ")
            what = ev.get("label") or ev.get("type") or ""
            detail = str(ev.get("detail") or "")
            if len(detail) > 420:
                detail = detail[:420].rstrip() + " …"
            if not ev.get("success", True):
                what += " (feilet)"
            p_rows.append([Paragraph(_esc_txt(when), s_small),
                           Paragraph(_esc_txt(what), s_small),
                           Paragraph(_esc_txt(detail), s_small)])
        p_tbl = Table(p_rows, colWidths=[2.7 * cm, 4.6 * cm, inner_w - 7.3 * cm],
                      repeatRows=1)
        p_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
            ("TEXTCOLOR",     (0, 0), (-1, 0), C_WHITE),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        story.append(p_tbl)
        if len(provenance) > 60:
            story.append(Paragraph(f"… og {len(provenance) - 60} hendelse(r) til "
                                   f"(se PREMIS-fila).", s_small))

    # ══ FILTYPE-DIAGRAM ═══════════════════════════════════════════════════════
    if format_counts:
        all_sorted = sorted(format_counts.items(), key=lambda x: -x[1])
        total_files = sum(v for _, v in all_sorted)

        if total_files > 0:
            story.append(Spacer(1, 0.8 * cm))
            story.append(Paragraph("Identifiserte filtyper", s_section))
            story.append(HRFlowable(width="100%", thickness=1.5, color=C_ACCENT,
                                    spaceAfter=6))
            n_types = len(all_sorted)
            _ident = ((ctx_results or {}).get("blob_convert") or {})
            _ident = _ident.get("identified", 0) if isinstance(_ident, dict) else 0
            _rest = int(_ident or 0) - total_files
            story.append(Paragraph(
                f"Totalt {_n(total_files)} fil(er) fordelt på {n_types} format(er), "
                f"slik de ble identifisert før konverteringen."
                + (f" {_n(_rest)} fil(er) til (arkiver og andre filer som behandles "
                   f"for seg) er ikke med i fordelingen." if _rest > 0 else ""),
                s_body))
            story.append(Spacer(1, 0.4 * cm))

            # Farger per filtype
            _FC = {
                "pdf": "#e74c3c", "pdfa": "#e74c3c",
                "doc": "#2980b9", "docx": "#2980b9",
                "odt": "#5dade2", "rtf": "#85c1e9",
                "xls": "#27ae60", "xlsx": "#27ae60", "ods": "#52be80",
                "csv": "#a9dfbf",
                "ppt": "#e67e22", "pptx": "#e67e22", "odp": "#f0b27a",
                "jpg": "#8e44ad", "jpeg": "#8e44ad", "png": "#9b59b6",
                "tif": "#af7ac5", "tiff": "#af7ac5", "bmp": "#c39bd3",
                "gif": "#d2b4de", "jp2": "#6c3483",
                "mp3": "#17a589", "wav": "#1abc9c", "flac": "#76d7c4",
                "mp4": "#2471a3", "mpg": "#2e86c1", "avi": "#5dade2",
                "xml": "#f1c40f", "json": "#f4d03f", "html": "#f9e79f",
                "txt": "#aab7b8", "msg": "#717d7e", "eml": "#717d7e",
                "zip": "#7f8c8d", "7z": "#7f8c8d", "rar": "#7f8c8d",
                "bin": "#566573", "ingen": "#4d5656",
            }
            _DEF_CLR = "#4f8ef7"
            _ANDRE_CLR = "#95a5a6"

            def _hex(ext):
                return _FC.get(ext.lower(), _DEF_CLR)

            # ── Kakediagram: topp 8 + "Andre" ────────────────────────────────
            if include_charts:
                PIE_TOP = 8
                pie_slices = all_sorted[:PIE_TOP]
                andre_count = sum(v for _, v in all_sorted[PIE_TOP:])

                pie_labels  = [f".{e}" if e != "ingen" else "(ingen)"
                               for e, _ in pie_slices]
                pie_values  = [float(v) for _, v in pie_slices]
                pie_colors  = [colors.HexColor(_hex(e)) for e, _ in pie_slices]

                if andre_count > 0:
                    pie_labels.append(f"Andre ({n_types - PIE_TOP})")
                    pie_values.append(float(andre_count))
                    pie_colors.append(colors.HexColor(_ANDRE_CLR))

                pie = _pie_chart(pie_labels, pie_values,
                                 "Fordeling av filtyper", pie_colors)
                if pie:
                    story.append(pie)
                    story.append(Spacer(1, 0.5 * cm))

            # ── Tabell: alle filtyper med stolpe ─────────────────────────────
            show_fc = all_sorted[:25]
            max_cnt = show_fc[0][1] if show_fc else 1

            BAR_W = inner_w * 0.38
            ext_col = 2.0 * cm
            bar_col = BAR_W + 0.2 * cm
            cnt_col = 1.6 * cm
            pct_col = 1.5 * cm

            bar_rows  = [["Format", "", "Antall", "Andel"]]
            bar_style = [
                ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
                ("TEXTCOLOR",     (0, 0), (-1, 0), C_WHITE),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 8),
                ("ALIGN",         (0, 0), (-1, 0),  "CENTER"),
                ("ALIGN",         (0, 1), (0, -1),  "RIGHT"),
                ("ALIGN",         (2, 1), (3, -1),  "RIGHT"),
                ("TOPPADDING",    (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("LEFTPADDING",   (0, 0), (-1, -1), 5),
                ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
                ("GRID",          (0, 0), (-1, -1), 0.3,
                 colors.HexColor("#c8d6e5")),
            ]

            for ri, (ext, cnt) in enumerate(show_fc, 1):
                pct   = cnt / total_files * 100
                bar_w = max(1, BAR_W * cnt / max_cnt)
                clr   = colors.HexColor(_hex(ext))

                bar_cell = Table([[""]], colWidths=[bar_w], rowHeights=[10])
                bar_cell.setStyle(TableStyle([
                    ("BACKGROUND",    (0, 0), (0, 0), clr),
                    ("TOPPADDING",    (0, 0), (0, 0), 0),
                    ("BOTTOMPADDING", (0, 0), (0, 0), 0),
                    ("LEFTPADDING",   (0, 0), (0, 0), 0),
                    ("RIGHTPADDING",  (0, 0), (0, 0), 0),
                ]))
                bar_rows.append([
                    f".{ext}" if ext != "ingen" else "(ingen)",
                    bar_cell,
                    f"{_n(cnt)}",
                    f"{pct:.1f} %",
                ])
                bg = C_ALT_ROW if ri % 2 == 0 else C_WHITE
                bar_style += [
                    ("BACKGROUND", (0, ri), (0, ri), clr),
                    ("TEXTCOLOR",  (0, ri), (0, ri), C_WHITE),
                    ("BACKGROUND", (1, ri), (3, ri), bg),
                ]

            bt = Table(bar_rows, colWidths=[ext_col, bar_col, cnt_col, pct_col])
            bt.setStyle(TableStyle(bar_style))
            story.append(bt)

            if n_types > 25:
                story.append(Spacer(1, 0.2 * cm))
                story.append(Paragraph(
                    f"Viser 25 av {n_types} filtyper. "
                    "Stolpe-bredde er relativ til den mest vanlige filtypen.",
                    s_caption))
            else:
                story.append(Spacer(1, 0.2 * cm))
                story.append(Paragraph(
                    "Stolpe-bredde er relativ til den mest vanlige filtypen.",
                    s_caption))

    # ══ DETALJSEKSJONER ═══════════════════════════════════════════════════════
    if include_details and step_results:
        story.append(Spacer(1, 0.8 * cm))
        story.append(Paragraph("Detaljerte resultater", s_section))
        story.append(HRFlowable(width="100%", thickness=1.5, color=C_ACCENT,
                                 spaceAfter=4))

        for idx, step in enumerate(step_results, 1):
            op_id   = step.get("id", "")
            skipped = step.get("skipped", False)
            message = step.get("message", "")
            op_data = step.get("data") or ctx_results.get(op_id) or {}
            if op_id == "blob_convert" and isinstance(op_data, dict) and format_counts:
                op_data = {**op_data, "_format_counts": dict(format_counts)}
            # Operasjoner kan legge en enkeltverdi under sin egen id (SHA-256
            # legger sjekksummen der før resultatet er klart) — tål det
            if not isinstance(op_data, dict):
                op_data = {"sha256": op_data} if op_id == "sha256" \
                    and isinstance(op_data, str) else {}

            story.append(Spacer(1, 0.35 * cm))
            story.append(_step_header(idx, step))

            if step.get("rerun"):
                _rr = step["rerun"]
                story.append(Paragraph(
                    f"Kjørt på nytt (forsøk {_rr.get('attempt')}). Forrige forsøk "
                    f"{_rr.get('prev_when', '')}: {_rr.get('prev_status', '')}"
                    + (f" — {_rr.get('prev_message')[:200]}" if _rr.get('prev_message') else ""),
                    _style("rerun_style", fontName="Helvetica-Oblique",
                           fontSize=9, textColor=colors.HexColor("#7a5a00"),
                           leftIndent=10, spaceAfter=2)))
            if message:
                story.append(Paragraph(
                    f"Melding: {_esc_txt(message)}",
                    _style("msg_style", fontName="Helvetica-Oblique",
                           fontSize=9, textColor=colors.HexColor("#444444"),
                           leftIndent=10, spaceAfter=4)))

            if skipped or not op_data:
                continue

            # ── Operasjonsspesifikk innhold ────────────────────────────────
            detail = _op_detail(
                op_id, op_data, step, include_charts,
                inner_w, _kv_table, s_sub, s_body, s_small, s_mono, s_caption,
                C_PRIMARY, C_SUCCESS, C_ERROR, C_WARNING, C_SKIP,
                C_SUCCESS_BG, C_ERROR_BG, C_BORDER, C_WHITE,
                _pie_chart,
            )
            story.extend(detail)

    # ══ FOTNOTE / SIDEHODE ════════════════════════════════════════════════════
    def _header_footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#999999"))
        y_foot = 1.0 * cm
        canvas.drawString(2 * cm, y_foot,
                          f"SIARD Workflow Manager  —  {siard_path.name}")
        canvas.drawRightString(W - 2 * cm, y_foot, f"Side {doc.page}")
        if doc.page > 1:
            canvas.setStrokeColor(colors.HexColor("#dddddd"))
            canvas.setLineWidth(0.5)
            canvas.line(2 * cm, H - 1.5 * cm, W - 2 * cm, H - 1.5 * cm)
        canvas.restoreState()

    doc.build(story,
              onFirstPage=_header_footer,
              onLaterPages=_header_footer)


# ─────────────────────────────────────────────────────────────────────────────
# Filer som ikke ble konvertert (BLOB-konvertering)
# ─────────────────────────────────────────────────────────────────────────────

def _date_span_section(ds: dict, inner_w, _kv_table, s_sub, s_small,
                       C_PRIMARY, C_BORDER, C_WHITE, C_WARNING) -> list:
    """Datospenn (date_span): fra metadata.xml, eller estimat — tydelig merket som estimat."""
    from xml.sax.saxutils import escape
    from reportlab.lib import colors as rl_colors
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle

    note = ParagraphStyle("DsNote", parent=s_small, textColor=C_WARNING, fontName="Helvetica-Oblique")
    kv_val = ParagraphStyle("DsKv", parent=s_small, fontSize=9, leading=11,
                            textColor=C_PRIMARY, spaceAfter=0)

    def _kv(rows: list) -> object:
        """kv-tabell der lange verdier brytes (rene strenger brytes ikke i Table)."""
        return _kv_table([(k, Paragraph(escape(v), kv_val) if len(v) > 60 else v)
                          for k, v in rows], col_w=[4.4 * cm, inner_w - 4.4 * cm])
    if ds and ds.get("source") == "metadata":
        out: list = [Paragraph("Datospenn for uttrekket (fra metadata.xml)", s_sub)]
        out.append(_kv([
            ("Fra–til:", f"{ds['from_year']}–{ds['to_year']}  "
                         f"({ds.get('from_date', '')} – {ds.get('to_date', '')})"),
            ("Kilde:", f"metadata.xml <dataOriginTimespan> «{ds.get('metadata_timespan', '')}»"),
            ("Estimert:", "JA — verdien ble estimert ut fra uttrekket ved en tidligere "
                          "kjøring (se XML-kommentar i metadata.xml); ikke nødvendigvis "
                          "100 % korrekt" if ds.get("metadata_estimated")
             else "nei — metadata.xml oppgir spennet"),
            ("DIAS-pakke:", "brukes som periode (STARTDATE/ENDDATE)"),
        ]))
        return out
    out = [Paragraph("Estimert datospenn for uttrekket", s_sub)]
    if ds and ds.get("found") and ds.get("accepted") is False:
        out.append(_kv([
            ("Estimert fra–til:", f"{ds['from_year']}–{ds['to_year']}  "
                                  f"({ds.get('from_date', '')} – {ds.get('to_date', '')})"),
            ("Grunnlag:", ds.get("basis_text", "")),
            ("Brukt:", f"NEI — forkastet ({ds.get('accept_reason', '')}); ikke skrevet til "
                       "metadata.xml, ikke brukt i DIAS-pakken"),
        ]))
        return out
    if not ds or not ds.get("found"):
        out.append(Paragraph(escape(ds.get("message", "Ingen datoer funnet.") if ds else
                                    "Ingen resultat."), s_small))
        return out
    rows = [
        ("Estimert fra–til:", f"{ds['from_year']}–{ds['to_year']}  "
                              f"({ds.get('from_date', '')} – {ds.get('to_date', '')})"),
        ("Grunnlag:", ds.get("basis_text", "")),
        ("Sikkerhet:", ds.get("confidence", "")),
    ]
    if ds.get("absolute_first"):
        rows.append(("Ytterpunkter:", f"{ds['absolute_first']} – {ds['absolute_last']}"
                     + (f" ({_n(ds['outside_trim'])} verdier regnet som avvik)"
                        if ds.get("outside_trim") else "")))
    rows.append(("metadata.xml:", f"«{ds['metadata_timespan']}» — ikke et tolkbart datospenn"
                 if ds.get("metadata_timespan") else "oppga ikke datospenn"))
    if ds.get("written_to_metadata"):
        rows.append(("Skrevet til metadata.xml:",
                     f"dataOriginTimespan = «{ds.get('written_value', '')}», "
                     "med XML-kommentar om at verdien er estimert"))
    for t in ds.get("tiers_tried", []):
        rows.append(("Prøvd først:", t))
    rows.append(("DIAS-pakke:", "brukes som periode (STARTDATE/ENDDATE)"
                 if ds.get("accepted", ds.get("reliable", True))
                 else "brukes ikke — estimatet er bare veiledende"))
    out.append(_kv(rows))
    out.append(Spacer(1, 0.15 * cm))
    out.append(Paragraph(escape(ds.get("note", "Estimat — ikke nødvendigvis 100 % korrekt.")), note))
    tabs = ds.get("tables", [])[:40]
    if tabs:
        cell = ParagraphStyle("DsTCell", parent=s_small, fontSize=7.5, leading=9, spaceAfter=0)
        head = ParagraphStyle("DsTHead", parent=cell, fontName="Helvetica-Bold", textColor=C_WHITE)
        warn = ParagraphStyle("DsTWarn", parent=cell, textColor=C_WARNING, fontName="Helvetica-Bold")
        data = [[Paragraph(h, head) for h in ("Tabell", "Hovedkolonne", "Datoer",
                                              "Estimert fra–til", "Merknad")]]
        for t in tabs:
            span = f"{t['from_year']}–{t['to_year']}" if t.get("from_year") else "–"
            # Et smalere spenn innenfor estimatet er ikke et avvik
            _outside = bool(t.get("deviates") and t.get("from_year")
                            and ds.get("from_year") and ds.get("to_year")
                            and (t["from_year"] < ds["from_year"]
                                 or t["to_year"] > ds["to_year"]))
            note_txt = ("AVVIKER — holdt utenfor estimatet" if t.get("off")
                        else "går utenfor estimatet" if _outside
                        else "dekker en del av perioden" if t.get("deviates")
                        else "brukt" + (" (rader med LOB-fil)" if t.get("lob_rows") else ""))
            st = warn if (t.get("off") or _outside) else cell
            data.append([Paragraph(escape(t["table"]), cell), Paragraph(escape(t["column"]), cell),
                         Paragraph(_n(t['n']), cell),
                         Paragraph(span, st), Paragraph(escape(note_txt), st)])
        tt = Table(data, colWidths=[inner_w * 0.30, inner_w * 0.22, inner_w * 0.11,
                                    inner_w * 0.15, inner_w * 0.22], repeatRows=1)
        tt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), C_PRIMARY), ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.5, C_BORDER),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.HexColor("#f8f9fa"), C_WHITE]),
        ]))
        out.append(Spacer(1, 0.2 * cm))
        out.append(Paragraph("Tabeller i estimatet — estimert fra–til per tabell", s_small))
        out.append(tt)
    cols = [c for c in ds.get("columns", []) if c.get("excluded")][:15]
    if cols:
        cell = ParagraphStyle("DsCell", parent=s_small, fontSize=7.5, leading=9, spaceAfter=0)
        head = ParagraphStyle("DsHead", parent=cell, fontName="Helvetica-Bold", textColor=C_WHITE)
        data = [[Paragraph(h, head) for h in ("Tabell.kolonne", "Datoer", "I LOB-rader",
                                              "År", "Merknad")]]
        for c in cols:
            data.append([Paragraph(escape(f"{c['table']}.{c['column']}"), cell),
                         Paragraph(_n(c['n']), cell),
                         Paragraph(_n(c['n_lob']), cell),
                         Paragraph(escape(c.get("span", "")), cell),
                         Paragraph(escape("utelatt: " + c["excluded"] if c.get("excluded")
                                          else "brukt (rader med LOB-fil)" if c.get("lob_rows")
                                          else "brukt (alle rader)"), cell)])
        t = Table(data, colWidths=[inner_w * 0.36, inner_w * 0.11, inner_w * 0.12,
                                   inner_w * 0.13, inner_w * 0.28], repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), C_PRIMARY), ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.5, C_BORDER),
            ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.HexColor("#f8f9fa"), C_WHITE]),
        ]))
        out.append(Spacer(1, 0.2 * cm))
        out.append(Paragraph("Utelatte datokolonner", s_small))
        out.append(t)
    return out


MAX_FAILURE_ROWS = 300          # resten står i feillista (CSV)

# Støy fra LibreOffice sin innebygde Python i feiltekster lagret av eldre
# versjoner (nye kjøringer filtrerer den bort før den lagres)
import re as _re
_NOISE_RE = _re.compile(r"Could not find platform (?:in)?dependent libraries <\w+>\s*",
                        _re.IGNORECASE)


def format_changes(format_counts: dict, to_pdfa: dict, fs: dict) -> list:
    """
    [(format, antall før, hva som skjedde)] — formatene slik de ble identifisert
    og hva de er etter konverteringen. Tom uten opplysning om hva som ble
    sendt til konvertering (eldre prosjektfiler).
    """
    if not format_counts or not to_pdfa:
        return []
    kept_keys = set((fs or {}).get("kept_keys") or _KEPT_DEFAULT)
    failed_by: dict = {}
    for e in (fs or {}).get("entries") or []:
        if e.get("kategori") not in kept_keys:
            f = str(e.get("format") or "").lower().lstrip(".")
            failed_by[f] = failed_by.get(f, 0) + 1
    rows = []
    for ext, n in sorted(format_counts.items(), key=lambda x: -x[1]):
        key = str(ext).lower().lstrip(".")
        sent = int(to_pdfa.get(key, 0))
        if not sent:
            rows.append((f".{key}", n, "beholdt i originalformat"))
            continue
        failed = min(sent, failed_by.get(key, 0))
        parts = [f"{_n(sent - failed)} konvertert til PDF/A"]
        if failed:
            parts.append(f"{_n(failed)} feilet (beholdt)")
        if n - sent > 0:
            parts.append(f"{_n(n - sent)} beholdt")
        rows.append((f".{key}", n, ", ".join(parts)))
    return rows


def _format_change_section(format_counts, to_pdfa, fs, inner_w, s_sub, s_small,
                           C_PRIMARY, C_BORDER, C_WHITE) -> list:
    from reportlab.lib import colors as rl_colors
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle
    rows = format_changes(format_counts, to_pdfa, fs)
    if not rows:
        return []
    data = [["Format før", "Antall", "Etter konverteringen"]]
    data += [[ext, _n(n), Paragraph(txt, s_small)] for ext, n, txt in rows]
    tbl = Table(data, colWidths=[inner_w * 0.18, inner_w * 0.16, inner_w * 0.66],
                repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
        ("TEXTCOLOR",     (0, 0), (-1, 0), C_WHITE),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("ALIGN",         (1, 0), (1, -1), "RIGHT"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.HexColor("#f8f9fa"), C_WHITE]),
    ]))
    return [Paragraph("Formater før og etter konverteringen", s_sub), tbl,
            Spacer(1, 6)]


def _failure_section(fs: dict, inner_w, _kv_table, s_sub, s_small,
                     C_PRIMARY, C_BORDER, C_WHITE) -> list:
    """Opptelling per årsak + tabell over filene (fra ConversionErrorLogger.summary)."""
    from xml.sax.saxutils import escape
    from reportlab.lib import colors as rl_colors
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle

    labels = fs.get("labels", {})
    counts = fs.get("counts", {})
    total = int(fs.get("total", 0))
    n_failed, n_kept, kept_keys = _split_failures(fs)
    out: list = []
    if n_failed:
        out.append(Paragraph(
            f"Filer som ikke lot seg konvertere — bør følges opp ({_n(n_failed)})", s_sub))
        out.append(_kv_table(
            [(f"{labels.get(k, k)}:", _n(n)) for k, n in counts.items()
             if k not in kept_keys], col_w=[7 * cm, inner_w - 7 * cm]))
    if n_kept:
        out.append(Paragraph(
            f"Beholdt uendret etter regelen — ikke feil ({_n(n_kept)})", s_sub))
        out.append(_kv_table(
            [(f"{labels.get(k, k)}:", _n(n)) for k, n in counts.items()
             if k in kept_keys], col_w=[7 * cm, inner_w - 7 * cm]))
        if counts.get("signert"):
            out.append(Paragraph(
                "Digitalt signerte PDF-er røres aldri: en konvertering ville gjort "
                "signaturen ugyldig. Filene er listet i regnearket, ikke her.", s_small))
    out.append(Spacer(1, 0.25 * cm))

    cell = ParagraphStyle("FailCell", parent=s_small, fontSize=7.5, leading=9,
                          spaceAfter=0, textColor=C_PRIMARY)
    head = ParagraphStyle("FailHead", parent=cell, fontName="Helvetica-Bold",
                          textColor=C_WHITE)
    order = list(labels)
    # Signerte PDF-er listes ikke enkeltvis (kan være tusenvis, og er i orden)
    entries = sorted((e for e in fs.get("entries", []) if e["kategori"] != "signert"),
                     key=lambda e: (order.index(e["kategori"])
                                    if e["kategori"] in order else 99, e["fil"]))
    shown = entries[:MAX_FAILURE_ROWS]
    total = total - int(counts.get("signert", 0))
    data = [[Paragraph(h, head) for h in ("Årsak", "Fil", "Tabell/kolonne", "Format", "Detaljer")]]
    for e in shown:
        fil = e["fil"].split("::")[-1]
        data.append([
            Paragraph(escape(labels.get(e["kategori"], e["kategori"])), cell),
            Paragraph(escape(fil), cell),
            Paragraph(escape(e.get("plassering", "")), cell),
            Paragraph(escape(e.get("format", "")), cell),
            Paragraph(escape(_NOISE_RE.sub("", e.get("arsak", "")))[:400], cell),
        ])
    tbl = Table(data, colWidths=[inner_w * 0.15, inner_w * 0.27, inner_w * 0.24,
                                 inner_w * 0.07, inner_w * 0.27], repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.HexColor("#fdf2f2"), C_WHITE]),
    ]))
    if shown:
        out.append(tbl)
    rest = total - len(shown)
    note = []
    if rest > 0:
        note.append(f"{_n(rest)} fil(er) til er ikke vist her.")
    if fs.get("csv_path"):
        note.append(f"Komplett liste (regneark): {Path(fs['csv_path']).name}")
    if fs.get("log_path"):
        note.append(f"Feillogg: {Path(fs['log_path']).name}")
    if note:
        out.append(Spacer(1, 0.15 * cm))
        out.append(Paragraph(escape("  ·  ".join(note)), s_small))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Operasjonsspesifikke detaljseksjoner
# ─────────────────────────────────────────────────────────────────────────────

def _op_detail(
    op_id: str, op_data: dict, step: dict, include_charts: bool,
    inner_w, _kv_table, s_sub, s_body, s_small, s_mono, s_caption,
    C_PRIMARY, C_SUCCESS, C_ERROR, C_WARNING, C_SKIP,
    C_SUCCESS_BG, C_ERROR_BG, C_BORDER, C_WHITE,
    _pie_chart,
) -> list:
    """Returnerer en liste av Flowable for gitt operasjon."""

    from reportlab.lib import colors as rl_colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle

    _S = getSampleStyleSheet()

    def _style(name, parent="Normal", **kw):
        return ParagraphStyle(name=name, parent=_S[parent], **kw)

    out = []

    # ── SHA-256 ──────────────────────────────────────────────────────────────
    if op_id == "sha256":
        digest = op_data.get("sha256", "")
        if digest:
            out.append(Paragraph("Kontrollsum", s_sub))
            out.append(Paragraph(digest, s_mono))
            out.append(Paragraph(
                "SHA-256-sjekksummen kan brukes til å verifisere at "
                "SIARD-filen ikke er endret etter at den ble skannet.",
                s_small))

    # ── BLOB/CLOB Kontroll ───────────────────────────────────────────────────
    elif op_id == "blob_check":
        total_ci = op_data.get("total_clob_inline",  0)
        total_ce = op_data.get("total_clob_extern",  0)
        total_bf = op_data.get("total_blob_files",   0)
        has_blobs = bool(op_data.get("has_blobs", (total_ci + total_ce + total_bf) > 0))

        out.append(Paragraph("Opptelling av binærinnhold", s_sub))
        rows = [
            ("Inline CLOB-felt:",      f"{_n(total_ci)}"),
            ("Ekstern CLOB-felt:",     f"{_n(total_ce)}"),
            ("BLOB-filer (lob-mapper):", f"{_n(total_bf)}"),
            ("Totalt:",                f"{_n(total_ci + total_ce + total_bf)}"),
        ]
        out.append(_kv_table(rows))

        if include_charts and (total_ci + total_ce + total_bf) > 0:
            chart = _pie_chart(
                labels=["Inline CLOB", "Ekstern CLOB", "BLOB-filer"],
                values=[float(total_ci), float(total_ce), float(total_bf)],
                title="Fordeling av binærinnhold",
                slice_colors=[
                    rl_colors.HexColor("#2980b9"),
                    rl_colors.HexColor("#27ae60"),
                    rl_colors.HexColor("#e67e22"),
                ],
            )
            if chart is not None:
                out.append(Spacer(1, 0.7 * cm))
                out.append(chart)
                out.append(Spacer(1, 0.4 * cm))
                out.append(Paragraph(
                    "Kakediagrammet viser andelen av hvert binærinnholdstype.",
                    s_caption))

        if not has_blobs:
            out.append(Paragraph(
                "Ingen BLOB/CLOB-innhold funnet i uttrekket.", s_small))

        # Per-tabell-sammendrag (maks 10 tabeller)
        per_table = op_data.get("per_table", {})
        if per_table:
            out.append(Spacer(1, 0.3 * cm))
            out.append(Paragraph("Innhold per tabell (topp 10)", s_sub))
            sorted_tables = sorted(
                per_table.items(),
                key=lambda kv: (kv[1].get("clob_inline", 0)
                                + kv[1].get("clob_extern", 0)
                                + kv[1].get("blob_files", 0)),
                reverse=True,
            )[:10]
            pt_rows = [["Tabell", "Inline CLOB", "Ekstern CLOB", "BLOB-filer"]]
            for tname, td in sorted_tables:
                pt_rows.append([
                    tname,
                    str(td.get("clob_inline", 0)),
                    str(td.get("clob_extern", 0)),
                    str(td.get("blob_files", 0)),
                ])
            pt_tbl = Table(pt_rows,
                           colWidths=[inner_w * 0.4, inner_w * 0.2,
                                      inner_w * 0.2, inner_w * 0.2])
            pt_tbl.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0), C_PRIMARY),
                ("TEXTCOLOR",     (0, 0), (-1, 0), C_WHITE),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE",      (0, 0), (-1, -1), 8),
                ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
                ("GRID",          (0, 0), (-1, -1), 0.5, C_BORDER),
                ("TOPPADDING",    (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING",   (0, 0), (-1, -1), 6),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                 [rl_colors.HexColor("#f8f9fa"), C_WHITE]),
            ]))
            out.append(pt_tbl)

    # ── BLOB Konverter ───────────────────────────────────────────────────────
    elif op_id == "blob_convert":
        detected = op_data.get("detected", 0)
        converted = op_data.get("converted", 0)
        kept = op_data.get("kept", 0)
        failed = op_data.get("failed", 0)
        inline_ex = op_data.get("inline_extracted", 0)
        xml_upd = op_data.get("xml_updated", 0)

        out.append(Paragraph("Konverteringsresultat", s_sub))
        rows = []
        if op_data.get("identified"):
            rows.append(("LOB-filer identifisert:", _n(op_data["identified"])))
        rows += [
            ("Sendt til konvertering:",        _n(detected)),
            ("Konvertert til PDF/A:",          _n(converted)),
            ("Beholdt i originalformat:",      _n(kept)),
            ("Inline LOB-er hentet ut:",       _n(inline_ex)),
            ("Filreferanser oppdatert i XML:", _n(xml_upd)),
            ("Konvertering feilet:",           _n(failed)),
        ]
        if op_data.get("msg_converted"):
            rows.append(("E-post (MSG/EML) til PDF/A:", _n(op_data["msg_converted"])))
        _gp = op_data.get("gs_postprocess") or {}
        if _gp.get("files"):
            _b, _a = _gp.get("before", 0), _gp.get("after", 0)
            rows.append(("Etterbehandlet (Ghostscript):",
                         f"{_n(_gp['files'])} PDF/A-filer, {_human_size(_b)} → {_human_size(_a)} "
                         f"(spart {_human_size(_b - _a)}, "
                         f"{(1 - _a / _b) * 100 if _b else 0:.1f} %)"))
        _fs = op_data.get("failure_summary") or {}
        if _fs.get("total"):
            _nf, _nk, _ = _split_failures(_fs)
            if _nk:
                rows.append(("Beholdt uendret etter regelen:",
                             f"{_n(_nk)}  (signert eller passordbeskyttet — se under)"))
        out.append(_kv_table(rows, col_w=[5.6 * cm, inner_w - 5.6 * cm]))

        if include_charts and (converted + kept + failed) > 0:
            chart = _pie_chart(
                labels=["Konvertert (PDF/A)", "Beholdt", "Feil"],
                values=[float(converted), float(kept), float(failed)],
                title="Konverteringsutfall",
                slice_colors=[
                    rl_colors.HexColor("#27ae60"),
                    rl_colors.HexColor("#2980b9"),
                    rl_colors.HexColor("#c0392b"),
                ],
            )
            if chart is not None:
                out.append(Spacer(1, 0.7 * cm))
                out.append(chart)
                out.append(Spacer(1, 0.4 * cm))
                out.append(Paragraph(
                    "Grønn = konvertert til PDF/A  •  Blå = beholdt originalformat  "
                    "•  Rød = feil under konvertering",
                    s_caption))

        fs = op_data.get("failure_summary") or {}
        out.extend(_format_change_section(
            op_data.get("_format_counts") or {}, op_data.get("to_pdfa_formats") or {},
            fs, inner_w, s_sub, s_small, C_PRIMARY, C_BORDER, C_WHITE))
        if fs.get("total"):
            out.extend(_failure_section(fs, inner_w, _kv_table, s_sub, s_small,
                                        C_PRIMARY, C_BORDER, C_WHITE))
        elif failed > 0:
            out.append(Paragraph(
                f"Advarsel: {failed} fil(er) kunne ikke konverteres. "
                "Se kjøreloggen for detaljer om de aktuelle filene.",
                _style("warn_txt", fontName="Helvetica-Bold", fontSize=9,
                        textColor=rl_colors.HexColor("#e67e22"),
                        leftIndent=10, spaceAfter=4)))

        # Output-fil
        out_path = op_data.get("output_path", "")
        if out_path:
            out.append(Spacer(1, 0.2 * cm))
            out.append(Paragraph(f"Ny SIARD-fil: {Path(out_path).name}", s_small))

    # ── Estimert datospenn ───────────────────────────────────────────────────
    elif op_id == "date_span":
        out.extend(_date_span_section(op_data, inner_w, _kv_table, s_sub, s_small,
                                      C_PRIMARY, C_BORDER, C_WHITE, C_WARNING))

    # ── HEX Inline Extract ───────────────────────────────────────────────────
    elif op_id == "hex_extract":
        hex_exp = op_data.get("hex_exported", 0)
        tables  = op_data.get("tables_processed", 0)
        rows = [
            ("HEX-felt eksportert:", f"{_n(hex_exp)}"),
        ]
        if tables:
            rows.append(("Tabeller behandlet:", f"{_n(tables)}"))
        out.append(Paragraph("HEX-ekstraksjon", s_sub))
        out.append(_kv_table(rows))
        if hex_exp == 0:
            out.append(Paragraph("Ingen HEX-kodede CLOB-felt funnet.", s_small))

    # ── Virusskan ────────────────────────────────────────────────────────────
    elif op_id == "virus_scan":
        step_ok = step.get("success", False)
        msg     = step.get("message", "")
        av_exe  = op_data.get("av_executable", "")
        scanned = op_data.get("files_scanned", 0)

        out.append(Paragraph("Skanneresultat", s_sub))
        rows = []
        if av_exe:
            rows.append(("Antivirusprogram:", Path(av_exe).name))
        if scanned:
            rows.append(("Filer skannet:", f"{_n(scanned)}"))
        verdict = "Ingen trusler funnet" if step_ok else "Trusler / feil"
        rows.append(("Konklusjon:", verdict))
        if rows:
            out.append(_kv_table(rows))

        color = C_SUCCESS if step_ok else C_ERROR
        out.append(Paragraph(
            msg,
            _style("scan_res", fontName="Helvetica-Bold", fontSize=10,
                    textColor=color, leftIndent=10, spaceBefore=4, spaceAfter=4)))

    # ── Pakk ut SIARD ────────────────────────────────────────────────────────
    elif op_id == "unpack_siard":
        n_files = op_data.get("files_extracted", 0)
        ex_path = op_data.get("extracted_path", "")
        rows = [("Filer pakket ut:", f"{_n(n_files)}")]
        if ex_path:
            rows.append(("Temp-mappe:", ex_path))
        out.append(Paragraph("Utpakking", s_sub))
        out.append(_kv_table(rows))

    # ── Pakk sammen SIARD ────────────────────────────────────────────────────
    elif op_id == "repack_siard":
        n_written = op_data.get("files_written", 0)
        size_mb   = op_data.get("size_mb", 0.0)
        out_path  = op_data.get("output_path", "")
        rows = [
            ("Filer skrevet:",   f"{_n(n_written)}"),
            ("Filstørrelse:",    fmt.size(float(size_mb or 0) * 1024 * 1024)),
        ]
        if out_path:
            rows.append(("Ny SIARD-fil:", Path(out_path).name))
        out.append(Paragraph("Sammenpakning", s_sub))
        out.append(_kv_table(rows))

    # ── XML-validering ───────────────────────────────────────────────────────
    elif op_id == "xml_validation":
        step_ok = step.get("success", False)
        out.append(Paragraph("Valideringsresultat", s_sub))
        verdict = "Alle XML-filer er gyldige" if step_ok else "Valideringsfeil funnet"
        color   = C_SUCCESS if step_ok else C_ERROR
        out.append(Paragraph(
            verdict,
            _style("val_res", fontName="Helvetica-Bold", fontSize=10,
                    textColor=color, leftIndent=10, spaceAfter=4)))
        errors = op_data.get("errors", [])
        if errors:
            out.append(Paragraph(
                f"{len(errors)} feil registrert. Se kjøreloggen for detaljer.",
                s_small))

    # ── Metadata-uttrekk ─────────────────────────────────────────────────────
    elif op_id == "metadata_extract":
        meta    = op_data.get("metadata", {}) or op_data
        tables  = meta.get("tables", []) or op_data.get("tables", [])
        n_tab   = len(tables) if isinstance(tables, list) else op_data.get("table_count", 0)
        pdf_out = op_data.get("pdf_path", "") or op_data.get("report_path", "")
        rows = []
        db_name = meta.get("dbName", "") or meta.get("db_name", "")
        if db_name:
            rows.append(("Databasenavn:", db_name))
        if n_tab:
            rows.append(("Antall tabeller:", str(n_tab)))
        if pdf_out:
            rows.append(("Metadata-PDF:", Path(pdf_out).name))
        if rows:
            out.append(Paragraph("Metadata", s_sub))
            out.append(_kv_table(rows))

    # ── Generisk fallback ─────────────────────────────────────────────────────
    else:
        # Vis enkle nøkkel/verdi-par for kjente skalarverdier
        _labels = _RESULT_LABELS.get(op_id, {})
        simple_rows = [
            (_labels.get(k, k) + ":",
             _n(v) if isinstance(v, int) and not isinstance(v, bool) else str(v))
            for k, v in op_data.items()
            if isinstance(v, (str, int, float, bool)) and not k.startswith("_")
            and not (_labels and k not in _labels)
        ][:12]
        if simple_rows:
            out.append(Paragraph("Resultater", s_sub))
            out.append(_kv_table(simple_rows, col_w=[6.5 * cm, inner_w - 6.5 * cm]))
        if op_id == "xsd_type_fix" and op_data.get("changes"):
            from xml.sax.saxutils import escape
            from siard_workflow.core.column_xsd_types import group_lines
            grouped = group_lines(op_data["changes"])
            out.append(Paragraph("Rettingene (tapsfrie utvidelser)", s_sub))
            for line in grouped[:15]:
                out.append(Paragraph("•  " + escape(line), s_small))
            if len(grouped) > 15:
                out.append(Paragraph(f"… og {len(grouped) - 15} linje(r) til "
                                     f"(se kjøreloggen)", s_small))

    return out
