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

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


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

        step_results = list(ctx.metadata.get("step_results", []))
        # Fjern selve rapport-operasjonen fra listen (den er ikke ferdig enda)
        step_results = [s for s in step_results if s.get("id") != self.operation_id]

        suffix = (self.params.get("report_suffix") or "_workflow_rapport").strip()
        ts     = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = siard_path.parent / f"{siard_path.stem}{suffix}_{ts}.pdf"

        try:
            _generate_pdf(
                pdf_path        = pdf_path,
                siard_path      = siard_path,
                step_results    = step_results,
                ctx_results     = dict(ctx.results),
                include_charts  = bool(self.params.get("include_charts", True)),
                include_details = bool(self.params.get("include_details", True)),
            )
        except ImportError:
            return self._fail(
                "reportlab er ikke installert — kan ikke generere PDF-rapport. "
                "Installer med: pip install reportlab")
        except Exception as exc:
            return self._fail(f"Kunne ikke generere rapport: {exc}")

        size_kb = pdf_path.stat().st_size / 1024
        return self._ok(
            data={"report_path": str(pdf_path), "size_kb": round(size_kb, 1)},
            message=f"Rapport lagret: {pdf_path.name}  ({size_kb:.0f} KB)")


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
    include_charts:  bool = True,
    include_details: bool = True,
) -> None:
    """Genererer selve PDF-filen. Krever reportlab."""

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

    # ── Beregn samlet status ──────────────────────────────────────────────────
    now          = datetime.datetime.now()
    total_steps  = len(step_results)
    ok_steps     = sum(1 for s in step_results if not s.get("skipped") and s.get("success"))
    fail_steps   = sum(1 for s in step_results if not s.get("skipped") and not s.get("success"))
    skip_steps   = sum(1 for s in step_results if s.get("skipped"))
    run_steps    = total_steps - skip_steps
    overall_ok   = (fail_steps == 0 and run_steps > 0)
    total_elapsed = sum(s.get("elapsed", 0.0) for s in step_results)

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
        if skipped:
            status_txt = "Hoppet over"
            sc = C_SKIP
            bg = C_SKIP_BG
        elif success:
            status_txt = "✓  Vellykket"
            sc = C_SUCCESS
            bg = C_SUCCESS_BG
        else:
            status_txt = "✗  Feil"
            sc = C_ERROR
            bg = C_ERROR_BG

        dur_txt = "" if skipped else f"  ({_fmt_elapsed(elapsed)})"
        tbl = Table(
            [[f"{idx}. {label}", f"{status_txt}{dur_txt}"]],
            colWidths=[inner_w * 0.68, inner_w * 0.32],
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
        pie.labels = [f"{l}: {v/total*100:.2f}%"
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
    story.append(Spacer(1, 2.5 * cm))

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
        ["Varighet:", _fmt_elapsed(total_elapsed)],
    ]
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
    story.append(Spacer(1, 1.5 * cm))

    # Samlet status-boks
    if total_steps > 0:
        stat_txt  = "GODKJENT" if overall_ok else "FEIL OPPDAGET"
        stat_sym  = "✓" if overall_ok else "✗"
        stat_col  = C_SUCCESS if overall_ok else C_ERROR
        stat_bg   = C_SUCCESS_BG if overall_ok else C_ERROR_BG
        sum_txt   = (f"{ok_steps} av {run_steps} steg OK"
                     + (f"  •  {skip_steps} hoppet over" if skip_steps else "")
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
            ("FONTSIZE",      (0, 0), (0, -1), 17),
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
        col_w = [0.9 * cm, 6.2 * cm, 2.9 * cm, 2.8 * cm, 2.4 * cm]
        tbl_rows = [["#", "Operasjon", "Kategori", "Status", "Varighet"]]
        for idx, step in enumerate(step_results, 1):
            skipped = step.get("skipped", False)
            success = step.get("success", False)
            label   = step.get("label", f"Steg {idx}")
            cat     = step.get("category", "")
            elapsed = step.get("elapsed", 0.0)
            if skipped:
                st = "Hoppet over"
            elif success:
                st = "OK"
            else:
                st = "FEIL"
            tbl_rows.append([
                str(idx), label, cat, st,
                _fmt_elapsed(elapsed) if not skipped else "—",
            ])

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
            op_data = ctx_results.get(op_id) or {}

            story.append(Spacer(1, 0.35 * cm))
            story.append(_step_header(idx, step))

            if message:
                story.append(Paragraph(
                    f"Melding: {message}",
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
            ("Inline CLOB-felt:",      f"{total_ci:,}"),
            ("Ekstern CLOB-felt:",     f"{total_ce:,}"),
            ("BLOB-filer (lob-mapper):", f"{total_bf:,}"),
            ("Totalt:",                f"{total_ci + total_ce + total_bf:,}"),
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
        rows = [
            ("Filer identifisert:",        f"{detected:,}"),
            ("Konvertert til PDF/A:",      f"{converted:,}"),
            ("Beholdt originalformat:",    f"{kept:,}"),
            ("Inline LOBs hentet:",        f"{inline_ex:,}"),
            ("XML-filer oppdatert:",       f"{xml_upd:,}"),
            ("Konverteringsfeil:",         f"{failed:,}"),
        ]
        out.append(_kv_table(rows))

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

        if failed > 0:
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

    # ── HEX Inline Extract ───────────────────────────────────────────────────
    elif op_id == "hex_extract":
        hex_exp = op_data.get("hex_exported", 0)
        tables  = op_data.get("tables_processed", 0)
        rows = [
            ("HEX-felt eksportert:", f"{hex_exp:,}"),
        ]
        if tables:
            rows.append(("Tabeller behandlet:", f"{tables:,}"))
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
            rows.append(("Filer skannet:", f"{scanned:,}"))
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
        rows = [("Filer pakket ut:", f"{n_files:,}")]
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
            ("Filer skrevet:",   f"{n_written:,}"),
            ("Filstørrelse:",    f"{size_mb:.1f} MB"),
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
        simple_rows = [
            (k, str(v)) for k, v in op_data.items()
            if isinstance(v, (str, int, float, bool)) and not k.startswith("_")
        ][:12]
        if simple_rows:
            out.append(Paragraph("Resultater", s_sub))
            out.append(_kv_table(simple_rows))

    return out
