"""
siard_workflow/operations/report_style.py
-----------------------------------------
Delt stil-verktøykasse for «depot-rapportene» (Arkivinformasjon,
Konverteringsrapport, Filorganisering, Behandlingssammendrag).

Stilen følger «dagens rapport» (workflow_report_operation.py): navyblå
hovedfarge (#1a3a5c), blå aksent (#2980b9), grønn/rød/oransje status, A4,
reportlab. Innholdet/målepunktene er hentet fra det eksterne rapportsettet i
extras/-mappen, men presentasjonen er KDRS SIARD Managers egen.

Bruk:
    tk = build_toolkit()              # laster reportlab én gang
    story = []
    story += tk.cover("ARKIVINFORMASJON", "Undertittel")
    story += tk.section("Database-informasjon")
    story.append(tk.kv_table([("Navn", "verdi"), ...]))
    tk.build(out_path, "Tittel", story, footer_label="Arkivnavn")

Modulen importerer reportlab lat (inne i build_toolkit), slik at resten av
applikasjonen kan importere modulen uten at reportlab er installert.
"""
from __future__ import annotations

from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Rene hjelpefunksjoner (ingen reportlab)
# ─────────────────────────────────────────────────────────────────────────────

def fmt_int(n) -> str:
    """Heltall med hardt mellomrom som tusenskille: 472694 → '472 694'."""
    try:
        return f"{int(n):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(n)


def fmt_pct(part: float, whole: float, decimals: int = 1) -> str:
    """Returnerer 'NN,N %' (norsk desimalkomma). 0/0 → '0,0 %'."""
    if not whole:
        pct = 0.0
    else:
        pct = part / whole * 100.0
    s = f"{pct:.{decimals}f}".replace(".", ",")
    return f"{s} %"


def human_size(n) -> str:
    """Menneskelig lesbar filstørrelse (12.4 MB)."""
    if n is None:
        return "–"
    try:
        n = float(n)
    except (TypeError, ValueError):
        return str(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{int(n)} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


# ─────────────────────────────────────────────────────────────────────────────
# Verktøykasse (binder reportlab-objekter)
# ─────────────────────────────────────────────────────────────────────────────

def build_toolkit():
    """Laster reportlab og returnerer en ferdig konfigurert Toolkit.

    Raises ImportError hvis reportlab mangler.
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        PageBreak, HRFlowable, KeepTogether,
    )

    return _Toolkit(
        colors=colors, A4=A4, cm=cm,
        getSampleStyleSheet=getSampleStyleSheet, ParagraphStyle=ParagraphStyle,
        TA_CENTER=TA_CENTER, TA_RIGHT=TA_RIGHT, TA_LEFT=TA_LEFT,
        SimpleDocTemplate=SimpleDocTemplate, Paragraph=Paragraph, Spacer=Spacer,
        Table=Table, TableStyle=TableStyle, PageBreak=PageBreak,
        HRFlowable=HRFlowable, KeepTogether=KeepTogether,
    )


class _Toolkit:
    """Samling av farger, stiler og flowable-byggere i «dagens rapport»-stil."""

    def __init__(self, **rl):
        self._rl = rl
        c = rl["colors"]
        cm = rl["cm"]

        # ── Fargepalett (identisk med workflow_report_operation.py) ──────────
        self.C_PRIMARY    = c.HexColor("#1a3a5c")
        self.C_ACCENT     = c.HexColor("#2980b9")
        self.C_SUCCESS    = c.HexColor("#27ae60")
        self.C_ERROR      = c.HexColor("#c0392b")
        self.C_WARNING    = c.HexColor("#e67e22")
        self.C_SKIP       = c.HexColor("#7f8c8d")
        self.C_LIGHT_BG   = c.HexColor("#f4f6f8")
        self.C_LIGHT_BLUE = c.HexColor("#eaf3fb")
        self.C_BORDER     = c.HexColor("#d5d8dc")
        self.C_SUCCESS_BG = c.HexColor("#d5f5e3")
        self.C_ERROR_BG   = c.HexColor("#fadbd8")
        self.C_WARNING_BG = c.HexColor("#fdebd0")
        self.C_SKIP_BG    = c.HexColor("#eaecee")
        self.C_WHITE      = c.white
        self.C_ALT_ROW    = c.HexColor("#f0f4f8")

        # ── Sideoppsett ──────────────────────────────────────────────────────
        W, H = rl["A4"]
        self.W, self.H = W, H
        self.margin = 2 * cm
        self.inner_w = W - 2 * self.margin

        # ── Tekststiler ──────────────────────────────────────────────────────
        S = rl["getSampleStyleSheet"]()
        PS = rl["ParagraphStyle"]

        def st(name, parent="Normal", **kw):
            return PS(name=name, parent=S[parent], **kw)

        self.s_body  = st("DBody", fontName="Helvetica", fontSize=10,
                          textColor=c.HexColor("#333333"), leading=14, spaceAfter=4)
        self.s_small = st("DSmall", fontName="Helvetica", fontSize=9,
                          textColor=c.HexColor("#555555"), leading=12, spaceAfter=2)
        self.s_section = st("DSec", fontName="Helvetica-Bold", fontSize=13,
                            textColor=self.C_PRIMARY, spaceBefore=14, spaceAfter=6)
        self.s_sub = st("DSub", fontName="Helvetica-Bold", fontSize=10,
                        textColor=self.C_ACCENT, spaceBefore=8, spaceAfter=4)
        self.s_mono = st("DMono", fontName="Courier", fontSize=9,
                         textColor=self.C_PRIMARY, leading=12, spaceAfter=2)
        self.s_caption = st("DCap", fontName="Helvetica-Oblique", fontSize=9,
                            textColor=c.HexColor("#7f8c8d"),
                            alignment=rl["TA_CENTER"], spaceAfter=6)
        self.s_cell = st("DCell", fontName="Helvetica", fontSize=8.5,
                         textColor=c.HexColor("#333333"), leading=11)
        self.s_cell_bold = st("DCellB", fontName="Helvetica-Bold", fontSize=8.5,
                              textColor=self.C_PRIMARY, leading=11)

    # ── Generelle flowables ──────────────────────────────────────────────────
    def spacer(self, h_cm=0.4):
        return self._rl["Spacer"](1, h_cm * self._rl["cm"])

    def page_break(self):
        return self._rl["PageBreak"]()

    def para(self, text, style=None):
        return self._rl["Paragraph"](text, style or self.s_body)

    def caption(self, text):
        return self._rl["Paragraph"](text, self.s_caption)

    # ── Forside / topptittel-boks ────────────────────────────────────────────
    def cover(self, title: str, subtitle: str = "",
              info_rows: list[tuple[str, str]] | None = None) -> list:
        """Navyblå tittelboks + valgfri undertittel + info-tabell."""
        cm = self._rl["cm"]
        Table = self._rl["Table"]
        TableStyle = self._rl["TableStyle"]
        Paragraph = self._rl["Paragraph"]
        out = [self.spacer(0.4)]

        hdr = Table([[title]], colWidths=[self.inner_w])
        hdr.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), self.C_PRIMARY),
            ("TEXTCOLOR",     (0, 0), (-1, -1), self.C_WHITE),
            ("FONTNAME",      (0, 0), (-1, -1), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 22),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",    (0, 0), (-1, -1), 18),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 18),
        ]))
        out.append(hdr)

        if subtitle:
            out.append(self.spacer(0.25))
            out.append(Paragraph(
                subtitle,
                self._rl["ParagraphStyle"](
                    name="cover_sub", parent=self.s_body,
                    fontName="Helvetica", fontSize=11,
                    textColor=self.C_ACCENT, alignment=self._rl["TA_CENTER"])))

        if info_rows:
            out.append(self.spacer(0.7))
            out.append(self.kv_table(info_rows, key_w_cm=3.6))

        out.append(self.spacer(0.5))
        return out

    # ── Seksjonshode (navy + tynn aksentlinje) ───────────────────────────────
    def section(self, title: str) -> list:
        return [
            self._rl["Paragraph"](title, self.s_section),
            self._rl["HRFlowable"](width="100%", thickness=1.5,
                                   color=self.C_ACCENT, spaceAfter=6),
        ]

    def subsection(self, title: str):
        return self._rl["Paragraph"](title, self.s_sub)

    def colored_heading(self, title: str, color) -> object:
        """Farget ■-overskrift (som i det eksterne rapportsettet)."""
        c = self._rl["colors"]
        hexc = "#%02x%02x%02x" % (int(color.red * 255),
                                  int(color.green * 255),
                                  int(color.blue * 255))
        return self._rl["Paragraph"](
            f'<font color="{hexc}">■</font> {title}',
            self._rl["ParagraphStyle"](
                name="colhead", parent=self.s_sub,
                textColor=color, fontSize=11, spaceBefore=10, spaceAfter=4))

    # ── Nøkkel/verdi-tabell ──────────────────────────────────────────────────
    def kv_table(self, rows: list[tuple[str, str]], key_w_cm: float = 4.5):
        cm = self._rl["cm"]
        Table = self._rl["Table"]
        TableStyle = self._rl["TableStyle"]
        Paragraph = self._rl["Paragraph"]
        kw = key_w_cm * cm
        data = [[Paragraph(str(k), self.s_cell_bold),
                 Paragraph("" if v is None else str(v), self.s_cell)]
                for k, v in rows]
        tbl = Table(data, colWidths=[kw, self.inner_w - kw])
        tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, -1), self.C_LIGHT_BLUE),
            ("FONTSIZE",      (0, 0), (-1, -1), 9),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("GRID",          (0, 0), (-1, -1), 0.5, self.C_BORDER),
        ]))
        return tbl

    # ── Generell datatabell med farget topprad ───────────────────────────────
    def data_table(self, header: list[str], rows: list[list],
                   col_widths: list[float], *,
                   header_bg=None, right_cols: list[int] | None = None,
                   center_cols: list[int] | None = None,
                   row_bgs: dict[int, object] | None = None,
                   total_row: bool = False,
                   font_size: float = 8.5):
        """header_bg er reportlab-farge (default navy). col_widths i punkter.

        row_bgs: {radindeks (0-basert i data) -> farge} for statusfarging.
        total_row: stiler siste rad som sum (fet, grå bakgrunn).
        """
        Table = self._rl["Table"]
        TableStyle = self._rl["TableStyle"]
        Paragraph = self._rl["Paragraph"]
        header_bg = header_bg or self.C_PRIMARY
        right_cols = right_cols or []
        center_cols = center_cols or []

        cell_style = self._rl["ParagraphStyle"](
            name="dt_cell", parent=self.s_cell, fontSize=font_size,
            leading=font_size + 2.5)
        head_style = self._rl["ParagraphStyle"](
            name="dt_head", parent=self.s_cell_bold, fontSize=font_size,
            textColor=self.C_WHITE, leading=font_size + 2.5)

        data = [[Paragraph(str(h), head_style) for h in header]]
        for r in rows:
            data.append([Paragraph("" if v is None else str(v), cell_style)
                         for v in r])

        tbl = Table(data, colWidths=col_widths, repeatRows=1)
        style = [
            ("BACKGROUND",    (0, 0), (-1, 0), header_bg),
            ("FONTSIZE",      (0, 0), (-1, -1), font_size),
            ("GRID",          (0, 0), (-1, -1), 0.4, self.C_BORDER),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]
        for ci in right_cols:
            style.append(("ALIGN", (ci, 0), (ci, -1), "RIGHT"))
        for ci in center_cols:
            style.append(("ALIGN", (ci, 0), (ci, -1), "CENTER"))

        # Alternerende rader (lys) der ingen eksplisitt farge er satt
        n = len(rows)
        for i in range(n):
            data_idx = i + 1  # +1 for topprad
            if row_bgs and i in row_bgs:
                style.append(("BACKGROUND", (0, data_idx), (-1, data_idx),
                              row_bgs[i]))
            elif i % 2 == 1:
                style.append(("BACKGROUND", (0, data_idx), (-1, data_idx),
                              self.C_ALT_ROW))
        if total_row and n:
            last = n  # rad-indeks i tabellen
            style += [
                ("BACKGROUND", (0, last), (-1, last), self.C_SKIP_BG),
                ("FONTNAME",   (0, last), (-1, last), "Helvetica-Bold"),
            ]
        tbl.setStyle(TableStyle(style))
        return tbl

    # ── Statusboks (GODKJENT / AVVIK / FEIL) ─────────────────────────────────
    def status_box(self, verdict: str, detail: str, kind: str = "ok"):
        Table = self._rl["Table"]
        TableStyle = self._rl["TableStyle"]
        col = {"ok": self.C_SUCCESS, "warn": self.C_WARNING,
               "fail": self.C_ERROR}.get(kind, self.C_SKIP)
        bg = {"ok": self.C_SUCCESS_BG, "warn": self.C_WARNING_BG,
              "fail": self.C_ERROR_BG}.get(kind, self.C_SKIP_BG)
        sym = {"ok": "✓", "warn": "!", "fail": "✗"}.get(kind, "•")
        st_tbl = Table([[f"{sym}  {verdict}", detail]],
                       colWidths=[self.inner_w * 0.42, self.inner_w * 0.58])
        st_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, -1), bg),
            ("BACKGROUND",    (1, 0), (1, -1), self.C_LIGHT_BG),
            ("FONTNAME",      (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTNAME",      (1, 0), (1, -1), "Helvetica"),
            ("FONTSIZE",      (0, 0), (0, -1), 15),
            ("FONTSIZE",      (1, 0), (1, -1), 10),
            ("TEXTCOLOR",     (0, 0), (0, -1), col),
            ("TEXTCOLOR",     (1, 0), (1, -1), self.C_PRIMARY),
            ("ALIGN",         (0, 0), (0, -1), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",    (0, 0), (-1, -1), 14),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
            ("BOX",           (0, 0), (-1, -1), 1.2, col),
            ("LINEAFTER",     (0, 0), (0, -1), 0.5, self.C_BORDER),
        ]))
        return st_tbl

    # ── Punktliste / nummerert anbefalingsliste ──────────────────────────────
    def bullets(self, items: list[str], numbered: bool = False) -> list:
        out = []
        for i, it in enumerate(items, 1):
            prefix = f"{i}. " if numbered else "•&nbsp;"
            out.append(self._rl["Paragraph"](
                f"{prefix}{it}",
                self._rl["ParagraphStyle"](
                    name="bullet", parent=self.s_body, fontSize=9.5,
                    leftIndent=10, spaceAfter=4, leading=13)))
        return out

    def note(self, text: str, kind: str = "info"):
        col = {"info": self.C_ACCENT, "ok": self.C_SUCCESS,
               "warn": self.C_WARNING, "fail": self.C_ERROR}.get(kind, self.C_ACCENT)
        return self._rl["Paragraph"](
            text,
            self._rl["ParagraphStyle"](
                name="note", parent=self.s_small, fontName="Helvetica-Bold",
                fontSize=9.5, textColor=col, leftIndent=10,
                spaceBefore=4, spaceAfter=4, leading=13))

    # ── Kakediagram (valgfritt) ──────────────────────────────────────────────
    def pie(self, labels: list[str], values: list[float], title: str,
            slice_colors: list):
        total = sum(values)
        if total <= 0:
            return None
        try:
            from reportlab.graphics.shapes import Drawing, String
            from reportlab.graphics.charts.piecharts import Pie
        except ImportError:
            return None
        dw, dh = 300, 200
        d = Drawing(dw, dh)
        d.add(String(dw / 2, dh - 14, title, fontName="Helvetica-Bold",
                     fontSize=9, fillColor=self.C_PRIMARY, textAnchor="middle"))
        pie = Pie()
        pie.x, pie.y = 20, 28
        pie.width = pie.height = 110
        pie.data = values
        pie.labels = [f"{l}: {v / total * 100:.1f}%"
                      for l, v in zip(labels, values)]
        pie.simpleLabels = False
        pie.checkLabelOverlap = True
        pie.slices.strokeWidth = 0.5
        pie.slices.strokeColor = self.C_WHITE
        pie.slices.labelRadius = 1.30
        pie.slices.fontSize = 7.5
        for i, col in enumerate(slice_colors):
            pie.slices[i].fillColor = col
        d.add(pie)
        return d

    # ── Bygg dokumentet ──────────────────────────────────────────────────────
    def build(self, out_path: Path, title: str, story: list,
              footer_label: str = "", author: str = "KDRS SIARD Manager"):
        cm = self._rl["cm"]
        doc = self._rl["SimpleDocTemplate"](
            str(out_path), pagesize=self._rl["A4"],
            leftMargin=self.margin, rightMargin=self.margin,
            topMargin=self.margin, bottomMargin=self.margin,
            title=title, author=author, subject="KDRS SIARD Manager depotrapport")

        W, H = self.W, self.H
        c = self._rl["colors"]

        def _hf(canvas, d):
            canvas.saveState()
            canvas.setFont("Helvetica", 8)
            canvas.setFillColor(c.HexColor("#999999"))
            y = 1.0 * cm
            canvas.drawString(2 * cm, y,
                              f"KDRS SIARD Manager  —  {footer_label}")
            canvas.drawRightString(W - 2 * cm, y, f"Side {d.page}")
            if d.page > 1:
                canvas.setStrokeColor(c.HexColor("#dddddd"))
                canvas.setLineWidth(0.5)
                canvas.line(2 * cm, H - 1.5 * cm, W - 2 * cm, H - 1.5 * cm)
            canvas.restoreState()

        doc.build(story, onFirstPage=_hf, onLaterPages=_hf)
