"""
siard_workflow/operations/metadata_pdf.py
-----------------------------------------
Genererer en profesjonell A4 PDF-rapport fra SIARD-arkiv-metadata.

Bruk:
    from pathlib import Path
    from siard_workflow.operations.metadata_pdf import generate_metadata_pdf

    generate_metadata_pdf(meta_dict, siard_path, pdf_path)

Krever reportlab (lazy-importert inne i funksjonen).
"""
from __future__ import annotations

from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Modul-nivå hjelpefunksjoner (ingen reportlab-avhengigheter)
# ─────────────────────────────────────────────────────────────────────────────

def _human_size(n: int) -> str:
    """Returnerer menneskelig lesbar filstørrelse (f.eks. '12.4 MB')."""
    if n is None:
        return "–"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            if unit == "B":
                return f"{n} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def _trunc_name(s: str, n: int = 15) -> str:
    """Avkorter et navn til maks n tegn med '…' på slutten."""
    if not s:
        return ""
    return s if len(s) <= n else s[: n - 1] + "…"


def _abbrev_type(type_str: str) -> str:
    """Forkorter SQL-type til maks ~18 tegn."""
    if not type_str:
        return ""
    t = type_str.strip()
    # Fjern tall i parentes for lange typer
    replacements = [
        ("CHARACTER VARYING", "VARCHAR"),
        ("CHARACTER LARGE OBJECT", "CLOB"),
        ("BINARY LARGE OBJECT", "BLOB"),
        ("BINARY VARYING", "VARBINARY"),
        ("DOUBLE PRECISION", "DOUBLE"),
        ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"),
        ("TIME WITH TIME ZONE", "TIMETZ"),
    ]
    upper = t.upper()
    for long_form, short_form in replacements:
        if upper.startswith(long_form):
            t = short_form + t[len(long_form):]
            break
    if len(t) > 18:
        t = t[:16] + "…"
    return t


# ─────────────────────────────────────────────────────────────────────────────
# Hoved-funksjon
# ─────────────────────────────────────────────────────────────────────────────

def generate_metadata_pdf(
    meta: dict,
    siard_path: Path,
    pdf_path: Path,
    options: dict | None = None,
) -> None:
    """
    Genererer en profesjonell A4 PDF-rapport fra SIARD-arkiv-metadata.

    Parameters
    ----------
    meta        : dict  – metadata-struktur (se modul-docstring for full spec)
    siard_path  : Path  – sti til SIARD-filen (brukes for visning)
    pdf_path    : Path  – ønsket sti for PDF-utdata

    Raises
    ------
    ImportError  hvis reportlab ikke er installert.
    """
    # ── Lazy-import av reportlab ───────────────────────────────────────────
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm, cm
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
            PageBreak, KeepTogether, HRFlowable,
        )
        from reportlab.graphics.shapes import (
            Drawing, Rect, String, Line, Polygon, Group,
        )
        from reportlab.graphics import renderPDF
        from reportlab.platypus.flowables import Flowable
        import reportlab.lib.colors as rl_colors
    except ImportError as exc:
        raise ImportError(
            "reportlab er ikke installert. Kjør: pip install reportlab"
        ) from exc

    from datetime import date as _date

    _opts = options or {}
    _generate_er = _opts.get("generate_er_diagram", True)

    # ── Fargepalett ────────────────────────────────────────────────────────
    C_NAVY       = colors.Color(0.10, 0.22, 0.42)
    C_BLUE       = colors.Color(0.20, 0.44, 0.68)
    C_LIGHT_BLUE = colors.Color(0.88, 0.93, 0.98)
    C_LOB_ORANGE = colors.Color(0.80, 0.38, 0.10)
    C_LOB_YELLOW = colors.Color(0.93, 0.85, 0.65)
    C_GREEN      = colors.Color(0.18, 0.60, 0.36)
    C_ALT_ROW    = colors.Color(0.96, 0.96, 0.97)
    C_BORDER     = colors.Color(0.78, 0.78, 0.82)
    C_WHITE      = colors.white
    C_BLACK      = colors.black
    C_DARK_GREY  = colors.Color(0.30, 0.30, 0.30)
    C_MED_GREY   = colors.Color(0.55, 0.55, 0.55)

    # Schema-fargepalett for ER-diagram (6 distinkte farger)
    SCHEMA_PALETTE = [
        colors.Color(0.20, 0.44, 0.68),  # blå
        colors.Color(0.18, 0.60, 0.36),  # grønn
        colors.Color(0.65, 0.18, 0.35),  # rød
        colors.Color(0.55, 0.35, 0.68),  # lilla
        colors.Color(0.80, 0.55, 0.10),  # gul-oransje
        colors.Color(0.15, 0.55, 0.65),  # teal
    ]

    # ── Sideoppsett ────────────────────────────────────────────────────────
    PAGE_W, PAGE_H = A4
    MARGIN_LEFT  = 20 * mm
    MARGIN_RIGHT = 20 * mm
    MARGIN_TOP   = 26 * mm
    MARGIN_BOT   = 26 * mm
    CONTENT_W = PAGE_W - MARGIN_LEFT - MARGIN_RIGHT

    today_str = _date.today().isoformat()
    siard_filename = siard_path.name if siard_path else ""
    db_name = meta.get("db_name") or "Ukjent database"

    # ── Stiler ─────────────────────────────────────────────────────────────
    base_styles = getSampleStyleSheet()

    def _style(name, **kwargs):
        return ParagraphStyle(name, parent=base_styles["Normal"], **kwargs)

    style_body = _style("body", fontSize=9, leading=12, textColor=C_DARK_GREY)
    style_body_small = _style("body_small", fontSize=8, leading=10, textColor=C_DARK_GREY)
    style_h1 = _style("h1", fontSize=16, leading=20, textColor=C_NAVY,
                       fontName="Helvetica-Bold", spaceAfter=4*mm)
    style_h2 = _style("h2", fontSize=13, leading=17, textColor=C_NAVY,
                       fontName="Helvetica-Bold", spaceAfter=3*mm, spaceBefore=4*mm)
    style_h3 = _style("h3", fontSize=11, leading=14, textColor=C_BLUE,
                       fontName="Helvetica-Bold", spaceAfter=2*mm, spaceBefore=3*mm)
    style_caption = _style("caption", fontSize=8, leading=10, textColor=C_MED_GREY,
                            fontName="Helvetica-Oblique")
    style_cell = _style("cell", fontSize=8, leading=10, textColor=C_DARK_GREY)
    style_cell_bold = _style("cell_bold", fontSize=8, leading=10,
                              textColor=C_DARK_GREY, fontName="Helvetica-Bold")
    style_cell_center = _style("cell_center", fontSize=8, leading=10,
                                textColor=C_DARK_GREY, alignment=TA_CENTER)
    style_kv_key = _style("kv_key", fontSize=8, leading=11,
                           textColor=C_NAVY, fontName="Helvetica-Bold")
    style_kv_val = _style("kv_val", fontSize=8, leading=11, textColor=C_DARK_GREY)
    style_kv_val_mono = _style("kv_val_mono", fontSize=7.5, leading=11,
                                textColor=C_DARK_GREY, fontName="Courier")

    # ── Header/footer callbacks ────────────────────────────────────────────
    def _draw_header_footer(canvas, doc):
        canvas.saveState()
        # Header-linje øverst
        y_header = PAGE_H - 14 * mm
        canvas.setStrokeColor(C_NAVY)
        canvas.setLineWidth(0.8)
        canvas.line(MARGIN_LEFT, y_header, PAGE_W - MARGIN_RIGHT, y_header)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(C_NAVY)
        canvas.drawString(MARGIN_LEFT, y_header + 2 * mm, "SIARD Metadata-rapport")
        canvas.drawRightString(PAGE_W - MARGIN_RIGHT, y_header + 2 * mm, db_name)

        # Footer-linje nederst
        y_footer = 10 * mm
        canvas.line(MARGIN_LEFT, y_footer + 4 * mm, PAGE_W - MARGIN_RIGHT, y_footer + 4 * mm)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(C_MED_GREY)
        canvas.drawString(MARGIN_LEFT, y_footer, f"Generert: {today_str}")
        cx = PAGE_W / 2
        canvas.drawCentredString(cx, y_footer, siard_filename)
        canvas.drawRightString(PAGE_W - MARGIN_RIGHT, y_footer,
                               f"Side {doc.page}")
        canvas.restoreState()

    def _on_first_page(canvas, doc):
        pass  # Ingen header/footer på forsiden

    def _on_later_pages(canvas, doc):
        _draw_header_footer(canvas, doc)

    # ── Hjelpefunksjoner for tabeller ──────────────────────────────────────
    def _std_table_style(header_bg=None, has_stripes=True, stripe_start=1):
        """Returnerer en TableStyle for standard datatabeller."""
        bg = header_bg or C_LIGHT_BLUE
        cmds = [
            ("BACKGROUND", (0, 0), (-1, 0), bg),
            ("TEXTCOLOR", (0, 0), (-1, 0), C_NAVY),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LEADING", (0, 0), (-1, -1), 10),
            ("GRID", (0, 0), (-1, -1), 0.4, C_BORDER),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]
        return TableStyle(cmds)

    def _p(txt, style=None):
        """Lag et Paragraph-objekt."""
        if style is None:
            style = style_cell
        return Paragraph(str(txt) if txt is not None else "", style)

    def _val(v):
        """Formater en verdi for visning i tabellcelle."""
        if v is None or v == "":
            return "–"
        return str(v)

    # ── ER-diagram-hjelper: wrap Drawing som Flowable ──────────────────────
    class DrawingFlowable(Flowable):
        """Wrapper som gjør en reportlab Drawing om til en Platypus Flowable."""
        def __init__(self, drawing):
            super().__init__()
            self.drawing = drawing
            self.width = drawing.width
            self.height = drawing.height

        def draw(self):
            renderPDF.draw(self.drawing, self.canv, 0, 0)

        def wrap(self, aw, ah):
            return self.width, self.height

    # ══════════════════════════════════════════════════════════════════════
    # Side 1: Forsidebygging
    # ══════════════════════════════════════════════════════════════════════

    def _build_cover() -> list:
        """Bygger forside-innhold som en liste av flowables."""
        story = []

        # Tittelbar (blokk-boks med navy bakgrunn) — tegnet som Drawing
        title_bar_h = 55 * mm
        d = Drawing(CONTENT_W, title_bar_h)
        d.add(Rect(0, 0, CONTENT_W, title_bar_h, fillColor=C_NAVY,
                   strokeColor=None))
        d.add(String(14, title_bar_h - 30, "KDRS",
                     fontSize=24, fillColor=C_WHITE,
                     fontName="Helvetica-Bold"))
        d.add(String(14, title_bar_h - 46, "SIARD Metadata-rapport",
                     fontSize=13, fillColor=colors.Color(0.75, 0.85, 0.97),
                     fontName="Helvetica-Bold"))
        story.append(DrawingFlowable(d))
        story.append(Spacer(1, 8 * mm))

        # Databasenavn
        story.append(Paragraph(db_name, _style("cover_db",
            fontSize=22, leading=26, textColor=C_NAVY,
            fontName="Helvetica-Bold", spaceAfter=2*mm)))

        dbms = _val(meta.get("db_product"))
        story.append(Paragraph(dbms, _style("cover_dbms",
            fontSize=13, leading=16, textColor=C_BLUE,
            fontName="Helvetica", spaceAfter=6*mm)))

        story.append(HRFlowable(width=CONTENT_W, thickness=1.2,
                                color=C_LIGHT_BLUE, spaceAfter=5*mm))

        # Info-tabell
        info_rows = [
            ["Filnavn:", siard_filename],
            ["Filstørrelse:", _human_size(meta.get("file_size"))],
            ["SIARD-versjon:", _val(meta.get("siard_version"))],
            ["Arkiveringsdato:", _val(meta.get("archival_date"))],
            ["Produsert av:", _val(meta.get("producer_app"))],
            ["Rapport generert:", today_str],
        ]
        tdata = [[_p(k, style_kv_key), _p(v, style_kv_val)]
                 for k, v in info_rows]
        col_w = [40 * mm, CONTENT_W - 40 * mm]
        t = Table(tdata, colWidths=col_w)
        t.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.3, C_BORDER),
            ("BACKGROUND", (0, 0), (0, -1), colors.Color(0.94, 0.96, 0.99)),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(t)
        story.append(Spacer(1, 8 * mm))

        # 4 statistikk-bokser
        stats = [
            ("Tabeller", str(meta.get("table_count", 0))),
            ("Rader (total)", f"{meta.get('row_count', 0):,}".replace(",", "\u00a0")),
            ("LOB-tabeller", str(meta.get("lob_table_count", 0))),
            ("Skjemaer", str(meta.get("schema_count", 0))),
        ]
        box_w = (CONTENT_W - 3 * 4 * mm) / 4
        stat_rows = [[]]
        for label_txt, val_txt in stats:
            d2 = Drawing(box_w, 28 * mm)
            d2.add(Rect(0, 0, box_w, 28 * mm,
                        fillColor=C_LIGHT_BLUE, strokeColor=C_BLUE,
                        strokeWidth=0.8))
            d2.add(String(box_w / 2, 17, val_txt,
                          fontSize=18, fillColor=C_NAVY,
                          fontName="Helvetica-Bold",
                          textAnchor="middle"))
            d2.add(String(box_w / 2, 5, label_txt,
                          fontSize=8, fillColor=C_BLUE,
                          fontName="Helvetica",
                          textAnchor="middle"))
            stat_rows[0].append(DrawingFlowable(d2))

        gap = 4 * mm
        stat_table = Table([stat_rows[0]],
                           colWidths=[box_w + gap] * 4)
        stat_table.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ]))
        story.append(stat_table)

        return story

    # ══════════════════════════════════════════════════════════════════════
    # Side 2: Databaseoversikt + søylediagram
    # ══════════════════════════════════════════════════════════════════════

    def _build_overview() -> list:
        story = []
        story.append(Paragraph("Databaseoversikt", style_h1))
        story.append(HRFlowable(width=CONTENT_W, thickness=1.0,
                                color=C_NAVY, spaceAfter=4*mm))

        # Nøkkelverdi-tabell (to kolonner)
        digest = _val(meta.get("message_digest"))
        algo   = _val(meta.get("message_digest_algo"))
        digest_str = f"{algo}: {digest}" if algo != "–" else digest
        if len(digest_str) > 60:
            digest_str = digest_str[:58] + "…"

        kv_rows = [
            ("Databasenavn",         _val(meta.get("db_name"))),
            ("DBMS",                 _val(meta.get("db_product"))),
            ("Opprinnelse",          _val(meta.get("db_origin"))),
            ("Tilkoblings-URL",      _val(meta.get("connection"))),
            ("DB-bruker",            _val(meta.get("db_user"))),
            ("Arkiveringsdato",      _val(meta.get("archival_date"))),
            ("Produsert av",         _val(meta.get("producer_app"))),
            ("Tidsperiode",             _val(meta.get("data_origin_time_span"))),
            ("SIARD-versjon",        _val(meta.get("siard_version"))),
            ("Meldingssammendrag",   digest_str),
            ("Antall ZIP-poster",    str(meta.get("zip_entry_count", "–"))),
            ("LOB-filer totalt",     str(meta.get("lob_file_count", "–"))),
            ("Filstørrelse",         _human_size(meta.get("file_size"))),
        ]
        desc = (meta.get("description") or "").strip()
        if desc:
            kv_rows.append(("Beskrivelse", desc))

        # Lag to-kolonne layout (venstre: nøkler, høyre: verdier)
        half = len(kv_rows)
        mid  = (half + 1) // 2
        left_rows  = kv_rows[:mid]
        right_rows = kv_rows[mid:]

        # Fyller opp til lik lengde
        while len(right_rows) < len(left_rows):
            right_rows.append(("", ""))

        col_kw = 35 * mm
        col_vw = (CONTENT_W / 2) - col_kw - 3 * mm
        rows = []
        for (lk, lv), (rk, rv) in zip(left_rows, right_rows):
            rows.append([
                _p(lk, style_kv_key),
                _p(lv, style_kv_val),
                _p(rk, style_kv_key),
                _p(rv, style_kv_val),
            ])

        col_widths = [col_kw, col_vw, col_kw + 3*mm, col_vw]
        t = Table(rows, colWidths=col_widths, repeatRows=0)
        t.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.3, C_BORDER),
            ("BACKGROUND", (0, 0), (0, -1), colors.Color(0.94, 0.96, 0.99)),
            ("BACKGROUND", (2, 0), (2, -1), colors.Color(0.94, 0.96, 0.99)),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(t)
        story.append(Spacer(1, 6 * mm))

        # ── Horisontalt søylediagram: topp-15 tabeller etter rader ────────
        all_tables = []
        schema_colors = {}
        schema_idx = {}
        for si, schema in enumerate(meta.get("schemas", [])):
            sc = SCHEMA_PALETTE[si % len(SCHEMA_PALETTE)]
            schema_colors[schema["name"]] = sc
            schema_idx[schema["name"]] = si
            for tbl in schema.get("tables", []):
                all_tables.append((tbl["rows"] or 0, tbl["name"],
                                   schema["name"], sc))

        if all_tables:
            all_tables.sort(reverse=True)
            top15 = all_tables[:15]

            story.append(Paragraph("Topp-15 tabeller etter radantall", style_h2))

            chart_h = max(60 * mm, len(top15) * 7 * mm + 20 * mm)
            chart_w = CONTENT_W
            bar_area_w = chart_w - 50 * mm   # venstre reservert til tabellnavn
            max_rows = max(r for r, _, _, _ in top15) if top15 else 1
            bar_max_w = bar_area_w - 20 * mm
            row_h = (chart_h - 15 * mm) / len(top15)

            d = Drawing(chart_w, chart_h)
            # Bakgrunn
            d.add(Rect(0, 0, chart_w, chart_h,
                       fillColor=colors.Color(0.98, 0.98, 0.99),
                       strokeColor=C_BORDER, strokeWidth=0.5))

            label_x = 2 * mm
            bar_start_x = 50 * mm
            y_offset = chart_h - 12 * mm

            for i, (rows, tname, sname, sc) in enumerate(top15):
                y = y_offset - i * row_h
                bar_h = row_h * 0.65
                frac = (rows / max_rows) if max_rows > 0 else 0
                bw = frac * bar_max_w

                # Tabellnavn (venstre)
                tname_disp = tname if len(tname) <= 22 else tname[:20] + "…"
                d.add(String(label_x, y - bar_h * 0.6, tname_disp,
                             fontSize=7, fillColor=C_DARK_GREY,
                             fontName="Helvetica"))
                # Søyle
                if bw > 0:
                    d.add(Rect(bar_start_x, y - bar_h, bw, bar_h,
                               fillColor=sc, strokeColor=None))
                # Verdi
                rows_str = f"{rows:,}".replace(",", "\u00a0")
                d.add(String(bar_start_x + bw + 2, y - bar_h * 0.65,
                             rows_str, fontSize=7,
                             fillColor=C_DARK_GREY, fontName="Helvetica"))

            story.append(DrawingFlowable(d))

            # Skjema-fargeforklaring
            if schema_colors:
                legend_parts = []
                for sname, sc in schema_colors.items():
                    legend_parts.append(
                        f'<font color="#{int(sc.red*255):02x}'
                        f'{int(sc.green*255):02x}{int(sc.blue*255):02x}">&#9632;</font>'
                        f' {_trunc_name(sname, 20)}'
                    )
                legend_txt = "   ".join(legend_parts)
                story.append(Paragraph(legend_txt, style_caption))

        return story

    # ══════════════════════════════════════════════════════════════════════
    # Side 3: Tabellinventar
    # ══════════════════════════════════════════════════════════════════════

    def _build_inventory() -> list:
        story = []
        story.append(Paragraph("Tabelloversikt", style_h1))
        story.append(HRFlowable(width=CONTENT_W, thickness=1.0,
                                color=C_NAVY, spaceAfter=4*mm))

        all_tables = []
        for schema in meta.get("schemas", []):
            for tbl in schema.get("tables", []):
                all_tables.append((schema["name"], tbl))

        if not all_tables:
            story.append(Paragraph("Ingen tabeller funnet.", style_body))
            return story

        # Sortert etter rader desc
        all_tables.sort(key=lambda x: (x[0].lower(), x[1].get("name", "").lower()))

        # Kolonne-bredder:
        #  Skjema(18) | Tabell(36) | Rader(20) | LOB(15) | PK(28) | Beskrivelse(rest)
        CW_SCHEMA = 18 * mm
        CW_NAME   = 36 * mm
        CW_ROWS   = 20 * mm
        CW_LOB    = 15 * mm
        CW_PK     = 28 * mm
        CW_DESC   = CONTENT_W - CW_SCHEMA - CW_NAME - CW_ROWS - CW_LOB - CW_PK
        col_widths = [CW_SCHEMA, CW_NAME, CW_ROWS, CW_LOB, CW_PK, CW_DESC]

        headers = [
            _p("Skjema",       style_cell_bold),
            _p("Tabell",       style_cell_bold),
            _p("Rader",        style_cell_bold),
            _p("LOB-filer",    style_cell_bold),
            _p("Primærnøkkel", style_cell_bold),
            _p("Beskrivelse",  style_cell_bold),
        ]

        C_TBL_ROW  = C_LIGHT_BLUE

        # Stil for beskrivelse under tabellnavn (liten, kursiv, grå)
        style_tbl_desc = ParagraphStyle(
            "tbl_desc",
            parent=style_cell,
            fontSize=6.5,
            leading=8,
            textColor=C_MED_GREY,
            fontName="Helvetica-Oblique",
            spaceBefore=1,
        )

        rows = [headers]
        row_styles: list[tuple] = [
            ("BACKGROUND", (0, 0), (-1, 0), C_LIGHT_BLUE),
            ("TEXTCOLOR",  (0, 0), (-1, 0), C_NAVY),
            ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ]

        ri = 1

        for sname, tbl in all_tables:
            lob_file_cnt = tbl.get("lob_file_count", 0)
            has_lob      = tbl.get("has_lob") or tbl.get("lob_col_count", 0) > 0 or lob_file_cnt > 0
            rows_val     = tbl.get("rows", 0) or 0
            rows_str     = f"{rows_val:,}".replace(",", "\u00a0")

            pk = tbl.get("primary_key")
            pk_str = ""
            if pk:
                pk_cols = pk.get("columns", [])
                pk_str = ", ".join(pk_cols[:4])
                if len(pk_cols) > 4:
                    pk_str += f" (+{len(pk_cols) - 4})"

            desc = (tbl.get("description") or "").strip()

            # Tabellnavn-celle: navn (fet) + tableX-mappe
            tbl_name   = tbl.get("name", "")
            tbl_folder = tbl.get("folder", "")
            name_parts = [Paragraph(f"<b>{tbl_name}</b>", style_cell)]
            if tbl_folder:
                name_parts.append(Paragraph(tbl_folder, style_tbl_desc))
            name_cell = name_parts if len(name_parts) > 1 else name_parts[0]

            # LOB-filer: vis antall (tom hvis ingen)
            lob_str = str(lob_file_cnt) if lob_file_cnt > 0 else ""

            # ── Tabellrad ──────────────────────────────────────────────────
            tbl_row = [
                _p(_trunc_name(sname), style_cell_bold),
                name_cell,
                _p(rows_str, style_cell_bold),
                _p(lob_str, style_cell_center),
                _p(pk_str, style_cell),
                _p(desc, style_cell),
            ]
            rows.append(tbl_row)

            bg = C_LOB_YELLOW if has_lob else C_TBL_ROW
            row_styles.append(("BACKGROUND", (0, ri), (-1, ri), bg))
            row_styles.append(("TOPPADDING",    (0, ri), (-1, ri), 4))
            row_styles.append(("BOTTOMPADDING", (0, ri), (-1, ri), 5))
            row_styles.append(("VALIGN",        (0, ri), (-1, ri), "TOP"))
            ri += 1

        t = Table(rows, colWidths=col_widths, repeatRows=1)
        ts = TableStyle([
            ("GRID",         (0, 0), (-1, -1), 0.4, C_BORDER),
            ("FONTSIZE",     (0, 0), (-1, -1), 7.5),
            ("LEADING",      (0, 0), (-1, -1), 10),
            ("LEFTPADDING",  (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING",   (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 3),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ])
        for cmd in row_styles:
            ts.add(*cmd)
        t.setStyle(ts)
        story.append(t)

        # Fargeforklaring
        story.append(Spacer(1, 2 * mm))
        # C_LOB_YELLOW = Color(0.93, 0.85, 0.65) → #EDD9A6  (same as LOB row background)
        story.append(Paragraph(
            '<font color="#EDD9A6">■</font>'
            ' Rader med denne bakgrunnsfargen = tabeller med LOB-data'
            '&nbsp;&nbsp;&nbsp;'
            '<b>LOB-filer</b> = antall binærfiler talt fra content/schema{n}/table{m}/lob*/',
            style_caption,
        ))

        return story

    # ══════════════════════════════════════════════════════════════════════
    # Side 4: ER-diagram
    # ══════════════════════════════════════════════════════════════════════

    def _build_er_diagram() -> list:
        import math as _math

        story = []
        story.append(Paragraph("ER-diagram", style_h1))
        story.append(HRFlowable(width=CONTENT_W, thickness=1.0,
                                color=C_NAVY, spaceAfter=4*mm))

        schemas = meta.get("schemas", [])
        all_tables: list[tuple] = []
        schema_color_map: dict = {}
        for si, schema in enumerate(schemas):
            sc = SCHEMA_PALETTE[si % len(SCHEMA_PALETTE)]
            schema_color_map[schema["name"]] = sc
            for tbl in schema.get("tables", []):
                all_tables.append((schema["name"], tbl, sc))

        if not all_tables:
            story.append(Paragraph("Ingen tabeller funnet.", style_body))
            return story

        n = len(all_tables)

        # ── Dimensjoner ────────────────────────────────────────────────────
        BOX_H  = 26.0   # fast høyde — kun header (tabellnavn + rader)
        GAP_X  = 12.0   # horisontalt mellomrom
        GAP_Y  = 16.0   # vertikalt mellomrom (nok plass til FK-labels)

        # Antall kolonner i grid
        if   n <= 4:  n_cols = 2
        elif n <= 9:  n_cols = 3
        elif n <= 20: n_cols = 4
        else:         n_cols = 5

        avail_w = float(CONTENT_W)
        box_w = (avail_w - GAP_X * (n_cols + 1)) / n_cols
        box_w = max(box_w, 65.0)

        n_rows_grid = (n + n_cols - 1) // n_cols

        # Grid-plassering: rad for rad, venstre til høyre
        col_assignments = [(idx % n_cols, idx // n_cols) for idx in range(n)]

        # ── Kant-punkt-hjelper ─────────────────────────────────────────────
        def _edge_pt(cx: float, cy: float, tx: float, ty: float) -> tuple[float, float]:
            """Punkt på bokskanten (cx,cy,box_w,BOX_H) i retning (tx,ty)."""
            dx, dy = tx - cx, ty - cy
            if dx == 0 and dy == 0:
                return cx, cy
            hw, hh = box_w / 2, BOX_H / 2
            t_x = hw / abs(dx) if dx != 0 else float("inf")
            t_y = hh / abs(dy) if dy != 0 else float("inf")
            t = min(t_x, t_y)
            return cx + dx * t, cy + dy * t

        # ── Sideberegning ──────────────────────────────────────────────────
        row_h     = BOX_H + GAP_Y
        page_avail_h = float(PAGE_H - MARGIN_TOP - MARGIN_BOT - 30 * mm)
        rows_per_pg  = max(1, int(page_avail_h / row_h))

        # ── Tegn side for side ─────────────────────────────────────────────
        page_row_start = 0
        while page_row_start < n_rows_grid:
            page_row_end = min(page_row_start + rows_per_pg, n_rows_grid)
            n_page_rows  = page_row_end - page_row_start

            draw_h = n_page_rows * row_h + 8.0

            d = Drawing(avail_w, draw_h)
            d.add(Rect(0, 0, avail_w, draw_h,
                       fillColor=colors.Color(0.98, 0.98, 0.99),
                       strokeColor=None))

            # Boks-sentre for FK-oppslag: tbl_name → (cx, cy)
            box_centers: dict[str, tuple[float, float]] = {}

            # ── Tegn alle bokser ───────────────────────────────────────────
            for idx, (sname, tbl, sc) in enumerate(all_tables):
                col_idx, row_idx = col_assignments[idx]
                if not (page_row_start <= row_idx < page_row_end):
                    continue

                local_row = row_idx - page_row_start
                # Bottom-up koordinater i Drawing
                x_left   = GAP_X / 2 + col_idx * (box_w + GAP_X)
                y_top    = local_row * row_h          # fra topp
                y_bottom = draw_h - y_top - BOX_H - 4

                cx = x_left + box_w / 2
                cy = y_bottom + BOX_H / 2
                box_centers[tbl.get("name", "")] = (cx, cy)

                # Boks-ramme
                d.add(Rect(x_left, y_bottom, box_w, BOX_H,
                           fillColor=C_WHITE, strokeColor=sc, strokeWidth=1.2))
                # Header-farge-stripe (øverste 16 pt)
                HDR = 16.0
                d.add(Rect(x_left, y_bottom + BOX_H - HDR, box_w, HDR,
                           fillColor=sc, strokeColor=None))

                # Tabellnavn
                tname = tbl.get("name", "")
                max_ch = max(10, int(box_w / 5.5))
                tname_disp = tname if len(tname) <= max_ch else tname[:max_ch - 1] + "…"
                d.add(String(x_left + box_w / 2,
                             y_bottom + BOX_H - HDR + 5,
                             tname_disp,
                             fontSize=7, fillColor=C_WHITE,
                             fontName="Helvetica-Bold",
                             textAnchor="middle"))

                # Radantall + tableX under header-stripa
                rows_val = tbl.get("rows", 0) or 0
                folder   = tbl.get("folder", "")
                sub_line = f"{rows_val:,}".replace(",", "\u00a0") + " rader"
                if folder:
                    sub_line += f"  ({folder})"
                d.add(String(x_left + box_w / 2,
                             y_bottom + 3,
                             sub_line,
                             fontSize=5.5, fillColor=C_MED_GREY,
                             textAnchor="middle"))

            # ── Tegn FK-piler ──────────────────────────────────────────────
            C_ARROW = colors.Color(0.45, 0.45, 0.60)
            for sname, tbl, sc in all_tables:
                src_name = tbl.get("name", "")
                if src_name not in box_centers:
                    continue
                scx, scy = box_centers[src_name]

                for fk in tbl.get("foreign_keys", []):
                    ref_tbl = fk.get("ref_table", "")
                    if ref_tbl not in box_centers or ref_tbl == src_name:
                        continue
                    tcx, tcy = box_centers[ref_tbl]

                    # Kant-punkter i stedet for sentre
                    ex1, ey1 = _edge_pt(scx, scy, tcx, tcy)   # ut fra kilde
                    ex2, ey2 = _edge_pt(tcx, tcy, scx, scy)   # inn i mål

                    # Linje
                    d.add(Line(ex1, ey1, ex2, ey2,
                               strokeColor=C_ARROW, strokeWidth=0.9))

                    # Pilhode ved kant av mål-boksen
                    ddx, ddy = ex2 - ex1, ey2 - ey1
                    length = _math.hypot(ddx, ddy)
                    if length > 0:
                        ux, uy = ddx / length, ddy / length
                        al, aw = 7.0, 3.0
                        ax = ex2 - ux * al
                        ay = ey2 - uy * al
                        px, py = -uy * aw, ux * aw
                        d.add(Polygon(
                            [ex2, ey2,
                             ax + px, ay + py,
                             ax - px, ay - py],
                            fillColor=C_ARROW, strokeColor=None,
                        ))

            story.append(DrawingFlowable(d))

            if page_row_end < n_rows_grid:
                story.append(PageBreak())
                story.append(Paragraph("ER-diagram (forts.)", style_h2))

            page_row_start = page_row_end

        # Fargeforklaring
        if schema_color_map:
            story.append(Spacer(1, 3 * mm))
            legend_items = []
            for sname, sc in schema_color_map.items():
                legend_items.append(
                    f'<font color="#{int(sc.red*255):02x}'
                    f'{int(sc.green*255):02x}{int(sc.blue*255):02x}">&#9632;</font>'
                    f' {_trunc_name(sname, 20)}'
                )
            story.append(Paragraph(
                "Skjema-farger: " + "   ".join(legend_items),
                style_caption,
            ))

        return story

    # ══════════════════════════════════════════════════════════════════════
    # Side 5: Skjema- og tabelldetaljer
    # ══════════════════════════════════════════════════════════════════════

    def _build_details() -> list:
        story = []
        story.append(Paragraph("Skjema- og tabelldetaljer", style_h1))
        story.append(HRFlowable(width=CONTENT_W, thickness=1.0,
                                color=C_NAVY, spaceAfter=4*mm))

        schemas = meta.get("schemas", [])
        if not schemas:
            story.append(Paragraph("Ingen skjemaer funnet.", style_body))
            return story

        for schema in schemas:
            schema_name = schema.get("name", "Ukjent")
            schema_desc = (schema.get("description") or "").strip()

            # ── Skjema-overskrift ────────────────────────────────────────
            schema_block = []
            schema_block.append(Spacer(1, 4 * mm))
            schema_block.append(Paragraph(f"Skjema: {schema_name}", style_h2))
            if schema_desc:
                schema_block.append(Paragraph(schema_desc, style_caption))
            schema_block.append(HRFlowable(width=CONTENT_W, thickness=0.6,
                                           color=C_BLUE, spaceAfter=2*mm))
            story.extend(schema_block)

            tables = schema.get("tables", [])
            if not tables:
                story.append(Paragraph("Ingen tabeller i dette skjemaet.", style_body))
                continue

            for tbl in tables:
                tname = tbl.get("name", "Ukjent")
                rows_val = tbl.get("rows", 0) or 0
                tbl_desc = (tbl.get("description") or "").strip()
                columns = tbl.get("columns", [])

                # ── Tabell-overskrift ────────────────────────────────────
                tbl_block = []
                rows_str = f"{rows_val:,}".replace(",", "\u00a0")
                tbl_block.append(Spacer(1, 3 * mm))
                tbl_block.append(
                    Paragraph(
                        f"{tname} &nbsp;&nbsp;"
                        f'<font color="grey" size="9">{rows_str} rader</font>',
                        style_h3,
                    )
                )
                if tbl_desc:
                    tbl_block.append(Paragraph(tbl_desc, style_caption))

                # ── Kolonnetabell ────────────────────────────────────────
                col_headers = ["Pos", "Kolonne", "SQL-type", "Orig.type",
                               "LOB", "Beskrivelse"]
                cw = [10*mm, 50*mm, 24*mm, 24*mm, 10*mm,
                      CONTENT_W - 138*mm]

                col_rows = [col_headers]
                col_row_styles = []
                for ci, col_info in enumerate(columns):
                    ri = ci + 1
                    is_lob = col_info.get("is_lob", False)
                    nullable = "Ja" if col_info.get("nullable", True) else "Nei"
                    lob_flag = "Ja" if is_lob else ""
                    mime = (col_info.get("mime_type") or "").strip() or "–"
                    cdesc = (col_info.get("description") or "").strip()
                    if len(cdesc) > 50:
                        cdesc = cdesc[:48] + "…"
                    orig_t = _abbrev_type(col_info.get("type_original", ""))

                    col_rows.append([
                        str(col_info.get("pos", ci + 1)),
                        col_info.get("name", ""),
                        _abbrev_type(col_info.get("type", "")),
                        orig_t or "–",
                        lob_flag,
                        cdesc,
                    ])
                    if is_lob:
                        col_row_styles.append(
                            ("BACKGROUND", (0, ri), (-1, ri), C_LOB_YELLOW))
                    elif ci % 2 == 1:
                        col_row_styles.append(
                            ("BACKGROUND", (0, ri), (-1, ri), C_ALT_ROW))

                ct = Table(col_rows, colWidths=cw, repeatRows=1)
                col_ts = _std_table_style()
                for cmd in col_row_styles:
                    col_ts.add(*cmd)
                col_ts.add("FONTSIZE", (0, 0), (-1, -1), 7.5)
                col_ts.add("LEADING", (0, 0), (-1, -1), 9)
                col_ts.add("ALIGN", (0, 1), (0, -1), "CENTER")
                col_ts.add("ALIGN", (4, 1), (5, -1), "CENTER")
                ct.setStyle(col_ts)
                tbl_block.append(ct)

                # ── Primærnøkkel ─────────────────────────────────────────
                pk = tbl.get("primary_key")
                if pk:
                    pk_cols = ", ".join(pk.get("columns", []))
                    tbl_block.append(Spacer(1, 1.5 * mm))
                    tbl_block.append(
                        Paragraph(
                            f'<b>Primærnøkkel</b> ({pk.get("name", "")}):'
                            f' {pk_cols}',
                            style_body_small,
                        )
                    )

                # ── Fremmednøkler ────────────────────────────────────────
                fks = tbl.get("foreign_keys", [])
                if fks:
                    tbl_block.append(Spacer(1, 1 * mm))
                    tbl_block.append(Paragraph("<b>Fremmednøkler:</b>",
                                               style_body_small))
                    for fk in fks:
                        refs = fk.get("references", [])
                        ref_pairs = ", ".join(
                            f'{r.get("column")} → {r.get("referenced")}'
                            for r in refs
                        )
                        ref_schema = fk.get("ref_schema", "")
                        ref_table  = fk.get("ref_table", "")
                        dest = f"{ref_schema}.{ref_table}" if ref_schema else ref_table
                        tbl_block.append(
                            Paragraph(
                                f'&nbsp;&nbsp;{fk.get("name", "")}:'
                                f' → {dest}: {ref_pairs}',
                                style_body_small,
                            )
                        )

                # ── Unike nøkler ─────────────────────────────────────────
                uks = tbl.get("unique_keys", [])
                if uks:
                    tbl_block.append(Spacer(1, 1 * mm))
                    tbl_block.append(Paragraph("<b>Unike nøkler:</b>",
                                               style_body_small))
                    for uk in uks:
                        uk_cols = ", ".join(uk.get("columns", []))
                        tbl_block.append(
                            Paragraph(
                                f'&nbsp;&nbsp;{uk.get("name", "")}: {uk_cols}',
                                style_body_small,
                            )
                        )

                tbl_block.append(Spacer(1, 2 * mm))
                try:
                    story.append(KeepTogether(tbl_block))
                except Exception:
                    story.extend(tbl_block)

        return story

    # ══════════════════════════════════════════════════════════════════════
    # Sett sammen og bygg PDF
    # ══════════════════════════════════════════════════════════════════════

    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=MARGIN_LEFT,
        rightMargin=MARGIN_RIGHT,
        topMargin=MARGIN_TOP,
        bottomMargin=MARGIN_BOT,
        title=f"SIARD Metadata-rapport — {db_name}",
        author="KDRS SIARD Manager",
        subject="SIARD metadata PDF-rapport",
    )

    story: list = []

    # Side 1: Forside
    story.extend(_build_cover())
    story.append(PageBreak())

    # Side 2: Databaseoversikt
    story.extend(_build_overview())
    story.append(PageBreak())

    # Side 3: Tabelloversikt
    story.extend(_build_inventory())
    story.append(PageBreak())

    # Side 4: ER-diagram (valgfritt)
    if _generate_er:
        story.extend(_build_er_diagram())
        story.append(PageBreak())

    # Side 4/5: Skjema- og tabelldetaljer
    story.extend(_build_details())

    doc.build(story,
              onFirstPage=_on_first_page,
              onLaterPages=_on_later_pages)
