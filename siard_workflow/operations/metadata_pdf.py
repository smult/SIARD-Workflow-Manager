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
    """Returnerer menneskelig lesbar filstørrelse (f.eks. «12,4 MB»)."""
    if n is None:
        return "–"
    from siard_workflow.core import fmt
    return fmt.size(n)


def _num(n) -> str:
    """Antall med (hardt) mellomrom som tusenskille — brytes ikke i en celle."""
    from siard_workflow.core import fmt
    return fmt.num(n).replace(" ", " ")


def _tbl_key(schema: str, table: str) -> str:
    """Stabilt ankernavn for lenker fra tabelloversikten til detaljene."""
    import re as _re
    return "t_" + _re.sub(r"[^A-Za-z0-9]", "_", f"{schema}__{table}")


def short_connection(conn) -> str:
    """
    Tilkoblingsstrengen forkortet til server og database. Rapporten følger
    pakken — brukernavn, passord og tekniske parametre hører ikke hjemme der.
    """
    import re as _re
    s = (conn or "").strip()
    if not s:
        return ""
    if "=" in s and "://" not in s:
        parts = {}
        for p in s.split(";"):
            if "=" in p:
                k, v = p.split("=", 1)
                parts[k.strip().lower()] = v.strip()
        server = (parts.get("data source") or parts.get("server")
                  or parts.get("host") or parts.get("address"))
        db = parts.get("initial catalog") or parts.get("database")
        out = [x for x in (f"server {server}" if server else "",
                           f"database {db}" if db else "") if x]
        return ", ".join(out) if out else "(oppgitt — se metadata.xml)"
    s = _re.sub(r"//[^/@\s]+@", "//", s)          # bruker:passord@
    return _re.split(r"[?;]", s, maxsplit=1)[0]


def referenced_by(schemas: list) -> dict:
    """{(skjema, tabell) med små bokstaver: [tabeller som har fremmednøkkel dit]}."""
    out: dict = {}
    for schema in schemas or []:
        sname = schema.get("name", "")
        for tbl in schema.get("tables", []):
            for fk in tbl.get("foreign_keys", []) or []:
                rs = (fk.get("ref_schema") or sname).lower()
                rt = (fk.get("ref_table") or "").lower()
                if not rt:
                    continue
                name = tbl.get("name", "")
                if rs != sname.lower():
                    name = f"{sname}.{name}"
                lst = out.setdefault((rs, rt), [])
                if name not in lst:
                    lst.append(name)
    for lst in out.values():
        lst.sort(key=str.lower)
    return out


def quality_notes(meta: dict) -> list:
    """Merknader til metadataene — det en mottaker bør vite eller rette."""
    notes: list = []
    span = (meta.get("data_origin_time_span") or "").strip()
    if span and not meta.get("data_origin_time_span_estimated"):
        try:
            from siard_workflow.core.date_span import guess_compact_span, metadata_span
            if metadata_span(span) is None:
                g = guess_compact_span(span)
                notes.append(
                    f"Tidsperioden «{span}» lar seg ikke tolke som datoer"
                    + (f" (ligner DDMMÅÅÅÅ: {g[0]} – {g[1]})" if g else "") + ".")
        except Exception:
            pass
    elif not span and not meta.get("data_start"):
        notes.append("Tidsperiode (dataOriginTimespan) er ikke oppgitt.")
    desc = (meta.get("description") or "").strip()
    if not desc or desc.lower() in ("(unspecified)", "unspecified", "-", "n/a"):
        notes.append("Uttrekket har ingen beskrivelse"
                     + (f" («{desc}»)." if desc else "."))
    tables = [(s.get("name", ""), t) for s in meta.get("schemas", [])
              for t in s.get("tables", [])]
    no_pk = sorted(t.get("name", "") for _s, t in tables if not t.get("primary_key"))
    if no_pk:
        shown = ", ".join(no_pk[:8]) + (f" og {len(no_pk) - 8} til" if len(no_pk) > 8 else "")
        notes.append(f"{len(no_pk)} tabell(er) har ingen primærnøkkel: {shown}.")
    n_empty = sum(1 for _s, t in tables if not (t.get("rows", 0) or 0))
    if n_empty:
        notes.append(f"{n_empty} av {len(tables)} tabeller er tomme (0 rader).")
    n_desc = sum(1 for _s, t in tables if (t.get("description") or "").strip())
    if tables and not n_desc:
        notes.append("Ingen tabeller har beskrivelse i metadata.xml.")
    return notes


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
    # Estimert datospenn fra «Estimer datospenn» (date_span) — alltid merket som
    # estimat. Når metadata.xml selv oppgir spennet, står det under «Tidsperiode»
    # og ingenting estimeres.
    _ds = _opts.get("date_span") or {}
    if _ds.get("source") == "metadata" or _ds.get("accepted") is False:
        _ds = {}                     # fra metadata.xml, eller forkastet av brukeren
    _ds_found = bool(_ds.get("found"))

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

    class Outline(Flowable):
        """Usynlig: setter et PDF-bokmerke (navigasjonspanelet) og et lenkemål."""
        def __init__(self, title, key, level=0):
            super().__init__()
            self.title, self.key, self.level = str(title), str(key), int(level)
            self.width = self.height = 0

        def wrap(self, aw, ah):
            return 0, 0

        def draw(self):
            self.canv.bookmarkPage(self.key)
            self.canv.addOutlineEntry(self.title, self.key, level=self.level,
                                      closed=self.level >= 1)

    _refs_by = referenced_by(meta.get("schemas", []))

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
        story.append(Outline("Databaseoversikt", "sec_overview", 0))
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
            ("Tilkobling",           _val(short_connection(meta.get("connection")))),
            ("DB-bruker",            _val(meta.get("db_user"))),
            ("Arkiveringsdato",      _val(meta.get("archival_date"))),
            ("Produsert av",         _val(meta.get("producer_app"))),
            ("Tidsperiode",             _val(meta.get("data_origin_time_span"))
             + (" (ESTIMERT ut fra uttrekket — ikke nødvendigvis 100 % korrekt)"
                if meta.get("data_origin_time_span_estimated") else "")),
            *([("Estimert datospenn",
                f"{_ds['from_year']}–{_ds['to_year']} (estimat fra uttrekket — se under)")]
              if _ds_found else []),
            ("SIARD-versjon",        _val(meta.get("siard_version"))),
            ("Meldingssammendrag",   digest_str),
            ("Antall ZIP-poster",    _num(meta.get("zip_entry_count", 0))),
            ("LOB-filer totalt",     _num(meta.get("lob_file_count", 0))),
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

        # ── Estimert datospenn (date_span) ────────────────────────────────
        if _ds:
            from xml.sax.saxutils import escape as _esc
            story.append(Paragraph("Estimert datospenn (fra uttrekket)", style_h2))
            if _ds_found:
                txt = (f"<b>{_ds['from_year']}–{_ds['to_year']}</b> "
                       f"({_esc(_ds.get('from_date', ''))} – {_esc(_ds.get('to_date', ''))}). "
                       f"Grunnlag: {_esc(_ds.get('basis_text', ''))}; "
                       f"sikkerhet: {_esc(_ds.get('confidence', ''))}.")
                txt += (f" metadata.xml oppga «{_esc(_ds['metadata_timespan'])}», som ikke er "
                        "et tolkbart datospenn." if _ds.get("metadata_timespan") else
                        " metadata.xml oppga ikke datospenn.")
                if _ds.get("written_to_metadata"):
                    txt += (f" Estimatet er skrevet inn i metadata.xml "
                            f"(&lt;dataOriginTimespan&gt;{_esc(_ds.get('written_value', ''))}"
                            f"&lt;/dataOriginTimespan&gt;) med en XML-kommentar om at "
                            f"verdien er estimert.")
            else:
                txt = _esc(_ds.get("message", "Ingen brukbare datoverdier funnet."))
            story.append(Paragraph(txt, style_body))
            story.append(Paragraph(
                "<i>" + _esc(_ds.get("note") or "Estimat — ikke nødvendigvis 100 % korrekt.")
                + "</i>", style_caption))
            story.append(Spacer(1, 4 * mm))

        # ── Merknader til metadataene ─────────────────────────────────────
        _notes = quality_notes(meta)
        if _notes:
            from xml.sax.saxutils import escape as _esc_n
            story.append(Paragraph("Merknader til metadataene", style_h2))
            for _note in _notes:
                story.append(Paragraph("•  " + _esc_n(_note), style_body))
            story.append(Spacer(1, 4 * mm))

        # ── Mest refererte tabeller (kjernen i datamodellen) ──────────────
        _central = sorted(((len(v), k[1]) for k, v in _refs_by.items()), reverse=True)[:8]
        if _central:
            _names = {t.get("name", "").lower(): t.get("name", "")
                      for s in meta.get("schemas", []) for t in s.get("tables", [])}
            story.append(Paragraph("Mest refererte tabeller", style_h2))
            story.append(Paragraph(
                "Tabellene flest andre tabeller peker til med fremmednøkkel: "
                + ", ".join(f"<b>{_names.get(t, t)}</b> ({n})" for n, t in _central) + ".",
                style_body))
            story.append(Spacer(1, 4 * mm))

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

            chart_h = max(60 * mm, len(top15) * 7 * mm + 20 * mm)
            # Overskriften skal ikke stå alene nederst på en side
            from reportlab.platypus import CondPageBreak
            story.append(CondPageBreak(chart_h + 20 * mm))
            story.append(Paragraph("Topp-15 tabeller etter radantall", style_h2))
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
        story.append(Outline("Tabelloversikt", "sec_inventory", 0))
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
        # «Beskrivelse» vises bare når noen tabell har en — ellers får
        # tabellnavnet plassen (lange navn ble brutt midt i ordet)
        has_desc = any((t.get("description") or "").strip() for _s, t in all_tables)
        CW_SCHEMA = 16 * mm
        CW_ROWS   = 20 * mm
        CW_LOB    = 18 * mm
        if has_desc:
            CW_NAME = 52 * mm
            CW_PK   = 30 * mm
            CW_DESC = CONTENT_W - CW_SCHEMA - CW_NAME - CW_ROWS - CW_LOB - CW_PK
            col_widths = [CW_SCHEMA, CW_NAME, CW_ROWS, CW_LOB, CW_PK, CW_DESC]
        else:
            CW_NAME = 68 * mm
            CW_PK   = CONTENT_W - CW_SCHEMA - CW_NAME - CW_ROWS - CW_LOB
            col_widths = [CW_SCHEMA, CW_NAME, CW_ROWS, CW_LOB, CW_PK]

        headers = [
            _p("Skjema",       style_cell_bold),
            _p("Tabell",       style_cell_bold),
            _p("Rader",        style_cell_bold),
            _p("LOB-filer",    style_cell_bold),
            _p("Primærnøkkel", style_cell_bold),
        ]
        if has_desc:
            headers.append(_p("Beskrivelse", style_cell_bold))
        story.append(Paragraph(
            "Klikk på et tabellnavn for å gå til kolonner og nøkler. Tabellene "
            "finnes også som bokmerker i PDF-leseren.", style_caption))
        story.append(Spacer(1, 2 * mm))

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
            name_cell = Paragraph(
                f'<a href="#{_tbl_key(sname, tbl_name)}" color="#1a3866">'
                f"<b>{tbl_name}</b></a>"
                + (f' &nbsp;<font size="6.5" color="#8c8c8c">{tbl_folder}</font>'
                   if tbl_folder else ""), style_cell)

            # LOB-filer: vis antall (tom hvis ingen)
            lob_str = _num(lob_file_cnt) if lob_file_cnt > 0 else ""

            # ── Tabellrad ──────────────────────────────────────────────────
            tbl_row = [
                _p(_trunc_name(sname), style_cell_bold),
                name_cell,
                _p(rows_str, style_cell_bold),
                _p(lob_str, style_cell_center),
                _p(pk_str, style_cell),
            ]
            if has_desc:
                tbl_row.append(_p(desc, style_cell))
            rows.append(tbl_row)

            bg = C_LOB_YELLOW if has_lob else C_TBL_ROW
            row_styles.append(("BACKGROUND", (0, ri), (-1, ri), bg))
            row_styles.append(("TOPPADDING",    (0, ri), (-1, ri), 2))
            row_styles.append(("BOTTOMPADDING", (0, ri), (-1, ri), 2.5))
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

        # ── LOB-kolonner: antall filer og størrelse per kolonne ───────────
        lob_rows = []
        for sname, tbl in all_tables:
            for lc in tbl.get("lob_columns") or []:
                lob_rows.append([
                    _p(f"<b>{tbl.get('name', '')}</b>", style_cell),
                    _p(lc.get("column") or lc.get("folder", ""), style_cell),
                    _p(_abbrev_type(lc.get("type", "")) or "–", style_cell),
                    _p(lc.get("folder", ""), style_cell),
                    _p(_num(lc.get("files", 0)), style_cell),
                    _p(_human_size(lc.get("bytes", 0)), style_cell),
                ])
        if lob_rows:
            story.append(Spacer(1, 6 * mm))
            story.append(Paragraph("LOB-kolonner (filer i uttrekket)", style_h2))
            lt = Table([[_p(h, style_cell_bold) for h in
                         ("Tabell", "Kolonne", "SQL-type", "Mappe", "Filer", "Størrelse")]]
                       + lob_rows,
                       colWidths=[52 * mm, 44 * mm, 22 * mm, 16 * mm, 18 * mm,
                                  CONTENT_W - 152 * mm], repeatRows=1)
            lt.setStyle(_std_table_style())
            story.append(lt)

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
        from xml.sax.saxutils import escape as _esc
        story = []
        story.append(Outline("Skjema- og tabelldetaljer", "sec_details", 0))
        story.append(Paragraph("Skjema- og tabelldetaljer", style_h1))
        story.append(HRFlowable(width=CONTENT_W, thickness=1.0,
                                color=C_NAVY, spaceAfter=4*mm))

        schemas = meta.get("schemas", [])
        if not schemas:
            story.append(Paragraph("Ingen skjemaer funnet.", style_body))
            return story

        def _keys_block(sname: str, tbl: dict) -> list:
            """Primærnøkkel, fremmednøkler, unike nøkler og «refereres av»."""
            out = []
            pk = tbl.get("primary_key")
            if pk:
                out.append(Spacer(1, 1.5 * mm))
                out.append(Paragraph(
                    f'<b>Primærnøkkel</b> ({_esc(pk.get("name", ""))}): '
                    + _esc(", ".join(pk.get("columns", []))), style_body_small))
            fks = tbl.get("foreign_keys", [])
            if fks:
                out.append(Spacer(1, 1 * mm))
                out.append(Paragraph("<b>Fremmednøkler:</b>", style_body_small))
                for fk in fks:
                    ref_pairs = ", ".join(
                        f'{r.get("column")} → {r.get("referenced")}'
                        for r in fk.get("references", []))
                    ref_schema = fk.get("ref_schema", "")
                    ref_table  = fk.get("ref_table", "")
                    dest = f"{ref_schema}.{ref_table}" if ref_schema else ref_table
                    out.append(Paragraph(
                        f'&nbsp;&nbsp;{_esc(fk.get("name", ""))}: → {_esc(dest)}: '
                        f'{_esc(ref_pairs)}', style_body_small))
            uks = tbl.get("unique_keys", [])
            if uks:
                out.append(Spacer(1, 1 * mm))
                out.append(Paragraph("<b>Unike nøkler:</b>", style_body_small))
                for uk in uks:
                    out.append(Paragraph(
                        f'&nbsp;&nbsp;{_esc(uk.get("name", ""))}: '
                        + _esc(", ".join(uk.get("columns", []))), style_body_small))
            refs = _refs_by.get((sname.lower(), tbl.get("name", "").lower())) or []
            if refs:
                shown = ", ".join(refs[:14])
                if len(refs) > 14:
                    shown += f" og {len(refs) - 14} til"
                out.append(Spacer(1, 1 * mm))
                out.append(Paragraph(
                    f"<b>Refereres av</b> ({len(refs)}): {_esc(shown)}", style_body_small))
            return out

        for schema in schemas:
            schema_name = schema.get("name", "Ukjent")
            schema_desc = (schema.get("description") or "").strip()

            # ── Skjema-overskrift ────────────────────────────────────────
            story.append(Spacer(1, 4 * mm))
            story.append(Outline(f"Skjema: {schema_name}", f"sch_{id(schema)}", 1))
            story.append(Paragraph(f"Skjema: {_esc(schema_name)}", style_h2))
            if schema_desc:
                story.append(Paragraph(_esc(schema_desc), style_caption))
            story.append(HRFlowable(width=CONTENT_W, thickness=0.6,
                                    color=C_BLUE, spaceAfter=2*mm))

            # Alfabetisk — som tabelloversikten. Tabeller med innhold først;
            # tomme tabeller samles kompakt til slutt.
            tables = sorted(schema.get("tables", []),
                            key=lambda t: t.get("name", "").lower())
            if not tables:
                story.append(Paragraph("Ingen tabeller i dette skjemaet.", style_body))
                continue
            filled = [t for t in tables if (t.get("rows", 0) or 0) > 0]
            empty  = [t for t in tables if (t.get("rows", 0) or 0) <= 0]

            for tbl in filled:
                tname = tbl.get("name", "Ukjent")
                tbl_desc = (tbl.get("description") or "").strip()
                columns = tbl.get("columns", [])
                key = _tbl_key(schema_name, tname)

                # ── Tabell-overskrift ────────────────────────────────────
                tbl_block = [Spacer(1, 3 * mm), Outline(tname, key, 2)]
                folder = tbl.get("folder", "")
                sub = f"{_num(tbl.get('rows', 0) or 0)} rader"
                if folder:
                    sub += f" · {folder}"
                if tbl.get("lob_file_count"):
                    sub += f" · {_num(tbl['lob_file_count'])} LOB-filer"
                tbl_block.append(Paragraph(
                    f'<a name="{key}"/>{_esc(tname)} &nbsp;&nbsp;'
                    f'<font color="grey" size="9">{_esc(sub)}</font>', style_h3))
                if tbl_desc:
                    tbl_block.append(Paragraph(_esc(tbl_desc), style_caption))

                # ── Kolonnetabell — tomme kolonner («LOB», «Beskrivelse») skjules
                has_desc = any((c.get("description") or "").strip() for c in columns)
                has_lob  = any(c.get("is_lob") for c in columns)
                col_headers = ["Pos", "Kolonne", "SQL-type", "Orig.type"]
                if has_desc:
                    cw = [10*mm, 46*mm, 24*mm, 24*mm]
                else:
                    cw = [10*mm, 70*mm, 42*mm, 0]
                if has_lob:
                    col_headers.append("LOB")
                    cw.append(10*mm)
                if has_desc:
                    col_headers.append("Beskrivelse")
                    cw.append(CONTENT_W - sum(cw))
                else:
                    cw[3] = CONTENT_W - sum(cw)

                col_rows = [col_headers]
                col_row_styles = []
                for ci, col_info in enumerate(columns):
                    ri = ci + 1
                    is_lob = col_info.get("is_lob", False)
                    row = [str(col_info.get("pos", ci + 1)),
                           _p(_esc(col_info.get("name", ""))),
                           _abbrev_type(col_info.get("type", "")),
                           _abbrev_type(col_info.get("type_original", "")) or "–"]
                    if has_lob:
                        row.append("Ja" if is_lob else "")
                    if has_desc:
                        row.append(_p(_esc((col_info.get("description") or "").strip())))
                    col_rows.append(row)
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
                if has_lob:
                    col_ts.add("ALIGN", (4, 1), (4, -1), "CENTER")
                ct.setStyle(col_ts)
                tbl_block.append(ct)
                tbl_block.extend(_keys_block(schema_name, tbl))
                tbl_block.append(Spacer(1, 2 * mm))
                # Små tabeller holdes samlet. Større får brytes over sider
                # (kolonneoverskriften gjentas) — ellers blir halve sider tomme;
                # overskriften skal likevel ikke stå alene nederst.
                if len(columns) <= 12:
                    story.append(KeepTogether(tbl_block))
                else:
                    from reportlab.platypus import CondPageBreak
                    story.append(CondPageBreak(45 * mm))
                    story.extend(tbl_block)

            if empty:
                story.append(Spacer(1, 4 * mm))
                story.append(Outline(f"Tomme tabeller ({len(empty)})",
                                     f"empty_{id(schema)}", 2))
                story.append(Paragraph(f"Tomme tabeller ({len(empty)})", style_h3))
                story.append(Paragraph(
                    "Tabellene under har ingen rader i uttrekket. Strukturen er "
                    "vist kompakt: kolonnenavn med SQL-type.", style_caption))
                story.append(Spacer(1, 1.5 * mm))
                for tbl in empty:
                    tname = tbl.get("name", "Ukjent")
                    key = _tbl_key(schema_name, tname)
                    cols_txt = ", ".join(
                        f"{c.get('name', '')} <font color='grey'>"
                        f"{_esc(_abbrev_type(c.get('type', '')))}</font>"
                        for c in tbl.get("columns", []))
                    folder = tbl.get("folder", "")
                    block = [Paragraph(
                        f'<a name="{key}"/><b>{_esc(tname)}</b>'
                        + (f' <font color="grey">({_esc(folder)})</font>' if folder else "")
                        + f" — {cols_txt}", style_body_small)]
                    block.extend(_keys_block(schema_name, tbl))
                    block.append(Spacer(1, 2 * mm))
                    story.append(KeepTogether(block))

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
