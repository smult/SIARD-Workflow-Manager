"""
gui/format_chart_panel.py  —  Horisontalt søylediagram for detekterte filformater

Viser fortløpende oppdaterte søyler for hvert format som identifiseres
under BLOB-konvertering. Plasseres under workflow-listen i venstre panel.
"""
from __future__ import annotations

import tkinter as tk
import tkinter.font as tkfont
import customtkinter as ctk
from gui.styles import COLORS, FONTS, FontRegistry

# Fargekart per filformat
_FMT_COLORS: dict[str, str] = {
    # Dokumenter → konvertert
    "pdf":   "#e05252",
    "doc":   "#4f8ef7",
    "docx":  "#5ba8ff",
    "rtf":   "#f7a84f",
    "odt":   "#4fe0b0",
    "ppt":   "#b04fe0",
    "pptx":  "#cc7cf7",
    "odp":   "#d4a4f7",
    # Regneark → beholdt
    "xls":   "#2ecc71",
    "xlsx":  "#4de89a",
    "ods":   "#a4e8c0",
    # Bilder → beholdt
    "tiff":  "#f74fcf",
    "tif":   "#f74fcf",
    "jpg":   "#f74f9e",
    "jpeg":  "#f74f9e",
    "png":   "#ff8ecf",
    "gif":   "#ffb3d9",
    "bmp":   "#f7c4e0",
    "jp2":   "#f799cc",
    "svg":   "#e87ad4",
    # Lyd → beholdt
    "mp3":   "#f7e24f",
    "wav":   "#f7f04f",
    "flac":  "#e8e04f",
    "ogg":   "#d4cc4f",
    # Video → beholdt
    "mp4":   "#ff7043",
    "mpg":   "#ff8a65",
    "mpeg":  "#ff8a65",
    "avi":   "#ffab91",
    # Tekst/markup → beholdt
    "txt":   "#5a637a",
    "xml":   "#7af7f7",
    "html":  "#f7e84f",
    "htm":   "#f7e84f",
    "csv":   "#a0c0a0",
    # Kart/GIS
    "sosi":  "#80cbc4",
    "gml":   "#4db6ac",
    "ifc":   "#26a69a",
    # Pakker/arkiv
    "zip":   "#a0a0a0",
    "tar":   "#909090",
    "gz":    "#808080",
    # E-post
    "msg":   "#d4a44f",
    "eml":   "#c8a060",
    # Ukjent/binær
    "bin":   "#3d4560",
    "exe":   "#4a4a5a",
    "7z":    "#888888",
    "rar":   "#999999",
    "warc":  "#70a0b0",
    # WPTools native (konvertert via RTF→PDF/A)
    "wpt":   "#c87a3a",
}
_DEFAULT_COLOR = "#4f8ef7"


class FormatChartPanel(ctk.CTkFrame):
    """
    Viser horisontale søyler for antall filer per detektert format.
    Oppdateres løpende via update(ext, count).
    """

    BAR_H      = 16   # min. søylehøyde px (skaleres med font)
    ROW_H      = 26   # min. totalhøyde per rad (skaleres med font)
    MAX_ROWS   = 20   # maks antall format-rader å vise
    MIN_HEIGHT = 60
    MAX_HEIGHT = MAX_ROWS * ROW_H + 40   # header + rader
    LABEL_BASE = 10   # base-fontstørrelse — SAMME som overskriften «Filformater»

    def __init__(self, parent, **kwargs):
        super().__init__(parent,
                         fg_color=COLORS["surface"],
                         corner_radius=8,
                         **kwargs)
        self._counts:   dict[str, int] = {}
        self._canvas:   tk.Canvas | None = None
        self._built     = False
        self._build()

    def _build(self):
        self.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            self,
            text="Filformater",
            font=ctk.CTkFont(family=FONTS["mono"], size=self.LABEL_BASE,
                             weight="bold"),
            text_color=COLORS["muted"],
            anchor="w",
        ).grid(row=0, column=0, padx=10, pady=(6, 2), sticky="w")

        self._canvas = tk.Canvas(
            self,
            bg=COLORS["surface"],
            highlightthickness=0,
            height=self.MIN_HEIGHT,
        )
        self._canvas.grid(row=1, column=0, padx=6, pady=(0, 6), sticky="ew")
        self._canvas.bind("<Configure>", self._on_resize)
        self._built = True

        # Canvas-tegnet tekst bygger ikke på CTkFont, så den må tegnes om
        # eksplisitt når +/- endrer fontstørrelsen.
        FontRegistry.add_observer(self._redraw)

    def reset(self):
        """Nullstill ved ny kjøring."""
        self._counts.clear()
        self._redraw()

    def update_format(self, ext: str, count: int):
        """
        Oppdater antall for et format. Kalles fra _poll_log_queue
        for hvert file_done-event.
        """
        ext = ext.lower().strip(".") or "bin"
        self._counts[ext] = self._counts.get(ext, 0) + count
        self._redraw()

    def set_counts(self, counts: dict[str, int]):
        """Sett alle tellere på én gang (f.eks. ved oppstart av ny kjøring)."""
        self._counts = dict(counts)
        self._redraw()

    def _redraw(self):
        if not self._built or not self._canvas:
            return
        c = self._canvas
        c.delete("all")

        if not self._counts:
            return

        # Sorter etter antall, ta de MAX_ROWS største
        sorted_items = sorted(self._counts.items(), key=lambda x: -x[1])[:self.MAX_ROWS]
        total   = sum(self._counts.values())
        max_val = sorted_items[0][1] if sorted_items else 1
        n_rows  = len(sorted_items)

        # Fontstørrelse følger +/- (samme base som overskriften «Filformater»).
        fs = FontRegistry.effective_size(self.LABEL_BASE)
        # CTkFont (overskriften) tegnes i PIKSLER (negativ tk-størrelse) og
        # skaleres med widget-scaling. Canvas-tekst med POSITIV størrelse tolkes
        # som PUNKTER og blir ~30 % større ved 96 DPI. Speil derfor CTkFont:
        # bruk negativ (piksel) størrelse × samme scaling, slik at ledeteksten
        # blir NØYAKTIG like stor som overskriften.
        try:
            ws = self._get_widget_scaling()
        except Exception:
            ws = 1.0
        px         = max(1, round(fs * ws))
        cell_font  = (FONTS["mono"], -px)
        measure    = tkfont.Font(root=c, family=FONTS["mono"], size=-px)

        # Kolonnebredder måles ut fra faktisk tekst ved gjeldende font, slik at
        # verken formatnavnet eller tallet bak streken kuttes.
        label_texts = [f".{ext}" for ext, _ in sorted_items]
        count_texts = [f"{count:,}" for _, count in sorted_items]
        label_w = max((measure.measure(t) for t in label_texts), default=30) + 10
        count_w = max((measure.measure(t) for t in count_texts), default=20) + 10

        # Rad-/søylehøyde skalerer med (piksel-)fonten så teksten får plass.
        bar_h = max(self.BAR_H, px + 4)
        row_h = max(self.ROW_H, bar_h + px)

        height = max(self.MIN_HEIGHT, n_rows * row_h + 6)
        c.configure(height=height)

        width = c.winfo_width()
        if width < 10:
            width = 320   # fallback før widget er tegnet

        # Streken (søylene) gjøres smalere for å gi plass til tekst + tall.
        bar_area = max(10, width - label_w - count_w - 16)

        y = 3
        for ext, count in sorted_items:
            bar_w = max(2, int(bar_area * count / max_val))
            color = _FMT_COLORS.get(ext, _DEFAULT_COLOR)
            bar_y = y + (row_h - bar_h) // 2
            mid_y = bar_y + bar_h // 2

            # Ledetekst (format) — høyrejustert mot søylestarten
            c.create_text(
                label_w - 5, mid_y,
                text=f".{ext}",
                anchor="e",
                fill=COLORS["text"],
                font=cell_font,
            )
            # Søyle-bakgrunn
            c.create_rectangle(
                label_w, bar_y,
                label_w + bar_area, bar_y + bar_h,
                fill=COLORS["panel"], outline="",
            )
            # Søyle
            c.create_rectangle(
                label_w, bar_y,
                label_w + bar_w, bar_y + bar_h,
                fill=color, outline="",
            )
            # Tall (antall filer) — venstrejustert etter søylen
            c.create_text(
                label_w + bar_area + 6, mid_y,
                text=f"{count:,}",
                anchor="w",
                fill=COLORS["muted"],
                font=cell_font,
            )
            y += row_h

    def _on_resize(self, event):
        self._redraw()
