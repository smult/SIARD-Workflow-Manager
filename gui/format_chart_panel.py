"""
gui/format_chart_panel.py  —  Horisontalt søylediagram for detekterte filformater

Viser fortløpende oppdaterte søyler for hvert format som identifiseres
under BLOB-konvertering. Plasseres under workflow-listen i venstre panel.
"""
from __future__ import annotations

import tkinter as tk
import customtkinter as ctk
from gui.styles import COLORS, FONTS

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
}
_DEFAULT_COLOR = "#4f8ef7"


class FormatChartPanel(ctk.CTkFrame):
    """
    Viser horisontale søyler for antall filer per detektert format.
    Oppdateres løpende via update(ext, count).
    """

    BAR_H      = 16   # søylehøyde px
    ROW_H      = 26   # totalhøyde per rad (søyle + label + margin)
    MAX_ROWS   = 20   # maks antall format-rader å vise
    MIN_HEIGHT = 60
    MAX_HEIGHT = MAX_ROWS * ROW_H + 40   # header + rader

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
            font=ctk.CTkFont(family=FONTS["mono"], size=10, weight="bold"),
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
        height  = max(self.MIN_HEIGHT, n_rows * self.ROW_H + 4)
        c.configure(height=height)

        width = c.winfo_width()
        if width < 10:
            width = 340   # fallback før widget er tegnet

        label_w = 46   # px for format-teksten til venstre
        count_w = 40   # px for tall til høyre
        bar_area = max(10, width - label_w - count_w - 12)

        y = 4
        for ext, count in sorted_items:
            bar_w   = max(2, int(bar_area * count / max_val))
            color   = _FMT_COLORS.get(ext, _DEFAULT_COLOR)
            bar_y   = y + (self.ROW_H - self.BAR_H) // 2

            # Label
            c.create_text(
                label_w - 4, bar_y + self.BAR_H // 2,
                text=f".{ext}",
                anchor="e",
                fill=COLORS["text"],
                font=(FONTS["mono"], 9),
            )
            # Søyle bakgrunn
            c.create_rectangle(
                label_w, bar_y,
                label_w + bar_area, bar_y + self.BAR_H,
                fill=COLORS["panel"], outline="",
            )
            # Søyle
            c.create_rectangle(
                label_w, bar_y,
                label_w + bar_w, bar_y + self.BAR_H,
                fill=color, outline="",
            )
            # Tall
            pct = count / total * 100 if total else 0
            c.create_text(
                label_w + bar_area + 4,
                bar_y + self.BAR_H // 2,
                text=f"{count:,}",
                anchor="w",
                fill=COLORS["muted"],
                font=(FONTS["mono"], 9),
            )
            y += self.ROW_H

    def _on_resize(self, event):
        self._redraw()
