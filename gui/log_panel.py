"""
gui/log_panel.py
Loggpanel - viser kjørelogg med fargekoding per nivå.
"""
from __future__ import annotations
import customtkinter as ctk
from gui.styles import COLORS, FONTS, LOG_COLORS


class LogPanel(ctk.CTkFrame):
    MAX_LINES      = 2000   # maks linjer i full-modus
    LIVE_LINES     = 25     # linjer som vises i live-modus (under konvertering)

    def __init__(self, parent):
        super().__init__(parent, fg_color=COLORS["surface"], corner_radius=10)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._live_mode = False
        self._line_count = 0
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.grid(row=0, column=0, sticky="ew", padx=12, pady=(10, 4))
        hdr.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(hdr, text="KJØRELOGG",
                     font=ctk.CTkFont(family=FONTS["mono"], size=10, weight="bold"),
                     text_color=COLORS["muted"]).grid(row=0, column=0, sticky="w")

        self._mode_lbl = ctk.CTkLabel(
            hdr, text="",
            font=ctk.CTkFont(family=FONTS["mono"], size=9),
            text_color=COLORS["accent"])
        self._mode_lbl.grid(row=0, column=1, padx=(0, 8))

        ctk.CTkButton(hdr, text="Tøm", width=50, height=22,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=10),
                      command=self.clear).grid(row=0, column=2)

        self._text = ctk.CTkTextbox(
            self,
            fg_color=COLORS["bg"],
            text_color=COLORS["text"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            corner_radius=8,
            wrap="word",
            state="disabled",
        )
        self._text.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))

        for level, color in LOG_COLORS.items():
            self._text.tag_config(level, foreground=color)

    def set_live_mode(self, active: bool):
        """
        Live-modus: vis kun siste LIVE_LINES linjer.
        Aktiveres når konvertering starter, deaktiveres når den er ferdig.
        """
        self._live_mode = active
        if active:
            self._mode_lbl.configure(text=f"● live ({self.LIVE_LINES} linjer)")
        else:
            self._mode_lbl.configure(text="")

    def append(self, msg: str, level: str = "info"):
        self._text.configure(state="normal")

        if self._live_mode:
            # I live-modus: slett øverste linje hvis over grensen
            current = int(self._text.index("end-1c").split(".")[0])
            if current >= self.LIVE_LINES:
                self._text.delete("1.0", "2.0")
        else:
            # I normal modus: slett øverste linje hvis over MAX_LINES
            current = int(self._text.index("end-1c").split(".")[0])
            if current >= self.MAX_LINES:
                self._text.delete("1.0", "2.0")

        self._text.insert("end", msg + "\n", level)
        self._text.configure(state="disabled")
        self._text.see("end")
        self._line_count += 1

    def clear(self):
        self._text.configure(state="normal")
        self._text.delete("1.0", "end")
        self._text.configure(state="disabled")
        self._line_count = 0
