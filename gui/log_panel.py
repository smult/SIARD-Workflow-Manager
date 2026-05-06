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
        self._live_mode  = False
        self._show_all   = False
        self._show_all_cb = None
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
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["accent"])
        self._mode_lbl.grid(row=0, column=1, padx=(0, 6))

        self._show_all_btn = ctk.CTkButton(
            hdr, text="Vis alle", width=64, height=22,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self._toggle_show_all)
        self._show_all_btn.grid(row=0, column=2, padx=(0, 4))

        ctk.CTkButton(hdr, text="Tøm", width=50, height=22,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=10),
                      command=self.clear).grid(row=0, column=3)

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

    # ── Offentlige metoder ────────────────────────────────────────────────────

    def set_show_all_callback(self, cb) -> None:
        self._show_all_cb = cb

    def set_live_mode(self, active: bool) -> None:
        """
        Live-modus: vis kun siste LIVE_LINES linjer (med mindre show_all er på).
        Aktiveres når konvertering starter, deaktiveres når den er ferdig.
        """
        self._live_mode = active
        self._update_mode_label()

    def append(self, msg: str, level: str = "info") -> None:
        self._text.configure(state="normal")

        if not self._show_all:
            if self._live_mode:
                current = int(self._text.index("end-1c").split(".")[0])
                if current >= self.LIVE_LINES:
                    self._text.delete("1.0", "2.0")
            else:
                current = int(self._text.index("end-1c").split(".")[0])
                if current >= self.MAX_LINES:
                    self._text.delete("1.0", "2.0")

        self._text.insert("end", msg + "\n", level)
        self._text.configure(state="disabled")
        self._text.see("end")
        self._line_count += 1

    def redraw_all(self, entries: list) -> None:
        """Tegn om tekstboksen med alle logg-oppføringer (ignorerer LIVE_LINES)."""
        self._text.configure(state="normal")
        self._text.delete("1.0", "end")
        for level, msg in entries[-self.MAX_LINES:]:
            self._text.insert("end", msg + "\n", level)
        self._text.configure(state="disabled")
        self._text.see("end")

    def clear(self) -> None:
        self._text.configure(state="normal")
        self._text.delete("1.0", "end")
        self._text.configure(state="disabled")
        self._line_count = 0

    # ── Interne metoder ───────────────────────────────────────────────────────

    def _toggle_show_all(self) -> None:
        self._show_all = not self._show_all
        if self._show_all:
            self._show_all_btn.configure(
                text="Vis siste", fg_color=COLORS["accent"],
                hover_color=COLORS["accent_dim"])
        else:
            self._show_all_btn.configure(
                text="Vis alle", fg_color=COLORS["btn"],
                hover_color=COLORS["btn_hover"])
        self._update_mode_label()
        if self._show_all_cb:
            self._show_all_cb(self._show_all)

    def _update_mode_label(self) -> None:
        if self._show_all:
            self._mode_lbl.configure(text="● alle linjer")
        elif self._live_mode:
            self._mode_lbl.configure(text=f"● live ({self.LIVE_LINES} linjer)")
        else:
            self._mode_lbl.configure(text="")
