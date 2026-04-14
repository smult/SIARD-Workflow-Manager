"""
gui/update_dialog.py  —  Oppdateringsdialog for SIARD Workflow Manager
"""
from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path

import customtkinter as ctk

from gui.styles import COLORS, FONTS


class UpdateDialog(ctk.CTkToplevel):
    """
    Vises når en ny versjon er tilgjengelig.
    Tilbyr Release Notes, Oppdater nå eller Hopp over.
    """

    def __init__(self, parent, info: dict):
        super().__init__(parent)
        self._info   = info
        self._parent = parent

        remote  = info.get("version", "?")
        from version import VERSION
        local   = VERSION

        self.title("Oppdatering tilgjengelig")
        self.geometry("520x380")
        self.resizable(False, False)
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self.lift()
        self.focus_force()

        # Sentrér over parent
        self.after(10, self._center)

        self._build(local, remote, info.get("release_notes", ""))

    def _center(self):
        p = self._parent
        px, py = p.winfo_x(), p.winfo_y()
        pw, ph = p.winfo_width(), p.winfo_height()
        w, h   = 520, 380
        x = px + (pw - w) // 2
        y = py + (ph - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _build(self, local: str, remote: str, notes: str):
        self.grid_columnconfigure(0, weight=1)

        # Header
        ctk.CTkLabel(
            self,
            text=f"Ny versjon tilgjengelig: v{remote}",
            font=ctk.CTkFont(family=FONTS["mono"], size=14, weight="bold"),
            text_color=COLORS["accent"],
        ).grid(row=0, column=0, padx=20, pady=(18, 2), sticky="w")

        ctk.CTkLabel(
            self,
            text=f"Din versjon: v{local}",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"],
        ).grid(row=1, column=0, padx=20, pady=(0, 10), sticky="w")

        # Release notes
        if notes:
            ctk.CTkLabel(
                self,
                text="Endringer:",
                font=ctk.CTkFont(family=FONTS["mono"], size=10, weight="bold"),
                text_color=COLORS["text"],
            ).grid(row=2, column=0, padx=20, pady=(0, 2), sticky="w")

            box = ctk.CTkTextbox(
                self,
                height=140,
                font=ctk.CTkFont(family=FONTS["mono"], size=10),
                fg_color=COLORS["panel"],
                text_color=COLORS["text_sub"],
                wrap="word",
                state="normal",
            )
            box.grid(row=3, column=0, padx=20, pady=(0, 12), sticky="ew")
            box.insert("end", notes)
            box.configure(state="disabled")

        # Progressbar (skjult til vi laster ned)
        self._prog_frame = ctk.CTkFrame(self, fg_color="transparent")
        self._prog_frame.grid(row=4, column=0, padx=20, pady=(0, 4), sticky="ew")
        self._prog_frame.grid_columnconfigure(0, weight=1)
        self._prog_frame.grid_remove()

        self._prog_bar = ctk.CTkProgressBar(
            self._prog_frame,
            fg_color=COLORS["bg"],
            progress_color=COLORS["accent"],
            height=8,
        )
        self._prog_bar.grid(row=0, column=0, sticky="ew")
        self._prog_bar.set(0)

        self._prog_lbl = ctk.CTkLabel(
            self._prog_frame,
            text="",
            font=ctk.CTkFont(family=FONTS["mono"], size=9),
            text_color=COLORS["muted"],
        )
        self._prog_lbl.grid(row=1, column=0, sticky="w", pady=(2, 0))

        # Statusmelding
        self._status = ctk.CTkLabel(
            self,
            text="",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"],
        )
        self._status.grid(row=5, column=0, padx=20, pady=(0, 6), sticky="w")

        # Knapper
        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.grid(row=6, column=0, padx=20, pady=(0, 16), sticky="e")

        btn_cfg = dict(
            height=30, corner_radius=6,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
        )

        self._skip_btn = ctk.CTkButton(
            btn_row, text="Hopp over",
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            command=self.destroy, width=110, **btn_cfg,
        )
        self._skip_btn.pack(side="left", padx=(0, 8))

        self._update_btn = ctk.CTkButton(
            btn_row, text="⬇  Oppdater nå",
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
            command=self._start_download, width=150, **btn_cfg,
        )
        self._update_btn.pack(side="left")

    # ── Nedlasting og installasjon ────────────────────────────────────────────

    def _start_download(self):
        url = self._info.get("download_url", "")
        if not url:
            self._set_status("Ingen nedlastingslenke i versjonsfilen.", error=True)
            return

        self._update_btn.configure(state="disabled")
        self._skip_btn.configure(state="disabled")
        self._prog_frame.grid()
        self._set_status("Laster ned …")

        def _worker():
            import updater as _upd
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tf:
                tmp_path = Path(tf.name)

            def _prog(done, total):
                if total > 0:
                    pct = done / total
                    self.after(0, lambda p=pct, d=done, t=total: self._update_progress(p, d, t))

            ok = _upd.download_zip(url, tmp_path, progress_cb=_prog)

            if not ok:
                self.after(0, lambda: self._set_status(
                    "Nedlasting feilet. Sjekk nettilgang.", error=True))
                self.after(0, lambda: self._update_btn.configure(state="normal"))
                self.after(0, lambda: self._skip_btn.configure(state="normal"))
                return

            self.after(0, lambda: self._set_status("Installerer …"))
            success, err = _upd.install_update(tmp_path)
            tmp_path.unlink(missing_ok=True)

            if success:
                self.after(0, self._install_done)
            else:
                self.after(0, lambda e=err: self._set_status(
                    f"Installasjon feilet: {e}", error=True))
                self.after(0, lambda: self._update_btn.configure(state="normal"))
                self.after(0, lambda: self._skip_btn.configure(state="normal"))

        threading.Thread(target=_worker, daemon=True).start()

    def _update_progress(self, pct: float, done: int, total: int):
        self._prog_bar.set(pct)
        mb_done  = done  / 1024 / 1024
        mb_total = total / 1024 / 1024
        self._prog_lbl.configure(
            text=f"{mb_done:.1f} / {mb_total:.1f} MB  ({pct*100:.0f}%)")

    def _install_done(self):
        self._prog_bar.set(1.0)
        self._prog_bar.configure(progress_color=COLORS["green"])
        self._set_status("✓ Installert! Start programmet på nytt for å ta i bruk ny versjon.")
        self._skip_btn.configure(text="Lukk", state="normal")
        self._update_btn.grid_remove() if hasattr(self._update_btn, "grid_remove") else None
        self._update_btn.configure(state="disabled")

    def _set_status(self, msg: str, error: bool = False):
        col = COLORS["red"] if error else COLORS["muted"]
        self._status.configure(text=msg, text_color=col)
