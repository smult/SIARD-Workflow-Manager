"""
gui/progress_panel.py

Vises kun når BlobConvertOperation kjører (init_run → grid(), finish → grid_remove()).

Layout:
  - Header: BLOB-KONVERTERING, ETA, Pause, Stopp
  - Tofarge Canvas-bar: blå=PDF/A, gul=beholdt, rød=feil
  - Fargeforklaring
  - Tellerbokser: Detektert / PDF/A / Beholdt / Feilet
  - 5 Canvas-fasebarer (ingen CTkProgressBar — full fargekontroll)
"""
from __future__ import annotations
import time
import tkinter as tk
import customtkinter as ctk
from gui.styles import COLORS, FONTS

_PHASE_LABELS = [
    "Skanner arkiv",
    "Pakker ut filer",
    "Detekterer filer",
    "Konverterer filer",
    "Oppdaterer XML",
    "Pakker ny SIARD",
]

_PHASE_COLORS = [
    COLORS["accent"],   # blå
    COLORS["accent"],   # blå
    COLORS["accent"],   # blå
    COLORS["accent"],   # blå
    COLORS["accent"],   # blå
    COLORS["accent"],   # blå
]


def _rr(c: tk.Canvas, x1, y1, x2, y2, r, fill):
    """Avrundet rektangel på Canvas."""
    if x2 <= x1:
        return
    r = min(r, (x2 - x1) // 2, max(1, (y2 - y1) // 2))
    if r <= 0:
        c.create_rectangle(x1, y1, x2, y2, fill=fill, outline="")
        return
    c.create_rectangle(x1+r, y1, x2-r, y2, fill=fill, outline="")
    c.create_rectangle(x1, y1+r, x2, y2-r, fill=fill, outline="")
    c.create_oval(x1,     y1,     x1+2*r, y1+2*r, fill=fill, outline="")
    c.create_oval(x2-2*r, y1,     x2,     y1+2*r, fill=fill, outline="")
    c.create_oval(x1,     y2-2*r, x1+2*r, y2,     fill=fill, outline="")
    c.create_oval(x2-2*r, y2-2*r, x2,     y2,     fill=fill, outline="")


class _PhaseCanvas:
    """Én Canvas-progressbar for en fase. Ingen CTkProgressBar."""
    H = 8

    def __init__(self, parent: tk.Widget):
        self._pct   = 0.0
        self._color = COLORS["muted"]
        self._c = tk.Canvas(parent, height=self.H, bd=0,
                            highlightthickness=0, bg=COLORS["bg"])
        self._c.bind("<Configure>", lambda e: self._draw())

    def widget(self) -> tk.Canvas:
        return self._c

    def set_waiting(self):
        self._pct   = 0.0
        self._color = COLORS["muted"]
        self._draw()

    def set_active(self, pct: float = 0.0):
        self._pct   = max(0.0, min(1.0, pct))
        self._color = COLORS["accent"]
        self._draw()

    def set_done(self):
        self._pct   = 1.0
        self._color = COLORS["green"]
        self._draw()

    def _draw(self):
        c = self._c
        w = c.winfo_width()
        h = c.winfo_height()
        if w < 4 or h < 2:
            return
        c.delete("all")
        r = h // 2
        _rr(c, 0, 0, w, h, r, COLORS["panel"])
        if self._pct > 0:
            fw = max(int(w * self._pct), r * 2)
            _rr(c, 0, 0, fw, h, r, self._color)


class ProgressPanel(ctk.CTkFrame):

    def __init__(self, parent):
        super().__init__(parent, fg_color=COLORS["surface"], corner_radius=10)
        self.grid_columnconfigure(0, weight=1)

        self._total     = 0
        self._converted = 0
        self._kept      = 0
        self._failed    = 0
        self._start_ts  = 0.0
        self._pause_cb  = None
        self._stop_cb   = None

        # Fase-tilstand
        self._current_phase  = 0
        self._phase_state:    list[str]   = []
        self._phase_progress: list[float] = []
        self._phase_canvases: list[_PhaseCanvas] = []

        self._build()
        self.grid_remove()   # skjult til init_run() kalles

    def _build(self):
        # ── Header ──────────────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.grid(row=0, column=0, sticky="ew", padx=14, pady=(10, 4))
        hdr.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(hdr, text="BLOB-KONVERTERING",
                     font=ctk.CTkFont(family=FONTS["mono"], size=10, weight="bold"),
                     text_color=COLORS["accent"]).grid(row=0, column=0, sticky="w")

        self._eta_lbl = ctk.CTkLabel(hdr, text="",
                                      font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                      text_color=COLORS["muted"])
        self._eta_lbl.grid(row=0, column=1, sticky="e")

        btn_cfg = dict(height=26, width=80, corner_radius=5,
                       font=ctk.CTkFont(family=FONTS["mono"], size=10))
        self._pause_btn = ctk.CTkButton(
            hdr, text="⏸ Pause",
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            command=self._on_pause, **btn_cfg)
        self._pause_btn.grid(row=0, column=2, padx=(8, 4))

        self._stop_btn = ctk.CTkButton(
            hdr, text="⏹ Stopp",
            fg_color="#3d1a1a", hover_color="#5a2020",
            text_color=COLORS["red"],
            command=self._on_stop, **btn_cfg)
        self._stop_btn.grid(row=0, column=3)

        # ── Tofarge Canvas-progressbar ───────────────────────────────────────
        bar_outer = ctk.CTkFrame(self, fg_color="transparent")
        bar_outer.grid(row=1, column=0, sticky="ew", padx=14, pady=(4, 2))
        bar_outer.grid_columnconfigure(0, weight=1)

        self._bar_canvas = tk.Canvas(bar_outer, height=20, bg=COLORS["panel"],
                                     highlightthickness=0, bd=0)
        self._bar_canvas.grid(row=0, column=0, sticky="ew")
        self._bar_canvas.bind("<Configure>", self._redraw_bar)

        self._pct_lbl = ctk.CTkLabel(bar_outer, text="0 / 0",
                                      font=ctk.CTkFont(family=FONTS["mono"],
                                                       size=11, weight="bold"),
                                      text_color=COLORS["text"], width=80, anchor="e")
        self._pct_lbl.grid(row=0, column=1, padx=(10, 0))

        # ── Fargeforklaring ───────────────────────────────────────────────────
        legend = ctk.CTkFrame(self, fg_color="transparent")
        legend.grid(row=2, column=0, sticky="w", padx=14, pady=(0, 4))
        for color, label in [(COLORS["accent"], "PDF/A"),
                              (COLORS["yellow"], "Beholdt/rename"),
                              (COLORS["red"],    "Feilet")]:
            dot = tk.Canvas(legend, width=10, height=10,
                            bg=COLORS["surface"], highlightthickness=0)
            dot.create_oval(1, 1, 9, 9, fill=color, outline="")
            dot.pack(side="left", padx=(0, 3))
            ctk.CTkLabel(legend, text=label,
                         font=ctk.CTkFont(family=FONTS["mono"], size=11),
                         text_color=COLORS["muted"]).pack(side="left", padx=(0, 12))

        # ── Tellerbokser ──────────────────────────────────────────────────────
        cnt = ctk.CTkFrame(self, fg_color="transparent")
        cnt.grid(row=3, column=0, sticky="ew", padx=14, pady=(0, 10))
        for i in range(4):
            cnt.grid_columnconfigure(i, weight=1)

        self._counters: dict[str, ctk.CTkLabel] = {}
        for col, (key, label, color) in enumerate([
            ("detected",  "Detektert", COLORS["text_sub"]),
            ("converted", "PDF/A",     COLORS["accent"]),
            ("kept",      "Beholdt",   COLORS["yellow"]),
            ("failed",    "Feilet",    COLORS["red"]),
        ]):
            box = ctk.CTkFrame(cnt, fg_color=COLORS["panel"], corner_radius=8)
            box.grid(row=0, column=col,
                     padx=(0 if col == 0 else 4, 0), sticky="ew")
            box.grid_columnconfigure(0, weight=1)
            num = ctk.CTkLabel(box, text="0",
                               font=ctk.CTkFont(family=FONTS["mono"],
                                                size=20, weight="bold"),
                               text_color=color)
            num.grid(row=0, column=0, pady=(8, 2))
            ctk.CTkLabel(box, text=label,
                         font=ctk.CTkFont(family=FONTS["mono"], size=11),
                         text_color=COLORS["muted"]).grid(
                             row=1, column=0, pady=(0, 8))
            self._counters[key] = num

        # ── 5 Canvas-fasebarer ────────────────────────────────────────────────
        pf = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=8)
        pf.grid(row=4, column=0, padx=14, pady=(0, 12), sticky="ew")
        pf.grid_columnconfigure(1, weight=1)   # progressbar-kolonne vokser
        pf.grid_columnconfigure(0, weight=0)
        pf.grid_columnconfigure(2, weight=0)
        pf.grid_columnconfigure(3, weight=0)

        self._phase_lbls: list[ctk.CTkLabel] = []
        self._phase_pcts: list[ctk.CTkLabel] = []

        for i, phase_label in enumerate(_PHASE_LABELS):
            pady = (10, 4) if i == 0 else (4, 4) if i < 4 else (4, 10)

            ctk.CTkLabel(pf, text=f"{i+1}",
                         font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                         text_color=COLORS["muted"],
                         width=16, anchor="e").grid(
                             row=i, column=0, padx=(10, 4), pady=pady)

            pc = _PhaseCanvas(pf)
            pc.widget().grid(row=i, column=1, sticky="ew", padx=(0, 6), pady=pady)
            self._phase_canvases.append(pc)

            lbl = ctk.CTkLabel(pf, text=phase_label,
                               font=ctk.CTkFont(family=FONTS["mono"], size=11),
                               text_color=COLORS["muted"],
                               anchor="w", width=170)
            lbl.grid(row=i, column=2, sticky="w", padx=(0, 4), pady=pady)
            self._phase_lbls.append(lbl)

            pct = ctk.CTkLabel(pf, text="",
                               font=ctk.CTkFont(family=FONTS["mono"], size=11),
                               text_color=COLORS["muted"],
                               anchor="e", width=70)
            pct.grid(row=i, column=3, padx=(0, 10), pady=pady)
            self._phase_pcts.append(pct)

        n = len(self._phase_canvases)
        self._phase_state    = ["venter"] * n
        self._phase_progress = [0.0]     * n

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def set_callbacks(self, pause_cb, stop_cb):
        self._pause_cb = pause_cb
        self._stop_cb  = stop_cb

    def _on_pause(self):
        if self._pause_cb:
            self._pause_cb()

    def _on_stop(self):
        if self._stop_cb:
            self._stop_cb()

    # ── Offentlige metoder ────────────────────────────────────────────────────

    def reset(self):
        """Full nullstilling — kalles ved valg av ny SIARD-fil."""
        self._total     = 0
        self._converted = 0
        self._kept      = 0
        self._failed    = 0
        self._start_ts  = time.time()

        self._pct_lbl.configure(text="", text_color=COLORS["muted"])
        self._eta_lbl.configure(text="", text_color=COLORS["muted"])
        for key in self._counters:
            self._counters[key].configure(text="0")

        n = len(self._phase_canvases)
        self._phase_state    = ["venter"] * n
        self._phase_progress = [0.0]     * n
        for pc in self._phase_canvases:
            pc.set_waiting()
        for lbl, default in zip(self._phase_lbls, _PHASE_LABELS):
            lbl.configure(text=default, text_color=COLORS["muted"])
        for pct in self._phase_pcts:
            pct.configure(text="")

        self._pause_btn.configure(state="disabled", text="⏸ Pause",
                                   fg_color=COLORS["btn"])
        self._stop_btn.configure(state="disabled")
        self._bar_canvas.after(10, self._redraw_bar)
        self.grid_remove()   # skjul panelet til ny kjøring starter

    def init_run(self, total: int):
        self._total     = total
        self._converted = 0
        self._kept      = 0
        self._failed    = 0
        self._start_ts  = time.time()

        self._pct_lbl.configure(text=f"0 / {total}", text_color=COLORS["text"])
        self._eta_lbl.configure(text="", text_color=COLORS["muted"])
        for key in self._counters:
            self._counters[key].configure(text="0")

        # Nullstill KUN faser som ikke allerede er ferdig/aktiv
        # — fase 1 og 2 kan allerede være grønne når init_run kalles
        if not self._phase_state or all(s == "venter" for s in self._phase_state):
            for pc in self._phase_canvases:
                pc.set_waiting()
            for lbl, default in zip(self._phase_lbls, _PHASE_LABELS):
                lbl.configure(text=default, text_color=COLORS["muted"])
            for pct in self._phase_pcts:
                pct.configure(text="")
            n = len(self._phase_canvases)
            self._phase_state    = ["venter"] * n
            self._phase_progress = [0.0]     * n

        self._pause_btn.configure(state="normal", text="⏸ Pause",
                                   fg_color=COLORS["btn"],
                                   hover_color=COLORS["btn_hover"])
        self._stop_btn.configure(state="normal")
        self._bar_canvas.after(10, self._redraw_bar)
        self.grid()

    def set_phase(self, phase: int, total_phases: int, label: str):
        # Forrige aktive fase → ferdig
        prev = getattr(self, "_current_phase", 0)
        if 0 < prev <= len(self._phase_state):
            if self._phase_state[prev - 1] == "aktiv":
                self._phase_state[prev - 1] = "ferdig"
                self._phase_progress[prev - 1] = 1.0
                self._phase_canvases[prev - 1].set_done()
                self._phase_lbls[prev - 1].configure(text_color=COLORS["green"])
                self._phase_pcts[prev - 1].configure(text="✓",
                                                     text_color=COLORS["green"])

        self._current_phase = phase
        idx = phase - 1
        if 0 <= idx < len(self._phase_canvases):
            self._phase_state[idx] = "aktiv"
            self._phase_progress[idx] = 0.0
            self._phase_canvases[idx].set_active(0.0)
            self._phase_lbls[idx].configure(
                text=label or _PHASE_LABELS[idx],
                text_color=COLORS["text"])
            self._phase_pcts[idx].configure(text="0%", text_color=COLORS["muted"])

    def set_phase_progress(self, done: int, total: int, label: str = ""):
        idx = self._current_phase - 1
        if 0 <= idx < len(self._phase_canvases):
            pct = done / total if total else 0
            self._phase_progress[idx] = pct
            self._phase_canvases[idx].set_active(pct)
            self._phase_pcts[idx].configure(
                text=f"{done:,}/{total:,}" if total < 10000 else f"{pct*100:.0f}%",
                text_color=COLORS["text"])

    def phase_done(self):
        idx = self._current_phase - 1
        if 0 <= idx < len(self._phase_canvases):
            self._phase_state[idx] = "ferdig"
            self._phase_progress[idx] = 1.0
            self._phase_canvases[idx].set_done()
            self._phase_lbls[idx].configure(text_color=COLORS["green"])
            self._phase_pcts[idx].configure(text="✓", text_color=COLORS["green"])

    def file_started(self, idx: int, filename: str,
                     detected_ext: str, mime: str):
        pass

    def file_done(self, idx: int, filename: str,
                  result_ext: str, ok: bool, msg: str, stats: dict):
        self._converted = stats.get("converted", self._converted)
        self._kept      = stats.get("kept",      self._kept)
        self._failed    = stats.get("failed",    0)
        done = self._converted + self._kept + self._failed

        self._pct_lbl.configure(text=f"{done} / {self._total}")
        self._redraw_bar()

        # Oppdater konverterings-progressbar (fase 4, indeks 3) — kun hvis aktiv
        conv_phase_idx = 3   # fase 4 = konvertering (0-indeksert)
        if (self._total > 0 and done > 0
                and 0 <= conv_phase_idx < len(self._phase_state)
                and self._phase_state[conv_phase_idx] == "aktiv"):
            pct = done / self._total
            self._phase_progress[conv_phase_idx] = pct
            self._phase_canvases[conv_phase_idx].set_active(pct)
            self._phase_pcts[conv_phase_idx].configure(
                text=f"{pct*100:.0f}%", text_color=COLORS["text"])

        elapsed = time.time() - self._start_ts
        if elapsed > 0 and 0 < done < self._total:
            eta   = elapsed / done * (self._total - done)
            m, s  = divmod(int(eta), 60)
            self._eta_lbl.configure(
                text=f"~{m}m {s}s igjen  ({done/elapsed:.1f} fil/s)",
                text_color=COLORS["muted"])
        elif done >= self._total and elapsed > 0:
            self._eta_lbl.configure(
                text=f"Ferdig  {elapsed:.1f}s  ({done/elapsed:.1f} fil/s)",
                text_color=COLORS["green"])
        self._update_counters(stats)

    def update_stats_and_bar(self, stats: dict, done: int):
        self._converted = stats.get("converted", self._converted)
        self._kept      = stats.get("kept",      self._kept)
        self._failed    = stats.get("failed",    0)
        self._pct_lbl.configure(text=f"{done} / {self._total}")
        self._redraw_bar()
        self._update_counters(stats)

        phase3_idx = 2
        if (self._total > 0 and done > 0
                and 0 <= phase3_idx < len(self._phase_state)
                and self._phase_state[phase3_idx] == "aktiv"):
            pct = done / self._total
            self._phase_progress[phase3_idx] = pct
            self._phase_canvases[phase3_idx].set_active(pct)
            self._phase_pcts[phase3_idx].configure(
                text=f"{pct*100:.0f}%", text_color=COLORS["text"])

    def update_spinner(self):
        pass   # Ikke lenger nødvendig — Canvas-barer re-renderes ikke av Tkinter

    def finish(self, stats: dict):
        self._converted = stats.get("converted", 0)
        self._kept      = stats.get("kept",      0)
        self._failed    = stats.get("failed",    0)
        self._redraw_bar()
        n_fail  = self._failed
        done    = self._converted + self._kept + n_fail
        color   = COLORS["red"] if n_fail > 0 else COLORS["green"]
        elapsed = time.time() - self._start_ts
        speed   = done / elapsed if elapsed > 0 else 0
        self._eta_lbl.configure(
            text=f"Fullfort  {elapsed:.1f}s  ({speed:.1f} fil/s)",
            text_color=color)
        self._pct_lbl.configure(text=f"{done} / {self._total}",
                                 text_color=color)
        self._update_counters(stats)
        # Merk aktive faser som ferdige
        for i, state in enumerate(self._phase_state):
            if state == "aktiv":
                self._phase_state[i] = "ferdig"
                self._phase_progress[i] = 1.0
                self._phase_canvases[i].set_done()
                self._phase_lbls[i].configure(text_color=COLORS["green"])
                self._phase_pcts[i].configure(text="✓", text_color=COLORS["green"])
        self.set_stopped()
        # Panelet forblir synlig etter ferdig — brukeren kan lese resultatene

    def set_paused(self, paused: bool):
        if paused:
            self._pause_btn.configure(text="▶ Fortsett",
                                       fg_color=COLORS["accent"],
                                       hover_color=COLORS["accent_dim"])
        else:
            self._pause_btn.configure(text="⏸ Pause",
                                       fg_color=COLORS["btn"],
                                       hover_color=COLORS["btn_hover"])

    def set_stopped(self):
        self._pause_btn.configure(state="disabled")
        self._stop_btn.configure(state="disabled")

    # ── Interne metoder ───────────────────────────────────────────────────────

    def _redraw_bar(self, event=None):
        c = self._bar_canvas
        w = c.winfo_width()
        h = c.winfo_height()
        if w < 2 or self._total == 0:
            return
        c.delete("all")
        r = 6
        _rr(c, 0, 0, w, h, r, COLORS["panel"])

        total    = self._total
        pct_conv = self._converted / total
        pct_kept = self._kept      / total
        pct_fail = self._failed    / total

        x = 0
        if pct_conv > 0:
            xe = int(w * pct_conv)
            _rr(c, x, 0, max(xe, r*2), h, r, COLORS["accent"])
            x = xe
        if pct_kept > 0:
            xe = int(w * (pct_conv + pct_kept))
            _rr(c, x, 0, max(xe, x+r), h, 0, COLORS["yellow"])
            x = xe
        if pct_fail > 0:
            xe = int(w * (pct_conv + pct_kept + pct_fail))
            _rr(c, x, 0, max(xe, x+r), h, 0, COLORS["red"])

    def _update_counters(self, stats: dict):
        for key in ("detected", "converted", "kept", "failed"):
            self._counters[key].configure(text=str(stats.get(key, 0)))
