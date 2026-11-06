"""
gui/header_panel.py
SIARD-header (feltene øverst i header/metadata.xml) i et panel til høyre for
kjøreloggen. Feltene kan endres mens workflowen går; endringene samles som
«ventende» og skrives inn i den utpakkede metadata.xml mellom stegene (av
kjøretråden i app.py) — senest rett før «Pakk sammen SIARD». Steg som endrer
headeren oppdaterer panelet etter at de er ferdige.

  HeaderDock   skillelinje (dra for bredde) + HeaderPanel (» minimerer), eller en smal
               stripe ytterst til høyre når panelet er minimert
  HeaderPanel  feltene; take_pending()/invalid_fields() er trådsikre
"""
from __future__ import annotations

import threading
import tkinter as tk

import customtkinter as ctk

from gui.styles import COLORS, FONTS
from siard_workflow.core.siard_header import FIELDS, validate

MIN_WIDTH = 260
MIN_LOG_WIDTH = 320      # kjøreloggen skal alltid ha minst så mye
STRIP_WIDTH = 30


def _norm(v: str) -> str:
    return (v or "").replace("\r\n", "\n").strip()


class HeaderPanel(ctk.CTkFrame):
    def __init__(self, parent, on_collapse=None):
        super().__init__(parent, fg_color=COLORS["surface"], corner_radius=10)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)
        self._on_collapse = on_collapse
        self._lock = threading.Lock()
        self._base: dict[str, str] = {}
        self._pending: dict[str, str] = {}
        self._invalid: dict[str, str] = {}
        self._editable = False
        self._loading = False
        self._vars: dict[str, tk.StringVar] = {}
        self._inputs: dict[str, object] = {}
        self._marks: dict[str, ctk.CTkLabel] = {}
        self._errors: dict[str, ctk.CTkLabel] = {}
        self._ro_labels: dict[str, ctk.CTkLabel] = {}
        self._build()

    # ── Oppbygging ────────────────────────────────────────────────────────────

    def _build(self) -> None:
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.grid(row=0, column=0, sticky="ew", padx=12, pady=(10, 0))
        hdr.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(hdr, text="SIARD-HEADER",
                     font=ctk.CTkFont(family=FONTS["mono"], size=10, weight="bold"),
                     text_color=COLORS["muted"]).grid(row=0, column=0, sticky="w")
        self._undo_btn = ctk.CTkButton(
            hdr, text="Angre", width=56, height=22,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
            border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self.undo, state="disabled")
        self._undo_btn.grid(row=0, column=1, padx=(0, 4))
        ctk.CTkButton(
            hdr, text="»", width=26, height=22,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
            border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=lambda: self._on_collapse and self._on_collapse(),
        ).grid(row=0, column=2)

        self._source_lbl = ctk.CTkLabel(
            self, text="", anchor="w", justify="left",
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["accent"])
        self._source_lbl.grid(row=1, column=0, sticky="ew", padx=12, pady=(2, 4))

        body = ctk.CTkScrollableFrame(self, fg_color="transparent")
        body.grid(row=2, column=0, sticky="nsew", padx=(6, 2), pady=0)
        body.grid_columnconfigure(0, weight=1)
        self._body = body

        lbl_font = ctk.CTkFont(family=FONTS["ui"], size=11)
        in_font = ctk.CTkFont(family=FONTS["mono"], size=11)
        small = ctk.CTkFont(family=FONTS["ui"], size=10)
        r = 0
        for f in FIELDS:
            if not f.editable:
                continue
            row = ctk.CTkFrame(body, fg_color="transparent")
            row.grid(row=r, column=0, sticky="ew", padx=4, pady=(6, 0))
            row.grid_columnconfigure(0, weight=1)
            ctk.CTkLabel(row, text=f.label + (" *" if f.required else "")
                         + (f"  ({f.hint})" if f.hint else ""),
                         font=lbl_font, text_color=COLORS["text_sub"],
                         anchor="w").grid(row=0, column=0, sticky="w")
            mark = ctk.CTkLabel(row, text="", font=small, text_color=COLORS["accent"])
            mark.grid(row=0, column=1, sticky="e")
            self._marks[f.tag] = mark
            r += 1
            if f.multiline:
                w = ctk.CTkTextbox(body, height=72, wrap="word", font=in_font,
                                   fg_color=COLORS["bg"], text_color=COLORS["text"],
                                   border_width=1, border_color=COLORS["border"])
                for ev in ("<KeyRelease>", "<FocusOut>", "<<Paste>>"):
                    w.bind(ev, lambda _e, t=f.tag: self.after_idle(self._on_change, t),
                           add="+")
            else:
                var = tk.StringVar(self)
                var.trace_add("write", lambda *_a, t=f.tag: self._on_change(t))
                self._vars[f.tag] = var
                w = ctk.CTkEntry(body, textvariable=var, font=in_font,
                                 fg_color=COLORS["bg"], text_color=COLORS["text"])
            w.grid(row=r, column=0, sticky="ew", padx=4, pady=(2, 0))
            self._inputs[f.tag] = w
            r += 1
            err = ctk.CTkLabel(body, text="", font=small, text_color=COLORS["red"],
                               anchor="w", justify="left", wraplength=280)
            err.grid(row=r, column=0, sticky="ew", padx=6)
            err.grid_remove()
            self._errors[f.tag] = err
            r += 1

        ctk.CTkLabel(body, text="Teknisk (endres ikke her)", font=lbl_font,
                     text_color=COLORS["muted"], anchor="w").grid(
                         row=r, column=0, sticky="w", padx=4, pady=(12, 2))
        r += 1
        for tag, label in (("version", "SIARD-versjon"), ("lobFolder", "LOB-mappe"),
                           ("messageDigest", "Sjekksum")):
            lbl = ctk.CTkLabel(body, text=f"{label}: –", font=small,
                               text_color=COLORS["muted"], anchor="w", justify="left",
                               wraplength=280)
            lbl.grid(row=r, column=0, sticky="ew", padx=6)
            self._ro_labels[tag] = (lbl, label)
            r += 1

        self._note = ctk.CTkLabel(self, text="", font=small, text_color=COLORS["muted"],
                                  anchor="w", justify="left", wraplength=300)
        self._note.grid(row=3, column=0, sticky="ew", padx=12, pady=(4, 10))
        self.bind("<Configure>", self._on_resize, add="+")
        self._set_inputs_state(False)

    def _on_resize(self, ev) -> None:
        wl = max(160, ev.width - 40)
        self._note.configure(wraplength=wl)
        for e in self._errors.values():
            e.configure(wraplength=wl)
        for lbl, _l in self._ro_labels.values():
            lbl.configure(wraplength=wl)

    # ── Verdier i feltene ─────────────────────────────────────────────────────

    def get_value(self, tag: str) -> str:
        w = self._inputs[tag]
        if tag in self._vars:
            return self._vars[tag].get()
        return w.get("1.0", "end-1c")

    def set_value(self, tag: str, value: str) -> None:
        """Sett feltet (som om brukeren skrev det)."""
        w = self._inputs[tag]
        if tag in self._vars:
            self._vars[tag].set(value)
            return
        state = w._textbox.cget("state")
        w.configure(state="normal")
        w.delete("1.0", "end")
        w.insert("1.0", value)
        w.configure(state=state)
        self._on_change(tag)

    def _put(self, tag: str, value: str) -> None:
        """Sett feltet fra fila (ikke en endring)."""
        self._loading = True
        try:
            self.set_value(tag, value)
        finally:
            self._loading = False

    def _on_change(self, tag: str) -> None:
        if self._loading or not self._editable:
            return
        v = self.get_value(tag)
        with self._lock:
            if _norm(v) == _norm(self._base.get(tag, "")):
                self._pending.pop(tag, None)
                self._invalid.pop(tag, None)
            else:
                err = validate(tag, v)
                if err:
                    self._invalid[tag] = err
                    self._pending.pop(tag, None)
                else:
                    self._pending[tag] = v
                    self._invalid.pop(tag, None)
        self._show_state(tag)

    def _show_state(self, tag: str) -> None:
        with self._lock:
            pending = tag in self._pending
            err = self._invalid.get(tag)
            any_change = bool(self._pending or self._invalid)
        self._marks[tag].configure(
            text="● endret" if pending else ("● ugyldig" if err else ""),
            text_color=COLORS["red"] if err else COLORS["accent"])
        lbl = self._errors[tag]
        if err:
            lbl.configure(text=err + " — skrives ikke")
            lbl.grid()
        else:
            lbl.grid_remove()
        self._undo_btn.configure(state="normal" if any_change and self._editable
                                 else "disabled")

    def _set_inputs_state(self, editable: bool) -> None:
        for w in self._inputs.values():
            w.configure(state="normal" if editable else "disabled",
                        text_color=COLORS["text"] if editable else COLORS["muted"])

    # ── Offentlig ─────────────────────────────────────────────────────────────

    def load(self, info: dict, source: str, editable: bool, note: str) -> None:
        """Vis headeren for en ny fil (forkaster ventende endringer)."""
        with self._lock:
            self._pending.clear()
            self._invalid.clear()
        self.refresh(info, editable, note, source=source)

    def refresh(self, info: "dict | None", editable: bool, note: str,
                source: "str | None" = None) -> None:
        """
        Oppdater fra fila etter et steg. Felt brukeren holder på å endre
        (ventende eller ugyldige) overskrives ikke.
        """
        values = (info or {}).get("values", {})
        if source is not None:
            self._source_src = source
        ver = (info or {}).get("version", "")
        src = getattr(self, "_source_src", "")
        self._source_lbl.configure(text=src + (f"  ·  SIARD {ver}" if ver else ""))
        if not editable:
            with self._lock:
                self._pending.clear()
                self._invalid.clear()
        self._editable = False          # _put skal ikke registrere endringer
        self._set_inputs_state(True)
        for f in FIELDS:
            if not f.editable:
                continue
            fv = values.get(f.tag, "")
            with self._lock:
                keep = f.tag in self._pending or f.tag in self._invalid
            self._base[f.tag] = fv
            if not keep:
                self._put(f.tag, fv)
        self._editable = editable
        self._set_inputs_state(editable)
        for f in FIELDS:
            if f.editable:
                if editable and (f.tag in self._pending or f.tag in self._invalid):
                    self._on_change(f.tag)    # ny base kan gjøre feltet uendret
                self._show_state(f.tag)
        for tag, (lbl, label) in self._ro_labels.items():
            v = ver if tag == "version" else values.get(tag, "")
            lbl.configure(text=f"{label}: {v or '–'}")
        self._note.configure(text=note or "")

    def undo(self) -> None:
        """Tilbakestill alle felt til verdiene i fila."""
        with self._lock:
            tags = set(self._pending) | set(self._invalid)
            self._pending.clear()
            self._invalid.clear()
        for tag in tags:
            self._put(tag, self._base.get(tag, ""))
            self._show_state(tag)

    def is_editable(self) -> bool:
        return self._editable

    def take_pending(self) -> dict:
        """Ventende, gyldige endringer (tømmes). Trådsikker."""
        with self._lock:
            d = dict(self._pending)
            self._pending.clear()
            return d

    def invalid_fields(self) -> dict:
        """{tag: feiltekst} for felt med ugyldig verdi. Trådsikker."""
        with self._lock:
            return dict(self._invalid)


class HeaderDock(ctk.CTkFrame):
    """Skillelinje + header-panel, eller en smal stripe når minimert."""

    def __init__(self, parent, persist: bool = True):
        super().__init__(parent, fg_color="transparent")
        self.grid_rowconfigure(0, weight=1)
        self._persist = persist
        width, collapsed = 380, False
        if persist:
            try:
                from settings import get_config
                width = int(get_config("header_panel_width") or 380)
                collapsed = bool(get_config("header_panel_collapsed"))
            except Exception:
                pass
        self._width = max(MIN_WIDTH, width)

        self.sash = ctk.CTkFrame(self, width=8, fg_color="transparent",
                                 corner_radius=0, cursor="sb_h_double_arrow")
        self.sash.grid(row=0, column=0, sticky="ns")
        grip = ctk.CTkFrame(self.sash, width=2, height=40, corner_radius=1,
                            fg_color=COLORS["border"], cursor="sb_h_double_arrow")
        grip.place(relx=0.5, rely=0.5, anchor="center")
        for w in (self.sash, grip):
            w.bind("<ButtonPress-1>", self._drag_start)
            w.bind("<B1-Motion>", self._drag)
            w.bind("<ButtonRelease-1>", self._drag_end)
            w.bind("<Double-Button-1>", lambda _e: self.set_collapsed(True))

        self.panel = HeaderPanel(self, on_collapse=lambda: self.set_collapsed(True))
        self.panel.configure(width=self._width)
        self.panel.grid_propagate(False)
        self.panel.grid(row=0, column=1, sticky="ns")

        self.strip = ctk.CTkFrame(self, width=STRIP_WIDTH, fg_color=COLORS["surface"],
                                  corner_radius=10, cursor="hand2")
        self.strip.grid_propagate(False)
        self.strip.grid_rowconfigure(1, weight=1)
        ctk.CTkButton(
            self.strip, text="«", width=22, height=22,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
            border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=lambda: self.set_collapsed(False),
        ).grid(row=0, column=0, padx=4, pady=(10, 6))
        cv = tk.Canvas(self.strip, width=STRIP_WIDTH - 8, highlightthickness=0,
                       bg=COLORS["surface"], cursor="hand2")
        cv.grid(row=1, column=0, sticky="ns", pady=(0, 10))
        cv.create_text(STRIP_WIDTH // 2 - 4, 8, text="SIARD-HEADER", angle=-90,
                       anchor="w", fill=COLORS["muted"],
                       font=(FONTS["mono"], 10, "bold"))
        for w in (self.strip, cv):
            w.bind("<Button-1>", lambda _e: self.set_collapsed(False))
        self._collapsed = False
        if collapsed:
            self.set_collapsed(True, save=False)

    # ── Størrelse / minimering ────────────────────────────────────────────────

    def _scaling(self) -> float:
        try:
            return ctk.ScalingTracker.get_widget_scaling(self) or 1.0
        except Exception:
            return 1.0

    def _max_width(self) -> int:
        # winfo_* er fysiske piksler; width= er uskalert (CTk-skalering)
        try:
            total = self.master.winfo_width() / self._scaling()
        except Exception:
            total = 0
        return max(MIN_WIDTH, total - MIN_LOG_WIDTH) if total > 1 else 2000

    def set_width(self, w: int, save: bool = False) -> None:
        self._width = int(max(MIN_WIDTH, min(w, self._max_width())))
        self.panel.configure(width=self._width)
        if save:
            self._save("header_panel_width", self._width)

    def _drag_start(self, ev) -> None:
        self._drag_x = ev.x_root
        self._drag_w = self._width

    def _drag(self, ev) -> None:
        if getattr(self, "_drag_x", None) is None:
            return
        self.set_width(self._drag_w - (ev.x_root - self._drag_x) / self._scaling())

    def _drag_end(self, _ev) -> None:
        if getattr(self, "_drag_x", None) is not None:
            self._drag_x = None
            self._save("header_panel_width", self._width)

    def set_collapsed(self, collapsed: bool, save: bool = True) -> None:
        self._collapsed = collapsed
        if collapsed:
            self.sash.grid_remove()
            self.panel.grid_remove()
            self.strip.grid(row=0, column=1, sticky="ns", padx=(6, 0))
        else:
            self.strip.grid_remove()
            self.sash.grid()
            self.panel.grid()
        if save:
            self._save("header_panel_collapsed", collapsed)

    def is_collapsed(self) -> bool:
        return self._collapsed

    def _save(self, key: str, value) -> None:
        if not self._persist:
            return
        try:
            from settings import set_config
            set_config(key, value)
        except Exception:
            pass

    # ── Innhold ───────────────────────────────────────────────────────────────

    def load(self, info: dict, source: str, editable: bool, note: str) -> None:
        self.grid()
        self.panel.load(info, source, editable, note)

    def refresh(self, info: "dict | None", editable: bool, note: str) -> None:
        self.panel.refresh(info, editable, note)
