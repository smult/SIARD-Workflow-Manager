"""
gui/header_panel.py
SIARD-header (feltene øverst i header/metadata.xml) i et panel til høyre for
kjøreloggen. Feltene kan endres mens workflowen går. Endringene må bekreftes
(«Bekreft endringer»); bekreftede skrives inn i den utpakkede metadata.xml
mellom stegene (av kjøretråden i app.py) — senest rett før «Pakk sammen
SIARD». Ubekreftede endringer gir et spørsmål før sammenpakkingen. Steg som
endrer headeren oppdaterer panelet etter at de er ferdige.

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
        self._base: dict[str, str] = {}       # verdiene i fila
        self._pending: dict[str, str] = {}    # endret, gyldig, ikke bekreftet
        self._confirmed: dict[str, str] = {}  # bekreftet, ikke skrevet ennå
        self._invalid: dict[str, str] = {}    # {tag: feiltekst}
        self.on_confirm = None                # kalles med {tag: verdi} ved «Bekreft»
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

        self._confirm_btn = ctk.CTkButton(
            self, text="Bekreft endringer", height=30,
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
            text_color=COLORS["on_accent"],
            font=ctk.CTkFont(family=FONTS["ui"], size=12, weight="bold"),
            command=self.confirm, state="disabled")
        self._confirm_btn.grid(row=3, column=0, sticky="ew", padx=12, pady=(8, 0))

        self._note = ctk.CTkLabel(self, text="", font=small, text_color=COLORS["muted"],
                                  anchor="w", justify="left", wraplength=300)
        self._note.grid(row=4, column=0, sticky="ew", padx=12, pady=(4, 10))
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
        """Brukeren endret feltet: ny (ubekreftet) endring, eller tilbake til fila."""
        if self._loading or not self._editable:
            return
        v = self.get_value(tag)
        with self._lock:
            self._confirmed.pop(tag, None)    # endret igjen → må bekreftes på nytt
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
            confirmed = tag in self._confirmed
            err = self._invalid.get(tag)
        if err:
            mark, color = "● ugyldig", COLORS["red"]
        elif pending:
            mark, color = "● ikke bekreftet", COLORS["yellow"]
        elif confirmed:
            mark, color = "✓ bekreftet", COLORS["green"]
        else:
            mark, color = "", COLORS["accent"]
        self._marks[tag].configure(text=mark, text_color=color)
        lbl = self._errors[tag]
        if err:
            lbl.configure(text=err + " — skrives ikke")
            lbl.grid()
        else:
            lbl.grid_remove()
        self._update_buttons()

    def _update_buttons(self) -> None:
        with self._lock:
            n_pending = len(self._pending)
            any_change = bool(self._pending or self._invalid or self._confirmed)
        on = self._editable
        self._undo_btn.configure(state="normal" if any_change and on else "disabled")
        self._confirm_btn.configure(
            state="normal" if n_pending and on else "disabled",
            text=f"Bekreft endringer ({n_pending})" if n_pending else "Bekreft endringer")

    def confirm(self) -> None:
        """«Bekreft endringer»: endringene skrives inn og tas med ved sammenpakking."""
        with self._lock:
            done = dict(self._pending)
            self._confirmed.update(done)
            self._pending.clear()
        for tag in done:
            self._show_state(tag)
        self._update_buttons()
        if done and self.on_confirm:
            self.on_confirm(done)

    def _set_inputs_state(self, editable: bool) -> None:
        for w in self._inputs.values():
            w.configure(state="normal" if editable else "disabled",
                        text_color=COLORS["text"] if editable else COLORS["muted"])

    # ── Offentlig ─────────────────────────────────────────────────────────────

    def load(self, info: dict, source: str, editable: bool, note: str) -> None:
        """Vis headeren for en ny fil (forkaster ventende endringer)."""
        with self._lock:
            self._pending.clear()
            self._confirmed.clear()
            self._invalid.clear()
        self.refresh(info, editable, note, source=source)

    def refresh(self, info: "dict | None", editable: bool, note: str,
                source: "str | None" = None) -> None:
        """
        Oppdater fra fila etter et steg. Felt med endringer som ikke er
        skrevet ennå (ubekreftede, bekreftede eller ugyldige) overskrives ikke.
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
                self._confirmed.clear()
                self._invalid.clear()
        self._editable = False          # _put skal ikke registrere endringer
        self._set_inputs_state(True)
        for f in FIELDS:
            if not f.editable:
                continue
            fv = values.get(f.tag, "")
            self._base[f.tag] = fv
            with self._lock:
                keep = (f.tag in self._pending or f.tag in self._confirmed
                        or f.tag in self._invalid)
                if keep and _norm(self.get_value(f.tag)) == _norm(fv):
                    # fila har nå samme verdi → ikke lenger en endring
                    for d in (self._pending, self._confirmed, self._invalid):
                        d.pop(f.tag, None)
                    keep = False
            if not keep:
                self._put(f.tag, fv)
        self._editable = editable
        self._set_inputs_state(editable)
        for f in FIELDS:
            if f.editable:
                self._show_state(f.tag)
        for tag, (lbl, label) in self._ro_labels.items():
            v = ver if tag == "version" else values.get(tag, "")
            lbl.configure(text=f"{label}: {v or '–'}")
        self._note.configure(text=note or "")

    def undo(self) -> None:
        """Tilbakestill alle felt til verdiene i fila."""
        with self._lock:
            tags = set(self._pending) | set(self._confirmed) | set(self._invalid)
            self._pending.clear()
            self._confirmed.clear()
            self._invalid.clear()
        for tag in tags:
            self._put(tag, self._base.get(tag, ""))
            self._show_state(tag)
        self._update_buttons()

    def is_editable(self) -> bool:
        return self._editable

    def take_confirmed(self) -> dict:
        """Bekreftede endringer som skal skrives (tømmes). Trådsikker."""
        with self._lock:
            d = dict(self._confirmed)
            self._confirmed.clear()
            return d

    def unconfirmed(self) -> dict:
        """Gyldige endringer som ikke er bekreftet (uten å tømme). Trådsikker."""
        with self._lock:
            return dict(self._pending)

    def take_unconfirmed(self) -> dict:
        """Ubekreftede endringer (tømmes). Trådsikker."""
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
        self._ghost: "tk.Frame | None" = None
        self._drag_x = None

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

    # Under draing vises en tom flate i stedet for panelet: å tegne om alle
    # CTk-feltene for hver musebevegelse var svært tregt. Panelet får den nye
    # bredden først når knappen slippes, og flaten oppdateres høyst hvert 30. ms.

    def _drag_start(self, ev) -> None:
        self._drag_x = ev.x_root
        self._drag_w = self._width
        self._drag_target = self._width
        self._drag_job = None

    def _drag(self, ev) -> None:
        if getattr(self, "_drag_x", None) is None:
            return
        if self._ghost is None or not self._ghost.winfo_ismapped():
            self._show_ghost()
        w = self._drag_w - (ev.x_root - self._drag_x) / self._scaling()
        self._drag_target = int(max(MIN_WIDTH, min(w, self._max_width())))
        if self._drag_job is None:
            self._drag_job = self.after(30, self._drag_apply)

    def _drag_apply(self) -> None:
        self._drag_job = None
        if self._ghost is not None:
            self._ghost.configure(width=round(self._drag_target * self._scaling()))

    def _drag_end(self, _ev) -> None:
        if getattr(self, "_drag_x", None) is None:
            return
        self._drag_x = None
        if self._drag_job is not None:
            self.after_cancel(self._drag_job)
            self._drag_job = None
        if self._ghost is not None and self._ghost.winfo_ismapped():
            self._ghost.grid_remove()
            self.set_width(self._drag_target)
            if not self._collapsed:
                self.panel.grid()
            self._save("header_panel_width", self._width)

    def _show_ghost(self) -> None:
        if self._ghost is None:
            self._ghost = tk.Frame(self, bg=COLORS["surface"], highlightthickness=1,
                                   highlightbackground=COLORS["border"])
            tk.Label(self._ghost, text="SIARD-header\nslipp for å endre bredde",
                     bg=COLORS["surface"], fg=COLORS["muted"],
                     font=(FONTS["ui"], 10)).place(relx=0.5, rely=0.5, anchor="center")
        self._ghost.configure(width=max(1, self.panel.winfo_width()))
        self.panel.grid_remove()
        self._ghost.grid(row=0, column=1, sticky="ns")

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
