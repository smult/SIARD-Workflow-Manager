"""
gui/anonymize_preview_dialog.py

Modal forhåndsvisning før SIARD-anonymisering. Viser:
  - Identifiserte PII-kolonner med type + eksempler (før → etter)
  - LOB-kolonner som byttes til dummy-filer
  - Om lokal Ollama brukes (modell)

Brukes av AnonymizeOperation via callback fra bakgrunnstråden. Mønster som
gui/schema_selector_dialog.py: dialog opprettes på hovedtråden, arbeidstråden
blokkeres med threading.Event til operatør bekrefter eller avbryter.
"""
from __future__ import annotations

import threading
import customtkinter as ctk

from gui.styles import COLORS, FONTS


class AnonymizePreviewDialog(ctk.CTkToplevel):
    """Modal dialog. Kaller on_confirm(True) ved «Anonymiser», on_confirm(False)
    ved avbryt."""

    def __init__(self, parent, summary: dict, on_confirm):
        super().__init__(parent)
        self.title("Forhåndsvisning — SIARD-anonymisering")
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self.resizable(True, True)
        self.geometry("860x620")
        self.minsize(680, 460)

        self._summary = summary or {}
        self._on_confirm = on_confirm
        self._answered = False
        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self._build()

    # ── UI ─────────────────────────────────────────────────────────────────────

    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        cols     = self._summary.get("columns", [])
        lob_cols = self._summary.get("lob_columns", [])
        ollama   = self._summary.get("ollama_used")
        model    = self._summary.get("ollama_model", "")

        # Header
        hdr = ctk.CTkFrame(self, fg_color=COLORS["bg"], corner_radius=0)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            hdr, text="Forhåndsvisning av anonymisering",
            font=ctk.CTkFont(family=FONTS["mono"], size=14, weight="bold"),
            text_color=COLORS["accent"]
        ).grid(row=0, column=0, padx=14, pady=(10, 2), sticky="w")
        ollama_txt = (f"Lokal Ollama: aktiv ({model})" if ollama
                      else "Lokal Ollama: ikke i bruk — regex/heuristikk")
        ctk.CTkLabel(
            hdr,
            text=(f"{len(cols)} PII-kolonne(r), {len(lob_cols)} LOB-kolonne(r). "
                  f"{ollama_txt}. Verdier byttes deterministisk (samme verdi → "
                  "samme fiktive verdi)."),
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["muted"], wraplength=820, justify="left"
        ).grid(row=1, column=0, padx=14, pady=(0, 10), sticky="w")

        # Scrollbart innhold
        body = ctk.CTkScrollableFrame(self, fg_color=COLORS["panel"],
                                      corner_radius=8,
                                      scrollbar_button_color=COLORS["border"])
        body.grid(row=1, column=0, padx=16, pady=(4, 8), sticky="nsew")
        body.grid_columnconfigure(0, weight=1)
        r = 0

        if cols:
            r = self._section(body, r, "PII-felter (før → etter)")
            for col in cols:
                r = self._column_block(body, r, col)
        else:
            r = self._section(body, r, "Ingen PII-felter identifisert")

        if lob_cols:
            r = self._section(body, r, "Filer/LOB som byttes til dummy")
            for lc in lob_cols:
                ctk.CTkLabel(
                    body,
                    text=(f"  • {lc.get('table','')}.{lc.get('column','')}  "
                          f"({lc.get('n_files', 0)} fil(er))  → dummy"),
                    font=ctk.CTkFont(family=FONTS["mono"], size=11),
                    text_color=COLORS["text"], anchor="w"
                ).grid(row=r, column=0, padx=14, pady=2, sticky="w")
                r += 1

        # Knapper
        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=2, column=0, padx=16, pady=(0, 14), sticky="e")
        ctk.CTkButton(btns, text="Avbryt", width=110,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=12),
                      command=self._cancel).pack(side="left", padx=(0, 8))
        ctk.CTkButton(btns, text="Anonymiser", width=150,
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=12, weight="bold"),
                      command=self._confirm).pack(side="left")

    def _section(self, parent, r, title):
        ctk.CTkLabel(parent, text=title,
                     font=ctk.CTkFont(family=FONTS["mono"], size=12, weight="bold"),
                     text_color=COLORS["accent"]).grid(
                         row=r, column=0, padx=12, pady=(12, 4), sticky="w")
        return r + 1

    def _column_block(self, parent, r, col):
        ctk.CTkLabel(
            parent,
            text=(f"  {col.get('table','')}.{col.get('column','')}  "
                  f"[{col.get('pii_type','')}]  (kilde: {col.get('source','')})"),
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            text_color=COLORS["text"], anchor="w"
        ).grid(row=r, column=0, padx=14, pady=(6, 0), sticky="w")
        r += 1
        examples = col.get("examples", [])
        if not examples:
            ctk.CTkLabel(parent, text="      (ingen eksempelverdier)",
                         font=ctk.CTkFont(family=FONTS["mono"], size=10),
                         text_color=COLORS["muted"], anchor="w").grid(
                             row=r, column=0, padx=14, sticky="w")
            return r + 1
        for ex in examples:
            ctk.CTkLabel(
                parent,
                text=f"      {ex.get('before','')!r}  →  {ex.get('after','')!r}",
                font=ctk.CTkFont(family=FONTS["mono"], size=10),
                text_color=COLORS["muted"], anchor="w"
            ).grid(row=r, column=0, padx=14, sticky="w")
            r += 1
        return r

    # ── Svar ───────────────────────────────────────────────────────────────────

    def _confirm(self):
        if self._answered:
            return
        self._answered = True
        self._on_confirm(True)
        self.destroy()

    def _cancel(self):
        if self._answered:
            return
        self._answered = True
        self._on_confirm(False)
        self.destroy()


def ask_anonymize_confirm_modal(parent_after, parent_widget, summary: dict) -> bool:
    """
    Vis forhåndsvisningen fra en bakgrunnstråd. Poster dialog-opprettelse til
    hovedtråden og blokkerer til operatør svarer.

    parent_after:  widget.after (poster til hovedtråden)
    parent_widget: parent for dialogen (rotvinduet)
    Returnerer True (anonymiser) eller False (avbryt).
    """
    event = threading.Event()
    result = [False]

    def _show():
        def _cb(confirmed):
            result[0] = bool(confirmed)
            event.set()
        AnonymizePreviewDialog(parent_widget, summary, on_confirm=_cb)

    parent_after(0, _show)
    event.wait()
    return result[0]
