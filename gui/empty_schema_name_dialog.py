"""
gui/empty_schema_name_dialog.py

Modal dialog som spør operatør om navn for schemas der `<name>` er tom i
metadata.xml. Hver tom schema får en rad med:
  - Beskrivelse (schema #N, folder=...)
  - Tekstfelt forhåndsfylt med folder-navnet
  - Mulighet til å bruke folder som default

Returnerer dict {schema_index: new_name} ved OK, eller None ved Avbryt.
"""
from __future__ import annotations

import threading
import customtkinter as ctk
from typing import Optional

from gui.styles import COLORS, FONTS


class EmptySchemaNameDialog(ctk.CTkToplevel):
    """
    Modal som ber operatør fylle inn schema-navn for tomme schemas.

    Konstruktør:
      empties: liste av {index, folder} for schemas med tom <name>
      existing_names: sett av schema-navn som allerede er i bruk i SIARD-en
                       (disse kan IKKE velges — kollisjon vil bli avvist)
      on_confirm(fixes_or_none): callback med {index: new_name} eller None
    """

    def __init__(self, parent, empties: list[dict],
                 existing_names: set | None = None,
                 on_confirm=None):
        super().__init__(parent)
        self.title("Manglende schema-navn")
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self.resizable(True, False)
        n = len(empties)
        h = min(620, 240 + 80 * n)
        self.geometry(f"640x{h}")
        self.minsize(500, 240)

        self._empties = empties
        self._existing_names = {str(s).strip() for s in (existing_names or set()) if s}
        self._on_confirm = on_confirm
        self._entries: dict[int, ctk.StringVar] = {}
        self._entry_widgets: dict[int, ctk.CTkEntry] = {}
        self._error_labels: dict[int, ctk.CTkLabel] = {}

        # Default-folder kan kollidere med eksisterende navn — finn unik variant
        self._default_names: dict[int, str] = {}
        used = set(self._existing_names)
        for e in empties:
            folder = (e["folder"] or f"schema{e['index']}").strip()
            candidate = folder
            n_dup = 1
            while candidate in used:
                n_dup += 1
                candidate = f"{folder}_{n_dup}"
            self._default_names[e["index"]] = candidate
            used.add(candidate)

        self._build()
        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # Header
        hdr = ctk.CTkFrame(self, fg_color=COLORS["bg"], corner_radius=0)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(hdr,
                     text=f"{len(self._empties)} schema mangler navn i metadata.xml",
                     font=ctk.CTkFont(family=FONTS["mono"],
                                       size=14, weight="bold"),
                     text_color="#e0a040"
                     ).grid(row=0, column=0, padx=14, pady=(10, 4), sticky="w")
        ctk.CTkLabel(hdr,
                     text=("Schema-navn er obligatorisk i SIARD. Skriv inn et "
                           "navn per schema. Forhåndsutfylt med folder-navn — "
                           "rediger fritt eller behold default."),
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["muted"], wraplength=600, justify="left"
                     ).grid(row=1, column=0, padx=14, pady=(0, 10), sticky="w")

        # Scrollable liste
        scroll = ctk.CTkScrollableFrame(
            self, fg_color=COLORS["bg"], corner_radius=8,
            scrollbar_button_color=COLORS["border"])
        scroll.grid(row=1, column=0, padx=14, pady=8, sticky="nsew")
        scroll.grid_columnconfigure(1, weight=1)

        # Visuell indikator hvis det er eksisterende navn brukeren må unngå
        if self._existing_names:
            preview = ", ".join(sorted(self._existing_names)[:5])
            if len(self._existing_names) > 5:
                preview += f" … (+{len(self._existing_names) - 5} til)"
            ctk.CTkLabel(
                scroll,
                text=(f"⚠  Navn allerede i bruk (kan ikke velges): {preview}"),
                font=ctk.CTkFont(family=FONTS["mono"], size=11),
                text_color="#e0a040", anchor="w", wraplength=600,
                justify="left"
            ).grid(row=0, column=0, columnspan=2,
                   padx=8, pady=(0, 6), sticky="ew")

        for i, item in enumerate(self._empties, start=1):
            idx = item["index"]
            folder = item["folder"]
            default_name = self._default_names[idx]

            row = ctk.CTkFrame(scroll, fg_color=COLORS["panel"],
                                corner_radius=6)
            row.grid(row=i, column=0, columnspan=2, sticky="ew", pady=4, padx=2)
            row.grid_columnconfigure(1, weight=1)

            ctk.CTkLabel(
                row,
                text=f"Schema #{idx}  (folder: {folder})",
                font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                text_color=COLORS["text"], anchor="w"
            ).grid(row=0, column=0, padx=(12, 4), pady=(8, 0), sticky="w")

            var = ctk.StringVar(value=default_name)
            self._entries[idx] = var
            entry = ctk.CTkEntry(
                row, textvariable=var,
                fg_color=COLORS["bg"],
                font=ctk.CTkFont(family=FONTS["mono"], size=12),
                border_color=COLORS["border"], border_width=1)
            entry.grid(row=1, column=0, columnspan=2,
                       padx=(12, 12), pady=(2, 4), sticky="ew")
            self._entry_widgets[idx] = entry

            err_lbl = ctk.CTkLabel(
                row, text="", anchor="w",
                font=ctk.CTkFont(family=FONTS["mono"], size=10),
                text_color=COLORS["red"])
            err_lbl.grid(row=2, column=0, columnspan=2,
                         padx=(12, 12), pady=(0, 2), sticky="ew")
            self._error_labels[idx] = err_lbl

            ctk.CTkButton(
                row, text=f"Bruk «{default_name}»", width=200, height=24,
                fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                font=ctk.CTkFont(family=FONTS["mono"], size=10),
                command=lambda v=var, n=default_name: v.set(n)
            ).grid(row=3, column=0, padx=(12, 6), pady=(0, 8), sticky="w")

            # Bind live-validering ved redigering
            var.trace_add("write", lambda *_a, i=idx: self._validate_field(i))

        # Footer-knapper
        btns = ctk.CTkFrame(self, fg_color=COLORS["bg"], corner_radius=0)
        btns.grid(row=2, column=0, sticky="ew")
        btns.grid_columnconfigure(0, weight=1)

        bar = ctk.CTkFrame(btns, fg_color="transparent")
        bar.grid(row=0, column=0, padx=14, pady=10, sticky="e")

        ctk.CTkButton(bar, text="Avbryt", width=100,
                      fg_color=COLORS["btn"],
                      hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=12),
                      command=self._cancel
                      ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(bar, text="OK", width=120,
                      fg_color=COLORS["accent"],
                      hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"],
                                       size=12, weight="bold"),
                      command=self._ok
                      ).pack(side="left")

    def _validate_field(self, idx: int) -> str:
        """
        Sjekk én rad. Returnerer feilmelding eller "" hvis OK.
        Oppdaterer rød ramme + feiletikett som side-effekt.
        """
        var = self._entries.get(idx)
        if var is None:
            return ""
        name = (var.get() or "").strip()
        err = ""
        if not name:
            err = "Navn kan ikke være tomt."
        elif name in self._existing_names:
            err = f"«{name}» er allerede i bruk av et annet schema."
        else:
            # Sjekk mot andre rader i samme dialog
            for other_idx, other_var in self._entries.items():
                if other_idx == idx:
                    continue
                other_name = (other_var.get() or "").strip()
                if other_name and other_name == name:
                    err = f"«{name}» er valgt for et annet schema her."
                    break

        # Oppdater UI
        widget = self._entry_widgets.get(idx)
        err_lbl = self._error_labels.get(idx)
        if widget is not None:
            widget.configure(
                border_color=COLORS["red"] if err else COLORS["border"])
        if err_lbl is not None:
            err_lbl.configure(text=err)
        return err

    def _validate_all(self) -> list[str]:
        """Returnerer liste av feilmeldinger (én per problem-rad)."""
        return [self._validate_field(idx) for idx in self._entries]

    def _ok(self):
        # Kjør validering på alle felter først
        errors = [e for e in self._validate_all() if e]
        if errors:
            from tkinter import messagebox
            uniq = list(dict.fromkeys(errors))[:10]
            messagebox.showerror(
                "Schema-navn-konflikter",
                "Rett opp feilene i de røde feltene før du fortsetter:\n\n"
                + "\n".join(f"  • {e}" for e in uniq),
                parent=self)
            return

        fixes: dict[int, str] = {}
        for idx, var in self._entries.items():
            name = (var.get() or "").strip()
            if not name:
                # Burde være fanget av validering, men fall tilbake til default
                name = self._default_names.get(idx, f"schema{idx}")
            fixes[idx] = name
        self._on_confirm(fixes)
        self.destroy()

    def _cancel(self):
        self._on_confirm(None)
        self.destroy()


def ask_fill_empty_schema_names(parent_after, parent_widget,
                                  empties: list[dict],
                                  existing_names: set | None = None,
                                  ) -> Optional[dict]:
    """
    Tråd-sikker hjelpefunksjon: vis dialog fra bakgrunnstråd, blokker
    til operatør svarer.

    empties: liste av {index, folder} for schemas med tom name
    existing_names: sett av schema-navn som allerede er i bruk
                     (kan ikke gjenbrukes av dialogen)

    Returnerer {index: new_name} ved OK, eller None ved Avbryt.
    """
    event = threading.Event()
    result: list = [None]

    def _show():
        def _cb(fixes):
            result[0] = fixes
            event.set()
        EmptySchemaNameDialog(parent_widget, empties,
                              existing_names=existing_names,
                              on_confirm=_cb)

    parent_after(0, _show)
    event.wait()
    return result[0]
