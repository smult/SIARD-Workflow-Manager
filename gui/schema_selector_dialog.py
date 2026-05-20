"""
gui/schema_selector_dialog.py

Modal dialog som lar operatør velge hvilke schemas som skal være med i
resultat-SIARD. Viser:
  - Schema-navn + folder
  - Antall tabeller
  - Antall LOB-filer
  - Total størrelse (formatert)

Brukes av SchemaSelectorOperation via callback fra bakgrunnstråden.
"""
from __future__ import annotations

import threading
import tkinter as tk
import customtkinter as ctk
from typing import Optional

from gui.styles import COLORS, FONTS


def _fmt_bytes(n: int) -> str:
    """Formater byte-antall til lesbar tekst (KB/MB/GB)."""
    for unit, factor in (("GB", 1024**3), ("MB", 1024**2),
                         ("kB", 1024), ("B", 1)):
        if n >= factor or unit == "B":
            return f"{n / factor:.1f} {unit}"
    return f"{n} B"


class SchemaSelectorDialog(ctk.CTkToplevel):
    """
    Modal dialog med checkbox-liste over schemas.

    Konstruktør tar liste av {name, folder, tables[], size_bytes, file_count}
    og kaller `on_confirm(selected_names)` ved OK, eller `on_confirm(None)`
    ved avbryt.
    """

    def __init__(self, parent, schemas: list[dict], on_confirm):
        super().__init__(parent)
        self.title("Velg schemas som skal være med i SIARD")
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self.resizable(True, True)
        self.geometry("760x560")
        self.minsize(600, 400)

        self._schemas = schemas
        self._on_confirm = on_confirm
        self._vars: dict[str, ctk.BooleanVar] = {}

        self._build()

    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # Tittel + info
        hdr = ctk.CTkFrame(self, fg_color=COLORS["bg"], corner_radius=0)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(hdr,
                     text=f"Velg schemas ({len(self._schemas)} funnet)",
                     font=ctk.CTkFont(family=FONTS["mono"],
                                       size=14, weight="bold"),
                     text_color=COLORS["accent"]
                     ).grid(row=0, column=0, padx=14, pady=(10, 4), sticky="w")
        ctk.CTkLabel(hdr,
                     text=("Hak av for schemas som skal beholdes i resultatet. "
                           "Schemas som ikke velges blir fjernet fra både "
                           "metadata.xml og content/."),
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["muted"], wraplength=720, justify="left"
                     ).grid(row=1, column=0, padx=14, pady=(0, 10), sticky="w")

        # Scrollable liste
        scroll = ctk.CTkScrollableFrame(
            self, fg_color=COLORS["bg"], corner_radius=8,
            scrollbar_button_color=COLORS["border"])
        scroll.grid(row=1, column=0, padx=14, pady=8, sticky="nsew")
        scroll.grid_columnconfigure(0, weight=1)

        # Totalsum
        total_bytes = sum(s["size_bytes"] for s in self._schemas)
        total_files = sum(s["file_count"] for s in self._schemas)
        total_tables = sum(len(s["tables"]) for s in self._schemas)

        # Per-schema-rad
        for i, s in enumerate(self._schemas):
            row = ctk.CTkFrame(scroll, fg_color=COLORS["panel"], corner_radius=6)
            row.grid(row=i, column=0, sticky="ew", pady=4, padx=2)
            row.grid_columnconfigure(1, weight=1)

            var = ctk.BooleanVar(value=True)
            self._vars[s["name"]] = var
            ctk.CTkCheckBox(
                row, text="", variable=var, width=24,
                fg_color=COLORS["accent"],
                hover_color=COLORS["accent_dim"],
            ).grid(row=0, column=0, padx=(10, 8), pady=8, sticky="w")

            info = ctk.CTkFrame(row, fg_color="transparent")
            info.grid(row=0, column=1, padx=(0, 10), pady=6, sticky="ew")
            info.grid_columnconfigure(0, weight=1)

            ctk.CTkLabel(info,
                         text=f"{s['name']}",
                         font=ctk.CTkFont(family=FONTS["mono"],
                                          size=12, weight="bold"),
                         text_color=COLORS["text"], anchor="w"
                         ).grid(row=0, column=0, sticky="w")
            ctk.CTkLabel(info,
                         text=f"folder: {s['folder']}",
                         font=ctk.CTkFont(family=FONTS["mono"], size=10),
                         text_color=COLORS["muted"], anchor="w"
                         ).grid(row=1, column=0, sticky="w")

            # Tabeller (max 8 linjer)
            tbl_lines = []
            for t in s["tables"][:8]:
                tbl_lines.append(
                    f"    • {t['name']}  ({t.get('rows', 0):,} rader)")
            if len(s["tables"]) > 8:
                tbl_lines.append(
                    f"    … og {len(s['tables']) - 8} tabell(er) til")
            if tbl_lines:
                ctk.CTkLabel(info,
                             text="\n".join(tbl_lines),
                             font=ctk.CTkFont(family=FONTS["mono"], size=10),
                             text_color=COLORS["muted"], anchor="w",
                             justify="left"
                             ).grid(row=2, column=0, sticky="w", pady=(2, 0))

            # Statistikk
            ctk.CTkLabel(
                info,
                text=(f"Tabeller: {len(s['tables']):,}     "
                      f"Filer: {s['file_count']:,}     "
                      f"Størrelse: {_fmt_bytes(s['size_bytes'])}"),
                font=ctk.CTkFont(family=FONTS["mono"], size=11),
                text_color=COLORS["text"], anchor="w"
            ).grid(row=3, column=0, sticky="w", pady=(4, 0))

        # Footer: totaler + knapper
        footer = ctk.CTkFrame(self, fg_color=COLORS["bg"], corner_radius=0)
        footer.grid(row=2, column=0, sticky="ew")
        footer.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(footer,
                     text=(f"Totalt: {len(self._schemas)} schemas, "
                           f"{total_tables:,} tabeller, "
                           f"{total_files:,} filer, "
                           f"{_fmt_bytes(total_bytes)}"),
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["muted"]
                     ).grid(row=0, column=0, padx=14, pady=(8, 4), sticky="w")

        # Knapper
        btns = ctk.CTkFrame(footer, fg_color="transparent")
        btns.grid(row=1, column=0, padx=14, pady=(0, 10), sticky="e")

        ctk.CTkButton(btns, text="Velg alle", width=90,
                      fg_color=COLORS["btn"],
                      hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self._select_all
                      ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btns, text="Velg ingen", width=90,
                      fg_color=COLORS["btn"],
                      hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self._select_none
                      ).pack(side="left", padx=(0, 12))
        ctk.CTkButton(btns, text="Avbryt", width=100,
                      fg_color=COLORS["btn"],
                      hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=12),
                      command=self._on_cancel
                      ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btns, text="OK", width=120,
                      fg_color=COLORS["accent"],
                      hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"],
                                       size=12, weight="bold"),
                      command=self._on_ok
                      ).pack(side="left")

        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

    def _select_all(self):
        for v in self._vars.values():
            v.set(True)

    def _select_none(self):
        for v in self._vars.values():
            v.set(False)

    def _on_ok(self):
        selected = [name for name, v in self._vars.items() if v.get()]
        if not selected:
            from tkinter import messagebox
            ok = messagebox.askyesno(
                "Ingen schemas valgt",
                "Du har ikke valgt noen schemas. Hvis du fortsetter, vil "
                "alle schemas bli fjernet og SIARD blir tom.\n\n"
                "Vil du fortsette?",
                parent=self)
            if not ok:
                return
        self._on_confirm(selected)
        self.destroy()

    def _on_cancel(self):
        self._on_confirm(None)
        self.destroy()


def ask_select_schemas_modal(parent_after, parent_widget,
                              schemas: list[dict]
                              ) -> Optional[list[str]]:
    """
    Hjelpefunksjon for å vise dialogen fra en bakgrunnstråd.
    Poster dialog-opprettelse til hoved-tråden og blokkerer til svar.

    parent_after: kallbar (typisk widget.after) for å poste til main-thread
    parent_widget: parent for dialogen (typisk root window)
    schemas: liste av schema-dicts

    Returnerer liste av valgte schema-navn, eller None ved avbryt.
    """
    event = threading.Event()
    result: list = [None]

    def _show():
        def _cb(selected):
            result[0] = selected
            event.set()
        SchemaSelectorDialog(parent_widget, schemas, on_confirm=_cb)

    parent_after(0, _show)
    event.wait()
    return result[0]
