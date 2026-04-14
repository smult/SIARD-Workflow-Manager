"""
gui/workflow_panel.py
Venstre panel: viser den aktive workflow-koen med
rekkefølge-kontroller, kjør-knapp og profilhåndtering.
"""
from __future__ import annotations
from pathlib import Path
from typing import Callable
import tkinter as tk
import customtkinter as ctk
from gui.styles import COLORS, FONTS, cat_color


class OperationRow(ctk.CTkFrame):

    def __init__(self, parent, op, index: int,
                 on_remove: Callable, on_move_up: Callable, on_move_down: Callable,
                 on_configure: Callable | None = None):
        super().__init__(parent,
                         fg_color=COLORS["panel"],
                         corner_radius=6,
                         border_color=COLORS["border"],
                         border_width=1,
                         height=1)         # nullstiller CTkFrame sin default 200px
        self.grid_propagate(True)          # la innhold styre høyden
        self.op = op
        self._on_remove    = on_remove
        self._on_move_up   = on_move_up
        self._on_move_down = on_move_down
        self._on_configure = on_configure
        self.grid_columnconfigure(1, weight=1)
        color = cat_color(op.category)

        # Fargebar til venstre
        ctk.CTkFrame(self, width=4, corner_radius=2,
                     fg_color=color, height=1).grid(
            row=0, column=0, padx=(4, 0), pady=2, sticky="ns")

        # Info: indeks + navn + kategori på én linje
        info = ctk.CTkFrame(self, fg_color="transparent", height=1)
        info.grid(row=0, column=1, padx=(4, 2), pady=2, sticky="ew")
        info.grid_propagate(True)
        info.grid_columnconfigure(2, weight=1)

        self.idx_label = ctk.CTkLabel(
            info, text=f"{index}.",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"], width=18, anchor="e")
        self.idx_label.grid(row=0, column=0, sticky="w")

        ctk.CTkLabel(
            info, text=op.label,
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            text_color=COLORS["text"], anchor="w").grid(
                row=0, column=1, sticky="w", padx=(4, 6))

        ctk.CTkLabel(
            info, text=op.category,
            font=ctk.CTkFont(family=FONTS["mono"], size=9),
            text_color=color, anchor="w").grid(
                row=0, column=2, sticky="w")

        # Knapper horisontalt — minimale
        btns = ctk.CTkFrame(self, fg_color="transparent", height=1)
        btns.grid(row=0, column=2, padx=(0, 4), pady=2)
        btns.grid_propagate(True)
        btn_cfg = dict(width=20, height=20, corner_radius=4,
                       fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                       font=ctk.CTkFont(size=10))
        ctk.CTkButton(btns, text="▲", **btn_cfg,
                      command=lambda: on_move_up(self)).pack(side="left", padx=(0, 2))
        ctk.CTkButton(btns, text="▼", **btn_cfg,
                      command=lambda: on_move_down(self)).pack(side="left", padx=(0, 2))
        ctk.CTkButton(btns, text="✕", width=20, height=20, corner_radius=4,
                      fg_color="#2a1515", hover_color="#3d2020",
                      text_color=COLORS["red"], font=ctk.CTkFont(size=10),
                      command=lambda: on_remove(self)).pack(side="left")

        # Høyreklikk-meny
        self._bind_right_click(self)
        for child in self.winfo_children():
            self._bind_right_click(child)
            for grandchild in child.winfo_children():
                self._bind_right_click(grandchild)

        # Tving riktig høyde etter at alle barn er lagt til.
        # CTkFrame resetter canvas til _desired_height ved _draw() —
        # vi overskriver ved å sette height=1 sist, slik at grid_propagate
        # kan ekspandere til faktisk innholdshøyde.
        self.after(0, lambda: self.configure(height=1))

    def _bind_right_click(self, widget):
        widget.bind("<Button-3>", self._show_context_menu, add="+")

    def _show_context_menu(self, event):
        menu = tk.Menu(self, tearoff=0,
                       bg=COLORS["panel"], fg=COLORS["text"],
                       activebackground=COLORS["accent"],
                       activeforeground=COLORS["bg"],
                       font=("Consolas", 10),
                       borderwidth=0, relief="flat")
        if self._on_configure:
            menu.add_command(label="⚙  Konfigurer operasjon",
                             command=lambda: self._on_configure(self))
        menu.add_separator()
        menu.add_command(label="▲  Flytt opp",
                         command=lambda: self._on_move_up(self))
        menu.add_command(label="▼  Flytt ned",
                         command=lambda: self._on_move_down(self))
        menu.add_separator()
        menu.add_command(label="✕  Fjern fra workflow",
                         command=lambda: self._on_remove(self),
                         foreground=COLORS["red"],
                         activeforeground=COLORS["red"])
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def set_active(self, active: bool):
        self.configure(border_color=COLORS["accent"] if active else COLORS["border"],
                       border_width=2 if active else 1)

    def set_index(self, i: int):
        self.idx_label.configure(text=f"{i}.")


class WorkflowPanel(ctk.CTkFrame):
    def __init__(self, parent, on_run, on_clear, on_save_profile,
                 on_settings_saved=None):
        super().__init__(parent, fg_color="transparent")
        self._on_run           = on_run
        self._on_clear         = on_clear
        self._on_save_profile  = on_save_profile
        self._on_settings_saved = on_settings_saved
        self._rows: list[OperationRow] = []
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        hdr.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(hdr, text="WORKFLOW",
                     font=ctk.CTkFont(family=FONTS["mono"], size=10, weight="bold"),
                     text_color=COLORS["muted"]).grid(row=0, column=0, sticky="w")
        self.file_lbl = ctk.CTkLabel(hdr, text="Ingen fil",
                                      font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                      text_color=COLORS["muted"])
        self.file_lbl.grid(row=1, column=0, sticky="w")

        self.scroll = ctk.CTkScrollableFrame(self, fg_color=COLORS["bg"],
                                              corner_radius=8,
                                              scrollbar_button_color=COLORS["border"])
        self.scroll.grid(row=1, column=0, sticky="nsew", pady=(0, 8))
        self.scroll.grid_columnconfigure(0, weight=1)

        self.empty_lbl = ctk.CTkLabel(self.scroll,
                                       text="Legg til operasjoner\nfra paletten til høyre",
                                       font=ctk.CTkFont(family=FONTS["mono"], size=11),
                                       text_color=COLORS["muted"])
        self.empty_lbl.grid(row=0, column=0, pady=40)

        bottom = ctk.CTkFrame(self, fg_color="transparent")
        bottom.grid(row=2, column=0, sticky="ew")
        bottom.grid_columnconfigure(0, weight=1)

        self.run_btn = ctk.CTkButton(
            bottom, text="Kjør workflow",
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
            font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
            height=38, command=self._on_run)
        self.run_btn.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        sub = ctk.CTkFrame(bottom, fg_color="transparent")
        sub.grid(row=1, column=0, sticky="ew")
        sub.grid_columnconfigure(0, weight=1)
        sub.grid_columnconfigure(1, weight=1)
        ctk.CTkButton(sub, text="Tøm", height=30,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self._on_clear).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ctk.CTkButton(sub, text="Lagre profil...", height=30,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self._prompt_save_profile).grid(row=0, column=1, sticky="ew", padx=(4, 0))

    def set_file(self, path: Path):
        short = path.name if len(path.name) < 35 else "..." + path.name[-32:]
        self.file_lbl.configure(text=f"{short}", text_color=COLORS["green"])

    def add_operation(self, op):
        self.empty_lbl.grid_remove()
        idx = len(self._rows) + 1
        row = OperationRow(self.scroll, op, idx,
                           on_remove=self._remove_row,
                           on_move_up=self._move_up,
                           on_move_down=self._move_down,
                           on_configure=self._configure_row)
        row.grid(row=len(self._rows), column=0, sticky="ew", pady=3, padx=2)
        self._rows.append(row)

    def _configure_row(self, row: OperationRow):
        """Åpne konfigurasjonsvindu for en eksisterende operasjon."""
        # Lazy import for å unngå sirkulær avhengighet
        from gui.operations_panel import OP_DEFS, ParamDialog
        op = row.op

        # Finn op_def for denne operasjonsklassen
        op_def = next((d for d in OP_DEFS if d["cls"] is type(op)), None)
        if op_def is None or not op_def.get("params"):
            return   # ingen parametere å konfigurere

        # Lag en kopi av op_def med gjeldende verdier som default (pre-fylt)
        current_params = op_def["params"]
        prefilled = []
        for p in current_params:
            current_val = op.params.get(p["key"], p["default"])
            prefilled.append({**p, "default": current_val})

        live_def = {**op_def, "params": prefilled}

        def _on_confirm(new_op):
            row.op = new_op
            row.configure(border_color=COLORS["accent"])
            row.after(1000, lambda: row.configure(border_color=COLORS["border"]))

        def _on_saved(op_id, params, settings_path, error=None):
            if self._on_settings_saved:
                self._on_settings_saved(op_id, params, settings_path, error)

        ParamDialog(self, live_def, on_confirm=_on_confirm, on_saved=_on_saved)

    def load_workflow(self, wf):
        self.clear()
        for op in wf:
            self.add_operation(op)

    def get_operations(self):
        return [r.op for r in self._rows]

    def clear(self):
        for row in self._rows:
            row.destroy()
        self._rows.clear()
        self.empty_lbl.grid(row=0, column=0, pady=40)

    def set_running(self, running: bool):
        self.run_btn.configure(
            state="disabled" if running else "normal",
            text="Kjører..." if running else "Kjør workflow")

    def highlight_step(self, idx: int):
        for i, row in enumerate(self._rows):
            row.set_active(i == idx)

    def _reindex(self):
        for i, row in enumerate(self._rows):
            row.set_index(i + 1)
            row.grid(row=i, column=0, sticky="ew", pady=3, padx=2)

    def _remove_row(self, row: OperationRow):
        self._rows.remove(row)
        row.destroy()
        self._reindex()
        if not self._rows:
            self.empty_lbl.grid(row=0, column=0, pady=40)

    def _move_up(self, row: OperationRow):
        i = self._rows.index(row)
        if i == 0:
            return
        self._rows[i], self._rows[i - 1] = self._rows[i - 1], self._rows[i]
        self._reindex()

    def _move_down(self, row: OperationRow):
        i = self._rows.index(row)
        if i >= len(self._rows) - 1:
            return
        self._rows[i], self._rows[i + 1] = self._rows[i + 1], self._rows[i]
        self._reindex()

    def _prompt_save_profile(self):
        dialog = ctk.CTkInputDialog(text="Gi profilen et navn:", title="Lagre profil")
        name = dialog.get_input()
        if name:
            self._on_save_profile(name.strip())
