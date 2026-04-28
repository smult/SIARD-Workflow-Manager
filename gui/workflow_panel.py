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


_STATUS_CFG = {
    "completed": ("✓", "#3a9a5c"),
    "failed":    ("✗", COLORS["red"]),
    "skipped":   ("–", COLORS["muted"]),
    "pending":   ("",  COLORS["muted"]),
}


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
        self._color_bar = ctk.CTkFrame(self, width=4, corner_radius=2,
                                        fg_color=color, height=1)
        self._color_bar.grid(row=0, column=0, padx=(4, 0), pady=2, sticky="ns")

        # Info: indeks + navn + kategori på én linje
        self._info = ctk.CTkFrame(self, fg_color="transparent", height=1)
        self._info.grid(row=0, column=1, padx=(4, 2), pady=2, sticky="ew")
        self._info.grid_propagate(True)
        self._info.grid_columnconfigure(2, weight=1)

        self.idx_label = ctk.CTkLabel(
            self._info, text=f"{index}.",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"], width=18, anchor="e")
        self.idx_label.grid(row=0, column=0, sticky="w")

        ctk.CTkLabel(
            self._info, text=op.label,
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            text_color=COLORS["text"], anchor="w").grid(
                row=0, column=1, sticky="w", padx=(4, 6))

        ctk.CTkLabel(
            self._info, text=op.category,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=color, anchor="w").grid(
                row=0, column=2, sticky="w")

        # Knapper horisontalt — minimale
        btns = ctk.CTkFrame(self, fg_color="transparent", height=1)
        btns.grid(row=0, column=2, padx=(0, 4), pady=2)
        btns.grid_propagate(True)

        # Statusikon (prosjektfil-checkpoint): ✓ / ✗ / –
        self._status_lbl = ctk.CTkLabel(
            btns, text="", width=16,
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            text_color=COLORS["muted"])
        self._status_lbl.pack(side="left", padx=(0, 4))

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

    def set_status(self, status: str) -> None:
        """Vis checkpoint-status ved siden av knappene (✓ / ✗ / –)."""
        sym, color = _STATUS_CFG.get(status, ("", COLORS["muted"]))
        self._status_lbl.configure(text=sym, text_color=color)


class WorkflowPanel(ctk.CTkFrame):
    def __init__(self, parent, on_run, on_clear, on_save_profile,
                 on_settings_saved=None,
                 on_open_project:  Callable | None = None,
                 on_save_project:  Callable | None = None,
                 on_reset_project: Callable | None = None):
        super().__init__(parent, fg_color="transparent")
        self._on_run            = on_run
        self._on_clear          = on_clear
        self._on_save_profile   = on_save_profile
        self._on_settings_saved = on_settings_saved
        self._on_open_project   = on_open_project
        self._on_save_project   = on_save_project
        self._on_reset_project  = on_reset_project
        self._rows: list[OperationRow] = []
        # Drag-reorder tilstand
        self._drag_row: OperationRow | None = None
        self._drag_start_y: int = 0
        self._drag_active: bool = False
        self._drag_target: OperationRow | None = None
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

        run_row = ctk.CTkFrame(bottom, fg_color="transparent")
        run_row.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        run_row.grid_columnconfigure(0, weight=1)

        self.run_btn = ctk.CTkButton(
            run_row, text="Kjør workflow",
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
            font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
            height=38, command=self._on_run)
        self.run_btn.grid(row=0, column=0, sticky="ew")

        self._reset_proj_btn = ctk.CTkButton(
            run_row, text="↻", width=38, height=38, corner_radius=6,
            fg_color=COLORS["btn"], hover_color="#3d2a0e",
            text_color="#e0a040",
            font=ctk.CTkFont(size=18),
            command=lambda: self._on_reset_project and self._on_reset_project())
        self._reset_proj_btn.grid(row=0, column=1, padx=(6, 0))

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

        proj = ctk.CTkFrame(bottom, fg_color="transparent")
        proj.grid(row=2, column=0, sticky="ew", pady=(4, 0))
        proj.grid_columnconfigure(0, weight=1)
        proj.grid_columnconfigure(1, weight=1)
        btn_proj_cfg = dict(height=28, fg_color=COLORS["btn"],
                            hover_color=COLORS["btn_hover"],
                            font=ctk.CTkFont(family=FONTS["mono"], size=10))
        ctk.CTkButton(proj, text="📂 Åpne prosjekt", **btn_proj_cfg,
                      command=lambda: self._on_open_project and self._on_open_project()
                      ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        self._save_proj_btn = ctk.CTkButton(
            proj, text="💾 Lagre prosjekt", **btn_proj_cfg,
            command=lambda: self._on_save_project and self._on_save_project())
        self._save_proj_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))

        # Prosjektfil-etikett
        self._proj_lbl = ctk.CTkLabel(
            bottom, text="",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"], anchor="w")
        self._proj_lbl.grid(row=3, column=0, sticky="ew", pady=(2, 0))

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
        self._bind_drag(row)

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

        if op_def.get("custom_dialog") == "DiasParamDialog":
            from gui.dias_dialog import DiasParamDialog
            DiasParamDialog(self, live_def, on_confirm=_on_confirm, on_saved=_on_saved)
        else:
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

    def set_step_status(self, idx: int, status: str) -> None:
        """Sett checkpoint-statusikon (✓/✗/–) for steg idx."""
        if 0 <= idx < len(self._rows):
            self._rows[idx].set_status(status)

    def clear_statuses(self) -> None:
        """Fjern alle checkpoint-statusikoner."""
        for row in self._rows:
            row.set_status("pending")

    def set_project_label(self, text: str) -> None:
        """Vis prosjektfilnavn under prosjektknappene."""
        self._proj_lbl.configure(text=text)

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

    # ─── Drag-and-drop reordering ────────────────────────────────────────────

    def _bind_drag(self, row: OperationRow):
        """Bind drag-reorder-events til rad og dens draggbare deler (ikke knapper)."""
        targets = [row, row._color_bar, row._info]
        for child in row._info.winfo_children():
            targets.append(child)
        for w in targets:
            w.bind("<ButtonPress-1>",   lambda e, r=row: self._drag_press(e, r),   add="+")
            w.bind("<B1-Motion>",       lambda e, r=row: self._drag_motion(e, r),  add="+")
            w.bind("<ButtonRelease-1>", lambda e, r=row: self._drag_release(e, r), add="+")

    def _drag_press(self, event, row: OperationRow):
        self._drag_row = row
        self._drag_start_y = event.y_root
        self._drag_active = False

    def _drag_motion(self, event, row: OperationRow):
        if self._drag_row is not row:
            return
        if not self._drag_active:
            if abs(event.y_root - self._drag_start_y) < 6:
                return
            self._drag_active = True
            row.configure(border_color=COLORS["accent"], border_width=2)
            self.scroll.configure(cursor="hand2")
        target = self._find_drop_target(event.y_root)
        if target is row:
            target = None
        if target is not self._drag_target:
            if self._drag_target is not None:
                self._drag_target.configure(border_color=COLORS["border"], border_width=1)
            self._drag_target = target
            if target is not None:
                target.configure(border_color=COLORS["green"], border_width=2)

    def _drag_release(self, event, row: OperationRow):
        if self._drag_row is None:
            return
        if self._drag_active and self._drag_target is not None:
            i_src = self._rows.index(row)
            i_tgt = self._rows.index(self._drag_target)
            self._rows.pop(i_src)
            self._rows.insert(i_tgt, row)
            self._reindex()
        row.configure(border_color=COLORS["border"], border_width=1)
        if self._drag_target is not None and self._drag_target in self._rows:
            self._drag_target.configure(border_color=COLORS["border"], border_width=1)
        self.scroll.configure(cursor="")
        self._drag_row = None
        self._drag_active = False
        self._drag_target = None

    def _find_drop_target(self, y_root: int) -> OperationRow | None:
        """Finn hvilken rad musemarkøren er over."""
        for row in self._rows:
            try:
                ry = row.winfo_rooty()
                rh = row.winfo_height()
                if ry <= y_root <= ry + rh:
                    return row
            except Exception:
                pass
        return None

    def _prompt_save_profile(self):
        dialog = ctk.CTkInputDialog(text="Gi profilen et navn:", title="Lagre profil")
        name = dialog.get_input()
        if name:
            self._on_save_profile(name.strip())
