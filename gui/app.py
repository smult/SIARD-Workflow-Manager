"""
gui/app.py  —  SIARD Workflow Manager hovedvindu
"""
from __future__ import annotations
import sys
import threading
import queue
import datetime
from pathlib import Path

import customtkinter as ctk

from gui.workflow_panel import WorkflowPanel
from gui.operations_panel import OperationsPanel, set_current_siard_path
from gui.log_panel import LogPanel
from gui.styles import COLORS, FONTS
from gui.settings_dialog import SettingsDialog

sys.path.insert(0, str(Path(__file__).parent.parent))
from siard_workflow import create_manager
from siard_workflow.core.workflow import Workflow
from siard_workflow.core.report import save_html, save_pdf
from siard_workflow.core.workflow_io import workflow_to_json, workflow_from_json
from siard_workflow.core.file_logger import WorkflowFileLogger
from gui.progress_panel import ProgressPanel
from gui.format_chart_panel import FormatChartPanel
from settings import get_config, set_config

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")


class _PipelineSuggestionDialog(ctk.CTkToplevel):
    """
    Dialog som foreslår å legge til 'Pakk ut SIARD' og 'Pakk sammen SIARD'
    når workflow inneholder operasjoner som drar nytte av pipeline-modus.
    result: "ja" | "nei" | "avbryt"
    """

    def __init__(self, parent, message: str):
        super().__init__(parent)
        self.result = "avbryt"
        self.title("Pipeline-forslag")
        self.resizable(False, False)
        self.grab_set()
        self.lift()

        # Sentrer over foreldrevinduet
        self.update_idletasks()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        px, py = parent.winfo_x(), parent.winfo_y()
        w, h = 520, 300
        x = px + (pw - w) // 2
        y = py + (ph - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")
        self.configure(fg_color=COLORS["bg"])

        # Ikon + tittel
        header = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        header.pack(fill="x", padx=0, pady=0)
        ctk.CTkLabel(
            header,
            text="  Pipeline-modus anbefalt",
            font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
            text_color=COLORS["accent"],
            anchor="w",
        ).pack(side="left", padx=12, pady=10)

        # Meldingstekst
        ctk.CTkLabel(
            self,
            text=message,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["text"],
            wraplength=480,
            justify="left",
            anchor="nw",
        ).pack(padx=16, pady=(14, 8), fill="x")

        # Knapper
        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(side="bottom", fill="x", padx=16, pady=14)
        btn_row.grid_columnconfigure((0, 1, 2), weight=1)

        ctk.CTkButton(
            btn_row, text="Ja, legg til automatisk",
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            height=34,
            command=self._on_ja,
        ).grid(row=0, column=0, padx=(0, 6), sticky="ew")

        ctk.CTkButton(
            btn_row, text="Nei, kjør uten",
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            height=34,
            command=self._on_nei,
        ).grid(row=0, column=1, padx=3, sticky="ew")

        ctk.CTkButton(
            btn_row, text="Avbryt",
            fg_color="#2a1515", hover_color="#3d2020",
            text_color=COLORS["red"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            height=34,
            command=self._on_avbryt,
        ).grid(row=0, column=2, padx=(6, 0), sticky="ew")

        self.protocol("WM_DELETE_WINDOW", self._on_avbryt)

    def _on_ja(self):
        self.result = "ja"
        self.destroy()

    def _on_nei(self):
        self.result = "nei"
        self.destroy()

    def _on_avbryt(self):
        self.result = "avbryt"
        self.destroy()


class App(ctk.CTk):
    TITLE   = "SIARD Workflow Manager"
    try:
        from version import VERSION
    except ImportError:
        VERSION = "?"
    MIN_W, MIN_H = 1100, 720

    def __init__(self):
        super().__init__()
        self.title(f"{self.TITLE}  v{self.VERSION}")
        self.geometry("1200x780")
        self.minsize(self.MIN_W, self.MIN_H)
        self.after(0, lambda: self.state("zoomed"))
        self.configure(fg_color=COLORS["bg"])

        self.manager         = create_manager()
        self.siard_path: Path | None = None        # aktiv fil (siste i køen under kjøring)
        self.siard_queue: list[Path] = []          # kø av SIARD-filer
        self.workflow: Workflow | None = None
        self._log_queue: queue.Queue = queue.Queue()
        self._running        = False
        self._log_entries: list[tuple[str,str]] = []
        self._current_run: "WorkflowRun | None" = None
        self._auto_log_dir: Path | None = None
        self._global_temp_dir: Path | None = None
        self._output_dir_override: str = ""   # satt av preflight hvis output-disk har lite plass
        self._conv_ctx = None
        self._stop_event  = threading.Event()
        self._pause_event = threading.Event()
        self._spinner_chars = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
        self._spinner_idx   = 0

        self._build_ui()
        self._load_persistent_profiles()
        self._load_saved_temp()
        self._detect_libreoffice()
        self._init_worker_config()
        self._poll_log_queue()

        # Sjekk for oppdateringer i bakgrunnen (2 sek forsinkelse for at GUI skal være klar)
        self.after(2000, self._check_for_updates)

    # ─── UI ──────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Rad 0: topbar, Rad 1: innhold, Rad 2: statusbar
        self.grid_columnconfigure(0, weight=0)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=0)
        self._build_topbar()
        self._build_statusbar()

        left = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=10, width=380)
        left.grid(row=1, column=0, padx=(12,6), pady=(0,0), sticky="nsew")
        left.grid_rowconfigure(0, weight=0)   # kø-panel
        left.grid_rowconfigure(1, weight=1)   # workflow
        left.grid_rowconfigure(2, weight=0)   # format-chart
        left.grid_columnconfigure(0, weight=1)
        left.grid_propagate(False)

        self._build_queue_panel(left)

        self.workflow_panel = WorkflowPanel(
            left,
            on_run=self._run_workflow,
            on_clear=self._clear_workflow,
            on_save_profile=self._save_profile,
            on_settings_saved=self._on_settings_saved,
        )
        self.workflow_panel.grid(row=1, column=0, padx=10, pady=(0,6), sticky="nsew")

        self.format_chart = FormatChartPanel(left)
        self.format_chart.grid(row=2, column=0, padx=10, pady=(0, 10), sticky="ew")

        right = ctk.CTkFrame(self, fg_color="transparent")
        right.grid(row=1, column=1, padx=(6,12), pady=(0,0), sticky="nsew")
        right.grid_rowconfigure(0, weight=0)
        right.grid_rowconfigure(1, weight=0)
        right.grid_rowconfigure(2, weight=1)
        right.grid_columnconfigure(0, weight=1)

        self.ops_panel = OperationsPanel(right, on_add=self._add_operation,
                                          on_saved=self._on_settings_saved)
        self.ops_panel.grid(row=0, column=0, pady=(0,8), sticky="ew")

        self.progress_panel = ProgressPanel(right)
        self.progress_panel.grid(row=1, column=0, pady=(0,8), sticky="ew")
        self.progress_panel.grid_remove()   # skjult ved oppstart; grid-info er lagret
        self.progress_panel.set_callbacks(
            pause_cb=self._conv_pause,
            stop_cb=self._conv_stop)

        self.log_panel = LogPanel(right)
        self.log_panel.grid(row=2, column=0, sticky="nsew")

    def _build_topbar(self):
        bar = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=0, height=48)
        bar.grid(row=0, column=0, columnspan=2, sticky="ew")
        bar.grid_columnconfigure(1, weight=1)
        bar.grid_propagate(False)

        ctk.CTkLabel(bar, text="SIARD WORKFLOW MANAGER",
                     font=ctk.CTkFont(family=FONTS["mono"], size=15, weight="bold"),
                     text_color=COLORS["accent"]).grid(
                         row=0, column=0, padx=20, pady=12, sticky="w")

        # Profil-meny
        self.profile_var = ctk.StringVar(value="-- profil --")
        self.profile_menu = ctk.CTkOptionMenu(
            bar, values=["-- profil --"] + self.manager.list_profiles(),
            variable=self.profile_var, width=160,
            fg_color=COLORS["btn"], button_color=COLORS["accent"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            command=self._load_profile)
        self.profile_menu.grid(row=0, column=1, padx=4, pady=8, sticky="w")

        menu_cfg = dict(height=30, width=100, fg_color=COLORS["btn"],
                        hover_color=COLORS["btn_hover"],
                        font=ctk.CTkFont(family=FONTS["mono"], size=10))

        col = 2
        for label, cmd in [
            ("Endre temp",   self._browse_temp),
            ("Rapport",      self._export_report),
            ("Logg til fil", self._toggle_auto_log),
            ("Innstillinger",self._open_settings),
        ]:
            ctk.CTkButton(bar, text=label, command=cmd, **menu_cfg).grid(
                row=0, column=col, padx=4, pady=8)
            col += 1

    def _build_statusbar(self):
        """Statuslinje nederst i vinduet — temp-info + spinner."""
        sb = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=0, height=26)
        sb.grid(row=2, column=0, columnspan=2, sticky="ew")
        sb.grid_columnconfigure(1, weight=1)
        sb.grid_propagate(False)

        self._status_spinner = ctk.CTkLabel(
            sb, text=" ", width=24,
            font=ctk.CTkFont(family=FONTS["mono"], size=12),
            text_color=COLORS["accent"])
        self._status_spinner.grid(row=0, column=0, padx=(8,0), pady=2)

        self.temp_label = ctk.CTkLabel(
            sb, text="Temp: (auto)",
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["muted"], anchor="w")
        self.temp_label.grid(row=0, column=1, padx=6, pady=2, sticky="w")

        self._status_right = ctk.CTkLabel(
            sb, text="",
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["muted"], anchor="e")
        self._status_right.grid(row=0, column=2, padx=(0,12), pady=2, sticky="e")

    def _build_queue_panel(self, parent):
        """Drag-drop + kø-liste over SIARD-filer øverst i venstre kolonne."""
        frm = ctk.CTkFrame(parent, fg_color=COLORS["bg"], corner_radius=8)
        frm.grid(row=0, column=0, padx=10, pady=(10, 4), sticky="ew")
        frm.grid_columnconfigure(0, weight=1)

        # Tittel
        ctk.CTkLabel(frm, text="SIARD-KØ",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                     text_color=COLORS["muted"]).grid(
                         row=0, column=0, sticky="w", padx=10, pady=(6, 2))

        # Drag-drop-felt
        self._drop_zone = ctk.CTkLabel(
            frm,
            text="Dra og slipp SIARD-filer hit",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"],
            fg_color=COLORS["panel"],
            corner_radius=6,
            height=44)
        self._drop_zone.grid(row=1, column=0, padx=8, pady=(2, 4), sticky="ew")

        # Prøv tkinterdnd2 først, deretter Windows shell DnD, deretter klikk-fallback
        self._dnd_active = False
        self._setup_dnd()

        # Kø-liste
        self._queue_frame = ctk.CTkScrollableFrame(
            frm, fg_color="transparent", height=100,
            scrollbar_button_color=COLORS["border"])
        self._queue_frame.grid(row=2, column=0, padx=4, pady=(0, 6), sticky="ew")
        self._queue_frame.grid_columnconfigure(0, weight=1)

        self._queue_rows: list[ctk.CTkFrame] = []
        self._queue_lbl_none = ctk.CTkLabel(
            self._queue_frame, text="(tom)",
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["muted"])
        self._queue_lbl_none.grid(row=0, column=0, pady=4)

    def _setup_dnd(self):
        """Sett opp drag-and-drop. Prøver tkinterdnd2, deretter klikk-fallback."""
        # Metode 1: tkinterdnd2
        try:
            self._drop_zone.drop_target_register("DND_Files")   # type: ignore
            self._drop_zone.dnd_bind("<<Drop>>", self._on_dnd_drop)  # type: ignore
            self._drop_zone.dnd_bind("<<DragEnter>>", self._on_dnd_enter)  # type: ignore
            self._drop_zone.dnd_bind("<<DragLeave>>", self._on_dnd_leave)  # type: ignore
            self._dnd_active = True
            return
        except Exception:
            pass

        # Metode 2: Windows shell DnD via tkinter intern-API
        try:
            import tkinter as tk
            inner = self._drop_zone._canvas if hasattr(self._drop_zone, '_canvas') \
                    else self._drop_zone
            inner.drop_target_register("DND_Files")   # type: ignore
            inner.dnd_bind("<<Drop>>", self._on_dnd_drop)  # type: ignore
            self._dnd_active = True
            return
        except Exception:
            pass

        # Fallback: klikk åpner filvelger
        self._drop_zone.configure(text="Klikk for å legge til SIARD-filer")
        self._drop_zone.bind("<Button-1>", lambda e: self._queue_add_file())
        self._drop_zone.bind("<Enter>",
            lambda e: self._drop_zone.configure(text_color=COLORS["accent"]))
        self._drop_zone.bind("<Leave>",
            lambda e: self._drop_zone.configure(text_color=COLORS["muted"]))

    def _on_dnd_enter(self, event):
        self._drop_zone.configure(
            fg_color=COLORS["accent_dim"] if "accent_dim" in COLORS else COLORS["btn"],
            text_color=COLORS["accent"])

    def _on_dnd_leave(self, event):
        self._drop_zone.configure(
            fg_color=COLORS["panel"],
            text_color=COLORS["muted"])

    def _browse_temp(self):
        """Manuell override av global temp-mappe."""
        from tkinter import filedialog
        start = str(self._global_temp_dir) if self._global_temp_dir else "."
        d = filedialog.askdirectory(title="Velg temp-mappe", initialdir=start)
        if d:
            self._global_temp_dir = Path(d)
            self._update_temp_label()
            self._log(f"Temp-mappe satt manuelt: {d}", "info")
            try:
                from settings import set_config
                set_config("global_temp_dir", d)
            except Exception:
                pass

    def _auto_select_temp(self, siard_path: Path):
        """Velg beste temp-disk automatisk basert på disktype."""
        try:
            from disk_selector import best_temp_disk, get_disk_candidates
            self._global_temp_dir = best_temp_disk(siard_path=siard_path)
            candidates = get_disk_candidates()
            self._log(f"Temp-mappe (auto): {self._global_temp_dir}", "info")
            for c in candidates:
                marker = "→" if c["path"] == self._global_temp_dir else " "
                self._log(f"  {marker} {c['label']}", "muted")
        except Exception:
            self._global_temp_dir = siard_path.parent
            self._log(f"Temp-mappe (fallback): {self._global_temp_dir}", "info")
        # Lagre til config.json
        try:
            from settings import set_config
            set_config("global_temp_dir", str(self._global_temp_dir))
        except Exception:
            pass
        self._update_temp_label()

    def _update_temp_label(self):
        if self._global_temp_dir:
            p = str(self._global_temp_dir)
            display = p if len(p) <= 55 else "…" + p[-55:]
            self.temp_label.configure(
                text=f"Temp: {display}", text_color=COLORS["text"])
        else:
            self.temp_label.configure(
                text="Temp: (auto)", text_color=COLORS["muted"])

    def _update_status_right(self, text: str):
        self._status_right.configure(text=text)

    def _tick_spinner(self):
        """Animert spinner i statusbar mens kjøring pågår."""
        if self._running:
            self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_chars)
            self._status_spinner.configure(
                text=self._spinner_chars[self._spinner_idx])
            self.after(100, self._tick_spinner)
        else:
            self._status_spinner.configure(text=" ")

    # ─── Fil ─────────────────────────────────────────────────────────────────

    # ─── SIARD-kø ─────────────────────────────────────────────────────────────

    def _queue_add_file(self):
        from tkinter import filedialog
        paths = filedialog.askopenfilenames(
            title="Legg til SIARD-fil(er) i kø",
            filetypes=[("SIARD", "*.siard"), ("ZIP", "*.zip"), ("Alle", "*.*")])
        for p in paths:
            self._queue_push(Path(p))

    def _on_dnd_drop(self, event):
        """Håndter drag-and-drop fra tkinterdnd2."""
        self._on_dnd_leave(event)  # nullstill farge
        raw = event.data
        import re
        # tkinterdnd2: {sti med mellomrom} eller sti\nsti
        paths = re.findall(r'\{([^}]+)\}|(\S+)', raw)
        for grp in paths:
            p = (grp[0] or grp[1]).strip()
            if p:
                path = Path(p)
                if path.suffix.lower() in (".siard", ".zip") and path.exists():
                    self._queue_push(path)
                elif path.exists():
                    # Aksepter uansett filendelse hvis filen finnes
                    self._queue_push(path)

    def _queue_push(self, path: Path):
        """Legg til én SIARD-fil i køen."""
        if path in self.siard_queue:
            self._log(f"Allerede i kø: {path.name}", "muted")
            return
        self.siard_queue.append(path)
        self._queue_render()
        self._log(f"Lagt til i kø: {path.name}", "info")
        # Sett som aktiv fil (siste valgte)
        self._set_active_file(path)

    def _queue_remove(self, path: Path):
        """Fjern én SIARD-fil fra køen."""
        if path in self.siard_queue:
            self.siard_queue.remove(path)
        self._queue_render()
        if self.siard_queue:
            self._set_active_file(self.siard_queue[-1])
        else:
            self.siard_path = None
            self._update_status_right("")

    def _queue_render(self):
        """Tegn kø-listen på nytt."""
        for row in self._queue_rows:
            row.destroy()
        self._queue_rows.clear()

        if not self.siard_queue:
            self._queue_lbl_none.grid(row=0, column=0, pady=4)
            return

        self._queue_lbl_none.grid_remove()

        for i, path in enumerate(self.siard_queue):
            row = ctk.CTkFrame(self._queue_frame, fg_color=COLORS["panel"],
                               corner_radius=4, height=1)
            row.grid(row=i, column=0, sticky="ew", pady=2, padx=2)
            row.grid_propagate(True)
            row.grid_columnconfigure(0, weight=1)

            lbl = ctk.CTkLabel(row,
                               text=path.name,
                               font=ctk.CTkFont(family=FONTS["mono"], size=10),
                               text_color=COLORS["text"], anchor="w")
            lbl.grid(row=0, column=0, padx=(6,2), pady=3, sticky="w")

            # Tooltip: full path ved hover
            def _enter(e, p=path, l=lbl):
                l.configure(text=str(p), text_color=COLORS["muted"])
            def _leave(e, p=path, l=lbl):
                l.configure(text=p.name, text_color=COLORS["text"])
            lbl.bind("<Enter>", _enter)
            lbl.bind("<Leave>", _leave)

            # Fjern-knapp
            def _rm(p=path):
                self._queue_remove(p)
            ctk.CTkButton(row, text="✕", width=20, height=20, corner_radius=3,
                          fg_color="#2a1515", hover_color="#3d2020",
                          text_color=COLORS["red"],
                          font=ctk.CTkFont(size=11),
                          command=_rm).grid(row=0, column=1, padx=(0,4), pady=3)

            self._queue_rows.append(row)

        self._update_status_right(
            f"Kø: {len(self.siard_queue)} fil(er)")

    def _set_active_file(self, path: Path):
        """Sett aktiv SIARD-fil (brukes av workflow og logging)."""
        self.siard_path = path
        self._auto_log_dir = path.parent
        set_current_siard_path(path)
        if self._global_temp_dir is None:
            self._auto_select_temp(path)
        self.workflow_panel.set_file(path)

    # ─── Gammel _browse_file beholdt for bakoverkompatibilitet ────────────────
    def _browse_file(self):
        self._queue_add_file()

    # ─── Profiler ─────────────────────────────────────────────────────────────

    def _load_profile(self, name: str):
        if name.startswith("--"):
            return
        # Bruk første fil i køen som anker, eller dummy-sti
        anchor = self.siard_path or (self.siard_queue[0] if self.siard_queue else None)
        if not anchor:
            self._log("Legg til minst én SIARD-fil i køen først", "warn")
            return
        wf = self.manager.create_workflow(anchor, profile=name)
        self.workflow = wf
        self.workflow_panel.load_workflow(wf)
        self._log(f"Profil '{name}' lastet  ({len(wf)} operasjoner)", "info")

    def _check_for_updates(self):
        """Sjekk GitHub for ny versjon i bakgrunnstråd."""
        try:
            from gui.update_checker import check_for_updates
            check_for_updates(self, self.VERSION)
        except Exception:
            pass

    def _load_saved_temp(self):
        """Last inn lagret global temp-mappe fra config.json ved oppstart."""
        try:
            saved = get_config("global_temp_dir", "")
            if saved and Path(saved).is_dir():
                self._global_temp_dir = Path(saved)
                self._update_temp_label()
        except Exception:
            pass

    def _detect_libreoffice(self):
        """Auto-detekter LibreOffice ved oppstart og lagre sti til config.json."""
        try:
            saved = get_config("lo_executable", "")
            if saved and Path(saved).is_file():
                return  # allerede funnet og lagret
            from siard_workflow.systemspecific_operations.cosdoc_operation import _find_libreoffice
            found = _find_libreoffice("")
            if found:
                set_config("lo_executable", found)
        except Exception:
            pass

    def _init_worker_config(self):
        """Sett max_workers og lo_batch_size fra maskinvare ved første kjøring (verdi == 0)."""
        try:
            if int(get_config("max_workers", 0) or 0) == 0:
                from siard_workflow.operations.blob_convert_operation import suggest_lo_defaults
                hw = suggest_lo_defaults()
                from settings import save_config
                save_config({
                    "max_workers":   hw["max_workers"],
                    "lo_batch_size": hw["lo_batch_size"],
                })
        except Exception:
            pass

    def _load_persistent_profiles(self):
        """Last lagrede profiler fra settings.json ved oppstart."""
        try:
            from settings import get_profiles
            from siard_workflow.core.workflow_io import ops_from_dict
            from siard_workflow.core.manager import BaseProfile
            from siard_workflow.core.workflow import Workflow
            for name, ops_data in get_profiles().items():
                try:
                    ops_snapshot = ops_from_dict(ops_data)
                    class _Dyn(BaseProfile):
                        _ops = ops_snapshot
                        @classmethod
                        def build(cls, workflow_name, stop_on_error=False):
                            wf = Workflow(name=workflow_name,
                                          stop_on_error=stop_on_error)
                            for op in cls._ops:
                                wf.add(op)
                            return wf
                    _Dyn._ops = ops_snapshot
                    self.manager.register_profile(name, _Dyn)
                except Exception:
                    pass
            self.profile_menu.configure(
                values=["-- profil --"] + self.manager.list_profiles())
        except Exception:
            pass

    def _save_profile(self, name: str):
        if not name.strip():
            return
        from siard_workflow.core.manager import BaseProfile
        from siard_workflow.core.workflow import Workflow
        ops_snapshot = list(self.workflow_panel.get_operations())

        class _Dyn(BaseProfile):
            _ops = ops_snapshot
            @classmethod
            def build(cls, workflow_name, stop_on_error=False):
                wf = Workflow(name=workflow_name, stop_on_error=stop_on_error)
                for op in cls._ops:
                    wf.add(op)
                return wf
        _Dyn._ops = ops_snapshot
        self.manager.register_profile(name, _Dyn)
        self.profile_menu.configure(
            values=["-- profil --"] + self.manager.list_profiles())

        # Lagre persistent til settings.json
        try:
            from settings import save_profile_ops
            from siard_workflow.core.workflow_io import ops_to_dict
            save_profile_ops(name, ops_to_dict(ops_snapshot))
        except Exception:
            pass
        self._log(f"Profil '{name}' lagret", "success")

    def _on_settings_saved(self, op_id: str, params: dict,
                            settings_path, error=None):
        """Kalles når en operasjons parametre er lagret til settings.json."""
        if error:
            self._log(f"  [FEIL] Innstillinger ikke lagret: {error}", "error")
        else:
            self._log(
                f"  Innstillinger lagret → {settings_path}", "success")
            changed = ", ".join(
                f"{k}={v}" for k, v in params.items()
                if not k.startswith("_"))
            self._log(f"    {op_id}: {changed}", "info")

    # ─── Workflow import/eksport ───────────────────────────────────────────────

    def _export_workflow(self):
        from tkinter import filedialog, messagebox
        ops = list(self.workflow_panel.get_operations())
        if not ops:
            messagebox.showinfo("Tom workflow", "Legg til operasjoner først.")
            return
        wf = Workflow(name=self.siard_path.stem if self.siard_path else "workflow")
        for op in ops:
            wf.add(op)
        idir = str(self.siard_path.parent) if self.siard_path else "."
        sp = filedialog.asksaveasfilename(
            title="Eksporter workflow",
            initialdir=idir,
            initialfile=(wf.name or "workflow") + ".json",
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("Alle", "*.*")])
        if not sp:
            return
        try:
            workflow_to_json(wf, Path(sp))
            self._log(f"Workflow eksportert: {sp}", "success")
        except Exception as e:
            messagebox.showerror("Feil", str(e))

    def _import_workflow(self):
        from tkinter import filedialog, messagebox
        sp = filedialog.askopenfilename(
            title="Importer workflow",
            filetypes=[("JSON", "*.json"), ("Alle", "*.*")])
        if not sp:
            return
        try:
            wf = workflow_from_json(Path(sp))
            self.workflow = wf
            self.workflow_panel.load_workflow(wf)
            self._log(f"Workflow importert: {Path(sp).name}  ({len(wf)} operasjoner)", "success")
        except Exception as e:
            messagebox.showerror("Importfeil", str(e))

    # ─── Rapport ──────────────────────────────────────────────────────────────

    def _export_report(self):
        from tkinter import filedialog, messagebox
        if self._current_run is None:
            messagebox.showinfo("Ingen kjøring", "Kjør en workflow først for å generere rapport.")
            return
        idir = str(self.siard_path.parent) if self.siard_path else "."
        ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = self.siard_path.stem if self.siard_path else "rapport"
        sp = filedialog.asksaveasfilename(
            title="Lagre rapport",
            initialdir=idir,
            initialfile=f"{stem}_rapport_{ts}.html",
            defaultextension=".html",
            filetypes=[("HTML", "*.html"), ("Alle", "*.*")])
        if not sp:
            return
        try:
            saved = save_html(self._current_run, Path(sp),
                              workflow_name=stem)
            self._log(f"Rapport lagret: {saved}", "success")
            import webbrowser
            webbrowser.open(saved.as_uri())
        except Exception as e:
            messagebox.showerror("Feil", str(e))

    # ─── Auto-logg til fil ────────────────────────────────────────────────────

    def _toggle_auto_log(self):
        from tkinter import filedialog
        start = str(self._auto_log_dir) if self._auto_log_dir else "."
        d = filedialog.askdirectory(
            title="Velg logg-mappe",
            initialdir=start)
        if d:
            self._auto_log_dir = Path(d)
            self._log(f"Logg-mappe endret til: {d}", "success")

    # ─── Batch ────────────────────────────────────────────────────────────────

    def _open_settings(self):
        """Åpne global innstillinger-dialog."""
        SettingsDialog(self, on_save=self._on_global_settings_saved)

    def _on_global_settings_saved(self, cfg: dict):
        """Kalles når global innstillinger lagres."""
        from settings import save_config
        save_config(cfg)
        # Oppdater temp-mappe hvis endret
        td = cfg.get("global_temp_dir", "").strip()
        if td and Path(td).is_dir():
            self._global_temp_dir = Path(td)
            self._update_temp_label()
        # Oppdater AV-sti i VirusScan-operasjon sine lagrede params
        av = cfg.get("av_executable", "").strip()
        if av:
            try:
                from settings import save_op_params, get_op_params
                vp = get_op_params("virus_scan", {})
                vp["av_executable"] = av
                save_op_params("virus_scan", vp)
            except Exception:
                pass
        # Oppdater workers/batch i blob_convert sine lagrede params
        try:
            from settings import save_op_params, get_op_params
            bp = get_op_params("blob_convert", {})
            if "max_workers" in cfg:
                bp["max_workers"]  = int(cfg["max_workers"])
            if "lo_batch_size" in cfg:
                bp["lo_batch_size"] = int(cfg["lo_batch_size"])
            if "lo_timeout" in cfg:
                bp["lo_timeout"]   = int(cfg["lo_timeout"])
            save_op_params("blob_convert", bp)
        except Exception:
            pass
        self._log("Globale innstillinger lagret til config.json", "ok")

    def _add_operation(self, op):
        if self.workflow is None:
            # Lag tom workflow — ingen fil trengs for å bygge opp operasjonslisten
            from siard_workflow.core.workflow import Workflow as _WF
            self.workflow = _WF(name="workflow")
        self.workflow.add(op)
        self.workflow_panel.add_operation(op)
        self._log(f"Lagt til: {op.label}", "info")

    # ─── Kjoring ──────────────────────────────────────────────────────────────

    def _pipeline_preflight(self, ops: list) -> bool:
        """
        Sjekker om workflow inneholder operasjoner som krever utpakket SIARD
        (requires_unpack=True) uten at UnpackSiardOperation og
        RepackSiardOperation er lagt til. Viser en dialog med forslag om å
        legge dem til automatisk.

        Returnerer True hvis workflow kan kjøres, False hvis brukeren avbrøt.
        """
        from siard_workflow.operations import UnpackSiardOperation, RepackSiardOperation

        needs_unpack = [op for op in ops if getattr(op, "requires_unpack", False)]
        if not needs_unpack:
            return True   # ingen operasjoner krever utpakking

        has_unpack = any(isinstance(op, UnpackSiardOperation) for op in ops)
        has_repack = any(isinstance(op, RepackSiardOperation) for op in ops)

        if has_unpack and has_repack:
            return True   # allerede konfigurert korrekt

        missing = []
        if not has_unpack:
            missing.append("Pakk ut SIARD")
        if not has_repack:
            missing.append("Pakk sammen SIARD")

        op_names = ", ".join(f"'{op.label}'" for op in needs_unpack[:3])
        if len(needs_unpack) > 3:
            op_names += f" (+{len(needs_unpack) - 3} til)"

        msg = (
            f"Workflowen inneholder {op_names} som kan dra nytte av "
            f"pipeline-modus (én utpakking/sammenpakning).\n\n"
            f"Mangler: {', '.join(missing)}.\n\n"
            f"Vil du legge til disse automatisk?\n"
            f"• 'Pakk ut SIARD' settes først i workflowen\n"
            f"• 'Pakk sammen SIARD' settes sist i workflowen"
        )

        dialog = _PipelineSuggestionDialog(self, msg)
        self.wait_window(dialog)

        if dialog.result == "ja":
            current_ops = list(self.workflow_panel.get_operations())
            new_ops = []
            if not has_unpack:
                new_ops.append(UnpackSiardOperation())
            new_ops.extend(current_ops)
            if not has_repack:
                new_ops.append(RepackSiardOperation())
            self.workflow_panel.clear()
            for op in new_ops:
                self.workflow_panel.add_operation(op)
            self._log(
                "Pipeline-operasjoner lagt til: 'Pakk ut SIARD' og/eller "
                "'Pakk sammen SIARD' — klikk Kjør for å starte.", "ok")
            return False   # stopp denne kjøringen; bruker klikker Kjør på nytt

        if dialog.result == "nei":
            # Fortsett uten pipeline-operasjoner (gammel ZIP-modus)
            return True

        # Avbrutt
        return False

    def _report_preflight(self, ops: list) -> bool:
        """
        Spør om brukeren vil legge til 'Kjørerapport (PDF)' dersom den ikke
        allerede er med i workflowen.

        Returnerer True = fortsett, False = avbryt.
        """
        from siard_workflow.operations import WorkflowReportOperation

        if any(isinstance(op, WorkflowReportOperation) for op in ops):
            return True   # allerede med

        msg = (
            "Workflowen inneholder ingen 'Kjørerapport (PDF)'-operasjon.\n\n"
            "En PDF-sluttrapport gir saksbehandler en lesbar oversikt over "
            "hva som ble utført og hva resultatet ble.\n\n"
            "Vil du legge til rapporten automatisk (sist i workflowen)?"
        )

        dialog = _PipelineSuggestionDialog(self, msg)
        dialog.title("Sluttrapport")
        self.wait_window(dialog)

        if dialog.result == "ja":
            self.workflow_panel.add_operation(WorkflowReportOperation())
            self._log(
                "'Kjørerapport (PDF)' lagt til sist i workflowen "
                "— klikk Kjør for å starte.", "ok")
            return False   # stopp denne kjøringen; bruker klikker Kjør på nytt

        if dialog.result == "nei":
            return True   # brukeren vil ikke ha rapport — fortsett uten

        return False       # avbrutt

    def _disk_space_preflight(self, ops: list) -> bool:
        """
        Pre-flight diskplass-sjekk for BlobConvertOperation:
          1. Temp-disk: nok plass til utpakking og konvertering?
          2. Output-disk: nok plass til å skrive ferdig konvertert SIARD ved kildefilen?

        Viser advarsel-dialog(er) og ber om alternativt lagringssted ved output-problem.
        Returnerer True = fortsett, False = avbryt.
        """
        import shutil as _shutil
        from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
        from disk_selector import check_disk_space, format_bytes, _disk_root
        from tkinter import messagebox, filedialog

        blob_ops = [op for op in ops if isinstance(op, BlobConvertOperation)]
        if not blob_ops:
            return True

        self._output_dir_override = ""   # nullstill fra forrige kjøring

        # Effektiv temp-dir: global overstyrer per-op
        if self._global_temp_dir:
            effective_temp = Path(str(self._global_temp_dir))
        else:
            per_op_dir = blob_ops[0].params.get("temp_dir", "").strip()
            effective_temp = Path(per_op_dir) if per_op_dir else None

        temp_warnings:   list[tuple[Path, dict]] = []
        output_warnings: list[tuple[Path, int, int]] = []  # (path, required, available)

        for siard_path in self.siard_queue:
            # — Temp-sjekk —
            r = check_disk_space(siard_path, effective_temp)
            if not r["ok"]:
                temp_warnings.append((siard_path, r))

            # — Output-sjekk —
            # Estimert output-størrelse ≈ total utpakket innhold
            # (PDF/A komprimerer dårlig i ZIP, så dette er realistisk øvre grense)
            output_estimate = r["uncompressed_bytes"]
            try:
                out_free = _shutil.disk_usage(siard_path.parent).free
                if out_free < output_estimate:
                    output_warnings.append((siard_path, output_estimate, out_free))
            except Exception:
                pass

        # ── Temp-advarsel ────────────────────────────────────────────────────
        if temp_warnings:
            lines: list[str] = [
                "Advarsel: estimert temp-diskbehov overstiger tilgjengelig plass!\n"
            ]
            for siard_path, r in temp_warnings:
                lines.append(f"  Fil      : {siard_path.name}")
                lines.append(f"  Behov    : {format_bytes(r['required_bytes'])}"
                             f"  (utpakket: {format_bytes(r['uncompressed_bytes'])})")
                lines.append(f"  Ledig    : {format_bytes(r['available_bytes'])}"
                             f"  på {r['temp_path']}")
                if r["alternatives"]:
                    lines.append("  Disker med nok plass:")
                    for alt in r["alternatives"][:3]:
                        lines.append(f"    • {alt['label']}")
                else:
                    lines.append("  Ingen andre disker med tilstrekkelig plass funnet.")
                lines.append("")
            lines.append(
                "Tips: endre temp-mappe i operasjonens innstillinger, "
                "eller rydd opp på gjeldende disk.\n\nVil du fortsette uansett?"
            )
            if not messagebox.askyesno(
                "Lite temp-diskplass", "\n".join(lines),
                icon="warning", default="no",
            ):
                return False

        # ── Output-advarsel ──────────────────────────────────────────────────
        if output_warnings:
            lines = [
                "Advarsel: estimert plass for ferdig konvertert SIARD-fil "
                "overstiger ledig plass der kilde-filen ligger!\n"
            ]
            for siard_path, needed, avail in output_warnings:
                lines.append(f"  Fil      : {siard_path.name}")
                lines.append(f"  Estimert : {format_bytes(needed)}")
                lines.append(f"  Ledig    : {format_bytes(avail)}"
                             f"  ({siard_path.parent})")
                lines.append("")
            lines.append(
                "Velg et alternativt lagringssted for den konverterte filen, "
                "eller avbryt og rydd opp plass først."
            )
            messagebox.showwarning("Lite plass for output-fil", "\n".join(lines))

            chosen = filedialog.askdirectory(
                title="Velg alternativ lagringsmappe for konvertert SIARD",
                mustexist=True,
            )
            if not chosen:
                return False   # Bruker avbrøt mappe-velger

            # Verifiser at valgt mappe faktisk har nok plass
            try:
                worst_needed = max(n for _, n, _ in output_warnings)
                chosen_free  = _shutil.disk_usage(chosen).free
                if chosen_free < worst_needed:
                    messagebox.showerror(
                        "Ikke nok plass",
                        f"Valgt mappe har bare {format_bytes(chosen_free)} ledig "
                        f"— trenger minst {format_bytes(worst_needed)}.\n"
                        "Velg en annen mappe eller rydd opp plass.",
                    )
                    return False
            except Exception:
                pass

            self._output_dir_override = chosen

        return True

    def _run_workflow(self):
        if self._running:
            return
        if not self.siard_queue:
            self._log("Ingen SIARD-filer i køen", "warn")
            return
        ops = list(self.workflow_panel.get_operations())
        if not ops:
            self._log("Workflowen er tom", "warn")
            return

        if not self._pipeline_preflight(ops):
            return

        # Re-hent ops etter preflight — pipeline_preflight kan ha lagt til
        # UnpackSiardOperation og RepackSiardOperation i panelet
        ops = list(self.workflow_panel.get_operations())

        if not self._report_preflight(ops):
            return

        # Re-hent ops igjen i tilfelle rapport-operasjon ble lagt til
        ops = list(self.workflow_panel.get_operations())

        if not self._disk_space_preflight(ops):
            return

        self._running = True
        self._current_run = None
        self.workflow_panel.set_running(True)
        self.log_panel.clear()
        self._log_entries.clear()

        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log("=" * 56, "muted")
        self._log(f"Start: {ts}", "step")
        self._log(f"Kø:    {len(self.siard_queue)} fil(er)", "info")
        self._log(f"Steg:  {len(ops)}", "info")
        self._log("=" * 56, "muted")

        # Start spinner
        self.after(0, self._tick_spinner)

        # Kjør alle filer i køen sekvensielt i én bakgrunnstråd
        filer = list(self.siard_queue)
        thread = threading.Thread(
            target=self._run_queue_thread, args=(filer, ops), daemon=True)
        thread.start()

    def _run_queue_thread(self, filer: list, ops: list):
        """Kjør workflow sekvensielt på alle filer i køen."""
        import time
        from siard_workflow.core.context import WorkflowContext
        from siard_workflow.core.workflow import WorkflowRun

        for q_idx, path in enumerate(filer):
            if self._stop_event.is_set():
                break

            self._log_queue.put(("queue_file_start", q_idx, len(filer), path))

            wf = Workflow(name=path.stem)
            for op in ops:
                wf.add(op)

            ctx = WorkflowContext(siard_path=path)
            run = WorkflowRun(path)
            self._conv_ctx = ctx

            def _progress_cb(event, **kw):
                self._log_queue.put(("conv_" + event, kw))

            ctx.metadata["progress_cb"]  = _progress_cb
            ctx.metadata["paused"]       = False
            ctx.metadata["stopped"]      = False
            self._stop_event.clear()
            self._pause_event.clear()
            ctx.metadata["stop_event"]   = self._stop_event
            ctx.metadata["pause_event"]  = self._pause_event
            if self._global_temp_dir:
                ctx.metadata["temp_dir"] = str(self._global_temp_dir)
            if self._output_dir_override:
                ctx.metadata["output_dir_override"] = self._output_dir_override
            log_dir = path.parent
            if self._auto_log_dir:
                log_dir = self._auto_log_dir
            ctx.metadata["log_dir"] = str(log_dir)

            run.start_time = time.time()
            file_logger = None
            try:
                file_logger = WorkflowFileLogger(log_dir, path.stem)
                file_logger.__enter__()
                ctx.metadata["file_logger"] = file_logger
            except Exception:
                pass

            ctx.metadata["step_results"] = []

            for i, op in enumerate(wf):
                self._log_queue.put(("step_start", i, op.label))
                if file_logger:
                    file_logger.log(f"[{i+1}] {op.label}", "step")
                if not op.should_run(ctx):
                    self._log_queue.put(("skip", op.operation_id, op.label))
                    run.skipped.append(op.operation_id)
                    if file_logger:
                        file_logger.log("  Hoppet over (vilkår ikke oppfylt)", "muted")
                    ctx.metadata["step_results"].append({
                        "id":       op.operation_id,
                        "label":    op.label,
                        "category": getattr(op, "category", ""),
                        "success":  None,
                        "message":  "Hoppet over (vilkår ikke oppfylt)",
                        "skipped":  True,
                        "elapsed":  0.0,
                    })
                    continue
                t0 = time.time()
                try:
                    result = op.run(ctx)
                    ctx.set_result(op.operation_id, result.data)
                    # Hvis operasjonen produserte en ny SIARD-fil, oppdater input-stien
                    new_siard = ctx.chain_siard(op.produces_siard, result.data)
                    if new_siard:
                        self._log_queue.put(("log",
                            f"  ↪ Neste steg bruker: {new_siard.name}", "info"))
                except Exception as exc:
                    result = op._fail(str(exc))
                elapsed = time.time() - t0
                run.results.append(result)
                self._log_queue.put(("result", result))
                if file_logger:
                    lvl = "ok" if result.success else "feil"
                    file_logger.log(f"  {lvl.upper()}: {result.message}", lvl)
                ctx.metadata["step_results"].append({
                    "id":       op.operation_id,
                    "label":    op.label,
                    "category": getattr(op, "category", ""),
                    "success":  result.success,
                    "message":  result.message,
                    "skipped":  False,
                    "elapsed":  elapsed,
                })
                # Stopp workflowen umiddelbart ved kritisk feil
                if not result.success and getattr(op, "halt_on_failure", False):
                    self._log_queue.put((
                        "workflow_halted",
                        op.label,
                        result.message,
                    ))
                    break

            run.end_time = time.time()
            if file_logger:
                file_logger.log(
                    f"Fullført: {'SUKSESS' if run.success else 'FEIL'}  ({run.elapsed:.2f}s)")
                file_logger.__exit__(None, None, None)
                self._log_queue.put(("log_saved", str(file_logger.log_path)))

            self._log_queue.put(("done_one", run, q_idx, len(filer), path))

        self._log_queue.put(("queue_done", None))

    def _poll_log_queue(self):
        # Behandle maks 50 meldinger per runde.
        # update_idletasks() kalles kun ved slutten (ikke per melding).
        processed = 0
        needs_redraw = False
        try:
            while processed < 50:
                item = self._log_queue.get_nowait()
                kind = item[0]
                processed += 1

                if kind == "step_start":
                    _, idx, label = item
                    self._log(f"  [{idx+1}] {label}", "step")
                    self.workflow_panel.highlight_step(idx)
                    self.progress_panel.show_simple(label)
                    needs_redraw = True
                elif kind == "skip":
                    _, op_id, label = item
                    self._log(f"      [-] Hoppet over", "muted")
                    self.progress_panel.reset()
                    needs_redraw = True
                elif kind == "result":
                    _, result = item
                    ok  = result.success
                    lvl = "success" if ok else "error"
                    self._log(f"      [{'OK' if ok else 'FEIL'}] {result.message}", lvl)
                    self.progress_panel.step_complete(ok)
                    needs_redraw = True
                elif kind == "log_saved":
                    _, path = item
                    self._log(f"Kjørelogg skrevet til: {path}", "muted")
                elif kind == "queue_file_start":
                    _, q_idx, q_tot, path = item
                    self.siard_path = path
                    self._log("─" * 40, "muted")
                    self._log(f"Fil [{q_idx+1}/{q_tot}]: {path.name}", "step")
                    self._log("─" * 40, "muted")
                    self.workflow_panel.highlight_step(-1)
                    self.progress_panel.reset()
                    self.format_chart.reset()
                    self._update_status_right(
                        f"Kjører [{q_idx+1}/{q_tot}]: {path.name}")
                    needs_redraw = True
                elif kind == "done_one":
                    _, run, q_idx, q_tot, path = item
                    self._current_run = run
                    self.workflow_panel.highlight_step(-1)
                    status = "SUKSESS" if run.success else "FEIL"
                    lvl    = "success" if run.success else "error"
                    self._log(f"Fullført: {status}  ({run.elapsed:.2f}s)", lvl)
                    if run.skipped:
                        self._log(f"Hoppet over: {', '.join(run.skipped)}", "muted")
                elif kind == "workflow_halted":
                    _, op_label, msg = item
                    self._log("!" * 56, "error")
                    self._log(f"  WORKFLOW STOPPET — {op_label.upper()}", "error")
                    self._log(f"  {msg}", "error")
                    self._log("  Ingen videre operasjoner vil bli kjørt.", "error")
                    self._log("!" * 56, "error")
                    from tkinter import messagebox
                    messagebox.showerror(
                        "Workflow stoppet",
                        f"Operasjonen «{op_label}» feilet:\n\n{msg}\n\n"
                        "Ingen videre operasjoner vil bli kjørt for denne filen.\n"
                        "Undersøk filen manuelt før du fortsetter.",
                    )
                    needs_redraw = True
                elif kind == "queue_done":
                    self._running = False
                    self.workflow_panel.set_running(False)
                    self.workflow_panel.highlight_step(-1)
                    self._log("=" * 56, "muted")
                    self._log("Kø fullført", "success")
                    self._log("=" * 56, "muted")
                    self._update_status_right(
                        f"Ferdig — {len(self.siard_queue)} fil(er) prosessert")

                # ── BLOB-konvertering fremdrift ────────────────────────────
                elif kind == "conv_init":
                    _, kw = item
                    self.progress_panel.init_run(kw["total"])
                    self.format_chart.reset()
                    self.log_panel.set_live_mode(True)
                    self._log(f"  Konverterer {kw['total']:,} fil(er) ...", "step")
                    needs_redraw = True
                elif kind == "conv_phase":
                    _, kw = item
                    self.progress_panel.set_phase(
                        kw["phase"], kw["total_phases"], kw["label"])
                    self._log(f"  [{kw['phase']}/{kw['total_phases']}] {kw['label']}", "step")
                    needs_redraw = True
                elif kind == "conv_phase_done":
                    _, kw = item
                    self.progress_panel.phase_done()
                    needs_redraw = True
                elif kind == "conv_phase_progress":
                    _, kw = item
                    self.progress_panel.set_phase_progress(
                        kw["done"], kw["total"], kw.get("label", ""))
                    needs_redraw = True
                elif kind == "conv_error":
                    _, kw = item
                    self._log(f"  [FEIL] {kw['file']}: {kw['error']}", "error")
                elif kind == "conv_log":
                    _, kw = item
                    msg = kw.get("msg", "")
                    lvl = kw.get("level", "info")
                    gui_lvl = {"ok": "success", "feil": "error",
                               "warn": "warn"}.get(lvl, "info")
                    self._log(msg, gui_lvl)
                    needs_redraw = True
                elif kind == "conv_file_start":
                    _, kw = item
                    self.progress_panel.file_started(
                        kw["idx"], kw["filename"],
                        kw["detected_ext"], kw["mime"])
                    needs_redraw = True
                elif kind == "conv_file_done":
                    _, kw = item
                    self.progress_panel.file_done(
                        kw["idx"], kw["filename"],
                        kw["result_ext"], kw["ok"],
                        kw["msg"], kw["stats"])
                    # Oppdater format-diagram
                    self.format_chart.update_format(kw["detected_ext"], 1)
                    # Logg kun feil til tekstpanel — ikke hver fil (kan vaere 100k+)
                    if not kw["ok"]:
                        self._log(
                            f"  [FEIL] {kw['filename']}  "
                            f"{kw['detected_ext']} → {kw['result_ext']}  "
                            f"{kw['msg']}", "warn")
                    needs_redraw = True
                elif kind == "conv_stats_update":
                    _, kw = item
                    self.progress_panel.update_stats_and_bar(kw["stats"], kw["done"])
                    needs_redraw = True
                elif kind == "conv_rename_format_counts":
                    _, kw = item
                    for ext, count in kw["counts"].items():
                        self.format_chart.update_format(ext, count)
                    needs_redraw = True
                elif kind == "conv_finish":
                    _, kw = item
                    self.progress_panel.finish(kw["stats"])
                    self.log_panel.set_live_mode(False)
                    self._conv_ctx = None
                    needs_redraw = True
                elif kind == "conv_aborted":
                    _, kw = item
                    self.progress_panel.finish(kw["stats"])
                    self.log_panel.set_live_mode(False)
                    self._log("  Konvertering stoppet av bruker", "warn")
                    self._conv_ctx = None
                    needs_redraw = True

        except queue.Empty:
            pass
        except Exception as _poll_exc:
            import traceback
            self._log(f"[INTERN FEIL] {_poll_exc}", "error")
            self._log(traceback.format_exc(), "error")

        # Spinner-animasjon: tick alle aktive rader kvar 80ms
        self.progress_panel.update_spinner()

        # Tvung omtegning — men ikke for ofte (kostbart med mange widgets)
        if needs_redraw:
            self.update_idletasks()

        self.after(50, self._poll_log_queue)

    def _clear_workflow(self):
        self.workflow = None
        self.workflow_panel.clear()
        self.log_panel.clear()
        self._log_entries.clear()
        self._log("Workflow tomt", "muted")

    # ─── Konvertering pause/stopp ────────────────────────────────────────────

    def _conv_pause(self):
        if self._pause_event.is_set():
            # Allerede pauset — fortsett
            self._pause_event.clear()
            if self._conv_ctx:
                self._conv_ctx.metadata["paused"] = False
            self.progress_panel.set_paused(False)
            self._log("Konvertering fortsetter", "warn")
        else:
            # Sett pause
            self._pause_event.set()
            if self._conv_ctx:
                self._conv_ctx.metadata["paused"] = True
            self.progress_panel.set_paused(True)
            self._log("Konvertering pauset", "warn")

    def _conv_stop(self):
        self._stop_event.set()
        self._pause_event.clear()   # frigjør pause-loop
        if self._conv_ctx:
            self._conv_ctx.metadata["stopped"] = True
            self._conv_ctx.metadata["paused"]  = False
        self.progress_panel.set_stopped()
        self._log("Stopper konvertering ...", "warn")

    def _log(self, msg: str, level: str = "info"):
        self.log_panel.append(msg, level)
        self._log_entries.append((level, msg))
