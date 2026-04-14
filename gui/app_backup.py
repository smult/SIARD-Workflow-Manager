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
from gui.operations_panel import OperationsPanel
from gui.log_panel import LogPanel
from gui.styles import COLORS, FONTS
from gui.settings_dialog import SettingsDialog

sys.path.insert(0, str(Path(__file__).parent.parent))
from siard_workflow import create_manager
from siard_workflow.core.workflow import Workflow
from siard_workflow.core.report import save_html, save_pdf
from siard_workflow.core.workflow_io import workflow_to_json, workflow_from_json
from siard_workflow.core.file_logger import WorkflowFileLogger
from gui.batch_panel import BatchWindow
from gui.progress_panel import ProgressPanel
from gui.format_chart_panel import FormatChartPanel

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")


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
        # Start maksimert
        self.after(0, lambda: self.state("zoomed"))
        self.configure(fg_color=COLORS["bg"])

        self.manager         = create_manager()
        self.siard_path: Path | None = None
        self.workflow: Workflow | None = None
        self._log_queue: queue.Queue = queue.Queue()
        self._running        = False
        self._log_entries: list[tuple[str,str]] = []
        self._current_run: "WorkflowRun | None" = None
        self._auto_log_dir: Path | None = None
        self._global_temp_dir: Path | None = None
        self._conv_ctx = None
        self._stop_event  = threading.Event()
        self._pause_event = threading.Event()

        self._build_ui()
        self._load_persistent_profiles()
        self._load_saved_temp()
        self._poll_log_queue()

    # ─── UI ──────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=0)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self._build_topbar()

        left = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=10, width=380)
        left.grid(row=1, column=0, padx=(12,6), pady=(0,12), sticky="nsew")
        left.grid_rowconfigure(1, weight=1)
        left.grid_rowconfigure(2, weight=0)
        left.grid_columnconfigure(0, weight=1)
        left.grid_propagate(False)

        self.workflow_panel = WorkflowPanel(
            left,
            on_run=self._run_workflow,
            on_clear=self._clear_workflow,
            on_save_profile=self._save_profile,
            on_settings_saved=self._on_settings_saved,
        )
        self.workflow_panel.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

        self.format_chart = FormatChartPanel(left)
        self.format_chart.grid(row=2, column=0, padx=10, pady=(0, 10), sticky="ew")

        right = ctk.CTkFrame(self, fg_color="transparent")
        right.grid(row=1, column=1, padx=(6,12), pady=(0,12), sticky="nsew")
        right.grid_rowconfigure(0, weight=0)   # ops_panel
        right.grid_rowconfigure(1, weight=0)   # progress_panel (skjult/synlig)
        right.grid_rowconfigure(2, weight=1)   # log_panel
        right.grid_columnconfigure(0, weight=1)

        self.ops_panel = OperationsPanel(right, on_add=self._add_operation,
                                          on_saved=self._on_settings_saved)
        self.ops_panel.grid(row=0, column=0, pady=(0,8), sticky="ew")

        self.progress_panel = ProgressPanel(right)
        self.progress_panel.grid(row=1, column=0, pady=(0,8), sticky="ew")
        self.progress_panel.set_callbacks(
            pause_cb=self._conv_pause,
            stop_cb=self._conv_stop)

        self.log_panel = LogPanel(right)
        self.log_panel.grid(row=2, column=0, sticky="nsew")

    def _build_topbar(self):
        bar = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=0, height=56)
        bar.grid(row=0, column=0, columnspan=2, sticky="ew")
        bar.grid_columnconfigure(3, weight=1)   # filsti + temp fyller ledig plass
        bar.grid_propagate(False)

        # Kolonne 0: appnavn
        ctk.CTkLabel(bar, text="SIARD WORKFLOW MANAGER",
                     font=ctk.CTkFont(family=FONTS["mono"], size=15, weight="bold"),
                     text_color=COLORS["accent"]).grid(
                         row=0, column=0, padx=20, pady=14, sticky="w")

        # Kolonne 1: Velg fil
        self.file_btn = ctk.CTkButton(
            bar, text="Velg fil", width=90,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            command=self._browse_file)
        self.file_btn.grid(row=0, column=1, padx=(10, 4), pady=10)

        # Kolonne 2: Profil
        self.profile_var = ctk.StringVar(value="-- profil --")
        self.profile_menu = ctk.CTkOptionMenu(
            bar, values=["-- profil --"] + self.manager.list_profiles(),
            variable=self.profile_var, width=160,
            fg_color=COLORS["btn"], button_color=COLORS["accent"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            command=self._load_profile)
        self.profile_menu.grid(row=0, column=2, padx=4, pady=10)

        # Kolonne 3: Filsti + temp-info (vokser)
        info_frame = ctk.CTkFrame(bar, fg_color="transparent")
        info_frame.grid(row=0, column=3, padx=(8, 4), pady=6, sticky="ew")
        info_frame.grid_columnconfigure(0, weight=1)

        self.file_label = ctk.CTkLabel(
            info_frame, text="Ingen fil valgt",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"], anchor="w")
        self.file_label.grid(row=0, column=0, sticky="ew")

        self.temp_label = ctk.CTkLabel(
            info_frame, text="Temp: (auto)",
            font=ctk.CTkFont(family=FONTS["mono"], size=9),
            text_color=COLORS["muted"], anchor="w")
        self.temp_label.grid(row=1, column=0, sticky="ew")

        # Meny-knapper (høyre side)
        menu_cfg = dict(height=30, width=100, fg_color=COLORS["btn"],
                        hover_color=COLORS["btn_hover"],
                        font=ctk.CTkFont(family=FONTS["mono"], size=10))

        ctk.CTkButton(bar, text="Endre temp",
                      command=self._browse_temp, **menu_cfg).grid(
                          row=0, column=4, padx=4, pady=10)
        ctk.CTkButton(bar, text="Importer WF",
                      command=self._import_workflow, **menu_cfg).grid(
                          row=0, column=5, padx=4, pady=10)
        ctk.CTkButton(bar, text="Eksporter WF",
                      command=self._export_workflow, **menu_cfg).grid(
                          row=0, column=6, padx=4, pady=10)
        ctk.CTkButton(bar, text="Rapport",
                      command=self._export_report, **menu_cfg).grid(
                          row=0, column=7, padx=4, pady=10)
        ctk.CTkButton(bar, text="Logg til fil",
                      command=self._toggle_auto_log, **menu_cfg).grid(
                          row=0, column=8, padx=4, pady=10)
        ctk.CTkButton(bar, text="Innstillinger",
                      command=self._open_settings, **menu_cfg).grid(
                          row=0, column=9, padx=4, pady=10)
        ctk.CTkButton(bar, text="Batch",
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      height=30, width=70,
                      font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                      command=self._open_batch).grid(
                          row=0, column=10, padx=(4, 8), pady=10)

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
            display = p if len(p) <= 45 else "…" + p[-45:]
            self.temp_label.configure(
                text=f"Temp: {display}", text_color=COLORS["text"])
        else:
            self.temp_label.configure(
                text="Temp: (auto)", text_color=COLORS["muted"])

    # ─── Fil ─────────────────────────────────────────────────────────────────

    def _browse_file(self):
        from tkinter import filedialog
        path = filedialog.askopenfilename(
            title="Velg SIARD-fil",
            filetypes=[("SIARD", "*.siard"), ("ZIP", "*.zip"), ("Alle", "*.*")])
        if path:
            self._set_file(Path(path))

    def _set_file(self, path: Path):
        self.siard_path = path
        full = str(path)
        display = full if len(full) <= 60 else "…" + full[-60:]
        self.file_label.configure(text=display, text_color=COLORS["green"])
        self._log(f"Fil: {path}", "info")
        self.workflow_panel.set_file(path)
        self._auto_log_dir = path.parent
        self._log(f"Logg-mappe: {path.parent}", "muted")

        # Auto-velg best temp-disk — kun hvis ingen manuell override er satt
        if self._global_temp_dir is None:
            self._auto_select_temp(path)
        else:
            self._log(f"Temp-mappe (beholdt): {self._global_temp_dir}", "muted")

        # Nullstill all fremgangsstatus fra forrige kjøring
        self.progress_panel.reset()
        self.format_chart.reset()
        self._log_entries.clear()
        self.log_panel.clear()

    # ─── Profiler ─────────────────────────────────────────────────────────────

    def _load_profile(self, name: str):
        if name.startswith("--"):
            return
        if not self.siard_path:
            self._log("Velg en SIARD-fil først", "warn")
            return
        wf = self.manager.create_workflow(self.siard_path, profile=name)
        self.workflow = wf
        self.workflow_panel.load_workflow(wf)
        self._log(f"Profil '{name}' lastet  ({len(wf)} operasjoner)", "info")

    def _load_saved_temp(self):
        """Last inn lagret global temp-mappe fra config.json ved oppstart."""
        try:
            from settings import get_config
            saved = get_config("global_temp_dir", "")
            if saved and Path(saved).is_dir():
                self._global_temp_dir = Path(saved)
                self._update_temp_label()
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

    def _open_batch(self):
        BatchWindow(self,
                    get_workflow_ops=lambda: list(self.workflow_panel.get_operations()),
                    manager=self.manager)

    # ─── Operasjoner ──────────────────────────────────────────────────────────

    def _add_operation(self, op):
        if self.workflow is None:
            if self.siard_path:
                self.workflow = self.manager.create_workflow(self.siard_path)
            else:
                self._log("Velg en SIARD-fil først", "warn")
                return
        self.workflow.add(op)
        self.workflow_panel.add_operation(op)
        self._log(f"Lagt til: {op.label}", "info")

    # ─── Kjoring ──────────────────────────────────────────────────────────────

    def _run_workflow(self):
        if self._running:
            return
        if not self.siard_path or not self.siard_path.exists():
            self._log("Ingen gyldig SIARD-fil valgt", "warn")
            return
        if not self._auto_log_dir:
            self._auto_log_dir = self.siard_path.parent
        ops = list(self.workflow_panel.get_operations())
        if not ops:
            self._log("Workflowen er tom", "warn")
            return

        wf = Workflow(name=self.siard_path.stem)
        for op in ops:
            wf.add(op)

        self._running = True
        self._current_run = None
        self.workflow_panel.set_running(True)
        self.log_panel.clear()
        self._log_entries.clear()
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log("=" * 56, "muted")
        self._log(f"Start: {ts}", "step")
        self._log(f"Fil:   {self.siard_path.name}", "info")
        self._log(f"Steg:  {len(ops)}", "info")
        self._log("=" * 56, "muted")

        thread = threading.Thread(target=self._run_thread, args=(wf,), daemon=True)
        thread.start()

    def _run_thread(self, wf: Workflow):
        import time
        from siard_workflow.core.context import WorkflowContext
        from siard_workflow.core.workflow import WorkflowRun

        ctx = WorkflowContext(siard_path=self.siard_path)
        run = WorkflowRun(self.siard_path)
        self._conv_ctx = ctx   # brukes av pause/stopp-knapper

        # Koble progress-callback til GUI via queue
        def _progress_cb(event, **kw):
            self._log_queue.put(("conv_" + event, kw))

        ctx.metadata["progress_cb"]  = _progress_cb
        ctx.metadata["paused"]       = False
        ctx.metadata["stopped"]      = False
        # Direkte Event-referanser for trådsikker pause/stopp
        self._stop_event.clear()
        self._pause_event.clear()
        ctx.metadata["stop_event"]   = self._stop_event
        ctx.metadata["pause_event"]  = self._pause_event
        # Global temp-mappe for alle operasjoner
        if self._global_temp_dir:
            ctx.metadata["temp_dir"] = str(self._global_temp_dir)
        # Logg-mappe for CSV-detaljlogg
        if self._auto_log_dir:
            ctx.metadata["log_dir"] = self._auto_log_dir
        run.start_time = time.time()

        file_logger = None
        if self._auto_log_dir:
            file_logger = WorkflowFileLogger(self._auto_log_dir, self.siard_path.stem)
            file_logger.__enter__()
            ctx.metadata["file_logger"] = file_logger

        for i, op in enumerate(wf):
            self._log_queue.put(("step_start", i, op.label))
            if file_logger:
                file_logger.log(f"[{i+1}] {op.label}", "step")

            if not op.should_run(ctx):
                self._log_queue.put(("skip", op.operation_id, op.label))
                run.skipped.append(op.operation_id)
                if file_logger:
                    file_logger.log(f"  Hoppet over (vilkar ikke oppfylt)", "muted")
                continue

            try:
                result = op.run(ctx)
                ctx.set_result(op.operation_id, result.data)
            except Exception as exc:
                result = op._fail(str(exc))

            run.results.append(result)
            self._log_queue.put(("result", result))
            if file_logger:
                lvl = "ok" if result.success else "feil"
                file_logger.log(f"  {lvl.upper()}: {result.message}", lvl)

        run.end_time = time.time()

        if file_logger:
            file_logger.log(f"Fullført: {'SUKSESS' if run.success else 'FEIL'}  ({run.elapsed:.2f}s)")
            file_logger.__exit__(None, None, None)
            self._log_queue.put(("log_saved", str(file_logger.log_path)))

        self._log_queue.put(("done", run))

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
                elif kind == "skip":
                    _, op_id, label = item
                    self._log(f"      [-] Hoppet over", "muted")
                elif kind == "result":
                    _, result = item
                    ok  = result.success
                    lvl = "success" if ok else "error"
                    self._log(f"      [{'OK' if ok else 'FEIL'}] {result.message}", lvl)
                elif kind == "log_saved":
                    _, path = item
                    self._log(f"Kjørelogg skrevet til: {path}", "muted")
                elif kind == "done":
                    _, run = item
                    self._running = False
                    self._current_run = run
                    self.workflow_panel.set_running(False)
                    self.workflow_panel.highlight_step(-1)
                    status = "SUKSESS" if run.success else "FEIL"
                    lvl    = "success" if run.success else "error"
                    self._log("", "info")
                    self._log(f"Fullført: {status}  ({run.elapsed:.2f}s)", lvl)
                    if run.skipped:
                        self._log(f"Hoppet over: {', '.join(run.skipped)}", "muted")
                    self._log("=" * 56, "muted")

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
