"""
gui/app.py  —  SIARD Workflow Manager hovedvindu
"""
from __future__ import annotations
import sys
import threading
import queue
import datetime
import time
from pathlib import Path

import customtkinter as ctk

try:
    from tkinterdnd2 import TkinterDnD as _TkinterDnD
    _HAS_TKDND = True
except ImportError:
    _HAS_TKDND = False

from gui.workflow_panel import WorkflowPanel
from gui.operations_panel import OperationsPanel, set_current_siard_path
from gui.log_panel import LogPanel
from gui.header_panel import HeaderDock
from gui.styles import COLORS, FONTS
from gui.settings_dialog import SettingsDialog

sys.path.insert(0, str(Path(__file__).parent.parent))
from siard_workflow import create_manager
from siard_workflow.core.workflow import Workflow
from siard_workflow.core.report import save_html, save_pdf
from siard_workflow.core.workflow_io import workflow_to_json, workflow_from_json
from siard_workflow.core.file_logger import WorkflowFileLogger, resolve_log_dir
from siard_workflow.core.premis_logger import PremisProvenanceLogger
from gui.progress_panel import ProgressPanel
from gui.format_chart_panel import FormatChartPanel
from settings import get_config, set_config

# Initialiser tema før noen widgets bygges. apply_theme leser ikke fra config
# selv, så vi gjør det her én gang.
try:
    from gui.styles import apply_theme as _apply_theme_initial
    _apply_theme_initial(get_config("theme_mode", "dark"))
except Exception:
    ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")


def _sync_checkpoints_to_ops(old_checkpoints: list, ops: list) -> list:
    """Bygg ny checkpoint-liste fra workflow-panelets operasjoner.

    For hvert steg i `ops` brukes eksisterende checkpoint (samme operation_id)
    om det finnes — slik at status/output/ctx_data bevares. Label og params
    oppdateres fra panelet. Nye steg får en pending-checkpoint. Hver gammel
    checkpoint kan kun konsumeres én gang, slik at duplikate operation_ids
    håndteres posisjonelt.
    """
    from siard_workflow.core.project_file import OpCheckpoint
    pool: dict = {}
    for cp in old_checkpoints:
        pool.setdefault(cp.operation_id, []).append(cp)
    new_list: list = []
    for op in ops:
        bucket = pool.get(op.operation_id)
        if bucket:
            cp = bucket.pop(0)
            cp.label  = op.label
            cp.params = dict(op.params)
            new_list.append(cp)
        else:
            new_list.append(OpCheckpoint(
                operation_id=op.operation_id,
                label=op.label,
                params=dict(op.params)))
    return new_list


def _apply_checkpoint_params(ops: list, checkpoints: list) -> int:
    """Gi workflow-panelets operasjoner parameterne fra en prosjektfil.

    Brukes når en prosjektfil lastes automatisk (samme navn som SIARD-filen):
    panelet beholder stegene, men verdiene (f.eks. DIAS «Submission Agreement»)
    skal være prosjektets — ellers overskrev neste kjøring prosjektfila med
    panelets verdier. Steg matches på operation_id, posisjonelt ved duplikater.
    Returnerer antall operasjoner som fikk parametere.
    """
    pool: dict = {}
    for cp in checkpoints:
        pool.setdefault(cp.operation_id, []).append(cp)
    n = 0
    for op in ops:
        bucket = pool.get(op.operation_id)
        if bucket:
            cp = bucket.pop(0)
            if cp.params:
                op.params = {**op.params, **cp.params}
                n += 1
    return n


class _PipelineSuggestionDialog(ctk.CTkToplevel):
    """
    Dialog som foreslår å legge til 'Pakk ut SIARD' og 'Pakk sammen SIARD'
    når workflow inneholder operasjoner som drar nytte av pipeline-modus.
    result: "ja" | "nei" | "avbryt"
    """

    def __init__(self, parent, message: str, heading: str = "Pipeline-modus anbefalt",
                 yes_text: str = "Ja, legg til automatisk", no_text: str = "Nei, kjør uten",
                 cancel: bool = True):
        super().__init__(parent)
        self.result = "avbryt" if cancel else "nei"
        self.title("Pipeline-forslag")
        self.resizable(False, False)
        self.grab_set()
        self.lift()

        self.configure(fg_color=COLORS["bg"])

        w    = 560
        wrap = w - 64

        # Ikon + tittel
        header = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        header.pack(side="top", fill="x", padx=0, pady=0)
        ctk.CTkLabel(
            header,
            text=f"  {heading}",
            font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
            text_color=COLORS["accent"],
            anchor="w",
        ).pack(side="left", padx=12, pady=10)

        # Knapper — pakkes FØR meldingen (side=bottom) slik at de alltid er
        # synlige og aldri skyves ut, uansett hvor langt innholdet er.
        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(side="bottom", fill="x", padx=16, pady=14)
        btn_row.grid_columnconfigure((0, 1, 2) if cancel else (0, 1), weight=1)

        ctk.CTkButton(
            btn_row, text=yes_text,
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"], text_color=COLORS["on_accent"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            height=34,
            command=self._on_ja,
        ).grid(row=0, column=0, padx=(0, 6), sticky="ew")

        ctk.CTkButton(
            btn_row, text=no_text,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"], text_color=COLORS["btn_text"], border_color=COLORS["btn_border"], border_width=1,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            height=34,
            command=self._on_nei,
        ).grid(row=0, column=1, padx=3, sticky="ew")

        if cancel:
            ctk.CTkButton(
                btn_row, text="Avbryt",
                fg_color=COLORS["danger_bg"], hover_color=COLORS["danger_hover"], text_color=COLORS["danger_text"], border_color=COLORS["danger_border"], border_width=1,
                font=ctk.CTkFont(family=FONTS["mono"], size=11),
                height=34,
                command=self._on_avbryt,
            ).grid(row=0, column=2, padx=(6, 0), sticky="ew")

        # Meldingstekst i rullbar ramme — fyller plassen mellom header og
        # knapper. Rammen scroller dersom innholdet er høyere enn maks-høyden.
        msg_frame = ctk.CTkScrollableFrame(self, fg_color="transparent")
        msg_frame.pack(side="top", fill="both", expand=True, padx=0, pady=0)
        msg_label = ctk.CTkLabel(
            msg_frame,
            text=message,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["text"],
            wraplength=wrap,
            justify="left",
            anchor="nw",
        )
        msg_label.pack(padx=16, pady=(14, 8), fill="x", anchor="nw")

        self.protocol("WM_DELETE_WINDOW", self._on_avbryt if cancel else self._on_nei)

        # Dynamisk høyde ut fra faktisk innhold (klemt til 85 % av skjermen),
        # sentrert over foreldrevinduet.
        self.update_idletasks()
        needed = (header.winfo_reqheight()
                  + msg_label.winfo_reqheight()
                  + btn_row.winfo_reqheight()
                  + 64)                       # marginer/paddings + scroll-ramme
        h = max(240, min(needed, int(self.winfo_screenheight() * 0.85)))
        pw, ph = parent.winfo_width(), parent.winfo_height()
        px, py = parent.winfo_x(), parent.winfo_y()
        x = px + (pw - w) // 2
        y = py + (ph - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _on_ja(self):
        self.result = "ja"
        self.destroy()

    def _on_nei(self):
        self.result = "nei"
        self.destroy()

    def _on_avbryt(self):
        self.result = "avbryt"
        self.destroy()


class _LibreOfficeWarningDialog(ctk.CTkToplevel):
    """
    Advarsel om LibreOffice før kjøring (ødelagt installasjon, eller versjon
    som henger ved gjenbruk av profil). result: "stopp" | "fortsett".
    dont_show_again: brukeren krysset av for «ikke vis igjen» (kun gjenbruk).
    """

    def __init__(self, parent, title: str, message: str, *,
                 can_continue: bool = True, allow_dont_show: bool = True):
        super().__init__(parent)
        self.result = "stopp"
        self.dont_show_again = False
        self.title(title)
        self.resizable(False, False)
        self.configure(fg_color=COLORS["bg"])
        self.grab_set()
        self.lift()
        w, wrap = 620, 620 - 64

        header = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        header.pack(side="top", fill="x")
        ctk.CTkLabel(header, text=f"  ⚠  {title}",
                     font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
                     text_color=COLORS.get("warn", COLORS["accent"]), anchor="w"
                     ).pack(side="left", padx=12, pady=10)

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(side="bottom", fill="x", padx=16, pady=14)
        btn_row.grid_columnconfigure((0, 1, 2), weight=1)
        ctk.CTkButton(btn_row, text="Stopp kjøringen",
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      text_color=COLORS["on_accent"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                      height=34, command=self._stopp
                      ).grid(row=0, column=0, padx=(0, 6), sticky="ew")
        ctk.CTkButton(btn_row, text="Åpne nedlastingsside",
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
                      border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      height=34, command=self._open_download
                      ).grid(row=0, column=1, padx=3, sticky="ew")
        if can_continue:
            ctk.CTkButton(btn_row, text="Fortsett likevel",
                          fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                          text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
                          border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=11),
                          height=34, command=self._fortsett
                          ).grid(row=0, column=2, padx=(6, 0), sticky="ew")

        self._dont_var = ctk.BooleanVar(value=False)
        if allow_dont_show and can_continue:
            ctk.CTkCheckBox(self, text="Ikke vis igjen for denne LibreOffice-installasjonen",
                            variable=self._dont_var,
                            font=ctk.CTkFont(family=FONTS["mono"], size=11),
                            text_color=COLORS["muted"]
                            ).pack(side="bottom", anchor="w", padx=18, pady=(0, 2))

        msg_frame = ctk.CTkScrollableFrame(self, fg_color="transparent")
        msg_frame.pack(side="top", fill="both", expand=True)
        msg_label = ctk.CTkLabel(msg_frame, text=message,
                                 font=ctk.CTkFont(family=FONTS["mono"], size=11),
                                 text_color=COLORS["text"], wraplength=wrap,
                                 justify="left", anchor="nw")
        msg_label.pack(padx=16, pady=(14, 8), fill="x", anchor="nw")
        self.protocol("WM_DELETE_WINDOW", self._stopp)
        self.bind("<Escape>", lambda _e: self._stopp())

        self.update_idletasks()
        needed = header.winfo_reqheight() + msg_label.winfo_reqheight() + btn_row.winfo_reqheight() + 100
        h = max(280, min(needed, int(self.winfo_screenheight() * 0.85)))
        x = parent.winfo_x() + (parent.winfo_width() - w) // 2
        y = parent.winfo_y() + (parent.winfo_height() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _open_download(self):
        import webbrowser
        from siard_workflow.core.lo_runner import DOWNLOAD_URL
        try:
            webbrowser.open(DOWNLOAD_URL)
        except Exception:
            pass

    def _stopp(self):
        self.result = "stopp"
        self.destroy()

    def _fortsett(self):
        self.result = "fortsett"
        self.dont_show_again = bool(self._dont_var.get())
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
        # Initialiser tkinterdnd2 DnD-støtte på root-vinduet
        if _HAS_TKDND:
            try:
                _TkinterDnD._require(self)
            except Exception:
                pass
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
        self._global_temp_dir: Path | None = None
        self._output_dir_override: str = ""   # satt av preflight hvis output-disk har lite plass
        self._preflight_active: bool = False
        self._preflight_lines: list = []      # (nivå, melding) — kopieres til fil-loggen
        self._pf_status: str | None = None
        self._conv_ctx = None
        self._stop_event        = threading.Event()
        self._pause_event       = threading.Event()
        self._step_resume_event = threading.Event()
        # Mobilvarsling (core/remote_notify) — satt bare mens en kjøring pågår
        self._remote_hub = None
        self._remote_rep = None
        self._step_stopped      = False
        self._abort_requested   = False
        # Valgt grunnlag per fil når temp-mappa må lages på nytt (kjøreplanen)
        self._run_plan_bases: dict = {}
        # Prosjektfil-tilstand
        self._project_file: "ProjectFile | None" = None   # type: ignore[name-defined]
        self._project_path: Path | None = None
        self._spinner_chars = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
        self._spinner_idx   = 0

        self._build_ui()
        # Last lagret font-offset og appliser
        from gui.styles import FontRegistry
        _saved_offset = get_config("font_offset") or 0
        if _saved_offset:
            FontRegistry.set_offset(int(_saved_offset))
        self._load_persistent_profiles()
        self._load_saved_temp()
        self._detect_libreoffice()
        self._init_worker_config()
        self._poll_log_queue()

        # Sjekk for oppdateringer i bakgrunnen (2 sek forsinkelse for at GUI skal være klar)
        self.after(2000, self._check_for_updates)
        # Programvare/avhengigheter (pakker + eksterne programmer) i bakgrunnen
        self.after(1200, self._startup_dependency_check)

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
            on_open_project=self._open_project,
            on_save_project=self._save_project,
            on_reset_project=self._reset_project,
        )
        self.workflow_panel.project_status_fn = self._step_project_status
        self.workflow_panel.project_action_fn = self._step_project_action
        self.workflow_panel.grid(row=1, column=0, padx=10, pady=(0,6), sticky="nsew")

        self.format_chart = FormatChartPanel(left)
        self.format_chart.grid(row=2, column=0, padx=10, pady=(0, 10), sticky="ew")
        # Skjult til det finnes formatdata — workflow-listen får høyden.
        # Panelet viser/skjuler seg selv ved update_format()/reset().
        self.format_chart.grid_remove()

        right = ctk.CTkFrame(self, fg_color="transparent")
        right.grid(row=1, column=1, padx=(6,12), pady=(0,0), sticky="nsew")
        right.grid_rowconfigure(0, weight=0)
        right.grid_rowconfigure(1, weight=0)
        right.grid_rowconfigure(2, weight=1)
        right.grid_columnconfigure(0, weight=1)

        self.ops_panel = OperationsPanel(right, on_add=self._add_operation,
                                          on_saved=self._on_settings_saved,
                                          get_workflow_ops=self.workflow_panel.get_operations)
        self.ops_panel.grid(row=0, column=0, pady=(0,8), sticky="ew")

        self.progress_panel = ProgressPanel(right)
        self.progress_panel.grid(row=1, column=0, pady=(0,8), sticky="ew")
        self.progress_panel.grid_remove()   # skjult ved oppstart; grid-info er lagret
        self.progress_panel.set_callbacks(
            pause_cb=self._conv_pause,
            stop_cb=self._conv_stop,
            resume_cb=self._conv_resume,
            abort_cb=self._conv_abort)

        # Kjøreloggen med SIARD-header-panelet til høyre (skjult til en
        # workflow starter; bredde dras i skillelinja, » minimerer til en stripe)
        log_area = ctk.CTkFrame(right, fg_color="transparent")
        log_area.grid(row=2, column=0, sticky="nsew")
        log_area.grid_rowconfigure(0, weight=1)
        log_area.grid_columnconfigure(0, weight=1)

        self.log_panel = LogPanel(log_area)
        self.log_panel.set_show_all_callback(self._on_log_show_all)
        self.log_panel.grid(row=0, column=0, sticky="nsew")

        self.header_dock = HeaderDock(log_area)
        self.header_dock.grid(row=0, column=1, sticky="ns")
        self.header_dock.grid_remove()
        self.header_dock.panel.on_confirm = self._on_header_confirm

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
            text_color=COLORS["btn_text"],
            dropdown_fg_color=COLORS["panel"], dropdown_text_color=COLORS["text"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            command=self._load_profile)
        self.profile_menu.grid(row=0, column=1, padx=4, pady=8, sticky="w")

        menu_cfg = dict(height=30, width=100, fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"], text_color=COLORS["btn_text"], border_color=COLORS["btn_border"], border_width=1,
                        font=ctk.CTkFont(family=FONTS["mono"], size=10))

        col = 2
        for label, cmd in [
            ("Endre temp-mappe",   self._browse_temp),
            #("Rapport",      self._export_report),
            ("Innstillinger",self._open_settings),
        ]:
            ctk.CTkButton(bar, text=label, command=cmd, **menu_cfg).grid(
                row=0, column=col, padx=4, pady=8)
            col += 1

        # Font-skalering A− / A+ + tema-bryter (lyspære)
        font_frm = ctk.CTkFrame(bar, fg_color="transparent")
        font_frm.grid(row=0, column=col, padx=(8, 16), pady=8)
        for txt, delta in [("A−", -1), ("A+", +1)]:
            ctk.CTkButton(
                font_frm, text=txt, width=32, height=26, corner_radius=5,
                fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"], text_color=COLORS["btn_text"], border_color=COLORS["btn_border"], border_width=1,
                font=ctk.CTkFont(family=FONTS["mono"], size=10),
                command=lambda d=delta: self._font_scale(d),
            ).pack(side="left", padx=2)
        # Lyspære: 💡 = light, 🌙 = dark (vises etter aktivt tema)
        from gui.styles import current_theme as _cur_theme
        _icon = "💡" if _cur_theme() == "dark" else "🌙"
        self._theme_btn = ctk.CTkButton(
            font_frm, text=_icon, width=32, height=26, corner_radius=5,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"], text_color=COLORS["btn_text"], border_color=COLORS["btn_border"], border_width=1,
            font=ctk.CTkFont(size=14),
            command=self._toggle_theme,
        )
        self._theme_btn.pack(side="left", padx=(8, 2))

    def _build_statusbar(self):
        """Statuslinje nederst i vinduet — temp-info, ledig disk,
        tråd/batch-innstillinger, fildeteksjons-backend, spinner."""
        sb = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=0, height=26)
        sb.grid(row=2, column=0, columnspan=2, sticky="ew")
        # Kolonner:
        # 0: spinner | 1: temp (vokser) | 2: ledig disk | 3: tråder/batch
        # 4: deteksjons-backend | 5: status-right
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

        # Separator
        _sep_font = ctk.CTkFont(family=FONTS["mono"], size=11)
        ctk.CTkLabel(sb, text="│", font=_sep_font,
                     text_color=COLORS["border"]).grid(
                         row=0, column=2, padx=2, pady=2)

        self._status_disk = ctk.CTkLabel(
            sb, text="Ledig: …",
            font=_sep_font, text_color=COLORS["muted"], anchor="e")
        self._status_disk.grid(row=0, column=3, padx=4, pady=2, sticky="e")

        ctk.CTkLabel(sb, text="│", font=_sep_font,
                     text_color=COLORS["border"]).grid(
                         row=0, column=4, padx=2, pady=2)

        self._status_workers = ctk.CTkLabel(
            sb, text="Tråder: …",
            font=_sep_font, text_color=COLORS["muted"], anchor="e")
        self._status_workers.grid(row=0, column=5, padx=4, pady=2, sticky="e")

        ctk.CTkLabel(sb, text="│", font=_sep_font,
                     text_color=COLORS["border"]).grid(
                         row=0, column=6, padx=2, pady=2)

        self._status_backend = ctk.CTkLabel(
            sb, text="Deteksjon: …",
            font=_sep_font, text_color=COLORS["muted"], anchor="e")
        self._status_backend.grid(row=0, column=7, padx=4, pady=2, sticky="e")

        ctk.CTkLabel(sb, text="│", font=_sep_font,
                     text_color=COLORS["border"]).grid(
                         row=0, column=8, padx=2, pady=2)

        self._status_right = ctk.CTkLabel(
            sb, text="",
            font=_sep_font, text_color=COLORS["muted"], anchor="e")
        self._status_right.grid(row=0, column=9, padx=(0,12), pady=2, sticky="e")

        # Oppstart: les innstillinger én gang, start disk-oppdatering-loop
        self.after(500, self._update_statusbar_settings)
        self.after(500, self._update_statusbar_disk)

    def _build_queue_panel(self, parent):
        """Drag-drop + kø-liste over SIARD-filer øverst i venstre kolonne."""
        frm = ctk.CTkFrame(parent, fg_color=COLORS["bg"], corner_radius=8,
                            border_width=1, border_color=COLORS["border"])
        frm.grid(row=0, column=0, padx=10, pady=(10, 4), sticky="ew")
        frm.grid_columnconfigure(0, weight=1)
        self._queue_panel_frm = frm

        # Tittel
        ctk.CTkLabel(frm, text="SIARD-KØ",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                     text_color=COLORS["muted"]).grid(
                         row=0, column=0, sticky="w", padx=10, pady=(4, 0))

        # Drag-drop-felt
        self._drop_zone = ctk.CTkLabel(
            frm,
            text="Dra og slipp SIARD-filer hit",
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["muted"],
            fg_color=COLORS["dropzone"],
            corner_radius=6,
            wraplength=180,
            height=38)
        self._drop_zone.grid(row=1, column=0, padx=8, pady=(2, 2), sticky="ew")
        # Oppdater wraplength én gang etter at layout er ferdig — unngår Configure-loop
        self.after(200, self._update_dropzone_wrap)

        # Prøv tkinterdnd2 først, deretter Windows shell DnD, deretter klikk-fallback
        self._dnd_active = False
        self._setup_dnd()

        # Kø-liste
        # Kompakt: kø-panelet deler venstrekolonnen med workflow-listen,
        # som skal ha mest mulig av høyden.
        _QUEUE_LIST_H = 90   # ~3 kø-rader før rulling
        self._queue_frame = ctk.CTkScrollableFrame(
            frm, fg_color="transparent", height=_QUEUE_LIST_H,
            scrollbar_button_color=COLORS["border"])
        # CTkScrollbar ber om 200 px høyde som standard, og CTkScrollableFrame
        # vokser til rullefeltets ønskede høyde uavhengig av `height`. Uten
        # dette ble kø-listen ~270 px (skalert) — sett rullefeltet eksplisitt.
        try:
            self._queue_frame._scrollbar.configure(height=_QUEUE_LIST_H)
        except Exception:
            pass
        self._queue_frame.grid(row=2, column=0, padx=4, pady=(0, 4), sticky="ew")
        self._queue_frame.grid_columnconfigure(0, weight=1)

        self._queue_rows: list[ctk.CTkFrame] = []
        self._queue_lbl_none = ctk.CTkLabel(
            self._queue_frame, text="(tom)",
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["muted"])
        self._queue_lbl_none.grid(row=0, column=0, pady=4)

        # Utvid DnD til hele kø-panelet
        if self._dnd_active:
            self._extend_dnd_to_queue()
        else:
            # Fallback: klikk på kø-rammen åpner filvelger
            self._queue_frame.bind("<Button-1>", lambda e: self._queue_add_file())

    def _setup_dnd(self):
        """Sett opp drag-and-drop. Prøver tkinterdnd2, deretter klikk-fallback."""
        # Metode 1: tkinterdnd2
        try:
            self._drop_zone.drop_target_register("DND_Files")   # type: ignore
            self._drop_zone.dnd_bind("<<Drop>>", self._on_dnd_drop)  # type: ignore
            self._drop_zone.dnd_bind("<<DropEnter>>", self._on_dnd_enter)  # type: ignore
            self._drop_zone.dnd_bind("<<DropLeave>>", self._on_dnd_leave)  # type: ignore
            self._dnd_active = True
            self._setup_click_on_drop_zone()
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
            self._setup_click_on_drop_zone()
            return
        except Exception:
            pass

        # Fallback: kun klikk
        self._setup_click_on_drop_zone()

    def _setup_click_on_drop_zone(self):
        """Klikk på drop-sonen åpner alltid filvelgeren — uavhengig av DnD-status."""
        self._drop_zone.configure(
            text="Dra og slipp eller klikk for å legge til SIARD-filer",
            cursor="hand2")
        self._drop_zone.bind("<Button-1>", lambda e: self._queue_add_file())
        self._drop_zone.bind("<Enter>",
            lambda e: self._drop_zone.configure(text_color=COLORS["accent"]))
        self._drop_zone.bind("<Leave>",
            lambda e: self._drop_zone.configure(text_color=COLORS["muted"]))

    def _extend_dnd_to_queue(self):
        """Registrer hele kø-panelet (liste-feltet) som ekstra drop-target."""
        for widget in [self._queue_panel_frm, self._queue_frame]:
            try:
                widget.drop_target_register("DND_Files")   # type: ignore
                widget.dnd_bind("<<Drop>>", self._on_dnd_drop)  # type: ignore
                widget.dnd_bind("<<DropEnter>>", self._on_dnd_enter_queue)  # type: ignore
                widget.dnd_bind("<<DropLeave>>", self._on_dnd_leave_queue)  # type: ignore
            except Exception:
                pass

    @staticmethod
    def _dnd_is_siard(event) -> bool:
        """Returner True dersom drag-event inneholder .siard- eller .zip-fil."""
        import re
        raw = getattr(event, "data", "") or ""
        paths = re.findall(r'\{([^}]+)\}|(\S+)', raw)
        return any(
            (g[0] or g[1]).strip().lower().endswith((".siard", ".zip"))
            for g in paths
        )

    # Bakgrunnsfarger for kø-panelet under drag
    _DND_BG_HOVER = "#0d1a2a"   # mørk blå — fil holdes over feltet
    _DND_BG_OK    = "#0a2016"   # mørk grønn — .siard/.zip sluppet
    _DND_BG_WARN  = "#22080a"   # mørk rød — ugyldig filtype sluppet
    _DND_BG_NONE  = COLORS["bg"]

    def _dnd_highlight(self):
        """Visuell feedback mens fil holdes over kø-panelet.
        event.data er alltid tom i <<DropEnter>>, så filtype kan ikke sjekkes her."""
        self._drop_zone.configure(fg_color=COLORS["accent_dim"],
                                   text_color=COLORS["accent"])
        self._queue_panel_frm.configure(fg_color=self._DND_BG_HOVER,
                                         border_color=COLORS["accent"], border_width=2)
        try:
            self._queue_frame.configure(fg_color=self._DND_BG_HOVER)
        except Exception:
            pass

    def _dnd_flash(self, ok: bool):
        """Kort grønn/rød-blink etter drop for å bekrefte om filen ble akseptert."""
        bg   = self._DND_BG_OK   if ok else self._DND_BG_WARN
        bord = COLORS["green"]   if ok else COLORS["red"]
        self._queue_panel_frm.configure(fg_color=bg, border_color=bord, border_width=2)
        try:
            self._queue_frame.configure(fg_color=bg)
        except Exception:
            pass
        self.after(500, self._dnd_reset)

    def _dnd_reset(self):
        """Tilbakestill all visuell DnD-feedback."""
        self._drop_zone.configure(fg_color=COLORS["dropzone"], text_color=COLORS["muted"])
        self._queue_panel_frm.configure(fg_color=self._DND_BG_NONE,
                                         border_color=COLORS["border"], border_width=1)
        try:
            self._queue_frame.configure(fg_color="transparent")
        except Exception:
            pass

    def _update_dropzone_wrap(self):
        """Sett wraplength til faktisk labelbredde. Kalles én gang etter layout."""
        try:
            w = self._drop_zone.winfo_width()
            if w > 20:
                self._drop_zone.configure(wraplength=max(60, w - 16))
        except Exception:
            pass

    def _on_dnd_enter(self, event):
        self._dnd_highlight()

    def _on_dnd_leave(self, event):
        self._dnd_reset()

    def _on_dnd_enter_queue(self, event):
        self._dnd_highlight()

    def _on_dnd_leave_queue(self, event):
        self._dnd_reset()

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

    def _update_statusbar_disk(self) -> None:
        """
        Oppdater ledig diskplass på tempdisk. Kjøres jevnlig (hvert 30 sek)
        via `after()`-loop.
        """
        try:
            import shutil as _shutil
            import tempfile as _tempfile
            from pathlib import Path as _Path
            temp_path = (self._global_temp_dir
                         if self._global_temp_dir
                         else _Path(_tempfile.gettempdir()))
            try:
                free = _shutil.disk_usage(str(temp_path)).free
                # Formatér til lesbar enhet
                for unit, factor in (("TB", 1024**4), ("GB", 1024**3),
                                     ("MB", 1024**2), ("kB", 1024)):
                    if free >= factor:
                        free_str = f"{free / factor:.1f} {unit}"
                        break
                else:
                    free_str = f"{free} B"
                # Fargekoding: rødt under 10 GB, gult under 50 GB, ellers normal
                if free < 10 * 1024**3:
                    color = COLORS["red"]
                elif free < 50 * 1024**3:
                    color = "#e0a040"
                else:
                    color = COLORS["text"]
                self._status_disk.configure(
                    text=f"Ledig: {free_str}", text_color=color)
            except Exception:
                self._status_disk.configure(
                    text="Ledig: (ukjent)", text_color=COLORS["muted"])
        except Exception:
            pass
        # Planlegg neste disk-oppdatering om 30 sek
        try:
            self.after(30000, self._update_statusbar_disk)
        except Exception:
            pass

    def _update_statusbar_settings(self) -> None:
        """
        Oppdater statusbar-felter som avhenger av globale innstillinger:
        antall tråder + batch-størrelse, og deteksjons-backend.
        Kalles ved oppstart og når global settings lagres
        (fra `_on_global_settings_saved`).
        """
        try:
            from settings import get_config as _get_cfg
            try:
                workers = int(_get_cfg("max_workers", 4) or 4)
            except Exception:
                workers = 4
            try:
                batch = int(_get_cfg("lo_batch_size", 50) or 50)
            except Exception:
                batch = 50
            self._status_workers.configure(
                text=f"Tråder: {workers} (batch {batch})",
                text_color=COLORS["text"])

            use_sf = False
            try:
                use_sf = bool(_get_cfg("use_siegfried", False))
            except Exception:
                pass
            if use_sf:
                try:
                    from siard_workflow.core.identifiers import installer as _sf_inst
                    if _sf_inst.is_installed():
                        self._status_backend.configure(
                            text="Deteksjon: Siegfried/PRONOM",
                            text_color=COLORS["accent"])
                    else:
                        self._status_backend.configure(
                            text="Deteksjon: Magic-bytes (SF ikke installert)",
                            text_color="#e0a040")
                except Exception:
                    self._status_backend.configure(
                        text="Deteksjon: Siegfried/PRONOM",
                        text_color=COLORS["accent"])
            else:
                self._status_backend.configure(
                    text="Deteksjon: Magic-bytes",
                    text_color=COLORS["muted"])
        except Exception:
            pass

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
        raw = event.data
        import re
        # tkinterdnd2: {sti med mellomrom} eller sti\nsti
        paths = re.findall(r'\{([^}]+)\}|(\S+)', raw)
        accepted = False
        for grp in paths:
            p = (grp[0] or grp[1]).strip()
            if p:
                path = Path(p)
                if path.suffix.lower() in (".siard", ".zip") and path.exists():
                    self._queue_push(path)
                    accepted = True
                elif path.exists():
                    self._log(f"Ignorert: {path.name} (kun .siard/.zip aksepteres)", "warn")
        self._dnd_flash(accepted)

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
            self._clear_project_state()

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
                          fg_color=COLORS["danger_bg"], hover_color=COLORS["danger_hover"], text_color=COLORS["danger_text"], border_color=COLORS["danger_border"], border_width=1,
                          font=ctk.CTkFont(size=11),
                          command=_rm).grid(row=0, column=1, padx=(0,4), pady=3)

            self._queue_rows.append(row)

        self._update_status_right(
            f"Kø: {len(self.siard_queue)} fil(er)")

    def _set_active_file(self, path: Path):
        """Sett aktiv SIARD-fil (brukes av workflow og logging)."""
        self.siard_path = path
        set_current_siard_path(path)
        if self._global_temp_dir is None:
            self._auto_select_temp(path)
        self.workflow_panel.set_file(path)
        self._sync_project_to_siard(path)

    def _sync_project_to_siard(self, path: Path) -> None:
        """Synkroniser prosjektfil-tilstand med ny aktiv SIARD.

        Allerede riktig prosjekt: ingen endring.
        Matchende .siardwf ved siden av ny fil: auto-last.
        Ingen match: tøm prosjektstatus.
        """
        # Allerede riktig prosjekt?
        if (self._project_file
                and Path(self._project_file.source_siard) == path):
            return

        # Finnes det en .siardwf med samme navn ved siden av SIARD-filen?
        auto_path = path.with_suffix(".siardwf")
        if auto_path.exists():
            try:
                from siard_workflow.core.project_file import ProjectFile
                pf = ProjectFile.load(auto_path)
                if Path(pf.source_siard) == path:
                    self._project_file = pf
                    self._project_path = auto_path
                    self.workflow_panel.set_project_label(
                        f"Prosjekt: {auto_path.name}")
                    # Prosjektets lagrede verdier gjelder (ikke panelets)
                    _apply_checkpoint_params(
                        list(self.workflow_panel.get_operations()), pf.operations)
                    # Oppdater statusikoner (posisjonelt — samme steg kan
                    # forekomme flere ganger, f.eks. SHA-256 først og sist)
                    self._refresh_step_statuses()
                    self._log(
                        f"Prosjektfil lastet: {auto_path.name}  "
                        f"({pf.completed_count}/{len(pf.operations)} steg fullført)",
                        "info")
                    return
            except Exception:
                pass

        # Ingen matchende prosjektfil — tøm prosjektstatus
        self._clear_project_state()

    def _reset_project(self) -> None:
        """Nullstill alle operasjoner i prosjektfilen til 'pending' og start på nytt."""
        from tkinter import messagebox
        if self._project_file is None:
            messagebox.showinfo(
                "Ingen prosjektfil",
                "Ingen prosjektfil er lastet. Kjør workflowen for å opprette en.")
            return
        svar = messagebox.askyesno(
            "Start fra steg 1",
            f"Nullstill alle steg i '{self._project_path.name}' og "
            f"start workflowen helt fra begynnelsen?\n\n"
            f"Alle registrerte resultater vil bli fjernet fra prosjektfilen, "
            f"men output-filer på disk beholdes.")
        if not svar:
            return
        for cp in self._project_file.operations:
            self._project_file.reset(cp, "hele arbeidsflyten startes på nytt")
            cp.output_siard = None
            cp.ctx_data     = {}
        for cp in self._project_file.operations:
            cp.attempts = []
            cp.reset_reason = None
        self._project_file.answers = {}
        self._project_file.workdir = {}
        self._project_file.format_counts = {}
        self._project_file.runs = []
        self._project_file.failure_files = []
        _prog = self._forget_blob_progress(self._project_file)
        if _prog:
            self._log(f"  Blob-sjekkpunkt{_prog}", "muted")
        try:
            self._project_file.save(self._project_path)
        except Exception as exc:
            messagebox.showerror("Lagringsfeil", str(exc))
            return
        self.workflow_panel.clear_statuses()
        # Slett blob-resume-sjekkpunkt slik at neste kjøring starter fra steg 1
        src = Path(self._project_file.source_siard) if self._project_file.source_siard else None
        if src:
            from siard_workflow.operations.blob_convert_operation import (
                _resume_json_path, _sf_cache_path)
            log_dir = resolve_log_dir(src)
            for rjp in (_resume_json_path(log_dir, src.stem),
                        _sf_cache_path(log_dir, src.stem)):
                if rjp.exists():
                    try:
                        rjp.unlink()
                        self._log(f"  Blob-sjekkpunkt slettet: {rjp.name}", "muted")
                    except Exception:
                        pass
        self._log(f"Prosjekt nullstilt: {self._project_path.name} — alle steg kjøres på nytt", "info")

    def _clear_project_state(self) -> None:
        """Tøm lastet prosjektfil og nullstill statusikoner."""
        if self._project_file is not None or self._project_path is not None:
            self._project_file = None
            self._project_path = None
            self.workflow_panel.set_project_label("")
            self.workflow_panel.clear_statuses()

    # ─── Kjøreplan og tilbakestilling av enkeltsteg ──────────────────────────

    def _confirm_run_plan(self, ops: list):
        """Vis kjøreplanen når det aktive prosjektet er kjørt før.

        Returnerer None (avbrutt), False (ny kjøring — ingen plan å vise) eller
        {"unpack": bool, "blob": bool} (fortsatt kjøring: hvilke tunge steg
        som skal kjøres, så preflight kan hoppe over det som ikke trengs).
        """
        import copy
        from types import SimpleNamespace
        from siard_workflow.core.run_plan import (
            BASE_ORIGINAL, BASE_OUTPUT, build_run_plan)
        pf = self._project_file
        if pf is None or not pf.has_history or not self.siard_path \
                or Path(pf.source_siard) != Path(self.siard_path):
            return False
        cps = _sync_checkpoints_to_ops(copy.deepcopy(pf.operations), ops)
        view = SimpleNamespace(operations=cps, workdir=pf.workdir)
        workdir = self._project_workdir(view, ops)
        out = self._project_output(view, ops)
        plan = build_run_plan(ops, cps, workdir is not None, out)
        if plan.nothing_to_do:
            from tkinter import messagebox
            messagebox.showinfo(
                "Alt er fullført",
                "Alle stegene i prosjektet er fullført.\n\n"
                "Høyreklikk et steg og velg «Kjør dette steget på nytt» for å "
                "kjøre det igjen, eller bruk ↻ for å starte hele arbeidsflyten "
                "på nytt.", parent=self)
            return None

        if (not plan.has_history_skips and not plan.base_choice
                and not any(s.action == "rerun" for s in plan.steps)):
            return False          # alt kjøres uansett — ingen plan å vise

        def _count(p):
            return sum(1 for s in p.steps if s.runs)

        msg = ("Prosjektet er kjørt før. Fullførte steg hoppes over, og bare "
               "det som gjenstår kjøres:\n\n" + "\n".join(plan.lines()))
        if plan.base_choice:
            alt = BASE_OUTPUT if plan.base_suggestion == BASE_ORIGINAL else BASE_ORIGINAL
            p_alt = build_run_plan(ops, cps, False, out, alt)
            p_org = plan if plan.base == BASE_ORIGINAL else p_alt
            p_out = plan if plan.base == BASE_OUTPUT else p_alt
            msg += ("\n\nTemp-mappa fra forrige kjøring er borte. Hva skal stegene "
                    "bygge på?\n"
                    f"  • Original-SIARD: alle innholdssteg kjøres på nytt "
                    f"({_count(p_org)} steg)\n"
                    f"  • Siste resultat ({Path(out).name}): bare stegene som må "
                    f"kjøres ({_count(p_out)} steg)\n\n"
                    f"Forslag: {'original-SIARD' if plan.base_suggestion == BASE_ORIGINAL else 'siste resultat'}"
                    f" — {plan.base_reason}.")
            if plan.base_suggestion == BASE_OUTPUT:
                msg += "\n\nPlanen over viser forslaget."
            org_txt = "Original-SIARD" + (" (anbefalt)" if plan.base_suggestion == BASE_ORIGINAL else "")
            out_txt = "Siste resultat" + (" (anbefalt)" if plan.base_suggestion == BASE_OUTPUT else "")
            dlg = _PipelineSuggestionDialog(self, msg, heading="Kjøreplan",
                                            yes_text=org_txt, no_text=out_txt, cancel=True)
            dlg.title("Kjøreplan")
            self.wait_window(dlg)
            if dlg.result == "avbryt":
                self._log("Kjøringen startes ikke.", "warn")
                return None
            chosen = BASE_ORIGINAL if dlg.result == "ja" else BASE_OUTPUT
            self._run_plan_bases[str(self.siard_path)] = chosen
            plan = build_run_plan(ops, cps, False, out, chosen)
        else:
            dlg = _PipelineSuggestionDialog(self, msg, heading="Kjøreplan",
                                            yes_text="Kjør", no_text="Avbryt",
                                            cancel=False)
            dlg.title("Kjøreplan")
            self.wait_window(dlg)
            if dlg.result != "ja":
                self._log("Kjøringen startes ikke.", "warn")
                return None
        runs = {s.op_id for s in plan.steps if s.runs}
        return {"unpack": "unpack_siard" in runs, "blob": "blob_convert" in runs}

    def _project_checkpoints_for_panel(self) -> list:
        """Prosjektets checkpoints posisjonelt synket mot panelets steg (kopi)."""
        import copy
        if self._project_file is None:
            return []
        return _sync_checkpoints_to_ops(copy.deepcopy(self._project_file.operations),
                                        list(self.workflow_panel.get_operations()))

    def _refresh_step_statuses(self) -> None:
        self.workflow_panel.clear_statuses()
        for i, cp in enumerate(self._project_checkpoints_for_panel()):
            if cp.status != "pending" or cp.reset_reason:
                self.workflow_panel.set_step_status(
                    i, "reset" if cp.status == "pending" else cp.status)

    def _step_project_action(self, index: int, action: str) -> None:
        """Høyreklikk på et steg: «reset» (kjør på nytt), «skip» (hopp over i
        neste kjøring) eller «unskip» (ta med igjen)."""
        from tkinter import messagebox
        if self._running:
            messagebox.showinfo("Kjøring pågår",
                                "Vent til kjøringen er ferdig.", parent=self)
            return
        if self._project_file is None:
            messagebox.showinfo(
                "Ingen prosjektfil",
                "Arbeidsflyten er ikke kjørt for denne fila ennå — alle steg "
                "kjøres uansett.", parent=self)
            return
        ops = list(self.workflow_panel.get_operations())
        if not (0 <= index < len(ops)):
            return
        pf = self._project_file
        pf.operations = _sync_checkpoints_to_ops(pf.operations, ops)
        cp = pf.operations[index]
        op = ops[index]
        if action == "reset":
            pf.reset(cp, "tilbakestilt av bruker")
            # Stegets lagrede svar gjelder ikke lenger
            pf.forget_answers(f"{op.operation_id}:")
            extra = ""
            if op.operation_id == "blob_convert":
                extra = self._forget_blob_progress(pf)
            self._log(f"«{op.label}» kjøres på nytt ved neste kjøring{extra}", "info")
        elif action == "skip":
            cp.status = "user_skip"
            self._log(f"«{op.label}» hoppes over i neste kjøring", "info")
        elif action == "unskip":
            pf.reset(cp, "tatt med igjen")
            self._log(f"«{op.label}» er tatt med igjen", "info")
        try:
            pf.save(self._project_path)
        except Exception as exc:
            messagebox.showerror("Lagringsfeil", str(exc), parent=self)
            return
        self._refresh_step_statuses()

    def _forget_blob_progress(self, pf) -> str:
        """Slett BLOB-progresjonen, så steget starter fra begynnelsen."""
        from siard_workflow.core.blob_progress import progress_path
        src = Path(pf.source_siard) if pf.source_siard else None
        if src is None:
            return ""
        log_dir = resolve_log_dir(src)
        removed = []
        stems = {src.stem}
        for cp in pf.operations:
            if cp.operation_id == "repack_siard" and cp.output_siard:
                stems.add(Path(cp.output_siard).stem)
        for stem in stems:
            for p in (progress_path(log_dir, stem),):
                if p.exists():
                    try:
                        p.unlink()
                        removed.append(p.name)
                    except Exception:
                        pass
        return f" (progresjon slettet: {', '.join(removed)})" if removed else ""

    def _rename_project_for(self, old: Path, new: Path) -> None:
        """SIARD-fila har fått nytt navn: flytt <navn>.siardwf og oppdater kilden."""
        from siard_workflow.core.project_file import ProjectFile
        old_pf, new_pf = old.with_suffix(".siardwf"), new.with_suffix(".siardwf")
        try:
            if old_pf.exists() and not new_pf.exists():
                pf = ProjectFile.load(old_pf)
                pf.source_siard = str(new)
                pf.save(new_pf)
                old_pf.unlink()
                if self._project_path == old_pf:
                    self._project_file, self._project_path = pf, new_pf
                    self.workflow_panel.set_project_label(f"Prosjekt: {new_pf.name}")
        except Exception as exc:
            self._log(f"    Prosjektfila kunne ikke få nytt navn: {exc}", "warn")

    def _step_project_status(self, index: int) -> str:
        """Status for steg nr. `index` i prosjektet (til høyreklikkmenyen)."""
        cps = self._project_checkpoints_for_panel()
        if 0 <= index < len(cps):
            cp = cps[index]
            return "reset" if cp.status == "pending" and cp.reset_reason else cp.status
        return ""

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
        """Auto-detekter LibreOffice ved oppstart og lagre sti til config.json.

        Finnes ikke LibreOffice på noen av de vanlige stedene, blir brukeren
        bedt om å peke ut installasjonsmappen — og valget verifiseres før det
        lagres.
        """
        try:
            from siard_workflow.core.libreoffice import (
                find_libreoffice, verify_libreoffice_path)
            saved = get_config("lo_executable", "")
            if saved:
                # Verifiser at lagret sti faktisk peker på soffice på DENNE
                # maskinen — config.json kan stamme fra en annen maskin.
                verified = verify_libreoffice_path(saved)
                if verified:
                    if verified != saved:
                        set_config("lo_executable", verified)  # normaliser
                    return
                # Stale/ugyldig sti — nullstill og prøv å finne på nytt
                set_config("lo_executable", "")
            found = find_libreoffice("")
            if found:
                set_config("lo_executable", found)
                return
            # Ikke funnet automatisk: vises i oppstartsoversikten over manglende
            # programvare (_startup_dependency_check), med «Pek ut …» og «Last ned»
        except Exception:
            pass

    def _prompt_for_libreoffice(self, ask: bool = True):
        """Be bruker velge LibreOffice program-mappe og verifiser valget.

        Verifiseringen (siard_workflow.core.libreoffice.verify_libreoffice_path)
        godtar både selve soffice-binæren, program-mappen, installasjonsroten
        og en macOS .app-pakke. Lagrer verifisert soffice-sti til config.json.
        """
        from tkinter import filedialog, messagebox
        from siard_workflow.core.libreoffice import verify_libreoffice_path

        if ask and not messagebox.askyesno(
                "LibreOffice ikke funnet",
                "LibreOffice ble ikke funnet automatisk.\n\n"
                "LibreOffice kreves for å konvertere dokumenter til PDF/A.\n\n"
                "Vil du peke ut LibreOffice-mappen manuelt nå?",
                parent=self):
            return

        while True:
            chosen = filedialog.askdirectory(
                title="Velg LibreOffice-mappe (installasjonsrot eller program-mappe)",
                parent=self,
            )
            if not chosen:
                return  # avbrutt av bruker
            verified = verify_libreoffice_path(chosen)
            if verified:
                set_config("lo_executable", verified)
                messagebox.showinfo(
                    "LibreOffice verifisert",
                    f"LibreOffice ble funnet og lagret:\n{verified}",
                    parent=self)
                return
            if not messagebox.askretrycancel(
                    "Ugyldig mappe",
                    "Fant ingen soffice-binær i den valgte mappen.\n\n"
                    "Velg installasjonsmappen for LibreOffice — typisk\n"
                    r"C:\Program Files\LibreOffice  (eller undermappen \program)."
                    "\n\nPrøv igjen?",
                    parent=self):
                return

    # ─── Programvare / avhengigheter ─────────────────────────────────────────

    def _startup_dependency_check(self):
        """
        Sjekk Python-pakker og eksterne programmer i bakgrunnen etter oppstart
        (se dependencies.py). Status logges alltid. Mangler noe som vanlig
        installasjon skal ha, vises en oversikt over hva som ikke virker, med
        installasjon etter bekreftelse — med mindre brukeren har valgt «ikke vis
        igjen» for akkurat dette settet av mangler.
        """
        import threading
        import dependencies as _deps
        box: dict = {}

        def _work():
            try:
                box["r"] = _deps.check_all()
            except Exception as exc:
                box["err"] = exc
        t = threading.Thread(target=_work, daemon=True)
        t.start()

        def _poll():
            try:
                if t.is_alive():
                    self.after(250, _poll)
                    return
                if "err" in box:
                    self._log(f"Programvare-sjekk feilet: {box['err']}", "warn")
                    return
                statuses = box.get("r") or []
                self._log_dependency_summary(statuses)
                problems = _deps.startup_problems(statuses)
                if not problems or self._running or self._preflight_active:
                    return
                if _deps.signature(problems) == get_config("deps_ack_signature", ""):
                    return
                from gui.dependencies_dialog import DependenciesDialog
                dlg = DependenciesDialog(self, statuses, mode="startup")
                self.wait_window(dlg)
                if dlg.dont_show_again:
                    set_config("deps_ack_signature",
                               _deps.signature(_deps.startup_problems(dlg.statuses)))
            except Exception:
                pass            # f.eks. vinduet er lukket før sjekken ble ferdig
        self.after(250, _poll)

    def _log_dependency_summary(self, statuses: list) -> None:
        import dependencies as _deps
        missing = [s for s in statuses if s.missing]
        n_ok = sum(1 for s in statuses if s.ok and s.in_use)
        if not missing:
            self._log(f"Programvare: alt på plass ({n_ok} komponenter)", "ok")
            return
        std = [s for s in missing if s.level != _deps.OPTIONAL]
        self._log(f"Programvare: {n_ok} på plass, {len(std)} standard og "
                  f"{len(missing) - len(std)} valgfri(e) mangler "
                  f"(oversikt: Innstillinger → Programvare og avhengigheter)",
                  "warn" if std else "muted")
        for s in missing:
            opt = s.level == _deps.OPTIONAL
            self._log(f"  {'•' if opt else '✗'} {s.label}: {s.detail} — uten: {s.affects}",
                      "muted" if opt else "warn")

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
        elif op_id == "dias_package":
            # Bare felt krysset av for «Husk» lagres i programmets innstillinger
            self._log(f"  DIAS: felt som huskes til neste kjøring → {settings_path}",
                      "success")
            self._log("    " + (", ".join(f"{k}={v}" for k, v in params.items())
                                or "(ingen felt er krysset av for «Husk»)"), "info")
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

    # ─── Prosjektfil ──────────────────────────────────────────────────────────

    def _save_project(self):
        from tkinter import filedialog, messagebox
        from siard_workflow.core.project_file import ProjectFile
        ops = list(self.workflow_panel.get_operations())
        if not ops:
            messagebox.showinfo("Tom workflow", "Legg til operasjoner først.")
            return
        src = self.siard_path or (self.siard_queue[0] if self.siard_queue else None)
        if not src:
            messagebox.showinfo("Ingen fil", "Legg til en SIARD-fil i køen først.")
            return
        # Bygg alltid operasjonslista fra gjeldende workflow-panel. Hvis det
        # finnes en lastet prosjektfil, behold checkpoint-status for steg som
        # matcher (samme operation_id) — nye steg legges til som pending, og
        # fjernede steg forsvinner.
        if self._project_file is None:
            pf = ProjectFile.from_ops(src, ops)
        else:
            pf = self._project_file
            pf.source_siard = str(src)
            pf.operations = _sync_checkpoints_to_ops(pf.operations, ops)
        idir = str(self._project_path.parent) if self._project_path else str(src.parent)
        ifile = (self._project_path.name
                 if self._project_path
                 else src.stem + ".siardwf")
        sp = filedialog.asksaveasfilename(
            title="Lagre prosjektfil",
            initialdir=idir,
            initialfile=ifile,
            defaultextension=".siardwf",
            filetypes=[("SIARD Workflow prosjekt", "*.siardwf"), ("Alle", "*.*")])
        if not sp:
            return
        try:
            pf.save(Path(sp))
            self._project_file = pf
            self._project_path = Path(sp)
            self.workflow_panel.set_project_label(f"Prosjekt: {Path(sp).name}")
            self._log(f"Prosjektfil lagret: {sp}", "success")
        except Exception as exc:
            messagebox.showerror("Lagringsfeil", str(exc))

    def _open_project(self):
        from tkinter import filedialog, messagebox
        from siard_workflow.core.project_file import ProjectFile
        from gui.operations_panel import OP_DEFS
        sp = filedialog.askopenfilename(
            title="Åpne prosjektfil",
            filetypes=[("SIARD Workflow prosjekt", "*.siardwf"), ("Alle", "*.*")])
        if not sp:
            return
        try:
            pf = ProjectFile.load(Path(sp))
        except Exception as exc:
            messagebox.showerror("Åpningsfeil", f"Kunne ikke lese prosjektfil:\n{exc}")
            return

        # ── Gjenoppbygg workflow-panel fra prosjektfil ────────────────────────
        self.workflow_panel.clear()
        missing = []
        for cp in pf.operations:
            op_def = next((d for d in OP_DEFS
                           if d["cls"].operation_id == cp.operation_id), None)
            if op_def is None:
                missing.append(cp.operation_id)
                continue
            op = op_def["cls"](**cp.params)
            self.workflow_panel.add_operation(op)

        if missing:
            messagebox.showwarning(
                "Ukjente operasjoner",
                "Følgende operasjoner i prosjektfilen ble ikke gjenkjent og hoppet over:\n"
                + "\n".join(f"  • {m}" for m in missing))

        # ── Legg til kilde-SIARD i køen ───────────────────────────────────────
        src = Path(pf.source_siard) if pf.source_siard else None
        if src and src.exists():
            if src not in self.siard_queue:
                self._queue_push(src)
            self._set_active_file(src)
        elif src:
            messagebox.showwarning(
                "Kildefil mangler",
                f"Kilde-SIARD fra prosjektfilen finnes ikke:\n{src}\n\n"
                "Legg til riktig SIARD-fil i køen manuelt.")

        self._project_file = pf
        self._project_path = Path(sp)
        self.workflow_panel.set_project_label(f"Prosjekt: {Path(sp).name}")
        # Statusikoner (posisjonelt)
        self._refresh_step_statuses()

        done = pf.completed_count
        total = len(pf.operations)
        self._log(f"Prosjekt åpnet: {Path(sp).name}  ({done}/{total} steg fullført)", "success")

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

    # ─── Batch ────────────────────────────────────────────────────────────────

    def _open_settings(self):
        """Åpne global innstillinger-dialog."""
        SettingsDialog(self, on_save=self._on_global_settings_saved)

    def _on_global_settings_saved(self, cfg: dict):
        """Kalles når global innstillinger lagres."""
        from settings import save_config
        save_config(cfg)
        # Reset cached fildeteksjons-backend slik at endret valg tar effekt
        try:
            from siard_workflow.core.file_identifier import reset_identifier
            reset_identifier()
        except Exception:
            pass
        # Reflekter endringer i statusbar umiddelbart.
        # _update_statusbar_disk() oppdaterer også loop-en — men en
        # ekstra rask oppdatering her gjør at brukeren ser endring av
        # temp-mappe med en gang.
        try:
            self._update_statusbar_settings()
        except Exception:
            pass
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
        if op.operation_id == "siardmapper" and self._place_before_blob_convert(op):
            return
        self.workflow_panel.add_operation(op)
        self._log(f"Lagt til: {op.label}", "info")

    def _place_before_blob_convert(self, op) -> bool:
        """
        SIARDMapper venter på operatøren (redigeringsdialog). Står den etter en
        BLOB-konvertering som går over natta eller helga, står hele kjøringen
        stille til noen svarer. Foreslå å legge den før BLOB-konverteringen.
        True = plassert (før BLOB-konverteringen), False = legg sist som vanlig.
        """
        ops = list(self.workflow_panel.get_operations())
        blob = next((o for o in ops if o.operation_id == "blob_convert"), None)
        if blob is None:
            return False
        dialog = _PipelineSuggestionDialog(
            self,
            f"«{op.label}» venter på deg i en dialog for beskrivelser som mangler.\n\n"
            f"Står den etter «{blob.label}», som kan ta mange timer (for eksempel "
            "over helga), blir kjøringen stående og vente til noen svarer.\n\n"
            f"Anbefalt: legg den før «{blob.label}», så er spørsmålet besvart før "
            "den lange konverteringen starter.",
            heading="Plassering av SIARDMapper",
            yes_text="Før BLOB-konvertering (anbefalt)",
            no_text="Sist i arbeidsflyten",
            cancel=False)
        dialog.title("SIARDMapper")
        self.wait_window(dialog)
        if dialog.result != "ja":
            return False
        self.workflow_panel.insert_operation_before(op, "blob_convert")
        self._log(f"Lagt til: {op.label} — før «{blob.label}» "
                  f"(så kjøringen ikke venter på svar etter konverteringen)", "info")
        return True

    # ─── Kjoring ──────────────────────────────────────────────────────────────

    def _pf(self, status: str) -> None:
        """Sett resultattekst for gjeldende preflight-steg (vises i loggen)."""
        self._pf_status = status

    @staticmethod
    def _ops_signature(ops: list) -> list:
        return [(op.operation_id, sorted((k, repr(v)) for k, v in op.params.items()))
                for op in ops]

    def _run_preflight_step(self, label: str, fn, ops: list) -> bool:
        """
        Kjør ett preflight-steg med tilbakemelding i logg og statuslinje:
        «▸ <steg> …» før, og «<resultat> (t s)» etter. Resultatet settes av
        steget via _pf(); uten det brukes «ingen funn» (True) / «avbrutt»
        (False), eller «arbeidsflyten oppdatert» hvis operasjonene endret seg.
        """
        self._pf_status = None
        self._log(f"  ▸ {label} …", "info")
        self._update_status_right(f"Preflight: {label} …")
        self.update_idletasks()
        before = self._ops_signature(ops)
        t0 = time.monotonic()
        try:
            ok = bool(fn(ops))
        except Exception as exc:
            self._log(f"    FEIL i preflight «{label}»: {exc} — hopper over steget", "error")
            return True
        after = self._ops_signature(list(self.workflow_panel.get_operations()))
        status = self._pf_status
        if status is None:
            if not ok:
                status = "avbrutt av operatør"
            elif after != before:
                status = "funn — arbeidsflyten oppdatert (se over)"
            else:
                status = "ingen funn"
        elif ok and after != before and "lagt til" not in status and "slått på" not in status:
            status += " — arbeidsflyten oppdatert"
        lvl = "muted" if ok and status.startswith(("ingen funn", "ikke aktuelt", "allerede")) \
              else ("ok" if ok else "warn")
        self._log(f"    {status} ({time.monotonic() - t0:.1f} s)", lvl)
        return ok

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
            self._pf("ikke aktuelt (ingen operasjoner krever utpakking)")
            return True   # ingen operasjoner krever utpakking

        has_unpack = any(isinstance(op, UnpackSiardOperation) for op in ops)
        has_repack = any(isinstance(op, RepackSiardOperation) for op in ops)

        if has_unpack and has_repack:
            self._pf("allerede på plass (Pakk ut + Pakk sammen)")
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
            self._pf("operasjoner lagt til — klikk Kjør på nytt")
            return False   # stopp denne kjøringen; bruker klikker Kjør på nytt

        if dialog.result == "nei":
            # Fortsett uten pipeline-operasjoner (gammel ZIP-modus)
            self._pf("operatøren fortsetter i ZIP-modus uten pipeline")
            return True

        # Avbrutt
        self._pf("avbrutt av operatør")
        return False

    def _lobfolder_preflight(self, ops: list) -> bool:
        """
        Skanner SIARD-filen(e) for lobFolder-inkompatibiliteter (SCFC→DBPTK).
        Viser dialog og tilbyr å sette inn LobFolderFixOperation automatisk
        rett etter UnpackSiardOperation.

        Returnerer True = fortsett kjøring, False = avbryt.
        """
        from siard_workflow.operations import UnpackSiardOperation
        from siard_workflow.operations.lobfolder_fix_operation import (
            LobFolderFixOperation, scan_lobfolder_issues)

        # Kun relevant i pipeline-modus
        if not any(isinstance(op, UnpackSiardOperation) for op in ops):
            self._pf("ikke aktuelt (krever «Pakk ut SIARD»)")
            return True

        # Allerede lagt til — ikke spør igjen
        if any(isinstance(op, LobFolderFixOperation) for op in ops):
            self._pf(f"«Korriger lobFolder» allerede i arbeidsflyten")
            return True

        # Rask skanning av SIARD-filene i køen
        all_issues: dict = {}
        for path in self.siard_queue:
            found = scan_lobfolder_issues(path)
            if found:
                all_issues[path] = found

        if not all_issues:
            return True
        self._pf(f"{sum(len(v) for v in all_issues.values())} funn i "
                 f"{len(all_issues)} fil(er) — spør operatøren")

        lines = [
            "lobFolder-inkompatibiliteter ble oppdaget i SIARD-filen(e).\n",
            "Dette er et kjent problem med SCFC-uttrekk (se issue #1 / DBPTK #749)",
            "som gjør at LOBs ikke lar seg indeksere i DBPTK.\n",
        ]
        for path, issues in all_issues.items():
            lines.append(f"  {path.name}:")
            for issue in issues:
                lines.append(f"    • {issue}")
        lines += [
            "",
            "Vil du legge til «Rett lobFolder» automatisk etter «Pakk ut SIARD»?",
        ]

        dialog = _PipelineSuggestionDialog(self, "\n".join(lines))
        dialog.title("lobFolder-problemer oppdaget")
        self.wait_window(dialog)

        if dialog.result == "ja":
            self.workflow_panel.insert_operation_after(
                LobFolderFixOperation(), "unpack_siard")
            self._log(
                "«Korriger lobFolder (SCFC→DBPTK)» lagt til etter «Pakk ut SIARD»",
                "ok")
            self._pf("funn — «Korriger lobFolder» lagt til")
            return True  # ops re-hentes av kalleren

        if dialog.result == "nei":
            self._pf((self._pf_status or "funn") + " → operatøren fortsetter uten retting")
            return True  # operatøren avslo — fortsett uten

        self._pf("avbrutt av operatør")
        return False  # avbrutt

    def _lob_fullpath_preflight(self, ops: list) -> bool:
        """
        Filreferansene (file=) kan tilpasses ENTEN KDRS Søk & Vis ELLER
        DBPTK-validering — ikke begge:

          * KDRS Søk & Vis (standard): filnavn i file= («recordN.txt»).
            DBPTK-validatoren (A_M_5.6-1-2) limer "content/" + lobFolder + file
            uten skilletegn og melder «not found external lob» for hver LOB.
          * DBPTK-validering: full sti i file= (content/schemaN/tableM/lobK/…).
            Da finner KDRS Søk & Vis ikke filene.

        Er «Korriger lobFolder» med i arbeidsflyten, avgjør innstillingen dens
        (ingen dialog). Ellers spør vi bare når stikkprøven viser referanser
        DBPTK-validatoren ikke finner — med KDRS Søk & Vis som anbefalt valg.

        Returnerer True = fortsett kjøring, False = avbryt.
        """
        from siard_workflow.operations import UnpackSiardOperation
        from siard_workflow.operations.lobfolder_fix_operation import (
            REF_DBPTK, REF_KDRS, LobFolderFixOperation, scan_lob_ref_issues)

        # Kun relevant i pipeline-modus
        if not any(isinstance(op, UnpackSiardOperation) for op in ops):
            self._pf("ikke aktuelt (krever «Pakk ut SIARD»)")
            return True

        existing = next((op for op in ops if isinstance(op, LobFolderFixOperation)), None)
        if existing is not None:
            if not existing.use_full_paths():
                self._pf(f"«Korriger lobFolder»: {REF_KDRS} valgt — filnavn beholdes")
                return True
            ids = [o.operation_id for o in ops]
            pos = ops.index(existing)
            later = {"hex_extract", "blob_convert", "anonymize", "standardize_ext"}
            if any(i in later for i in ids[pos + 1:]) and "repack_siard" in ids:
                self.workflow_panel.move_operation_before(existing, "repack_siard")
                self._log("«Korriger lobFolder» med DBPTK-validering (full sti) flyttet "
                          "rett før «Pakk sammen SIARD» (må kjøre etter HEX/BLOB)", "ok")
                self._pf(f"«Korriger lobFolder»: {REF_DBPTK} — flyttet sist")
                return True
            self._pf(f"«Korriger lobFolder»: {REF_DBPTK} valgt")
            return True

        hits: dict = {}
        n_missing = 0
        for path in self.siard_queue:
            lr = scan_lob_ref_issues(path)
            if lr and lr.get("fixable"):
                hits[path] = lr
            n_missing += (lr or {}).get("missing", 0)
        if not hits:
            if n_missing:
                self._pf(f"ingen rettbare funn; {n_missing:,} referanser til filer "
                         f"som mangler (se «XML-validering»)")
            return True
        self._pf(f"{sum(lr['fixable'] for lr in hits.values()):,} referanser med bare "
                 f"filnavn (stikkprøve) — operatøren velger KDRS Søk & Vis eller DBPTK")

        lines = [
            "Filreferansene (file=) kan tilpasses ENTEN KDRS Søk & Vis ELLER "
            "DBPTK-validering — begge går ikke i samme fil.\n",
            "KDRS SØK & VIS (anbefalt, standard)",
            "  file=\"recordN.txt\" — filnavnet beholdes. Søk & Vis finner filene.",
            "  DBPTK-validatoren melder da «not found external lob» (A_M_5.6-1-2) for",
            "  hver LOB, selv om filene finnes i arkivet.\n",
            "DBPTK-VALIDERING",
            "  file=\"content/schemaN/tableM/lobK/recordN.ext\" — full sti. Passerer",
            "  DBPTK-validatoren og -importen, men KDRS Søk & Vis finner IKKE filene.\n",
            "Funnet i stikkprøven:",
        ]
        for path, lr in hits.items():
            ex = next((c for c in lr["columns"] if c["fixable"]), None)
            approx = "minst " if lr.get("sampled") else ""
            lines.append(f"  {path.name}: {approx}{lr['fixable']:,} referanser med bare "
                         f"filnavn i {lr['tables_scanned']} tabell(er)")
            if ex:
                lines.append(f"    \u2022 f.eks. {ex['schema']}/{ex['table']} c{ex['col']}: "
                             f"file=\"{ex['example_ref']}\" → {ex['example_full']}")
        if n_missing:
            lines.append(f"\n  I tillegg {n_missing:,} referanser til filer som ikke "
                         f"finnes i arkivet (se «XML-validering»).")
        lines += ["", "Hvilket mål skal filreferansene tilpasses?"]

        dialog = _PipelineSuggestionDialog(
            self, "\n".join(lines),
            heading="Filreferanser: KDRS Søk & Vis eller DBPTK-validering",
            yes_text="KDRS Søk & Vis (anbefalt)",
            no_text="DBPTK-validering (full sti)")
        dialog.title("Filreferanser: velg KDRS Søk & Vis eller DBPTK-validering")
        self.wait_window(dialog)

        if dialog.result == "ja":
            self._pf((self._pf_status or "funn") + f" → {REF_KDRS} (filnavn beholdes)")
            self._log(f"Filreferanser: {REF_KDRS} — filnavn i file= beholdes "
                      "(DBPTK-validatoren vil melde «not found external lob»)", "info")
            return True

        if dialog.result == "nei":
            # Full sti må skrives SIST (rett før «Pakk sammen SIARD»): HEX Inline
            # Extract og BLOB-konvertering lager/omdøper LOB-filer og skriver
            # nye referanser med bare filnavn — ellers blir formatet blandet.
            op = LobFolderFixOperation(ref_target=REF_DBPTK)
            self.workflow_panel.insert_operation_before(op, "repack_siard")
            self._log("«Korriger lobFolder (SCFC→DBPTK)» med DBPTK-validering (full sti "
                      "i file=) lagt til rett før «Pakk sammen SIARD» — KDRS Søk & Vis "
                      "finner ikke filene i denne SIARD-filen", "ok")
            self._pf(f"funn → {REF_DBPTK}: «Korriger lobFolder» lagt til sist")
            return True  # ops re-hentes av kalleren

        self._pf("avbrutt av operatør")
        return False  # avbrutt

    def _segfolder_preflight(self, ops: list) -> bool:
        """
        Skanner SIARD-filen(e) for SIARD 2.2 LOB-segmentering (seg_N-mapper)
        og tilbyr å sette inn SegFolderFixOperation rett etter
        UnpackSiardOperation — men kun når mål-versjonen er 2.1, ettersom
        SIARD 2.2 tillater segmentering.

        Returnerer True = fortsett kjøring, False = avbryt.
        """
        from siard_workflow.operations import UnpackSiardOperation
        from siard_workflow.operations.segfolder_fix_operation import (
            SegFolderFixOperation, scan_segfolder_issues)

        # Kun relevant i pipeline-modus
        if not any(isinstance(op, UnpackSiardOperation) for op in ops):
            self._pf("ikke aktuelt (krever «Pakk ut SIARD»)")
            return True

        # Allerede lagt til — ikke spør igjen
        if any(isinstance(op, SegFolderFixOperation) for op in ops):
            self._pf(f"«Fjern LOB-segmentering» allerede i arbeidsflyten")
            return True

        # SIARD 2.2 tillater segmentering — kun relevant ved nedgradering til 2.1
        try:
            from siard_workflow.core.siard_format import get_target_siard_version
            if get_target_siard_version() != "2.1":
                self._pf("ikke aktuelt (mål-versjon er ikke 2.1)")
                return True
        except Exception:
            pass

        # Rask skanning av SIARD-filene i køen
        all_issues: dict = {}
        for path in self.siard_queue:
            found = scan_segfolder_issues(path)
            if found:
                all_issues[path] = found

        if not all_issues:
            return True
        self._pf(f"segmentering i {len(all_issues)} fil(er) — spør operatøren")

        lines = [
            "SIARD 2.2 LOB-segmentering (seg_N-mapper) ble oppdaget.\n",
            "Segmentering ble innført i SIARD 2.2 og støttes IKKE av SIARD 2.1.",
            "Mål-versjonen er satt til 2.1, så LOB-filene må flyttes ut av "
            "seg-mappene og file=-referansene i tableX.xml må oppdateres.\n",
        ]
        for path, issues in all_issues.items():
            lines.append(f"  {path.name}:")
            for issue in issues:
                lines.append(f"    • {issue}")
        lines += [
            "",
            "Vil du legge til «Fjern LOB-segmentering» automatisk etter "
            "«Pakk ut SIARD»?",
        ]

        dialog = _PipelineSuggestionDialog(self, "\n".join(lines))
        dialog.title("LOB-segmentering oppdaget (SIARD 2.2)")
        self.wait_window(dialog)

        if dialog.result == "ja":
            self.workflow_panel.insert_operation_after(
                SegFolderFixOperation(), "unpack_siard")
            self._log(
                "«Fjern LOB-segmentering (SIARD 2.2→2.1)» lagt til etter "
                "«Pakk ut SIARD»", "ok")
            self._pf("funn — «Fjern LOB-segmentering» lagt til")
            return True  # ops re-hentes av kalleren

        if dialog.result == "nei":
            self._pf((self._pf_status or "funn") + " → operatøren fortsetter uten retting")
            return True  # operatøren avslo — fortsett uten

        self._pf("avbrutt av operatør")
        return False  # avbrutt

    def _xsd_type_preflight(self, ops: list) -> bool:
        """
        Skanner SIARD-filene for tableX.xsd-avvik DBPTK-validatoren avviser:
        kolonnetyper som ikke stemmer med metadata.xml (P_4.3-3, typisk
        FLOAT(53) med xs:float) og datofasetter på dateType/dateTimeType
        (T_6.3-1). Tilbyr å sette inn XsdTypeFixOperation rett etter
        UnpackSiardOperation når minst ett avvik kan rettes tapsfritt.

        Returnerer True = fortsett kjøring, False = avbryt.
        """
        from siard_workflow.operations import UnpackSiardOperation
        from siard_workflow.operations.xsd_type_fix_operation import XsdTypeFixOperation
        from siard_workflow.core.column_xsd_types import (
            scan_xsd_type_issues, format_finding)

        # Kun relevant i pipeline-modus
        if not any(isinstance(op, UnpackSiardOperation) for op in ops):
            self._pf("ikke aktuelt (krever «Pakk ut SIARD»)")
            return True

        # Allerede lagt til — ikke spør igjen
        if any(isinstance(op, XsdTypeFixOperation) for op in ops):
            self._pf(f"«Rett tableX.xsd» allerede i arbeidsflyten")
            return True

        all_fixable: dict = {}
        n_manual = 0
        for path in self.siard_queue:
            found = scan_xsd_type_issues(path)
            fixable = [f for f in found if f.get("fix_to")]
            n_manual += len(found) - len(fixable)
            if fixable:
                all_fixable[path] = fixable

        if not all_fixable:
            if n_manual:
                self._pf(f"ingen rettbare funn; {n_manual} avvik krever manuell "
                         f"vurdering (se «XML-validering»)")
            return True
        self._pf(f"{sum(len(v) for v in all_fixable.values())} rettbare avvik i "
                 f"{len(all_fixable)} fil(er) — spør operatøren")

        lines = [
            "tableX.xsd avviker fra det DBPTK-validatoren krever.\n",
            "P_4.3-3: kolonnetyper må stemme med metadata.xml (f.eks. xs:double",
            "for FLOAT(p), ikke xs:float). T_6.3-1: dateType/dateTimeType må ha",
            "DBPTKs eksakte minInclusive/maxExclusive (år 0001–9999).",
            "Rettingene er tapsfrie — ingen verdier i tableX.xml endres, og",
            "metadata.xml røres ikke.\n",
        ]
        MAX_LINES = 12
        for path, issues in all_fixable.items():
            lines.append(f"  {path.name}: {len(issues)} rettbare avvik")
            for f in issues[:MAX_LINES]:
                lines.append(f"    \u2022 {format_finding(f)}")
            if len(issues) > MAX_LINES:
                lines.append(f"    … og {len(issues) - MAX_LINES} til")
        if n_manual:
            lines.append(f"\n  I tillegg {n_manual} avvik som ikke kan rettes "
                         f"automatisk (se «XML-validering»).")
        lines += [
            "",
            "Vil du legge til «Rett tableX.xsd» automatisk etter "
            "«Pakk ut SIARD»?",
        ]

        dialog = _PipelineSuggestionDialog(self, "\n".join(lines))
        dialog.title("tableX.xsd avviker fra DBPTK-kravene")
        self.wait_window(dialog)

        if dialog.result == "ja":
            self.workflow_panel.insert_operation_after(
                XsdTypeFixOperation(), "unpack_siard")
            self._log(
                "«Rett tableX.xsd (DBPTK P_4.3-3 / T_6.3-1)» lagt til etter "
                "«Pakk ut SIARD»", "ok")
            self._pf("funn — «Rett tableX.xsd» lagt til")
            return True  # ops re-hentes av kalleren

        if dialog.result == "nei":
            self._pf((self._pf_status or "funn") + " → operatøren fortsetter uten retting")
            return True  # operatøren avslo — fortsett uten

        self._pf("avbrutt av operatør")
        return False  # avbrutt

    def _metadata_quality_preflight(self, ops: list) -> bool:
        """
        Skanner metadata.xml for innholdsfeil (filsti i <dbname>, omvendt
        <dataOriginTimespan>) og tilbyr å sette inn MetadataQualityOperation
        rett etter UnpackSiardOperation.

        Merk: dette er METADATAKVALITET, ikke kompatibilitet. Begge feltene er
        fri tekst i SIARD-skjemaet og DBPTK laster filen uansett — derfor er
        rettingen operatørstyrt og aldri automatisk.

        Returnerer True = fortsett kjøring, False = avbryt.
        """
        from siard_workflow.operations import UnpackSiardOperation
        from siard_workflow.operations.metadata_quality_operation import (
            MetadataQualityOperation, scan_metadata_quality)

        # Kun relevant i pipeline-modus
        if not any(isinstance(op, UnpackSiardOperation) for op in ops):
            self._pf("ikke aktuelt (krever «Pakk ut SIARD»)")
            return True

        # Allerede lagt til — ikke spør igjen
        if any(isinstance(op, MetadataQualityOperation) for op in ops):
            self._pf(f"«Rett metadata-kvalitet» allerede i arbeidsflyten")
            return True

        # Rask skanning av SIARD-filene i køen
        all_issues: dict = {}
        for path in self.siard_queue:
            found = scan_metadata_quality(path)
            if found:
                all_issues[path] = found

        if not all_issues:
            return True
        self._pf(f"{sum(len(v) for v in all_issues.values())} funn i "
                 f"{len(all_issues)} fil(er) — spør operatøren")

        lines = [
            "Innholdsfeil i metadata.xml ble oppdaget.\n",
            "Dette hindrer IKKE innlasting i DBPTK — begge feltene er fri tekst",
            "i SIARD-skjemaet. Men innholdet er feil, og stien lekker intern",
            "nettverksstruktur inn i et arkivobjekt som skal bevares langsiktig.\n",
        ]
        for path, issues in all_issues.items():
            lines.append(f"  {path.name}:")
            for issue in issues:
                lines.append(f"    \u2022 {issue}")
        lines += [
            "",
            "Vil du legge til «Rett metadata-kvalitet» automatisk etter "
            "«Pakk ut SIARD»?",
        ]

        dialog = _PipelineSuggestionDialog(self, "\n".join(lines))
        dialog.title("Metadata-kvalitetsproblemer oppdaget")
        self.wait_window(dialog)

        if dialog.result == "ja":
            self.workflow_panel.insert_operation_after(
                MetadataQualityOperation(), "unpack_siard")
            self._log(
                "«Rett metadata-kvalitet (dbname/datospenn)» lagt til etter "
                "«Pakk ut SIARD»", "ok")
            self._pf("funn — «Rett metadata-kvalitet» lagt til")
            return True  # ops re-hentes av kalleren

        if dialog.result == "nei":
            self._pf((self._pf_status or "funn") + " → operatøren fortsetter uten retting")
            return True  # operatøren avslo — fortsett uten

        self._pf("avbrutt av operatør")
        return False  # avbrutt

    def _antivirus_preflight(self, ops: list) -> bool:
        """
        Uttrekk med svært mange filer (MANY_FILES) og aktivt antivirus: hver fil
        som pakkes ut, skannes — 4–5 millioner små filer kan ta over en time
        bare å opprette. Forklar, mål hastigheten i temp-mappa nå, anslå tiden
        og oppgi mappa som bør unntas fra skanning. Nevner også mekanisk disk
        og korte 8.3-navn — med knapp som slår 8.3 av i et administrator-vindu
        (gui/many_files_dialog.py). Vises også uten antivirus når temp-disken
        lager 8.3-navn og én mappe har ≥ LARGE_FOLDER filer. Fortsett / Fortsett,
        ikke vis igjen / Avbryt. Returnerer True = fortsett, False = avbryt.
        """
        import tempfile as _tf
        from siard_workflow.core import av_check as AV
        if not ({"unpack_siard", "blob_convert", "hex_extract"}
                & {op.operation_id for op in ops}):
            self._pf("ikke aktuelt (ingen utpakking)")
            return True
        counts = {p: AV.zip_entry_count(p) or 0 for p in self.siard_queue}
        n_max = max(counts.values(), default=0)
        if n_max < AV.MANY_FILES:
            self._pf(f"ikke aktuelt ({n_max:,} filer)".replace(",", " "))
            return True
        temp_root = Path(str(self._global_temp_dir)) if self._global_temp_dir \
            else Path(_tf.gettempdir())
        av = AV.active_antivirus()
        vol = AV.volume_info(temp_root)
        # Største mappe (oftest en LOB-mappe) — 8.3-navn bremser mer jo fuller den blir
        big_dir, big_n = "", 0
        for p in self.siard_queue:
            for d, c in AV.largest_folders(p, top=1):
                if c > big_n:
                    big_dir, big_n = d, c
        short_big = bool(vol.get("short_names")) and big_n >= AV.LARGE_FOLDER
        if not av and not vol.get("hdd") and not short_big:
            self._pf(f"{n_max:,}".replace(",", " ") + " filer — ikke funnet aktivt "
                     "antivirus, HDD eller 8.3-problem")
            return True
        rate = AV.measure_file_rate(temp_root)
        n_txt = f"{n_max:,}".replace(",", " ")
        big_txt = f"{big_n:,}".replace(",", " ")
        drive = temp_root.resolve().drive or "disken"
        summary = (f"{n_txt} filer (største mappe {big_txt}); antivirus: "
                   f"{', '.join(av) or 'ikke funnet'}"
                   + (f"; {rate:,.0f} filer/s".replace(",", " ") if rate else "")
                   + ("; HDD" if vol.get("hdd") else "")
                   + (f"; 8.3-navn PÅ for {drive}" if vol.get("short_names") else
                      f"; 8.3-navn av for {drive}" if vol.get("short_names") is False else ""))
        # «Ikke vis igjen» gjelder antivirus/HDD — 8.3 kan rettes herfra, og vises
        # så lenge det er på og en mappe er stor
        if get_config("av_warning_dismissed") and not short_big:
            self._pf(summary + " — varsel slått av")
            return True
        self._pf(summary + " — viser varsel")

        lines = [f"Uttrekket har {n_txt} filer. Alle pakkes ut til temp-mappa "
                 "for identifisering og konvertering, og pakkes inn igjen til slutt.",
                 f"Flest filer i én mappe: {big_dir or '–'} — {big_txt} filer.", ""]
        if av:
            lines += [f"Antivirus er aktivt ({', '.join(av)}) og skanner hver fil som "
                      "opprettes. Med millioner av små filer er det dette som tar tid "
                      "— ikke selve utpakkingen.", ""]
        if rate:
            mins = max(1, round(n_max / rate / 60))
            lines += [f"Målt nå: ca. {rate:,.0f} nye filer per sekund i temp-mappa. ".replace(",", " ")
                      + f"Utpakkingen alene tar da minst ca. {mins} min, og "
                        "sammenpakkingen omtrent like lenge.", ""]
        if av:
            lines += ["Et unntak i antivirus for temp-mappa gjør typisk utpakking, "
                      "konvertering og sammenpakking 3–10 ganger raskere. Be IT legge "
                      "inn unntak (sanntidsskanning) for:",
                      f"    {temp_root}", ""]
        if vol.get("hdd"):
            lines += ["Temp-mappa ligger på en mekanisk disk (HDD). En SSD er langt "
                      "raskere for mange små filer — velg en temp-mappe på en SSD i "
                      "innstillingene.", ""]
        if vol.get("short_names"):
            lines += [f"VIKTIG: {drive} lager korte 8.3-filnavn (som «RECORD~1.TXT») for "
                      "hver fil. Det går stadig tregere jo flere filer en mappe har — "
                      "allerede ved 300 000 filer i én mappe er det målbart, og mapper "
                      "med millioner av filer kan bli svært trege. Slå det av før "
                      "utpakkingen (se under).", ""]
        lines += ["Dette er bare informasjon — kjøringen kan fortsette."]
        parts = (["antivirus"] if av else []) + (["mekanisk disk"] if vol.get("hdd") else []) \
            + (["8.3-filnavn"] if vol.get("short_names") else [])
        from gui.many_files_dialog import ManyFilesDialog
        dialog = ManyFilesDialog(
            self, "\n".join(lines), heading="Mange filer — " + ", ".join(parts),
            short_names=bool(vol.get("short_names")), drive=drive,
            commands=AV.disable_8dot3_commands(temp_root) if vol.get("short_names") else [],
            run_elevated=AV.run_elevated,
            recheck=lambda: AV.volume_info(temp_root).get("short_names"))
        self.wait_window(dialog)
        if dialog.result == "nei":
            try:
                from settings import save_config
                save_config({"av_warning_dismissed": True})
            except Exception:
                pass
            self._pf((self._pf_status or "") + " → varsel slått av")
            return True
        if dialog.result == "ja":
            return True
        self._pf("avbrutt av operatør")
        return False

    def _date_span_preflight(self, ops: list) -> bool:
        """
        Mangler <dataOriginTimespan> (tolkbart datospenn) i metadata.xml, og
        «Estimer datospenn» ikke er i arbeidsflyten: forklar operasjonen og
        tilby å legge den til tidlig — rett etter «Pakk ut SIARD» når den finnes.

        Returnerer True = fortsett kjøring, False = avbryt.
        """
        from siard_workflow.core.date_span import metadata_span, read_timespan
        from siard_workflow.operations import DateSpanOperation

        if any(op.operation_id == "date_span" for op in ops):
            self._pf("«Estimer datospenn» allerede i arbeidsflyten")
            return True
        missing = []
        for path in self.siard_queue:
            ts = read_timespan(path)
            if not metadata_span(ts):
                missing.append((path, ts))
        if not missing:
            self._pf("alle filer oppgir datospenn")
            return True
        self._pf(f"{len(missing)} fil(er) uten datospenn — spør operatøren")

        lines = ["metadata.xml oppgir ikke datospenn (<dataOriginTimespan>) for:\n"]
        for path, ts in missing[:10]:
            lines.append(f"  \u2022 {path.name}" + (f"  (står: «{ts}»)" if ts else "  (tomt)"))
        if len(missing) > 10:
            lines.append(f"  … og {len(missing) - 10} til")
        lines += [
            "",
            "«Estimer datospenn (fra–til)» finner første og siste dato ut fra "
            "datoverdiene i uttrekket — i prioritert rekkefølge sak-/journaltabeller, "
            "person-/klienttabeller (opprettet-/registrert-dato) og LOB-tabeller. "
            "Fødselsdatoer, frister og enkeltstående feilregistreringer avgjør ikke "
            "spennet, og kjørerapporten viser fra–til per tabell.",
            "",
            "Når et datospenn er funnet, spør programmet om det skal skrives til "
            "metadata.xml (med en XML-kommentar om at verdien er estimert) og brukes "
            "videre, også som periode i DIAS-pakken. Svarer du nei, forkastes det.",
            "",
        ]
        from siard_workflow.operations.dias_package_operation import is_period_date
        if any(op.operation_id == "dias_package"
               and not (is_period_date(op.params.get("period_start"))
                        and is_period_date(op.params.get("period_end")))
               for op in ops):
            lines += [
                "DIAS-pakken i arbeidsflyten har ikke periode (fra–til). Uten «Estimer "
                "datospenn» spør programmet når DIAS-steget starter, og kan da kjøre "
                "estimeringen for å foreslå datoer.",
                "",
            ]
        lines += [
            "Vil du legge til «Estimer datospenn» "
            + ("rett etter «Pakk ut SIARD»?"
               if any(op.operation_id == "unpack_siard" for op in ops)
               else "først i arbeidsflyten?"),
        ]
        dialog = _PipelineSuggestionDialog(self, "\n".join(lines),
                                           heading="Datospenn mangler i metadata.xml")
        dialog.title("Datospenn mangler")
        self.wait_window(dialog)

        if dialog.result == "ja":
            from siard_workflow.core.workflow_validator import date_span_insert_index
            ids = [op.operation_id for op in ops]
            idx = date_span_insert_index(ids)
            self.workflow_panel.insert_operation_at(DateSpanOperation(), idx)
            where = ("etter «Pakk ut SIARD»" if "unpack_siard" in ids
                     else "først i arbeidsflyten" if idx == 0 else "etter SHA256")
            self._log(f"«Estimer datospenn (fra–til)» lagt til {where}", "ok")
            self._pf(f"«Estimer datospenn» lagt til {where}")
            return True
        if dialog.result == "nei":
            self._pf((self._pf_status or "funn") + " → operatøren fortsetter uten")
            return True
        self._pf("avbrutt av operatør")
        return False

    # ── Mobilvarsling (core/remote_notify) ──────────────────────────────────

    def _remote_start(self, n_files: int) -> None:
        """Start varsling til mobil for en kjøring (når slått på og satt opp)."""
        self._remote_hub = self._remote_rep = None
        try:
            from settings import get_config
            from siard_workflow.core.remote_notify import build_hub, RunReporter
            hub = build_hub(get_config,
                            log=lambda m, lvl="warn": self._log_queue.put(("log", m, lvl)))
            if hub is None:
                return
            hub.start()
            self._remote_rep = RunReporter(
                hub,
                hide_filenames=bool(get_config("notify_hide_filenames", False)),
                each_step=bool(get_config("notify_each_step", False)),
                heartbeat=bool(get_config("notify_heartbeat", True)))
            self._remote_rep.queue_start(n_files)
            self._remote_hub = hub
            names = {"ntfy": "ntfy-appen", "teams": "Teams"}
            self._log_queue.put(("log", "Mobilvarsling på: " + ", ".join(
                names.get(c.name, c.name) for c in hub.channels), "muted"))
        except Exception as exc:
            self._remote_hub = self._remote_rep = None
            self._log_queue.put(("log", f"Mobilvarsling kunne ikke startes: {exc}", "warn"))

    def _remote_finish(self) -> None:
        rep, hub = self._remote_rep, self._remote_hub
        self._remote_rep = self._remote_hub = None
        try:
            if rep:
                rep.queue_done(stopped=self._stop_event.is_set())
            if hub:
                hub.stop()
        except Exception:
            pass

    def _remote_wait(self, done, title: str, text: str, choices: list,
                     on_remote, timeout: "float | None" = None) -> "str | None":
        """
        Vent på svar i et lokalt vindu (done settes når det lukkes) — og, når
        mobilvarsling er på, på svar fra mobilen. Første svar gjelder:
        on_remote(key) kjøres på hovedtråden og lukker vinduet med tilsvarende
        valg (returnerer False hvis det ikke går, f.eks. skjemafeil). Uten
        mobilvarsling: som done.wait(). Returnerer mobilens valg eller None.

        Ventingen står i kjøreloggen — ellers ser et ubesvart spørsmål ut som
        timer uten fremdrift (livstegnet viser siste loggmelding).
        """
        import time as _time
        from siard_workflow.core.zip_progress import fmt_duration
        logq = self.__dict__.get("_log_queue")
        t_wait = _time.monotonic()
        if logq is not None:
            logq.put(("log", f"  Venter på svar: {title} …", "info"))
        remote = None
        try:
            hub = self._remote_hub
            if hub is None:
                done.wait(timeout)
                return None
            if not hub.can_ask:                      # bare Teams: si fra at det ventes
                hub.notify(f"Venter på svar: {title}", f"{text}\n\nSvar på PC-en.",
                           priority=4, tags=("question",), status_button=False)
                done.wait(timeout)
                return None
            labels = dict(choices)
            ticket = hub.ask(title, text, choices)
            deadline = None if timeout is None else _time.monotonic() + timeout
            try:
                while not done.wait(1.0):
                    if remote is None and ticket.poll() is not None:
                        remote = ticket.poll()
                        self._log_queue.put(("log", f"  Svar fra mobil: «{labels.get(remote)}» "
                                                    f"({title})", "info"))
                        self.after(0, lambda k=remote: self._apply_remote(on_remote, k, title))
                    if deadline is not None and _time.monotonic() > deadline:
                        break
            finally:
                ticket.close("på mobil" if remote else "på PC-en",
                             labels.get(remote, "") if remote else "")
            return remote
        finally:
            waited = _time.monotonic() - t_wait
            if logq is not None and waited >= 60:
                logq.put(("log", f"  Svar på «{title}» etter {fmt_duration(waited)}", "info"))

    @staticmethod
    def _answer_suggestion(dlg, key: str) -> bool:
        """Svar i et _PipelineSuggestionDialog: «ja» / «nei» / «avbryt»."""
        if dlg is None or not dlg.winfo_exists():
            return False
        {"ja": dlg._on_ja, "nei": dlg._on_nei}.get(key, dlg._on_avbryt)()
        return True

    def _apply_remote(self, on_remote, key: str, title: str) -> None:
        """Hovedtråden: bruk mobilsvaret i vinduet, eller si fra hvis det ikke går."""
        try:
            ok = on_remote(key)
        except Exception as exc:
            ok = False
            self._log(f"  Mobilsvaret kunne ikke brukes: {exc}", "warn")
        if ok is False:
            self._log(f"  Mobilsvaret kunne ikke brukes for «{title}» — svar på PC-en", "warn")
            hub = self._remote_hub
            if hub:
                hub.notify(f"Må svares på PC-en: {title}",
                           "Svaret fra mobilen kunne ikke brukes (f.eks. felt som må "
                           "fylles ut). Vinduet står åpent på PC-en.",
                           priority=4, tags=("warning",), status_button=False)

    def _ask_dias_config(self, op) -> "dict | None":
        """
        Kalles fra DIAS-pakkingen (arbeidstråd) når steget starter: vis
        konfigurasjonsvinduet med operasjonens gjeldende innstillinger (perioden
        er allerede hentet fra metadata.xml / estimatet). Returnerer de
        godkjente parameterne, eller None hvis brukeren avbryter.
        """
        import threading as _threading
        done = _threading.Event()
        out: list = [None]

        def _show():
            try:
                from gui.operations_panel import OP_DEFS, set_upstream_op_ids
                from gui.dias_dialog import DiasParamDialog
                op_def = next(d for d in OP_DEFS if d["cls"] is type(op))
                live_def = {**op_def, "params": [
                    {**p, "default": op.params.get(p["key"], p["default"])}
                    for p in op_def["params"]],
                    "initial_extra_files": op.params.get("extra_files", "[]"),
                    "period_note": getattr(op, "period_note", "")}
                ids = [o.operation_id for o in self.workflow_panel.get_operations()]
                set_upstream_op_ids(ids[:ids.index("dias_package")]
                                    if "dias_package" in ids else ids)

                def _confirm(new_op):
                    out[0] = dict(new_op.params) if new_op is not None else None
                dlg = DiasParamDialog(self, live_def, on_confirm=_confirm, runtime=True)
                box["dlg"] = dlg
                self.wait_window(dlg)
            except Exception as exc:
                self._log(f"  DIAS-kontrollvinduet kunne ikke vises: {exc} — pakker med "
                          "lagrede innstillinger", "warn")
                out[0] = dict(op.params)
            finally:
                done.set()

        box: dict = {}

        def _remote(key: str) -> bool:
            dlg = box.get("dlg")
            if dlg is None or not dlg.winfo_exists():
                return False
            if key == "avbryt":
                dlg._cancel()
                return True
            if not all(fn() for fn in dlg._validators):
                return False                 # feil i skjemaet — må rettes på PC-en
            dlg._confirm()
            return True

        p = op.params
        text = "\n".join([
            "DIAS-pakken er klar til å lages med disse innstillingene:",
            f"Pakke: {p.get('label', '')}",
            f"System: {p.get('system', '')} {p.get('system_version', '')}".rstrip(),
            f"Submission Agreement: {p.get('submission_agreement', '')}",
            f"Periode: {p.get('period_start', '')} – {p.get('period_end', '')}",
            f"Mappe: {p.get('output_dir', '') or '(standard)'}",
            "",
            "Fortsett = pakk med innstillingene som vises på PC-en.",
            "Endringer i skjemaet må gjøres på PC-en."])
        self.after(0, _show)
        self._remote_wait(done, "DIAS-pakking: kontroller innstillingene", text,
                          [("fortsett", "Fortsett"), ("avbryt", "Avbryt pakking")], _remote)
        return out[0]

    def _ask_dias_date_span(self, op) -> bool:
        """
        Kalles fra DIAS-pakkingen (arbeidstråd) når periode fra–til ikke er fylt
        ut og uttrekket ikke oppgir datospenn: kjøre «Estimer datospenn» nå, før
        pakkingen, for å få forslag til periode? True = ja.
        """
        import threading as _threading
        done = _threading.Event()
        answer = [False]
        text = "\n".join([
            "DIAS-pakken mangler periode (fra–til), og metadata.xml oppgir ikke "
            "datospenn (<dataOriginTimespan>).",
            "",
            "«Estimer datospenn (fra–til)» kan finne første og siste dato ut fra "
            "datoverdiene i uttrekket — sak-/journaltabeller, person-/klienttabeller "
            "og LOB-tabeller, i den rekkefølgen.",
            "",
            "Ja: kjør «Estimer datospenn» nå. Datoene foreslås i DIAS-vinduet, der du "
            "kan kontrollere og rette dem før pakkingen. Uttrekket endres ikke.",
            "Nei: fyll inn perioden selv i DIAS-vinduet.",
        ])
        box: dict = {}

        def _show():
            dlg = _PipelineSuggestionDialog(
                self, text, heading="Periode mangler i DIAS-pakken",
                yes_text="Ja, estimer datospenn", no_text="Nei, fyll inn selv",
                cancel=False)
            dlg.title("Periode mangler")
            box["dlg"] = dlg
            self.wait_window(dlg)
            answer[0] = dlg.result == "ja"
            done.set()

        self.after(0, _show)
        self._remote_wait(done, "DIAS: periode mangler",
                          "DIAS-pakken mangler periode (fra–til), og metadata.xml oppgir "
                          "ikke datospenn.\n\nJa = kjør «Estimer datospenn» nå og foreslå "
                          "datoene i DIAS-vinduet.\nNei = fyll inn perioden selv.",
                          [("ja", "Ja, estimer"), ("nei", "Nei")],
                          lambda k: self._answer_suggestion(box.get("dlg"), k))
        return answer[0]

    def _ask_date_span(self, res: dict) -> bool:
        """
        Kalles fra «Estimer datospenn» (arbeidstråd) når et datospenn er
        estimert: skal det skrives til metadata.xml og brukes videre (rapporter
        og DIAS-pakke)? True = ja, False = forkast og fortsett uten.
        """
        import threading as _threading
        done = _threading.Event()
        answer = [False]

        def _show():
            lines = [
                f"Estimert datospenn: {res['from_year']}–{res['to_year']}  "
                f"({res.get('from_date', '')} – {res.get('to_date', '')})",
                f"Grunnlag: {res.get('basis_text', '')}",
                f"Sikkerhet: {res.get('confidence', '')}",
            ]
            if not res.get("reliable", True):
                lines += ["", "ADVARSEL: fant ingen sak-/journal-, person-/klient- eller "
                              "LOB-tabeller med datoer — estimatet bygger på alle tabeller "
                              "og er bare veiledende."]
            tabs = res.get("tables", [])
            if tabs:
                lines += ["", "Per tabell (estimert fra–til):"]
                for t in tabs[:12]:
                    span = (f"{t['from_year']}–{t['to_year']}" if t.get("from_year")
                            else "ingen datoer")
                    flag = ("  ← AVVIKER, holdt utenfor" if t.get("off")
                            else "  ← avviker" if t.get("deviates") else "")
                    n_txt = f"{t['n']:,}".replace(",", " ")
                    lines.append(f"  \u2022 {t['table']}.{t['column']}: {span} "
                                 f"({n_txt} datoer){flag}")
                if len(tabs) > 12:
                    lines.append(f"  … og {len(tabs) - 12} tabell(er) til")
            lines += [
                "",
                "Estimatet er ikke nødvendigvis 100 % korrekt.",
                "",
                "Ja: skriv datospennet til metadata.xml (<dataOriginTimespan>, med en "
                "XML-kommentar om at verdien er estimert) og bruk det videre — i "
                "rapportene og som periode i DIAS-pakken.",
                "Nei: forkast estimatet og fortsett uten.",
            ]
            box["text"] = "\n".join(lines)
            dlg = _PipelineSuggestionDialog(
                self, box["text"], heading="Datospenn funnet",
                yes_text="Ja, skriv og bruk", no_text="Nei, fortsett uten", cancel=False)
            dlg.title("Estimert datospenn")
            box["dlg"] = dlg
            self.wait_window(dlg)
            answer[0] = dlg.result == "ja"
            done.set()

        box: dict = {}
        self.after(0, _show)
        _txt = (f"Estimert datospenn: {res.get('from_year')}–{res.get('to_year')}\n"
                f"Grunnlag: {res.get('basis_text', '')}\n"
                f"Sikkerhet: {res.get('confidence', '')}\n\n"
                "Ja = skriv til metadata.xml og bruk videre (rapporter, DIAS).\n"
                "Nei = forkast estimatet og fortsett uten.")
        self._remote_wait(done, "Datospenn funnet", _txt,
                          [("ja", "Ja, skriv og bruk"), ("nei", "Nei")],
                          lambda k: self._answer_suggestion(box.get("dlg"), k))
        return answer[0]

    def _dependency_preflight(self, ops: list) -> bool:
        """
        Sjekk at Python-pakker og eksterne programmer som operasjonene i
        arbeidsflyten trenger, er installert (se dependencies.py). LibreOffice
        har eget steg. Det som bare gir redusert funksjonalitet, logges; det som
        får en operasjon til å feile eller gi feil resultat, gir en dialog med
        installasjon, «Fortsett likevel» og «Stopp».
        """
        import threading, shutil as _sh, time as _time
        import dependencies as _deps
        op_ids = {op.operation_id for op in ops}
        box: dict = {}

        def _work():
            try:
                box["r"] = _deps.check_all(only_ops=op_ids)
            except Exception as exc:
                box["err"] = exc
        t = threading.Thread(target=_work, daemon=True)
        t.start()
        while t.is_alive():
            self.update()
            _time.sleep(0.05)
        if "err" in box:
            self._pf(f"kunne ikke sjekkes ({box['err']}) — fortsetter")
            return True
        statuses = box.get("r") or []
        impacts = _deps.impacts_for_ops(ops, statuses, skip_keys=("libreoffice",))

        def _own_av(op_id: str) -> bool:
            # Virusskanning med eget, gyldig AV-program i operasjonens parametre
            for op in ops:
                if op.operation_id == op_id:
                    p = str(op.params.get("av_executable", "") or "").strip()
                    if p and (Path(p).exists() or _sh.which(p)):
                        return True
            return False
        impacts = [i for i in impacts
                   if not (i.dep.key == "antivirus" and _own_av(i.op_id))]
        if not impacts:
            self._pf("alt arbeidsflyten trenger er installert")
            return True
        soft = [i for i in impacts if not i.blocking]
        hard = [i for i in impacts if i.blocking]
        for i in soft:
            self._log(f"    • «{i.op_label}»: {i.dep.label} mangler — {i.dep.affects}", "muted")
        if not hard:
            self._pf(f"{len(soft)} valgfri(e) komponent(er) mangler — redusert "
                     f"funksjonalitet (se over)")
            return True

        from gui.dependencies_dialog import DependenciesDialog
        dlg = DependenciesDialog(self, statuses, mode="preflight", impacts=impacts, ops=ops)
        self.wait_window(dlg)
        still = sorted({i.dep.label for i in hard
                        if any(s.key == i.dep.key and s.missing for s in dlg.statuses)})
        if dlg.result == "stopp":
            self._pf("stoppet: mangler " + ", ".join(still or [i.dep.label for i in hard]))
            return False
        if still:
            for i in hard:
                if i.dep.label in still:
                    self._log(f"    ✗ «{i.op_label}»: {i.dep.label} mangler — {i.dep.affects}",
                              "warn")
            self._pf("mangler " + ", ".join(still) + " — operatøren fortsetter")
        else:
            self._pf("manglende programvare installert")
        return True

    def _libreoffice_preflight(self, ops: list) -> bool:
        """
        Helsesjekk av LibreOffice FØR kjøring når arbeidsflyten bruker det
        (BLOB-konvertering / CosDoc). Kjøres i bakgrunnstråd så vinduet ikke
        fryser. Første gang for en installasjon tar det opptil ~50 s (gjenbruks-
        testen); resultatet lagres per installasjon, så senere tar det ~5 s.

        * LibreOffice virker ikke → advarsel, anbefalt å stoppe.
        * Versjon som henger ved gjenbruk av profil (LibreOffice 26.2.1) →
          advarsel med valg: stopp for å installere annen versjon, åpne
          nedlastingssiden, eller fortsette (tregt) — evt. «ikke vis igjen».
        """
        from siard_workflow.core import lo_runner as _lr
        from siard_workflow.core.libreoffice import find_libreoffice
        uses_lo = [op for op in ops if op.operation_id in ("blob_convert", "cosdoc_mailmerge")]
        if not uses_lo:
            self._pf("ikke aktuelt (ingen LibreOffice-konvertering)")
            return True
        hint = ""
        for op in uses_lo:
            hint = str(op.params.get("libreoffice_bin", "") or "")
            if hint:
                break
        lo_bin = find_libreoffice(hint or "soffice")
        if not lo_bin:
            dlg = _LibreOfficeWarningDialog(
                self, "LibreOffice ikke funnet",
                "Arbeidsflyten konverterer dokumenter med LibreOffice, men "
                "LibreOffice ble ikke funnet.\n\nInstaller LibreOffice (anbefalt: siste "
                "feilrettingsversjon i en moden serie) og prøv igjen, eller angi stien "
                "i operasjonens innstillinger.", can_continue=True, allow_dont_show=False)
            self.wait_window(dlg)
            if dlg.result == "stopp":
                self._pf("stoppet: LibreOffice ikke funnet")
                return False
            self._pf("LibreOffice ikke funnet — operatøren fortsetter")
            return True

        version = _lr.lo_version(lo_bin) or "ukjent versjon"
        self._update_status_right(f"Preflight: tester LibreOffice {version} …")
        import threading, tempfile, shutil as _sh, time as _time
        result: dict = {}
        work = Path(tempfile.mkdtemp(prefix="siard_lo_preflight_"))

        def _run():
            try:
                result["r"] = _lr.health_check(lo_bin, work)
            except Exception as exc:
                result["r"] = (False, f"{type(exc).__name__}: {exc}", True)
        t = threading.Thread(target=_run, daemon=True)
        t.start()
        dots = 0
        while t.is_alive():
            self.update()
            _time.sleep(0.1)
            dots += 1
            if dots % 20 == 0:
                self._update_status_right(
                    f"Preflight: tester LibreOffice {version} … ({dots // 10} s)")
        _sh.rmtree(work, ignore_errors=True)
        ok, err, reuse_ok = result.get("r", (False, "ukjent feil", True))

        if not ok:
            dlg = _LibreOfficeWarningDialog(
                self, "LibreOffice fungerer ikke",
                f"LibreOffice {version} ({lo_bin}) klarte ikke å konvertere en enkel "
                f"tekstfil.\n\nFeil: {err}\n\nDokumentkonverteringen vil feile. "
                "Reparer eller installer LibreOffice på nytt før du kjører.\n\n"
                "Test i ledetekst:\n  soffice --headless --convert-to pdf test.txt",
                can_continue=True, allow_dont_show=False)
            self.wait_window(dlg)
            if dlg.result == "stopp":
                self._pf(f"stoppet: LibreOffice {version} fungerer ikke")
                return False
            self._pf(f"LibreOffice {version} fungerer ikke — operatøren fortsetter")
            return True

        if reuse_ok:
            self._pf(f"LibreOffice {version}: OK")
            return True

        if _lr.reuse_warning_acknowledged(lo_bin):
            self._pf(f"LibreOffice {version}: henger ved gjenbruk av profil — ny profil "
                     f"per kall (advarsel skjult etter valg)")
            return True

        msg = (
            f"LibreOffice {version} henger når samme profil brukes mer enn én gang. "
            "Dette er en feil i denne LibreOffice-versjonen.\n\n"
            "Programmet kan jobbe rundt det ved å lage en ny profil for hvert "
            "LibreOffice-kall, men det gir ca. 5 sekunder ekstra per kall. "
            "E-postkonvertering (MSG/EML) og store uttrekk blir da betydelig tregere.\n\n"
            "Anbefalt: stopp kjøringen og installer en annen LibreOffice-versjon — "
            "siste feilrettingsversjon i en moden serie (per september 2026: 26.2.6), "
            "eller 25.8.7 hvis feilen fortsatt finnes. Unngå x.y.0-utgivelser.\n\n"
            "Programmet oppdager ny installasjon selv og tester på nytt ved neste kjøring.")
        dlg = _LibreOfficeWarningDialog(
            self, "LibreOffice-versjon med kjent feil", msg,
            can_continue=True, allow_dont_show=True)
        self.wait_window(dlg)
        if dlg.result == "stopp":
            self._log(f"Kjøringen stoppet for å bytte LibreOffice {version}. "
                      f"Nedlasting: {_lr.DOWNLOAD_URL}", "warn")
            self._pf(f"stoppet: LibreOffice {version} henger ved gjenbruk av profil")
            return False
        if dlg.dont_show_again:
            _lr.acknowledge_reuse_warning(lo_bin)
        self._pf(f"LibreOffice {version}: henger ved gjenbruk av profil — operatøren "
                 f"fortsetter med ny profil per kall (tregere)")
        return True

    def _filename_preflight(self, ops: list) -> bool:
        """
        Ødelagte filnavn i køen («1504_Ã…lesund» for «1504_Ålesund»): navnet
        arves av alt som lages (ny SIARD, PREMIS, CSV-er, rapporter), og
        kontrolltegnet i det kan ikke skrives med Windows-tegnsettet. Tilby å
        gi fila riktig navn før kjøringen.

        Returnerer True = fortsett, False = avbryt.
        """
        from siard_workflow.core.filename_check import has_control_chars, repair_mojibake

        fixes = []          # (sti, nytt navn)
        unfixable = []
        for path in self.siard_queue:
            fixed = repair_mojibake(path.name)
            if fixed:
                fixes.append((path, fixed))
            elif has_control_chars(path.name):
                unfixable.append(path)
        if not fixes and not unfixable:
            return True

        def _show(name: str) -> str:
            return "".join(f"\\x{ord(c):02x}" if has_control_chars(c) else c for c in name)

        for path in unfixable:
            self._log(f"    ADVARSEL: {_show(path.name)} har kontrolltegn i navnet — "
                      f"gi fila nytt navn før arkivering", "warn")
        if not fixes:
            self._pf(f"{len(unfixable)} fil(er) med kontrolltegn i navnet (se over)")
            return True

        lines = "\n".join(f"  {_show(p.name)}\n    → {n}" for p, n in fixes[:8])
        if len(fixes) > 8:
            lines += f"\n  … og {len(fixes) - 8} til"
        msg = (
            f"{len(fixes)} SIARD-fil(er) har et ødelagt filnavn — «Å», «Æ» eller «Ø» er "
            f"lagret feil (typisk etter utpakking av en zip-fil eller en overføring):\n\n"
            f"{lines}\n\n"
            "Navnet arves av alt som lages: ny SIARD, PREMIS, CSV-er og rapporter.\n\n"
            "Vil du gi fila riktig navn før kjøringen?"
        )
        dialog = _PipelineSuggestionDialog(self, msg, heading="Ødelagt filnavn",
                                           yes_text="Ja, gi riktig navn",
                                           no_text="Nei, behold navnet")
        dialog.title("Filnavn")
        self.wait_window(dialog)

        if dialog.result == "ja":
            renamed = 0
            for path, fixed in fixes:
                target = path.with_name(fixed)
                try:
                    if target.exists():
                        raise FileExistsError(f"{fixed} finnes fra før")
                    path.rename(target)
                except OSError as exc:
                    self._log(f"    Kunne ikke gi nytt navn til {_show(path.name)}: {exc}",
                              "warn")
                    continue
                self.siard_queue[self.siard_queue.index(path)] = target
                if self.siard_path == path:
                    self.siard_path = target
                # Prosjektfila følger med, så kjøringen kan gjenopptas
                self._rename_project_for(path, target)
                self._log(f"    Nytt navn: {fixed}", "ok")
                renamed += 1
            self._queue_render()
            self._pf(f"{renamed} av {len(fixes)} fil(er) fikk riktig navn")
            return True
        if dialog.result == "nei":
            self._pf("ødelagt filnavn beholdt — utdatafilene arver det")
            return True
        self._pf("avbrutt av operatør")
        return False

    def _report_preflight(self, ops: list) -> bool:
        """
        Spør om brukeren vil legge til 'Kjørerapport (PDF)' dersom den ikke
        allerede er med i workflowen.

        Returnerer True = fortsett, False = avbryt.
        """
        from siard_workflow.operations import WorkflowReportOperation

        if any(isinstance(op, WorkflowReportOperation) for op in ops):
            self._pf("allerede med i arbeidsflyten")
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
            self._pf("rapport lagt til — klikk Kjør på nytt")
            return False   # stopp denne kjøringen; bruker klikker Kjør på nytt

        if dialog.result == "nei":
            self._pf("operatøren vil ikke ha sluttrapport")
            return True   # brukeren vil ikke ha rapport — fortsett uten

        self._pf("avbrutt av operatør")
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
            self._pf("ikke aktuelt (ingen BLOB-konvertering)")
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

        if not temp_warnings and not output_warnings:
            self._pf(f"tilstrekkelig temp- og output-plass for "
                     f"{len(self.siard_queue)} fil(er)")
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
                self._pf("avbrutt: for lite temp-plass")
                return False
            self._pf("lite temp-plass — operatøren fortsetter")

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
                self._pf("avbrutt: ingen alternativ lagringsmappe valgt")
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
                    self._pf("avbrutt: valgt mappe har for lite plass")
                    return False
            except Exception:
                pass

            self._output_dir_override = chosen
            self._pf(f"lite output-plass — alternativ lagringsmappe: {chosen}")

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

        if self.workflow_panel.has_order_violations():
            from tkinter import messagebox
            messagebox.showerror(
                "Ugyldig arbeidsflyt",
                "Arbeidsflyten har operasjoner i feil rekkefølge.\n"
                "Rett opp rekkefølgen (se ⚠-markeringene) før du kjører.",
                parent=self)
            return

        # ── Kjøreplan: prosjektet er kjørt før ───────────────────────────────
        # Vis hva som hoppes over / kjøres (på nytt) før noe skjer, og la
        # brukeren velge grunnlag når temp-mappa må lages på nytt.
        self._run_plan_bases = {}
        resume = self._confirm_run_plan(ops)
        if resume is None:
            return

        # ── Preflight: synlig i logg og statuslinje ──────────────────────────
        # Hvert steg logges med navn, resultat og tidsbruk. Steg som endrer
        # arbeidsflyten (eller stopper kjøringen) logger det selv; dialoger
        # vises kun ved funn. Ops re-hentes mellom stegene siden et steg kan
        # ha lagt til eller endret operasjoner.
        self.log_panel.clear()
        self._log_entries.clear()
        self._preflight_lines = []
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log("=" * 56, "muted")
        self._log(f"Preflight {ts} — {len(self.siard_queue)} fil(er), "
                  f"{len(ops)} operasjon(er)", "step")
        self._log("  Sjekker køen før kjøring. Dialog vises bare ved funn.", "muted")
        self._preflight_active = True
        self._status_spinner.configure(text="⏳")
        t_pf = time.monotonic()
        steps = [
            ("Filnavn (tegnkoding)",                       self._filename_preflight),
            ("Pipeline-modus (Pakk ut / Pakk sammen)",     self._pipeline_preflight),
            ("lobFolder (SCFC→DBPTK)",                     self._lobfolder_preflight),
            ("LOB-referanser (DBPTK A_M_5.6-1-2)",         self._lob_fullpath_preflight),
            ("LOB-segmentering (SIARD 2.2→2.1)",           self._segfolder_preflight),
            ("tableX.xsd (DBPTK P_4.3-3 / T_6.3-1)",       self._xsd_type_preflight),
            ("Metadata-kvalitet (dbname/datospenn)",       self._metadata_quality_preflight),
            ("Datospenn i metadata.xml",                   self._date_span_preflight),
            ("Sluttrapport (PDF)",                         self._report_preflight),
            ("Programvare (pakker og programmer)",         self._dependency_preflight),
            ("LibreOffice (konvertering)",                 self._libreoffice_preflight),
            ("Diskplass (temp/output)",                    self._disk_space_preflight),
            ("Antivirus og temp-disk (mange filer)",       self._antivirus_preflight),
        ]
        # Prosjektet er kjørt før: forslagene om å legge til steg (og
        # skanningene bak dem) ble besvart i første kjøring — ikke spør igjen.
        # Programvare, LibreOffice og antivirus sjekkes fortsatt.
        _answered = {self._filename_preflight, self._pipeline_preflight,
                     self._lobfolder_preflight, self._lob_fullpath_preflight,
                     self._segfolder_preflight, self._xsd_type_preflight,
                     self._metadata_quality_preflight, self._date_span_preflight,
                     self._report_preflight}
        if resume:
            if not resume.get("unpack"):
                _answered.add(self._disk_space_preflight)
            if not resume.get("blob"):
                _answered.add(self._libreoffice_preflight)

            def _already(_ops, _self=self):
                _self._pf("hoppet over — prosjektet er kjørt før, svarene fra "
                          "første kjøring gjelder")
                return True
            steps = [(label, _already if fn in _answered else fn)
                     for label, fn in steps]
        try:
            for label, fn in steps:
                ops = list(self.workflow_panel.get_operations())
                if not self._run_preflight_step(label, fn, ops):
                    self._log("Kjøringen startes ikke.", "warn")
                    self._update_status_right("Preflight: kjøring ikke startet")
                    return
        finally:
            self._preflight_active = False
            if not self._running:
                self._status_spinner.configure(text=" ")
        self._log(f"Preflight ferdig ({time.monotonic() - t_pf:.1f} s) — starter kjøring",
                  "ok")
        self._update_status_right("Preflight ferdig — starter kjøring")
        self.update_idletasks()
        # Kopi til fil-loggen(e) som opprettes per SIARD i arbeidstråden
        self._preflight_lines = list(self._log_entries)
        ops = list(self.workflow_panel.get_operations())

        self._running = True
        self._current_run = None
        self.workflow_panel.set_running(True)

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
            target=self._run_queue_thread,
            args=(filer, ops, self._project_file, self._project_path),
            daemon=True)
        thread.start()

    def _run_queue_thread(self, filer: list, ops: list,
                          pf_arg=None, pf_path_arg=None):
        """Kjør workflow sekvensielt på alle filer i køen."""
        import time
        from siard_workflow.core.context import WorkflowContext
        from siard_workflow.core.workflow import WorkflowRun
        from siard_workflow.core.project_file import sanitize_result
        from siard_workflow.core.run_plan import held_after_unpack_failure

        self._remote_start(len(filer))
        _rep = self._remote_rep

        for q_idx, path in enumerate(filer):
            if self._stop_event.is_set():
                break

            self._log_queue.put(("queue_file_start", q_idx, len(filer), path))

            wf = Workflow(name=path.stem)
            for op in ops:
                wf.add(op)
            if _rep:
                _rep.file_start(q_idx, len(filer), path.name, len(ops))

            # ── Prosjektfil-checkpoint ─────────────────────────────────────────
            # Lastet prosjekt for denne fila, ellers en eksisterende
            # <siard>.siardwf ved siden av fila (alle filer i køen — ikke bare
            # den aktive), ellers en ny. Kjøreplanen (core/run_plan.py)
            # bestemmer så hvilke steg som hoppes over, kjøres eller må kjøres
            # på nytt.
            _pf, _pf_path = self._project_for_run(path, wf, pf_arg, pf_path_arg)
            _plan, _workdir = self._plan_for_run(_pf, wf, path)
            if _pf is not None:
                self._log_queue.put(("project_checkpoint_path", str(_pf_path)))

            ctx = WorkflowContext(siard_path=path)
            run = WorkflowRun(path)
            self._conv_ctx = ctx

            def _progress_cb(event, **kw):
                self._log_queue.put(("conv_" + event, kw))
                if _rep and event in ("log", "phase_progress"):
                    _rep.progress(event, kw)       # for «Status nå» på mobilen
                # Akkumuler filtype-data i bakgrunnstråden — tilgjengelig for rapport-operasjonen
                if event == "file_done":
                    ext = kw.get("detected_ext", "")
                    if ext:
                        fc = ctx.metadata.setdefault("format_counts", {})
                        fc[ext] = fc.get(ext, 0) + 1
                elif event == "rename_format_counts":
                    fc = ctx.metadata.setdefault("format_counts", {})
                    for e, c in kw.get("counts", {}).items():
                        fc[e] = fc.get(e, 0) + c

            ctx.metadata["progress_cb"]  = _progress_cb
            ctx.metadata["paused"]       = False
            ctx.metadata["stopped"]      = False
            self._stop_event.clear()
            self._pause_event.clear()
            ctx.metadata["stop_event"]          = self._stop_event
            ctx.metadata["pause_event"]         = self._pause_event
            ctx.metadata["siardmapper_dialog_cb"] = self._siardmapper_dialog_cb
            # Registrer callback for "Standardiser filendelser" kun hvis
            # operasjonen ikke allerede er i workflowen
            _has_stdext = any(
                getattr(op, "operation_id", "") == "standardize_ext"
                for op in wf)
            # Svar på spørsmål under kjøringen huskes i prosjektfila
            # (_remembered) — de stilles ikke på nytt når prosjektet kjøres igjen
            if not _has_stdext:
                ctx.metadata["ask_standardize_cb"] = self._remembered(
                    _pf, _pf_path, "unpack_siard:standardize_ext",
                    "Standardiser filendelser", self._ask_add_standardize_ext)

            # Schema-velger: callback for å spørre om auto-add når
            # flere schemas oppdages, og dialog-callback for selve valget.
            _has_schemasel = any(
                getattr(op, "operation_id", "") == "schema_selector"
                for op in wf)
            if not _has_schemasel:
                ctx.metadata["ask_schema_selector_cb"] = self._remembered(
                    _pf, _pf_path, "unpack_siard:schema_selector",
                    "Legg til schema-velger", self._ask_add_schema_selector)
            ctx.metadata["ask_schema_select_cb"] = self._remembered(
                _pf, _pf_path, "schema_selector:selection",
                "Valgte schemaer", self._schema_select_dialog_cb)

            # Callback for å fylle inn tomme <schema><name>-noder
            ctx.metadata["ask_fill_empty_schema_names_cb"] = \
                self._ask_fill_empty_schema_names

            # Callback for å velge ny mål-mappe ved disk-plassmangel i DIAS-pakking
            ctx.metadata["ask_dias_output_dir_cb"] = self._ask_dias_output_dir

            # Callback: Ghostscript-etterbehandling av PDF/A (BLOB-konvertering)
            ctx.metadata["ask_gs_postprocess_cb"] = self._remembered(
                _pf, _pf_path, "blob_convert:gs_postprocess",
                "Ghostscript-etterbehandling", self._ask_gs_postprocess)

            # Callback: skal et estimert datospenn skrives til metadata.xml og brukes?
            ctx.metadata["ask_date_span_cb"] = self._remembered(
                _pf, _pf_path, "date_span:use_estimate",
                "Bruk estimert datospenn", self._ask_date_span)

            # Callback: DIAS-konfigurasjonen vises når pakkesteget starter
            ctx.metadata["ask_dias_config_cb"] = self._ask_dias_config

            # Callback: DIAS mangler periode — kjøre «Estimer datospenn» først?
            ctx.metadata["ask_dias_date_span_cb"] = self._remembered(
                _pf, _pf_path, "dias_package:date_span",
                "Estimer datospenn for DIAS-perioden", self._ask_dias_date_span)

            # Callback: ukjent filformat i BLOB-konverteringen (konverter/behold/hopp over)
            ctx.metadata["ask_unknown_format_cb"] = self._ask_unknown_format

            # Callback for forhåndsvisning/bekreftelse av SIARD-anonymisering
            ctx.metadata["anonymize_preview_cb"] = self._anonymize_preview_cb

            if self._global_temp_dir:
                ctx.metadata["temp_dir"] = str(self._global_temp_dir)
            if self._output_dir_override:
                ctx.metadata["output_dir_override"] = self._output_dir_override
            # «Logg-mappe» i Innstillinger; tom = mappa SIARD-fila ligger i
            log_dir = resolve_log_dir(path)
            try:
                log_dir.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                self._log_queue.put(("log", f"  Logg-mappa {log_dir} kan ikke brukes ({exc}) "
                                            f"— loggene skrives ved SIARD-fila", "warn"))
                log_dir = path.parent
            ctx.metadata["log_dir"] = str(log_dir)

            run.start_time = time.time()
            file_logger = None
            try:
                # Livstegn hver halvtime — også i GUI-loggen
                file_logger = WorkflowFileLogger(
                    log_dir, path.stem,
                    on_heartbeat=lambda m: (self._log_queue.put(("log", m, "muted")),
                                            _rep and _rep.heartbeat(m)))
                file_logger.__enter__()
                ctx.metadata["file_logger"] = file_logger
                # Preflight kjørte på hovedtråden før fil-loggen fantes —
                # skriv den inn først, så loggfilen viser hva som ble sjekket.
                _pf_lines = getattr(self, "_preflight_lines", None) or []
                if _pf_lines:
                    # GUI-nivåer og fil-loggerens LEVELS deler nøkler
                    # (info/step/success/warn/error/muted); "ok" → success.
                    for _lvl, _msg in _pf_lines:
                        file_logger.log(_msg, "success" if _lvl == "ok" else _lvl)
                    file_logger.log("", "info")
            except Exception:
                pass

            # PREMIS-proveniens: én event per innholdsendrende operasjon,
            # skrives til {base}_premis.xml i log-mappa og auto-inkluderes i
            # DIAS-pakka. Kan slås av via config.
            premis_logger = None
            if get_config("enable_premis_provenance"):
                try:
                    premis_logger = PremisProvenanceLogger(
                        log_dir, path, agent_version=str(self.VERSION))
                    ctx.metadata["premis_logger"] = premis_logger
                except Exception:
                    premis_logger = None

            ctx.metadata["step_results"] = []

            # SIARD-header-panelet: vis headeren nå; endringer i panelet skrives
            # inn mellom stegene og tas med ved «Pakk sammen SIARD»
            _hdr_ids = {op.operation_id for op in wf}
            _hdr = {"can_edit": {"unpack_siard", "repack_siard"} <= _hdr_ids,
                    "repacked": False}
            self._header_publish(ctx, _hdr, first=True)

            # ── Kjøring i prosjektfila + kjøreplanen i loggen ──────────────────
            _run_id = None
            _log_file = str(file_logger.log_path) if file_logger and file_logger.log_path else None
            if _pf is not None:
                _run_id = _pf.start_run(_log_file)
                ctx.metadata["project_file"] = _pf
                ctx.metadata["run_id"] = _run_id
                if premis_logger is not None:
                    # Proveniens samles opp på tvers av kjøringer
                    premis_logger.append_existing = len(_pf.runs) > 1
                self._project_save(_pf, _pf_path)
            if _plan is not None and _pf is not None and _pf.has_history:
                _plines = ["Kjøreplan (prosjektet er kjørt før):"] + [
                    "  " + ln for ln in _plan.lines()]
                if _plan.base:
                    _plines.append(
                        "  Grunnlag: "
                        + ("original-SIARD" if _plan.base == "original" else "siste resultat")
                        + (f" — {_plan.base_reason}" if _plan.base_reason else ""))
                for _ln in _plines:
                    self._log_queue.put(("log", _ln, "info"))
                    if file_logger:
                        file_logger.log(_ln, "info")
            # Filtyper fra en tidligere BLOB-konvertering (rapporten)
            if _pf is not None and _pf.format_counts:
                _blob_steps = [s for s in (_plan.steps if _plan else [])
                               if s.op_id == "blob_convert"]
                if _blob_steps and not any(s.runs for s in _blob_steps):
                    ctx.metadata["format_counts"] = dict(_pf.format_counts)

            _plan_of = {}
            if _plan is not None:
                for _sp, _o in zip(_plan.steps, list(wf)):
                    _plan_of[id(_o)] = _sp
            _held: dict = {}      # id(op) → hvorfor steget holdes igjen
            _aborted = False

            i = 0
            while i < len(wf):
                op = list(wf)[i]
                # Stoppet mellom steg — vent på «▶ Fortsett» eller «Avslutt»
                if self._stop_event.is_set():
                    if not self._wait_after_stop(i, op.label):
                        _aborted = True
                        break

                _sp = _plan_of.get(id(op))
                _cp = _pf.at(i, op.operation_id) if _pf is not None else None

                self._log_queue.put(("step_start", i, op.label))
                if _rep:
                    _rep.step_start(i, len(wf), op.label)
                if file_logger:
                    file_logger.set_step(f"[{i+1}] {op.label}")
                    file_logger.log(f"[{i+1}] {op.label}", "step")

                def _skip_record(message, success=True, cp=None, _op=op, _i=i):
                    ctx.metadata["step_results"].append({
                        "id":       _op.operation_id,
                        "label":    _op.label,
                        "category": getattr(_op, "category", ""),
                        "success":  success,
                        "message":  message,
                        "skipped":  True,
                        "elapsed":  0.0,
                        "index":    _i,
                        "from_run": getattr(cp, "run_id", None) if cp else None,
                    })

                # ── Holdt igjen: et innholdssteg før feilet/ble avbrutt ────────
                if id(op) in _held:
                    _why = _held[id(op)]
                    _msg = f"  Holdt igjen — {_why}"
                    self._log_queue.put(("log", _msg, "warn"))
                    if file_logger:
                        file_logger.log(_msg, "warn")
                    if _pf is not None and _cp is not None:
                        _pf.mark_held(_cp, _why)
                        _pf.note_step_run(_run_id, op.label, "held")
                        self._project_save(_pf, _pf_path)
                    self._log_queue.put(("project_step_saved", i, "held"))
                    run.skipped.append(op.operation_id)
                    _skip_record(f"Holdt igjen — {_why}", success=None)
                    i += 1
                    continue

                # ── Kjøreplan: hoppes over ────────────────────────────────────
                if _sp is not None and _sp.action in ("skip", "user_skip") and _cp is not None:
                    if _sp.action == "user_skip":
                        self._log_queue.put(("log", "      [⤼] Hoppet over (valgt)", "muted"))
                        self._log_queue.put(("project_step_saved", i, "user_skip"))
                        if file_logger:
                            file_logger.log("  Hoppet over (valgt: «Hopp over i neste kjøring»)",
                                            "muted")
                        run.skipped.append(op.operation_id)
                        _skip_record("Hoppet over (valgt av bruker)", success=None, cp=_cp)
                        i += 1
                        continue
                    _out_name = self._restore_skipped_step(ctx, op, _cp, _workdir)
                    self._log_queue.put(("project_skip", i, op.label, _out_name))
                    run.skipped.append(op.operation_id)
                    if file_logger:
                        file_logger.log(f"  Hoppet over ({_sp.reason or 'allerede fullført'})",
                                        "muted")
                    _skip_record(_cp.message or "Allerede fullført — hoppet over", cp=_cp)
                    i += 1
                    continue

                if _sp is not None and _sp.action == "rerun" and _sp.reason:
                    _msg = f"  Kjøres på nytt: {_sp.reason}"
                    self._log_queue.put(("log", _msg, "info"))
                    if file_logger:
                        file_logger.log(_msg, "info")
                elif _sp is not None and _sp.reason and file_logger:
                    file_logger.log(f"  ({_sp.reason})", "muted")

                # «Pakk ut» på nytt: original-SIARD eller siste resultat
                if op.operation_id == "unpack_siard" and _sp is not None and _sp.runs:
                    ctx.extracted_path = None
                    if _plan.base == "output" and _plan.output_siard:
                        ctx.siard_path = Path(_plan.output_siard)
                        # «Pakk sammen» navngir etter originalen (…_konvertert_1.siard)
                        ctx.metadata["repack_name_base"] = str(path)
                        _msg = (f"  Bygger på siste resultat: pakker ut "
                                f"{Path(_plan.output_siard).name}")
                        self._log_queue.put(("log", _msg, "info"))
                        if file_logger:
                            file_logger.log(_msg, "info")

                if _pf is not None and _cp is not None:
                    _pf.mark_started(_cp, _run_id, _log_file)
                    self._project_save(_pf, _pf_path)

                if not op.should_run(ctx):
                    self._log_queue.put(("skip", op.operation_id, op.label))
                    run.skipped.append(op.operation_id)
                    if file_logger:
                        file_logger.log("  Hoppet over (vilkår ikke oppfylt)", "muted")
                    if _pf is not None and _cp is not None:
                        _pf.mark_skipped(_cp)
                        _pf.note_step_run(_run_id, op.label, "skipped")
                        self._project_save(_pf, _pf_path)
                    ctx.metadata["step_results"].append({
                        "id":       op.operation_id,
                        "label":    op.label,
                        "category": getattr(op, "category", ""),
                        "success":  None,
                        "message":  "Hoppet over (vilkår ikke oppfylt)",
                        "skipped":  True,
                        "elapsed":  0.0,
                        "index":    i,
                    })
                    i += 1
                    continue

                if _hdr["can_edit"] and not _hdr["repacked"]:
                    _is_repack = op.operation_id == "repack_siard"
                    _hdr_written = self._header_apply_pending(
                        ctx, file_logger, premis_logger, before_repack=_is_repack)
                    _hdr["packing"] = _is_repack      # låst mens det pakkes
                    if _hdr_written or _is_repack:
                        self._header_publish(ctx, _hdr)

                if op.operation_id == "workflow_report":
                    ctx.metadata["held_steps"] = [o.label for o in list(wf)
                                                  if id(o) in _held]

                t0 = time.time()
                try:
                    result = op.run(ctx)
                    ctx.set_result(op.operation_id, result.data)
                    # PREMIS-proveniens for innholdsendrende operasjoner
                    if (premis_logger and getattr(op, "modifies_content", False)
                            and op.premis_should_record(result, ctx)):
                        premis_logger.record(op, result, ctx)
                    # Hvis operasjonen produserte en ny SIARD-fil, oppdater input-stien
                    new_siard = ctx.chain_siard(op.produces_siard, result.data)
                    if new_siard:
                        self._log_queue.put(("log",
                            f"  ↪ Neste steg bruker: {new_siard.name}", "info"))
                    # Dynamisk innlegg av operasjon (f.eks. StandardizeExtOperation
                    # eller SchemaSelectorOperation). Støtter både enkelt-op og
                    # liste — liste settes inn i samme rekkefølge etter gjeldende.
                    _to_insert = ctx.metadata.pop("_insert_after_current", None)
                    if _to_insert is not None:
                        _ops_to_insert = (_to_insert if isinstance(_to_insert, list)
                                          else [_to_insert])
                        # 1. Oppdater Workflow-objektet (bakgrunnstråd-trygt —
                        #    rene Python-lister, ingen Tk-objekter).
                        for k, _op in enumerate(_ops_to_insert):
                            wf.insert(i + 1 + k, _op)
                            self._log_queue.put(("log",
                                f"  ✚ Lagt til i arbeidsflyt: {_op.label}", "ok"))

                        # 2. Oppdater workflow_panel (UI) — MÅ kjøres på
                        #    hovedtråden. Sett inn etter forrige op-id slik at
                        #    rekkefølgen blir bevart: unpack → ny1 → ny2 → ...
                        _prev_id = op.operation_id
                        _ops_copy = list(_ops_to_insert)
                        def _do_gui_insert(prev_id=_prev_id, ops_list=_ops_copy):
                            prev = prev_id
                            for _op in ops_list:
                                self.workflow_panel.insert_operation_after(
                                    _op, prev, silent=True)
                                prev = _op.operation_id
                        self.after(0, _do_gui_insert)
                except Exception as exc:
                    result = op._fail(str(exc))
                elapsed = time.time() - t0
                # Stoppet av brukeren midt i steget (og steget ble ikke ferdig):
                # AVBRUTT — fortsetter ved «▶ Fortsett» eller neste kjøring
                _interrupted = (not result.success) and (
                    bool((result.data or {}).get("interrupted"))
                    or self._stop_event.is_set())
                run.results.append(result)
                self._log_queue.put(("result", result))
                if _rep:
                    _rep.step_done(i, op.label, result.success, result.message, elapsed)
                if file_logger:
                    lvl = "ok" if result.success else ("warn" if _interrupted else "feil")
                    _tag = "OK" if result.success else ("AVBRUTT" if _interrupted else "FEIL")
                    file_logger.log(f"  {_tag}: {result.message}", lvl)
                ctx.metadata["step_results"].append({
                    "id":       op.operation_id,
                    "label":    op.label,
                    "category": getattr(op, "category", ""),
                    "success":  result.success,
                    "message":  result.message,
                    "skipped":  False,
                    "elapsed":  elapsed,
                    "index":    i,
                    "interrupted": _interrupted,
                })

                # Steget kan ha endret headeren — vis gjeldende verdier
                if op.operation_id == "repack_siard":
                    _hdr["packing"] = False
                    _hdr["repacked"] = bool(result.success)
                self._header_publish(ctx, _hdr)

                # ── Prosjektfil: lagre status og resultat etter steget ────────
                _status = ("completed" if result.success
                           else "interrupted" if _interrupted else "failed")
                if _pf is not None:
                    try:
                        # Steg kan ha blitt satt inn under kjøringen — synk først,
                        # så checkpoint-posisjonene følger arbeidsflyten
                        _pf.operations = _sync_checkpoints_to_ops(_pf.operations, list(wf))
                        _cp = _pf.at(i, op.operation_id)
                        _data = sanitize_result(result.data or {})
                        if result.success:
                            out_p = (result.data or {}).get("output_path")
                            _pf.mark_completed(_cp, Path(out_p) if out_p else None,
                                               ctx_data=_data, message=result.message,
                                               elapsed=elapsed)
                        else:
                            _pf.mark_failed(_cp, result.message, elapsed=elapsed,
                                            ctx_data=_data, interrupted=_interrupted)
                        _pf.note_step_run(_run_id, op.label, _status)
                        self._project_after_step(_pf, op, result, ctx, _run_id)
                        self._project_save(_pf, _pf_path)
                    except Exception as exc:
                        self._log_queue.put(("log", f"  Prosjektfila kunne ikke "
                                                    f"oppdateres: {exc}", "warn"))
                self._log_queue.put(("project_step_saved", i, _status))
                if result.success and op.operation_id == "repack_siard":
                    self._cleanup_after_repack(ctx, path, _pf, _pf_path)

                # ── Feil eller avbrudd i et innholdssteg: hold igjen ──────────
                if not result.success and _plan is not None and _plan.mode == "pipeline":
                    _kind = _sp.kind if _sp is not None else ""
                    _hold_idx: list = []
                    if op.operation_id == "unpack_siard":
                        _hold_idx = held_after_unpack_failure(_plan)
                    elif _kind == "content" or (_sp is None and getattr(
                            op, "modifies_content", False)):
                        _hold_idx = list(_plan.held_on_content_failure)
                    _hold_ops = [o for o in list(wf)[i + 1:]
                                 if _plan_of.get(id(o)) is not None
                                 and _plan_of[id(o)].index in _hold_idx
                                 and id(o) not in _held]
                    if _hold_ops:
                        _why = (f"«{op.label}» "
                                f"{'ble avbrutt' if _interrupted else 'feilet'}")
                        for _o in _hold_ops:
                            _held[id(_o)] = _why
                        _msg = (f"  ⏸ {_why} — {', '.join(o.label for o in _hold_ops)} "
                                f"holdes igjen. Temp-mappa beholdes, så neste kjøring "
                                f"tar bare det som gjenstår.")
                        self._log_queue.put(("log", _msg, "warn"))
                        if file_logger:
                            file_logger.log(_msg, "warn")

                # Stopp workflowen umiddelbart ved kritisk feil
                if not result.success and getattr(op, "halt_on_failure", False):
                    self._log_queue.put((
                        "workflow_halted",
                        op.label,
                        result.message,
                    ))
                    if _rep:
                        _rep.halted(op.label, result.message)
                    break

                # Avbrutt midt i steget: vent på «▶ Fortsett» (steget fortsetter
                # der det stoppet) eller «Avslutt» (prosjektet er lagret)
                if _interrupted:
                    if self._wait_after_stop(i, op.label, interrupted=True):
                        # Prøv steget igjen — og slipp stegene som ble holdt igjen
                        for _o in list(wf)[i + 1:]:
                            _held.pop(id(_o), None)
                        continue
                    _aborted = True
                    break
                i += 1

            if _aborted:
                _msg = ("Kjøringen ble avsluttet. Prosjektet er lagret — kjør "
                        "arbeidsflyten igjen for å fortsette der den stoppet.")
                self._log_queue.put(("log", _msg, "warn"))
                if file_logger:
                    file_logger.log(_msg, "warn")

            # Stoppet før sammenpakking: skriv ventende header-endringer inn i
            # den utpakkede mappa, så de er med når prosjektet kjøres videre
            if _hdr["can_edit"] and not _hdr["repacked"]:
                self._header_apply_pending(ctx, file_logger, premis_logger,
                                           finishing=True)
            self._header_publish(ctx, _hdr, done=True)

            run.end_time = time.time()
            # Skriv PREMIS-proveniensfil før loggen avsluttes (slik at en
            # eventuell speiling rekker å nå kjøreloggen).
            if premis_logger and premis_logger.has_events():
                try:
                    _pp = premis_logger.finalize(ctx.siard_path, ctx)
                    if _pp and file_logger:
                        file_logger.log(f"PREMIS-proveniens skrevet: {_pp.name}", "info")
                except Exception:
                    pass
            _outcome = ("avbrutt" if _aborted else "foreløpig" if _held
                        else "ok" if run.success else "feil")
            if _pf is not None and _run_id:
                _pf.end_run(_run_id, _outcome)
                self._project_save(_pf, _pf_path)
            if file_logger:
                if _held:
                    file_logger.log(f"Holdt igjen: {len(_held)} steg — kjør arbeidsflyten "
                                    f"igjen når feilen er rettet", "warn")
                file_logger.log(
                    f"Fullført: {'SUKSESS' if run.success else 'FEIL'}  ({run.elapsed:.2f}s)")
                file_logger.__exit__(None, None, None)
                self._log_queue.put(("log_saved", str(file_logger.log_path)))

            self._log_queue.put(("done_one", run, q_idx, len(filer), path))
            if _rep:
                _rep.file_done(q_idx, len(filer), run.success)
            if _aborted:
                break

        self._remote_finish()
        self._log_queue.put(("queue_done", None))

    # ── Prosjekt og kjøreplan (kalles fra kjøretråden) ────────────────────────

    def _remembered(self, pf, pf_path, key: str, label: str, ask_fn):
        """Spørsmål under kjøring som huskes i prosjektfila.

        Er spørsmålet besvart i en tidligere kjøring av samme kilde-SIARD,
        brukes svaret (og det logges) — ellers spørres det, og svaret lagres.
        «Kjør dette steget på nytt» glemmer stegets svar."""
        if pf is None:
            return ask_fn
        from siard_workflow.core.project_file import source_fingerprint
        fp = source_fingerprint(pf.source_siard)

        def _wrapped(*args, **kwargs):
            prev = pf.get_answer(key, fp)
            if prev is not None:
                shown = ("ja" if prev is True else "nei" if prev is False
                         else ", ".join(map(str, prev)) if isinstance(prev, list)
                         else str(prev))
                msg = f"  Bruker svaret fra forrige kjøring ({label}): {shown}"
                self._log_queue.put(("log", msg, "info"))
                fl = self._conv_ctx.metadata.get("file_logger") if self._conv_ctx else None
                if fl:
                    fl.log(msg, "info")
                return prev
            ans = ask_fn(*args, **kwargs)
            if ans is not None and isinstance(ans, (bool, str, int, float, list, tuple)):
                pf.set_answer(key, list(ans) if isinstance(ans, tuple) else ans, fp)
                self._project_save(pf, pf_path)
            return ans
        return _wrapped


    def _project_for_run(self, path: Path, wf, pf_arg=None, pf_path_arg=None):
        """Prosjektfila for `path`: lastet prosjekt, eksisterende .siardwf ved
        siden av fila, eller ny. Checkpoints synkes posisjonelt mot `wf`."""
        from siard_workflow.core.project_file import ProjectFile
        pf, pf_path = None, None
        if pf_arg is not None and Path(pf_arg.source_siard) == path:
            pf, pf_path = pf_arg, pf_path_arg
        else:
            pf_path = path.with_suffix(".siardwf")
            if pf_path.exists():
                try:
                    pf = ProjectFile.load(pf_path)
                    if Path(pf.source_siard) != path:
                        pf = None
                except Exception:
                    pf = None
            if pf is None:
                pf = ProjectFile.from_ops(path, list(wf))
        pf.operations = _sync_checkpoints_to_ops(pf.operations, list(wf))
        try:
            pf.save(pf_path)
        except Exception as exc:
            self._log_queue.put(("log", f"  Prosjektfila kan ikke lagres ({exc}) — "
                                        f"kjøringen kan ikke gjenopptas", "warn"))
            return None, None
        return pf, pf_path

    @staticmethod
    def _project_workdir(pf, ops) -> "Path | None":
        """Temp-mappa fra en fullført «Pakk ut», hvis den fortsatt finnes."""
        for cp, op in zip(pf.operations, ops):
            if op.operation_id == "unpack_siard" and cp.status == "completed":
                ext = (cp.ctx_data or {}).get("extracted_path") or \
                    (pf.workdir or {}).get("path")
                if ext and Path(ext).is_dir():
                    return Path(ext)
        return None

    @staticmethod
    def _project_output(pf, ops) -> "str | None":
        """Siste «Pakk sammen»-resultat på disk."""
        for cp, op in zip(pf.operations, ops):
            if (op.operation_id == "repack_siard" and cp.status == "completed"
                    and cp.output_siard and Path(cp.output_siard).exists()):
                return cp.output_siard
        return None

    def _plan_for_run(self, pf, wf, path: Path):
        """Kjøreplan for fila (None uten prosjekt)."""
        from siard_workflow.core.run_plan import build_run_plan
        if pf is None:
            return None, None
        ops = list(wf)
        workdir = self._project_workdir(pf, ops)
        base = (getattr(self, "_run_plan_bases", None) or {}).get(str(path))
        plan = build_run_plan(ops, pf.operations, workdir is not None,
                              self._project_output(pf, ops), base)
        return plan, workdir

    def _project_save(self, pf, pf_path) -> None:
        if pf is None or pf_path is None:
            return
        try:
            pf.save(pf_path)
        except Exception as exc:
            if not getattr(self, "_pf_save_warned", False):
                self._pf_save_warned = True
                self._log_queue.put(("log", f"  Prosjektfila kunne ikke lagres: {exc}",
                                     "warn"))

    @staticmethod
    def _restore_skipped_step(ctx, op, cp, workdir) -> str:
        """Gjenopprett et fullført stegs resultat i ctx (steget hoppes over)."""
        data = dict(cp.ctx_data or {})
        if op.operation_id == "unpack_siard":
            if workdir is not None:
                ctx.extracted_path = Path(workdir)
            ctx.set_result("unpack_siard", data)
            return Path(workdir).name if workdir is not None else ""
        if data:
            ctx.set_result(op.operation_id, data)
        out = Path(cp.output_siard) if cp.output_siard else None
        # Bare «Pakk sammen» (og steg uten pipeline) gir en ny SIARD som neste
        # steg skal bruke — i pipeline peker f.eks. BLOB-stegets output_path på
        # en fil som aldri skrives (eller en gammel)
        if out is not None and out.exists() and (
                op.operation_id == "repack_siard" or ctx.extracted_path is None
                and getattr(type(op), "produces_siard", False)
                and op.operation_id not in ("blob_convert",)):
            ctx.siard_path = out
        return out.name if out is not None else ""

    def _project_after_step(self, pf, op, result, ctx, run_id) -> None:
        """Prosjektdata utover stegets egen status (temp-mappe, filtyper, feillogger)."""
        data = result.data or {}
        if op.operation_id == "unpack_siard" and result.success:
            pf.workdir = {
                "path":       str(data.get("extracted_path") or ctx.extracted_path or ""),
                "base":       "output" if ctx.metadata.get("repack_name_base") else "original",
                "base_siard": str(ctx.siard_path),
                "run_id":     run_id,
            }
        elif op.operation_id == "repack_siard" and result.success:
            if not op.params.get("keep_temp"):
                pf.workdir = {}
        elif op.operation_id == "blob_convert":
            run_fc = dict(ctx.metadata.get("format_counts") or {})
            if data.get("resumed_files"):
                merged = dict(pf.format_counts or {})
                for k, v in run_fc.items():
                    merged[k] = merged.get(k, 0) + v
                pf.format_counts = merged
            else:
                pf.format_counts = run_fc
            ctx.metadata["format_counts"] = dict(pf.format_counts)
            fs = data.get("failure_summary") or {}
            if not data.get("resumed_files"):
                # Ny konvertering fra start: loggene fra tidligere forsøk gjelder
                # ikke lenger (DIAS/depotrapporter tar bare med gjeldende)
                for _e in pf.failure_files:
                    _e["superseded"] = True
            pf.add_failure_files(run_id,
                                 blob_csv=ctx.metadata.get("blob_csv_path"),
                                 error_log=fs.get("log_path") if isinstance(fs, dict) else None,
                                 error_csv=fs.get("csv_path") if isinstance(fs, dict) else None)

    def _cleanup_after_repack(self, ctx, path: Path, pf=None, pf_path=None) -> None:
        """Etter vellykket «Pakk sammen»: BLOB-progresjon og Siegfried-cache
        trengs ikke lenger (de gjaldt den utpakkede mappa)."""
        for key in ("blob_progress_file", "blob_sf_cache_file"):
            p = ctx.metadata.get(key)
            if not p:
                continue
            try:
                if Path(p).exists():
                    Path(p).unlink()
                    self._log_queue.put(("log", f"  Ryddet: {Path(p).name}", "muted"))
            except Exception:
                pass

    def _wait_after_stop(self, i: int, label: str, interrupted: bool = False) -> bool:
        """Blokker kjøretråden etter «⏹ Stopp» til brukeren velger.

        True = «▶ Fortsett» (fortsett med steget / neste steg),
        False = «⏹ Avslutt» (kjøringen avsluttes; prosjektet er lagret)."""
        self._log_queue.put(("workflow_step_stopped", i, label, interrupted))
        self._step_resume_event.clear()
        self._step_resume_event.wait()          # blokkerer bakgrunnstråden
        if getattr(self, "_abort_requested", False):
            self._abort_requested = False
            return False
        self._stop_event.clear()
        if self._conv_ctx is not None:
            self._conv_ctx.metadata["stopped"] = False
        self._log_queue.put(("workflow_step_resumed",))
        return True

    # ── SIARD-header-panelet (kalles fra kjøretråden) ─────────────────────────

    @staticmethod
    def _header_meta(ctx) -> "Path | None":
        """header/metadata.xml i den utpakkede mappa (None før utpakking/etter opprydding)."""
        from siard_workflow.core.siard_header import metadata_path
        ext = getattr(ctx, "extracted_path", None)
        return metadata_path(Path(ext)) if ext and Path(ext).is_dir() else None

    def _header_publish(self, ctx, hdr: dict, first: bool = False,
                        done: bool = False) -> None:
        """Les headeren (utpakket metadata.xml, ellers SIARD-fila) og vis den i panelet."""
        from siard_workflow.core.siard_header import read_header
        meta = None if hdr["repacked"] else self._header_meta(ctx)
        info = read_header(meta) if meta else read_header(ctx.siard_path)
        packing = hdr.get("packing", False)
        editable = (hdr["can_edit"] and not hdr["repacked"] and not packing
                    and not done and info is not None)
        if info is None:
            note = "Fant ikke header/metadata.xml."
        elif not hdr["can_edit"]:
            note = ("Skrivebeskyttet: endringer krever «Pakk ut SIARD» og "
                    "«Pakk sammen SIARD» i arbeidsflyten.")
        elif hdr["repacked"]:
            note = f"Pakket inn som header i {ctx.siard_path.name}."
        elif done and meta:
            note = ("Kjøringen stoppet før sammenpakking. Bekreftede endringer "
                    "ligger i den utpakkede mappa og tas med når prosjektet "
                    "kjøres videre.")
        elif done:
            note = "Kjøringen er ferdig."
        elif packing:
            note = "Pakker sammen SIARD — headeren er låst."
        else:
            note = ("Trykk «Bekreft endringer» for å ta endringene med. De skrives "
                    "inn " + ("etter utpakkingen" if meta is None else
                              "i metadata.xml før neste steg")
                    + " og pakkes med i den nye SIARD-fila.")
        self._log_queue.put(("header_show", first, info, ctx.siard_path.name,
                             editable, note))

    def _ask_header_unconfirmed(self, unconf: dict) -> bool:
        """Arbeidstråden, før «Pakk sammen SIARD»: ta med ubekreftede header-endringer?"""
        from siard_workflow.core.siard_header import FIELD_BY_TAG

        def _short(v: str) -> str:
            v = " ".join(v.split())
            return v if len(v) <= 120 else v[:117] + "…"
        lines = "\n".join(
            f"   • {FIELD_BY_TAG[t].label if t in FIELD_BY_TAG else t}: «{_short(v)}»"
            for t, v in unconf.items())
        return self._ask_yes_no(
            "Ubekreftede endringer i SIARD-header",
            "Det er gjort endringer i SIARD-headeren som ikke er bekreftet med "
            f"«Bekreft endringer»:\n\n{lines}\n\n"
            "Lagre endringer i SIARD-header ved pakking av ny siard?",
            "Ja, lagre endringene", "Nei, forkast")

    def _on_header_confirm(self, values: dict) -> None:
        """Hovedtråden: «Bekreft endringer» i header-panelet."""
        self._log(f"  SIARD-header: {len(values)} endring(er) bekreftet "
                  f"({', '.join(values)}) — skrives inn før neste steg", "info")

    def _header_apply_pending(self, ctx, file_logger, premis_logger,
                              before_repack: bool = False,
                              finishing: bool = False) -> bool:
        """Skriv endringer fra header-panelet inn i den utpakkede metadata.xml.
        True når noe ble skrevet."""
        meta = self._header_meta(ctx)
        if meta is None:
            return False                  # ikke utpakket ennå — endringene venter
        panel = self.header_dock.panel

        def _w(msg: str, lvl: str = "info") -> None:
            self._log_queue.put(("log", msg, lvl))
            if file_logger:
                file_logger.log(msg, lvl)

        # Bare bekreftede endringer («Bekreft endringer») skrives automatisk.
        # Ubekreftede: spør før sammenpakkingen (også på mobil); ved avbrudd
        # lagres de ikke.
        values = panel.take_confirmed()
        if before_repack:
            for tag, err in panel.invalid_fields().items():
                _w(f"  SIARD-header: <{tag}> har ugyldig verdi i panelet og tas "
                   f"ikke med ({err})", "warn")
            unconf = panel.unconfirmed()
            if unconf:
                if self._ask_header_unconfirmed(unconf):
                    values.update(panel.take_unconfirmed())
                    _w("  SIARD-header: ubekreftede endringer lagres (valgt før "
                       "sammenpakking)", "info")
                else:
                    panel.take_unconfirmed()
                    _w("  SIARD-header: ubekreftede endringer forkastet (valgt før "
                       f"sammenpakking): {', '.join(unconf)}", "warn")
        elif finishing:
            unconf = panel.unconfirmed()
            if unconf:
                _w("  SIARD-header: endringer som ikke var bekreftet, er ikke "
                   f"lagret: {', '.join(unconf)}", "warn")
        if not values:
            return False
        from siard_workflow.core.siard_header import apply_header_file
        try:
            changes = apply_header_file(meta, values)
        except Exception as exc:
            _w(f"  SIARD-header kunne ikke skrives: {exc}", "error")
            return False
        done = [c for c in changes if not c.startswith("IKKE")]
        if done:
            _w("  SIARD-header endret i panelet:", "info")
        for c in changes:
            _w(f"    {c}", "warn" if c.startswith("IKKE") else "info")
        if done and premis_logger:
            from types import SimpleNamespace
            detail = ("Manuell endring av SIARD-header (header/metadata.xml): "
                      + "; ".join(done))
            premis_logger.record(
                SimpleNamespace(operation_id="header_edit",
                                premis_event_type="Adjustment",
                                premis_event_label="metadataendring (manuell)",
                                premis_detail=lambda _r, _c: detail),
                SimpleNamespace(success=True, message=detail), ctx)
        return bool(done)

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
                elif kind == "project_skip":
                    _, idx, label, out_name = item
                    suffix = f" → {out_name}" if out_name else ""
                    self._log(f"      [✓] Allerede fullført — hoppet over{suffix}", "success")
                    self.workflow_panel.set_step_status(idx, "completed")
                    self.progress_panel.reset()
                    needs_redraw = True
                elif kind == "project_step_saved":
                    _, idx, status = item
                    self.workflow_panel.set_step_status(idx, status)
                    needs_redraw = True
                elif kind == "project_temp_lost":
                    _, rerun_labels = item
                    from tkinter import messagebox
                    ops_str = "\n".join(f"  • {n}" for n in rerun_labels)
                    self._log("!" * 56, "error")
                    self._log("  ADVARSEL: Temp-mappe fra forrige kjøring er borte", "error")
                    self._log("  Følgende steg kjøres på nytt:", "warn")
                    for lbl in rerun_labels:
                        self._log(f"    • {lbl}", "warn")
                    self._log("  Øvrige steg (SHA-256, XML-validering m.m.) hoppes over.", "info")
                    self._log("!" * 56, "error")
                    messagebox.showwarning(
                        "Temp-mappe borte — delvis gjenopptakelse",
                        "Temp-mappen fra forrige kjøring finnes ikke lenger.\n\n"
                        f"Følgende steg må kjøres på nytt:\n{ops_str}\n\n"
                        "Steg uten temp-avhengighet (SHA-256, XML-validering o.l.) "
                        "hoppes fortsatt over.")
                    needs_redraw = True
                elif kind == "project_checkpoint_path":
                    _, cp_path = item
                    self._log(f"  Checkpoint: {cp_path}", "muted")
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
                elif kind == "header_show":
                    _, first, info, name, editable, note = item
                    if first:
                        self.header_dock.load(info or {"values": {}}, name, editable, note)
                    elif info is not None:
                        self.header_dock.refresh(info, editable, note)
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
                elif kind == "siardmapper_dialog":
                    (_, matches, siard_path, extracted_path,
                     json_path, suggestion_map,
                     done_event, result_holder, remote_box) = item
                    from gui.siardmapper_dialog import SiardMapperDialog, build_dialog_tables
                    tables = build_dialog_tables(matches)
                    dlg = SiardMapperDialog(
                        self, tables, siard_path, extracted_path,
                        json_path=json_path,
                        suggestion_map=suggestion_map or {},
                    )
                    remote_box["dlg"] = dlg          # svar fra mobil (_siardmapper_dialog_cb)
                    self.wait_window(dlg)
                    result_holder[0] = dlg.get_result()
                    done_event.set()
                    needs_redraw = True
                elif kind == "workflow_step_stopped":
                    step_label = item[2]
                    if len(item) > 3 and item[3]:
                        self._log(
                            f"  ⏸ «{step_label}» ble avbrutt. «▶ Fortsett» fortsetter "
                            "steget der det stoppet; «⏹ Avslutt» lagrer prosjektet "
                            "så du kan fortsette senere.", "warn")
                    else:
                        self._log(
                            f"  ⏸ Stoppet — neste steg: {step_label}. "
                            "Klikk '▶ Fortsett' for å gjenoppta, eller "
                            "'⏹ Avslutt' for å avslutte (prosjektet er lagret).", "warn")
                    self.progress_panel.set_step_stopped(True)
                    needs_redraw = True
                elif kind == "workflow_step_resumed":
                    self.progress_panel.set_step_stopped(False)
                    needs_redraw = True
                elif kind == "queue_done":
                    self._running = False
                    self._step_stopped = False
                    self.workflow_panel.set_running(False)
                    self.workflow_panel.highlight_step(-1)
                    self.progress_panel.reset()   # skjuler panel og Stopp-knapp
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
                        kw["done"], kw["total"], kw.get("label", ""),
                        text=kw.get("text", ""), eta=kw.get("eta", ""))
                    needs_redraw = True
                elif kind == "conv_error":
                    _, kw = item
                    self._log(f"  [FEIL] {kw['file']}: {kw['error']}", "error")
                elif kind == "log":
                    # ("log", melding, nivå) — fra arbeidstråden og livstegnet
                    _, msg, lvl = (tuple(item) + ("info",))[:3]
                    self._log(msg, {"ok": "success", "feil": "error"}.get(lvl, lvl))
                    needs_redraw = True
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
        self._step_stopped = True
        self._step_resume_event.clear()
        if self._conv_ctx:
            self._conv_ctx.metadata["stopped"] = True
            self._conv_ctx.metadata["paused"]  = False
        self.progress_panel.set_stopped()
        self._log("Stopper workflow ...", "warn")

    def _conv_resume(self):
        """Gjenoppta workflow etter steg-nivå stopp (et avbrutt steg fortsetter
        der det stoppet)."""
        self._step_stopped = False
        self._abort_requested = False
        self._stop_event.clear()
        if self._conv_ctx is not None:
            self._conv_ctx.metadata["stopped"] = False
        self._step_resume_event.set()
        self._log("Gjenopptar workflow ...", "info")

    def _conv_abort(self):
        """Avslutt kjøringen etter stopp. Prosjektet er lagret, så neste kjøring
        fortsetter der denne stoppet."""
        self._abort_requested = True
        self._step_stopped = False
        self._step_resume_event.set()
        self._log("Avslutter kjøringen — prosjektet er lagret", "warn")

    def _ask_add_standardize_ext(self, count: int) -> bool:
        """
        Kalles fra bakgrunnstråden (UnpackSiardOperation) når ikke-standard
        LOB-filer oppdages. Poster spørsmål til hoved-tråden og blokkerer til svar.
        """
        text = (f"{count} LOB-fil(er) har ikke .bin/.txt-endelse eller "
                f"heter ikke recordN.\n\n"
                "Ikke-standard endelser kan gi utfordringer i KDRS Søk & Vis; "
                "andre filnavn enn recordN underkjennes av DBPTK-validatoren.\n\n"
                "Vil du legge til «Standardiser filendelser» i arbeidsflyten?")
        return self._ask_yes_no("Ikke-standard LOB-filer funnet", text,
                                "Ja, legg til", "Nei")

    def _ask_yes_no(self, title: str, text: str, yes_text: str = "Ja",
                    no_text: str = "Nei") -> bool:
        """
        Ja/nei-spørsmål fra arbeidstråden i et vindu som også kan besvares fra
        mobilen (erstatter messagebox.askyesno, som ikke kan lukkes utenfra).
        """
        import threading as _threading
        done = _threading.Event()
        box: dict = {"result": False}

        def _show():
            dlg = _PipelineSuggestionDialog(self, text, heading=title, yes_text=yes_text,
                                            no_text=no_text, cancel=False)
            dlg.title(title)
            box["dlg"] = dlg
            self.wait_window(dlg)
            box["result"] = dlg.result == "ja"
            done.set()

        self.after(0, _show)
        self._remote_wait(done, title, text, [("ja", yes_text), ("nei", no_text)],
                          lambda k: self._answer_suggestion(box.get("dlg"), k))
        return box["result"]

    def _ask_add_schema_selector(self, schema_count: int) -> bool:
        """
        Kalles fra UnpackSiardOperation når > 1 schema oppdages.
        Spør om Schema-velger skal legges til arbeidsflyten.
        """
        return self._ask_yes_no(
            "Flere schemas funnet",
            f"SIARD-arkivet inneholder {schema_count} schemas.\n\n"
            "Vil du legge til «Schema-velger» i arbeidsflyten slik at du "
            "kan velge hvilke schemas som skal være med i resultatet?",
            "Ja, legg til", "Nei")

    def _schema_select_dialog_cb(self, schemas: list) -> "list[str] | None":
        """
        Kalles fra SchemaSelectorOperation. Viser modal dialog der operatør
        velger schemas. Returnerer liste av navn å beholde, eller None ved
        avbryt.
        """
        import threading as _threading
        from gui.schema_selector_dialog import SchemaSelectorDialog
        done = _threading.Event()
        box: dict = {"result": None}

        def _cb(selected):
            box["result"] = selected
            done.set()

        def _show():
            box["dlg"] = SchemaSelectorDialog(self, schemas, on_confirm=_cb)

        def _remote(key: str) -> bool:
            dlg = box.get("dlg")
            if dlg is None or not dlg.winfo_exists():
                return False
            if key == "alle":
                dlg._select_all()
                dlg._on_ok()
            else:
                dlg._on_cancel()
            return True

        names = [str(s.get("name", "")) for s in schemas]
        text = (f"SIARD-arkivet har {len(names)} schemas: " + ", ".join(names[:20])
                + (" …" if len(names) > 20 else "")
                + "\n\nBehold alle = alle schemas tas med. Et utvalg må gjøres på PC-en.")
        self.after(0, _show)
        self._remote_wait(done, "Velg schemas", text,
                          [("alle", "Behold alle"), ("avbryt", "Avbryt")], _remote)
        return box["result"]

    def _anonymize_preview_cb(self, summary: dict) -> bool:
        """
        Kalles fra AnonymizeOperation. Viser modal forhåndsvisning av
        identifiserte PII-felter og LOB-bytte. Returnerer True (anonymiser)
        eller False (avbryt).
        """
        import threading as _threading
        from gui.anonymize_preview_dialog import AnonymizePreviewDialog
        done = _threading.Event()
        box: dict = {"result": False}

        def _cb(confirmed):
            box["result"] = bool(confirmed)
            done.set()

        def _show():
            box["dlg"] = AnonymizePreviewDialog(self, summary, on_confirm=_cb)

        def _remote(key: str) -> bool:
            dlg = box.get("dlg")
            if dlg is None or not dlg.winfo_exists():
                return False
            (dlg._confirm if key == "ja" else dlg._cancel)()
            return True

        self.after(0, _show)
        self._remote_wait(done, "Anonymisering: bekreft",
                          "Forhåndsvisningen av anonymiseringen er klar på PC-en "
                          "(felt som endres og LOB-bytte).\n\nAnonymiser = gjennomfør "
                          "som vist. Avbryt = ingen endringer.",
                          [("ja", "Anonymiser"), ("nei", "Avbryt")], _remote)
        return box["result"]

    def _ask_gs_postprocess(self, est: dict) -> bool:
        """
        Kalles fra BLOB-konverteringen (arbeidstråd) etter testen på et utvalg:
        vis målt besparelse for utvalget og beregnet besparelse for alle
        PDF/A-filene, og la brukeren velge. True = etterbehandle alle.
        """
        from siard_workflow.operations.blob_convert_operation import _fmt_size

        skipped = "".join(f"\n   • {n} beholdt: {r}"
                          for r, n in est.get("skipped", {}).items())
        mins = max(1, round(est.get("est_seconds", 0) / 60))
        pct = f"{est['sample_saving_pct']:.1f}".replace(".", ",")   # norsk desimalkomma
        n_total = f"{est['n_total']:,}".replace(",", " ")           # 48 210
        labels = est.get("origin_labels", {})
        groups = "".join(
            f"\n   • {f'{n:,}'.replace(',', ' ')} {labels.get(o, o)}"
            for o, n in est.get("origins", {}).items())
        msg = (
            "Dokumentkonverteringen er ferdig. PDF-filene kan etterbehandles med "
            f"Ghostscript {est.get('gs_version', '')} for å spare lagringsplass:"
            f"{groups}\n\n"
            + ("MERK: «Tillat JPEG-omkoding av bilder» er slått på — bilder kan "
               "kodes om til JPEG (tapsbeheftet). Tekst, "
               if est.get("allow_jpeg") else
               "Innholdet endres ikke: bilder komprimeres ikke med tap, og tekst, ")
            + "sidetall og utseende kontrolleres for hver fil. Filer som ikke blir "
            "mindre — eller er signert, krypterte, har vedlegg eller skjemafelt — "
            "beholdes. PDF-er som ikke var PDF/A, blir PDF/A.\n\n"
            f"Test på {est['n_sample']} filer:\n"
            f"   {_fmt_size(est['sample_before'])} → {_fmt_size(est['sample_after'])}  "
            f"({pct} % mindre, {est['n_ok']} av {est['n_sample']} filer "
            f"forbedret){skipped}\n\n"
            f"Beregnet for alle {n_total} filer:\n"
            f"   {_fmt_size(est['total_before'])} → ca. {_fmt_size(est['est_after'])}  "
            f"(spart ca. {_fmt_size(est['est_saved'])})\n"
            f"   Tidsbruk: ca. {mins} min\n\n"
            "Vil du etterbehandle alle PDF-filene?\n"
            "(Nei = ingen filer endres.)")
        return self._ask_yes_no("Ghostscript-etterbehandling", msg,
                                "Ja, etterbehandle alle", "Nei, ingen endres")

    def _ask_unknown_format(self, ext: str, antall: int, eksempler: str,
                            mime: str) -> str:
        """
        Kalles fra BLOB-konverteringen (arbeidstråd) for hvert ukjent, gyldig
        filformat. Returnerer «konverter», «behold» eller «hopp_over». Uten svar
        innen fristen (5 min, eller 30 min med mobilvarsling) beholdes filene.
        """
        import threading as _threading
        done = _threading.Event()
        box: dict = {"result": "behold"}
        timeout = 1800 if self._remote_hub is not None else 300
        e = ext.upper()
        text = (f"Ukjent filformat funnet: .{e}\n"
                f"MIME: {mime or 'ukjent'}\n"
                f"Antall filer: {antall}\n"
                f"Eksempler: {eksempler}\n\n"
                f"Hva skal gjøres med .{e}-filer?\n\n"
                f"Konverter: forsøk konvertering til PDF/A (formatet legges til i "
                f"listen over formater som konverteres).\n"
                f"Behold: behold originalformatet (legges til i listen over formater "
                f"som beholdes).\n"
                f"Avbryt: hopp over filene i denne kjøringen.\n\n"
                f"Uten svar innen {timeout // 60} minutter beholdes filene.")

        def _show():
            dlg = _PipelineSuggestionDialog(self, text, heading=f"Ukjent format: .{e}",
                                            yes_text="Konverter", no_text="Behold",
                                            cancel=True)
            dlg.title(f"Ukjent format: .{e}")
            box["dlg"] = dlg
            self.wait_window(dlg)
            box["result"] = {"ja": "konverter", "nei": "behold"}.get(dlg.result, "hopp_over")
            done.set()

        self.after(0, _show)
        self._remote_wait(done, f"Ukjent format: .{e}", text,
                          [("ja", "Konverter"), ("nei", "Behold"), ("avbryt", "Hopp over")],
                          lambda k: self._answer_suggestion(box.get("dlg"), k),
                          timeout=timeout)
        if not done.is_set():                        # fristen gikk ut
            self.after(0, lambda: self._answer_suggestion(box.get("dlg"), "nei"))
            return "behold"
        return box["result"]

    def _ask_dias_output_dir(self, current_dir, needed_bytes: int,
                              free_bytes: int) -> "str | None":
        """
        Kalles fra DiasPackageOperation når mål-disken ikke har nok plass.
        Viser dialog som ber operatør velge ny lokasjon, eller avbryte.

        Returnerer ny absolutt sti, eller None ved avbryt.
        """
        import threading as _threading
        import shutil as _shutil
        from tkinter import filedialog, messagebox

        result_event  = _threading.Event()
        result_holder: list = [None]

        def _fmt(n: int) -> str:
            for unit, factor in (("TB", 1024**4), ("GB", 1024**3),
                                 ("MB", 1024**2), ("kB", 1024)):
                if n >= factor:
                    return f"{n / factor:.1f} {unit}"
            return f"{n} B"

        msg = (
            f"Ikke nok plass på mål-disk for DIAS-pakke.\n\n"
            f"Mål-mappe:    {current_dir}\n"
            f"Estimert størrelse: {_fmt(needed_bytes)}\n"
            f"Ledig plass:        {_fmt(free_bytes)}\n\n"
            "Vil du velge en annen lagringsmappe?\n"
            "(Avbryt vil stoppe DIAS-pakking.)"
        )
        box: dict = {}

        def _show_dialog():
            # 1. Vis advarsel med info om plass (kan besvares «Avbryt» fra mobilen)
            dlg = _PipelineSuggestionDialog(
                self, msg, heading="Ikke nok diskplass for DIAS-pakke",
                yes_text="Velg annen mappe …", no_text="Avbryt pakking", cancel=False)
            dlg.title("Ikke nok diskplass for DIAS-pakke")
            box["dlg"] = dlg
            self.wait_window(dlg)
            box.pop("dlg", None)
            if dlg.result != "ja":
                result_event.set()
                return

            # 2. Filvelger — sjekk plass igjen før retur
            while True:
                chosen = filedialog.askdirectory(
                    title="Velg lagringsmappe for DIAS-pakke",
                    parent=self)
                if not chosen:
                    # Brukeren avbrøt filvelger
                    result_event.set()
                    return
                try:
                    chosen_free = _shutil.disk_usage(chosen).free
                except Exception:
                    chosen_free = 0
                if chosen_free < needed_bytes:
                    retry = messagebox.askyesno(
                        "Fortsatt ikke nok plass",
                        f"Valgt mappe har bare {_fmt(chosen_free)} ledig — "
                        f"trenger {_fmt(needed_bytes)}.\n\n"
                        "Vil du velge en annen mappe?",
                        parent=self)
                    if not retry:
                        result_event.set()
                        return
                    continue
                result_holder[0] = chosen
                result_event.set()
                return

        self.after(0, _show_dialog)
        # Fra mobilen kan pakkingen bare avbrytes — ny mappe velges på PC-en
        self._remote_wait(result_event, "Ikke nok diskplass for DIAS-pakke",
                          msg.replace("Vil du velge en annen lagringsmappe?\n",
                                      "En annen lagringsmappe må velges på PC-en.\n"),
                          [("nei", "Avbryt pakking")],
                          lambda k: self._answer_suggestion(box.get("dlg"), k))
        return result_holder[0]

    def _ask_fill_empty_schema_names(self, empties: list,
                                       existing_names: set | None = None
                                       ) -> "dict | None":
        """
        Kalles fra UnpackSiardOperation når <schema><name> er tom i
        metadata.xml. Viser dialog der operatør fyller inn navn.

        existing_names: schema-navn som allerede er i bruk i samme SIARD —
            dialogen validerer mot duplikater.

        Returnerer {schema_index: new_name} eller None ved Avbryt.
        """
        import threading as _threading
        from gui.empty_schema_name_dialog import EmptySchemaNameDialog
        done = _threading.Event()
        box: dict = {"result": None}

        def _cb(fixes):
            box["result"] = fixes
            done.set()

        def _show():
            box["dlg"] = EmptySchemaNameDialog(self, empties, existing_names=existing_names,
                                               on_confirm=_cb)

        def _remote(key: str) -> bool:
            dlg = box.get("dlg")
            if dlg is None or not dlg.winfo_exists():
                return False
            dlg._cancel()                 # None → operasjonen bruker mappenavnene
            return True

        folders = [str(e.get("folder", "")) for e in empties]
        self.after(0, _show)
        self._remote_wait(done, "Schema uten navn",
                          f"{len(folders)} schema(s) mangler navn i metadata.xml "
                          f"(mappe: {', '.join(folders[:10])}).\n\n"
                          "Bruk mappenavn = schemaet får mappenavnet. Andre navn må "
                          "skrives inn på PC-en.",
                          [("mappenavn", "Bruk mappenavn")], _remote)
        return box["result"]

    def _siardmapper_dialog_cb(self, matches, siard_path, extracted_path,
                               json_path=None, suggestion_map=None):
        """
        Kalles fra bakgrunnstråden (SiardMapperOperation) ved delvis JSON-treff.
        Poster dialog-forespørsel til hoved-tråden via _log_queue og blokkerer
        til dialog er lukket.
        """
        result_event  = threading.Event()
        result_holder = [None]
        box: dict = {}
        self._log_queue.put((
            "siardmapper_dialog",
            matches, siard_path, extracted_path,
            json_path, suggestion_map,
            result_event, result_holder, box,
        ))

        def _remote(key: str) -> bool:
            dlg = box.get("dlg")
            if dlg is None or not dlg.winfo_exists():
                return False
            (dlg._save if key == "lagre" else dlg._cancel)()
            return True

        self._remote_wait(result_event, "Metadata-berikelse (SIARDMapper)",
                          "JSON-malen passet bare delvis. Beskrivelsene som ble funnet, "
                          "vises på PC-en.\n\nLagre og fortsett = bruk det som er funnet. "
                          "Avbryt kjøring = stopp berikelsen.",
                          [("lagre", "Lagre og fortsett"), ("avbryt", "Avbryt kjøring")],
                          _remote)
        return result_holder[0]

    def _toggle_theme(self) -> None:
        """
        Bytt mellom lyst og mørkt tema. Lagrer valget i config og starter
        programmet på nytt slik at alle custom-fargede widgets bygges med
        riktige farger. Operatør bekrefter restart i dialog først.

        Hvis en workflow kjører, varsles operatør om at den vil avbrytes.
        """
        from gui.styles import current_theme
        from settings import set_config
        from tkinter import messagebox

        new_mode = "light" if current_theme() == "dark" else "dark"
        mode_no  = "lys" if new_mode == "light" else "mørk"

        # Bygg advarselsmelding (med ekstra advarsel hvis workflow kjører)
        if self._running:
            msg = (
                f"Bytte til {mode_no} tema krever omstart av programmet.\n\n"
                "⚠  En workflow kjører akkurat nå — den vil bli AVBRUTT.\n\n"
                "Trykk OK for å lagre tema-valget og restarte nå.\n"
                "Trykk Avbryt for å beholde gjeldende tema."
            )
            icon = "warning"
        else:
            msg = (
                f"Bytte til {mode_no} tema krever omstart av programmet.\n\n"
                "Trykk OK for å lagre tema-valget og restarte nå.\n"
                "Trykk Avbryt for å beholde gjeldende tema."
            )
            icon = "question"

        if not messagebox.askokcancel(
                "Bytte tema krever omstart", msg,
                icon=icon, parent=self):
            return

        # Lagre nytt tema-valg i config — leses ved oppstart
        try:
            set_config("theme_mode", new_mode)
        except Exception:
            pass

        # Start på nytt
        self._restart_app()

    def _restart_app(self) -> None:
        """
        Start applikasjonen på nytt ved å spawne en ny prosess og avslutte
        gjeldende. Fungerer både for source-kjøring og PyInstaller-bundle.
        """
        import subprocess as _sp
        import sys as _sys
        import os as _os

        try:
            # Bygg argumenter for ny prosess
            if getattr(_sys, "frozen", False):
                # PyInstaller-bundle: kjør .exe direkte
                args = [_sys.executable]
            else:
                # Source-kjøring: python + script + evt. CLI-argumenter
                args = [_sys.executable, *_sys.argv]

            # Spawn ny prosess (frikoblet, så den overlever vår exit)
            _sp.Popen(args, close_fds=False)
        except Exception:
            # Hvis restart-spawn feiler: la appen være som den er
            return

        # Avslutt gjeldende prosess. self.destroy() river ned tk-loopen,
        # men subprocess-tråder kan henge — os._exit garanterer at vi går.
        try:
            self.destroy()
        except Exception:
            pass
        # Gi tk-loopen et øyeblikk å rive ned før vi forcerer exit
        _os._exit(0)

    def _font_scale(self, delta: int) -> None:
        """Juster font-størrelse for alle widgets dynamisk."""
        from gui.styles import FontRegistry
        from settings import save_config
        FontRegistry.scale(delta)
        save_config({"font_offset": FontRegistry.current_offset()})

    def _on_log_show_all(self, show_all: bool) -> None:
        """Callback fra LogPanel når 'Vis alle/Vis siste'-toggle trykkes."""
        if show_all:
            self.log_panel.redraw_all(self._log_entries)

    def _log(self, msg: str, level: str = "info"):
        self.log_panel.append(msg, level)
        self._log_entries.append((level, msg))
