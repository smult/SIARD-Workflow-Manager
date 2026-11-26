"""
gui/siardmapper_dialog.py

Flat redigeringstabell à la original SIARDMapper:
  - Trevisning med native expand/collapse (ttk.Treeview show="tree headings")
  - Kolonner: Navn | Status | JSON-beskrivelse | Egendefinert beskrivelse
  - Inline tk.Entry-overlay på «Egendefinert»-kolonnen; Enter hopper til neste rad
  - Forslag-popup basert på lignende kolonnebeskrivelser
  - Datatabell nederst med rader fra valgt tabell: velg antall, bla med
    Start / Forrige / Neste / Slutt, «Unike» viser bare rader med ulikt innhold
  - Autolagring til JSON etter hver endring
  - Re-match-knapp
"""

from __future__ import annotations

import html as _html_mod
import json
import queue
import threading
import time
import tkinter as tk
import tkinter.font as tkfont
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import ttk
from typing import Dict, List, Optional

import customtkinter as ctk

from gui.styles import COLORS, FONTS, FontRegistry
from siard_workflow.core.fmt import num as _fnum, size as _fsize
from siard_workflow.core.table_rows import (
    Cancelled, RowPage, SiardZip, TableRows, table_opener)


# ── Datamodeller ──────────────────────────────────────────────────────────────

@dataclass
class _ColItem:
    name: str
    norm: str
    json_desc: str
    col_index: int


@dataclass
class _TblItem:
    name: str
    norm: str
    json_desc: str
    folder: str
    matched: bool
    columns: List[_ColItem] = field(default_factory=list)
    schema_folder: str = ""              # content/<schema_folder>/<folder>/
    n_rows: Optional[int] = None         # antall rader ifølge metadata.xml


# ── Inline-editor ─────────────────────────────────────────────────────────────

class _InlineEditor:
    """tk.Entry-overlay plassert over riktig celle. Enter/Tab → neste rad."""

    def __init__(self, tree, col_id, on_commit, on_navigate, on_activate,
                 font=None):
        self._tree       = tree
        self._col_id     = col_id
        self._on_commit  = on_commit
        self._on_nav     = on_navigate
        self._on_activate = on_activate
        self._iid: Optional[str] = None
        self._active = False
        self._placed = None      # sist brukte celle-bbox
        self._watching = False

        self._var = tk.StringVar()
        self._entry = tk.Entry(
            tree,
            textvariable=self._var,
            bg="#1e2535", fg="#d4daf0",
            insertbackground="#4f8ef7",
            selectbackground="#3a70d4", selectforeground="#ffffff",
            relief="flat", font=font or ("Courier New", 10), bd=1,
            highlightthickness=1, highlightbackground="#4f8ef7",
        )
        self._entry.bind("<Return>",    self._on_enter)
        self._entry.bind("<Tab>",       self._on_tab)
        self._entry.bind("<Shift-Tab>", self._on_shift_tab)
        self._entry.bind("<Escape>",    self._on_escape)
        self._entry.bind("<FocusOut>",  self._on_focusout)
        self._entry.bind("<KeyRelease>", self._on_key)

    def show(self, iid: str, value: str):
        self._commit_current()
        self._iid    = iid
        self._active = True
        self._var.set(value)
        self._place()
        self._entry.lift()
        self._entry.focus_set()
        self._entry.select_range(0, "end")
        self._on_activate(iid, self._entry)
        if not self._watching:
            self._watching = True
            self._tree.after(self._WATCH_MS, self._watch)

    _WATCH_MS = 100

    def _watch(self):
        """
        Hold feltet på cellen så lenge det er åpent. Cellen flytter seg eller
        endrer bredde når kolonnekanten dras, vinduet endres, det rulles, eller
        tabeller over åpnes/lukkes — ikke alt dette gir en hendelse å lytte på.
        """
        if not self._active or not self._iid:
            self._watching = False
            return
        try:
            bb = self._tree.bbox(self._iid, self._col_id) or None
        except Exception:
            self._watching = False
            return
        if bb != self._placed:
            self._place()
        self._tree.after(self._WATCH_MS, self._watch)

    def hide(self):
        self._entry.place_forget()
        self._active = False
        self._iid    = None

    def current_iid(self):
        return self._iid

    def reposition(self):
        if self._active and self._iid:
            self._place()

    def _place(self):
        try:
            bb = self._tree.bbox(self._iid, self._col_id)
        except Exception:
            return
        self._placed = bb or None
        if bb:
            x, y, w, h = bb
            self._entry.place(x=x, y=y, width=w, height=h)
        else:
            # Raden er rullet ut av syne — feltet skal ikke bli hengende igjen
            self._entry.place_forget()

    def _commit_current(self):
        if self._iid and self._active:
            self._on_commit(self._iid, self._var.get())

    def _on_enter(self, _=None):
        iid = self._iid
        self._commit_current()
        self._on_nav(iid, +1)
        return "break"

    def _on_tab(self, _=None):
        iid = self._iid
        self._commit_current()
        self._on_nav(iid, +1)
        return "break"

    def _on_shift_tab(self, _=None):
        iid = self._iid
        self._commit_current()
        self._on_nav(iid, -1)
        return "break"

    def _on_escape(self, _=None):
        self.hide()
        self._tree.focus_set()

    def _on_focusout(self, _=None):
        self._tree.after(100, self._delayed_focusout)

    def _delayed_focusout(self):
        if self._active and self._iid:
            try:
                focused = self._entry.focus_get()
            except Exception:
                focused = None
            if focused is not self._entry:
                self._commit_current()
                self.hide()

    def _on_key(self, _=None):
        pass   # suggestions-triggering håndteres av dialogen


# ── Forslags-popup ────────────────────────────────────────────────────────────

class _SuggestionPopup:
    """Liten Listbox under editor-feltet med beskrivelsesforslag."""

    def __init__(self, parent_tree, on_pick, font=None):
        self._tree    = parent_tree
        self._on_pick = on_pick
        self._lb_frame = tk.Frame(parent_tree, bg=COLORS["border"], bd=1)
        self._lb = tk.Listbox(
            self._lb_frame,
            bg="#1e2535", fg="#d4daf0",
            selectbackground=COLORS["accent_dim"],
            selectforeground="#ffffff",
            font=font or ("Courier New", 10),
            height=4, activestyle="none",
            relief="flat", bd=0,
        )
        self._lb.pack(fill="both", expand=True, padx=1, pady=1)
        self._lb.bind("<ButtonRelease-1>", self._on_click)
        self._lb.bind("<Return>",          self._on_click)
        self._visible = False

    def show(self, entry: tk.Entry, suggestions: List[str]):
        if not suggestions:
            self.hide()
            return
        self._lb.delete(0, "end")
        for s in suggestions[:6]:
            short = s if len(s) <= 70 else s[:67] + "…"
            self._lb.insert("end", short)
        self._lb.configure(height=min(len(suggestions), 6))
        # Plasser under entry
        try:
            x = entry.winfo_x()
            y = entry.winfo_y() + entry.winfo_height()
            w = entry.winfo_width()
        except Exception:
            return
        self._lb_frame.place(x=x, y=y, width=w)
        self._lb_frame.lift()
        self._visible = True

    def hide(self):
        if self._visible:
            self._lb_frame.place_forget()
            self._visible = False

    def _on_click(self, _=None):
        sel = self._lb.curselection()
        if sel:
            val = self._lb.get(sel[0])
            self.hide()
            self._on_pick(val)


# ── Hoveddialog ───────────────────────────────────────────────────────────────

class SiardMapperDialog(ctk.CTkToplevel):

    _COL_STATUS = "status"
    _COL_JSON   = "json_desc"
    _COL_CUSTOM = "custom_desc"

    _BG_TBL = "#181e2e"
    _BG_COL = "#0f1118"
    _FG_TBL = "#8ab4f8"
    _CLR_OK  = "#1a3020"
    _CLR_PAR = "#2e2a10"
    _CLR_BAD = "#2e1515"
    _CLR_COK  = "#142010"
    _CLR_CBAD = "#241010"

    def __init__(self, parent, tables: List[_TblItem],
                 siard_path, extracted_path,
                 json_path: Optional[Path] = None,
                 suggestion_map: Optional[Dict[str, List[str]]] = None):
        super().__init__(parent)
        self._tables        = tables
        self._siard         = siard_path
        self._extracted     = extracted_path
        self._json_path     = json_path
        self._smap          = suggestion_map or {}
        self._cancelled     = True

        # Datavisning: én radleser per tabell (husker posisjoner i fila),
        # leste sider, og hvor brukeren står i hver tabell
        self._zip = SiardZip(siard_path)
        self._readers: Dict[str, TableRows] = {}
        self._page_cache: Dict[tuple, RowPage] = {}
        self._table_pos: Dict[str, tuple] = {}
        self._data_tbl: Optional[_TblItem] = None
        self._data_hl: Optional[int] = None
        self._data_page: Optional[RowPage] = None
        self._load_q: "queue.Queue" = queue.Queue()
        self._load_gen = 0
        self._load_waiting: Optional[int] = None
        self._load_polling = False
        self._load_started = 0.0
        self._load_bytes = 0
        self._closed = False

        # Redigeringer: {iid: str}
        self._edits: Dict[str, str] = {}
        # Tabeller markert for sletting fra resultat-SIARD (set av tbl.norm)
        self._marked_for_deletion: set[str] = set()
        # Flat liste for navigasjon
        self._items: List[dict] = []
        # Ferdige tabeller brukeren selv har åpnet (tbl.norm) — lukkes ikke
        # automatisk ved endring
        self._keep_open: set[str] = set()

        # Autolags-sti
        self._save_path: Optional[Path] = None
        if json_path and json_path.exists():
            stem = json_path.stem
            if stem.endswith("_metadata_mal") or stem.endswith("_egendefinert"):
                self._save_path = json_path          # generert eller allerede egendefinert
            else:
                self._save_path = json_path.with_name(stem + "_egendefinert.json")

        # Mal-metadata redigert av bruker (topp-nivå-felt unntatt tables/tabeller).
        # Lastes fra _save_path hvis den allerede finnes (bevarer forrige sessions endringer),
        # ellers fra _json_path (base-malen).
        self._meta_overrides: dict = {}
        _meta_src = (self._save_path
                     if (self._save_path and self._save_path.exists())
                     else json_path)
        if _meta_src and _meta_src.exists():
            try:
                _raw = json.loads(_meta_src.read_text(encoding="utf-8"))
                if isinstance(_raw, dict):
                    self._meta_overrides = {
                        k: v for k, v in _raw.items()
                        if k not in ("tables", "tabeller")
                        and isinstance(v, (str, int, float))
                    }
            except Exception:
                pass

        # Les innstillinger fra config
        try:
            from settings import get_config
            self._show_data = bool(get_config("siardmapper_show_data", True))
            self._data_rows = int(get_config("siardmapper_rows", 5) or 5)
            self._unique = bool(get_config("siardmapper_unique", False))
        except Exception:
            self._show_data = True
            self._data_rows = 5
            self._unique = False
        self._data_rows = max(1, min(self._MAX_ROWS, self._data_rows))

        # Skrift i tabellene og kontrollraden (ttk/tk — følger A−/A+ og
        # Ctrl +/− via _apply_fonts). Grunnstørrelse per skrift.
        _eff = FontRegistry.effective_size
        def _font(base, **kw):
            f = tkfont.Font(root=parent, family="Courier New", size=_eff(base), **kw)
            self._fonts.append((f, base))
            return f
        self._fonts: list = []
        self._f_cell = _font(10)
        self._f_bold = _font(10, weight="bold")
        self._f_del  = _font(10, weight="bold", overstrike=1)
        self._f_head = _font(9, weight="bold")
        self._f_ctrl = _font(9)
        self._f_ctrl_b = _font(9, weight="bold")

        self.title("SIARDMapper — berik metadata")
        self.configure(fg_color=COLORS["bg"])
        self.resizable(True, True)
        self.grab_set()
        self.lift()

        self._build()
        self._apply_style()
        self._populate()
        self._update_stats()
        FontRegistry.add_observer(self._apply_fonts)
        self.after(50, lambda: self.state("zoomed"))   # start maksimert

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build(self):
        self.grid_rowconfigure(1, weight=1)   # PanedWindow tar all plass
        self.grid_columnconfigure(0, weight=1)

        # ── Statistikkrad ─────────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_columnconfigure(1, weight=1)

        self._stats_lbl = ctk.CTkLabel(
            hdr, text="",
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["text"], anchor="w")
        self._stats_lbl.grid(row=0, column=0, padx=14, pady=7, sticky="w")

        save_lbl_text = (f"Autolagres til: {self._save_path.name}"
                         if self._save_path else "Ingen JSON-fil valgt")
        self._save_lbl = ctk.CTkLabel(
            hdr, text=save_lbl_text,
            font=ctk.CTkFont(family=FONTS["mono"], size=9),
            text_color=COLORS["muted"], anchor="center")
        self._save_lbl.grid(row=0, column=1, padx=8, pady=7)

        ctk.CTkButton(
            hdr, text="✎ Endre metadata for mal", height=26, width=190,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"], text_color=COLORS["btn_text"], border_color=COLORS["btn_border"], border_width=1,
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self._edit_meta,
        ).grid(row=0, column=2, padx=(8, 6), pady=7)

        ctk.CTkButton(
            hdr, text="↺ Re-match mot JSON", height=26, width=160,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"], text_color=COLORS["btn_text"], border_color=COLORS["btn_border"], border_width=1,
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self._rematch,
        ).grid(row=0, column=3, padx=(0, 14), pady=7)

        # ── PanedWindow: tabell (topp) + datavisning (bunn) ──────────────────
        self._paned = tk.PanedWindow(
            self, orient="vertical",
            sashwidth=5, sashrelief="flat",
            bg=COLORS["border"],    # sash-fargen = tynn linje
            handlesize=0,           # skjul håndtaks-firkant
        )
        self._paned.grid(row=1, column=0, sticky="nsew", padx=6, pady=(4, 0))

        # ── Topp-pane: hovedtabell ────────────────────────────────────────────
        tbl_frm = tk.Frame(self._paned, bg=COLORS["bg"])
        self._paned.add(tbl_frm, stretch="always", minsize=100)
        tbl_frm.grid_rowconfigure(0, weight=1)
        tbl_frm.grid_columnconfigure(0, weight=1)

        # Kolonner: #0=tree/navn, #1=status, #2=json, #3=custom
        self._tree = ttk.Treeview(
            tbl_frm,
            style="SMap.Treeview",
            columns=(self._COL_STATUS, self._COL_JSON, self._COL_CUSTOM),
            show="tree headings",
            selectmode="browse",
        )
        self._tree.heading("#0",             text="Tabell / kolonne",        anchor="w")
        self._tree.heading(self._COL_STATUS, text="",                        anchor="center")
        self._tree.heading(self._COL_JSON,   text="JSON-beskrivelse",         anchor="w")
        self._tree.heading(self._COL_CUSTOM, text="✎  Egendefinert beskrivelse", anchor="w")
        self._tree.column("#0",              width=250, minwidth=250, stretch=True)
        self._tree.column(self._COL_STATUS,  width=25,  minwidth=25,  stretch=False)
        self._tree.column(self._COL_JSON,    width=305, minwidth=100, stretch=True)
        self._tree.column(self._COL_CUSTOM,  width=308, minwidth=100, stretch=True)

        vsb = ttk.Scrollbar(tbl_frm, orient="vertical",
                             command=lambda *a: (self._tree.yview(*a),
                                                 self._editor.reposition()))
        hsb = ttk.Scrollbar(tbl_frm, orient="horizontal",
                             command=lambda *a: (self._tree.xview(*a),
                                                 self._editor.reposition()))
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tbl_frm.grid_columnconfigure(0, weight=1)

        # Kolonnerekkefølge: #0=tree, #1=status, #2=json, #3=custom
        self._editor = _InlineEditor(
            self._tree, self._COL_CUSTOM,
            on_commit=self._commit,
            on_navigate=self._navigate,
            on_activate=self._on_editor_activate,
            font=self._f_cell,
        )
        self._suggestions = _SuggestionPopup(
            self._tree, on_pick=self._on_suggestion_pick, font=self._f_cell)

        self._tree.bind("<ButtonRelease-1>", self._on_click)
        self._tree.bind("<<TreeviewSelect>>", self._on_tree_sel)
        # Redigeringsfeltet følger cellen straks ved vindusstørrelse og rulling;
        # _InlineEditor._watch fanger resten (f.eks. når kolonnekanten dras)
        self._tree.bind("<Configure>",    lambda _: self._editor.reposition())
        self._tree.bind("<MouseWheel>",
                        lambda _: self._tree.after_idle(self._editor.reposition))
        # En ferdig tabell som brukeren selv åpner, lukkes ikke automatisk igjen
        self._tree.bind("<<TreeviewOpen>>",  self._on_tbl_opened)
        self._tree.bind("<<TreeviewClose>>", self._on_tbl_closed)
        self._tree.bind("<Motion>",       self._on_motion)
        self._tree.bind("<Leave>",        self._hide_tooltip)
        self._tree.bind("<Button-3>",     self._on_right_click)

        # Tooltip-tilstand
        self._tooltip_win:  tk.Toplevel | None = None
        self._tooltip_after: str | None = None
        self._tooltip_cell = (None, None)   # (iid, col)

        # ── Bunn-pane: datatabell (kontrollrad + tabell) ─────────────────────
        data_outer = tk.Frame(self._paned, bg=COLORS["bg"])
        self._paned.add(data_outer, stretch="never", minsize=50, height=180)
        data_outer.grid_rowconfigure(1, weight=1)
        data_outer.grid_columnconfigure(0, weight=1)

        # Kontrollrad: [☐ Vis data] [Antall rader: ___] [☐ Unike]
        #              [⏮ Start] [◀ Forrige N] [Neste N ▶] [Slutt ⏭]  EKSEMPELDATA
        ctrl = tk.Frame(data_outer, bg=COLORS["bg"])
        ctrl.grid(row=0, column=0, sticky="ew", pady=(0, 2))

        self._show_data_var = tk.BooleanVar(value=self._show_data)
        tk.Checkbutton(
            ctrl,
            text="Vis data",
            variable=self._show_data_var,
            command=self._on_show_data_toggle,
            bg=COLORS["bg"], fg=COLORS["text"],
            activebackground=COLORS["bg"], activeforeground=COLORS["accent"],
            selectcolor=COLORS["surface"],
            font=self._f_ctrl_b,
            bd=0, highlightthickness=0,
        ).pack(side="left", padx=(0, 12))

        tk.Label(ctrl, text="Antall rader:",
                 bg=COLORS["bg"], fg=COLORS["muted"],
                 font=self._f_ctrl).pack(side="left")

        # Antallet tas i bruk ved Enter eller når feltet forlates — ikke for
        # hvert tastetrykk
        self._rows_var = tk.StringVar(value=str(self._data_rows))
        self._rows_entry = tk.Entry(
            ctrl,
            textvariable=self._rows_var,
            width=5,
            bg=COLORS["surface"], fg=COLORS["text"],
            insertbackground=COLORS["accent"],
            relief="flat", font=self._f_ctrl, bd=1,
        )
        self._rows_entry.pack(side="left", padx=(4, 0))
        self._rows_entry.bind("<Return>",   self._on_rows_enter)
        self._rows_entry.bind("<KP_Enter>", self._on_rows_enter)
        self._rows_entry.bind("<FocusOut>", self._apply_rows_entry)

        # «Unike»: bare rader med ulikt innhold — ulik verdi i valgt kolonne,
        # eller ulik rad når en tabell er valgt
        self._unique_var = tk.BooleanVar(value=self._unique)
        tk.Checkbutton(
            ctrl,
            text="Unike",
            variable=self._unique_var,
            command=self._on_unique_toggle,
            bg=COLORS["bg"], fg=COLORS["text"],
            activebackground=COLORS["bg"], activeforeground=COLORS["accent"],
            selectcolor=COLORS["surface"],
            font=self._f_ctrl,
            bd=0, highlightthickness=0,
        ).pack(side="left", padx=(10, 10))

        self._nav_btns: Dict[str, tk.Button] = {}
        for key, text in (("start", "⏮ Start"), ("prev", "◀ Forrige"),
                          ("next", "Neste ▶"), ("end", "Slutt ⏭")):
            btn = tk.Button(
                ctrl, text=text,
                command=lambda k=key: self._nav(k),
                bg=COLORS["surface"], fg=COLORS["text"],
                activebackground=COLORS["border"], activeforeground=COLORS["text"],
                disabledforeground=COLORS["muted"],
                relief="flat", bd=0, padx=8, pady=1,
                font=self._f_ctrl, cursor="hand2", takefocus=0,
                state="disabled",
            )
            btn.pack(side="left", padx=(0, 3))
            self._nav_btns[key] = btn
        self._update_nav(None)

        self._data_hdr = tk.Label(
            ctrl, text="   EKSEMPELDATA",
            bg=COLORS["bg"], fg=COLORS["muted"],
            font=self._f_head, anchor="w")
        self._data_hdr.pack(side="left", padx=(9, 0))

        # Dataramme som skjules/vises av avkrysningsboksen
        self._data_frm = tk.Frame(data_outer, bg=COLORS["bg"])
        self._data_frm.grid(row=1, column=0, sticky="nsew")
        self._data_frm.grid_rowconfigure(0, weight=1)
        self._data_frm.grid_columnconfigure(0, weight=1)

        self._data_tree = ttk.Treeview(
            self._data_frm, style="Data.Treeview", show="headings",
            height=min(self._data_rows, 8))
        dvsb = ttk.Scrollbar(self._data_frm, orient="vertical",   command=self._data_tree.yview)
        dhsb = ttk.Scrollbar(self._data_frm, orient="horizontal", command=self._data_tree.xview)
        self._data_tree.configure(yscrollcommand=dvsb.set, xscrollcommand=dhsb.set)
        self._data_tree.grid(row=0, column=0, sticky="nsew")
        dvsb.grid(row=0, column=1, sticky="ns")
        dhsb.grid(row=1, column=0, sticky="ew")
        self._data_cols: List[str] = []

        # Anvend starttilstand fra config
        if not self._show_data:
            self._data_frm.grid_remove()

        # ── Footer ────────────────────────────────────────────────────────────
        foot = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        foot.grid(row=2, column=0, sticky="ew")
        foot.grid_columnconfigure(1, weight=1)

        ctk.CTkButton(
            foot, text="Lagre og fortsett",
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"], text_color=COLORS["on_accent"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            height=34, command=self._save,
        ).grid(row=0, column=0, padx=(16, 6), pady=10)

        ctk.CTkButton(
            foot, text="Avbryt kjøring",
            fg_color=COLORS["danger_bg"], hover_color=COLORS["danger_hover"], text_color=COLORS["danger_text"], border_color=COLORS["danger_border"], border_width=1,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            height=34, command=self._cancel,
        ).grid(row=0, column=2, padx=(6, 16), pady=10)

        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _apply_style(self):
        s = ttk.Style()
        s.theme_use("clam")

        _SEP_CLR = "#2a3045"   # farge for separator-kolonner og radseparator

        # Radseparator-effekt: rowheight=23 med fieldbackground som "lim"-farge
        # Rad-bakgrunnen er litt annerledes enn fieldbackground → 1px "gap" vises
        s.configure("SMap.Treeview",
            background=self._BG_COL,
            foreground=COLORS["text"],
            fieldbackground=_SEP_CLR,   # synlig mellom rader = horisontal border
            rowheight=self._f_cell.metrics("linespace") + 7,
            font=self._f_cell,
            borderwidth=0,
        )
        s.configure("SMap.Treeview.Heading",
            background="#1a1f30",
            foreground=COLORS["muted"],
            font=self._f_head,
            relief="ridge",
            borderwidth=1,
            padding=(4, 3),
        )
        s.map("SMap.Treeview",
            background=[("selected", COLORS["accent_dim"])],
            foreground=[("selected", "#ffffff")])

        s.configure("Data.Treeview",
            background=COLORS["bg"],
            foreground=COLORS["text"],
            fieldbackground=_SEP_CLR,
            rowheight=self._f_cell.metrics("linespace") + 5,
            font=self._f_cell,
            borderwidth=0,
        )
        s.configure("Data.Treeview.Heading",
            background="#1a1f30",
            foreground=COLORS["muted"],
            font=self._f_head,
            relief="ridge",
            borderwidth=1,
            padding=(4, 2),
        )
        s.map("Data.Treeview",
            background=[("selected", COLORS["border"])])

    # ── Populer ───────────────────────────────────────────────────────────────

    def _has_desc(self, iid: str, json_desc: str) -> bool:
        """Et felt har beskrivelse hvis egendefinert ELLER JSON ikke er tom."""
        return bool(self._edits.get(iid, "").strip() or json_desc)

    def _tbl_completion(self, tbl: _TblItem) -> float:
        """Andel beskrevne felt [0.0–1.0] — brukes til sortering."""
        total = 1 + len(tbl.columns)
        done  = int(self._has_desc(f"T:{tbl.norm}", tbl.json_desc))
        done += sum(1 for c in tbl.columns
                    if self._has_desc(f"C:{tbl.norm}:{c.norm}", c.json_desc))
        return done / total if total else 1.0

    def _populate(self, preserve_open: Optional[Dict[str, bool]] = None):
        """Bygg tabelltred. preserve_open={tbl_norm: is_open} bevarer kollapstilstand."""
        for iid in self._tree.get_children():
            self._tree.delete(iid)
        self._items.clear()

        # Fargekodet: grønn for ferdig, rød for mangler, gul for delvis
        self._tree.tag_configure("table",
            background=self._BG_TBL, foreground=self._FG_TBL,
            font=self._f_bold)
        self._tree.tag_configure("col",     background=self._BG_COL)
        self._tree.tag_configure("tbl_ok",  background="#1a4d2a", foreground="#7eeea0",
                                  font=self._f_bold)
        self._tree.tag_configure("tbl_par", background="#3d3515", foreground="#e8d57e",
                                  font=self._f_bold)
        self._tree.tag_configure("tbl_bad", background="#4d1515", foreground="#ee7e7e",
                                  font=self._f_bold)
        self._tree.tag_configure("col_ok",  background="#0f2914", foreground="#7eeea0")
        self._tree.tag_configure("col_bad", background="#2e1010", foreground="#ee8080")
        # Markert for sletting fra resultat-SIARD
        self._tree.tag_configure("tbl_del", background="#5a1010", foreground="#ffb0b0",
                                  font=self._f_del)

        # Sorter: uferdige tabeller øverst (A–Å), ferdige nederst (A–Å)
        sorted_tables = sorted(
            self._tables,
            key=lambda t: (self._tbl_completion(t) >= 1.0, t.name.lower())
        )

        for tbl in sorted_tables:
            t_iid  = f"T:{tbl.norm}"
            ed_tbl = self._edits.get(t_iid, "")
            if t_iid not in self._edits:
                self._edits[t_iid] = ""   # egendefinert starter tom

            n_cm   = sum(1 for c in tbl.columns
                         if self._has_desc(f"C:{tbl.norm}:{c.norm}", c.json_desc))
            has_td = self._has_desc(t_iid, tbl.json_desc)

            if has_td and n_cm == len(tbl.columns):
                t_tag, ico = "tbl_ok",  "✓"
            elif has_td or n_cm > 0:
                t_tag, ico = "tbl_par", "~"
            else:
                t_tag, ico = "tbl_bad", "✗"

            is_complete = (has_td and n_cm == len(tbl.columns))
            if preserve_open is None:
                open_state = not is_complete
            else:
                open_state = preserve_open.get(tbl.norm, not is_complete)

            # Bevar visuell markering hvis tabellen er merket for sletting
            if tbl.norm in self._marked_for_deletion:
                display_name = f"🗑  {tbl.name}  (slettes)"
                row_tags = ("table", "tbl_del")
            else:
                display_name = tbl.name
                row_tags = ("table", t_tag)
            self._tree.insert("", "end", iid=t_iid,
                text=display_name,
                values=(ico, _cell_text(tbl.json_desc), _cell_text(ed_tbl) or "─"),
                open=open_state,
                tags=row_tags)
            self._items.append({"iid": t_iid, "type": "table",
                                 "tbl": tbl, "col": None,
                                 "tbl_norm": tbl.norm, "col_norm": None})

            for col in tbl.columns:
                c_iid  = f"C:{tbl.norm}:{col.norm}"
                ed_col = self._edits.get(c_iid, "")
                if c_iid not in self._edits:
                    self._edits[c_iid] = ""   # egendefinert starter tom
                has_c  = self._has_desc(c_iid, col.json_desc)
                c_tag  = "col_ok" if has_c else "col_bad"
                self._tree.insert(t_iid, "end", iid=c_iid,
                    text=f"  {col.name}",
                    values=("✓" if has_c else "✗",
                            _cell_text(col.json_desc),
                            _cell_text(ed_col) or "─"),
                    tags=("col", c_tag))
                self._items.append({"iid": c_iid, "type": "col",
                                     "tbl": tbl, "col": col,
                                     "tbl_norm": tbl.norm, "col_norm": col.norm})

    def _update_stats(self):
        n_t  = len(self._tables)
        n_td = sum(1 for t in self._tables
                   if self._has_desc(f"T:{t.norm}", t.json_desc))
        n_c  = sum(len(t.columns) for t in self._tables)
        n_cd = sum(
            sum(1 for c in t.columns
                if self._has_desc(f"C:{t.norm}:{c.norm}", c.json_desc))
            for t in self._tables)
        self._stats_lbl.configure(
            text=f"Tabeller: {n_td}/{n_t} beskrevet   "
                 f"Kolonner: {n_cd}/{n_c} beskrevet")

    # ── Klikk, valg, navigasjon ───────────────────────────────────────────────

    def _on_click(self, event):
        region = self._tree.identify("region", event.x, event.y)
        col    = self._tree.identify_column(event.x)
        iid    = self._tree.identify_row(event.y)
        if not iid or region != "cell":
            return
        # Kolonnerekkefølge: #0=tree, #1=status, #2=json, #3=custom
        if col == "#3":
            self._open_editor(iid)
        else:
            self._editor._commit_current()
            self._editor.hide()
            self._suggestions.hide()
            self._on_row_selected(iid)

    def _on_tree_sel(self, _=None):
        sel = self._tree.selection()
        if sel and sel[0] != self._editor.current_iid():
            self._on_row_selected(sel[0])

    def _on_motion(self, event):
        col = self._tree.identify_column(event.x)
        iid = self._tree.identify_row(event.y)
        self._tree.configure(cursor="xterm" if col == "#3" else "")
        # Tooltip ved hovering over avkortede celler
        cell = (iid, col)
        if cell != self._tooltip_cell:
            self._hide_tooltip()
            self._tooltip_cell = cell
            if iid and col in ("#0", "#2", "#3"):
                xr, yr = event.x_root, event.y_root
                self._tooltip_after = self._tree.after(
                    550, lambda: self._show_tooltip(xr, yr, iid, col))

    def _show_tooltip(self, xr: int, yr: int, iid: str, col: str):
        text = self._full_cell_text(iid, col)
        if not text or not self._is_clipped(iid, col, text):
            return
        self._hide_tooltip()
        tip = tk.Toplevel(self._tree)
        tip.wm_overrideredirect(True)
        tip.wm_geometry(f"+{xr + 14}+{yr + 18}")
        # Plasser innenfor skjermen
        lbl = tk.Label(
            tip, text=text,
            justify="left", wraplength=480,
            bg="#1e2535", fg="#d4daf0",
            font=self._f_cell,
            relief="flat", bd=1, padx=10, pady=5,
        )
        lbl.pack()
        self._tooltip_win = tip

    def _is_clipped(self, iid: str, col: str, text: str) -> bool:
        """True når teksten ikke får plass i cellen slik kolonnen er nå."""
        if "\n" in text.strip():
            return True
        try:
            bb = self._tree.bbox(iid, col)
            if not bb:
                return len(text) > 55
            # #0 har innrykk og åpne/lukke-merke foran teksten
            pad = (60 if self._tree.parent(iid) else 30) if col == "#0" else 10
            return self._f_cell.measure(_cell_text(text)) > bb[2] - pad
        except Exception:
            return len(text) > 55

    def _hide_tooltip(self, _=None):
        if self._tooltip_after:
            try:
                self._tree.after_cancel(self._tooltip_after)
            except Exception:
                pass
            self._tooltip_after = None
        if self._tooltip_win:
            self._tooltip_win.destroy()
            self._tooltip_win = None
        self._tooltip_cell = (None, None)

    def _full_cell_text(self, iid: str, col: str) -> str:
        """Returner fullstendig tekst for en celle."""
        if not iid:
            return ""
        info = next((it for it in self._items if it["iid"] == iid), None)
        if not info:
            return ""
        if col == "#0":
            # Navn fra treet
            return self._tree.item(iid, "text").strip()
        elif col == "#2":
            # JSON-beskrivelse
            if info["type"] == "col" and info["col"]:
                return info["col"].json_desc
            return info["tbl"].json_desc if info["type"] == "table" else ""
        elif col == "#3":
            # Egendefinert beskrivelse
            return self._edits.get(iid, "")
        return ""

    # ── Høyreklikk-meny ───────────────────────────────────────────────────────

    def _on_right_click(self, event):
        iid = self._tree.identify_row(event.y)
        if not iid:
            return
        info = next((it for it in self._items if it["iid"] == iid), None)
        if not info:
            return
        json_desc = ""
        if info["type"] == "col" and info["col"]:
            json_desc = info["col"].json_desc
        elif info["type"] == "table":
            json_desc = info["tbl"].json_desc

        # For col-rader: bare vise meny hvis det finnes en json_desc å kopiere
        # For tabell-rader: ALLTID vise meny (sletting er alltid mulig)
        if info["type"] == "col" and not json_desc:
            return

        menu = tk.Menu(
            self._tree, tearoff=0,
            bg=COLORS["surface"], fg=COLORS["text"],
            activebackground=COLORS["accent_dim"],
            activeforeground="#ffffff",
            font=("Courier New", 10),
            relief="flat", bd=1,
        )
        if json_desc:
            menu.add_command(
                label="📋  Kopier JSON-beskrivelse → Egendefinert",
                command=lambda i=iid, d=json_desc: self._copy_json_to_custom(i, d),
            )
        # Marker for sletting — kun for tabeller
        if info["type"] == "table":
            tbl_norm = info["tbl_norm"]
            if json_desc:
                menu.add_separator()
            if tbl_norm in self._marked_for_deletion:
                menu.add_command(
                    label="↩  Fjern slette-markering",
                    command=lambda n=tbl_norm: self._toggle_delete_mark(n, False),
                )
            else:
                menu.add_command(
                    label="🗑  Merk for sletting fra resultat-SIARD",
                    foreground=COLORS["red"],
                    activeforeground="#ffffff",
                    activebackground="#7a2020",
                    command=lambda n=tbl_norm: self._toggle_delete_mark(n, True),
                )
        menu.post(event.x_root, event.y_root)
        menu.bind("<FocusOut>", lambda _: menu.destroy())

    def _toggle_delete_mark(self, tbl_norm: str, mark: bool) -> None:
        """Sett eller fjern slette-markering på en tabell. Oppdaterer UI."""
        if mark:
            self._marked_for_deletion.add(tbl_norm)
        else:
            self._marked_for_deletion.discard(tbl_norm)
        # Oppdater visuell visning av tabell-raden
        t_iid = f"T:{tbl_norm}"
        tbl = next((info["tbl"] for info in self._items
                    if info["iid"] == t_iid and info["type"] == "table"), None)
        if tbl is None:
            return
        if mark:
            # Vis prefix "🗑 " og tag "tbl_del"
            display_name = f"🗑  {tbl.name}  (slettes)"
            self._tree.item(t_iid, text=display_name, tags=("table", "tbl_del"))
        else:
            # Tilbake til normal — rebuild items vil sette riktig tag
            self._refresh_status(t_iid)
            self._tree.item(t_iid, text=tbl.name)

    def _copy_json_to_custom(self, iid: str, json_desc: str):
        was_complete = self._row_tbl_complete(iid)
        self._edits[iid] = json_desc
        vals = list(self._tree.item(iid, "values"))
        if len(vals) >= 3:
            vals[2] = _cell_text(json_desc)
            self._tree.item(iid, values=vals)
        self._refresh_status(iid)
        self._update_stats()
        self._autosave()
        self._auto_close_if_complete(iid, was_complete)
        # Oppdater editor om den er aktiv på denne raden
        if self._editor.current_iid() == iid:
            self._editor._var.set(json_desc)

    def _open_editor(self, iid: str):
        custom = self._edits.get(iid, "")
        self._editor.show(iid, custom)
        self._tree.selection_set(iid)
        self._tree.see(iid)

    def _commit(self, iid: str, value: str):
        was_complete = self._row_tbl_complete(iid)
        self._edits[iid] = value.strip()
        # Oppdater synlig verdi i treet (indeks 2 = custom_desc)
        vals = list(self._tree.item(iid, "values"))
        if len(vals) >= 3:
            vals[2] = _cell_text(value.strip()) or "─"
            self._tree.item(iid, values=vals)
        self._refresh_status(iid)
        self._update_stats()
        self._autosave()
        self._auto_close_if_complete(iid, was_complete)

    def _navigate(self, from_iid: str, direction: int):
        self._suggestions.hide()
        idx = next((i for i, it in enumerate(self._items)
                    if it["iid"] == from_iid), None)
        if idx is None:
            return
        next_idx = idx + direction
        while 0 <= next_idx < len(self._items):
            next_item = self._items[next_idx]
            next_iid = next_item["iid"]
            # Hopp over rader som er kollapset (forelder er lukket)
            parent = self._tree.parent(next_iid)
            if parent and not self._tree.item(parent, "open"):
                next_idx += direction
                continue
            self._tree.see(next_iid)
            self._open_editor(next_iid)
            return
        # Ingen flere rader — flytt fokus tilbake til treet
        self._editor.hide()
        self._tree.focus_set()

    def _on_editor_activate(self, iid: str, entry: tk.Entry):
        """
        Autocomplete-logikk ved aktivering av editorfeltet:
          0 forslag → ingenting
          1 unikt forslag → fyll inn direkte (bare hvis feltet er tomt)
          Flere forslag → vis dropdown
        """
        info = next((it for it in self._items if it["iid"] == iid), None)
        if not info:
            return
        self._on_row_selected(iid)

        # Finn json-beskrivelse for dette feltet
        json_desc_for_field = ""
        if info["type"] == "col" and info["col"]:
            json_desc_for_field = info["col"].json_desc
        elif info["type"] == "table":
            json_desc_for_field = info["tbl"].json_desc

        # Ikke foreslå når feltet allerede har json-beskrivelse —
        # da trenger operatøren bare å bekrefte eller overstyre manuelt.
        if json_desc_for_field:
            self._suggestions.hide()
            return

        # Samle unike forslag kun for felt uten json-beskrivelse
        suggestions: List[str] = []
        if info["type"] == "col" and info["col"]:
            col_norm = info["col"].norm
            seen: set = set()
            for s in self._smap.get(col_norm, []):
                s = s.strip()
                if s and s not in seen:
                    suggestions.append(s)
                    seen.add(s)
            for it in self._items:
                if (it["type"] == "col" and it["col"]
                        and it["col"].norm == col_norm
                        and it["iid"] != iid):
                    ed = self._edits.get(it["iid"], "").strip()
                    if ed and ed not in seen:
                        suggestions.append(ed)
                        seen.add(ed)

        current = entry.get().strip()
        if not suggestions:
            self._suggestions.hide()
        elif len(suggestions) == 1:
            if not current:
                entry.delete(0, "end")
                entry.insert(0, suggestions[0])
                entry.select_range(0, "end")
                self._commit(iid, suggestions[0])
            self._suggestions.hide()
        else:
            self._suggestions.show(entry, suggestions)

    def _on_suggestion_pick(self, value: str):
        iid = self._editor.current_iid()
        if iid:
            self._editor._var.set(value)
            self._commit(iid, value)

    def _on_row_selected(self, iid: str):
        info = next((it for it in self._items if it["iid"] == iid), None)
        if not info:
            return
        self._data_tbl = info["tbl"]
        self._data_hl = info["col"].col_index if info["type"] == "col" else None
        self._reload_data()

    # ── Statusoppdatering ─────────────────────────────────────────────────────

    def _refresh_status(self, iid: str):
        info = next((it for it in self._items if it["iid"] == iid), None)
        if not info:
            return
        tbl = info["tbl"]
        vals = list(self._tree.item(iid, "values"))
        tags = [t for t in self._tree.item(iid, "tags")
                if t not in ("tbl_ok","tbl_par","tbl_bad","col_ok","col_bad")]

        if info["type"] == "table":
            has_d = self._has_desc(iid, tbl.json_desc)
            n_cm  = sum(1 for c in tbl.columns
                        if self._has_desc(f"C:{tbl.norm}:{c.norm}", c.json_desc))
            if has_d and n_cm == len(tbl.columns):
                ico, tag = "✓", "tbl_ok"
            elif has_d or n_cm > 0:
                ico, tag = "~", "tbl_par"
            else:
                ico, tag = "✗", "tbl_bad"
            if vals:
                vals[0] = ico
            self._tree.item(iid, values=vals, tags=tags + ["table", tag])
        else:
            has_d = bool(self._edits.get(iid, "").strip())
            ico, tag = ("✓","col_ok") if has_d else ("✗","col_bad")
            if vals:
                vals[0] = ico
            self._tree.item(iid, values=vals, tags=tags + ["col", tag])
            # Oppdater tabellrad
            t_iid = f"T:{info['tbl_norm']}"
            if self._tree.exists(t_iid):
                self._refresh_status(t_iid)

    # ── Datatabell ────────────────────────────────────────────────────────────

    _MAX_ROWS = 1000          # rader per side
    _LOAD_NOTE_AFTER = 0.25   # sek. før «henter rader …» vises

    def _reader_for(self, tbl: _TblItem) -> TableRows:
        reader = self._readers.get(tbl.norm)
        if reader is None:
            reader = TableRows(
                table_opener(tbl.folder or tbl.name, tbl.schema_folder,
                             self._extracted, self._zip),
                len(tbl.columns))
            self._readers[tbl.norm] = reader
        return reader

    def _reload_data(self, direction: Optional[str] = None, pos=None):
        """
        Vis en side med rader for valgt tabell. Uten argumenter: der brukeren
        sist sto i tabellen. direction "fwd" = fra og med rad `pos`,
        "back" = radene før rad `pos` (None = slutten av tabellen).
        Lesingen skjer i en egen tråd, så vinduet ikke fryser for store tabeller.
        """
        tbl = self._data_tbl
        if tbl is None:
            return
        if direction is None:
            direction, pos = self._table_pos.get(tbl.norm, ("fwd", 0))
        else:
            self._table_pos[tbl.norm] = (direction, pos)
        if not self._show_data:
            return
        ukey = None
        if self._unique:
            ukey = self._data_hl if self._data_hl is not None else -1
        req = (tbl.norm, direction, pos, self._data_rows, ukey)
        self._load_gen += 1                 # eldre forespørsler er uaktuelle
        self._load_waiting = None
        page = self._page_cache.get(req)
        if page is None and not (self._extracted or self._siard):
            page = RowPage(total=0, error="ingen SIARD-data tilgjengelig")
        if page is not None:
            self._show_page(req, page)
            return

        self._data_page = None              # blaing venter til siden er lest
        self._update_nav(None)
        gen = self._load_gen
        reader = self._reader_for(tbl)
        limit = self._data_rows
        if ukey is None:
            key = None
        elif ukey < 0:
            key = tuple                     # hele raden
        else:
            key = lambda cells, i=ukey: cells[i] if i < len(cells) else ""
        q = self._load_q
        cancel = lambda: gen != self._load_gen or self._closed
        last = [0.0]

        def _progress(n_bytes):
            now = time.monotonic()
            if now - last[0] > 0.2:
                last[0] = now
                q.put(("progress", gen, n_bytes))

        def _work():
            try:
                if direction == "back":
                    page = reader.page_backward(pos, limit, key, cancel, _progress)
                else:
                    page = reader.page_forward(pos or 0, limit, key, cancel, _progress)
            except Cancelled:
                return
            except FileNotFoundError:
                page = RowPage(total=0, error="fant ikke tabellfila")
            except Exception as exc:
                page = RowPage(error=f"kunne ikke lese tabellen ({exc})")
            q.put(("page", gen, req, page))

        self._load_waiting = gen
        self._load_started = time.monotonic()
        self._load_bytes = 0
        threading.Thread(target=_work, daemon=True,
                         name="siardmapper-rader").start()
        if not self._load_polling:
            self._load_polling = True
            self.after(15, self._poll_loads)

    def _poll_loads(self):
        if self._closed:
            return
        read_bytes = None
        try:
            while True:
                item = self._load_q.get_nowait()
                if item[0] == "page":
                    _, gen, req, page = item
                    if not page.error:
                        if len(self._page_cache) > 200:
                            self._page_cache.clear()
                        self._page_cache[req] = page
                    if gen == self._load_gen:
                        self._load_waiting = None
                        self._show_page(req, page)
                elif item[1] == self._load_gen:
                    read_bytes = item[2]
        except queue.Empty:
            pass
        if self._load_waiting is None:
            self._load_polling = False
            return
        if read_bytes:
            self._load_bytes = read_bytes
        if time.monotonic() - self._load_started > self._LOAD_NOTE_AFTER:
            note = "henter rader …"
            if self._load_bytes:
                note += f" ({_fsize(self._load_bytes)} lest)"
            self._data_hdr.configure(
                text=f"   EKSEMPELDATA — {self._data_name()}  ·  {note}")
        self.after(40, self._poll_loads)

    def _data_name(self) -> str:
        tbl, hl = self._data_tbl, self._data_hl
        if tbl is None:
            return ""
        if hl is not None and hl < len(tbl.columns):
            return f"{tbl.name}.{tbl.columns[hl].name}"
        return tbl.name

    def _show_page(self, req: tuple, page: RowPage):
        tbl = self._data_tbl
        if tbl is None or req[0] != tbl.norm:
            return
        self._data_page = page
        self._set_data_tree([c.name for c in tbl.columns], page, self._data_hl)
        self._data_hdr.configure(text=self._page_caption(tbl, page, req[4]))
        self._update_nav(page)

    def _page_caption(self, tbl: _TblItem, page: RowPage, ukey) -> str:
        head = f"   EKSEMPELDATA — {self._data_name()}  ·  "
        if page.error:
            return head + page.error
        total = page.total if page.total is not None else tbl.n_rows
        of = f" av {_fnum(total)}" if total is not None else ""
        if not page.rows:
            return head + "ingen rader"
        if ukey is None:
            first, last = page.numbers[0] + 1, page.numbers[-1] + 1
            span = f"rad {_fnum(first)}" if first == last \
                else f"rad {_fnum(first)}–{_fnum(last)}"
            return head + span + of
        if ukey < 0 or ukey >= len(tbl.columns):
            what = "unike rader" if len(page.rows) != 1 else "unik rad"
        else:
            what = (f"unike verdier i {tbl.columns[ukey].name}"
                    if len(page.rows) != 1
                    else f"unik verdi i {tbl.columns[ukey].name}")
        text = (f"{len(page.rows)} {what} blant rad "
                f"{_fnum(page.scan_from + 1)}–{_fnum(page.scan_to)}{of}")
        if page.capped:
            text += "  (letingen stoppet her — «Neste» leter videre)"
        return head + text

    def _update_nav(self, page: Optional[RowPage]):
        n = self._data_rows
        self._nav_btns["prev"].configure(text=f"◀ Forrige {n}")
        self._nav_btns["next"].configure(text=f"Neste {n} ▶")
        ok = page is not None and not page.error
        back = "normal" if ok and not page.at_start else "disabled"
        fwd = "normal" if ok and not page.at_end else "disabled"
        self._nav_btns["start"].configure(state=back)
        self._nav_btns["prev"].configure(state=back)
        self._nav_btns["next"].configure(state=fwd)
        self._nav_btns["end"].configure(state=fwd)

    def _nav(self, where: str):
        """Start / Forrige N / Neste N / Slutt."""
        self._apply_rows_entry(reload=False)
        if self._data_tbl is None:
            return
        page = self._data_page
        if where == "start":
            self._reload_data("fwd", 0)
        elif where == "end":
            self._reload_data("back", None)
        elif page is None:
            return                          # forrige side er ikke ferdig lest
        elif where == "next":
            self._reload_data("fwd", page.scan_to)
        elif where == "prev":
            self._reload_data("back", page.scan_from)

    def _set_data_tree(self, col_names: List[str], page: RowPage,
                       highlight: Optional[int]):
        # Sikre kolonne-ID-er: «n» = radnummer, c0.. = tabellens kolonner
        n = len(col_names)
        new_cols = ["n"] + [f"c{i}" for i in range(n)]
        self._data_tree.delete(*self._data_tree.get_children())
        fresh = self._data_cols != new_cols
        ch = max(6, self._f_cell.measure("0"))
        if fresh:
            self._data_tree["columns"] = new_cols
            self._data_cols = new_cols
            self._data_tree.heading("n", text="Rad", anchor="e")
            self._data_tree.column("n", width=ch * 9, minwidth=ch * 4,
                                   stretch=False, anchor="e")
        for i, cname in enumerate(col_names):
            cid = f"c{i}"
            self._data_tree.heading(
                cid, text=f"▶ {cname}" if i == highlight else cname, anchor="w")
            if fresh:
                self._data_tree.column(
                    cid, width=ch * (17 if i == highlight else 12),
                    minwidth=50, stretch=True)
        for num, row in zip(page.numbers, page.rows):
            vals = [_fnum(num + 1)] + [
                _cell_text(row[i], 500) if i < len(row) else "" for i in range(n)]
            self._data_tree.insert("", "end", values=vals)

    # ── Vis-data-innstillinger ────────────────────────────────────────────────

    def _on_show_data_toggle(self):
        show = self._show_data_var.get()
        self._show_data = show
        if show:
            self._data_frm.grid()
            self._reload_data()
        else:
            self._data_frm.grid_remove()
        self._save_data_settings()

    def _on_unique_toggle(self):
        self._unique = bool(self._unique_var.get())
        self._save_data_settings()
        self._reload_data()

    def _apply_rows_entry(self, _=None, reload: bool = True) -> bool:
        """
        Ta i bruk antallet i feltet. Kalles ved Enter og når feltet forlates —
        ikke for hvert tastetrykk (da ble dataene lest på nytt for hvert siffer,
        og «199» stoppet på 19). Returnerer True når antallet ble endret.
        """
        raw = self._rows_var.get().strip()
        try:
            n = int(raw)
        except ValueError:
            n = self._data_rows
        n = max(1, min(self._MAX_ROWS, n))
        if str(n) != raw:
            self._rows_var.set(str(n))
        if n == self._data_rows:
            return False
        self._data_rows = n
        self._update_nav(self._data_page)
        self._save_data_settings()
        if reload:
            self._reload_data()
        return True

    def _on_rows_enter(self, _=None):
        self._apply_rows_entry()
        try:
            self._rows_entry.select_range(0, "end")
        except Exception:
            pass
        return "break"

    def _save_data_settings(self):
        try:
            from settings import save_config
            save_config({
                "siardmapper_show_data": self._show_data,
                "siardmapper_rows":      self._data_rows,
                "siardmapper_unique":    self._unique,
            })
        except Exception:
            pass

    # ── Skriftstørrelse ───────────────────────────────────────────────────────

    def _apply_fonts(self):
        """
        A−/A+ og Ctrl +/−: tabellene her er ttk-/tk-widgeter og følger ikke
        CTkFont-registeret av seg selv. Kalles av FontRegistry ved endring.
        """
        try:
            if self._closed or not self.winfo_exists():
                return
        except Exception:
            return
        for font, base in self._fonts:
            font.configure(size=FontRegistry.effective_size(base))
        s = ttk.Style()
        s.configure("SMap.Treeview",
                    rowheight=self._f_cell.metrics("linespace") + 7)
        s.configure("Data.Treeview",
                    rowheight=self._f_cell.metrics("linespace") + 5)
        self.after_idle(self._editor.reposition)

    def destroy(self):
        self._closed = True
        self._load_gen += 1                 # avbryt lesing som pågår
        try:
            self._zip.close()
        except Exception:
            pass
        super().destroy()

    # ── Autolagring ───────────────────────────────────────────────────────────

    def _autosave(self):
        if not self._save_path:
            return
        try:
            data = self._build_json()
            self._save_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _row_tbl_complete(self, iid: str) -> bool:
        """Er tabellen raden hører til, ferdig beskrevet nå?"""
        info = next((it for it in self._items if it["iid"] == iid), None)
        return bool(info) and self._tbl_completion(info["tbl"]) >= 1.0

    def _on_tbl_opened(self, _=None):
        """Brukeren åpnet en tabell. Er den alt ferdig, er det for å endre noe —
        da skal den stå åpen til brukeren selv lukker den."""
        iid = self._tree.focus()
        info = next((it for it in self._items
                     if it["iid"] == iid and it["type"] == "table"), None)
        if info and self._tbl_completion(info["tbl"]) >= 1.0:
            self._keep_open.add(info["tbl"].norm)

    def _on_tbl_closed(self, _=None):
        iid = self._tree.focus() or ""
        if iid.startswith("T:"):
            self._keep_open.discard(iid[2:])

    def _auto_close_if_complete(self, iid: str, was_complete: bool = False):
        """
        Lukk tabellnoden automatisk i det den BLIR ferdig beskrevet.
        Var tabellen ferdig fra før (was_complete), eller har brukeren selv
        åpnet en ferdig tabell, er det en endring i en ferdig tabell — da
        lukkes den ikke før brukeren gjør det selv.
        """
        info = next((it for it in self._items if it["iid"] == iid), None)
        if not info:
            return
        tbl = info["tbl"]
        if was_complete or tbl.norm in self._keep_open:
            return
        if self._tbl_completion(tbl) >= 1.0:
            self._tree.item(f"T:{tbl.norm}", open=False)

    def _build_json(self) -> dict:
        """Bygg JSON med alle beskrivelser: egendefinert overstyrer json-mal, json-mal er fallback.
        Topp-nivå-metadata fra opprinnelig mal (name, description, systemName, osv.) bevares.
        """
        # Start med topp-nivå-felt fra opprinnelig mal — bevar alt unntatt tabellister
        base: dict = {}
        src = self._json_path
        if src and src.exists():
            try:
                raw = json.loads(src.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    base = {k: v for k, v in raw.items()
                            if k not in ("tables", "tabeller")}
            except Exception:
                pass
        # Brukerens metadata-endringer overstyrer mal-feltene
        base.update(self._meta_overrides)

        tables = []
        for tbl in self._tables:
            t_iid    = f"T:{tbl.norm}"
            tbl_desc = self._edits.get(t_iid, "").strip() or tbl.json_desc
            cols = []
            for col in tbl.columns:
                c_iid    = f"C:{tbl.norm}:{col.norm}"
                col_desc = self._edits.get(c_iid, "").strip() or col.json_desc
                cols.append({
                    "name":        col.name,
                    "description": col_desc,
                })
            tables.append({
                "name":        tbl.name,
                "description": tbl_desc,
                "columns":     cols,
            })
        base["tables"] = tables
        return base

    # ── Rediger mal-metadata ──────────────────────────────────────────────────

    def _edit_meta(self):
        """Åpne dialog for å redigere topp-nivå-metadata i JSON-malen."""
        # Bygg visningsdata: mal-base + brukerens in-session overrides
        _KNOWN = [
            ("name",          "Navn"),
            ("description",   "Beskrivelse"),
            ("systemName",    "System/applikasjon"),
            ("systemVersjon", "Systemversjon"),
            ("modelVersion",  "Mal-versjon"),
        ]
        _KNOWN_KEYS = {k for k, _ in _KNOWN}

        # Hent alle skalare topp-nivå-felt fra malen
        base_meta: dict = {}
        if self._json_path and self._json_path.exists():
            try:
                raw = json.loads(self._json_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    base_meta = {k: v for k, v in raw.items()
                                 if k not in ("tables", "tabeller")
                                 and isinstance(v, (str, int, float))}
            except Exception:
                pass
        # Brukerens overrides tar presedens for visning
        current: dict = {**base_meta, **{
            k: v for k, v in self._meta_overrides.items()
            if isinstance(v, (str, int, float))
        }}

        # Ekstrafelter (ikke blant kjente)
        extras = [(k, k) for k in current if k not in _KNOWN_KEYS]
        fields = _KNOWN + extras

        win = ctk.CTkToplevel(self)
        win.title("Rediger mal-metadata")
        win.configure(fg_color=COLORS["bg"])
        win.grab_set()
        win.lift()

        meta_hdr = ctk.CTkFrame(win, fg_color=COLORS["panel"], corner_radius=0)
        meta_hdr.pack(fill="x")
        ctk.CTkLabel(
            meta_hdr,
            text="  Rediger mal-metadata",
            font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
            text_color=COLORS["accent"], anchor="w",
        ).pack(side="left", padx=12, pady=10)

        scroll = ctk.CTkScrollableFrame(win, fg_color=COLORS["bg"])
        scroll.pack(fill="both", expand=True, padx=12, pady=8)
        scroll.grid_columnconfigure(0, weight=1)

        widgets: dict[str, tk.StringVar | ctk.CTkTextbox] = {}

        for r, (key, label) in enumerate(fields):
            val = str(current.get(key, ""))
            ctk.CTkLabel(
                scroll, text=f"{label}:",
                font=ctk.CTkFont(family=FONTS["mono"], size=11),
                text_color=COLORS["text_sub"], anchor="w",
            ).grid(row=r * 2, column=0, sticky="w", pady=(8, 1))

            if key == "description":
                tb = ctk.CTkTextbox(
                    scroll, height=80,
                    font=ctk.CTkFont(family=FONTS["mono"], size=11),
                    fg_color=COLORS["surface"],
                    border_color=COLORS["border"], border_width=1,
                )
                tb.grid(row=r * 2 + 1, column=0, sticky="ew", pady=(0, 2))
                tb.insert("1.0", val)
                widgets[key] = tb
            else:
                sv = tk.StringVar(value=val)
                ctk.CTkEntry(
                    scroll, textvariable=sv,
                    font=ctk.CTkFont(family=FONTS["mono"], size=11),
                    fg_color=COLORS["surface"],
                    border_color=COLORS["border"],
                ).grid(row=r * 2 + 1, column=0, sticky="ew", pady=(0, 2))
                widgets[key] = sv

        btn_frm = ctk.CTkFrame(win, fg_color="transparent")
        btn_frm.pack(fill="x", padx=12, pady=(4, 14))
        btn_frm.grid_columnconfigure((0, 1), weight=1)

        def _ok():
            overrides = {}
            for key, widget in widgets.items():
                if isinstance(widget, ctk.CTkTextbox):
                    overrides[key] = widget.get("1.0", "end-1c").strip()
                else:
                    overrides[key] = widget.get().strip()
            self._meta_overrides = overrides
            self._autosave()
            win.destroy()

        ctk.CTkButton(
            btn_frm, text="OK",
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"], text_color=COLORS["on_accent"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            height=34, command=_ok,
        ).grid(row=0, column=0, padx=(0, 6), sticky="ew")

        ctk.CTkButton(
            btn_frm, text="Avbryt",
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"], text_color=COLORS["btn_text"], border_color=COLORS["btn_border"], border_width=1,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            height=34, command=win.destroy,
        ).grid(row=0, column=1, sticky="ew")

        win.protocol("WM_DELETE_WINDOW", win.destroy)
        win.update_idletasks()
        w = 520
        h = min(620, win.winfo_reqheight() + 60)
        x = self.winfo_x() + (self.winfo_width()  - w) // 2
        y = self.winfo_y() + (self.winfo_height() - h) // 2
        win.geometry(f"{w}x{h}+{x}+{y}")
        win.resizable(True, True)

    # ── Re-match ──────────────────────────────────────────────────────────────

    def _rematch(self):
        """Last JSON-fil på nytt og oppdater treff og beskrivelser."""
        load_path = self._save_path or self._json_path
        if not load_path or not load_path.exists():
            return
        try:
            from siard_workflow.operations.siardmapper_operation import (
                _parse_json_template, _match)
            json_lookup = _parse_json_template(load_path)
            new_matches = _match(self._tables, json_lookup)
        except Exception:
            return

        # Bevar kollapstilstand
        open_state = {
            tbl.norm: self._tree.item(f"T:{tbl.norm}", "open")
            for tbl in self._tables
            if self._tree.exists(f"T:{tbl.norm}")
        }
        # Oppdater suggestion-map
        for m in new_matches:
            for c in m.table.columns:
                d = m.col_descs.get(c.norm, "")
                if d:
                    self._smap.setdefault(c.norm, [])
                    if d not in self._smap[c.norm]:
                        self._smap[c.norm].append(d)

        # Oppdater kun json_desc på _TblItem/_ColItem — _edits (egendefinert) røres ikke
        for m in new_matches:
            tbl = next((t for t in self._tables if t.norm == m.table.norm), None)
            if tbl:
                tbl.json_desc = m.json_table_desc or ""
                for col in tbl.columns:
                    col.json_desc = m.col_descs.get(col.norm, "")

        self._populate(preserve_open=open_state)
        self._update_stats()

    # ── Lagre / avbryt ───────────────────────────────────────────────────────

    def _save(self):
        self._editor._commit_current()
        self._editor.hide()
        self._suggestions.hide()

        # Vis advarsel hvis noen tabeller er merket for sletting
        if self._marked_for_deletion:
            from tkinter import messagebox
            marked_names = sorted(
                tbl.name for tbl in self._tables
                if tbl.norm in self._marked_for_deletion
            )
            preview = "\n".join(f"  • {n}" for n in marked_names[:25])
            if len(marked_names) > 25:
                preview += f"\n  … og {len(marked_names) - 25} til"
            ans = messagebox.askokcancel(
                "Tabeller markert for sletting",
                f"Følgende tabeller er markert for sletting i SIARD-filen:\n\n"
                f"{preview}\n\n"
                f"Totalt: {len(marked_names)} tabell(er).\n\n"
                "Tabellene blir permanent fjernet fra metadata.xml og "
                "content/-mappene. Denne handlingen kan ikke angres.\n\n"
                "OK for å fortsette, Avbryt for å gå tilbake.",
                parent=self,
                icon="warning",
            )
            if not ans:
                return  # gå tilbake til dialogen

        self._autosave()
        self._cancelled = False
        self.destroy()

    def _cancel(self):
        self._cancelled = True
        self.destroy()

    def get_result(self):
        if self._cancelled:
            return None
        # Beskrivelser per tabell/kolonne (bakoverkompatibelt med eksisterende API)
        edits: Dict[str, dict] = {}
        for info in self._items:
            tn = info["tbl_norm"]
            if tn not in edits:
                edits[tn] = {"desc": "", "cols": {}}
            iid = info["iid"]
            val = self._edits.get(iid, "").strip()
            if info["type"] == "table":
                edits[tn]["desc"] = val
            else:
                edits[tn]["cols"][info["col_norm"]] = val

        # Tabeller markert for sletting (med navn + folder)
        marked: List[dict] = []
        for tbl in self._tables:
            if tbl.norm in self._marked_for_deletion:
                marked.append({
                    "name":   tbl.name,
                    "norm":   tbl.norm,
                    "folder": tbl.folder,
                })

        # Ny struktur: dict med to nøkler. Operation må håndtere det.
        return {
            "edits": edits,
            "marked_for_deletion": marked,
        }



# ── Hjelpere ──────────────────────────────────────────────────────────────────

def _cell_text(text: str, n: int = 2000) -> str:
    """
    Celletekst på én linje. Treeview klipper selv ved kolonnekanten, så
    teksten følger kolonnebredden (før ble den alltid kuttet ved 55 tegn,
    uansett hvor bred kolonnen var).
    """
    if not text:
        return ""
    t = " ".join(str(text).split())
    return t if len(t) <= n else t[:n - 1] + "…"


def _shorten(text: str, n: int = 55) -> str:
    if not text:
        return ""
    return text if len(text) <= n else text[:n - 1] + "…"


# ── Fabrikk ───────────────────────────────────────────────────────────────────

def _decode_desc(text: str) -> str:
    """Dekod HTML-entiteter iterativt (sikkerhetsnett i dialogen)."""
    if not text:
        return ""
    result = text
    for _ in range(4):
        decoded = _html_mod.unescape(result)
        if decoded == result:
            break
        result = decoded
    return result.strip()


def build_dialog_tables(matches) -> List[_TblItem]:
    result = []
    for m in matches:
        tbl = _TblItem(
            name=m.table.name,
            norm=m.table.norm,
            json_desc=_decode_desc(m.json_table_desc or ""),
            folder=getattr(m.table, "folder", None) or m.table.name,
            matched=m.json_table_desc is not None,
            schema_folder=getattr(m.table, "schema_folder", None) or "",
            n_rows=getattr(m.table, "rows", None),
        )
        for i, col in enumerate(m.table.columns):
            tbl.columns.append(_ColItem(
                name=col.name,
                norm=col.norm,
                json_desc=_decode_desc(m.col_descs.get(col.norm, "")),
                col_index=i,
            ))
        result.append(tbl)
    return result
