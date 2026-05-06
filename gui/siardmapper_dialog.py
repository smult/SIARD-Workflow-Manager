"""
gui/siardmapper_dialog.py

Flat redigeringstabell à la original SIARDMapper:
  - Trevisning med native expand/collapse (ttk.Treeview show="tree headings")
  - Kolonner: Navn | Status | JSON-beskrivelse | Egendefinert beskrivelse
  - Inline tk.Entry-overlay på «Egendefinert»-kolonnen; Enter hopper til neste rad
  - Forslag-popup basert på lignende kolonnebeskrivelser
  - Datatabell (5 eksempelrader) nederst, oppdateres ved valg
  - Autolagring til JSON etter hver endring
  - Re-match-knapp
"""

from __future__ import annotations

import json
import tkinter as tk
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from tkinter import ttk
from typing import Dict, List, Optional

import customtkinter as ctk

from gui.styles import COLORS, FONTS


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


# ── Eksempelrader ─────────────────────────────────────────────────────────────

def _read_sample_rows(folder, n_cols, siard_path, extracted_path, limit=5):
    def _parse(raw):
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            return []
        rows = []
        for row_el in root:
            tag = row_el.tag.split("}")[-1] if "}" in row_el.tag else row_el.tag
            if tag != "row":
                continue
            cells = [""] * n_cols
            for c in row_el:
                ct = c.tag.split("}")[-1] if "}" in c.tag else c.tag
                if ct.startswith("c") and ct[1:].isdigit():
                    i = int(ct[1:]) - 1
                    if 0 <= i < n_cols:
                        cells[i] = (c.text or "").strip()
            rows.append(cells)
            if len(rows) >= limit:
                break
        return rows

    clean = (folder or "").strip("/")
    if extracted_path and extracted_path.is_dir():
        for f in extracted_path.rglob("*.xml"):
            rel = f.relative_to(extracted_path).as_posix()
            if f"/{clean}/" in f"/{rel}" and "header" not in rel:
                try:
                    return _parse(f.read_bytes())
                except Exception:
                    pass
    if siard_path and Path(siard_path).exists():
        try:
            with zipfile.ZipFile(siard_path, "r", allowZip64=True) as zf:
                cands = [n for n in zf.namelist()
                         if f"/{clean}/" in f"/{n}" and n.endswith(".xml")
                         and not n.startswith("header/")]
                if cands:
                    return _parse(zf.read(cands[0]))
        except Exception:
            pass
    return []


# ── Inline-editor ─────────────────────────────────────────────────────────────

class _InlineEditor:
    """tk.Entry-overlay plassert over riktig celle. Enter/Tab → neste rad."""

    def __init__(self, tree, col_id, on_commit, on_navigate, on_activate):
        self._tree       = tree
        self._col_id     = col_id
        self._on_commit  = on_commit
        self._on_nav     = on_navigate
        self._on_activate = on_activate
        self._iid: Optional[str] = None
        self._active = False

        self._var = tk.StringVar()
        self._entry = tk.Entry(
            tree,
            textvariable=self._var,
            bg="#1e2535", fg="#d4daf0",
            insertbackground="#4f8ef7",
            selectbackground="#3a70d4", selectforeground="#ffffff",
            relief="flat", font=("Courier New", 10), bd=1,
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
        if bb:
            x, y, w, h = bb
            self._entry.place(x=x, y=y, width=w, height=h)

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

    def __init__(self, parent_tree, on_pick):
        self._tree    = parent_tree
        self._on_pick = on_pick
        self._lb_frame = tk.Frame(parent_tree, bg=COLORS["border"], bd=1)
        self._lb = tk.Listbox(
            self._lb_frame,
            bg="#1e2535", fg="#d4daf0",
            selectbackground=COLORS["accent_dim"],
            selectforeground="#ffffff",
            font=("Courier New", 10),
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
        self._cache: Dict[str, List[List[str]]] = {}

        # Redigeringer: {iid: str}
        self._edits: Dict[str, str] = {}
        # Flat liste for navigasjon
        self._items: List[dict] = []

        # Autolags-sti
        self._save_path: Optional[Path] = None
        if json_path and json_path.exists():
            stem = json_path.stem
            if stem.endswith("_metadata_mal"):
                self._save_path = json_path          # generert fil
            else:
                self._save_path = json_path.with_name(stem + "_egendefinert.json")

        self.title("SIARDMapper — berik metadata")
        self.configure(fg_color=COLORS["bg"])
        self.resizable(True, True)
        self.grab_set()
        self.lift()

        self._build()
        self._apply_style()
        self._populate()
        self._update_stats()
        self.after(50, lambda: self.state("zoomed"))   # start maksimert

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build(self):
        self.grid_rowconfigure(1, weight=3)
        self.grid_rowconfigure(2, weight=1)
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
            hdr, text="↺ Re-match mot JSON", height=26, width=160,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self._rematch,
        ).grid(row=0, column=2, padx=(8, 14), pady=7)

        # ── Hovedtabell ───────────────────────────────────────────────────────
        tbl_frm = tk.Frame(self, bg=COLORS["bg"])
        tbl_frm.grid(row=1, column=0, sticky="nsew", padx=6, pady=(4, 0))
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
        self._tree.column("#0",              width=200, minwidth=100, stretch=False)
        self._tree.column(self._COL_STATUS,  width=28,  minwidth=28,  stretch=False)
        self._tree.column(self._COL_JSON,    width=310, minwidth=100, stretch=True)
        self._tree.column(self._COL_CUSTOM,  width=310, minwidth=100, stretch=True)

        vsb = ttk.Scrollbar(tbl_frm, orient="vertical",
                             command=lambda *a: (self._tree.yview(*a),
                                                 self._editor.reposition()))
        hsb = ttk.Scrollbar(tbl_frm, orient="horizontal", command=self._tree.xview)
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
        )
        self._suggestions = _SuggestionPopup(
            self._tree, on_pick=self._on_suggestion_pick)

        self._tree.bind("<ButtonRelease-1>", self._on_click)
        self._tree.bind("<<TreeviewSelect>>", self._on_tree_sel)
        self._tree.bind("<Configure>",    lambda _: self._editor.reposition())
        self._tree.bind("<MouseWheel>",   lambda _: self._editor.reposition())
        self._tree.bind("<Motion>",       self._on_motion)

        # ── Datatabell ────────────────────────────────────────────────────────
        data_frm = tk.Frame(self, bg=COLORS["bg"])
        data_frm.grid(row=2, column=0, sticky="nsew", padx=6, pady=(3, 0))
        data_frm.grid_rowconfigure(1, weight=1)
        data_frm.grid_columnconfigure(0, weight=1)

        self._data_hdr = tk.Label(
            data_frm, text="EKSEMPELDATA",
            bg=COLORS["bg"], fg=COLORS["muted"],
            font=("Courier New", 8, "bold"), anchor="w")
        self._data_hdr.grid(row=0, column=0, columnspan=2, sticky="w", pady=(2, 1))

        self._data_tree = ttk.Treeview(
            data_frm, style="Data.Treeview", show="headings", height=5)
        dvsb = ttk.Scrollbar(data_frm, orient="vertical",   command=self._data_tree.yview)
        dhsb = ttk.Scrollbar(data_frm, orient="horizontal", command=self._data_tree.xview)
        self._data_tree.configure(yscrollcommand=dvsb.set, xscrollcommand=dhsb.set)
        self._data_tree.grid(row=1, column=0, sticky="nsew")
        dvsb.grid(row=1, column=1, sticky="ns")
        dhsb.grid(row=2, column=0, sticky="ew")
        self._data_cols: List[str] = []

        # ── Footer ────────────────────────────────────────────────────────────
        foot = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        foot.grid(row=3, column=0, sticky="ew")
        foot.grid_columnconfigure(1, weight=1)

        ctk.CTkButton(
            foot, text="Lagre og fortsett",
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
            height=34, command=self._save,
        ).grid(row=0, column=0, padx=(16, 6), pady=10)

        ctk.CTkButton(
            foot, text="Avbryt kjøring",
            fg_color="#2a1515", hover_color="#3d2020",
            text_color=COLORS["red"],
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
            rowheight=23,
            font=("Courier New", 10),
            borderwidth=0,
        )
        s.configure("SMap.Treeview.Heading",
            background="#1a1f30",
            foreground=COLORS["muted"],
            font=("Courier New", 9, "bold"),
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
            rowheight=21,
            font=("Courier New", 10),
            borderwidth=0,
        )
        s.configure("Data.Treeview.Heading",
            background="#1a1f30",
            foreground=COLORS["muted"],
            font=("Courier New", 9, "bold"),
            relief="ridge",
            borderwidth=1,
            padding=(4, 2),
        )
        s.map("Data.Treeview",
            background=[("selected", COLORS["border"])])

    # ── Populer ───────────────────────────────────────────────────────────────

    def _populate(self, preserve_open: Optional[Dict[str, bool]] = None):
        """Bygg tabelltred. preserve_open={tbl_norm: is_open} bevarer kollapstilstand."""
        for iid in self._tree.get_children():
            self._tree.delete(iid)
        self._items.clear()

        self._tree.tag_configure("table",
            background=self._BG_TBL, foreground=self._FG_TBL,
            font=("Courier New", 10, "bold"))
        self._tree.tag_configure("col",    background=self._BG_COL)
        self._tree.tag_configure("tbl_ok", background=self._CLR_OK)
        self._tree.tag_configure("tbl_par",background=self._CLR_PAR)
        self._tree.tag_configure("tbl_bad",background=self._CLR_BAD)
        self._tree.tag_configure("col_ok", background=self._CLR_COK)
        self._tree.tag_configure("col_bad",background=self._CLR_CBAD)

        for tbl in self._tables:
            t_iid   = f"T:{tbl.norm}"
            ed_tbl  = self._edits.get(t_iid, tbl.json_desc)
            if t_iid not in self._edits:
                self._edits[t_iid] = tbl.json_desc

            n_cm   = sum(1 for c in tbl.columns
                         if self._edits.get(f"C:{tbl.norm}:{c.norm}", c.json_desc))
            has_td = bool(ed_tbl)
            if has_td and n_cm == len(tbl.columns):
                t_tag, ico = "tbl_ok",  "✓"
            elif has_td or n_cm > 0:
                t_tag, ico = "tbl_par", "~"
            else:
                t_tag, ico = "tbl_bad", "✗"

            is_complete = (has_td and n_cm == len(tbl.columns))
            if preserve_open is None:
                open_state = not is_complete   # lukk fullstendige ved oppstart
            else:
                open_state = preserve_open.get(tbl.norm, not is_complete)
            self._tree.insert("", "end", iid=t_iid,
                text=tbl.name,
                values=(ico, _shorten(tbl.json_desc), _shorten(ed_tbl) or "─"),
                open=open_state,
                tags=("table", t_tag))
            self._items.append({"iid": t_iid, "type": "table",
                                 "tbl": tbl, "col": None,
                                 "tbl_norm": tbl.norm, "col_norm": None})

            for col in tbl.columns:
                c_iid  = f"C:{tbl.norm}:{col.norm}"
                ed_col = self._edits.get(c_iid, col.json_desc)
                if c_iid not in self._edits:
                    self._edits[c_iid] = col.json_desc
                has_c  = bool(ed_col)
                c_tag  = "col_ok" if has_c else "col_bad"
                self._tree.insert(t_iid, "end", iid=c_iid,
                    text=f"  {col.name}",
                    values=("✓" if has_c else "✗",
                            _shorten(col.json_desc),
                            _shorten(ed_col) or "─"),
                    tags=("col", c_tag))
                self._items.append({"iid": c_iid, "type": "col",
                                     "tbl": tbl, "col": col,
                                     "tbl_norm": tbl.norm, "col_norm": col.norm})

    def _update_stats(self):
        n_t  = len(self._tables)
        n_td = sum(1 for t in self._tables
                   if self._edits.get(f"T:{t.norm}", "").strip())
        n_c  = sum(len(t.columns) for t in self._tables)
        n_cd = sum(
            sum(1 for c in t.columns
                if self._edits.get(f"C:{t.norm}:{c.norm}", "").strip())
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
        """Endre kursor til tekstkursor over custom-kolonnen for å antyde editering."""
        col = self._tree.identify_column(event.x)
        self._tree.configure(cursor="xterm" if col == "#3" else "")

    def _open_editor(self, iid: str):
        custom = self._edits.get(iid, "")
        self._editor.show(iid, custom)
        self._tree.selection_set(iid)
        self._tree.see(iid)

    def _commit(self, iid: str, value: str):
        self._edits[iid] = value.strip()
        # Oppdater synlig verdi i treet (indeks 2 = custom_desc)
        vals = list(self._tree.item(iid, "values"))
        if len(vals) >= 3:
            vals[2] = _shorten(value.strip()) or "─"
            self._tree.item(iid, values=vals)
        self._refresh_status(iid)
        self._update_stats()
        self._autosave()
        self._auto_close_if_complete(iid)

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

        # Samle unike forslag for kolonnen
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
            # Fyll inn direkte kun om feltet er tomt
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
        tbl = info["tbl"]
        col = info["col"]
        rows = self._get_rows(tbl)
        col_names = [c.name for c in tbl.columns]
        if info["type"] == "table":
            self._data_hdr.configure(
                text=f"EKSEMPELDATA — {tbl.name}  "
                     f"({len(rows)} rad(er), alle {len(col_names)} kolonner)")
            self._set_data_tree(col_names, rows, highlight=None)
        else:
            self._data_hdr.configure(
                text=f"EKSEMPELDATA — {tbl.name}.{col.name}  "
                     f"({len(rows)} rad(er))")
            self._set_data_tree(col_names, rows, highlight=col.col_index)

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
            has_d = bool(self._edits.get(iid, "").strip())
            n_cm  = sum(1 for c in tbl.columns
                        if self._edits.get(f"C:{tbl.norm}:{c.norm}", "").strip())
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

    def _get_rows(self, tbl: _TblItem) -> List[List[str]]:
        if tbl.norm not in self._cache:
            self._cache[tbl.norm] = _read_sample_rows(
                tbl.folder or tbl.name,
                len(tbl.columns),
                self._siard, self._extracted,
            )
        return self._cache[tbl.norm]

    def _set_data_tree(self, col_names: List[str], rows: List[List[str]],
                       highlight: Optional[int]):
        # Bruk sikre numeriske kolonne-ID-er
        n = len(col_names)
        new_cols = [f"c{i}" for i in range(n)]

        # Nullstill datatabell
        if self._data_cols != new_cols:
            self._data_tree.delete(*self._data_tree.get_children())
            self._data_tree["columns"] = new_cols
            self._data_cols = new_cols
            for i, (cid, cname) in enumerate(zip(new_cols, col_names)):
                htext = f"▶ {cname}" if i == highlight else cname
                self._data_tree.heading(cid, text=htext, anchor="w")
                w = 140 if i == highlight else 100
                self._data_tree.column(cid, width=w, minwidth=50, stretch=True)
        else:
            self._data_tree.delete(*self._data_tree.get_children())
            for i, (cid, cname) in enumerate(zip(new_cols, col_names)):
                htext = f"▶ {cname}" if i == highlight else cname
                self._data_tree.heading(cid, text=htext, anchor="w")

        for row in rows:
            vals = [row[i] if i < len(row) else "" for i in range(n)]
            self._data_tree.insert("", "end", values=vals)

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

    def _auto_close_if_complete(self, iid: str):
        """Lukk tabellnoden automatisk når alle felt er beskrevet."""
        info = next((it for it in self._items if it["iid"] == iid), None)
        if not info:
            return
        tbl = info["tbl"]
        t_iid = f"T:{tbl.norm}"
        has_tbl = bool(self._edits.get(t_iid, "").strip())
        n_done  = sum(1 for c in tbl.columns
                      if self._edits.get(f"C:{tbl.norm}:{c.norm}", "").strip())
        if has_tbl and n_done == len(tbl.columns):
            self._tree.item(t_iid, open=False)

    def _build_json(self) -> dict:
        tables = []
        for tbl in self._tables:
            t_iid = f"T:{tbl.norm}"
            tbl_desc = self._edits.get(t_iid, tbl.json_desc)
            cols = []
            for col in tbl.columns:
                c_iid = f"C:{tbl.norm}:{col.norm}"
                cols.append({
                    "name":        col.name,
                    "description": self._edits.get(c_iid, col.json_desc),
                })
            tables.append({
                "name":        tbl.name,
                "description": tbl_desc,
                "columns":     cols,
            })
        return {"tables": tables}

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

        # Oppdater tabelldata med nye JSON-treff, men bevar brukerediteringer
        for m in new_matches:
            t_iid = f"T:{m.table.norm}"
            existing = self._edits.get(t_iid, "")
            if not existing:
                self._edits[t_iid] = m.json_table_desc or ""
            for col in m.table.columns:
                c_iid = f"C:{m.table.norm}:{col.norm}"
                existing_c = self._edits.get(c_iid, "")
                if not existing_c:
                    self._edits[c_iid] = m.col_descs.get(col.norm, "")
            # Oppdater json_desc på tabellobjektene
            m.table.description = m.json_table_desc
            for col in m.table.columns:
                col.description = m.col_descs.get(col.norm, "")

        # Gjenbygg tabelltred med bevart tilstand
        # Oppdater _tables med nye JSON-beskrivelser fra matches
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
        self._autosave()
        self._cancelled = False
        self.destroy()

    def _cancel(self):
        self._cancelled = True
        self.destroy()

    def get_result(self):
        if self._cancelled:
            return None
        result: Dict[str, dict] = {}
        for info in self._items:
            tn = info["tbl_norm"]
            if tn not in result:
                result[tn] = {"desc": "", "cols": {}}
            iid = info["iid"]
            val = self._edits.get(iid, "").strip()
            if info["type"] == "table":
                result[tn]["desc"] = val
            else:
                result[tn]["cols"][info["col_norm"]] = val
        return result



# ── Hjelpere ──────────────────────────────────────────────────────────────────

def _shorten(text: str, n: int = 55) -> str:
    if not text:
        return ""
    return text if len(text) <= n else text[:n - 1] + "…"


# ── Fabrikk ───────────────────────────────────────────────────────────────────

def build_dialog_tables(matches) -> List[_TblItem]:
    result = []
    for m in matches:
        tbl = _TblItem(
            name=m.table.name,
            norm=m.table.norm,
            json_desc=m.json_table_desc or "",
            folder=getattr(m.table, "folder", None) or m.table.name,
            matched=m.json_table_desc is not None,
        )
        for i, col in enumerate(m.table.columns):
            tbl.columns.append(_ColItem(
                name=col.name,
                norm=col.norm,
                json_desc=m.col_descs.get(col.norm, ""),
                col_index=i,
            ))
        result.append(tbl)
    return result
