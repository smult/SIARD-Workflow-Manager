"""
gui/dias_dialog.py
------------------
Utvidet DIAS-pakke-dialog med to-kolonne-layout:
  Venstre: parameter-skjema (alle metadata-felt)
  Høyre:   interaktiv filtrestruktur (ttk.Treeview) med auto-oppdagelse av
           loggfiler, SHA256, rapport og prosjektfil.

Brukeren kan dra-slippe filer mellom mapper, legge til egne filer og fjerne
filer de ikke ønsker med. Resultatet sendes som JSON-streng i «extra_files»-
parameteren til DiasPackageOperation.
"""
from __future__ import annotations

import datetime
import json
import sys
import tkinter as tk
import tkinter.ttk as ttk
from pathlib import Path
from tkinter import filedialog

import customtkinter as ctk

sys.path.insert(0, str(Path(__file__).parent.parent))
from gui.styles import COLORS, FONTS
from settings import save_op_params, _SETTINGS_FILE

# ── Konstanter ────────────────────────────────────────────────────────────────

_FOLDERS = {
    "root":     "",
    "content":  "content",
    "adm":      "administrative_metadata",
    "repo_ops": "administrative_metadata/repository_operations",
    "desc":     "descriptive_metadata",
}

# Kjente operasjonssuffikser som strippes for å finne opprinnelig arkivnavn
_OP_SUFFIXES = [
    "_konvertert", "_hex_extracted", "_cosdoc", "_blob", "_dias",
]

# ── Hjelpefunksjoner ──────────────────────────────────────────────────────────

def _base_stem(siard_stem: str) -> str:
    """
    Strip kjente operasjonssuffikser iterativt for å finne opprinnelig
    arkivnavn.  Eks: «WIS-Marnardal_2024_hex_extracted_konvertert» → «WIS-Marnardal_2024».
    """
    stem = siard_stem
    changed = True
    while changed:
        changed = False
        for suf in _OP_SUFFIXES:
            if stem.lower().endswith(suf.lower()):
                stem = stem[: -len(suf)]
                changed = True
    return stem


def _discover_files(siard_path: Path) -> list[dict]:
    """
    Finn logg-filer, SHA256, rapport og prosjektfil som tilhører dette
    uttrekket.  Tar utgangspunkt i opprinnelig arkivnavn (uten operasjons-
    suffikser) som prefix, slik at filer fra andre SIARD-arkiver i samme
    mappe ikke plukkes opp.

    Returnerer liste av {src, dest, folder_id, tag="auto", name}.
    """
    parent = siard_path.parent
    base   = _base_stem(siard_path.stem)   # opprinnelig arkivnavn
    found:      list[dict] = []
    seen_names: set[str]   = set()

    def _add(path: Path, folder_id: str) -> None:
        if path.name not in seen_names and path.exists():
            dest_dir = _FOLDERS[folder_id]
            found.append({
                "src":       str(path),
                "dest":      f"{dest_dir}/{path.name}",
                "folder_id": folder_id,
                "tag":       "auto",
                "name":      path.name,
            })
            seen_names.add(path.name)

    # Mønstre er nå forankret til base-prefiks med underscore-separator:
    # - {base}_* fanger bare filer fra dette uttrekket, ikke andre arkiver
    # - {base}.ext bruker dot-separator for nøyaktig filnavn-match

    for path in sorted(parent.glob(f"{base}_*_blob_konvertering.csv")):
        _add(path, "repo_ops")

    for path in sorted(parent.glob(f"{base}_*_konvertering_feil.log")):
        _add(path, "repo_ops")

    for path in sorted(parent.glob(f"{base}_*_workflow_rapport_*.html")):
        _add(path, "repo_ops")

    # Workflow-logg: {base}_YYYYMMDD_HHMMSS.log — ekskluder feilogg-varianter
    for path in sorted(parent.glob(f"{base}_*.log")):
        if "_konvertering_feil" not in path.name:
            _add(path, "repo_ops")

    # SHA256 og prosjektfil: nøyaktig filnavn (dot-separator)
    _add(parent / f"{base}.sha256",  "adm")
    _add(parent / f"{base}.siardwf", "adm")

    return found


def _find_content_siard(siard_path: Path) -> Path:
    """
    Finn SIARD-filen som faktisk skal pakkes.

    Returnerer den nyeste prosesserte varianten (f.eks. _konvertert.siard)
    hvis den finnes i samme mappe.  Faller tilbake til kilde-SIARD hvis
    ingen prosessert variant eksisterer.
    """
    parent = siard_path.parent
    base   = _base_stem(siard_path.stem)

    # Finn alle SIARD-filer i mappen som starter med base og har suffiks
    candidates = [
        p for p in parent.glob(f"{base}_*.siard")
        if p.is_file()
    ]

    if candidates:
        # Velg nyeste (mest nylig prosesserte)
        return max(candidates, key=lambda p: p.stat().st_mtime)

    return siard_path


def _mime_icon(name: str) -> str:
    ext = Path(name).suffix.lower().lstrip(".")
    return {"log": "📋", "csv": "📊", "html": "🌐", "sha256": "🔑",
            "siard": "🗄", "siardwf": "🗂", "pdf": "📄"}.get(ext, "📄")


# ── Hoveddialog ───────────────────────────────────────────────────────────────

class DiasParamDialog(ctk.CTkToplevel):
    """
    Utvidet konfigurasjonsdialog for DIAS-pakke-operasjonen.
    Viser param-skjema til venstre og interaktiv filtrestruktur til høyre.
    """

    def __init__(self, parent, op_def: dict, on_confirm, on_saved=None):
        super().__init__(parent)
        self.title("DIAS-pakking: Konfigurer og velg filer")
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self.geometry("1160x780")
        self.minsize(960, 620)
        self.resizable(True, True)

        self._op_def      = op_def
        self._on_confirm  = on_confirm
        self._on_saved    = on_saved
        self._vars: dict  = {}

        # Filtre-data: item_id → {src, dest, folder_id, tag, name}
        self._file_entries: dict[str, dict] = {}

        # Drag-state
        self._drag_item:         str | None = None
        self._drag_folder_hover: str | None = None

        # Finn aktiv SIARD-sti
        try:
            from gui.operations_panel import _current_siard_path
            self._siard_path: Path | None = _current_siard_path
        except Exception:
            self._siard_path = None

        self._build()
        self._populate_tree()

    # ── Bygg hoveddialog ──────────────────────────────────────────────────────

    def _build(self):
        # Kolonne 0 (metadata): 2/3, kolonne 1 (tre): 1/3, minimum 300px
        self.grid_columnconfigure(0, weight=2, minsize=500)
        self.grid_columnconfigure(1, weight=1, minsize=300)
        # Rad 0: kompakt tittel, rad 1: innhold, rad 2: knapper
        self.grid_rowconfigure(0, weight=0)
        self.grid_rowconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=0)

        # ── Tittellinje (kompakt) ──────────────────────────────────────────
        ctk.CTkLabel(self,
                     text="DIAS-pakking (SIP/AIC)",
                     font=ctk.CTkFont(family=FONTS["mono"], size=14, weight="bold"),
                     text_color=COLORS["accent"]
                     ).grid(row=0, column=0, columnspan=2,
                            padx=20, pady=(12, 6), sticky="w")

        # ── Venstre: metadata-skjema ───────────────────────────────────────
        left = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=8)
        left.grid(row=1, column=0, padx=(16, 6), pady=(0, 8), sticky="nsew")
        # Rad 0 = liten tittel, rad 1 = skjema (vokser)
        left.grid_rowconfigure(0, weight=0)
        left.grid_rowconfigure(1, weight=1)
        left.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(left, text="METADATA",
                     font=ctk.CTkFont(family=FONTS["mono"], size=12, weight="bold"),
                     text_color=COLORS["muted"]
                     ).grid(row=0, column=0, padx=14, pady=(8, 2), sticky="w")

        self._build_form(left)   # plasserer skjema i rad 1

        # ── Høyre: filtrestruktur ──────────────────────────────────────────
        right = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=8)
        right.grid(row=1, column=1, padx=(6, 16), pady=(0, 8), sticky="nsew")
        # Rad 0 = liten tittel, rad 1 = tre (vokser), rad 2 = knapper, rad 3 = legende
        right.grid_rowconfigure(0, weight=0)
        right.grid_rowconfigure(1, weight=1)
        right.grid_rowconfigure(2, weight=0)
        right.grid_rowconfigure(3, weight=0)
        right.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(right, text="PAKKESTRUKTUR",
                     font=ctk.CTkFont(family=FONTS["mono"], size=12, weight="bold"),
                     text_color=COLORS["muted"]
                     ).grid(row=0, column=0, padx=14, pady=(8, 2), sticky="w")

        self._build_tree_panel(right)  # plasserer tre i rad 1-3

        # ── Knapper ────────────────────────────────────────────────────────
        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=2, column=0, columnspan=2,
                  padx=16, pady=(0, 14), sticky="e")
        ctk.CTkButton(btns, text="Avbryt", width=100,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=13),
                      command=self.destroy).pack(side="left", padx=(0, 8))
        ctk.CTkButton(btns, text="Legg til i workflow", width=170,
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
                      command=self._confirm).pack(side="left")

    # ── Param-skjema ──────────────────────────────────────────────────────────

    def _build_form(self, parent: ctk.CTkFrame):
        """Bygg skjema i rad 1 av parent (rad 0 er tittel, satt i _build)."""
        frm = ctk.CTkScrollableFrame(
            parent, fg_color="transparent",
            scrollbar_button_color=COLORS["border"])
        frm.grid(row=1, column=0, padx=10, pady=(2, 10), sticky="nsew")
        # Kolonne 0: felt-titler — styres av lengste label, ingen fast bredde
        # Kolonne 1: input-bokser — fyller 2/3 av resterende bredde
        # Kolonne 2: tom spacer  — fyller 1/3 (gir smalere input-felt)
        frm.grid_columnconfigure(0, weight=0)
        frm.grid_columnconfigure(1, weight=2)
        frm.grid_columnconfigure(2, weight=1)

        from gui.operations_panel import _get_autocomplete_list, _AutocompleteEntry

        _inp = dict(padx=(6, 12), sticky="ew")   # felles plasseringsargumenter

        for i, p in enumerate(self._op_def.get("params", [])):
            ctk.CTkLabel(frm, text=p["label"],
                         font=ctk.CTkFont(family=FONTS["mono"], size=12),
                         text_color=COLORS["text"]
                         ).grid(row=i, column=0, padx=(10, 4), pady=5, sticky="w")

            if p["type"] == "bool":
                var = ctk.BooleanVar(value=p["default"])
                ctk.CTkSwitch(frm, text="", variable=var,
                              onvalue=True, offvalue=False,
                              button_color=COLORS["accent"]
                              ).grid(row=i, column=1, **_inp)
                self._vars[p["key"]] = (var, "bool")
                continue

            elif p["type"] == "choice":
                var = ctk.StringVar(value=str(p["default"]))
                ctk.CTkOptionMenu(
                    frm, variable=var,
                    values=p.get("choices", [str(p["default"])]),
                    fg_color=COLORS["bg"],
                    button_color=COLORS["accent"],
                    button_hover_color=COLORS["accent_dim"],
                    dropdown_fg_color=COLORS["panel"],
                    font=ctk.CTkFont(family=FONTS["mono"], size=12),
                    width=1,          # minimumverdi — grid stretcher til kolonne-bredde
                    dynamic_resizing=False,
                ).grid(row=i, column=1, **_inp)
                self._vars[p["key"]] = (var, "choice")

            elif p["type"] == "autocomplete":
                var = ctk.StringVar(value=str(p["default"]))
                ac = _AutocompleteEntry(
                    frm,
                    full_list=_get_autocomplete_list(p.get("source", "")),
                    variable=var,
                    siard_source=p.get("source", ""),
                    width=1)          # minimumverdi — grid stretcher til kolonne-bredde
                ac.grid(row=i, column=1, **_inp)
                self._vars[p["key"]] = (var, "str")
                continue

            elif p["type"] == "int":
                var = ctk.StringVar(value=str(p["default"]))
                ctk.CTkEntry(frm, textvariable=var, width=1,
                             fg_color=COLORS["bg"],
                             font=ctk.CTkFont(family=FONTS["mono"], size=12),
                             justify="left",
                             ).grid(row=i, column=1, **_inp)

            else:
                if (p["key"] == "uttrekksdato"
                        and self._siard_path
                        and self._siard_path.exists()):
                    mtime    = self._siard_path.stat().st_mtime
                    _default = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")
                else:
                    _default = str(p["default"])
                var = ctk.StringVar(value=_default)
                ctk.CTkEntry(frm, textvariable=var, width=1,
                             fg_color=COLORS["bg"],
                             font=ctk.CTkFont(family=FONTS["mono"], size=12),
                             justify="left",
                             ).grid(row=i, column=1, **_inp)

            self._vars[p["key"]] = (var, p["type"])

    # ── Filtrestruktur ────────────────────────────────────────────────────────

    def _build_tree_panel(self, parent: ctk.CTkFrame):
        """Bygg tre i rad 1, knapper i rad 2, legende i rad 3 av parent."""
        self._style_tree()

        tree_outer = tk.Frame(parent, bg=COLORS["border"], bd=1, relief="flat")
        tree_outer.grid(row=1, column=0, padx=10, pady=(2, 6), sticky="nsew")

        self._tree = ttk.Treeview(
            tree_outer,
            columns=("source",),
            show="tree",
            selectmode="browse",
            style="Dias.Treeview")
        self._tree.column("#0",     width=320, stretch=True)
        self._tree.column("source", width=0,   stretch=False)

        vsb = ttk.Scrollbar(tree_outer, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.pack(side="left",  fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self._tree.bind("<ButtonPress-1>",   self._drag_press)
        self._tree.bind("<B1-Motion>",       self._drag_motion)
        self._tree.bind("<ButtonRelease-1>", self._drag_release)

        # Knapper
        btn_bar = ctk.CTkFrame(parent, fg_color="transparent")
        btn_bar.grid(row=2, column=0, padx=10, pady=(0, 4), sticky="ew")
        btn_cfg = dict(height=30, fg_color=COLORS["btn"],
                       hover_color=COLORS["btn_hover"],
                       font=ctk.CTkFont(family=FONTS["mono"], size=12))
        ctk.CTkButton(btn_bar, text="➕  Legg til fil", **btn_cfg,
                      command=self._add_file).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btn_bar, text="🗑  Fjern", **btn_cfg,
                      command=self._remove_file).pack(side="left")
        ctk.CTkButton(btn_bar, text="🔄  Oppdater",
                      height=30,
                      fg_color=COLORS["accent_dim"],
                      hover_color=COLORS["accent"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=12),
                      command=self._refresh_auto).pack(side="right")

        # Legende
        ctk.CTkLabel(parent,
                     text="🟡 Auto-oppdaget   ⚪ Manuelt lagt til   🔒 Låst",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["muted"]
                     ).grid(row=3, column=0, padx=10, pady=(0, 8), sticky="w")

    def _style_tree(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        bg = COLORS["panel"]
        style.configure("Dias.Treeview",
                         background=bg,
                         foreground=COLORS["text"],
                         fieldbackground=bg,
                         bordercolor=COLORS["border"],
                         rowheight=26,
                         font=("Courier New", 10))
        style.configure("Dias.Treeview.Heading",
                         background=COLORS["surface"],
                         foreground=COLORS["muted"],
                         font=("Courier New", 10))
        style.map("Dias.Treeview",
                  background=[("selected", COLORS["accent"])],
                  foreground=[("selected", "#ffffff")])

    # ── Fyll inn tre ──────────────────────────────────────────────────────────

    def _populate_tree(self):
        tree = self._tree
        for item in tree.get_children():
            tree.delete(item)
        self._file_entries.clear()

        # Bruk prosessert SIARD (_konvertert etc.) hvis den finnes, ellers kilde
        content_siard = (
            _find_content_siard(self._siard_path)
            if self._siard_path else None)
        siard_name = content_siard.name if content_siard else "arkiv.siard"
        pkg_label  = _base_stem(siard_name.replace(".siard", ""))

        self._folder_ids: dict[str, str] = {}

        root_id = tree.insert("", "end", text=f"📦  {pkg_label}/",
                               open=True, tags=("folder",))
        self._folder_ids["root"] = root_id

        cnt_id = tree.insert(root_id, "end", text="📁  content/",
                              open=True, tags=("folder",))
        self._folder_ids["content"] = cnt_id

        adm_id = tree.insert(root_id, "end", text="📁  administrative_metadata/",
                              open=True, tags=("folder",))
        self._folder_ids["adm"] = adm_id

        repo_id = tree.insert(adm_id, "end", text="📁  repository_operations/",
                               open=True, tags=("folder",))
        self._folder_ids["repo_ops"] = repo_id

        desc_id = tree.insert(root_id, "end", text="📁  descriptive_metadata/",
                               open=False, tags=("folder",))
        self._folder_ids["desc"] = desc_id

        tree.tag_configure("folder", foreground="#4f8ef7")
        tree.tag_configure("locked", foreground=COLORS["muted"])
        tree.tag_configure("auto",   foreground="#f0c040")
        tree.tag_configure("user",   foreground=COLORS["text"])
        tree.tag_configure("drag_hover", foreground="#2ecc71")

        if content_siard:
            self._insert_file(
                src=str(content_siard),
                dest=f"content/{siard_name}",
                folder_id="content",
                name=siard_name,
                tag="locked")
            for ef in _discover_files(self._siard_path):
                self._insert_file(**ef)

    def _insert_file(self, src: str, dest: str, folder_id: str,
                     name: str = "", tag: str = "user") -> str:
        if not name:
            name = Path(src).name
        label   = f"{_mime_icon(name)}  {name}"
        parent  = self._folder_ids.get(folder_id, self._folder_ids["adm"])
        item_id = self._tree.insert(parent, "end", text=label,
                                     values=(src,), tags=(tag,))
        self._file_entries[item_id] = {
            "src": src, "dest": dest,
            "folder_id": folder_id, "tag": tag, "name": name,
        }
        return item_id

    def _refresh_auto(self):
        for item_id, info in list(self._file_entries.items()):
            if info["tag"] == "auto":
                self._tree.delete(item_id)
                del self._file_entries[item_id]
        if self._siard_path:
            for ef in _discover_files(self._siard_path):
                self._insert_file(**ef)

    # ── Legg til / fjern ──────────────────────────────────────────────────────

    def _add_file(self):
        path = filedialog.askopenfilename(title="Legg til fil i pakken", parent=self)
        if not path:
            return
        sel = self._tree.focus()
        folder_id = "adm"
        if sel:
            if "folder" in self._tree.item(sel, "tags"):
                for fid, nid in self._folder_ids.items():
                    if nid == sel:
                        folder_id = fid
                        break
            elif sel in self._file_entries:
                folder_id = self._file_entries[sel]["folder_id"]
        fname    = Path(path).name
        dest_dir = _FOLDERS.get(folder_id, "administrative_metadata")
        self._insert_file(src=path, dest=f"{dest_dir}/{fname}",
                          folder_id=folder_id, name=fname, tag="user")

    def _remove_file(self):
        sel = self._tree.focus()
        if sel and sel in self._file_entries:
            if self._file_entries[sel]["tag"] != "locked":
                self._tree.delete(sel)
                del self._file_entries[sel]

    # ── Drag-and-drop ─────────────────────────────────────────────────────────

    def _drag_press(self, event):
        item = self._tree.identify_row(event.y)
        tags = self._tree.item(item, "tags") if item else ()
        self._drag_item         = item if ("auto" in tags or "user" in tags) else None
        self._drag_folder_hover = None

    def _drag_motion(self, event):
        if not self._drag_item:
            return
        target = self._tree.identify_row(event.y)
        if not target:
            return
        tags      = self._tree.item(target, "tags")
        new_hover = target if "folder" in tags else self._tree.parent(target) or None
        if new_hover != self._drag_folder_hover:
            if self._drag_folder_hover:
                self._tree.item(self._drag_folder_hover, tags=("folder",))
            if new_hover:
                self._tree.item(new_hover, tags=("folder", "drag_hover"))
            self._drag_folder_hover = new_hover

    def _drag_release(self, event):
        target_folder = self._drag_folder_hover
        drag_item     = self._drag_item
        self._drag_item = None
        if target_folder:
            self._tree.item(target_folder, tags=("folder",))
        self._drag_folder_hover = None

        if not target_folder or not drag_item or drag_item not in self._file_entries:
            return
        new_folder_id = next(
            (fid for fid, nid in self._folder_ids.items() if nid == target_folder), None)
        if new_folder_id is None:
            return
        info      = self._file_entries[drag_item]
        dest_dir  = _FOLDERS.get(new_folder_id, "administrative_metadata")
        self._tree.move(drag_item, target_folder, "end")
        info["folder_id"] = new_folder_id
        info["dest"]      = f"{dest_dir}/{info['name']}"

    # ── Bekreft ───────────────────────────────────────────────────────────────

    def _confirm(self):
        kwargs: dict = {}
        for key, (var, typ) in self._vars.items():
            val = var.get()
            if typ == "int":
                try:
                    val = int(val)
                except ValueError:
                    val = 0
            elif typ == "bool":
                val = bool(val)
            kwargs[key] = val

        extra = [
            {"src": info["src"], "dest": info["dest"]}
            for info in self._file_entries.values()
            if info["tag"] != "locked"
        ]
        kwargs["extra_files"] = json.dumps(extra, ensure_ascii=False)

        op_cls = self._op_def.get("cls")
        op     = op_cls(**kwargs) if op_cls else None

        if op and op.operation_id:
            save_kwargs = {k: v for k, v in kwargs.items() if k != "extra_files"}
            try:
                save_op_params(op.operation_id, save_kwargs)
                if self._on_saved:
                    self._on_saved(op.operation_id, save_kwargs, _SETTINGS_FILE)
            except Exception as exc:
                if self._on_saved:
                    self._on_saved(op.operation_id, save_kwargs, None, error=str(exc))

        self._on_confirm(op)
        self.destroy()
