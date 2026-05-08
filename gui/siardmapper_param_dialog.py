"""
gui/siardmapper_param_dialog.py

Parameterinnstillinger for SiardMapperOperation.
Viser tilgjengelige JSON-maler fra konfigurert mappe, sortert etter
prosentvis match med valgt SIARD-fil. Støtter manuell filvelger og
generering av tom mal.
"""

from __future__ import annotations

import json
import threading
import tkinter as tk
import zipfile
from pathlib import Path
from tkinter import filedialog, messagebox

import customtkinter as ctk

from gui.styles import COLORS, FONTS


# ── Hjelp: beregn match-prosent ───────────────────────────────────────────────

def _scan_siard_tables(siard_path: Path) -> tuple[list[str], dict[str, list[str]]]:
    """
    Les tabellnavn og kolonnenavn fra SIARD-metadata.
    Returnerer (table_norms, {table_norm: [col_norms]}).
    """
    import xml.etree.ElementTree as ET
    _NS_URI = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
    _META = ("header/metadata.xml", "metadata.xml")

    try:
        with zipfile.ZipFile(siard_path, "r") as zf:
            nl = {n.lower(): n for n in zf.namelist()}
            me = next((nl[c] for c in _META if c in nl), None)
            if not me:
                return [], {}
            xml_bytes = zf.read(me)
    except Exception:
        return [], {}

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return [], {}

    tables: list[str] = []
    cols: dict[str, list[str]] = {}
    schemas_el = root.find(f"{{{_NS_URI}}}schemas")
    if not schemas_el:
        return [], {}
    for schema_el in schemas_el.findall(f"{{{_NS_URI}}}schema"):
        tables_el = schema_el.find(f"{{{_NS_URI}}}tables")
        if not tables_el:
            continue
        for table_el in tables_el.findall(f"{{{_NS_URI}}}table"):
            name_el = table_el.find(f"{{{_NS_URI}}}name")
            if name_el is None or not name_el.text:
                continue
            tn = name_el.text.strip().lower()
            tables.append(tn)
            col_list: list[str] = []
            cols_el = table_el.find(f"{{{_NS_URI}}}columns")
            if cols_el:
                for col_el in cols_el.findall(f"{{{_NS_URI}}}column"):
                    cn_el = col_el.find(f"{{{_NS_URI}}}name")
                    if cn_el is not None and cn_el.text:
                        col_list.append(cn_el.text.strip().lower())
            cols[tn] = col_list
    return tables, cols


def _analyze_template(json_path: Path,
                      siard_tables: list[str],
                      siard_cols: dict[str, list[str]]) -> dict:
    """
    Leser JSON-malfil én gang og returnerer match-prosenter + mal-metadata.
    Nøkler: tbl_pct, col_pct, desc_count, total_fields,
            name, description, systemName, systemVersjon
    """
    total_fields = len(siard_tables) + sum(len(v) for v in siard_cols.values())
    result = {
        "tbl_pct": 0, "col_pct": 0, "desc_count": 0, "total_fields": total_fields,
        "name": "", "description": "",
        "systemName": "", "systemVersjon": "", "modelVersion": "",
    }

    # Les JSON-filen alltid — metadata trengs uavhengig av om SIARD har tabeller
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        return result

    if not isinstance(data, dict):
        return result

    # Finn schema-objektet (kan ligge på toppnivå eller nestet under f.eks. templateSchema)
    def _find_schema(d: dict):
        for k in ("tables", "tabeller"):
            if isinstance(d.get(k), list):
                return d, d[k]
        for v in d.values():
            if isinstance(v, dict):
                for k in ("tables", "tabeller"):
                    if isinstance(v.get(k), list):
                        return v, v[k]
        return d, []

    schema, tbl_list = _find_schema(data)

    # Hent metadata fra schema-objektet; faller tilbake til rot-objektet
    def _get(*keys: str) -> str:
        for k in keys:
            v = schema.get(k) or data.get(k)
            if v and isinstance(v, str):
                return v.strip()
        return ""

    result["name"]          = _get("name")
    result["description"]   = _get("description")
    result["systemName"]    = _get("systemName")
    result["systemVersjon"] = _get("systemVersjon", "systemVersion")
    result["modelVersion"]  = _get("modelVersion", "modelVersjon")

    # Hopp over match-beregning om SIARD-filen ikke har tabeller
    if not siard_tables:
        return result

    json_tbls:  dict[str, list[str]] = {}
    json_descs: dict[str, dict]      = {}
    for t in tbl_list:
        if not isinstance(t, dict):
            continue
        tn = t.get("name", "").strip().lower()
        if not tn:
            continue
        json_tbls[tn] = []
        json_descs[tn] = {"__tbl__": (t.get("description") or "").strip()}
        for c in t.get("columns", []):
            if not isinstance(c, dict):
                continue
            cn = c.get("name", "").strip().lower()
            if cn:
                json_tbls[tn].append(cn)
                json_descs[tn][cn] = (c.get("description") or "").strip()

    matched_t = sum(1 for t in siard_tables if t in json_tbls)
    tbl_pct   = round(matched_t / len(siard_tables) * 100) if siard_tables else 0

    total_c   = sum(len(v) for v in siard_cols.values())
    matched_c = 0
    for tn, cn_list in siard_cols.items():
        jcols = set(json_tbls.get(tn, []))
        matched_c += sum(1 for c in cn_list if c in jcols)
    col_pct = round(matched_c / total_c * 100) if total_c else 0

    desc_count = 0
    siard_set = set(siard_tables)
    for tn in siard_set:
        if tn not in json_descs:
            continue
        if json_descs[tn].get("__tbl__"):
            desc_count += 1
        siard_cn_set = set(siard_cols.get(tn, []))
        for cn, desc in json_descs[tn].items():
            if cn != "__tbl__" and cn in siard_cn_set and desc:
                desc_count += 1

    result.update(tbl_pct=tbl_pct, col_pct=col_pct,
                  desc_count=desc_count, total_fields=total_fields)
    return result


# ── Param-dialog ──────────────────────────────────────────────────────────────

class SiardMapperParamDialog(ctk.CTkToplevel):

    def __init__(self, parent, op_def: dict, on_confirm, on_saved=None):
        super().__init__(parent)
        self._op_def   = op_def
        self._confirm  = on_confirm
        self._on_saved = on_saved

        from settings import get_config
        self._template_dir = Path(get_config("json_template_dir") or "")

        # Hent gjeldende parameterverdier
        existing = {p["key"]: p.get("default", "")
                    for p in op_def.get("params", [])}
        self._json_path_var = ctk.StringVar(
            value=existing.get("json_template", ""))
        self._overwrite_var = ctk.BooleanVar(
            value=existing.get("overwrite_existing", False))

        # Aktiv SIARD-fil fra operations panel
        from gui.operations_panel import _current_siard_path
        initial_siard = str(_current_siard_path) if _current_siard_path else ""
        self._siard_var = ctk.StringVar(value=initial_siard)

        # Intern state
        self._siard_tables: list[str] = []
        self._siard_cols:   dict[str, list[str]] = {}
        self._template_entries: list[dict] = []   # {path, tbl_pct, col_pct, name, ...}
        self._list_items:  list[str] = []          # display strings i listbox
        self._loading = False

        # Tooltip-state
        self._tooltip_win:   tk.Toplevel | None = None
        self._tooltip_after: str | None = None
        self._tooltip_idx = -1

        self.title("Berik SIARD-metadata — innstillinger")
        self.resizable(True, False)
        self.grab_set()
        self.lift()
        self._build()
        self._center()

        # Last SIARD-tabeller og JSON-maler i bakgrunn
        if initial_siard and Path(initial_siard).exists():
            self.after(100, self._refresh_async)

    # ── Bygging ───────────────────────────────────────────────────────────────

    def _build(self):
        self.configure(fg_color=COLORS["bg"])
        PAD = {"padx": 16, "pady": 6}

        # Header
        hdr = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        hdr.pack(fill="x")
        ctk.CTkLabel(hdr, text="  SIARDMapper — velg JSON-mal",
                     font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
                     text_color=COLORS["accent"], anchor="w",
        ).pack(side="left", padx=12, pady=10)

        body = ctk.CTkFrame(self, fg_color="transparent")
        body.pack(fill="x", padx=0, pady=0)
        body.grid_columnconfigure(0, weight=1)

        r = 0
        # ── SIARD-fil ─────────────────────────────────────────────────────────
        ctk.CTkLabel(body, text="SIARD-fil:",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"], anchor="w",
        ).grid(row=r, column=0, sticky="w", **PAD)
        r += 1

        #siard_row = ctk.CTkFrame(body, fg_color="transparent")
        #siard_row.grid(row=r, column=0, sticky="ew", padx=16, pady=(0, 4))
        #siard_row.grid_columnconfigure(0, weight=1)
        #self._siard_entry = ctk.CTkEntry(
        #    siard_row, textvariable=self._siard_var,
        #    font=ctk.CTkFont(family=FONTS["mono"], size=10),
        #    fg_color=COLORS["surface"], border_color=COLORS["border"],
        #)
        #self._siard_entry.grid(row=0, column=0, sticky="ew", padx=(0, 4))
        #ctk.CTkButton(
        #    siard_row, text="Bla…", width=54, height=28,
        #    fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
        #    font=ctk.CTkFont(family=FONTS["mono"], size=10),
        #    command=self._browse_siard,
        #).grid(row=0, column=1)
        #r += 1

        # ── JSON-mal-liste ────────────────────────────────────────────────────
        tdir_txt = (f"JSON-maler fra: {self._template_dir}"
                    if self._template_dir and self._template_dir.is_dir()
                    else "JSON-maler (ingen mappe konfigurert i innstillinger)")
        self._dir_lbl = ctk.CTkLabel(
            body, text=tdir_txt,
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"], anchor="w",
        )
        self._dir_lbl.grid(row=r, column=0, sticky="w", padx=16, pady=(8, 2))
        r += 1

        list_frm = tk.Frame(body, bg=COLORS["border"], bd=1)
        list_frm.grid(row=r, column=0, sticky="ew", padx=16, pady=(0, 2))
        list_frm.grid_columnconfigure(0, weight=1)

        self._listbox = tk.Listbox(
            list_frm,
            bg=COLORS["surface"], fg=COLORS["text"],
            selectbackground=COLORS["accent_dim"], selectforeground="#fff",
            font=("Courier New", 10),
            height=8, activestyle="none",
            relief="flat", bd=0,
        )
        sb = tk.Scrollbar(list_frm, orient="vertical", command=self._listbox.yview)
        self._listbox.configure(yscrollcommand=sb.set)
        self._listbox.grid(row=0, column=0, sticky="ew", padx=1, pady=1)
        sb.grid(row=0, column=1, sticky="ns", pady=1)
        self._listbox.bind("<<ListboxSelect>>", self._on_list_select)
        self._listbox.bind("<Double-1>", lambda _: self._ok())
        self._listbox.bind("<Motion>", self._on_list_motion)
        self._listbox.bind("<Leave>",  self._on_list_leave)
        r += 1

        btn_row = ctk.CTkFrame(body, fg_color="transparent")
        btn_row.grid(row=r, column=0, sticky="w", padx=16, pady=(0, 4))
        self._refresh_btn = ctk.CTkButton(
            btn_row, text="↻ Oppdater liste", height=26, width=130,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self._refresh_async,
        )
        self._refresh_btn.pack(side="left", padx=(0, 6))
        self._status_lbl = ctk.CTkLabel(
            btn_row, text="",
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            text_color=COLORS["muted"],
        )
        self._status_lbl.pack(side="left")
        r += 1

        # Skillelinje
        ctk.CTkFrame(body, fg_color=COLORS["border"], height=1).grid(
            row=r, column=0, sticky="ew", padx=16, pady=(4, 0)); r += 1

        # ── Manuelt valg + generer ────────────────────────────────────────────
        ctk.CTkLabel(body, text="Eller velg JSON-fil manuelt:",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text_sub"], anchor="w",
        ).grid(row=r, column=0, sticky="w", padx=16, pady=(6, 2))
        r += 1

        json_row = ctk.CTkFrame(body, fg_color="transparent")
        json_row.grid(row=r, column=0, sticky="ew", padx=16, pady=(0, 4))
        json_row.grid_columnconfigure(0, weight=1)
        self._json_entry = ctk.CTkEntry(
            json_row, textvariable=self._json_path_var,
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            fg_color=COLORS["surface"], border_color=COLORS["border"],
        )
        self._json_entry.grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ctk.CTkButton(
            json_row, text="Bla…", width=54, height=28,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self._browse_json,
        ).grid(row=0, column=1)
        r += 1

        ctk.CTkButton(
            body, text="Generer tom JSON-mal fra valgt SIARD",
            height=28, anchor="w",
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            font=ctk.CTkFont(family=FONTS["mono"], size=10),
            command=self._generate_template,
        ).grid(row=r, column=0, sticky="w", padx=16, pady=(0, 6))
        r += 1

        # ── Innstillinger ─────────────────────────────────────────────────────
        ctk.CTkFrame(body, fg_color=COLORS["border"], height=1).grid(
            row=r, column=0, sticky="ew", padx=16, pady=(0, 4)); r += 1

        ctk.CTkCheckBox(
            body, text="Overstyr eksisterende beskrivelser",
            variable=self._overwrite_var,
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
            text_color=COLORS["text"],
            fg_color=COLORS["accent"],
        ).grid(row=r, column=0, sticky="w", padx=16, pady=(4, 8))
        r += 1

        # ── Knapper ───────────────────────────────────────────────────────────
        btns = ctk.CTkFrame(body, fg_color="transparent")
        btns.grid(row=r, column=0, sticky="ew", padx=16, pady=(0, 14))
        btns.grid_columnconfigure((0, 1), weight=1)

        ctk.CTkButton(btns, text="OK",
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                      height=34, command=self._ok,
        ).grid(row=0, column=0, padx=(0, 6), sticky="ew")

        ctk.CTkButton(btns, text="Avbryt",
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      height=34, command=self.destroy,
        ).grid(row=0, column=1, sticky="ew")

        self.protocol("WM_DELETE_WINDOW", self.destroy)

    def _center(self):
        self.update_idletasks()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        w  = 560
        h  = self.winfo_reqheight() or 680
        px, py = self.master.winfo_x(), self.master.winfo_y()
        pw, ph = self.master.winfo_width(), self.master.winfo_height()
        x = max(40, min(px + (pw - w) // 2, sw - w - 40))
        y = max(40, min(py + (ph - h) // 2, sh - h - 60))
        self.geometry(f"{w}x{h}+{x}+{y}")

    # ── SIARD-velger og oppdatering ───────────────────────────────────────────

    def _browse_siard(self):
        p = filedialog.askopenfilename(
            title="Velg SIARD-fil",
            filetypes=[("SIARD-filer", "*.siard"), ("Alle filer", "*.*")],
            parent=self,
        )
        if p:
            self._siard_var.set(p)
            self._refresh_async()

    def _refresh_async(self):
        if self._loading:
            return
        siard = self._siard_var.get().strip()
        if not siard or not Path(siard).exists():
            self._set_status("Velg en gyldig SIARD-fil")
            return
        if not self._template_dir or not self._template_dir.is_dir():
            self._set_status("Ingen JSON-mal-mappe konfigurert i innstillinger")
            return
        self._loading = True
        self._set_status("Laster…")
        self._refresh_btn.configure(state="disabled")
        threading.Thread(target=self._do_refresh,
                         args=(Path(siard),), daemon=True).start()

    def _do_refresh(self, siard_path: Path):
        from concurrent.futures import ThreadPoolExecutor
        from settings import get_config

        tables, cols = _scan_siard_tables(siard_path)
        json_files = sorted(self._template_dir.glob("*.json"))
        total = len(json_files)

        if not total:
            self.after(0, lambda: self._apply_refresh(tables, cols, []))
            return

        max_workers = max(1, int(get_config("max_workers") or 4))
        done_count = [0]

        def _match_one(jf):
            try:
                entry = _analyze_template(jf, tables, cols)
            except Exception:
                entry = {"tbl_pct": 0, "col_pct": 0, "desc_count": 0,
                         "total_fields": 0, "name": "", "description": "",
                         "systemName": "", "systemVersjon": ""}
            entry["path"] = jf
            done_count[0] += 1
            n = done_count[0]
            self.after(0, lambda: self._set_status(f"Matcher {n}/{total}…"))
            return entry

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            entries = list(ex.map(_match_one, json_files))

        # Sorter: strukturell treff først, deretter beskrivelsesdekning
        entries.sort(
            key=lambda e: (e["tbl_pct"] + e["col_pct"], e["desc_count"]),
            reverse=True)
        self.after(0, lambda: self._apply_refresh(tables, cols, entries))

    def _apply_refresh(self, tables, cols, entries):
        self._hide_tooltip()
        if self._tooltip_after:
            self.after_cancel(self._tooltip_after)
            self._tooltip_after = None
        self._tooltip_idx = -1

        self._siard_tables = tables
        self._siard_cols   = cols
        self._template_entries = entries
        self._loading = False
        self._refresh_btn.configure(state="normal")

        self._listbox.delete(0, "end")
        self._list_items.clear()

        current = self._json_path_var.get().strip()
        sel_idx = None

        for i, e in enumerate(entries):
            tp, cp = e["tbl_pct"], e["col_pct"]
            dc, tf = e["desc_count"], e["total_fields"]

            if tp == 0 and cp == 0:
                pct_str  = "0% treff"
                desc_str = ""
            else:
                pct_str = f"{tp:3d}% tab  {cp:3d}% kol"
                if tf > 0:
                    dp = round(dc / tf * 100)
                    desc_str = f"  {dp:3d}% beskrevet ({dc}/{tf})"
                else:
                    desc_str = ""

            sys_name  = e.get("systemName", "")
            sys_ver   = e.get("systemVersjon", "")
            mdl_ver   = e.get("modelVersion", "")
            parts = []
            if sys_name:
                parts.append(f"{sys_name} ({sys_ver})" if sys_ver else sys_name)
            if mdl_ver:
                parts.append(mdl_ver)
            name_part = " - ".join(parts) if parts else e["path"].stem
            if len(name_part) > 44:
                name_part = name_part[:41] + "…"
            line = f"  {name_part:<44}  {pct_str}{desc_str}"
            self._listbox.insert("end", line)
            self._list_items.append(str(e["path"]))

            # Grønt kun om 100% strukturell treff OG alle felt er beskrevet
            fully_described = (tf > 0 and dc == tf)
            combined = tp + cp
            if combined == 200 and fully_described:
                fg = "#2ecc71"          # grønn — 100% match og beskrevet
            elif combined == 200:
                fg = "#7ec8e3"          # lys blå — 100% match men mangler beskrivelser
            elif combined >= 120:
                fg = "#f0c040"          # gul — god match
            elif combined >= 40:
                fg = COLORS["text_sub"]
            else:
                fg = COLORS["muted"]
            self._listbox.itemconfigure(i, fg=fg)
            # Forhåndsvelg om dette er gjeldende valg
            if current and str(e["path"]) == current:
                sel_idx = i

        # Auto-velg øverste mal om den har noen treffprosent og ingenting er forhåndsvalgt
        if sel_idx is None and entries:
            top = entries[0]
            if top["tbl_pct"] > 0 or top["col_pct"] > 0:
                sel_idx = 0
                self._json_path_var.set(self._list_items[0])

        if sel_idx is not None:
            self._listbox.selection_set(sel_idx)
            self._listbox.see(sel_idx)

        n = len(entries)
        self._set_status(f"{n} mal(er) funnet" if n else "Ingen JSON-maler funnet")

    def _set_status(self, txt: str):
        try:
            self._status_lbl.configure(text=txt)
        except Exception:
            pass

    # ── Tooltip ───────────────────────────────────────────────────────────────

    def _on_list_motion(self, event):
        idx = self._listbox.nearest(event.y)
        if idx == self._tooltip_idx:
            return
        self._tooltip_idx = idx
        if self._tooltip_after:
            self.after_cancel(self._tooltip_after)
            self._tooltip_after = None
        self._hide_tooltip()
        if 0 <= idx < len(self._template_entries):
            rx = self._listbox.winfo_rootx() + event.x + 14
            ry = self._listbox.winfo_rooty() + event.y + 14
            self._tooltip_after = self.after(
                400, lambda e=self._template_entries[idx], x=rx, y=ry:
                     self._show_tooltip(e, x, y))

    def _on_list_leave(self, _=None):
        self._tooltip_idx = -1
        if self._tooltip_after:
            self.after_cancel(self._tooltip_after)
            self._tooltip_after = None
        self._hide_tooltip()

    def _show_tooltip(self, entry: dict, x: int, y: int):
        self._hide_tooltip()
        lines = []
        name = entry.get("name", "")
        if name:
            lines.append(f"Navn:         {name}")
        desc = entry.get("description", "")
        if desc:
            lines.append(f"Beskrivelse:  {desc}")
        sys_name  = entry.get("systemName", "")
        sys_ver   = entry.get("systemVersjon", "")
        mdl_ver   = entry.get("modelVersion", "")
        if sys_name or sys_ver:
            lines.append(f"System:       {' '.join(filter(None, [sys_name, sys_ver]))}")
        if mdl_ver:
            lines.append(f"Mal-versjon:  {mdl_ver}")
        lines.append(f"Fil:          {entry['path'].name}")

        win = tk.Toplevel(self)
        win.wm_overrideredirect(True)
        win.wm_geometry(f"+{x}+{y}")
        win.configure(bg="#2a2a3a")
        tk.Label(
            win, text="\n".join(lines),
            bg="#2a2a3a", fg="#e0e0e0",
            font=("Courier New", 9),
            justify="left", padx=10, pady=6,
            wraplength=440,
        ).pack()
        self._tooltip_win = win

    def _hide_tooltip(self):
        if self._tooltip_win:
            try:
                self._tooltip_win.destroy()
            except Exception:
                pass
            self._tooltip_win = None

    def _on_list_select(self, _=None):
        sel = self._listbox.curselection()
        if sel and sel[0] < len(self._list_items):
            self._json_path_var.set(self._list_items[sel[0]])

    # ── Filvelger + generer mal ───────────────────────────────────────────────

    def _browse_json(self):
        p = filedialog.askopenfilename(
            title="Velg JSON-malfil",
            filetypes=[("JSON-filer", "*.json"), ("Alle filer", "*.*")],
            parent=self,
        )
        if p:
            self._json_path_var.set(p)

    def _generate_template(self):
        siard_path_str = self._siard_var.get().strip()
        if not siard_path_str or not Path(siard_path_str).exists():
            messagebox.showwarning("Ingen SIARD-fil",
                                   "Velg en SIARD-fil øverst i dialogen.",
                                   parent=self)
            return

        # Bestem målmappe
        target_dir = self._template_dir if (
            self._template_dir and self._template_dir.is_dir()) else None

        if target_dir is None:
            # Be bruker velge mappe og lagre som ny json_template_dir
            chosen = filedialog.askdirectory(
                title="Velg mappe for JSON-maler (lagres i innstillinger)",
                parent=self)
            if not chosen:
                return
            target_dir = Path(chosen)
            from settings import save_config
            save_config({"json_template_dir": str(target_dir)})
            self._template_dir = target_dir
            self._dir_lbl.configure(text=f"JSON-maler fra: {target_dir}")

        stem = Path(siard_path_str).stem
        save_path_default = target_dir / f"{stem}_metadata_mal.json"

        save_path_str = filedialog.asksaveasfilename(
            title="Lagre tom JSON-mal",
            defaultextension=".json",
            filetypes=[("JSON-filer", "*.json")],
            initialdir=str(target_dir),
            initialfile=save_path_default.name,
            parent=self,
        )
        if not save_path_str:
            return

        try:
            template = _build_empty_template(Path(siard_path_str))
            Path(save_path_str).write_text(
                json.dumps(template, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            self._json_path_var.set(save_path_str)
            messagebox.showinfo("Mal generert",
                                f"Tom JSON-mal lagret til:\n{save_path_str}",
                                parent=self)
            self._refresh_async()
        except Exception as exc:
            messagebox.showerror("Feil", f"Kunne ikke generere mal:\n{exc}",
                                 parent=self)

    # ── OK ────────────────────────────────────────────────────────────────────

    def _ok(self):
        json_path = self._json_path_var.get().strip()
        if not json_path:
            messagebox.showwarning("Ingen mal valgt",
                                   "Velg en JSON-malfil eller generer en ny.",
                                   parent=self)
            return
        op = self._op_def["cls"](
            json_template=json_path,
            overwrite_existing=self._overwrite_var.get(),
        )
        self.destroy()
        self._confirm(op)


# ── Hjelp: generer tom mal ────────────────────────────────────────────────────

def _build_empty_template(siard_path: Path) -> dict:
    import xml.etree.ElementTree as ET
    _NS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
    _META = ("header/metadata.xml", "metadata.xml")

    with zipfile.ZipFile(siard_path, "r") as zf:
        nl = {n.lower(): n for n in zf.namelist()}
        me = next((nl[c] for c in _META if c in nl), None)
        if not me:
            raise ValueError("metadata.xml ikke funnet")
        xml_bytes = zf.read(me)

    root = ET.fromstring(xml_bytes)

    def _txt(el, tag):
        child = el.find(f"{{{_NS}}}{tag}")
        return (child.text or "").strip() if child is not None else ""

    tables = []
    schemas_el = root.find(f"{{{_NS}}}schemas")
    if schemas_el:
        for schema_el in schemas_el.findall(f"{{{_NS}}}schema"):
            tables_el = schema_el.find(f"{{{_NS}}}tables")
            if not tables_el:
                continue
            for table_el in tables_el.findall(f"{{{_NS}}}table"):
                tname = _txt(table_el, "name")
                if not tname:
                    continue
                cols = []
                cols_el = table_el.find(f"{{{_NS}}}columns")
                if cols_el:
                    for col_el in cols_el.findall(f"{{{_NS}}}column"):
                        cname = _txt(col_el, "name")
                        if cname:
                            cols.append({"name": cname, "description": ""})
                tables.append({"name": tname, "description": "", "columns": cols})

    return {"tables": tables}
