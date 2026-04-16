from __future__ import annotations
from typing import Callable
import sys
import tkinter as tk
from pathlib import Path
import customtkinter as ctk

sys.path.insert(0, str(Path(__file__).parent.parent))

from gui.styles import COLORS, FONTS, cat_color
from siard_workflow.operations import (
    SHA256Operation, BlobCheckOperation, BlobConvertOperation,
    HexExtractOperation,
    XMLValidationOperation, MetadataExtractOperation,
    VirusScanOperation, ConditionalOperation,
)
from settings import save_op_params, save_config, get_config, _SETTINGS_FILE


class _ToolTip:
    """Balloon-tooltip for tkinter/CustomTkinter-widgets."""

    def __init__(self, widget, text: str, delay: int = 500):
        self._widget  = widget
        self._text    = text
        self._delay   = delay
        self._tip_win = None
        self._job_id  = None
        widget.bind("<Enter>",       self._schedule, add="+")
        widget.bind("<Leave>",       self._hide,     add="+")
        widget.bind("<ButtonPress>", self._hide,     add="+")

    def _schedule(self, _=None):
        self._cancel()
        self._job_id = self._widget.after(self._delay, self._show)

    def _cancel(self):
        if self._job_id:
            self._widget.after_cancel(self._job_id)
            self._job_id = None

    def _show(self):
        if self._tip_win:
            return
        x = self._widget.winfo_rootx() + 8
        y = self._widget.winfo_rooty() + self._widget.winfo_height() + 4
        self._tip_win = tw = tk.Toplevel(self._widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        tk.Label(
            tw, text=self._text,
            justify="left",
            background=COLORS["panel"],
            foreground=COLORS["text"],
            font=("Courier New", 11),
            relief="solid", borderwidth=1,
            wraplength=300, padx=8, pady=5,
        ).pack()

    def _hide(self, _=None):
        self._cancel()
        if self._tip_win:
            self._tip_win.destroy()
            self._tip_win = None


def _dim(hex_color, factor=0.3):
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    bg_r, bg_g, bg_b = 0x19, 0x1d, 0x28
    r2 = int(r * factor + bg_r * (1 - factor))
    g2 = int(g * factor + bg_g * (1 - factor))
    b2 = int(b * factor + bg_b * (1 - factor))
    return "#{:02x}{:02x}{:02x}".format(r2, g2, b2)


OP_DEFS = [
    {
        "cls": SHA256Operation,
        "label": "SHA-256 Sjekksum",
        "category": "Integritet",
        "desc": "Beregner SHA-256 sjekksum for hele SIARD-filen.",
        "params": [
            {"key": "save_to_file", "label": "Lagre .sha256-fil", "type": "bool", "default": False},
            {"key": "chunk_size",   "label": "Chunk-storrelse (bytes)", "type": "int", "default": 8192},
        ],
    },
    {
        "cls": BlobCheckOperation,
        "label": "BLOB/CLOB Kontroll",
        "category": "Innhold",
        "desc": "Sjekker om uttrekket inneholder binærfiler i Content/SchemaX/tableX.",
        "params": [
            {"key": "content_prefix", "label": "Content-prefiks", "type": "str", "default": "content/"},
        ],
    },
    {
        "cls": BlobConvertOperation,
        "label": "BLOB Konverter til PDF/A",
        "category": "Innhold",
        "desc": "Identifiserer blob-filer (.bin/.txt/andre), konverterer dokumenter til PDF/A, ekstraher inline NBLOB/NCLOB. Filer som er ren tekst, XML eller ukjent format beholdes. Oppdaterer SIARD-arkivet.",
        "params": [
            {"key": "output_suffix",      "label": "Suffix ny SIARD-fil",          "type": "str",    "default": "_konvertert"},
            {"key": "pdfa_version",       "label": "PDF/A-versjon",                 "type": "choice",
             "default": get_config("pdfa_version") or "PDF/A-2u (ISO 19005-2, level U)",
             "choices": [
                 "PDF/A-1a (ISO 19005-1, level A)",
                 "PDF/A-1b (ISO 19005-1, level B)",
                 "PDF/A-2b (ISO 19005-2, level B)",
                 "PDF/A-2u (ISO 19005-2, level U)",
                 "PDF/A-3b (ISO 19005-3, level B)",
             ]},
            {"key": "lo_timeout",         "label": "LibreOffice timeout (s)",       "type": "int",    "default": 300},
            {"key": "max_workers",        "label": "Parallelle tråder",             "type": "hw_int", "default": get_config("max_workers"), "hw_key": "max_workers"},
            {"key": "lo_batch_size",      "label": "Batch-størrelse (filer/batch)", "type": "int",    "default": get_config("lo_batch_size")},
            {"key": "skip_existing_pdf",  "label": "Hopp over eksist. PDF",         "type": "bool",   "default": True},
            {"key": "extract_inline",     "label": "Ekstraher inline NBLOB/NCLOB",  "type": "bool",   "default": True},
            {"key": "dry_run",            "label": "Tørkjøring (ikke skriv)",       "type": "bool",   "default": False},
        ],
    },
    {
        "cls": HexExtractOperation,
        "label": "HEX Inline Extract",
        "category": "Innhold",
        "desc": "Dekoder inline HEX CLOB-tekst i tableX.xml og eksporterer til eksterne .txt-filer. Kjøres før BLOB Konverter.",
        "params": [
            {"key": "min_text_length", "label": "Min. tekstlengde (tegn)",    "type": "int",  "default": 30},
            {"key": "dry_run",         "label": "Tørkjøring (ikke skriv)",     "type": "bool", "default": False},
        ],
    },
    {
        "label": "XML-validering",
        "category": "Validering",
        "desc": "Validerer metadata.xml og tabellskjemaer.",
        "params": [
            {"key": "check_table_xsd", "label": "Sjekk tableX.xsd", "type": "bool", "default": True},
        ],
    },
    {
        "cls": MetadataExtractOperation,
        "label": "Metadata-uttrekk",
        "category": "Metadata",
        "desc": "Henter databasenavn, DBMS, tabeller og rader.",
        "params": [],
    },
    {
        "cls": VirusScanOperation,
        "label": "Virusskan",
        "category": "Sikkerhet",
        "desc": "Pakker ut SIARD og kjører valgfritt antivirus rekursivt på alle filer. AV-sti og innstillinger hentes fra config.json.",
        "params": [
            {"key": "keep_temp", "label": "Behold utpakket mappe", "type": "bool", "default": False},
        ],
    },
    {
        "cls": None,
        "label": "Betinget (IF-flagg)",
        "category": "Kontroll",
        "desc": "Kjorer en operasjon kun hvis et kontekstflagg er True/False.",
        "params": [],
        "special": "conditional",
    },
]


class ParamDialog(ctk.CTkToplevel):
    def __init__(self, parent, op_def, on_confirm, on_saved=None):
        super().__init__(parent)
        self.title("Konfigurer: " + op_def["label"])
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self._op_def    = op_def
        self._on_confirm = on_confirm
        self._on_saved   = on_saved   # kalles med (operation_id, params, settings_path)
        self._vars = {}
        self._build()
        n_params = len(op_def.get("params", []))
        row_h    = 52
        header_h = 120
        footer_h = 70
        has_wide = any(p.get("type") in ("hw_int",) or p.get("key") == "temp_dir"
                       for p in op_def.get("params", []))
        width    = 640 if has_wide else 520
        height   = min(header_h + n_params * row_h + footer_h, 720)
        height   = max(height, 300)
        self.geometry(f"{width}x{height}")
        self.minsize(width, 300)
        self.resizable(True, True)

    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)   # param-seksjonen vokser
        self.grid_rowconfigure(10, weight=0)  # knapper forblir nederst
        ctk.CTkLabel(self, text=self._op_def["label"],
                     font=ctk.CTkFont(family=FONTS["mono"], size=14, weight="bold"),
                     text_color=COLORS["accent"]).grid(row=0, column=0, padx=20, pady=(18,4), sticky="w")
        ctk.CTkLabel(self, text=self._op_def["desc"],
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["muted"], wraplength=380).grid(row=1, column=0, padx=20, pady=(0,14), sticky="w")
        params = self._op_def.get("params", [])
        if not params:
            ctk.CTkLabel(self, text="Ingen parametere.",
                         font=ctk.CTkFont(family=FONTS["mono"], size=11),
                         text_color=COLORS["text_sub"]).grid(row=2, column=0, padx=20, pady=20)
        else:
            frm = ctk.CTkScrollableFrame(
                self, fg_color=COLORS["panel"], corner_radius=8,
                scrollbar_button_color=COLORS["border"])
            frm.grid(row=2, column=0, padx=16, pady=(0, 14), sticky="nsew")
            frm.grid_columnconfigure(1, weight=1)
            for i, p in enumerate(params):
                ctk.CTkLabel(frm, text=p["label"],
                             font=ctk.CTkFont(family=FONTS["mono"], size=11),
                             text_color=COLORS["text"]).grid(row=i, column=0, padx=12, pady=8, sticky="w")
                if p["type"] == "bool":
                    var = ctk.BooleanVar(value=p["default"])
                    ctk.CTkSwitch(frm, text="", variable=var,
                                  onvalue=True, offvalue=False,
                                  button_color=COLORS["accent"]).grid(row=i, column=1, padx=12, sticky="e")
                    self._vars[p["key"]] = (var, "bool")
                    continue
                elif p["type"] == "choice":
                    var = ctk.StringVar(value=str(p["default"]))
                    ctk.CTkOptionMenu(
                        frm,
                        variable=var,
                        values=p.get("choices", [p["default"]]),
                        fg_color=COLORS["bg"],
                        button_color=COLORS["accent"],
                        button_hover_color=COLORS["accent_dim"],
                        dropdown_fg_color=COLORS["panel"],
                        font=ctk.CTkFont(family=FONTS["mono"], size=10),
                        width=280,
                    ).grid(row=i, column=1, padx=12, sticky="e")
                    self._vars[p["key"]] = (var, "choice")
                elif p["type"] == "int":
                    var = ctk.StringVar(value=str(p["default"]))
                    ctk.CTkEntry(frm, textvariable=var, width=100, fg_color=COLORS["bg"],
                                 font=ctk.CTkFont(family=FONTS["mono"], size=11)).grid(row=i, column=1, padx=12, sticky="e")
                elif p["type"] == "hw_int":
                    # Int-felt med Auto-knapp som foreslår basert på maskinvare
                    var = ctk.StringVar(value=str(p["default"]))
                    cell = ctk.CTkFrame(frm, fg_color="transparent")
                    cell.grid(row=i, column=1, padx=12, sticky="e")
                    ctk.CTkEntry(cell, textvariable=var, width=80,
                                 fg_color=COLORS["bg"],
                                 font=ctk.CTkFont(family=FONTS["mono"], size=11)
                                 ).pack(side="left", padx=(0, 4))

                    def _auto_hw(v=var):
                        try:
                            from siard_workflow.operations.blob_convert_operation \
                                import suggest_lo_defaults
                            hw = suggest_lo_defaults()
                            v.set(str(hw["max_workers"]))
                            if "lo_batch_size" in self._vars:
                                self._vars["lo_batch_size"][0].set(str(hw["lo_batch_size"]))
                            save_config({
                                "max_workers":   hw["max_workers"],
                                "lo_batch_size": hw["lo_batch_size"],
                            })
                            from tkinter import messagebox
                            messagebox.showinfo(
                                "Maskinvare-forslag",
                                f"Prosessor: {hw['_cpus']} kjerner\n"
                                f"RAM: {hw['_ram_gb']} GB\n\n"
                                f"Tråder satt til: {hw['max_workers']}\n"
                                f"Batch-størrelse satt til: {hw['lo_batch_size']}",
                                parent=self)
                        except Exception as exc:
                            from tkinter import messagebox
                            messagebox.showerror("Feil", str(exc), parent=self)

                    ctk.CTkButton(cell, text="Auto", width=52,
                                  fg_color=COLORS["accent"],
                                  hover_color=COLORS["accent_dim"],
                                  font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                  command=_auto_hw).pack(side="left")
                    self._vars[p["key"]] = (var, "int")
                elif p["key"] == "temp_dir":
                    # Spesialbehandling: tekstfelt + Bla-knapp + Auto-knapp
                    var = ctk.StringVar(value=str(p["default"]))
                    cell = ctk.CTkFrame(frm, fg_color="transparent")
                    cell.grid(row=i, column=1, padx=12, sticky="e")
                    entry = ctk.CTkEntry(cell, textvariable=var, width=160,
                                         fg_color=COLORS["bg"],
                                         font=ctk.CTkFont(family=FONTS["mono"], size=10))
                    entry.pack(side="left", padx=(0, 4))

                    def _browse_temp(v=var):
                        from tkinter import filedialog
                        d = filedialog.askdirectory(title="Velg temp-mappe")
                        if d:
                            v.set(d)

                    def _auto_temp(v=var):
                        try:
                            from disk_selector import get_disk_candidates, format_bytes
                            cands = get_disk_candidates()
                            if cands:
                                best = cands[0]
                                v.set(str(best["path"]))
                                # Vis alle kandidater i en popup-label
                                info = "\n".join(
                                    f"{'✓' if j==0 else ' '} {c['label']}"
                                    for j, c in enumerate(cands))
                                from tkinter import messagebox
                                messagebox.showinfo(
                                    "Tilgjengelige disker", info, parent=self)
                            else:
                                v.set("")
                                from tkinter import messagebox
                                messagebox.showwarning(
                                    "Ingen disk",
                                    "Ingen disk med nok ledig plass funnet.\n"
                                    "Legg inn mappe manuelt.", parent=self)
                        except Exception as exc:
                            from tkinter import messagebox
                            messagebox.showerror("Feil", str(exc), parent=self)

                    ctk.CTkButton(cell, text="Bla…", width=46,
                                  fg_color=COLORS["btn"],
                                  hover_color=COLORS["btn_hover"],
                                  font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                  command=_browse_temp).pack(side="left", padx=(0, 4))
                    ctk.CTkButton(cell, text="Auto", width=46,
                                  fg_color=COLORS["accent"],
                                  hover_color=COLORS["accent_dim"],
                                  font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                  command=_auto_temp).pack(side="left")
                else:
                    var = ctk.StringVar(value=str(p["default"]))
                    ctk.CTkEntry(frm, textvariable=var, width=200, fg_color=COLORS["bg"],
                                 font=ctk.CTkFont(family=FONTS["mono"], size=11)).grid(row=i, column=1, padx=12, sticky="e")
                self._vars[p["key"]] = (var, p["type"])
        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=10, column=0, padx=16, pady=(0,16), sticky="e")
        ctk.CTkButton(btns, text="Avbryt", width=90,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self.destroy).pack(side="left", padx=(0,8))
        ctk.CTkButton(btns, text="Legg til", width=110,
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                      command=self._confirm).pack(side="left")

    def _confirm(self):
        kwargs = {}
        for key, (var, typ) in self._vars.items():
            val = var.get()
            if typ == "int":
                try: val = int(val)
                except ValueError: val = 0
            elif typ == "bool":
                val = bool(val)
            kwargs[key] = val

        op_cls = self._op_def.get("cls")
        op     = op_cls(**kwargs) if op_cls else None

        # Lagre innstillinger permanent til settings.json
        if op and op.operation_id:
            try:
                save_op_params(op.operation_id, kwargs)
                # Lagre maskinvare- og format-innstillinger til config.json
                _config_keys = {"max_workers", "lo_batch_size", "pdfa_version"}
                _config_updates = {k: kwargs[k] for k in _config_keys if k in kwargs}
                if _config_updates:
                    save_config(_config_updates)
                if self._on_saved:
                    self._on_saved(op.operation_id, kwargs, _SETTINGS_FILE)
            except Exception as e:
                if self._on_saved:
                    self._on_saved(op.operation_id, kwargs, None,
                                   error=str(e))

        self._on_confirm(op)
        self.destroy()


class _ConditionalDialog(ctk.CTkToplevel):
    _FLAG_OPTS = ["has_blobs", "virus_found"]

    def __init__(self, parent, on_add):
        super().__init__(parent)
        self.title("Betinget operasjon")
        self.geometry("420x320")
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self._on_add = on_add
        self._build()

    def _build(self):
        ctk.CTkLabel(self, text="Konfigurer IF-operasjon",
                     font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
                     text_color=COLORS["accent"]).pack(padx=20, pady=(18,4), anchor="w")
        frm = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=8)
        frm.pack(padx=16, pady=8, fill="x")
        frm.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(frm, text="Flagg:", font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"]).grid(row=0, column=0, padx=12, pady=8, sticky="w")
        self._flag_var = ctk.StringVar(value=self._FLAG_OPTS[0])
        ctk.CTkOptionMenu(frm, values=self._FLAG_OPTS, variable=self._flag_var,
                          fg_color=COLORS["bg"], button_color=COLORS["accent"],
                          font=ctk.CTkFont(family=FONTS["mono"], size=11),
                          width=180).grid(row=0, column=1, padx=12, sticky="e")
        ctk.CTkLabel(frm, text="Kjor nar:", font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"]).grid(row=1, column=0, padx=12, pady=8, sticky="w")
        self._when_var = ctk.StringVar(value="True")
        ctk.CTkOptionMenu(frm, values=["True", "False"], variable=self._when_var,
                          fg_color=COLORS["bg"], button_color=COLORS["accent"],
                          font=ctk.CTkFont(family=FONTS["mono"], size=11),
                          width=180).grid(row=1, column=1, padx=12, sticky="e")
        ctk.CTkLabel(frm, text="Operasjon:", font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"]).grid(row=2, column=0, padx=12, pady=8, sticky="w")
        inner_ops = [d["label"] for d in OP_DEFS if d.get("cls") and not d.get("special")]
        self._inner_var = ctk.StringVar(value=inner_ops[0])
        ctk.CTkOptionMenu(frm, values=inner_ops, variable=self._inner_var,
                          fg_color=COLORS["bg"], button_color=COLORS["accent"],
                          font=ctk.CTkFont(family=FONTS["mono"], size=11),
                          width=180).grid(row=2, column=1, padx=12, sticky="e")
        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.pack(padx=16, pady=12, anchor="e")
        ctk.CTkButton(btns, text="Avbryt", width=90,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self.destroy).pack(side="left", padx=(0,8))
        ctk.CTkButton(btns, text="Legg til", width=110,
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                      command=self._confirm).pack(side="left")

    def _confirm(self):
        flag     = self._flag_var.get()
        run_when = self._when_var.get() == "True"
        inner_d  = next(d for d in OP_DEFS if d["label"] == self._inner_var.get())
        op = ConditionalOperation(inner_d["cls"](), flag=flag, run_when=run_when)
        self._on_add(op)
        self.destroy()


class OperationCard(ctk.CTkFrame):
    def __init__(self, parent, op_def, on_add, on_saved=None):
        color = cat_color(op_def["category"])
        super().__init__(parent,
                         fg_color=COLORS["panel"],
                         corner_radius=8,
                         border_color=_dim(color, 0.5),
                         border_width=1)
        self.grid_columnconfigure(0, weight=1)

        top = ctk.CTkFrame(self, fg_color="transparent")
        top.grid(row=0, column=0, padx=6, pady=(6, 6), sticky="ew")
        top.grid_columnconfigure(1, weight=1)

        ctk.CTkFrame(top, width=4, height=4, corner_radius=2,
                     fg_color=color).grid(row=0, column=0, padx=(0, 5), sticky="ns")

        lbl = ctk.CTkLabel(top, text=op_def["label"],
                           font=ctk.CTkFont(family=FONTS["mono"], size=11,
                                            weight="bold"),
                           text_color=COLORS["text"], anchor="w",
                           wraplength=120)
        lbl.grid(row=0, column=1, sticky="ew")

        btn = ctk.CTkButton(top, text="+", width=24, height=24, corner_radius=5,
                            fg_color=_dim(color, 0.35),
                            hover_color=_dim(color, 0.65),
                            text_color=color,
                            font=ctk.CTkFont(size=14, weight="bold"),
                            command=lambda: self._clicked(op_def, on_add, on_saved))
        btn.grid(row=0, column=2, padx=(4, 0))

        # Beskrivelsen vises som balloon-tooltip ved hover
        for widget in (self, top, lbl, btn):
            _ToolTip(widget, op_def["desc"])

    def _clicked(self, op_def, on_add, on_saved=None):
        if op_def.get("special") == "conditional":
            _ConditionalDialog(self, on_add)
        elif op_def.get("params"):
            ParamDialog(self, op_def, on_confirm=on_add, on_saved=on_saved)
        else:
            op = op_def["cls"]()
            on_add(op)


class OperationsPanel(ctk.CTkFrame):
    def __init__(self, parent, on_add, on_saved=None):
        super().__init__(parent, fg_color=COLORS["surface"], corner_radius=10)
        self._on_add   = on_add
        self._on_saved = on_saved
        self.grid_columnconfigure(0, weight=1)
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.grid(row=0, column=0, sticky="ew", padx=12, pady=(10,6))
        ctk.CTkLabel(hdr, text="TILGJENGELIGE OPERASJONER",
                     font=ctk.CTkFont(family=FONTS["mono"], size=10, weight="bold"),
                     text_color=COLORS["muted"]).pack(side="left")

        categories = list(dict.fromkeys(d["category"] for d in OP_DEFS))
        self._tabs = ctk.CTkTabview(
            self, height=90,
            fg_color=COLORS["panel"],
            segmented_button_fg_color=COLORS["bg"],
            segmented_button_selected_color=COLORS["accent"],
            segmented_button_selected_hover_color=COLORS["accent_dim"],
            text_color=COLORS["text"],
            text_color_disabled=COLORS["muted"],
        )
        self._tabs.grid(row=1, column=0, sticky="ew", padx=10, pady=(0,10))

        for cat in categories:
            tab = self._tabs.add(cat)
            tab.grid_columnconfigure((0, 1, 2), weight=1)
            ops = [d for d in OP_DEFS if d["category"] == cat]
            for i, op_def in enumerate(ops):
                OperationCard(tab, op_def,
                              on_add=self._on_add,
                              on_saved=self._on_saved).grid(
                    row=i // 3, column=i % 3, padx=4, pady=4, sticky="ew")
