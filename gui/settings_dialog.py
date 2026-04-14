"""
gui/settings_dialog.py — Global innstillinger-dialog for SIARD Workflow Manager

Viser og redigerer config.json i program-mappen.
"""
from __future__ import annotations
import sys
from pathlib import Path
from tkinter import filedialog
import customtkinter as ctk

sys.path.insert(0, str(Path(__file__).parent.parent))
from gui.styles import COLORS, FONTS


class SettingsDialog(ctk.CTkToplevel):
    """
    Modal dialog for globale innstillinger:
      - Temp-mappe
      - Antivirus-program (sti til exe)
      - Max parallelle LO-instanser
      - Batch-størrelse
      - LO-timeout
    """

    def __init__(self, parent, on_save=None):
        super().__init__(parent)
        self.title("Globale innstillinger")
        self.configure(fg_color=COLORS["surface"])
        self.grab_set()
        self.resizable(True, False)
        self._on_save = on_save
        self._vars: dict[str, ctk.Variable] = {}
        self._build()
        self.geometry("780x520")
        self.minsize(780, 420)

    def _build(self):
        from settings import load_config, _CONFIG_FILE
        cfg = load_config()

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # Tittel
        hdr = ctk.CTkFrame(self, fg_color=COLORS["bg"], corner_radius=0)
        hdr.grid(row=0, column=0, sticky="ew")
        hdr.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(hdr, text="Globale innstillinger",
                     font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
                     text_color=COLORS["accent"]).grid(
                         row=0, column=0, padx=20, pady=12, sticky="w")
        ctk.CTkLabel(hdr, text=f"Lagres i: {_CONFIG_FILE}",
                     font=ctk.CTkFont(family=FONTS["mono"], size=9),
                     text_color=COLORS["muted"]).grid(
                         row=1, column=0, padx=20, pady=(0, 10), sticky="w")

        # Innhold
        frm = ctk.CTkScrollableFrame(self, fg_color=COLORS["panel"],
                                      corner_radius=8,
                                      scrollbar_button_color=COLORS["border"])
        frm.grid(row=1, column=0, padx=16, pady=(8, 8), sticky="nsew")
        frm.grid_columnconfigure(1, weight=1)

        def _seksjon(tekst: str, row: int):
            ctk.CTkLabel(frm, text=tekst,
                         font=ctk.CTkFont(family=FONTS["mono"], size=10,
                                          weight="bold"),
                         text_color=COLORS["accent"]).grid(
                             row=row, column=0, columnspan=2,
                             padx=12, pady=(14, 4), sticky="w")

        def _rad(label: str, key: str, typ: str, row: int,
                 default="", browse_dir=False, browse_file=False):
            ctk.CTkLabel(frm, text=label,
                         font=ctk.CTkFont(family=FONTS["mono"], size=11),
                         text_color=COLORS["text"],
                         anchor="w").grid(row=row, column=0,
                                          padx=(12, 8), pady=6, sticky="w")
            if typ == "bool":
                var = ctk.BooleanVar(value=bool(cfg.get(key, default)))
                ctk.CTkSwitch(frm, text="", variable=var,
                              onvalue=True, offvalue=False,
                              button_color=COLORS["accent"]).grid(
                                  row=row, column=1, padx=12, pady=6, sticky="e")
            else:
                var = ctk.StringVar(value=str(cfg.get(key, default)))
                if browse_dir or browse_file:
                    cell = ctk.CTkFrame(frm, fg_color="transparent")
                    cell.grid(row=row, column=1, padx=12, pady=6, sticky="ew")
                    cell.grid_columnconfigure(0, weight=1)
                    ctk.CTkEntry(cell, textvariable=var, width=340,
                                 fg_color=COLORS["bg"],
                                 font=ctk.CTkFont(family=FONTS["mono"], size=10)
                                 ).grid(row=0, column=0, sticky="ew")

                    def _browse(v=var, d=browse_dir):
                        if d:
                            p = filedialog.askdirectory(title="Velg mappe",
                                                        initialdir=v.get() or ".")
                        else:
                            p = filedialog.askopenfilename(
                                title="Velg program",
                                filetypes=[("Exe", "*.exe"), ("Alle", "*.*")])
                        if p:
                            v.set(p)

                    ctk.CTkButton(cell, text="Bla…", width=52,
                                  fg_color=COLORS["btn"],
                                  hover_color=COLORS["btn_hover"],
                                  font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                  command=_browse).grid(
                                      row=0, column=1, padx=(4, 0))
                else:
                    ctk.CTkEntry(frm, textvariable=var, width=160,
                                 fg_color=COLORS["bg"],
                                 font=ctk.CTkFont(
                                     family=FONTS["mono"], size=11)).grid(
                                         row=row, column=1, padx=12,
                                         pady=6, sticky="e")
            self._vars[key] = var

        r = 0
        _seksjon("Temp-mappe", r);         r += 1
        _rad("Temp-mappe", "global_temp_dir", "str", r,
             default="", browse_dir=True);  r += 1
        ctk.CTkLabel(frm,
                     text="Tom = auto-velg raskeste disk ved filvalg",
                     font=ctk.CTkFont(family=FONTS["mono"], size=9),
                     text_color=COLORS["muted"]).grid(
                         row=r, column=0, columnspan=2,
                         padx=14, pady=(0, 4), sticky="w"); r += 1

        _seksjon("Antivirus", r);          r += 1
        _rad("Sti til AV-program", "av_executable", "str", r,
             default="", browse_file=True); r += 1
        ctk.CTkLabel(frm,
                     text="Eks: C:\\Program Files\\ClamAV\\clamscan.exe  (tom = autodetekter)",
                     font=ctk.CTkFont(family=FONTS["mono"], size=9),
                     text_color=COLORS["muted"]).grid(
                         row=r, column=0, columnspan=2,
                         padx=14, pady=(0, 4), sticky="w"); r += 1
        _rad("Returkode ved funn",    "av_infected_rc", "int", r, default=1);  r += 1
        _rad("Tidsavbrudd skan (s)",  "av_timeout",     "int", r, default=300);r += 1
        # av_args som kommaseparert tekst
        ctk.CTkLabel(frm, text="AV-argumenter",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"],
                     anchor="w").grid(row=r, column=0,
                                      padx=(12, 8), pady=6, sticky="w")
        raw_args = cfg.get("av_args", [])
        if isinstance(raw_args, list):
            init_args = " ".join(raw_args)
        else:
            init_args = str(raw_args)
        args_var = ctk.StringVar(value=init_args)
        self._vars["av_args"] = args_var
        ctk.CTkEntry(frm, textvariable=args_var, width=420,
                     fg_color=COLORS["bg"],
                     font=ctk.CTkFont(family=FONTS["mono"], size=10)).grid(
                         row=r, column=1, padx=12, pady=6, sticky="ew"); r += 1
        ctk.CTkLabel(frm,
                     text="Tom = auto. Eks: --recursive --infected {scan_path}",
                     font=ctk.CTkFont(family=FONTS["mono"], size=9),
                     text_color=COLORS["muted"]).grid(
                         row=r, column=0, columnspan=2,
                         padx=14, pady=(0, 4), sticky="w"); r += 1

        _seksjon("Blob-konvertering (LibreOffice)", r); r += 1
        _rad("Parallelle LO-instanser", "max_workers",   "int", r, default=4);  r += 1
        _rad("Filer per batch",         "lo_batch_size", "int", r, default=50); r += 1
        _rad("Tidsavbrudd per batch (s)","lo_timeout",   "int", r, default=300);r += 1

        # lo_convertible som kommaseparert tekstfelt
        ctk.CTkLabel(frm, text="Formater til PDF/A",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"],
                     anchor="w").grid(row=r, column=0,
                                      padx=(12, 8), pady=6, sticky="w")
        raw_list = cfg.get("lo_convertible", [])
        init_val = ", ".join(raw_list) if isinstance(raw_list, list) else str(raw_list)
        conv_var = ctk.StringVar(value=init_val)
        self._vars["lo_convertible"] = conv_var
        ctk.CTkEntry(frm, textvariable=conv_var, width=420,
                     fg_color=COLORS["bg"],
                     font=ctk.CTkFont(family=FONTS["mono"], size=10)).grid(
                         row=r, column=1, padx=12, pady=6, sticky="ew"); r += 1
        ctk.CTkLabel(frm,
                     text="Kommaseparert, f.eks: doc, docx, rtf, odt, txt",
                     font=ctk.CTkFont(family=FONTS["mono"], size=9),
                     text_color=COLORS["muted"]).grid(
                         row=r, column=0, columnspan=2,
                         padx=14, pady=(0, 4), sticky="w"); r += 1

        # rename_only som kommaseparert tekstfelt
        ctk.CTkLabel(frm, text="Formater beholdt uendret",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"],
                     anchor="w").grid(row=r, column=0,
                                      padx=(12, 8), pady=6, sticky="w")
        raw_rename = cfg.get("rename_only", [])
        init_rename = ", ".join(raw_rename) if isinstance(raw_rename, list) \
                      else str(raw_rename)
        rename_var = ctk.StringVar(value=init_rename)
        self._vars["rename_only"] = rename_var
        ctk.CTkEntry(frm, textvariable=rename_var, width=420,
                     fg_color=COLORS["bg"],
                     font=ctk.CTkFont(family=FONTS["mono"], size=10)).grid(
                         row=r, column=1, padx=12, pady=6, sticky="ew"); r += 1
        ctk.CTkLabel(frm,
                     text="Kommaseparert, f.eks: jpg, png, xlsx, pptx, mp3",
                     font=ctk.CTkFont(family=FONTS["mono"], size=9),
                     text_color=COLORS["muted"]).grid(
                         row=r, column=0, columnspan=2,
                         padx=14, pady=(0, 4), sticky="w"); r += 1

        # lo_upgrade som nøkkel=verdi-streng
        ctk.CTkLabel(frm, text="Formatoppgradering (gammel=ny)",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"],
                     anchor="w").grid(row=r, column=0,
                                      padx=(12, 8), pady=6, sticky="w")
        raw_upg = cfg.get("lo_upgrade", {})
        init_upg = ", ".join(f"{k}={v}" for k, v in raw_upg.items()) \
                   if isinstance(raw_upg, dict) else str(raw_upg)
        upg_var = ctk.StringVar(value=init_upg)
        self._vars["lo_upgrade"] = upg_var
        ctk.CTkEntry(frm, textvariable=upg_var, width=420,
                     fg_color=COLORS["bg"],
                     font=ctk.CTkFont(family=FONTS["mono"], size=10)).grid(
                         row=r, column=1, padx=12, pady=6, sticky="ew"); r += 1
        ctk.CTkLabel(frm,
                     text="Kommaseparert, f.eks: xls=xlsx, ppt=pptx, doc=docx",
                     font=ctk.CTkFont(family=FONTS["mono"], size=9),
                     text_color=COLORS["muted"]).grid(
                         row=r, column=0, columnspan=2,
                         padx=14, pady=(0, 4), sticky="w"); r += 1

        # Knapper
        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=2, column=0, padx=16, pady=(0, 14), sticky="e")
        ctk.CTkButton(btns, text="Avbryt", width=90,
                      fg_color=COLORS["btn"],
                      hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self.destroy).pack(side="left", padx=(0, 8))
        ctk.CTkButton(btns, text="Lagre", width=110,
                      fg_color=COLORS["accent"],
                      hover_color=COLORS["accent_dim"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11,
                                       weight="bold"),
                      command=self._save).pack(side="left")

    def _save(self):
        cfg: dict = {}
        int_keys  = {"max_workers", "lo_batch_size", "lo_timeout",
                     "av_infected_rc", "av_timeout"}
        list_keys = {"lo_convertible", "rename_only"}
        args_keys = {"av_args"}
        dict_keys = {"lo_upgrade"}  # kommaseparert nøkkel=verdi → dict
        for key, var in self._vars.items():
            val = var.get()
            if key in int_keys:
                try:    val = int(val)
                except ValueError: val = 0
            elif key in list_keys:
                val = [e.strip().lower() for e in val.split(",") if e.strip()]
            elif key in args_keys:
                val = [e.strip() for e in val.split() if e.strip()]
            elif key in dict_keys:
                # "xls=xlsx, ppt=pptx" → {"xls": "xlsx", "ppt": "pptx"}
                result = {}
                for pair in val.split(","):
                    pair = pair.strip()
                    if "=" in pair:
                        k, _, v = pair.partition("=")
                        k, v = k.strip().lower(), v.strip().lower()
                        if k and v:
                            result[k] = v
                val = result
            cfg[key] = val
        if self._on_save:
            self._on_save(cfg)
        self.destroy()
