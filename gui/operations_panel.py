from __future__ import annotations
from typing import Callable
import csv
import io
import json
import sys
import threading
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
    UnpackSiardOperation, RepackSiardOperation,
    WorkflowReportOperation, DiasPackageOperation,
)
from siard_workflow.systemspecific_operations import CosDocMailMergeOperation
from settings import save_op_params, save_config, get_config, _SETTINGS_FILE


# ─────────────────────────────────────────────────────────────────────────────
# Autocomplete-datakilder
# ─────────────────────────────────────────────────────────────────────────────

_SOURCES_DIR        = Path(__file__).parent.parent / "siard_workflow" / "sources"
_KOMMUNENUMMER_CSV  = _SOURCES_DIR / "kommunenummer.csv"
_KILDESYSTEM_CACHE  = _SOURCES_DIR / "kildesystem_cache.json"
_SHEETS_URL         = ("https://docs.google.com/spreadsheets/d/"
                       "1JJr_MBt97aZasAInCnvhGmyTcv7OizrYApQCkQOjyLw/export?format=csv")

_kommunenummer_list: list[str] | None = None
_kildesystem_list:   list[str] | None = None


def _get_kommunenummer() -> list[str]:
    global _kommunenummer_list
    if _kommunenummer_list is not None:
        return _kommunenummer_list
    result = []
    try:
        with open(_KOMMUNENUMMER_CSV, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                kode = row.get("Kodeverdi", "").strip()
                navn = row.get("Navn", "").strip()
                if kode and navn:
                    label = f"{kode} - {navn} kommune"
                    if label not in result:
                        result.append(label)
        result.sort()
    except Exception:
        pass
    _kommunenummer_list = result
    return result


def _get_kildesystem() -> list[str]:
    global _kildesystem_list
    if _kildesystem_list is not None:
        return _kildesystem_list

    # Forsøk cache først
    cached = _load_kildesystem_cache()
    if cached is not None:
        _kildesystem_list = cached
        _refresh_kildesystem_async()   # oppdater i bakgrunnen
        return cached

    # Ingen cache — hent synkront (første gang)
    fetched = _fetch_kildesystem()
    _kildesystem_list = fetched if fetched is not None else []
    return _kildesystem_list


def _load_kildesystem_cache() -> list[str] | None:
    try:
        if _KILDESYSTEM_CACHE.exists():
            data = json.loads(_KILDESYSTEM_CACHE.read_text(encoding="utf-8"))
            if isinstance(data, list) and data:
                return data
    except Exception:
        pass
    return None


def _fetch_kildesystem() -> list[str] | None:
    try:
        import urllib.request
        with urllib.request.urlopen(_SHEETS_URL, timeout=8) as resp:
            raw = resp.read().decode("utf-8")
        result = []
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            navn = (row.get("Navn") or "").strip()
            lev  = (row.get("Leverandør") or "").strip()
            if navn:
                result.append(f"{navn} ({lev})" if lev else navn)
        result.sort()
        try:
            _KILDESYSTEM_CACHE.write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return result
    except Exception:
        return None


def _refresh_kildesystem_async():
    def _worker():
        global _kildesystem_list
        fetched = _fetch_kildesystem()
        if fetched:
            _kildesystem_list = fetched
    threading.Thread(target=_worker, daemon=True).start()


def _get_autocomplete_list(source: str) -> list[str]:
    if source == "kommunenummer":
        return _get_kommunenummer()
    if source == "kildesystem":
        return _get_kildesystem()
    return []


# ── SIARD-metadata-forslag ────────────────────────────────────────────────────

_current_siard_path: "Path | None" = None


def set_current_siard_path(path: "Path | None") -> None:
    global _current_siard_path
    _current_siard_path = path


def _get_siard_suggestions(source: str) -> list[str]:
    """Henter forslag fra metadata.xml i aktiv SIARD-fil."""
    if _current_siard_path is None:
        return []
    try:
        import zipfile, xml.etree.ElementTree as ET
        with zipfile.ZipFile(_current_siard_path, "r") as zf:
            candidates = [n for n in zf.namelist()
                          if n.lower().endswith("metadata.xml")]
            if not candidates:
                return []
            xml_bytes = zf.read(candidates[0])
        root = ET.fromstring(xml_bytes)
        ns_strip = __import__("re").compile(r"\{[^}]*\}")

        def _text(tag: str) -> str:
            for el in root.iter():
                if ns_strip.sub("", el.tag).lower() == tag.lower():
                    return (el.text or "").strip()
            return ""

        if source == "kommunenummer":
            origin = _text("databaseOrigin") or _text("dbOrigin") or _text("archiveName")
            if not origin:
                return []
            full = _get_kommunenummer()
            low = origin.lower()
            # Treff på navn-del (etter " - ")
            return [x for x in full if low in x.split(" - ", 1)[-1].lower()][:10]

        if source == "kildesystem":
            app = _text("producerApplication") or _text("databaseProduct")
            if not app:
                return []
            full = _get_kildesystem()
            low = app.lower()
            return [x for x in full if low in x.lower()][:10]

    except Exception:
        pass
    return []


# ── Autocomplete-widget ───────────────────────────────────────────────────────

class _AutocompleteEntry(ctk.CTkFrame):
    """CTkEntry med nedtrekksliste (Listbox-popup) for autocomplete."""

    MIN_CHARS = 3
    MAX_ITEMS = 60

    def __init__(self, master, full_list: list[str], variable: ctk.StringVar,
                 siard_source: str = "", width: int = 280, **kwargs):
        super().__init__(master, fg_color="transparent", width=width, height=32)
        self._full   = full_list
        self._var    = variable
        self._source = siard_source
        self._popup  = None
        self._lb     = None
        self._skip_focusout = False

        self._entry = ctk.CTkEntry(
            self, textvariable=variable, width=width,
            fg_color=COLORS["bg"],
            font=ctk.CTkFont(family=FONTS["mono"], size=11),
        )
        self._entry.pack(fill="x", expand=True)
        self._entry.bind("<KeyRelease>", self._on_key)
        self._entry.bind("<FocusOut>",   self._on_focusout)
        self._entry.bind("<Down>",       self._focus_list)
        self._entry.bind("<Escape>",     lambda e: self._close())
        self._entry.bind("<Return>",     lambda e: self._close())

    # ── Filtrering ───────────────────────────────────────────────────────────

    def _filtered(self) -> list[str]:
        text = self._var.get().lower()
        if not text:
            return []
        matches = [x for x in self._full if text in x.lower()]
        # SIARD-forslag øverst (uthevet med "★ ")
        if self._source:
            siard = _get_siard_suggestions(self._source)
            top = [x for x in siard if text in x.lower()]
            for x in top:
                if x in matches:
                    matches.remove(x)
            prefixed = [f"★  {x}" for x in top]
            matches  = prefixed + matches
        return matches[:self.MAX_ITEMS]

    # ── Tastatur-hendelser ───────────────────────────────────────────────────

    def _on_key(self, event):
        if len(self._var.get()) >= self.MIN_CHARS:
            items = self._filtered()
            if items:
                self._show(items)
                return
        self._close()

    def _focus_list(self, event):
        if self._lb and self._lb.winfo_exists() and self._lb.size() > 0:
            self._lb.focus_set()
            self._lb.selection_set(0)
            self._lb.activate(0)

    # ── Popup ────────────────────────────────────────────────────────────────

    def _show(self, items: list[str]):
        inner = self._entry._entry          # underliggende tk.Entry
        x  = inner.winfo_rootx()
        y  = inner.winfo_rooty() + inner.winfo_height() + 2
        w  = inner.winfo_width()
        n  = min(len(items), 8)
        lh = 19

        if self._popup is None or not self._popup.winfo_exists():
            self._popup = tk.Toplevel(self._entry)
            self._popup.wm_overrideredirect(True)
            self._popup.configure(bg=COLORS["border"])

            sb = tk.Scrollbar(self._popup, width=12,
                              troughcolor=COLORS["panel"],
                              bg=COLORS["border"])
            sb.pack(side="right", fill="y")

            self._lb = tk.Listbox(
                self._popup,
                yscrollcommand=sb.set,
                bg=COLORS["panel"],
                fg=COLORS["text"],
                selectbackground=COLORS["accent"],
                selectforeground="#ffffff",
                font=("Courier New", 10),
                bd=0, highlightthickness=0,
                activestyle="none",
                relief="flat",
            )
            self._lb.pack(side="left", fill="both", expand=True)
            sb.config(command=self._lb.yview)
            self._lb.bind("<<ListboxSelect>>", self._on_select)
            self._lb.bind("<FocusOut>",        self._on_focusout)
            self._lb.bind("<Escape>",          lambda e: self._close())
            self._lb.bind("<Return>",          self._on_select)

        self._popup.geometry(f"{w}x{n * lh + 4}+{x}+{y}")
        self._popup.lift()
        self._lb.delete(0, "end")
        for item in items:
            self._lb.insert("end", item)

    def _close(self):
        if self._popup and self._popup.winfo_exists():
            self._popup.destroy()
        self._popup = None
        self._lb    = None

    # ── Valg ─────────────────────────────────────────────────────────────────

    def _on_select(self, event):
        if not self._lb:
            return
        sel = self._lb.curselection()
        if sel:
            val = self._lb.get(sel[0])
            # Fjern "★  "-prefiks fra SIARD-forslag
            if val.startswith("★  "):
                val = val[3:]
            self._var.set(val)
            self._skip_focusout = True
            self._close()
            self._entry.focus_set()

    # ── Fokus ─────────────────────────────────────────────────────────────────

    def _on_focusout(self, event):
        if self._skip_focusout:
            self._skip_focusout = False
            return
        self.after(120, self._check_focus)

    def _check_focus(self):
        try:
            focused = self.focus_get()
            if self._lb and focused is self._lb:
                return
            if hasattr(self._entry, "_entry") and focused is self._entry._entry:
                return
            self._close()
        except Exception:
            self._close()


# ─────────────────────────────────────────────────────────────────────────────

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
        # ── Pipeline-operasjoner ─────────────────────────────────────────────────
    {
        "cls": UnpackSiardOperation,
        "label": "Pakk ut SIARD",
        "category": "Pipeline",
        "desc": (
            "Pakker ut SIARD-arkivet til en midlertidig mappe én gang. "
            "Etterfølgende operasjoner (Virusskan, HEX Extract, BLOB Konverter) "
            "jobber direkte på filsystemet uten å åpne ZIP-filen på nytt. "
            "Bruk alltid 'Pakk sammen SIARD' som siste operasjon."
        ),
        "status": UnpackSiardOperation.status,
        "params": [],
    },
    {
        "cls": RepackSiardOperation,
        "label": "Pakk sammen SIARD",
        "category": "Pipeline",
        "desc": (
            "Pakker den utpakkede SIARD-strukturen til en ny .siard-fil og "
            "rydder temp-mappen. Bruk alltid som siste operasjon etter "
            "'Pakk ut SIARD' i pipeline-arbeidsflyten."
        ),
        "status": RepackSiardOperation.status,
        "params": [
            {"key": "output_suffix", "label": "Suffix ny SIARD-fil",
             "type": "str", "default": "_konvertert"},
            {"key": "keep_temp", "label": "Behold temp-mappe",
             "type": "bool", "default": False},
        ],
    },
    # ── Standard operasjoner ─────────────────────────────────────────────────
    {
        "cls": SHA256Operation,
        "label": "SHA-256 Sjekksum",
        "category": "Integritet",
        "desc": "Beregner SHA-256 sjekksum for hele SIARD-filen.",
        "status": SHA256Operation.status,
        "params": [
            {"key": "save_to_file", "label": "Lagre .sha256-fil", "type": "bool", "default": False},
            {"key": "chunk_size",   "label": "Chunk-storrelse (bytes)", "type": "int", "default": 8192},
        ],
    },
    {
        "cls": VirusScanOperation,
        "label": "Virusskan",
        "category": "Sikkerhet",
        "desc": (
            "Kjører valgfritt antivirus mot SIARD-filen eller utpakket innhold. "
            "Bruk {FILE} i argumentfeltet som plassholder for skannemålet. "
            "Tom exe-felt = auto-detect (Windows Defender / clamscan). "
            "Eksempel args Windows Defender: scan /ScanType:3 /File:{FILE}  "
            "Eksempel clamscan: --recursive --infected {FILE}"
        ),
        "status": VirusScanOperation.status,
        "params": [
            {"key": "scan_target",   "label": "Skannemål",
             "type": "choice", "choices": ["file", "folder"],
             "default": "file",
             "hint": "file = SIARD-fila direkte  |  folder = pakk ut til temp-mappe først"},
            {"key": "av_executable", "label": "AV-program (sti)",
             "type": "str",  "default": "",
             "hint": "Tom = hent fra Innstillinger / auto-detect"},
            {"key": "av_args",       "label": "AV-argumenter",
             "type": "str",  "default": "",
             "hint": "Bruk {FILE} som plassholder. Eks: --recursive --infected {FILE}"},
            {"key": "av_infected_rc","label": "Infisert returkode",
             "type": "str",  "default": "",
             "hint": "Tom = hent fra Innstillinger (standard: 1)"},
            {"key": "keep_temp",     "label": "Behold utpakket mappe",
             "type": "bool", "default": False},
        ],
    },
    {
        "cls": BlobCheckOperation,
        "label": "BLOB/CLOB Kontroll",
        "category": "Innhold",
        "desc": "Sjekker om uttrekket inneholder binærfiler i Content/SchemaX/tableX.",
        "status": BlobCheckOperation.status,
        "params": [
            {"key": "content_prefix", "label": "Content-prefiks", "type": "str", "default": "content/"},
        ],
    },
    {
        "cls": BlobConvertOperation,
        "label": "BLOB Konverter til PDF/A",
        "category": "Innhold",
        "desc": "Identifiserer blob-filer (.bin/.txt/andre), konverterer dokumenter til PDF/A, ekstraher inline NBLOB/NCLOB. Filer som er ren tekst, XML eller ukjent format beholdes. Oppdaterer SIARD-arkivet.",
        "status": BlobConvertOperation.status,
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
        "status": HexExtractOperation.status,
        "params": [
            {"key": "min_text_length", "label": "Min. tekstlengde (tegn)",    "type": "int",  "default": 30},
            {"key": "dry_run",         "label": "Tørkjøring (ikke skriv)",     "type": "bool", "default": False},
        ],
    },
    # ── Systemspesifikke operasjoner ─────────────────────────────────────────
    {
        "cls": CosDocMailMergeOperation,
        "label": "CosDoc: Lås opp og flett dokumenter",
        "category": "Systemspesifikt",
        "desc": (
            "CosDoc-spesifikk: Låser opp passordbeskyttede dokumenter i "
            "Eef_ElFiler-tabellen og utfører mailmerge for dokumentpar "
            "(SeqNr 1+2 med samme EveID). "
            "Passord utledes av filnavnet (R + omvendt filstamme). "
            "Krever: msoffcrypto-tool (pip) og docx-mailmerge2 (pip)."
        ),
        "status": CosDocMailMergeOperation.status,
        "params": [
            {"key": "output_suffix", "label": "Suffix ny SIARD-fil",      "type": "str",  "default": "_cosdoc"},
            {"key": "table_name",    "label": "Tabellnavn (Eef_ElFiler)",  "type": "str",  "default": "Eef_ElFiler"},
            {"key": "lo_executable",  "label": "LibreOffice (soffice-sti)", "type": "str",  "default": ""},
            {"key": "lo_timeout",     "label": "LO timeout per fil (s)",    "type": "int",  "default": 120},
            {"key": "dry_run",        "label": "Tørkjøring (ikke skriv)",   "type": "bool", "default": False},
        ],
    },
    {
        "cls": XMLValidationOperation,
        "label": "XML-validering",
        "category": "Integritet",
        "desc": "Validerer metadata.xml og tabellskjemaer.",
        "status": XMLValidationOperation.status,
        "params": [
            {"key": "check_table_xsd", "label": "Sjekk tableX.xsd", "type": "bool", "default": True},
        ],
    },
    {
        "cls": MetadataExtractOperation,
        "label": "Metadata-uttrekk",
        "category": "Rapport",
        "desc": "Henter komplett metadata og genererer PDF-rapport med tabelloversikt, ER-diagram og kolonnedetaljer.",
        "status": MetadataExtractOperation.status,
        "params": [
            {"key": "generate_pdf",        "label": "Generer PDF-rapport",  "type": "bool", "default": True},
            {"key": "generate_er_diagram", "label": "Inkluder ER-diagram",  "type": "bool", "default": True},
            {"key": "pdf_suffix",          "label": "PDF-filsuffiks",        "type": "str",  "default": "_metadata_rapport"},
        ],
    },
    # ── Pakking ──────────────────────────────────────────────────────────────
    {
        "cls": DiasPackageOperation,
        "label": "DIAS-pakking (SIP/AIC)",
        "category": "SIP/AIC-Pakking",
        "desc": (
            "Pakker den ferdigbehandlede SIARD-filen inn i et DIAS-pakkeformat "
            "i henhold til METS- og DIAS_PREMIS-standardene, klar for innsending "
            "til langtidsbevaringsplatform (ESSArch). Produserer en AIC-mappe med "
            "SIP, mets.xml, premis.xml, log.xml og komprimert tar-arkiv."
        ),
        "status": DiasPackageOperation.status,
        "params": [
            {"key": "submission_agreement", "label": "Submission Agreement",           "type": "str",    "default": DiasPackageOperation.default_params["submission_agreement"]},
            {"key": "uttrekksdato",         "label": "Uttrekksdato (ÅÅÅÅ-MM-DD)",      "type": "str",    "default": DiasPackageOperation.default_params["uttrekksdato"]},
            {"key": "label",                "label": "Pakketittel",                    "type": "str",    "default": DiasPackageOperation.default_params["label"]},
            {"key": "system",               "label": "Kildesystem",                    "type": "autocomplete", "source": "kildesystem", "default": DiasPackageOperation.default_params["system"]},
            {"key": "system_version",       "label": "Systemversjon",                  "type": "str",    "default": DiasPackageOperation.default_params["system_version"]},
            {"key": "archivist_type",       "label": "Arkivtype",
             "type": "choice", "default": DiasPackageOperation.default_params["archivist_type"],
             "choices": ["SIARD", "NOARK-5", "Postjournaler", "Annet"]},
            {"key": "period_start",         "label": "Periodens start (ÅÅÅÅ-MM-DD)",   "type": "str",    "default": DiasPackageOperation.default_params["period_start"]},
            {"key": "period_end",           "label": "Periodens slutt (ÅÅÅÅ-MM-DD)",   "type": "str",    "default": DiasPackageOperation.default_params["period_end"]},
            {"key": "owner_org",            "label": "Eierorganisasjon",               "type": "autocomplete", "source": "kommunenummer", "default": DiasPackageOperation.default_params["owner_org"]},
            {"key": "archivist_org",        "label": "Arkivorganisasjon",              "type": "str",    "default": DiasPackageOperation.default_params["archivist_org"]},
            {"key": "submitter_org",        "label": "Avleverende organisasjon",       "type": "str",    "default": DiasPackageOperation.default_params["submitter_org"]},
            {"key": "submitter_person",     "label": "Avleverende person",             "type": "str",    "default": DiasPackageOperation.default_params["submitter_person"]},
            {"key": "producer_org",         "label": "Produsent (org)",                "type": "str",    "default": DiasPackageOperation.default_params["producer_org"]},
            {"key": "producer_person",      "label": "Produsent (person)",             "type": "str",    "default": DiasPackageOperation.default_params["producer_person"]},
            {"key": "producer_software",    "label": "Produsent (programvare)",        "type": "str",    "default": DiasPackageOperation.default_params["producer_software"]},
            {"key": "creator",              "label": "Skaper",                         "type": "str",    "default": DiasPackageOperation.default_params["creator"]},
            {"key": "preserver",            "label": "Bevaringsansvarlig",             "type": "str",    "default": DiasPackageOperation.default_params["preserver"]},
            {"key": "username",             "label": "Brukernavn",                     "type": "str",    "default": DiasPackageOperation.default_params["username"]},
            {"key": "schema_dir",           "label": "Skjemamappe (mets.xsd osv)",     "type": "str",    "default": DiasPackageOperation.default_params["schema_dir"]},
            {"key": "output_dir",           "label": "Utdatamappe (tom = SIARD-mappe)","type": "str",    "default": DiasPackageOperation.default_params["output_dir"]},
        ],
    },
        # ── Rapport ──────────────────────────────────────────────────────────────
    {
        "cls": WorkflowReportOperation,
        "label": "Kjørerapport (PDF)",
        "category": "Rapport",
        "desc": (
            "Genererer en PDF-sluttrapport med oversikt over alle utførte steg, "
            "resultater og grafisk fremstilling av nøkkeltall. "
            "Rapporten lagres automatisk i mappen der kilde-SIARD-filen befinner seg. "
            "Legg denne operasjonen sist i workflowen for best resultat."
        ),
        "status": WorkflowReportOperation.status,
        "params": [
            {"key": "report_suffix",   "label": "Filsuffiks rapport",     "type": "str",  "default": "_workflow_rapport"},
            {"key": "include_charts",  "label": "Inkluder kakediagrammer", "type": "bool", "default": True},
            {"key": "include_details", "label": "Inkluder detaljseksjoner","type": "bool", "default": True},
        ],
    },
    #{
    #    "cls": None,
    #    "label": "Betinget (IF-flagg)",
    #    "category": "Kontroll",
    #    "desc": "Kjorer en operasjon kun hvis et kontekstflagg er True/False.",
    #    "status": 2,
    #    "params": [],
    #    "special": "conditional",
    #},
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
        has_wide = any(p.get("type") in ("hw_int", "autocomplete") or p.get("key") == "temp_dir"
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
                elif p["type"] == "autocomplete":
                    var = ctk.StringVar(value=str(p["default"]))
                    ac = _AutocompleteEntry(
                        frm,
                        full_list=_get_autocomplete_list(p.get("source", "")),
                        variable=var,
                        siard_source=p.get("source", ""),
                        width=280,
                    )
                    ac.grid(row=i, column=1, padx=12, sticky="e")
                    self._vars[p["key"]] = (var, "str")
                    continue
                else:
                    if (p["key"] == "uttrekksdato"
                            and _current_siard_path
                            and _current_siard_path.exists()):
                        import datetime
                        _mtime = _current_siard_path.stat().st_mtime
                        _default = datetime.datetime.fromtimestamp(_mtime).strftime("%Y-%m-%d")
                    else:
                        _default = str(p["default"])
                    var = ctk.StringVar(value=_default)
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
                # Lagre format-innstillinger til config.json
                _config_keys = {"pdfa_version"}
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
                           wraplength=300)
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

        # Filtrer operasjoner basert på min_operation_status fra config.json.
        # 0 = vis alle, 1 = vis beta + ok, 2 = vis kun ok/releaset (standard).
        try:
            min_status = int(get_config("min_operation_status") or 2)
        except (TypeError, ValueError):
            min_status = 2
        visible = [d for d in OP_DEFS if d.get("status", 2) >= min_status]

        # Bygg kun kategorier som har minst én synlig operasjon
        categories = list(dict.fromkeys(d["category"] for d in visible))
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
            ops = [d for d in visible if d["category"] == cat]
            for i, op_def in enumerate(ops):
                OperationCard(tab, op_def,
                              on_add=self._on_add,
                              on_saved=self._on_saved).grid(
                    row=i // 3, column=i % 3, padx=4, pady=4, sticky="ew")
