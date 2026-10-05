"""dependencies.py

Samlet oversikt over hva SIARD Workflow Manager trenger for å kjøre, og hva
som slutter å virke når noe mangler.

Brukes av
  * main.py              — påkrevde pakker før GUI starter (med bekreftelse)
  * gui/app.py           — oppstartsoversikt + preflight-steget «Programvare»
  * gui/dependencies_dialog.py — visning og installasjon

Nivåer
  REQUIRED  programmet starter ikke uten (customtkinter)
  STANDARD  del av vanlig installasjon (requirements.txt); én eller flere
            operasjoner feiler eller gir feil resultat uten
  OPTIONAL  tillegg med reserveløsning; kun redusert funksjonalitet

Modulen bruker bare standardbiblioteket på toppnivå, slik at main.py kan
importere den før noen tredjepartspakker er installert.
"""
from __future__ import annotations

import hashlib
import importlib
import importlib.util
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

try:
    from app_paths import FROZEN            # pakket program: pakkene følger med, ingen pip
except Exception:
    FROZEN = bool(getattr(sys, "frozen", False))

REQUIRED = "påkrevd"
STANDARD = "standard"
OPTIONAL = "valgfri"

MIN_PYTHON = (3, 10)


# ─── Python-pakker ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PyPackage:
    key: str
    import_name: str
    pip_spec: str               # "" = kan ikke installeres automatisk
    level: str
    label: str
    affects: str                # hva som ikke virker uten pakken
    ops: tuple = ()             # operation_id-er som trenger pakken
    blocking_ops: tuple = ()    # ... og som feiler / gir feil resultat uten
    probe_import: bool = False  # importer for å sjekke (pakker med innebygd DLL)
    note: str = ""


_MAGIC_SPEC = "python-magic-bin>=0.4.14" if sys.platform == "win32" else "python-magic>=0.4.27"

PY_PACKAGES: tuple[PyPackage, ...] = (
    PyPackage("customtkinter", "customtkinter", "customtkinter>=5.2.2", REQUIRED,
              "customtkinter", "Brukergrensesnittet — programmet starter ikke uten."),
    PyPackage("reportlab", "reportlab", "reportlab>=4.0", STANDARD, "ReportLab",
              "PDF-rapporter: Kjørerapport, Depotrapporter og PDF fra Metadata-uttrekk.",
              ops=("workflow_report", "depot_reports", "metadata_extract"),
              blocking_ops=("workflow_report", "depot_reports")),
    PyPackage("lxml", "lxml", "lxml>=4.9.0", STANDARD, "lxml",
              "SIARD Mapper (metadata fra JSON-mal).",
              ops=("siardmapper",), blocking_ops=("siardmapper",)),
    PyPackage("pymupdf", "fitz", "PyMuPDF>=1.24", STANDARD, "PyMuPDF",
              "E-post (MSG/EML) → PDF/A i BLOB-konvertering: sammenslåing med vedlegg "
              "og bilder. Uten feiler e-postkonverteringen.",
              ops=("blob_convert",), blocking_ops=("blob_convert",)),
    PyPackage("olefile", "olefile", "olefile>=0.46", STANDARD, "olefile",
              "Gjenkjenning og lesing av Outlook-MSG (BLOB-konvertering) og Word-filer "
              "i CosDoc. Uten blir MSG-filer feilaktig behandlet som Word-dokumenter.",
              ops=("blob_convert", "cosdoc_mailmerge"),
              blocking_ops=("blob_convert", "cosdoc_mailmerge")),
    PyPackage("msoffcrypto", "msoffcrypto", "msoffcrypto-tool>=5.0", STANDARD,
              "msoffcrypto-tool", "CosDoc: dekryptering av passordbeskyttede Office-filer.",
              ops=("cosdoc_mailmerge",), blocking_ops=("cosdoc_mailmerge",)),
    PyPackage("python-docx", "docx", "python-docx>=1.1", STANDARD, "python-docx",
              "CosDoc: lesing av datakilde-tabeller i Word-filer.",
              ops=("cosdoc_mailmerge",), blocking_ops=("cosdoc_mailmerge",)),
    PyPackage("mailmerge", "mailmerge", "docx-mailmerge2>=0.6", STANDARD, "docx-mailmerge2",
              "CosDoc: fletting av flettefelt (MERGEFIELD).",
              ops=("cosdoc_mailmerge",), blocking_ops=("cosdoc_mailmerge",)),
    PyPackage("psutil", "psutil", "psutil>=5.9", STANDARD, "psutil",
              "Nøyaktig måling av minne ved automatisk valg av antall parallelle "
              "konverteringer (enklere reserveløsning brukes uten)."),
    PyPackage("tkinterdnd2", "tkinterdnd2", "tkinterdnd2>=0.3", STANDARD, "tkinterdnd2",
              "Dra-og-slipp av SIARD-filer inn i køen (Legg til-knappen virker uansett).",
              note="Krever omstart av programmet etter installasjon."),
    PyPackage("py7zr", "py7zr", "py7zr>=0.20", OPTIONAL, "py7zr",
              "BLOB-konvertering: utpakking av 7z-arkiver i LOB-er (beholdes ellers urørt).",
              ops=("blob_convert",)),
    PyPackage("rarfile", "rarfile", "rarfile>=4.0", OPTIONAL, "rarfile",
              "BLOB-konvertering: utpakking av RAR-arkiver i LOB-er (beholdes ellers urørt).",
              ops=("blob_convert",),
              note="Krever i tillegg UnRAR eller 7-Zip på maskinen."),
    PyPackage("python-magic", "magic", _MAGIC_SPEC, OPTIONAL, "python-magic",
              "DIAS-pakke: MIME-type via libmagic (innebygd gjenkjenning brukes uten).",
              ops=("dias_package",), probe_import=True),
)


def package_available(pkg: PyPackage) -> bool:
    if pkg.probe_import:
        try:
            importlib.import_module(pkg.import_name)
            return True
        except Exception:
            return False
    try:
        return importlib.util.find_spec(pkg.import_name) is not None
    except (ImportError, ValueError):
        return False


def package_version(pkg: PyPackage) -> str:
    try:
        from importlib.metadata import version
        return version(pkg.pip_spec.split(">")[0].split("=")[0].split("<")[0].strip())
    except Exception:
        return ""


def missing_packages(levels: tuple = (REQUIRED,)) -> list[PyPackage]:
    return [p for p in PY_PACKAGES if p.level in levels and not package_available(p)]


def pip_install(specs: list[str], timeout: int = 900) -> tuple[bool, str]:
    """
    Installer pakker med pip for DENNE Python-tolken. pip velger selv
    brukerinstallasjon når site-packages ikke er skrivbar (f.eks. Program Files).
    Returnerer (ok, melding).
    """
    if not specs:
        return True, ""
    exe = sys.executable
    # pythonw.exe kan kjøre pip, men python.exe gir samme resultat og er sikrere
    if exe.lower().endswith("pythonw.exe"):
        alt = Path(exe).with_name("python.exe")
        if alt.exists():
            exe = str(alt)
    cmd = [exe, "-m", "pip", "install", "--disable-pip-version-check", *specs]
    kw: dict = {}
    if sys.platform == "win32":
        kw["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                           encoding="utf-8", errors="replace", **kw)
    except subprocess.TimeoutExpired:
        return False, f"pip brukte mer enn {timeout} s"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    importlib.invalidate_caches()
    if r.returncode != 0:
        tail = "\n".join((r.stderr or r.stdout or "").strip().splitlines()[-8:])
        return False, tail or f"pip returnerte {r.returncode}"
    return True, ""


def manual_pip_command(specs: list[str]) -> str:
    return "python -m pip install " + " ".join(f'"{s}"' if ">" in s or "<" in s else s
                                              for s in specs)


# ─── Eksterne programmer ──────────────────────────────────────────────────────

@dataclass
class DepStatus:
    """Status for én avhengighet (pakke eller program) på denne maskinen."""
    key: str
    kind: str                   # "pakke" | "program"
    label: str
    level: str
    ok: bool                    # finnes / virker
    in_use: bool = True         # False = slått av i innstillinger (ikke «mangler»)
    detail: str = ""            # versjon/sti eller hvorfor den mangler
    affects: str = ""
    ops: tuple = ()
    blocking_ops: tuple = ()
    action: str = ""            # "pip" | "component" | "link" | "settings" | ""
    pip_spec: str = ""
    url: str = ""
    note: str = ""
    component: str = ""         # components.KINDS-nøkkel for action="component"
    bundled: str = ""           # medfølgende installasjonsfil i komponenter/ (offline)

    @property
    def missing(self) -> bool:
        return self.in_use and not self.ok


def _cfg(key: str, default=None):
    try:
        from settings import get_config
        v = get_config(key, default)
        return default if v is None else v
    except Exception:
        return default


def check_libreoffice() -> DepStatus:
    from siard_workflow.core.lo_runner import DOWNLOAD_URL
    st = DepStatus("libreoffice", "program", "LibreOffice", STANDARD, False,
                   affects=("BLOB-konvertering av Office-dokumenter, RTF, ODF, HTML m.m. til "
                            "PDF/A, e-postvedlegg og CosDoc-fletting. Uten beholdes slike "
                            "filer uendret."),
                   ops=("blob_convert", "cosdoc_mailmerge"),
                   blocking_ops=("blob_convert", "cosdoc_mailmerge"),
                   action="component", component="libreoffice", url=DOWNLOAD_URL)
    try:
        from siard_workflow.core.libreoffice import find_libreoffice, verify_libreoffice_path
        saved = str(_cfg("lo_executable", "") or "")
        lo = (verify_libreoffice_path(saved) if saved else None) or find_libreoffice("")
    except Exception:
        lo = None
    if lo:
        st.ok = True
        try:
            from siard_workflow.core.lo_runner import lo_version
            ver = lo_version(lo)
        except Exception:
            ver = ""
        st.detail = f"{ver + ' — ' if ver else ''}{lo}"
    else:
        st.detail = "ikke funnet"
    return st


def check_ghostscript() -> DepStatus:
    st = DepStatus("ghostscript", "program", "Ghostscript", OPTIONAL, False,
                   affects=("PDF/A-normalisering av e-post-PDF-er og PDF-vedlegg. Uten "
                            "brukes LibreOffice alene (resultatet er ikke alltid gyldig PDF/A)."),
                   ops=("blob_convert",), action="component", component="ghostscript",
                   note="Installeres fra Artifex (GitHub) i brukermappen. AGPL-lisens.")
    if not bool(_cfg("use_ghostscript", True)):
        st.in_use = False
        st.detail = "slått av i Innstillinger"
    try:
        from siard_workflow.core import ghostscript as G
        gs = G.find_ghostscript()
        if gs:
            st.ok = True
            st.detail = f"{G.get_version(gs) or '?'} — {gs}" + (
                "" if st.in_use else " (slått av i Innstillinger)")
        elif st.in_use:
            st.detail = "ikke funnet"
    except Exception as exc:
        st.detail = st.detail or f"kunne ikke sjekkes ({exc})"
    return st


def check_siegfried() -> DepStatus:
    st = DepStatus("siegfried", "program", "Siegfried (PRONOM)", OPTIONAL, False,
                   affects=("PRONOM-basert filidentifisering. Uten brukes innebygd "
                            "magic-bytes-gjenkjenning."),
                   ops=("blob_convert",), action="component", component="siegfried")
    if not bool(_cfg("use_siegfried", False)):
        st.in_use = False
        st.detail = "ikke valgt (magic-bytes brukes)"
    try:
        from siard_workflow.core.identifiers import installer as I
        if I.is_installed():
            st.ok = True
            st.detail = (I.get_version() or str(_cfg("sf_executable", ""))) + (
                "" if st.in_use else " (ikke valgt i Innstillinger)")
        elif st.in_use:
            st.detail = "valgt i Innstillinger, men ikke installert"
    except Exception as exc:
        st.detail = st.detail or f"kunne ikke sjekkes ({exc})"
    return st


def check_antivirus() -> DepStatus:
    st = DepStatus("antivirus", "program", "Antivirus (Defender/ClamAV)", STANDARD, False,
                   affects="Virusskanning av SIARD-innhold.",
                   ops=("virus_scan",), blocking_ops=("virus_scan",), action="settings",
                   note="Sett «AV-program» i Innstillinger, eller installer ClamAV.")
    configured = str(_cfg("av_executable", "") or "").strip()
    try:
        import shutil
        if configured and (Path(configured).exists() or shutil.which(configured)):
            st.ok, st.detail = True, configured
            return st
        from siard_workflow.operations.virus_scan_operation import _auto_detect_av
        exe, _args, _rc = _auto_detect_av()
        if exe:
            st.ok, st.detail = True, str(exe)
        else:
            st.detail = ("konfigurert program finnes ikke: " + configured) if configured \
                else "ingen funnet (Defender er av eller mangler, ClamAV ikke installert)"
    except Exception as exc:
        st.detail = f"kunne ikke sjekkes ({exc})"
    return st


def check_ollama() -> DepStatus:
    st = DepStatus("ollama", "program", "Ollama (lokal KI)", OPTIONAL, False,
                   affects=("KI-assistert PII-deteksjon og utvalgsanbefaling i Anonymisering. "
                            "Uten brukes kun regelbasert deteksjon."),
                   ops=("anonymize",), action="link", url="https://ollama.com/download")
    if not bool(_cfg("ollama_enabled", True)):
        st.in_use = False
        st.detail = "slått av i Innstillinger"
        return st
    try:
        from siard_workflow.core.anonymize.ollama_client import OllamaClient
        c = OllamaClient(host=str(_cfg("ollama_host", "127.0.0.1")),
                         port=int(_cfg("ollama_port", 11434) or 11434), timeout=3)
        if c.is_alive():
            st.ok = True
            models = c.list_models()
            st.detail = f"kjører — {len(models)} modell(er)" if models else \
                "kjører, men ingen modeller er lastet ned"
        else:
            st.detail = "kjører ikke / ikke installert"
    except Exception as exc:
        st.detail = f"kunne ikke sjekkes ({exc})"
    return st


# (sjekk, operasjoner som bruker programmet) — preflight sjekker bare det som er relevant
_TOOL_CHECKS = (
    (check_libreoffice, ("blob_convert", "cosdoc_mailmerge")),
    (check_ghostscript, ("blob_convert",)),
    (check_siegfried,   ("blob_convert",)),
    (check_antivirus,   ("virus_scan",)),
    (check_ollama,      ("anonymize",)),
)


def package_statuses() -> list[DepStatus]:
    out = []
    for p in PY_PACKAGES:
        ok = package_available(p)
        if FROZEN:
            # Pakket program: alt følger med; en mangel er en feil i pakken
            detail = (package_version(p) or "følger med programpakken") if ok else \
                "mangler i programpakken — hent en ny pakke"
            action, note = "", ""
        else:
            detail = (package_version(p) or "installert") if ok else "ikke installert"
            action, note = ("pip" if p.pip_spec else ""), p.note
        out.append(DepStatus(
            p.key, "pakke", p.label, p.level, ok, detail=detail,
            affects=p.affects, ops=p.ops, blocking_ops=p.blocking_ops,
            action=action, pip_spec=p.pip_spec, note=note))
    return out


def _mark_bundled(st: DepStatus) -> DepStatus:
    """Sett `bundled` når installasjonsfilen følger med pakken (komponenter/)."""
    if st.action != "component" or not st.component:
        return st
    try:
        from siard_workflow.core import components as C
        f = C.bundled(st.component)
        if f:
            st.bundled = str(f)
            if not st.ok:
                ver = C.bundled_version(st.component)
                st.note = ((st.note + "\n") if st.note else "") + \
                    f"Installasjonsfil følger med pakken{' (' + ver + ')' if ver else ''} " \
                    "— kan installeres uten nett."
    except Exception:
        pass
    return st


def check_all(include_tools: bool = True,
              only_ops: "set | None" = None) -> list[DepStatus]:
    """Status for alle pakker og (valgfritt) eksterne programmer. `only_ops`:
    sjekk bare programmer som brukes av disse operasjonene (raskere)."""
    out = package_statuses()
    if include_tools:
        for fn, tool_ops in _TOOL_CHECKS:
            if only_ops is not None and not set(tool_ops) & set(only_ops):
                continue
            try:
                out.append(_mark_bundled(fn()))
            except Exception as exc:
                out.append(DepStatus(fn.__name__.replace("check_", ""), "program",
                                     fn.__name__.replace("check_", ""), OPTIONAL, False,
                                     detail=f"kunne ikke sjekkes ({exc})"))
    return out


def startup_problems(statuses: list[DepStatus]) -> list[DepStatus]:
    """Det som bør vises ved oppstart: manglende standardkomponenter, og
    valgfrie som er slått PÅ i innstillingene men ikke finnes."""
    return [s for s in statuses if s.missing and s.level in (REQUIRED, STANDARD)
            or s.missing and s.level == OPTIONAL and s.key in ("ghostscript", "siegfried")
            # Følger installasjonsfilen med pakken, tilbys den ved oppstart
            # også når komponenten er valgfri/avslått
            or s.bundled and not s.ok]


def signature(statuses: list[DepStatus]) -> str:
    """Stabil nøkkel for «ikke vis igjen»: endres når settet av mangler endres."""
    keys = sorted(s.key for s in statuses)
    return hashlib.sha1("|".join(keys).encode()).hexdigest()[:16] if keys else ""


@dataclass
class OpImpact:
    op_id: str
    op_label: str
    dep: DepStatus
    blocking: bool


def impacts_for_ops(ops: list, statuses: "list[DepStatus] | None" = None,
                    skip_keys: tuple = ()) -> list[OpImpact]:
    """Manglende avhengigheter for operasjonene i arbeidsflyten."""
    if statuses is None:
        statuses = check_all()
    out: list[OpImpact] = []
    for op in ops:
        oid = getattr(op, "operation_id", "")
        label = getattr(op, "label", oid)
        for s in statuses:
            if not s.missing or s.key in skip_keys or oid not in s.ops:
                continue
            blocking = oid in s.blocking_ops
            # Metadata-uttrekk: ReportLab trengs bare når PDF skal lages
            if oid == "metadata_extract" and s.key == "reportlab":
                if not (getattr(op, "params", {}) or {}).get("generate_pdf", True):
                    continue
            out.append(OpImpact(oid, label, s, blocking))
    return out
