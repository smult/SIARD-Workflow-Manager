"""siard_workflow/core/components.py

Store eksterne programmer som programmet bruker, men som ikke bygges inn i
selve programpakken: LibreOffice, Ghostscript og Siegfried.

De kan følge med den ferdige pakken som installasjonsfiler i `komponenter/`
ved siden av SIARDManager.exe (se packaging/build_release.py), og installeres
da UTEN nett. Uten medfølgende fil lastes de ned når maskinen er på nett.
Alle installeres i brukermappa (%LOCALAPPDATA%\\SIARDManager\\…) og krever
ikke administratorrettigheter:

  LibreOffice  MSI «administrativ installasjon» (msiexec /a) = utpakking av
               programfilene uten registrering i Windows. Verifisert med
               LibreOffice 26.2.6 (ca. 2 min, 1,5 GB).
  Ghostscript  NSIS-installasjon med /S /D=<mappe>.
  Siegfried    zip + medfølgende PRONOM-signaturer (default.sig).
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import Callable, Optional

from siard_workflow.core.subproc import hidden_kwargs

ProgressCb = Optional[Callable[[str], None]]

KINDS = ("libreoffice", "ghostscript", "siegfried")
LABELS = {"libreoffice": "LibreOffice", "ghostscript": "Ghostscript",
          "siegfried": "Siegfried"}

LO_STABLE_URL = "https://download.documentfoundation.org/libreoffice/stable/"
LO_MSI_GLOB = "LibreOffice_*_Win_x86-64.msi"


def _say(progress: ProgressCb, msg: str) -> None:
    if progress:
        try:
            progress(msg)
        except Exception:
            pass


def components_dir() -> Path:
    try:
        from app_paths import components_dir as _cd
        return _cd()
    except Exception:
        return Path(__file__).resolve().parents[2] / "komponenter"


def _glob_for(kind: str) -> str:
    if kind == "libreoffice":
        return LO_MSI_GLOB
    if kind == "ghostscript":
        from siard_workflow.core.ghostscript import INSTALLER_GLOB
        return INSTALLER_GLOB
    from siard_workflow.core.identifiers.installer import ARCHIVE_GLOB
    return ARCHIVE_GLOB


def _version_key(p: Path) -> tuple:
    return tuple(int(x) for x in re.findall(r"\d+", p.name)[:4]) or (0,)


def bundled(kind: str, directory: "Path | None" = None) -> "Path | None":
    """Medfølgende installasjonsfil for `kind` i komponenter/ (nyeste), ellers None."""
    d = Path(directory) if directory else components_dir()
    if not d.is_dir():
        return None
    files = sorted((p for p in d.glob(_glob_for(kind)) if p.is_file()),
                   key=_version_key, reverse=True)
    return files[0] if files else None


def bundled_version(kind: str) -> str:
    f = bundled(kind)
    if not f:
        return ""
    m = re.search(r"(\d+[.\-_]\d+(?:[.\-_]\d+)*)", f.name)
    return m.group(1).replace("-", ".").replace("_", ".") if m else ""


def is_online(timeout: float = 3.0) -> bool:
    for url in ("https://api.github.com", "https://download.documentfoundation.org"):
        try:
            urllib.request.urlopen(urllib.request.Request(url, method="HEAD"),
                                   timeout=timeout)
            return True
        except Exception:
            continue
    return False


# ── LibreOffice ──────────────────────────────────────────────────────────────

def lo_install_root() -> Path:
    return Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "SIARDManager" / "libreoffice"


def pick_lo_version(listing_html: str) -> str:
    """
    Velg LibreOffice-versjon fra «stable»-listen: høyeste versjon med
    feilrettingsnummer ≥ 2 (moden serie — x.y.0/x.y.1 unngås; 26.2.1 hadde
    feilen der gjenbruk av profil henger). Faller tilbake til høyeste.
    """
    vers = sorted({tuple(int(x) for x in v.split("."))
                   for v in re.findall(r'href="(\d+\.\d+\.\d+)/"', listing_html)},
                  reverse=True)
    if not vers:
        raise RuntimeError("Fant ingen LibreOffice-versjoner i nedlastingslisten")
    mature = [v for v in vers if v[2] >= 2]
    return ".".join(map(str, (mature or vers)[0]))


def download_libreoffice(dest_dir: Path, version: str = "",
                         progress: ProgressCb = None) -> Path:
    """Last ned LibreOffice-MSI (64-bit Windows) til `dest_dir`, SHA256-verifisert."""
    if not version:
        _say(progress, "Finner siste modne LibreOffice-versjon...")
        with urllib.request.urlopen(LO_STABLE_URL, timeout=30) as r:
            version = pick_lo_version(r.read().decode("utf-8", "replace"))
    name = f"LibreOffice_{version}_Win_x86-64.msi"
    base = f"{LO_STABLE_URL}{version}/win/x86_64/{name}"
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / name
    expected = ""
    try:
        with urllib.request.urlopen(base + ".sha256", timeout=30) as r:
            expected = r.read().decode("utf-8", "replace").split()[0].lower()
    except Exception:
        pass
    if target.exists() and expected and _sha256(target) == expected:
        _say(progress, f"{name} finnes allerede (verifisert)")
        return target
    _say(progress, f"Laster ned {name} (ca. 370 MB)...")
    part = target.with_suffix(".part")
    urllib.request.urlretrieve(base, str(part))
    if expected:
        _say(progress, "Verifiserer SHA256...")
        if _sha256(part) != expected:
            part.unlink(missing_ok=True)
            raise RuntimeError(f"SHA256 stemmer ikke for {name}")
    part.replace(target)
    return target


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def install_libreoffice_from_msi(msi: Path, progress: ProgressCb = None,
                                 timeout: int = 1800) -> Path:
    """
    Installer LibreOffice fra en lokal MSI uten administratorrettigheter
    (msiexec /a = administrativ installasjon/utpakking) til
    %LOCALAPPDATA%\\SIARDManager\\libreoffice\\<versjon>. Lagrer stien i config
    (`lo_executable`) og nullstiller helsesjekk-cachen. Returnerer soffice.exe.
    """
    if sys.platform != "win32":
        raise RuntimeError("Installasjon fra MSI støttes bare på Windows.")
    msi = Path(msi)
    m = re.search(r"LibreOffice_(\d+(?:\.\d+)+)_", msi.name)
    ver = m.group(1) if m else "ny"
    target = lo_install_root() / ver
    target.mkdir(parents=True, exist_ok=True)
    log = Path(tempfile.gettempdir()) / f"siard_lo_install_{ver}.log"
    _say(progress, f"Installerer LibreOffice {ver} til {target} (tar et par minutter)...")
    r = subprocess.run(["msiexec", "/a", str(msi), "/qn", f"TARGETDIR={target}",
                        "/L*", str(log)], timeout=timeout, check=False, **hidden_kwargs())
    soffice = target / "program" / "soffice.exe"
    if not soffice.exists():
        found = next(iter(target.rglob("soffice.exe")), None)
        if found is None:
            raise RuntimeError(f"LibreOffice-installasjonen feilet (msiexec {r.returncode}). "
                               f"Logg: {log}")
        soffice = found
    try:
        from settings import set_config
        set_config("lo_executable", str(soffice))
    except Exception:
        pass
    try:
        from siard_workflow.core import lo_runner
        lo_runner.reset_session_cache()
    except Exception:
        pass
    _say(progress, f"LibreOffice installert: {soffice}")
    return soffice


# ── Felles ───────────────────────────────────────────────────────────────────

def install(kind: str, progress: ProgressCb = None, source: "Path | None" = None) -> Path:
    """
    Installer `kind` fra medfølgende fil (komponenter/) eller `source`; uten
    fil lastes den ned (krever nett). Returnerer sti til programmet.
    """
    src = Path(source) if source else bundled(kind)
    if kind == "libreoffice":
        if src is None:
            tmp = Path(tempfile.mkdtemp(prefix="lo_dl_"))
            try:
                return install_libreoffice_from_msi(download_libreoffice(tmp, "", progress),
                                                    progress)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
        return install_libreoffice_from_msi(src, progress)
    if kind == "ghostscript":
        from siard_workflow.core import ghostscript as G
        return G.install_from_installer(src, progress) if src else G.install_ghostscript(progress)
    if kind == "siegfried":
        from siard_workflow.core.identifiers import installer as I
        if src:
            sf = I.install_from_archive(src, progress, sig_dir=src.parent / I.SIG_DIRNAME)
        else:
            sf = I.install_siegfried(progress)
        try:
            from settings import set_config
            set_config("use_siegfried", True)   # brukeren har valgt å installere den
            from siard_workflow.core.file_identifier import reset_identifier
            reset_identifier()
        except Exception:
            pass
        return sf
    raise ValueError(f"Ukjent komponent: {kind}")


def download(kind: str, dest_dir: Path, progress: ProgressCb = None,
             lo_version: str = "") -> Path:
    """Last ned installasjonsfil for `kind` til `dest_dir` (brukes når pakken bygges)."""
    if kind == "libreoffice":
        return download_libreoffice(dest_dir, lo_version, progress)
    if kind == "ghostscript":
        from siard_workflow.core import ghostscript as G
        return G.download_installer(dest_dir, progress)
    if kind == "siegfried":
        from siard_workflow.core.identifiers import installer as I
        return I.download_archive(dest_dir, progress, with_signatures=True)
    raise ValueError(f"Ukjent komponent: {kind}")
