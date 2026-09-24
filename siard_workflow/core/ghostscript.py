"""siard_workflow/core/ghostscript.py

Valgfri Ghostscript-støtte: PDF/A-normalisering av sammensatte PDF-er
(e-post → PDF/A) og PDF-vedlegg som ikke selv er PDF/A.

* `find_ghostscript()` — konfigurert sti (`ghostscript_bin`), PATH, standard
  installasjonsmapper (Program Files / AppData\\Local\\Programs / vår egen
  installasjonsmappe). Nyeste versjon velges.
* `to_pdfa()` — `pdfwrite` med `-dPDFA=2|3`, sRGB OutputIntent fra
  Ghostscripts egne ICC-profiler.
* `install_ghostscript()` — «on demand» fra offisielle Artifex-utgivelser på
  GitHub (`ArtifexSoftware/ghostpdl-downloads`), verifisert mot `SHA512SUMS`,
  stille NSIS-installasjon (`/S /D=…`) til
  `%LOCALAPPDATA%\\SIARDManager\\ghostscript\\gs<versjon>`. Ber Windows om
  administratorrettigheter bare hvis installasjonsprogrammet krever det.

Ghostscript er AGPL/kommersielt lisensiert (Artifex). Det lastes ned av
brukeren fra Artifex sin kilde og følger ikke med SIARD Manager.
"""
from __future__ import annotations

import glob
import hashlib
import json
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

_GITHUB_API = "https://api.github.com/repos/ArtifexSoftware/ghostpdl-downloads/releases/latest"
_EXE_NAMES = ("gswin64c.exe", "gswin32c.exe") if sys.platform == "win32" else ("gs",)


# ── Oppdaging ────────────────────────────────────────────────────────────────

def install_target_root() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "SIARDManager"
    return base / "ghostscript"


def _version_key(p: Path) -> tuple:
    m = re.search(r"gs(\d+)\.(\d+)(?:\.(\d+))?", str(p).replace("\\", "/"))
    return tuple(int(x or 0) for x in m.groups()) if m else (0,)


def _candidates() -> list[Path]:
    found: list[Path] = []
    for name in _EXE_NAMES:
        w = shutil.which(name)
        if w:
            found.append(Path(w))
    if sys.platform == "win32":
        roots = [os.environ.get("ProgramFiles", r"C:\Program Files"),
                 os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"),
                 str(Path(os.environ.get("LOCALAPPDATA", "")) / "Programs"),
                 str(install_target_root().parent)]
        for root in roots:
            for pat in ("gs/gs*/bin/gswin64c.exe", "gs/gs*/bin/gswin32c.exe",
                        "ghostscript/gs*/bin/gswin64c.exe"):
                found += [Path(x) for x in glob.glob(str(Path(root) / pat))]
    return found


def find_ghostscript() -> "Path | None":
    """Sti til Ghostscript (konfigurert, ellers nyeste funnet), eller None."""
    try:
        from settings import get_config
        cfg = str(get_config("ghostscript_bin", "") or "").strip()
    except Exception:
        cfg = ""
    if cfg and Path(cfg).is_file():
        return Path(cfg)
    cands = [p for p in _candidates() if p.is_file()]
    if not cands:
        return None
    return sorted(set(cands), key=_version_key)[-1]


def ghostscript_enabled() -> bool:
    try:
        from settings import get_config
        return bool(get_config("use_ghostscript", True))
    except Exception:
        return True


def active_ghostscript() -> "Path | None":
    """Ghostscript som skal brukes (aktivert i innstillinger og funnet)."""
    return find_ghostscript() if ghostscript_enabled() else None


def get_version(gs: "Path | None" = None) -> "str | None":
    gs = gs or find_ghostscript()
    if not gs:
        return None
    try:
        r = subprocess.run([str(gs), "--version"], capture_output=True, timeout=30,
                           **hidden_kwargs())
        return r.stdout.decode("ascii", "replace").strip() or None
    except Exception:
        return None


# ── PDF/A-normalisering ──────────────────────────────────────────────────────

def _ps_string(path: Path) -> str:
    s = str(path).replace("\\", "/")
    return "(" + s.replace("(", r"\(").replace(")", r"\)") + ")"


def _find_icc(gs: Path) -> "Path | None":
    for base in (gs.parent.parent, gs.parent):
        for name in ("srgb.icc", "default_rgb.icc"):
            p = base / "iccprofiles" / name
            if p.is_file():
                return p
    hits = glob.glob(str(gs.parent.parent / "**" / "srgb.icc"), recursive=True)
    return Path(hits[0]) if hits else None


def to_pdfa(src: Path, dst: Path, part: int = 2, gs: "Path | None" = None,
            timeout: float = 600) -> "tuple[bool, str]":
    """
    Normaliser `src` til PDF/A-`part`b i `dst` med Ghostscript. (True, "") ved
    suksess. Innebygde filer og tagging beholdes ikke — legg inn original-
    vedlegg (PDF/A-3) ETTER normaliseringen.
    """
    gs = gs or active_ghostscript()
    if not gs:
        return False, "Ghostscript ikke funnet"
    icc = _find_icc(gs)
    if not icc:
        return False, f"fant ikke sRGB ICC-profil under {gs.parent.parent}"
    work = Path(tempfile.mkdtemp(prefix="gs_pdfa_"))
    try:
        defps = work / "PDFA_def.ps"
        defps.write_text(
            "%!\n"
            f"/ICCProfile {_ps_string(icc)} def\n"
            "[/_objdef {icc_PDFA} /type /stream /OBJ pdfmark\n"
            "[{icc_PDFA} <</N 3>> /PUT pdfmark\n"
            "[{icc_PDFA} ICCProfile (r) file /PUT pdfmark\n"
            "[/_objdef {OutputIntent_PDFA} /type /dict /OBJ pdfmark\n"
            "[{OutputIntent_PDFA} <<\n"
            "  /Type /OutputIntent /S /GTS_PDFA1\n"
            "  /DestOutputProfile {icc_PDFA}\n"
            "  /OutputConditionIdentifier (sRGB IEC61966-2.1)\n"
            ">> /PUT pdfmark\n"
            "[{Catalog} <</OutputIntents [ {OutputIntent_PDFA} ]>> /PUT pdfmark\n",
            "ascii")
        tmp_out = work / "out.pdf"
        cmd = [str(gs), f"-dPDFA={part}", "-dBATCH", "-dNOPAUSE", "-dQUIET",
               "-dNOOUTERSAVE", "-dPDFACompatibilityPolicy=1",
               "-sColorConversionStrategy=RGB", "-sProcessColorModel=DeviceRGB",
               "-sDEVICE=pdfwrite",
               f"--permit-file-read={str(icc.parent).replace(chr(92), '/')}/",
               f"-sOutputFile={tmp_out}", str(defps), str(src)]
        from siard_workflow.core.lo_runner import run_lo
        rc, out, err, timed_out = run_lo(cmd, timeout)
        if timed_out:
            return False, f"Ghostscript: tidsavbrudd etter {int(timeout)} s"
        if rc != 0 or not tmp_out.exists() or tmp_out.stat().st_size == 0:
            msg = (err or out).decode("utf-8", "replace").strip()
            return False, f"Ghostscript feilet (rc={rc}): {msg[:300]}"
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(tmp_out), str(dst))
        return True, ""
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ── Installasjon «on demand» ─────────────────────────────────────────────────

def _say(progress: ProgressCb, msg: str) -> None:
    if progress:
        try:
            progress(msg)
        except Exception:
            pass


def install_ghostscript(progress: ProgressCb = None) -> Path:
    """
    Last ned og installer siste Ghostscript for Windows (64-bit) til
    %LOCALAPPDATA%\\SIARDManager\\ghostscript\\gs<versjon>. Lagrer stien i
    config (`ghostscript_bin`). Returnerer sti til gswin64c.exe.
    """
    if sys.platform != "win32":
        raise RuntimeError(
            "Automatisk installasjon støttes bare på Windows. Installer "
            "Ghostscript med pakkebehandleren: «brew install ghostscript» "
            "(macOS) eller «sudo apt install ghostscript» (Linux).")
    _say(progress, "Henter utgivelsesinfo fra GitHub (ArtifexSoftware/ghostpdl-downloads)...")
    req = urllib.request.Request(_GITHUB_API, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rel = json.load(r)
    assets = rel.get("assets", [])
    asset = next((a for a in assets if re.fullmatch(r"gs\d+w64\.exe", a["name"])), None)
    if asset is None:
        raise RuntimeError("Fant ingen 64-bit Windows-installasjonsfil (gs*w64.exe) i "
                           f"utgivelse {rel.get('tag_name', '?')}")
    sums = next((a for a in assets if a["name"].upper() == "SHA512SUMS"), None)

    tmp = Path(tempfile.mkdtemp(prefix="gs_install_"))
    try:
        exe = tmp / asset["name"]
        size_mb = asset.get("size", 0) / 1e6
        _say(progress, f"Laster ned {asset['name']} ({size_mb:.0f} MB)...")
        urllib.request.urlretrieve(asset["browser_download_url"], str(exe))

        if sums is not None:
            _say(progress, "Verifiserer SHA512...")
            with urllib.request.urlopen(sums["browser_download_url"], timeout=30) as r:
                lines = r.read().decode("utf-8", "replace").splitlines()
            expected = next((ln.split()[0].lower() for ln in lines
                             if ln.strip().endswith(asset["name"])), None)
            actual = hashlib.sha512(exe.read_bytes()).hexdigest().lower()
            if expected and actual != expected:
                raise RuntimeError(f"SHA512 stemmer ikke for {asset['name']}")

        ver = re.sub(r"\D", "", asset["name"].split("w64")[0]) or "ny"
        target = install_target_root() / f"gs{ver}"
        target.parent.mkdir(parents=True, exist_ok=True)
        _say(progress, f"Installerer til {target} ...")
        # NSIS: /S = stille, /D=<sti> MÅ være siste argument og uten anførselstegn
        args = ["/S", f"/D={target}"]
        try:
            subprocess.run([str(exe), *args], timeout=900, check=False, **hidden_kwargs())
        except OSError as exc:
            if getattr(exc, "winerror", None) != 740:       # 740 = krever heving
                raise
            _say(progress, "Installasjonsprogrammet krever administratorrettigheter — "
                           "bekreft i Windows-dialogen...")
            ps_args = ",".join(f"'{a}'" for a in args)
            subprocess.run(["powershell", "-NoProfile", "-Command",
                            f"Start-Process -FilePath '{exe}' -ArgumentList {ps_args} "
                            f"-Verb RunAs -Wait"], timeout=900, check=False,
                           **hidden_kwargs())
        gs = next((p for p in target.rglob("gswin64c.exe") if p.is_file()), None)
        if gs is None:
            gs = find_ghostscript()
        if gs is None:
            raise RuntimeError("Installasjonen ble ikke fullført (fant ikke gswin64c.exe)")
        try:
            from settings import set_config
            set_config("ghostscript_bin", str(gs))
        except Exception:
            pass
        _say(progress, f"Ghostscript installert: {gs}")
        return gs
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
