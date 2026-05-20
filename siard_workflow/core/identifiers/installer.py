"""
siard_workflow/core/identifiers/installer.py
Auto-installasjon av Siegfried fra offisielle GitHub-releases.

Strategi:
  1. Hent siste release fra https://api.github.com/repos/richardlehane/siegfried/releases/latest
  2. Velg asset for plattform (windows-amd64, darwin-amd64, linux-amd64).
  3. Last ned, verifiser SHA256, pakk ut til LOCALAPPDATA/SIARDManager/siegfried/.
  4. Kjør `sf -update` for å hente PRONOM-signaturer.
  5. Lagre sti i config (`sf_executable`).

Brukes fra innstillinger-dialogen via «Installer/Oppdater Siegfried»-knappen.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Optional


_GITHUB_API = "https://api.github.com/repos/richardlehane/siegfried/releases/latest"


# ── Plattform-detektering ────────────────────────────────────────────────────

def _asset_patterns() -> tuple[list[str], str]:
    """
    Returnerer (mønstre, binary_name) for nåværende plattform.
    Mønstrene matches mot asset-navn case-insensitivt (substring).
    Foretrukne navn først; første treff vinner.

    Eksempler på Siegfried-asset-navn (versjon 1.11.4):
      siegfried_1-11-4_win64.zip          → Windows 64-bit
      siegfried_1-11-4_win64_static.zip   → Windows 64-bit (statisk)
      siegfried_1-11-4_linux64.zip        → Linux 64-bit
      siegfried_1-11-4_mac64.tar.gz       → macOS 64-bit
    """
    system = platform.system().lower()
    if system == "windows":
        # Foretrekk standard (dynamisk) framfor static
        return (["_win64.zip", "_win64_static.zip", "_win7.zip"], "sf.exe")
    if system == "darwin":
        return (["_mac64.tar.gz", "_mac64.zip"], "sf")
    return (["_linux64.tar.gz", "_linux64.zip", "_linux64_static.zip"], "sf")


def install_target_dir() -> Path:
    """Plattform-spesifikk installasjonsplassering for Siegfried."""
    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "SIARDManager"
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support" / "SIARDManager"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME",
                                   str(Path.home() / ".local" / "share"))) / "SIARDManager"
    return base / "siegfried"


# ── Status-helpere ───────────────────────────────────────────────────────────

def is_installed() -> bool:
    """True hvis konfigurert sf-binær eksisterer på disk."""
    try:
        from settings import get_config
        p = get_config("sf_executable", "")
    except Exception:
        return False
    return bool(p) and Path(p).exists()


def get_version() -> Optional[str]:
    """Returnerer 'siegfried X.Y.Z (signature)' eller None."""
    try:
        from settings import get_config
        p = get_config("sf_executable", "")
    except Exception:
        return None
    if not p or not Path(p).exists():
        return None
    try:
        r = subprocess.run([p, "-version"], capture_output=True,
                           text=True, timeout=5,
                           encoding="utf-8", errors="replace")
        return (r.stdout or r.stderr).strip().split("\n")[0] or None
    except Exception:
        return None


# ── Hovedinstallasjon ────────────────────────────────────────────────────────

ProgressCb = Optional[Callable[[str], None]]


def install_siegfried(progress: ProgressCb = None) -> Path:
    """
    Last ned siste Siegfried fra GitHub, pakk ut, kjør sf -update.
    Returnerer sti til sf-binær. Kaster Exception ved feil.
    """
    def _say(msg: str) -> None:
        if progress is not None:
            try:
                progress(msg)
            except Exception:
                pass

    patterns, binary_name = _asset_patterns()

    # 1. Hent release-info
    _say("Henter release-info fra GitHub...")
    req = urllib.request.Request(_GITHUB_API,
                                 headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rel = json.load(r)

    tag = rel.get("tag_name", "?")
    assets = rel.get("assets", [])

    # Velg første asset som matcher et av plattformens mønstre
    # (rangert etter prioritet — første mønster vinner)
    asset = None
    for pat in patterns:
        pat_low = pat.lower()
        asset = next((a for a in assets
                      if pat_low in a["name"].lower()), None)
        if asset is not None:
            break
    if asset is None:
        raise RuntimeError(
            f"Fant ingen Siegfried-asset som matcher {patterns} i "
            f"release {tag}. Tilgjengelig: "
            + ", ".join(a["name"] for a in assets))

    # Sjekksum-asset (valgfri) — Siegfried publiserer ikke alltid .sha256
    checksum_asset = next(
        (a for a in assets
         if a["name"].endswith(".sha256") and asset["name"].rsplit(".", 1)[0]
         in a["name"]),
        None)

    # 2. Last ned binær
    _say(f"Laster ned {asset['name']} (Siegfried {tag})...")
    tmp_dir = Path(tempfile.mkdtemp(prefix="sf_install_"))
    archive_path = tmp_dir / asset["name"]
    try:
        urllib.request.urlretrieve(asset["browser_download_url"],
                                   str(archive_path))
    except urllib.error.URLError as exc:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError(f"Nedlasting feilet: {exc}") from exc

    # 3. Verifiser SHA256 hvis tilgjengelig
    if checksum_asset is not None:
        _say("Verifiserer SHA256...")
        try:
            with urllib.request.urlopen(checksum_asset["browser_download_url"],
                                        timeout=30) as r:
                expected = r.read().decode("utf-8").strip().split()[0].lower()
            actual = hashlib.sha256(archive_path.read_bytes()).hexdigest().lower()
            if actual != expected:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                raise RuntimeError(
                    f"SHA256-feil for {asset['name']}: "
                    f"forventet {expected}, fikk {actual}")
        except urllib.error.URLError:
            # Manglende sjekksum-asset er ikke fatal — vi har TLS fra GitHub
            pass

    # 4. Pakk ut
    _say("Pakker ut...")
    target = install_target_dir()
    target.mkdir(parents=True, exist_ok=True)
    name_low = asset["name"].lower()
    if name_low.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as z:
            z.extractall(target)
    elif name_low.endswith((".tar.gz", ".tgz", ".tar")):
        with tarfile.open(archive_path) as t:
            t.extractall(target)
    else:
        raise RuntimeError(f"Ukjent arkivtype: {asset['name']}")

    # 5. Finn binæren (kan ligge i en undermappe)
    sf_path: Optional[Path] = None
    for cand in target.rglob(binary_name):
        if cand.is_file():
            sf_path = cand
            break
    if sf_path is None:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError(f"Fant ikke '{binary_name}' i utpakket arkiv.")

    if sys.platform != "win32":
        try:
            sf_path.chmod(0o755)
        except Exception:
            pass

    # 6. Hent PRONOM-signaturer
    _say("Henter PRONOM-signaturer (sf -update)...")
    try:
        subprocess.run([str(sf_path), "-update"],
                       capture_output=True, timeout=180, check=True)
    except subprocess.CalledProcessError as exc:
        # Ikke fatal — sf kommer ofte med default.sig bundlet
        _say(f"  Advarsel: sf -update returnerte {exc.returncode}")
    except subprocess.TimeoutExpired:
        _say("  Advarsel: sf -update timeout — bruker innebygde signaturer")

    # 7. Rydd opp
    shutil.rmtree(tmp_dir, ignore_errors=True)

    # 8. Lagre i config
    try:
        from settings import set_config
        set_config("sf_executable", str(sf_path))
    except Exception:
        pass

    _say(f"Ferdig — sf installert til {sf_path}")
    return sf_path


def update_signatures(progress: ProgressCb = None) -> bool:
    """Kjør sf -update for å oppdatere PRONOM-signaturer. Returnerer True ved suksess."""
    try:
        from settings import get_config
        sf = get_config("sf_executable", "")
    except Exception:
        return False
    if not sf or not Path(sf).exists():
        return False
    if progress:
        try: progress("Oppdaterer PRONOM-signaturer...")
        except Exception: pass
    try:
        subprocess.run([sf, "-update"], capture_output=True,
                       timeout=180, check=True)
        return True
    except Exception:
        return False
