"""app_paths.py

Stier som avhenger av om programmet kjøres fra kildekode eller som ferdig
pakket program (PyInstaller, `SIARDManager.exe`).

  bundle_dir()      skrivebeskyttede ressurser som følger med programmet
                    (siard_workflow/sources, config_example.json …).
                    Kildekode: programmappa. Pakket: _internal-mappa (sys._MEIPASS).
  data_dir()        brukerdata som programmet skriver: config.json, dias.json,
                    siard_manager.log, cacher. Kildekode: programmappa (som før).
                    Pakket: mappa der SIARDManager.exe ligger (portabel), eller
                    %LOCALAPPDATA%\\SIARDManager\\data hvis den ikke er skrivbar
                    (f.eks. under Program Files).
  components_dir()  installasjonsfiler for store eksterne programmer som kan
                    følge med pakken (LibreOffice, Ghostscript, Siegfried) —
                    `komponenter/` ved siden av SIARDManager.exe.

Kun standardbiblioteket — importeres av main.py før noe annet.
"""
from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path

FROZEN: bool = bool(getattr(sys, "frozen", False))
_SOURCE_ROOT = Path(__file__).resolve().parent
COMPONENTS_DIRNAME = "komponenter"


def bundle_dir() -> Path:
    if FROZEN:
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).parent))
    return _SOURCE_ROOT


def install_dir() -> Path:
    """Mappa programmet er lagt i (der SIARDManager.exe / main.py ligger)."""
    return Path(sys.executable).resolve().parent if FROZEN else _SOURCE_ROOT


def _writable(d: Path) -> bool:
    try:
        d.mkdir(parents=True, exist_ok=True)
        probe = d / ".skrivetest"
        probe.write_text("x", "utf-8")
        probe.unlink()
        return True
    except Exception:
        return False


@lru_cache(maxsize=1)
def data_dir() -> Path:
    d = install_dir()
    if not FROZEN or _writable(d):
        return d
    alt = Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "SIARDManager" / "data"
    alt.mkdir(parents=True, exist_ok=True)
    return alt


def components_dir() -> Path:
    return install_dir() / COMPONENTS_DIRNAME


def resource(*parts: str) -> Path:
    """Sti til en skrivebeskyttet ressurs som følger med programmet."""
    return bundle_dir().joinpath(*parts)
