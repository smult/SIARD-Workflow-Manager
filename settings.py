"""
settings.py  —  Persistente brukerinnstillinger for SIARD Workflow Manager

To separate filer:
  config.json  — globale innstillinger i program-mappen (temp, AV, workers, batch)
  settings.json — operasjonsparametre og profiler i brukerprofilen

config.json-plassering: samme mappe som settings.py (program-mappen)
settings.json-plassering:
  Windows:  %APPDATA%/SIARDManager/settings.json
  Linux/Mac: ~/.config/SIARDManager/settings.json
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# ── Program-lokal konfigurasjon (config.json) ────────────────────────────────

_PROGRAM_DIR  = Path(__file__).parent
_CONFIG_FILE  = _PROGRAM_DIR / "config.json"

_CONFIG_DEFAULTS: dict = {
    "global_temp_dir":  "",          # tom = auto-velg ved filvalg
    "av_executable":    "",          # sti til antivirus-program
    "av_args":          [],          # tom = auto-sett basert på program
    "av_infected_rc":   1,           # returkode som betyr funn
    "av_timeout":       300,         # sekunder maks skanning
    "max_workers":      4,           # parallelle LO-instanser
    "lo_batch_size":    50,          # filer per batch
    "lo_timeout":       300,         # sekunder per batch
    "lo_convertible": [              # filformater som konverteres til PDF/A
        "doc", "docx", "dot", "dotx",
        "odt", "ott", "odg",
        "rtf",
        "wpd", "wp", "wp5", "wp6", "wps",
        "wri", "lwp", "sxw", "sdw",
    ],
    "rename_only": [                 # filformater som beholdes i originalformat
        "csv",                       # ren tekst/tabellformat — ikke konverter
        "tiff", "tif", "jpg", "jpeg", "png", "gif", "bmp",
        "pptx", "potx", "odp",
        "xlsx", "xltx", "ods",
        "mp3", "wav", "flac", "ogg",
        "mpg", "mpeg", "mp4", "m4v", "avi",
        "sosi", "gml", "ifc", "warc",
        "msg", "eml",
        "jp2", "jpe", "webp", "svg",
        "exe", "7z", "rar",
    ],
    "lo_upgrade": {                  # gamle formater som oppgraderes til nyere
        "xls":  "xlsx",   "xlt":  "xlsx",
        "ppt":  "pptx",   "pot":  "pptx",
    },
    "pdfa_version":         "PDF/A-2u (ISO 19005-2, level U)",  # standard PDF/A-versjon
    "disk_overhead_factor": 2.0,   # multiplikator for estimert temp-diskbehov
}


def load_config() -> dict:
    """Last globale innstillinger fra config.json i program-mappen."""
    try:
        with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
            stored = json.load(f)
        merged = dict(_CONFIG_DEFAULTS)
        for k, v in stored.items():
            if k in merged:
                merged[k] = v
        return merged
    except Exception:
        return dict(_CONFIG_DEFAULTS)


def save_config(data: dict) -> None:
    """Skriv globale innstillinger til config.json i program-mappen."""
    try:
        # Behold eksisterende verdier, overstyr med nye
        current = load_config()
        current.update({k: v for k, v in data.items() if k in _CONFIG_DEFAULTS})
        with open(_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def get_config(key: str, default=None):
    """Hent én global konfigurasjonsverdi."""
    return load_config().get(key, default if default is not None
                              else _CONFIG_DEFAULTS.get(key))


def set_config(key: str, value) -> None:
    """Sett og lagre én global konfigurasjonsverdi."""
    if key in _CONFIG_DEFAULTS:
        save_config({key: value})


# ── Bruker-lokal settings (settings.json i APPDATA) ─────────────────────────

def _settings_path() -> Path:
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home()))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    d = base / "SIARDManager"
    d.mkdir(parents=True, exist_ok=True)
    return d / "settings.json"


_SETTINGS_FILE = _settings_path()


def load_settings() -> dict:
    """Last innstillinger fra disk. Returnerer tom dict ved feil."""
    try:
        with open(_SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_settings(data: dict) -> None:
    """Skriv innstillinger til disk."""
    try:
        with open(_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def get_op_params(operation_id: str, defaults: dict) -> dict:
    """
    Hent lagrede parametre for en operasjon.
    Manglende nøkler fylles fra defaults.
    """
    settings = load_settings()
    stored   = settings.get("op_params", {}).get(operation_id, {})
    # Start med defaults, overstyr med det som er lagret
    merged = dict(defaults)
    for k, v in stored.items():
        if k in merged:   # ikke ta inn ukjente nøkler
            merged[k] = v
    return merged


def save_op_params(operation_id: str, params: dict) -> None:
    """Lagre parametre for en operasjon."""
    settings = load_settings()
    if "op_params" not in settings:
        settings["op_params"] = {}
    settings["op_params"][operation_id] = dict(params)
    save_settings(settings)


def get_profiles() -> dict[str, list]:
    """Hent lagrede workflow-profiler. Returnerer {navn: [op_dicts]}."""
    return load_settings().get("profiles", {})


def save_profile_ops(name: str, ops_json: list) -> None:
    """Lagre en workflow-profil som liste av operasjon-dicts."""
    settings = load_settings()
    if "profiles" not in settings:
        settings["profiles"] = {}
    settings["profiles"][name] = ops_json
    save_settings(settings)


def delete_profile(name: str) -> None:
    """Slett en lagret profil."""
    settings = load_settings()
    if "profiles" in settings and name in settings["profiles"]:
        del settings["profiles"][name]
        save_settings(settings)


def get_pref(key: str, default=None):
    """Hent en enkel preferanse-verdi."""
    return load_settings().get("prefs", {}).get(key, default)


def save_pref(key: str, value) -> None:
    """Lagre en enkel preferanse-verdi."""
    settings = load_settings()
    if "prefs" not in settings:
        settings["prefs"] = {}
    settings["prefs"][key] = value
    save_settings(settings)
