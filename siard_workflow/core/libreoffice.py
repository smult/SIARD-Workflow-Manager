"""
siard_workflow/core/libreoffice.py  —  Felles LibreOffice-deteksjon

Samler all logikk for å finne LibreOffice/OpenOffice (soffice) på tvers av
plattformer, slik at operasjoner og GUI bruker samme robuste kode.

Offentlig API:
  find_libreoffice(hint="soffice")  -> str | None
      Søk gjennom hint, PATH og vanlige installasjonssteder.

  verify_libreoffice_path(path)     -> str | None
      Verifiser at en bruker-valgt sti/mappe er en LibreOffice program-mappe
      (eller selve soffice-binæren) og returner stien til soffice. None hvis
      ikke gyldig.
"""
from __future__ import annotations

import os
import shutil
import sys

# Navn på soffice-binæren per plattform. .bin er den ekte binæren på
# Windows/Linux; .exe/.com er launchere. soffice (uten endelse) på Unix.
_SOFFICE_NAMES = ("soffice.exe", "soffice", "soffice.bin")

# Undermappenavn vi prøver under Program Files o.l.
_INSTALL_SUBDIRS = ("LibreOffice", "LibreOffice 7", "LibreOffice 24", "OpenOffice")


def _program_dir_candidates() -> list[str]:
    """Baser i Program Files / LOCALAPPDATA der LibreOffice typisk ligger."""
    bases = []
    for env in ("PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"):
        v = os.environ.get(env)
        if v:
            bases.append(v)
    # Fallback til kjente standardstier dersom miljøvariabler mangler
    for hard in (r"C:\Program Files", r"C:\Program Files (x86)"):
        if hard not in bases:
            bases.append(hard)
    return bases


def find_libreoffice(hint: str = "soffice") -> str | None:
    """
    Finn soffice-binæren. Søkerekkefølge:
      1. `hint` — enten en eksekverbar i PATH eller en eksakt fil/mappe-sti
      2. Windows: kjente installasjonssteder (Program Files, LOCALAPPDATA)
                   + skann etter mapper med "libre"/"openoffice" i navnet
      3. macOS:   /Applications/{LibreOffice,OpenOffice}.app/...
      4. PATH:    soffice / libreoffice / libreoffice7 / libreoffice24
    Returnerer absolutt sti (eller PATH-navn) til soffice, ellers None.
    """
    if hint:
        # Eksekverbar i PATH?
        if shutil.which(hint):
            return hint
        # Eksakt fil eller mappe valgt av bruker?
        verified = verify_libreoffice_path(hint)
        if verified:
            return verified

    if sys.platform == "win32":
        candidates: list[str] = []
        for base in _program_dir_candidates():
            for sub in _INSTALL_SUBDIRS:
                candidates.append(os.path.join(base, sub, "program", "soffice.exe"))
        # Skann Program Files for mapper med "libre"/"openoffice" i navnet
        for base in _program_dir_candidates():
            if os.path.isdir(base):
                try:
                    for entry in os.listdir(base):
                        low = entry.lower()
                        if "libre" in low or "openoffice" in low:
                            candidates.append(
                                os.path.join(base, entry, "program", "soffice.exe"))
                except OSError:
                    pass
        for c in candidates:
            if os.path.isfile(c):
                return c

    if sys.platform == "darwin":
        for p in ("/Applications/LibreOffice.app/Contents/MacOS/soffice",
                  "/Applications/OpenOffice.app/Contents/MacOS/soffice"):
            if os.path.isfile(p):
                return p

    for name in ("soffice", "libreoffice", "libreoffice7", "libreoffice24"):
        found = shutil.which(name)
        if found:
            return found

    return None


def _soffice_in_dir(directory: str) -> str | None:
    """Returner soffice-binæren i `directory` dersom den finnes."""
    for name in _SOFFICE_NAMES:
        cand = os.path.join(directory, name)
        if os.path.isfile(cand):
            return cand
    return None


def verify_libreoffice_path(path: str) -> str | None:
    """
    Verifiser at `path` peker på en gyldig LibreOffice-installasjon og
    returner stien til soffice-binæren. Aksepterer at brukeren har valgt:

      • selve soffice-binæren (soffice.exe / soffice / soffice.bin)
      • program-mappen (inneholder soffice direkte)
      • installasjonsroten (inneholder undermappen «program/»)
      • en macOS .app-pakke (inneholder Contents/MacOS/soffice)

    Returnerer None dersom ingen soffice-binær finnes på noen av disse stedene.
    """
    if not path:
        return None
    path = os.path.expanduser(path.strip().strip('"'))

    # 1. Brukeren valgte selve binæren
    if os.path.isfile(path):
        base = os.path.basename(path).lower()
        if base in _SOFFICE_NAMES:
            return path
        return None

    if not os.path.isdir(path):
        return None

    # 2. Program-mappen direkte
    found = _soffice_in_dir(path)
    if found:
        return found

    # 3. Installasjonsrot med «program/»-undermappe
    found = _soffice_in_dir(os.path.join(path, "program"))
    if found:
        return found

    # 4. macOS .app-pakke
    found = _soffice_in_dir(os.path.join(path, "Contents", "MacOS"))
    if found:
        return found

    return None
