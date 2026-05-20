"""
siard_workflow/core/file_identifier.py
Pluggbar fildeteksjonsmotor — felles API for magic-bytes og Siegfried/PRONOM.

Kontrakt:
    identify(data=None, path=None) -> (ext, mime, is_encrypted)

Bruk:
    from siard_workflow.core.file_identifier import get_identifier
    ident = get_identifier()
    ident.pre_scan(extract_dir)   # valgfritt; tom no-op for magic-bytes
    ext, mime, enc = ident.identify(data=blob_bytes[:65536])

Hvilken backend som brukes styres av config-nøkkelen `use_siegfried`.
Etter at brukeren endrer valget i innstillinger må reset_identifier() kalles
slik at neste kall til get_identifier() laster valgt backend på nytt.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Protocol


class FileIdentifier(Protocol):
    """Felles protokoll for fildeteksjons-backends."""

    name: str

    def identify(self, data: Optional[bytes] = None,
                 path: Optional[Path] = None) -> tuple[str, str, bool]:
        """Returner (ext, mime, is_encrypted). Minst én av data/path må gis."""
        ...

    def pre_scan(self, root: Path,
                 files: Optional[list[Path]] = None,
                 max_workers: Optional[int] = None,
                 progress_cb: Optional[Callable[[int, int], None]] = None
                 ) -> None:
        """
        Valgfritt: forhåndsskann filer for cache.

        Hvis `files` er gitt: skann kun disse stiene (anbefalt — unngår
        unødvendige treff på XML/XSD og metadata-filer).
        Hvis `files` er None: skann hele `root` rekursivt (fallback).

        max_workers: antall parallelle backend-prosesser. None betyr at
            backend-en velger selv (typisk basert på config eller cpu_count).
            Ignoreres av backends som ikke spawner subprocesser (magic-bytes).
        progress_cb(done, total): kalles etter hver fullført batch.
        """
        ...


_active: Optional[FileIdentifier] = None


def get_identifier() -> FileIdentifier:
    """
    Returnerer aktiv backend basert på config (`use_siegfried` + `sf_executable`).
    Hvis Siegfried er valgt men ikke installert, faller den tilbake til
    magic-bytes med en konsoll-advarsel.
    """
    global _active
    if _active is not None:
        return _active

    use_sf = False
    try:
        from settings import get_config
        use_sf = bool(get_config("use_siegfried", False))
    except Exception:
        pass

    if use_sf:
        try:
            from siard_workflow.core.identifiers.siegfried import SiegfriedIdentifier
            sf = SiegfriedIdentifier()
            if sf.is_available():
                _active = sf
                return _active
        except Exception as exc:
            import sys
            print(f"[file_identifier] Siegfried-backend feilet å laste, "
                  f"faller tilbake til magic-bytes: {exc}", file=sys.stderr)

    from siard_workflow.core.identifiers.magic_bytes import MagicIdentifier
    _active = MagicIdentifier()
    return _active


def reset_identifier() -> None:
    """Kalles fra innstillinger-dialogen når valget endres."""
    global _active
    _active = None
