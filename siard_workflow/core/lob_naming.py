"""siard_workflow/core/lob_naming.py

Navngivning av eksterne LOB-filer — felles for hex_extract, BlobConvert og
Standardiser filendelser.

DBPTK-importen bryr seg ikke om basenavnet (kun .bin/.txt-endelsen), men
DBPTK-VALIDATOREN (P_4.2-3, SIARDStructureValidator) krever at hver LOB-fil
matcher ``record[0-9]+.*`` og ligger i ``lob[0-9]+/``. SCFC skriver ``recN``,
SIARD Suite ``recN``, og SIARD Manager skrev tidligere ``xrecN``/``LOBnnnn``
— alle feiler validatoren.

Innstillingen ``dbptk_lob_names`` (config.json, default på) styrer om nye
LOB-filer navngis ``recordN`` og om Standardiser filendelser omdøper
eksisterende filer til dette mønsteret.
"""
from __future__ import annotations

import re

# Det DBPTK-validatoren krever (P_4.2-3): basenavn må starte med recordN
DBPTK_LOB_NAME_RE = re.compile(r"record[0-9]+.*")

# Kjente stammer som kan mappes entydig til recordN:
#   rec12, xrec12, record0012, LOB0012, lob12
_KNOWN_STEM_RE = re.compile(r"^(?:x?rec|record|lob)0*(\d+)$", re.IGNORECASE)


def dbptk_names_enabled() -> bool:
    """Les 'dbptk_lob_names' fra config.json (default True)."""
    try:
        from settings import get_config
        return bool(get_config("dbptk_lob_names", True))
    except Exception:
        return True


def lob_file_stem(n: int, legacy_prefix: str = "rec") -> str:
    """
    Stamme for ny LOB-fil nr. ``n``: ``recordN`` når dbptk_lob_names er på,
    ellers ``<legacy_prefix>N`` (historisk navngivning).
    """
    if dbptk_names_enabled():
        return f"record{n}"
    return f"{legacy_prefix}{n}"


def is_dbptk_lob_name(name: str) -> bool:
    """Sann hvis basenavnet allerede tilfredsstiller validatoren."""
    return DBPTK_LOB_NAME_RE.fullmatch(name) is not None


def dbptk_stem(stem: str) -> str | None:
    """
    Standardisert stamme (``recordN``) for en kjent stamme, eller None hvis
    stammen allerede er på formen eller ikke kan mappes entydig.

    >>> dbptk_stem("rec5"), dbptk_stem("xrec5"), dbptk_stem("LOB0005")
    ('record5', 'record5', 'record5')
    >>> dbptk_stem("record5"), dbptk_stem("dokument_5")
    (None, None)
    """
    m = _KNOWN_STEM_RE.match(stem)
    if not m:
        return None
    new = f"record{int(m.group(1))}"
    return None if new == stem else new


def dbptk_file_name(name: str) -> str | None:
    """
    Nytt filnavn med standardisert stamme og uendret endelse(r), eller None
    hvis navnet ikke skal endres. Stammen er alt frem til første punktum.
    """
    stem, dot, rest = name.partition(".")
    new_stem = dbptk_stem(stem)
    if new_stem is None:
        return None
    return new_stem + dot + rest
