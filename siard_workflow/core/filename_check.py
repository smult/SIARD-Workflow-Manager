"""siard_workflow/core/filename_check.py

Ødelagte filnavn («mojibake») på SIARD-filer.

Et filnavn med «Å» lagret som UTF-8 (bytene C3 85) og lest som Latin-1 blir
«Ã» + U+0085 — et usynlig kontrolltegn. Det skjer typisk når en zip-fil uten
UTF-8-flagg pakkes ut, eller ved overføring mellom systemer. Navnet arves av
alt som lages fra fila (ny SIARD, PREMIS, CSV-er, rapporter), og kontrolltegnet
kan ikke skrives med Windows-tegnsettet (cp1252) — SHA-256-fila feilet slik.
"""
from __future__ import annotations

import re

# C1-kontrolltegn (U+0080–U+009F): finnes aldri i et ekte filnavn
_C1 = re.compile("[\x80-\x9f]")


def has_control_chars(name: str) -> bool:
    """True hvis navnet har kontrolltegn (C0 eller C1)."""
    return any(ord(ch) < 0x20 for ch in name) or bool(_C1.search(name))


def repair_mojibake(name: str) -> "str | None":
    """
    Riktig navn for et UTF-8-navn som er lest som Latin-1/Windows-1252
    («1504_Ã\\x85lesund» / «1504_Ã…lesund» → «1504_Ålesund»), ellers None.

    Et ekte navn som «Ålesund» lar seg ikke tolke som UTF-8 på denne måten,
    så det blir aldri «rettet».
    """
    if "Ã" not in name and "Â" not in name and not _C1.search(name):
        return None
    for enc in ("latin-1", "cp1252"):
        try:
            fixed = name.encode(enc).decode("utf-8")
        except UnicodeError:
            continue
        if fixed != name and not has_control_chars(fixed):
            return fixed
    return None
