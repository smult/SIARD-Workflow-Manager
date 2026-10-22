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


class LobNameAllocator:
    """
    Ledige filnavnstammer per LOB-mappe for NYE LOB-filer.

    Bakgrunn (HK-Data PPT, okt. 2026): inline-dokumenter ble skrevet ut som
    ``record1.rtf``, ``record2.rtf`` … i en ``lob2``-mappe der originalen
    allerede hadde ``record11.txt`` osv. for andre rader. Begge ble konvertert
    til ``record11.bin`` — det ene dokumentet overskrev det andre, og 5 623
    rader pekte på feil dokument. Konvertering og XML-oppdatering knytter
    filene sammen på STAMMEN (alt før første punktum), så en ny fil må få en
    stamme som ikke finnes i mappa fra før — uansett endelse.

    ``list_names(lob_dir)`` gir navnene som allerede finnes i mappa (disk eller
    ZIP). Ønsket stamme brukes når den er ledig; ellers neste ledige nummer over
    det høyeste i mappa, med samme prefiks. Trådsikker.
    """

    _NUM_RE = re.compile(r"^([A-Za-z_]*?)(\d+)$")

    def __init__(self, list_names):
        import threading
        self._list = list_names
        self._used: dict = {}
        self._max: dict = {}
        self._lock = threading.Lock()
        self.collisions = 0

    def _load(self, lob_dir: str) -> None:
        used: set = set()
        mx = 0
        try:
            names = list(self._list(lob_dir) or [])
        except Exception:
            names = []
        for name in names:
            self._mark(used, name.split(".", 1)[0])
            m = self._NUM_RE.match(name.split(".", 1)[0])
            if m:
                mx = max(mx, int(m.group(2)))
        self._used[lob_dir] = used
        self._max[lob_dir] = mx

    @staticmethod
    def _mark(used: set, stem: str) -> None:
        used.add(stem.lower())
        alias = dbptk_stem(stem)                   # rec11 ↔ record11 (Standardiser)
        if alias:
            used.add(alias.lower())

    def take(self, lob_dir: str, stem: str) -> str:
        """Ledig stamme for en ny fil i ``lob_dir`` — ``stem`` hvis den er fri."""
        with self._lock:
            if lob_dir not in self._used:
                self._load(lob_dir)
            used = self._used[lob_dir]
            m = self._NUM_RE.match(stem)
            alias = (dbptk_stem(stem) or stem).lower()
            if stem.lower() not in used and alias not in used:
                self._mark(used, stem)
                if m:
                    self._max[lob_dir] = max(self._max[lob_dir], int(m.group(2)))
                return stem
            self.collisions += 1
            prefix = m.group(1) if m else "record"
            width = len(m.group(2)) if (m and m.group(2).startswith("0")) else 0
            n = self._max[lob_dir] + 1
            while True:
                cand = f"{prefix}{n:0{width}d}" if width else f"{prefix}{n}"
                if cand.lower() not in used and (dbptk_stem(cand) or cand).lower() not in used:
                    break
                n += 1
            self._mark(used, cand)
            self._max[lob_dir] = n
            return cand
