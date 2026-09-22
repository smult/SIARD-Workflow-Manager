"""siard_workflow/core/anonymize/kommune_names.py

Anonymisering av norske kommunenavn: «Marnardal» → «Fiktivdal».

Kommunenavnet identifiserer hvor et uttrekk kommer fra og hvem det handler
om (f.eks. «Marnardal legesenter», «Marnardal kommune» som arbeidsgiver,
poststed «MARNARDAL»). Alle kjente kommunenavn 1990–2026 (SSB Klass 131,
se kommune_data.py) erstattes derfor i ALLE tekstfelt — også i kolonner som
ikke er klassifisert som personopplysning — med «Fiktiv» + stedsnavn-endelsen
fra originalen (dal/nes/vik/sund/…), eller «Fiktivby» når endelsen ikke er
en kjent stedsnavn-endelse (Oslo, Bergen, samiske navn).

Regler
======
* Hele ord, bokstav-grenser (æøå inkludert); valgfri genitiv-s beholdes
  («Marnardals kommune» → «Fiktivdals kommune»). Sammensetninger uten grense
  («Marnardalsveien») røres ikke.
* Bokstavform bevares: MARNARDAL → FIKTIVDAL, marnardal → fiktivdal.
* TVETYDIGE navn — også vanlige ord/etternavn (Berg, Lund, Moss, Hole, Sande,
  Sund, Vang, Lier, Strand, Vik, Time, Sola, Fjell, Eid, Ås, Os, Bø, Ål …
  og alle på ≤ 3 bokstaver) — erstattes bare når de står foran «kommune»
  eller utgjør hele celleverdien. Ellers ville «Berg» som etternavn i
  fritekst og «Time» i engelsk tekst bli ødelagt.
* Kjøres FØR navnespenn i fritekst, slik at fiktive etternavn (Berg, Lund,
  Strand …) fra navnegeneratoren aldri treffes.
"""

from __future__ import annotations

import re

from .kommune_data import KOMMUNE_NAMES

FIKTIV_PREFIX = "Fiktiv"
FIKTIV_FALLBACK = "Fiktivby"

# Stedsnavn-endelser (lengste først). Endelsen tas fra siste ord i navnet.
_PLACE_SUFFIXES = (
    "fjorden", "strand", "dalen", "fjord", "heim", "stad", "sand", "foss",
    "bygd", "fjell", "haug", "hamn", "havn", "holm", "lund", "mark", "vatn",
    "vann", "skog", "berg", "land", "sund", "ness", "vang", "anger", "øya",
    "øyar", "dal", "nes", "vik", "eid", "vær", "våg", "sjø", "hus", "mo",
    "bu", "by", "øy", "ås", "bø", "ør",
)

# Navn som også er vanlige ord/etternavn — krever kontekst («… kommune»)
# eller hel celle. Alle navn på ≤ 3 bokstaver legges til automatisk.
_AMBIGUOUS_EXTRA = frozenset({
    "Berg", "Lund", "Moss", "Hole", "Sande", "Sund", "Vang", "Lier", "Strand",
    "Vik", "Time", "Sola", "Fjell", "Eid", "Ås", "Os", "Bø", "Ål", "Sel", "Fet",
    "Hol", "Lom", "Nes", "Hå", "Hof", "Gran", "Vega", "Rana", "Aure", "Nome",
    "Kvam", "Voss", "Stange", "Alta", "Sula", "Tana", "Leka", "Rissa", "Selje",
    "Luster", "Selje", "Grane", "Marker", "Flå", "Gol", "Re", "Ski",
})

AMBIGUOUS: frozenset[str] = frozenset(
    n for n in KOMMUNE_NAMES if len(n) <= 3) | _AMBIGUOUS_EXTRA

_LETTER = "A-Za-zÆØÅæøåÄÖÜäöüÉéÁáŠšŽžŋŊđĐčČ"
_NAME_ALT = "|".join(re.escape(n) for n in KOMMUNE_NAMES)   # lengste først
KOMMUNE_RE = re.compile(
    rf"(?<![{_LETTER}\-])(?P<name>{_NAME_ALT})(?P<gen>s)?(?![{_LETTER}])",
    re.IGNORECASE,
)
_KOMMUNE_LOOKUP = {n.lower(): n for n in KOMMUNE_NAMES}
_KOMMUNE_FOLLOW_RE = re.compile(r"^\s+kommune", re.IGNORECASE)


def is_kommune_name(value: str) -> bool:
    """True hvis hele verdien (trimmet) er et kjent kommunenavn."""
    return (value or "").strip().lower() in _KOMMUNE_LOOKUP


def fake_kommune(name: str) -> str:
    """«Fiktiv» + stedsnavn-endelse fra siste ord i navnet, ellers «Fiktivby».
    Bokstavformen (ALL CAPS / små) følger originalen."""
    raw = (name or "").strip()
    last = re.split(r"[\s\-]+", raw)[-1].lower() if raw else ""
    fake = FIKTIV_FALLBACK
    for suf in _PLACE_SUFFIXES:
        if last.endswith(suf) and len(last) >= len(suf):
            fake = FIKTIV_PREFIX + suf
            break
    letters = [c for c in raw if c.isalpha()]
    if letters and all(c.isupper() for c in letters):
        return fake.upper()
    if letters and all(c.islower() for c in letters):
        return fake.lower()
    return fake


def replace_kommune_names(text: str, *, whole_cell: bool = False) -> "tuple[str, int]":
    """
    Erstatt kommunenavn i `text`. Returnerer (ny_tekst, antall).

    whole_cell=True: teksten er én celleverdi — et tvetydig navn erstattes også
    når det utgjør hele verdien (poststed «BERG», kommunekolonne «Time»).
    """
    if not text:
        return text, 0
    stripped = text.strip()
    if whole_cell and stripped.lower() in _KOMMUNE_LOOKUP:
        lead = text[:len(text) - len(text.lstrip())]
        trail = text[len(text.rstrip()):]
        return lead + fake_kommune(stripped) + trail, 1

    n = 0

    def _sub(m: re.Match) -> str:
        nonlocal n
        name = m.group("name")
        canon = _KOMMUNE_LOOKUP.get(name.lower(), name)
        if canon in AMBIGUOUS:
            # Krever «… kommune» rett etter
            if not _KOMMUNE_FOLLOW_RE.match(text[m.end():]):
                return m.group(0)
        n += 1
        return fake_kommune(name) + (m.group("gen") or "")

    return KOMMUNE_RE.sub(_sub, text), n
