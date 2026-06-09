"""
siard_workflow/core/anonymize/fake_generators.py

Deterministiske, syntetiske erstatningsverdier for PII. Ren Python, ingen
eksterne avhengigheter (PyInstaller-vennlig).

Determinisme er kjernen: samme (type, original) → samme fake overalt i arkivet,
slik at fremmednøkler/koblinger fortsatt stemmer etter anonymisering. Dette
oppnås med en seedet sha256-hash (`stable_index`) som velger fra faste lister.

Fødselsnummer genereres i det norske SYNTETISKE området (måned + 80) slik at de
er mod-11-gyldige, men aldri kan kollidere med ekte personer.

Alle navn/gateadresser holdes bevisst ASCII for å unngå XML-/SIARD-escaping-
kanttilfeller ved tilbakeskriving.
"""
from __future__ import annotations

import hashlib
import threading

from .pii_detect import PiiType, fnr_control_digits, _digits

_SALT = "siard-anon-v1"


def stable_index(original: str, salt: str, modulo: int) -> int:
    """Deterministisk indeks i [0, modulo) fra (salt, original)."""
    if modulo <= 0:
        return 0
    h = hashlib.sha256(f"{_SALT}\x00{salt}\x00{original}".encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") % modulo


# ── Datasett (ASCII-trygge) ───────────────────────────────────────────────────

_FIRST_NAMES = [
    "Jan", "Per", "Bjorn", "Ole", "Lars", "Kjell", "Knut", "Svein", "Geir",
    "Arne", "Tor", "Odd", "Hans", "Terje", "Morten", "Rune", "Trond", "Bjarne",
    "Anne", "Inger", "Kari", "Marit", "Ingrid", "Liv", "Eva", "Berit", "Hilde",
    "Bente", "Anita", "Nina", "Marianne", "Solveig", "Randi", "Tone", "Astrid",
    "Sigrid", "Hanne", "Else", "Gerd", "Turid",
]

_LAST_NAMES = [
    "Hansen", "Johansen", "Olsen", "Larsen", "Andersen", "Pedersen", "Nilsen",
    "Kristiansen", "Jensen", "Karlsen", "Johnsen", "Pettersen", "Eriksen",
    "Berg", "Haugen", "Hagen", "Johannessen", "Andreassen", "Jacobsen",
    "Dahl", "Jorgensen", "Halvorsen", "Lund", "Moen", "Iversen", "Strand",
    "Solberg", "Bakke", "Moe", "Lie", "Holm", "Aas", "Myhre", "Nguyen",
]

# Standard Lorem Ipsum-ordforråd
_LOREM_WORDS = (
    "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod "
    "tempor incididunt ut labore et dolore magna aliqua enim ad minim veniam "
    "quis nostrud exercitation ullamco laboris nisi aliquip ex ea commodo "
    "consequat duis aute irure in reprehenderit voluptate velit esse cillum "
    "fugiat nulla pariatur excepteur sint occaecat cupidatat non proident"
).split()


def lorem_ipsum(n_words: int = 50, seed: str = "lorem") -> str:
    """Deterministisk Lorem Ipsum-tekst på ca. n_words ord."""
    n_words = max(1, int(n_words))
    words = [_LOREM_WORDS[stable_index(seed, f"w{i}", len(_LOREM_WORDS))]
             for i in range(n_words)]
    text = " ".join(words)
    return text[:1].upper() + text[1:] + "."


# ── Per-type generatorer ──────────────────────────────────────────────────────

def fake_fnr(original: str) -> str:
    """Syntetisk, mod-11-gyldig fødselsnummer (måned + 80). Deterministisk.

    Beholder fødselsdag og -måned fra originalen der det er mulig (måned + 80
    gir det norske syntetiske området), år beholdes; individnummer utledes av
    hash og justeres til kontrollsifrene blir gyldige.
    """
    d = _digits(original)
    # Ikke et fullt 11-sifret fnr (f.eks. 5-sifret «PersonNr» = individnr+kontroll)
    # → behold lengden, generer deterministiske sifre. Unngår brudd på CHAR(n).
    if len(d) != 11:
        if not d:
            return original
        return "".join(str(stable_index(original, f"num{i}", 10))
                       for i in range(len(d)))
    day = month = year = None
    if len(d) >= 6:
        day, month, year = int(d[0:2]), int(d[2:4]), int(d[4:6])
        # Normaliser D-nummer (dag + 40) og allerede-syntetisk (måned + 80/40)
        if day > 40:
            day -= 40
        if month > 80:
            month -= 80
        elif month > 40:
            month -= 40
    if not (day and 1 <= day <= 31 and month and 1 <= month <= 12):
        day   = stable_index(original, "fnr-day", 28) + 1
        month = stable_index(original, "fnr-month", 12) + 1
        year  = stable_index(original, "fnr-year", 100)
    if day > 28:
        day = 28                       # trygt for alle måneder (syntetisk uansett)
    syn_month = month + 80             # syntetisk område
    base_ind = stable_index(original, "fnr-ind", 1000)
    for attempt in range(1000):
        ind = (base_ind + attempt) % 1000
        d9 = f"{day:02d}{syn_month:02d}{year:02d}{ind:03d}"
        ctrl = fnr_control_digits(d9)
        if ctrl:
            k1, k2 = ctrl
            return f"{d9}{k1}{k2}"
    return f"{day:02d}{syn_month:02d}{year:02d}00000"   # praktisk talt uoppnåelig


def fake_first_name(original: str) -> str:
    return _FIRST_NAMES[stable_index(original, "first", len(_FIRST_NAMES))]


def fake_last_name(original: str) -> str:
    return _LAST_NAMES[stable_index(original, "last", len(_LAST_NAMES))]


def fake_full_name(original: str) -> str:
    """Fiktivt fullt navn. Bevarer «Etternavn, Fornavn»-format hvis originalen
    bruker komma."""
    first = _FIRST_NAMES[stable_index(original, "fn-first", len(_FIRST_NAMES))]
    last  = _LAST_NAMES[stable_index(original, "fn-last", len(_LAST_NAMES))]
    if "," in (original or ""):
        return f"{last}, {first}"
    return f"{first} {last}"


def fake_phone(original: str) -> str:
    """Fiktivt norsk 8-sifret telefonnummer. Bevarer ledende +47 hvis originalen
    har det."""
    first = "4" if stable_index(original, "ph-pre", 2) == 0 else "9"
    rest = "".join(str(stable_index(original, f"ph{i}", 10)) for i in range(7))
    number = first + rest
    src = (original or "").strip()
    if src.startswith("+47") or src.startswith("0047"):
        return "+47" + number
    return number


# Faste, tydelig fiktive verdier (etter ønske fra KDRS):
#   adresse → «Fiktivveien <nr>», postnr → «9999», sted → «Fiktivby»,
#   e-post  → «<fornavn>.<etternavn>@fiktivadresse.no»
_FAKE_POSTNR = "9999"
_FAKE_CITY   = "Fiktivby"
_FAKE_STREET = "Fiktivveien"
_FAKE_EMAIL_DOMAIN = "fiktivadresse.no"


def fake_email(original: str) -> str:
    first = fake_first_name(original).lower()
    last  = fake_last_name(original).lower()
    return f"{first}.{last}@{_FAKE_EMAIL_DOMAIN}"


def fake_address(original: str) -> str:
    """Alle adresser → «Fiktivveien <nr>» (nr deterministisk av originalen)."""
    num = stable_index(original, "street-no", 98) + 1
    return f"{_FAKE_STREET} {num}"


def fake_postnr(original: str) -> str:
    return _FAKE_POSTNR


def fake_city(original: str) -> str:
    return _FAKE_CITY


def fake_postnr_sted(original: str) -> str:
    """For «0150 OSLO»-spenn i fritekst: returner «9999 Fiktivby»."""
    return f"{_FAKE_POSTNR} {_FAKE_CITY}"


def fake_postnr_value(original: str) -> str:
    """Postnr-erstatning: «0150» → «9999»; «0150 OSLO» (fritekst) → «9999 Fiktivby»."""
    if any(c.isalpha() for c in (original or "")):
        return fake_postnr_sted(original)
    return fake_postnr(original)


_DISPATCH = {
    PiiType.FNR:        fake_fnr,
    PiiType.FIRST_NAME: fake_first_name,
    PiiType.LAST_NAME:  fake_last_name,
    PiiType.FULL_NAME:  fake_full_name,
    PiiType.PHONE:      fake_phone,
    PiiType.EMAIL:      fake_email,
    PiiType.ADDRESS:    fake_address,
    PiiType.POSTNR:     fake_postnr_value,  # 4-sifret kolonne ELLER postnr+sted-spenn
    PiiType.CITY:       fake_city,
}


def fake_value(pii_type: PiiType, original: str) -> str:
    """Deterministisk fiktiv verdi for en gitt PII-type. Ukjent type → uendret."""
    gen = _DISPATCH.get(pii_type)
    if gen is None:
        return original
    return gen(original or "")


# ── Mapping-lager (deler én instans for hele kjøringen) ───────────────────────

class MappingStore:
    """
    Trådsikker, deterministisk mapping original → fake. Cacher resultater slik at
    rapporten kan vise alle erstatninger, og garanterer at samme original alltid
    gir samme fake (selv om generatorene allerede er deterministiske).

    KRITISK: én instans må deles på tvers av ALLE tabeller i en kjøring for å
    bevare referanseintegritet.
    """

    def __init__(self):
        self._map: dict[tuple[str, str], str] = {}
        self._lock = threading.Lock()

    def map(self, pii_type: PiiType, original: str) -> str:
        if original is None:
            return original
        key = (pii_type.value, original)
        with self._lock:
            cached = self._map.get(key)
            if cached is not None:
                return cached
            fake = fake_value(pii_type, original)
            # Garanter at den fiktive verdien aldri er lik originalen (unngå at
            # et navn tilfeldig mappes til seg selv). Re-roll deterministisk ved
            # å forstyrre hash-seedet med nullbytes — som er både ikke-siffer og
            # ikke-bokstav, slik at generatorenes format-/lengde-logikk (fnr,
            # postnr) ikke påvirkes. Samme original → samme re-roll.
            attempt = 0
            while fake == original and attempt < 8:
                attempt += 1
                fake = fake_value(pii_type, original + "\x00" * attempt)
            self._map[key] = fake
            return fake

    def items(self) -> "list[tuple[str, str, str]]":
        """(pii_type, original, fake) for rapportering."""
        with self._lock:
            return [(t, o, f) for (t, o), f in self._map.items()]

    def __len__(self) -> int:
        with self._lock:
            return len(self._map)
