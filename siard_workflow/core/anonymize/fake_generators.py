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

import datetime as _dt

from .pii_detect import (
    PiiType, fnr_control_digits, _digits, fnr_birthdate, fnr_period,
    parse_date, format_date, KEY_TYPES,
)

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

# ── Fødselsdato og dekomponerbart fnr ────────────────────────────────────────
#
# Konsistensprinsipp: det fiktive fnr-et bygges av deler som hver KUN avhenger
# av sin originaldel —
#   datodel      = shift_birthdate(fødselsdato)          (samme funksjon som for
#                                                          fødselsdato-kolonner)
#   individnr    = f(original individnr) i samme århundreserie og med samme
#                  kjønnsparitet
#   kontroll     = beregnes på nytt (mod-11)
# Dermed stemmer en fødselsdato-kolonne i én tabell med datodelen i fnr-et i
# en annen, og et femsifret personnummer kan komponeres (dato + 5 sifre →
# oppslag i felles mapping) når raden også har datoen.
# Måneden i fnr-et markeres med +80 (Skatteetatens syntetiske område) slik at
# ingen ekte person kan treffes; fødselsdato-kolonner viser den forskjøvne
# datoen uten markering.

MAX_SHIFT_DAYS = 365


def shift_birthdate(dt: "_dt.date") -> "_dt.date":
    """Deterministisk forskyvning 1–365 dager (aldri 0), innenfor samme
    fnr-århundreserie (1854–1899 / 1900–1999 / 2000–2039). Avhenger KUN av
    datoen, så alle kolonner som bærer samme dato får samme resultat."""
    iso = dt.isoformat()
    offset = 1 + stable_index(iso, "bd-offset", MAX_SHIFT_DAYS)
    sign = 1 if stable_index(iso, "bd-sign", 2) else -1
    start, end, _ = fnr_period(dt.year)
    lo, hi = _dt.date(start, 1, 1), _dt.date(end, 12, 31)
    cand = dt + _dt.timedelta(days=sign * offset)
    if not (lo <= cand <= hi):
        cand = dt - _dt.timedelta(days=sign * offset)
    if not (lo <= cand <= hi):
        cand = min(max(cand, lo), hi)
        if cand == dt:
            cand = dt + _dt.timedelta(days=1 if dt < hi else -1)
    return cand


def fake_birthdate(original: str) -> str:
    """Forskjøvet fødselsdato i samme format som originalen (ISO m/ evt.
    klokkeslett/Z, dd.mm.yyyy, dd/mm/yyyy, yyyymmdd). Ikke-dato → uendret."""
    core = (original or "").rstrip("\x00")
    parsed = parse_date(core)
    if parsed is None:
        return original
    dt, fmt, rest = parsed
    return format_date(shift_birthdate(dt), fmt, rest)


def _fake_individ(orig_ind: int, year: int, seed: str) -> "list[int]":
    """Kandidat-individnumre (prioritert) i samme århundreserie som `year` og
    med samme kjønnsparitet som originalen."""
    _, _, (lo, hi) = fnr_period(year)
    parity = orig_ind % 2
    cands = [n for n in range(lo, hi + 1) if n % 2 == parity]
    start = stable_index(seed, "fnr-ind", len(cands))
    return cands[start:] + cands[:start]


def fake_fnr(original: str) -> str:
    """Syntetisk, mod-11-gyldig og DEKOMPONERBART fødselsnummer (se over).

    Datodelen = shift_birthdate(fødselsdato fra originalen) med måned + 80;
    individnr i samme århundreserie og kjønnsparitet; kontrollsifre beregnes.
    Ikke-11-sifret input (f.eks. femsifret personnummer uten dato) → se
    fake_pnr5 / deterministiske sifre med samme lengde.
    """
    d = _digits(original)
    if len(d) != 11:
        if not d:
            return original
        if len(d) == 5:
            return fake_pnr5(original)
        return "".join(str(stable_index(original, f"num{i}", 10))
                       for i in range(len(d)))

    bd = fnr_birthdate(d)
    if bd is None:
        # Ugyldig dato i originalen → deterministisk dato i 1900-serien
        year = 1900 + stable_index(original, "fnr-year", 100)
        bd = _dt.date(year, stable_index(original, "fnr-month", 12) + 1,
                      stable_index(original, "fnr-day", 28) + 1)
    new_bd = shift_birthdate(bd)
    orig_ind = int(d[6:9])
    date_part = f"{new_bd.day:02d}{new_bd.month + 80:02d}{new_bd.year % 100:02d}"
    for ind in _fake_individ(orig_ind, new_bd.year, original):
        d9 = f"{date_part}{ind:03d}"
        ctrl = fnr_control_digits(d9)
        if ctrl:
            k1, k2 = ctrl
            return f"{d9}{k1}{k2}"
    return f"{date_part}00000"   # praktisk talt uoppnåelig


def fake_pnr5(original: str) -> str:
    """Femsifret personnummer (individnr + kontroll) UTEN kjent fødselsdato:
    deterministisk fem-til-fem-mapping med bevart kjønnsparitet. Konsistent
    mellom femsifrede kolonner, men kontrollsifrene kan ikke stemme mot en
    ukjent dato — bruk compose via fnr når raden har fødselsdato."""
    d = _digits(original)
    if len(d) != 5:
        return original
    parity = int(d[2]) % 2
    ind = stable_index(original, "pnr5-ind", 500) * 2 + parity      # 0–999, samme paritet
    ctrl = stable_index(original, "pnr5-ctrl", 100)
    return f"{ind:03d}{ctrl:02d}"


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
    PiiType.PNR5:       fake_pnr5,
    PiiType.BIRTHDATE:  fake_birthdate,
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


# ── Case-bevaring for navn ────────────────────────────────────────────────────

# Navnetyper der matching er case-uavhengig: «OLA», «ola» og «Ola» skal mappes til
# SAMME fiktive verdi (samme person), og resultatet skal følge originalens form.
_CASE_TYPES = frozenset({PiiType.FIRST_NAME, PiiType.LAST_NAME, PiiType.FULL_NAME})


def apply_case(original: str, fake: str) -> str:
    """Gi `fake` samme bokstav-form som `original`:
      «OLA NORDMANN» → «PER HANSEN»  (ALL-CAPS)
      «ola nordmann» → «per hansen»  (lowercase)
      «Ola Nordmann» / blandet       → uendret (generatorene gir Title Case)
    """
    letters = [c for c in (original or "") if c.isalpha()]
    if not letters:
        return fake
    if all(c.isupper() for c in letters):
        return fake.upper()
    if all(c.islower() for c in letters):
        return fake.lower()
    return fake


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
        # Injektivitet for nøkkeltyper (fnr/pnr5/e-post): fiktive verdier som
        # allerede er i bruk → to ulike originaler får aldri samme fake.
        self._used: dict[str, set[str]] = {}
        self._lock = threading.Lock()

    def map(self, pii_type: PiiType, original: str) -> str:
        if original is None:
            return original
        # Navn matches case-uavhengig: nøkkelen normaliseres til små bokstaver slik
        # at «OLA»/«ola»/«Ola» gir SAMME fake (bevarer relasjoner). Resultatet får
        # originalens bokstav-form via apply_case().
        is_name = pii_type in _CASE_TYPES
        seed = original.lower() if is_name else original
        key = (pii_type.value, seed)
        with self._lock:
            cached = self._map.get(key)
            if cached is None:
                fake = fake_value(pii_type, seed)
                # Garanter at den fiktive verdien aldri er lik originalen (unngå at
                # et navn tilfeldig mappes til seg selv). Re-roll deterministisk ved
                # å forstyrre hash-seedet med nullbytes — som er både ikke-siffer og
                # ikke-bokstav, slik at generatorenes format-/lengde-logikk (fnr,
                # postnr) ikke påvirkes. Samme original → samme re-roll.
                attempt = 0
                while fake == seed and attempt < 8:
                    attempt += 1
                    fake = fake_value(pii_type, seed + "\x00" * attempt)
                if pii_type in KEY_TYPES:
                    used = self._used.setdefault(pii_type.value, set())
                    # Kollisjon med en annen originals fake → trekk på nytt
                    # (deterministisk). Datodelen i fnr påvirkes ikke (den er
                    # seedet av datoen alene), kun individnummeret.
                    while (fake in used or fake == seed) and attempt < 200:
                        attempt += 1
                        fake = fake_value(pii_type, seed + "\x00" * attempt)
                    used.add(fake)
                self._map[key] = fake
                cached = fake
        return apply_case(original, cached) if is_name else cached

    def items(self) -> "list[tuple[str, str, str]]":
        """(pii_type, original, fake) for rapportering."""
        with self._lock:
            return [(t, o, f) for (t, o), f in self._map.items()]

    def __len__(self) -> int:
        with self._lock:
            return len(self._map)
