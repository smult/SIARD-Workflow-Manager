"""
siard_workflow/core/anonymize/pii_detect.py

PII-deteksjon for SIARD-anonymisering. Ren Python, ingen eksterne avhengigheter.

To nivåer:
  1. Kolonne-klassifisering (classify_column): avgjør én gang hva en hel kolonne
     inneholder, basert på kolonnenavn-heuristikk + sampling av verdier + valgfri
     lokal Ollama for tvetydige tilfeller.
  2. Fritekst-spenn (find_*): finn strukturert PII (fnr, e-post, telefon,
     postnr+sted) INNE i fritekst, slik at kun spennene erstattes.

Fødselsnummer valideres med mod-11 (begge kontrollsifre) for å unngå falske treff.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum


class PiiType(str, Enum):
    """Type personidentifiserende informasjon. str-Enum → JSON-serialiserbar."""
    FNR        = "FNR"
    FIRST_NAME = "FIRST_NAME"
    LAST_NAME  = "LAST_NAME"
    FULL_NAME  = "FULL_NAME"
    ADDRESS    = "ADDRESS"
    POSTNR     = "POSTNR"
    CITY       = "CITY"
    PHONE      = "PHONE"
    EMAIL      = "EMAIL"
    FREE_TEXT  = "FREE_TEXT"   # fritekst som kan inneholde innebygde PII-spenn
    LOB        = "LOB"         # fil/BLOB/CLOB — håndteres som dummy-fil
    OTHER      = "OTHER"       # ikke personidentifiserende — la stå


# Typer som anonymiseres som hel-celle-verdi (hele cellen byttes deterministisk).
# Omfang (etter ønske fra KDRS): KUN personnavn, personnummer, e-post og
# stedsangivelser ned på stedsnivå (adresse/postnr/sted). Telefon anonymiseres
# IKKE (PHONE er bevisst utelatt).
VALUE_TYPES = frozenset({
    PiiType.FNR, PiiType.FIRST_NAME, PiiType.LAST_NAME, PiiType.FULL_NAME,
    PiiType.ADDRESS, PiiType.POSTNR, PiiType.CITY, PiiType.EMAIL,
})


@dataclass
class Span:
    """Et PII-treff inne i en fritekststreng (start/slutt er tegn-indekser)."""
    start: int
    end: int
    pii_type: PiiType
    text: str


@dataclass
class ColumnClass:
    """Resultat av kolonne-klassifisering."""
    pii_type: PiiType
    source: str = "none"   # "name" | "value" | "ollama" | "metadata" | "none"


# ── Fødselsnummer (mod-11) ────────────────────────────────────────────────────

_FNR_W1 = (3, 7, 6, 1, 8, 9, 4, 5, 2)
_FNR_W2 = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)


def _digits(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit())


def fnr_control_digits(d9: str) -> "tuple[int, int] | None":
    """Beregn (k1, k2) for de 9 første sifrene. None hvis kontrollsiffer blir 10."""
    if len(d9) != 9 or not d9.isdigit():
        return None
    k1 = 11 - (sum(int(d9[i]) * _FNR_W1[i] for i in range(9)) % 11)
    if k1 == 11:
        k1 = 0
    if k1 == 10:
        return None
    first10 = d9 + str(k1)
    k2 = 11 - (sum(int(first10[i]) * _FNR_W2[i] for i in range(10)) % 11)
    if k2 == 11:
        k2 = 0
    if k2 == 10:
        return None
    return k1, k2


def is_valid_fnr(s: str) -> bool:
    """True hvis s er et gyldig 11-sifret norsk fødselsnummer (mod-11)."""
    d = _digits(s)
    if len(d) != 11:
        return False
    ctrl = fnr_control_digits(d[:9])
    if ctrl is None:
        return False
    k1, k2 = ctrl
    return k1 == int(d[9]) and k2 == int(d[10])


# ── Regex for fritekst-spenn ──────────────────────────────────────────────────

# 11 sifre, evt. med ett mellomrom etter de 6 første (DDMMYY NNNNN)
_FNR_RE   = re.compile(r"\b\d{6}[ ]?\d{5}\b")
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
# Norsk telefon: valgfri +47/0047, 8 sifre, evt. gruppert med mellomrom
_PHONE_RE = re.compile(
    r"(?<![\d])(?:(?:\+|00)47[ ]?)?(?:\d[ ]?){8}(?![\d])")
# Postnr + poststed: 4 sifre + STORE bokstaver (poststed skrives med versaler)
_POSTNR_STED_RE = re.compile(
    r"\b(\d{4})[ ]+([A-ZÆØÅ][A-ZÆØÅ .\-]{1,40})\b")


def find_fnr(text: str) -> "list[Span]":
    out: list[Span] = []
    for m in _FNR_RE.finditer(text):
        if is_valid_fnr(m.group(0)):
            out.append(Span(m.start(), m.end(), PiiType.FNR, m.group(0)))
    return out


def find_email(text: str) -> "list[Span]":
    return [Span(m.start(), m.end(), PiiType.EMAIL, m.group(0))
            for m in _EMAIL_RE.finditer(text)]


# Kjente filendelser — verdier som ser ut som filnavn skal ALDRI anonymiseres
# (filer skal ikke endre navn).
_FILE_EXTS = {
    "doc", "docx", "dot", "dotx", "pdf", "rtf", "txt", "odt", "ott", "wpd", "wpt",
    "xls", "xlsx", "xlt", "ods", "ppt", "pptx", "pot", "odp",
    "jpg", "jpeg", "png", "gif", "bmp", "tif", "tiff", "webp", "jp2", "svg", "ico",
    "mp3", "wav", "ogg", "flac", "aac", "m4a", "mp4", "m4v", "avi", "mov", "mkv",
    "wmv", "mpg", "mpeg", "webm",
    "zip", "rar", "7z", "gz", "tar", "bin", "dat",
    "xml", "html", "htm", "csv", "json", "eml", "msg",
}
_FILE_EXT_RE = re.compile(r"\.([A-Za-z0-9]{1,5})$")


def looks_like_filename(s: str) -> bool:
    """True hvis verdien ser ut som et filnavn (har en kjent filendelse).
    Brukes for å hindre at filnavn anonymiseres/«endrer navn»."""
    s = (s or "").strip()
    if not s or "@" in s:          # e-post ekskluderes (slutter også på .no/.com)
        return False
    m = _FILE_EXT_RE.search(s.lower())
    return bool(m and m.group(1) in _FILE_EXTS)


def is_norwegian_phone(s: str) -> bool:
    """Streng validering av norsk telefonnummer: nøyaktig 8 sifre (evt. med
    +47/0047-prefiks og gruppe-mellomrom), første siffer 2–9.

    KUN sifre og mellomrom tillates i selve nummeret. Bindestrek, skråstrek,
    punktum o.l. avvises — slik at års-spenn («2017-2020»), datoer og kontonr
    ikke feiltolkes som telefon."""
    s = (s or "").strip()
    if not s:
        return False
    core = s
    if core.startswith("+47"):
        core = core[3:]
    elif core.startswith("0047"):
        core = core[4:]
    core = core.strip()
    # Resten skal kun bestå av sifre og mellomrom (ikke -, /, ., bokstaver osv.)
    if not re.fullmatch(r"[0-9 ]+", core):
        return False
    digits = core.replace(" ", "")
    # Tillat også landskode 47 (uten +) når totalen blir 10 sifre
    if len(digits) == 10 and digits.startswith("47"):
        digits = digits[2:]
    if len(digits) != 8:
        return False
    return digits[0] not in "01"   # norske abonnentnumre starter på 2–9


def find_phone(text: str) -> "list[Span]":
    out: list[Span] = []
    for m in _PHONE_RE.finditer(text):
        raw = m.group(0).strip()
        # Ikke del av et filnavn: tall etterfulgt av «.<bokstav/tall>» (f.eks.
        # «23456789.doc»). Et punktum som setningsslutt («ring 98765432.») er ok.
        end = m.start() + len(raw)
        if end + 1 < len(text) and text[end] == "." and text[end + 1].isalnum():
            continue
        if is_norwegian_phone(raw):
            out.append(Span(m.start(), m.start() + len(raw), PiiType.PHONE, raw))
    return out


def find_postnr_sted(text: str) -> "list[Span]":
    out: list[Span] = []
    for m in _POSTNR_STED_RE.finditer(text):
        out.append(Span(m.start(), m.end(), PiiType.POSTNR, m.group(0)))
    return out


def find_all_pii(text: str) -> "list[Span]":
    """Finn alle ikke-overlappende PII-spenn i en fritekststreng.

    Prioritet ved overlapp: FNR > EMAIL > POSTNR+STED > PHONE (lengste/sikreste
    først). Returneres sortert på startindeks.
    """
    if not text:
        return []
    candidates = (find_fnr(text) + find_email(text)
                  + find_postnr_sted(text) + find_phone(text))
    # Fjern overlapp — behold spennet med høyest prioritet / lengst lengde
    prio = {PiiType.FNR: 4, PiiType.EMAIL: 3, PiiType.POSTNR: 2, PiiType.PHONE: 1}
    candidates.sort(key=lambda s: (prio.get(s.pii_type, 0), s.end - s.start),
                    reverse=True)
    chosen: list[Span] = []
    taken: list[tuple[int, int]] = []
    for sp in candidates:
        if any(not (sp.end <= a or sp.start >= b) for a, b in taken):
            continue
        chosen.append(sp)
        taken.append((sp.start, sp.end))
    chosen.sort(key=lambda s: s.start)
    return chosen


# ── Kolonne-klassifisering ────────────────────────────────────────────────────

# Heuristikk på kolonnenavn. Rekkefølgen er PRIORITERT — FULL_NAME sist slik at
# "fornavn"/"etternavn" fanges før den generiske "navn"-regelen.
_HEUR_ORDER: "list[tuple[PiiType, tuple[str, ...]]]" = [
    (PiiType.FNR,        ("fodselsnummer", "foedselsnummer", "fodselsnr", "foedselsnr",
                          "personnummer", "personnr", "fnr", "pnr", "pnummer",
                          "fnummer", "ssn", "fnumber", "fodselnr")),
    (PiiType.EMAIL,      ("epostadresse", "epost", "email", "emailaddress", "mail",
                          "epostadr", "epostadresse1", "epostadresse2")),
    # NB: telefon anonymiseres ikke (utenfor omfanget) — ingen PHONE-heuristikk.
    (PiiType.POSTNR,     ("postnummer", "postnr", "postalcode", "postcode", "postkode",
                          "zipcode", "zip", "poststednr")),
    (PiiType.CITY,       ("bosted", "city", "kommune", "sted", "stad",
                          "place", "ort", "by")),
    (PiiType.ADDRESS,    ("gateadresse", "bostedsadresse", "postadresse", "adresse",
                          "address", "gatenavn", "veinavn", "vegnavn", "veiadresse",
                          "streetaddress", "gate", "vei", "veg")),
    (PiiType.FIRST_NAME, ("fornavn", "firstname", "givenname", "forename")),
    (PiiType.LAST_NAME,  ("etternavn", "slektsnavn", "lastname", "surname",
                          "familyname")),
    (PiiType.FULL_NAME,  ("fulltnavn", "fullname", "kontaktperson", "avsender",
                          "mottaker", "navn", "name", "innbygger", "person")),
    (PiiType.FREE_TEXT,  ("fritekst", "kommentar", "merknad", "merknader",
                          "beskrivelse", "notat", "notater", "memo", "innhold",
                          "sakstekst", "brevtekst", "meldingstekst", "remarks",
                          "description", "comments", "saksbeskrivelse")),
]

# Korte/tvetydige nøkkelord som KUN skal matche eksakt kolonnenavn (ikke som
# delstreng) — ellers gir de falske treff (f.eks. "ort" i "sortering").
_EXACT_ONLY = frozenset({"vei", "veg", "gate", "by", "ort", "stad", "place",
                         "street"})


def _kw_match(kw: str, norm: str) -> bool:
    if kw in _EXACT_ONLY:
        return kw == norm
    return kw == norm or kw in norm


# Navn-/sted-/adressetyper — disse skal IKKE brukes på numeriske kode-kolonner
_NAMEISH = frozenset({PiiType.CITY, PiiType.ADDRESS, PiiType.FIRST_NAME,
                      PiiType.LAST_NAME, PiiType.FULL_NAME})

# Verdier som åpenbart ikke er PII selv om navnet matcher. Inkluderer
# «<ting>navn»-sammensetninger som aldri er personnavn (fylkenavn, fagnavn, …).
# Bare-«Navn»-kolonner forblir tvetydige og avgjøres av Ollama på innhold.
_NAME_FALSE_FRIENDS = ("brukernavn", "username", "filnavn", "filename", "tabellnavn",
                       "feltnavn", "kolonnenavn", "systemnavn", "schemanavn",
                       "databasenavn", "arkivnavn", "domenenavn", "hostname",
                       "servernavn", "fila", "dokumentnavn", "objektnavn",
                       # Ikke-person «<ting>navn»-sammensetninger (overlapper
                       # IKKE adresse/sted-nøkkelord — de skal fortsatt anonymiseres)
                       "fylkenavn", "kommunenavn", "landnavn", "fagnavn",
                       "skjemanavn", "rapportnavn", "gruppenavn", "klassenavn",
                       "romnavn", "skolenavn", "bedriftsnavn", "firmanavn",
                       "produktnavn", "prosjektnavn", "menynavn", "kategorinavn",
                       "typenavn", "statusnavn", "listenavn", "malnavn",
                       "tjenestenavn", "modulnavn", "knappenavn")

# Felter som ALDRI skal anonymiseres — bl.a. sammensatte poststed-felter
# (postnummer + sted), som ikke er personidentifiserende og ikke trenger
# endring. Hard ekskludering (overstyrer både heuristikk og Ollama).
_EXCLUDE_FIELDS = ("poststed", "poststad", "postalplace", "postplace")


def is_excluded_field(col_name: str) -> bool:
    """True hvis kolonnen er eksplisitt unntatt fra anonymisering."""
    norm = _norm_col(col_name)
    return bool(norm) and any(x in norm for x in _EXCLUDE_FIELDS)


# Spesifikke navne-nøkkelord som er entydig personnavn (skal IKKE trenge
# innholdssjekk). Generiske treff ("navn"/"name"/"person" osv.) er tvetydige —
# f.eks. NavnBM/NavnNN/FagNavn er felt-/skjematitler, ikke personnavn.
_STRONG_NAME_KW = ("fornavn", "etternavn", "slektsnavn", "mellomnavn", "fulltnavn",
                   "firstname", "lastname", "surname", "givenname", "familyname",
                   "forename")


def is_ambiguous_name(col_name: str) -> bool:
    """True hvis kolonnenavnet kun matcher navn via et GENERISK nøkkelord
    (f.eks. «navn»/«name»/«person») og dermed bør innholds-verifiseres."""
    norm = _norm_col(col_name)
    return bool(norm) and not any(k in norm for k in _STRONG_NAME_KW)


_NAME_PARTICLES = {"von", "van", "de", "der", "den", "af", "la", "le", "du",
                   "di", "da", "av", "el"}


def looks_like_person_name(value: str) -> bool:
    """Grov VERDI-heuristikk: ser verdien ut som et personnavn? 1–4 ord, ingen
    sifre. Brukes som fallback uten Ollama OG som per-verdi-vakt på navnekolonner.

    CASE-UAVHENGIG: matcher mot navneordboka på små bokstaver, slik at verdier
    skrevet med BARE STORE («OLA NORDMANN»), bare små («ola nordmann») eller blandet
    («Ola Nordmann») alle gjenkjennes. Ordboka er primærsignalet.

    For navn som IKKE finnes i ordboka faller vi tilbake på form-heuristikken, som
    krever Title Case for å unngå falske treff på vanlige ord/akronymer
    («Ordinær grunnskole» → liten forbokstav, «SFO» → akronym).
    """
    from .name_dictionary import is_known_name_token

    v = (value or "").strip()
    if not v or any(ch.isdigit() for ch in v):
        return False
    words = [w for w in re.split(r"[ ,\-]+", v) if w]
    if not (1 <= len(words) <= 4):
        return False

    # Primær: ordbok-treff (case-uavhengig) — minst ett kjent fornavn/etternavn.
    if any(is_known_name_token(w) for w in words):
        return True

    # Fallback: form-heuristikk for ukjente navn (krever Title Case).
    for w in words:
        wl = w.strip(".'")
        if not wl or wl.lower() in _NAME_PARTICLES:
            continue
        if len(wl) < 2:                    # enkeltbokstaver (J, G) → ikke navn
            return False
        if not wl[0].isupper():            # liten forbokstav (skole, er) → ikke navn
            return False
        if wl.isupper():                   # ALL-CAPS akronym (KG, SFO) → ikke navn
            return False
        if not wl.replace("'", "").isalpha():  # spesialtegn (<Ny>) → ikke navn
            return False
    return True


def is_recognized_name(value: str) -> bool:
    """True hvis minst ett ord i verdien er et kjent norsk fornavn/etternavn."""
    from .name_dictionary import is_known_name_token
    v = (value or "").strip()
    if not v:
        return False
    return any(is_known_name_token(w) for w in re.split(r"[ ,\-]+", v) if w)


_NAME_WORD_RE = re.compile(r"[A-Za-zÆØÅÄÖÉÜæøåäöéü][A-Za-zÆØÅÄÖÉÜæøåäöéü'\-]*")


def find_name_spans(text: str) -> "list[Span]":
    """Finn personnavn-spenn i fritekst via navneordboka (uten LLM).

    Fanger sammenhengende sekvenser av kapitaliserte ord som er kjente norske
    fornavn/etternavn. For å holde presisjonen høy:
      • ett enkelt FORNAVN godtas (lite tvetydig: «Ola», «Kari»)
      • ett enkelt ETTERNAVN alene godtas IKKE (mange er også vanlige ord/steder:
        «Berg», «Strand», «Lund») — men to+ navne-ord på rad («Ola Nordmann»,
        «Hansen Berg») godtas som fullt navn.
    """
    from .name_dictionary import FIRST_NAMES, LAST_NAMES, AMBIGUOUS_FIRST
    toks = [(m.group(0), m.start(), m.end()) for m in _NAME_WORD_RE.finditer(text)]
    spans: list[Span] = []
    i, n = 0, len(toks)
    while i < n:
        word, s, _e = toks[i]
        wl = word.strip("'-").lower()
        # CASE-UAVHENGIG anker: et ord starter et navn hvis det (i små bokstaver)
        # finnes i ordboka — uansett om det er CAPS, lowercase eller Title Case.
        if not (wl in FIRST_NAMES or wl in LAST_NAMES):
            i += 1
            continue
        # Utvid sekvensen. Neste ord absorberes hvis det er et kjent navn (uansett
        # bokstavstørrelse), ELLER (når vi allerede har et fornavn) et navne-formet
        # ord med stor forbokstav eller helt i versaler — da er det nesten alltid
        # etternavnet, selv om det ikke står i ordboka («Ola Nordmann»,
        # «OLA NORDMANN», «Kari Bjørnstad»). Maks 4 ord i et navn.
        j, end, has_first = i, _e, (wl in FIRST_NAMES)
        unknown_absorbed = False
        while j < n and (j - i) < 4:
            w2, _s2, e2 = toks[j]
            w2c = w2.strip("'-")
            w2l = w2c.lower()
            if w2l in FIRST_NAMES or w2l in LAST_NAMES:
                has_first = has_first or (w2l in FIRST_NAMES)
                end = e2
                j += 1
                continue
            # Ukjent etternavn: krev fornavn foran, stor forbokstav (Title eller
            # ALL-CAPS) og ≥3 bokstaver. Absorber HØYST ETT ukjent ord — ellers
            # ville hele setninger i ALL-CAPS-tekst slukes («OLA NORDMANN KOM»).
            if (has_first and not unknown_absorbed and w2[:1].isupper()
                    and len(w2c) >= 3 and w2c.isalpha()):
                unknown_absorbed = True
                end = e2
                j += 1
                continue
            break
        run_len = j - i
        start, back = s, False
        # «Etternavn, Fornavn»-rekkefølge: når et (ikke-tvetydig) FORNAVN starter
        # runen og forrige ord er et kapitalisert navne-formet ord skilt med
        # KOMMA, ta det med som etternavn — også når etternavnet ikke er i
        # ordboka («Sætre, Pål», «Jensen, Petter»).
        if wl in FIRST_NAMES and wl not in AMBIGUOUS_FIRST and i > 0:
            pw, ps, pe = toks[i - 1]
            pwc = pw.strip("'-")
            sep = text[pe:s]
            if "," in sep and not sep.strip(", ") and pw[:1].isupper() \
                    and len(pwc) >= 2 and pwc.isalpha() and not pwc.isupper():
                start, back = ps, True
        # Sterkt signal: et fler-ords navn-run godtas KUN hvis det har stor
        # forbokstav et sted (Title/CAPS) ELLER minst ett utvetydig navne-ord.
        # Ren-lowercase runer av bare tvetydige fellesord («per dag», «sortert
        # per dag») avvises som vanlig tekst. Komma-formen er allerede sterk.
        run_toks = toks[i:j]
        has_caps = any(t[0][:1].isupper() for t in run_toks)
        has_strong = any(
            (t[0].strip("'-").lower() in FIRST_NAMES
             or t[0].strip("'-").lower() in LAST_NAMES)
            and t[0].strip("'-").lower() not in AMBIGUOUS_FIRST
            for t in run_toks)
        strong = back or has_caps or has_strong
        if (back or run_len >= 2) and strong:
            spans.append(Span(start, end, PiiType.FULL_NAME, text[start:end]))
        elif run_len == 1 and has_first and wl not in AMBIGUOUS_FIRST:
            # Ett enkelt fornavn alene — hopp over de som også er vanlige ord
            # (utløses likevel i fulle navn der et etternavn følger).
            spans.append(Span(start, end, PiiType.FIRST_NAME, text[start:end]))
        i = max(j, i + 1)
    return spans


def _norm_col(name: str) -> str:
    """Normaliser kolonnenavn: små bokstaver, kun bokstaver/tall."""
    return re.sub(r"[^a-z0-9æøå]", "", (name or "").lower())


def _ratio(values: "list[str]", pred) -> float:
    vals = [v for v in values if v and v.strip()]
    if not vals:
        return 0.0
    return sum(1 for v in vals if pred(v)) / len(vals)


def _looks_email(v: str) -> bool:
    return bool(_EMAIL_RE.fullmatch(v.strip()))


def _has_11_digits(v: str) -> bool:
    """True hvis verdien inneholder nøyaktig 11 sifre (fnr-form)."""
    return len(_digits(v)) == 11


def _is_4_digits(v: str) -> bool:
    """True hvis verdien er nøyaktig 4 sifre (postnr-form)."""
    v = (v or "").strip()
    return len(v) == 4 and v.isdigit()


def _avg_len(values: "list[str]") -> float:
    vals = [v for v in values if v]
    return (sum(len(v) for v in vals) / len(vals)) if vals else 0.0


def classify_column(col_name: str, sample_values: "list[str]",
                    *, ollama=None) -> ColumnClass:
    """
    Klassifiser én kolonne. Strategi (kolonne-først, billig → dyr):
      1. Kolonnenavn-heuristikk (sterkest signal).
      2. Verdi-sampling: andel gyldige fnr / e-post / telefon.
      3. Lang fritekst → FREE_TEXT (skannes for innebygde spenn).
      4. Tvetydig + lokal Ollama tilgjengelig → spør modellen.
    """
    norm = _norm_col(col_name)
    vals = [v for v in (sample_values or []) if v and v.strip()]

    # Eksplisitt unntatte felter (f.eks. poststed/poststad/postalplace) → aldri
    # anonymisert. Overstyrer all videre klassifisering.
    if is_excluded_field(col_name):
        return ColumnClass(PiiType.OTHER, "excluded")

    # Filnavn-kolonne (verdier ser ut som filnavn) → aldri PII. Filer skal ikke
    # endre navn.
    if vals and _ratio(vals, looks_like_filename) >= 0.5:
        return ColumnClass(PiiType.OTHER, "filename")

    if norm and not any(ff in norm for ff in _NAME_FALSE_FRIENDS):
        for ptype, kws in _HEUR_ORDER:
            if any(_kw_match(kw, norm) for kw in kws):
                # Navn-/sted-/adressetreff, men verdiene er numeriske → dette er
                # en kode (f.eks. KommuneNr), ikke et navn. La kolonnen stå.
                if ptype in _NAMEISH and vals \
                        and _ratio(vals, lambda v: v.strip().isdigit()) >= 0.7:
                    break
                # Fnr må ha 11-sifrede verdier (kolonner med færre sifre, f.eks.
                # 5-sifret PersonNr, skal IKKE matches som fnr).
                if ptype is PiiType.FNR and vals \
                        and _ratio(vals, _has_11_digits) < 0.5:
                    break
                # Postnr må ha 4-sifrede verdier (ikke kontonr o.l.).
                if ptype is PiiType.POSTNR and vals \
                        and _ratio(vals, _is_4_digits) < 0.5:
                    break
                return ColumnClass(ptype, "name")

    if vals:
        # Fnr krever gyldige 11-sifrede verdier (mod-11).
        if _ratio(vals, is_valid_fnr) >= 0.6:
            return ColumnClass(PiiType.FNR, "value")
        if _ratio(vals, _looks_email) >= 0.6:
            return ColumnClass(PiiType.EMAIL, "value")
        # Fritekst som faktisk INNEHOLDER innebygd PII (fnr/e-post/postnr)
        if _ratio(vals, lambda v: bool(find_all_pii(v))) >= 0.3:
            return ColumnClass(PiiType.FREE_TEXT, "value")

    # Tvetydig — spør lokal Ollama hvis tilgjengelig
    if ollama is not None:
        try:
            if ollama.is_alive():
                guess = (ollama.classify_column(col_name, vals[:8]) or "").upper()
                if guess in PiiType.__members__:
                    pt = PiiType[guess]
                    if pt is not PiiType.OTHER:
                        return ColumnClass(pt, "ollama")
        except Exception:
            pass

    # Lang fritekst som kan inneholde innebygde navn/adresser
    if vals and (_avg_len(vals) > 40 or any(len(v) > 120 for v in vals)):
        return ColumnClass(PiiType.FREE_TEXT, "value")

    return ColumnClass(PiiType.OTHER, "none")


def should_anonymize(pii_type: PiiType, value: str) -> bool:
    """Per-verdi-vakt brukt ved selve omskrivingen — siste skanse mot å endre
    verdier som ikke faktisk matcher typen:
      • filnavn endres aldri (filer skal ikke endre navn)
      • fnr kun for 11-sifrede verdier
      • telefon kun for gyldig norsk telefon-mønster
    """
    v = (value or "").strip()
    if not v:
        return False
    if looks_like_filename(v):
        return False
    if pii_type is PiiType.FNR:
        return _has_11_digits(v)
    if pii_type is PiiType.PHONE:
        return is_norwegian_phone(v)
    if pii_type is PiiType.POSTNR:
        # Postnr endres kun for nøyaktig 4 sifre (ikke kontonr o.l.)
        return _is_4_digits(v)
    if pii_type is PiiType.EMAIL:
        # E-post endres kun for faktiske e-postadresser
        return _looks_email(v)
    if pii_type in (PiiType.CITY, PiiType.ADDRESS):
        # Sted/adresse må inneholde bokstaver — beskytter rene tallkoder
        # (f.eks. KommuneNr/VigoNr som Ollama av og til feilflagger som sted).
        return any(ch.isalpha() for ch in v)
    if pii_type in (PiiType.FIRST_NAME, PiiType.LAST_NAME, PiiType.FULL_NAME):
        # Endre kun verdier som faktisk har personnavn-form (ikke koder/etiketter)
        return looks_like_person_name(v)
    return True
