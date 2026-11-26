"""
Uttrekkets grunnavn — navnet alle sidefilene er laget fra.

En kjøring lager filer ved siden av SIARD-fila: kjørelogg, CSV-er, rapporter,
SHA-256 og PREMIS-proveniens («Uttrekk_premis.xml»). De heter alle etter
ORIGINAL-fila. Resultatfila heter noe annet («Uttrekk_konvertert.siard»,
«Uttrekk_konvertert_anonymisert.siard», «Uttrekk_konvertert_1.siard» når
navnet var opptatt), og den som skal finne sidefilene ut fra resultatfila —
DIAS-pakkingen — må komme tilbake til samme grunnavn.

Før fantes regelen i fire kopier som ikke var like: DIAS-pakkingen strippet
bare ett suffiks per sort og ikke løpenummer, så «…_konvertert_anonymisert»
og «…_konvertert_1» ga feil grunnavn, og PREMIS-fila kom ikke med i pakken.
"""
from __future__ import annotations

import re
from pathlib import Path

# Suffikser operasjonene legger på resultatfila
OP_SUFFIXES = ("_konvertert", "_hex_extracted", "_cosdoc", "_blob", "_dias",
               "_anonymisert", "_stdext", "_lobfix", "_xsdfix", "_metafix",
               "_segfix")

_COUNTER = re.compile(r"_\d{1,4}$")


def _ends_with_suffix(stem: str) -> "str | None":
    low = stem.lower()
    for suf in OP_SUFFIXES:
        if low.endswith(suf) and len(stem) > len(suf):
            return suf
    return None


def sidecar_base(name) -> str:
    """
    Grunnavnet for en SIARD-fil (sti, filnavn eller navn uten endelse):

        Uttrekk.siard                          → Uttrekk
        Uttrekk_hex_extracted_konvertert.siard → Uttrekk
        Uttrekk_konvertert_anonymisert.siard   → Uttrekk
        Uttrekk_konvertert_1.siard             → Uttrekk
        WIS-Marnardal_2024.siard               → WIS-Marnardal_2024

    Løpenummer («_1») fjernes bare når det står etter et kjent suffiks —
    et årstall eller nummer i selve arkivnavnet røres ikke.
    """
    if isinstance(name, Path):
        stem = name.stem
    else:
        stem = str(name)
        if stem.lower().endswith(".siard"):
            stem = stem[:-len(".siard")]
    while True:
        suf = _ends_with_suffix(stem)
        if suf:
            stem = stem[:-len(suf)]
            continue
        m = _COUNTER.search(stem)
        if m and _ends_with_suffix(stem[:m.start()]):
            stem = stem[:m.start()]
            continue
        return stem
