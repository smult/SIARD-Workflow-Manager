"""
siard_workflow.core.anonymize
-----------------------------
Felles byggeklosser for SIARD-anonymisering:

  pii_detect       — PII-klassifisering av kolonner + regex/validering (fnr mod-11,
                     e-post, telefon, postnr) i fritekst. Ren Python.
  fake_generators  — deterministiske, syntetiske erstatningsverdier (samme
                     original → samme fake). Ren Python, ingen avhengigheter.
  dummy_files      — dummy LOB-innhold (Lorem Ipsum-PDF/RTF/tekst + media-stubber).
  ollama_client    — valgfri LOKAL Ollama-klient for å øke treffsikkerhet. Degraderer
                     til regex/heuristikk hvis Ollama ikke kjører. Aldri sky.

Determinismen sikrer referanseintegritet: en person (fnr/navn) får samme fiktive
identitet i hele arkivet, slik at fremmednøkler og koblinger fortsatt stemmer.
"""
from __future__ import annotations

from .pii_detect import (
    PiiType, Span, ColumnClass,
    is_valid_fnr, classify_column,
    find_fnr, find_email, find_phone, find_postnr_sted, find_all_pii,
)
from .fake_generators import fake_value, MappingStore, lorem_ipsum

__all__ = [
    "PiiType", "Span", "ColumnClass",
    "is_valid_fnr", "classify_column",
    "find_fnr", "find_email", "find_phone", "find_postnr_sted", "find_all_pii",
    "fake_value", "MappingStore", "lorem_ipsum",
]
