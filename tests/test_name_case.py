"""
Tester for case-uavhengig navnegjenkjenning + case-bevarende erstatning, samt
kopitabell-deteksjon og entitets-prioritering i subset.

Kjør:  python -X utf8 tests/test_name_case.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.anonymize.pii_detect import (
    looks_like_person_name, find_name_spans, PiiType)
from siard_workflow.core.anonymize.fake_generators import MappingStore, apply_case
from siard_workflow.core.anonymize import subset as S


def test_looks_like_person_name_case():
    # Ordbok-navn gjenkjennes uansett bokstavstørrelse
    assert looks_like_person_name("OLA NORDMANN")
    assert looks_like_person_name("ola nordmann")
    assert looks_like_person_name("Ola Nordmann")
    assert looks_like_person_name("KARI")
    assert looks_like_person_name("kari")
    assert looks_like_person_name("hansen")          # etternavn i ordbok
    assert looks_like_person_name("ABDULLAHI OMAR")  # innvandrernavn
    # Ikke navn (uansett case)
    assert not looks_like_person_name("SFO")
    assert not looks_like_person_name("ordinær grunnskole")
    assert not looks_like_person_name("avdeling 3")
    print("[ok] looks_like_person_name er case-uavhengig (ordbok-først)")


def test_find_name_spans_case():
    def names(t): return [s.text for s in find_name_spans(t)]
    # ALL-CAPS fullt navn (ukjent etternavn absorbert via caps-form)
    assert names("ELEVEN OLA NORDMANN KOM") == ["OLA NORDMANN"], names("ELEVEN OLA NORDMANN KOM")
    # lowercase med kjent etternavn
    assert names("kontakt per hansen i dag") == ["per hansen"], names("kontakt per hansen i dag")
    # lowercase fullt navn med kjent etternavn
    assert names("ola nordmann") == ["ola"], names("ola nordmann")  # ukjent lowercase etternavn ikke absorbert
    # Falske treff fortsatt unngått (begge tvetydige fellesord, lowercase)
    assert names("sortert per dag") == []
    assert names("10 kr per stk") == []
    print("[ok] find_name_spans fanger CAPS/lowercase, unngår fellesord-fraser")


def test_case_preserving_fake():
    m = MappingStore()
    # Samme person i ulik form → samme fake (case-uavhengig nøkkel), men
    # resultatet følger originalens form.
    up = m.map(PiiType.FULL_NAME, "OLA NORDMANN")
    lo = m.map(PiiType.FULL_NAME, "ola nordmann")
    ti = m.map(PiiType.FULL_NAME, "Ola Nordmann")
    assert up.isupper(), up
    assert lo.islower(), lo
    assert ti[0].isupper() and not ti.isupper(), ti
    # Samme underliggende fake (case-normalisert)
    assert up.lower() == lo == ti.lower(), (up, lo, ti)
    assert apply_case("PER", "kari hansen") == "KARI HANSEN"
    assert apply_case("per", "Kari Hansen") == "kari hansen"
    print("[ok] case-bevarende fake: samme person → samme fake, originalens form")


def test_detect_copy_tables():
    tables = {
        "s/t0": {"table_name": "Person", "schema_name": "s", "table_folder": "t0",
                 "schema_folder": "s", "columns": []},
        "s/t1": {"table_name": "Person_tmp", "schema_name": "s", "table_folder": "t1",
                 "schema_folder": "s", "columns": []},
        "s/t2": {"table_name": "Person_2023", "schema_name": "s", "table_folder": "t2",
                 "schema_folder": "s", "columns": []},
        "s/t3": {"table_name": "Sak_kopi", "schema_name": "s", "table_folder": "t3",
                 "schema_folder": "s", "columns": []},
        "s/t4": {"table_name": "Sak", "schema_name": "s", "table_folder": "t4",
                 "schema_folder": "s", "columns": []},
        "s/t5": {"table_name": "Kontakt", "schema_name": "s", "table_folder": "t5",
                 "schema_folder": "s", "columns": []},
        "s/t6": {"table_name": "kontakt_historikk", "schema_name": "s",
                 "table_folder": "t6", "schema_folder": "s", "columns": []},
    }
    copies = S.detect_copy_tables(tables)
    assert copies == {"s/t1", "s/t2", "s/t3"}, copies
    # Ekte tabeller utelates ikke
    assert "s/t0" not in copies and "s/t4" not in copies and "s/t5" not in copies
    # _historikk er IKKE en kopi (eget innhold, ikke tmp/dato)
    assert "s/t6" not in copies
    print("[ok] kopitabeller (_tmp/_kopi/dato) oppdaget, ekte tabeller skånet")


def test_entity_priority():
    tables = {
        "s/t0": {"table_name": "Logg", "schema_name": "s"},
        "s/t1": {"table_name": "Person", "schema_name": "s"},
        "s/t2": {"table_name": "Kodeverk", "schema_name": "s"},
        "s/t3": {"table_name": "Journal", "schema_name": "s"},
        "s/t4": {"table_name": "Innstilling", "schema_name": "s"},
    }
    rels = {tk: S.Relation() for tk in tables}
    rc = {"s/t0": 9999, "s/t1": 50, "s/t2": 9999, "s/t3": 80, "s/t4": 9999}
    rec = S.recommend_important_heuristic(tables, rels, rc, top_k=2)
    # Person + Journal skal prioriteres FRAMFOR store logg/kodeverk-tabeller
    assert set(rec) == {"s/t1", "s/t3"}, rec
    print("[ok] entitets-tabeller (Person/Journal) prioriteres foran logg/kodeverk")


def main() -> int:
    test_looks_like_person_name_case()
    test_find_name_spans_case()
    test_case_preserving_fake()
    test_detect_copy_tables()
    test_entity_priority()
    print("\nALLE NAVN/CASE-TESTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
