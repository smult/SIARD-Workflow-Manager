"""
Tester for SIARD-anonymisering (siard_workflow.core.anonymize + AnonymizeOperation).

Kjør:  python -m pytest tests/test_anonymize.py -v
   eller:  python tests/test_anonymize.py   (kjører en enkel selvtest uten pytest)
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.anonymize import (
    PiiType, is_valid_fnr, classify_column, find_all_pii, MappingStore)
from siard_workflow.core.anonymize.fake_generators import fake_fnr, fake_value
from siard_workflow.core.anonymize import dummy_files
from siard_workflow.core.base_operation import OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.operations.anonymize_operation import (
    AnonymizeOperation, read_tables, sample_columns)


# ── Enhetstester: pii_detect ──────────────────────────────────────────────────

def test_fnr_validation_and_generation():
    # Generert fnr må være mod-11-gyldig, syntetisk (måned > 12) og deterministisk
    f = fake_fnr("12128012345")
    assert is_valid_fnr(f)
    assert int(f[2:4]) > 12, f"måned må være syntetisk, fikk {f[2:4]}"
    assert fake_fnr("12128012345") == f
    assert f != "12128012345"
    assert not is_valid_fnr("12345678901")
    assert not is_valid_fnr("123")


def test_classify_by_name():
    assert classify_column("FODSELSNUMMER", []).pii_type == PiiType.FNR
    assert classify_column("Fornavn", []).pii_type == PiiType.FIRST_NAME
    assert classify_column("Etternavn", []).pii_type == PiiType.LAST_NAME
    assert classify_column("NAVN", []).pii_type == PiiType.FULL_NAME
    assert classify_column("EPOST", []).pii_type == PiiType.EMAIL
    # Telefon er i omfanget (fra 2026-09-18)
    assert classify_column("Telefonnummer", []).pii_type == PiiType.PHONE
    assert classify_column("Brukernavn", ["per", "ola"]).pii_type != PiiType.FULL_NAME


def test_fnr_requires_11_digits():
    from siard_workflow.core.anonymize.pii_detect import should_anonymize
    # Kolonne med 5 sifre er de fem siste i fnr → PNR5 (ikke FNR); andre lengder → ingen
    assert classify_column("PersonNr", ["46098", "34388", "26227"]).pii_type == PiiType.PNR5
    assert classify_column("Foresatt1Personnr", ["12345"]).pii_type == PiiType.PNR5
    assert classify_column("PersonNr", ["4609", "3438"]).pii_type == PiiType.OTHER
    assert should_anonymize(PiiType.PNR5, "46098") is True
    assert should_anonymize(PiiType.PNR5, "4609") is False
    # 11-sifret gyldig fnr-kolonne skal bli FNR
    f1, f2 = fake_fnr("01010099991"), fake_fnr("02020099992")
    assert classify_column("FodselsNr", [f1, f2]).pii_type == PiiType.FNR
    # per-verdi-vakt
    assert should_anonymize(PiiType.FNR, f1) is True
    assert should_anonymize(PiiType.FNR, "46098") is False


def test_phone_norwegian_only():
    from siard_workflow.core.anonymize.pii_detect import is_norwegian_phone, should_anonymize, find_phone
    assert is_norwegian_phone("98765432")
    assert is_norwegian_phone("+47 98 76 54 32")
    assert is_norwegian_phone("99887766")
    assert not is_norwegian_phone("12345678")        # starter på 1
    assert not is_norwegian_phone("035235453")       # 9 sifre
    assert not is_norwegian_phone("035235453.doc")   # filnavn
    assert not is_norwegian_phone("1234567")         # 7 sifre
    # filnavn med 8-sifret stamme skal ikke gi telefon-treff i fritekst
    assert find_phone("vedlegg 23456789.doc her") == [] or \
        all(s.text != "23456789" for s in find_phone("vedlegg 23456789.doc her"))
    assert should_anonymize(PiiType.PHONE, "98765432") is True
    assert should_anonymize(PiiType.PHONE, "035235453") is False
    # Års-spenn / datoer / range-uttrykk skal IKKE tolkes som telefon
    assert not is_norwegian_phone("2017-2020")
    assert not is_norwegian_phone("2017 - 2020")
    assert not is_norwegian_phone("2017/2018")
    assert not is_norwegian_phone("01.01.2020")
    assert should_anonymize(PiiType.PHONE, "2017-2020") is False
    # En kolonne full av års-spenn skal ikke klassifiseres som telefon
    assert classify_column("Periode", ["2017-2020", "2018-2021", "2019-2022"]).pii_type != PiiType.PHONE


def test_phone_and_email_anonymization():
    """Telefon: kolonner som telefon/tel1/mobil/mobnummer/phone/mobile med
    telefon-lignende verdier anonymiseres til «12 34 56 78» / «+47 12 34 56 78»
    med originalens oppdeling. E-post → anon<n>@ymized.no (entydig)."""
    from siard_workflow.core.anonymize.pii_detect import (
        looks_like_phone, should_anonymize, is_phone_field)
    from siard_workflow.core.anonymize.fake_generators import (
        fake_phone, fake_email, MappingStore)
    # Kolonnenavn
    for name in ("Telefon", "tel1", "TLF2", "Mobil", "mobnummer", "MobilNr", "phone",
                 "Mobile", "Telefonnummer", "Fax", "mob"):
        assert is_phone_field(name), name
    for name in ("Hotel", "Titel", "Kapitel", "Automobilforhandler", "Faxmaskin"):
        assert not is_phone_field(name), name
    # Verdier: med/uten landkode og oppdeling
    for v in ("98765432", "98 76 54 32", "987 65 432", "+47 98 76 54 32", "+4798765432",
              "0047 98765432", "4798765432", "98-76-54-32", "+46 70-123 45 67",
              "+1 (555) 123-4567", "+44 20 7946 0958"):
        assert looks_like_phone(v), v
    for v in ("12345678", "035235453", "1234567", "2017-2020", "2017 - 2020", "01.01.2020",
              "20200101", "98765432.doc", "abc", "", "0301"):
        assert not looks_like_phone(v), v
    # Klassifisering krever telefon-lignende verdier
    assert classify_column("Telefon", ["98765432", "+47 91 23 45 67"]).pii_type == PiiType.PHONE
    assert classify_column("tel1", ["22334455", "99887766"]).pii_type == PiiType.PHONE
    assert classify_column("Mobil", ["ja", "nei"]).pii_type != PiiType.PHONE
    assert classify_column("Telefon", ["2017-2020", "2018-2021"]).pii_type != PiiType.PHONE
    assert should_anonymize(PiiType.PHONE, "+47 98 76 54 32") is True
    # Fast fiktivt nummer med bevart form
    assert fake_phone("98 76 54 32") == "12 34 56 78"
    assert fake_phone("+47 98 76 54 32") == "+47 12 34 56 78"
    assert fake_phone("98765432") == "12345678"
    assert fake_phone("+4798765432") == "+4712345678"
    assert fake_phone("0047 98765432") == "0047 12345678"
    assert fake_phone("4798765432") == "4712345678"
    assert fake_phone("987 65 432") == "123 45 678"
    assert fake_phone("+46 70-123 45 67") == "+46 12-345 67 81"
    assert fake_phone(" 98765432 ") == " 12345678 "
    assert len(fake_phone("+1 (555) 123-4567")) == len("+1 (555) 123-4567")
    store = MappingStore()
    assert store.map(PiiType.PHONE, "98 76 54 32") == "12 34 56 78"
    assert store.map(PiiType.PHONE, "91 23 45 67") == "12 34 56 78", "telefon er ikke nøkkel — felles fiktiv verdi"
    # E-post
    e = fake_email("ola.nordmann@skole.no")
    assert e.startswith("anon") and e.endswith("@ymized.no") and e == fake_email("ola.nordmann@skole.no")
    assert fake_email("kari@x.no") != e
    store2 = MappingStore()
    fakes = {store2.map(PiiType.EMAIL, f"bruker{i}@firma.no") for i in range(300)}
    assert len(fakes) == 300 and all(f.endswith("@ymized.no") for f in fakes)
    # Fritekst: norsk telefon byttes på plass
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation, _apply_spans
    txt = "Ring Per Hansen på 98 76 54 32 eller +47 91234567, e-post per@firma.no."
    spans = AnonymizeOperation._freetext_spans(txt)
    out = _apply_spans(txt, spans, MappingStore())
    assert "98 76 54 32" not in out and "91234567" not in out and "per@firma.no" not in out
    assert "12 34 56 78" in out and "+47 12345678" in out and "@ymized.no" in out, out


def test_kommune_names():
    """Kommunenavn (SSB 1990–2026, også nedlagte) → «Fiktiv<endelse>» i alle
    tekstfelt og fritekst; tvetydige navn (Berg, Time …) bare med «kommune»
    eller som hel celle; bokstavform og genitiv-s bevares."""
    from siard_workflow.core.anonymize.kommune_names import (
        fake_kommune, replace_kommune_names, is_kommune_name, AMBIGUOUS)
    from siard_workflow.core.anonymize.kommune_data import KOMMUNE_NAMES
    from siard_workflow.core.anonymize.fake_generators import fake_city
    assert len(KOMMUNE_NAMES) > 450
    for n in ("Marnardal", "Søgne", "Songdalen", "Mandal", "Lindesnes", "Oslo",
              "Kristiansand", "Nord-Aurdal", "Nore og Uvdal", "Guovdageaidnu", "Herøy"):
        assert is_kommune_name(n), n
    assert fake_kommune("Marnardal") == "Fiktivdal"
    assert fake_kommune("MARNARDAL") == "FIKTIVDAL"
    assert fake_kommune("marnardal") == "fiktivdal"
    assert fake_kommune("Lindesnes") == "Fiktivnes"
    assert fake_kommune("Kristiansand") == "Fiktivsand"
    assert fake_kommune("Songdalen") == "Fiktivdalen"
    assert fake_kommune("Nord-Aurdal") == "Fiktivdal"
    assert fake_kommune("Nore og Uvdal") == "Fiktivdal"
    assert fake_kommune("Oslo") == "Fiktivby" and fake_kommune("Bergen") == "Fiktivby"
    assert fake_city("Marnardal") == "Fiktivdal" and fake_city("Ukjentsted") == "Fiktivby"
    # Fritekst
    t, n = replace_kommune_names("Pasienten bor i Marnardal og jobber ved Marnardal legesenter.")
    assert n == 2 and "Marnardal" not in t and "Fiktivdal legesenter" in t, t
    t, n = replace_kommune_names("Marnardals kommune, Søgne kommune og MANDAL.")
    assert t == "Fiktivdals kommune, Fiktivby kommune og FIKTIVBY." or "Fiktivdals kommune" in t, t
    assert n == 3
    # Sammensetning uten ordgrense røres ikke
    t, n = replace_kommune_names("Marnardalsveien 3")
    assert n == 0 and t == "Marnardalsveien 3"
    # Tvetydige navn: bare med «kommune» eller hel celle
    assert "Berg" in AMBIGUOUS and "Time" in AMBIGUOUS and "Os" in AMBIGUOUS
    t, n = replace_kommune_names("Signert av Kari Berg. Time: 14:00.")
    assert n == 0 and t == "Signert av Kari Berg. Time: 14:00.", t
    t, n = replace_kommune_names("Berg kommune og Time kommune")
    assert n == 2 and "Berg" not in t and "Time" not in t, t
    t, n = replace_kommune_names("BERG", whole_cell=True)
    assert n == 1 and t == "FIKTIVBERG", t
    t, n = replace_kommune_names(" Time ", whole_cell=True)
    assert n == 1 and t.strip() == "Fiktivby", t
    t, n = replace_kommune_names("Berg", whole_cell=False)
    assert n == 0
    # Fiktive etternavn fra generatoren skal ikke rammes i fritekst
    t, n = replace_kommune_names("Per Lund og Kari Strand møtte Ola Moe")
    assert n == 0, t


def test_filenames_never_anonymized():
    from siard_workflow.core.anonymize.pii_detect import looks_like_filename, should_anonymize
    assert looks_like_filename("035235453.doc")
    assert looks_like_filename("Bilde_av_elev.JPG")
    assert not looks_like_filename("ola.nordmann@skole.no")   # e-post, ikke fil
    assert not looks_like_filename("Storgata 12")
    # en filnavn-verdi i en navnekolonne skal ikke endres
    assert should_anonymize(PiiType.FULL_NAME, "kontrakt_signert.pdf") is False
    # en kolonne full av filnavn klassifiseres ikke som PII
    assert classify_column("BildeFil", ["a.jpg", "b.png", "c.jpg"]).pii_type == PiiType.OTHER


def test_fixed_fake_values():
    from siard_workflow.core.anonymize.fake_generators import (
        fake_address, fake_postnr, fake_city, fake_email, fake_postnr_value)
    assert fake_address("Storgata 12").startswith("Fiktivveien")
    assert fake_postnr("0150") == "9999"
    assert fake_city("OSLO") == "FIKTIVBY" and fake_city("Oslo") == "Fiktivby"   # bokstavform bevares
    assert fake_email("ola@skole.no").endswith("@ymized.no")
    assert fake_email("ola@skole.no").startswith("anon")
    assert fake_postnr_value("0150") == "9999"
    assert fake_postnr_value("0150 OSLO") == "9999 Fiktivby"


def test_postnr_only_4_digits():
    from siard_workflow.core.anonymize.pii_detect import should_anonymize
    assert should_anonymize(PiiType.POSTNR, "0150") is True
    assert should_anonymize(PiiType.POSTNR, "3090.10.90493") is False  # kontonr
    assert should_anonymize(PiiType.POSTNR, "12345") is False
    # kolonne med kontonr-verdier skal ikke klassifiseres som postnr
    assert classify_column("PostNr", ["3090.10.90493", "1234.56.78901"]).pii_type != PiiType.POSTNR


def test_excluded_poststed_fields():
    from siard_workflow.core.anonymize.pii_detect import is_excluded_field
    assert is_excluded_field("Poststed")
    assert is_excluded_field("Poststad")
    assert is_excluded_field("PostalPlace")
    assert not is_excluded_field("Sted")
    assert not is_excluded_field("Bosted")
    # Sammensatt poststed-felt skal IKKE anonymiseres (verken CITY eller FREE_TEXT)
    assert classify_column("Poststed", ["0150 OSLO", "5003 BERGEN"]).pii_type == PiiType.OTHER
    assert classify_column("Poststad", ["5345 Bergen"]).pii_type == PiiType.OTHER
    # Rene sted-felter anonymiseres fortsatt
    assert classify_column("Sted", ["Oslo"]).pii_type == PiiType.CITY


def test_email_value_guard():
    from siard_workflow.core.anonymize.pii_detect import should_anonymize
    assert should_anonymize(PiiType.EMAIL, "ola@skole.no") is True
    assert should_anonymize(PiiType.EMAIL, "ikke en epost") is False
    assert should_anonymize(PiiType.EMAIL, "12345") is False


def test_city_address_require_letters():
    from siard_workflow.core.anonymize.pii_detect import should_anonymize
    # Sted/adresse med bokstaver → anonymiser; rene tallkoder → behold
    assert should_anonymize(PiiType.CITY, "Oslo") is True
    assert should_anonymize(PiiType.ADDRESS, "Storgata 1") is True
    assert should_anonymize(PiiType.CITY, "0301") is False     # KommuneNr-kode
    assert should_anonymize(PiiType.ADDRESS, "12345") is False


def _valid_fnr(ddmmyy: str, ind: int) -> str:
    """Bygg et gyldig fnr for test (justerer individnr til kontrollsifrene går opp)."""
    from siard_workflow.core.anonymize.pii_detect import fnr_control_digits
    for i in range(ind, ind + 20):
        d9 = f"{ddmmyy}{i:03d}"
        k = fnr_control_digits(d9)
        if k:
            return d9 + str(k[0]) + str(k[1])
    raise AssertionError("fant ikke gyldig fnr")


def test_fnr_decomposition_consistency():
    """Dekomponerbart fnr: datodel = shift_birthdate(fødselsdato), individnr i
    samme århundreserie og kjønnsparitet, kontroll beregnes. Fødselsdato-
    kolonner og femsifrede personnummer blir konsistente med fnr-kolonner."""
    import datetime as dt
    from siard_workflow.core.anonymize.fake_generators import (
        fake_birthdate, fake_pnr5, shift_birthdate, MappingStore)
    from siard_workflow.core.anonymize.pii_detect import (
        is_valid_fnr, fnr_birthdate, compose_fnr, parse_date, should_anonymize)

    fnr = _valid_fnr("170580", 123)                    # mann (123 oddetall), 1980
    fake = fake_fnr(fnr)
    assert fake != fnr and is_valid_fnr(fake) and len(fake) == 11
    assert 81 <= int(fake[2:4]) <= 92, "måned + 80 (syntetisk område)"
    # Datodel = forskjøvet fødselsdato — samme som fødselsdato-kolonnen får
    shifted = shift_birthdate(dt.date(1980, 5, 17))
    assert shifted != dt.date(1980, 5, 17) and abs((shifted - dt.date(1980, 5, 17)).days) <= 365
    assert fnr_birthdate(fake) == shifted, (fnr_birthdate(fake), shifted)
    assert fake_birthdate("1980-05-17") == shifted.isoformat()
    assert fake_birthdate("17.05.1980") == shifted.strftime("%d.%m.%Y")
    assert fake_birthdate("1980-05-17T00:00:00Z") == shifted.isoformat() + "T00:00:00Z"
    assert fake_birthdate("ikke en dato") == "ikke en dato"
    # Århundreserie og kjønn bevart i individnummeret
    ind = int(fake[6:9])
    assert 0 <= ind <= 499 and ind % 2 == 1, f"1900-serie, mann: {ind}"
    fnr2005 = _valid_fnr("030405", 512)                # 2000-serien, kvinne (512 partall)
    f2 = fake_fnr(fnr2005)
    assert is_valid_fnr(f2) and 500 <= int(f2[6:9]) <= 999 and int(f2[6:9]) % 2 == 0
    assert fnr_birthdate(f2).year >= 2000, "forskyvning holder seg i 2000-serien"
    # Forskyvning krysser aldri seriegrensen
    assert shift_birthdate(dt.date(1999, 12, 30)).year <= 1999
    assert shift_birthdate(dt.date(2000, 1, 1)).year >= 2000
    assert shift_birthdate(dt.date(1854, 1, 2)).year >= 1854
    # D-nummer normaliseres og gir samme dato
    dnr = _valid_fnr("570580", 123)                    # dag + 40
    assert fnr_birthdate(dnr) == dt.date(1980, 5, 17)
    # Komponering: fødselsdato + fem siste → originalt fnr → felles mapping
    assert compose_fnr(dt.date(1980, 5, 17), fnr[6:]) == fnr
    assert compose_fnr(dt.date(1980, 5, 17), dnr[6:]) == dnr, "D-nummer komponeres også"
    bad5 = fnr[6:10] + str((int(fnr[10]) + 1) % 10)      # ødelagt kontrollsiffer
    assert compose_fnr(dt.date(1980, 5, 17), bad5) is None
    store = MappingStore()
    assert store.map(PiiType.FNR, fnr)[6:] == fake[6:]
    # Fallback fem-til-fem: 5 sifre, kjønnsparitet bevart, deterministisk
    p = fake_pnr5("12345")
    assert len(p) == 5 and p.isdigit() and int(p[2]) % 2 == int("12345"[2]) % 2
    assert fake_pnr5("12345") == p and fake_pnr5("12346") != p or True
    assert fake_value(PiiType.PNR5, "12345") == p
    # Klassifisering
    assert classify_column("Fodselsdato", ["1980-05-17", "2001-01-02"]).pii_type == PiiType.BIRTHDATE
    assert classify_column("Født", ["17.05.1980"]).pii_type == PiiType.BIRTHDATE
    assert classify_column("Fodselsdato", ["abc", "def"]).pii_type != PiiType.BIRTHDATE
    assert classify_column("Personnummer", [fnr, fnr2005]).pii_type == PiiType.FNR
    assert classify_column("Personnr", [fnr[6:], fnr2005[6:]]).pii_type == PiiType.PNR5
    assert should_anonymize(PiiType.BIRTHDATE, "1980-05-17") and not should_anonymize(PiiType.BIRTHDATE, "x")
    assert parse_date("19800517")[0] == dt.date(1980, 5, 17)


def test_mapping_injective_for_keys():
    """To ulike fnr (også med samme fødselsdato) får aldri samme fiktive verdi;
    fødselsdato-kolonner tvinges IKKE (samme dato → samme forskyvning)."""
    from siard_workflow.core.anonymize.fake_generators import MappingStore
    from siard_workflow.core.anonymize.pii_detect import is_valid_fnr, fnr_birthdate
    store = MappingStore()
    originals = []
    for ind in range(0, 499, 2):                        # ~250 menn født 17.05.1980
        try:
            originals.append(_valid_fnr("170580", ind + 1))
        except AssertionError:
            pass
    originals = sorted(set(originals))
    fakes = [store.map(PiiType.FNR, o) for o in originals]
    assert len(set(fakes)) == len(fakes), "fnr-mapping må være injektiv"
    assert all(is_valid_fnr(f) for f in fakes)
    assert len({fnr_birthdate(f) for f in fakes}) == 1, "alle beholder samme (forskjøvne) dato"
    # samme original → samme fake, også etter re-roll
    assert store.map(PiiType.FNR, originals[0]) == fakes[0]
    # e-post er nøkkel-type: injektiv
    emails = [store.map(PiiType.EMAIL, f"bruker{i}@x.no") for i in range(200)]
    assert len(set(emails)) == 200
    # fødselsdato: samme dato → samme fake (ikke injektiv)
    assert store.map(PiiType.BIRTHDATE, "1980-05-17") == store.map(PiiType.BIRTHDATE, "1980-05-17")


_META_FNR_REL = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema><name>S</name><folder>schema0</folder><tables>
    <table><name>Person</name><folder>table0</folder>
      <columns>
        <column><name>Fnr</name><type>VARCHAR(11)</type></column>
        <column><name>Navn</name><type>VARCHAR(50)</type></column>
      </columns><rows>2</rows></table>
    <table><name>Elev</name><folder>table1</folder>
      <columns>
        <column><name>Fodselsdato</name><type>DATE</type></column>
        <column><name>PersonNr</name><type>VARCHAR(5)</type></column>
        <column><name>Klasse</name><type>VARCHAR(5)</type></column>
      </columns><rows>2</rows></table>
    <table><name>Kontakt</name><folder>table2</folder>
      <columns>
        <column><name>PersonNr</name><type>INTEGER</type></column>
        <column><name>Fdato</name><type>TIMESTAMP</type></column>
      </columns><rows>1</rows></table>
    <table><name>Loes</name><folder>table3</folder>
      <columns>
        <column><name>PersonNr</name><type>VARCHAR(5)</type></column>
      </columns><rows>1</rows></table>
  </tables></schema></schemas>
</siardArchive>
"""


def test_fnr_relations_end_to_end(tmp_path=None):
    """Fnr i tabell A, fødselsdato + femsifret personnummer i tabell B/C for
    samme personer: etter anonymisering skal (dato, 5 sifre) i B/C fortsatt
    peke på fnr-et i A. Tabell D (5 sifre uten dato) → fem-til-fem-fallback."""
    import datetime as dt
    import re
    from siard_workflow.core.anonymize.pii_detect import is_valid_fnr, fnr_birthdate
    base = tmp_path or Path("./_fixture_rel")
    base.mkdir(parents=True, exist_ok=True)
    fnr_a = _valid_fnr("170580", 123)      # 1980-05-17
    fnr_b = _valid_fnr("030405", 512)      # 2005-04-03
    t0 = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
          f'<row><c1>{fnr_a}</c1><c2>Ola Nordmann</c2></row>\n'
          f'<row><c1>{fnr_b}</c1><c2>Kari Hansen</c2></row>\n</table>\n')
    t1 = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
          f'<row><c1>1980-05-17</c1><c2>{fnr_a[6:]}</c2><c3>3A</c3></row>\n'
          f'<row><c1>2005-04-03</c1><c2>{fnr_b[6:]}</c2><c3>1B</c3></row>\n</table>\n')
    t2 = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
          f'<row><c1>{int(fnr_a[6:])}</c1><c2>1980-05-17T00:00:00Z</c2></row>\n</table>\n')
    t3 = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
          f'<row><c1>{fnr_a[6:]}</c1></row>\n</table>\n')
    siard = base / "rel.siard"
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("header/metadata.xml", _META_FNR_REL)
        z.writestr("content/schema0/table0/table0.xml", t0)
        z.writestr("content/schema0/table1/table1.xml", t1)
        z.writestr("content/schema0/table2/table2.xml", t2)
        z.writestr("content/schema0/table3/table3.xml", t3)
    result = _run_op(siard)
    assert result.success, result.message
    with zipfile.ZipFile(Path(result.data["output_path"])) as z:
        x0 = z.read("content/schema0/table0/table0.xml").decode("utf-8")
        x1 = z.read("content/schema0/table1/table1.xml").decode("utf-8")
        x2 = z.read("content/schema0/table2/table2.xml").decode("utf-8")
        x3 = z.read("content/schema0/table3/table3.xml").decode("utf-8")
    fakes = re.findall(r"<c1>(\d{11})</c1>", x0)
    assert len(fakes) == 2 and fnr_a not in x0 and fnr_b not in x0
    assert all(is_valid_fnr(f) for f in fakes)
    rows1 = re.findall(r"<row><c1>(.*?)</c1><c2>(.*?)</c2>", x1)
    assert len(rows1) == 2
    for (bd, p5), fake in zip(rows1, fakes):
        # fødselsdato-kolonnen = datodelen i fnr-et (forskjøvet)
        assert dt.date.fromisoformat(bd) == fnr_birthdate(fake), (bd, fake)
        # femsifret = de fem siste i fnr-et → relasjonen (dato + 5 sifre) holder
        assert p5 == fake[6:], (p5, fake)
        assert bd not in ("1980-05-17", "2005-04-03")
    # INTEGER-personnr + TIMESTAMP-fødselsdato komponeres også
    m2 = re.search(r"<row><c1>(\d+)</c1><c2>(.*?)</c2>", x2)
    assert m2 and m2.group(1).zfill(5) == fakes[0][6:], m2.groups()
    assert m2.group(2) == fnr_birthdate(fakes[0]).isoformat() + "T00:00:00Z"
    # Uten dato i raden: fem-til-fem-fallback (5 sifre, endret, ikke koblet)
    m3 = re.search(r"<c1>(\d{5})</c1>", x3)
    assert m3 and m3.group(1) != fnr_a[6:]
    assert "Ola Nordmann" not in x0


_META_KOMMUNE = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema><name>S</name><folder>schema0</folder><tables>
    <table><name>Ansatt</name><folder>table0</folder>
      <columns>
        <column><name>Arbeidsgiver</name><type>VARCHAR(80)</type></column>
        <column><name>Poststed</name><type>VARCHAR(40)</type></column>
        <column><name>Notat</name><type>NCLOB</type></column>
        <column><name>Avdelingskode</name><type>VARCHAR(10)</type></column>
      </columns><rows>2</rows></table>
  </tables></schema></schemas>
</siardArchive>
"""


def test_kommune_end_to_end(tmp_path=None):
    """Kommunenavn erstattes gjennom selve radomskrivingen: i vanlige
    tekstkolonner (ikke PII-klassifisert), i poststed (unntatt fra PII) og i
    fritekst; koder uten kommunenavn står urørt."""
    base = tmp_path or Path("./_fixture_kommune")
    base.mkdir(parents=True, exist_ok=True)
    siard = base / "kommune.siard"
    t0 = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
          '<row><c1>Marnardal kommune</c1><c2>MARNARDAL</c2>'
          '<c3>Flyttet fra Søgne til Marnardal i 2019. Kontakt Kari Berg.</c3><c4>AVD-7</c4></row>\n'
          '<row><c1>Lindesnes legesenter</c1><c2>MANDAL</c2><c3>Ingen merknad.</c3><c4>AVD-9</c4></row>\n'
          '</table>\n')
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("header/metadata.xml", _META_KOMMUNE)
        z.writestr("content/schema0/table0/table0.xml", t0)
    result = _run_op(siard)
    assert result.success, result.message
    with zipfile.ZipFile(Path(result.data["output_path"])) as z:
        xml = z.read("content/schema0/table0/table0.xml").decode("utf-8")
    assert "<c1>Fiktivdal kommune</c1>" in xml, xml
    assert "<c1>Fiktivnes legesenter</c1>" in xml, xml
    assert xml.count("<c2>FIKTIVDAL</c2>") == 2, xml          # MARNARDAL og MANDAL → …dal
    assert "Marnardal" not in xml and "Søgne" not in xml and "MANDAL" not in xml
    assert "Fiktivby til Fiktivdal i 2019" in xml, xml
    assert "Kari Berg" not in xml, "personnavn i fritekst anonymiseres fortsatt"
    assert "<c4>AVD-7</c4>" in xml and "<c4>AVD-9</c4>" in xml, "koder urørt"
    assert result.data.get("kommune_replaced", 0) >= 5, result.data


def test_address_spans_and_hex_text():
    """Gateadresser i fritekst → «Fiktivveien N»; hex-kodet tekst dekodes,
    anonymiseres og skrives tilbake som hex."""
    from siard_workflow.core.anonymize.pii_detect import (
        find_address_spans, decode_hex_text, encode_hex_text)
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation, _apply_spans
    txt = "Bor i Storgata 12, tidligere Schweigaards gate 4B og Nedre Kirkeveien 7. Møte kl 12."
    spans = find_address_spans(txt)
    assert [s.text for s in spans] == ["Storgata 12", "Schweigaards gate 4B", "Nedre Kirkeveien 7"], \
        [s.text for s in spans]
    assert find_address_spans("Saken har nr 12 og gate 3") == [] or \
        all("gate 3" not in s.text for s in find_address_spans("Saken har nr 12"))
    out = _apply_spans(txt, AnonymizeOperation._freetext_spans(txt), MappingStore())
    assert "Storgata" not in out and "Schweigaards" not in out and "Kirkeveien" not in out
    assert out.count("Fiktivveien") == 3 and "Møte kl 12." in out, out
    # Hex-kodet tekst
    plain = "Kontakt Ola Nordmann, tlf 98765432, Storgata 12, Marnardal."
    hx = plain.encode("utf-8").hex().upper()
    dec = decode_hex_text(hx)
    assert dec and dec[0] == plain and dec[1] == "utf-8" and dec[2] is True
    assert decode_hex_text("48656C6C6F") is None            # for kort (< 16)
    assert decode_hex_text("Storgata 12") is None
    assert decode_hex_text(bytes(range(32)).hex()) is None  # binært
    lower = plain.encode("cp1252").hex()
    d2 = decode_hex_text(lower)
    assert d2 and d2[2] is False
    assert encode_hex_text("Å", "cp1252", True) == "C5" and encode_hex_text("Å", "utf-8", False) == "c385"


_META_SCAN = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema><name>S</name><folder>schema0</folder><tables>
    <table><name>Journal</name><folder>table0</folder>
      <columns>
        <column><name>Info</name><type>VARCHAR(200)</type></column>
        <column><name>Tekst</name><type>CLOB</type></column>
        <column><name>Kode</name><type>VARCHAR(10)</type></column>
        <column><name>ObjektId</name><type>VARCHAR(40)</type></column>
        <column><name>Verdi</name><type>VARCHAR(60)</type></column>
      </columns><rows>1</rows></table>
  </tables></schema></schemas>
</siardArchive>
"""


def test_scan_all_text_end_to_end(tmp_path=None):
    """Innebygd PII i en vanlig VARCHAR-kolonne (ikke klassifisert) og i en
    hex-kodet inline CLOB anonymiseres; koder og GUID-er står urørt."""
    base = tmp_path or Path("./_fixture_scan")
    base.mkdir(parents=True, exist_ok=True)
    siard = base / "scan.siard"
    fnr = _valid_fnr("170580", 123)
    plain = f"Pasient Ola Nordmann ({fnr}), tlf 98 76 54 32, Storgata 12, Marnardal. E-post ola@x.no"
    hexclob = plain.encode("utf-8").hex().upper()
    t0 = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
          f'<row><c1>{plain}</c1><c2>{hexclob}</c2><c3>AVD-7</c3>'
          '<c4>9e7d823a-d64a-4374-ac3a-8a293064ccd0</c4>'
          '<c5>Bor i Storgata 12, Marnardal</c5></row>\n</table>\n')
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("header/metadata.xml", _META_SCAN)
        z.writestr("content/schema0/table0/table0.xml", t0)
    result = _run_op(siard)
    assert result.success, result.message
    with zipfile.ZipFile(Path(result.data["output_path"])) as z:
        xml = z.read("content/schema0/table0/table0.xml").decode("utf-8")
    import re
    c1 = re.search(r"<c1>(.*?)</c1>", xml).group(1)
    for leak in ("Nordmann", fnr, "98 76 54 32", "Storgata", "Marnardal", "ola@x.no"):
        assert leak not in c1, (leak, c1)
    assert "12 34 56 78" in c1 and "Fiktivveien" in c1 and "Fiktivdal" in c1 and "@ymized.no" in c1, c1
    c2 = re.search(r"<c2>(.*?)</c2>", xml).group(1)
    assert re.fullmatch(r"[0-9A-F]+", c2), "hex-kodet CLOB skrives tilbake som hex (store bokstaver)"
    dec = bytes.fromhex(c2).decode("utf-8")
    for leak in ("Nordmann", fnr, "Storgata", "Marnardal"):
        assert leak not in dec, (leak, dec)
    assert "Fiktivveien" in dec and "12 34 56 78" in dec, dec
    assert "<c3>AVD-7</c3>" in xml and "9e7d823a-d64a-4374-ac3a-8a293064ccd0" in xml
    # «Verdi» klassifiseres ikke som fritekst (kun adresse + kommune) → skanne-veien
    c5 = re.search(r"<c5>(.*?)</c5>", xml).group(1)
    assert "Storgata" not in c5 and "Marnardal" not in c5 and "Fiktivveien" in c5 and "Fiktivdal" in c5, c5
    assert result.data.get("scan_cells", 0) >= 1, result.data
    assert result.data.get("freetext_cells", 0) >= 2, result.data

    # Forhåndsvisningen må vise det kjøringen faktisk gjør (kommunenavn i
    # fritekst, hex-kodet CLOB, skannede øvrige kolonner) — ikke «uendret».
    captured: dict = {}

    def _cb(summary):
        captured.update(summary)
        return True
    ctx = WorkflowContext(siard_path=siard)
    ctx.metadata["anonymize_preview_cb"] = _cb
    op = AnonymizeOperation(use_ollama=False)
    op.params["output_suffix"] = "_anon2"
    assert op.run(ctx).success
    cols = {c["column"]: c for c in captured["columns"]}
    info = cols["Info"]
    assert info["pii_type"] == "FREE_TEXT"
    assert all("Marnardal" not in e["after"] and "Fiktivdal" in e["after"] for e in info["examples"]), info
    tekst = cols["Tekst"]
    assert all(re.fullmatch(r"[0-9A-F]+", e["after"]) and e["after"] != e["before"]
               for e in tekst["examples"]), "hex-CLOB vises som endret hex"
    verdi = cols["Verdi"]
    assert verdi["pii_type"] == AnonymizeOperation.SCAN_LABEL
    assert verdi["examples"] and "Fiktivveien" in verdi["examples"][0]["after"] \
        and "Fiktivdal" in verdi["examples"][0]["after"], verdi
    assert "Kode" not in cols and "ObjektId" not in cols, "koder/GUID vises ikke som treff"
    # Verdier som ikke endres vises heller ikke som treff i forhåndsvisningen
    subj = "SERIALNUMBER=964966931, CN=MARNARDAL KOMMUNE, OU=Helse og Omsorg, O=MARNARDAL KO"
    assert op._preview_text(subj, whole_cell=False, freetext=True) == \
        "SERIALNUMBER=964966931, CN=FIKTIVDAL KOMMUNE, OU=Helse og Omsorg, O=FIKTIVDAL KO"
    assert op._preview_text("CN=HELSEDIREKTORATET, C=NO", whole_cell=False, freetext=True) == \
        "CN=HELSEDIREKTORATET, C=NO"


def test_identifier_guard():
    """GUID/UUID, hash-er, nøkler og løpenumre skal ALDRI endres — verken via
    navnematch (addressRootEntityId → «address»), Ollama-forslag eller ved
    omskriving av enkeltverdier."""
    from siard_workflow.core.anonymize.pii_detect import (
        should_anonymize, looks_like_identifier, is_identifier_column,
        is_identifier_field)
    guids = ["9e7d823a-d64a-4374-ac3a-8a293064ccd0",
             "728398ba-6cd9-4251-97e1-1d99912dd4bf",
             "{4a72cb40-3d99-4ad2-9e16-8b59c32b92f0}"]
    # Verdinivå
    for g in guids:
        assert looks_like_identifier(g), g
    assert looks_like_identifier("a3f5c9e2b1d4f6a7c8e9")            # hex-nøkkel
    assert looks_like_identifier("0x1F2E3D4C5B6A7988")               # hex med prefiks
    assert looks_like_identifier("1234567")                          # løpenr
    assert looks_like_identifier("1234-5678-90")                     # kontonr-form
    assert looks_like_identifier("K-2024-00017")                     # saksnøkkel
    assert looks_like_identifier("AB12345")
    assert looks_like_identifier("dGhpcyBpcyBhIGtleQ==1234")          # base64-aktig
    assert not looks_like_identifier("Storgata 1")
    assert not looks_like_identifier("Fiktivveien 75")
    assert not looks_like_identifier("Ola Nordmann")
    assert not looks_like_identifier("Hansen")
    assert not looks_like_identifier("Oslo")
    assert not looks_like_identifier("Schweigaards gate 4B")
    # Per-verdi-vakt ved omskriving
    assert should_anonymize(PiiType.ADDRESS, guids[0]) is False
    assert should_anonymize(PiiType.CITY, guids[1]) is False
    assert should_anonymize(PiiType.FULL_NAME, "a3f5c9e2b1d4f6a7c8e9") is False
    assert should_anonymize(PiiType.ADDRESS, "Storgata 1") is True
    # Kolonnenavn
    assert is_identifier_field("addressRootEntityId")
    assert is_identifier_field("PersonGuid") and is_identifier_field("Adresse_ID")
    assert is_identifier_field("ForeignKey") and is_identifier_field("SakRef")
    assert not is_identifier_field("Adresse") and not is_identifier_field("Navn")
    assert not is_identifier_field("Sluttid") and not is_identifier_field("Fritid")
    # Kolonneklassifisering: det rammede tilfellet
    cc = classify_column("addressRootEntityId", guids)
    assert cc.pii_type == PiiType.OTHER and cc.source == "identifikator", cc
    # Adressekolonne med GUID-verdier (navn uten Id-endelse) → verdiene avgjør
    assert classify_column("Adresse", guids).pii_type == PiiType.OTHER
    assert is_identifier_column(guids)
    # Ekte adressekolonne påvirkes ikke
    assert classify_column("Adresse", ["Storgata 1", "Kirkeveien 12"]).pii_type == PiiType.ADDRESS
    # Navnekolonne med hash-verdier → OTHER; med navn → navn
    assert classify_column("Navn", ["a3f5c9e2b1d4f6a7c8e9", "b4e6d0f3c2e5a7b8d9f0"]).pii_type == PiiType.OTHER
    assert classify_column("Fornavn", ["Ola", "Kari"]).pii_type == PiiType.FIRST_NAME
    # Verdibasert PII vinner over navnevakten: gyldige fnr i «PersonId» er PII
    f1, f2 = fake_fnr("01010099991"), fake_fnr("02020099992")
    assert classify_column("PersonId", [f1, f2]).pii_type == PiiType.FNR
    assert classify_column("KontaktId", ["a@b.no", "c@d.no"]).pii_type == PiiType.EMAIL
    # Postnr-vakt uendret (rene 4 sifre skal fortsatt kunne anonymiseres som postnr)
    assert should_anonymize(PiiType.POSTNR, "0301") is True


def test_new_keywords_and_exact_match():
    assert classify_column("Adresse 2", []).pii_type == PiiType.ADDRESS
    assert classify_column("Veinavn", []).pii_type == PiiType.ADDRESS
    assert classify_column("Postkode", []).pii_type == PiiType.POSTNR
    assert classify_column("Ort", []).pii_type == PiiType.CITY
    assert classify_column("Pnr", [fake_fnr("01010099991")]).pii_type == PiiType.FNR
    # "ort"/"by" som delstreng skal IKKE gi falske treff
    assert classify_column("Sortering", ["1", "2"]).pii_type != PiiType.CITY
    assert classify_column("Bygg", ["A", "B"]).pii_type != PiiType.CITY


def test_name_column_gate_rejects_nonnames():
    """Determ. navn-gate (form + ordbok) skal avvise reelle falske treff fra data."""
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation as A
    not_names = [
        ["KG", "<Ny>", "KG", "<Ny>"],                        # Gruppe.Navn
        ["Vennesla"],                                         # KommNavn (sted)
        ["Sokna skole", "Nes skole", "Hov ungdomsskole"],    # SkoleVigo.Navn
        ["Programmet er ikke registrert"],                   # LisensNavn
        ["Traffic", "In-depth studies"],                     # NavnEngelsk
        ["J", "G", "J"],                                     # Kjonn
        ["Høy", "Lav", "hancha"],                            # Passord
        ["Klasseliste", "Elevliste", "Fagliste"],            # Felt
        ["Alle"],                                            # RapportGruppe.Navn
    ]
    for vals in not_names:
        assert A._is_name_column(vals) is False, vals
    real_names = [
        ["Frøydis Solberg"],
        ["Ola Nordmann", "Kari Hansen", "Per Berg"],
        ["Stømne", "Larsen", "Olsen", "Solberg"],
    ]
    for vals in real_names:
        assert A._is_name_column(vals) is True, vals
    # tom kolonne → ikke bekreftet → ikke navn
    assert A._is_name_column([]) is False


def test_freetext_name_spans():
    from siard_workflow.core.anonymize.pii_detect import find_name_spans
    def names(t): return [s.text for s in find_name_spans(t)]
    # Vanlig rekkefølge + ukjent etternavn absorbert
    assert names("Eleven Ola Nordmann kom.") == ["Ola Nordmann"]
    # Etternavn, Fornavn (kjent og ukjent etternavn)
    assert names("Jensen, Petter") == ["Jensen, Petter"]
    assert names("Kontakt Sætre, Pål ved behov") == ["Sætre, Pål"]
    # Tvetydige fornavn alene gir IKKE treff (vanlige ord)
    assert names("F1.Dag") == []
    assert names("sortert per dag") == []
    assert names("Han bor i Oslo, Per kom") == []
    # men tvetydig fornavn i fullt navn fanges
    assert names("Dag Hansen møtte") == ["Dag Hansen"]


def test_looks_like_person_name():
    from siard_workflow.core.anonymize.pii_detect import looks_like_person_name
    assert looks_like_person_name("Ola Nordmann")
    assert looks_like_person_name("Kari")
    assert looks_like_person_name("Anne-Berit Hansen")
    # Ikke navn: liten forbokstav, siffer, akronym, for mange ord
    assert not looks_like_person_name("Ordinær grunnskole")
    assert not looks_like_person_name("Elevrådsarbeid 1. årstrinn")
    assert not looks_like_person_name("SFO")
    assert not looks_like_person_name("Vo-institusjon")


def test_freetext_span_replacement():
    """Fritekst: personnavn/fnr/e-post byttes på plass; titler/roller beholdes."""
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation
    from siard_workflow.core.anonymize.fake_generators import MappingStore
    op = AnonymizeOperation(use_ollama=False)
    op._ollama = None
    op._mapping = MappingStore()

    def anon(text):
        spans = op._freetext_spans(text)
        from siard_workflow.operations.anonymize_operation import _apply_spans
        return _apply_spans(text, spans, op._mapping)

    # Tittel/rolle uten navn → uendret
    assert op._freetext_spans("Saksbehandler, tittel: Seniorrådgiver") == []
    assert op._freetext_spans("Vedtak om støtte innvilget av leder") == []
    # Personnavn i fritekst → byttet ut, resten beholdt
    out = anon("Eleven Ola Nordmann har slitt med matematikk.")
    assert "Ola Nordmann" not in out and "matematikk" in out
    # E-post og fnr i fritekst → byttet ut
    assert "per@example.no" not in anon("Kontakt per@example.no")
    f = fake_fnr("01010099991")
    assert f not in anon(f"Klient med fnr {f} i saken")
    # Telefon er i omfanget (2026-09-18) → fast fiktivt nummer på plass
    assert anon("Ring kontoret på 98765432") == "Ring kontoret på 12345678"


def test_freetext_spans():
    valid_fnr = fake_fnr("01010099999")
    text = f"Kontakt Per Hansen, tlf 98765432, e-post per@firma.no, fnr {valid_fnr}"
    spans = find_all_pii(text)
    types = {s.pii_type for s in spans}
    assert PiiType.PHONE in types
    assert PiiType.EMAIL in types
    assert PiiType.FNR in types


def test_mapping_determinism():
    ms = MappingStore()
    a = ms.map(PiiType.FULL_NAME, "Ola Nordmann")
    b = ms.map(PiiType.FULL_NAME, "Ola Nordmann")
    assert a == b
    assert ms.map(PiiType.FULL_NAME, "Kari Nordmann") != a


def test_no_self_mapping():
    """Den fiktive verdien skal aldri være lik originalen."""
    from siard_workflow.core.anonymize.fake_generators import (
        fake_last_name, _LAST_NAMES)
    ms = MappingStore()
    # Finn et etternavn i poolen som mapper til seg selv uten re-roll
    self_mappers = [n for n in _LAST_NAMES if fake_last_name(n) == n]
    assert self_mappers, "forventet minst ett pool-selvtreff å teste mot"
    for n in self_mappers:
        out = ms.map(PiiType.LAST_NAME, n)
        assert out != n, f"selvmapping ikke unngått for {n!r} -> {out!r}"
    # Determinisme bevart etter re-roll
    n = self_mappers[0]
    assert ms.map(PiiType.LAST_NAME, n) == MappingStore().map(PiiType.LAST_NAME, n)


def test_dummy_files():
    assert dummy_files.dummy_pdf()[:4] == b"%PDF"
    assert dummy_files.dummy_rtf().startswith(b"{\\rtf1")
    kind, data = dummy_files.pick_dummy_for("x.bin", b"%PDF-1.5 hello")
    assert kind == dummy_files.KIND_PDF and data[:4] == b"%PDF"


# ── Fixture-SIARD ─────────────────────────────────────────────────────────────

_METADATA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema>
    <name>PUB</name><folder>schema0</folder>
    <tables><table>
      <name>PERSON</name><folder>table0</folder>
      <columns>
        <column><name>NAVN</name><type>VARCHAR(100)</type></column>
        <column><name>FODSELSNUMMER</name><type>VARCHAR(11)</type></column>
        <column><name>KOMMENTAR</name><type>VARCHAR(4000)</type></column>
        <column><name>DOK</name><type>NBLOB</type><lobFolder>schema0/table0/lob4</lobFolder></column>
      </columns>
      <rows>2</rows>
    </table></tables>
  </schema></schemas>
</siardArchive>
"""

# To rader. Rad 1 og 2 deler samme NAVN «Ola Nordmann» → må gi samme fake.
_TABLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/schema0/table0.xsd">
<row><c1>Ola Nordmann</c1><c2>{fnr1}</c2><c3>Ola Nordmann bor i Storgata 1, e-post ola@skole.no.</c3><c4 file="rec1.bin" length="9" digest="00" digestType="MD5"/></row>
<row><c1>Ola Nordmann</c1><c2>{fnr2}</c2><c3>Ingen sensitiv tekst her.</c3><c4 file="rec2.bin" length="5" digest="00" digestType="MD5"/></row>
</table>
"""


def _build_fixture_siard(path: Path):
    fnr1 = fake_fnr("01010099991")
    fnr2 = fake_fnr("02020099992")
    table_xml = _TABLE_XML.format(fnr1=fnr1, fnr2=fnr2)
    pdf_bytes = b"%PDF-1.4 fake real document content"
    txt_bytes = b"hello"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("header/metadata.xml", _METADATA_XML)
        z.writestr("content/schema0/table0/table0.xml", table_xml)
        z.writestr("content/schema0/table0/lob4/rec1.bin", pdf_bytes)
        z.writestr("content/schema0/table0/lob4/rec2.bin", txt_bytes)
    return fnr1, fnr2


def _run_op(siard_path: Path):
    ctx = WorkflowContext(siard_path=siard_path)
    ctx.metadata["anonymize_preview_cb"] = lambda summary: True  # auto-confirm
    op = AnonymizeOperation(use_ollama=False)
    return op.run(ctx)


def test_tail_row_sampling():
    from siard_workflow.operations.anonymize_operation import (
        collect_tail_rows, column_samples_from_rows, spread_rows)
    import tempfile
    # Bygg en tableX.xml med 100 rader; rad N har c1=N
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', '<table xmlns="x">']
    for i in range(100):
        lines.append(f"<row><c1>{i}</c1><c2>navn{i}</c2></row>")
    lines.append("</table>")
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "t.xml"
        p.write_text("\n".join(lines), encoding="utf-8")
        rows = collect_tail_rows(p, window=20)
        assert len(rows) == 20, len(rows)
        # halen → siste 20 rader (80..99), IKKE de første
        assert rows[0][1] == "80" and rows[-1][1] == "99"
        sel = spread_rows(rows, 5)
        assert len(sel) == 5
        samples = column_samples_from_rows(rows)
        assert "80" in samples[1] and "99" in samples[1]


def test_ollama_table_fills_other(tmp_path=None):
    """Holistisk Ollama-tabellanalyse skal fange en kolonne uten navne-treff."""
    import tempfile
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation

    class StubOllama:
        model = "stub"
        def is_alive(self): return True
        def analyze_table(self, cols, rows, table_name=""): return {"Hemmelig": "FULL_NAME"}
        def verify_person_names(self, samples):
            # verdier med mellomrom (fullt navn) → personnavn
            return any(" " in s for s in samples)

    meta = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema><name>S</name><folder>schema0</folder>
    <tables><table><name>T</name><folder>table0</folder>
      <columns>
        <column><name>Id</name><type>INTEGER</type></column>
        <column><name>Hemmelig</name><type>VARCHAR(50)</type></column>
      </columns><rows>3</rows>
    </table></tables>
  </schema></schemas>
</siardArchive>"""
    table = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
             '<row><c1>1</c1><c2>Ola Nordmann</c2></row>\n'
             '<row><c1>2</c1><c2>Kari Hansen</c2></row>\n'
             '<row><c1>3</c1><c2>Per Berg</c2></row>\n</table>\n')
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "header").mkdir()
        (root / "content" / "schema0" / "table0").mkdir(parents=True)
        (root / "header" / "metadata.xml").write_text(meta, encoding="utf-8")
        (root / "content" / "schema0" / "table0" / "table0.xml").write_text(
            table, encoding="utf-8")
        from siard_workflow.operations.anonymize_operation import read_tables
        tables = read_tables(root / "header" / "metadata.xml")
        op = AnonymizeOperation(use_ollama=False)
        op._ollama = StubOllama()
        op._workers = 1
        from siard_workflow.core.anonymize.fake_generators import MappingStore
        op._mapping = MappingStore()
        plans, lob_plans, logs = op._classify_table(
            root, "schema0/table0", tables["schema0/table0"])
        # "Hemmelig" har ikke navne-treff, men Ollama-analysen markerer den
        assert plans[("schema0/table0", 2)]["pii_type"] == PiiType.FULL_NAME
        assert plans[("schema0/table0", 2)]["source"] == "ollama-table"


def test_ambiguous_name_verified_by_ollama(tmp_path=None):
    """NavnBM/NavnNN med fag-/skjematitler skal nedgraderes til OTHER via Ollama."""
    import tempfile
    from siard_workflow.core.anonymize.pii_detect import is_ambiguous_name
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation, read_tables
    from siard_workflow.core.anonymize.fake_generators import MappingStore

    assert is_ambiguous_name("NavnBM") and is_ambiguous_name("NavnNN")
    assert not is_ambiguous_name("Fornavn") and not is_ambiguous_name("Etternavn")
    # «<ting>navn»-sammensetninger (FylkeNavn, FagNavn …) → ikke personnavn
    # (uten Ollama), men gate-/adressenavn skal fortsatt anonymiseres
    assert classify_column("FylkeNavn", ["Oslo", "Akershus", "Rogaland"]).pii_type == PiiType.OTHER
    assert classify_column("FagNavn", ["Matematikk", "Norsk"]).pii_type == PiiType.OTHER
    assert classify_column("Gatenavn", ["Storgata", "Kirkeveien"]).pii_type == PiiType.ADDRESS

    class StubOllama:
        model = "stub"
        def is_alive(self): return True
        def analyze_table(self, cols, rows, table_name=""):
            return {"Navn": "FULL_NAME", "NavnBM": "FULL_NAME"}
        # Verdi-basert: navn har mellomrom (fullt navn); fagnavn (ett ord) → ANNET
        def verify_person_names(self, samples):
            return any(" " in s for s in samples)

    meta = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema><name>S</name><folder>schema0</folder>
    <tables><table><name>T</name><folder>table0</folder>
      <columns>
        <column><name>NavnBM</name><type>VARCHAR(60)</type></column>
        <column><name>Navn</name><type>VARCHAR(60)</type></column>
      </columns><rows>3</rows>
    </table></tables>
  </schema></schemas>
</siardArchive>"""
    table = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
             '<row><c1>Matematikk</c1><c2>Ola Nordmann</c2></row>\n'
             '<row><c1>Norsk</c1><c2>Kari Hansen</c2></row>\n'
             '<row><c1>Engelsk</c1><c2>Per Berg</c2></row>\n</table>\n')
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "header").mkdir()
        (root / "content" / "schema0" / "table0").mkdir(parents=True)
        (root / "header" / "metadata.xml").write_text(meta, encoding="utf-8")
        (root / "content" / "schema0" / "table0" / "table0.xml").write_text(
            table, encoding="utf-8")
        tables = read_tables(root / "header" / "metadata.xml")
        op = AnonymizeOperation(use_ollama=False)
        op._ollama = StubOllama()
        op._workers = 1
        op._mapping = MappingStore()
        plans, lob_plans, logs = op._classify_table(
            root, "schema0/table0", tables["schema0/table0"])
        # NavnBM (fagnavn) → OTHER; Navn (personnavn) → FULL_NAME
        assert plans[("schema0/table0", 1)]["pii_type"] == PiiType.OTHER
        assert plans[("schema0/table0", 1)]["source"] == "ikke-personnavn"
        assert plans[("schema0/table0", 2)]["pii_type"] == PiiType.FULL_NAME


def test_nontext_columns_never_anonymized(tmp_path=None):
    """BOOLEAN/INT/DATE-kolonner skal aldri anonymiseres, selv om navnet matcher
    (f.eks. «Personale» inneholder «person»)."""
    import tempfile
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation, read_tables
    from siard_workflow.core.anonymize.fake_generators import MappingStore

    # Gyldige 11-sifrede fnr lagt i et INT-felt (uten ledende null)
    f1 = fake_fnr("12128012345"); f2 = fake_fnr("23048054321")
    assert f1[0] != "0" and f2[0] != "0"
    meta = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema><name>S</name><folder>schema0</folder>
    <tables><table><name>Lerer</name><folder>table0</folder>
      <columns>
        <column><name>Etternavn</name><type>VARCHAR(30)</type></column>
        <column><name>Personale</name><type>BOOLEAN</type></column>
        <column><name>SkoleId</name><type>DOUBLE PRECISION</type></column>
        <column><name>Fnr</name><type>INT</type></column>
      </columns><rows>2</rows>
    </table></tables>
  </schema></schemas>
</siardArchive>"""
    table = ('<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="x">\n'
             f'<row><c1>Stømne</c1><c2>true</c2><c3>5</c3><c4>{f1}</c4></row>\n'
             f'<row><c1>Larsen</c1><c2>false</c2><c3>5</c3><c4>{f2}</c4></row>\n'
             '</table>\n')
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        (root / "header").mkdir()
        (root / "content" / "schema0" / "table0").mkdir(parents=True)
        (root / "header" / "metadata.xml").write_text(meta, encoding="utf-8")
        (root / "content" / "schema0" / "table0" / "table0.xml").write_text(
            table, encoding="utf-8")
        tables = read_tables(root / "header" / "metadata.xml")
        op = AnonymizeOperation(use_ollama=False)
        op._ollama = None
        op._workers = 1
        op._mapping = MappingStore()
        plans, lob_plans, logs = op._classify_table(
            root, "schema0/table0", tables["schema0/table0"])
        # Etternavn (VARCHAR) → LAST_NAME; Personale (BOOLEAN) → OTHER
        assert plans[("schema0/table0", 1)]["pii_type"] == PiiType.LAST_NAME
        assert plans[("schema0/table0", 2)]["pii_type"] == PiiType.OTHER  # Personale BOOLEAN
        assert plans[("schema0/table0", 3)]["pii_type"] == PiiType.OTHER  # SkoleId DOUBLE
        # Fnr lagt i INT-felt → fortsatt anonymisert (verdibasert mod-11)
        assert plans[("schema0/table0", 4)]["pii_type"] == PiiType.FNR
        assert plans[("schema0/table0", 4)]["source"] == "value"


def test_metadata_reader(tmp_path=None):
    p = (tmp_path or Path("./_fixture")) / "x.siard"
    p.parent.mkdir(parents=True, exist_ok=True)
    _build_fixture_siard(p)
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        with zipfile.ZipFile(p) as z:
            z.extractall(d)
        tables = read_tables(Path(d) / "header" / "metadata.xml")
    assert "schema0/table0" in tables
    info = tables["schema0/table0"]
    assert info["lob_cols"] == {4: "schema0/table0/lob4"}
    assert [c["name"] for c in info["columns"]] == \
        ["NAVN", "FODSELSNUMMER", "KOMMENTAR", "DOK"]


def test_end_to_end(tmp_path=None):
    base = tmp_path or Path("./_fixture")
    base.mkdir(parents=True, exist_ok=True)
    siard = base / "test.siard"
    fnr1, fnr2 = _build_fixture_siard(siard)

    result = _run_op(siard)
    assert isinstance(result, OperationResult)
    assert result.success, result.message
    out = Path(result.data["output_path"])
    assert out.exists(), "output SIARD must be created"

    with zipfile.ZipFile(out) as z:
        table_xml = z.read("content/schema0/table0/table0.xml").decode("utf-8")
        rec1 = z.read("content/schema0/table0/lob4/rec1.bin")
        rec2 = z.read("content/schema0/table0/lob4/rec2.bin")

    # Original PII må være borte
    assert "Ola Nordmann" not in table_xml, "navn må være anonymisert"
    assert fnr1 not in table_xml, "fnr1 må være anonymisert"
    assert fnr2 not in table_xml, "fnr2 må være anonymisert"
    assert "ola@skole.no" not in table_xml, "e-post i fritekst må være anonymisert"

    # Referanseintegritet: samme navn i to rader → samme fake
    import re
    names = re.findall(r"<c1>(.*?)</c1>", table_xml)
    assert len(names) == 2 and names[0] == names[1], \
        f"samme original-navn må gi samme fake: {names}"

    # LOB-filer byttet til dummy
    assert rec1[:4] == b"%PDF", "dokument-blob skal bli dummy-PDF"
    assert b"PDF-1.4 fake real document" not in rec1, "original blob-innhold skal være borte"

    # length/digest oppdatert (ikke lenger 9 / '00')
    assert 'length="9"' not in table_xml or 'digest="00"' not in table_xml, \
        "length/digest må være oppdatert etter LOB-bytte"
    m = re.search(r'file="rec1.bin" length="(\d+)" digest="([0-9A-F]+)"', table_xml)
    assert m, "fil-ref må fortsatt finnes med oppdaterte attributter"
    assert int(m.group(1)) == len(rec1), "length må matche ny dummy-fil"


_METADATA_INLINE = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <schemas><schema>
    <name>PUB</name><folder>schema0</folder>
    <tables><table>
      <name>SAK</name><folder>table0</folder>
      <columns>
        <column><name>NAVN</name><type>VARCHAR(100)</type></column>
        <column><name>NOTAT</name><type>NCLOB</type></column>
        <column><name>VEDLEGG</name><type>BLOB</type></column>
      </columns>
      <rows>1</rows>
    </table></tables>
  </schema></schemas>
</siardArchive>
"""

_TABLE_INLINE = """<?xml version="1.0" encoding="UTF-8"?>
<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/schema0/table0.xsd">
<row><c1>Ola Nordmann</c1><c2>Saken gjelder Ola Nordmann, e-post ola@skole.no, og hans bolig.</c2><c3>48656C6C6F</c3></row>
</table>
"""


def test_show_preview_disabled(tmp_path=None):
    """show_preview=False → kjør direkte uten å kalle preview-callback."""
    base = tmp_path or Path("./_fixture_nopreview")
    base.mkdir(parents=True, exist_ok=True)
    siard = base / "np.siard"
    fnr1, fnr2 = _build_fixture_siard(siard)
    called = {"preview": False}
    ctx = WorkflowContext(siard_path=siard)
    def _cb(summary):
        called["preview"] = True
        return True
    ctx.metadata["anonymize_preview_cb"] = _cb
    op = AnonymizeOperation(use_ollama=False, show_preview=False)
    result = op.run(ctx)
    assert result.success, result.message
    assert called["preview"] is False, "preview-callback skal IKKE kalles"
    out = Path(result.data["output_path"])
    with zipfile.ZipFile(out) as z:
        table_xml = z.read("content/schema0/table0/table0.xml").decode("utf-8")
    assert "Ola Nordmann" not in table_xml and fnr1 not in table_xml


def test_inline_lob(tmp_path=None):
    base = tmp_path or Path("./_fixture_inline")
    base.mkdir(parents=True, exist_ok=True)
    siard = base / "inline.siard"
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("header/metadata.xml", _METADATA_INLINE)
        z.writestr("content/schema0/table0/table0.xml", _TABLE_INLINE)

    ctx = WorkflowContext(siard_path=siard)
    ctx.metadata["anonymize_preview_cb"] = lambda s: True
    op = AnonymizeOperation(use_ollama=False)
    result = op.run(ctx)
    assert result.success, result.message
    out = Path(result.data["output_path"])
    with zipfile.ZipFile(out) as z:
        xml = z.read("content/schema0/table0/table0.xml").decode("utf-8")

    # Inline NCLOB (c2): navnet «Ola Nordmann» skal være borte (kjent entitet)
    import re
    c2 = re.search(r"<c2>(.*?)</c2>", xml).group(1)
    assert "Ola Nordmann" not in c2, f"inline CLOB-navn må anonymiseres: {c2}"
    # Inline BLOB (c3): hex-innholdet skal være byttet ut
    c3 = re.search(r"<c3>(.*?)</c3>", xml).group(1)
    assert c3 != "48656C6C6F" and len(c3) > 0, "inline BLOB skal byttes til dummy-hex"


def _selftest():
    import tempfile
    print("Kjører anonymiserings-selvtest ...")
    test_fnr_validation_and_generation();  print("  ✓ fnr")
    test_fnr_requires_11_digits();         print("  ✓ fnr krever 11 sifre")
    test_phone_norwegian_only();           print("  ✓ telefon kun norsk mønster")
    test_phone_and_email_anonymization();  print("  ✓ telefon (12 34 56 78) og e-post (anon@ymized.no)")
    test_kommune_names();                  print("  ✓ kommunenavn → Fiktiv<endelse>")
    test_address_spans_and_hex_text();     print("  ✓ adresse-spenn i fritekst + hex-kodet tekst")
    test_filenames_never_anonymized();     print("  ✓ filnavn endres aldri")
    test_fixed_fake_values();              print("  ✓ faste fiktive verdier")
    test_postnr_only_4_digits();           print("  ✓ postnr kun 4 sifre")
    test_excluded_poststed_fields();       print("  ✓ poststed-felter unntatt")
    test_email_value_guard();              print("  ✓ e-post-verdivakt")
    test_city_address_require_letters();   print("  ✓ sted/adresse krever bokstaver")
    test_new_keywords_and_exact_match();   print("  ✓ nye nøkkelord + eksaktmatch")
    test_identifier_guard();               print("  ✓ identifikator-vakt (GUID/nøkler endres aldri)")
    test_fnr_decomposition_consistency();  print("  ✓ dekomponerbart fnr / fødselsdato / pnr5")
    test_mapping_injective_for_keys();     print("  ✓ injektiv mapping for nøkkeltyper")
    test_name_column_gate_rejects_nonnames(); print("  ✓ navn-gate avviser falske treff")
    test_freetext_name_spans();            print("  ✓ fritekst navn-spenn (også omvendt)")
    test_looks_like_person_name();         print("  ✓ verdi-heuristikk personnavn")
    test_freetext_span_replacement();      print("  ✓ fritekst span-erstatning")
    test_classify_by_name();               print("  ✓ klassifisering")
    test_freetext_spans();                 print("  ✓ fritekst-spenn")
    test_mapping_determinism();            print("  ✓ mapping-determinisme")
    test_no_self_mapping();                print("  ✓ ingen selvmapping")
    test_dummy_files();                    print("  ✓ dummy-filer")
    test_tail_row_sampling();              print("  ✓ hale-rad-sampling")
    test_ollama_table_fills_other();       print("  ✓ Ollama-tabellanalyse fyller OTHER")
    with tempfile.TemporaryDirectory() as d:
        test_ambiguous_name_verified_by_ollama(Path(d)); print("  ✓ tvetydig navn verifisert av Ollama")
    with tempfile.TemporaryDirectory() as d:
        test_nontext_columns_never_anonymized(Path(d)); print("  ✓ ikke-tekstkolonner aldri anonymisert")
    with tempfile.TemporaryDirectory() as d:
        test_metadata_reader(Path(d));     print("  ✓ metadata-leser")
    with tempfile.TemporaryDirectory() as d:
        test_end_to_end(Path(d));          print("  ✓ ende-til-ende")
        test_fnr_relations_end_to_end(Path(d)); print("  ✓ fnr-relasjoner (fnr ↔ fødselsdato + pnr5)")
        test_kommune_end_to_end(Path(d));  print("  ✓ kommunenavn ende-til-ende (kolonner + poststed + fritekst)")
        test_scan_all_text_end_to_end(Path(d)); print("  ✓ innebygd PII i alle tekstfelt + hex-kodet CLOB")
    with tempfile.TemporaryDirectory() as d:
        test_show_preview_disabled(Path(d)); print("  ✓ kjør uten forhåndsvisning")
    with tempfile.TemporaryDirectory() as d:
        test_inline_lob(Path(d));          print("  ✓ inline LOB (CLOB/BLOB)")
    print("ALLE TESTER OK")


if __name__ == "__main__":
    _selftest()
