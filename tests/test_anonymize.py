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
    # Telefon er utenfor omfanget → ikke klassifisert/anonymisert
    assert classify_column("Telefonnummer", []).pii_type == PiiType.OTHER
    assert classify_column("Brukernavn", ["per", "ola"]).pii_type != PiiType.FULL_NAME


def test_fnr_requires_11_digits():
    from siard_workflow.core.anonymize.pii_detect import should_anonymize
    # Kolonne med <11 sifre skal IKKE klassifiseres som fnr
    assert classify_column("PersonNr", ["46098", "34388", "26227"]).pii_type != PiiType.FNR
    assert classify_column("Foresatt1Personnr", ["12345"]).pii_type != PiiType.FNR
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
    assert fake_city("OSLO") == "Fiktivby"
    assert fake_email("ola@skole.no").endswith("@fiktivadresse.no")
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


def test_new_keywords_and_exact_match():
    assert classify_column("Adresse 2", []).pii_type == PiiType.ADDRESS
    assert classify_column("Veinavn", []).pii_type == PiiType.ADDRESS
    assert classify_column("Postkode", []).pii_type == PiiType.POSTNR
    assert classify_column("Ort", []).pii_type == PiiType.CITY
    assert classify_column("Pnr", [fake_fnr("01010099991")]).pii_type == PiiType.FNR
    # "ort"/"by" som delstreng skal IKKE gi falske treff
    assert classify_column("Sortering", ["1", "2"]).pii_type != PiiType.CITY
    assert classify_column("Bygg", ["A", "B"]).pii_type != PiiType.CITY


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


def test_freetext_direct_identifier_only():
    """Fritekst Lorem-ipsum-erstattes kun ved DIREKTE identifikator (uten Ollama)."""
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation
    op = AnonymizeOperation(use_ollama=False)
    op._ollama = None
    # Tittel/rolle uten direkte identifikator → IKKE identifiserende
    assert op._is_identifiable("Saksbehandler, tittel: Seniorrådgiver") is False
    assert op._is_identifiable("Vedtak om støtte innvilget av leder") is False
    # Telefon er UTENFOR omfanget → ikke en direkte identifikator
    assert op._is_identifiable("Ring kontoret på 98765432 ved spørsmål") is False
    # Direkte identifikator (epost/fnr) → identifiserende
    assert op._is_identifiable("Kontakt per@example.no") is True
    f = fake_fnr("01010099991")
    assert op._is_identifiable(f"Klient med fnr {f} har sak") is True


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
    test_filenames_never_anonymized();     print("  ✓ filnavn endres aldri")
    test_fixed_fake_values();              print("  ✓ faste fiktive verdier")
    test_postnr_only_4_digits();           print("  ✓ postnr kun 4 sifre")
    test_excluded_poststed_fields();       print("  ✓ poststed-felter unntatt")
    test_email_value_guard();              print("  ✓ e-post-verdivakt")
    test_new_keywords_and_exact_match();   print("  ✓ nye nøkkelord + eksaktmatch")
    test_looks_like_person_name();         print("  ✓ verdi-heuristikk personnavn")
    test_freetext_direct_identifier_only(); print("  ✓ fritekst kun ved direkte id")
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
    with tempfile.TemporaryDirectory() as d:
        test_show_preview_disabled(Path(d)); print("  ✓ kjør uten forhåndsvisning")
    with tempfile.TemporaryDirectory() as d:
        test_inline_lob(Path(d));          print("  ✓ inline LOB (CLOB/BLOB)")
    print("ALLE TESTER OK")


if __name__ == "__main__":
    _selftest()
