"""
Test for PREMIS-proveniens av SIARD-bearbeiding.

Verifiserer at:
  1. PremisProvenanceLogger skriver en gyldig DIAS_PREMIS v2.0-fil med ett
     premis:object, riktig antall premis:event og ett premis:agent.
  2. Original SHA256 fra konteksten havner som premis:fixity på objektet.
  3. Workflow.execute() fører kun innholdsendrende operasjoner — lesende steg
     og endrende steg som rapporterer «ingen endring» (premis_should_record =
     False) utelates.
  4. Bryteren enable_premis_provenance=False slår av loggingen.

Kjør:  python -X utf8 tests/test_premis_provenance.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from xml.etree import ElementTree as ET

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.base_operation import BaseOperation
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.premis_logger import (
    PremisProvenanceLogger, _base_name, VALID_EVENT_TYPES, DEFAULT_EVENT_TYPE,
)
from siard_workflow.core.workflow import Workflow

PNS = "http://arkivverket.no/standarder/PREMIS"
_NS = {"premis": PNS}

_SOURCES = _ROOT / "siard_workflow" / "sources"
_PREMIS_XSD = _SOURCES / "DIAS_PREMIS.xsd"
_XLINK_XSD  = _SOURCES / "xlink.xsd"


# ── XSD-validering (offline) ──────────────────────────────────────────────────

def _load_schema():
    """
    Last DIAS_PREMIS.xsd som lxml.XMLSchema. DIAS_PREMIS importerer xlink fra
    en loc.gov-URL; en lokal resolver peker den til sources/xlink.xsd slik at
    valideringen kjører helt uten nett.
    """
    from lxml import etree

    class _LocalResolver(etree.Resolver):
        def resolve(self, url, pubid, context):
            if url and url.rstrip("/").endswith("xlink.xsd"):
                return self.resolve_filename(str(_XLINK_XSD), context)
            return None

    parser = etree.XMLParser(no_network=True)
    parser.resolvers.add(_LocalResolver())
    xsd_doc = etree.parse(str(_PREMIS_XSD), parser)
    return etree.XMLSchema(xsd_doc)


def _assert_validates(xml_path: Path) -> None:
    from lxml import etree
    schema = _load_schema()
    doc = etree.parse(str(xml_path))
    if not schema.validate(doc):
        msgs = "\n".join(str(e) for e in schema.error_log)
        raise AssertionError(f"PREMIS validerer IKKE mot DIAS_PREMIS.xsd:\n{msgs}")


# ── Fake-operasjoner ─────────────────────────────────────────────────────────

class _FakeMutate(BaseOperation):
    operation_id = "fake_mutate"
    label = "Fake endring"
    modifies_content = True
    premis_event_type  = "Migration"
    premis_event_label = "fake-endring"

    def run(self, ctx):
        return self._ok({"changed": 5}, "5 ting endret")


class _FakeNoop(BaseOperation):
    operation_id = "fake_noop"
    label = "Fake noop"
    modifies_content = True
    premis_event_type  = "Adjustment"
    premis_event_label = "fake-noop"

    def premis_should_record(self, result, ctx) -> bool:
        return False

    def run(self, ctx):
        return self._ok({}, "ingen endring")


class _FakeReadonly(BaseOperation):
    operation_id = "fake_ro"
    label = "Fake lesing"

    def run(self, ctx):
        return self._ok({}, "lest, ingen endring")


# ── Tester ───────────────────────────────────────────────────────────────────

def test_logger_writes_valid_premis():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        siard = d / "Uttrekk_konvertert.siard"
        siard.write_bytes(b"PK\x03\x04dummy")

        ctx = WorkflowContext(siard_path=siard)
        ctx.results["sha256"] = "abc123def456"

        pl = PremisProvenanceLogger(d, siard, agent_version="1.3.6")
        op = _FakeMutate()
        pl.record(op, op.run(ctx), ctx)
        pl.record(op, op.run(ctx), ctx)
        out = pl.finalize(siard, ctx)

        assert out is not None and out.exists(), "premis-fil ble ikke skrevet"
        # base-navn skal være strippet for _konvertert-suffiks
        assert out.name == "Uttrekk_premis.xml", out.name

        root = ET.parse(out).getroot()
        objs = root.findall("premis:object", _NS)
        events = root.findall("premis:event", _NS)
        agents = root.findall("premis:agent", _NS)
        assert len(objs) == 1, f"forventet 1 object, fikk {len(objs)}"
        assert len(events) == 2, f"forventet 2 events, fikk {len(events)}"
        assert len(agents) == 1, f"forventet 1 agent, fikk {len(agents)}"

        # Fixity fra SHA256 i konteksten
        digest = root.find(".//premis:fixity/premis:messageDigest", _NS)
        assert digest is not None and digest.text == "abc123def456", "mangler fixity"

        # eventType skal være en gyldig DIAS-enum; den beskrivende kategorien
        # (label) bevares i eventDetail.
        etype = events[0].find("premis:eventType", _NS)
        edet = events[0].find("premis:eventDetail", _NS)
        assert etype is not None and etype.text == "Migration", etype
        assert edet is not None and "fake-endring" in edet.text, edet
        assert "5 ting endret" in edet.text, edet

        # Validér mot XSD
        _assert_validates(out)
        print("[ok] logger skriver gyldig DIAS_PREMIS med object/event/agent + fixity")
        print("[ok] skrevet _premis.xml validerer mot DIAS_PREMIS.xsd")


def test_base_name_strips_suffixes():
    assert _base_name(Path("A_hex_extracted_konvertert.siard")) == "A"
    assert _base_name(Path("B.siard")) == "B"
    print("[ok] _base_name stripper kjente suffikser")


def test_workflow_records_only_changing_ops():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        siard = d / "Test.siard"
        siard.write_bytes(b"PK\x03\x04dummy")

        wf = (Workflow("PremisTest")
              .add(_FakeReadonly())   # lesende → ikke ført
              .add(_FakeMutate())     # endrende → ført
              .add(_FakeNoop()))      # endrende, men premis_should_record=False
        wf.execute(siard, verbose=False)

        premis = d / "Test_premis.xml"
        assert premis.exists(), "premis-fil mangler etter workflow"
        root = ET.parse(premis).getroot()
        events = root.findall("premis:event", _NS)
        assert len(events) == 1, f"forventet 1 event, fikk {len(events)}"
        etype = events[0].find("premis:eventType", _NS).text
        assert etype == "Migration", etype
        obj_val = root.find(".//premis:objectIdentifierValue", _NS).text
        assert obj_val == "Test.siard", obj_val
        print("[ok] workflow fører kun innholdsendrende operasjoner")


def test_no_events_no_file():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        siard = d / "Tom.siard"
        siard.write_bytes(b"PK\x03\x04dummy")

        wf = Workflow("PremisTomTest").add(_FakeReadonly())
        wf.execute(siard, verbose=False)

        # Ingen endrende steg → ingen premis-fil
        assert not (d / "Tom_premis.xml").exists(), "premis-fil burde ikke finnes"
        print("[ok] ingen premis-fil når ingen endringer skjer")


def test_all_operations_use_valid_event_type():
    """
    Alle innholdsendrende operasjoner (modifies_content=True) må deklarere en
    premis_event_type som er en gyldig DIAS_PREMIS-enumverdi.
    """
    import importlib
    import pkgutil
    import siard_workflow.operations as ops_pkg

    bad: list[str] = []
    checked = 0
    for mod in pkgutil.iter_modules(ops_pkg.__path__):
        m = importlib.import_module(f"{ops_pkg.__name__}.{mod.name}")
        for attr in vars(m).values():
            if (isinstance(attr, type) and issubclass(attr, BaseOperation)
                    and attr is not BaseOperation
                    and getattr(attr, "modifies_content", False)):
                etype = getattr(attr, "premis_event_type", "")
                checked += 1
                if etype not in VALID_EVENT_TYPES:
                    bad.append(f"{attr.__name__}: {etype!r}")
    assert not bad, "Ugyldig premis_event_type i: " + ", ".join(bad)
    assert checked >= 8, f"forventet å sjekke ≥8 operasjoner, sjekket {checked}"
    print(f"[ok] {checked} innholdsendrende operasjoner har gyldig eventType-enum")


def test_invalid_event_type_falls_back_and_validates():
    """En operasjon med ugyldig (legacy) eventType skal falle tilbake til
    DEFAULT_EVENT_TYPE, bevare teksten i eventDetail, og fortsatt validere."""
    class _LegacyOp(BaseOperation):
        operation_id = "legacy_op"
        label = "Legacy"
        modifies_content = True
        premis_event_type = "fri-kategoritekst"     # ugyldig enum

        def run(self, ctx):
            return self._ok({}, "gjorde noe")

    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        siard = d / "Legacy.siard"
        siard.write_bytes(b"PK\x03\x04dummy")
        ctx = WorkflowContext(siard_path=siard)
        pl = PremisProvenanceLogger(d, siard, agent_version="1.0")
        op = _LegacyOp()
        pl.record(op, op.run(ctx), ctx)
        out = pl.finalize(siard, ctx)

        root = ET.parse(out).getroot()
        ev = root.find("premis:event", _NS)
        etype = ev.find("premis:eventType", _NS).text
        edet = ev.find("premis:eventDetail", _NS).text
        assert etype == DEFAULT_EVENT_TYPE, etype
        assert "fri-kategoritekst" in edet, edet
        _assert_validates(out)
        print("[ok] ugyldig eventType faller tilbake til "
              f"{DEFAULT_EVENT_TYPE} og validerer (tekst bevart i eventDetail)")


def main() -> int:
    test_logger_writes_valid_premis()
    test_base_name_strips_suffixes()
    test_workflow_records_only_changing_ops()
    test_no_events_no_file()
    test_all_operations_use_valid_event_type()
    test_invalid_event_type_falls_back_and_validates()
    print("\nALLE PREMIS-PROVENIENS-TESTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
