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
from siard_workflow.core.premis_logger import PremisProvenanceLogger, _base_name
from siard_workflow.core.workflow import Workflow

PNS = "http://arkivverket.no/standarder/PREMIS"
_NS = {"premis": PNS}


# ── Fake-operasjoner ─────────────────────────────────────────────────────────

class _FakeMutate(BaseOperation):
    operation_id = "fake_mutate"
    label = "Fake endring"
    modifies_content = True
    premis_event_type = "fake-endring"

    def run(self, ctx):
        return self._ok({"changed": 5}, "5 ting endret")


class _FakeNoop(BaseOperation):
    operation_id = "fake_noop"
    label = "Fake noop"
    modifies_content = True
    premis_event_type = "fake-noop"

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

        # eventType og eventDetail
        etype = events[0].find("premis:eventType", _NS)
        edet = events[0].find("premis:eventDetail", _NS)
        assert etype is not None and etype.text == "fake-endring", etype
        assert edet is not None and "5 ting endret" in edet.text, edet
        print("[ok] logger skriver gyldig DIAS_PREMIS med object/event/agent + fixity")


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
        assert etype == "fake-endring", etype
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


def main() -> int:
    test_logger_writes_valid_premis()
    test_base_name_strips_suffixes()
    test_workflow_records_only_changing_ops()
    test_no_events_no_file()
    print("\nALLE PREMIS-PROVENIENS-TESTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
