"""
Tester for navngiving av resultat-SIARD fra «Pakk sammen SIARD».

  * Anonymisering med i pipelinen → filnavnet inneholder «_anonymisert»
    (et uttrekk med fiktive personopplysninger må ikke forveksles med originalen).
  * Uten anonymisering → kun konfigurert suffix (_konvertert).
  * Allerede «anonymisert» i navnet (standalone-kjøring tidligere i kjeden) →
    ikke dobbelt.
  * Kjente suffikser strippes ved utledning av basenavn (PREMIS/DIAS).

Kjør:  python -X utf8 tests/test_repack_naming.py
"""
from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.context import WorkflowContext
from siard_workflow.operations.pipeline_operations import RepackSiardOperation


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_META = ('<?xml version="1.0" encoding="UTF-8"?>\n'
         '<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">'
         '<dbname>t</dbname><schemas><schema><name>s</name><folder>schema0</folder><tables>'
         '<table><name>t0</name><folder>table0</folder><columns>'
         '<column><name>id</name><type>INTEGER</type></column></columns><rows>1</rows>'
         '</table></tables></schema></schemas></siardArchive>\n').encode("utf-8")
_TABLE = ('<?xml version="1.0" encoding="UTF-8"?>\n'
          '<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd" version="2.1">\n'
          '<row><c1>1</c1></row>\n</table>\n').encode("utf-8")


def _extracted(root: Path) -> None:
    (root / "header").mkdir(parents=True)
    (root / "header" / "metadata.xml").write_bytes(_META)
    (root / "content" / "schema0" / "table0").mkdir(parents=True)
    (root / "content" / "schema0" / "table0" / "table0.xml").write_bytes(_TABLE)


def _repack(tmp: Path, name: str, anonymized: bool) -> Path:
    root = tmp / f"ext_{name}_{anonymized}"
    _extracted(root)
    src = tmp / f"{name}.siard"
    src.write_bytes(b"")                       # kildefil trengs bare for navn/plassering
    ctx = WorkflowContext(siard_path=src, metadata={"file_logger": None, "progress_cb": None})
    ctx.extracted_path = root
    ctx.set_result("unpack_siard", {"original_namelist": []})
    if anonymized:
        ctx.set_result("anonymize", {"cells_anonymized": 3})
    res = RepackSiardOperation().run(ctx)
    assert res.success, res.message
    out = Path(res.data["output_path"])
    with zipfile.ZipFile(out) as zf:
        assert "header/metadata.xml" in zf.namelist()
    return out


def test_repack_naming(tmp: Path) -> None:
    print("test_repack_naming")
    _ok(_repack(tmp, "Uttrekk", False).name == "Uttrekk_konvertert.siard",
        "uten anonymisering → _konvertert")
    _ok(_repack(tmp, "Uttrekk2", True).name == "Uttrekk2_konvertert_anonymisert.siard",
        "med anonymisering → _konvertert_anonymisert")
    _ok(_repack(tmp, "Uttrekk_anonymisert", True).name == "Uttrekk_anonymisert_konvertert.siard",
        "allerede «anonymisert» i navnet → ikke dobbelt")


def test_base_name_strips_suffixes() -> None:
    print("test_base_name_strips_suffixes")
    from siard_workflow.core.premis_logger import _base_name
    _ok(_base_name(Path("A_konvertert_anonymisert.siard")) == "A",
        "PREMIS-basenavn stripper _anonymisert")
    _ok(_base_name(Path("A_hex_extracted_konvertert.siard")) == "A", "eksisterende suffikser som før")
    from gui.dias_dialog import _OP_SUFFIXES  # type: ignore
    _ok("_anonymisert" in _OP_SUFFIXES, "DIAS-dialogen kjenner _anonymisert")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="repack_naming_") as td:
        tmp = Path(td)
        test_repack_naming(tmp)
        test_base_name_strips_suffixes()
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
