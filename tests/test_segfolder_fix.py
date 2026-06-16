"""
Tester for SegFolderFixOperation — fjerning av SIARD 2.2 LOB-segmentering
(seg_N-mapper) ved nedgradering til SIARD 2.1.

Verifiserer:
  1. scan_segfolder_issues() oppdager seg-mapper og _part-filer i en .siard-zip.
  2. _strip_seg_from_path() fjerner seg_N-ledd og bevarer øvrige ledd.
  3. Pipeline-modus: filer flyttes ut av seg-mapper, seg-mappene slettes og
     file=-referansene i tableX.xml patches — relativ kolonnesti bevares.
  4. Mål-versjon 2.2 gir trygt no-op.
  5. Standalone-modus: skriver gyldig <original>_segfix.siard uten seg-mapper.

Kjør:  python -X utf8 tests/test_segfolder_fix.py
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import settings
from siard_workflow.core.context import WorkflowContext
from siard_workflow.operations.segfolder_fix_operation import (
    SegFolderFixOperation, scan_segfolder_issues, _strip_seg_from_path,
)


# ── Testdata-bygging ──────────────────────────────────────────────────────────

# Kolonnemappe med to seg-mapper (file number/size-grense som i spec-eksempelet)
_SEG_LAYOUT = {
    "content/schema0/table2/lob4/seg_0/t2_c4_r1.bin": b"BLOB-1",
    "content/schema0/table2/lob4/seg_0/t2_c4_r2.bin": b"BLOB-2",
    "content/schema0/table2/lob4/seg_1/t2_c4_r5.bin": b"BLOB-5",
    "content/schema0/table2/lob4/seg_1/t2_c4_r8.bin": b"BLOB-8",
}

# tableX.xml med file=-referanser som inneholder seg-ledd, relativt til
# kolonnens lobFolder (lob4/). Etter retting skal seg-leddet være borte.
_TABLE_XML = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b'<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd" version="2.2">\n'
    b'<row><c1>1</c1><c4 file="lob4/seg_0/t2_c4_r1.bin" length="6"/></row>\n'
    b'<row><c1>2</c1><c4 file="lob4/seg_0/t2_c4_r2.bin" length="6"/></row>\n'
    b'<row><c1>5</c1><c4 file="lob4/seg_1/t2_c4_r5.bin" length="6"/></row>\n'
    b'<row><c1>8</c1><c4 file="lob4/seg_1/t2_c4_r8.bin" length="6"/></row>\n'
    b'</table>\n'
)

_METADATA_XML = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b'<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"'
    b' version="2.2"><lobFolder>content</lobFolder></siardArchive>\n'
)


def _build_extracted(root: Path) -> None:
    """Bygg en utpakket SIARD-struktur med seg-mapper på disk."""
    for rel, data in _SEG_LAYOUT.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    (root / "content" / "schema0" / "table2" / "table2.xml").write_bytes(_TABLE_XML)
    (root / "header").mkdir(parents=True, exist_ok=True)
    (root / "header" / "metadata.xml").write_bytes(_METADATA_XML)


def _build_siard_zip(path: Path) -> None:
    """Bygg en .siard-zip med seg-mapper."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel, data in _SEG_LAYOUT.items():
            zf.writestr(rel, data)
        zf.writestr("content/schema0/table2/table2.xml", _TABLE_XML)
        zf.writestr("header/metadata.xml", _METADATA_XML)


# ── Hjelpere ──────────────────────────────────────────────────────────────────

class _CfgPatch:
    """Midlertidig overstyring av siard_output_version i settings."""
    def __init__(self, version: str):
        self.version = version
        self._orig = None

    def __enter__(self):
        self._orig = settings.get_config
        ver = self.version

        def _patched(key, default=None):
            if key == "siard_output_version":
                return ver
            return self._orig(key, default)
        settings.get_config = _patched
        # siard_format importerer get_config fra settings ved kall — patch der
        import siard_workflow.core.siard_format as sf
        sf.get_config = _patched  # type: ignore[attr-defined]
        return self

    def __exit__(self, *a):
        settings.get_config = self._orig
        import siard_workflow.core.siard_format as sf
        sf.get_config = self._orig  # type: ignore[attr-defined]


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


# ── Tester ────────────────────────────────────────────────────────────────────

def test_strip_helper() -> None:
    print("test_strip_helper")
    _ok(_strip_seg_from_path(b"lob4/seg_0/t2_c4_r1.bin") == b"lob4/t2_c4_r1.bin",
        "seg_0-ledd fjernet, lob4 bevart")
    _ok(_strip_seg_from_path(b"seg_2/t2_c4_r8.bin") == b"t2_c4_r8.bin",
        "ledende seg-ledd fjernet")
    _ok(_strip_seg_from_path(b"lob4/record_seg_value.bin")
        == b"lob4/record_seg_value.bin",
        "delstreng 'seg' i filnavn røres ikke")
    _ok(_strip_seg_from_path(b"lob4/seg0/x.bin") == b"lob4/x.bin",
        "underscore-løs seg0 også fjernet")


def test_scan(tmp: Path) -> None:
    print("test_scan")
    siard = tmp / "scan.siard"
    _build_siard_zip(siard)
    issues = scan_segfolder_issues(siard)
    _ok(len(issues) >= 1 and "seg-mappe" in issues[0],
        "scan oppdager seg-mapper")
    # Ren 2.1-fil uten seg → ingen funn
    plain = tmp / "plain.siard"
    with zipfile.ZipFile(plain, "w") as zf:
        zf.writestr("content/schema0/table0/table0.xml", b"<table/>")
    _ok(scan_segfolder_issues(plain) == [], "scan tom for fil uten seg-mapper")


def test_pipeline(tmp: Path) -> None:
    print("test_pipeline (mål 2.1)")
    extract = tmp / "extract"
    _build_extracted(extract)

    ctx = WorkflowContext(siard_path=tmp / "dummy.siard")
    ctx.extracted_path = extract

    with _CfgPatch("2.1"):
        res = SegFolderFixOperation().run(ctx)

    _ok(res.success, "operasjon vellykket")
    _ok(res.data["files_moved"] == 4, f"4 filer flyttet (fikk {res.data['files_moved']})")
    _ok(res.data["seg_removed"] == 2, f"2 seg-mapper fjernet (fikk {res.data['seg_removed']})")

    lob4 = extract / "content" / "schema0" / "table2" / "lob4"
    _ok(not (lob4 / "seg_0").exists(), "seg_0-mappe slettet")
    _ok(not (lob4 / "seg_1").exists(), "seg_1-mappe slettet")
    for rel in ("t2_c4_r1.bin", "t2_c4_r2.bin", "t2_c4_r5.bin", "t2_c4_r8.bin"):
        _ok((lob4 / rel).exists(), f"{rel} flyttet opp til kolonnemappe")
    _ok((lob4 / "t2_c4_r1.bin").read_bytes() == b"BLOB-1", "filinnhold bevart")

    xml = (extract / "content" / "schema0" / "table2" / "table2.xml").read_bytes()
    _ok(b"seg_0" not in xml and b"seg_1" not in xml, "seg-ledd fjernet fra XML")
    _ok(b'file="lob4/t2_c4_r1.bin"' in xml, "file=-referanse patchet (kolonnesti bevart)")
    _ok(res.data["xml_updated"] == 4, f"4 XML-referanser oppdatert (fikk {res.data['xml_updated']})")


def test_noop_22(tmp: Path) -> None:
    print("test_noop (mål 2.2)")
    extract = tmp / "extract22"
    _build_extracted(extract)
    ctx = WorkflowContext(siard_path=tmp / "dummy.siard")
    ctx.extracted_path = extract
    with _CfgPatch("2.2"):
        res = SegFolderFixOperation().run(ctx)
    _ok(res.success and res.data["files_moved"] == 0, "2.2: ingen endring (no-op)")
    _ok((extract / "content" / "schema0" / "table2" / "lob4" / "seg_0").exists(),
        "2.2: seg-mappe beholdt")


def test_standalone(tmp: Path) -> None:
    print("test_standalone (mål 2.1)")
    siard = tmp / "arkiv.siard"
    _build_siard_zip(siard)
    ctx = WorkflowContext(siard_path=siard)
    with _CfgPatch("2.1"):
        res = SegFolderFixOperation().run(ctx)
    _ok(res.success, "standalone vellykket")
    out = Path(res.data["output_path"])
    _ok(out.exists() and out.name == "arkiv_segfix.siard", "ny _segfix.siard skrevet")

    with zipfile.ZipFile(out) as zf:
        names = zf.namelist()
        _ok(not any("seg_0" in n or "seg_1" in n for n in names),
            "ingen seg-mapper i ny zip")
        _ok("content/schema0/table2/lob4/t2_c4_r1.bin" in names,
            "LOB flyttet opp i ny zip")
        xml = zf.read("content/schema0/table2/table2.xml")
        _ok(b"seg_0" not in xml and b'file="lob4/t2_c4_r1.bin"' in xml,
            "XML i ny zip patchet")


def main() -> None:
    import tempfile
    test_strip_helper()
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_scan(tmp)
        test_pipeline(tmp)
        test_noop_22(tmp)
        test_standalone(tmp)
    print("\nAlle tester bestått ✓")


if __name__ == "__main__":
    main()
