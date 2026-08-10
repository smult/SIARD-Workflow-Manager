"""
Tester for eksterne SIARD-fillager (ekstern lobFolder) — siard_workflow.core.external_lob
og integrasjon i Unpack/Repack.

Verifiserer:
  1. Hjelpere: read_db_lobfolder / is_external_lobfolder / resolve_external_base.
  2. internalize(): eksterne LOB-filer kopieres inn i content/ og db-nivå
     <lobFolder> settes til 'content'.
  3. Full runde: UnpackSiardOperation internaliserer et eksternt arkiv, og
     RepackSiardOperation (intern) gir et selvstendig arkiv med LOB-ene i ZIP-en.
  4. RepackSiardOperation (ekstern) legger LOB-ene i søstermappe, holder dem ute
     av ZIP-en, peker <lobFolder> eksternt og bevarer at hver file= løser opp.

Kjør:  python -X utf8 tests/test_external_lob.py
"""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import settings
from siard_workflow.core import external_lob
from siard_workflow.core.context import WorkflowContext
from siard_workflow.operations.pipeline_operations import (
    UnpackSiardOperation, RepackSiardOperation,
)


# ── Testdata ──────────────────────────────────────────────────────────────────

# Cellene refererer LOB-ene med basisnavn, relativt til kolonnens lobFolder
# (schema0/table2/lob4). Ingen seg-mapper her — det er SegFolderFix sitt domene.
_TABLE_XML = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b'<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd" version="2.1">\n'
    b'<row><c1>1</c1><c4 file="rec1.bin" length="6"/></row>\n'
    b'<row><c1>2</c1><c4 file="rec2.bin" length="6"/></row>\n'
    b'</table>\n'
)

# metadata.xml med EKSTERN db-nivå lobFolder (WinMed-konvensjon).
_METADATA_EXTERNAL = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b'<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"'
    b' version="2.1">\n'
    b'\t<dbname>test</dbname>\n'
    b'\t<lobFolder>..\\arkiv.siard_documents\\content</lobFolder>\n'
    b'\t<schemas>\n\t\t<schema>\n\t\t\t<name>schema0</name>\n'
    b'\t\t\t<folder>schema0</folder>\n\t\t</schema>\n\t</schemas>\n'
    b'</siardArchive>\n'
)

_LOB_FILES = {
    "content/schema0/table2/lob4/rec1.bin": b"BLOB-1",
    "content/schema0/table2/lob4/rec2.bin": b"BLOB-2",
}


def _build_external_siard(tmp: Path) -> Path:
    """
    Bygg et eksternt SIARD-arkiv: arkiv.siard (uten LOB-er) + søstermappen
    arkiv.siard_documents/content/... med LOB-filene.
    """
    siard = tmp / "arkiv.siard"
    with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("header/metadata.xml", _METADATA_EXTERNAL)
        zf.writestr("content/schema0/table2/table2.xml", _TABLE_XML)

    docs = tmp / "arkiv.siard_documents"
    for rel, data in _LOB_FILES.items():
        p = docs / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return siard


class _CfgPatch:
    """Midlertidig overstyring av siard_output_version i settings."""
    def __init__(self, version: str = "2.1"):
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

def test_helpers() -> None:
    print("test_helpers")
    _ok(external_lob.read_db_lobfolder(_METADATA_EXTERNAL)
        == "..\\arkiv.siard_documents\\content", "db-lobFolder lest ut")
    _ok(external_lob.is_external_lobfolder("..\\x\\content"), "backslash+.. = eksternt")
    _ok(external_lob.is_external_lobfolder("C:/store/content"), "drev-sti = eksternt")
    _ok(not external_lob.is_external_lobfolder("content"), "'content' = internt")
    _ok(not external_lob.is_external_lobfolder("schema0/table1/lob4"),
        "kolonne-sti = internt")
    _ok(not external_lob.is_external_lobfolder(""), "tom = internt")


def test_resolve(tmp: Path) -> None:
    print("test_resolve")
    docs = tmp / "arkiv.siard_documents" / "content"
    docs.mkdir(parents=True, exist_ok=True)
    base = external_lob.resolve_external_base(
        tmp / "arkiv.siard", "..\\arkiv.siard_documents\\content")
    _ok(base == docs.resolve(), f"ekstern base løst korrekt ({base})")


def test_internalize(tmp: Path) -> None:
    print("test_internalize")
    extract = tmp / "extract"
    (extract / "content" / "schema0" / "table2").mkdir(parents=True)
    (extract / "content" / "schema0" / "table2" / "table2.xml").write_bytes(_TABLE_XML)
    (extract / "header").mkdir(parents=True)
    (extract / "header" / "metadata.xml").write_bytes(_METADATA_EXTERNAL)
    # ekstern søstermappe
    docs = tmp / "arkiv.siard_documents"
    for rel, data in _LOB_FILES.items():
        p = docs / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    stats = external_lob.internalize(extract, tmp / "arkiv.siard")
    _ok(stats["external_lobs_imported"] == 2,
        f"2 eksterne LOB-er importert (fikk {stats['external_lobs_imported']})")
    lob = extract / "content" / "schema0" / "table2" / "lob4"
    _ok((lob / "rec1.bin").read_bytes() == b"BLOB-1", "rec1.bin kopiert inn m/innhold")
    _ok((lob / "rec2.bin").exists(), "rec2.bin kopiert inn")
    meta = (extract / "header" / "metadata.xml").read_bytes()
    _ok(external_lob.read_db_lobfolder(meta) == "content",
        "db-lobFolder skrevet om til 'content'")


def test_pipeline_intern(tmp: Path) -> None:
    print("test_pipeline_intern (Unpack → Repack intern)")
    siard = _build_external_siard(tmp)
    ctx = WorkflowContext(siard_path=siard)

    with _CfgPatch("2.1"):
        res_u = UnpackSiardOperation().run(ctx)
        _ok(res_u.success, "Unpack vellykket")
        _ok(res_u.data.get("external_lobs_imported") == 2,
            "Unpack importerte 2 eksterne LOB-er")
        # LOB-ene skal nå ligge internt i den utpakkede mappen
        lob = ctx.extracted_path / "content" / "schema0" / "table2" / "lob4"
        _ok((lob / "rec1.bin").exists(), "LOB intern i utpakket mappe")

        res_r = RepackSiardOperation().run(ctx)
        _ok(res_r.success, "Repack (intern) vellykket")
        out = Path(res_r.data["output_path"])
        _ok(out.exists(), f"ny SIARD skrevet: {out.name}")

    with zipfile.ZipFile(out) as zf:
        names = set(zf.namelist())
        _ok("content/schema0/table2/lob4/rec1.bin" in names,
            "LOB ligger i ZIP-en (internt)")
        meta = zf.read("header/metadata.xml")
        _ok(external_lob.read_db_lobfolder(meta) == "content",
            "db-lobFolder = 'content' i resultat")
    _ok(not (out.parent / (out.name + "_documents")).exists(),
        "ingen søstermappe ved intern lagring")


def test_pipeline_ekstern(tmp: Path) -> None:
    print("test_pipeline_ekstern (Unpack → Repack ekstern)")
    siard = _build_external_siard(tmp)
    ctx = WorkflowContext(siard_path=siard)

    with _CfgPatch("2.1"):
        UnpackSiardOperation().run(ctx)
        op = RepackSiardOperation()
        op.params = {**op.default_params, "lob_storage": "ekstern"}
        res_r = op.run(ctx)
        _ok(res_r.success, "Repack (ekstern) vellykket")
        out = Path(res_r.data["output_path"])

    docs = out.parent / (out.name + "_documents")
    ext_lob = docs / "content" / "schema0" / "table2" / "lob4" / "rec1.bin"
    _ok(ext_lob.read_bytes() == b"BLOB-1", "LOB skrevet til søstermappe m/innhold")

    with zipfile.ZipFile(out) as zf:
        names = set(zf.namelist())
        _ok(not any("/lob4/" in n for n in names), "ingen LOB-filer i ZIP-en")
        _ok("content/schema0/table2/table2.xml" in names, "tabell-XML fortsatt i ZIP")
        meta = zf.read("header/metadata.xml")
        val = external_lob.read_db_lobfolder(meta)
        _ok(val is not None and external_lob.is_external_lobfolder(val),
            f"db-lobFolder peker eksternt ({val})")

    # Hver file= skal løse opp mot en eksisterende ekstern fil
    with zipfile.ZipFile(out) as zf:
        table_xml = zf.read("content/schema0/table2/table2.xml").decode("utf-8")
    import re
    refs = re.findall(r'file="([^"]+)"', table_xml)
    col_lobfolder = "schema0/table2/lob4"   # fra metadata (kolonne-nivå)
    base = external_lob.resolve_external_base(out, val)
    for ref in refs:
        target = base / col_lobfolder / ref
        _ok(target.exists(), f"file=\"{ref}\" løser til ekstern fil")


def test_collect_external_standalone(tmp: Path) -> None:
    print("test_collect_external_standalone (blob-convert uten unpack)")
    from collections import defaultdict
    from siard_workflow.operations.blob_convert_operation import _collect_external_blobs

    siard = tmp / "arkiv.siard"   # trenger ikke eksistere for denne enheten
    # Utpakket arkiv slik standalone blob-convert ser det: metadata m/ EKSTERN
    # db-lobFolder + table-xml som refererer seg-fil, men INGEN LOB-filer på disk.
    ext = tmp / "extracted"
    (ext / "header").mkdir(parents=True)
    (ext / "header" / "metadata.xml").write_bytes(_METADATA_EXTERNAL)
    tdir = ext / "content" / "schema0" / "table2"
    tdir.mkdir(parents=True)
    (tdir / "table2.xml").write_bytes(
        b'<table><row><c4 file="seg5/rec5647.txt" length="6"/></row></table>')
    # Eksternt fillager (søstermappe): filen ligger i lob4/seg5/
    docs = tmp / "arkiv.siard_documents" / "content" / "schema0" / "table2" / "lob4" / "seg5"
    docs.mkdir(parents=True)
    (docs / "rec5647.txt").write_bytes(b"HELLO!")

    table_xml_map = {"schema0/table2": "content/schema0/table2/table2.xml"}
    table_blobs = defaultdict(list)
    extra, ext_ref_map, missing, _fmt = _collect_external_blobs(
        table_xml_map, ext, siard.parent, [], table_blobs,
        lambda m, l="info": None, siard_path=siard)

    _ok(not missing, f"ingen manglende refs (ekstern db-lobFolder løst) — fikk {dict(missing)}")
    dest = ext / "content" / "schema0" / "table2" / "ext_lob" / "rec5647.txt"
    _ok(dest.exists() and dest.read_bytes() == b"HELLO!",
        "ekstern seg-fil kopiert inn i ext_lob/")
    _ok(ext_ref_map.get("seg5/rec5647.txt") == "content/schema0/table2/ext_lob/rec5647.txt",
        "ref remappet til intern ext_lob-sti (for XML-prepatch)")


def test_dias_sidecar(tmp: Path) -> None:
    print("test_dias_sidecar (eksternt fillager inkluderes i DIAS-content)")
    from siard_workflow.operations.dias_package_operation import _copy_external_sidecar

    siard = _build_external_siard(tmp)   # arkiv.siard + arkiv.siard_documents/
    # Etterlik content-bygging i DiasPackageOperation.run
    content_dir = tmp / "content" / siard.stem
    content_dir.mkdir(parents=True)
    import shutil as _sh
    _sh.copy2(siard, content_dir / siard.name)

    logs: list[str] = []
    _copy_external_sidecar(siard, content_dir, logs.append)

    side = content_dir / "arkiv.siard_documents" / "content" / "schema0" / "table2" / "lob4"
    _ok((side / "rec1.bin").read_bytes() == b"BLOB-1",
        "søstermappe kopiert inn i content/ ved siden av .siard")
    _ok(any("Eksternt fillager inkludert" in m for m in logs),
        "inkludering logget")

    # Intern SIARD → ingen søstermappe kopieres
    intern = tmp / "intern.siard"
    with zipfile.ZipFile(intern, "w") as zf:
        zf.writestr("header/metadata.xml",
                    b'<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/'
                    b'metadata.xsd"><lobFolder>content</lobFolder></siardArchive>')
    cdir2 = tmp / "content2"
    cdir2.mkdir()
    _copy_external_sidecar(intern, cdir2, logs.append)
    _ok(not any(cdir2.iterdir()), "intern SIARD: ingen søstermappe kopiert (no-op)")


def main() -> None:
    import tempfile
    test_helpers()
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        for sub in ("r", "i", "pi", "pe", "ds", "cs"):
            (tmp / sub).mkdir()
        test_resolve(tmp / "r")
        test_internalize(tmp / "i")
        test_pipeline_intern(tmp / "pi")
        test_pipeline_ekstern(tmp / "pe")
        test_collect_external_standalone(tmp / "cs")
        test_dias_sidecar(tmp / "ds")
    print("\nAlle tester bestått ✓")


if __name__ == "__main__":
    main()
