"""
Regresjonstester for feil funnet i testkjøring av BLOB-konvertering på
testsiard/TestDokumentarkiv.siard (2026-09-23):

  1. UTF-8-tekst ble «bin» når byte 512 kuttet et flerbyte-tegn (æ/ø/å).
  2. Referansereparasjonen tolket full content/-sti i file= som hengende,
     «rettet» 26 referanser til seg selv, advarte feilaktig og ga PREMIS-tekst
     om omtyping som aldri skjedde.
  3. «Korriger lobFolder» med «Full sti i file=» ble lagt rett etter «Pakk ut»,
     før HEX Inline Extract → blandede referanseformater. Skal ligge sist.
  4. LibreOffice som ikke kan åpne filer ga én feil per fil med gjetning om
     passord; nå helsesjekk med én tydelig melding.

Kjør:  python -X utf8 tests/test_blob_run_fixes.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_utf8_boundary() -> None:
    print("test_utf8_boundary")
    from siard_workflow.core.identifiers.magic_bytes import _detect, _is_utf8_prefix
    for pad in range(0, 4):
        text = ("a" * (511 - pad) + "ø" * 20 + " Videre oppfølging avtales.").encode("utf-8")
        _ok(_detect(text)[0] == "txt", f"æøå over 512-grensen (forskyvning {pad}) → txt")
    euro = ("x" * 510 + "€" * 10).encode("utf-8")      # 3-byte tegn kuttet
    _ok(_detect(euro)[0] == "txt", "3-byte tegn (€) kuttet av vinduet → txt")
    _ok(not _is_utf8_prefix(b"abc\xff\xfedef"), "ugyldig UTF-8 midt i → ikke tekst")
    _ok(not _is_utf8_prefix(b"abc\x00def"), "NUL → ikke tekst")
    _ok(_is_utf8_prefix(b"a" * 511 + b"\xc3"), "avkuttet 2-byte-start i siste byte godtas")
    _ok(not _is_utf8_prefix(b"a" * 400 + b"\xc3" + b"a" * 111), "ufullstendig tegn midt i → ikke tekst")


def test_ref_repair_full_paths(tmp: Path) -> None:
    print("test_ref_repair_full_paths")
    from siard_workflow.core.lob_column_types import (
        repair_dangling_lob_refs, reconcile_lob_column_types)
    root = tmp / "refs"
    lob = root / "content/schema0/table0/lob7"; lob.mkdir(parents=True)
    (lob / "record1.bin").write_bytes(b"%PDF-1.4")
    (lob / "record2.txt").write_bytes(b"x")        # hengende referanse under
    (root / "header").mkdir()
    (root / "header/metadata.xml").write_text(
        '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd">'
        '<lobFolder>content</lobFolder><schemas><schema><folder>schema0</folder><tables><table>'
        '<folder>table0</folder><columns><column><name>id</name><type>INTEGER</type></column>'
        '<column><name>dok</name><lobFolder>schema0/table0/lob7</lobFolder><type>BLOB</type></column>'
        '</columns></table></tables></schema></schemas></siardArchive>', "utf-8")
    txml = root / "content/schema0/table0/table0.xml"
    txml.write_text(
        '<table>\n<row><c1>1</c1><c2 file="content/schema0/table0/lob7/record1.bin"/></row>\n'
        '<row><c1>2</c1><c2 file="content/schema0/table0/lob7/record2.bin"/></row>\n</table>\n', "utf-8")
    st = repair_dangling_lob_refs(root)
    _ok(st["refs_repaired"] == 1, f"kun den reelt hengende referansen rettes (fikk {st['refs_repaired']})")
    x = txml.read_text("utf-8")
    _ok('file="content/schema0/table0/lob7/record1.bin"' in x, "gyldig full sti urørt")
    _ok('file="content/schema0/table0/lob7/record2.txt"' in x, "hengende .bin → eksisterende .txt (full sti bevart)")
    st2 = reconcile_lob_column_types(root)
    _ok(st2["refs_repaired"] == 0 and st2["retype_details"] == [] and st2["repair_details"] == [],
        "andre kjøring: ingen falske reparasjoner, ingen omtyping")

    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    from siard_workflow.core.base_operation import OperationResult
    op = BlobConvertOperation()
    r = OperationResult("blob_convert", True, {"lob_type_details": [], "lob_ref_repairs": ["x: 1 ref"]}, "")
    d = op.premis_detail(r, None)
    _ok("omtypet" not in d and "pekt tilbake" in d, f"PREMIS: reparasjon beskrevet som reparasjon ({d})")


def test_panel_move_before() -> None:
    print("test_panel_move_before")
    import os, threading
    sys.argv = ["app"]
    threading.Timer(60, lambda: os._exit(3)).start()
    from gui.app import App
    from siard_workflow.operations import (UnpackSiardOperation, RepackSiardOperation,
                                           HexExtractOperation, BlobConvertOperation,
                                           LobFolderFixOperation)
    app = App(); app.after(1500, app.quit); app.mainloop(); app.update()
    try:
        wp = app.workflow_panel
        lf = LobFolderFixOperation()
        for o in (UnpackSiardOperation(), lf, HexExtractOperation(), BlobConvertOperation(),
                  RepackSiardOperation()):
            wp.add_operation(o, silent=True)
        wp.move_operation_before(lf, "repack_siard")
        ids = [o.operation_id for o in wp.get_operations()]
        _ok(ids == ["unpack_siard", "hex_extract", "blob_convert", "lobfolder_fix", "repack_siard"],
            f"flyttet rett før repack: {ids}")
        wp.clear()
        for o in (UnpackSiardOperation(), HexExtractOperation(), RepackSiardOperation()):
            wp.add_operation(o, silent=True)
        wp.insert_operation_before(LobFolderFixOperation(), "repack_siard")
        ids = [o.operation_id for o in wp.get_operations()]
        _ok(ids == ["unpack_siard", "hex_extract", "lobfolder_fix", "repack_siard"],
            f"satt inn rett før repack: {ids}")
        _ok(not wp.has_order_violations(), "ingen rekkefølgebrudd")
    finally:
        app.destroy()


def test_lo_health_check(tmp: Path) -> None:
    print("test_lo_health_check")
    from siard_workflow.operations import blob_convert_operation as B
    ok, err = B.lo_health_check(str(tmp / "finnes_ikke_soffice.exe"), tmp / "hc1", timeout=10)
    _ok(not ok and err, f"manglende LibreOffice → feil med melding ({err[:60]})")
    _ok(issubclass(B.LibreOfficeUnavailable, RuntimeError), "egen unntaksklasse")
    msg = B._err_clean("Error: source file could not be loaded")
    _ok("passord" not in msg.split("(")[0] and "kunne ikke åpne" in msg, f"ingen passord-gjetning: {msg}")


def test_health_cache_persisted(tmp: Path) -> None:
    """Gjenbrukstesten lagres per LibreOffice-installasjon (config) og kjøres
    ikke på nytt i en ny økt — før ny installasjon (endret soffice-fil)."""
    print("test_health_cache_persisted")
    import time as _t, os as _os
    import settings
    from siard_workflow.core import lo_runner as R
    sys.path.insert(0, str(_ROOT / "tests"))
    from test_msg_to_pdf import _fake_soffice
    store: dict = {}
    og, os_ = settings.get_config, settings.set_config
    settings.get_config = lambda k, d=None: store.get(k, d if d is not None else {})
    settings.set_config = lambda k, v: store.__setitem__(k, v)
    try:
        fake = _fake_soffice(tmp)
        R.reset_session_cache()
        ok, _e, reuse = R.health_check(fake, tmp / "h1", timeout=30, reuse_timeout=3)
        _ok(ok and reuse is False and list(store.get("lo_profile_reuse_cache", {}).values()) == [False],
            "resultat lagret i config per installasjon")
        R.reset_session_cache()                         # ny programøkt
        t0 = _t.monotonic()
        ok2, _e2, reuse2 = R.health_check(fake, tmp / "h2", timeout=30, reuse_timeout=3)
        _ok(ok2 and reuse2 is False and not R.profile_reuse_ok() and _t.monotonic() - t0 < 2.5,
            f"ny økt: lagret resultat brukt, ingen ny gjenbrukstest ({_t.monotonic() - t0:.1f} s)")
        _os.utime(fake, (_t.time() + 100, _t.time() + 100))   # «ny installasjon»
        R.reset_session_cache()
        t0 = _t.monotonic()
        R.health_check(fake, tmp / "h3", timeout=30, reuse_timeout=3)
        _ok(_t.monotonic() - t0 >= 2.5 and len(store["lo_profile_reuse_cache"]) >= 1,
            "endret soffice-fil → gjenbrukstesten kjøres på nytt")
    finally:
        settings.get_config, settings.set_config = og, os_
        R.reset_session_cache()


def test_blob_summary_clean(tmp: Path) -> None:
    print("test_blob_summary_clean")
    import threading
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    root = tmp / "sum"
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(
        '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd">'
        '<lobFolder>content</lobFolder><schemas><schema><folder>schema0</folder><tables><table>'
        '<folder>table0</folder><columns><column><name>id</name><type>INTEGER</type></column>'
        '<column><name>b</name><lobFolder>schema0/table0/lob2</lobFolder><type>BLOB</type></column>'
        '</columns><rows>1</rows></table></tables></schema></schemas></siardArchive>', "utf-8")
    lob = root / "content/schema0/table0/lob2"; lob.mkdir(parents=True)
    import io
    from PIL import Image
    b = io.BytesIO(); Image.new("RGB", (8, 8)).save(b, "PNG"); (lob / "record1.bin").write_bytes(b.getvalue())
    (root / "content/schema0/table0/table0.xml").write_text(
        '<table>\n<row><c1>1</c1><c2 file="record1.bin"/></row>\n</table>\n', "utf-8")
    logs: list = []
    ctx = WorkflowContext(siard_path=tmp / "sum.siard", metadata={
        "file_logger": None, "progress_cb": lambda ev, **k: logs.append(k.get("msg", "")) if ev == "log" else None,
        "stop_event": threading.Event(), "pause_event": threading.Event()})
    ctx.extracted_path = root
    r = BlobConvertOperation().run(ctx)
    txt = "\n".join(logs)
    _ok(r.success, f"BLOB i pipeline-modus ok ({r.message})")
    _ok("lob_type_details" not in txt and "lob_ref_repairs" not in txt and "lob_columns_retyped" not in txt,
        "ingen interne nøkler i oppsummeringen")
    _ok("Ny SIARD lages av «Pakk sammen SIARD»" in txt and "Destinasjon:" not in txt,
        "pipeline: ingen misvisende destinasjon/«Ny SIARD»-sti")


def test_lo_run_root_unique(tmp: Path) -> None:
    """Hver BLOB-konvertering får egen arbeidsmappe for LO-profiler og batcher
    (samtidige kjøringer på samme temp-disk delte før lo_profiles/workerN)."""
    print("test_lo_run_root_unique")
    import threading
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    parent = tmp / "tempdisk"; parent.mkdir()
    ext1, ext2 = parent / "siard_pipeline_a", parent / "siard_pipeline_b"
    ext1.mkdir(); ext2.mkdir()
    seen: list = []
    barrier = threading.Barrier(2, timeout=10)

    class Op(BlobConvertOperation):
        def _convert_all_run(self, *a, lo_run_root=None, **k):
            seen.append(lo_run_root)
            (lo_run_root / "profiles/worker0").mkdir(parents=True)
            barrier.wait()                       # begge kjøringer aktive samtidig
            if k.get("err_log") == "krasj":
                raise RuntimeError("krasj")

    def run(ext, err=None):
        try:
            Op()._convert_all([], ext, {}, lambda *a, **k: None, lambda *a, **k: None,
                              "soffice", threading.Event(), threading.Event(), err_log=err)
        except RuntimeError:
            pass
    ts = [threading.Thread(target=run, args=(ext1,)),
          threading.Thread(target=run, args=(ext2, "krasj"))]
    for t in ts: t.start()
    for t in ts: t.join()
    _ok(len(seen) == 2 and seen[0] != seen[1], "samtidige kjøringer får hver sin arbeidsmappe")
    _ok(all(p.parent == parent and p.name.startswith("siard_lo_") for p in seen),
        "arbeidsmappa ligger på samme temp-disk (ikke i utpakket SIARD)")
    _ok(not any(p.exists() for p in seen), "arbeidsmappa ryddes — også når kjøringen krasjer")
    _ok(not (parent / "lo_profiles").exists(), "ingen delt lo_profiles-mappe på temp-disken")


def test_no_console_windows() -> None:
    """Eksterne konsollprogrammer (Ghostscript, LibreOffice, taskkill) startes
    uten eget konsollvindu — ellers blinker ett vindu opp per konvertering når
    programmet kjører med pythonw."""
    print("test_no_console_windows")
    import subprocess
    from siard_workflow.core.subproc import hidden_kwargs
    from siard_workflow.core import lo_runner as R
    kw = hidden_kwargs()
    if sys.platform != "win32":
        _ok(kw == {}, "ikke-Windows: ingen ekstra argumenter")
        return
    _ok(kw["creationflags"] & subprocess.CREATE_NO_WINDOW, "CREATE_NO_WINDOW satt")
    seen: list = []
    og_popen, og_run = subprocess.Popen, subprocess.run

    class FakePopen:
        def __init__(self, cmd, **k):
            seen.append(k); self.pid = 1; self.returncode = 0
        def communicate(self, timeout=None):
            return b"", b""
    subprocess.Popen = FakePopen
    subprocess.run = lambda cmd, **k: seen.append(k)
    try:
        R.run_lo(["gswin64c.exe", "-v"], 5)
        R.kill_tree(123)
    finally:
        subprocess.Popen, subprocess.run = og_popen, og_run
    _ok(len(seen) == 2 and all(k.get("creationflags", 0) & subprocess.CREATE_NO_WINDOW for k in seen),
        "run_lo (LibreOffice/Ghostscript) og kill_tree (taskkill) skjuler konsollvindu")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="blobfix_") as td:
        tmp = Path(td)
        test_utf8_boundary()
        test_ref_repair_full_paths(tmp)
        test_lo_health_check(tmp)
        test_health_cache_persisted(tmp)
        test_blob_summary_clean(tmp)
        test_lo_run_root_unique(tmp)
        test_no_console_windows()
        test_panel_move_before()
    print("\nAlle tester OK")
    import os
    os._exit(0)


if __name__ == "__main__":
    main()
