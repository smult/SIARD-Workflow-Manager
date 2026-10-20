"""
Utpakking og sammenpakking av SIARD med jevn fremdrift (core/zip_progress.py,
Pakk ut / Pakk sammen SIARD).

Verifiserer:
  1. extract_all gir samme filer og innhold som ZipFile.extract — også med
     flere tråder, nøstede mapper, store filer (strømmet i blokker) og
     stier med «..» (holdes inne i målmappa)
  2. fremdrift i bytes: GUI-meldinger med tekst (filer, GB, MB/s) og jevne
     prosent, også gjennom én stor fil; logglinje per 10 %
  3. Pakk ut → Pakk sammen: alt innhold bevart; stor tableX.xml strømmes
     (versjon/navnerom i hodet skrives om, dataradene urørt); allerede
     komprimert innhold lagres ukomprimert (smart-skip) også for store filer
  4. fillisten bygges med fremdrift

Kjør:  python -X utf8 tests/test_zip_progress.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core import zip_progress as Z   # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


BIG_XML_ROWS = 700_000          # ~ 15 MB > BIG_FILE


def _make_siard(path: Path) -> dict:
    files = {
        "header/metadata.xml": (b'<?xml version="1.0"?>\n<siardArchive xmlns='
                                b'"http://www.bar.admin.ch/xmlns/siard/2.2/metadata.xsd" '
                                b'version="2.2"><dbname>x</dbname></siardArchive>\n'),
        "header/siardversion/2.2/": b"",
        "content/schema0/table0/table0.xml": (
            b'<?xml version="1.0"?>\n<table xmlns="http://www.bar.admin.ch/xmlns/siard/2.2/'
            b'table.xsd" version="2.2">\n'
            + b"".join(b'<row><c1>%d</c1><c2>version="2.2" i data</c2></row>\n' % i
                       for i in range(BIG_XML_ROWS)) + b"</table>\n"),
        "content/schema0/table0/lob3/record1.bin": b"%PDF-1.7 liten",
        "content/schema0/table0/lob3/record2.bin": b"PK\x03\x04" + os.urandom(9 << 20),
    }
    for i in range(300):
        files[f"content/schema0/table1/lob2/record{i}.bin"] = f"tekst {i} ".encode() * 50
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            if name.endswith("/"):
                zf.writestr(zipfile.ZipInfo(name), b"")
            else:
                zf.writestr(name, data)
    return files


def test_extract_all(tmp: Path) -> None:
    print("test_extract_all")
    z = tmp / "a.siard"
    files = _make_siard(z)
    with zipfile.ZipFile(z, "a") as zf:
        zf.writestr("../utenfor.txt", b"skal havne inne i mappa")
    ref = tmp / "ref"
    with zipfile.ZipFile(z) as zf:
        for n in zf.namelist():
            zf.extract(n, ref)
    events: list = []
    logs: list = []
    for workers in (1, 4):
        out = tmp / f"ny{workers}"
        names, n = Z.extract_all(
            z, out, lambda nf, nb: Z.ByteProgress("Pakker ut", nf, nb,
                                                  lambda ev, **k: events.append(k),
                                                  lambda m, l="info": logs.append(m)),
            workers=workers)
        got = {p.relative_to(out).as_posix(): p.read_bytes() for p in out.rglob("*") if p.is_file()}
        want = {p.relative_to(ref).as_posix(): p.read_bytes() for p in ref.rglob("*") if p.is_file()}
        _ok(got == want and n == len(want),
            f"{workers} tråd(er): samme filer og innhold som ZipFile.extract ({n} filer)")
    _ok("utenfor.txt" in got and not (tmp / "utenfor.txt").exists(),
        "«../» i filnavn holdes inne i målmappa")
    _ok((out / "header/siardversion/2.2").is_dir(), "tomme mapper fra ZIP-en opprettes")
    texts = [e for e in events if e.get("text")]
    _ok(texts and "filer ·" in texts[-1]["text"] and "av" in texts[-1]["text"],
        f"GUI-fremdrift med tekst: «{texts[-1]['text']}»")
    _ok(texts[-1]["done"] == 1000 and texts[-1]["total"] == 1000, "ferdig = 100 %")
    _ok(any("% —" in m for m in logs), "logglinje per 10 %")


def test_progress_big_file() -> None:
    print("test_progress_big_file")
    events: list = []
    old = Z.GUI_INTERVAL_S
    Z.GUI_INTERVAL_S = 0
    try:
        p = Z.ByteProgress("Pakker ut", 1, 100 * Z.CHUNK, lambda ev, **k: events.append(k))
        for _ in range(100):
            p.add(Z.CHUNK)             # én stor fil i blokker
    finally:
        Z.GUI_INTERVAL_S = old
    steps = sorted({e["done"] for e in events})
    _ok(len(steps) >= 90, f"jevn fremdrift gjennom én stor fil ({len(steps)} trinn)")


def test_unpack_repack(tmp: Path) -> None:
    print("test_unpack_repack")
    import settings
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations import UnpackSiardOperation, RepackSiardOperation
    cfg = {"siard_output_version": "2.1", "siard_compress_level": 6,
           "siard_smart_skip": True, "max_workers": 4}
    orig_get = settings.get_config
    settings.get_config = lambda k, d=None: cfg.get(k, orig_get(k, d))
    try:
        src = tmp / "uttrekk.siard"
        files = _make_siard(src)
        events: list = []
        ctx = WorkflowContext(siard_path=src, metadata={
            "file_logger": None, "stop_event": threading.Event(),
            "progress_cb": lambda ev, **k: events.append((ev, k))})
        r1 = UnpackSiardOperation().run(ctx)
        ctx.set_result("unpack_siard", r1.data)
        _ok(r1.success and r1.data["files_extracted"] == len([n for n in files if not n.endswith("/")]),
            f"Pakk ut: {r1.message}")
        r2 = RepackSiardOperation().run(ctx)
        _ok(r2.success, f"Pakk sammen: {r2.message}")
        out = Path(r2.data["output_path"])
        with zipfile.ZipFile(out) as zf:
            names = set(zf.namelist())
            big = zf.read("content/schema0/table0/table0.xml")
            info_zip = zf.getinfo("content/schema0/table0/lob3/record2.bin")
            same = all(zf.read(n) == d for n, d in files.items()
                       if not n.endswith("/") and n.startswith("content/schema0/table1"))
            lob_big = zf.read("content/schema0/table0/lob3/record2.bin")
        _ok("header/siardversion/2.1/" in names, "versjonsmappa skrevet om til 2.1")
        _ok(same and lob_big == files["content/schema0/table0/lob3/record2.bin"],
            "LOB-innhold bevart byte for byte (også stor fil strømmet)")
        _ok(big.count(b"<row>") == BIG_XML_ROWS and b'version="2.1"' in big[:400]
            and b"siard/2/table.xsd" in big[:400]
            and big.count(b'version="2.2" i data') == BIG_XML_ROWS,
            "stor tableX.xml strømmet: hodet skrevet om til 2.1, dataradene urørt")
        _ok(info_zip.compress_type == zipfile.ZIP_STORED,
            "allerede komprimert stor fil lagret ukomprimert (smart-skip)")
        texts = [k.get("text", "") for ev, k in events if ev == "phase_progress"]
        _ok(any(t.startswith("Pakker ut:") for t in texts)
            and any(t.startswith("Pakker sammen:") for t in texts)
            and any(t.startswith("Finner filer:") for t in texts),
            "GUI-fremdrift for utpakking, fillisting og sammenpakking")
    finally:
        settings.get_config = orig_get


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="zipprog_") as td:
        tmp = Path(td)
        test_extract_all(tmp)
        test_progress_big_file()
        test_unpack_repack(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
