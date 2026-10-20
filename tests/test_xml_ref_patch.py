"""
Oppdatering av filreferanser i tableX.xml etter BLOB-konvertering — og
livstegn i kjøreloggen.

Bakgrunn (Profil, Kristiansand, okt. 2026): oppdateringen av referansene tok
døgn. For hver linje med en filreferanse ble HELE lista med
konverteringskommentarer gjennomsøkt (og omdøpingstabellen kopiert) —
kvadratisk i antall filer (4 mill. filer i én tabell ≈ uker).

Verifiserer:
  1. hver referanse slås opp direkte: tidsbruk per linje uavhengig av antall filer
  2. length/digest oppdateres bare i referansens EGET element (rad på én linje
     med flere LOB-celler fikk tidligere samme length/digest i alle)
  3. kolonnekollisjoner (samme filnavn i to lob-mapper) slås opp per <cN>
     — også når hele raden står på én linje
  4. referanser med full sti beholder mappe-prefikset
  5. riktig konverteringskommentar per fil (ikke delstreng: rec1 ≠ rec12)
  6. lengde/MD5 registrert under konverteringen gjenbrukes (filene leses ikke
     på nytt), men beregnes på nytt hvis fila er endret etterpå
  7. stor fil på disk: fremdrift også på tid (halvtime), med anslag over gjenstående tid
  8. kjøreloggen: livstegn med steg/siste melding, og datolinje når døgnet skifter

Kjør:  python -X utf8 tests/test_xml_ref_patch.py
"""
from __future__ import annotations

import hashlib
import sys
import tempfile
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.operations import blob_convert_operation as B   # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest().upper()


def test_one_line_row(tmp: Path) -> None:
    print("test_one_line_row")
    a, b = tmp / "rec1.pdf", tmp / "rec12.pdf"
    a.write_bytes(b"A" * 10)
    b.write_bytes(b"BB" * 20)
    ren = {b"rec1.bin": b"rec1.pdf", b"rec12.bin": b"rec12.pdf"}
    paths = {b"rec1.bin": a, b"rec12.bin": b}
    line = (b'<row><c1>1</c1><c2 file="rec1.bin" length="3" digestType="MD5" digest="X"/>'
            b'<c3 file="rec12.bin" length="4" digestType="MD5" digest="Y"/></row>\n')
    comments = {b"rec1.bin": "konvertert A", b"rec12.bin": "konvertert B"}
    out, n, notes = B._patch_line_refs(line, B._make_ref_lookup(ren, paths), comments)
    _ok(n == 2, "to referanser i samme linje oppdatert")
    _ok(f'<c2 file="rec1.pdf" length="10" digestType="MD5" digest="{_md5(b"A" * 10)}"/>'.encode() in out
        and f'<c3 file="rec12.pdf" length="40" digestType="MD5" digest="{_md5(b"BB" * 20)}"/>'.encode() in out,
        "length/digest bare i referansens eget element (ikke kopiert til nabocellen)")
    _ok(notes == ["konvertert A", "konvertert B"],
        f"riktig kommentar per fil, ikke delstreng rec1 ⊂ rec12: {notes}")
    # bakoverkompatibel innpakning (brukes av anonymisering)
    out2, n2 = B._patch_line_with_digest(line, ren, paths)
    _ok(out2 == out and n2 == 2, "_patch_line_with_digest gir samme resultat")


def test_columns_and_full_paths(tmp: Path) -> None:
    print("test_columns_and_full_paths")
    f2, f3 = tmp / "c2_rec5.pdf", tmp / "c3_rec5.pdf"
    f2.write_bytes(b"2" * 7)
    f3.write_bytes(b"3" * 9)
    col_b = {2: {b"rec5.bin": b"rec5.pdf"}, 3: {b"rec5.bin": b"rec5.pdf"}}
    col_p = {2: {b"rec5.bin": f2}, 3: {b"rec5.bin": f3}}
    lookup = B._make_ref_lookup({}, {}, col_b, col_p)
    line = b'<row><c2 file="rec5.bin" length="1"/><c3 file="rec5.bin" length="1"/></row>\n'
    out, n, _ = B._patch_line_refs(line, lookup)
    _ok(n == 2 and b'<c2 file="rec5.pdf" length="7"/>' in out
        and b'<c3 file="rec5.pdf" length="9"/>' in out,
        "kolonnekollisjon: hver <cN> får filen fra sin egen lob-mappe (rad på én linje)")
    full = tmp / "full.pdf"
    full.write_bytes(b"f" * 3)
    ren = {b"content/schema0/table1/lob2/rec7.bin": b"rec7.pdf"}
    paths = {b"content/schema0/table1/lob2/rec7.bin": full}
    out, n, _ = B._patch_line_refs(
        b'<c2 file="content/schema0/table1/lob2/rec7.bin" length="9"/>\n',
        B._make_ref_lookup(ren, paths))
    _ok(out == b'<c2 file="content/schema0/table1/lob2/rec7.pdf" length="3"/>\n',
        "full sti: mappe-prefikset beholdes, length fra ny fil")
    out, n, _ = B._patch_line_refs(b'<c2 file="seg0/rec9.bin"/>\n',
                                   B._make_ref_lookup({b"rec9.bin": b"rec9.pdf"},
                                                      {b"rec9.bin": full}))
    _ok(out == b'<c2 file="seg0/rec9.pdf"/>\n', "relativ sti: oppslag på filnavn, prefiks beholdt")
    out, n, _ = B._patch_line_refs(b'<c2 file="annen.bin" length="1"/>\n',
                                   B._make_ref_lookup({b"rec9.bin": b"rec9.pdf"},
                                                      {b"rec9.bin": full}))
    _ok(n == 0 and out == b'<c2 file="annen.bin" length="1"/>\n', "ukjent fil: urørt")


def test_speed(tmp: Path) -> None:
    print("test_speed")
    f = tmp / "x.pdf"
    f.write_bytes(b"x")
    times = {}
    for N in (2_000, 200_000):
        ren = {f"record{i}.bin".encode(): f"record{i}.pdf".encode() for i in range(N)}
        paths = {k: f for k in ren}
        comments = {k: "Konvertert" for k in ren}
        lookup = B._make_ref_lookup(ren, paths)
        lines = [f'<row><c1>{i}</c1><c2 file="record{i}.bin" length="3"/></row>\n'.encode()
                 for i in range(0, N, max(1, N // 500))]
        t = time.perf_counter()
        for line in lines:
            B._patch_line_refs(line, lookup, comments)
        times[N] = (time.perf_counter() - t) / len(lines)
    _ok(times[200_000] < 5 * times[2_000] + 0.0005,
        f"tid per linje uavhengig av antall filer: {times[2_000] * 1e6:.0f} µs (2 000) → "
        f"{times[200_000] * 1e6:.0f} µs (200 000)")


def test_parallel_digests(tmp: Path) -> None:
    print("test_parallel_digests")
    import concurrent.futures as cf
    files = {}
    for i in range(3000):
        f = tmp / f"p{i}.pdf"
        f.write_bytes(bytes([i % 256]) * (i % 50 + 1))
        files[f"r{i}.bin".encode()] = f
    lookup = B._make_ref_lookup({k: v.name.encode() for k, v in files.items()}, files)
    lines = [f'<c2 file="r{i}.bin" length="0" digest="X"/>\n'.encode() for i in range(3000)]
    seq = [B._patch_line_refs(ln, lookup)[0] for ln in lines]
    with cf.ThreadPoolExecutor(4) as pool:
        par = [ln for ln, _n in B._patch_lines(iter(lines), lookup, pool=pool, batch=500)]
    _ok(par == seq, "parallell MD5-beregning i bunter gir identisk resultat, i samme rekkefølge")


def test_digest_cache(tmp: Path) -> None:
    print("test_digest_cache")
    import concurrent.futures as cf
    import os
    cache = B._DigestCache()
    files = {}
    for i in range(500):
        f = tmp / f"dc{i}.pdf"
        f.write_bytes(b"%PDF " + bytes([i % 256]) * (i + 1))
        files[f"d{i}.bin".encode()] = f
        cache.remember(f)
    f0 = files[b"d0.bin"]
    _ok(cache.get(f0) == B._file_size_md5(f0), "lengde/MD5 fra konverteringen = beregnet fra fila")
    lookup = B._make_ref_lookup({k: v.name.encode() for k, v in files.items()}, files)
    lines = [f'<c2 file="d{i}.bin" length="0" digest="X"/>\n'.encode() for i in range(500)]
    expected = [B._patch_line_refs(ln, lookup)[0] for ln in lines]
    reads = []
    orig_safe, orig_md5 = B._safe_size_md5, B._file_size_md5
    B._safe_size_md5 = lambda p: (reads.append(p), orig_safe(p))[1]
    B._file_size_md5 = lambda p: (reads.append(p), orig_md5(p))[1]
    try:
        with cf.ThreadPoolExecutor(4) as pool:
            got = [ln for ln, _n in B._patch_lines(iter(lines), lookup, pool=pool, known=cache)]
        _ok(got == expected and not reads,
            "XML-oppdateringen bruker lagrede verdier — ingen filer leses på nytt")
        # endret etter konvertering (f.eks. Ghostscript) → beregnes på nytt
        f5 = files[b"d5.bin"]
        f5.write_bytes(b"%PDF ny og annerledes")
        os.utime(f5, ns=(1, 1))
        with cf.ThreadPoolExecutor(4) as pool:
            got = [ln for ln, _n in B._patch_lines(iter(lines[:10]), lookup, pool=pool, known=cache)]
        _ok(f'length="{len(b"%PDF ny og annerledes")}"'.encode() in got[5] and f5 in reads,
            "fil endret etter konverteringen → lengde/MD5 beregnes på nytt (ikke foreldet verdi)")
    finally:
        B._safe_size_md5, B._file_size_md5 = orig_safe, orig_md5
    _ok(cache.hits >= 500 and cache.misses >= 1, f"treff/bom telles ({cache.hits}/{cache.misses})")


def test_inplace_progress(tmp: Path) -> None:
    print("test_inplace_progress")
    new = tmp / "ny.pdf"
    new.write_bytes(b"pdfdata")
    xml = tmp / "table1.xml"
    rows = [f'<row><c1>{i}</c1><c2 file="rec{i}.bin" length="1" digest="X"/></row>\n'
            for i in range(30_000)]
    xml.write_text('<?xml version="1.0" encoding="utf-8"?>\n<table>\n' + "".join(rows)
                   + "</table>\n", "utf-8")
    renames = {f"rec{i}.bin": new for i in range(30_000)}
    msgs: list = []
    old = B.PROGRESS_HEARTBEAT_S
    B.PROGRESS_HEARTBEAT_S = 0      # fremdrift ved hver sjekk (hver 2048. linje)
    try:
        n = B.BlobConvertOperation()._patch_xml_file_inplace(
            xml, renames, lambda m, lvl="info": msgs.append(m),
            line_comments={"rec1.bin": "konvertert"})
    finally:
        B.PROGRESS_HEARTBEAT_S = old
    data = xml.read_text("utf-8")
    _ok(n == 30_000 and data.count('length="7"') == 30_000, "30 000 referanser oppdatert på disk")
    _ok(data.count("<!-- konvertert -->") == 1, "kommentar bare på rec1 (ikke rec10, rec100 …)")
    prog = [m for m in msgs if "linjer (" in m]
    _ok(11 < len(prog) < 60, f"fremdrift også på tid, ikke bare hver 10 % ({len(prog)} meldinger)")


def test_heartbeat_and_dates(tmp: Path) -> None:
    print("test_heartbeat_and_dates")
    import datetime
    from siard_workflow.core.file_logger import WorkflowFileLogger
    echoed: list = []
    with WorkflowFileLogger(tmp / "logg", "test", heartbeat_s=0.2,
                            on_heartbeat=echoed.append) as fl:
        fl.set_step("[8] BLOB Konverter til PDF/A")
        fl.log("table348.xml: 23,430,530/234,305,302 linjer (10%)", "info")
        time.sleep(0.7)
        fl._day = datetime.date.today() - datetime.timedelta(days=1)   # simuler døgnskifte
        fl.log("etter midnatt", "info")
        path = fl.log_path
    text = path.read_text("utf-8")
    _ok("LIVSTEGN: Fortsatt i gang" in text and "steg «[8] BLOB Konverter til PDF/A»" in text
        and "siste loggmelding" in text and "table348.xml" in text,
        "livstegn med steg og siste loggmelding")
    _ok(echoed and echoed[0].startswith("LIVSTEGN"), "livstegnet sendes også til GUI-loggen")
    today = datetime.date.today()
    _ok(f"{today:%d.%m.%Y} ----------" in text
        and text.index(f"{today:%d.%m.%Y}") < text.index("etter midnatt"),
        "datolinje skrives når døgnet skifter")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="xmlref_") as td:
        tmp = Path(td)
        test_one_line_row(tmp)
        test_columns_and_full_paths(tmp)
        test_speed(tmp)
        test_parallel_digests(tmp)
        test_digest_cache(tmp)
        test_inplace_progress(tmp)
        test_heartbeat_and_dates(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
