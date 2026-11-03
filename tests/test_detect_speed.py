"""
Deteksjonen etter Siegfried skal ikke lese hele PDF-mengden flere ganger.

Bakgrunn (nov. 2026): ved 3–4 millioner LOB-filer sto programmet i timer uten
melding etter Siegfried-skanningen. Kategoriseringen sjekket signatur
(is_signed_pdf leste hele PDF-en) for hver PDF i én tråd uten fremdrift, og
UTF-8-gjenopprettingen leste hele filen for hver PDF over 64 kB.

Verifiserer:
  1. is_signed_pdf på fil leses i blokker: /ByteRange delt av blokkgrensen,
     langt ut i filen, og usignert stor fil — samme svar som før
  2. _utf8_candidate: binær PDF-start → hele filen leses ikke; UTF-8-kodet
     binærdata (også med kuttet tegn i vinduet) → leses
  3. BLOB-konvertering: signert PDF holdes utenfor (nå sjekket i den
     parallelle deteksjonen), og fremdriften logges med filer/s

Kjør:  python -X utf8 tests/test_detect_speed.py
"""
from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_signed_chunks(tmp: Path) -> None:
    print("test_signed_chunks")
    import siard_workflow.core.file_integrity as FI
    sig = b"/ByteRange [0 840 960 16010]"
    chunk = FI._SIG_CHUNK
    head = b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n"
    # 1) delt av blokkgrensen
    pre = chunk - len(head) - 10
    f1 = tmp / "grense.pdf"
    f1.write_bytes(head + b"x" * pre + sig + b"y" * 1000)
    _ok(FI.is_signed_pdf(f1), "/ByteRange delt av blokkgrensen (8 MB) gjenkjennes")
    # 2) langt ut i en stor fil
    f2 = tmp / "langt_ut.pdf"
    f2.write_bytes(head + b"z" * (chunk * 2 + 12345) + sig + b"%%EOF")
    _ok(FI.is_signed_pdf(f2), "signatur etter 16 MB gjenkjennes")
    # 3) usignert, med «/ByteRange» uten gyldig tabell
    f3 = tmp / "usignert.pdf"
    f3.write_bytes(head + b"q" * (chunk + 500) + b"/ByteRange [ugyldig]" + b"%%EOF")
    _ok(not FI.is_signed_pdf(f3), "usignert stor PDF (ugyldig /ByteRange) er ikke signert")
    f4 = tmp / "ikke_pdf.bin"
    f4.write_bytes(b"ikke pdf" + sig)
    _ok(not FI.is_signed_pdf(f4), "ikke-PDF med /ByteRange er ikke signert")


def test_utf8_candidate() -> None:
    print("test_utf8_candidate")
    from siard_workflow.operations.blob_convert_operation import _utf8_candidate
    pdf_head = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n" + bytes(range(128, 256)) * 400
    _ok(not _utf8_candidate(pdf_head[:65536]), "binær PDF-start → hele filen leses ikke")
    binary = bytes(range(256)) * 300
    enc = binary.decode("latin-1").encode("utf-8")
    _ok(_utf8_candidate(enc[:65536]), "UTF-8-kodet binærdata → kandidat")
    cut = enc[:1001]
    while cut and (cut[-1] & 0xC0) != 0xC0:
        cut = cut[:-1]
    _ok(_utf8_candidate(cut), "tegn kuttet av vindusgrensen godtas")
    _ok(not _utf8_candidate(b"vanlig ascii-tekst " * 100), "ren ASCII → ikke kandidat")


def test_blob_signed_and_progress(tmp: Path) -> None:
    print("test_blob_signed_and_progress")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    sys.path.insert(0, str(_ROOT / "tests"))
    from test_signed_pdf import _signed_pdf
    root = tmp / "x"
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(
        '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd">'
        '<lobFolder>content</lobFolder><schemas><schema><folder>schema0</folder><tables><table>'
        '<folder>table0</folder><columns><column><name>id</name><type>INTEGER</type></column>'
        '<column><name>d</name><lobFolder>schema0/table0/lob2</lobFolder><type>BLOB</type></column>'
        '</columns><rows>40</rows></table></tables></schema></schemas></siardArchive>', "utf-8")
    lob = root / "content/schema0/table0/lob2"
    lob.mkdir(parents=True)
    signed = _signed_pdf()
    (lob / "rec0.bin").write_bytes(signed)
    rows = ['<row><c1>0</c1><c2 file="rec0.bin"/></row>']
    for i in range(1, 40):
        (lob / f"rec{i}.bin").write_bytes(f"tekst nummer {i}\n".encode() * 20)
        rows.append(f'<row><c1>{i}</c1><c2 file="rec{i}.bin"/></row>')
    (root / "content/schema0/table0/table0.xml").write_text(
        "<table>\n" + "\n".join(rows) + "\n</table>\n", "utf-8")
    logs: list = []
    ctx = WorkflowContext(siard_path=tmp / "x.siard", metadata={
        "file_logger": None,
        "progress_cb": lambda ev, **k: logs.append(k.get("msg", "")) if ev == "log" else None,
        "stop_event": threading.Event(), "pause_event": threading.Event()})
    ctx.extracted_path = root
    r = BlobConvertOperation().run(ctx)
    txt = "\n".join(logs)
    _ok(r.success, f"BLOB-konvertering ok ({r.message})")
    _ok((lob / "rec0.bin").read_bytes() == signed, "signert PDF er urørt")
    _ok("Digitalt signerte PDF-er (røres ikke): 1" in txt, "signert PDF funnet i deteksjonen")
    _ok("filer/s" in txt and "Detektert: 40/40 filer (100 %" in txt,
        "fremdriften viser antall, prosent og filer/s")
    _ok("Sorterer 40 filer" in txt and "Sortert på" in txt, "sorteringen logges")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="detect_") as td:
        tmp = Path(td)
        test_signed_chunks(tmp)
        test_utf8_candidate()
        test_blob_signed_and_progress(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
