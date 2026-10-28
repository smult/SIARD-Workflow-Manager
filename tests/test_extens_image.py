"""
Extens-bilder (innpakket Windows DIB uten filsignatur) → PNG.

Bakgrunn (Extens, Agder fylkeskommune, okt. 2026): LOB-er med bilder (bl.a.
våpenskjold) ble ikke identifisert — 12 byte lengdefelt, bildedata, og
BITMAPINFOHEADER + palett til SLUTT i fila. I noen filer ligger headeren k byte
for tidlig; da mangler de første k bytene av bildedataene.

Verifiserer:
  1. analyse(): 1/8/24 bit, med og uten forskjøvet header (k = 0/14/28/42/56/112)
  2. to_png(): riktige mål og piksler (manglende bytes fylles fra raden over)
  3. ingen falske positive på tilfeldige data, PNG, BMP og tekst
  4. BLOB-konverteringen (hele run()): gjenkjent, konvertert til PNG, navn
     etter «Standardiser .bin» (rec.bin m/ PNG-innhold / rec.png), og file=
     i tableX.xml peker på resultatet

Kjør:  python -X utf8 tests/test_extens_image.py
"""
from __future__ import annotations

import io
import random
import struct
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


def _image(mode: str, w: int, h: int):
    """Testbilde der de to nederste radene er like (fyllingen fra raden over
    gir da eksakt samme bilde)."""
    from PIL import Image
    img = Image.new(mode, (w, h))
    rnd = random.Random(w * h)
    px = img.load()
    for y in range(h):
        for x in range(w):
            yy = min(y, h - 2)              # nederste rad = raden over
            v = (x * 7 + yy * 13 + rnd.randint(0, 3)) % 256 if yy == y else None
            if v is None:
                px[x, y] = px[x, y - 1]
            elif mode == "1":
                px[x, y] = 255 if (x // 3 + y // 4) % 2 else 0
            elif mode == "P":
                px[x, y] = v
            else:
                px[x, y] = (v, (v * 3) % 256, 255 - v)
    if mode == "P":
        img.putpalette([c for i in range(256) for c in (i, 255 - i, (i * 5) % 256)])
    return img


def _extens_bytes(img, shift: int = 0) -> bytes:
    """Bygg en Extens-fil av et PIL-bilde: header k=shift byte for tidlig."""
    b = io.BytesIO()
    img.save(b, "BMP")
    bmp = b.getvalue()
    off = struct.unpack_from("<I", bmp, 10)[0]
    hdr = bmp[14:off]                       # BITMAPINFOHEADER + palett
    bits = bmp[off:]
    size, hsz = len(bits), len(hdr)
    rnd = random.Random(shift)
    tail = bytes(rnd.getrandbits(8) for _ in range(shift))
    return (struct.pack("<HHII", 2160, 1440, size, hsz)
            + bits[shift:] + hdr + tail)


def test_roundtrip(tmp: Path) -> None:
    print("test_roundtrip")
    from PIL import Image, ImageChops
    from siard_workflow.core import extens_image as E
    cases = [("1", 153, 175, 28), ("1", 40, 30, 0), ("P", 350, 193, 14),
             ("P", 37, 21, 0), ("RGB", 222, 124, 42), ("RGB", 549, 192, 0),
             ("RGB", 301, 84, 56), ("RGB", 307, 89, 112)]   # VAF: 3007×839 hadde 112
    for mode, w, h, k in cases:
        img = _image(mode, w, h)
        f = tmp / f"rec_{mode}_{w}_{k}.bin"
        f.write_bytes(_extens_bytes(img, k))
        info = E.analyse(f)
        _ok(info is not None and (info["width"], info["height"], info["shift"]) == (w, h, k),
            f"{mode} {w}×{h}, header forskjøvet {k}: gjenkjent ({info and info['bpp']} bit)")
        out = Image.open(io.BytesIO(E.to_png(f, info)))
        same = (out.size == (w, h) and ImageChops.difference(
            out.convert("RGB"), img.convert("RGB")).getbbox() is None)
        _ok(out.format == "PNG" and same, "  PNG med identiske piksler")


def test_no_false_positives(tmp: Path) -> None:
    print("test_no_false_positives")
    from PIL import Image
    from siard_workflow.core import extens_image as E
    rnd = random.Random(7)
    f = tmp / "x.bin"
    hits = 0
    for _ in range(2000):
        f.write_bytes(bytes(rnd.getrandbits(8) for _ in range(rnd.randint(0, 3000))))
        hits += E.analyse(f) is not None
    _ok(hits == 0, "tilfeldige data: ingen treff")
    for fmt in ("PNG", "BMP", "JPEG"):
        b = io.BytesIO()
        Image.new("RGB", (30, 20), (10, 20, 30)).save(b, fmt)
        f.write_bytes(b.getvalue())
        _ok(E.analyse(f) is None, f"{fmt}: ikke Extens")
    f.write_bytes("Vanlig tekst æøå\n".encode("utf-8") * 50)
    _ok(E.analyse(f) is None, "tekst: ikke Extens")
    # Riktig lengdefelt, men ingen BITMAPINFOHEADER
    f.write_bytes(struct.pack("<HHII", 1, 1, 100, 40) + bytes(140))
    _ok(E.analyse(f) is None, "lengdefelt uten header: ikke Extens")


def _cfg(**kw):
    import settings
    orig = settings.get_config
    settings.get_config = lambda k, d=None: kw.get(k, orig(k, d))
    return lambda: setattr(settings, "get_config", orig)


def _run_blob(tmp: Path, name: str, standardize: bool):
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    root = tmp / name
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(
        '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd">'
        '<lobFolder>content</lobFolder><schemas><schema><folder>schema0</folder><tables><table>'
        '<folder>table0</folder><columns><column><name>id</name><type>INTEGER</type></column>'
        '<column><name>bilde</name><lobFolder>schema0/table0/lob2</lobFolder><type>BLOB</type></column>'
        '</columns><rows>2</rows></table></tables></schema></schemas></siardArchive>', "utf-8")
    lob = root / "content/schema0/table0/lob2"
    lob.mkdir(parents=True)
    img = _image("P", 120, 60)
    ext_data = _extens_bytes(img, 14)
    (lob / "rec124228.bin").write_bytes(ext_data)
    (lob / "rec5.bin").write_bytes(bytes(random.Random(5).getrandbits(8) for _ in range(500)))
    (root / "content/schema0/table0/table0.xml").write_text(
        f'<table>\n<row><c1>1</c1><c2 file="rec124228.bin" length="{len(ext_data)}"/></row>\n'
        '<row><c1>2</c1><c2 file="rec5.bin" length="500"/></row>\n</table>\n', "utf-8")
    logs: list = []
    counts: dict = {}

    def _pcb(ev, **k):
        if ev == "log":
            logs.append(k.get("msg", ""))
        elif ev == "rename_format_counts":
            for e, n in k.get("counts", {}).items():
                counts[e] = counts.get(e, 0) + n
    ctx = WorkflowContext(siard_path=tmp / f"{name}.siard", metadata={
        "file_logger": None,
        "progress_cb": _pcb,
        "stop_event": threading.Event(), "pause_event": threading.Event()})
    ctx.extracted_path = root
    restore = _cfg(standardize_bin_ext=standardize)
    try:
        r = BlobConvertOperation().run(ctx)
    finally:
        restore()
    xml = (root / "content/schema0/table0/table0.xml").read_text("utf-8")
    _run_blob.counts = counts
    return r, lob, xml, "\n".join(logs), img


def test_blob_convert(tmp: Path) -> None:
    print("test_blob_convert")
    from PIL import Image, ImageChops
    r, lob, xml, txt, img = _run_blob(tmp, "std", standardize=True)
    _ok(r.success, f"BLOB-konvertering ok ({r.message})")
    out = lob / "rec124228.bin"
    data = out.read_bytes() if out.exists() else b""
    _ok(data[:8] == b"\x89PNG\r\n\x1a\n", "Standardiser .bin: rec124228.bin inneholder nå PNG")
    _ok(ImageChops.difference(Image.open(out).convert("RGB"),
                              img.convert("RGB")).getbbox() is None,
        "  bildet er rekonstruert riktig")
    _ok("Extens-bilder (DIB) → PNG: 1" in txt and "Extens-bilde (DIB) → PNG" in txt,
        "kategorisering og oppsummering i loggen")
    _ok(_run_blob.counts.get("dib") == 1,
        f"formatdiagrammet teller Extens-bildet som .dib: {_run_blob.counts}")
    _ok('file="rec124228.bin"' in xml and f'length="{len(data)}"' in xml,
        "tableX.xml: referansen beholdes, lengden er PNG-fila sin")
    _ok("<!--" in xml and "Extens-bilde (Windows DIB) til .png" in xml,
        "konverteringskommentar i tableX.xml")
    _ok((lob / "rec5.bin").exists(), "ukjent .bin uten Extens-struktur er urørt")

    r, lob, xml, txt, img = _run_blob(tmp, "png", standardize=False)
    _ok(r.success and (lob / "rec124228.png").exists() and not (lob / "rec124228.bin").exists(),
        "uten Standardiser .bin: rec124228.png")
    _ok('file="rec124228.png"' in xml, "tableX.xml peker på rec124228.png")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="extens_") as td:
        tmp = Path(td)
        test_roundtrip(tmp)
        test_no_false_positives(tmp)
        test_blob_convert(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
