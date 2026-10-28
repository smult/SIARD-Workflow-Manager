"""siard_workflow/core/extens_image.py

Bilder lagret i LOB-er av Extens (skoleadministrativt system, Agder
fylkeskommune): en innpakket Windows DIB uten kjent filsignatur — derfor
kjenner verken Siegfried/DROID eller magic-bytes dem igjen.

Format (fastslått fra prøver med 1, 8 og 24 bit per piksel):

    byte 0–3    to u16 — ikke bildets mål (trolig visningsstørrelse)
    byte 4–7    u32  bildedatastørrelse (size)
    byte 8–11   u32  header+palett-størrelse (hsz): 40 (24/32 bit),
                     40+4·2^bpp for palettbilder (48 = 1 bit, 1064 = 8 bit)
    byte 12–    bildedata (DIB: nederste rad først, rader 4-byte-justert)
    deretter    BITMAPINFOHEADER (biSize 40) + palett — til SLUTT i fila

    filstørrelse = 12 + size + hsz

I noen filer ligger headeren k byte for tidlig (k = 12 + size − headerposisjon,
sett 14/28/42/56/112). Da mangler de FØRSTE k bytene av bildedataene (begynnelsen av
nederste rad): å lese fra byte 12 forskyver hver rad k byte og gir en loddrett
«omslag»-strek. De k bytene etter header+palett er ikke bildedata. Ved
rekonstruksjonen fylles de manglende bytene fra raden over.
"""
from __future__ import annotations

import io
import struct
from pathlib import Path
from typing import Optional

EXT = "extens_dib"
MIME = "image/x-extens-dib"
LABEL = "Extens-bilde (Windows DIB)"
_BPP = (1, 4, 8, 16, 24, 32)
# Headerforskyvningen er et multiplum av 14 (størrelsen på et BMP-filhode);
# sett 14–112 (VAF: 3007×839 24 bit hadde 112). Søk godt utover det.
_MAX_SHIFT = 4096


def _palette_ok(hsz: int) -> bool:
    return hsz == 40 or hsz == 52 or (hsz > 40 and (hsz - 40) % 4 == 0 and hsz <= 40 + 1024)


def analyse(path: Path) -> "Optional[dict]":
    """
    Beskrivelse av et Extens-bilde, eller None hvis fila ikke er et.
    Leser bare starten og slutten av fila.
    """
    try:
        path = Path(path)
        total = path.stat().st_size
        if total < 12 + 40:
            return None
        with open(path, "rb") as f:
            head = f.read(12)
            a, b, size, hsz = struct.unpack_from("<HHII", head, 0)
            if not _palette_ok(hsz) or 12 + size + hsz != total or size == 0:
                return None
            tail_from = max(12, 12 + size - _MAX_SHIFT)
            f.seek(tail_from)
            tail = f.read()
    except (OSError, struct.error):
        return None
    # BITMAPINFOHEADER: ved 12+size, eller inntil _MAX_SHIFT byte tidligere
    for hp in range(12 + size, tail_from - 1, -1):
        rel = hp - tail_from
        if rel + hsz > len(tail) or tail[rel:rel + 4] != b"\x28\x00\x00\x00":
            continue
        w, h, planes, bpp, comp = struct.unpack_from("<iiHHI", tail, rel + 4)
        if planes != 1 or bpp not in _BPP or comp not in (0, 3) or not (0 < w < 30000) \
                or h == 0 or abs(h) > 30000:
            continue
        stride = ((w * bpp + 31) // 32) * 4
        need = stride * abs(h)
        if not (size - 8 <= need <= size):
            continue
        if bpp <= 8 and hsz < 40 + 4 * (1 << bpp) and _clr_used(tail, rel) == 0:
            continue                       # palettbilde uten plass til paletten
        return {"header_pos": hp, "shift": 12 + size - hp, "width": w, "height": abs(h),
                "bpp": bpp, "stride": stride, "size": size, "hsz": hsz}
    return None


def _clr_used(buf: bytes, rel: int) -> int:
    try:
        return struct.unpack_from("<I", buf, rel + 32)[0]
    except struct.error:
        return 0


def is_extens_image(path: Path) -> bool:
    return analyse(path) is not None


def to_png(path: Path, info: "Optional[dict]" = None) -> bytes:
    """Rekonstruer bildet og returner PNG-bytes. Kaster ValueError hvis ikke Extens-bilde."""
    from PIL import Image
    info = info or analyse(path)
    if not info:
        raise ValueError("ikke et Extens-bilde")
    d = Path(path).read_bytes()
    hp, hsz, stride = info["header_pos"], info["hsz"], info["stride"]
    header = d[hp:hp + hsz]
    need = stride * info["height"]
    k = info["shift"]
    bits = bytearray(k) + d[12:hp]                 # de første k bytene mangler
    for i in reversed(range(k)):                   # fyll fra raden over
        j = i + stride
        bits[i] = bits[j] if j < len(bits) else 0xFF
    bits = bytes(bits[:need]).ljust(need, b"\x00")
    bmp = (b"BM" + struct.pack("<IHHI", 14 + hsz + need, 0, 0, 14 + hsz) + header + bits)
    img = Image.open(io.BytesIO(bmp))
    img.load()
    out = io.BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()
