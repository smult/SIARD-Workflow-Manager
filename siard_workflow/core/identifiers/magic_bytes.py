"""
siard_workflow/core/identifiers/magic_bytes.py
Header/magic-byte-basert fildeteksjon (default backend).

Identisk logikk med tidligere _detect() i blob_convert_operation.py — flyttet hit
for å støtte pluggbare backends. Konstanter og hjelpefunksjoner re-eksporteres
fra blob_convert_operation for bakoverkompatibilitet.
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Optional


# ── Magic-byte signaturer ─────────────────────────────────────────────────────

_MAGIC: list[tuple[bytes, str, str]] = [
    (b"%PDF",             "pdf",  "application/pdf"),
    (b"\xd0\xcf\x11\xe0","doc",  "application/msword"),
    (b"\x89PNG",          "png",  "image/png"),
    (b"\xff\xd8\xff",     "jpg",  "image/jpeg"),
    (b"GIF8",             "gif",  "image/gif"),
    (b"BM",               "bmp",  "image/bmp"),
    (b"II*\x00",          "tiff", "image/tiff"),
    (b"MM\x00*",          "tiff", "image/tiff"),
    (b"\x1f\x8b",         "gz",   "application/gzip"),
    (b"BZh",              "bz2",  "application/x-bzip2"),
    (b"7z\xbc\xaf",       "7z",   "application/x-7z-compressed"),
    (b"Rar!",             "rar",  "application/x-rar-compressed"),
    (b"PK\x03\x04",       "zip",  "application/zip"),
    (b"{\\rtf",           "rtf",  "application/rtf"),
    (b"{\\RTF",           "rtf",  "application/rtf"),
    (b"<html",            "html", "text/html"),
    (b"<HTML",            "html", "text/html"),
    (b"<?xml",            "xml",  "application/xml"),
    (b"<?XML",            "xml",  "application/xml"),
    (b"MZ",               "exe",  "application/x-msdownload"),
    (b"ID3",              "mp3",  "audio/mpeg"),
    (b"\xff\xfb",         "mp3",  "audio/mpeg"),
    (b"\xff\xf3",         "mp3",  "audio/mpeg"),
    (b"\xff\xf2",         "mp3",  "audio/mpeg"),
    (b"RIFF",             "wav",  "audio/wav"),
    (b"fLaC",             "flac", "audio/flac"),
    (b"OggS",             "ogg",  "audio/ogg"),
    (b"\x00\x00\x01\xba", "mpg",  "video/mpeg"),
    (b"\x00\x00\x01\xb3", "mpg",  "video/mpeg"),
    (b"\xff\x57\x50\x43", "wpd",  "application/vnd.wordperfect"),
    (b"\x1a\x00\x00\x04", "wpd",  "application/vnd.wordperfect"),
    (b"\x31\xbe\x00\x00", "wri",  "application/x-mswrite"),
    (b"\x32\xbe\x00\x00", "wri",  "application/x-mswrite"),
    (b"\x00\x00\x00\x0cftyp", "mp4", "video/mp4"),
    (b"\x00\x00\x00\x18ftyp", "mp4", "video/mp4"),
    (b"\x00\x00\x00\x20ftyp", "mp4", "video/mp4"),
]

_OLE2_SUBTYPES: list[tuple[bytes, str, str]] = [
    (b"Microsoft Excel",      "xls", "application/vnd.ms-excel"),
    (b"Microsoft PowerPoint", "ppt", "application/vnd.ms-powerpoint"),
    (b"Calc",                 "xls", "application/vnd.ms-excel"),
    (b"Impress",              "ppt", "application/vnd.ms-powerpoint"),
]

_OOXML_SIGS = {
    "word/":       ("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    "xl/":         ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    "ppt/":        ("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
    "content.xml": ("odt",  "application/vnd.oasis.opendocument.text"),
}

_WORD_TAIL_SIGS = (
    b"W\x00o\x00r\x00d\x00D\x00o\x00c\x00u\x00m\x00e\x00n\x00t\x00",
    b"Word.Document",
    b"1\x00T\x00a\x00b\x00l\x00e\x00",
    b"0\x00T\x00a\x00b\x00l\x00e\x00",
)


# ── OLE2-deteksjon ────────────────────────────────────────────────────────────

def _detect_ole2_type(data: bytes) -> "tuple[str, str, bool] | None":
    """
    Les OLE2 compound document directory og returner (ext, mime, is_encrypted).

    is_encrypted=True hvis filen er passordbeskyttet ("EncryptionInfo"-strøm
    finnes, noe som gjelder for både Office 2007+ OOXML kryptert og eldre
    CryptoAPI-krypterte .doc/.xls/.ppt).
    """
    import struct as _struct

    _CLSID_WORD  = (b"\x06\x09\x02\x00\x00\x00\x00\x00"
                    b"\xc0\x00\x00\x00\x00\x00\x00\x46")
    _CLSID_EXCEL = (b"\x20\x08\x02\x00\x00\x00\x00\x00"
                    b"\xc0\x00\x00\x00\x00\x00\x00\x46")
    _CLSID_PPT   = (b"\x10\x8d\x81\x64\x9b\x4f\xcf\x11"
                    b"\x86\xea\x00\xaa\x00\xb9\x29\xe8")

    try:
        sector_size = 1 << _struct.unpack_from('<H', data, 30)[0]
        dir_sector  = _struct.unpack_from('<I', data, 48)[0]
        if dir_sector == 0xFFFFFFFE or sector_size < 64:
            return None
        dir_offset  = 512 + dir_sector * sector_size
    except Exception:
        return None

    root_clsid = b""
    if dir_offset + 96 <= len(data):
        root_clsid = data[dir_offset + 80: dir_offset + 96]

    top_names: set[str] = set()
    for i in range(64):
        offset = dir_offset + i * 128
        if offset + 128 > len(data):
            break
        try:
            name_len   = _struct.unpack_from('<H', data, offset + 64)[0]
            entry_type = data[offset + 66]
            if entry_type not in (1, 2) or name_len < 2 or name_len > 64:
                continue
            name = data[offset:offset + name_len - 2].decode('utf-16-le',
                                                              errors='replace')
            name = name.strip('\x00')
            if name:
                top_names.add(name)
        except Exception:
            continue

    is_encrypted = "EncryptionInfo" in top_names

    if is_encrypted and "EncryptedPackage" in top_names:
        if root_clsid == _CLSID_EXCEL:
            return ("xlsx",
                    "application/vnd.openxmlformats-officedocument"
                    ".spreadsheetml.sheet",
                    True)
        if root_clsid == _CLSID_PPT:
            return ("pptx",
                    "application/vnd.openxmlformats-officedocument"
                    ".presentationml.presentation",
                    True)
        return ("docx",
                "application/vnd.openxmlformats-officedocument"
                ".wordprocessingml.document",
                True)

    if "WordDocument" in top_names:
        return "doc", "application/msword", is_encrypted
    if "Workbook" in top_names or "Book" in top_names:
        return "xls", "application/vnd.ms-excel", is_encrypted
    if "PowerPoint Document" in top_names:
        return "ppt", "application/vnd.ms-powerpoint", is_encrypted

    _scan = data[:32768]
    _WORD_U16 = b"W\x00o\x00r\x00d\x00D\x00o\x00c\x00u\x00m\x00e\x00n\x00t\x00"
    if _WORD_U16 in _scan or b"Word.Document" in _scan:
        return "doc", "application/msword", is_encrypted

    _XLS_U16 = b"W\x00o\x00r\x00k\x00b\x00o\x00o\x00k\x00"
    if _XLS_U16 in _scan or b"Excel.Sheet" in _scan:
        return "xls", "application/vnd.ms-excel", is_encrypted

    _PPT_U16 = b"P\x00o\x00w\x00e\x00r\x00P\x00o\x00i\x00n\x00t\x00 \x00D\x00o\x00c\x00u\x00m\x00e\x00n\x00t\x00"
    if _PPT_U16 in _scan or b"PowerPoint.Show" in _scan:
        return "ppt", "application/vnd.ms-powerpoint", is_encrypted

    early = data[:4096]
    for marker, ext, mime in _OLE2_SUBTYPES:
        if marker in early:
            return ext, mime, is_encrypted

    return None


def _ole2_tail_is_word(data: bytes, tail_size: int = 16384) -> bool:
    """
    Sjekk om halen av en OLE2-fil inneholder Word-spesifikke signaturer.
    Brukes som tiebreaker når vanlig deteksjon feilidentifiserer en Word-fil
    med innebygde Excel/PPT-objekter som XLS eller PPT.
    """
    tail = data[-tail_size:] if len(data) > tail_size else data
    return any(sig in tail for sig in _WORD_TAIL_SIGS)


# ── Hovedfunksjon ─────────────────────────────────────────────────────────────

def _detect(data: bytes) -> tuple[str, str, bool]:
    """
    Returner (ext, mime, is_encrypted).
    is_encrypted=True betyr at filen er passordbeskyttet og ikke skal
    konverteres av LibreOffice — kun kopiere med riktig filendelse.
    """
    if not data:
        return "bin", "application/octet-stream", False

    _BOMS = (
        b"\xef\xbb\xbf",
        b"\xff\xfe",
        b"\xfe\xff",
        b"\xff\xfe\x00\x00",
        b"\x00\x00\xfe\xff",
    )
    stripped = data
    for bom in _BOMS:
        if stripped.startswith(bom):
            stripped = stripped[len(bom):]
            break
    stripped = stripped.lstrip(b" \t\r\n")

    search_window = data[:512]

    if data[:4] == b"\xd0\xcf\x11\xe0" or stripped[:4] == b"\xd0\xcf\x11\xe0":
        ole_type = _detect_ole2_type(data)
        if ole_type:
            return ole_type
        return "doc", "application/msword", False

    if len(data) > 10 and data[4:7] == b"BZh" and data[7:8] in b"123456789":
        return "bz2", "application/x-bzip2", False

    if data[:4] == b"PK\x03\x04" or stripped[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                names = "\n".join(z.namelist())
                for k, v in _OOXML_SIGS.items():
                    if k in names:
                        return v[0], v[1], False
        except Exception:
            pass
        return "zip", "application/zip", False

    for sig, ext, mime in _MAGIC:
        slen = len(sig)
        if stripped[:slen] == sig:
            return ext, mime, False
        if sig[0:1] in (b"{", b"<", b"%") and sig in search_window:
            return ext, mime, False

    if stripped[:16].startswith(b"<!WPTools_Format"):
        return "wpt", "application/x-wptools", False

    if stripped.startswith(b"<"):
        return "xml", "application/xml", False

    try:
        if b"\x00" not in data[:512] and data[:512]:
            data[:512].decode("utf-8")
            return "txt", "text/plain", False
    except (UnicodeDecodeError, ValueError):
        pass

    return "bin", "application/octet-stream", False


# ── Backend-klasse ────────────────────────────────────────────────────────────

class MagicIdentifier:
    """Magic-byte/header-deteksjon — felles backend-API."""

    name = "magic"

    def identify(self, data: Optional[bytes] = None,
                 path: Optional[Path] = None) -> tuple[str, str, bool]:
        if data is None and path is not None:
            try:
                data = Path(path).read_bytes()[:65536]
            except Exception:
                return ("bin", "application/octet-stream", False)
        if not data:
            return ("bin", "application/octet-stream", False)
        return _detect(data)

    def pre_scan(self, root: Path,
                 files: Optional[list] = None,
                 max_workers: Optional[int] = None,
                 progress_cb=None) -> None:
        """
        Magic-bytes har ingen batch-fase — no-op.
        Signaturen følger Protocol slik at backenden kan brukes om hverandre.
        Parametrene max_workers og progress_cb ignoreres.
        """
        return None
