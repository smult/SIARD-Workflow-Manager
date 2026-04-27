"""
BlobConvertOperation  —  SIARD BLOB/NBLOB/NCLOB-konvertering
=======================================================================

Arkitektur for ytelse:
1. ZIP ekstraheres til tmpdir med extractall()
2. Alle filer detekteres parallelt              (CPU-bundet, ingen I/O)
3. LibreOffice kjøres i BATCH-modus per worker  (én LO-oppstart per worker,
   konverterer N filer — eliminerer startup-overhead som er ~3s/fil)
4. Ny SIARD pakkes fra filsystem               (streaming, ikke writestr loop)
5. tableX.xml patches                           (inline NBLOB/NCLOB + rename_map)

Pause/stopp: threading.Event via ctx.metadata["stop_event"/"pause_event"]
(trådsikre, ingen dict-race-condition)

Fremdrift via ctx.metadata["progress_cb"]:
    progress("init",       total=N)
    progress("phase",      phase=N, total_phases=M, label=str)
    progress("phase_done")
    progress("file_start", idx=i, filename=str, detected_ext=str, mime=str)
    progress("file_done",  idx=i, filename=str, detected_ext=str,
                           result_ext=str, ok=bool, msg=str, stats=dict)
    progress("error",      file=str, error=str)
    progress("finish",     stats=dict)
    progress("aborted",    stats=dict)
"""

from __future__ import annotations

import concurrent.futures
import csv
import datetime
import hashlib
import base64
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path, PurePosixPath

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.blob_csv_logger import BlobCsvLogger, ConversionErrorLogger
from siard_workflow.core.siard_format import (
    detect_siard_version, siard_version_transform,
    get_target_siard_version, is_siard_xml,
    sanitize_metadata_schema_names,
)

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
    (b"BZh",              "bz2",  "application/x-bzip2"),  # standard bzip2
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
    # Lyd — godkjent av §5-17
    (b"ID3",              "mp3",  "audio/mpeg"),
    (b"\xff\xfb",         "mp3",  "audio/mpeg"),
    (b"\xff\xf3",         "mp3",  "audio/mpeg"),
    (b"\xff\xf2",         "mp3",  "audio/mpeg"),
    (b"RIFF",             "wav",  "audio/wav"),
    (b"fLaC",             "flac", "audio/flac"),
    (b"OggS",             "ogg",  "audio/ogg"),
    # Video — godkjent av §5-17
    (b"\x00\x00\x01\xba", "mpg",  "video/mpeg"),   # MPEG-2 PS
    (b"\x00\x00\x01\xb3", "mpg",  "video/mpeg"),   # MPEG sequence
    # WordPerfect
    (b"\xff\x57\x50\x43", "wpd",  "application/vnd.wordperfect"),
    (b"\x1a\x00\x00\x04", "wpd",  "application/vnd.wordperfect"),
    # MS Write
    (b"\x31\xbe\x00\x00", "wri",  "application/x-mswrite"),
    (b"\x32\xbe\x00\x00", "wri",  "application/x-mswrite"),
    # TIFF via ftyp-boks (JPEG2000, MP4/H.264)
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

# ── Formatkategorier basert på riksarkivarens forskrift §5-17 ─────────────────

# Konverteres til PDF/A via LibreOffice
def _get_lo_convertible() -> set:
    """Les konverterbare filformater fra config.json. Faller tilbake til innebygd liste."""
    _DEFAULT = {
        "doc", "docx", "dot", "dotx",
        "odt", "ott", "odg",
        "rtf",
        "wpd", "wp", "wp5", "wp6", "wps",
        "wri", "lwp", "sxw", "sdw",
    }
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _root = _Path(__file__).parent.parent.parent
        if str(_root) not in _sys.path:
            _sys.path.insert(0, str(_root))
        from settings import get_config
        lst = get_config("lo_convertible")
        if lst and isinstance(lst, list):
            return {str(e).lower().strip() for e in lst if e}
    except Exception:
        pass
    return _DEFAULT


_LO_CONVERTIBLE = _get_lo_convertible()

# Beholdes som-er (kun rename) — godkjente arkivformater etter §5-17
def _get_rename_only() -> set:
    """Les behold-formater fra config.json. Faller tilbake til innebygd liste."""
    _DEFAULT = {
        "csv",
        "tiff", "tif", "jpg", "jpeg", "png", "gif", "bmp",
        "ppt", "pptx", "pot", "potx", "odp",
        "xls", "xlsx", "xlt", "xltx", "ods",
        "mp3", "wav", "flac", "ogg",
        "mpg", "mpeg", "mp4", "m4v", "avi",
        "sosi", "gml", "ifc", "warc",
        "zip", "tar", "gz", "bz2",
        "msg", "eml",
        "jp2", "jpe", "webp", "svg",
        "exe", "7z", "rar",
    }
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _root = _Path(__file__).parent.parent.parent
        if str(_root) not in _sys.path:
            _sys.path.insert(0, str(_root))
        from settings import get_config
        lst = get_config("rename_only")
        if lst and isinstance(lst, list):
            return {str(e).lower().strip() for e in lst if e}
    except Exception:
        pass
    return _DEFAULT


_RENAME_ONLY_EXTS = _get_rename_only()


def _get_lo_upgrade() -> dict:
    """
    Les oppgraderingstabell fra config.json.
    Returnerer {gammelt_ext: nytt_ext}, f.eks. {"xls": "xlsx", "ppt": "pptx"}.
    """
    _DEFAULT = {
        "xls": "xlsx", "xlt": "xlsx",
        "ppt": "pptx", "pot": "pptx",
    }
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _root = _Path(__file__).parent.parent.parent
        if str(_root) not in _sys.path:
            _sys.path.insert(0, str(_root))
        from settings import get_config
        mapping = get_config("lo_upgrade")
        if mapping and isinstance(mapping, dict):
            return {str(k).lower(): str(v).lower() for k, v in mapping.items()}
    except Exception:
        pass
    return _DEFAULT


_LO_UPGRADE: dict[str, str] = _get_lo_upgrade()

_PDFA_VERSIONS: dict[str, tuple[str, str]] = {
    # label: (SelectPdfVersion, UseTaggedPDF)
    "PDF/A-1a (ISO 19005-1, level A)": ("1", "true"),
    "PDF/A-1b (ISO 19005-1, level B)": ("2", "false"),
    "PDF/A-2b (ISO 19005-2, level B)": ("6", "false"),
    "PDF/A-2u (ISO 19005-2, level U)": ("7", "false"),
    "PDF/A-3b (ISO 19005-3, level B)": ("9", "false"),
}
_PDFA_DEFAULT = "PDF/A-2u (ISO 19005-2, level U)"


def _build_pdfa_filter(version_label: str) -> str:
    """Bygg LibreOffice --convert-to filter-streng for valgt PDF/A-versjon."""
    ver, tagged = _PDFA_VERSIONS.get(version_label, _PDFA_VERSIONS[_PDFA_DEFAULT])
    return (
        f'pdf:writer_pdf_Export:{{"SelectPdfVersion":{{"type":"long","value":"{ver}"}},'
        f'"UseTaggedPDF":{{"type":"boolean","value":"{tagged}"}}}}'
    )

_NS_RE = re.compile(r"\{([^}]+)\}")

_INLINE_TAGS = {
    "nblob", "nclob", "blob", "clob",
    "blobvalue", "clobvalue", "nblobvalue", "nclobvalue",
    "ncharacterlargeobject", "characterlargeobject", "binarylargeobject",
}

_CHECKSUM_TAGS = {
    "md5", "sha1", "sha256", "sha-1", "sha-256", "sha512",
    "digest", "checksum", "hash", "messagedigest",
}


# ── Hjelpefunksjoner ──────────────────────────────────────────────────────────

def _detect_ole2_type(data: bytes) -> tuple[str, str, bool] | None:
    """
    Les OLE2 compound document directory og returner (ext, mime, is_encrypted).

    is_encrypted=True hvis filen er passordbeskyttet ("EncryptionInfo"-strøm
    finnes, noe som gjelder for både Office 2007+ OOXML kryptert og eldre
    CryptoAPI-krypterte .doc/.xls/.ppt).

    For OOXML-krypterte filer (EncryptedPackage + EncryptionInfo):
    bestemmes filtypen fra root-entryens CLSID, med docx som fallback.

    Kun directory entries med type=2 (stream) eller type=1 (storage) i det
    øverste nivået teller. Innebygde OLE-objekter (Excel embedded i Word,
    osv.) vil aldri vises på rot-nivå og forstyrrer ikke deteksjonen.
    """
    import struct as _struct

    # Kjente root-entry CLSIDs (little-endian GUID) for krypterte OOXML-filer
    _CLSID_WORD  = (b"\x06\x09\x02\x00\x00\x00\x00\x00"
                    b"\xc0\x00\x00\x00\x00\x00\x00\x46")  # Word.Document.8
    _CLSID_EXCEL = (b"\x20\x08\x02\x00\x00\x00\x00\x00"
                    b"\xc0\x00\x00\x00\x00\x00\x00\x46")  # Excel.Sheet.8
    _CLSID_PPT   = (b"\x10\x8d\x81\x64\x9b\x4f\xcf\x11"
                    b"\x86\xea\x00\xaa\x00\xb9\x29\xe8")  # PowerPoint.Show

    # OLE2-sektordimensjoner fra header
    try:
        sector_size = 1 << _struct.unpack_from('<H', data, 30)[0]  # SectorShift
        dir_sector  = _struct.unpack_from('<I', data, 48)[0]       # FirstDirSectorLoc
        if dir_sector == 0xFFFFFFFE or sector_size < 64:
            return None
        dir_offset  = 512 + dir_sector * sector_size
    except Exception:
        return None

    # Les root-entry CLSID (bytes 80-95 av entry 0, type=5).
    # Brukes til å identifisere krypterte OOXML-filer.
    root_clsid = b""
    if dir_offset + 96 <= len(data):
        root_clsid = data[dir_offset + 80: dir_offset + 96]

    # Les directory-entries lineært (type 1=storage, 2=stream).
    # Entry-format: 128 bytes, navn i UTF-16LE, lengde i byte 64+65.
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

    # ── Krypteringsdeteksjon ──────────────────────────────────────────────────
    # "EncryptionInfo"-strøm finnes i alle moderne krypterte Office-filer.
    # "EncryptedPackage" + "EncryptionInfo" = OOXML-fil kryptert med passord.
    is_encrypted = "EncryptionInfo" in top_names

    if is_encrypted and "EncryptedPackage" in top_names:
        # OOXML kryptert — original filtype leses fra root-entry CLSID
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
        # Word CLSID eller ukjent → docx som fallback
        return ("docx",
                "application/vnd.openxmlformats-officedocument"
                ".wordprocessingml.document",
                True)

    # ── Prioritert strøm-navnsjekk (kryptert eller ikke) ─────────────────────
    if "WordDocument" in top_names:
        return "doc", "application/msword", is_encrypted
    if "Workbook" in top_names or "Book" in top_names:
        return "xls", "application/vnd.ms-excel", is_encrypted
    if "PowerPoint Document" in top_names:
        return "ppt", "application/vnd.ms-powerpoint", is_encrypted

    # Fallback 1: Søk de første 32 KB etter OLE2-katalognavn (UTF-16LE) og
    # CompObj ProgID-strenger (ASCII) — fanger tilfeller der FAT-kjeden
    # gjør at katalogentryene ikke er sammenhengende (se Word.Document.8).
    # Begrensning til 32 KB unngår falske treff fra innebygde objekter eller
    # referanser som kan forekomme langt inn i filen (f.eks. "Excel.Sheet"
    # i et Word-dokument med innebygd regneark).
    # Word-sjekk MÅ komme før PPT-sjekk; et Word-dokument med innebygd
    # PPT-objekt kan ha "Microsoft PowerPoint" tidlig i datastrømmen.
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

    # Fallback 2: subtype-markører kun i de første 4 KB
    # (ikke hele filen — unngår treffer på innebygde objekter)
    early = data[:4096]
    for marker, ext, mime in _OLE2_SUBTYPES:
        if marker in early:
            return ext, mime, is_encrypted

    return None


def _detect(data: bytes) -> tuple[str, str, bool]:
    """
    Returner (ext, mime, is_encrypted).
    is_encrypted=True betyr at filen er passordbeskyttet og ikke skal
    konverteres av LibreOffice — kun kopiere med riktig filendelse.
    """
    if not data:
        return "bin", "application/octet-stream", False

    # Strip kjente BOM-er og leading whitespace for å nå frem til faktisk header
    # Mange eldre systemer skriver UTF-8/UTF-16 BOM foran RTF, HTML etc.
    _BOMS = (
        b"\xef\xbb\xbf",       # UTF-8 BOM
        b"\xff\xfe",            # UTF-16 LE BOM
        b"\xfe\xff",            # UTF-16 BE BOM
        b"\xff\xfe\x00\x00",   # UTF-32 LE BOM
        b"\x00\x00\xfe\xff",   # UTF-32 BE BOM
    )
    stripped = data
    for bom in _BOMS:
        if stripped.startswith(bom):
            stripped = stripped[len(bom):]
            break
    # Strip leading whitespace/newlines (maks 64 bytes)
    stripped = stripped.lstrip(b" \t\r\n")

    # Søk også litt inn i filen (maks 512 bytes fra start) for headere
    # som kan ha metadata/kommentarer foran seg
    search_window = data[:512]

    # OLE2 (DOC/XLS/PPT) — alltid i byte 0
    if data[:4] == b"\xd0\xcf\x11\xe0" or stripped[:4] == b"\xd0\xcf\x11\xe0":
        # Les OLE2 directory for å finne root stream-navn.
        # Dette er den eneste pålitelige metoden — subtype-markører i innholdet
        # kan stamme fra innebygde OLE-objekter (f.eks. Excel embedded i Word).
        ole_type = _detect_ole2_type(data)
        if ole_type:
            return ole_type  # 3-tuple (ext, mime, is_encrypted)
        return "doc", "application/msword", False

    # BZ2 med 4-byte størrelses-header [uint32_LE][BZh9...]
    # Brukes av norske fagsystemer (f.eks. PPT-tjenester, KITH-meldinger)
    if len(data) > 10 and data[4:7] == b"BZh" and data[7:8] in b"123456789":
        return "bz2", "application/x-bzip2", False

    # OOXML/ODF (ZIP-basert) — alltid i byte 0
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

    # MAGIC-tabell — sjekk både stripped og søkevindu
    for sig, ext, mime in _MAGIC:
        slen = len(sig)
        if stripped[:slen] == sig:
            return ext, mime, False
        # Søk etter signaturen innen de første 512 bytes for tekst-baserte formater
        if sig[0:1] in (b"{", b"<", b"%") and sig in search_window:
            return ext, mime, False

    # ── Tekst-basert fallback (ingen magic-byte treff) ───────────────────────
    # WPTools native format — starter med <!WPTools_Format etter BOM/whitespace
    if stripped[:16].startswith(b"<!WPTools_Format"):
        return "wpt", "application/x-wptools", False

    # Generisk XML/markup-innhold som ikke matchet kjent MAGIC-signatur
    if stripped.startswith(b"<"):
        return "xml", "application/xml", False

    # Gyldig UTF-8 uten null-bytes → ren tekst
    try:
        if b"\x00" not in data[:512] and data[:512]:
            data[:512].decode("utf-8")
            return "txt", "text/plain", False
    except (UnicodeDecodeError, ValueError):
        pass

    # Ukjent binærformat
    return "bin", "application/octet-stream", False


def _wpt_to_rtf(wpt_bytes: bytes) -> bytes:
    """
    Konverter WPTools native format (WPT/XML) til RTF via tekstuttrekk.

    WPT er et proprietært XML-format fra WPCubed. Formatet er ikke offentlig
    dokumentert, men er XML-basert med tekst i element-innhold og attributter.
    Denne funksjonen gjør et best-effort tekstuttrekk og pakker det i gyldig RTF.

    Strukturen vi utnytter:
      - Tekst i <P>-elementer (avsnitt)
      - Tabellceller i <C>-elementer
      - Overskrifter identifisert via style-attributter (f.eks. H1, H2)
      - Inline-tekst i <T>-elementer med attributter for fett/kursiv

    Returnerer RTF-bytes, eller tomt bytes ved parsefeil.
    """
    try:
        raw = wpt_bytes
        # Fjern BOM og WPTools-prosesseringsinstruksjon som ikke er gyldig XML
        # <!WPTools_Format V=N/> er ikke gyldig XML — fjern den
        text = raw.decode("utf-8", errors="replace")
        # Fjern <!WPTools_Format ...?> eller <!WPTools_Format .../>
        text = re.sub(r"<\!WPTools_Format[^>]*/?>", "", text, count=1)
        # Fjern andre ikke-XML-konstruksjoner (processing instructions med !)
        text = re.sub(r"<![A-Z][^>]*>", "", text)
        # Pakk i rot-element hvis mangler
        stripped = text.strip()
        if not stripped.startswith("<"):
            return b""
        if not stripped.startswith("<?xml") and not re.match(r"<[A-Za-z]", stripped):
            return b""
        # Prøv å parse som XML
        try:
            root = ET.fromstring(stripped)
        except ET.ParseError:
            # Forsøk med wrapper-rot
            try:
                root = ET.fromstring(f"<ROOT>{stripped}</ROOT>")
            except ET.ParseError:
                # Siste utvei: ren tekstuttrekk via regex
                return _wpt_raw_text_to_rtf(wpt_bytes)

        paragraphs = _wpt_extract_paragraphs(root)
        return _paragraphs_to_rtf(paragraphs)

    except Exception:
        return _wpt_raw_text_to_rtf(wpt_bytes)


def _wpt_extract_paragraphs(root: ET.Element) -> list[tuple[str, str]]:
    """
    Trekk ut avsnitt fra WPT XML-tre.
    Returnerer liste av (tekst, stil) der stil er "h1","h2","h3","normal","table".
    """
    paragraphs: list[tuple[str, str]] = []

    def _text_of(el: ET.Element) -> str:
        """Rekursivt saml all tekst fra et element."""
        parts = []
        if el.text:
            parts.append(el.text.strip())
        for child in el:
            parts.append(_text_of(child))
            if child.tail:
                parts.append(child.tail.strip())
        return " ".join(p for p in parts if p)

    def _style_of(el: ET.Element) -> str:
        """Gjett stiltype fra attributter."""
        for attr in ("Style", "style", "ParaStyle", "parastyle", "Class", "class"):
            val = el.get(attr, "").lower()
            if "h1" in val or "heading1" in val or "tittel" in val:
                return "h1"
            if "h2" in val or "heading2" in val:
                return "h2"
            if "h3" in val or "heading3" in val:
                return "h3"
        return "normal"

    def _walk(el: ET.Element, in_table: bool = False):
        tag = el.tag.split("}")[-1].upper() if "}" in el.tag else el.tag.upper()

        # Tabellrad/-celle
        if tag in ("TR", "ROW"):
            cells = []
            for child in el:
                ct = child.tag.split("}")[-1].upper() if "}" in child.tag else child.tag.upper()
                if ct in ("TD", "TH", "C", "CELL"):
                    cells.append(_text_of(child))
            if cells:
                paragraphs.append(("  |  ".join(cells), "table"))
            return

        # Avsnitt
        if tag in ("P", "PARA", "PARAGRAPH"):
            txt = _text_of(el)
            if txt:
                paragraphs.append((txt, _style_of(el)))
            return

        # Overskrifter
        if tag in ("H1", "H2", "H3", "H4"):
            txt = _text_of(el)
            if txt:
                paragraphs.append((txt, tag.lower()))
            return

        # Gå ned
        for child in el:
            _walk(child, in_table)

    _walk(root)

    # Fallback: om vi ikke fant noe, gjør flat teksthøsting
    if not paragraphs:
        all_text = " ".join(
            t.strip() for t in root.itertext() if t.strip()
        )
        if all_text:
            # Del opp på punktum+mellomrom som pseudo-avsnitt
            for sentence in re.split(r"(?<=[.!?])\s{2,}", all_text):
                s = sentence.strip()
                if s:
                    paragraphs.append((s, "normal"))

    return paragraphs


def _paragraphs_to_rtf(paragraphs: list[tuple[str, str]]) -> bytes:
    """Pakk avsnittsliste inn i gyldig RTF med enkel formatering."""

    def _rtf_escape(s: str) -> str:
        s = s.replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")
        out = []
        for ch in s:
            cp = ord(ch)
            if cp < 128:
                out.append(ch)
            else:
                out.append(f"\\u{cp}?")
        return "".join(out)

    lines = [
        r"{\rtf1\ansi\deff0",
        r"{\fonttbl{\f0\froman\fcharset0 Times New Roman;}{\f1\fswiss Arial;}}",
        r"{\colortbl;\red0\green0\blue0;}",
        r"\paperw11906\paperh16838\margl1800\margr1800\margt1400\margb1400",
    ]

    for txt, stil in paragraphs:
        escaped = _rtf_escape(txt)
        if stil == "h1":
            lines.append(
                r"{\pard\sb240\sa120\b\f1\fs28 " + escaped + r"\par}")
        elif stil == "h2":
            lines.append(
                r"{\pard\sb200\sa80\b\f1\fs24 " + escaped + r"\par}")
        elif stil == "h3":
            lines.append(
                r"{\pard\sb160\sa60\b\f1\fs22 " + escaped + r"\par}")
        elif stil == "table":
            lines.append(
                r"{\pard\sb60\sa60\f0\fs20\li360 " + escaped + r"\par}")
        else:
            lines.append(
                r"{\pard\sb120\sa60\f0\fs22 " + escaped + r"\par}")

    lines.append("}")
    return "\n".join(lines).encode("ascii", errors="replace")


def _wpt_raw_text_to_rtf(wpt_bytes: bytes) -> bytes:
    """
    Siste-utvei: trekk ut synlig tekst fra WPT via regex (uten XML-parsing).
    Fjerner alle tagger og returnerer ren tekst pakket i RTF.
    """
    try:
        text = wpt_bytes.decode("utf-8", errors="replace")
        # Fjern alle XML-tagger
        text = re.sub(r"<[^>]+>", " ", text)
        # Komprimer whitespace
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return b""
        paragraphs = []
        for chunk in re.split(r"(?<=[.!?])\s{2,}|\n{2,}", text):
            c = chunk.strip()
            if c:
                paragraphs.append((c, "normal"))
        return _paragraphs_to_rtf(paragraphs)
    except Exception:
        return b""


def _err_clean(msg: str) -> str:
    """Fjern intern batch-filinformasjon og berik kjente feilmeldinger."""
    if not msg:
        return msg
    # Fjern "Filer: ..." (interne batch-filnavn)
    idx = msg.find(". Filer: ")
    cleaned = msg[:idx] if idx >= 0 else msg
    # Legg til hint om passordbeskyttelse ved LibreOffice-lastfeil
    if "source file could not be loaded" in cleaned:
        cleaned += ". Trolig grunnet passordbeskyttelse og feil passord."
    return cleaned


def _strip_rtf_ole_objects(data: bytes) -> bytes:
    """
    Fjern alle {\\object...} grupper fra RTF-binærdata.

    OLE1/OLE2-innebygde objekter (f.eks. Word.Picture.8 fra Word 97/2000) kan
    hindre LibreOffice i å laste RTF-filen. Dersom objektgruppen inneholder en
    {\\result ...} sub-gruppe (visuell fallback-rendering, typisk \\pict/EMF),
    beholdes innholdet i den. Alt annet — inkludert \\objdata-blokken — fjernes.

    Returnerer modifisert RTF, eller originaldata uendret ved feil.
    """
    BS       = 92    # backslash
    OB       = 123   # {
    CB       = 125   # }
    MARKER   = b'{\\object'
    RESULT_M = b'{\\result'
    n        = len(data)

    if MARKER not in data:
        return data

    def _group_end(buf: bytes, start: int) -> int:
        """Returner indeks etter avsluttende } for gruppen som starter på start."""
        depth = 0
        j = start
        while j < len(buf):
            c = buf[j]
            if c == BS and j + 1 < len(buf):
                j += 2
                continue
            if c == OB:
                depth += 1
            elif c == CB:
                depth -= 1
                if depth == 0:
                    return j + 1
            j += 1
        return len(buf)

    def _find_result_content(obj_block: bytes) -> bytes:
        """
        Finn {\\result ...} sub-gruppen i et object-block og returner
        innholdet (uten de ytre klammene). Returnerer b'' om ikke funnet.
        """
        pos = obj_block.find(RESULT_M)
        if pos < 0:
            return b""
        end = _group_end(obj_block, pos)
        inner = obj_block[pos + len(RESULT_M):end - 1]
        return inner.strip()

    out = bytearray()
    i   = 0
    try:
        while i < n:
            if data[i:i + len(MARKER)] == MARKER:
                end        = _group_end(data, i)
                obj_block  = data[i:end]
                result_content = _find_result_content(obj_block)
                if result_content:
                    out.extend(result_content)
                i = end
            else:
                out.append(data[i])
                i += 1
    except Exception:
        return data     # sikker fallback: returner original

    return bytes(out)


def _try_decode_utf8_binary(data: bytes) -> bytes | None:
    """
    Forsøk å gjenopprette binærinnhold fra UTF-8-kodet binærdata.

    Noen fagsystemer lagrer binærfiler ved å lese innholdet som Windows-1252
    og deretter skrive det som UTF-8. Resultatet er en gyldig UTF-8-fil uten
    gjenkjennbare magic-bytes i råformat — men gjenvunne bytes kan detekteres
    som kjent binærformat (OLE2, PDF, osv.).

    Kodingen er cp1252 med C1-fallback: Python-cp1252 er undefined for bytene
    0x81, 0x8D, 0x8F, 0x90, 0x9D, men Windows-1252 mapper disse til tilsvarende
    Unicode-kontrollpunkter (U+0081 osv.).  For disse brukes kodepoint-verdien
    direkte.  Tegn utenfor U+0000–U+00FF indikerer at innholdet ikke er binær.

    Returnerer gjenvunne bytes, eller None om innholdet sannsynligvis er
    ren tekst (< 10 ikke-ASCII-bytes i de første 512 bytene) eller inneholder
    tegn utenfor byte-domenet.
    """
    if sum(1 for b in data[:512] if b > 127) < 10:
        return None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None

    # Rask sti: cp1252 dekker all innholdet direkte (ingen C1-kontrolltegn)
    try:
        return text.encode("cp1252")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    # Sakte sti: Python-cp1252 mangler 0x81, 0x8D, 0x8F, 0x90, 0x9D.
    # Windows-1252 mapper disse til tilsvarende C1-kontrollpunkter (U+0081 osv.)
    # — bruk kodepoint-verdien direkte for disse.
    # Prosesser teksten i chunks mellom de problematiske tegnene for ytelse.
    _C1_UNDEFINED = frozenset("\x81\x8d\x8f\x90\x9d")
    result  = bytearray()
    start   = 0
    n       = len(text)
    while start < n:
        # Finn neste C1-kontrolltegn eller slutt
        end = start
        while end < n and text[end] not in _C1_UNDEFINED:
            end += 1
        if end > start:
            try:
                result.extend(text[start:end].encode("cp1252"))
            except (UnicodeEncodeError, UnicodeDecodeError):
                return None   # Tegn utenfor byte-domenet → ikke binær
        if end < n:
            result.append(ord(text[end]))   # C1-kontrollpunkt direkte
            end += 1
        start = end
    return bytes(result)


def _count_lines(path: Path) -> int:
    """Tell antall linjer i en fil raskt via chunk-basert newline-telling."""
    count = 0
    try:
        with open(path, "rb") as f:
            buf = memoryview(bytearray(256 * 1024))
            while True:
                n = f.readinto(buf)
                if not n:
                    break
                count += buf[:n].tobytes().count(b"\n")
    except Exception:
        pass
    return max(count, 1)


def _inject_conversion_comment(data: bytes) -> bytes:
    """
    Sett inn konverteringskommentar etter siste eksisterende kommentar
    i XML-headeren (dvs. etter '-->' på siste kommentar-linje).
    Hvis ingen kommentar finnes: sett inn etter XML-deklarasjonen.
    """
    comment = _conversion_comment()
    # Finn siste --> i headeren (dvs. avslutning av siste kommentar)
    # Søk bare i de første 4096 bytes — kommentarer er alltid i headeren
    header = data[:4096]
    last_end = header.rfind(b"-->")
    if last_end != -1:
        # Finn linjeskift etter -->
        nl = data.find(b"\n", last_end)
        if nl != -1:
            return data[:nl+1] + comment + data[nl+1:]
    # Fallback: sett inn etter XML-deklarasjonen
    pi_end = data.find(b"?>")
    if pi_end != -1:
        nl = data.find(b"\n", pi_end)
        if nl != -1:
            return data[:nl+1] + comment + data[nl+1:]
    return data


def _restore_xml_header(original: bytes, et_output: bytes) -> bytes:
    """
    ET.write() fjerner kommentarer og omskriver namespace-prefiks.
    Denne funksjonen erstatter det ET produserte opp til og med rot-elementets
    åpnings-tag med originalen — slik bevares BOM, XML-deklarasjon,
    kommentarer og namespace slik de var.
    """
    # Finn rot-element i ET-output (første '<' som ikke er '<?' eller '<!--')
    et_root_start = _find_root_tag_start(et_output)
    orig_root_start = _find_root_tag_start(original)
    if et_root_start == -1 or orig_root_start == -1:
        return et_output
    # Behold original header, lim på ET-body fra og med rot-tag
    return original[:orig_root_start] + et_output[et_root_start:]


def _find_root_tag_start(data: bytes) -> int:
    """Finn byte-offset til første ekte element-tag (ikke PI eller kommentar)."""
    i = 0
    n = len(data)
    while i < n:
        idx = data.find(b"<", i)
        if idx == -1:
            return -1
        if data[idx:idx+4] == b"<!--":
            end = data.find(b"-->", idx + 4)
            i = end + 3 if end != -1 else n
        elif data[idx:idx+2] == b"<?":
            end = data.find(b"?>", idx + 2)
            i = end + 2 if end != -1 else n
        else:
            return idx
    return -1


def _conversion_comment(version: str = "?") -> bytes:
    """Bygg konverteringskommentar som bytes."""
    import datetime, socket
    ts       = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    hostname = socket.gethostname()
    try:
        from version import VERSION
        version = VERSION
    except ImportError:
        pass
    return (f"<!--Dokumentkonvertering utfort av SIARD Manager v{version}"
            f" / {ts} / {hostname}-->\r\n").encode("utf-8")


def _patch_line_with_digest(
        line: bytes,
        ren_bytes: dict[bytes, bytes],
        ren_paths: dict[bytes, Path]) -> tuple[bytes, int]:
    """
    Erstatt filnavn, length og digest i én XML-linje.

    Finner file="gammelt_navn", erstatter med nytt navn, og oppdaterer
    length og digest-attributtene fra den faktiske nye filen på disk.
    Returnerer (ny_linje, antall_endringer).
    """
    import hashlib as _hashlib

    MARKERS = (b'file="', b"file='", b'fileName="', b"fileName='",
               b'href="',  b"href='")
    n = 0
    for marker in MARKERS:
        pos = 0
        while True:
            idx = line.find(marker, pos)
            if idx == -1:
                break
            start = idx + len(marker)
            quote = marker[-1:]
            end   = line.find(quote, start)
            if end == -1:
                break
            key = line[start:end]
            if key in ren_bytes:
                new_name = ren_bytes[key]
                new_path = ren_paths[key]
            elif b"/" in key:
                # Full-sti ref (f.eks. DBPT-format eller ekstern blob):
                # slå opp på filnavn-delen alene
                basename = key.rsplit(b"/", 1)[-1]
                if basename not in ren_bytes:
                    pos = end + 1
                    continue
                new_basename = ren_bytes[basename]
                new_path     = ren_paths[basename]
                if key.startswith(b"content/"):
                    # Intern full-sti: bevar mappe-prefiks, bytt bare filnavn
                    dir_prefix = key.rsplit(b"/", 1)[0] + b"/"
                    new_name   = dir_prefix + new_basename
                else:
                    # Ekstern ref: filen er nå intern — bruk bare filnavn
                    new_name = new_basename
            else:
                pos = end + 1
                continue
            # Erstatt filnavn
            line = line[:start] + new_name + line[end:]
            n   += 1
            pos  = start + len(new_name) + 1
            # Oppdater length og digest fra ny fil
            try:
                data   = new_path.read_bytes()
                size   = str(len(data)).encode()
                digest = _hashlib.md5(data).hexdigest().upper().encode()
                line = re.sub(rb'length="[^"]*"',  b'length="'  + size   + b'"', line)
                line = re.sub(rb"length='[^']*'",  b"length='"  + size   + b"'", line)
                line = re.sub(rb'digest="[^"]*"',  b'digest="'  + digest + b'"', line)
                line = re.sub(rb"digest='[^']*'",  b"digest='"  + digest + b"'", line)
            except Exception:
                pass   # fil ikke tilgjengelig — behold gammel verdi
    return line, n


def _unpack_single_file(path: Path) -> tuple[bytes, str] | None:
    """
    Prøv å pakke ut en komprimert fil. Returnerer (innhold, filnavn) hvis
    arkivet inneholder nøyaktig én fil, ellers None.

    Støtter: ZIP, GZ, BZ2, BZ2 med 4-byte størrelses-header, TAR.
    RAR og 7z støttes hvis biblioteker er installert.
    """
    import tarfile

    # ZIP
    try:
        with zipfile.ZipFile(path, "r") as zf:
            members = [m for m in zf.infolist() if not m.filename.endswith("/")]
            if len(members) == 1:
                data = zf.read(members[0].filename)
                return data, members[0].filename
    except Exception:
        pass

    # GZ (single-file gzip)
    try:
        import gzip
        with gzip.open(path, "rb") as gz:
            data = gz.read(64 * 1024 * 1024)
        inner_name = path.stem
        return data, inner_name
    except Exception:
        pass

    # BZ2 — standard og variant med 4-byte størrelses-header
    # Varianten: [4 bytes LE størrelse][BZh...]  brukes av bl.a. norske fagsystemer
    try:
        import bz2
        raw = path.read_bytes()
        # Prøv standard bzip2 direkte
        candidates = [raw]
        # Prøv med 4-byte header strippet (vanlig variant i norske fagsystemer)
        if len(raw) > 8 and raw[4:6] == b"BZ":
            candidates.append(raw[4:])
        for candidate in candidates:
            try:
                data = bz2.decompress(candidate)
                if len(data) > 0:
                    inner_name = path.stem
                    return data, inner_name
            except Exception:
                continue
    except Exception:
        pass

    # TAR / TAR.GZ / TAR.BZ2
    try:
        with tarfile.open(path, "r:*") as tf:
            members = [m for m in tf.getmembers() if m.isfile()]
            if len(members) == 1:
                f = tf.extractfile(members[0])
                if f:
                    data = f.read()
                    return data, members[0].name
    except Exception:
        pass

    # RAR — krever rarfile-bibliotek
    try:
        import rarfile
        with rarfile.RarFile(str(path)) as rf:
            members = [m for m in rf.infolist() if not m.is_dir()]
            if len(members) == 1:
                data = rf.read(members[0].filename)
                return data, members[0].filename
    except Exception:
        pass

    # 7z — krever py7zr-bibliotek
    try:
        import py7zr
        with py7zr.SevenZipFile(str(path), mode="r") as sz:
            all_files = sz.list()
            files = [f for f in all_files if not f.is_directory]
            if len(files) == 1:
                data_dict = sz.read([files[0].filename])
                buf = data_dict.get(files[0].filename)
                if buf:
                    return buf.read(), files[0].filename
    except Exception:
        pass

    return None


def _try_decode_base64(data: bytes) -> bytes | None:
    """
    Prøv å dekode data som base64. Returnerer dekodede bytes eller None.

    Håndterer:
    - Standard base64 (RFC 4648)
    - URL-safe base64 (- og _ i stedet for + og /)
    - MIME-format med linjeskift hver 64 eller 76 tegn
    - Manglende padding (legges til automatisk)

    Krever at ≥99 % av tegn (ekskl. whitespace) er gyldige base64-tegn,
    og at dekodede data er ≥4 bytes. For korte sekvenser (<48 tegn) hoppes over
    for å unngå falske positiver på korte tekster som tilfeldigvis er base64-like.
    """
    # Strip linjeskift (gyldige i MIME base64), behold resten for ratio-sjekk
    clean = data.strip().replace(b'\r\n', b'').replace(b'\n', b'').replace(b'\r', b'')

    if len(clean) < 48:
        return None

    # Sjekk tegn-ratio — standard og URL-safe
    invalid_std = len(re.sub(rb'[A-Za-z0-9+/= ]', b'', clean))
    invalid_url = len(re.sub(rb'[A-Za-z0-9\-_= ]', b'', clean))
    best_invalid = min(invalid_std, invalid_url)
    if best_invalid / len(clean) > 0.01:   # mer enn 1 % ugyldige tegn
        return None

    # Fjern mellomrom og legg til manglende padding
    clean = clean.replace(b' ', b'')
    rem = len(clean) % 4
    if rem:
        clean += b'=' * (4 - rem)

    for decode_fn in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = decode_fn(clean)
            if len(decoded) >= 4:
                return decoded
        except Exception:
            pass

    return None


def _replace_file_attrs_in_line(
        line: bytes,
        ren_bytes: dict[bytes, bytes],
        markers: tuple) -> tuple[bytes, int]:
    """
    Ekstraher filnavn fra attributt i XML-linje og gjør O(1) dict-oppslag.

    Returnerer (ny_linje, antall_erstatninger).
    Håndterer begge anførselstegn-varianter og flere attributter per linje.
    """
    updates = 0
    for marker in markers:
        pos = 0
        while True:
            idx = line.find(marker, pos)
            if idx == -1:
                break
            start = idx + len(marker)
            quote = marker[-1:]   # b'"' eller b"'"
            end   = line.find(quote, start)
            if end == -1:
                break
            key = line[start:end]
            if key in ren_bytes:
                new_val = ren_bytes[key]
                line    = line[:start] + new_val + line[end:]
                updates += 1
                pos = start + len(new_val) + 1
            else:
                pos = end + 1
    return line, updates


def _build_lob_type_col_map(xml_path: Path,
                             table_blob_list: list[str]
                             ) -> dict[str, list[str]]:
    """
    Skann tableX.xml og finn koblingen {lob_kolonne: [type_kolonner]}.

    For hver rad: hvis c3 har file= og c9 inneholder kjent type-streng,
    registrerer vi at c3 og c9 er koblet. Returnerer f.eks. {"c3": ["c9"]}.

    Dette er applikasjonsspesifikt og kan ikke leses fra SIARD-metadata —
    vi utleder det fra faktisk rad-innhold.
    """
    _KNOWN_TYPES = {
        'RTF','DOC','DOCX','PDF','XLS','XLSX','PPT','PPTX',
        'TXT','HTML','HTM','XML','ODT','ODS','ODP',
        'MSG','EML','TIFF','TIF','JPG','JPEG','PNG','BMP','GIF','ZIP','CSV',
        'WPD','WRI','LWP',
    }
    blob_basenames: set[str] = {PurePosixPath(p).name for p in table_blob_list}
    lob_to_types:  dict[str, set[str]] = {}

    try:
        row_buf: list[bytes] = []
        in_row = False
        n_sampled = 0
        with open(xml_path, "rb") as f:
            for line in f:
                if b"<row>" in line or b"<row " in line:
                    in_row  = True
                    row_buf = [line]
                elif in_row:
                    row_buf.append(line)
                    if b"</row>" in line:
                        in_row = False
                        n_sampled += 1
                        if n_sampled > 2000:
                            break   # nok for å etablere mønsteret
                        row_bytes = b"".join(row_buf)

                        file_cols = re.findall(
                            rb'<(c\d+) [^>]*file="([^"]+)"', row_bytes)
                        if not file_cols:
                            continue

                        type_cols: dict[str, str] = {}
                        for m in re.finditer(
                                rb'<(c\d+)>([A-Za-z]{2,8})</\1>', row_bytes):
                            val = m.group(2).decode("utf-8", errors="ignore").upper()
                            if val in _KNOWN_TYPES:
                                type_cols[m.group(1).decode()] = val

                        for fc_b, fname_b in file_cols:
                            fc    = fc_b.decode()
                            fname = fname_b.decode()
                            if PurePosixPath(fname).name in blob_basenames:
                                if fc not in lob_to_types:
                                    lob_to_types[fc] = set()
                                lob_to_types[fc].update(type_cols.keys())
    except Exception:
        pass

    return {k: sorted(v) for k, v in lob_to_types.items()}


def _build_type_hints_from_xml(xml_path: Path,
                               table_blob_list: list[str]) -> dict[str, str]:
    """
    Skann tableX.xml linje for linje og bygg {filnavn: ext}-mapping
    basert på type-kolonner i radene.

    Strategi: for hver rad som har en LOB-kolonne med file="recN.txt",
    søk i andre kolonner etter kjente type-strenger (RTF, DOC, PDF etc.).
    Typen brukes som hint i _detect_one for å overstyre bytes-deteksjon
    når filinnholdet ikke har tydelig magic header.

    Returnerer kun hints for filer som faktisk er i table_blob_list.
    """
    # Kjente type-strenger -> ext
    _TYPE_MAP: dict[str, str] = {
        "RTF": "rtf", "DOC": "doc", "DOCX": "docx",
        "PDF": "pdf", "XLS": "xls", "XLSX": "xlsx",
        "PPT": "ppt", "PPTX": "pptx", "TXT": "txt",
        "HTML": "html", "HTM": "html", "XML": "xml",
        "ODT": "odt", "ODS": "ods", "ODP": "odp",
        "MSG": "msg", "EML": "eml",
        "TIFF": "tiff", "TIF": "tiff",
        "JPG": "jpg", "JPEG": "jpg",
        "PNG": "png", "BMP": "bmp", "GIF": "gif",
        "ZIP": "zip", "CSV": "csv",
    }

    # Filnavn vi leter etter (bare basenavn, uten mappe-prefix)
    blob_basenames: set[str] = {
        PurePosixPath(p).name for p in table_blob_list
    }

    result: dict[str, str] = {}

    # Chunk-basert linje-for-linje lesing — håndterer store XML-filer
    # En rad strekker seg over flere linjer, så vi samler til </row>
    try:
        row_buf: list[bytes] = []
        in_row = False
        with open(xml_path, "rb") as f:
            for line in f:
                if b"<row>" in line or b"<row " in line:
                    in_row = True
                    row_buf = [line]
                elif in_row:
                    row_buf.append(line)
                    if b"</row>" in line:
                        in_row = False
                        row_bytes = b"".join(row_buf)

                        # Finn file="..." i raden
                        file_matches = re.findall(
                            rb'file="([^"]+)"', row_bytes)
                        if not file_matches:
                            continue

                        # Finn type-verdier: alle tekst-noder som er kjente typer
                        text_vals = re.findall(
                            rb'<c\d+>([A-Za-z]{2,8})</c\d+>', row_bytes)
                        row_type: str = ""
                        for tv in text_vals:
                            tv_up = tv.decode("utf-8", errors="ignore").strip().upper()
                            if tv_up in _TYPE_MAP:
                                row_type = _TYPE_MAP[tv_up]
                                break

                        if not row_type:
                            continue

                        # Knytt type til alle LOB-filer i raden
                        for fname_b in file_matches:
                            fname = fname_b.decode("utf-8", errors="ignore")
                            basename = PurePosixPath(fname).name
                            if basename in blob_basenames:
                                result[basename] = row_type

    except Exception:
        pass

    return result


def _unescape_siard(text: str) -> bytes:
    """
    Dekod SIARD unicode-escaped tekst til bytes.
    SIARD bruker \\uXXXX for å escape spesialtegn i tekst-noder,
    f.eks. \\u005c = \\ (backslash). RTF-innhold lagres slik.
    """
    import re as _re
    unescaped = _re.sub(
        r'\\u([0-9a-fA-F]{4})',
        lambda m: chr(int(m.group(1), 16)),
        text
    )
    try:
        return unescaped.encode("latin-1")
    except (UnicodeEncodeError, ValueError):
        return unescaped.encode("utf-8", errors="replace")


def _read_col_metadata(metadata_path: Path) -> dict[str, dict]:
    """
    Les metadata.xml og bygg full kolonne-metadata per tabell.

    Returnerer:
    {
      "schema0/table45": {
        "lob_cols":  {6: "schema0/table45/lob6"},   # col_idx -> lob_folder
        "mime_cols": {6: [9]},                        # lob_col_idx -> [mime_col_idx]
        "digest_cols": {6: {"digestType": 8, "digest": 9}},  # hvis digest finnes
        "col_names": {1: "DOK_LOEPENR", 6: "DOK_DOKUMENT", 9: "DOK_MIMETYPE", ...}
      }
    }

    Mime-type-kolonner identifiseres ved:
      - Kolonnenavn inneholder "mime", "mimetype", "contenttype", "mediatype", "filetype", "format"
      - Kolonnetypen er VARCHAR/CHAR/NVARCHAR (tekst, ikke LOB)
      - Kolonnen er i samme tabell som en LOB-kolonne

    digest-kolonner identifiseres ved:
      - Kolonnenavn inneholder "digest", "md5", "sha", "checksum", "hash"
    """
    _MIME_KEYWORDS  = ("mime", "contenttype", "mediatype", "filetype", "format",
                       "doktype", "filformat", "contentformat")
    _DIGEST_KW      = ("digest", "md5", "sha1", "sha256", "checksum", "hash")
    _DIGESTTYPE_KW  = ("digesttype", "hashtype", "checksumtype")

    result: dict[str, dict] = {}

    if not metadata_path.exists():
        return result
    try:
        tree = ET.parse(metadata_path)
        root = tree.getroot()

        for schema in root.iter():
            if _local(schema.tag).lower() != "schema":
                continue
            schema_name = next(
                ((ch.text or "").strip() for ch in schema
                 if _local(ch.tag).lower() == "name"), "")

            for table in schema.iter():
                if _local(table.tag).lower() != "table":
                    continue
                table_name = next(
                    ((ch.text or "").strip() for ch in table
                     if _local(ch.tag).lower() == "name"), "")
                table_key = f"{schema_name}/{table_name}"

                # Samle alle kolonner med index, navn, type, lobFolder
                cols: list[dict] = []
                col_idx = 0
                for col in table.iter():
                    if _local(col.tag).lower() != "column":
                        continue
                    col_idx += 1
                    info: dict = {"idx": col_idx, "name": "", "type": "", "lob_folder": ""}
                    for ch in col:
                        tag = _local(ch.tag).lower()
                        if tag == "name":
                            info["name"] = (ch.text or "").strip()
                        elif tag == "type":
                            info["type"] = (ch.text or "").strip().upper()
                        elif tag == "lobfolder":
                            info["lob_folder"] = (ch.text or "").strip()
                    cols.append(info)

                if not cols:
                    continue

                # Finn LOB-kolonner
                lob_cols: dict[int, str] = {}
                for c in cols:
                    if c["type"] in ("NCLOB","CLOB","NBLOB","BLOB") and c["lob_folder"]:
                        lob_cols[c["idx"]] = c["lob_folder"]

                if not lob_cols:
                    continue

                # Finn mime-type-kolonner (VARCHAR/CHAR med navn som inneholder mime-keywords)
                mime_col_idxs: list[int] = []
                digest_col_idxs: list[int] = []
                digesttype_col_idxs: list[int] = []
                for c in cols:
                    name_lower = c["name"].lower()
                    type_upper = c["type"]
                    is_text = any(t in type_upper for t in ("VARCHAR","CHAR","NCHAR","TEXT","CLOB","NCLOB","STRING"))
                    if is_text and any(kw in name_lower for kw in _MIME_KEYWORDS):
                        mime_col_idxs.append(c["idx"])
                    if any(kw in name_lower for kw in _DIGEST_KW):
                        digest_col_idxs.append(c["idx"])
                    if any(kw in name_lower for kw in _DIGESTTYPE_KW):
                        digesttype_col_idxs.append(c["idx"])

                result[table_key] = {
                    "lob_cols":         lob_cols,
                    "mime_cols":        mime_col_idxs,
                    "digest_cols":      digest_col_idxs,
                    "digesttype_cols":  digesttype_col_idxs,
                    "col_names":        {c["idx"]: c["name"] for c in cols},
                }

    except Exception:
        pass
    return result


def _read_lob_columns(metadata_path: Path) -> dict[str, dict[int, str]]:
    """
    Bakoverkompatibelt wrapper — returnerer {table_key: {col_idx: lob_folder}}.
    Brukes av _extract_inline.
    """
    meta = _read_col_metadata(metadata_path)
    return {k: v["lob_cols"] for k, v in meta.items()}


def _is_hex(s: str) -> bool:
    """Returner True hvis s er en hex-streng (med ev. intern whitespace)."""
    # Fjern all whitespace inkl. linjeskift som noen systemer bruker for chunking
    s = re.sub(r"\s+", "", s)
    return len(s) >= 8 and len(s) % 2 == 0 and bool(re.fullmatch(r"[0-9a-fA-F]+", s))


def _hex_decode(s: str) -> bytes:
    """Dekod hex-streng med intern whitespace."""
    return bytes.fromhex(re.sub(r"\s+", "", s))


def _checksum(data: bytes, algo: str) -> str:
    a = algo.lower().replace("-", "")
    mapping = {"md5": hashlib.md5, "sha1": hashlib.sha1,
               "sha256": hashlib.sha256, "sha512": hashlib.sha512,
               "digest": hashlib.sha256, "checksum": hashlib.md5,
               "hash": hashlib.sha256, "messagedigest": hashlib.sha256}
    fn = mapping.get(a)
    if fn is None:
        try:
            h = hashlib.new(a)
        except ValueError:
            return ""
    else:
        h = fn()
    h.update(data)
    return h.hexdigest()


def _local(tag: str) -> str:
    return _NS_RE.sub("", tag)


def _ns_prefix(tag: str) -> str:
    m = _NS_RE.match(tag)
    return "{" + m.group(1) + "}" if m else ""


def _find_libreoffice(hint: str = "soffice") -> str | None:
    import sys
    if shutil.which(hint):
        return hint
    if sys.platform == "win32":
        candidates = []
        for base in (os.environ.get("PROGRAMFILES", r"C:\Program Files"),
                     os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"),
                     os.environ.get("LOCALAPPDATA", "")):
            if base:
                for sub in ("LibreOffice", "LibreOffice 7", "LibreOffice 24", "OpenOffice"):
                    candidates.append(os.path.join(base, sub, "program", "soffice.exe"))
        for base in (r"C:\Program Files", r"C:\Program Files (x86)"):
            if os.path.isdir(base):
                try:
                    for entry in os.listdir(base):
                        if "libre" in entry.lower() or "openoffice" in entry.lower():
                            candidates.append(os.path.join(base, entry, "program", "soffice.exe"))
                except OSError:
                    pass
        for c in candidates:
            if os.path.isfile(c):
                return c
    if sys.platform == "darwin":
        for p in ("/Applications/LibreOffice.app/Contents/MacOS/soffice",
                  "/Applications/OpenOffice.app/Contents/MacOS/soffice"):
            if os.path.isfile(p):
                return p
    for name in ("soffice", "libreoffice", "libreoffice7", "libreoffice24"):
        found = shutil.which(name)
        if found:
            return found
    return None


def suggest_lo_defaults() -> dict:
    """
    Foreslå optimale workers og batch-størrelse basert på maskinvare.

    Workers:
    - LibreOffice bruker ca 300-500 MB RAM per instans ved konvertering
    - Sett maks 60 % av tilgjengelig RAM til LO-instanser
    - Sett maks 75 % av CPU-kjerner (resten trenger OS og GUI)
    - Ta minimum av de to, men aldri mer enn 8 (diminishing returns)
    - Minimum 1

    Batch-størrelse:
    - Større batch → færre LO-oppstarter → bedre ytelse
    - Men: feilede filer sendes til retry, så stor batch er OK
    - Skaler fra 25 (lite RAM) til 100 (mye RAM)
    """
    import os
    cpus   = os.cpu_count() or 4
    ram_gb = 8.0  # trygt fallback

    try:
        import psutil
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    except Exception:
        # Fallback: Windows native via ctypes, deretter /proc/meminfo på Linux
        try:
            import ctypes
            if sys.platform == "win32":
                class _MEMSTATUS(ctypes.Structure):
                    _fields_ = [
                        ("dwLength",                ctypes.c_ulong),
                        ("dwMemoryLoad",             ctypes.c_ulong),
                        ("ullTotalPhys",             ctypes.c_ulonglong),
                        ("ullAvailPhys",             ctypes.c_ulonglong),
                        ("ullTotalPageFile",         ctypes.c_ulonglong),
                        ("ullAvailPageFile",         ctypes.c_ulonglong),
                        ("ullTotalVirtual",          ctypes.c_ulonglong),
                        ("ullAvailVirtual",          ctypes.c_ulonglong),
                        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
                    ]
                stat = _MEMSTATUS()
                stat.dwLength = ctypes.sizeof(stat)
                ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
                ram_gb = stat.ullTotalPhys / (1024 ** 3)
            else:
                with open("/proc/meminfo") as _f:
                    for _line in _f:
                        if _line.startswith("MemTotal:"):
                            ram_gb = int(_line.split()[1]) / (1024 ** 2)
                            break
        except Exception:
            pass

    lo_ram_mb          = 400           # konservativt estimat per LO-instans
    ram_budget_mb      = ram_gb * 1024 * 0.60
    workers_by_ram     = max(1, int(ram_budget_mb / lo_ram_mb))
    workers_by_cpu     = max(1, int(cpus * 0.75))
    workers            = min(workers_by_ram, workers_by_cpu, 8)

    if ram_gb >= 32:
        batch = 100
    elif ram_gb >= 16:
        batch = 75
    elif ram_gb >= 8:
        batch = 50
    else:
        batch = 25

    return {
        "max_workers":   workers,
        "lo_batch_size": batch,
        "_cpus":         cpus,
        "_ram_gb":       round(ram_gb, 1),
    }


# ── Resume-hjelper ────────────────────────────────────────────────────────────

_RESUME_SUFFIX = "_blob_resume.json"


def _resume_json_path(log_dir, siard_stem: str) -> Path:
    return Path(log_dir) / f"{siard_stem}{_RESUME_SUFFIX}"


def _load_resume_done_set(resume_json: Path) -> set[str]:
    """
    Les ..._blob_resume.json og returner settet av zip_sti-er som er
    ferdig behandlet i den avbrutte kjøringen.  Returnerer tomt sett
    ved enhver feil (fil mangler, CSV mangler, parse-feil).
    """
    try:
        data     = json.loads(resume_json.read_text(encoding="utf-8"))
        csv_path = Path(data.get("csv_path", ""))
        if not csv_path.exists():
            return set()
        done: set[str] = set()
        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)          # hopp over overskriftsrad
            for row in reader:
                if row:
                    done.add(row[0])    # fra_fil = full zip_sti
        return done
    except Exception:
        return set()


def _collect_external_blobs(
        table_xml_map: dict[str, str],
        extract_dir: Path,
        siard_dir: Path,
        all_blobs: list[str],
        w) -> list[str]:
    """
    Skann alle tableX.xml-filer for file="..."-referanser som IKKE ble funnet
    av filsystemskanningen.  Håndterer to tilfeller:

    1. Intern full-sti ref (DBPT-format): filen ligger i extract_dir men ble
       ikke plukket opp av skannen (f.eks. uvanlig mappestruktur).  Legges
       direkte til listen.

    2. Ekstern ref: filen ligger utenfor SIARD-arkivet.  Løses opp relativt
       til siard_dir, kopieres inn i extract_dir under tableX sin mappe og
       legges til listen.

    Returnerer liste over nye zip_sti-er som ble lagt til.
    """
    _known_basenames = {PurePosixPath(p).name for p in all_blobs}
    _known_paths     = set(all_blobs)
    extra: list[str] = []

    for table_key, xml_sti in table_xml_map.items():
        xml_file = extract_dir / xml_sti
        if not xml_file.exists():
            continue
        try:
            xml_bytes = xml_file.read_bytes()
        except Exception:
            continue

        refs = re.findall(rb'file=["\']([^"\']+)["\']', xml_bytes)
        for ref_b in refs:
            ref = ref_b.decode("utf-8", errors="replace").replace("\\", "/")
            basename = PurePosixPath(ref).name

            # Hopp over om allerede funnet (enten full sti eller bare filnavn)
            if ref in _known_paths or basename in _known_basenames:
                continue

            # Case 1: intern full-sti — sjekk om filen faktisk finnes
            candidate_internal = extract_dir / ref
            if candidate_internal.exists() and candidate_internal.is_file():
                extra.append(ref)
                _known_paths.add(ref)
                _known_basenames.add(basename)
                w(f"  Intern full-sti blob lagt til: {ref}", "info")
                continue

            # Case 2: ekstern blob — prøv relativt til SIARD-mappa
            if not siard_dir or not siard_dir.is_dir():
                continue
            candidate_external = (siard_dir / ref).resolve()
            # Sikkerhet: ikke la ../ navigere utenfor forventet område
            try:
                candidate_external.relative_to(siard_dir.resolve().parent)
            except ValueError:
                w(f"  Ekstern blob utenfor tillatt sti, ignorert: {ref}", "warn")
                continue

            if not candidate_external.exists() or not candidate_external.is_file():
                w(f"  Ekstern blob ikke funnet: {ref}", "warn")
                continue

            # Kopier inn i extract_dir under tableX sin innholdsmappe
            schema, table = table_key.split("/", 1)
            dest_rel = f"content/{schema}/{table}/ext_lob/{basename}"
            dest     = extract_dir / dest_rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(str(candidate_external), str(dest))
            except Exception as exc:
                w(f"  Kunne ikke kopiere ekstern blob {ref}: {exc}", "warn")
                continue

            extra.append(dest_rel)
            _known_paths.add(dest_rel)
            _known_basenames.add(basename)
            w(f"  Ekstern blob kopiert inn: {ref} → {dest_rel}", "info")

    return extra


# ── Hoved-operasjon ───────────────────────────────────────────────────────────

class BlobConvertOperation(BaseOperation):
    """
    Konverterer .bin/.txt-filer og inline NBLOB/NCLOB i SIARD til PDF/A.

    Ytelsesarkitektur:
    - extractall() til tmpdir for rask sekvensiell utpakking
    - Deteksjon parallelt med ThreadPoolExecutor
    - LibreOffice i batch-modus: én LO-oppstart konverterer N filer
    - Ny SIARD pakkes fra filsystem
    """
    operation_id    = "blob_convert"
    label           = "BLOB Konverter til PDF/A"
    description     = (
        "Konverterer .bin/.txt-filer og inline NBLOB/NCLOB til riktige filtyper "
        "og PDF/A. Bruker batch-konvertering for høy ytelse."
    )
    category        = "Innhold"
    status          = 2
    produces_siard  = True
    requires_unpack = True
    default_params = {
        "output_suffix":        "_konvertert",
        "libreoffice_bin":      "soffice",
        "lo_timeout":           300,
        "skip_existing_pdf":    True,
        "dry_run":              False,
        "temp_dir":             "",
        "pdfa_version":         _PDFA_DEFAULT,
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        # Les parallellitet og batch fra globale innstillinger (config.json)
        from settings import get_config as _get_cfg
        self.params["max_workers"]  = max(1, int(_get_cfg("max_workers",  4)  or 4))
        self.params["lo_batch_size"] = max(1, int(_get_cfg("lo_batch_size", 50) or 50))

        # Normaliser øvrige int-parametere
        for _key in ("lo_timeout",):
            try:
                self.params[_key] = int(self.params[_key])
            except (ValueError, TypeError):
                self.params[_key] = self.default_params[_key]

        # Meldingsnivåer som vises i GUI-logg
        _GUI_LEVELS = {"step", "ok", "warn", "feil", "error"}
        # Prefiks som aldri sendes til GUI (for høy volum)
        _GUI_SKIP_PREFIX = (
            "    Bisect ",    # bisect-intern splitt-info
            "    Base64",     # per-fil deteksjon
            "    Pakket ut",  # per-fil deteksjon
            "    Enkeltfil forsøk",  # vises separat som warn
        )

        def w(msg, lvl="info"):
            if log:
                log.log(msg, lvl)
            # Send til GUI hvis nivå er relevant og ikke for detaljert
            if lvl in _GUI_LEVELS or lvl == "info":
                if not any(msg.startswith(p) for p in _GUI_SKIP_PREFIX):
                    progress("log", msg=msg, level=lvl)

        def progress(event, **kw):
            if pcb:
                pcb(event, **kw)

        # Hent pause/stopp Events (trådsikre)
        stop_ev  = ctx.metadata.get("stop_event",  threading.Event())
        pause_ev = ctx.metadata.get("pause_event", threading.Event())

        # CSV-detaljlogg og feilogg per kjøring
        log_dir  = ctx.metadata.get("log_dir")   # satt av app.py via ctx
        csv_log  = None
        err_log  = None
        if log_dir:
            siard_name = ctx.siard_path.stem if ctx.siard_path else "blob"
            csv_log = BlobCsvLogger(log_dir, siard_name)
            csv_log.__enter__()
            w(f"  CSV-logg: {csv_log.log_path}", "info")
            err_log = ConversionErrorLogger(log_dir, siard_name)
            err_log.__enter__()

        w("=" * 56)
        w("  BLOB-KONVERTERING", "step")
        w("=" * 56)

        # ── Resume-sjekkpunkt ─────────────────────────────────────────────────
        # Finn evt. uferdig kjøring fra forrige gang.  Finnes resume-filen betyr
        # det at forrige kjøring ble avbrutt.  Ferdig-settet lastes inn og sendes
        # via ctx.metadata slik at _process_pipeline / _process kan filtrere
        # all_blobs.  Finnes den ikke, opprettes den nå (ny kjøring).
        _resume_json = None
        _done_set: set[str] = set()
        if log_dir:
            _resume_json = _resume_json_path(log_dir, ctx.siard_path.stem if ctx.siard_path else "blob")
            if _resume_json.exists():
                _done_set = _load_resume_done_set(_resume_json)
                if _done_set:
                    w(f"  Resume: finner {_resume_json.name} — "
                      f"{len(_done_set):,} filer allerede behandlet vil hoppes over", "info")
            else:
                try:
                    _resume_json.write_text(json.dumps({
                        "csv_path": str(csv_log.log_path) if csv_log and csv_log.log_path else "",
                        "started":  datetime.datetime.now().isoformat(timespec="seconds"),
                    }, indent=2), encoding="utf-8")
                except Exception:
                    _resume_json = None   # lagring feilet — fortsett uten
        ctx.metadata["_blob_resume_done"] = _done_set

        # Pipeline-modus: ikke produser ny SIARD — RepackSiard tar seg av det
        pipeline_mode = bool(
            getattr(ctx, "extracted_path", None)
            and ctx.extracted_path.is_dir())
        if pipeline_mode:
            w("  Pipeline-modus: ingen ny SIARD-fil — repakking via 'Pakk sammen SIARD'",
              "info")
            self.produces_siard = False
        else:
            self.produces_siard = True

        lo_bin = _find_libreoffice(self.params["libreoffice_bin"])
        if not lo_bin:
            msg = "LibreOffice ikke funnet — installer fra https://www.libreoffice.org"
            w(f"  FEIL: {msg}", "feil")
            return self._fail(msg)
        w(f"  LibreOffice : {lo_bin}", "info")
        w(f"  Tråder      : {self.params['max_workers']}", "info")
        w(f"  Batch-størr : {self.params['lo_batch_size']} filer/batch", "info")

        src_path = ctx.siard_path
        dst_path = src_path.with_name(
            src_path.stem + self.params["output_suffix"] + src_path.suffix)

        # Alternativt lagringssted satt av GUI preflight (lite plass ved kilde-fil)
        output_dir_override = ctx.metadata.get("output_dir_override", "").strip() \
            if hasattr(ctx, "metadata") else ""
        if output_dir_override:
            dst_path = Path(output_dir_override) / dst_path.name
            w(f"  Output-mappe: {dst_path.parent}  (alternativt lagringssted)", "info")

        dst_path = self._resolve_dst_path(dst_path, w)

        # Velg temp-mappe: global (fra ctx), ellers finn beste disk automatisk
        td = ctx.metadata.get("temp_dir", "").strip() if hasattr(ctx, "metadata") else ""
        if not td:
            td = self.params.get("temp_dir", "").strip()  # bakoverkompatibilitet
        if td:
            temp_root = Path(td)
            if not temp_root.exists():
                try:
                    temp_root.mkdir(parents=True, exist_ok=True)
                except Exception as exc:
                    w(f"  Advarsel: kan ikke bruke temp_dir={temp_root}, "
                      f"faller tilbake til automatisk valg: {exc}", "warn")
                    temp_root = None
        else:
            temp_root = None

        if temp_root is None:
            try:
                from disk_selector import best_temp_disk, get_disk_candidates, format_bytes
                candidates = get_disk_candidates()
                temp_root  = best_temp_disk(siard_path=src_path)
                w(f"  Temp-disk (auto): {temp_root}", "info")
                if candidates:
                    for c in candidates:
                        w(f"    {'→' if c['path'] == temp_root else ' '} "
                          f"{c['label']}", "info")
            except Exception:
                temp_root = src_path.parent
                w(f"  Temp-disk (fallback SIARD-mappe): {temp_root}", "info")

        w(f"  Temp-mappe  : {temp_root}", "info")

        stats = {
            "detected": 0, "converted": 0, "kept": 0,
            "failed": 0, "xml_updated": 0, "inline_extracted": 0,
        }

        try:
            self._process(ctx, src_path, dst_path, stats, w, progress,
                          lo_bin, stop_ev, pause_ev, csv_log, temp_root,
                          err_log=err_log)
        except Exception as exc:
            import traceback
            w(f"  Uventet feil: {exc}\n{traceback.format_exc()}", "feil")
            progress("finish", stats=stats)
            if csv_log:
                csv_log.__exit__(None, None, None)
            if err_log:
                err_log.__exit__(None, None, None)
            return self._fail(str(exc))
        finally:
            if csv_log:
                try:
                    csv_log.__exit__(None, None, None)
                except Exception:
                    pass
            if err_log:
                try:
                    err_log.__exit__(None, None, None)
                    if err_log.log_path and err_log.count > 0:
                        w(f"  Feilogg ({err_log.count} feil): "
                          f"{err_log.log_path}", "warn")
                except Exception:
                    pass

        w("  OPPSUMMERING:", "step")
        STAT_LABELS = {
            "detected":         "Detektert",
            "converted":        "Konvertert til PDF/A",
            "kept":             "Beholdt originalformat",
            "failed":           "Konvertering feilet",
            "xml_updated":      "XML-filer oppdatert",
            "inline_extracted": "Inline NBLOB/NCLOB",
        }
        for k, v in stats.items():
            label = STAT_LABELS.get(k, k)
            lvl = "ok" if v and k == "converted" else ("warn" if k == "failed" and v else "info")
            w(f"    {label:<28} {v}", lvl)
        if not self.params["dry_run"]:
            w(f"    Ny SIARD: {dst_path}", "ok")
        w("=" * 56)

        progress("finish", stats=stats)

        # Slett resume-sjekkpunkt — kjøringen fullførte vellykket
        if _resume_json and _resume_json.exists():
            try:
                _resume_json.unlink()
            except Exception:
                pass

        return self._ok(
            data={**stats, "output_path": str(dst_path)},
            message=(f"{stats['converted']} konvertert, {stats['kept']} beholdt, "
                     f"{stats['inline_extracted']} inline, {stats['failed']} feil"))

    # ── Pipeline-prosessering (utpakket filsystem) ────────────────────────────

    def _process_pipeline(self, ctx, extract_dir: Path,
                          stats: dict, w, progress,
                          lo_bin: str,
                          stop_ev: threading.Event,
                          pause_ev: threading.Event,
                          csv_log=None,
                          err_log=None) -> None:
        """
        Pipeline-modus: kjem rett inn i prosessering-fasene uten å pakke ut
        eller pakke inn ZIP. extract_dir er allerede utpakket av
        UnpackSiardOperation. Repacking gjøres av RepackSiardOperation.
        """
        PHASES = 4   # scan, inline, convert, patch XML  (ingen utpakking/repakking)

        def phase(n, label):
            progress("phase", phase=n, total_phases=PHASES, label=label)

        schema_re = re.compile(r"^schema\d+$", re.IGNORECASE)
        table_re  = re.compile(r"^table\d+$",  re.IGNORECASE)

        # ── Fase 1: Skann filsystemet ─────────────────────────────────────────
        phase(1, "Skanner utpakket filsystem")

        # Versjondeteksjon
        metadata_xml = extract_dir / "header" / "metadata.xml"
        src_version  = "2.1"
        if metadata_xml.exists():
            try:
                src_version = detect_siard_version(metadata_xml.read_bytes())
            except Exception:
                pass
        target_version = get_target_siard_version()
        w(f"  Kilde SIARD: {src_version}  →  Mål SIARD: {target_version}", "info")

        table_blobs:   dict = defaultdict(list)
        table_xml_map: dict = {}

        content_dir = extract_dir / "content"
        if content_dir.exists():
            all_paths = list(content_dir.rglob("*"))
            n_total   = len(all_paths)
            for scan_i, fp in enumerate(all_paths, 1):
                if not fp.is_file():
                    continue
                rel_parts = fp.relative_to(extract_dir).parts
                if (len(rel_parts) < 4
                        or rel_parts[0].lower() != "content"
                        or not schema_re.match(rel_parts[1])
                        or not table_re.match(rel_parts[2])):
                    continue
                table_key  = f"{rel_parts[1]}/{rel_parts[2]}"
                fname_lower = fp.name.lower()
                arc_name   = str(fp.relative_to(extract_dir)).replace("\\", "/")
                if len(rel_parts) == 4 and fname_lower.endswith(".xml"):
                    table_xml_map[table_key] = arc_name
                elif not fname_lower.endswith(".xml") and not fname_lower.endswith(".xsd"):
                    table_blobs[table_key].append(arc_name)
                if scan_i % max(1, n_total // 20) == 0 or scan_i == n_total:
                    progress("phase_progress", done=scan_i, total=n_total)

        all_blobs = [p for paths in table_blobs.values() for p in paths]

        # Supplerer med blob-er referert i tableX.xml men ikke funnet av skannen
        # (intern full-sti / DBPT-format, eller eksternt lagrede blobs)
        _xml_extra = _collect_external_blobs(
            table_xml_map, extract_dir, ctx.siard_path.parent, all_blobs, w)
        if _xml_extra:
            all_blobs.extend(_xml_extra)

        ext_counts: dict = {}
        for p in all_blobs:
            e = PurePosixPath(p).suffix.lstrip(".").lower() or "ingen"
            ext_counts[e] = ext_counts.get(e, 0) + 1
        ext_summary = ", ".join(
            f"{n}×.{e}" for e, n in sorted(ext_counts.items(), key=lambda x: -x[1]))
        w(f"  Fant: {len(all_blobs):,} blob-filer ({ext_summary}), "
          f"{len(table_xml_map)} tableX.xml", "info")
        progress("phase_done")

        # ── Fase 2: Inline NBLOB/NCLOB ───────────────────────────────────────
        inline_new: dict = {}
        xml_pre:    dict = {}
        col_meta:   dict = {}
        lob_type_map: dict = {}

        if self.params.get("extract_inline"):
            phase(2, "Ekstraherer inline NBLOB/NCLOB")
            col_meta = _read_col_metadata(metadata_xml) if metadata_xml.exists() else {}
            lob_cols = {k: v["lob_cols"] for k, v in col_meta.items()}

            for table_key, xml_sti in table_xml_map.items():
                xml_file = extract_dir / xml_sti
                if not xml_file.exists():
                    continue
                try:
                    xml_bytes = xml_file.read_bytes()
                except Exception as exc:
                    w(f"  FEIL les {xml_sti}: {exc}", "feil")
                    continue
                patched, new_files, n = self._extract_inline(
                    xml_bytes, xml_sti, table_key, stats, w, lob_cols=lob_cols)
                if n > 0:
                    xml_pre[xml_sti]  = patched
                    inline_new.update(new_files)
                    for lob_sti, lob_data in new_files.items():
                        lob_path = extract_dir / lob_sti
                        lob_path.parent.mkdir(parents=True, exist_ok=True)
                        lob_path.write_bytes(lob_data)
                    w(f"  {table_key}: {n} inline ekstrahert", "ok")

            # Oppdater all_blobs etter inline-ekstraksjon
            if inline_new:
                for lob_sti in inline_new:
                    rel_parts = PurePosixPath(lob_sti).parts
                    if (len(rel_parts) >= 4
                            and schema_re.match(rel_parts[1])
                            and table_re.match(rel_parts[2])):
                        table_key = f"{rel_parts[1]}/{rel_parts[2]}"
                        table_blobs[table_key].append(lob_sti)
                        all_blobs.append(lob_sti)

            # Filtype-hints
            xml_type_hints: dict = {}
            for table_key, xml_sti in table_xml_map.items():
                xml_file = extract_dir / xml_sti
                if not xml_file.exists():
                    continue
                blob_list = table_blobs.get(table_key, [])
                hints = _build_type_hints_from_xml(xml_file, blob_list)
                if hints:
                    xml_type_hints.update(hints)
                col_map = _build_lob_type_col_map(xml_file, blob_list)
                if col_map:
                    lob_type_map[table_key] = col_map
            progress("phase_done")
        else:
            inline_new = {}
            xml_pre    = {}
            xml_type_hints = {}
            phase(2, "Inline-ekstraksjon hoppet over")
            progress("phase_done")

        if stop_ev.is_set():
            w("  Avbrutt av bruker", "warn")
            progress("aborted", stats=dict(stats))
            return

        # ── Resume-filtrering ─────────────────────────────────────────────────
        _done = ctx.metadata.get("_blob_resume_done", set())
        if _done:
            _before = len(all_blobs)
            all_blobs = [z for z in all_blobs if z not in _done]
            w(f"  Resume: {_before - len(all_blobs):,} allerede behandlet hoppet over, "
              f"{len(all_blobs):,} gjenstår", "info")

        # ── Fase 3: Detekter og konverter blob-filer ──────────────────────────
        total = len(all_blobs)
        if total == 0:
            w("  Ingen blob-filer å behandle", "info")
            phase(3, "Konvertering hoppet over (ingen blobs)")
            progress("phase_done")
        else:
            phase(3, "Detekterer og konverterer blob-filer")
            progress("init", total=total)
            self._convert_all(
                all_blobs, extract_dir, stats, w, progress,
                lo_bin, stop_ev, pause_ev, csv_log,
                xml_type_hints=xml_type_hints,
                err_log=err_log)
            progress("phase_done")

        if stop_ev.is_set():
            w("  Konvertering avbrutt av bruker", "warn")
            progress("aborted", stats=dict(stats))
            return

        # ── Fase 4: Patch tableX.xml ──────────────────────────────────────────
        phase(4, "Oppdaterer tableX.xml")
        self._patch_all_xml(
            table_xml_map, table_blobs, extract_dir,
            inline_new, xml_pre, stats, w, progress,
            lob_type_map=lob_type_map,
            col_meta=col_meta)
        progress("phase_done")

        w("  Pipeline-modus: repakking overlates til 'Pakk sammen SIARD'.", "info")

    # ── Hoved-prosessering ────────────────────────────────────────────────────

    def _resolve_dst_path(self, dst_path: Path, w) -> Path:
        """Finn skrivbart filnavn — bruker faktisk filopprettelse, ikke os.access."""
        def _writable(p: Path) -> bool:
            try:
                with open(p, "ab"):
                    pass
                if p.exists() and p.stat().st_size == 0:
                    try: p.unlink()
                    except Exception: pass
                return True
            except (PermissionError, OSError):
                return False

        w(f"  Destinasjon: {dst_path}", "info")
        if _writable(dst_path):
            return dst_path
        stem, suffix, parent = dst_path.stem, dst_path.suffix, dst_path.parent
        for counter in range(1, 1000):
            candidate = parent / f"{stem}_{counter}{suffix}"
            if _writable(candidate):
                w(f"  OBS: {dst_path.name} ikke skrivbar — "
                  f"skriver til {candidate.name}", "warn")
                return candidate
        w(f"  FEIL: Fant ikke skrivbart filnavn — prøver {dst_path.name}", "feil")
        return dst_path

    def _process(self, ctx, src_path: Path, dst_path: Path,
                 stats: dict, w, progress,
                 lo_bin: str,
                 stop_ev: threading.Event,
                 pause_ev: threading.Event,
                 csv_log=None,
                 temp_root: Path = None,
                 err_log=None) -> None:

        # ── Pipeline-modus: utpakket mappe finnes allerede ───────────────────
        pre_dir = getattr(ctx, "extracted_path", None)
        if pre_dir is not None and pre_dir.is_dir():
            self._process_pipeline(
                ctx, pre_dir, stats, w, progress,
                lo_bin, stop_ev, pause_ev, csv_log, err_log)
            return

        PHASES = 6
        def phase(n, label):
            progress("phase", phase=n, total_phases=PHASES, label=label)

        schema_re = re.compile(r"^schema\d+$", re.IGNORECASE)
        table_re  = re.compile(r"^table\d+$",  re.IGNORECASE)

        # Bruk temp_root for tmpdir — unngår å fylle systemdisken
        with tempfile.TemporaryDirectory(dir=temp_root) as _tmpdir:
            tmpdir = Path(_tmpdir)

            # ── Fase 1: Skann ZIP ─────────────────────────────────────────────
            phase(1, "Skanner SIARD-arkiv")
            try:
                src_zip = zipfile.ZipFile(src_path, "r")
            except Exception as exc:
                raise RuntimeError(f"Kan ikke åpne SIARD: {exc}") from exc

            with src_zip:
                namelist = src_zip.namelist()

                # ── Versjondeteksjon ──────────────────────────────────────────
                _meta_name = next(
                    (n for n in namelist
                     if n.lower().endswith("header/metadata.xml")), None)
                src_version = "2.1"
                if _meta_name:
                    try:
                        src_version = detect_siard_version(
                            src_zip.read(_meta_name))
                    except Exception:
                        pass
                target_version = get_target_siard_version()
                w(f"  Kilde SIARD: {src_version}  →  "
                  f"Mål SIARD: {target_version}", "info")

                table_blobs:   dict[str, list[str]] = defaultdict(list)
                table_xml_map: dict[str, str]        = {}

                n_total_scan = len(namelist)
                SCAN_REPORT  = max(1, n_total_scan // 20)  # 5% intervaller
                for scan_i, name in enumerate(namelist, 1):
                    parts = PurePosixPath(name).parts
                    if len(parts) < 4 or parts[0].lower() != "content":
                        pass
                    elif not schema_re.match(parts[1]) or not table_re.match(parts[2]):
                        pass
                    else:
                        table_key = f"{parts[1]}/{parts[2]}"
                        fname_lower = parts[-1].lower()
                        if len(parts) == 4 and fname_lower.endswith(".xml"):
                            table_xml_map[table_key] = name
                        elif len(parts) >= 4:
                            # Samle ALLE filer i tabell-undermapper som blobs
                            # (ikke bare .bin og .txt — SIARD tillater .lob,
                            # ingen endelse, og direkte filendelser som .doc)
                            if not fname_lower.endswith(".xml") and \
                               not fname_lower.endswith(".xsd"):
                                table_blobs[table_key].append(name)
                    if scan_i % SCAN_REPORT == 0 or scan_i == n_total_scan:
                        progress("phase_progress",
                                 done=scan_i, total=n_total_scan)

                all_blobs = [p for paths in table_blobs.values() for p in paths]
                ext_counts: dict[str, int] = {}
                for p in all_blobs:
                    e = PurePosixPath(p).suffix.lstrip(".").lower() or "ingen"
                    ext_counts[e] = ext_counts.get(e, 0) + 1
                ext_summary = ", ".join(
                    f"{n}×.{e}" for e, n in sorted(ext_counts.items(), key=lambda x: -x[1]))
                w(f"  Fant: {len(all_blobs):,} blob-filer ({ext_summary}), "
                  f"{len(table_xml_map)} tableX.xml", "info")
                progress("phase_done")

                # ── Fase 2: Pakk ut SIARD til tmpdir ─────────────────────────
                phase(2, "Pakker ut filer til tmpdir")
                extract_dir = tmpdir / "extracted"
                extract_dir.mkdir()
                corrupt_in_zip: set[str] = set()

                n_total = len(namelist)
                n_done  = 0
                EXTRACT_REPORT = max(1, n_total // 40)  # ~2.5% intervaller
                for name in namelist:
                    try:
                        src_zip.extract(name, extract_dir)
                    except Exception as exc:
                        w(f"    [KORRUPT] {name}: {exc}", "feil")
                        progress("error", file=name, error=str(exc))
                        corrupt_in_zip.add(name)
                    n_done += 1
                    if n_done % EXTRACT_REPORT == 0 or n_done == n_total:
                        progress("phase_progress",
                                 done=n_done, total=n_total)
                w(f"  Utpakking fullfort: {n_done:,} filer", "info")

                progress("phase_done")

                # ── Fase 2b: Inline NBLOB/NCLOB ──────────────────────────────
                if self.params["extract_inline"]:
                    # Les metadata.xml for å finne LOB/MIME-kolonner per tabell
                    metadata_xml = extract_dir / "header" / "metadata.xml"
                    col_meta  = _read_col_metadata(metadata_xml)
                    lob_cols  = {k: v["lob_cols"] for k, v in col_meta.items()}
                    if lob_cols:
                        n_lob_tables = len(lob_cols)
                        n_lob_cols   = sum(len(v) for v in lob_cols.values())
                        n_mime_tables = sum(1 for v in col_meta.values() if v["mime_cols"])
                        w(f"  LOB-kolonner: {n_lob_cols} i {n_lob_tables} tabeller"
                          + (f", mimetype funnet i {n_mime_tables} tabeller" if n_mime_tables else ""),
                          "info")
                    else:
                        col_meta = {}
                        w("  metadata.xml ikke funnet — bruker kun INLINE_TAGS", "warn")

                    # ── Sjekk for inline HEX i LOB-kolonner ──────────────────
                    # Hvis det finnes HEX-felt som ikke er håndtert av
                    # extract_inline, må HexExtractOperation kjøres først.
                    hex_tables: list[str] = []
                    for table_key, xml_sti in table_xml_map.items():
                        xml_file = extract_dir / xml_sti
                        if not xml_file.exists():
                            continue
                        tbl_lob_cols = lob_cols.get(table_key, {})
                        if not tbl_lob_cols:
                            continue
                        # Les kun første 512 KB for rask sjekk
                        try:
                            sample = xml_file.read_bytes()[:524288]
                            sample_str = sample.decode("utf-8", errors="replace")
                        except Exception:
                            continue
                        # Enkel heuristikk: lang hex-streng (>16 tegn) uten file=
                        hex_pattern = re.compile(
                            r"<c\d+>([0-9a-fA-F\s]{16,})</c\d+>")
                        if hex_pattern.search(sample_str):
                            hex_tables.append(table_key)

                    if hex_tables:
                        w("", "warn")
                        w("  ╔══════════════════════════════════════════════════╗",
                          "warn")
                        w("  ║  INLINE HEX OPPDAGET — HANDLING NØDVENDIG       ║",
                          "warn")
                        w("  ╠══════════════════════════════════════════════════╣",
                          "warn")
                        w(f"  ║  Tabeller med inline HEX: {len(hex_tables)}",
                          "warn")
                        for t in hex_tables[:10]:
                            w(f"  ║    • {t}", "warn")
                        if len(hex_tables) > 10:
                            w(f"  ║    ... og {len(hex_tables)-10} til", "warn")
                        w("  ╠══════════════════════════════════════════════════╣",
                          "warn")
                        w("  ║  Kjør 'HEX Inline Extract' FØR denne operasjonen║",
                          "warn")
                        w("  ║  for å eksportere HEX-feltene til eksterne filer.║",
                          "warn")
                        w("  ╚══════════════════════════════════════════════════╝",
                          "warn")
                        w("", "warn")
                        w("  Stopper konvertering — legg til HEX Inline Extract "
                          "i workflow og kjør på nytt.", "feil")
                        progress("aborted", stats=dict(stats))
                        return

                    inline_new: dict[str, bytes] = {}
                    xml_pre:    dict[str, bytes]  = {}
                    for table_key, xml_sti in table_xml_map.items():
                        xml_file = extract_dir / xml_sti
                        if not xml_file.exists():
                            continue
                        try:
                            xml_bytes = xml_file.read_bytes()
                        except Exception as exc:
                            w(f"  FEIL les {xml_sti}: {exc}", "feil")
                            continue
                        patched, new_files, n = self._extract_inline(
                            xml_bytes, xml_sti, table_key, stats, w,
                            lob_cols=lob_cols)
                        if n > 0:
                            xml_pre[xml_sti]  = patched
                            inline_new.update(new_files)
                            for lob_sti, lob_data in new_files.items():
                                lob_path = extract_dir / lob_sti
                                lob_path.parent.mkdir(parents=True, exist_ok=True)
                                lob_path.write_bytes(lob_data)
                            w(f"  {table_key}: {n} inline ekstrahert", "ok")
                else:
                    inline_new = {}
                    xml_pre    = {}

                # ── Fase 2c: Bygg filtype-hints og lob→type-kobling fra XML ──
                xml_type_hints: dict[str, str] = {}
                lob_type_map:   dict[str, dict[str, list[str]]] = {}
                for table_key, xml_sti in table_xml_map.items():
                    xml_file = extract_dir / xml_sti
                    if not xml_file.exists():
                        continue
                    blob_list = table_blobs.get(table_key, [])
                    hints = _build_type_hints_from_xml(xml_file, blob_list)
                    if hints:
                        xml_type_hints.update(hints)
                        w(f"  {table_key}: {len(hints)} filtype-hint fra XML", "info")
                    col_map = _build_lob_type_col_map(xml_file, blob_list)
                    if col_map:
                        lob_type_map[table_key] = col_map
                        w(f"  {table_key}: kobling {col_map}", "info")

                # Supplerer med blob-er referert i tableX.xml men ikke i ZIP-en
                # (intern full-sti / DBPT-format, eller eksternt lagrede blobs)
                _xml_extra = _collect_external_blobs(
                    table_xml_map, extract_dir, src_path.parent, all_blobs, w)
                if _xml_extra:
                    all_blobs.extend(_xml_extra)

                # ── Resume-filtrering ─────────────────────────────────────────
                _done = ctx.metadata.get("_blob_resume_done", set())
                if _done:
                    _before = len(all_blobs)
                    all_blobs = [z for z in all_blobs if z not in _done]
                    w(f"  Resume: {_before - len(all_blobs):,} allerede behandlet hoppet over, "
                      f"{len(all_blobs):,} gjenstår", "info")

                # ── Fase 3+4: Detekter og konverter .bin/.txt ────────────────
                total = len(all_blobs)
                if total == 0:
                    w("  Ingen blob-filer å behandle", "info")
                else:
                    progress("init", total=total)
                    self._convert_all(
                        all_blobs, extract_dir, stats, w, progress,
                        lo_bin, stop_ev, pause_ev, csv_log,
                        xml_type_hints=xml_type_hints,
                        err_log=err_log)
                    progress("phase_done")

                if stop_ev.is_set():
                    w("  Konvertering avbrutt av bruker", "warn")
                    progress("aborted", stats=dict(stats))
                    return

                # ── Fase 4: Patch tableX.xml ──────────────────────────────────
                phase(5, "Oppdaterer tableX.xml")
                self._patch_all_xml(
                    table_xml_map, table_blobs, extract_dir,
                    inline_new, xml_pre, stats, w, progress,
                    lob_type_map=lob_type_map,
                    col_meta=col_meta)
                progress("phase_done")

                # ── Fase 5: Pakk ny SIARD ─────────────────────────────────────
                if not self.params["dry_run"]:
                    phase(6, "Pakker ny SIARD-fil")
                    self._pack_new_zip(extract_dir, namelist, inline_new,
                                       corrupt_in_zip, dst_path,
                                       target_version, w, progress,
                                       src_version=src_version)
                    progress("phase_done")

        # ── Rydd opp temp-mappe ───────────────────────────────────────────────
        # TemporaryDirectory rydder normalt automatisk, men på Windows kan
        # filer bli låst av LO-prosesser. Prøv eksplisitt opprydding.
        try:
            if tmpdir.exists():
                shutil.rmtree(tmpdir, ignore_errors=True)
                if not tmpdir.exists():
                    w(f"  Temp-mappe ryddet: {tmpdir}", "ok")
                else:
                    w(f"  Advarsel: temp-mappe kunne ikke slettes: {tmpdir}", "warn")
        except Exception as exc:
            w(f"  Advarsel: temp-opprydding feilet: {exc}", "warn")

    # ── Parallell konvertering ────────────────────────────────────────────────

    def _convert_all(self, all_blobs: list[str], extract_dir: Path,
                     stats: dict, w, progress,
                     lo_bin: str,
                     stop_ev: threading.Event,
                     pause_ev: threading.Event,
                     csv_log=None,
                     xml_type_hints: dict | None = None,
                     err_log=None) -> None:
        """
        Deteksjon parallelt + LO batch-konvertering med unik brukerprofil per instans.
        xml_type_hints: {filnavn: ext} fra type-kolonner i tableX.xml.
        """
        lock  = threading.Lock()
        max_w = max(1, min(self.params["max_workers"], os.cpu_count() or 2))
        hints = xml_type_hints or {}

        # Steg A: Deteksjon parallelt
        _COMPRESSED = {"zip", "gz", "bz2", "tar", "rar", "7z"}

        _utf8_bin_count: list[int] = [0]   # trådsikker teller via GIL-beskyttet int
        _utf8_bin_lock  = threading.Lock()

        def _detect_one(zip_sti: str) -> tuple[str, tuple[str, str, bool]]:
            p = extract_dir / zip_sti
            try:
                data = p.read_bytes()[:65536]
            except Exception:
                return zip_sti, ("bin", "application/octet-stream", False)

            ext, mime, is_encrypted = _detect(data)

            # ── Base64-dekoding ───────────────────────────────────────────────
            # Hvis innholdet er "txt" (ukjent tekst), prøv å tolke det som base64.
            # Gjelder også komprimerte base64-filer (base64 av zip/gz etc.)
            # Vi gjør ingenting hvis dekodingen bare gir txt/bin — da er det
            # sannsynligvis base64 av ren tekst, og originalen er allerede korrekt.
            if ext == "txt":
                full_data = p.read_bytes() if len(data) == 65536 else data
                decoded = _try_decode_base64(full_data)
                if decoded is not None:
                    inner_ext, inner_mime, inner_enc = _detect(decoded[:65536])
                    # Kjør evt. utpakking på dekodede data hvis det er et arkiv
                    if inner_ext in _COMPRESSED:
                        tmp_arc = p.with_suffix(".b64tmp")
                        try:
                            tmp_arc.write_bytes(decoded)
                            unpacked = _unpack_single_file(tmp_arc)
                            if unpacked is not None:
                                unpacked_data, unpacked_name = unpacked
                                inner_ext, inner_mime, inner_enc = _detect(unpacked_data[:65536])
                                p.write_bytes(unpacked_data)
                                w(f"    Base64+utpakket: {PurePosixPath(zip_sti).name} "
                                  f"({PurePosixPath(unpacked_name).name} → {inner_ext})", "info")
                                return zip_sti, (inner_ext, inner_mime, inner_enc)
                            else:
                                # Arkiv med flere filer — behold dekodert arkiv
                                p.write_bytes(decoded)
                                w(f"    Base64→{inner_ext}: {PurePosixPath(zip_sti).name}", "info")
                                return zip_sti, (inner_ext, inner_mime, inner_enc)
                        except Exception:
                            pass
                        finally:
                            tmp_arc.unlink(missing_ok=True)
                    elif inner_ext not in ("txt", "bin"):
                        # Kjent binærformat etter dekoding — erstatt og re-detekter
                        try:
                            p.write_bytes(decoded)
                            ext          = inner_ext
                            mime         = inner_mime
                            is_encrypted = inner_enc
                            w(f"    Base64→{ext}: {PurePosixPath(zip_sti).name}", "info")
                        except Exception:
                            pass
                    # inner_ext == "txt" eller "bin": dekoding ga ingenting nyttig
                    # — behold original og behandle som vanlig txt

            # ── Utpakking av komprimerte filer ───────────────────────────────
            elif ext in _COMPRESSED:
                result = _unpack_single_file(p)
                if result is not None:
                    inner_data, inner_name = result
                    inner_ext, inner_mime, inner_enc = _detect(inner_data[:65536])
                    if inner_ext not in ("bin", "txt") or len(inner_data) > 0:
                        try:
                            p.write_bytes(inner_data)
                            ext          = inner_ext
                            mime         = inner_mime
                            is_encrypted = inner_enc
                            w(f"    Pakket ut: {PurePosixPath(zip_sti).name} "
                              f"({PurePosixPath(inner_name).name} → {inner_ext})", "info")
                        except Exception:
                            pass
                elif ext == "zip":
                    # ZIP med flere filer — sjekk om innholdet er XML/tekst-struktur
                    # (f.eks. kommunal fagsystem som lagrer XML-dokumenter i ZIP)
                    try:
                        import zipfile as _zf
                        with _zf.ZipFile(p, "r") as zf:
                            members = [m for m in zf.infolist()
                                       if not m.filename.endswith("/")]
                            if members:
                                xml_count = 0
                                for m in members:
                                    chunk = zf.read(m.filename)[:512]
                                    member_ext, _, _ = _detect(chunk)
                                    if member_ext in ("xml", "txt", "html"):
                                        xml_count += 1
                                if xml_count == len(members):
                                    # Alle filer er XML/tekst — behold som zip
                                    # men logg at det er et XML-basert arkiv
                                    w(f"    ZIP(XML-arkiv): {PurePosixPath(zip_sti).name} "
                                      f"({len(members)} XML/tekst-filer) — beholdes som .zip",
                                      "info")
                    except Exception:
                        pass

            # ── UTF-8-kodet binærdata ─────────────────────────────────────────
            # Noen fagsystemer lagrer binærfiler ved å lese som cp1252 og skrive
            # som UTF-8.  Resultatet er en gyldig UTF-8-fil uten synlige magic-bytes
            # i binær form — men magic kan fremdeles matche som ASCII (f.eks. %PDF).
            # Ekte binære PDF-er er ikke gyldig UTF-8 (standard binær-marker
            # %âãÏÓ = \xe2\xe3... er ugyldig UTF-8), så risiko for falsk positiv er
            # minimal.  Kjøres for txt/bin (ingen ASCII-magic) og pdf (ASCII-magic).
            if ext in ("txt", "bin", "pdf"):
                full_data = p.read_bytes() if len(data) == 65536 else data
                recovered = _try_decode_utf8_binary(full_data)
                if recovered is not None:
                    rec_ext, rec_mime, rec_enc = _detect(recovered[:65536])
                    if rec_ext not in ("txt", "bin"):
                        try:
                            p.write_bytes(recovered)
                            ext          = rec_ext
                            mime         = rec_mime
                            is_encrypted = rec_enc
                            with _utf8_bin_lock:
                                _utf8_bin_count[0] += 1
                        except Exception:
                            pass

            # ── XML-type-hint fra tabell-metadata ────────────────────────────
            basename = PurePosixPath(zip_sti).name
            if ext in ("txt", "bin") and basename in hints:
                hint_ext = hints[basename]
                if hint_ext in _LO_CONVERTIBLE or hint_ext in _RENAME_ONLY_EXTS \
                        or hint_ext in ("rtf", "pdf"):
                    ext  = hint_ext
                    mime = f"application/{hint_ext}"

            return zip_sti, (ext, mime, is_encrypted)

        w(f"  Detekterer {len(all_blobs):,} filer ...", "info")
        det_results: dict[str, tuple[str, str, bool]] = {}
        n_total       = len(all_blobs)
        DETECT_REPORT = max(1, n_total // 10)   # logg ~10 ganger totalt
        n_detected    = 0
        progress("phase", phase=3, total_phases=6,
                 label=f"Detekterer {n_total:,} filer ...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as pool:
            futs = {pool.submit(_detect_one, z): z for z in all_blobs}
            for fut in concurrent.futures.as_completed(futs):
                zip_sti, result = fut.result()
                det_results[zip_sti] = result
                n_detected += 1
                if n_detected % DETECT_REPORT == 0 or n_detected == n_total:
                    w(f"  Detektert: {n_detected:,}/{n_total:,} filer ...", "info")
                    progress("phase_progress",
                             done=n_detected, total=n_total)

        if _utf8_bin_count[0]:
            w(f"  UTF-8-kodet binærdata: {_utf8_bin_count[0]:,} filer gjenopprettet", "info")

        # Steg B: Kategoriser
        to_convert:     list[tuple[int, str, str, str]] = []
        to_upgrade:     list[tuple[int, str, str, str]] = []  # gammel→ny format via LO
        to_rename_only: list[tuple[int, str, str, str]] = []
        to_wpt:         list[tuple[int, str, str, str]] = []  # WPTools native
        to_archive:     list[tuple[int, str, str, str]] = []  # komprimerte flerfil-arkiver
        unknown_items:  dict[str, list] = {}   # ext -> [(idx,zip_sti,ext,mime)]

        # Komprimerte formater som skal behandles spesielt
        _ARCHIVE_EXTS = {"zip", "gz", "bz2", "tar", "rar", "7z"}

        # Last live versjon av upgrade-mappingen
        _upgrade_live = _get_lo_upgrade()

        for idx, zip_sti in enumerate(all_blobs):
            ext, mime, is_encrypted = det_results[zip_sti]
            file_ext  = PurePosixPath(zip_sti).suffix.lstrip(".").lower()

            # Bevar CSV-ekstensjon: innhold er alltid ren tekst → detektert som "txt",
            # men hvis blob er lagret med .csv-ekstensjon skal den forbli .csv
            if ext == "txt" and file_ext == "csv":
                ext  = "csv"
                mime = "text/csv"

            # Ikke nedgrader .txt til .bin: kildefilen er eksplisitt lagret som
            # tekst, selv om byteinnholdet ikke er gyldig UTF-8 (kan skyldes
            # legacy-encoding, partial korrupsjon, eller spesielt tekstformat).
            # "bin" er catch-all for ukjent binærformat og er strengt dårligere.
            if ext == "bin" and file_ext == "txt":
                ext  = "txt"
                mime = "text/plain"

            # Passordbeskyttede filer: ikke konverter — kopier med riktig endelse
            if is_encrypted:
                to_rename_only.append((idx, zip_sti, ext, mime))
                w(f"  {PurePosixPath(zip_sti).name}: passordbeskyttet "
                  f"({ext.upper()}) — beholdes uten konvertering", "warn")
                continue

            if ext == "wpt":
                to_wpt.append((idx, zip_sti, ext, mime))
            elif ext in ("txt", "xml", "bin"):
                to_rename_only.append((idx, zip_sti, ext, mime))
            elif ext == "pdf" and self.params["skip_existing_pdf"]:
                to_rename_only.append((idx, zip_sti, ext, mime))
            elif ext in ("html", "htm"):
                to_rename_only.append((idx, zip_sti, ext, mime))
            elif ext in _ARCHIVE_EXTS:
                to_archive.append((idx, zip_sti, ext, mime))
            elif ext in _upgrade_live:
                # Eldre format som skal oppgraderes til nyere åpent format
                to_upgrade.append((idx, zip_sti, ext, mime))
            elif ext in _RENAME_ONLY_EXTS:
                to_rename_only.append((idx, zip_sti, ext, mime))
            elif ext in _LO_CONVERTIBLE:
                to_convert.append((idx, zip_sti, ext, mime))
            else:
                unknown_items.setdefault(ext, []).append((idx, zip_sti, ext, mime))

        # ── Spør bruker om ukjente, gyldige filtyper ─────────────────────────
        # Samle opp alle ukjente, gruppert per ext, og still ett spørsmål per ext
        if unknown_items:
            # Reloader listene slik at tidligere svar i samme sesjon tas hensyn til
            _lo_conv_live  = _get_lo_convertible()
            _rename_live   = _get_rename_only()

            for unk_ext, unk_files in sorted(unknown_items.items()):
                # Dobbeltsjekk at ext ikke ble lagt til av et tidligere svar
                if unk_ext in _lo_conv_live:
                    for item in unk_files:
                        to_convert.append(item)
                    continue
                if unk_ext in _rename_live:
                    for item in unk_files:
                        to_rename_only.append(item)
                    continue

                # Bygg eksempeliste (maks 3 filnavn)
                eks = [PurePosixPath(z).name for _, z, _, _ in unk_files[:3]]
                eks_str = ", ".join(eks)
                if len(unk_files) > 3:
                    eks_str += f" … (+{len(unk_files)-3})"

                svar = self._spor_ukjent_format(
                    ext=unk_ext,
                    antall=len(unk_files),
                    eksempler=eks_str,
                    mime=unk_files[0][3],
                )
                # svar: "konverter", "behold", "hopp_over"
                if svar == "konverter":
                    for item in unk_files:
                        to_convert.append(item)
                    # Legg til i config.json
                    try:
                        from settings import get_config, set_config
                        lst = list(get_config("lo_convertible") or [])
                        if unk_ext not in lst:
                            lst.append(unk_ext)
                            set_config("lo_convertible", lst)
                            w(f"  '{unk_ext}' lagt til i lo_convertible (config.json)", "ok")
                    except Exception:
                        pass
                elif svar == "behold":
                    for item in unk_files:
                        to_rename_only.append(item)
                    # Legg til i config.json
                    try:
                        from settings import get_config, set_config
                        lst = list(get_config("rename_only") or [])
                        if unk_ext not in lst:
                            lst.append(unk_ext)
                            set_config("rename_only", lst)
                            w(f"  '{unk_ext}' lagt til i rename_only (config.json)", "ok")
                    except Exception:
                        pass
                else:
                    # hopp_over — ignorer disse filene
                    w(f"  '{unk_ext}' ({len(unk_files)} filer) hoppet over", "warn")

        w(f"  Til PDF/A    : {len(to_convert):,}", "info")
        if to_convert:
            fmt_count: dict[str, int] = {}
            for _, _, ext, _ in to_convert:
                fmt_count[ext] = fmt_count.get(ext, 0) + 1
            fmt_str = ", ".join(f"{n}×{e.upper()}" for e, n in sorted(fmt_count.items()))
            w(f"    Formater: {fmt_str}", "info")
            if len(to_convert) <= 20:
                for _, zip_sti, ext, _ in to_convert:
                    sz = 0
                    try: sz = (extract_dir / zip_sti).stat().st_size
                    except Exception: pass
                    w(f"    → {PurePosixPath(zip_sti).name}  [{ext.upper()}  {sz:,} bytes]", "info")

        w(f"  Oppgradering : {len(to_upgrade):,}", "info")
        if to_upgrade:
            upg_count: dict[str, int] = {}
            for _, _, ext, _ in to_upgrade:
                target = _upgrade_live.get(ext, "?")
                key = f"{ext}→{target}"
                upg_count[key] = upg_count.get(key, 0) + 1
            w(f"    Formater: {', '.join(f'{n}×{k.upper()}' for k,n in sorted(upg_count.items()))}", "info")

        w(f"  Arkiver      : {len(to_archive):,}", "info")
        if to_archive:
            arc_fmt: dict[str, int] = {}
            for _, _, ext, _ in to_archive:
                arc_fmt[ext] = arc_fmt.get(ext, 0) + 1
            w(f"    Typer: {', '.join(f'{n}×{e.upper()}' for e,n in sorted(arc_fmt.items()))}", "info")

        w(f"  WPTools (wpt): {len(to_wpt):,}", "info")
        w(f"  Rename/behold: {len(to_rename_only):,}", "info")

        # Logg hva .txt-filer faktisk ble detektert som
        txt_blobs = [z for z in all_blobs if z.lower().endswith(".txt")]
        if txt_blobs:
            txt_by_ext: dict[str, int] = {}
            for z in txt_blobs:
                e = det_results[z][0]
                txt_by_ext[e] = txt_by_ext.get(e, 0) + 1
            parts = ", ".join(f"{n}×{e}" for e, n in sorted(txt_by_ext.items()))
            w(f"  .txt-deteksjon: {parts}", "info")

        # Steg B2: Arkiver — pakk ut, konverter innhold, repakk
        if to_archive and not stop_ev.is_set():
            self._process_archives(
                to_archive, to_convert, to_rename_only, stats,
                extract_dir, lo_bin, stop_ev, pause_ev,
                csv_log, err_log, w, lock, max_workers=max_w)

        # Steg B3: Formatoppgradering — xls→xlsx, ppt→pptx, doc→docx osv.
        if to_upgrade and not stop_ev.is_set():
            self._upgrade_formats(
                to_upgrade, _upgrade_live, stats,
                extract_dir, lo_bin, stop_ev, pause_ev,
                csv_log, err_log, w, lock)

        # Steg C: Rename-bare — behandle uten GUI-event per fil (kan vaere 100k+)
        # Send kun periodiske stats-oppdateringer for å holde GUI responsiv
        REPORT_INTERVAL = 200   # oppdater GUI hver N fil
        rename_ext_counts: dict[str, int] = {}   # teller per detektert ext
        for i, (idx, zip_sti, ext, mime) in enumerate(to_rename_only):
            if stop_ev.is_set():
                break
            src_file = extract_dir / zip_sti
            fra_sz   = src_file.stat().st_size if src_file.exists() else 0
            src_ext  = PurePosixPath(zip_sti).suffix.lstrip(".").lower()

            self._rename_file(extract_dir, zip_sti, ext)
            with lock:
                stats["kept"] += 1
            rename_ext_counts[ext] = rename_ext_counts.get(ext, 0) + 1

            # Etter rename: finn ny filsti
            stem     = PurePosixPath(zip_sti).stem
            new_sti  = str(PurePosixPath(zip_sti).parent / f"{stem}.{ext}")
            new_file = extract_dir / new_sti
            til_sz   = new_file.stat().st_size if new_file.exists() else fra_sz

            if csv_log:
                if ext == src_ext:
                    kommentar = "Beholdt originalformat"
                elif ext == "pdf":
                    kommentar = "Allerede PDF - ingen konvertering"
                elif src_ext in ("bin", ""):
                    kommentar = f"Detektert som {ext} - endret filendelse"
                else:
                    kommentar = f"Detektert som {ext}"
                csv_log.write(
                    zip_sti, fra_sz, src_ext,
                    new_sti, til_sz, ext,
                    kommentar)

            if (i + 1) % REPORT_INTERVAL == 0 or i == len(to_rename_only) - 1:
                with lock:
                    s = dict(stats)
                progress("stats_update", stats=s, done=idx + 1)

        # Advarsel for arkivformater som beholdes ubehandlet
        _ARCHIVE_WARN = {"7z", "rar"}
        for arc_ext in sorted(_ARCHIVE_WARN & set(rename_ext_counts)):
            cnt = rename_ext_counts[arc_ext]
            w(
                f"  ADVARSEL: {cnt} fil(er) med format '.{arc_ext}' ble beholdt ubehandlet "
                f"(rename_only). Innholdet er ikke tilgjengelig uten utpakking og er "
                f"uegnet for langtidsbevaring.",
                "warn",
            )

        # Send aggregert format-teller for rename-only til GUI
        if rename_ext_counts:
            progress("rename_format_counts", counts=dict(rename_ext_counts))

        if not to_convert and not to_wpt:
            return

        # Steg C2: WPTools-filer — rename til .wpt + generer _ext_wpt.rtf,
        # legg RTF i to_convert for viderekonvertering til PDF/A via LO
        if to_wpt:
            w(f"  Behandler {len(to_wpt):,} WPTools-filer ...", "info")
            wpt_rtf_for_lo: list[tuple[int, str, str, str]] = []

            for idx, zip_sti, ext, mime in to_wpt:
                if stop_ev.is_set():
                    break
                src_file = extract_dir / zip_sti
                fra_sz   = src_file.stat().st_size if src_file.exists() else 0
                src_ext  = PurePosixPath(zip_sti).suffix.lstrip(".").lower()
                stem     = PurePosixPath(zip_sti).stem
                parent   = PurePosixPath(zip_sti).parent

                # 1. Rename original til .wpt
                wpt_sti  = str(parent / f"{stem}.wpt")
                wpt_path = extract_dir / wpt_sti
                if src_file.exists() and str(src_file) != str(wpt_path):
                    try:
                        src_file.rename(wpt_path)
                    except Exception as exc:
                        w(f"    [FEIL] rename {zip_sti}: {exc}", "feil")
                        wpt_path = src_file

                # 2. Generer _ext_wpt.rtf ved siden av
                rtf_sti  = str(parent / f"{stem}_ext_wpt.rtf")
                rtf_path = extract_dir / rtf_sti
                rtf_ok   = False
                try:
                    wpt_bytes = wpt_path.read_bytes()
                    rtf_bytes = _wpt_to_rtf(wpt_bytes)
                    if rtf_bytes:
                        rtf_path.write_bytes(rtf_bytes)
                        w(f"    [WPT] {stem}.wpt → {stem}_ext_wpt.rtf "
                          f"({len(rtf_bytes):,} bytes) — sendes til PDF/A", "ok")
                        # Legg RTF-filen i kø for LO-konvertering til PDF/A
                        wpt_rtf_for_lo.append((idx, rtf_sti, "rtf",
                                               "application/rtf"))
                        rtf_ok = True
                        kommentar = "WPTools native — RTF tekstuttrekk sendt til PDF/A"
                    else:
                        w(f"    [WPT] {stem}.wpt — tekstuttrekk tomt", "warn")
                        kommentar = "WPTools native — tekstuttrekk feilet, beholdt original"
                except Exception as exc:
                    w(f"    [FEIL] WPT-konvertering {stem}: {exc}", "feil")
                    kommentar = f"WPTools native — feil: {exc}"

                with lock:
                    if rtf_ok:
                        pass   # telles som converted etter LO er ferdig
                    else:
                        stats["kept"] += 1
                    s = dict(stats)

                if csv_log:
                    til_sz = rtf_path.stat().st_size if rtf_path.exists() else 0
                    res_name = (f"{stem}.wpt + {stem}_ext_wpt.rtf → PDF/A"
                                if rtf_ok else f"{stem}.wpt")
                    csv_log.write(
                        zip_sti, fra_sz, src_ext,
                        res_name, til_sz, "wpt+rtf→pdf",
                        kommentar)

            # Rapporter WPT-filer til format-diagram
            if to_wpt:
                progress("rename_format_counts", counts={"wpt": len(to_wpt)})

            # Legg WPT-genererte RTF-filer til vanlig konvertering
            if wpt_rtf_for_lo:
                w(f"  {len(wpt_rtf_for_lo)} WPT-RTF-filer legges til batch", "info")
                to_convert = list(to_convert) + wpt_rtf_for_lo

        if not to_convert:
            return

        # Steg D: LO batch-konvertering
        progress("phase", phase=4, total_phases=6,
                 label="Konverterer filer (batch LO)")
        batch_size  = max(1, self.params["lo_batch_size"])
        all_batches = [to_convert[i:i+batch_size]
                       for i in range(0, len(to_convert), batch_size)]
        profiles_root = extract_dir.parent / "lo_profiles"
        profiles_root.mkdir(exist_ok=True)

        _zip_to_idx = {zip_sti: idx
                       for idx, zip_sti, _, _
                       in to_convert}

        w(f"  LO: {len(all_batches)} batch(er) à ~{batch_size} filer, "
          f"{max_w} parallelle instanser", "info")

        pdfa_version = self.params.get("pdfa_version", _PDFA_DEFAULT)
        pdfa_filter  = _build_pdfa_filter(pdfa_version)
        w(f"  PDF/A-format: {pdfa_version}", "info")

        # Opprett et fast antall gjenbrukbare profiler — én per worker.
        # Disse initialiseres én gang og gjenbrukes for alle batches.
        # Unngår at Windows-filsystemet overbelastes av tusenvis av
        # opprett/slett-sykluser som kan korruptere LO-profilen.
        worker_profiles: list[Path] = []
        for wi in range(max_w):
            pd = profiles_root / f"worker{wi}"
            pd.mkdir(exist_ok=True)
            worker_profiles.append(pd)

        # Semaphore-basert tildeling av profil per worker-slot
        import queue as _queue
        profile_pool: _queue.Queue = _queue.Queue()
        for pd in worker_profiles:
            profile_pool.put(pd)

        def _lo_profile_url(profile_dir: Path) -> str:
            return profile_dir.as_uri()

        def _run_lo_chunk(items: list, batch_tag: str,
                          profile_dir: Path, timeout: int,
                          work_root: Path) -> tuple[bool, str, list[tuple[Path,str,str,str]]]:
            """
            Kjør LO på en liste filer. Returnerer (lo_ok, lo_err, input_map).
            input_map = [(batch_file, zip_sti, ext, mime), ...]
            """
            out_dir = work_root / "out"
            work_root.mkdir(exist_ok=True)
            out_dir.mkdir(exist_ok=True)

            input_map: list[tuple[Path, str, str, str]] = []
            for item_i, (_, zip_sti, ext, mime) in enumerate(items):
                src = extract_dir / zip_sti
                if not src.exists():
                    continue
                dst_name = f"{batch_tag}_{item_i:04d}_{src.stem}.{ext}"
                dst      = work_root / dst_name
                try:
                    shutil.copy2(src, dst)
                    if ext == "rtf":
                        raw      = dst.read_bytes()
                        stripped = _strip_rtf_ole_objects(raw)
                        if stripped != raw:
                            dst.write_bytes(stripped)
                    input_map.append((dst, zip_sti, ext, mime))
                except Exception as exc:
                    w(f"    Kopi feilet {zip_sti}: {exc}", "feil")

            if not input_map:
                return True, "", []

            cmd = [
                lo_bin,
                f"-env:UserInstallation={_lo_profile_url(profile_dir)}",
                "--headless", "--norestore", "--nofirststartwizard",
                "--convert-to", pdfa_filter,
                "--outdir", str(out_dir),
            ] + [str(f) for f, _, _, _ in input_map]

            lo_ok  = False
            lo_err = ""

            def _kill_pid(pid: int) -> None:
                """Drep prosess-tre ved PID. Bruker Win32 API direkte — ingen subprocess."""
                if sys.platform == "win32":
                    import ctypes, ctypes.wintypes
                    TH32CS_SNAPPROCESS = 0x00000002
                    class PROCESSENTRY32(ctypes.Structure):
                        _fields_ = [
                            ("dwSize",              ctypes.wintypes.DWORD),
                            ("cntUsage",            ctypes.wintypes.DWORD),
                            ("th32ProcessID",       ctypes.wintypes.DWORD),
                            ("th32DefaultHeapID",   ctypes.POINTER(ctypes.c_ulong)),
                            ("th32ModuleID",        ctypes.wintypes.DWORD),
                            ("cntThreads",          ctypes.wintypes.DWORD),
                            ("th32ParentProcessID", ctypes.wintypes.DWORD),
                            ("pcPriClassBase",      ctypes.c_long),
                            ("dwFlags",             ctypes.wintypes.DWORD),
                            ("szExeFile",           ctypes.c_char * 260),
                        ]
                    k32 = ctypes.windll.kernel32
                    # Bygg parent→children map fra snapshot
                    snap = k32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
                    children: list[int] = []
                    if snap != ctypes.wintypes.HANDLE(-1).value:
                        pe = PROCESSENTRY32()
                        pe.dwSize = ctypes.sizeof(PROCESSENTRY32)
                        if k32.Process32First(snap, ctypes.byref(pe)):
                            while True:
                                if pe.th32ParentProcessID == pid:
                                    children.append(pe.th32ProcessID)
                                if not k32.Process32Next(snap, ctypes.byref(pe)):
                                    break
                        k32.CloseHandle(snap)
                    # Drep barn rekursivt, deretter forelder
                    for child_pid in children:
                        _kill_pid(child_pid)
                    PROCESS_TERMINATE = 0x0001
                    h = k32.OpenProcess(PROCESS_TERMINATE, False, pid)
                    if h:
                        k32.TerminateProcess(h, 1)
                        k32.CloseHandle(h)
                else:
                    import signal as _sig, os as _os
                    try:
                        _os.killpg(_os.getpgid(pid), _sig.SIGKILL)
                    except Exception:
                        try:
                            _os.kill(pid, _sig.SIGKILL)
                        except Exception:
                            pass

            def _popen(c) -> subprocess.Popen:
                kwargs: dict = dict(
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if sys.platform != "win32":
                    kwargs["start_new_session"] = True
                return subprocess.Popen(c, **kwargs)

            def _run_with_timeout(c, t) -> tuple[bool, str]:
                """
                Kjør LO med aktivitets-watchdog.
                Garanterer retur innen ACTIVITY_TIMEOUT + 2s etter siste PDF.
                Reader-tråder er daemon og avsluttes automatisk ved prosess-kill.
                """
                import time as _time

                ACTIVITY_TIMEOUT = 30   # sekunder uten ny PDF → kill
                STARTUP_GRACE    = 15   # sekunder før watchdog aktiveres

                stdout_buf: list[str] = []
                stderr_buf: list[str] = []
                stop_read  = threading.Event()
                p          = None

                def _read_pipe(pipe, buf):
                    """Les pipe ikke-blokkerende; stopper når stop_read settes."""
                    try:
                        while not stop_read.is_set():
                            line = pipe.readline()
                            if not line:
                                break
                            buf.append(line)
                    except Exception:
                        pass

                out_dir_watch = work_root / "out"
                def _pdf_count() -> int:
                    try:
                        return sum(1 for f in out_dir_watch.iterdir()
                                   if f.suffix.lower() == ".pdf")
                    except Exception:
                        return 0

                def _kill_and_clean(proc) -> None:
                    """Drep prosess og lukk pipes — alltid ikke-blokkerende."""
                    stop_read.set()
                    if proc is None:
                        return
                    try:
                        pid = proc.pid
                        # Drep prosessen FØRST — dette sender EOF til pipes og
                        # unblokker readline() slik at pipe.close() ikke henger
                        _kill_pid(pid)
                        # Lukk pipe-handles etter prosessen er drept
                        for pipe in (proc.stdin, proc.stdout, proc.stderr):
                            try:
                                if pipe: pipe.close()
                            except Exception:
                                pass
                    except Exception:
                        pass

                try:
                    p = _popen(c)
                    t_out = threading.Thread(
                        target=_read_pipe, args=(p.stdout, stdout_buf), daemon=True)
                    t_err = threading.Thread(
                        target=_read_pipe, args=(p.stderr, stderr_buf), daemon=True)
                    t_out.start()
                    t_err.start()

                    start_time    = _time.monotonic()
                    deadline      = start_time + t
                    last_activity = start_time
                    last_pdf_n    = _pdf_count()

                    while _time.monotonic() < deadline:
                        if p.poll() is not None:
                            # LO ferdig — vent maks 1s på reader-tråder
                            t_out.join(timeout=1)
                            t_err.join(timeout=1)
                            ok  = p.returncode == 0
                            err = "".join(stderr_buf) or "".join(stdout_buf)
                            return ok, ("" if ok else err[:300])

                        _time.sleep(0.25)

                        n = _pdf_count()
                        if n != last_pdf_n:
                            last_pdf_n    = n
                            last_activity = _time.monotonic()

                        elapsed = _time.monotonic() - start_time
                        if elapsed >= STARTUP_GRACE:
                            idle = _time.monotonic() - last_activity
                            if idle >= ACTIVITY_TIMEOUT:
                                try:
                                    filer = [f.name for f, _, _, _ in input_map]
                                    filer_str = ", ".join(str(f) for f in filer)
                                except Exception:
                                    filer_str = "(ukjent)"
                                msg = (f"Ingen aktivitet på {ACTIVITY_TIMEOUT}s — avbrutt. "
                                       f"Filer: {filer_str}")
                                w(f"  [VAKTPOST] {msg}", "warn")
                                return False, msg

                    return False, f"Tidsavbrudd (>{t}s)"

                except Exception as exc:
                    return False, str(exc)
                finally:
                    # Kjør kill i daemon-tråd — maks 2s, aldri blokkerende
                    _kt = threading.Thread(target=_kill_and_clean, args=(p,), daemon=True)
                    _kt.start()
                    _kt.join(timeout=2)
                    if _kt.is_alive():
                        w(f"  [ADVARSEL] Kill-tråd svarte ikke etter 2s — fortsetter", "warn")

            lo_ok, lo_err = _run_with_timeout(cmd, timeout)
            if not lo_ok and lo_err and \
                    ("UserInstallation" in lo_err or "Fatal Error" in lo_err):
                # Korrupt profil — reset og ett nytt forsøk
                shutil.rmtree(profile_dir, ignore_errors=True)
                profile_dir.mkdir(exist_ok=True)
                lo_ok, lo_err = _run_with_timeout(cmd, timeout)
                if lo_ok:
                    w("  OK etter profil-tilbakestilling", "ok")

            return lo_ok, lo_err, input_map

        def _process_file_result(
                batch_file: Path, zip_sti: str, ext: str,
                out_dir: Path, lo_ok: bool, lo_err: str) -> None:
            """Håndter resultatet for én fil: flytt PDF eller behold original."""
            filename  = PurePosixPath(zip_sti).name
            pdf_path  = out_dir / (batch_file.stem + ".pdf")
            ok_this   = pdf_path.exists()

            orig_file = extract_dir / zip_sti
            fra_sz    = orig_file.stat().st_size if orig_file.exists() else 0
            src_ext   = PurePosixPath(zip_sti).suffix.lstrip(".").lower()
            pdf_sti   = None  # settes i ok_this-blokken

            if ok_this:
                # Alltid inkluder detektert format i filnavnet:
                # record001.bin (doc) → record001.doc.pdf
                # record001.doc (doc) → record001.doc.pdf
                # record001.txt (rtf) → record001.rtf.pdf
                stem_base  = PurePosixPath(zip_sti).stem
                final_stem = f"{stem_base}.{ext}"

                pdf_sti    = str(PurePosixPath(zip_sti).parent /
                                 (final_stem + ".pdf"))
                pdf_target = extract_dir / pdf_sti
                pdf_target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(str(pdf_path), str(pdf_target))
                    orig = extract_dir / zip_sti
                    if orig.exists():
                        orig.unlink()
                    result_ext = "pdf"
                    file_ok    = True
                except Exception as exc:
                    w(f"    Flytt feilet {filename}: {exc}", "feil")
                    result_ext = ext
                    file_ok    = False
            else:
                result_ext = ext
                file_ok    = False

            with lock:
                if file_ok:
                    stats["converted"] += 1
                else:
                    self._rename_file(extract_dir, zip_sti, ext)
                    stats["failed"] += 1
                    w(f"  Konvertering feilet: {filename} — beholder som .{ext}"
                      + (f" ({lo_err[:80]})" if lo_err else ""), "warn")
                    # Loggfør til feilogg
                    if err_log:
                        err_log.write(zip_sti, ext, _err_clean(lo_err or "Ingen PDF produsert"))
            if csv_log:
                if file_ok and pdf_sti:
                    til_file = extract_dir / pdf_sti
                else:
                    til_file = extract_dir / (
                        str(PurePosixPath(zip_sti).parent /
                            (PurePosixPath(zip_sti).stem + f".{result_ext}")))
                til_sz    = til_file.stat().st_size if til_file.exists() else 0
                kommentar = ("Konvertert til PDF/A" if file_ok
                             else f"Konvertering feilet: {lo_err[:80]}" if lo_err
                             else "Ingen PDF produsert — mulig korrupt fil")
                csv_log.write(
                    zip_sti, fra_sz, src_ext,
                    str(PurePosixPath(zip_sti).parent / Path(str(til_file)).name),
                    til_sz, result_ext,
                    kommentar)

            with lock:
                stats["detected"] += 1
                s = dict(stats)

            fidx = _zip_to_idx.get(zip_sti, 0)
            progress("file_done",
                     idx=fidx,
                     filename=filename,
                     detected_ext=ext,
                     result_ext=result_ext,
                     ok=file_ok,
                     msg="konvertert til PDF/A" if file_ok else f"feilet -> .{ext}",
                     stats=s)
            if not file_ok and lo_err:
                progress("error", file=filename, error=lo_err[:120])

        def _run_one_batch(items: list, batch_tag: str,
                           profile_dir: Path, timeout: int,
                           work_root: Path) -> list:
            """
            Kjør én batch. Vellykkede filer flyttes til PDF/A umiddelbart.
            Returnerer liste av items som ikke ble konvertert (for retry).
            """
            lo_ok, lo_err, input_map = _run_lo_chunk(
                items, batch_tag, profile_dir, timeout, work_root)

            failed_items = []
            for batch_file, zip_sti, ext, mime in input_map:
                if stop_ev.is_set():
                    break
                pdf_exists = (work_root / "out" / (batch_file.stem + ".pdf")).exists()
                if pdf_exists:
                    _process_file_result(
                        batch_file, zip_sti, ext,
                        work_root / "out", True, "")
                else:
                    # Ikke marker som feilet ennå — legg i retry-liste
                    orig = next((it for it in items if it[1] == zip_sti), None)
                    if orig:
                        failed_items.append(orig)

            shutil.rmtree(work_root, ignore_errors=True)
            return failed_items

        def _run_batch(batch_idx_and_batch: tuple[int, list]) -> None:
            batch_idx, batch = batch_idx_and_batch

            if stop_ev.is_set():
                return
            while pause_ev.is_set():
                if stop_ev.is_set():
                    return
                import time as _t; _t.sleep(0.1)

            profile_dir = None
            while profile_dir is None:
                if stop_ev.is_set():
                    return
                try:
                    import queue as _queue
                    profile_dir = profile_pool.get(timeout=5)
                except _queue.Empty:
                    continue
            try:
                timeout   = self.params["lo_timeout"]
                work_root = extract_dir.parent / f"b{batch_idx}"

                w(f"  Batch {batch_idx+1}/{len(all_batches)}: "
                  f"{len(batch)} filer (profil {profile_dir.name})", "info")

                try:
                    failed = _run_one_batch(
                        batch, f"b{batch_idx}", profile_dir, timeout, work_root)
                    if failed:
                        with retry_lock:
                            retry_list.extend(failed)
                except Exception as exc:
                    w(f"  [FEIL] Batch {batch_idx+1} krasjet: {exc} — "
                      f"filer legges til nytt forsøk", "feil")
                    with retry_lock:
                        retry_list.extend(batch)
                    shutil.rmtree(work_root, ignore_errors=True)
            finally:
                profile_pool.put(profile_dir)

        retry_lock: threading.Lock = threading.Lock()
        retry_list: list = []

        # Kjør alle batches parallelt
        indexed    = list(enumerate(all_batches))
        import time as _time_mod
        _hb_stop   = threading.Event()   # dedikert stopp for heartbeat

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as pool:
            fs = {pool.submit(_run_batch, ib): ib[0] for ib in indexed}

            _last_done   = [0]
            _last_report = [_time_mod.monotonic()]

            def _heartbeat():
                while not _hb_stop.is_set():
                    _time_mod.sleep(60)
                    if _hb_stop.is_set():
                        break
                    elapsed = _time_mod.monotonic() - _last_report[0]
                    done_now = sum(1 for f in fs if f.done())
                    pending  = sum(1 for f in fs if not f.done())
                    if done_now == _last_done[0]:
                        w(f"  [HJERTESLAG] {done_now}/{len(fs)} ferdig, "
                          f"{pending} venter, ingen fremgang siste {elapsed:.0f}s", "warn")

                        # Dump stack traces for alle aktive tråder
                        if elapsed >= 120:
                            import traceback as _tb
                            frames = []
                            for tid, frame in sys._current_frames().items():
                                # Finn siste relevante frame
                                stack = _tb.extract_stack(frame)
                                relevant = [
                                    f"L{f.lineno} {f.name}() — {f.line}"
                                    for f in stack
                                    if any(k in (f.filename or "")
                                           for k in ("blob_convert", "blob_csv",
                                                     "subprocess", "rmtree"))
                                    and f.line
                                ]
                                if relevant:
                                    frames.append(
                                        f"  Tråd {tid}: " + " → ".join(relevant[-3:]))
                            if frames:
                                w("  [HEARTBEAT] Stack-traces:\n" +
                                  "\n".join(frames[:10]), "warn")

                        # Etter 10 minutter uten fremgang — avbryt
                        if elapsed >= 600:
                            w(f"  [HJERTESLAG] 10 min uten fremgang — avbryter", "feil")
                            stop_ev.set()
                            for f in fs:
                                f.cancel()
                            _hb_stop.set()
                            break
                    else:
                        _last_done[0]   = done_now
                        _last_report[0] = _time_mod.monotonic()

            hb = threading.Thread(target=_heartbeat, daemon=True)
            hb.start()

            for fut in concurrent.futures.as_completed(fs):
                if stop_ev.is_set():
                    for f in fs:
                        f.cancel()
                    break
                try:
                    fut.result()
                except Exception as exc:
                    w(f"  Batch-fremtidsfeil: {exc}", "feil")

        # Stopp heartbeat
        _hb_stop.set()

        # Nullstill stop_ev hvis det var heartbeat som satte den
        if not pause_ev.is_set():
            stop_ev.clear()

        # ── Retry-runde: parallell batch-konvertering av feilede filer ───────
        if retry_list and not stop_ev.is_set():
            retry_timeout = min(60, self.params["lo_timeout"])
            # Retry en og en fil — isolerer hengende filer og unngår at én fil
            # blokkerer hele batchen igjen
            retry_batches = [[item] for item in retry_list]
            w(f"  Nytt forsøk: {len(retry_list)} filer enkeltvis, {max_w} worker(e) "
              f"(timeout {retry_timeout}s) ...", "info")

            converted_before = stats.get("converted", 0)

            def _run_retry_batch(rb_args):
                rb_idx, rb_items = rb_args
                if stop_ev.is_set():
                    return
                while pause_ev.is_set():
                    if stop_ev.is_set():
                        return
                    import time as _t; _t.sleep(0.1)

                rb_profile = None
                while rb_profile is None:
                    if stop_ev.is_set():
                        return
                    try:
                        import queue as _qr
                        rb_profile = profile_pool.get(timeout=5)
                    except _qr.Empty:
                        continue

                try:
                    retry_root = extract_dir.parent / f"retry_b{rb_idx}"
                    lo_ok, lo_err, input_map = _run_lo_chunk(
                        rb_items, f"retry_b{rb_idx}", rb_profile,
                        retry_timeout, retry_root)

                    expected: dict[str, tuple] = {
                        zip_sti: (ext, mime)
                        for _, zip_sti, ext, mime in rb_items
                    }
                    processed_stis: set[str] = set()

                    for batch_file, zip_sti, ext, mime in input_map:
                        if stop_ev.is_set():
                            break
                        processed_stis.add(zip_sti)
                        fname  = PurePosixPath(zip_sti).name
                        pdf_ok = (retry_root / "out" /
                                  (batch_file.stem + ".pdf")).exists()
                        if pdf_ok:
                            _process_file_result(
                                batch_file, zip_sti, ext,
                                retry_root / "out", True, "")
                            w(f"    Nytt forsøk OK: {fname}", "ok")
                        else:
                            err = lo_err or "Ingen PDF produsert"
                            _process_file_result(
                                batch_file, zip_sti, ext,
                                retry_root / "out", False, err)
                            w(f"    Nytt forsøk feilet: {fname} "
                              f"— beholdes som .{ext}", "warn")

                    # Filer som ikke kom med i input_map (kopi feilet)
                    for zip_sti, (ext, mime) in expected.items():
                        if zip_sti not in processed_stis:
                            fname = PurePosixPath(zip_sti).name
                            self._rename_file(extract_dir, zip_sti, ext)
                            with lock:
                                stats["failed"] += 1
                            w(f"    Nytt forsøk kopi-feil: {fname}", "warn")
                            if err_log:
                                err_log.write(zip_sti, ext, "Retry kopi-feil")

                    shutil.rmtree(retry_root, ignore_errors=True)
                finally:
                    profile_pool.put(rb_profile)

            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_w) as retry_pool:
                list(retry_pool.map(_run_retry_batch,
                                    enumerate(retry_batches)))

            retry_ok = stats.get("converted", 0) - converted_before
            w(f"  Nytt forsøk ferdig: {retry_ok}/{len(retry_list)} konvertert",
              "info")

        # Rydd profiler ved slutten
        shutil.rmtree(profiles_root, ignore_errors=True)


    def _upgrade_formats(self,
                         to_upgrade:    list,
                         upgrade_map:   dict,
                         stats:         dict,
                         extract_dir:   Path,
                         lo_bin:        str,
                         stop_ev:       threading.Event,
                         pause_ev:      threading.Event,
                         csv_log,
                         err_log,
                         w,
                         lock:          threading.Lock) -> None:
        """
        Oppgrader eldre Office-formater til nyere åpne formater via LibreOffice.

        Konverteringstabellen (fra config.json «lo_upgrade»):
            xls/xlt  → xlsx   (Calc OOXML)
            ppt/pot  → pptx   (Impress OOXML)
            doc/dot  → docx   (Writer OOXML)

        Navngivning: record001.xls → record001.xls.xlsx
        Filer som feiler beholdes i originalformat og telles som «kept».
        """
        # LO filter-strenger per målformat
        _LO_FILTERS: dict[str, str] = {
            "xlsx": "Calc MS Excel 2007 XML",
            "pptx": "Impress MS PowerPoint 2007 XML",
            "docx": "MS Word 2007 XML",
        }

        def _lo_convert(src: Path, target_ext: str, work_dir: Path) -> Path | None:
            """Konverter én fil med LO til target_ext. Returnerer resultat-fil eller None."""
            lo_filter = _LO_FILTERS.get(target_ext)
            if not lo_filter:
                return None
            out_dir = work_dir / "out"
            out_dir.mkdir(parents=True, exist_ok=True)
            filter_str = f"{target_ext}:{lo_filter}"
            try:
                result = subprocess.run(
                    [lo_bin, "--headless", "--norestore",
                     "--convert-to", filter_str,
                     "--outdir", str(out_dir),
                     str(src)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    timeout=self.params.get("lo_timeout", 300))
                # LO skriver til out_dir/<stem>.<target_ext>
                expected = out_dir / (src.stem + "." + target_ext)
                if expected.exists() and expected.stat().st_size > 0:
                    return expected
                # Søk etter noen fil med riktig endelse
                for f in out_dir.iterdir():
                    if f.suffix.lower() == "." + target_ext and f.stat().st_size > 0:
                        return f
            except Exception:
                pass
            return None

        max_w = max(1, min(self.params.get("max_workers", 4), os.cpu_count() or 2))
        tmp_base = Path(tempfile.mkdtemp(prefix="siard_upg_", dir=extract_dir))
        n_ok = 0
        n_fail = 0

        try:
            def _process_one(args):
                idx, zip_sti, src_ext, mime = args
                if stop_ev.is_set():
                    return

                blob_path = extract_dir / zip_sti
                if not blob_path.exists():
                    return

                target_ext = upgrade_map.get(src_ext)
                if not target_ext:
                    with lock:
                        stats["kept"] += 1
                    return

                stem_base = PurePosixPath(zip_sti).stem
                orig_suffix = PurePosixPath(zip_sti).suffix.lstrip(".").lower()
                final_stem = f"{stem_base}.{src_ext}"   # alltid dobbel endelse

                work_dir = tmp_base / f"upg_{idx}_{stem_base}"
                work_dir.mkdir(parents=True, exist_ok=True)

                fra_sz = blob_path.stat().st_size

                result_file = _lo_convert(blob_path, target_ext, work_dir)

                if result_file:
                    new_name = f"{final_stem}.{target_ext}"
                    new_path = blob_path.parent / new_name
                    try:
                        shutil.move(str(result_file), str(new_path))
                        blob_path.unlink(missing_ok=True)
                        til_sz = new_path.stat().st_size if new_path.exists() else 0
                        with lock:
                            stats["converted"] += 1
                            n_ok_ref[0] += 1
                        w(f"  Oppgradert: {blob_path.name} → {new_name}", "ok")
                        if csv_log:
                            csv_log.write(
                                zip_sti, fra_sz, src_ext,
                                new_name, til_sz, target_ext,
                                f"Oppgradert {src_ext.upper()}→{target_ext.upper()}")
                    except Exception as exc:
                        w(f"  Flytt feilet {blob_path.name}: {exc}", "warn")
                        with lock:
                            stats["kept"] += 1
                            n_fail_ref[0] += 1
                else:
                    # Behold original med korrekt endelse
                    self._rename_file(extract_dir, zip_sti, src_ext)
                    with lock:
                        stats["kept"] += 1
                        n_fail_ref[0] += 1
                    w(f"  Oppgradering feilet: {blob_path.name} — beholder .{src_ext}", "warn")
                    if err_log:
                        err_log.write(zip_sti, src_ext,
                                      f"Oppgradering {src_ext}→{target_ext} feilet")
                    if csv_log:
                        new_name = f"{stem_base}.{src_ext}"
                        csv_log.write(
                            zip_sti, fra_sz, src_ext,
                            new_name, fra_sz, src_ext,
                            f"Oppgradering {src_ext.upper()}→{target_ext.upper()} feilet")

            # Teller-referanser for bruk i closure
            n_ok_ref   = [0]
            n_fail_ref = [0]

            w(f"  Oppgraderer {len(to_upgrade):,} filer ({max_w} parallelle tråder) ...", "info")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as pool:
                futs = [pool.submit(_process_one, item) for item in to_upgrade]
                for fut in concurrent.futures.as_completed(futs):
                    try:
                        fut.result()
                    except Exception as exc:
                        w(f"  [Oppgradering-feil] {exc}", "warn")

            w(f"  Oppgradering ferdig: {n_ok_ref[0]} OK, {n_fail_ref[0]} feil", "info")

        finally:
            shutil.rmtree(str(tmp_base), ignore_errors=True)

    def _process_archives(self,
                          to_archive:     list,
                          to_convert:     list,
                          to_rename_only: list,
                          stats:          dict,
                          extract_dir:    Path,
                          lo_bin:         str,
                          stop_ev:        threading.Event,
                          pause_ev:       threading.Event,
                          csv_log,
                          err_log,
                          w,
                          lock:           threading.Lock,
                          max_workers:    int = 1) -> None:
        """
        Behandler komprimerte blob-filer (zip/gz/bz2/tar/rar/7z):

        For hvert arkiv:
          - Pakk ut alle filer til en arbeidsmappe
          - Identifiser innholdet (ext per fil)
          - Konverter filer i _LO_CONVERTIBLE til PDF/A med LO
          - Pakk innholdet tilbake som .zip HVIS det var mer enn én fil,
            ELLER erstatt blob med den ene utpakkede/konverterte filen
          - Oppdater stats og rename-registrering på disk slik at
            _patch_all_xml finner riktig ny filnavn via stem-oppslag
        """
        import tarfile

        def _lo_convert_file(src: Path, tmp_work: Path) -> Path | None:
            """Konverter én fil med LO til PDF/A. Returnerer ferdig PDF eller None."""
            try:
                import subprocess as _sp
                out_dir = tmp_work / "lo_out"
                out_dir.mkdir(exist_ok=True)
                result = _sp.run(
                    [lo_bin,
                     "--headless", "--norestore",
                     "--convert-to", "pdf:writer_pdf_Export",
                     "--outdir", str(out_dir),
                     str(src)],
                    stdout=_sp.PIPE, stderr=_sp.PIPE,
                    timeout=self.params.get("lo_timeout", 300)
                )
                for f in out_dir.iterdir():
                    if f.suffix.lower() == ".pdf":
                        return f
            except Exception:
                pass
            return None

        def _unpack_archive(path: Path, work_dir: Path) -> list[Path]:
            """Pakk ut arkiv til work_dir. Returnerer liste av utpakkede filer."""
            files: list[Path] = []
            # ZIP
            try:
                with zipfile.ZipFile(path, "r") as zf:
                    members = [m for m in zf.infolist() if not m.filename.endswith("/")]
                    for m in members:
                        zf.extract(m, work_dir)
                        files.append(work_dir / m.filename)
                    return files
            except Exception:
                pass
            # GZ
            try:
                import gzip
                out = work_dir / path.stem
                with gzip.open(path, "rb") as gz:
                    out.write_bytes(gz.read())
                return [out]
            except Exception:
                pass
            # BZ2
            try:
                import bz2
                raw = path.read_bytes()
                for candidate in ([raw] + ([raw[4:]] if len(raw) > 8 and raw[4:6] == b"BZ" else [])):
                    try:
                        data = bz2.decompress(candidate)
                        out  = work_dir / path.stem
                        out.write_bytes(data)
                        return [out]
                    except Exception:
                        continue
            except Exception:
                pass
            # TAR
            try:
                with tarfile.open(path, "r:*") as tf:
                    members = [m for m in tf.getmembers() if m.isfile()]
                    for m in members:
                        tf.extract(m, work_dir, set_attrs=False)
                        files.append(work_dir / m.name)
                    return files
            except Exception:
                pass
            # 7z — krever py7zr
            try:
                import py7zr
                with py7zr.SevenZipFile(str(path), mode="r") as sz:
                    sz.extractall(path=str(work_dir))
                    return [f for f in work_dir.rglob("*") if f.is_file()]
            except Exception:
                pass
            # RAR — krever rarfile
            try:
                import rarfile
                with rarfile.RarFile(str(path)) as rf:
                    members = [m for m in rf.infolist() if not m.is_dir()]
                    for m in members:
                        rf.extract(m.filename, work_dir)
                        files.append(work_dir / m.filename)
                    return files
            except Exception:
                pass
            return []

        n_ok   = [0]
        n_fail = [0]
        tmp_base = Path(tempfile.mkdtemp(prefix="siard_arc_", dir=extract_dir))

        def _process_one_archive(args):
            i, (idx, zip_sti, arc_ext, arc_mime) = args
            if stop_ev.is_set():
                return

            blob_path = extract_dir / zip_sti
            if not blob_path.exists():
                return

            stem     = PurePosixPath(zip_sti).stem
            parent   = PurePosixPath(zip_sti).parent
            work_dir = tmp_base / f"arc_{i}_{stem}"
            work_dir.mkdir(parents=True, exist_ok=True)
            lo_work  = work_dir / "lo_tmp"
            lo_work.mkdir(exist_ok=True)

            w(f"  Arkiv [{i+1}/{len(to_archive)}]: {PurePosixPath(zip_sti).name}"
              f" ({arc_ext.upper()})", "info")

            # Pakk ut
            inner_files = _unpack_archive(blob_path, work_dir)
            if not inner_files:
                w(f"    Kunne ikke pakke ut — beholdes som {arc_ext}", "warn")
                with lock:
                    to_rename_only.append((idx, zip_sti, arc_ext, arc_mime))
                    n_fail[0] += 1
                return

            w(f"    Pakket ut: {len(inner_files)} fil(er)", "info")

            # Identifiser og konverter innhold
            result_files: list[Path] = []
            for src in inner_files:
                if not src.exists() or not src.is_file():
                    continue
                try:
                    data = src.read_bytes()[:65536]
                except Exception:
                    result_files.append(src)
                    continue

                inner_ext, _, inner_enc = _detect(data)

                if not inner_enc and inner_ext in _LO_CONVERTIBLE:
                    w(f"    Konverterer: {src.name} ({inner_ext.upper()} → PDF/A)", "info")
                    pdf = _lo_convert_file(src, lo_work)
                    if pdf:
                        result_files.append(pdf)
                        with lock:
                            stats["converted"] += 1
                    else:
                        w(f"    LO-konvertering feilet: {src.name} — beholdes", "warn")
                        result_files.append(src)
                        with lock:
                            stats["failed"] += 1
                else:
                    result_files.append(src)

            if not result_files:
                with lock:
                    to_rename_only.append((idx, zip_sti, arc_ext, arc_mime))
                    n_fail[0] += 1
                return

            # Én fil: erstatt blob direkte
            if len(result_files) == 1:
                single = result_files[0]
                single_ext, _, _ = _detect(single.read_bytes()[:65536])
                new_name = f"{stem}.{single_ext}"
                new_path = blob_path.parent / new_name
                try:
                    shutil.copy2(str(single), str(new_path))
                    if new_path != blob_path:
                        blob_path.unlink(missing_ok=True)
                    w(f"    Enkeltfil → {new_name}", "ok")
                    with lock:
                        stats["kept"] += 1
                        n_ok[0] += 1
                except Exception as exc:
                    w(f"    Kopi-feil: {exc}", "warn")
                    with lock:
                        to_rename_only.append((idx, zip_sti, arc_ext, arc_mime))
                        n_fail[0] += 1

            else:
                # Flere filer: pakk som ny .zip
                new_name = f"{stem}.zip"
                new_path = blob_path.parent / new_name
                try:
                    with zipfile.ZipFile(new_path, "w",
                                         compression=zipfile.ZIP_DEFLATED) as zout:
                        for rf in result_files:
                            zout.write(rf, rf.name)
                    if new_path != blob_path:
                        blob_path.unlink(missing_ok=True)
                    w(f"    {len(result_files)} filer → {new_name}", "ok")
                    with lock:
                        stats["kept"] += 1
                        n_ok[0] += 1
                except Exception as exc:
                    w(f"    ZIP-pakking feilet: {exc} — beholder original", "warn")
                    with lock:
                        to_rename_only.append((idx, zip_sti, arc_ext, arc_mime))
                        n_fail[0] += 1

        try:
            max_arc_w = max(1, min(max_workers, len(to_archive), os.cpu_count() or 2))
            w(f"  Arkiver: {len(to_archive)} fordelt på {max_arc_w} worker(e) ...", "info")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_arc_w) as arc_pool:
                list(arc_pool.map(_process_one_archive, enumerate(to_archive)))
        finally:
            shutil.rmtree(str(tmp_base), ignore_errors=True)

        w(f"  Arkiver ferdig: {n_ok[0]} OK, {n_fail[0]} feil", "info")

    def _spor_ukjent_format(self, ext: str, antall: int,
                             eksempler: str, mime: str) -> str:
        """
        Spør bruker via GUI-dialog hva som skal gjøres med ukjent filformat.
        Returnerer: 'konverter', 'behold', eller 'hopp_over'.
        Kjøres i hoved-tråden via tkinter-kall.
        """
        try:
            import tkinter as tk
            from tkinter import messagebox

            tekst = (
                f"Ukjent filformat funnet: .{ext.upper()}\n"
                f"MIME: {mime or 'ukjent'}\n"
                f"Antall filer: {antall}\n"
                f"Eksempler: {eksempler}\n\n"
                f"Hva skal gjøres med .{ext.upper()}-filer?\n\n"
                f"  [Ja]   → Forsøk konvertering til PDF/A\n"
                f"          (legges til i lo_convertible)\n\n"
                f"  [Nei]  → Behold i originalformat\n"
                f"          (legges til i rename_only)\n\n"
                f"  [Avbryt] → Hopp over (ignoreres denne kjøringen)"
            )
            root = tk._default_root
            if root is None:
                # Ingen GUI tilgjengelig — behold som standard
                return "behold"

            # Bruk after + wait_variable for å kjøre i GUI-tråden
            result = tk.StringVar(value="")

            def _vis():
                svar = messagebox.askyesnocancel(
                    f"Ukjent format: .{ext.upper()}",
                    tekst,
                    icon="question",
                    parent=root,
                )
                if svar is True:
                    result.set("konverter")
                elif svar is False:
                    result.set("behold")
                else:
                    result.set("hopp_over")

            root.after(0, _vis)
            # Vent til svar er satt (blokkerer denne tråden, ikke GUI-tråden)
            timeout = 300  # sekunder
            import time as _time
            deadline = _time.monotonic() + timeout
            while result.get() == "" and _time.monotonic() < deadline:
                _time.sleep(0.1)

            return result.get() or "behold"
        except Exception:
            return "behold"

    def _rename_file(self, extract_dir: Path, zip_sti: str,
                     ext: str) -> str:
        """Rename fil i extract_dir fra .bin/.txt til riktig ext. Returnerer ny zip_sti."""
        old_path = extract_dir / zip_sti
        stem     = PurePosixPath(zip_sti).stem
        new_sti  = str(PurePosixPath(zip_sti).parent / f"{stem}.{ext}")
        new_path = extract_dir / new_sti
        if old_path.exists() and new_sti != zip_sti:
            try:
                old_path.rename(new_path)
            except Exception:
                pass
        return new_sti

    # ── Inline NBLOB/NCLOB ───────────────────────────────────────────────────

    def _extract_inline(self, xml_bytes: bytes, xml_sti: str,
                        table_key: str, stats: dict, w,
                        lob_cols: dict | None = None,
                        ) -> tuple[bytes, dict[str, bytes], int]:
        """
        Ekstraher inline LOB-innhold fra tableX.xml til eksterne filer.

        To strategier kombineres:
        A) _INLINE_TAGS (SIARD 1.0): <nclob>, <blob> etc. med tekstinnhold
        B) lob_cols (SIARD 2.x): <cN>-elementer der kolonnen er NCLOB/BLOB
           i metadata.xml. Nøkkelen er col_idx (1-basert) fra lob_cols.

        Støttede encodings i tekst-noder:
          - \\uXXXX-escaped tekst (SIARD unicode-escaping av RTF etc.)
          - Hex-streng (alle tegn er 0-9a-f)
          - Base64
          - Direkte bytes (UTF-8 eller latin-1)
        """
        try:
            tree = ET.parse(io.BytesIO(xml_bytes))
            root = tree.getroot()
        except ET.ParseError as e:
            w(f"    XML-parsefeil {xml_sti}: {e}", "feil")
            return xml_bytes, {}, 0

        # Bygg set av LOB-kolonneindekser for denne tabellen (1-basert)
        # table_key kan være "schema0/table58" eller bare "table58"
        lob_col_map: dict[int, str] = {}
        if lob_cols:
            lob_col_map = lob_cols.get(table_key, {})
            # Prøv også bare tabellnavn uten schema-prefix
            if not lob_col_map:
                short_key = table_key.split("/")[-1] if "/" in table_key else table_key
                for k, v in lob_cols.items():
                    if k.endswith(f"/{short_key}") or k == short_key:
                        lob_col_map = v
                        break

        new_files:  dict[str, bytes] = {}
        lob_counter = 0
        n_extracted = 0
        base_path   = str(PurePosixPath(xml_sti).parent)

        for elem in root.iter():
            tag_local = _local(elem.tag).lower()

            # Bestem om dette elementet er et LOB-felt
            is_inline_tag = tag_local in _INLINE_TAGS
            is_lob_col    = False
            lob_folder    = ""
            if not is_inline_tag and lob_col_map:
                # Sjekk om tagnavn er cN der N er i lob_col_map
                if tag_local.startswith("c") and tag_local[1:].isdigit():
                    col_idx = int(tag_local[1:])
                    if col_idx in lob_col_map:
                        is_lob_col = True
                        lob_folder = lob_col_map[col_idx]

            if not is_inline_tag and not is_lob_col:
                continue

            # Hopp over hvis allerede ekstern referanse
            if elem.get("file") or elem.get("fileName") or elem.get("href"):
                continue

            text = (elem.text or "").strip()
            if not text:
                continue

            # ── Dekod innhold ──────────────────────────────────────────────
            file_bytes: bytes
            detected_as = "tekst"

            # 1. SIARD \uXXXX-escaped (vanligst for RTF/tekst i NCLOB)
            if r"\u" in text and not text.startswith("\\x"):
                file_bytes  = _unescape_siard(text)
                detected_as = "unicode-escaped"
            # 2. Hex-kodet binærdata
            elif _is_hex(text):
                try:
                    file_bytes  = _hex_decode(text)
                    detected_as = "hex"
                except Exception:
                    file_bytes = text.encode("utf-8")
            # 3. Base64
            else:
                try:
                    import base64 as _b64
                    file_bytes  = _b64.b64decode(text, validate=True)
                    detected_as = "base64"
                except Exception:
                    file_bytes = text.encode("utf-8")

            # ── Detekter filtype ───────────────────────────────────────────
            ext, mime, _ = _detect(file_bytes)

            # ── Bestem lob-mappe og filnavn ────────────────────────────────
            lob_counter += 1
            if lob_folder:
                # Bruk lobFolder fra metadata som rotmappe
                lob_dir  = lob_folder
                filename = f"rec{lob_counter}.{ext}"
            else:
                lob_dir  = f"{base_path}/lob{lob_counter}"
                filename = f"LOB{lob_counter:04d}.{ext}"

            zip_sti_lob = f"{lob_dir}/{filename}"

            new_files[zip_sti_lob] = file_bytes
            elem.text = None
            elem.set("file", filename)

            # Oppdater søsken-elementer (mimeType, length, checksum)
            parent = self._find_parent(root, elem)
            if parent is not None:
                self._update_sibling(parent, "mimeType",    mime,                 w)
                self._update_sibling(parent, "length",      str(len(file_bytes)), w)
                for cs_tag in _CHECKSUM_TAGS:
                    node = self._find_sibling_tag(parent, cs_tag)
                    if node is not None:
                        node.text = _checksum(file_bytes, cs_tag)

            n_extracted += 1
            stats["inline_extracted"] += 1
            w(f"    {xml_sti} <{tag_local}>: {detected_as} → {ext} "
              f"({len(file_bytes):,} bytes) → {zip_sti_lob}", "info")

        if n_extracted == 0:
            return xml_bytes, {}, 0

        out = io.BytesIO()
        tree.write(out, xml_declaration=True, encoding="utf-8")
        result = _restore_xml_header(xml_bytes, out.getvalue())
        return result, new_files, n_extracted

    # ── Patch tableX.xml ─────────────────────────────────────────────────────

    def _patch_all_xml(self, table_xml_map: dict, table_blobs: dict,
                       extract_dir: Path,
                       inline_new: dict, xml_pre: dict,
                       stats: dict, w, progress,
                       lob_type_map: dict | None = None,
                       col_meta: dict | None = None) -> None:
        """
        Patch alle tableX.xml parallelt.
        lob_type_map: {table_key: {lob_col: [type_cols]}} fra XML-analyse.
        col_meta:     {table_key: {mime_cols, digest_cols, ...}} fra metadata.xml.
        """
        n_xml_total = len(table_xml_map)
        if n_xml_total == 0:
            return

        lock        = threading.Lock()
        n_xml_done  = 0
        xml_updated = 0
        max_w       = max(1, min(self.params["max_workers"], os.cpu_count() or 2))
        _lob_type_map = lob_type_map or {}
        _col_meta     = col_meta or {}

        sorted_items = sorted(
            table_xml_map.items(),
            key=lambda kv: len(table_blobs.get(kv[0], [])),
            reverse=True)

        w(f"  Patcher {n_xml_total} tableX.xml ({max_w} parallelle tråder) ...", "info")

        def _patch_one(args):
            nonlocal n_xml_done, xml_updated
            table_key, xml_sti = args

            old_blobs = table_blobs.get(table_key, [])
            renames: dict[str, Path] = {}

            if old_blobs:
                from collections import defaultdict
                by_parent: dict[Path, list[tuple[str, str]]] = defaultdict(list)
                for zip_sti in old_blobs:
                    p   = PurePosixPath(zip_sti)
                    pdir = extract_dir / str(p.parent)
                    by_parent[pdir].append((p.stem, p.name))

                for parent_dir, stem_names in by_parent.items():
                    if not parent_dir.exists():
                        continue
                    # Bygg oppslag: rot-stamme (før første punktum) → filsti
                    # Dette håndterer både:
                    #   record001.pdf       → rot-stamme "record001"
                    #   record001.doc.pdf   → rot-stamme "record001"
                    #   record001.docx      → rot-stamme "record001"
                    stem_to_file: dict[str, Path] = {}
                    for f in parent_dir.iterdir():
                        if f.suffix.lower() in (".bin", ".txt"):
                            continue
                        # Rot-stamme: ta hele navnet og fjern alle endelser
                        # slik at "record001.doc.pdf" → "record001"
                        rot = f.name.split(".")[0]
                        # Foretrekk filer med kortere navn (mer spesifikk fil
                        # ikke allerede registrert, eller erstatt .bin/.txt-rester)
                        if rot not in stem_to_file:
                            stem_to_file[rot] = f
                        else:
                            # Foretrekk PDF over andre formater
                            existing = stem_to_file[rot]
                            if f.suffix.lower() == ".pdf" and existing.suffix.lower() != ".pdf":
                                stem_to_file[rot] = f

                    for stem, orig_name in stem_names:
                        if stem in stem_to_file:
                            renames[orig_name] = stem_to_file[stem]
                        else:
                            orig = parent_dir / orig_name
                            if orig.exists():
                                renames[orig_name] = orig

            # Finn type-kolonne-kobling for denne tabellen
            col_map  = _lob_type_map.get(table_key, {})
            tbl_meta = _col_meta.get(table_key, {})
            mime_cols    = tbl_meta.get("mime_cols",       [])
            digest_cols  = tbl_meta.get("digest_cols",     [])
            digesttype_cols = tbl_meta.get("digesttype_cols", [])

            if not renames and xml_sti not in xml_pre:
                with lock:
                    n_xml_done += 1
                    progress("phase_progress", done=n_xml_done, total=n_xml_total)
                return

            xml_file_path = extract_dir / xml_sti
            sz_mb = xml_file_path.stat().st_size // 1024 // 1024 if xml_file_path.exists() else 0
            extras = []
            if col_map:    extras.append(f"type-kol: {col_map}")
            if mime_cols:  extras.append(f"mime-kol: c{mime_cols}")
            if digest_cols: extras.append(f"digest-kol: c{digest_cols}")
            w(f"  [{n_xml_done+1}/{n_xml_total}] {xml_sti} "
              f"({sz_mb} MB, {len(renames):,} renames"
              + (", " + ", ".join(extras) if extras else "") + ")", "info")

            # Les XML
            if xml_sti in xml_pre:
                xml_bytes = xml_pre[xml_sti]
            else:
                try:
                    xml_bytes = xml_file_path.read_bytes()
                except Exception as exc:
                    w(f"  FEIL les {xml_sti}: {exc}", "feil")
                    with lock:
                        n_xml_done += 1
                        progress("phase_progress", done=n_xml_done, total=n_xml_total)
                    return

            # Patch
            SIZE_THRESHOLD = 50 * 1024 * 1024

            if xml_sti not in xml_pre and xml_file_path.stat().st_size >= SIZE_THRESHOLD:
                n = self._patch_xml_file_inplace(xml_file_path, renames, w,
                                                  col_map=col_map,
                                                  mime_col_idxs=mime_cols,
                                                  digest_col_idxs=digest_cols,
                                                  lob_col_idxs=list(tbl_meta.get("lob_cols", {}).keys()))
                with lock:
                    xml_updated += n
                    n_xml_done  += 1
                    progress("phase_progress", done=n_xml_done, total=n_xml_total)
                return

            # Liten/middels fil eller pre-patchet: les inn, patch, skriv tilbake
            patched, n = self._patch_xml_bytes(xml_bytes, xml_sti, renames,
                                               extract_dir, w)

            # Skriv tilbake
            if n > 0 or xml_sti in xml_pre:
                xml_out = extract_dir / xml_sti
                try:
                    xml_out.write_bytes(patched)
                except Exception as exc:
                    w(f"  FEIL skriv {xml_sti}: {exc}", "feil")
                    n = 0

            w(f"    → {xml_sti}: {n:,} oppdateringer", "ok")
            with lock:
                xml_updated += n
                n_xml_done  += 1
                progress("phase_progress", done=n_xml_done, total=n_xml_total)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as pool:
            list(pool.map(_patch_one, sorted_items))

        with lock:
            stats["xml_updated"] += xml_updated

        w(f"  XML ferdig: {xml_updated} referanser oppdatert", "info")

    def _patch_xml_bytes(self, xml_bytes: bytes, xml_sti: str,
                          renames: dict[str, Path],
                          extract_dir: Path, w) -> tuple[bytes, int]:
        """
        Patch tableX.xml for omdøpte blob-filer.

        Bruker bytes-streaming for begge størrelser — bevarer kommentarer,
        BOM, CRLF og original namespace/XML-deklarasjon intakt.
        mimeType/length/checksum oppdateres kun for filer < 50 MB via ET,
        men skrives tilbake med bevart header.
        """
        SIZE_THRESHOLD = 50 * 1024 * 1024

        if len(xml_bytes) >= SIZE_THRESHOLD:
            return self._patch_xml_streaming(xml_bytes, xml_sti, renames, w)

        # Bytes-streaming for rename-referanser (bevarer alt originalt innhold)
        patched, updates = self._patch_xml_streaming(xml_bytes, xml_sti, renames, w)

        if updates == 0:
            return xml_bytes, 0

        return patched, updates

    def _parent_file_match_fast(
            self,
            parent_map: dict,
            elem,
            renames: dict[str, Path]) -> "Path | None":
        """Slår opp parent via ferdigbygd parent-map — O(1) i stedet for O(n)."""
        parent = parent_map.get(elem)
        if parent is None:
            return None
        for child in parent:
            for attr in ("file", "fileName", "href"):
                val = child.get(attr)
                if val:
                    if val in renames:
                        return renames[val]
                    for old, new_path in renames.items():
                        if new_path.name == val:
                            return new_path
            if child.text and child.text.strip() in renames:
                return renames[child.text.strip()]
        return None

    def _patch_xml_streaming(
            self,
            xml_bytes: bytes,
            xml_sti: str,
            renames: dict[str, Path],
            w) -> tuple[bytes, int]:
        """
        Linje-for-linje patch av XML-bytes.
        Oppdaterer file=, length= og digest= i filreferanse-noder.
        """
        if not renames:
            return xml_bytes, 0

        ren_bytes = {k.encode("utf-8"): v.name.encode("utf-8")
                     for k, v in renames.items()
                     if k != v.name}
        ren_paths = {k.encode("utf-8"): v
                     for k, v in renames.items()
                     if k != v.name}
        if not ren_bytes:
            return xml_bytes, 0

        updates = 0
        out_buf = io.BytesIO()

        for line in io.BytesIO(xml_bytes):
            if b'file' in line or b'href' in line:
                new_line, n = _patch_line_with_digest(line, ren_bytes, ren_paths)
                if n > 0:
                    line = new_line
                    updates += n
            out_buf.write(line)

        if updates == 0:
            return xml_bytes, 0

        result = _inject_conversion_comment(out_buf.getvalue())
        w(f"    Streaming patch ({len(xml_bytes)//1024//1024} MB): "
          f"{updates} oppdateringer", "ok")
        return result, updates

    def _patch_xml_file_inplace(
            self,
            xml_path: Path,
            renames: dict[str, Path],
            w,
            **kwargs) -> int:
        """
        Patch stor tableX.xml direkte på disk.
        Oppdaterer i filreferanse-noden:
          - file="recN.bin"   → file="recN.pdf"
          - length="GAMMEL"   → length="NY" (fra ny PDF-fil)
          - digest="GAMMEL"   → digest="NY MD5" (beregnet fra ny PDF-fil)
        Linje-for-linje, ingen full fil i RAM. O(1) dict-oppslag per linje.
        """
        import hashlib as _hashlib

        ren_bytes: dict[bytes, bytes] = {
            k.encode("utf-8"): v.name.encode("utf-8")
            for k, v in renames.items()
            if k != v.name
        }
        ren_paths: dict[bytes, Path] = {
            k.encode("utf-8"): v
            for k, v in renames.items()
            if k != v.name
        }
        if not ren_bytes:
            return 0

        tmp_path    = xml_path.with_suffix(".tmp_patch")
        updates     = 0
        line_no     = 0
        file_sz     = xml_path.stat().st_size
        total_lines = _count_lines(xml_path)
        REPORT      = max(1, total_lines // 10)

        try:
            comment_written = False
            comment_bytes   = _conversion_comment()

            with open(xml_path, "rb") as src, \
                 open(tmp_path, "wb", buffering=256 * 1024) as dst:
                for line in src:
                    line_no += 1

                    if b'file' in line or b'href' in line:
                        new_line, n = _patch_line_with_digest(
                            line, ren_bytes, ren_paths)
                        if n > 0:
                            line = new_line
                            updates += n

                    dst.write(line)

                    if not comment_written and b'-->' in line:
                        comment_written = True
                        dst.write(comment_bytes)

                    if line_no % REPORT == 0 or line_no == total_lines:
                        pct = src.tell() / file_sz * 100 if file_sz else 0
                        msg = (f"    {xml_path.name}: "
                               f"{line_no:,}/{total_lines:,} linjer "
                               f"({pct:.0f}%), {updates:,} oppdateringer")
                        w(msg, "info")

            tmp_path.replace(xml_path)
            sz_mb    = xml_path.stat().st_size // 1024 // 1024
            done_msg = (f"    Ferdig: {xml_path.name} ({sz_mb} MB), "
                        f"{line_no:,} linjer, {updates:,} oppdateringer")
            w(done_msg, "ok")

        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            w(f"    FEIL: {xml_path.name}: {exc}", "feil")
            return 0

        return updates

    # ── Pakk ny ZIP ──────────────────────────────────────────────────────────

    def _pack_new_zip(self, extract_dir: Path, orig_namelist: list[str],
                      inline_new: dict, corrupt_files: set[str],
                      dst_path: Path, target_version: str,
                      w, progress,
                      src_version: str = "") -> None:
        w(f"  Pakker: {dst_path.name}", "step")
        # Samle alle filer fra extract_dir
        all_files = list(extract_dir.rglob("*"))
        n_written = 0
        n_skipped = 0
        n_transformed = 0

        # Finn den faktiske versjonen i siardversion-mappa fra ZIP-listen.
        # Bruker mapper-stien direkte (f.eks. header/siardversion/2.2/) i stedet
        # for å stole på detect_siard_version fra XML-innholdet, som kan ha
        # blitt transformert av et tidligere steg til generisk /2/-namespace.
        import re as _re
        _folder_version = src_version   # fallback til XML-detektert versjon
        for _n in orig_namelist:
            _fm = _re.match(r'header/siardversion/(\d+\.\d+)/', _n,
                            _re.IGNORECASE)
            if _fm:
                _folder_version = _fm.group(1)
                break

        # Hjelpefunksjon: erstatt versjon i siardversion-mappa i header-stier.
        def _ver_path(name: str) -> str:
            if _folder_version and _folder_version != target_version \
                    and _folder_version in name \
                    and name.startswith("header/"):
                return name.replace(_folder_version, target_version)
            return name

        # Katalogoppføringer fra original ZIP (f.eks. header/siardversion/).
        # Disse er påkrevd for korrekt SIARD-validering, men filtreres bort
        # av is_file()-sjekken nedenfor. Gjenopprettes eksplisitt fra orig_namelist.
        orig_dir_entries = sorted(n for n in orig_namelist if n.endswith("/"))

        def _unique_arc_name(name: str, seen: set[str]) -> str:
            """Returnerer name uendret, eller name med løpenummer hvis duplikat."""
            if name not in seen:
                return name
            # Skill ut katalogdel, stamme og endelse
            slash = name.rfind("/")
            prefix = name[:slash + 1] if slash >= 0 else ""
            tail   = name[slash + 1:]
            dot    = tail.rfind(".")
            if dot >= 0:
                base, ext_part = tail[:dot], tail[dot:]
            else:
                base, ext_part = tail, ""
            counter = 2
            while True:
                candidate = f"{prefix}{base}_{counter}{ext_part}"
                if candidate not in seen:
                    w(f"    Duplikat i ZIP: {name!r} → {candidate!r}", "warn")
                    return candidate
                counter += 1

        written_names: set[str] = set()

        with zipfile.ZipFile(dst_path, "w", zipfile.ZIP_DEFLATED,
                             allowZip64=True) as zf:
            # 1. Skriv katalogoppføringer (tomme mapper) fra original ZIP,
            #    med versjonstreng transformert i header-stier.
            for dir_entry in orig_dir_entries:
                dir_entry_out = _ver_path(dir_entry)
                dir_entry_out = _unique_arc_name(dir_entry_out, written_names)
                written_names.add(dir_entry_out)
                dir_info = zipfile.ZipInfo(dir_entry_out)
                zf.writestr(dir_info, b"")
                n_written += 1
            if orig_dir_entries:
                w(f"  Kataloger: {len(orig_dir_entries)} oppføringer "
                  f"({', '.join(e for e in orig_dir_entries[:4])}"
                  f"{'…' if len(orig_dir_entries) > 4 else ''})", "info")

            # 2. Skriv alle filer fra extract_dir
            for file_path in sorted(all_files):
                if not file_path.is_file():
                    continue
                arc_name = str(file_path.relative_to(extract_dir)).replace("\\", "/")
                arc_name = _ver_path(arc_name)
                arc_name = _unique_arc_name(arc_name, written_names)
                written_names.add(arc_name)
                try:
                    if is_siard_xml(arc_name):
                        data = file_path.read_bytes()
                        if arc_name.lower().endswith("header/metadata.xml"):
                            data = sanitize_metadata_schema_names(data)
                        data = siard_version_transform(data, target_version)
                        zf.writestr(arc_name, data)
                        n_transformed += 1
                    else:
                        zf.write(file_path, arc_name)
                    n_written += 1
                except Exception as exc:
                    w(f"    FEIL skriv {arc_name}: {exc}", "feil")
                    progress("error", file=arc_name, error=str(exc))
                    n_skipped += 1
        if n_transformed:
            w(f"  SIARD-versjon: {n_transformed} XML-filer transformert "
              f"til versjon {target_version}", "info")

        sz = dst_path.stat().st_size
        w(f"  Ferdig: {n_written:,} filer  {sz:,} bytes", "ok")
        if n_skipped:
            w(f"  OBS: {n_skipped} filer hoppet over", "warn")

    # ── XML-hjelpere ─────────────────────────────────────────────────────────

    def _find_parent(self, root: ET.Element, child: ET.Element) -> ET.Element | None:
        for p in root.iter():
            if child in list(p):
                return p
        return None

    def _find_sibling_tag(self, parent: ET.Element, tag: str) -> ET.Element | None:
        for child in parent:
            if _local(child.tag).lower() == tag.lower():
                return child
        return None

    def _update_sibling(self, parent: ET.Element, tag: str, value: str, w) -> None:
        node = self._find_sibling_tag(parent, tag)
        if node is not None and node.text != value:
            old = node.text or ""
            node.text = value
            w(f"      {tag}: {old} -> {value}", "ok")

    def _parent_file_match(self, root: ET.Element, elem: ET.Element,
                            renames: dict[str, Path]) -> Path | None:
        parent = self._find_parent(root, elem)
        if parent is None:
            return None
        for child in parent:
            for attr in ("file", "fileName", "href"):
                val = child.get(attr)
                if val:
                    # Sjekk gammelt navn
                    if val in renames:
                        return renames[val]
                    # Sjekk nytt navn
                    for old, new_path in renames.items():
                        if new_path.name == val:
                            return new_path
            if child.text and child.text.strip() in renames:
                return renames[child.text.strip()]
        return None
