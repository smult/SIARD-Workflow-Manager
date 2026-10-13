"""siard_workflow/core/file_integrity.py

Enkel strukturkontroll av kontorfiler FØR de sendes til LibreOffice.

Bakgrunn (verifisert 2026-10-05, LibreOffice 26.2.6): en skadet fil som
gjenkjennes som f.eks. Word (OLE2-signatur) avvises ikke av LibreOffice — den
importeres som ren tekst, og resultatet er en «vellykket» PDF/A med flere sider
binært søppel. Slike filer ble talt som konvertert. Her sjekkes at filen har
den grunnstrukturen formatet krever; mangler den, regnes filen som skadet og
beholdes uendret i stedet.

  check_structure(path, ext) -> None (ser riktig ut) | årsak (str)

Kontrollen er bevisst grov og rask (ingen full parsing): OLE2-katalogen må
kunne leses og inneholde formatets hovedstrøm; OOXML/ODF-pakker må ha gyldig
zip-katalog og formatets hoveddel.
"""
from __future__ import annotations

import re
import zipfile
from pathlib import Path

_OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

# format → (beskrivelse, påkrevde OLE2-strømmer (én av dem))
_OLE2_FORMATS = {
    "doc": ("Word 97–2003", ("worddocument",)),
    "dot": ("Word-mal 97–2003", ("worddocument",)),
    "xls": ("Excel 97–2003", ("workbook", "book")),
    "xlt": ("Excel-mal 97–2003", ("workbook", "book")),
    "ppt": ("PowerPoint 97–2003", ("powerpoint document",)),
    "pps": ("PowerPoint 97–2003", ("powerpoint document",)),
    "pot": ("PowerPoint-mal 97–2003", ("powerpoint document",)),
}
# format → (beskrivelse, påkrevd del (prefiks) i zip-pakken)
_ZIP_FORMATS = {
    "docx": ("Word (OOXML)", "word/"), "dotx": ("Word-mal (OOXML)", "word/"),
    "docm": ("Word (OOXML)", "word/"),
    "xlsx": ("Excel (OOXML)", "xl/"), "xltx": ("Excel-mal (OOXML)", "xl/"),
    "xlsm": ("Excel (OOXML)", "xl/"),
    "pptx": ("PowerPoint (OOXML)", "ppt/"), "ppsx": ("PowerPoint (OOXML)", "ppt/"),
    "potx": ("PowerPoint-mal (OOXML)", "ppt/"),
    "odt": ("OpenDocument tekst", "content.xml"), "ott": ("OpenDocument-mal", "content.xml"),
    "ods": ("OpenDocument regneark", "content.xml"),
    "odp": ("OpenDocument presentasjon", "content.xml"),
    "odg": ("OpenDocument tegning", "content.xml"),
}

CHECKED_FORMATS = frozenset(_OLE2_FORMATS) | frozenset(_ZIP_FORMATS)

# ── Signerte PDF-er ───────────────────────────────────────────────────────────
# Regel (KDRS, 2026-10-12): en signert PDF skal ALDRI røres av konvertering —
# enhver omskriving (PDF/A, Ghostscript, sammenslåing) gjør signaturen ugyldig.
#
# En PDF-signatur har en signaturordbok med /ByteRange [a b c d] som angir
# hvilke byte som er signert. Ordboken kan ikke ligge i en komprimert
# objektstrøm (signaturverdien må kunne adresseres i filen), så et søk etter
# /ByteRange i rådataene finner signaturer uten å tolke PDF-en — også i
# krypterte filer, sertifiseringssignaturer (DocMDP) og tidsstempler.
_BYTERANGE = re.compile(rb"/ByteRange\s*\[\s*\d+\s+\d+\s+\d+\s+\d+\s*\]")


def is_signed_pdf(source: "Path | bytes") -> bool:
    """True hvis PDF-en (fil eller bytes) har minst én digital signatur."""
    try:
        if isinstance(source, (bytes, bytearray, memoryview)):
            data = bytes(source)
        else:
            data = Path(source).read_bytes()
    except OSError:
        return False
    head = data[:1024]
    if b"%PDF" not in head:
        return False
    return _BYTERANGE.search(data) is not None


_CLSID_EXCEL = "00020820-0000-0000-C000-000000000046"
_CLSID_PPT = "64818D10-4F9B-11CF-86EA-00AA00B929E8"


def ole2_office_type(path: Path) -> "tuple[str, str, bool] | None":
    """
    (ext, mime, kryptert) for en OLE2-fil, ut fra HELE katalogen (olefile).

    Identifiseringen ser normalt bare de første 64 kB. Ligger OLE2-katalogen
    lenger ut i filen (vanlig for Word-filer på 70 kB+), ser den ingen
    strømnavn og faller tilbake til tekstsøk — som kan treffe ord i selve
    dokumentteksten (f.eks. «Impress» → PowerPoint). Dette er den sikre
    avgjørelsen. None når filen ikke er OLE2 eller typen er ukjent.
    """
    try:
        with open(path, "rb") as f:
            if f.read(8) != _OLE2_MAGIC:
                return None
        import olefile
        with olefile.OleFileIO(str(path)) as ole:
            top = {e[0] for e in ole.listdir(streams=True, storages=True)}
            clsid = (ole.root.clsid or "").upper()
    except Exception:
        return None
    if "__properties_version1.0" in top or any(n.startswith("__substg1.0_") for n in top):
        return "msg", "application/vnd.ms-outlook", False
    encrypted = "EncryptionInfo" in top
    if encrypted and "EncryptedPackage" in top:
        if clsid == _CLSID_EXCEL:
            return ("xlsx", "application/vnd.openxmlformats-officedocument"
                    ".spreadsheetml.sheet", True)
        if clsid == _CLSID_PPT:
            return ("pptx", "application/vnd.openxmlformats-officedocument"
                    ".presentationml.presentation", True)
        return ("docx", "application/vnd.openxmlformats-officedocument"
                ".wordprocessingml.document", True)
    if "WordDocument" in top:
        return "doc", "application/msword", encrypted
    if "Workbook" in top or "Book" in top:
        return "xls", "application/vnd.ms-excel", encrypted
    if "PowerPoint Document" in top:
        return "ppt", "application/vnd.ms-powerpoint", encrypted
    return None


def check_structure(path: Path, ext: str) -> "str | None":
    """None når filen har formatets grunnstruktur, ellers en årsak (norsk)."""
    ext = (ext or "").lower()
    path = Path(path)
    try:
        if ext in _OLE2_FORMATS:
            desc, streams = _OLE2_FORMATS[ext]
            with open(path, "rb") as f:
                if f.read(8) != _OLE2_MAGIC:
                    return f"Mangler OLE2-signatur for {desc} — filen er skadet"
            import olefile
            try:
                with olefile.OleFileIO(str(path)) as ole:
                    names = {"/".join(e).lower() for e in ole.listdir(streams=True,
                                                                         storages=False)}
            except Exception as exc:
                return (f"{desc}-filen er skadet: OLE2-strukturen kan ikke leses "
                        f"({type(exc).__name__}: {str(exc)[:80]})")
            if not any(s in names for s in streams):
                return (f"Ser ut som {desc} (OLE2), men mangler innholdsstrømmen "
                        f"«{streams[0]}» — skadet eller et annet OLE2-format")
            return None
        if ext in _ZIP_FORMATS:
            desc, part = _ZIP_FORMATS[ext]
            try:
                with zipfile.ZipFile(path) as z:
                    names = z.namelist()
            except Exception as exc:
                return (f"{desc}-filen er skadet: pakken (zip) kan ikke leses "
                        f"({type(exc).__name__}: {str(exc)[:80]})")
            if ext.startswith("od"):
                ok = part in names
            else:
                ok = "[Content_Types].xml" in names and any(n.startswith(part) for n in names)
            if not ok:
                return (f"{desc}-filen er skadet eller ufullstendig: mangler «{part}» "
                        f"i pakken")
            return None
    except OSError as exc:
        return f"Filen kunne ikke leses: {exc}"
    return None
