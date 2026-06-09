"""
siard_workflow/core/anonymize/dummy_files.py

Dummy LOB-innhold for anonymisering. Filer i SIARD-arkivet (BLOB/CLOB/eksterne
filer) byttes ut med trygt, ikke-sensitivt placeholder-innhold:

  • Dokumentformater (PDF/DOC/DOCX/ODT/…)  → Lorem Ipsum-PDF
  • RTF                                     → dummy-RTF
  • Ren tekst / XML / CSV                   → Lorem Ipsum-tekst
  • Bilde/lyd/video (når replace_binary_media) → minimal gyldig placeholder

Innholdet er konstant (ikke PII) og holdes minimalt for å unngå korrupte arkiv.
Typen avgjøres med den pluggbare fildeteksjonen (magic bytes / Siegfried), samme
motor som dokumentkonverteringen bruker — slik at en `.bin` som EGENTLIG er et
dokument byttes til PDF, ikke til tekst.
"""
from __future__ import annotations

from pathlib import Path

from .fake_generators import lorem_ipsum

# Resultat-typer (kind) fra pick_dummy_for
KIND_PDF   = "pdf"
KIND_RTF   = "rtf"
KIND_TEXT  = "text"
KIND_IMAGE = "image"
KIND_AV    = "audio_video"
KIND_BIN   = "binary"


# ── Dokument: Lorem Ipsum-PDF ─────────────────────────────────────────────────

# Minimal, gyldig 1-side PDF (fallback hvis reportlab ikke er tilgjengelig).
_FALLBACK_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 595 842]"
    b"/Resources<</Font<</F1 4 0 R>>>>/Contents 5 0 R>>endobj\n"
    b"4 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"5 0 obj<</Length 58>>stream\n"
    b"BT /F1 24 Tf 72 760 Td (Lorem Ipsum - anonymisert) Tj ET\n"
    b"endstream endobj\n"
    b"xref\n0 6\n0000000000 65535 f \n"
    b"trailer<</Root 1 0 R/Size 6>>\n"
    b"startxref\n0\n%%EOF\n"
)

_cached_pdf: "bytes | None" = None


def dummy_pdf() -> bytes:
    """Lorem Ipsum-PDF. Bruker reportlab hvis tilgjengelig, ellers en innebygd
    minimal PDF."""
    global _cached_pdf
    if _cached_pdf is not None:
        return _cached_pdf
    try:
        import io
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4, title="Anonymisert dokument")
        styles = getSampleStyleSheet()
        story = [
            Paragraph("Lorem Ipsum", styles["Heading1"]),
            Spacer(1, 12),
            Paragraph(lorem_ipsum(80), styles["Normal"]),
            Spacer(1, 12),
            Paragraph(lorem_ipsum(80, seed="lorem2"), styles["Normal"]),
        ]
        doc.build(story)
        _cached_pdf = buf.getvalue()
    except Exception:
        _cached_pdf = _FALLBACK_PDF
    return _cached_pdf


# ── RTF ───────────────────────────────────────────────────────────────────────

def dummy_rtf(text: "str | None" = None) -> bytes:
    """Minimal gyldig RTF med Lorem Ipsum."""
    body = (text or lorem_ipsum(60)).replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")
    return (r"{\rtf1\ansi\deff0{\fonttbl{\f0 Helvetica;}}\f0\fs24 "
            + body + r"\par}").encode("latin-1", errors="replace")


# ── Ren tekst ─────────────────────────────────────────────────────────────────

def dummy_text() -> bytes:
    """Lorem Ipsum-tekst som UTF-8."""
    return (lorem_ipsum(60) + "\n").encode("utf-8")


# ── Media-placeholdere (minimale, gyldige) ────────────────────────────────────

# 1x1 transparent PNG
_PNG_1x1 = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000d49444154789c6360000002000100ffff03000006000557bf"
    "abd40000000049454e44ae426082")
# 1x1 GIF
_GIF_1x1 = (b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00"
            b"!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01"
            b"\x00\x00\x02\x02D\x01\x00;")
_IMAGE_BY_EXT = {
    "png": _PNG_1x1, "gif": _GIF_1x1,
    # jpg/jpeg/bmp/tiff o.l. får en gyldig 1x1 PNG som trygt bilde-placeholder
    # (vi beholder original filendelse i SIARD-referansen uansett).
    "jpg": _PNG_1x1, "jpeg": _PNG_1x1,
}


def dummy_image(ext: str = "png") -> bytes:
    """Minimal gyldig bilde-placeholder. Ukjent bildetype → 1x1 PNG."""
    return _IMAGE_BY_EXT.get((ext or "").lower(), _PNG_1x1)


def dummy_binary(ext: str = "bin") -> bytes:
    """Generisk placeholder for binærtyper vi ikke har en gyldig stub for
    (lyd/video/ukjent). Holdes liten og uten sensitivt innhold."""
    return ("ANONYMISERT-PLACEHOLDER (" + (ext or "bin") + ")\n").encode("utf-8")


# ── Klassifisering + valg av dummy ────────────────────────────────────────────

_DOC_EXTS = {"pdf", "doc", "docx", "dot", "dotx", "odt", "ott", "wpd", "wpt",
             "wp", "wps", "sxw", "sdw", "ppt", "pptx", "odp", "xls", "xlsx", "ods"}
_TEXT_EXTS = {"txt", "xml", "csv", "html", "htm", "json", "log", "md", "tsv"}
_IMAGE_EXTS = {"png", "jpg", "jpeg", "gif", "bmp", "tif", "tiff", "webp", "jp2",
               "svg"}
_AV_EXTS = {"mp3", "wav", "flac", "ogg", "aac", "m4a", "mp4", "m4v", "avi",
            "mpg", "mpeg", "mov", "wmv", "mkv", "webm"}


def _identify(head_bytes: bytes) -> "tuple[str, str]":
    """(ext, mime) fra fildeteksjonsmotoren. Robust mot feil → ('bin','')."""
    try:
        from siard_workflow.core.file_identifier import get_identifier
        ext, mime, _enc = get_identifier().identify(data=head_bytes[:65536])
        return (ext or "bin").lower(), (mime or "").lower()
    except Exception:
        return "bin", ""


def pick_dummy_for(blob_path: "str | Path", head_bytes: bytes,
                   *, replace_binary_media: bool = True) -> "tuple[str, bytes]":
    """
    Velg (kind, dummy_bytes) for en LOB-fil basert på faktisk innhold.

    replace_binary_media=False → bilde/lyd/video beholdes (returnerer
    (KIND_BIN, b"") som signal om «ikke bytt»). True → byttes til placeholder.
    """
    ext, mime = _identify(head_bytes)

    # RTF (sjekk før generisk dokument)
    if ext == "rtf" or mime == "application/rtf" or head_bytes[:5] == b"{\\rtf":
        return KIND_RTF, dummy_rtf()

    # Dokumentformater → PDF
    if ext in _DOC_EXTS or mime == "application/pdf" or "officedocument" in mime \
            or "msword" in mime or "opendocument" in mime:
        return KIND_PDF, dummy_pdf()

    # Ren tekst
    if ext in _TEXT_EXTS or mime.startswith("text/"):
        return KIND_TEXT, dummy_text()

    # Bilder
    if ext in _IMAGE_EXTS or mime.startswith("image/"):
        if not replace_binary_media:
            return KIND_BIN, b""
        return KIND_IMAGE, dummy_image(ext)

    # Lyd / video
    if ext in _AV_EXTS or mime.startswith("audio/") or mime.startswith("video/"):
        if not replace_binary_media:
            return KIND_BIN, b""
        return KIND_AV, dummy_binary(ext)

    # Ukjent binær (magic-bytes ga 'bin'): kunne vært et dokument som ville
    # blitt plukket opp i konverteringen → bruk PDF som trygt dokument-placeholder.
    if not replace_binary_media:
        return KIND_BIN, b""
    return KIND_PDF, dummy_pdf()
