"""siard_workflow/core/msg_to_pdf.py

E-post (Outlook .msg og MIME .eml) → én PDF/A-fil: toppside med meldingshode
og vedleggsliste, meldingstekst, og deretter hvert vedlegg konvertert til PDF.

Innholdet leses UTEN LibreOffice: MSG med olefile, EML med Pythons `email`.
LibreOffice brukes bare til toppsiden (HTML → PDF/A) og til Office-vedlegg.
Bilder gjøres om til PDF-sider med PyMuPDF, PDF-vedlegg tas med som de er.
Finnes Ghostscript (valgfritt, se core/ghostscript.py), normaliseres hele
resultatet til PDF/A — da blir også PDF-vedlegg PDF/A. Feiler LibreOffice på
toppsiden, tegnes den med PyMuPDF i stedet (og normaliseres med Ghostscript
hvis tilgjengelig).

Bakgrunn
========
MSG og DOC bruker begge OLE2-containerformatet (samme fire startbytes). Den
gamle magic-byte-deteksjonen kjente bare Word/Excel/PowerPoint og falt
tilbake til «Word» for ukjent OLE2 — alle MSG-filer ble dermed behandlet som
DOC. `is_msg_bytes` / `is_msg_file` gjenkjenner MSG på rotstrømmene
`__properties_version1.0` og `__substg1.0_*`.

Oppbygging av PDF-en
====================
1. Toppside + meldingstekst (HTML → LibreOffice Writer → PDF/A):
   Emne, Fra, Til, Kopi, Blindkopi, Dato, og en vedleggsliste med status per
   vedlegg (konvertert / kun listet / feilet). Meldingsteksten tas fra HTML
   (renset: script/iframe/object og eksterne/cid-bilder fjernes), ellers ren
   tekst, ellers komprimert RTF (LZFu-dekomprimert, konvertert separat).
2. Vedlegg i rekkefølge:
     * PDF → tas med som den er (ikke nødvendigvis PDF/A — logges)
     * Office/ODF/RTF/tekst/HTML → LibreOffice → PDF/A
     * Bilder (jpg/png/gif/bmp/tif) → PyMuPDF, én A4-side per bilde/ramme
     * Vedlagt e-post (MSG/EML) → rekursivt samme behandling
     * Øvrige (zip, exe, ukjent, kryptert) → kun listet på toppsiden
3. Delene slås sammen med PyMuPDF. Toppsiden (LibreOffice-produsert PDF/A)
   er basis, slik at katalogens OutputIntent og XMP-metadata (pdfaid) beholdes.

PDF/A-nivå
==========
PDF/A-2b som standard. Er BLOB-konverteringen satt opp med PDF/A-3, brukes
det nivået og den ORIGINALE MSG-filen legges inn som tilknyttet fil
(AFRelationship /Source). Da er konverteringen tapsfri: alle vedlegg — også
de som bare ble listet (zip, exe …) — finnes i originalen. PDF/A-2 tillater
bare innebygde PDF/A-filer, så i 2b-modus listes ikke-konverterbare vedlegg.
"""

from __future__ import annotations

import datetime as _dt
import html
import io
import re
import shutil
import struct
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
MSG_MIME = "application/vnd.ms-outlook"

_PROPS = "__properties_version1.0"
_U16_PROPS = _PROPS.encode("utf-16-le")
_U16_SUBSTG = "__substg1.0_".encode("utf-16-le")


# ── Gjenkjenning ──────────────────────────────────────────────────────────────

def is_msg_bytes(data: bytes) -> bool:
    """
    True hvis `data` er en Outlook-MSG. Rask sti: OLE2-header + MSG-strømnavn
    i vinduet (katalogsektoren ligger normalt tidlig). Faller tilbake til
    olefile når hele filen er gitt.
    """
    if not data or data[:8] != OLE2_MAGIC:
        return False
    if _U16_PROPS in data[:262144] and _U16_SUBSTG in data[:262144]:
        return True
    try:
        import olefile
        with olefile.OleFileIO(io.BytesIO(data)) as ole:
            return _ole_is_msg(ole)
    except Exception:
        return False


def is_msg_file(path: Path) -> bool:
    """True hvis filen er en Outlook-MSG (leser hele katalogen med olefile)."""
    try:
        with open(path, "rb") as fh:
            head = fh.read(8)
        if head != OLE2_MAGIC:
            return False
        import olefile
        with olefile.OleFileIO(str(path)) as ole:
            return _ole_is_msg(ole)
    except Exception:
        return False


_EML_HEADERS = ("from", "to", "subject", "date", "mime-version", "received",
                "message-id", "return-path", "cc", "reply-to", "delivered-to",
                "x-mailer", "content-type")
_HEADER_LINE = re.compile(rb"^[A-Za-z][A-Za-z0-9\-]{1,60}:[ \t]")


def is_eml_bytes(data: bytes) -> bool:
    """
    True hvis `data` ser ut som en MIME-e-post (.eml): første linje er et
    hode («Navn: verdi») og hodeblokken inneholder minst tre kjente
    e-posthoder, deriblant From eller Received.
    """
    if not data or data[:8] == OLE2_MAGIC:
        return False
    head = data[:16384].lstrip(b"\xef\xbb\xbf")
    first = head.split(b"\n", 1)[0]
    if not _HEADER_LINE.match(first):
        return False
    block = re.split(rb"\r?\n\r?\n", head, maxsplit=1)[0]
    names = {m.group(1).decode("ascii", "ignore").lower()
             for m in re.finditer(rb"^([A-Za-z][A-Za-z0-9\-]{1,60}):", block, re.M)}
    known = names & set(_EML_HEADERS)
    return len(known) >= 3 and bool(names & {"from", "received"})


def parse_eml(data: bytes) -> "MsgMessage":
    """MIME-e-post → MsgMessage (hoder, HTML-/tekstkropp, vedlegg, vedlagte e-poster)."""
    import email
    from email import policy
    msg = email.message_from_bytes(data, policy=policy.default)
    return _from_email_message(msg)


def _from_email_message(msg) -> "MsgMessage":
    from email.utils import getaddresses, parsedate_to_datetime

    def _addrs(field: str) -> list[str]:
        vals = msg.get_all(field, [])
        out = []
        for name, addr in getaddresses([str(v) for v in vals]):
            out.append(f"{name} <{addr}>" if name and addr and name != addr else (name or addr))
        return [a for a in out if a]

    m = MsgMessage()
    m.subject = str(msg.get("subject", "") or "").strip()
    frm = _addrs("from")
    m.sender = frm[0] if frm else ""
    m.to, m.cc, m.bcc = _addrs("to"), _addrs("cc"), _addrs("bcc")
    try:
        m.date = parsedate_to_datetime(str(msg.get("date"))) if msg.get("date") else None
    except Exception:
        m.date = None
    try:
        html_part = msg.get_body(preferencelist=("html",))
        if html_part is not None:
            m.body_html = html_part.get_content()
        text_part = msg.get_body(preferencelist=("plain",))
        if text_part is not None:
            m.body_text = text_part.get_content()
    except Exception:
        pass
    for i, part in enumerate(msg.iter_attachments()):
        name = part.get_filename() or f"vedlegg{i + 1}"
        att = MsgAttachment(name=name, mime=part.get_content_type())
        try:
            if part.get_content_type() == "message/rfc822":
                inner = part.get_content()
                inner = inner[0] if isinstance(inner, list) else inner
                att.embedded = _from_email_message(inner)
                if not att.embedded.subject:
                    att.embedded.subject = name
            else:
                content = part.get_content()
                att.data = content if isinstance(content, bytes) else \
                    str(content).encode(part.get_content_charset() or "utf-8", "replace")
        except Exception:
            att.data = part.get_payload(decode=True) or b""
        m.attachments.append(att)
    return m


def parse_email_file(path: Path) -> "tuple[MsgMessage, str]":
    """(melding, type) for en .msg- eller .eml-fil; type er «msg» eller «eml»."""
    import olefile
    data = Path(path).read_bytes()
    if data[:8] == OLE2_MAGIC:
        with olefile.OleFileIO(io.BytesIO(data)) as ole:
            return parse_msg(ole), "msg"
    if is_eml_bytes(data):
        return parse_eml(data), "eml"
    raise ValueError("verken MSG eller EML")


def _ole_is_msg(ole) -> bool:
    top = {e[0] for e in ole.listdir(streams=True, storages=True) if e}
    return _PROPS in top and any(n.startswith("__substg1.0_") for n in top)


# ── Parsing ───────────────────────────────────────────────────────────────────

@dataclass
class MsgAttachment:
    name: str
    data: bytes = b""
    mime: str = ""
    embedded: "MsgMessage | None" = None      # vedlagt e-post
    hidden: bool = False


@dataclass
class MsgMessage:
    subject: str = ""
    sender: str = ""
    to: list[str] = field(default_factory=list)
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    date: "_dt.datetime | None" = None
    body_text: str = ""
    body_html: str = ""
    body_rtf: bytes = b""
    attachments: list[MsgAttachment] = field(default_factory=list)


def _read_stream(ole, path: list[str]) -> "bytes | None":
    try:
        if ole.exists("/".join(path)):
            return ole.openstream(path).read()
    except Exception:
        pass
    return None


def _prop_str(ole, base: list[str], prop: str) -> str:
    """Strengegenskap: 001F (UTF-16) foretrekkes, ellers 001E (8-bit, cp1252)."""
    raw = _read_stream(ole, base + [f"__substg1.0_{prop}001F"])
    if raw is not None:
        return raw.decode("utf-16-le", errors="replace").rstrip("\x00").strip()
    raw = _read_stream(ole, base + [f"__substg1.0_{prop}001E"])
    if raw is not None:
        return raw.decode("cp1252", errors="replace").rstrip("\x00").strip()
    return ""


def _fixed_props(ole, base: list[str], header_len: int) -> dict[int, bytes]:
    """{property-id: 8-byte verdi} fra __properties_version1.0."""
    raw = _read_stream(ole, base + [_PROPS])
    out: dict[int, bytes] = {}
    if not raw:
        return out
    for off in range(header_len, len(raw) - 15, 16):
        tag = struct.unpack_from("<I", raw, off)[0]
        out[(tag >> 16) & 0xFFFF] = raw[off + 8: off + 16]
    return out


def _filetime(v: bytes) -> "_dt.datetime | None":
    try:
        ft = struct.unpack("<Q", v)[0]
        if not ft:
            return None
        return (_dt.datetime(1601, 1, 1, tzinfo=_dt.timezone.utc)
                + _dt.timedelta(microseconds=ft // 10))
    except Exception:
        return None


def parse_msg(ole, base: "list[str] | None" = None, embedded: bool = False) -> MsgMessage:
    """Les meldingshode, tekst og vedlegg fra en MSG (evt. innebygd)."""
    base = base or []
    m = MsgMessage()
    m.subject = _prop_str(ole, base, "0037")
    name = _prop_str(ole, base, "0C1A") or _prop_str(ole, base, "0042")
    addr = (_prop_str(ole, base, "5D01") or _prop_str(ole, base, "0C1F")
            or _prop_str(ole, base, "0065"))
    m.sender = f"{name} <{addr}>" if name and addr and addr != name else (name or addr)

    props = _fixed_props(ole, base, 24 if embedded else 32)
    for pid in (0x0039, 0x0E06, 0x3007):     # innsendt / levert / opprettet
        if pid in props:
            m.date = _filetime(props[pid])
            if m.date:
                break

    # Mottakere fra __recip_version1.0_#N (med type 1=Til, 2=Kopi, 3=Blindkopi)
    recips: dict[int, list[str]] = {1: [], 2: [], 3: []}
    prefix = base
    for entry in ole.listdir(streams=False, storages=True):
        if len(entry) != len(prefix) + 1 or entry[:len(prefix)] != prefix:
            continue
        if not entry[-1].startswith("__recip_version1.0_"):
            continue
        rb = list(entry)
        rname = _prop_str(ole, rb, "3001")
        raddr = _prop_str(ole, rb, "39FE") or _prop_str(ole, rb, "3003")
        rp = _fixed_props(ole, rb, 8)
        rtype = struct.unpack("<I", rp[0x0C15][:4])[0] if 0x0C15 in rp else 1
        label = f"{rname} <{raddr}>" if rname and raddr and raddr != rname else (rname or raddr)
        if label:
            recips.setdefault(rtype, []).append(label)
    m.to, m.cc, m.bcc = recips.get(1, []), recips.get(2, []), recips.get(3, [])
    if not m.to and _prop_str(ole, base, "0E04"):
        m.to = [s.strip() for s in _prop_str(ole, base, "0E04").split(";") if s.strip()]
    if not m.cc and _prop_str(ole, base, "0E03"):
        m.cc = [s.strip() for s in _prop_str(ole, base, "0E03").split(";") if s.strip()]

    # Meldingstekst
    m.body_text = _prop_str(ole, base, "1000")
    html_raw = (_read_stream(ole, base + ["__substg1.0_10130102"])
                or _read_stream(ole, base + ["__substg1.0_1013001F"]))
    if html_raw:
        m.body_html = _decode_html(html_raw)
    rtf_raw = _read_stream(ole, base + ["__substg1.0_10090102"])
    if rtf_raw:
        try:
            m.body_rtf = decompress_rtf(rtf_raw)
        except Exception:
            m.body_rtf = b""

    # Vedlegg
    for entry in sorted(ole.listdir(streams=False, storages=True)):
        if len(entry) != len(prefix) + 1 or entry[:len(prefix)] != prefix:
            continue
        if not entry[-1].startswith("__attach_version1.0_"):
            continue
        ab = list(entry)
        aname = (_prop_str(ole, ab, "3707") or _prop_str(ole, ab, "3704")
                 or _prop_str(ole, ab, "3001") or entry[-1])
        att = MsgAttachment(name=aname, mime=_prop_str(ole, ab, "370E"))
        ap = _fixed_props(ole, ab, 8)
        if 0x7FFE in ap:
            att.hidden = bool(ap[0x7FFE][0])
        emb = ab + ["__substg1.0_3701000D"]
        if ole.exists("/".join(emb)):
            try:
                att.embedded = parse_msg(ole, emb, embedded=True)
                if not att.embedded.subject and aname:
                    att.embedded.subject = aname
            except Exception:
                att.embedded = None
        else:
            att.data = _read_stream(ole, ab + ["__substg1.0_37010102"]) or b""
        m.attachments.append(att)
    return m


def _decode_html(raw: bytes) -> str:
    if raw.startswith(b"\xff\xfe") or (len(raw) > 1 and raw[1:2] == b"\x00"):
        try:
            return raw.decode("utf-16-le", errors="replace").rstrip("\x00")
        except Exception:
            pass
    m = re.search(rb'charset\s*=\s*["\']?([A-Za-z0-9_\-]+)', raw[:4096], re.I)
    for enc in ([m.group(1).decode("ascii", "ignore")] if m else []) + ["utf-8", "cp1252"]:
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("cp1252", errors="replace")


# ── Komprimert RTF (MS-OXRTFCP, «LZFu») ───────────────────────────────────────

_RTF_PREBUF = (b"{\\rtf1\\ansi\\mac\\deff0\\deftab720{\\fonttbl;}{\\f0\\fnil \\froman "
               b"\\fswiss \\fmodern \\fscript \\fdecor MS Sans SerifSymbolArialTimes New "
               b"RomanCourier{\\colortbl\\red0\\green0\\blue0\r\n\\par \\pard\\plain\\f0\\"
               b"fs20\\b\\i\\u\\tab\\tx")


def decompress_rtf(data: bytes) -> bytes:
    """Dekomprimer PR_RTF_COMPRESSED (MELA = ukomprimert, LZFu = komprimert)."""
    if len(data) < 16:
        return b""
    _comp_size, raw_size, magic, _crc = struct.unpack_from("<IIII", data, 0)
    body = data[16:]
    if magic == 0x414C454D:          # «MELA» — ukomprimert
        return body[:raw_size]
    if magic != 0x75465A4C:          # «LZFu»
        raise ValueError("ukjent RTF-komprimering")
    dic = bytearray(4096)
    dic[:len(_RTF_PREBUF)] = _RTF_PREBUF
    wpos = len(_RTF_PREBUF)
    out = bytearray()
    i = 0
    n = len(body)
    while i < n:
        flags = body[i]
        i += 1
        for bit in range(8):
            if i >= n:
                break
            if flags & (1 << bit):
                if i + 1 >= n:
                    break
                ref = (body[i] << 8) | body[i + 1]
                i += 2
                off, length = ref >> 4, (ref & 0xF) + 2
                if off == wpos:
                    return bytes(out)
                for k in range(length):
                    ch = dic[(off + k) % 4096]
                    out.append(ch)
                    dic[wpos] = ch
                    wpos = (wpos + 1) % 4096
            else:
                ch = body[i]
                i += 1
                out.append(ch)
                dic[wpos] = ch
                wpos = (wpos + 1) % 4096
    return bytes(out[:raw_size]) if raw_size else bytes(out)


# ── Rendering ────────────────────────────────────────────────────────────────

_PDFA_LEVELS = {                  # label → SelectPdfVersion
    "PDF/A-2b (ISO 19005-2, level B)": "6",
    "PDF/A-3b (ISO 19005-3, level B)": "9",
}
DEFAULT_LEVEL = "PDF/A-2b (ISO 19005-2, level B)"

# filendelse → LibreOffice-eksportfilter for PDF
_LO_EXPORT = {
    **{e: "writer_pdf_Export" for e in (
        "doc", "docx", "dot", "dotx", "odt", "ott", "rtf", "txt", "wpd", "wps",
        "htm", "html", "xml", "csv")},
    **{e: "calc_pdf_Export" for e in ("xls", "xlsx", "xlt", "xltx", "ods")},
    **{e: "impress_pdf_Export" for e in ("ppt", "pptx", "pps", "ppsx", "pot", "potx", "odp")},
    **{e: "draw_pdf_Export" for e in (
        "jpg", "jpeg", "png", "gif", "bmp", "tif", "tiff", "svg", "odg", "emf", "wmf")},
}


def pdfa_filter(export: str, level_label: str) -> str:
    ver = _PDFA_LEVELS.get(level_label, _PDFA_LEVELS[DEFAULT_LEVEL])
    return (f'pdf:{export}:{{"SelectPdfVersion":{{"type":"long","value":"{ver}"}},'
            f'"UseTaggedPDF":{{"type":"boolean","value":"false"}}}}')


@dataclass
class AttachmentResult:
    name: str
    status: str          # "konvertert" | "tatt med (PDF)" | "kun listet" | "feilet"
    note: str = ""
    size: int = 0


@dataclass
class MsgConversionResult:
    ok: bool
    pdf_path: "Path | None" = None
    level: str = DEFAULT_LEVEL
    embedded_original: bool = False
    kind: str = "msg"                 # «msg» eller «eml»
    pdfa_normalized: bool = False     # normalisert med Ghostscript
    header_fallback: bool = False     # toppside tegnet med PyMuPDF (LO feilet)
    attachments: list[AttachmentResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    error: str = ""

    @property
    def summary(self) -> str:
        n_conv = sum(a.status in ("konvertert", "tatt med (PDF)") for a in self.attachments)
        n_list = sum(a.status == "kun listet" for a in self.attachments)
        n_fail = sum(a.status == "feilet" for a in self.attachments)
        parts = [f"{len(self.attachments)} vedlegg"]
        if self.attachments:
            parts.append(f"{n_conv} konvertert")
            if n_list:
                parts.append(f"{n_list} kun listet")
            if n_fail:
                parts.append(f"{n_fail} feilet")
        if self.embedded_original:
            parts.append(f"original {self.kind.upper()} innebygd")
        if self.pdfa_normalized:
            parts.append("PDF/A-normalisert med Ghostscript")
        if self.header_fallback:
            parts.append("toppside uten LibreOffice")
        return ", ".join(parts)


def _detect_ext(name: str, data: bytes) -> str:
    """Filtype for et vedlegg: innhold (magic bytes) først, ellers filendelse."""
    try:
        from siard_workflow.core.identifiers.magic_bytes import _detect
        if is_msg_bytes(data):
            return "msg"
        if is_eml_bytes(data):
            return "eml"
        ext, _mime, enc = _detect(data)
        if enc:
            return "encrypted"
        if ext not in ("bin", "txt"):
            return ext
    except Exception:
        pass
    suf = Path(name).suffix.lstrip(".").lower()
    return suf or "bin"


def _safe_name(name: str, fallback: str) -> str:
    s = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name or "").strip(" .")
    return (s or fallback)[:120]


def _sanitize_html(body: str) -> str:
    """Fjern aktivt innhold og bilder som ikke kan hentes (cid:/http)."""
    body = re.sub(r"(?is)<(script|iframe|object|embed|form)\b.*?</\1\s*>", "", body)
    body = re.sub(r"(?is)<(script|iframe|object|embed|link|meta|base)\b[^>]*/?>", "", body)
    body = re.sub(r'(?is)<img\b[^>]*\bsrc\s*=\s*["\']?(?:cid:|https?:)[^>]*>',
                  "<span style='color:#777'>[bilde]</span>", body)
    m = re.search(r"(?is)<body\b[^>]*>(.*)</body\s*>", body)
    return m.group(1) if m else body


def _fmt_date(d: "_dt.datetime | None") -> str:
    if not d:
        return ""
    try:
        return d.astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        return d.strftime("%d.%m.%Y %H:%M")


def _header_html(msg: MsgMessage, att_results: list[AttachmentResult],
                 include_body_html: bool, embedded_original: bool) -> str:
    esc = html.escape
    rows = [("Emne", msg.subject), ("Fra", msg.sender), ("Til", "; ".join(msg.to)),
            ("Kopi", "; ".join(msg.cc)), ("Blindkopi", "; ".join(msg.bcc)),
            ("Dato", _fmt_date(msg.date))]
    head = "".join(
        f"<tr><td style='font-weight:bold;padding-right:12px;vertical-align:top'>{k}:</td>"
        f"<td>{esc(v)}</td></tr>" for k, v in rows if v)
    if att_results:
        items = "".join(
            f"<li>{esc(a.name)}"
            + (f" ({a.size:,} byte)".replace(",", " ") if a.size else "")
            + f" — <i>{esc(a.status)}</i>"
            + (f": {esc(a.note)}" if a.note else "") + "</li>"
            for a in att_results)
        att_html = f"<p style='margin-bottom:2px'><b>Vedlegg ({len(att_results)}):</b></p><ul>{items}</ul>"
    else:
        att_html = "<p><b>Vedlegg:</b> ingen</p>"
    note = ("Original e-post (MSG) er innebygd i denne PDF-filen."
            if embedded_original else
            "Konvertert fra Outlook-e-post (MSG). Vedlegg følger etter meldingsteksten.")
    if include_body_html:
        body = _sanitize_html(msg.body_html)
    elif msg.body_text:
        body = f"<pre style='white-space:pre-wrap;font-family:sans-serif'>{esc(msg.body_text)}</pre>"
    elif msg.body_rtf:
        body = "<p><i>Meldingsteksten følger på neste side.</i></p>"
    else:
        body = "<p><i>(ingen meldingstekst)</i></p>"
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<style>body{font-family:'Liberation Sans',Arial,sans-serif;font-size:10pt}"
        "table{border-collapse:collapse;margin-bottom:6px}</style></head><body>"
        f"<table>{head}</table>{att_html}"
        f"<p style='color:#555;font-size:8pt'>{esc(note)}</p><hr/>"
        f"{body}</body></html>")


def _lo_to_pdf(lo_bin: str, src: Path, out_dir: Path, export: str, level: str,
               profile_dir: Path, timeout: int, infilter: "str | None" = None) -> "Path | None":
    """LibreOffice → PDF/A via lo_runner (ny profil per kall når gjenbruk
    henger; hele prosesstreet drepes ved tidsavbrudd)."""
    from siard_workflow.core import lo_runner as _lr
    ok, _err = _lr.convert(lo_bin, src, out_dir, pdfa_filter(export, level),
                           profile_dir, timeout, infilter=infilter)
    pdf = Path(out_dir) / (Path(src).stem + ".pdf")
    return pdf if ok and pdf.exists() else None


_IMAGE_EXTS = {"jpg", "jpeg", "png", "gif", "bmp", "tif", "tiff"}


def _image_to_pdf(data: bytes, ext: str, out_pdf: Path) -> bool:
    """Bilde → PDF med én A4-side per bilde/ramme (tilpasset, sentrert)."""
    import fitz
    try:
        img = fitz.open(stream=data, filetype="jpg" if ext == "jpeg" else ext)
        pdf_bytes = img.convert_to_pdf()
        img.close()
        src = fitz.open("pdf", pdf_bytes)
        out = fitz.open()
        margin = 36
        for pno in range(src.page_count):
            r = src[pno].rect
            landscape = r.width > r.height
            page = out.new_page(width=842 if landscape else 595,
                                height=595 if landscape else 842)
            area = fitz.Rect(margin, margin, page.rect.width - margin,
                             page.rect.height - margin)
            # Små bilder forstørres ikke
            if r.width <= area.width and r.height <= area.height:
                x0 = area.x0 + (area.width - r.width) / 2
                y0 = area.y0 + (area.height - r.height) / 2
                area = fitz.Rect(x0, y0, x0 + r.width, y0 + r.height)
            page.show_pdf_page(area, src, pno, keep_proportion=True)
        out_pdf.parent.mkdir(parents=True, exist_ok=True)
        out.save(str(out_pdf), garbage=3, deflate=True)
        out.close()
        src.close()
        return out_pdf.exists()
    except Exception:
        return False


def _header_pdf_fallback(html_text: str, out_pdf: Path) -> bool:
    """Toppside/meldingstekst tegnet med PyMuPDF (reserve når LibreOffice feiler)."""
    import fitz
    try:
        story = fitz.Story(html=html_text)
        writer = fitz.DocumentWriter(str(out_pdf))
        mediabox = fitz.paper_rect("a4")
        where = mediabox + (50, 50, -50, -50)
        more = True
        while more:
            dev = writer.begin_page(mediabox)
            more, _ = story.place(where)
            story.draw(dev)
            writer.end_page()
        writer.close()
        return out_pdf.exists() and out_pdf.stat().st_size > 0
    except Exception:
        return False


def _embed_original(doc, data: bytes, filename: str, mime: str = MSG_MIME) -> None:
    """Legg original-MSG inn som PDF/A-3-tilknyttet fil (AFRelationship /Source)."""
    doc.embfile_add(filename, data, filename=filename, ufilename=filename,
                    desc="Original e-post (" + ("Outlook MSG" if mime == MSG_MIME
                                                else "MIME/EML") + ")")
    fs_xref = 0
    try:
        names = doc.xref_get_key(doc.pdf_catalog(), "Names/EmbeddedFiles/Names")
        m = re.findall(r"(\d+) 0 R", names[1] if names else "")
        fs_xref = int(m[-1]) if m else 0
    except Exception:
        fs_xref = 0
    if not fs_xref:
        return
    doc.xref_set_key(fs_xref, "AFRelationship", "/Source")
    ef = doc.xref_get_key(fs_xref, "EF/F")
    mm = re.match(r"(\d+) 0 R", ef[1] if ef else "")
    if mm:
        sx = int(mm.group(1))
        doc.xref_set_key(sx, "Subtype", "/" + mime.replace("/", "#2F"))
        now = _dt.datetime.now(_dt.timezone.utc).strftime("D:%Y%m%d%H%M%SZ")
        doc.xref_set_key(sx, "Params", f"<</ModDate({now})/Size {len(data)}>>")
    doc.xref_set_key(doc.pdf_catalog(), "AF", f"[{fs_xref} 0 R]")


def convert_msg_to_pdf(msg_path: Path, out_pdf: Path, lo_bin: str,
                       level: str = DEFAULT_LEVEL, timeout: int = 300,
                       work_dir: "Path | None" = None,
                       embed_original: "bool | None" = None,
                       _depth: int = 0,
                       profile_dir: "Path | None" = None) -> MsgConversionResult:
    """
    Konverter én MSG til PDF/A. Skriver ikke `out_pdf` ved feil.
    embed_original: None → følger nivået (PDF/A-3 ⇒ innebygd original).
    profile_dir: LibreOffice-brukerprofil (gjenbrukes per tråd — å opprette en
    ny profil tar flere sekunder).
    """
    if level not in _PDFA_LEVELS:
        level = DEFAULT_LEVEL
    if embed_original is None:
        embed_original = level.startswith("PDF/A-3")
    res = MsgConversionResult(ok=False, level=level)
    own_tmp = work_dir is None
    tmp = Path(work_dir or tempfile.mkdtemp(prefix="msg2pdf_"))
    tmp.mkdir(parents=True, exist_ok=True)
    profile = profile_dir or (tmp / "lo_profile")
    try:
        raw = msg_path.read_bytes()
        msg, kind = parse_email_file(msg_path)
        res.kind = kind
        return _render(msg, raw, msg_path.name, out_pdf, lo_bin, level, timeout,
                       tmp, profile, embed_original, res, _depth)
    except Exception as exc:
        res.error = f"{type(exc).__name__}: {exc}"
        return res
    finally:
        if own_tmp:
            shutil.rmtree(tmp, ignore_errors=True)


def _render(msg: MsgMessage, raw: "bytes | None", orig_name: str, out_pdf: Path,
            lo_bin: str, level: str, timeout: int, tmp: Path, profile: Path,
            embed_original: bool, res: MsgConversionResult, depth: int) -> MsgConversionResult:
    import fitz

    # 1) Vedlegg → PDF-deler (rekkefølge bevares)
    parts: list[Path] = []
    for i, att in enumerate(msg.attachments):
        name = att.name or f"vedlegg{i + 1}"
        ar = AttachmentResult(name=name, status="kun listet", size=len(att.data))
        adir = tmp / f"a{depth}_{i}"
        adir.mkdir(parents=True, exist_ok=True)
        try:
            if att.embedded is not None:
                if depth >= 3:
                    ar.note = "vedlagt e-post nestet for dypt"
                else:
                    sub_pdf = adir / "embedded.pdf"
                    sub = MsgConversionResult(ok=False, level=level, kind=res.kind)
                    _render(att.embedded, None, name, sub_pdf, lo_bin, level, timeout,
                            adir, profile, False, sub, depth + 1)
                    if sub.ok:
                        parts.append(sub_pdf)
                        ar.status, ar.note = "konvertert", "vedlagt e-post"
                    else:
                        ar.status, ar.note = "feilet", sub.error or "vedlagt e-post"
            elif not att.data:
                ar.note = "tomt vedlegg"
            else:
                ext = _detect_ext(name, att.data)
                src = adir / f"{_safe_name(Path(name).stem, 'vedlegg')}.{ext}"
                src.write_bytes(att.data)
                if ext in ("msg", "eml"):
                    sub_pdf = adir / f"{ext}.pdf"
                    sub = convert_msg_to_pdf(src, sub_pdf, lo_bin, level, timeout,
                                             adir / "w", False, depth + 1, profile)
                    if sub.ok:
                        parts.append(sub_pdf)
                        ar.status, ar.note = "konvertert", "vedlagt e-post"
                    else:
                        ar.status, ar.note = "feilet", sub.error
                elif ext == "pdf":
                    parts.append(src)
                    ar.status = "tatt med (PDF)"
                    res.warnings.append(f"vedlegg «{name}» tatt med som PDF uten PDF/A-kontroll")
                elif ext == "encrypted":
                    ar.note = "passordbeskyttet"
                elif ext in _IMAGE_EXTS:
                    ipdf = adir / "bilde.pdf"
                    if _image_to_pdf(att.data, ext, ipdf):
                        parts.append(ipdf)
                        ar.status = "konvertert"
                    else:
                        ar.status, ar.note = "feilet", f"{ext.upper()}-bilde kunne ikke leses"
                elif ext in _LO_EXPORT:
                    pdf = _lo_to_pdf(lo_bin, src, adir / "out", _LO_EXPORT[ext], level,
                                     profile, timeout)
                    if pdf:
                        parts.append(pdf)
                        ar.status = "konvertert"
                    else:
                        ar.status, ar.note = "feilet", f"{ext.upper()} kunne ikke konverteres"
                else:
                    ar.note = f"{ext.upper()} kan ikke vises i PDF"
                    if embed_original:
                        ar.note += " (finnes i innebygd original)"
        except Exception as exc:
            ar.status, ar.note = "feilet", f"{type(exc).__name__}: {exc}"
        res.attachments.append(ar)

    # 2) Toppside + meldingstekst
    use_html = bool(msg.body_html.strip())
    hdr_src = tmp / f"melding{depth}.html"
    hdr_html = _header_html(msg, res.attachments, use_html, embed_original)
    hdr_src.write_text(hdr_html, "utf-8")
    hdr_pdf = _lo_to_pdf(lo_bin, hdr_src, tmp / f"hout{depth}", "writer_pdf_Export", level,
                         profile, timeout, infilter="HTML (StarWriter)") if lo_bin else None
    if not hdr_pdf:
        fb = tmp / f"hout{depth}" / "toppside_pymupdf.pdf"
        fb.parent.mkdir(parents=True, exist_ok=True)
        if _header_pdf_fallback(hdr_html, fb):
            hdr_pdf = fb
            res.header_fallback = True
            res.warnings.append("toppside/meldingstekst tegnet uten LibreOffice (PyMuPDF)")
        else:
            res.error = "toppside/meldingstekst kunne ikke konverteres (LibreOffice/PyMuPDF)"
            return res
    body_parts: list[Path] = []
    if not use_html and not msg.body_text and msg.body_rtf:
        rtf = tmp / f"melding{depth}.rtf"
        rtf.write_bytes(msg.body_rtf)
        rpdf = _lo_to_pdf(lo_bin, rtf, tmp / f"rout{depth}", "writer_pdf_Export", level,
                          profile, timeout)
        if rpdf:
            body_parts.append(rpdf)
        else:
            res.warnings.append("RTF-meldingstekst kunne ikke konverteres")

    # 3) Slå sammen — toppsiden er basis (beholder OutputIntent + XMP/pdfaid)
    merged = tmp / f"samlet{depth}.pdf"
    doc = fitz.open(str(hdr_pdf))
    try:
        for p in body_parts + parts:
            try:
                with fitz.open(str(p)) as part:
                    doc.insert_pdf(part)
            except Exception as exc:
                res.warnings.append(f"del {p.name} kunne ikke slås sammen: {exc}")
        doc.save(str(merged), garbage=3, deflate=True)
    finally:
        doc.close()

    # 4) PDF/A-normalisering med Ghostscript (valgfritt; kun øverste nivå —
    #    vedlagte e-poster normaliseres som del av hoveddokumentet)
    final = merged
    if depth == 0:
        try:
            from siard_workflow.core import ghostscript as _gs
            gs_bin = _gs.active_ghostscript()
        except Exception:
            gs_bin = None
        if gs_bin:
            norm = tmp / "normalisert.pdf"
            ok, gerr = _gs.to_pdfa(merged, norm, part=3 if level.startswith("PDF/A-3") else 2,
                                   gs=gs_bin, timeout=max(timeout, 300))
            if ok:
                final = norm
                res.pdfa_normalized = True
                # PDF-vedlegg er nå også PDF/A — fjern advarselen om manglende kontroll
                res.warnings = [x for x in res.warnings if "uten PDF/A-kontroll" not in x]
            else:
                res.warnings.append(f"PDF/A-normalisering feilet, beholder LibreOffice-PDF: {gerr}")
        if res.header_fallback and not res.pdfa_normalized:
            res.warnings.append("toppsiden er ikke PDF/A (LibreOffice feilet og Ghostscript mangler)")

    # 5) Original e-post innebygd (PDF/A-3) — etter normaliseringen, som ellers
    #    ville fjernet den
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    if embed_original and raw is not None:
        with fitz.open(str(final)) as d2:
            ext = "." + res.kind
            _embed_original(d2, raw, orig_name if orig_name.lower().endswith(ext)
                            else Path(orig_name).stem + ext,
                            MSG_MIME if res.kind == "msg" else "message/rfc822")
            d2.save(str(out_pdf), garbage=3, deflate=True)
        res.embedded_original = True
    else:
        shutil.copyfile(str(final), str(out_pdf))
    res.ok = out_pdf.exists() and out_pdf.stat().st_size > 0
    res.pdf_path = out_pdf if res.ok else None
    return res


def pdfa_identification(pdf_path: Path) -> "tuple[str, str] | None":
    """(part, conformance) fra XMP pdfaid, eller None — enkel egenkontroll."""
    try:
        import fitz
        with fitz.open(str(pdf_path)) as d:
            xmp = d.get_xml_metadata() or ""
        part = re.search(r"pdfaid:part(?:>|=[\"'])\s*(\d)", xmp)
        conf = re.search(r"pdfaid:conformance(?:>|=[\"'])\s*([A-Za-z])", xmp)
        return (part.group(1), conf.group(1).upper()) if part and conf else None
    except Exception:
        return None
