"""
Tester for Outlook-e-post (MSG) → PDF/A i BLOB-konverteringen.

Bakgrunn: MSG og DOC deler OLE2-container. Magic-byte-deteksjonen falt
tilbake til «Word» for ukjent OLE2, så alle MSG-filer ble behandlet som DOC.

Verifiserer:
  1. Gjenkjenning: syntetisk MSG (med og uten DOC-vedlegg, med «WordDocument»
     i vedleggsdataene) → «msg», også når bare de første 64 kB leses. Ekte
     Word-fil og ukjent OLE2 påvirkes ikke.
  2. Parsing: emne, avsender, mottakere (Til/Kopi), dato, HTML-/tekstkropp,
     komprimert RTF (MELA), vedlegg og vedlagt e-post (MSG i MSG).
  3. Konvertering med LibreOffice erstattet av en stedfortreder (PyMuPDF-
     generert PDF med pdfaid i XMP): sideantall og rekkefølge, vedleggsstatus
     (konvertert / tatt med / kun listet), PDF/A-2b uten innebygd fil,
     PDF/A-3 med original MSG innebygd (AFRelationship /Source, catalog /AF).
  4. Ekte eksempelfil (eksempelfiler/test på msg.msg) hvis den finnes lokalt.

Selve LibreOffice-kallet testes ikke her (krever fungerende LO-installasjon).

Kjør:  python -X utf8 tests/test_msg_to_pdf.py
"""
from __future__ import annotations

import io
import struct
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import olefile  # noqa: E402

from siard_workflow.core import msg_to_pdf as M  # noqa: E402
from siard_workflow.core import ghostscript as GS  # noqa: E402
from siard_workflow.core.identifiers.magic_bytes import _detect  # noqa: E402

_REAL_ACTIVE_GS = GS.active_ghostscript
GS.active_ghostscript = lambda: None      # av som standard i testene


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


# ── Minimal OLE2-skriver (for syntetiske MSG-er) ──────────────────────────────
# olefile kan ikke skrive nye filer. Denne skriveren lager en gyldig compound
# file med vilkårlig mappe-/strømtre (512-byte sektorer, ingen mini-stream:
# alle strømmer utfylles til ≥ 4096 byte er ikke nødvendig — vi setter
# mini-stream-cutoff til 0 slik at alt ligger i vanlige sektorer).

def _write_ole(entries: dict[str, bytes], root_clsid: bytes = b"\x00" * 16) -> bytes:
    SEC = 512
    # Tre: sti → barn
    nodes = {"": {"name": "Root Entry", "type": 5, "children": [], "data": b""}}
    for path in sorted(entries):
        parts = path.split("/")
        for i in range(1, len(parts)):
            sp = "/".join(parts[:i])
            if sp not in nodes:
                nodes[sp] = {"name": parts[i - 1], "type": 1, "children": [], "data": b""}
                nodes["/".join(parts[:i - 1])]["children"].append(sp)
        # Alle strømmer ≥ 4096 byte → ligger i vanlige sektorer (ingen mini-stream).
        # Utfylling med NUL tåles av parseren (strenger strippes, lengder fra header).
        nodes[path] = {"name": parts[-1], "type": 2, "children": [],
                       "data": entries[path].ljust(4096, b"\x00")}
        nodes["/".join(parts[:-1])]["children"].append(path)
    order = [""] + [p for p in nodes if p]
    sid = {p: i for i, p in enumerate(order)}

    # Datasektorer
    sectors: list[bytes] = []
    start: dict[str, int] = {}
    fat: list[int] = []
    for p in order[1:]:
        d = nodes[p]["data"]
        if nodes[p]["type"] != 2 or not d:
            continue
        n = (len(d) + SEC - 1) // SEC
        start[p] = len(sectors)
        for k in range(n):
            sectors.append(d[k * SEC:(k + 1) * SEC].ljust(SEC, b"\x00"))
            fat.append(len(sectors) if k < n - 1 else 0xFFFFFFFE)

    # Katalog: barn som balansert-nok binærtre (lenket liste via right-sibling)
    def _entry(p: str) -> bytes:
        nd = nodes[p]
        name = nd["name"].encode("utf-16-le") + b"\x00\x00"
        kids = sorted(nd["children"], key=lambda c: (len(nodes[c]["name"]), nodes[c]["name"].upper()))
        child = sid[kids[0]] if kids else 0xFFFFFFFF
        # søsken: finn neste i foreldrenes sorterte liste
        right = 0xFFFFFFFF
        if p:
            par = "/".join(p.split("/")[:-1])
            sib = sorted(nodes[par]["children"], key=lambda c: (len(nodes[c]["name"]), nodes[c]["name"].upper()))
            i = sib.index(p)
            if i + 1 < len(sib):
                right = sid[sib[i + 1]]
        e = bytearray(128)
        e[0:len(name)] = name
        struct.pack_into("<HBB", e, 64, len(name), nd["type"], 1)
        struct.pack_into("<III", e, 68, 0xFFFFFFFF, right, child)
        if p == "":
            e[80:96] = root_clsid
        st = start.get(p, 0xFFFFFFFE if nd["type"] != 5 else 0xFFFFFFFE)
        struct.pack_into("<IQ", e, 116, st, len(nd["data"]))
        return bytes(e)

    dir_bytes = b"".join(_entry(p) for p in order)
    dir_bytes = dir_bytes.ljust(((len(dir_bytes) + SEC - 1) // SEC) * SEC, b"\x00")
    dir_start = len(sectors)
    n_dir = len(dir_bytes) // SEC
    for k in range(n_dir):
        sectors.append(dir_bytes[k * SEC:(k + 1) * SEC])
        fat.append(len(sectors) if k < n_dir - 1 else 0xFFFFFFFE)

    # FAT-sektorer (plass til 128 innslag per sektor)
    n_fat = 1
    while (len(sectors) + n_fat) > n_fat * 128:
        n_fat += 1
    fat_start = len(sectors)
    for _ in range(n_fat):
        fat.append(0xFFFFFFFD)
    fat += [0xFFFFFFFF] * (n_fat * 128 - len(fat))
    fat_bytes = struct.pack(f"<{len(fat)}I", *fat)
    for k in range(n_fat):
        sectors.append(fat_bytes[k * SEC:(k + 1) * SEC])

    hdr = bytearray(512)
    hdr[0:8] = M.OLE2_MAGIC
    struct.pack_into("<HHHHH", hdr, 24, 0x3E, 3, 0xFFFE, 9, 6)
    # 40: #dir-sektorer (0 i v3), 44: #FAT, 48: første dir-sektor, 52: transaksjon,
    # 56: mini-stream-cutoff (4096; alle strømmer er utfylt til minst dette),
    # 60: første miniFAT, 64: #miniFAT, 68: første DIFAT, 72: #DIFAT
    struct.pack_into("<IIIIIIIII", hdr, 40, 0, n_fat, dir_start, 0, 4096,
                     0xFFFFFFFE, 0, 0xFFFFFFFE, 0)
    difat = [fat_start + k for k in range(n_fat)] + [0xFFFFFFFF] * (109 - n_fat)
    struct.pack_into("<109I", hdr, 76, *difat)
    return bytes(hdr) + b"".join(sectors)


def _u16(s: str) -> bytes:
    return s.encode("utf-16-le")


def _props(header_len: int, fixed: dict[int, tuple[int, bytes]]) -> bytes:
    """__properties_version1.0: header + 16-byte innslag {pid: (type, 8 byte)}."""
    out = bytearray(header_len)
    for pid, (ptype, val) in fixed.items():
        out += struct.pack("<II", (pid << 16) | ptype, 6) + val.ljust(8, b"\x00")
    return bytes(out)


def _filetime(y, mo, d, h, mi) -> bytes:
    import datetime as dt
    t = dt.datetime(y, mo, d, h, mi, tzinfo=dt.timezone.utc)
    ft = int((t - dt.datetime(1601, 1, 1, tzinfo=dt.timezone.utc)).total_seconds() * 10_000_000)
    return struct.pack("<Q", ft)


_WORD_LIKE = (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 64
              + "WordDocument".encode("utf-16-le") + b"Word.Document" + b"\x00" * 400)


def _msg_entries(prefix: str = "", header_len: int = 32, *, subject="Møtereferat",
                 html=None, text=None, rtf=None, attachments=()) -> dict[str, bytes]:
    pre = prefix + "/" if prefix else ""
    e = {
        pre + "__properties_version1.0": _props(header_len, {0x0039: (0x0040, _filetime(2019, 5, 17, 10, 30))}),
        pre + "__substg1.0_0037001F": _u16(subject),
        pre + "__substg1.0_0C1A001F": _u16("Kari Nordmann"),
        pre + "__substg1.0_5D01001F": _u16("kari@marnardal.kommune.no"),
        pre + "__recip_version1.0_#00000000/__properties_version1.0": _props(8, {0x0C15: (0x0003, struct.pack("<I", 1))}),
        pre + "__recip_version1.0_#00000000/__substg1.0_3001001F": _u16("Ola Hansen"),
        pre + "__recip_version1.0_#00000000/__substg1.0_39FE001F": _u16("ola@x.no"),
        pre + "__recip_version1.0_#00000001/__properties_version1.0": _props(8, {0x0C15: (0x0003, struct.pack("<I", 2))}),
        pre + "__recip_version1.0_#00000001/__substg1.0_3001001F": _u16("Per Berg"),
    }
    if html is not None:
        e[pre + "__substg1.0_10130102"] = html.encode("utf-8")
    if text is not None:
        e[pre + "__substg1.0_1000001F"] = _u16(text)
    if rtf is not None:
        e[pre + "__substg1.0_10090102"] = struct.pack("<IIII", len(rtf) + 12, len(rtf), 0x414C454D, 0) + rtf
    for i, (name, data) in enumerate(attachments):
        ap = f"{pre}__attach_version1.0_#{i:08X}"
        e[ap + "/__properties_version1.0"] = _props(8, {})
        e[ap + "/__substg1.0_3707001F"] = _u16(name)
        if isinstance(data, dict):           # vedlagt e-post
            for k, v in data.items():
                e[ap + "/__substg1.0_3701000D/" + k] = v
        else:
            e[ap + "/__substg1.0_37010102"] = data
    return e


def _build_msg(**kw) -> bytes:
    return _write_ole(_msg_entries(**kw))


# ── Stedfortreder for LibreOffice ─────────────────────────────────────────────

_XMP = ('<?xpacket begin="" id="W5M0MpCehiHzreSzNTczkc9d"?><x:xmpmeta xmlns:x="adobe:ns:meta/">'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description rdf:about="" xmlns:pdfaid="http://www.aiim.org/pdfa/ns/id/">'
        '<pdfaid:part>{part}</pdfaid:part><pdfaid:conformance>B</pdfaid:conformance>'
        '</rdf:Description></rdf:RDF></x:xmpmeta><?xpacket end="w"?>')


def _fake_lo(calls: list):
    import fitz

    def _lo(lo_bin, src, out_dir, export, level, profile_dir, timeout, infilter=None):
        calls.append((Path(src).suffix, export, level, infilter))
        out_dir.mkdir(parents=True, exist_ok=True)
        text = Path(src).read_text("utf-8", errors="replace")[:2000] \
            if Path(src).suffix in (".html", ".rtf", ".txt") else f"[{Path(src).name}]"
        d = fitz.open()
        pg = d.new_page()
        pg.insert_textbox(fitz.Rect(40, 40, 560, 800), text, fontsize=8)
        d.set_xml_metadata(_XMP.format(part="3" if "3b" in level.lower() else "2"))
        out = out_dir / (Path(src).stem + ".pdf")
        d.save(str(out))
        d.close()
        return out
    return _lo


# ── Tester ────────────────────────────────────────────────────────────────────

def test_detection() -> None:
    print("test_detection")
    msg_doc = _build_msg(html="<p>Hei</p>", attachments=[("rapport.doc", _WORD_LIKE)])
    msg_plain = _build_msg(text="Hei")
    for label, data in (("MSG med DOC-vedlegg", msg_doc), ("MSG uten vedlegg", msg_plain)):
        _ok(_detect(data)[0] == "msg", f"{label} → msg (full fil)")
        _ok(_detect(data[:65536])[0] == "msg", f"{label} → msg (64 kB-vindu)")
        _ok(M.is_msg_bytes(data), f"{label}: is_msg_bytes")
    with olefile.OleFileIO(io.BytesIO(msg_doc)) as o:
        _ok(o.exists("__attach_version1.0_#00000000/__substg1.0_37010102"), "syntetisk MSG leses av olefile")
    word = _write_ole({"WordDocument": b"\x00" * 600, "1Table": b"\x00" * 600,
                       "\x05SummaryInformation": b"\x00" * 100})
    _ok(_detect(word)[0] == "doc" and not M.is_msg_bytes(word), "ekte Word-fil forblir doc")
    _ok(not M.is_msg_bytes(b"%PDF-1.4"), "ikke-OLE2 → ikke MSG")


def test_parse() -> None:
    print("test_parse")
    inner = {k: v for k, v in _msg_entries(header_len=24, subject="Videresendt melding",
                                             text="Innebygd tekst").items()}
    data = _build_msg(html="<html><body><p>Hei <b>Ola</b></p><script>x()</script>"
                           "<img src='cid:bilde1'></body></html>",
                      attachments=[("rapport.doc", _WORD_LIKE), ("data.zip", b"PK\x03\x04" + b"\x00" * 50),
                                   ("videresendt.msg", inner)])
    with olefile.OleFileIO(io.BytesIO(data)) as o:
        m = M.parse_msg(o)
    _ok(m.subject == "Møtereferat", f"emne: {m.subject}")
    _ok(m.sender == "Kari Nordmann <kari@marnardal.kommune.no>", f"avsender: {m.sender}")
    _ok(m.to == ["Ola Hansen <ola@x.no>"] and m.cc == ["Per Berg"], f"mottakere: {m.to} {m.cc}")
    _ok(m.date is not None and m.date.year == 2019 and m.date.month == 5, f"dato: {m.date}")
    _ok("<b>Ola</b>" in m.body_html, "HTML-kropp")
    _ok([a.name for a in m.attachments] == ["rapport.doc", "data.zip", "videresendt.msg"], "vedleggsnavn")
    emb = m.attachments[2].embedded
    _ok(emb is not None and emb.subject == "Videresendt melding" and emb.body_text == "Innebygd tekst",
        "vedlagt e-post parset rekursivt")
    s = M._sanitize_html(m.body_html)
    _ok("<script" not in s and "cid:" not in s and "[bilde]" in s, "HTML renset (script/cid-bilder)")
    _ok(M.decompress_rtf(struct.pack("<IIII", 17, 5, 0x414C454D, 0) + b"{\\rtf}") == b"{\\rtf",
        "RTF: ukomprimert (MELA)")


def test_convert(tmp: Path) -> None:
    print("test_convert")
    import fitz
    calls: list = []
    orig = M._lo_to_pdf
    M._lo_to_pdf = _fake_lo(calls)
    try:
        inner = _msg_entries(header_len=24, subject="Videresendt", text="Innebygd")
        existing_pdf = fitz.open(); existing_pdf.new_page(); pdf_bytes = existing_pdf.tobytes(); existing_pdf.close()
        src = tmp / "record1.msg"
        src.write_bytes(_build_msg(
            html="<p>Se vedlagt rapport.</p>",
            attachments=[("rapport.doc", _WORD_LIKE), ("kart.pdf", pdf_bytes),
                         ("data.zip", b"PK\x03\x04" + b"\x00" * 60), ("videresendt.msg", inner)]))

        # PDF/A-2b: uten innebygd original
        out = tmp / "r2b.pdf"
        r = M.convert_msg_to_pdf(src, out, "soffice", M.DEFAULT_LEVEL, work_dir=tmp / "w2b")
        _ok(r.ok and not r.embedded_original, f"2b ok, ingen innebygd original ({r.error})")
        st = {a.name: a.status for a in r.attachments}
        _ok(st == {"rapport.doc": "konvertert", "kart.pdf": "tatt med (PDF)",
                   "data.zip": "kun listet", "videresendt.msg": "konvertert"}, f"vedleggsstatus: {st}")
        with fitz.open(str(out)) as d:
            _ok(d.page_count == 4, f"4 sider: toppside + doc + pdf + videresendt e-post (fikk {d.page_count})")
            _ok("Videresendt" in d[3].get_text(), "videresendt e-post sist, med egen toppside")
            first = d[0].get_text()
            _ok("Møtereferat" in first and "Kari Nordmann" in first and "data.zip" in first
                and "kun listet" in first, "toppside: emne, avsender og vedleggsliste med status")
            _ok(d.embfile_count() == 0, "2b: ingen innebygde filer")
        _ok(M.pdfa_identification(out) == ("2", "B"), "pdfaid fra toppsiden beholdt (2B)")
        _ok(all(c[2] == M.DEFAULT_LEVEL for c in calls), "alle deler konvertert med PDF/A-2b")
        _ok(any(c[3] == "HTML (StarWriter)" for c in calls), "toppside via Writer (HTML-infilter)")
        _ok(any(c[0] == ".doc" and c[1] == "writer_pdf_Export" for c in calls), "DOC via Writer-eksport")
        _ok(not any(c[1] == "draw_pdf_Export" for c in calls), "ingen LibreOffice-kall for bilder")
        _ok(r.warnings and "kart.pdf" in r.warnings[0], "advarsel for PDF uten PDF/A-kontroll")

        # PDF/A-3b: original MSG innebygd som tilknyttet fil
        out3 = tmp / "r3b.pdf"
        r3 = M.convert_msg_to_pdf(src, out3, "soffice", "PDF/A-3b (ISO 19005-3, level B)",
                                  work_dir=tmp / "w3b")
        _ok(r3.ok and r3.embedded_original, "3b ok med innebygd original")
        with fitz.open(str(out3)) as d:
            _ok(d.embfile_names() == ["record1.msg"], f"innebygd fil: {d.embfile_names()}")
            _ok(d.embfile_get("record1.msg") == src.read_bytes(), "innebygd original er byte-identisk")
            af = d.xref_get_key(d.pdf_catalog(), "AF")
            _ok(af[0] == "array", f"catalog /AF satt ({af})")
            names = d.xref_get_key(d.pdf_catalog(), "Names/EmbeddedFiles/Names")[1]
            import re
            fs = int(re.findall(r"(\d+) 0 R", names)[-1])
            _ok(d.xref_get_key(fs, "AFRelationship")[1] == "/Source", "AFRelationship /Source")
        _ok(M.pdfa_identification(out3) == ("3", "B"), "pdfaid 3B")
        _ok("original MSG innebygd" in r3.summary, f"oppsummering: {r3.summary}")

        # LibreOffice feiler helt → toppside tegnes med PyMuPDF (reserve)
        M._lo_to_pdf = lambda *a, **k: None
        fb = tmp / "fallback.pdf"
        rf = M.convert_msg_to_pdf(src, fb, "soffice", work_dir=tmp / "wfb")
        _ok(rf.ok and rf.header_fallback and fb.exists(), f"LO-feil → toppside via PyMuPDF ({rf.summary})")
        with fitz.open(str(fb)) as d:
            _ok("Møtereferat" in d[0].get_text(), "reserve-toppside har meldingshodet")
        st = {a.name: a.status for a in rf.attachments}
        _ok(st["rapport.doc"] == "feilet" and st["kart.pdf"] == "tatt med (PDF)",
            "DOC krever LO (feilet), PDF tas med uten LO")
        _ok(any("uten LibreOffice" in w for w in rf.warnings)
            and any("ikke PDF/A" in w for w in rf.warnings),
            "advarsler: toppside uten LO og ikke PDF/A (ingen Ghostscript)")
        # Også reserven feiler → ok=False, ingen utfil
        orig_fb = M._header_pdf_fallback
        M._header_pdf_fallback = lambda *a, **k: False
        try:
            bad = tmp / "bad.pdf"
            rb = M.convert_msg_to_pdf(src, bad, "soffice", work_dir=tmp / "wbad")
        finally:
            M._header_pdf_fallback = orig_fb
        _ok(not rb.ok and not bad.exists() and "LibreOffice" in rb.error,
            "både LO og reserve feiler → ingen PDF, feilmelding")
    finally:
        M._lo_to_pdf = orig


def test_real_sample(tmp: Path) -> None:
    sample = _ROOT / "eksempelfiler" / "test på msg.msg"
    if not sample.exists():
        print("test_real_sample (hoppet over — ingen eksempelfil)")
        return
    print("test_real_sample")
    data = sample.read_bytes()
    _ok(_detect(data)[0] == "msg" and _detect(data[:65536])[0] == "msg", "eksempelfil gjenkjent som msg")
    with olefile.OleFileIO(str(sample)) as o:
        m = M.parse_msg(o)
    _ok(m.subject and m.body_html and [a.name for a in m.attachments] == ["dokument.doc"],
        f"eksempelfil parset: «{m.subject}», vedlegg {[a.name for a in m.attachments]}")
    calls: list = []
    orig = M._lo_to_pdf
    M._lo_to_pdf = _fake_lo(calls)
    try:
        r = M.convert_msg_to_pdf(sample, tmp / "sample.pdf", "soffice", work_dir=tmp / "ws")
    finally:
        M._lo_to_pdf = orig
    _ok(r.ok and r.attachments[0].status == "konvertert", f"eksempelfil konvertert: {r.summary}")


def test_blob_convert_integration(tmp: Path) -> None:
    """BlobConvertOperation._convert_msgs: MSG i LOB-mappen erstattes av PDF
    (<stamme>.msg.pdf, eller <rot>.bin + konverteringsregister ved «Standardiser
    .bin»); feil → originalen beholdes som .msg."""
    print("test_blob_convert_integration")
    import threading
    import fitz
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    calls: list = []
    orig = M._lo_to_pdf
    M._lo_to_pdf = _fake_lo(calls)
    logs: list = []
    w = lambda msg, lvl="info": logs.append((lvl, msg))
    try:
        for standardize in (False, True):
            ext_dir = tmp / f"ext_{standardize}"
            lob = ext_dir / "content/schema0/table0/lob3"
            lob.mkdir(parents=True)
            (lob / "record1.bin").write_bytes(_build_msg(
                html="<p>Hei</p>", attachments=[("rapport.doc", _WORD_LIKE)]))
            op = BlobConvertOperation()
            stats = {"converted": 0, "failed": 0, "kept": 0}
            reg: dict = {}
            op._convert_msgs(
                [(0, "content/schema0/table0/lob3/record1.bin", "msg", "application/vnd.ms-outlook")],
                stats, ext_dir, "soffice", threading.Event(), None, None, w,
                threading.Lock(), 2, reg, threading.Lock(), standardize)
            names = sorted(f.name for f in lob.iterdir())
            if standardize:
                _ok(names == ["record1.bin"], f"standardiser .bin: {names}")
                _ok("record1.bin" in reg and "msg til .pdf" in reg["record1.bin"][1],
                    f"konverteringsregister: {reg}")
            else:
                _ok(names == ["record1.msg.pdf"], f"uten standardisering: {names}")
            out = lob / names[0]
            with fitz.open(str(out)) as d:
                _ok(d.page_count == 2 and d.embfile_count() == 0, "PDF (2 sider, 2b uten innebygd fil)")
            _ok(stats["converted"] == 1 and stats.get("msg_converted") == 1, f"statistikk {stats}")
        # PDF/A-3 valgt i oppsettet → original innebygd
        ext_dir = tmp / "ext_3b"
        lob = ext_dir / "content/schema0/table0/lob3"; lob.mkdir(parents=True)
        (lob / "record2.bin").write_bytes(_build_msg(text="Hei"))
        op = BlobConvertOperation(); op.params["pdfa_version"] = "PDF/A-3b (ISO 19005-3, level B)"
        stats = {"converted": 0, "failed": 0, "kept": 0}
        op._convert_msgs([(0, "content/schema0/table0/lob3/record2.bin", "msg", "")], stats, ext_dir,
                         "soffice", threading.Event(), None, None, w, threading.Lock(), 1, {}, threading.Lock(), False)
        with fitz.open(str(lob / "record2.msg.pdf")) as d:
            _ok(d.embfile_count() == 1, "PDF/A-3 i oppsettet → original MSG innebygd")
        _ok(any("PDF/A-3b" in m and "innebygd original" in m for _, m in logs), "logg nevner nivå og innebygd original")
        # Feil → original beholdes som .msg (LO og PyMuPDF-reserven feiler)
        M._lo_to_pdf = lambda *a, **k: None
        _orig_fb = M._header_pdf_fallback
        M._header_pdf_fallback = lambda *a, **k: False
        ext_dir = tmp / "ext_fail"
        lob = ext_dir / "content/schema0/table0/lob3"; lob.mkdir(parents=True)
        (lob / "record3.bin").write_bytes(_build_msg(text="Hei"))
        stats = {"converted": 0, "failed": 0, "kept": 0}
        BlobConvertOperation()._convert_msgs(
            [(0, "content/schema0/table0/lob3/record3.bin", "msg", "")], stats, ext_dir, "soffice",
            threading.Event(), None, None, w, threading.Lock(), 1, {}, threading.Lock(), False)
        _ok(sorted(f.name for f in lob.iterdir()) == ["record3.msg"] and stats["failed"] == 1,
            "feil → originalen beholdes som .msg, telles som feilet")
        M._header_pdf_fallback = _orig_fb
    finally:
        M._lo_to_pdf = orig


# ── EML, bilder, LibreOffice-profil, Ghostscript ─────────────────────────────

def _build_eml() -> bytes:
    from email.message import EmailMessage
    inner = EmailMessage()
    inner["From"] = "Per Berg <per@x.no>"; inner["To"] = "post@kommune.no"
    inner["Subject"] = "Opprinnelig henvendelse"; inner["Date"] = "Tue, 03 Sep 2024 10:00:00 +0200"
    inner.set_content("Hei, status i saken?")
    m = EmailMessage()
    m["From"] = "Kari Nordmann <kari@marnardal.kommune.no>"
    m["To"] = "Ola Hansen <ola@x.no>"; m["Cc"] = "Per Berg <per@x.no>"
    m["Subject"] = "Befaring og bilder"; m["Date"] = "Wed, 04 Sep 2024 14:30:00 +0200"
    m["Message-ID"] = "<abc@kommune.no>"; m["MIME-Version"] = "1.0"
    m.set_content("Se vedlagte bilder.")
    m.add_alternative("<p>Se vedlagte <b>bilder</b>.</p>", subtype="html")
    import io as _io
    from PIL import Image
    b = _io.BytesIO(); Image.new("RGB", (1200, 600), (200, 100, 50)).save(b, "PNG")
    m.add_attachment(b.getvalue(), maintype="image", subtype="png", filename="befaring.png")
    m.add_attachment(inner)
    return bytes(m)


def test_eml_and_images(tmp: Path) -> None:
    print("test_eml_and_images")
    import fitz
    data = _build_eml()
    _ok(M.is_eml_bytes(data) and _detect(data)[0] == "eml", "EML gjenkjent (is_eml_bytes + _detect)")
    _ok(not M.is_eml_bytes(b"Journalnotat 4\n\nSamtale med ..."), "vanlig tekst er ikke EML")
    _ok(not M.is_eml_bytes(b"Subject: hei\nDette er en tekst"), "ett hode alene er ikke EML")
    m = M.parse_eml(data)
    _ok(m.subject == "Befaring og bilder" and m.sender.startswith("Kari Nordmann")
        and m.to == ["Ola Hansen <ola@x.no>"] and m.cc == ["Per Berg <per@x.no>"]
        and m.date is not None and "<b>bilder</b>" in m.body_html, "EML-hoder og HTML-kropp")
    _ok([a.name for a in m.attachments][0] == "befaring.png" and m.attachments[1].embedded is not None
        and m.attachments[1].embedded.subject == "Opprinnelig henvendelse",
        "EML-vedlegg: bilde + vedlagt e-post (message/rfc822)")
    img_pdf = tmp / "bilde.pdf"
    _ok(M._image_to_pdf(m.attachments[0].data, "png", img_pdf), "bilde → PDF uten LibreOffice")
    with fitz.open(str(img_pdf)) as d:
        _ok(d.page_count == 1 and d[0].rect.width > d[0].rect.height, "liggende bilde → liggende A4")
    calls: list = []
    orig = M._lo_to_pdf
    M._lo_to_pdf = _fake_lo(calls)
    try:
        src = tmp / "record9.eml"; src.write_bytes(data)
        r = M.convert_msg_to_pdf(src, tmp / "eml.pdf", "soffice", work_dir=tmp / "weml")
        _ok(r.ok and r.kind == "eml" and [a.status for a in r.attachments] == ["konvertert", "konvertert"],
            f"EML → PDF: {r.summary}")
        r3 = M.convert_msg_to_pdf(src, tmp / "eml3.pdf", "soffice", "PDF/A-3b (ISO 19005-3, level B)",
                                  work_dir=tmp / "weml3")
        with fitz.open(str(tmp / "eml3.pdf")) as d:
            _ok(d.embfile_names() == ["record9.eml"] and "original EML innebygd" in r3.summary,
                "PDF/A-3: original EML innebygd")
    finally:
        M._lo_to_pdf = orig


def _fake_soffice(tmp: Path) -> str:
    """Falsk soffice: lager PDF, men HENGER når profilen er brukt før (som LO 26.2)."""
    script = tmp / "fake_soffice.py"
    script.write_text(
        "import sys, time, pathlib, urllib.parse, urllib.request\n"
        "a = sys.argv[1:]\n"
        "prof = next(x.split('=',1)[1] for x in a if x.startswith('-env:UserInstallation='))\n"
        "p = pathlib.Path(urllib.request.url2pathname(urllib.parse.urlparse(prof).path))\n"
        "mark = p / 'brukt'\n"
        "if mark.exists():\n"
        "    time.sleep(60)\n"
        "p.mkdir(parents=True, exist_ok=True); mark.write_text('x')\n"
        "out = pathlib.Path(a[a.index('--outdir')+1]); out.mkdir(parents=True, exist_ok=True)\n"
        "src = pathlib.Path(a[-1]); (out / (src.stem + '.pdf')).write_bytes(b'%PDF-1.4 falsk')\n",
        "utf-8")
    if sys.platform == "win32":
        cmd = tmp / "fake_soffice.cmd"
        cmd.write_text(f'@"{sys.executable}" "{script}" %*\n', "utf-8")
        return str(cmd)
    sh = tmp / "fake_soffice.sh"
    sh.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n', "utf-8")
    sh.chmod(0o755)
    return str(sh)


def test_lo_runner(tmp: Path) -> None:
    print("test_lo_runner")
    import time as _t
    import settings
    from siard_workflow.core import lo_runner as R
    # Ikke skriv til brukerens config.json (gjenbruksresultat lagres der)
    _store: dict = {}
    _og, _os = settings.get_config, settings.set_config
    settings.get_config = lambda k, d=None: _store.get(k, d if d is not None else {})
    settings.set_config = lambda k, v: _store.__setitem__(k, v)
    t0 = _t.monotonic()
    rc, _o, _e, to = R.run_lo([sys.executable, "-c", "import time; time.sleep(30)"], 1.5)
    _ok(to and rc is None and _t.monotonic() - t0 < 15, "tidsavbrudd: prosessen drepes raskt")
    fake = _fake_soffice(tmp)
    R.reset_session_cache()
    ok, err, reuse = R.health_check(fake, tmp / "hc", timeout=30, reuse_timeout=3)
    _ok(ok and not reuse and not R.profile_reuse_ok(),
        "helsesjekk oppdager at gjenbruk av profil henger → ny profil per kall")
    src = tmp / "a.txt"; src.write_text("hei", "utf-8")
    t0 = _t.monotonic()
    res = [R.convert(fake, src, tmp / f"o{i}", "pdf", tmp / "delt_profil", 10)[0] for i in range(3)]
    _ok(all(res) and _t.monotonic() - t0 < 25, "tre kall med samme profilmappe lykkes (profil tømmes)")
    ok2, _e2, reuse2 = R.health_check(fake, tmp / "hc2", timeout=30, reuse_timeout=3)
    _ok(ok2 and reuse2 is False, "resultatet gjenbrukes i økten (ingen ny 45 s-test)")
    R.set_profile_reuse_ok(True)
    t0 = _t.monotonic()
    ok3, err3 = R.convert(fake, src, tmp / "o9", "pdf", tmp / "delt_profil", 3)
    _ok(not ok3 and "tidsavbrudd" in err3, "med gjenbruk på: andre kall henger og avbrytes")
    R.reset_session_cache()
    settings.get_config, settings.set_config = _og, _os


def test_ghostscript_normalization(tmp: Path) -> None:
    gs = _REAL_ACTIVE_GS()
    if not gs:
        print("test_ghostscript_normalization (hoppet over — Ghostscript ikke funnet)")
        return
    print("test_ghostscript_normalization")
    import fitz
    GS.active_ghostscript = lambda: gs
    calls: list = []
    orig = M._lo_to_pdf
    M._lo_to_pdf = _fake_lo(calls)
    try:
        existing_pdf = fitz.open(); existing_pdf.new_page(); pdf_bytes = existing_pdf.tobytes(); existing_pdf.close()
        src = tmp / "gs.msg"
        src.write_bytes(_build_msg(html="<p>Hei</p>", attachments=[("kart.pdf", pdf_bytes)]))
        r = M.convert_msg_to_pdf(src, tmp / "gs2.pdf", "soffice", work_dir=tmp / "wgs")
        _ok(r.ok and r.pdfa_normalized and not any("PDF/A-kontroll" in w for w in r.warnings),
            f"Ghostscript normaliserte, advarsel om PDF-vedlegg borte ({r.summary})")
        _ok(M.pdfa_identification(tmp / "gs2.pdf") == ("2", "B"), "pdfaid 2B etter Ghostscript")
        r3 = M.convert_msg_to_pdf(src, tmp / "gs3.pdf", "soffice", "PDF/A-3b (ISO 19005-3, level B)",
                                  work_dir=tmp / "wgs3")
        with fitz.open(str(tmp / "gs3.pdf")) as d:
            _ok(r3.pdfa_normalized and d.embfile_names() == ["gs.msg"]
                and M.pdfa_identification(tmp / "gs3.pdf") == ("3", "B"),
                "PDF/A-3: normalisert og original innebygd etterpå")
    finally:
        M._lo_to_pdf = orig
        GS.active_ghostscript = lambda: None


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="msg2pdf_test_") as td:
        tmp = Path(td)
        test_detection()
        test_parse()
        test_convert(tmp)
        test_real_sample(tmp)
        test_blob_convert_integration(tmp)
        test_eml_and_images(tmp)
        test_lo_runner(tmp)
        test_ghostscript_normalization(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
