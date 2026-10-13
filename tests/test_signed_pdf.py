"""
Regel: en digitalt signert PDF skal ALDRI røres av konvertering — enhver
omskriving gjør signaturen ugyldig.

  * gjenkjenning (file_integrity.is_signed_pdf) via /ByteRange
  * BLOB-konvertering: signert PDF beholdes byte for byte, kategori «signert»
  * e-post med signert PDF-vedlegg (også i vedlagt e-post) → zip-fil med
    e-posten som PDF/A og de signerte PDF-ene byte for byte urørt

Testfilene er syntetiske (ingen ekte dokumenter).

Kjør:  python -X utf8 tests/test_signed_pdf.py
"""
from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for p in (_ROOT, _ROOT / "tests"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _pdf(text: str = "Avtale") -> bytes:
    import fitz
    d = fitz.open()
    d.new_page().insert_text((72, 72), text)
    return d.tobytes()


def _signed_pdf() -> bytes:
    """Usignert PDF + inkrementell oppdatering med signaturordbok (som ved
    signering i Acrobat: /ByteRange dekker alt unntatt /Contents)."""
    base = _pdf("Signert avtale")
    sig = (b"\n99 0 obj\n<</Type/Sig/Filter/Adobe.PPKLite/SubFilter/adbe.pkcs7.detached"
           b"/ByteRange [0 1234 5678 900]/Contents <3082" + b"0" * 64 + b">"
           b"/M(D:20261012120000+02'00')>>\nendobj\n%%EOF\n")
    return base + sig


def test_detection(tmp: Path) -> None:
    print("test_detection")
    from siard_workflow.core.file_integrity import is_signed_pdf
    s, u = _signed_pdf(), _pdf()
    _ok(is_signed_pdf(s), "signert PDF gjenkjennes (bytes)")
    f = tmp / "signert.bin"
    f.write_bytes(s)
    _ok(is_signed_pdf(f), "signert PDF gjenkjennes (fil)")
    _ok(not is_signed_pdf(u), "usignert PDF regnes ikke som signert")
    _ok(not is_signed_pdf(b"/ByteRange [0 1 2 3] men ikke PDF"), "ikke-PDF regnes ikke som signert")


def _siard(root: Path, files: dict, refs: list) -> Path:
    from test_failure_report import _make_siard
    return _make_siard(root, files, refs)


def _run_blob(root: Path, tmp: Path):
    import settings
    settings.set_config = lambda k, v: None
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    logs: list = []
    ctx = WorkflowContext(siard_path=tmp / "Sig.siard", metadata={
        "file_logger": None,
        "progress_cb": lambda ev, **k: logs.append(k.get("msg", "")) if ev == "log" else None,
        "stop_event": threading.Event(), "pause_event": threading.Event()})
    ctx.extracted_path = root
    op = BlobConvertOperation()
    op.params["skip_existing_pdf"] = False      # selv uten «hopp over PDF»
    return op.run(ctx), logs


def test_blob_signed_pdf(tmp: Path) -> None:
    print("test_blob_signed_pdf")
    signed = _signed_pdf()
    root = tmp / "b"
    lob = _siard(root, {"record1.bin": signed}, ["record1.bin"])
    r, logs = _run_blob(root, tmp)
    out = [p for p in lob.iterdir() if p.is_file()]
    _ok(r.success and len(out) == 1 and out[0].read_bytes() == signed,
        f"signert PDF beholdt byte for byte ({out[0].name})")
    fs = r.data.get("failure_summary") or {}
    _ok(fs.get("counts", {}).get("signert") == 1 and not fs["counts"].get("konverteringsfeil"),
        f"telles som «signert», ikke som feil ({fs.get('counts')})")


def test_mail_with_signed_attachment(tmp: Path) -> None:
    print("test_mail_with_signed_attachment")
    from test_msg_to_pdf import _build_msg
    from siard_workflow.core import msg_to_pdf as M
    signed = _signed_pdf()
    msg = _build_msg(subject="Signert kontrakt", html="<p>Se vedlegg</p>",
                     attachments=[("kontrakt.pdf", signed)])
    f = tmp / "m.msg"
    f.write_bytes(msg)
    res = M.convert_msg_to_pdf(f, tmp / "m.pdf", "soffice-finnes-ikke", timeout=10)
    _ok(not res.ok and res.signed_attachment == "kontrakt.pdf" and not (tmp / "m.pdf").exists(),
        f"e-post med signert vedlegg konverteres ikke ({res.error[:60]}…)")
    eml = (b"From: a@example.no\r\nTo: b@example.no\r\nSubject: Videresendt\r\n"
           b"Date: Mon, 12 Oct 2026 10:00:00 +0200\r\nMIME-Version: 1.0\r\n"
           b"Content-Type: multipart/mixed; boundary=XX\r\n\r\n--XX\r\n"
           b"Content-Type: text/plain\r\n\r\nSe vedlagt e-post\r\n--XX\r\n"
           b"Content-Type: application/vnd.ms-outlook; name=\"kontrakt.msg\"\r\n"
           b"Content-Disposition: attachment; filename=\"kontrakt.msg\"\r\n"
           b"Content-Transfer-Encoding: base64\r\n\r\n")
    import base64
    eml += base64.encodebytes(msg) + b"--XX--\r\n"
    g = tmp / "v.eml"
    g.write_bytes(eml)
    res2 = M.convert_msg_to_pdf(g, tmp / "v.pdf", "soffice-finnes-ikke", timeout=10)
    _ok(not res2.ok and "kontrakt.pdf" in res2.signed_attachment,
        f"signert PDF i vedlagt e-post oppdages ({res2.signed_attachment})")
    plain = _build_msg(subject="Usignert", html="<p>x</p>", attachments=[("a.pdf", _pdf())])
    ok_msg = M.parse_email_file(_write(tmp / "u.msg", plain))[0]
    _ok(M.find_signed_pdf_attachment(ok_msg) == "", "usignert PDF-vedlegg stopper ikke konvertering")

    # Gjennom BLOB-konverteringen (krever LibreOffice for helsesjekken)
    import settings
    from siard_workflow.core.libreoffice import find_libreoffice
    if not find_libreoffice(settings.get_config("lo_executable", "") or "soffice"):
        print("  (LibreOffice ikke funnet — BLOB-delen hoppet over)")
        return
    import zipfile, io
    from siard_workflow.core.msg_to_pdf import pdfa_identification
    import fitz
    other = _pdf("Vanlig vedlegg")
    msg2 = _build_msg(subject="Signert kontrakt", html="<p>Se vedlegg</p>",
                      attachments=[("kontrakt.pdf", signed), ("notat.pdf", other)])
    root = tmp / "m"
    lob = _siard(root, {"record1.bin": msg2}, ["record1.bin"])
    r, logs = _run_blob(root, tmp)
    out = [p for p in lob.iterdir() if p.is_file()]
    _ok(len(out) == 1 and zipfile.is_zipfile(out[0]), f"e-posten er blitt en zip-fil ({out[0].name})")
    with zipfile.ZipFile(out[0]) as z:
        names = z.namelist()
        # Testens MSG-skriver fyller strømmer opp til 4096 byte — sammenlign
        # med vedlegget slik det faktisk ligger lagret i e-posten
        stored = M.parse_email_file(_write(tmp / "m2.msg", msg2))[0].attachments[0].data
        _ok("kontrakt.pdf" in names and z.read("kontrakt.pdf") == stored
            and stored.startswith(signed),
            f"signert PDF ligger byte for byte urørt i zip-filen ({names})")
        mail = next(n for n in names if n != "kontrakt.pdf")
        (tmp / "epost.pdf").write_bytes(z.read(mail))
    _ok(pdfa_identification(tmp / "epost.pdf") is not None, f"e-posten ({mail}) er PDF/A")
    with fitz.open(str(tmp / "epost.pdf")) as d:
        text = " ".join(pg.get_text() for pg in d)
    _ok("ligger urørt i zip-filen" in text and "Vanlig vedlegg" in text,
        "toppsiden viser at den signerte PDF-en ligger i zip-filen; øvrige vedlegg er med")
    _ok(r.data.get("msg_signed_zip") == 1 and r.data.get("failed", 0) == 0,
        "telles som konvertert e-post (ZIP), ikke som feil")
    _ok(any("signert PDF urørt" in m for m in logs), "loggen forklarer hva som skjedde")


def _write(p: Path, data: bytes) -> Path:
    p.write_bytes(data)
    return p


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="signert_") as td:
        tmp = Path(td)
        test_detection(tmp)
        test_blob_signed_pdf(tmp)
        test_mail_with_signed_attachment(tmp)
    print("\nAlle tester OK")
    import os
    os._exit(0)


if __name__ == "__main__":
    main()
