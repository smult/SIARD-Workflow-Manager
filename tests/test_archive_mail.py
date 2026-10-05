"""
E-post og dokumenter inne i arkiv-LOB-er (BLOB-konvertering, _process_archives):

  * MSG/EML i en zip → én PDF/A per melding via msg_to_pdf (som vanlige LOB-er,
    med Ghostscript når den finnes)
  * dokumenter i zip → valgt PDF/A-versjon (før: vanlig PDF)
  * zip i zip pakkes ut og behandles rekursivt
  * mappestruktur i arkivet bevares, like filnavn overskriver ikke hverandre

Krever LibreOffice (hoppes over uten). Bruker ekte e-post fra testsiard/rec*.bin
når de finnes, ellers en syntetisk EML.

Kjør:  python -X utf8 tests/test_archive_mail.py
"""
from __future__ import annotations

import io
import sys
import tempfile
import threading
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_EML = (b"From: Ola Nordmann <ola@example.no>\r\nTo: kari@example.no\r\n"
        b"Subject: Testmelding i arkiv\r\nDate: Mon, 1 Jan 2024 10:00:00 +0100\r\n"
        b"MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Hei Kari, her er referatet. Mvh Ola\r\n")


def _docx_bytes(text: str) -> bytes:
    import docx
    d = docx.Document()
    d.add_paragraph(text)
    b = io.BytesIO()
    d.save(b)
    return b.getvalue()


def _zip(entries: dict) -> bytes:
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return b.getvalue()


def _make_siard(root: Path, lob_bytes: bytes) -> Path:
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(
        '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd">'
        '<lobFolder>content</lobFolder><schemas><schema><folder>schema0</folder><tables><table>'
        '<folder>table0</folder><columns><column><name>id</name><type>INTEGER</type></column>'
        '<column><name>vedlegg</name><lobFolder>schema0/table0/lob2</lobFolder><type>BLOB</type></column>'
        '</columns><rows>1</rows></table></tables></schema></schemas></siardArchive>', "utf-8")
    lob = root / "content/schema0/table0/lob2"
    lob.mkdir(parents=True)
    (lob / "record1.bin").write_bytes(lob_bytes)
    (root / "content/schema0/table0/table0.xml").write_text(
        '<table>\n<row><c1>1</c1><c2 file="record1.bin"/></row>\n</table>\n', "utf-8")
    return lob


def main() -> int:
    import settings
    settings.set_config = lambda k, v: None          # ikke skriv brukerens config.json
    from siard_workflow.core.libreoffice import find_libreoffice
    lo = find_libreoffice(settings.get_config("lo_executable", "") or "soffice")
    if not lo:
        print("LibreOffice ikke funnet — test hoppet over")
        return 0
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.core.msg_to_pdf import pdfa_identification
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation

    msgs = sorted((_ROOT / "testsiard").glob("rec*.bin"))[:2]
    msg_a = msgs[0].read_bytes() if msgs else _EML
    msg_b = msgs[1].read_bytes() if len(msgs) > 1 else _EML
    a_ext = "msg" if msgs else "eml"
    inner_zip = _zip({"melding_i_indre.msg" if len(msgs) > 1 else "indre.eml": msg_b,
                      "notat.txt": "Bare tekst".encode()})
    lob_bytes = _zip({
        f"post/melding.{a_ext}": msg_a,
        "post/svar.eml": _EML,
        "dok/referat.docx": _docx_bytes("Referat fra møtet, æøå."),
        "dok2/referat.docx": _docx_bytes("Et annet referat med samme filnavn."),
        "vedlegg.zip": inner_zip,
    })

    with tempfile.TemporaryDirectory(prefix="arkivpost_") as td:
        root = Path(td) / "uttrekk"
        lob = _make_siard(root, lob_bytes)
        logs: list = []
        ctx = WorkflowContext(siard_path=Path(td) / "t.siard", metadata={
            "file_logger": None,
            "progress_cb": lambda ev, **k: logs.append(k.get("msg", "")) if ev == "log" else None,
            "stop_event": threading.Event(), "pause_event": threading.Event()})
        ctx.extracted_path = root
        op = BlobConvertOperation()
        op.params["libreoffice_bin"] = lo
        r = op.run(ctx)
        text = "\n".join(logs)
        _ok(r.success, f"BLOB-konvertering ok ({r.message})")

        out = [p for p in lob.iterdir() if p.is_file()]
        _ok(len(out) == 1, f"én LOB-fil igjen ({[p.name for p in out]})")
        with zipfile.ZipFile(out[0]) as z:
            names = sorted(z.namelist())
            print("    innhold:", names)
            _ok(f"post/melding.{a_ext}.pdf" in names and "post/svar.eml.pdf" in names,
                "e-post i arkivet → <navn>.<msg|eml>.pdf (mappe bevart)")
            _ok("dok/referat.pdf" in names and "dok2/referat.pdf" in names,
                "like filnavn i ulike mapper overskriver ikke hverandre")
            _ok(not any(n.endswith((".msg", ".eml", ".docx")) for n in names),
                "ingen ukonverterte e-poster/dokumenter igjen")
            _ok("vedlegg.zip" in names, "indre arkiv lagt tilbake som zip")
            for n in [n for n in names if n.endswith(".pdf")]:
                p = Path(td) / "ut" / n
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(z.read(n))
                ident = pdfa_identification(p)
                _ok(ident is not None, f"{n}: PDF/A ({ident})")
            inner = zipfile.ZipFile(io.BytesIO(z.read("vedlegg.zip")))
            inames = sorted(inner.namelist())
            _ok(any(n.endswith((".msg.pdf", ".eml.pdf")) for n in inames) and "notat.txt" in inames,
                f"e-post i zip-i-zip konvertert, tekst beholdt ({inames})")
        _ok("Arkiv i arkiv: vedlegg.zip" in text, "loggen viser arkiv i arkiv")
        _ok("E-post: post/" in text, "loggen viser e-postkonvertering i arkiv")
        if "Ghostscript" in text:
            print("    (Ghostscript brukt: «PDF/A-normalisert med Ghostscript» i e-postsammendraget)")
    print("\nAlle tester OK")
    import os
    os._exit(0)


if __name__ == "__main__":
    main()
