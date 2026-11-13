"""
Rettinger etter gjennomgang av kjøreloggen for 1504 Ålesund Profil (1.4.22).

  1. Ødelagt filnavn («1504_Ã\\x85lesund»): SHA-256-fila skrives (UTF-8), og
     kjørerapporten tåler at sjekksummen ligger som streng i resultatene.
  2. PREMIS: fixity er selve sjekksummen også etter fullført SHA-256-steg.
  3. Preflight «Filnavn (tegnkoding)» finner og retter ødelagte navn.
  4. Ventetid på dialoger står i kjøreloggen.
  5. Ghostscript-innsamlingen leser ingen PDF-er når etterbehandlingen er av.
  6. Opprydding (rmtree_progress), LOB-telling i HEX-steget og LOB-kontrollen
     melder fremdrift.

Kjør:  python -X utf8 tests/test_run_review_fixes.py
"""
from __future__ import annotations

import os
import queue
import stat
import subprocess
import sys
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

BROKEN = "1504_Ã\x85lesund_Profil"          # «Å» som UTF-8 lest som Latin-1


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_sha256_broken_name(tmp: Path) -> None:
    print("test_sha256_broken_name")
    siard = tmp / f"{BROKEN}.siard"
    siard.write_bytes(b"PK\x03\x04innhold")
    # Uten UTF-8-modus, slik programmet kjører på Windows (cp1252)
    code = (
        "import sys; sys.path.insert(0, sys.argv[1])\n"
        "from pathlib import Path\n"
        "from siard_workflow.core.context import WorkflowContext\n"
        "from siard_workflow.operations.sha256_operation import SHA256Operation\n"
        "r = SHA256Operation().run(WorkflowContext(siard_path=Path(sys.argv[2])))\n"
        "print('OK' if r.success else 'FEIL')\n"
    )
    env = dict(os.environ, PYTHONUTF8="0")
    p = subprocess.run([sys.executable, "-X", "utf8=0", "-c", code, str(_ROOT), str(siard)],
                       capture_output=True, text=True, env=env, timeout=120)
    _ok(p.returncode == 0 and p.stdout.strip() == "OK",
        f"SHA-256 lykkes med «Ã»+U+0085 i navnet uten UTF-8-modus ({p.stderr.strip()[-200:]})")
    side = siard.with_suffix(".sha256").read_bytes().decode("utf-8")
    _ok(side.rstrip("\r\n").endswith(f"  {siard.name}") and len(side.split()[0]) == 64,
        "sjekksumfila er skrevet som UTF-8 med filnavnet")


def test_report_tolerates_string_result(tmp: Path) -> None:
    print("test_report_tolerates_string_result")
    from siard_workflow.operations.workflow_report_operation import _generate_pdf
    steps = [{"id": "sha256", "label": "SHA-256 Sjekksum", "success": False,
              "message": "FEIL: 'charmap' codec …", "elapsed": 1.0}]
    for name, val in (("streng", "ab" * 32), ("dict", {"sha256": "cd" * 32})):
        pdf = tmp / f"rapport_{name}.pdf"
        _generate_pdf(pdf, tmp / f"{BROKEN}.siard", steps, {"sha256": val})
        _ok(pdf.exists() and pdf.stat().st_size > 0,
            f"kjørerapporten lages når sjekksummen ligger som {name}")


def test_premis_fixity_after_success(tmp: Path) -> None:
    print("test_premis_fixity_after_success")
    from xml.etree import ElementTree as ET
    from siard_workflow.core.base_operation import BaseOperation
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.core.premis_logger import PremisProvenanceLogger

    class _Op(BaseOperation):
        operation_id = "fake_mutate"
        label = "Fake"
        modifies_content = True
        premis_event_type = "Migration"
        premis_event_label = "fake"

        def run(self, ctx):
            return self._ok({}, "endret")

    siard = tmp / "U.siard"
    siard.write_bytes(b"PK")
    ctx = WorkflowContext(siard_path=siard)
    # Slik app.py lagrer resultatet etter et fullført SHA-256-steg
    ctx.results["sha256"] = {"sha256": "ef" * 32, "file": str(siard)}
    pl = PremisProvenanceLogger(tmp, siard)
    op = _Op()
    pl.record(op, op.run(ctx), ctx)
    out = pl.finalize(siard, ctx)
    ns = {"premis": "http://arkivverket.no/standarder/PREMIS"}
    d = ET.parse(out).getroot().find(".//premis:fixity/premis:messageDigest", ns)
    _ok(d is not None and d.text == "ef" * 32,
        "PREMIS-fixity er sjekksummen, ikke resultat-tabellen")


def test_filename_check() -> None:
    print("test_filename_check")
    from siard_workflow.core.filename_check import has_control_chars, repair_mojibake
    _ok(repair_mojibake(f"{BROKEN}.siard") == "1504_Ålesund_Profil.siard",
        "«Ã»+U+0085 (Latin-1) → «Å»")
    _ok(repair_mojibake("1504_Ã…lesund.siard") == "1504_Ålesund.siard",
        "«Ã…» (Windows-1252) → «Å»")
    _ok(repair_mojibake("Ã˜rsta_Ã¦rlig_Ã¸.siard") == "Ørsta_ærlig_ø.siard",
        "Ø, æ og ø rettes også")
    _ok(repair_mojibake("1504_Ålesund.siard") is None
        and repair_mojibake("Uttrekk_2024.siard") is None,
        "riktige navn røres ikke")
    _ok(has_control_chars(BROKEN) and not has_control_chars("1504_Ålesund"),
        "kontrolltegn oppdages")


def test_filename_preflight(tmp: Path) -> None:
    print("test_filename_preflight")
    import gui.app as A

    answers = {"svar": "ja"}

    class _Dlg:
        def __init__(self, parent, message, **kw):
            self.message = message
            self.result = answers["svar"]

        def title(self, _t):
            pass

    orig = A._PipelineSuggestionDialog
    A._PipelineSuggestionDialog = _Dlg
    try:
        broken = tmp / f"{BROKEN}.siard"
        broken.write_bytes(b"PK")
        ok_file = tmp / "Riktig.siard"
        ok_file.write_bytes(b"PK")
        logs: list = []
        fake = SimpleNamespace(siard_queue=[broken, ok_file], siard_path=broken,
                               _pf_status=None)
        fake._log = lambda m, lvl="info": logs.append(m)
        fake._pf = lambda s: setattr(fake, "_pf_status", s)
        fake._queue_render = lambda: None
        fake.wait_window = lambda d: None
        fake._project_path = None
        fake._rename_project_for = A.App._rename_project_for.__get__(fake)
        from siard_workflow.core.project_file import ProjectFile
        ProjectFile.from_ops(broken, []).save(broken.with_suffix(".siardwf"))
        run = A.App._filename_preflight.__get__(fake)

        answers["svar"] = "nei"
        _ok(run([]) and broken.exists() and "beholdt" in fake._pf_status,
            "«Nei»: fortsetter uten å endre navnet")
        answers["svar"] = "avbryt"
        _ok(run([]) is False, "«Avbryt» stopper kjøringen")
        answers["svar"] = "ja"
        _ok(run([]), "«Ja»: fortsetter")
        fixed = tmp / "1504_Ålesund_Profil.siard"
        _ok(fixed.exists() and not broken.exists()
            and fake.siard_queue == [fixed, ok_file] and fake.siard_path == fixed,
            "fila har fått riktig navn, og køen og aktiv fil er oppdatert")
        _pfw = fixed.with_suffix(".siardwf")
        _ok(_pfw.exists() and not broken.with_suffix(".siardwf").exists()
            and ProjectFile.load(_pfw).source_siard == str(fixed),
            "prosjektfila følger med og peker på det nye navnet")
        fake._pf_status = None
        _ok(run([]) and fake._pf_status is None, "ingen funn når navnene er riktige")
    finally:
        A._PipelineSuggestionDialog = orig


def test_remote_wait_logged() -> None:
    print("test_remote_wait_logged")
    import gui.app as A
    fake = SimpleNamespace(_remote_hub=None, _log_queue=queue.Queue())
    done = threading.Event()
    done.set()
    A.App._remote_wait(fake, done, "Legge til Standardiser filendelser?", "tekst",
                       [("ja", "Ja"), ("nei", "Nei")], lambda k: True)
    msgs = []
    while not fake._log_queue.empty():
        msgs.append(fake._log_queue.get()[1])
    _ok(any("Venter på svar: Legge til Standardiser filendelser?" in m for m in msgs),
        "ventingen på et svar står i kjøreloggen")


def test_gs_collect_off(tmp: Path) -> None:
    print("test_gs_collect_off")
    from siard_workflow.core import file_integrity, ghostscript
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    pdf = tmp / "a.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF")
    calls = []
    orig_signed, orig_gs = file_integrity.is_signed_pdf, ghostscript.active_ghostscript
    file_integrity.is_signed_pdf = lambda p: (calls.append(p), False)[1]
    try:
        op = BlobConvertOperation()
        op._pp_produced = [pdf] * 3
        op._pp_system = [pdf]
        logs: list = []
        w = lambda m, lvl="info": logs.append(m)
        op.params["gs_postprocess"] = "Av"
        op._gs_postprocess_collect({}, w, 2, tmp / "gs", threading.Event())
        _ok(not calls and not logs, "«Av»: ingen PDF-er leses, ingen meldinger")
        ghostscript.active_ghostscript = lambda: None
        op.params["gs_postprocess"] = "ja"
        op._gs_postprocess_collect({}, w, 2, tmp / "gs", threading.Event())
        _ok(not calls and any("ikke funnet" in m for m in logs),
            "uten Ghostscript: ingen PDF-er leses, loggen sier hvorfor")
    finally:
        file_integrity.is_signed_pdf, ghostscript.active_ghostscript = orig_signed, orig_gs


def test_rmtree_progress(tmp: Path) -> None:
    print("test_rmtree_progress")
    from siard_workflow.core import zip_progress as Z
    root = tmp / "temp_pipeline"
    for i in range(3):
        d = root / "content" / "schema0" / f"table{i}" / "lob1"
        d.mkdir(parents=True)
        for j in range(10):
            (d / f"record{j}.bin").write_bytes(b"x")
    ro = root / "content" / "schema0" / "table0" / "lob1" / "record0.bin"
    os.chmod(ro, stat.S_IREAD)
    logs: list = []
    orig = Z.RMTREE_LOG_INTERVAL_S
    Z.RMTREE_LOG_INTERVAL_S = 0
    try:
        n = Z.rmtree_progress(root, lambda m, lvl="info": logs.append(m),
                              "Rydder temp-mappe", total=30)
    finally:
        Z.RMTREE_LOG_INTERVAL_S = orig
    _ok(n == 30 and not root.exists(), "alle filer (også skrivebeskyttede) og mapper slettet")
    _ok(logs[0].startswith("  Rydder temp-mappe:") and any("filer slettet" in m for m in logs),
        "start og fremdrift står i loggen")
    _ok(Z.rmtree_progress(root) == 0, "manglende mappe: ingenting å gjøre")


def test_hex_lob_count(tmp: Path) -> None:
    print("test_hex_lob_count")
    from siard_workflow.operations.hex_extract_operation import _count_lob_files
    t = tmp / "x" / "content" / "schema0" / "table0"
    (t / "lob1").mkdir(parents=True)
    (t / "lob2").mkdir()
    (t / "table0.xml").write_text("<x/>")
    for i in range(4):
        (t / "lob1" / f"record{i}.bin").write_bytes(b"1")
    (t / "lob2" / "record0.txt").write_text("t")
    root = tmp / "x"
    old = sum(1 for f in root.rglob("*") if f.is_file() and "lob" in f.parent.name.lower())
    _ok(_count_lob_files(root) == old == 5, "samme antall som før (5 LOB-filer)")


def test_lob_check_progress(tmp: Path) -> None:
    print("test_lob_check_progress")
    from siard_workflow.core import lob_column_types as L
    logs: list = []
    w = lambda m, lvl="info": logs.append(m)
    L.reconcile_lob_column_types(tmp / "tom", w)
    _ok(any("Kontrollerer LOB-referanser og LOB-kolonnetyper" in m for m in logs),
        "LOB-kontrollen melder at den starter")
    big = tmp / "table350.xml"
    big.write_bytes(b"x" * 2048)
    orig = L._BIG_TABLE_XML
    L._BIG_TABLE_XML = 1024
    try:
        L._note_big_table(w, "Kontrollerer LOB-referanser", big, "schema0", "table350")
    finally:
        L._BIG_TABLE_XML = orig
    _ok(any("schema0/table350 (2,0 kB)" in m for m in logs),
        "store tabeller meldes før de leses")


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        test_sha256_broken_name(tmp)
        test_report_tolerates_string_result(tmp)
        test_premis_fixity_after_success(tmp)
        test_filename_check()
        test_filename_preflight(tmp)
        test_remote_wait_logged()
        test_gs_collect_off(tmp)
        test_rmtree_progress(tmp)
        test_hex_lob_count(tmp)
        test_lob_check_progress(tmp)
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
