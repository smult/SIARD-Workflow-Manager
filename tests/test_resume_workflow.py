"""
Gjenopptakbar arbeidsflyt: prosjektfil v2, kjøreplan, BLOB-progresjon,
samlet kjørerapport og PREMIS på tvers av kjøringer.

  1. ProjectFile v2: v1-filer lastes, resultat renses til JSON, forsøk
     arkiveres, tilbakestilling, lagrede svar (fingeravtrykk), gjeldende logger.
  2. Kjøreplan (core/run_plan.py):
       - steg 4 og 8 av 10 feiler → bare de, + «Pakk sammen» og det som bygger
         på resultatet, kjøres (temp-mappa finnes)
       - fullført prosess, BLOB + rapport tilbakestilt, temp borte → forslag
         «original»; med «output» kjøres bare BLOB + pakking + avledede steg
       - bare SIARDMapper tilbakestilt → forslag «output»
       - bare rapporten tilbakestilt → bare rapporten kjøres
       - HEX kjøres på nytt → BLOB kjøres også (bygger på HEX)
       - steg som skal holdes igjen når et innholdssteg feiler
  3. BLOB-progresjon (core/blob_progress.py): samles over to avbrudd, annen
     utpakket mappe / endrede navneinnstillinger forkaster fila, register og
     statistikk, CSV-loggerens on_write.
  4. Kjørerapporten: steg fra tidligere kjøringer med data, «kjørt på nytt»,
     holdt igjen → foreløpig; PDF-en har seksjonen «Kjøringer».
  5. PREMIS: hendelser fra forrige kjøring beholdes.
  6. App-hjelpere: lagrede svar brukes uten å spørre, gjenoppretting av
     hoppede steg, filtyper summeres når BLOB fortsetter.

Kjør:  python -X utf8 tests/test_resume_workflow.py
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _ops(ids):
    from gui.operations_panel import OP_DEFS
    m = {d["cls"].operation_id: d["cls"] for d in OP_DEFS}
    return [m[i]() for i in ids]


_IDS = ["sha256", "unpack_siard", "hex_extract", "blob_convert", "siardmapper",
        "xsd_type_fix", "repack_siard", "sha256", "workflow_report", "dias_package"]


# ── 1. Prosjektfil v2 ─────────────────────────────────────────────────────────

def test_project_file_v2(tmp: Path) -> None:
    print("test_project_file_v2")
    from siard_workflow.core.project_file import (
        ProjectFile, sanitize_result, source_fingerprint)
    v1 = {"version": 1, "source_siard": str(tmp / "a.siard"), "created": "x",
          "updated": "y", "operations": [
              {"operation_id": "sha256", "label": "SHA", "params": {},
               "status": "completed", "output_siard": None,
               "completed_at": "2026-11-01T10:00:00", "error_msg": None,
               "ctx_data": {"sha256": "abc"}}]}
    (tmp / "a.siardwf").write_text(json.dumps(v1), encoding="utf-8")
    pf = ProjectFile.load(tmp / "a.siardwf")
    _ok(pf.operations[0].status == "completed" and pf.operations[0].attempts == [],
        "v1-fil lastes (nye felt får standardverdier)")

    data = sanitize_result({"p": tmp, "s": {3, 1}, "d": {"x": (1, 2)},
                            "original_namelist": ["a/", "a/b.xml", "c/"],
                            "failure_summary": {"entries": list(range(5000))}})
    json.dumps(data)
    _ok(data["p"] == str(tmp) and data["s"] == [1, 3] and data["d"]["x"] == [1, 2],
        "resultat renses til JSON (Path, set, tuple)")
    _ok(data["original_namelist"] == ["a/", "c/"], "original_namelist: bare kataloger")
    _ok(len(data["failure_summary"]["entries"]) == 1000, "feiloppføringer kortes ned")

    cp = pf.operations[0]
    cp.run_id = "r1"
    pf.mark_started(cp, "r2", "logg.log")
    pf.mark_failed(cp, "brakk", elapsed=3.0)
    _ok(cp.status == "failed" and len(cp.attempts) == 1
        and cp.attempts[0]["status"] == "completed" and cp.attempt_no == 2,
        "nytt forsøk arkiverer forrige (forsøk 2)")
    pf.mark_failed(cp, "stopp", interrupted=True)
    _ok(cp.status == "interrupted", "avbrudd lagres som «interrupted»")
    pf.reset(cp, "tilbakestilt av bruker")
    _ok(cp.status == "pending" and cp.reset_reason and len(cp.attempts) == 2,
        "tilbakestilling: pending + grunn, historikken beholdes")

    src = tmp / "a.siard"
    src.write_bytes(b"x")
    fp = source_fingerprint(src)
    pf.set_answer("blob_convert:gs_postprocess", True, fp)
    _ok(pf.get_answer("blob_convert:gs_postprocess", fp) is True, "lagret svar hentes")
    _ok(pf.get_answer("blob_convert:gs_postprocess", "9:9") is None,
        "endret kildefil → svaret gjelder ikke")
    _ok(pf.forget_answers("blob_convert:") == 1 and not pf.answers, "svar glemmes per steg")

    a, b2 = tmp / "a.csv", tmp / "b.csv"
    a.write_text("x"), b2.write_text("y")
    pf.add_failure_files("r1", blob_csv=a)
    pf.failure_files[0]["superseded"] = True
    pf.add_failure_files("r2", blob_csv=b2)
    _ok(pf.current_failure_files("blob_csv") == [str(b2)],
        "gjeldende logger: erstattede forsøk tas ikke med")

    rid = pf.start_run("l.log")
    pf.note_step_run(rid, "SHA", "completed")
    pf.end_run(rid, "ok")
    pf.save(tmp / "a.siardwf")
    pf2 = ProjectFile.load(tmp / "a.siardwf")
    _ok(pf2.runs and pf2.runs[0]["result"] == "ok" and pf2.operations[0].attempts,
        "kjøringer og forsøk lagres og lastes")


# ── 2. Kjøreplan ──────────────────────────────────────────────────────────────

def _plan(ops, pf, workdir_ok, out=None, base=None):
    from siard_workflow.core.run_plan import build_run_plan
    return build_run_plan(ops, pf.operations, workdir_ok, out, base)


def _actions(plan):
    return [s.action for s in plan.steps]


def test_run_plan(tmp: Path) -> None:
    print("test_run_plan")
    from siard_workflow.core.project_file import ProjectFile
    ops = _ops(_IDS)
    out = tmp / "a_konvertert.siard"
    out.write_bytes(b"x")

    def fresh():
        pf = ProjectFile.from_ops(tmp / "a.siard", ops)
        for cp in pf.operations:
            cp.status, cp.run_id = "completed", "r1"
        return pf

    pf = ProjectFile.from_ops(tmp / "a.siard", ops)
    p = _plan(ops, pf, False)
    _ok(all(s.action == "run" for s in p.steps), "ny kjøring: alle steg kjøres")

    pf = fresh()
    pf.operations[3].status = "failed"        # BLOB
    pf.operations[7].status = "failed"        # SHA-256 etter
    p = _plan(ops, pf, True)
    _ok(_actions(p) == ["skip", "skip", "skip", "run", "skip", "skip",
                        "rerun", "run", "rerun", "rerun"],
        "steg 4 og 8 feilet: bare de + pakking og avledede steg kjøres")
    _ok(p.held_on_content_failure == [6, 7, 9],
        "feiler et innholdssteg, holdes pakking/SHA-etter/DIAS igjen (ikke rapporten)")

    pf = fresh()
    pf.reset(pf.operations[3])
    pf.reset(pf.operations[8])
    p = _plan(ops, pf, False, str(out))
    _ok(p.base_choice and p.base_suggestion == "original",
        "BLOB tilbakestilt, temp borte: forslag «original-SIARD»")
    _ok(_actions(p)[1:7] == ["rerun"] * 2 + ["run"] + ["rerun"] * 3,
        "«original»: alle innholdssteg kjøres på nytt")
    p = _plan(ops, pf, False, str(out), base="output")
    _ok(_actions(p) == ["skip", "rerun", "skip", "run", "skip", "skip",
                        "rerun", "rerun", "run", "rerun"],
        "«siste resultat»: bare BLOB + pakking + avledede steg")

    pf = fresh()
    pf.reset(pf.operations[4])
    p = _plan(ops, pf, False, str(out))
    _ok(p.base == "output" and _actions(p)[4] == "run" and _actions(p)[3] == "skip",
        "bare SIARDMapper tilbakestilt: bygger på siste resultat, BLOB hoppes over")

    pf = fresh()
    pf.reset(pf.operations[8])
    p = _plan(ops, pf, False, str(out))
    _ok(_actions(p) == ["skip"] * 8 + ["run", "skip"],
        "bare rapporten tilbakestilt: bare rapporten kjøres")

    pf = fresh()
    pf.operations[2].status = "failed"
    for k in (6, 7, 9):
        pf.operations[k].status = "held"
    p = _plan(ops, pf, True)
    _ok(_actions(p)[2:4] == ["run", "rerun"] and "HEX" in p.steps[3].reason,
        "HEX kjøres på nytt → BLOB også (fortsetter med bare nye filer)")

    pf = fresh()
    pf.operations[4].status = "user_skip"
    pf.reset(pf.operations[5])
    p = _plan(ops, pf, True)
    _ok(p.steps[4].action == "user_skip", "«Hopp over i neste kjøring» respekteres")

    pf = fresh()
    p = _plan(ops, pf, False, str(out))
    _ok(p.nothing_to_do, "alt fullført: ingenting å kjøre")

    # Uten «Pakk ut»/«Pakk sammen»: gammel regel (alt etter første steg som må kjøres)
    chain = _ops(["sha256", "xsd_type_fix", "siardmapper", "workflow_report"])
    pf = ProjectFile.from_ops(tmp / "a.siard", chain)
    for cp in pf.operations:
        cp.status = "completed"
    pf.operations[1].status = "failed"
    p = _plan(chain, pf, False)
    _ok(p.mode == "chain" and _actions(p) == ["skip", "run", "rerun", "rerun"],
        "uten pipeline: steg etter første som må kjøres, kjøres også")


# ── 3. BLOB-progresjon ────────────────────────────────────────────────────────

def test_blob_progress(tmp: Path) -> None:
    print("test_blob_progress")
    from siard_workflow.core.blob_progress import (
        BlobProgress, PersistentRegistry, progress_path)
    from siard_workflow.core.blob_csv_logger import BlobCsvLogger
    ext = tmp / "siard_pipeline_x"
    lob = ext / "content" / "schema0" / "table1" / "lob3"
    lob.mkdir(parents=True)
    settings = {"standardize_bin_ext": True, "dbptk_lob_names": True}
    pp = progress_path(tmp, "uttrekk")

    # Kjøring 1 — avbrutt etter to filer
    bp = BlobProgress(pp, ext, settings)
    _ok(bp.load() == 0, "ingen progresjon før første kjøring")
    bp.open(append=False)
    csv = BlobCsvLogger(tmp, "uttrekk").__enter__()
    csv.on_write = bp.record_done
    (lob / "record1.bin").write_bytes(b"%PDF")
    csv.write("content/schema0/table1/lob3/record1.doc", 10, "doc",
              "record1.bin", 4, "bin", "Konvertert til PDF/A")
    reg = PersistentRegistry(bp)
    reg["record1.doc"] = (lob / "record1.bin", "Filinnhold : doc - endret til .bin")
    csv.write("content/schema0/table1/lob3/record2.bin", 5, "bin",
              "record2.bin", 5, "bin", "Beholdt; med\ttab")
    bp.record_stats({"converted": 1, "kept": 1, "failed": 0})
    bp.close()
    csv.__exit__(None, None, None)
    _ok(reg["record1.doc"][0] == lob / "record1.bin", "registeret virker som dict")

    # Kjøring 2 — fortsetter, én fil til, avbrutt igjen
    bp2 = BlobProgress(pp, ext, settings)
    _ok(bp2.load() == 2, "andre kjøring: 2 filer allerede behandlet")
    bp2.open(append=True)
    bp2.record_done("content/schema0/table1/lob3/record3.bin", "record3.bin", "x")
    bp2.record_stats({"converted": 0, "kept": 1, "failed": 1})
    bp2.close()

    bp3 = BlobProgress(pp, ext, settings)
    _ok(bp3.load() == 3, "tredje kjøring: ferdige filer samles over to avbrudd")
    tot = bp3.total_stats({"converted": 5, "kept": 0, "failed": 0})
    _ok(tot == {"converted": 6, "kept": 2, "failed": 1},
        "tellingene summeres over alle kjøringene")
    refs = bp3.resumed_refs()
    _ok(list(refs) == ["schema0/table1"]
        and refs["schema0/table1"][0][0] == "record1.doc"
        and refs["schema0/table1"][0][2] == lob / "record1.bin"
        and refs["schema0/table1"][0][3].startswith("Filinnhold"),
        "XML-retting: record1.doc → record1.bin med kommentar fra registeret")

    other = BlobProgress(pp, tmp / "siard_pipeline_y", settings)
    _ok(other.load() == 0 and "annen utpakket mappe" in other.discarded_reason,
        "ny utpakking (annen temp-mappe) → progresjonen brukes ikke")
    changed = BlobProgress(pp, ext, {**settings, "dbptk_lob_names": False})
    _ok(changed.load() == 0 and "innstillingene" in changed.discarded_reason,
        "endrede navneinnstillinger → progresjonen brukes ikke")


# ── 4. Samlet kjørerapport ────────────────────────────────────────────────────

def test_report_history(tmp: Path) -> None:
    print("test_report_history")
    from siard_workflow.core.project_file import ProjectFile
    from siard_workflow.operations.workflow_report_operation import (
        _generate_pdf, project_history)
    ops = _ops(["unpack_siard", "blob_convert", "siardmapper", "repack_siard",
                "workflow_report"])
    pf = ProjectFile.from_ops(tmp / "a.siard", ops)
    r1 = pf.start_run("a_20261101_100000.log")
    for i in range(4):
        cp = pf.operations[i]
        pf.mark_started(cp, r1)
        pf.mark_completed(cp, ctx_data={"converted": 7, "kept": 2, "failed": 0,
                                        "detected": 9} if i == 1 else {},
                          message=f"steg {i} ok", elapsed=10.0 * (i + 1))
    pf.mark_failed(pf.operations[2], "SIARDMapper feilet")
    pf.end_run(r1, "feil")
    r2 = pf.start_run("a_20261102_100000.log")
    pf.mark_started(pf.operations[2], r2)
    pf.mark_completed(pf.operations[2], message="beriket", elapsed=5.0)

    step_results = [
        {"id": "unpack_siard", "label": "Pakk ut", "skipped": True, "success": True,
         "message": "", "elapsed": 0.0, "index": 0},
        {"id": "blob_convert", "label": "BLOB", "skipped": True, "success": True,
         "message": "", "elapsed": 0.0, "index": 1},
        {"id": "siardmapper", "label": "SIARDMapper", "skipped": False, "success": True,
         "message": "beriket", "elapsed": 5.0, "index": 2},
        {"id": "repack_siard", "label": "Pakk sammen", "skipped": True, "success": None,
         "message": "Holdt igjen", "elapsed": 0.0, "index": 3},
    ]
    pf.mark_held(pf.operations[3], "«X» feilet")
    merged, hist = project_history(pf, r2, step_results, report_index=4)
    _ok(merged[1].get("carried") and merged[1]["data"]["converted"] == 7
        and merged[1]["success"] is True,
        "BLOB fra forrige kjøring: vises som OK med resultatet derfra")
    _ok(merged[2].get("rerun") and merged[2]["rerun"]["prev_status"] == "FEIL",
        "SIARDMapper kjørt på nytt: forrige status FEIL")
    _ok(merged[3].get("held") and hist["preliminary"], "holdt igjen → foreløpig rapport")
    _ok(len(hist["runs"]) == 2 and hist["runs"][1]["current"], "begge kjøringene listes")

    pdf = tmp / "rapport.pdf"
    _generate_pdf(pdf_path=pdf, siard_path=tmp / "a.siard", step_results=merged,
                  ctx_results={}, format_counts={"pdf": 7, "doc": 2},
                  history=hist)
    import pymupdf as fitz
    text = "".join(pg.get_text() for pg in fitz.open(pdf))
    _ok("Kjøringer" in text and "FORELØPIG" in text and "Holdt igjen" in text,
        "PDF: seksjonen «Kjøringer», FORELØPIG og «Holdt igjen»")
    _ok("Kjørt på nytt" in text and "forsøk 2" in text, "PDF: steg kjørt på nytt (forsøk 2)")


# ── 5. PREMIS samles opp ──────────────────────────────────────────────────────

def test_premis_append(tmp: Path) -> None:
    print("test_premis_append")
    from siard_workflow.core.premis_logger import PremisProvenanceLogger
    import xml.etree.ElementTree as ET
    op = SimpleNamespace(operation_id="blob_convert", premis_event_type="Migration",
                         premis_event_label="konvertering",
                         premis_detail=lambda r, c: "10 filer")
    res = SimpleNamespace(success=True, message="ok")
    ctx = SimpleNamespace(metadata={}, results={})
    p1 = PremisProvenanceLogger(tmp, tmp / "a.siard", "1.0")
    p1.record(op, res, ctx)
    out = p1.finalize(tmp / "a.siard", ctx)
    p2 = PremisProvenanceLogger(tmp, tmp / "a.siard", "1.0")
    p2.append_existing = True
    p2.record(op, res, ctx)
    p2.finalize(tmp / "a.siard", ctx)
    from siard_workflow.core.premis_logger import PREMIS_NS
    ns = "{" + PREMIS_NS + "}"
    n = len(ET.parse(out).getroot().findall(f"{ns}event"))
    _ok(n == 2, f"PREMIS: hendelsen fra forrige kjøring beholdes ({n} hendelser)")


# ── 6. App-hjelpere ───────────────────────────────────────────────────────────

def test_app_helpers(tmp: Path) -> None:
    print("test_app_helpers")
    import queue
    import gui.app as A
    from siard_workflow.core.project_file import ProjectFile
    from siard_workflow.core.context import WorkflowContext
    src = tmp / "b.siard"
    src.write_bytes(b"zip")
    pf = ProjectFile.from_ops(src, _ops(["blob_convert"]))
    fake = SimpleNamespace(_log_queue=queue.Queue(), _conv_ctx=None)
    fake._project_save = lambda pf_, p_: None
    asked = []

    def ask(est):
        asked.append(est)
        return True
    w1 = A.App._remembered(fake, pf, tmp / "b.siardwf", "blob_convert:gs_postprocess",
                           "Ghostscript", ask)
    _ok(w1({"x": 1}) is True and len(asked) == 1, "første gang: spørsmålet stilles")
    w2 = A.App._remembered(fake, pf, tmp / "b.siardwf", "blob_convert:gs_postprocess",
                           "Ghostscript", ask)
    _ok(w2({"x": 1}) is True and len(asked) == 1, "neste kjøring: svaret brukes uten å spørre")
    logged = [fake._log_queue.get()[1] for _ in range(fake._log_queue.qsize())]
    _ok(any("Bruker svaret fra forrige kjøring" in m for m in logged),
        "gjenbrukt svar logges")

    # Gjenoppretting av hoppede steg
    ops = _ops(["unpack_siard", "blob_convert", "repack_siard"])
    pf = ProjectFile.from_ops(src, ops)
    wd = tmp / "siard_pipeline_z"
    wd.mkdir()
    out = tmp / "b_konvertert.siard"
    out.write_bytes(b"x")
    pf.operations[0].ctx_data = {"extracted_path": str(wd), "original_namelist": ["a/"]}
    pf.operations[1].output_siard = str(out)       # BLOB i pipeline: ikke skrevet
    pf.operations[2].output_siard = str(out)
    ctx = WorkflowContext(siard_path=src)
    A.App._restore_skipped_step(ctx, ops[0], pf.operations[0], wd)
    A.App._restore_skipped_step(ctx, ops[1], pf.operations[1], wd)
    _ok(ctx.extracted_path == wd and ctx.siard_path == src,
        "«Pakk ut» hoppet over: temp-mappa brukes; BLOB endrer ikke SIARD-stien")
    _ok(ctx.get_result("unpack_siard")["original_namelist"] == ["a/"],
        "katalogposter fra «Pakk ut» gjenopprettes (til «Pakk sammen»)")
    A.App._restore_skipped_step(ctx, ops[2], pf.operations[2], wd)
    _ok(ctx.siard_path == out, "«Pakk sammen» hoppet over: neste steg bruker resultatet")

    # Filtyper summeres når BLOB fortsetter etter avbrudd
    pf.format_counts = {"pdf": 5}
    ctx.metadata["format_counts"] = {"pdf": 2, "doc": 1}
    res = SimpleNamespace(success=True, data={"resumed_files": 5})
    A.App._project_after_step(fake, pf, ops[1], res, ctx, "r2")
    _ok(pf.format_counts == {"pdf": 7, "doc": 1}, "filtyper summeres over kjøringene")
    res = SimpleNamespace(success=True, data={})
    ctx.metadata["format_counts"] = {"pdf": 3}
    A.App._project_after_step(fake, pf, ops[1], res, ctx, "r3")
    _ok(pf.format_counts == {"pdf": 3}, "ny konvertering fra start erstatter filtypene")


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="resume_wf_"))
    try:
        for fn in (test_project_file_v2, test_run_plan, test_blob_progress,
                   test_report_history, test_premis_append, test_app_helpers):
            sub = tmp / fn.__name__
            sub.mkdir()
            fn(sub)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
