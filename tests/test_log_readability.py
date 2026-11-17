"""
Lesbar kjørelogg og kjørerapport (nov. 2026, etter Lindesnes barnevern).

  1. Fremdriftslinjer («Batch N/M», «Detektert: N/M filer», «10 % — …») tynnes
     ut i loggfila: første linje, hver fjerdedel og siste. Andre linjer urørt.
  2. Flerlinjede meldinger blir én linje; «ok»/«feil» får riktig nivå; advarsler
     og feil telles til sluttsammendraget.
  3. Like XSD-rettinger i mange tabeller grupperes til én linje.
  4. LibreOffice-støy («Could not find platform independent libraries») fjernes
     fra feilteksten — den reelle feilen står igjen.
  5. Feiloversikten skiller «feilet» fra «beholdt etter regelen»; reelle feil
     kommer først i den begrensede lista.
  6. Sluttsammendraget: FULLFØRT MED AVVIK, steg med varighet, ventetid, filer.
  7. PREMIS kan skrives flere ganger (foreløpig før DIAS) uten doble hendelser.
  8. DIAS: samme målsti tas bare med én gang.
  9. Kjørerapporten: «GODKJENT MED AVVIK» + «Dette bør følges opp»; signerte
     PDF-er listes ikke enkeltvis; norske ledetekster.

Kjør:  python -X utf8 tests/test_log_readability.py
"""
from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_progress_thinning(tmp: Path) -> None:
    print("test_progress_thinning")
    from siard_workflow.core.file_logger import WorkflowFileLogger
    with WorkflowFileLogger(tmp, "t", heartbeat_s=0,
                            header_info=[("Program", "SIARD Workflow Manager v9")]) as log:
        for i in range(1, 1523):
            log.log(f"  Batch {i}/1522: 100 filer (profil worker{i % 12})")
        for p in range(2, 101, 2):
            log.log(f"  Detektert: {211836 * p // 100:,}/211 836 filer ({p} %)".replace(",", " "))
        for p in range(10, 100, 10):
            log.log(f"    {p} % — {211751 * p // 100:,}/211,751 filer, 3,0 GB av 29,6 GB")
        for p in range(1, 11):          # to tabeller parallelt — hver sin serie
            log.log(f"    table43.xml: {2110655 * p // 10:,}/2,110,655 linjer ({p * 10}%)")
            log.log(f"    table25.xml: {2383038 * p // 10:,}/2,383,038 linjer ({p * 10}%)")
        log.log("  [195/203] content/schema0/table173/table173.xml (3 KB): 15 filreferanser oppdatert")
        log.log("  Nytt forsøk ferdig: 0/6 konvertert i 6 runde(r)")
        log.log("  Batch 2/5: 3 filer (profil worker1)")
        log.log("  Konvertering feilet: a.bin (linje1\nError: source file could not be loaded)", "warn")
        log.log("  FEIL: steget feilet", "feil")
        log.log("  OK: ferdig", "ok")
        _ok(log.n_warnings == 1 and log.n_errors == 1, "advarsler og feil telles")
        _ok(log.suppressed > 1500, f"fremdriftslinjer holdt utenfor fila ({log.suppressed})")
        path = log.log_path
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    _ok("Program:   SIARD Workflow Manager v9" in text, "toppteksten har fakta om kjøringen")
    batch = [ln for ln in lines if "Batcher ferdig" in ln]
    _ok(len(batch) == 4 and "1522/1522 (100 %)" in batch[-1],
        f"1 522 batch-linjer → {len(batch)} (hver fjerdedel)")
    det = [ln for ln in lines if "Detektert:" in ln]
    _ok(len(det) == 5 and "211 836/211 836" in det[-1], f"50 «Detektert» → {len(det)}")
    pct = [ln for ln in lines if " % — " in ln]
    _ok(len(pct) == 4, f"9 prosentlinjer → {len(pct)}")
    t43 = [ln for ln in lines if "table43.xml" in ln]
    t25 = [ln for ln in lines if "table25.xml" in ln]
    _ok(len(t43) == 5 and len(t25) == 5 and "(100%)" in t43[-1] and "(100%)" in t25[-1],
        "parallelle serier tynnes hver for seg, siste linje er med")
    _ok(any("[195/203]" in ln for ln in lines) and any("0/6 konvertert" in ln for ln in lines),
        "resultatlinjer med N/M er urørt")
    _ok(any("Batch 2/5: 3 filer" in ln for ln in lines), "korte batch-serier skrives som før")
    warn = [ln for ln in lines if "[WARN]" in ln]
    _ok(len(warn) == 1 and "linje1 Error: source file could not be loaded" in warn[0]
        and warn[0].startswith("["), "flerlinjet melding er én linje med klokkeslett")
    _ok("[ERR ]   FEIL: steget feilet" in text and "[OK  ]   OK: ferdig" in text,
        "«feil» og «ok» får riktig nivå i fila")
    _ok(len(lines) < 45, f"loggfila er kort ({len(lines)} linjer)")


def test_group_lines() -> None:
    print("test_group_lines")
    from siard_workflow.core.column_xsd_types import group_lines
    lines = [f"schema0/table{i}: dateType maxExclusive «9999-12-31Z» → rettes til «10000-01-01Z»"
             for i in range(203)]
    lines.append("schema0/table1.A (c2): FLOAT(53) forventer xs:double, XSD har xs:float "
                 "→ rettes til xs:double")
    out = group_lines(lines)
    _ok(len(out) == 2, f"204 linjer → {len(out)}")
    _ok(out[0].startswith("203 × dateType maxExclusive") and "og 200 til" in out[0]
        and "schema0/table0" in out[0], f"gruppert: {out[0][:90]}…")
    _ok(out[1].startswith("schema0/table1.A (c2):"), "enkeltfunn står som før")
    _ok(group_lines(lines[:3]) == lines[:3], "få like linjer listes enkeltvis")


def test_lo_error_text() -> None:
    print("test_lo_error_text")
    from siard_workflow.operations.blob_convert_operation import _err_clean, _lo_err_text
    raw = ("Could not find platform independent libraries <prefix>\n"
           "Error: source file could not be loaded\n")
    _ok(_lo_err_text(raw) == "Error: source file could not be loaded",
        "støylinja fjernes, feilen står igjen på én linje")
    _ok(_lo_err_text("Could not find platform independent libraries <prefix>")
        .startswith("Could not find"), "bare støy → teksten beholdes (ikke tom)")
    cleaned = _err_clean(raw)
    _ok(cleaned.startswith("Error: source file could not be loaded. LibreOffice kunne ikke")
        and "<prefix>" not in cleaned, f"feillogg-teksten: {cleaned[:70]}…")


def test_failure_summary_split(tmp: Path) -> None:
    print("test_failure_summary_split")
    from siard_workflow.core.blob_csv_logger import ConversionErrorLogger
    with ConversionErrorLogger(tmp, "T") as el:
        for i in range(30):
            el.write(f"content/schema0/table1/lob1/record{i}.bin", "pdf",
                     "Digitalt signert PDF — beholdt uendret", category="signert")
        el.write("content/schema0/table1/lob1/record90.bin", "docx",
                 "Passordbeskyttet", category="passordbeskyttet")
        el.write("content/schema0/table1/lob1/record91.bin", "rtf",
                 "Error: source file could not be loaded")
        fs = el.summary(max_entries=5)
    _ok(fs["total"] == 32 and fs["failed_total"] == 1 and fs["kept_total"] == 31,
        "feilet og beholdt etter regelen telles hver for seg")
    _ok(fs["entries"][0]["kategori"] == "skadet",
        "reelle feil står først i den begrensede lista")
    from siard_workflow.core.run_summary import failure_counts
    _ok(failure_counts({"blob_convert": {"failure_summary": fs}}) == (1, 31),
        "failure_counts leser oversikten")


def test_run_summary(tmp: Path) -> None:
    print("test_run_summary")
    from siard_workflow.core.run_summary import (
        build_run_summary, collect_outputs, overall_status)
    new_siard = tmp / "x_konvertert.siard"
    new_siard.write_bytes(b"x" * 2048)
    steps = [
        {"index": 0, "label": "SHA-256 Sjekksum", "success": True, "skipped": True, "elapsed": 0},
        {"index": 1, "label": "Pakk ut SIARD", "success": True, "skipped": False, "elapsed": 272.0},
        {"index": 2, "label": "BLOB Konverter til PDF/A", "success": True, "skipped": False,
         "elapsed": 2141.9},
    ]
    results = {"repack_siard": {"output_path": str(new_siard)},
               "blob_convert": {"output_path": str(new_siard),
                                "failure_summary": {"total": 2, "csv_path": str(tmp / "f.csv"),
                                                    "log_path": ""}},
               "dias_package": {"aic_path": str(tmp / "aic")}}
    outs = collect_outputs(results, [("Kjørelogg", tmp / "k.log")])
    _ok([o[0] for o in outs] == ["Ny SIARD", "Ikke konverterte filer (regneark)",
                                 "DIAS-pakke (AIC)", "Kjørelogg"],
        "filene kjøringen laget samles uten duplikater")
    lines = build_run_summary(
        steps, total_s=5105.3, wait_s=780, warnings=[("13:27:36", "ADVARSEL: x")],
        errors=[], n_warnings=1, n_errors=0, outputs=outs, failed_files=6, kept_files=100)
    text = "\n".join(t for _, t in lines)
    _ok("FULLFØRT MED AVVIK — 6 fil(er)" in text, "resultat: fullført med avvik")
    _ok("1 t 25 min" in text and "herav 13 min 00 s venting på svar" in text,
        "tid med ventetid i lesbar form")
    _ok("[2]  Pakk ut SIARD" in text and "4 min 32 s" in text and "hoppet over" in text,
        "stegene med status og varighet")
    _ok("Advarsler: 1   Feil: 0" in text and "13:27:36  ADVARSEL: ADVARSEL: x" in text,
        "advarslene gjentas")
    _ok(f"{new_siard}  (2,0 KB)" in text, "filer laget med størrelse")
    _ok(overall_status(steps)[1].startswith("FULLFØRT uten feil"), "uten avvik: fullført")
    _ok(overall_status(steps + [{"index": 3, "label": "X", "success": False,
                                 "skipped": False}])[1].startswith("FEIL — 1 steg"),
        "feilet steg → FEIL")
    _ok(overall_status(steps, aborted=True)[1].startswith("AVBRUTT"), "avbrutt")
    _ok(overall_status(steps, held=2)[1].startswith("FORELØPIG"), "holdt igjen → foreløpig")


def test_premis_twice(tmp: Path) -> None:
    print("test_premis_twice")
    import xml.etree.ElementTree as ET
    from siard_workflow.core.premis_logger import PREMIS_NS, PremisProvenanceLogger

    class Op:
        operation_id = "hex_extract"
        premis_event_type = "Migration"
        premis_event_label = "hex-ekstraksjon"

        def premis_detail(self, result, ctx):
            return "493 HEX-felt"

    class Res:
        success = True
        message = ""

    class Ctx:
        metadata: dict = {}
        results: dict = {}

    def n_events(p: Path) -> int:
        return len(ET.parse(p).getroot().findall(f"{{{PREMIS_NS}}}event"))

    siard = tmp / "P.siard"
    pl = PremisProvenanceLogger(tmp, siard)
    pl.append_existing = True
    pl.record(Op(), Res(), Ctx())
    p1 = pl.finalize(siard, Ctx())                # foreløpig — før DIAS-pakking
    _ok(p1 and p1.exists() and n_events(p1) == 1, "PREMIS-fila finnes før DIAS-steget")
    pl.record(Op(), Res(), Ctx())
    p2 = pl.finalize(siard, Ctx())                # endelig
    _ok(n_events(p2) == 2, f"endelig fil: 2 hendelser, ingen doble ({n_events(p2)})")
    pl2 = PremisProvenanceLogger(tmp, siard)      # neste kjøring av samme prosjekt
    pl2.append_existing = True
    pl2.record(Op(), Res(), Ctx())
    _ok(n_events(pl2.finalize(siard, Ctx())) == 3, "neste kjøring legger til (3 hendelser)")


def test_dias_unique() -> None:
    print("test_dias_unique")
    from siard_workflow.operations.dias_package_operation import unique_by_dest
    extras = [
        {"src": r"X:\a\k.log", "dest": "administrative_metadata/repository_operations/k.log"},
        {"src": "X:/a/k.log", "dest": "administrative_metadata/repository_operations/K.log"},
        {"src": r"X:\a\r.pdf", "dest": "administrative_metadata/repository_operations/r.pdf"},
    ]
    out = unique_by_dest(extras)
    _ok(len(out) == 2 and out[0]["src"] == r"X:\a\k.log", "samme målsti tas med én gang")


def test_report(tmp: Path) -> None:
    print("test_report")
    import fitz
    from siard_workflow.operations import workflow_report_operation as W
    entries = [{"fil": f"content/schema0/table25/lob6/record{i}.bin", "format": "pdf",
                "plassering": "dbo.FA_DOKUMENTER.DOK_DOKUMENT", "kategori": "signert",
                "arsak": "Digitalt signert PDF — beholdt uendret"} for i in range(99)]
    entries += [{"fil": f"content/schema0/table25/lob6/skadet{i}.bin", "format": "rtf",
                 "plassering": "dbo.FA_DOKUMENTER.DOK_DOKUMENT", "kategori": "skadet",
                 "arsak": "Could not find platform independent libraries <prefix> "
                          "Error: source file could not be loaded"} for i in range(6)]
    fs = {"total": 105, "counts": {"skadet": 6, "signert": 99},
          "labels": {"skadet": "Skadet fil / kan ikke åpnes",
                     "signert": "Signert PDF (beholdt uendret)"},
          "entries": entries, "csv_path": str(tmp / "feil.csv"), "log_path": ""}
    steps = [
        {"id": "standardize_ext", "label": "Standardiser filendelser", "category": "Kompatibilitet",
         "success": True, "skipped": False, "elapsed": 23.8, "index": 0, "message": "ok"},
        {"id": "xsd_type_fix", "label": "Rett tableX.xsd (DBPTK P_4.3-3 / T_6.3-1)",
         "category": "Kompatibilitet", "success": True, "skipped": False, "elapsed": 1.3,
         "index": 1, "message": "406 XSD-retting(er)"},
        {"id": "blob_convert", "label": "BLOB Konverter til PDF/A", "category": "Innhold",
         "success": True, "skipped": False, "elapsed": 2141.0, "index": 2,
         "message": "152154 konvertert, 6 feil <prefix>"},
        {"id": "metadata_extract", "label": "Metadata-uttrekk", "category": "Metadata",
         "success": True, "skipped": False, "elapsed": 2.9, "index": 3, "message": "ok"},
    ]
    ctx_results = {
        "standardize_ext": {"renamed": 0, "renamed_names": 211343, "xml_updated": 211343},
        "xsd_type_fix": {"fixes": 406, "unfixed": 0, "changes": [
            f"schema0/table{i}: dateType maxExclusive «9999-12-31Z» → rettes til «10000-01-01Z»"
            for i in range(203)]},
        "blob_convert": {"identified": 211836, "detected": 152157, "converted": 152154,
                         "kept": 59677, "failed": 6, "xml_updated": 211836,
                         "failure_summary": fs},
        "metadata_extract": {"db_name": "X", "data_origin_time_span": "01012020 - 01052025",
                             "data_start": None, "data_end": None},
    }
    items, failed = W.follow_up_items(steps, ctx_results)
    _ok(failed == 6 and any("6 fil(er) ble ikke konvertert" in i for i in items),
        "oppfølging: filer som ikke lot seg konvertere")
    _ok(any("01012020 - 01052025" in i for i in items),
        "oppfølging: tidsperioden i metadata.xml lar seg ikke tolke")
    ok_span = {**ctx_results, "metadata_extract": {"data_origin_time_span": "2005-2025"}}
    _ok(not any("Tidsperioden" in i for i in W.follow_up_items(steps, ok_span)[0]),
        "tolkbar tidsperiode gir ingen merknad")

    pdf = tmp / "rapport.pdf"
    W._generate_pdf(pdf_path=pdf, siard_path=tmp / "T_konvertert.siard", step_results=steps,
                    ctx_results=ctx_results, format_counts={"doc": 136808, "pdf": 34487})
    doc = fitz.open(pdf)
    text = "\n".join(p.get_text() for p in doc)
    flat = " ".join(text.split())
    _ok("GODKJENT MED AVVIK" in flat and "Dette bør følges opp" in flat,
        "forsiden: godkjent med avvik + oppfølgingsliste")
    _ok("bør følges opp (6)" in flat and "ikke feil (99)" in flat,
        "feiloversikten er delt i feilet og beholdt etter regelen")
    nows = "".join(text.split())          # filnavn brytes over linjer i cellene
    _ok(all(f"skadet{i}.bin" in nows for i in range(6)) and "record50.bin" not in nows,
        "bare reelle feil listes enkeltvis — signerte PDF-er er oppsummert")
    _ok("platform independent" not in flat, "LibreOffice-støy vises ikke i rapporten")
    _ok("Filer omdøpt til recordN" in flat and "renamed_names" not in flat
        and "Rettinger i tableX.xsd" in flat, "norske ledetekster i stedet for interne nøkler")
    _ok("203 × dateType maxExclusive" in flat, "XSD-rettingene vises gruppert")
    _ok("LOB-filer identifisert" in flat and "211 836" in flat and "211,836" not in flat,
        "riktige betegnelser og mellomrom som tusenskille")
    _ok(len(doc) <= 5, f"rapporten er kort ({len(doc)} sider)")
    doc.close()

    # Uten avvik: fortsatt GODKJENT
    clean = {**ctx_results, "blob_convert": {"identified": 5, "detected": 5, "converted": 5},
             "metadata_extract": {"db_name": "X"}}
    pdf2 = tmp / "rapport2.pdf"
    W._generate_pdf(pdf_path=pdf2, siard_path=tmp / "T_konvertert.siard", step_results=steps,
                    ctx_results=clean, format_counts={})
    doc = fitz.open(pdf2)
    flat = " ".join(doc[0].get_text().split())
    doc.close()
    _ok("GODKJENT" in flat and "MED AVVIK" not in flat and "følges opp" not in flat,
        "uten avvik: GODKJENT uten oppfølgingsliste")


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="log_read_"))
    try:
        test_progress_thinning(tmp)
        test_group_lines()
        test_lo_error_text()
        test_failure_summary_split(tmp)
        test_run_summary(tmp)
        test_premis_twice(tmp)
        test_dias_unique()
        test_report(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
