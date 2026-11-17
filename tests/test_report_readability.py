"""
Lesbar logg og rapporter, del 2 (nov. 2026, etter Lindesnes barnevern).

  1. Fil-loggeren skriver ikke stegets banner («=====» / NAVN) rett etter
     stegoverskriften; sammendragets skillelinjer (raw) skrives.
  2. Preflight: kontroller som hoppes over fordi prosjektet er kjørt før,
     samles i én linje.
  3. «01012020 - 01052025» gjenkjennes som DDMMÅÅÅÅ (bare som hint).
  4. Kjørerapporten: nøkkeltall på forsiden, formater før/etter, steg som
     gjenstår, endringer (PREMIS-hendelser), og sluttversjon med alle stegene.
  5. Metadatarapporten: forkortet tilkobling, merknader, «refereres av»,
     alfabetisk detaljdel med tomme tabeller samlet, bokmerker og lenker,
     LOB-kolonner med antall filer og størrelse.

Kjør:  python -X utf8 tests/test_report_readability.py
"""
from __future__ import annotations

import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_banner_and_raw(tmp: Path) -> None:
    print("test_banner_and_raw")
    from siard_workflow.core.file_logger import WorkflowFileLogger
    with WorkflowFileLogger(tmp, "b", heartbeat_s=0) as log:
        log.log("=" * 56)                                  # før første steg: skrives
        log.set_step("[3] Standardiser filendelser")
        log.log("[3] Standardiser filendelser", "step")
        log.log("=" * 56)
        log.log("  STANDARDISER FILENDELSER", "step")
        log.log("=" * 56)
        log.log("  OPPSUMMERING:", "step")
        log.log("=" * 56, "info", raw=True)
        path = log.log_path
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if "] [" in ln]
    _ok(sum("=====" in ln for ln in lines) == 2,
        "banner-linjene i steget er borte; linja før steget og rå linje skrives")
    _ok(not any("STANDARDISER FILENDELSER" in ln for ln in lines)
        and any("[3] Standardiser filendelser" in ln for ln in lines),
        "stegnavnet står én gang")
    _ok(any("OPPSUMMERING" in ln for ln in lines), "andre overskrifter i steget beholdes")


def test_compact_preflight() -> None:
    print("test_compact_preflight")
    from siard_workflow.core.run_summary import compact_preflight
    skip = "    hoppet over — prosjektet er kjørt før, svarene fra første kjøring gjelder (0.0 s)"
    lines = [("step", "Preflight — 1 fil(er), 10 operasjon(er)"),
             ("muted", "  Sjekker køen før kjøring."),
             ("info", "  ▸ Filnavn (tegnkoding) …"), ("ok", skip),
             ("info", "  ▸ Pipeline-modus (Pakk ut / Pakk sammen) …"), ("ok", skip),
             ("info", "  ▸ Programvare (pakker og programmer) …"),
             ("ok", "    alt arbeidsflyten trenger er installert (0.2 s)"),
             ("ok", "Preflight ferdig (15.8 s) — starter kjøring")]
    out = compact_preflight(lines)
    texts = [m for _l, m in out]
    _ok(len(out) == 6, f"9 linjer → {len(out)}")
    _ok("Hoppet over" in texts[2] and "Filnavn (tegnkoding), Pipeline-modus" in texts[2],
        f"én samlelinje med navnene: {texts[2][:80]}…")
    _ok(texts[3].strip().startswith("▸ Programvare") and "installert" in texts[4],
        "kontroller som kjørte, står som før")
    _ok(compact_preflight(lines[:2]) == lines[:2], "uten hoppede kontroller: uendret")


def test_compact_span() -> None:
    print("test_compact_span")
    import datetime
    from siard_workflow.core.date_span import guess_compact_span, metadata_span
    _ok(metadata_span("01012020 - 01052025") is None, "teksten tolkes ikke automatisk")
    _ok(guess_compact_span("01012020 - 01052025")
        == (datetime.date(2020, 1, 1), datetime.date(2025, 5, 1)), "DDMMÅÅÅÅ gjenkjennes")
    _ok(guess_compact_span("20200101-20250501")
        == (datetime.date(2020, 1, 1), datetime.date(2025, 5, 1)), "ÅÅÅÅMMDD gjenkjennes")
    _ok(guess_compact_span("2005 - 2025") is None and guess_compact_span("99999999 - 1") is None
        and guess_compact_span("") is None, "annet gir ingen gjetning")


def _steps() -> list:
    def st(i, op_id, label, **kw):
        return {"id": op_id, "label": label, "category": "", "success": True,
                "skipped": False, "elapsed": 10.0, "index": i, "message": "ok", **kw}
    return [st(0, "sha256", "SHA-256 Sjekksum"),
            st(1, "blob_convert", "BLOB Konverter til PDF/A"),
            st(2, "repack_siard", "Pakk sammen SIARD"),
            st(3, "metadata_extract", "Metadata-uttrekk")]


def _results(tmp: Path) -> dict:
    src = tmp / "K.siard"
    src.write_bytes(b"x" * 4096)
    return {
        "sha256": {"sha256": "ab" * 32, "file": str(src)},
        "blob_convert": {"identified": 300, "detected": 200, "converted": 198, "kept": 100,
                         "failed": 2, "xml_updated": 300,
                         "to_pdfa_formats": {"doc": 150, "rtf": 50},
                         "failure_summary": {
                             "total": 2, "counts": {"skadet": 2},
                             "labels": {"skadet": "Skadet fil / kan ikke åpnes"},
                             "entries": [{"fil": f"content/s/t/lob1/record{i}.bin",
                                          "format": "rtf", "plassering": "a.b.c",
                                          "kategori": "skadet", "arsak": "x"}
                                         for i in range(2)]}},
        "repack_siard": {"output_path": str(tmp / "K_konvertert.siard"), "size_mb": 2048.0,
                         "files_written": 10},
        "metadata_extract": {"db_name": "DB1", "db_product": "MSSQL", "table_count": 3,
                             "row_count": 1234567, "lob_file_count": 300, "siard_version": "2.1",
                             "data_origin_time_span": "2005 - 2025"},
    }


def test_report_parts(tmp: Path) -> None:
    print("test_report_parts")
    import fitz
    from siard_workflow.operations import workflow_report_operation as W
    steps, results = _steps(), _results(tmp)
    facts = dict(W.key_facts(steps, results, tmp / "K_konvertert.siard"))
    _ok(facts["Kilde:"].startswith("K.siard") and "4,0 KB" in facts["Kilde:"],
        f"nøkkeltall: kilde med størrelse ({facts['Kilde:']})")
    _ok(facts["Resultat:"] == "K_konvertert.siard  (2,0 GB)", "resultat-SIARD med størrelse")
    _ok("3 tabeller, 1 234 567 rader, 300 LOB-filer" in facts["Innhold:"], "innhold")
    _ok(facts["Dokumenter:"] == "198 konvertert til PDF/A, 100 beholdt i originalformat, 2 feilet",
        "dokumenter: konvertert / beholdt / feilet")
    _ok(facts["Periode:"] == "2005 - 2025 (metadata.xml)", "periode")

    rows = W.format_changes({"doc": 150, "rtf": 60, "pdf": 90},
                            results["blob_convert"]["to_pdfa_formats"],
                            results["blob_convert"]["failure_summary"])
    by = {r[0]: r for r in rows}
    _ok(by[".doc"][2] == "150 konvertert til PDF/A", "DOC: alle konvertert")
    _ok(by[".rtf"][2] == "48 konvertert til PDF/A, 2 feilet (beholdt), 10 beholdt",
        f"RTF: konvertert, feilet og beholdt ({by['.rtf'][2]})")
    _ok(by[".pdf"][2] == "beholdt i originalformat", "PDF: beholdt")
    _ok(W.format_changes({"doc": 1}, {}, {}) == [], "eldre data uten formatliste → ingen tabell")

    prov = [{"datetime": "2026-11-13T13:28:48+01:00", "type": "Adjustment",
             "label": "filnavn-standardisering", "detail": "211343 fil(er) omdøpt", "success": True}]
    pdf = tmp / "r.pdf"
    W._generate_pdf(pdf_path=pdf, siard_path=tmp / "K_konvertert.siard", step_results=steps,
                    ctx_results=results, format_counts={"doc": 150, "rtf": 60, "pdf": 90},
                    pending_steps=["DIAS-pakking (SIP/AIC)"], provenance=prov)
    doc = fitz.open(pdf)
    flat = " ".join(" ".join(p.get_text() for p in doc).split())
    doc.close()
    _ok("SHA-256 (kilde):" in flat and "Dokumenter:" in flat, "forsiden har nøkkeltall")
    _ok("laget før disse stegene var kjørt" in flat and "DIAS-pakking (SIP/AIC)" in flat,
        "rapporten sier fra om steg som gjenstår")
    _ok("Endringer i uttrekket" in flat
        and "filnavn-standardisering" in flat,
        "endringene (PREMIS-hendelsene) er listet")
    _ok("Formater før og etter konverteringen" in flat and "48 konvertert til PDF/A" in flat,
        "formater før og etter")


def test_final_report(tmp: Path) -> None:
    print("test_final_report")
    import fitz
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations import workflow_report_operation as W
    out_siard = tmp / "K_konvertert.siard"
    out_siard.write_bytes(b"x")
    ctx = WorkflowContext(siard_path=out_siard, metadata={})
    ctx.results.update(_results(tmp))
    ctx.metadata["step_results"] = _steps()
    ctx.metadata["steps_after_report"] = ["DIAS-pakking (SIP/AIC)"]
    r = W.WorkflowReportOperation().run(ctx)
    _ok(r.success, f"rapporten laget underveis ({r.message})")
    path = Path(r.data["report_path"])

    def text() -> str:
        with fitz.open(path) as d:
            return " ".join(" ".join(p.get_text() for p in d).split())
    first = text()
    _ok("laget før disse stegene" in first and "5. DIAS-pakking" not in first,
        "underveis: DIAS-steget er ikke med, men nevnt som gjenstående")
    ctx.metadata["step_results"] += [
        {"id": "workflow_report", "label": "Kjørerapport (PDF)", "success": True,
         "skipped": False, "elapsed": 0.2, "index": 4, "message": r.message},
        {"id": "dias_package", "label": "DIAS-pakking (SIP/AIC)", "success": True,
         "skipped": False, "elapsed": 1700.0, "index": 5, "message": "DIAS-pakke opprettet"}]
    _ok(W.write_final_report(ctx, path), "sluttversjonen skrives til samme fil")
    final = text()
    _ok("DIAS-pakking (SIP/AIC)" in final and "laget før disse stegene" not in final
        and "6 av 6 steg OK" in final, "sluttversjonen har alle stegene")
    _ok(not path.with_suffix(".tmp.pdf").exists(), "ingen midlertidig fil ligger igjen")


_META_XML = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
  <dbname>DB1</dbname><description>(unspecified)</description>
  <dataOriginTimespan>01012020 - 01052025</dataOriginTimespan>
  <producerApplication>Test</producerApplication><archivalDate>2025-10-30</archivalDate>
  <connection>Data Source=SRV1;Initial Catalog=DB1;User ID=SA;Password=hemmelig</connection>
  <databaseUser>SA</databaseUser>
  <schemas><schema><name>dbo</name><folder>schema0</folder><tables>
    <table><name>ZEBRA</name><folder>table0</folder><columns>
      <column><name>ID</name><type>INTEGER</type></column>
      <column><name>KLIENT_ID</name><type>INTEGER</type></column>
      <column><name>DOK</name><type>BLOB</type></column></columns>
      <primaryKey><name>PK_Z</name><column>ID</column></primaryKey>
      <foreignKeys><foreignKey><name>FK_Z_K</name><referencedSchema>dbo</referencedSchema>
        <referencedTable>KLIENT</referencedTable>
        <reference><column>KLIENT_ID</column><referenced>ID</referenced></reference>
      </foreignKey></foreignKeys><rows>5</rows></table>
    <table><name>KLIENT</name><folder>table1</folder><columns>
      <column><name>ID</name><type>INTEGER</type></column></columns>
      <primaryKey><name>PK_K</name><column>ID</column></primaryKey><rows>2</rows></table>
    <table><name>ARKIV_TOM</name><folder>table2</folder><columns>
      <column><name>A</name><type>VARCHAR(10)</type></column></columns><rows>0</rows></table>
  </tables></schema></schemas>
</siardArchive>
"""


def test_metadata_report(tmp: Path) -> None:
    print("test_metadata_report")
    import fitz
    from siard_workflow.operations import metadata_pdf as M
    from siard_workflow.operations.standard_operations import MetadataExtractOperation

    _ok(M.short_connection("Data Source=SRV1;Initial Catalog=DB1;User ID=SA;Password=hemmelig")
        == "server SRV1, database DB1", "tilkoblingen forkortes til server og database")
    _ok(M.short_connection("jdbc:sqlserver://bruker:pw@srv:1433;databaseName=X;user=sa")
        == "jdbc:sqlserver://srv:1433", "URL: uten bruker, passord og parametre")
    _ok(M.short_connection("") == "" and M.short_connection(None) == "", "tom gir tom")

    siard = tmp / "M.siard"
    with zipfile.ZipFile(siard, "w") as z:
        z.writestr("header/metadata.xml", _META_XML)
        z.writestr("header/siardversion/2.1/", "")
        for t in range(3):
            z.writestr(f"content/schema0/table{t}/table{t}.xml", "<table/>")
        for i in range(4):
            z.writestr(f"content/schema0/table0/lob3/record{i}.bin", b"x" * 1000)
    meta = MetadataExtractOperation()._extract_all(siard)
    zebra = next(t for t in meta["schemas"][0]["tables"] if t["name"] == "ZEBRA")
    _ok(zebra.get("lob_columns") == [{"folder": "lob3", "column": "DOK", "type": "BLOB",
                                      "files": 4, "bytes": 4000}],
        f"LOB-kolonner: filer og størrelse per kolonne ({zebra.get('lob_columns')})")
    _ok(zebra["lob_file_count"] == 4 and meta["lob_file_count"] == 4, "LOB-tellingen er som før")

    refs = M.referenced_by(meta["schemas"])
    _ok(refs == {("dbo", "klient"): ["ZEBRA"]}, f"refereres av: {refs}")
    notes = " ".join(M.quality_notes(meta))
    _ok("lar seg ikke tolke" in notes and "2020-01-01" in notes, "merknad: tidsperioden")
    _ok("ingen beskrivelse" in notes and "1 tabell(er) har ingen primærnøkkel: ARKIV_TOM" in notes
        and "1 av 3 tabeller er tomme" in notes, "merknader: beskrivelse, primærnøkkel, tomme")

    pdf = tmp / "meta.pdf"
    M.generate_metadata_pdf(meta, siard, pdf, options={"generate_er_diagram": False})
    doc = fitz.open(pdf)
    toc = [t[1] for t in doc.get_toc()]
    text = "\n".join(p.get_text() for p in doc)
    flat = " ".join(text.split())
    links = sum(len(p.get_links()) for p in doc)
    doc.close()
    _ok(toc[:3] == ["Databaseoversikt", "Tabelloversikt", "Skjema- og tabelldetaljer"]
        and toc[3:] == ["Skjema: dbo", "KLIENT", "ZEBRA", "Tomme tabeller (1)"],
        f"bokmerker, alfabetisk, tomme til slutt: {toc}")
    _ok(links >= 3, f"tabelloversikten lenker til detaljene ({links} lenker)")
    _ok("server SRV1, database DB1" in flat and "hemmelig" not in flat
        and "Password" not in flat, "passord og bruker i tilkoblingen vises ikke")
    _ok("Merknader til metadataene" in flat and "Mest refererte tabeller" in flat,
        "merknader og mest refererte tabeller på oversiktssiden")
    _ok("Refereres av (1): ZEBRA" in flat, "detaljene viser hvem som refererer tabellen")
    _ok("LOB-kolonner (filer i uttrekket)" in flat and "3,9 KB" in flat,
        "LOB-oversikt med størrelse")
    det = flat[flat.index("Skjema- og tabelldetaljer"):]
    _ok(det.index("KLIENT") < det.index("ZEBRA") < det.index("Tomme tabeller (1)")
        < det.index("ARKIV_TOM"), "detaljdelen: med innhold først (alfabetisk), så tomme")
    _ok("Beskrivelse" not in det, "tomme «Beskrivelse»-kolonner er skjult")


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="rep_read_"))
    try:
        test_banner_and_raw(tmp)
        test_compact_preflight()
        test_compact_span()
        test_report_parts(tmp)
        test_final_report(tmp)
        test_metadata_report(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
