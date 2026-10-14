"""
«Estimer datospenn (fra–til)» — DateSpanOperation / core/date_span.py

  * metadata.xml <dataOriginTimespan> brukes når den oppgir et spenn — ingen estimering
  * ellers estimeres spennet bare fra LOB-tabeller (med LOB-mappe) og journal-/sakstabeller
  * en CLOB-kolonne med bare inline tekst gjør ikke tabellen til en LOB-tabell
  * datoer i rader med LOB-filer brukes i LOB-tabellene
  * fødselsdato/frist-kolonner påvirker ikke estimatet
  * plassholdere (1900-01-01, 1753) og enkeltstående feilår avgjør ikke spennet
  * et kort første år (delår) beholdes
  * tekstdatoer (dd.mm.åååå) og talldatoer (ååååmmdd) gjenkjennes
  * utpakket mappe og .siard-fil gir samme svar
  * DIAS-pakken får perioden fra metadata.xml, ellers fra estimatet
  * et estimat skrives inn i <dataOriginTimespan> med XML-kommentar om at det er
    estimert (pipeline: på stedet, frittstående: kopi _datospenn.siard) og
    gjenkjennes som estimat ved senere kjøringer
  * rapportene skiller mellom spenn fra metadata.xml og estimat
  * rekkefølge: før «Metadata-uttrekk» og «Kjørerapport»

Kjør:  python -X utf8 tests/test_date_span.py
"""
from __future__ import annotations

import random
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


_META = """<?xml version="1.0" encoding="UTF-8"?>
<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd" version="2.1">
<dbname>Saksarkiv</dbname><dataOriginTimespan>{ts}</dataOriginTimespan>
<archivalDate>2016-03-01</archivalDate><lobFolder>content</lobFolder>
<schemas><schema><name>arkiv</name><folder>schema0</folder><tables>
 <table><name>dokument</name><folder>table0</folder><columns>
  <column><name>id</name><type>INTEGER</type></column>
  <column><name>journaldato</name><type>DATE</type></column>
  <column><name>fodselsdato</name><type>DATE</type></column>
  <column><name>fil</name><lobFolder>schema0/table0/lob4</lobFolder><type>BLOB</type></column>
  <column><name>frist</name><type>DATE</type></column>
 </columns><rows>0</rows></table>
 <table><name>journalpost</name><folder>table1</folder><columns>
  <column><name>id</name><type>INTEGER</type></column>
  <column><name>jp_dato</name><type>VARCHAR(10)</type></column>
 </columns><rows>0</rows></table>
 <table><name>noarksak</name><folder>table2</folder><columns>
  <column><name>id</name><type>INTEGER</type></column>
  <column><name>saksdato</name><type>INTEGER</type></column>
 </columns><rows>0</rows></table>
 <table><name>logg</name><folder>table3</folder><columns>
  <column><name>id</name><type>INTEGER</type></column>
  <column><name>hendelse_dato</name><type>DATE</type></column>
 </columns><rows>0</rows></table>
 <table><name>sakstype</name><folder>table4</folder><columns>
  <column><name>kode</name><type>VARCHAR(5)</type></column>
  <column><name>opprettet_dato</name><type>DATE</type></column>
 </columns><rows>0</rows></table>
 <table><name>notat</name><folder>table5</folder><columns>
  <column><name>tekst</name><type>CLOB</type></column>
  <column><name>notat_dato</name><type>DATE</type></column>
 </columns><rows>0</rows></table>
</tables></schema></schemas></siardArchive>"""


def _make(root: Path, ts: str = "") -> Path:
    rnd = random.Random(7)
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(_META.format(ts=ts), "utf-8")
    t0 = root / "content/schema0/table0"
    (t0 / "lob4").mkdir(parents=True)
    rows = []
    n = 0

    def row(jd: str, lob: bool = True) -> None:
        nonlocal n
        n += 1
        fd = f"19{rnd.randint(30, 70)}-0{rnd.randint(1, 9)}-15"
        fr = f"20{rnd.randint(16, 30)}-06-30"
        f = f'<c4 file="record{n}.bin" length="3"/>' if lob else ""
        if lob:
            (t0 / "lob4" / f"record{n}.bin").write_bytes(b"pdf")
        rows.append(f"<row><c1>{n}</c1><c2>{jd}</c2><c3>{fd}</c3>{f}<c5>{fr}</c5></row>")
    for _ in range(10):                                 # delår 2010 (okt–des)
        row(f"2010-{rnd.randint(10, 12)}-{rnd.randint(10, 28):02d}")
    for y in range(2011, 2016):
        for _ in range(120):
            row(f"{y}-{rnd.randint(1, 12):02d}-{rnd.randint(1, 28):02d}")
    row("1900-01-01")                                   # plassholder
    row("1753-01-01")                                   # SQL Server-minimum
    row("1985-05-05")                                   # enkeltstående feilregistrering
    for _ in range(40):                                 # rader uten LOB, tidligere år
        row(f"2009-0{rnd.randint(1, 9)}-11", lob=False)

    def table(folder: str, body: "list[str]") -> None:
        d = root / "content/schema0" / folder
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{folder}.xml").write_text("<table>\n" + "\n".join(body) + "\n</table>\n", "utf-8")
    table("table0", rows)
    # journalpost: tekstdato dd.mm.åååå, noarksak: talldato ååååmmdd — begge 2011–2015
    table("table1", [f"<row><c1>{i}</c1><c2>{rnd.randint(1, 28):02d}.0{rnd.randint(1, 9)}."
                     f"{rnd.randint(2011, 2015)}</c2></row>" for i in range(200)])
    table("table2", [f"<row><c1>{i}</c1><c2>{rnd.randint(2011, 2015)}0{rnd.randint(1, 9)}15"
                     f"</c2></row>" for i in range(150)])
    # logg og sakstype: ikke relevante — ville trukket spennet ut om de ble lest
    table("table3", [f"<row><c1>{i}</c1><c2>{rnd.randint(1995, 2016)}-03-03</c2></row>"
                     for i in range(2000)])
    table("table4", [f"<row><c1>K{i}</c1><c2>1991-01-0{i % 9 + 1}</c2></row>" for i in range(50)])
    # notat: CLOB med inline tekst (ingen LOB-mappe) — ikke en dokumenttabell
    table("table5", [f"<row><c1>kort notat {i}</c1><c2>{rnd.randint(1995, 2016)}-04-04</c2></row>"
                     for i in range(500)])
    return root


def _zip(root: Path, dst: Path) -> Path:
    with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
        for p in root.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(root).as_posix())
    return dst


def test_metadata_span() -> None:
    print("test_metadata_span")
    from datetime import date
    from siard_workflow.core.date_span import metadata_span as m
    _ok(m("01.10.2014 - 19.06.2020") == (date(2014, 10, 1), date(2020, 6, 19)), "dd.mm.åååå - dd.mm.åååå")
    _ok(m("2014-10-01 - 2020-06-19") == (date(2014, 10, 1), date(2020, 6, 19)), "ISO-datoer")
    _ok(m("2014 - 2020") == (date(2014, 1, 1), date(2020, 12, 31)), "rene årstall → hele år")
    _ok(m("31.7.2021 - 30.12.2013") == (date(2013, 12, 30), date(2021, 7, 31)), "omvendt rekkefølge")
    _ok(m("01.01.2010 til 31.12.2015") == (date(2010, 1, 1), date(2015, 12, 31)), "«til» som skille")
    _ok(m("2014-10-01") is None and m("ukjent") is None and m("") is None,
        "én dato / tekst uten datoer / tomt → ikke et spenn")


def test_metadata_priority(tmp: Path) -> None:
    print("test_metadata_priority")
    from siard_workflow.core import date_span as DS
    root = _make(tmp / "med_spenn", "01.10.2010 - 31.12.2015")
    r = DS.estimate(root)
    _ok(r["source"] == "metadata" and not r["estimated"], "metadata.xml oppgir spennet → brukes")
    _ok((r["from_date"], r["to_date"]) == ("2010-10-01", "2015-12-31"), f"{r['from_date']} – {r['to_date']}")
    _ok(not r["tables_scanned"] and "rows_scanned" not in r, "ingen tabeller lest")
    p = DS.period_for_dias(_zip(root, tmp / "med_spenn.siard"))
    _ok(p and p[:2] == ("2010-10-01", "2015-12-31") and "metadata.xml" in p[2],
        f"DIAS-periode fra metadata.xml: {p}")


def test_estimate(tmp: Path) -> dict:
    print("test_estimate")
    from siard_workflow.core import date_span as DS
    root = _make(tmp / "uttrekk")
    r = DS.estimate(root)
    _ok(r["source"] == "estimat" and r["found"] and (r["from_year"], r["to_year"]) == (2010, 2015),
        f"estimert 2010–2015 ({r['from_year']}–{r['to_year']}, {r['from_date']} – {r['to_date']})")
    read = {t["table"]: t["why"] for t in r["tables_scanned"]}
    _ok(set(read) == {"arkiv.dokument", "arkiv.journalpost", "arkiv.noarksak"},
        f"bare LOB- og journal-/sakstabeller lest: {read}")
    _ok(not {"arkiv.logg", "arkiv.sakstype", "arkiv.notat"} & set(read),
        "logg, kodetabellen sakstype og notat (CLOB uten LOB-mappe) er ikke lest")
    _ok(r["from_date"].startswith("2010-1"), "kort første år (okt–des 2010) er beholdt")
    cols = {c["column"]: c for c in r["columns"]}
    _ok(cols["journaldato"]["lob_rows"] and cols["journaldato"]["primary"],
        "dokument: datoer i rader med LOB-fil brukt (2009-radene uten LOB teller ikke)")
    _ok(cols["jp_dato"]["primary"] and cols["jp_dato"]["kind"] == "tekst"
        and cols["saksdato"]["primary"] and cols["saksdato"]["kind"] == "tall",
        "journalpost (dd.mm.åååå) og noarksak (ååååmmdd) brukt")
    excl = {c["column"] for c in r["columns"] if c["excluded"]}
    _ok({"fodselsdato", "frist"} <= excl, f"fødselsdato og frist er utelatt ({sorted(excl)})")
    _ok(r["absolute_first"] == "1985-05-05" and r["outside_trim"] >= 1,
        "1900-01-01/1753 regnes som usannsynlige; enkeltstående 1985 avgjør ikke spennet")
    rz = DS.estimate(_zip(root, tmp / "Saksarkiv.siard"))
    _ok((rz["from_year"], rz["to_year"], rz["n_values"]) == (r["from_year"], r["to_year"], r["n_values"]),
        "samme svar fra .siard-fil som fra utpakket mappe")
    p = DS.period_for_dias(root, r)
    _ok(p and p[0] == r["from_date"] and p[1] == r["to_date"] and "estimert" in p[2],
        f"DIAS-periode fra estimatet når metadata.xml mangler spenn: {p[:2]}")
    _ok(DS.period_for_dias(root, None) is None, "ingen periode uten spenn i metadata.xml og uten estimat")
    return r


def test_fallback_all_tables(tmp: Path) -> None:
    print("test_fallback_all_tables")
    from siard_workflow.core import date_span as DS
    root = tmp / "uten_relevante"
    (root / "header").mkdir(parents=True)
    (root / "header/metadata.xml").write_text(
        '<siardArchive><archivalDate>2016-01-01</archivalDate><schemas><schema><name>s</name>'
        '<folder>schema0</folder><tables><table><name>hendelse</name><folder>table0</folder>'
        '<columns><column><name>id</name><type>INTEGER</type></column><column><name>dato</name>'
        '<type>DATE</type></column></columns></table></tables></schema></schemas></siardArchive>',
        "utf-8")
    d = root / "content/schema0/table0"
    d.mkdir(parents=True)
    (d / "table0.xml").write_text("<table>" + "".join(
        f"<row><c1>{i}</c1><c2>{2001 + i % 4}-05-05</c2></row>" for i in range(200)) + "</table>",
        "utf-8")
    r = DS.estimate(root)
    _ok(r["basis"] == "alle" and (r["from_year"], r["to_year"]) == (2001, 2004)
        and "alle tabeller er lest" in r["basis_text"],
        "ingen LOB-/journal-/sakstabeller → alle tabeller leses, og det står i grunnlaget")


def _ctx(siard: Path, logs: list):
    from siard_workflow.core.context import WorkflowContext
    return WorkflowContext(siard_path=siard, metadata={
        "file_logger": None, "stop_event": threading.Event(), "pause_event": threading.Event(),
        "progress_cb": lambda ev, **k: logs.append(k.get("msg", "")) if ev == "log" else None})


def _reports(ctx, op, r) -> "tuple[str, str]":
    import fitz
    from siard_workflow.operations import WorkflowReportOperation
    from siard_workflow.operations.standard_operations import MetadataExtractOperation
    import siard_workflow.operations.standard_operations as SO
    SO._open_file = lambda p: None
    mo = MetadataExtractOperation()
    mo.params["generate_er_diagram"] = False
    mr = mo.run(ctx)
    with fitz.open(mr.data["pdf_path"]) as d:
        meta_txt = " ".join(p.get_text() for p in d)
    ctx.metadata["step_results"] = [{"id": "date_span", "label": op.label, "success": True,
                                     "elapsed": 0.2, "message": r.message, "category": "Innhold"}]
    rr = WorkflowReportOperation().run(ctx)
    with fitz.open(rr.data["report_path"]) as d:
        run_txt = " ".join(p.get_text() for p in d)
    return meta_txt, run_txt


def test_operation_and_reports(tmp: Path) -> None:
    print("test_operation_and_reports")
    import settings
    settings.set_config = lambda k, v: None
    from siard_workflow.operations import DateSpanOperation
    from siard_workflow.operations.dias_package_operation import DiasPackageOperation

    # a) estimat (metadata.xml uten spenn) — skrives til en kopi av SIARD-filen
    logs: list = []
    src = tmp / "Saksarkiv.siard"
    ctx = _ctx(src, logs)
    op = DateSpanOperation()
    r = op.run(ctx)
    ctx.set_result("date_span", r.data)
    out = Path(r.data.get("output_path", ""))
    _ok(r.data.get("written_to_metadata") and op.produces_siard
        and out.name == "Saksarkiv_datospenn.siard" and out.is_file(),
        f"frittstående: estimatet skrevet til kopi {out.name}")
    with zipfile.ZipFile(out) as z, zipfile.ZipFile(src) as z0:
        meta = z.read("header/metadata.xml").decode("utf-8")
        _ok(sorted(z.namelist()) == sorted(z0.namelist())
            and z.read("content/schema0/table0/table0.xml")
            == z0.read("content/schema0/table0/table0.xml"), "kopien har alt annet uendret")
    value = f"{r.data['from_date']} - {r.data['to_date']}"
    _ok(f"<dataOriginTimespan>{value}</dataOriginTimespan>" in meta
        and "<!-- dataOriginTimespan er estimert av SIARD Workflow Manager" in meta
        and "ikke nødvendigvis 100 % korrekt" in meta,
        f"metadata.xml: <dataOriginTimespan>{value}</dataOriginTimespan> med kommentar om estimat")
    import xml.etree.ElementTree as ET
    ET.fromstring(meta.encode("utf-8"))
    _ok(op.premis_should_record(r, ctx) and "estimert" in op.premis_detail(r, ctx),
        "PREMIS-event for endringen i metadata.xml")
    ctx.siard_path = out                     # som workflowens chain_siard
    _ok(r.success and "2010–2015" in r.message and r.data["source"] == "estimat", f"operasjonen: {r.message}")
    _ok(any("ESTIMERT DATOSPENN: 2010–2015" in m for m in logs)
        and any("ikke nødvendigvis 100 % korrekt" in m for m in logs)
        and any("oppgir ikke datospenn" in m for m in logs),
        "kjøreloggen viser estimatet, at det er et estimat, og hvorfor")
    _ok(op.category == "Innhold" and op.modifies_content,
        "kategori «Innhold», innholdsendrende (metadata.xml) — PREMIS bare når estimatet skrives")
    meta_txt, run_txt = _reports(ctx, op, r)
    _ok("Estimert datospenn" in meta_txt and "2010–2015" in meta_txt
        and "ikke nødvendigvis 100 % korrekt" in meta_txt and "ESTIMERT" in meta_txt,
        "metadata-rapporten viser estimatet, og Tidsperiode er merket ESTIMERT")
    _ok("Estimert datospenn for uttrekket" in run_txt and "ikke nødvendigvis 100 % korrekt" in run_txt
        and "fodselsdato" in run_txt and "journal/sak" in run_txt
        and "Skrevet til metadata.xml" in run_txt,
        "kjørerapporten viser estimatet, merknad, leste tabeller, utelatte kolonner og tilbakeskrivingen")
    dias = DiasPackageOperation(period_start="1999-01-01", period_end="1999-12-31")
    dias._apply_period(ctx, ctx.siard_path)
    _ok((dias.params["period_start"], dias.params["period_end"]) == (r.data["from_date"], r.data["to_date"]),
        "DIAS: estimatet erstatter gamle periodeverdier fra innstillingene")

    # Ny kjøring på kopien: spennet står i metadata.xml, men er gjenkjent som estimat
    r2 = DateSpanOperation().run(_ctx(out, []))
    _ok(r2.data["source"] == "metadata" and r2.data["metadata_estimated"]
        and "ESTIMERT" in r2.data["note"] and not r2.data.get("written_to_metadata"),
        "senere kjøring: verdien i metadata.xml gjenkjennes som estimert (XML-kommentaren)")

    # Innstilling av → ingenting skrives
    r3 = DateSpanOperation(write_metadata=False).run(_ctx(src, []))
    _ok(r3.data["source"] == "estimat" and not r3.data.get("written_to_metadata")
        and "output_path" not in r3.data, "write_metadata av → ingen kopi, ingen endring")

    # b) spenn i metadata.xml
    logs = []
    ctx = _ctx(tmp / "med_spenn.siard", logs)
    r = op.run(ctx)
    ctx.set_result("date_span", r.data)
    _ok(r.data["source"] == "metadata" and any("DATOSPENN FRA metadata.xml" in m for m in logs),
        f"operasjonen bruker metadata.xml: {r.message}")
    meta_txt, run_txt = _reports(ctx, op, r)
    _ok("Estimert datospenn" not in meta_txt and "01.10.2010 - 31.12.2015" in meta_txt,
        "metadata-rapporten viser bare tidsperioden fra metadata.xml (ingen estimat)")
    _ok("Datospenn for uttrekket (fra metadata.xml)" in run_txt and "Estimert datospenn" not in run_txt,
        "kjørerapporten sier at spennet er hentet fra metadata.xml")
    dias = DiasPackageOperation(period_start="1999-01-01", period_end="1999-12-31")
    dias._apply_period(ctx, ctx.siard_path)
    _ok((dias.params["period_start"], dias.params["period_end"]) == ("2010-10-01", "2015-12-31"),
        "DIAS: periode fra metadata.xml")


def test_pipeline_write(tmp: Path) -> None:
    print("test_pipeline_write")
    from siard_workflow.operations import DateSpanOperation
    root = _make(tmp / "pipeline")
    ctx = _ctx(tmp / "pipeline.siard", [])
    ctx.extracted_path = root
    op = DateSpanOperation()
    r = op.run(ctx)
    meta = (root / "header/metadata.xml").read_text("utf-8")
    _ok(r.data.get("written_to_metadata") and not op.produces_siard and "output_path" not in r.data,
        "pipeline: metadata.xml i den utpakkede mappa endret på stedet (ingen kopi)")
    _ok(f"<dataOriginTimespan>{r.data['written_value']}</dataOriginTimespan>" in meta
        and meta.index("<!-- dataOriginTimespan er estimert") < meta.index("<dataOriginTimespan>"),
        "kommentaren står rett foran elementet")


def test_order() -> None:
    print("test_order")
    from siard_workflow.core.workflow_validator import validate_workflow
    from siard_workflow.operations import DateSpanOperation, WorkflowReportOperation
    from siard_workflow.operations.standard_operations import MetadataExtractOperation
    bad = validate_workflow([MetadataExtractOperation(), DateSpanOperation(), WorkflowReportOperation()])
    _ok(any("Estimer datospenn" in m for _o, m in bad), "Metadata-uttrekk før estimatet → rekkefølgebrudd")
    good = validate_workflow([DateSpanOperation(), MetadataExtractOperation(), WorkflowReportOperation()])
    _ok(not good, "estimat før Metadata-uttrekk og Kjørerapport → OK")
    from siard_workflow.operations import UnpackSiardOperation, RepackSiardOperation
    after = validate_workflow([UnpackSiardOperation(), RepackSiardOperation(), DateSpanOperation()])
    _ok(any("før 'Pakk sammen SIARD'" in m for _o, m in after),
        "estimatet skrives til metadata.xml → må ligge før 'Pakk sammen SIARD'")
    inside = validate_workflow([UnpackSiardOperation(), DateSpanOperation(), RepackSiardOperation()])
    _ok(not inside, "mellom 'Pakk ut' og 'Pakk sammen' → OK")
    off = validate_workflow([UnpackSiardOperation(), RepackSiardOperation(),
                             DateSpanOperation(write_metadata=False)])
    _ok(not off, "uten tilbakeskriving kan den ligge etter 'Pakk sammen SIARD'")


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="datospenn_") as td:
        tmp = Path(td)
        test_metadata_span()
        test_metadata_priority(tmp)
        test_estimate(tmp)
        test_fallback_all_tables(tmp)
        test_operation_and_reports(tmp)
        test_pipeline_write(tmp)
        test_order()
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
