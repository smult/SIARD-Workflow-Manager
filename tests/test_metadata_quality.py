"""
Tester for MetadataQualityOperation — retting av innholdsfeil i metadata.xml
fra filbaserte uttrekk (typisk SC Full Convert):

  1. <dbname> inneholder tilkoblingsstien i stedet for databasenavnet
  2. <dataOriginTimespan> har sluttdato før startdato og/eller lokalt format

Merk at ingen av disse er skjemabrudd: begge feltene er `mandatoryString`
(xs:string, minLength 1) i den offisielle siard2-1-metadata.xsd, og DBPTKs
MetadataDatabaseInfoValidator sjekker kun at de ikke er tomme. Dette er
metadatakvalitet, ikke DBPTK-kompatibilitet.

Kjør:  python -X utf8 tests/test_metadata_quality.py
"""
from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.context import WorkflowContext
from siard_workflow.operations.metadata_quality_operation import (
    MetadataQualityOperation, scan_metadata_quality,
    derive_dbname, dbname_has_path, parse_timespan, timespan_is_reversed,
    _fix_metadata,
)


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


# Ekte utdrag: WIS Skole Flekkefjord, Full Convert mot Sybase Advantage
_META = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"'
    ' version="2.1">\n'
    '\t<dbname>Data.Q:\\Ikava\\WISFerdig\\Skole\\Flekkefjord historisk\\Data</dbname>\n'
    '\t<description>WIS Skole Flekkefjord kommune</description>\n'
    '\t<archiver>Flekkefjord kommune</archiver>\n'
    '\t<archiverContact>Turid Holen</archiverContact>\n'
    '\t<dataOwner>Flekkefjord kommune</dataOwner>\n'
    '\t<dataOriginTimespan>31.7.2021 - 30.12.2013</dataOriginTimespan>\n'
    '\t<lobFolder>content</lobFolder>\n'
    '\t<producerApplication>Full Convert Pro Annual 23.07.1671</producerApplication>\n'
    '\t<archivalDate>2024-02-12</archivalDate>\n'
    '\t<clientMachine>DDVB1001</clientMachine>\n'
    '\t<databaseProduct>(Sybase Advantage)</databaseProduct>\n'
    '\t<connection>Q:\\Ikava\\WISFerdig\\Skole\\Flekkefjord historisk\\Data</connection>\n'
    '\t<databaseUser>adssys</databaseUser>\n'
    '</siardArchive>\n'
)


def test_dbname_derivation() -> None:
    print("test_dbname_derivation")
    _ok(dbname_has_path("Data.Q:\\Ikava\\WISFerdig\\Data"), "sti oppdaget")
    _ok(not dbname_has_path("Northwind"), "rent navn er ikke sti")
    _ok(not dbname_has_path("WIS Skole 2021"), "navn med mellomrom er ikke sti")

    # Full Convert-mønsteret «<navn>.<sti>»
    _ok(derive_dbname("Data.Q:\\Ikava\\WISFerdig\\Skole\\"
                      "Flekkefjord historisk\\Data") == "Data",
        "«Data.Q:\\…» → «Data» (tekst foran stien)")
    # Ren sti uten prefiks → siste sti-ledd
    _ok(derive_dbname("Q:\\Ikava\\WISFerdig\\Data") == "Data",
        "ren sti → siste ledd")
    # UNC-sti
    _ok(derive_dbname("\\\\server\\share\\Basen") == "Basen", "UNC → siste ledd")
    # Bare et stasjonsledd gir ingen navn → feltet skal stå urørt
    _ok(derive_dbname("Q:\\") == "", "kun stasjonsbokstav → ingen utledning")


def test_timespan_parsing() -> None:
    print("test_timespan_parsing")
    p = parse_timespan("31.7.2021 - 30.12.2013")
    _ok(p is not None, "d.m.yyyy-spenn tolkes")
    _ok(p[0].isoformat() == "2021-07-31" and p[1].isoformat() == "2013-12-30",
        "dag-først tolkning (norsk konvensjon)")
    _ok(timespan_is_reversed("31.7.2021 - 30.12.2013"), "omvendt spenn oppdaget")
    _ok(not timespan_is_reversed("30.12.2013 - 31.7.2021"), "riktig spenn OK")

    # ISO med bindestrek som skilletegn må ikke splittes feil
    p2 = parse_timespan("2013-12-30 - 2021-07-31")
    _ok(p2 is not None and p2[2] is True, "ISO-spenn tolkes og merkes som ISO")
    _ok(not timespan_is_reversed("2013-12-30 - 2021-07-31"), "ISO-spenn ikke omvendt")

    # Fri tekst skal ikke tolkes — feltet er fri tekst i SIARD
    _ok(parse_timespan("ukjent") is None, "fri tekst gir ingen tolkning")
    _ok(not timespan_is_reversed("2013-2021"), "utolkbart spenn flagges ikke")


def test_fix_metadata() -> None:
    print("test_fix_metadata")
    out, changes = _fix_metadata(_META)
    _ok(len(changes) == 2, f"to endringer (fikk {len(changes)})")
    _ok("<dbname>Data</dbname>" in out, "dbname renset for sti")
    _ok("<dataOriginTimespan>2013-12-30 - 2021-07-31</dataOriginTimespan>" in out,
        "datospenn snudd og normalisert til ISO")

    # <connection> er proveniens og skal stå urørt
    _ok("<connection>Q:\\Ikava\\WISFerdig\\Skole\\Flekkefjord historisk\\Data"
        "</connection>" in out, "<connection> uberørt")
    _ok("<databaseUser>adssys</databaseUser>" in out, "<databaseUser> uberørt")
    _ok("<archivalDate>2024-02-12</archivalDate>" in out, "<archivalDate> uberørt")

    # Operatør-overstyring vinner over automatisk utledning
    out2, _ = _fix_metadata(_META, dbname_override="WIS Skole Flekkefjord")
    _ok("<dbname>WIS Skole Flekkefjord</dbname>" in out2, "dbname-overstyring")

    # normalize_dates=False skal fortsatt snu et omvendt spenn
    out3, _ = _fix_metadata(_META, normalize_dates=False)
    _ok("2013-12-30 - 2021-07-31" in out3,
        "omvendt spenn snus selv uten ISO-normalisering")

    # Ren metadata → ingen endringer (idempotens)
    _, changes2 = _fix_metadata(out)
    _ok(changes2 == [], "allerede rettet metadata → no-op")


def test_xml_escaping() -> None:
    print("test_xml_escaping")
    meta = ('<siardArchive><dbname>R&amp;D.C:\\data\\R&amp;D</dbname>'
            '<dataOriginTimespan>1.1.2000 - 2.2.2001</dataOriginTimespan>'
            '</siardArchive>')
    out, _ = _fix_metadata(meta)
    _ok("<dbname>R&amp;D</dbname>" in out,
        "ampersand av-escapes ved lesing og re-escapes ved skriving")


def test_scan_and_operation() -> None:
    print("test_scan_and_operation (ende-til-ende)")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        src = tmp / "WIS_Skole.siard"
        with zipfile.ZipFile(src, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("header/metadata.xml", _META.encode("utf-8"))
            z.writestr("content/schema0/table1/table1.xml", b"<table/>")

        issues = scan_metadata_quality(src)
        _ok(len(issues) == 2, f"skann fant to problemer (fikk {len(issues)})")
        _ok(any("dbname" in i and "Data" in i for i in issues),
            "dbname-funn rapportert med forslag")
        _ok(any("sluttdato før startdato" in i for i in issues),
            "omvendt datospenn rapportert")

        # Standalone-modus
        ctx = WorkflowContext(siard_path=src)
        res = MetadataQualityOperation().run(ctx)
        _ok(res.success, f"operasjon kjørte: {res.message}")
        out_path = Path(res.data["output_path"])
        _ok(out_path.exists(), f"skrev {out_path.name}")

        with zipfile.ZipFile(out_path) as z:
            fixed = z.read("header/metadata.xml").decode("utf-8")
            names = z.namelist()
        _ok("<dbname>Data</dbname>" in fixed, "ny SIARD har renset dbname")
        _ok("2013-12-30 - 2021-07-31" in fixed, "ny SIARD har rettet datospenn")
        _ok("content/schema0/table1/table1.xml" in names, "øvrig innhold bevart")

        # Skann av resultatet skal være rent
        _ok(scan_metadata_quality(out_path) == [], "rettet fil gir ingen funn")


def test_clean_file_is_noop() -> None:
    print("test_clean_file_is_noop")
    clean = ('<siardArchive><dbname>Northwind</dbname>'
             '<dataOriginTimespan>2013-12-30 - 2021-07-31</dataOriginTimespan>'
             '</siardArchive>')
    out, changes = _fix_metadata(clean)
    _ok(out == clean and changes == [], "ren metadata røres ikke")

    # Lokalt datoformat ALENE skal ikke slå ut i preflight-skanningen …
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.siard"
        local = ('<siardArchive><dbname>Northwind</dbname>'
                 '<dataOriginTimespan>1.1.2000 - 31.12.2010</dataOriginTimespan>'
                 '</siardArchive>')
        with zipfile.ZipFile(p, "w") as z:
            z.writestr("header/metadata.xml", local.encode("utf-8"))
        _ok(scan_metadata_quality(p) == [],
            "kun lokalt datoformat → ingen preflight-varsling")
    # … men operasjonen normaliserer det når den først er lagt til
    out2, changes2 = _fix_metadata(local)
    _ok("2000-01-01 - 2010-12-31" in out2 and len(changes2) == 1,
        "operasjonen normaliserer likevel formatet")


def main() -> None:
    test_dbname_derivation()
    test_timespan_parsing()
    test_fix_metadata()
    test_xml_escaping()
    test_scan_and_operation()
    test_clean_file_is_noop()
    print("\nAlle metadata-kvalitetstester bestått ✓")


if __name__ == "__main__":
    main()
