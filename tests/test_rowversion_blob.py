"""
Radversjonstellere (TIMESTAMP/ROWVERSION lagret som BLOB) skal ikke bli filer.

Bakgrunn (nov. 2026, CosDoc-uttrekk): SQL Server/Sybase har en kolonnetype som
heter TIMESTAMP. Den er ikke et tidspunkt, men en teller på 8 byte. Uttrekket
beskriver den som <type>BLOB</type> / <typeOriginal>TIMESTAMP</typeOriginal>
med verdien inline som hex (<c7>000000000041C3A8</c7>). «HEX Inline Extract»
dekodet hver verdi til en egen .bin-fil på 8 byte — én fil per rad.

Nå omtypes kolonnen til BINARY(8) og verdiene blir stående inline.

Verifiserer:
  1. gjenkjenning og metadata-redigering (core/rowversion)
  2. pipeline-modus: ingen filer, verdiene uendret, metadata.xml og tableX.xsd
     oppdatert, tabellfila validerer mot XSD-en; øvrige BLOB-kolonner som før
  3. verdier som alt ligger i småfiler leses inn igjen, filene slettes
  4. regelen er smal: for store verdier / annen opprinnelig type røres ikke
  5. ZIP-modus (uten «Pakk ut SIARD»)
  6. PREMIS-tekst og preflight-oppsummering

Alle data er syntetiske.

Kjør:  python -X utf8 tests/test_rowversion_blob.py
"""
from __future__ import annotations

import os
import re
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


_MNS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
_TNS = "http://www.bar.admin.ch/xmlns/siard/2/table.xsd"

_PNG = bytes.fromhex("89504E470D0A1A0A0000000D49484452000000010000000108060000001F15C489")
_TEXT = "Dette er en lengre tekst med æøå som ligger hex-kodet i en BLOB-kolonne."


def _col(name: str, typ: str, orig: str, lob: str = "") -> str:
    lobf = f"\n              <lobFolder>{lob}</lobFolder>" if lob else ""
    return (f"            <column>\n              <name>{name}</name>{lobf}\n"
            f"              <type>{typ}</type>\n"
            f"              <typeOriginal>{orig}</typeOriginal>\n"
            f"              <nullable>true</nullable>\n              <description />\n"
            f"            </column>\n")


def _table(name: str, folder: str, cols: str, rows: int) -> str:
    return (f"        <table>\n          <name>{name}</name>\n"
            f"          <folder>{folder}</folder>\n          <description />\n"
            f"          <columns>\n{cols}          </columns>\n"
            f"          <rows>{rows}</rows>\n        </table>\n")


def _metadata() -> str:
    t0 = (_col("kode", "INTEGER", "int") + _col("navn", "VARCHAR(50)", "varchar")
          + _col("timestamp", "BLOB", "TIMESTAMP", "schema0/table0/lob3"))
    t1 = (_col("id", "INTEGER", "int") + _col("bilde", "BLOB", "IMAGE", "schema0/table1/lob2")
          + _col("notat", "BLOB", "TEXT", "schema0/table1/lob3")
          + _col("radversjon", "BLOB", "rowversion", "schema0/table1/lob4"))
    t2 = (_col("for_stor", "BLOB", "TIMESTAMP", "schema0/table2/lob1")
          + _col("kort", "BLOB", "TIMESTAMP", "schema0/table2/lob2")
          + _col("annen_type", "BLOB", "IMAGE", "schema0/table2/lob3"))
    t3 = _col("timestamp", "BLOB", "TIMESTAMP", "schema0/table3/lob1")
    return (f'<?xml version="1.0" encoding="utf-8"?>\n<siardArchive xmlns="{_MNS}" version="2.1">\n'
            f"  <dbname>syntetisk</dbname>\n  <lobFolder>content</lobFolder>\n  <schemas>\n"
            f"    <schema>\n      <name>dbo</name>\n      <folder>schema0</folder>\n      <tables>\n"
            + _table("kodetabell", "table0", t0, 9) + _table("dokument", "table1", t1, 3)
            + _table("rar", "table2", t2, 2) + _table("tom", "table3", t3, 0)
            + "      </tables>\n    </schema>\n  </schemas>\n</siardArchive>\n")


def _xsd(types: list) -> str:
    cols = "".join(f'      <xs:element name="c{i}" type="{t}" minOccurs="0"/>\n'
                   for i, t in enumerate(types, 1))
    return (f'<?xml version="1.0" encoding="utf-8"?>\n'
            f'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns="{_TNS}" '
            f'attributeFormDefault="unqualified" elementFormDefault="qualified" '
            f'targetNamespace="{_TNS}">\n'
            f'  <xs:element name="table">\n    <xs:complexType>\n      <xs:sequence>\n'
            f'        <xs:element name="row" type="recordType" minOccurs="0" maxOccurs="unbounded"/>\n'
            f'      </xs:sequence>\n      <xs:attribute name="version" type="xs:string"/>\n'
            f'    </xs:complexType>\n  </xs:element>\n'
            f'  <xs:complexType name="recordType">\n    <xs:sequence>\n{cols}    </xs:sequence>\n'
            f'  </xs:complexType>\n'
            f'  <xs:complexType name="blobType">\n    <xs:simpleContent>\n'
            f'      <xs:extension base="xs:hexBinary">\n'
            f'        <xs:attribute name="file" type="xs:string"/>\n'
            f'        <xs:attribute name="length" type="xs:integer"/>\n'
            f'        <xs:attribute name="digestType" type="xs:string"/>\n'
            f'        <xs:attribute name="digest" type="xs:string"/>\n'
            f'      </xs:extension>\n    </xs:simpleContent>\n  </xs:complexType>\n</xs:schema>\n')


def _table_xml(folder: str, rows: list) -> str:
    body = "".join("\t<row>\n" + "".join(f"\t\t{c}\n" for c in r) + "\t</row>\n" for r in rows)
    return (f'<?xml version="1.0" encoding="utf-8"?>\n<!--Syntetisk testtabell-->\n'
            f'<table xsi:schemaLocation="{_TNS} {folder}.xsd" version="2.1" '
            f'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="{_TNS}">\n'
            f'{body}\t<!--Row count: {len(rows)}-->\n</table>')


# Radversjonsverdier (oppdiktede): 8 byte, fortløpende
_RV = [f"{0x00000000004A0000 + i:016X}" for i in range(9)]


def _build(root: Path, files_for_table1: bool = True) -> None:
    (root / "header").mkdir(parents=True)
    (root / "header" / "metadata.xml").write_text(_metadata(), encoding="utf-8")
    base = root / "content" / "schema0"

    def _put(folder: str, types: list, rows: list) -> Path:
        d = base / folder
        d.mkdir(parents=True)
        (d / f"{folder}.xsd").write_text(_xsd(types), encoding="utf-8")
        (d / f"{folder}.xml").write_text(_table_xml(folder, rows), encoding="utf-8")
        return d

    # table0: som kodetabellen i uttrekket — radversjon inline
    _put("table0", ["xs:integer", "xs:string", "blobType"],
         [[f"<c1>{i}</c1>", f"<c2>kode{i}</c2>", f"<c3>{v}</c3>"] for i, v in enumerate(_RV)])

    # table1: bilde (binært), notat (tekst som hex), radversjon i småfiler + én inline
    d1 = base / "table1"
    rows1 = []
    for i in range(1, 4):
        if files_for_table1 and i < 3:
            rv = f'<c4 file="record{i}.bin" length="8" digestType="MD5" digest="X"/>'
        else:
            rv = f"<c4>{_RV[i]}</c4>"
        rows1.append([f"<c1>{i}</c1>", f"<c2>{_PNG.hex().upper()}</c2>",
                      f"<c3>{_TEXT.encode('utf-8').hex().upper()}</c3>", rv])
    _put("table1", ["xs:integer", "blobType", "blobType", "blobType"], rows1)
    if files_for_table1:
        (d1 / "lob4").mkdir()
        for i in (1, 2):
            (d1 / "lob4" / f"record{i}.bin").write_bytes(bytes.fromhex(_RV[i]))

    # table2: grensetilfeller
    _put("table2", ["blobType", "blobType", "blobType"],
         [[f"<c1>{'AB' * 20}</c1>", "<c2>0000BEEF</c2>", f"<c3>{_RV[0]}</c3>"],
          [f"<c1>{_RV[1]}</c1>", f"<c2>{_RV[2]}</c2>", f"<c3>{_RV[3]}</c3>"]])

    # table3: tom tabell
    _put("table3", ["blobType"], [])


def _col_block(meta: str, table: str, col: str) -> str:
    t = re.search(rf"<table>\s*<name>{table}</name>.*?</table>", meta, re.S).group(0)
    return re.search(rf"<column>\s*<name>{col}</name>.*?</column>", t, re.S).group(0)


def _ctx(root: "Path | None", siard: Path, logs: list):
    from siard_workflow.core.context import WorkflowContext

    class _Log:
        def log(self, msg, lvl="info"):
            logs.append((lvl, msg))
    ctx = WorkflowContext(siard_path=siard, metadata={
        "file_logger": _Log(), "stop_event": threading.Event(),
        "pause_event": threading.Event()})
    if root is not None:
        ctx.extracted_path = root
    return ctx


def _validate(xml_path: Path, xsd_path: Path) -> "str | None":
    """None = gyldig; ellers første feil. (lxml)"""
    from lxml import etree
    schema = etree.XMLSchema(etree.parse(str(xsd_path)))
    doc = etree.parse(str(xml_path))
    return None if schema.validate(doc) else str(schema.error_log.last_error)


def _files(d: Path) -> list:
    return sorted(p.name for p in d.iterdir() if p.is_file()) if d.is_dir() else []


def main() -> int:
    threading.Timer(180, lambda: os._exit(3)).start()
    from siard_workflow.core import rowversion as rv
    from siard_workflow.core import column_xsd_types as cx
    from siard_workflow.operations.hex_extract_operation import HexExtractOperation

    print("\n[1] Gjenkjenning og metadata")
    _ok(all(rv.is_rowversion_original(t) for t in ("TIMESTAMP", "timestamp", " RowVersion ", "[timestamp]"))
        and not any(rv.is_rowversion_original(t) for t in ("IMAGE", "DATETIME", "TIMESTAMP WITH TIME ZONE", "")),
        "TIMESTAMP/ROWVERSION gjenkjennes; IMAGE, DATETIME og «TIMESTAMP WITH TIME ZONE» ikke")
    _ok(rv.is_candidate("BLOB", "TIMESTAMP") and rv.is_candidate("BINARY LARGE OBJECT", "rowversion")
        and not rv.is_candidate("TIMESTAMP", "TIMESTAMP") and not rv.is_candidate("CLOB", "TIMESTAMP"),
        "bare binære LOB-kolonner er kandidater — et ekte tidspunkt (<type>TIMESTAMP) røres ikke")
    _ok(rv.binary_type_for(8, 8) == "BINARY(8)" and rv.binary_type_for(None, 0) == "BINARY(8)"
        and rv.binary_type_for(4, 8) == "VARBINARY(8)",
        "alle verdier 8 byte (eller tom kolonne) → BINARY(8); kortere finnes → VARBINARY(8)")
    meta0 = _metadata().encode("utf-8")
    cols = rv.find_rowversion_columns(meta0)
    _ok([(c["folder"], c["col_name"]) for c in cols] == [
        ("table0", "timestamp"), ("table1", "radversjon"), ("table2", "for_stor"),
        ("table2", "kort"), ("table3", "timestamp")],
        "metadata.xml: fem kandidatkolonner funnet (IMAGE/TEXT-kolonnene er ikke med)")
    _ok("5 kolonne(r) i 4 tabell(er), 16 verdier" in rv.summarize(cols),
        f"oppsummering til preflight: {rv.summarize(cols)}")
    m2, hit = rv.retype_in_metadata(meta0, "schema0", "table0", 3, "BINARY(8)")
    blk = _col_block(m2.decode(), "kodetabell", "timestamp")
    _ok(hit and "<type>BINARY(8)</type>" in blk and "lobFolder" not in blk
        and "<typeOriginal>TIMESTAMP</typeOriginal>" in blk,
        "<type> satt, <lobFolder> fjernet, <typeOriginal> bevart")
    _ok(m2.count(b"<lobFolder>") == meta0.count(b"<lobFolder>") - 1
        and b"<lobFolder>content</lobFolder>" in m2,
        "bare denne kolonnens <lobFolder> er fjernet (db-nivået står)")
    _ok(m2.replace(b"<type>BINARY(8)</type>", b"<type>BLOB</type>").count(b"\n") == meta0.count(b"\n") - 1,
        "linja med <lobFolder> er fjernet rent (ingen tom linje igjen)")

    from siard_workflow.operations.hex_extract_operation import (
        _patch_table_xsd_bytes, _warn_xsd_not_patched)
    xsd_alt = (b'<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">'
               b'<xsd:complexType name="recordType"><xsd:sequence>'
               b'<xsd:element name="c1" type="xsd:integer"/>'
               b'<xsd:element name="c2" type="blobType" minOccurs="0"/>'
               b'</xsd:sequence></xsd:complexType></xsd:schema>')
    out_alt, k_alt = _patch_table_xsd_bytes(xsd_alt, {2: "xs:hexBinary"})
    _ok(k_alt == 1 and b'name="c2" type="xsd:hexBinary"' in out_alt,
        "XSD med prefikset xsd: får xsd:hexBinary (ikke et udeklarert xs:)")
    warned: list = []
    _warn_xsd_not_patched(out_alt, {2: "xs:hexBinary"}, "t.xsd", lambda m, l="info": warned.append(m))
    _warn_xsd_not_patched(out_alt, {2: "xs:hexBinary", 9: "xs:hexBinary"}, "t.xsd",
                          lambda m, l="info": warned.append(m))
    _ok(len(warned) == 1 and "c9" in warned[0] and "c2" not in warned[0],
        "advarsel bare for kolonner som ikke finnes i XSD-en")

    with tempfile.TemporaryDirectory(prefix="rowver_") as td:
        tmp = Path(td)

        print("\n[2] Pipeline-modus")
        root = tmp / "utpakket"
        _build(root)
        logs: list = []
        res = HexExtractOperation().run(_ctx(root, tmp / "dummy.siard", logs))
        _ok(res.success, f"steget fullført: {res.message}")
        base = root / "content" / "schema0"
        t0 = (base / "table0" / "table0.xml").read_text(encoding="utf-8")
        _ok(not (base / "table0" / "lob3").exists(), "table0: ingen lob-mappe, ingen filer")
        _ok(all(f"<c3>{v}</c3>" in t0 for v in _RV) and "file=" not in t0,
            "table0: alle ni verdier står uendret inline")
        _ok("<!--Row count: 9-->" in t0 and "<!--Syntetisk testtabell-->" in t0,
            "table0: kommentarene i tabellfila er bevart")
        meta = (root / "header" / "metadata.xml").read_text(encoding="utf-8")
        blk = _col_block(meta, "kodetabell", "timestamp")
        _ok("<type>BINARY(8)</type>" in blk and "lobFolder" not in blk
            and "<typeOriginal>TIMESTAMP</typeOriginal>" in blk,
            "metadata.xml: BINARY(8), uten <lobFolder>, <typeOriginal> bevart")
        xsd0 = (base / "table0" / "table0.xsd").read_text(encoding="utf-8")
        _ok('name="c3" type="xs:hexBinary"' in xsd0 and 'name="c1" type="xs:integer"' in xsd0,
            "table0.xsd: c3 er xs:hexBinary, øvrige kolonner urørt")
        err = _validate(base / "table0" / "table0.xml", base / "table0" / "table0.xsd")
        _ok(err is None, f"table0.xml validerer mot table0.xsd ({err})")

        # table1: øvrige BLOB-kolonner behandles som før
        t1 = (base / "table1" / "table1.xml").read_text(encoding="utf-8")
        _ok(len(_files(base / "table1" / "lob2")) == 3
            and (base / "table1" / "lob2" / _files(base / "table1" / "lob2")[0]).read_bytes() == _PNG,
            "table1.bilde (IMAGE): tre .bin-filer som før")
        _ok("<type>CLOB</type>" in _col_block(meta, "dokument", "notat"),
            "table1.notat (hex-tekst): omtypet til CLOB som før")
        _ok("<type>BLOB</type>" in _col_block(meta, "dokument", "bilde"),
            "table1.bilde: fortsatt BLOB")

        print("\n[3] Verdier som alt ligger i småfiler")
        _ok(all(f"<c4>{_RV[i]}</c4>" in t1 for i in (1, 2, 3)),
            "table1.radversjon: verdiene fra filene står nå inline (og den som alt var inline)")
        _ok(not (base / "table1" / "lob4").exists(), "småfilene og den tomme lob-mappa er slettet")
        _ok("<type>BINARY(8)</type>" in _col_block(meta, "dokument", "radversjon"),
            "typeOriginal «rowversion» → BINARY(8)")
        _ok(res.data.get("rowversion_files_inlined") == 2, "to småfiler lest inn igjen")
        _ok(not any(lvl == "warn" and "LOB-antall gikk ned" in m for lvl, m in logs)
            and any("LOB-validering OK" in m and "lest inn igjen" in m for _l, m in logs),
            "LOB-valideringen regner de innleste småfilene som forventet nedgang")
        err = _validate(base / "table1" / "table1.xml", base / "table1" / "table1.xsd")
        _ok(err is None, f"table1.xml validerer mot table1.xsd ({err})")

        print("\n[4] Regelen er smal")
        _ok("<type>BLOB</type>" in _col_block(meta, "rar", "for_stor")
            and len(_files(base / "table2" / "lob1")) == 2,
            "TIMESTAMP med en verdi på 20 byte: omtypes ikke, behandles som vanlig BLOB")
        _ok(any(lvl == "warn" and "for_stor" in m and "omtypes ikke" in m and "20 byte" in m
                for lvl, m in logs), "loggen sier hvorfor kolonnen ikke ble omtypet")
        _ok("<type>VARBINARY(8)</type>" in _col_block(meta, "rar", "kort")
            and not (base / "table2" / "lob2").exists(),
            "TIMESTAMP med en verdi på 4 byte → VARBINARY(8), inline")
        _ok("<type>BLOB</type>" in _col_block(meta, "rar", "annen_type")
            and len(_files(base / "table2" / "lob3")) == 2,
            "IMAGE med verdier på 8 byte: røres ikke av regelen (eksporteres som før)")
        _ok("<type>BINARY(8)</type>" in _col_block(meta, "tom", "timestamp"),
            "tom tabell: kolonnen omtypes likevel (samme type i alle tabeller)")
        _ok(res.data.get("rowversion_columns") == 4 and res.data.get("rowversion_values") == 14,
            f"statistikk: 4 kolonner, 14 verdier ({res.data.get('rowversion_columns')}, "
            f"{res.data.get('rowversion_values')})")
        findings = [f for f in cx.check_dir(root)]
        _ok(findings == [], f"metadata.xml og tableX.xsd stemmer overens (P_4.3-3): {findings[:2]}")
        err = _validate(base / "table2" / "table2.xml", base / "table2" / "table2.xsd")
        _ok(err is None, f"table2.xml validerer mot table2.xsd ({err})")

        # Kjørt en gang til: ingenting mer å gjøre
        before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
        res_b = HexExtractOperation().run(_ctx(root, tmp / "dummy.siard", []))
        after = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
        _ok(res_b.success and set(before) == set(after)
            and (root / "header" / "metadata.xml").read_bytes() == before[root / "header" / "metadata.xml"]
            and not res_b.data.get("rowversion_columns"),
            "kjørt en gang til: ingen nye filer, metadata.xml uendret")

        print("\n[5] ZIP-modus (uten «Pakk ut SIARD»)")
        src_root = tmp / "kilde"
        _build(src_root)
        siard = tmp / "uttrekk.siard"
        with zipfile.ZipFile(siard, "w", zipfile.ZIP_DEFLATED) as z:
            for p in sorted(src_root.rglob("*")):
                if p.is_file():
                    z.write(p, p.relative_to(src_root).as_posix())
        logs5: list = []
        res5 = HexExtractOperation().run(_ctx(None, siard, logs5))
        _ok(res5.success, f"steget fullført: {res5.message}")
        with zipfile.ZipFile(res5.data["output_path"]) as z:
            names = z.namelist()
            meta5 = z.read("header/metadata.xml").decode()
            t0z = z.read("content/schema0/table0/table0.xml").decode()
            xsd0z = z.read("content/schema0/table0/table0.xsd").decode()
        _ok(not any("/table0/lob3/" in n for n in names)
            and all(f"<c3>{v}</c3>" in t0z for v in _RV),
            "table0: ingen filer, verdiene inline")
        _ok("<type>BINARY(8)</type>" in _col_block(meta5, "kodetabell", "timestamp")
            and 'name="c3" type="xs:hexBinary"' in xsd0z,
            "metadata.xml og table0.xsd oppdatert i den nye SIARD-fila")
        _ok("<type>BLOB</type>" in _col_block(meta5, "dokument", "radversjon")
            and any("/table1/lob4/record1.bin" in n for n in names)
            and any(lvl == "warn" and "radversjon" in m and "Pakk ut SIARD" in m for lvl, m in logs5),
            "verdier i filer: leses bare inn igjen i pipeline-modus — kolonnen står urørt, med melding")

        print("\n[6] PREMIS og kjøredata")
        detail = HexExtractOperation().premis_detail(res, None)
        _ok("4 radversjonskolonne(r)" in detail and "schema0/table0.timestamp (c3): BLOB → BINARY(8)" in detail
            and "<typeOriginal> bevart" in detail and "lest inn igjen" in detail,
            "PREMIS-hendelsen beskriver omtypingen")
        _ok("tegn-LOB" in detail and "table1.notat" in detail and "notat" not in
            detail.split("radversjonskolonne(r)")[1],
            "tekst-omtypingen (CLOB) og radversjonskolonnene står hver for seg")
        _ok("radversjonskolonne(r) omtypet" in res.message, f"resultatmeldingen: {res.message}")

        print("\n[7] Tørrkjøring endrer ingenting")
        root7 = tmp / "torr"
        _build(root7)
        before7 = {p: p.read_bytes() for p in root7.rglob("*") if p.is_file()}
        res7 = HexExtractOperation(dry_run=True).run(_ctx(root7, tmp / "dummy.siard", []))
        after7 = {p: p.read_bytes() for p in root7.rglob("*") if p.is_file()}
        _ok(res7.success and before7 == after7, "dry_run: alle filer uendret (småfilene står)")

        print("\n[8] Preflight (bare metadata.xml leses, ingen dialog)")
        from gui.app import App

        class _Op:
            def __init__(self, oid):
                self.operation_id = oid

        class _Fake:
            siard_queue = [siard]
            status = None

            def _pf(self, text):
                self.status = text

        f1 = _Fake()
        _ok(App._rowversion_preflight(f1, [_Op("unpack_siard"), _Op("hex_extract")]) is True
            and "5 kolonne(r) i 4 tabell(er), 16 verdier" in f1.status
            and "omtyper dem til BINARY(8)" in f1.status and "ingen filer" in f1.status,
            f"med «HEX Inline Extract»: {f1.status}")
        f2 = _Fake()
        _ok(App._rowversion_preflight(f2, [_Op("unpack_siard")]) is True
            and "blir stående som de er" in f2.status,
            f"uten «HEX Inline Extract»: {f2.status}")
        f3 = _Fake()
        f3.siard_queue = [Path(res5.data["output_path"]), tmp / "finnes_ikke.siard"]
        _ok(App._rowversion_preflight(f3, [_Op("hex_extract")]) is True
            and "2 kolonne(r)" in f3.status,
            f"etter omtyping gjenstår bare kolonnene som ikke kunne omtypes: {f3.status}")

    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    except BaseException:
        import traceback
        traceback.print_exc()
        rc = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)
