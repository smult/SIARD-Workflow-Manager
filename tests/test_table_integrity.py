"""
Ufullstendige tabellfiler og feil ved utpakking (core/table_integrity.py,
core/atomic_write.py, Pakk ut SIARD, HEX Inline Extract).

Bakgrunn: en tableX.xml uten avsluttende </table> ga «no element found: line
128534, column 0» midt i HEX Inline Extract. Utpakkingen meldte feil bare som en
advarsel, og en halvskrevet fil ble liggende.

Verifiserer:
  1. slutten av en tabellfil gjenkjennes som hel/avkuttet (mellomrom, kommentar,
     navneromsprefiks, tom tabell, UTF-16)
  2. kontroll av utpakket mappe: avkuttet, tom og manglende tabellfil
  3. kontroll av SIARD-fil uten utpakking: komprimert og lagret oppføring, stor
     fil, skadet oppføring (CRC), fremdrift — samme funn som for mappa
  4. extract_all: feil samles i ExtractErrors, halvskrevet fil slettes, resten
     pakkes ut
  5. Pakk ut SIARD: stopper når data mangler (også uten operatør), temp-mappa
     ryddes; operatøren kan velge å fortsette — da står datatapet i resultatet
  6. HEX Inline Extract: forståelig feil for avkuttet tabellfil, fila urørt; ny
     tabellfil byttes inn i ett trinn (avbrudd etterlater den gamle hel)
  7. write_bytes_atomic: feil under skriving etterlater den gamle fila
  8. sluttsammendrag og kjørerapport: «FULLFØRT MED DATATAP» / oppfølgingspunkt
  9. full disk under utpakking: utpakkingen venter og spør (fortsett/avbryt);
     «fortsett» uten mer ledig plass gir nytt varsel; etter rydding pakkes
     fila ut på nytt og alt er med (disken «fylles» ved å simulere feilen)

Alle data er oppdiktet.  Kjør:  python -X utf8 tests/test_table_integrity.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core import table_integrity as ti          # noqa: E402
from siard_workflow.core import zip_progress as Z              # noqa: E402
from siard_workflow.core import atomic_write as aw             # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_MNS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
_TNS = "http://www.bar.admin.ch/xmlns/siard/2/table.xsd"
_HEAD = (f'<?xml version="1.0" encoding="utf-8"?>\n<table xmlns="{_TNS}" version="2.1">\n')
_TEXT = ("Oppdiktet journaltekst nummer %d med æøå, lang nok til å bli lagt i en "
         "egen fil når den dekodes fra hex. " * 3)


def _row(i: int, hex_doc: bool = True) -> str:
    doc = (_TEXT % (i, i, i)).encode("utf-8").hex().upper() if hex_doc else f"tekst {i}"
    return f"<row><c1>{i}</c1><c2>dok{i}</c2><c3>{doc}</c3></row>\n"


def _table_xml(n: int, complete: bool = True, hex_doc: bool = True) -> bytes:
    body = "".join(_row(i, hex_doc) for i in range(1, n + 1))
    return (_HEAD + body + ("</table>" if complete else "")).encode("utf-8")


def _metadata(tables: list) -> bytes:
    """tables: [(navn, mappe, rader)] — c3 er CLOB i alle."""
    def col(name, typ):
        return (f"<column><name>{name}</name><type>{typ}</type>"
                f"<typeOriginal>x</typeOriginal><nullable>true</nullable></column>")
    tb = "".join(
        f"<table><name>{name}</name><folder>{folder}</folder><description/>"
        f"<columns>{col('id', 'INTEGER')}{col('tittel', 'VARCHAR(50)')}"
        f"{col('dokument', 'CLOB')}</columns><rows>{rows}</rows></table>"
        for name, folder, rows in tables)
    return (f'<?xml version="1.0" encoding="utf-8"?>\n<siardArchive xmlns="{_MNS}" '
            f'version="2.1"><dbname>syntetisk</dbname><schemas><schema><name>dbo</name>'
            f"<folder>schema0</folder><tables>{tb}</tables></schema></schemas>"
            f"</siardArchive>\n").encode("utf-8")


_TABLES = [("HKJOURDOC", "table0", 40), ("KODER", "table1", 5),
           ("TOMTABELL", "table2", 0), ("UTENFIL", "table3", 0), ("BORTE", "table4", 7)]


def _files(cut: bool) -> dict:
    """Innholdet i et syntetisk uttrekk. cut=True: table0 mangler </table>."""
    return {
        "header/metadata.xml": _metadata(_TABLES),
        "content/schema0/table0/table0.xml": _table_xml(40, complete=not cut),
        "content/schema0/table1/table1.xml": _table_xml(5, hex_doc=False),
        "content/schema0/table2/table2.xml": (_HEAD.rstrip("\n")[:-1] + "/>\n").encode(),
        # table3: 0 rader og ingen fil (ikke et funn); table4: 7 rader, ingen fil
    }


def _write_dir(root: Path, files: dict) -> None:
    for name, data in files.items():
        p = root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


def _write_zip(path: Path, files: dict, stored: tuple = ()) -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data, zipfile.ZIP_STORED if name in stored
                        else zipfile.ZIP_DEFLATED)


def _ctx(siard: Path, logs: list, root: "Path | None" = None, **meta):
    from siard_workflow.core.context import WorkflowContext

    class _Log:
        def log(self, msg, lvl="info"):
            logs.append((lvl, msg))
    md = {"file_logger": _Log(), "stop_event": threading.Event(),
          "pause_event": threading.Event()}
    md.update(meta)
    ctx = WorkflowContext(siard_path=siard, metadata=md)
    if root is not None:
        ctx.extracted_path = root
    return ctx


# ─────────────────────────────────────────────────────────────────────────────

def test_tail() -> None:
    print("test_tail")
    _ok(ti.tail_complete(b"<row/></table>"), "</table> sist")
    _ok(ti.tail_complete(b"<row/></table>\r\n  \n"), "</table> + linjeskift/mellomrom")
    _ok(ti.tail_complete(b"</row>\n</table><!--Finished at: 2026-->\n"),
        "</table> + kommentar etterpå")
    _ok(ti.tail_complete(b"</s:row></s:table >\n"), "navneromsprefiks og mellomrom i taggen")
    _ok(ti.tail_complete(b'<table xmlns="x" version="2.1"/>\n'), "tom tabell (selvlukkende)")
    _ok(ti.tail_complete("</row>\n</table>\n".encode("utf-16-le")), "UTF-16")
    _ok(not ti.tail_complete(b"<row><c1>1</c1></row>\n"), "slutter etter en rad → avkuttet")
    _ok(not ti.tail_complete(b"<row><c1>1</c1></row>\n</tab"), "slutter midt i taggen → avkuttet")
    _ok(not ti.tail_complete(b"</table>\n<row><c1>2</c1>"), "</table> før mer innhold → avkuttet")
    _ok(not ti.tail_complete(b""), "tom → ikke hel")
    meta = ti.tables_in_metadata(_metadata(_TABLES))
    _ok([(t["table_name"], t["table_folder"], t["rows"]) for t in meta] == _TABLES,
        "tabeller og <rows> leses fra metadata.xml")


def test_check_dir(tmp: Path) -> None:
    print("test_check_dir")
    good = tmp / "hel"
    _write_dir(good, _files(cut=False))
    res = ti.check_dir(good)
    kinds = {f["table"]: f["kind"] for f in res["findings"]}
    _ok(kinds == {"table4": ti.MISSING} and res["checked"] == 3,
        f"hel mappe: bare tabellen med rader og uten fil meldes: {kinds}")

    bad = tmp / "kuttet"
    _write_dir(bad, _files(cut=True))
    (bad / "content/schema0/table1/table1.xml").write_bytes(b"")
    res = ti.check_dir(bad)
    kinds = {f["table"]: f["kind"] for f in res["findings"]}
    _ok(kinds == {"table0": ti.CUT, "table1": ti.EMPTY, "table4": ti.MISSING},
        f"avkuttet, tom og manglende tabellfil: {kinds}")
    line = ti.format_finding(res["findings"][0])
    _ok("schema0/table0 HKJOURDOC" in line and "</table>" in line and "40 rad" in line,
        f"funnlinja navngir tabellen: {line}")
    before = {p: p.read_bytes() for p in bad.rglob("*") if p.is_file()}
    ti.check_dir(bad)
    _ok(before == {p: p.read_bytes() for p in bad.rglob("*") if p.is_file()},
        "kontrollen skriver ingenting")


def test_check_zip(tmp: Path) -> None:
    print("test_check_zip")
    # Stor komprimert tabell (flere leseblokker) + lagret oppføring + avkuttet
    big = _HEAD.encode() + os.urandom(3 << 20).hex().encode() + b"\n" \
        + b"".join(_row(i).encode() for i in range(2000))
    files = _files(cut=True)
    files["content/schema0/table1/table1.xml"] = big + b"</table>\n"
    z = tmp / "kuttet.siard"
    _write_zip(z, files, stored=("content/schema0/table2/table2.xml",))
    seen: list = []

    class _P:
        def __init__(self):
            self.bytes = self.files = 0
            self._lock = threading.Lock()

        def add(self, nbytes=0, files=0):
            with self._lock:
                self.bytes += nbytes
                self.files += files

        def finish(self):
            seen.append("ferdig")
    progs: list = []

    def factory(n, total):
        progs.append((n, total, _P()))
        return progs[-1][2]
    res = ti.check_zip(z, factory, workers=3)
    kinds = {f["table"]: f["kind"] for f in res["findings"]}
    _ok(kinds == {"table0": ti.CUT, "table4": ti.MISSING} and res["checked"] == 3,
        f"SIARD-fil: avkuttet (komprimert) og manglende tabell funnet: {kinds}")
    n, total, p = progs[0]
    _ok(n == 3 and p.files == 3 and p.bytes == total == res["bytes"] and seen == ["ferdig"],
        "fremdrift: alle filer og alle (komprimerte) byte telt")

    files["content/schema0/table0/table0.xml"] = _table_xml(40)
    ok_zip = tmp / "hel.siard"
    _write_zip(ok_zip, files, stored=("content/schema0/table0/table0.xml",))
    kinds = {f["table"]: f["kind"] for f in ti.check_zip(ok_zip)["findings"]}
    _ok(kinds == {"table4": ti.MISSING}, "hel SIARD-fil (lagret og komprimert): ingen avkuttede")

    # Lagret og avkuttet: bare slutten leses
    files["content/schema0/table0/table0.xml"] = _table_xml(40, complete=False)
    st = tmp / "lagret_kuttet.siard"
    _write_zip(st, files, stored=("content/schema0/table0/table0.xml",))
    _ok({f["table"]: f["kind"] for f in ti.check_zip(st)["findings"]}.get("table0") == ti.CUT,
        "lagret (ukomprimert) oppføring: avkuttet funnet")

    # Skadet oppføring: byte endret inne i de komprimerte dataene
    raw = bytearray(ok_zip.read_bytes())
    with zipfile.ZipFile(ok_zip) as zf:
        zi = zf.getinfo("content/schema0/table1/table1.xml")
    pos = zi.header_offset + 30 + len(zi.filename) + zi.compress_size // 2
    raw[pos] ^= 0xFF
    dmg = tmp / "skadet.siard"
    dmg.write_bytes(bytes(raw))
    found = {f["table"]: f for f in ti.check_zip(dmg)["findings"]}
    _ok(found.get("table1", {}).get("kind") == ti.DAMAGED,
        f"skadet oppføring meldes: {found.get('table1', {}).get('detail')}")

    stopped = False
    try:
        ti.check_zip(ok_zip, stop=lambda: True)
    except ti.Stopped:
        stopped = True
    _ok(stopped, "kontrollen kan avbrytes")


def _damaged_archive(tmp: Path, name: str) -> Path:
    """Gyldig arkiv der én stor tabellfil (lagret) har et endret byte → feil CRC."""
    files = _files(cut=False)
    big = _HEAD.encode() + b"".join(_row(i).encode() for i in range(30000)) + b"</table>"
    assert len(big) > Z.BIG_FILE
    files["content/schema0/table0/table0.xml"] = big
    files["content/schema0/table4/table4.xml"] = _table_xml(7, hex_doc=False)
    z = tmp / name
    _write_zip(z, files, stored=("content/schema0/table0/table0.xml",))
    raw = bytearray(z.read_bytes())
    raw[raw.index(big[:200]) + len(big) // 2] ^= 0x01
    z.write_bytes(bytes(raw))
    return z


def test_extract_errors(tmp: Path) -> None:
    print("test_extract_errors")
    z = _damaged_archive(tmp, "crc.siard")
    out = tmp / "crc_ut"
    errors = Z.ExtractErrors()
    warns: list = []
    Z.extract_all(z, out, lambda nf, nb: Z.ByteProgress("x", nf, nb), workers=2,
                  warn=warns.append, errors=errors)
    _ok(errors.count == 1 and errors.items[0][0] == "content/schema0/table0/table0.xml"
        and "CRC" in errors.items[0][1] and len(warns) == 1,
        f"feilen samles med navn og årsak: {errors.items}")
    _ok(not (out / "content/schema0/table0/table0.xml").exists(),
        "den halvskrevne/feilaktige fila er slettet")
    _ok((out / "content/schema0/table1/table1.xml").read_bytes() == _table_xml(5, hex_doc=False)
        and (out / "header/metadata.xml").is_file(), "de andre filene er pakket ut")
    _ok(not errors.fatal, "CRC-feil avbryter ikke resten")
    full = Z.ExtractErrors()
    full.add("a", OSError(28, "No space left on device"))
    _ok(full.fatal == "disken er full", "full disk gir avbrudd (fatal)")


def test_unpack_operation(tmp: Path) -> None:
    print("test_unpack_operation")
    from siard_workflow.operations import UnpackSiardOperation
    temp_root = tmp / "temp"
    temp_root.mkdir()

    def run(z: Path, **meta):
        logs: list = []
        res = UnpackSiardOperation().run(_ctx(z, logs, temp_dir=str(temp_root), **meta))
        return res, logs

    # a) Skadet arkiv, ingen operatør: stopper, temp-mappa ryddes
    z = _damaged_archive(tmp, "skadet_arkiv.siard")
    src_bytes = z.read_bytes()
    op_a = UnpackSiardOperation()
    logs: list = []
    res = op_a.run(_ctx(z, logs, temp_dir=str(temp_root)))
    _ok(op_a.halt_on_failure is True, "stopp: ingen flere steg kjøres (halt_on_failure)")
    text ="\n".join(m for _l, m in logs)
    _ok(not res.success and "data mangler" in res.message,
        f"uten operatør stopper kjøringen: {res.message}")
    _ok("FEIL VED UTPAKKING — DATA MANGLER" in text and "table0.xml" in text
        and any(l == "feil" for l, _m in logs), "kraftig advarsel i loggen, med filnavn")
    _ok(not list(temp_root.iterdir()), "temp-mappa er ryddet")
    _ok(z.read_bytes() == src_bytes, "SIARD-fila er urørt")

    # b) Operatøren stopper
    asked: list = []
    res, _logs = run(z, ask_data_loss_cb=lambda t, x: asked.append((t, x)) or False)
    title, body = asked[0]
    _ok(not res.success and title.startswith("FEIL VED UTPAKKING")
        and "POTENSIELT STORT DATATAP" in body and "Du KAN fortsette" in body
        and "table0.xml" in body and "kjøringen er stoppet" in body,
        "operatøren får kraftig advarsel: stoppet, kan fortsette, mulig stort datatap")
    _ok(not list(temp_root.iterdir()), "stopp: temp-mappa er ryddet")

    # c) Operatøren fortsetter med datatap
    res, logs = run(z, ask_data_loss_cb=lambda t, x: True)
    d = res.data
    _ok(res.success and d.get("data_loss_accepted") and d.get("unpack_failed") == 1
        and "table0.xml" in d["unpack_failed_files"][0] and "ADVARSEL" in res.message,
        f"fortsetter: datatapet står i resultatet: {res.message}")
    _ok(any("OPERATØREN VALGTE Å FORTSETTE" in m for _l, m in logs),
        "valget står i loggen")
    ex = Path(d["extracted_path"])
    _ok(ex.is_dir() and not (ex / "content/schema0/table0/table0.xml").exists()
        and (ex / "content/schema0/table1/table1.xml").is_file(),
        "temp-mappa beholdes uten den skadede fila")
    Z.rmtree_progress(ex)

    # d) Feilfri utpakking, men tabellfila er ufullstendig i selve SIARD-fila
    files = _files(cut=True)
    files["content/schema0/table4/table4.xml"] = _table_xml(7, hex_doc=False)
    cut = tmp / "ufullstendig.siard"
    _write_zip(cut, files)
    asked.clear()
    res, logs = run(cut, ask_data_loss_cb=lambda t, x: asked.append((t, x)) or False)
    title, body = asked[0]
    _ok(not res.success and title.startswith("UFULLSTENDIGE TABELLFILER")
        and "schema0/table0 HKJOURDOC" in body and "nytt uttrekk" in body,
        "ufullstendig tabellfil i SIARD-fila: stopper, navngir tabellen, råder til nytt uttrekk")
    res, _logs = run(cut, ask_data_loss_cb=lambda t, x: True)
    _ok(res.success and res.data.get("tables_incomplete") == 1
        and res.data.get("unpack_failed") == 0, "fortsetter: ufullstendig tabell i resultatet")
    Z.rmtree_progress(Path(res.data["extracted_path"]))

    # e) Helt uttrekk: ingen spørsmål
    files["content/schema0/table0/table0.xml"] = _table_xml(40)
    good = tmp / "helt.siard"
    _write_zip(good, files)
    asked.clear()
    logs = []
    res = op_a.run(_ctx(good, logs, temp_dir=str(temp_root),
                        ask_data_loss_cb=lambda t, x: asked.append(t) or False))
    _ok(op_a.halt_on_failure is False, "samme steg kjørt på nytt uten feil: stopper ikke flyten")
    _ok(res.success and not asked and "data_loss_accepted" not in res.data
        and any("alle slutter med </table>" in m for _l, m in logs),
        "helt uttrekk: ingen spørsmål, kontrollen står i loggen")
    Z.rmtree_progress(Path(res.data["extracted_path"]))


def test_hex_extract(tmp: Path) -> None:
    print("test_hex_extract")
    import siard_workflow.operations.hex_extract_operation as H

    # Avkuttet tabellfil: forståelig feil, fila urørt, ingen midlertidig fil igjen
    root = tmp / "hex_kuttet"
    _write_dir(root, _files(cut=True))
    xml = root / "content/schema0/table0/table0.xml"
    before = xml.read_bytes()
    logs: list = []
    res = H.HexExtractOperation().run(_ctx(tmp / "dummy.siard", logs, root))
    _ok(not res.success and "table0.xml (HKJOURDOC) er ufullstendig" in res.message
        and "</table>" in res.message, f"forståelig feil: {res.message[:110]} …")
    _ok(xml.read_bytes() == before, "den avkuttede tabellfila er ikke endret")
    _ok(not list(root.rglob("*.tmp_hex")), "ingen midlertidig fil ligger igjen")

    # Hel tabellfil: verdiene flyttes til filer, tabellfila byttes inn
    root2 = tmp / "hex_hel"
    _write_dir(root2, _files(cut=False))
    xml2 = root2 / "content/schema0/table0/table0.xml"
    res = H.HexExtractOperation().run(_ctx(tmp / "dummy.siard", [], root2))
    new = xml2.read_bytes()
    _ok(res.success and new.count(b'file="') == 40 and ti.file_complete(xml2)
        and len(list((xml2.parent / "lob3").iterdir())) == 40,
        "hel tabell: 40 dokumenter flyttet til filer, ny tabellfil er hel")
    _ok(not list(root2.rglob("*.tmp_hex")), "midlertidig fil er byttet inn (ingen rester)")

    # Avbrudd idet den nye fila skal byttes inn: den gamle står hel
    root3 = tmp / "hex_avbrudd"
    _write_dir(root3, _files(cut=False))
    xml3 = root3 / "content/schema0/table0/table0.xml"
    before3 = xml3.read_bytes()
    real = H.replace_file

    def _boom(_tmp, _dst):
        raise OSError("simulert avbrudd")
    H.replace_file = _boom
    try:
        res = H.HexExtractOperation().run(_ctx(tmp / "dummy.siard", [], root3))
    finally:
        H.replace_file = real
    _ok(not res.success and xml3.read_bytes() == before3 and ti.file_complete(xml3),
        "avbrudd ved innbytting: den gamle tabellfila står urørt og hel")


def test_atomic_write(tmp: Path) -> None:
    print("test_atomic_write")
    p = tmp / "tabell.xml"
    p.write_bytes(b"gammel")
    aw.write_bytes_atomic(p, b"ny")
    _ok(p.read_bytes() == b"ny" and not list(tmp.glob("tabell.xml.tmp*")),
        "ny fil byttet inn, ingen midlertidig fil igjen")
    real = os.replace

    def _boom(_a, _b):
        raise OSError("simulert avbrudd")
    os.replace = _boom
    failed = False
    try:
        aw.write_bytes_atomic(p, b"halv")
    except OSError:
        failed = True
    finally:
        os.replace = real
    _ok(failed and p.read_bytes() == b"ny" and not list(tmp.glob("tabell.xml.tmp*")),
        "feil underveis: den gamle fila står, og den midlertidige er ryddet")
    calls: list = []

    def _busy(a, b):
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError("opptatt")
        real(a, b)
    os.replace = _busy
    try:
        aw.write_bytes_atomic(p, b"tredje")
    finally:
        os.replace = real
    _ok(p.read_bytes() == b"tredje" and len(calls) == 3,
        "målfila opptatt et øyeblikk (antivirus): nye forsøk")


def test_summary_and_report() -> None:
    print("test_summary_and_report")
    from siard_workflow.core.run_summary import (
        build_run_summary, data_loss_notes, overall_status)
    from siard_workflow.operations.workflow_report_operation import follow_up_items
    data = {"data_loss_accepted": True, "unpack_failed": 3, "tables_incomplete": 1}
    notes = data_loss_notes({"unpack_siard": data})
    _ok(len(notes) == 2 and "3 fil(er)" in notes[0] and "</table>" in notes[1],
        f"datatap som korte setninger: {notes}")
    _ok(data_loss_notes({"unpack_siard": {"files_extracted": 9}}) == []
        and data_loss_notes({}) == [], "ingen datatap → ingen setninger")
    steps = [{"id": "unpack_siard", "label": "Pakk ut SIARD", "success": True,
              "index": 0, "elapsed": 3, "data": data}]
    lvl, text = overall_status(steps, data_loss=notes)
    _ok(lvl == "error" and text.startswith("FULLFØRT MED DATATAP"), f"samlet status: {text[:60]} …")
    _ok(overall_status(steps) == ("success", "FULLFØRT uten feil"),
        "uten datatap er statusen som før")
    lines = build_run_summary(steps, total_s=3, data_loss=notes)
    _ok(sum(1 for _l, t in lines if t.startswith("  DATATAP:")) == 2,
        "sluttsammendraget lister datatapet")
    items, _failed = follow_up_items(steps, {})
    _ok(sum(1 for i in items if i.startswith("DATATAP:")) == 2,
        "kjørerapporten får oppfølgingspunkt (gir «GODKJENT MED AVVIK»)")
    _ok(follow_up_items([{"id": "unpack_siard", "label": "x", "success": True,
                          "data": {"files_extracted": 5}}], {})[0] == [],
        "uten datatap: ingen oppfølgingspunkt")


def test_disk_full(tmp: Path) -> None:
    """Full disk under utpakking: vent og spør — ikke avbryt automatisk."""
    print("test_disk_full")
    import errno
    from siard_workflow.operations import UnpackSiardOperation

    # ── Porten alene ─────────────────────────────────────────────────────────
    free = {"v": 100 << 20}
    real_free = Z.DiskFullGate.free
    Z.DiskFullGate.free = lambda self: free["v"]
    try:
        calls: list = []

        def ask(info):
            calls.append(dict(info))
            if len(calls) == 3:
                free["v"] += 10 << 30            # nå har operatøren ryddet
            return True
        gate = Z.DiskFullGate("X:\\temp", ask, lambda: 42 << 30)
        _ok(gate.handle("a.xml") is True and len(calls) == 3
            and [c["no_gain"] for c in calls] == [False, True, True]
            and calls[0]["remaining"] == 42 << 30 and calls[0]["free"] == 100 << 20,
            "«Fortsett» uten mer ledig plass → varsles på nytt; går videre når plassen har økt")
        _ok(gate.resumed == 1 and not gate.aborted, "utpakkingen fortsetter (ikke avbrutt)")

        free["v"] = 100 << 20
        small: list = []

        def ask_small(info):
            small.append(info["no_gain"])
            free["v"] += 1000 if len(small) == 1 else 5 << 20    # 1 kB er ikke «mer plass»
            return True
        g_small = Z.DiskFullGate("X:\\temp", ask_small)
        _ok(g_small.handle("a.xml") is True and small == [False, True],
            "noen få byte ekstra regnes ikke som mer ledig plass")

        asked: list = []
        g2 = Z.DiskFullGate("X:\\temp", lambda info: asked.append(1) or False)
        _ok(g2.handle("a.xml") is False and g2.aborted and g2.handle("b.xml") is False
            and len(asked) == 1, "«Avbryt»: avbrutt, og det spørres ikke igjen")

        # To tråder får «full disk» samtidig: operatøren spørres én gang
        free["v"] = 100 << 20
        n_ask: list = []
        inside = threading.Event()
        release = threading.Event()

        def ask_slow(info):
            n_ask.append(1)
            inside.set()
            release.wait(5)
            free["v"] += 1 << 30
            return True
        g3 = Z.DiskFullGate("X:\\temp", ask_slow)
        out: list = []
        t1 = threading.Thread(target=lambda: out.append(g3.handle("a.xml")))
        t1.start()
        inside.wait(5)
        t2 = threading.Thread(target=lambda: out.append(g3.handle("b.xml")))
        t2.start()
        release.set()
        t1.join(5)
        t2.join(5)
        _ok(out == [True, True] and len(n_ask) == 1,
            "flere tråder stopper samtidig: ett spørsmål, alle fortsetter")

        # ── extract_all: disken «blir full» midt i den store tabellfila ───────
        files = _files(cut=False)
        big = _HEAD.encode() + b"".join(_row(i).encode() for i in range(30000)) + b"</table>"
        files["content/schema0/table0/table0.xml"] = big
        files["content/schema0/table4/table4.xml"] = _table_xml(7, hex_doc=False)
        z = tmp / "stor.siard"
        _write_zip(z, files)
        state = {"full": True, "fails": 0}
        real_raw = Z._extract_raw

        def fake_raw(fh, offset, csize, size, method, crc, target, prog):
            if state["full"] and target.endswith("table0.xml"):
                with open(target, "wb") as f:
                    f.write(b"halvskrevet")
                prog.add(123456)                     # fremdrift meldt før feilen
                state["fails"] += 1
                raise OSError(errno.ENOSPC, "No space left on device")
            return real_raw(fh, offset, csize, size, method, crc, target, prog)
        Z._extract_raw = fake_raw
        try:
            def run_extract(name, on_full):
                progs: list = []
                errors = Z.ExtractErrors()
                state.update(full=True, fails=0)
                free["v"] = 50 << 20
                dest = tmp / name
                Z.extract_all(z, dest, lambda nf, nb: progs.append(Z.ByteProgress("x", nf, nb))
                              or progs[-1], workers=3, errors=errors, on_disk_full=on_full)
                return dest, progs[0], errors

            seen: list = []

            def on_full(info):
                seen.append(dict(info))
                if len(seen) == 2:                   # andre gang: plass ryddet
                    free["v"] += 20 << 30
                    state["full"] = False
                return True
            dest, prog, errors = run_extract("fortsatt", on_full)
            _ok(len(seen) == 2 and seen[1]["no_gain"] and seen[0]["name"].endswith("table0.xml")
                and seen[0]["remaining"] > 0,
                "full disk: operatøren spørres, og på nytt når plassen ikke har økt")
            _ok(not errors and not errors.fatal
                and (dest / "content/schema0/table0/table0.xml").read_bytes() == big
                and (dest / "content/schema0/table1/table1.xml").is_file(),
                "etter rydding: fila pakkes ut på nytt og alt er med — ingen feil")
            _ok(prog.bytes == prog.total and prog.files == prog.n_files,
                f"fremdriften teller ikke fila dobbelt: {prog.bytes} av {prog.total}")

            dest, prog, errors = run_extract("avbrutt", lambda info: False)
            _ok(errors.fatal == "disken er full" and errors.count >= 1
                and not (dest / "content/schema0/table0/table0.xml").exists(),
                "operatøren avbryter: utpakkingen stopper, halvskrevet fil er slettet")

            dest, prog, errors = run_extract("uten_operator", None)
            _ok(errors.fatal == "disken er full" and state["fails"] == 1,
                "uten operatør å spørre: avbrytes som før")

            # ── Pakk ut SIARD ────────────────────────────────────────────────
            temp_root = tmp / "temp"
            temp_root.mkdir()
            dialogs: list = []

            def ask_cb(title, text):
                dialogs.append((title, text))
                if len(dialogs) == 2:
                    free["v"] += 20 << 30
                    state["full"] = False
                return True
            state.update(full=True, fails=0)
            free["v"] = 50 << 20
            logs: list = []
            op = UnpackSiardOperation()
            res = op.run(_ctx(z, logs, temp_dir=str(temp_root), ask_disk_full_cb=ask_cb))
            text = "\n".join(m for _l, m in logs)
            title, body = dialogs[0]
            _ok(res.success and len(dialogs) == 2 and "data_loss_accepted" not in res.data,
                "Pakk ut SIARD: venter, fortsetter etter rydding — fullført uten datatap")
            _ok(title.startswith("DISKEN ER FULL") and "ingenting er avbrutt" in body
                and "Ledig nå" in body and "Gjenstår å pakke ut" in body
                and "Rydd plass" in body and "Avbryt kjøringen" in body,
                "varselet: disken er full, hva som gjenstår, rydd og fortsett eller avbryt")
            _ok("IKKE blitt mer ledig plass" in dialogs[1][1],
                "andre varsel sier at det ikke er blitt mer ledig plass")
            _ok("DISKEN ER FULL" in text and "varsler på nytt" in text
                and "utpakkingen fortsetter" in text, "venting og fortsettelse står i loggen")
            Z.rmtree_progress(Path(res.data["extracted_path"]))

            state.update(full=True, fails=0)
            free["v"] = 50 << 20
            logs = []
            res = op.run(_ctx(z, logs, temp_dir=str(temp_root),
                              ask_disk_full_cb=lambda t, x: False))
            _ok(not res.success and op.halt_on_failure and "disken er full" in res.message
                and not list(temp_root.iterdir())
                and any("avbrøt utpakkingen" in m for _l, m in logs),
                "Pakk ut SIARD: operatøren avbryter → stoppet, temp-mappa ryddet")
        finally:
            Z._extract_raw = real_raw
    finally:
        Z.DiskFullGate.free = real_free


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="tabellsjekk_") as _d:
        tmp = Path(_d)
        (tmp / "g").mkdir()
        test_disk_full(tmp / "g")
        test_tail()
        test_check_dir(tmp / "a")
        (tmp / "b").mkdir()
        test_check_zip(tmp / "b")
        (tmp / "c").mkdir()
        test_extract_errors(tmp / "c")
        (tmp / "d").mkdir()
        test_unpack_operation(tmp / "d")
        (tmp / "e").mkdir()
        test_hex_extract(tmp / "e")
        (tmp / "f").mkdir()
        test_atomic_write(tmp / "f")
        test_summary_and_report()
    print("\nALLE TESTER OK")
