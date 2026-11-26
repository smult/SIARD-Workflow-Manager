"""
Ny SIARD med endret metadata.xml — strømmet kopi med fremdrift (nov. 2026).

Bakgrunn: «Berik SIARD» som eneste steg (uten «Pakk ut SIARD») ga ingen
tilbakemelding på en SIARD-fil på 169 GB. Steget bygget HELE den nye fila i
minnet (io.BytesIO) før den ble skrevet til disk, og pakket hver fil ut og
inn igjen underveis. «Metadata-kvalitet» gjorde det samme.

Nå kopieres innholdet rått (de komprimerte bytene flyttes som de er), rett
til disk, med fremdrift i bytes og en linje per ferdig tabell.

Verifiserer:
  1. rå kopi: gyldig arkiv, uendret innhold, samme komprimering; utbyttet og
     utelatt oppføring; også arkiv skrevet med datadeskriptor og ZIP64
  2. stopp og for lite plass: ingen halvferdig fil blir liggende
  3. «Berik SIARD» alene: resultatfil, tilbakemelding per tabell, fremdrift
     med tabellnavn, og minnebruk langt under filstørrelsen
  4. «Metadata-kvalitet» alene gir fortsatt gyldig resultatfil
  5. «N/M tabeller»-linjene tynnes i loggfila som annen fremdrift

Kjør:  python -X utf8 tests/test_siard_rewrite.py
"""
from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import threading
import tracemalloc
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_NS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


class _NoSeek(io.RawIOBase):
    """Skrivestrøm uten seek → zipfile skriver datadeskriptor (flaggbit 3),
    slik bl.a. Java-verktøy (DBPTK) gjør."""

    def __init__(self, path):
        self._f = open(path, "wb")

    def writable(self):
        return True

    def write(self, b):
        return self._f.write(b)

    def flush(self):
        if not self._f.closed:
            self._f.flush()

    def close(self):
        if not self._f.closed:
            self._f.close()
        super().close()


def _metadata(tables: list) -> str:
    tbl = "".join(
        f"<table><name>{n}</name><folder>{f}</folder><description></description>"
        f"<columns><column><name>ID</name><type>INTEGER</type></column>"
        f"<column><name>NAVN</name><type>VARCHAR(50)</type></column></columns>"
        f"<rows>2</rows></table>" for n, f in tables)
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n<siardArchive xmlns="{_NS}" '
            f'version="2.1"><dbname>test</dbname><dataOwner>x</dataOwner>'
            f"<dataOriginTimespan>2010-2015</dataOriginTimespan>"
            f"<schemas><schema><name>dbo</name><folder>schema0</folder>"
            f"<tables>{tbl}</tables></schema></schemas></siardArchive>")


_TABLES = [("KLIENT", "table0"), ("SAK", "table1"), ("DOKUMENT", "table2")]


def _make_siard(path: Path, *, lob_mb: int = 0, streamed: bool = False) -> dict:
    """Syntetisk SIARD. Returnerer {navn: innhold} for kontroll."""
    content: dict = {"header/siardversion/2.1/": b"",
                     "header/metadata.xml": _metadata(_TABLES).encode()}
    for _n, folder in _TABLES:
        content[f"content/schema0/{folder}/{folder}.xml"] = (
            f"<table><row><c1>1</c1><c2>æøå {folder}</c2></row>"
            f"<row><c1>2</c1><c2>to</c2></row></table>" * 50).encode()
    content["content/schema0/table2/lob3/record1.pdf"] = b"%PDF-1.4 " + os.urandom(4000)
    content["content/schema0/table2/lob3/Særlig æøå.txt"] = "blåbærsyltetøy".encode()
    for i in range(lob_mb):
        content[f"content/schema0/table2/lob3/stor{i}.bin"] = os.urandom(1 << 20)
    target = _NoSeek(path) if streamed else path
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as z:
        for name, data in content.items():
            # tilfeldige data lagres ukomprimert, resten komprimeres
            ctype = zipfile.ZIP_STORED if name.endswith((".bin", "/")) else zipfile.ZIP_DEFLATED
            z.writestr(zipfile.ZipInfo(name, date_time=(2020, 5, 17, 12, 0, 0)), data,
                       compress_type=ctype)
    if streamed:
        target.close()
    return content


def _check_copy(src: Path, dst: Path, content: dict, replaced: dict, skipped: tuple) -> None:
    with zipfile.ZipFile(src) as a, zipfile.ZipFile(dst) as b:
        _ok(b.testzip() is None, "resultatet er et gyldig arkiv (alle CRC-er stemmer)")
        want = [n for n in a.namelist() if not n.startswith(skipped)]
        _ok(b.namelist() == want, f"samme oppføringer i samme rekkefølge ({len(want)})")
        same = all(b.read(n) == (replaced.get(n, content[n])) for n in want)
        _ok(same, "innholdet er uendret — bortsett fra det som er byttet ut")
        raw_ok = all(
            (a.getinfo(n).compress_type, a.getinfo(n).compress_size, a.getinfo(n).CRC,
             a.getinfo(n).date_time)
            == (b.getinfo(n).compress_type, b.getinfo(n).compress_size, b.getinfo(n).CRC,
                b.getinfo(n).date_time)
            for n in want if n not in replaced)
        _ok(raw_ok, "kopiert rått: samme komprimering, størrelse og dato som i kilden")
        _ok(not any(b.getinfo(n).flag_bits & 0x08 for n in want if n not in replaced),
            "størrelsene står i de lokale headerne (ingen datadeskriptor)")


def test_rewrite(tmp: Path) -> None:
    print("\n[1] Rå kopi med utbyttet metadata.xml")
    from siard_workflow.core.zip_progress import rewrite_zip
    new_meta = b"<siardArchive>ny metadata</siardArchive>"
    for label, streamed in (("vanlig arkiv", False), ("arkiv med datadeskriptor", True)):
        print(f"  — {label}")
        src, dst = tmp / f"kilde_{streamed}.siard", tmp / f"ny_{streamed}.siard"
        content = _make_siard(src, streamed=streamed)
        if streamed:
            with zipfile.ZipFile(src) as z:
                _ok(all(i.flag_bits & 0x08 for i in z.infolist() if i.file_size),
                    "(kilden har datadeskriptor på alle filer)")
        stats = rewrite_zip(src, dst, {"header/metadata.xml": new_meta},
                            skip=lambda n: n.startswith("content/schema0/table1/"))
        _ok(stats["replaced"] == 1 and stats["skipped"] == 1
            and stats["copied"] == len(content) - 2,
            f"1 byttet ut, 1 utelatt, {stats['copied']} kopiert")
        _check_copy(src, dst, content, {"header/metadata.xml": new_meta},
                    ("content/schema0/table1/",))
        _ok(not Path(str(dst) + ".part").exists(), "ingen .part-fil igjen")

    print("  — ZIP64 (store filer)")
    orig = zipfile.ZIP64_LIMIT
    zipfile.ZIP64_LIMIT = 2000            # alt over 2 kB behandles som «over 4 GB»
    try:
        src, dst = tmp / "kilde64.siard", tmp / "ny64.siard"
        content = _make_siard(src)
        rewrite_zip(src, dst, {"header/metadata.xml": new_meta})
    finally:
        zipfile.ZIP64_LIMIT = orig
    _check_copy(src, dst, content, {"header/metadata.xml": new_meta}, ("\0",))


def test_abort(tmp: Path) -> None:
    print("\n[2] Stopp og for lite plass")
    import shutil
    from siard_workflow.core import zip_progress as zp
    src, dst = tmp / "kilde_stopp.siard", tmp / "ny_stopp.siard"
    _make_siard(src, lob_mb=3)
    calls = {"n": 0}

    def stop() -> bool:
        calls["n"] += 1
        return calls["n"] > 6

    try:
        zp.rewrite_zip(src, dst, {}, stop=stop)
        raise AssertionError("skulle vært stoppet")
    except zp.RewriteCancelled:
        pass
    _ok(not dst.exists() and not Path(str(dst) + ".part").exists(),
        "stoppet underveis → verken resultatfil eller halvferdig fil ligger igjen")

    orig = shutil.disk_usage
    shutil.disk_usage = lambda _p: type("U", (), {"free": 1 << 20, "total": 1, "used": 1})()
    lines: list = []
    try:
        zp.write_siard_copy(src, dst, {}, w=lambda m, l="info": lines.append(m))
        raise AssertionError("skulle feilet på plass")
    except zp.NotEnoughSpace as exc:
        _ok("For lite ledig plass" in str(exc) and "trenger" in str(exc),
            f"for lite plass → klar feilmelding før noe skrives ({exc})".replace(str(tmp), "…"))
    finally:
        shutil.disk_usage = orig
    _ok(not dst.exists() and not Path(str(dst) + ".part").exists(), "og ingen fil er påbegynt")


def test_mapper_standalone(tmp: Path) -> None:
    print("\n[3] «Berik SIARD» som eneste steg")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.siardmapper_operation import SiardMapperOperation
    d = tmp / "mapper"
    d.mkdir()
    siard = d / "Uttrekk.siard"
    lob_mb = 48
    content = _make_siard(siard, lob_mb=lob_mb)
    tmpl = d / "mal.json"
    tmpl.write_text(json.dumps({"tables": [
        {"name": n, "description": f"Beskrivelse av {n}", "columns": [
            {"name": "ID", "description": "Løpenummer"},
            {"name": "NAVN", "description": "Navn"}]} for n, _f in _TABLES]}),
        encoding="utf-8")
    logs: list = []
    phases: list = []

    def pcb(kind, **kw):
        if kind == "log":
            logs.append(kw.get("msg", ""))
        elif kind == "phase_progress":
            phases.append(kw)

    ctx = WorkflowContext(siard_path=siard, metadata={
        "progress_cb": pcb, "stop_event": threading.Event()})
    op = SiardMapperOperation()
    op.params["json_template"] = str(tmpl)
    tracemalloc.start()
    r = op.run(ctx)
    _cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    out = d / "Uttrekk_beriket.siard"
    _ok(r.success and out.exists() and r.data.get("output_path") == str(out),
        f"resultatfil skrevet ({r.message})")
    size = siard.stat().st_size
    _ok(peak < size / 3,
        f"minnebruk {peak / 1e6:.0f} MB for en fil på {size / 1e6:.0f} MB "
        f"(før: hele fila i minnet)")
    with zipfile.ZipFile(out) as z:
        _ok(z.testzip() is None, "resultatet er en gyldig SIARD (alle CRC-er stemmer)")
        meta = z.read("header/metadata.xml").decode()
        _ok("Beskrivelse av KLIENT" in meta and "Løpenummer" in meta,
            "metadata.xml har fått beskrivelsene")
        rest = [n for n in content if n != "header/metadata.xml"]
        _ok(z.namelist() == list(content) and all(z.read(n) == content[n] for n in rest),
            "alt annet innhold er uendret")
    _ok(any(m.strip().startswith("Leser innholdslista i Uttrekk.siard") for m in logs),
        "første livstegn kommer før arkivet åpnes")
    done = [m.strip() for m in logs if "tabeller skrevet" in m]
    _ok([m.split(" (")[0] for m in done] == [
        "1/3 tabeller skrevet — KLIENT", "2/3 tabeller skrevet — SAK",
        "3/3 tabeller skrevet — DOKUMENT"],
        "én linje per ferdig tabell: " + " | ".join(done))
    _ok(any("Skriver ny SIARD-fil" in m and "ledig på disken" in m for m in logs),
        "størrelse og ledig plass meldes før skrivingen starter")
    texts = [p.get("text", "") for p in phases]
    _ok(phases and any("DOKUMENT (tabell 3 av 3)" in t for t in texts)
        and phases[-1]["done"] == 1000,
        f"fremdriften viser tabellen som skrives: «{texts[-1]}»")
    _ok(any("Skrevet: Uttrekk_beriket.siard" in m and " på " in m for m in logs),
        "sluttlinje med størrelse og tid")

    # Stoppet av brukeren
    out.unlink()
    ctx2 = WorkflowContext(siard_path=siard, metadata={"stop_event": threading.Event()})
    ctx2.metadata["stop_event"].set()
    r2 = op.run(ctx2)
    _ok(not r2.success and "Stoppet" in r2.message and not out.exists()
        and not Path(str(out) + ".part").exists(),
        "stopp → steget feiler med klar melding, og ingen fil blir liggende")


def test_metadata_quality(tmp: Path) -> None:
    print("\n[4] «Metadata-kvalitet» alene")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.metadata_quality_operation import MetadataQualityOperation
    d = tmp / "mq"
    d.mkdir()
    siard = d / "Arkiv.siard"
    content = _make_siard(siard)
    with zipfile.ZipFile(siard) as z:
        meta = z.read("header/metadata.xml").decode()
    # Filsti i dbname er det operasjonen retter
    bad = meta.replace("<dbname>test</dbname>", "<dbname>C:\\data\\uttrekk\\test.mdb</dbname>")
    src2 = d / "Arkiv2.siard"
    from siard_workflow.core.zip_progress import rewrite_zip
    rewrite_zip(siard, src2, {"header/metadata.xml": bad.encode()})
    logs: list = []
    ctx = WorkflowContext(siard_path=src2, metadata={
        "progress_cb": lambda kind, **kw: logs.append(kw.get("msg", "")) if kind == "log" else None})
    r = MetadataQualityOperation().run(ctx)
    out = Path(r.data.get("output_path", "")) if r.success else None
    _ok(r.success and out is not None and out.exists(), f"resultatfil skrevet ({r.message})")
    with zipfile.ZipFile(out) as z:
        _ok(z.testzip() is None and "C:\\data" not in z.read("header/metadata.xml").decode(),
            "gyldig SIARD med rettet metadata.xml")
        rest = [n for n in content if n != "header/metadata.xml"]
        _ok(all(z.read(n) == content[n] for n in rest), "alt annet innhold er uendret")
    _ok(any("Skriver ny SIARD-fil" in m for m in logs), "med tilbakemelding underveis")


def test_log_thinning(tmp: Path) -> None:
    print("\n[5] Loggfila: «N/M tabeller» tynnes som annen fremdrift")
    from siard_workflow.core.file_logger import WorkflowFileLogger
    d = tmp / "logg"
    d.mkdir()
    with WorkflowFileLogger(d) as fl:
        for i in range(1, 401):
            fl.log(f"  {i}/400 tabeller skrevet — TABELL_{i} (1,0 MB)", "info")
        fl.log("  Skrevet: Uttrekk_beriket.siard", "ok")
        path = Path(fl.log_path)
    text = path.read_text(encoding="utf-8")
    n = sum("tabeller skrevet" in ln for ln in text.splitlines())
    _ok(0 < n <= 12 and "400/400 tabeller" in text and "1/400 tabeller" in text,
        f"400 fremdriftslinjer ble {n} i loggfila (første, hver fjerdedel, siste)")


def main() -> int:
    threading.Timer(240, lambda: os._exit(3)).start()
    with tempfile.TemporaryDirectory(prefix="siardrewrite_") as td:
        tmp = Path(td)
        test_rewrite(tmp)
        test_abort(tmp)
        test_mapper_standalone(tmp)
        test_metadata_quality(tmp)
        test_log_thinning(tmp)
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
