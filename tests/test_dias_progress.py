"""
DIAS-pakking skal gi tilbakemelding underveis.

Bakgrunn (nov. 2026): DIAS-pakking av et uttrekk på 163,5 GB fra en
nettverksdisk. Loggvinduet sto stille etter «[12] DIAS-pakking (SIP/AIC)» i
over en halv time, og fremdriftslinja viste ingenting:
  * DIAS-steget skrev bare til loggfila — aldri til loggvinduet
  * fasene (sjekksum, tar-arkiv, sjekksum av tar-fila, flytting til utmappa)
    leste/skrev hele uttrekket hver gang uten fremdrift
  * tar-fila ble lest en ekstra gang bare for å beregne sjekksummen
  * Stopp virket ikke, og en feil lot en halvferdig tar-fil ligge i temp

Verifiserer:
  1. meldingene kommer i loggvinduet, og fasene melder fremdrift
  2. sjekksummen til tar-fila beregnes mens den skrives (ingen ny lesing) og
     stemmer med fila som ligger i pakken; METS har innholdets sjekksum
  3. flytting mellom ulike disker: kopi med fremdrift, innhold og tidsstempel
     bevart; halvferdig kopi slettes ved feil
  4. Stopp: steget avbrytes, temp ryddes, ingen pakke i utmappa

Kjør:  python -X utf8 tests/test_dias_progress.py
"""
from __future__ import annotations

import hashlib
import os
import re
import sys
import tarfile
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


def _siard(path: Path, mb: int = 24) -> Path:
    """Syntetisk SIARD med ukomprimerbart innhold (flere leseblokker)."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as z:
        z.writestr("header/metadata.xml",
                   '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/'
                   'xmlns/siard/2/metadata.xsd"><dbname>test</dbname><lobFolder>content'
                   '</lobFolder></siardArchive>')
        for i in range(mb):
            z.writestr(f"content/schema0/table0/lob1/record{i}.bin", os.urandom(1 << 20))
    return path


def _op(D, out: Path):
    op = D.DiasPackageOperation()
    for k in op._REQUIRED_META:
        op.params[k] = op.params.get(k) or "x"
    op.params["period_start"], op.params["period_end"] = "2010-01-01", "2015-12-31"
    op.params["output_dir"] = str(out)
    op.params["extra_files"] = "[]"
    return op


def _ctx(siard: Path, temp: Path, events: list, stop: threading.Event):
    from siard_workflow.core.context import WorkflowContext

    def pcb(event, **kw):
        events.append((event, kw))

    return WorkflowContext(siard_path=siard, metadata={
        "file_logger": None, "stop_event": stop, "pause_event": threading.Event(),
        "log_dir": str(siard.parent), "temp_dir": str(temp), "progress_cb": pcb})


def main() -> int:
    threading.Timer(180, lambda: os._exit(3)).start()
    from siard_workflow.core import zip_progress as zp
    from siard_workflow.operations import dias_package_operation as D
    zp.GUI_INTERVAL_S = 0.0            # hver blokk gir en fremdriftsmelding i testen

    with tempfile.TemporaryDirectory(prefix="diasprog_") as td:
        tmp = Path(td)
        temp = tmp / "temp"
        temp.mkdir()
        siard = _siard(tmp / "arkiv.siard")

        print("\n[1] Meldinger i loggvinduet og fremdrift i fasene")
        events: list = []
        hashed: list = []
        real_sha = D._sha256_file

        def _counting_sha(path):
            hashed.append(str(path))
            return real_sha(path)

        D._sha256_file = _counting_sha
        try:
            r = _op(D, tmp / "ut").run(_ctx(siard, temp, events, threading.Event()))
        finally:
            D._sha256_file = real_sha
        _ok(r.success, f"DIAS-pakke laget ({r.message})")
        logs = [kw.get("msg", "") for ev, kw in events if ev == "log"]
        for needle in ("Temp for pakking", "Beregner sjekksummer (SHA-256) for innholdet",
                       "Skriver tar-arkivet", "Tar-arkivet skrevet", "Ferdig! AIC-mappe"):
            _ok(any(needle in m for m in logs), f"loggvinduet får «{needle} …»")
        _ok(any("1 fil(er)" in m and "MB" in m for m in logs if "innholdet" in m),
            "loggen sier hvor mye som skal leses")
        texts = [kw.get("text", "") for ev, kw in events if ev == "phase_progress"]
        for verb in ("Beregner sjekksum", "Skriver tar-arkiv"):
            mine = [t for t in texts if t.startswith(verb + ":")]
            _ok(len(mine) >= 3, f"fasen «{verb}» melder fremdrift ({len(mine)} meldinger)")
        done = [kw["done"] for ev, kw in events
                if ev == "phase_progress" and kw.get("text", "").startswith("Skriver tar-arkiv")]
        _ok(done == sorted(done) and done[-1] == 1000,
            "fremdriften øker jevnt og ender på 100 %")

        print("\n[2] Sjekksummer")
        aic = Path(r.data["aic_path"])
        tar_file = next(aic.rglob("*.tar"))
        info = (aic / "info.xml").read_text(encoding="utf-8")
        disk_sha = hashlib.sha256(tar_file.read_bytes()).hexdigest()
        _ok(f'CHECKSUM="{disk_sha}"' in info,
            "info.xml har sjekksummen til tar-fila slik den ligger i pakken")
        _ok(f'SIZE="{tar_file.stat().st_size}"' in info, "info.xml har tar-filas størrelse")
        _ok(not any(h.endswith(".tar") for h in hashed),
            "tar-fila leses ikke på nytt for å beregne sjekksummen")
        with tarfile.open(tar_file) as t:
            names = t.getnames()
            member = next(n for n in names if n.endswith("/content/arkiv.siard"))
            data = t.extractfile(member).read()
            mets = t.extractfile(next(n for n in names if n.endswith("/mets.xml"))).read().decode()
            order = [n.rsplit("/", 1)[-1] for n in names]
        _ok(data == siard.read_bytes(), "SIARD-fila ligger uendret i tar-arkivet")
        _ok(hashlib.sha256(data).hexdigest() in mets, "METS har SIARD-filas sjekksum")
        _ok(order[-1] == "arkiv.siard" and "mets.xml" in order[:-1],
            "rekkefølgen i tar-arkivet er som før (metadata først, innhold sist)")
        _ok(not any(temp.iterdir()), "temp-mappa er ryddet")

        print("\n[3] Flytting mellom ulike disker")
        src = tmp / "flytt_fra"
        (src / "sip" / "content").mkdir(parents=True)
        (src / "sip" / "tom").mkdir()
        (src / "info.xml").write_text("<info/>", encoding="utf-8")
        big = src / "sip" / "content" / "pakke.tar"
        big.write_bytes(os.urandom(9 << 20))
        os.utime(big, (1_600_000_000, 1_600_000_000))
        want = hashlib.sha256(big.read_bytes()).hexdigest()
        real_rename = Path.rename

        def _no_rename(self, target):
            raise OSError(17, "ulike disker")

        ev3: list = []
        msgs3: list = []
        Path.rename = _no_rename
        try:
            dst = tmp / "ut" / "flyttet"
            D._move_package(src, dst, lambda m, l="info": msgs3.append(m),
                            lambda e, **kw: ev3.append((e, kw)), None)
        finally:
            Path.rename = real_rename
        got = dst / "sip" / "content" / "pakke.tar"
        _ok(hashlib.sha256(got.read_bytes()).hexdigest() == want
            and (dst / "info.xml").read_text(encoding="utf-8") == "<info/>"
            and (dst / "sip" / "tom").is_dir(), "innholdet (og tomme mapper) er kopiert")
        _ok(int(got.stat().st_mtime) == 1_600_000_000, "tidsstempelet er bevart")
        _ok(any("ulike disker" in m and "kopierer pakken" in m for m in msgs3),
            "loggen forklarer hvorfor pakken kopieres")
        _ok(sum(1 for e, kw in ev3 if kw.get("text", "").startswith(
            "Kopierer pakken til utmappa")) >= 3, "kopieringen melder fremdrift")

        stop3 = threading.Event()
        seen: list = []

        def _stop_midway(e, **kw):
            seen.append(e)
            if len(seen) == 2:
                stop3.set()

        Path.rename = _no_rename
        try:
            dst2 = tmp / "ut" / "avbrutt"
            try:
                D._move_package(src, dst2, lambda m, l="info": None, _stop_midway, stop3)
                stopped = False
            except D.PackagingStopped:
                stopped = True
        finally:
            Path.rename = real_rename
        _ok(stopped and not dst2.exists(), "Stopp under kopiering: halvferdig kopi slettes")
        _ok(big.exists(), "kilden i temp er urørt til kopien er ferdig")

        print("\n[4] Stopp under pakkingen")
        for when in ("Beregner sjekksum", "Skriver tar-arkiv"):
            stop = threading.Event()
            events4: list = []
            ctx = _ctx(siard, temp, events4, stop)
            inner = ctx.metadata["progress_cb"]

            def pcb(event, _when=when, _stop=stop, _inner=inner, **kw):
                _inner(event, **kw)
                if event == "phase_progress" and kw.get("text", "").startswith(_when):
                    _stop.set()

            ctx.metadata["progress_cb"] = pcb
            out4 = tmp / f"ut4_{when.split()[0]}"
            r4 = _op(D, out4).run(ctx)
            _ok(not r4.success and "stoppet" in r4.message.lower(),
                f"Stopp under «{when}»: {r4.message}")
            _ok(not any(temp.iterdir()), "  temp-mappa er ryddet (ingen halvferdig tar-fil)")
            _ok(not out4.exists() or not any(out4.iterdir()), "  ingen pakke i utmappa")

        print("\n[5] Feil underveis rydder temp")
        real_pack = D._pack_sip

        def _boom(*a, **k):
            real_pack(*a, **k)
            raise OSError("disken er full")

        D._pack_sip = _boom
        try:
            r5 = _op(D, tmp / "ut5").run(_ctx(siard, temp, [], threading.Event()))
        finally:
            D._pack_sip = real_pack
        _ok(not r5.success and "disken er full" in r5.message, f"feilen meldes: {r5.message}")
        _ok(not any(temp.iterdir()), "tar-fila i temp er slettet etter feilen")

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
