"""
Siegfried-identifisering gjenbrukes etter et avbrudd.

Bakgrunn (nov. 2026): en stor SIARD ble avbrutt i BLOB-konverteringen. Ved ny
kjøring fra prosjektfila ble hele Siegfried-forhåndsskanningen kjørt på nytt —
resultatene lå bare i minnet, og prosjektfila/resume-filen lagrer bare hele
steg og ferdig konverterte filer.

Verifiserer (med en falsk sf — teller filene som faktisk skannes):
  1. resultatene skrives fortløpende til <siard>_siegfried_cache.tsv
  2. ny prosess med samme filer: alt hentes fra cachen, sf kalles ikke;
     identify() gir samme resultat, og identifikasjonsloggen får alle filer
  3. endret fil (størrelse/tid) skannes på nytt; avbrutt (delvis) cache →
     bare resten skannes
  4. annen kilde-SIARD eller ny Siegfried-versjon → cachen brukes ikke
  5. BLOB-konverteringen: filnavn ved siden av resume-filen

Kjør:  python -X utf8 tests/test_sf_cache.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


class _FakeRun:
    """Erstatter subprocess.run i siegfried-modulen: «identifiserer» filene som PDF."""

    def __init__(self):
        self.scanned: list = []

    def __call__(self, cmd, **kw):
        files = cmd[4:]
        self.scanned += files

        class R:
            returncode = 0
            stdout = json.dumps({"files": [
                {"filename": f, "matches": [{"id": "fmt/276", "format": "Acrobat PDF 1.7",
                                             "basis": "byte match", "warning": ""}]}
                for f in files]})
        return R()


class _Log:
    def __init__(self):
        self.rows: list = []

    def write(self, **kw):
        self.rows.append(kw)


def _ident(sf_exe: Path, fake: _FakeRun):
    import siard_workflow.core.identifiers.siegfried as S
    S.subprocess.run = fake
    i = S.SiegfriedIdentifier()
    i.sf_exe = str(sf_exe)
    return i


def main() -> int:
    import subprocess as _sp
    import siard_workflow.core.identifiers.siegfried as S
    real_run = S.subprocess.run
    try:
        with tempfile.TemporaryDirectory(prefix="sfcache_") as td:
            tmp = Path(td)
            sf = tmp / "sf.exe"
            sf.write_bytes(b"falsk sf")
            lob = tmp / "x" / "content" / "schema0" / "table1" / "lob3"
            lob.mkdir(parents=True)
            files = []
            for n in range(250):
                p = lob / f"record{n}.bin"
                p.write_bytes(b"%PDF-1.7 " + str(n).encode() * (n % 7 + 1))
                files.append(p)
            cache = tmp / "uttrekk_siegfried_cache.tsv"
            src = "C:/uttrekk.siard|123|456"

            print("test_write")
            f1 = _FakeRun()
            i1 = _ident(sf, f1)
            i1.pre_scan(tmp, files=files, max_workers=2, cache_file=cache, source_key=src)
            lines = cache.read_text("utf-8").splitlines()
            _ok(len(f1.scanned) == 250 and i1.prescan_scanned == 250,
                "første kjøring: alle 250 filer skannes")
            _ok(lines[0].startswith("#siard-sf-cache\t1\t") and len(lines) == 251,
                "alle resultater er skrevet til cache-fila")

            print("test_reuse")
            f2 = _FakeRun()
            i2 = _ident(sf, f2)
            log = _Log()
            i2.set_logger(log)
            i2.pre_scan(tmp, files=files, max_workers=2, cache_file=cache, source_key=src)
            _ok(f2.scanned == [] and i2.prescan_reused == 250,
                "ny kjøring (ny prosess): alt fra cachen, sf kalles ikke")
            _ok(i2.identify(path=files[7])[0] == i1.identify(path=files[7])[0],
                "identify() gir samme resultat fra cachen")
            _ok(len(log.rows) == 250 and log.rows[0]["puid"] == "fmt/276"
                and log.rows[0]["filename"] == str(files[0]),
                "identifikasjonsloggen får alle filer (PUID og opprinnelig sti)")

            print("test_changed_and_partial")
            files[3].write_bytes(b"%PDF-1.4 endret innhold, annen lengde")
            f3 = _FakeRun()
            i3 = _ident(sf, f3)
            i3.pre_scan(tmp, files=files, max_workers=2, cache_file=cache, source_key=src)
            _ok([Path(p).name for p in f3.scanned] == ["record3.bin"],
                "endret fil skannes på nytt, resten gjenbrukes")
            cache.unlink()
            _ident(sf, _FakeRun()).pre_scan(tmp, files=files, max_workers=2,
                                            cache_file=cache, source_key=src)
            lines = cache.read_text("utf-8").splitlines()
            cache.write_text("\n".join(lines[:101]) + "\n", "utf-8")    # «avbrutt» etter 100
            f4 = _FakeRun()
            i4 = _ident(sf, f4)
            i4.pre_scan(tmp, files=files, max_workers=2, cache_file=cache, source_key=src)
            _ok(i4.prescan_reused == 100 and len(f4.scanned) == 150,
                "avbrutt (delvis) cache: bare de 150 manglende skannes")

            print("test_invalidate")
            f5 = _FakeRun()
            _ident(sf, f5).pre_scan(tmp, files=files, max_workers=2, cache_file=cache,
                                    source_key="C:/annen.siard|1|2")
            _ok(len(f5.scanned) == 250, "annen kilde-SIARD → cachen brukes ikke")
            f6 = _FakeRun()
            _ident(sf, f6).pre_scan(tmp, files=files, max_workers=2, cache_file=cache,
                                    source_key="C:/annen.siard|1|2")
            _ok(f6.scanned == [], "… og cache-fila er skrevet på nytt for den nye kilden")
            time.sleep(0.05)
            sf.write_bytes(b"ny versjon av sf")
            os.utime(sf, None)
            f7 = _FakeRun()
            _ident(sf, f7).pre_scan(tmp, files=files, max_workers=2, cache_file=cache,
                                    source_key="C:/annen.siard|1|2")
            _ok(len(f7.scanned) == 250, "ny Siegfried-versjon → alt skannes på nytt")

            print("test_blob_paths")
            from siard_workflow.operations.blob_convert_operation import (
                _resume_json_path, _sf_cache_path)
            _ok(_sf_cache_path(tmp, "Uttrekk").parent == _resume_json_path(tmp, "Uttrekk").parent
                and _sf_cache_path(tmp, "Uttrekk").name == "Uttrekk_siegfried_cache.tsv",
                "cache-fila ligger ved siden av resume-fila i loggmappa")
    finally:
        S.subprocess.run = real_run
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
