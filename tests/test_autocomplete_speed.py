"""
Forslagsfeltene «Kildesystem» og «Eierorganisasjon» (DIAS-vinduet og
parametervinduet) skal svare straks.

Bakgrunn (nov. 2026): feltene ble ekstremt trege. For hvert tastetrykk ble
SIARD-fila åpnet på nytt for å hente forslag fra metadata.xml. Å åpne en
SIARD betyr å lese hele innholdslista — ca. 2,5 s ved 500 000 LOB-filer — og
det skjedde i GUI-tråden. Tjenestene (kommunelista er en lokal fil,
systemlista en lokal cache som oppdateres i bakgrunnen) var ikke årsaken.

Verifiserer:
  1. SIARD-fila åpnes én gang, uansett hvor mange tastetrykk
  2. et tastetrykk venter aldri på lesingen (den går i bakgrunnen)
  3. endret SIARD-fil leses på nytt; manglende fil gir ingen feil
  4. metadata.xml med annen plassering/navnerom, og at lesingen stopper
     ved <schemas>
  5. feltet viser SIARD-forslagene øverst (★)

Kjør:  python -X utf8 tests/test_autocomplete_speed.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_NS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"


def _make_siard(path: Path, app: str, *, member: str = "header/metadata.xml",
                n_files: int = 50, ns: bool = True, tail: str = "") -> None:
    xmlns = f' xmlns="{_NS}"' if ns else ""
    meta = (f'<?xml version="1.0" encoding="UTF-8"?>\n<siardArchive{xmlns} version="2.1">'
            f"<dbname>fagsystem</dbname><dataOwner>Lindesnes kommune</dataOwner>"
            f"<producerApplication>{app}</producerApplication>"
            f"<databaseProduct>Microsoft SQL Server</databaseProduct>"
            f"<schemas><schema><name>dbo</name>{tail}</schema></schemas></siardArchive>")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as z:
        for i in range(n_files):
            z.writestr(f"content/schema0/table0/lob1/record{i}.pdf", b"")
        z.writestr(member, meta)


def _wait_ready(op, path, timeout: float = 10.0) -> dict:
    end = time.time() + timeout
    while time.time() < end:
        meta = op._siard_meta(path)
        if meta is not None:
            return meta
        time.sleep(0.02)
    raise AssertionError("metadata ble aldri klare")


def main() -> int:
    threading.Timer(120, lambda: os._exit(3)).start()
    import gui.operations_panel as op

    # Ingen nettoppslag i testen: systemlista settes direkte
    op._kildesystem_list = ["Acos WebSak (Acos)", "Visma Familia (Visma)",
                            "Visma Flyt Skole (Visma)"]

    opens: list[str] = []
    real_zip = zipfile.ZipFile

    class _CountingZip(real_zip):
        def __init__(self, file, mode="r", *a, **kw):
            if mode == "r":
                opens.append(str(file))
            super().__init__(file, mode, *a, **kw)

    tmp = Path(tempfile.mkdtemp(prefix="ac_test_"))
    zipfile.ZipFile = _CountingZip
    try:
        print("\n[1] SIARD-fila åpnes én gang")
        siard = tmp / "uttrekk.siard"
        _make_siard(siard, "Visma Familia")
        opens.clear()
        op.set_current_siard_path(siard)
        meta = _wait_ready(op, siard)
        _ok(meta.get("producerapplication") == "Visma Familia",
            "producerApplication er lest fra metadata.xml")
        t0 = time.perf_counter()
        for _ in range(300):                      # 300 «tastetrykk»
            got = op._get_siard_suggestions("kildesystem")
        dt = time.perf_counter() - t0
        _ok(got == ["Visma Familia (Visma)"], f"forslag fra SIARD: {got}")
        _ok(len(opens) == 1, f"SIARD-fila ble åpnet {len(opens)} gang for 300 tastetrykk")
        _ok(dt < 0.5, f"300 oppslag tok {dt * 1000:.0f} ms")
        _ok(op._get_siard_suggestions("kommunenummer") == [],
            "kommuneforslag uten treff gir tom liste (ingen feil)")

        print("\n[2] Et tastetrykk venter ikke på lesingen")
        slow = tmp / "stor.siard"
        _make_siard(slow, "Acos WebSak")
        real_read = op._read_siard_meta
        started = threading.Event()

        def _slow_read(path):
            started.set()
            time.sleep(1.0)                       # «SIARD med millioner av filer»
            return real_read(path)

        op._read_siard_meta = _slow_read
        try:
            t0 = time.perf_counter()
            op.set_current_siard_path(slow)
            first = op._get_siard_suggestions("kildesystem")
            dt = time.perf_counter() - t0
            _ok(first == [] and dt < 0.2,
                f"svarer straks mens fila leses ({dt * 1000:.0f} ms, ingen SIARD-forslag ennå)")
            _ok(started.wait(2), "lesingen går i en bakgrunnstråd")
            n_threads = sum(1 for t in threading.enumerate()
                            if t.name == "siard-meta-forslag")
            for _ in range(50):
                op._get_siard_suggestions("kildesystem")
            _ok(sum(1 for t in threading.enumerate()
                    if t.name == "siard-meta-forslag") == n_threads == 1,
                "flere tastetrykk starter ikke flere lesinger")
            _wait_ready(op, slow)
            _ok(op._get_siard_suggestions("kildesystem") == ["Acos WebSak (Acos)"],
                "forslagene kommer når lesingen er ferdig")
        finally:
            op._read_siard_meta = real_read

        print("\n[3] Endret og manglende fil")
        opens.clear()
        _make_siard(slow, "Visma Flyt Skole", n_files=60)     # ny størrelse/tid
        _wait_ready(op, slow)
        _ok(op._get_siard_suggestions("kildesystem") == ["Visma Flyt Skole (Visma)"],
            "endret SIARD-fil leses på nytt")
        _ok(len(opens) == 1 and
            sum(1 for k in op._siard_meta_cache if k[0] == str(slow)) == 1,
            "én lesing, og bare siste versjon ligger i minnet")
        op.set_current_siard_path(tmp / "finnes_ikke.siard")
        _ok(op._get_siard_suggestions("kildesystem") == [], "manglende fil → ingen forslag")
        op.set_current_siard_path(None)
        _ok(op._get_siard_suggestions("kildesystem") == [], "ingen aktiv fil → ingen forslag")
        bad = tmp / "ikke_zip.siard"
        bad.write_bytes(b"dette er ikke en zip-fil")
        op.set_current_siard_path(bad)
        _ok(_wait_ready(op, bad) == {} and op._get_siard_suggestions("kildesystem") == [],
            "ødelagt fil → ingen forslag, ingen feil")

        print("\n[4] Plassering, navnerom og stopp ved <schemas>")
        odd = tmp / "odd.siard"
        _make_siard(odd, "Acos WebSak", member="Header/Metadata.xml", ns=False)
        _ok(op._read_siard_meta(odd).get("producerapplication") == "Acos WebSak",
            "metadata.xml finnes også med annen skrivemåte og uten navnerom")
        big = tmp / "big.siard"
        # Ugyldig XML langt inne i <schemas>: leses ikke, så den gir ingen feil
        _make_siard(big, "Visma Familia", tail="<table>" * 3 + "</feil>")
        _ok(op._read_siard_meta(big).get("databaseproduct") == "Microsoft SQL Server",
            "lesingen stopper ved <schemas> (resten av metadata.xml leses ikke)")

        print("\n[5] Feltet viser SIARD-forslag øverst")
        import customtkinter as ctk
        root = ctk.CTk()
        root.withdraw()
        try:
            op.set_current_siard_path(siard)
            _wait_ready(op, siard)
            var = ctk.StringVar(value="vis")
            ac = op._AutocompleteEntry(root, op._kildesystem_list, var,
                                       siard_source="kildesystem")
            _ok(ac._filtered() == ["★  Visma Familia (Visma)", "Visma Flyt Skole (Visma)"],
                "SIARD-forslaget står først, resten av treffene under")
        finally:
            root.destroy()
    finally:
        zipfile.ZipFile = real_zip
        op.set_current_siard_path(None)
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)

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
