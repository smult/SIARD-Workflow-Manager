"""
Test for utpakking av komprimerte .bin-filer (zip/gz/bz2-container) i
BLOB-konverteringen. Verifiserer at:

  1. En .bin som egentlig er bzip2-pakket XML pakkes ut, og at den utpakkede
     fila får riktig filendelse (.xml) — også når «Standardiser .bin» er PÅ.
  2. Dette fungerer uavhengig av deteksjonsmotor (også når motoren returnerer
     «bin», slik Siegfried gjør for prefiks-bzip2).
  3. Ekte, ikke-pakket binærinnhold IKKE røres (beholder .bin).

Kjør:  python -X utf8 tests/test_bin_unpack.py
"""
from __future__ import annotations

import sys
import threading
import shutil
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import siard_workflow.core.file_identifier as fid
from siard_workflow.operations.blob_convert_operation import (
    BlobConvertOperation, _get_standardize_bin_ext,
)

_EXAMPLE = _ROOT / "extras" / "rec1014.bin"


def _example_bytes() -> bytes:
    """Bruk den ekte eksempelfila hvis den finnes, ellers syntetiser samme
    container-format: [uint32 LE ukomprimert lengde][bzip2-strøm] → XML."""
    if _EXAMPLE.exists():
        return _EXAMPLE.read_bytes()
    import bz2
    import struct
    xml = (b'<?xml version="1.0" encoding="ISO-8859-1"?>'
           b'<Message xmlns="http://www.kith.no/xmlstds/epikrise">'
           b'<Pasient><Navn>Ola Nordmann</Navn></Pasient></Message>')
    return struct.pack("<I", len(xml)) + bz2.compress(xml)


def _noop(*a, **k):
    pass


def _run_convert_all(extract_dir: Path, rel_paths: list[str]):
    op = BlobConvertOperation()
    op.params["max_workers"] = 1
    stats = {"detected": 0, "converted": 0, "kept": 0, "failed": 0,
             "xml_updated": 0, "inline_extracted": 0, "missing_blob_refs": 0}
    logs = []
    op._convert_all(
        rel_paths, extract_dir, stats,
        lambda m, lvl="info": logs.append((lvl, m)),  # w
        _noop,                                         # progress
        "soffice",                                     # lo_bin (ubrukt for xml)
        threading.Event(), threading.Event(),          # stop, pause
        csv_log=None, xml_type_hints=None, err_log=None,
        conversion_registry={}, creg_lock=threading.Lock(),
        emit_phase_events=False)
    return stats, logs


class _BinIdentifier:
    """Stub-motor som simulerer Siegfried: kjenner IKKE igjen prefiks-bzip2-
    containeren (svarer «bin»), men identifiserer rent XML-innhold korrekt
    (slik PRONOM fmt/101 gjør). Dette tester at container-sniffen får fila
    pakket ut selv om hovedmotoren bommer på containeren."""
    name = "stub-sf"

    def identify(self, data=None, path=None):
        head = (data or b"")[:64].lstrip()
        if head.startswith(b"<?xml") or head.startswith(b"<"):
            return ("xml", "application/xml", False)
        return ("bin", "application/octet-stream", False)

    def pre_scan(self, *a, **k):
        return None


def _setup(td: Path) -> tuple[Path, str]:
    lob = td / "content" / "schema0" / "table0" / "lob4"
    lob.mkdir(parents=True)
    (lob / "rec1014.bin").write_bytes(_example_bytes())
    return lob, "content/schema0/table0/lob4/rec1014.bin"


def _check_unpacked(lob: Path):
    files = sorted(p.name for p in lob.iterdir())
    assert files == ["rec1014.xml"], f"forventet rec1014.xml, fikk {files}"
    head = (lob / "rec1014.xml").read_bytes()[:40]
    assert head.startswith(b"<?xml"), f"innhold ikke XML: {head!r}"
    print(f"    [ok] rec1014.bin → rec1014.xml  (XML-innhold: {head[:30]!r}…)")


def main() -> int:
    src = "ekte eksempelfil" if _EXAMPLE.exists() else "syntetisk container"
    print(f"standardize_bin_ext = {_get_standardize_bin_ext()} (default) "
          f"| kilde: {src}")

    orig_active = fid._active
    try:
        # ── 1) Magic-backend (default) ───────────────────────────────────────
        fid._active = None  # tving reload → magic (use_siegfried default False)
        print("Test 1: magic-backend, standardize_bin PÅ")
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            lob, rel = _setup(td)
            stats, logs = _run_convert_all(td, [rel])
            _check_unpacked(lob)

        # ── 2) Motor som svarer «bin» (Siegfried-simulering) ─────────────────
        print("Test 2: motor svarer 'bin' (Siegfried-lignende), standardize_bin PÅ")
        fid._active = _BinIdentifier()
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            lob, rel = _setup(td)
            stats, logs = _run_convert_all(td, [rel])
            _check_unpacked(lob)

        # ── 3) Ekte binærfil skal IKKE røres ─────────────────────────────────
        print("Test 3: ekte binær .bin beholder .bin")
        fid._active = None  # magic igjen
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            lob = td / "content" / "schema0" / "table0" / "lob4"
            lob.mkdir(parents=True)
            # Tilfeldige bytes uten container-magic
            (lob / "rec9.bin").write_bytes(bytes(range(256)) * 8)
            rel = "content/schema0/table0/lob4/rec9.bin"
            stats, logs = _run_convert_all(td, [rel])
            names = sorted(p.name for p in lob.iterdir())
            assert names == ["rec9.bin"], f"ekte binær ble endret: {names}"
            print(f"    [ok] ekte binær beholdt: {names}")
    finally:
        fid._active = orig_active

    print("\nALLE TESTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
