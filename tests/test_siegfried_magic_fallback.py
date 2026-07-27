"""
Regresjonstest: Siegfried-backend skal falle tilbake til magic-bytes når
PRONOM/DROID ikke kjenner igjen filen.

Bakgrunn: PRONOM-signaturer krever ofte at sluttmarkøren (f.eks. «%%EOF» for
PDF) ligger nær slutten av fila. Database-eksporterte LOB-er har gjerne
etterfølgende padding eller et lengde-prefiks, slik at Siegfried returnerer
«bin» mens magic-bytes kjenner igjen formatet på header-en. Da skal backend-en
bruke magic-resultatet — ellers telles f.eks. PDF-er feilaktig som .bin.

Testen mocker bort selve sf-kallet (krever ikke sf installert) ved å sette
_sf_single/cache til å returnere «bin», og verifiserer at identify() likevel
gir «pdf» via fallbacken — og at ekte binærinnhold forblir «bin».

Kjør:  python -X utf8 tests/test_siegfried_magic_fallback.py
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.identifiers.siegfried import SiegfriedIdentifier

# En liten, men gyldig PDF-header. Magic-bytes trenger kun «%PDF».
_PDF = (b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\n"
        b"trailer<</Root 1 0 R>>\nstartxref\n9\n%%EOF")
_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  [ok] {msg}")


class _NoMatchSiegfried(SiegfriedIdentifier):
    """Siegfried-stub som later som sf alltid svarer «ingen match» (bin)."""

    def __init__(self):
        super().__init__()
        self.sf_exe = "sf"  # is_available() ser kun at strengen ikke er tom

    def is_available(self) -> bool:
        return True

    # sf-kallet: simuler «UNKNOWN» → _parse_match([]) gir bin
    def _sf_single(self, path, data):  # type: ignore[override]
        base = self._parse_match([])           # ("bin", ...)
        return self._magic_fallback(
            self._hybrid_encrypt(base, data, path), data, path)


def test_fallback_via_data() -> None:
    print("test_fallback_via_data")
    idf = _NoMatchSiegfried()
    _ok(idf.identify(data=_PDF)[0] == "pdf",
        "padded/ukjent PDF via data → magic-fallback gir pdf")
    _ok(idf.identify(data=_PNG)[0] == "png",
        "PNG via data → magic-fallback gir png")
    _ok(idf.identify(data=os.urandom(2048))[0] == "bin",
        "ekte binærinnhold forblir bin")


def test_fallback_via_path() -> None:
    print("test_fallback_via_path")
    idf = _NoMatchSiegfried()
    with tempfile.TemporaryDirectory() as d:
        # PDF med etterfølgende padding (typisk DB-LOB) lagret som .bin
        p = Path(d) / "record1.bin"
        p.write_bytes(_PDF + b"\x00" * 5000)
        _ok(idf.identify(path=p)[0] == "pdf",
            "padded PDF-fil (.bin) via path → pdf")
        _ok(idf.identify(data=p.read_bytes()[:65536], path=p)[0] == "pdf",
            "padded PDF med både data og path → pdf")


def test_cache_hit_gets_fallback() -> None:
    print("test_cache_hit_gets_fallback")
    idf = _NoMatchSiegfried()
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "rec.bin"
        p.write_bytes(_PDF)
        # Simuler at pre_scan cachet «bin» for denne stien
        idf._cache[idf._cache_key(p)] = ("bin", "application/octet-stream", False)
        _ok(idf.identify(data=_PDF, path=p)[0] == "pdf",
            "cache-treff «bin» overstyres av magic-fallback til pdf")


def test_real_match_is_preserved() -> None:
    print("test_real_match_is_preserved")
    idf = _NoMatchSiegfried()
    # Når Siegfried FAKTISK matcher (ikke bin), skal fallbacken ikke røre noe.
    base = ("tiff", "image/tiff", False)
    _ok(idf._magic_fallback(base, _PDF, None) == base,
        "ekte Siegfried-treff (tiff) bevares selv om header ser annerledes ut")


def main() -> int:
    test_fallback_via_data()
    test_fallback_via_path()
    test_cache_hit_gets_fallback()
    test_real_match_is_preserved()
    print("\nALLE SIEGFRIED-FALLBACK-TESTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
