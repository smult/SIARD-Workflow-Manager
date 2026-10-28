"""
Magic-bytes: tekst i Windows-1252 og binærdata som starter med «<».

Bakgrunn (Extens VAF, okt. 2026): 2 758 rapportbeskrivelser lagret som tekst
i LONG RAW-kolonner (Windows-1252, ikke UTF-8) ble stående som ukjent «bin»,
og 6-byte binærposter som startet med 0x3C 0x00 ble identifisert som XML.

Verifiserer:
  1. norsk tekst i Windows-1252 (æøå, «», ·, –) → txt
  2. ingen falske positive: tilfeldige data, korte hasher (16 byte),
     binærposter, tekst med NUL/kontrolltegn
  3. «<» + NUL uten BOM → ikke XML; vanlig XML og UTF-16-XML med BOM → xml
  4. Siegfried-backend: «bin» faller tilbake til magic-bytes → txt

Kjør:  python -X utf8 tests/test_legacy_text.py
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


TEXTS = [
    "Rapporten viser elever med fravær over grensen for skoleåret.",
    "En rapport som gir oversikt over karakterer – fordelt på fag og klasse.\r\n"
    "· Standpunkt\r\n· Eksamen\r\n",
    "Brev til foresatte om «varsel om fare for ikke vurdering» i faget.",
    "Oversikt over søkere til Vg1 på skolen",
]


def test_legacy_text() -> None:
    print("test_legacy_text")
    from siard_workflow.core.identifiers.magic_bytes import _detect
    for t in TEXTS:
        b = t.encode("cp1252")
        _ok(_detect(b)[0] == "txt", f"Windows-1252 → txt: {t[:40]!r}")
    _ok(_detect(TEXTS[0].encode("utf-8"))[0] == "txt", "UTF-8 er fortsatt txt")


def test_no_false_positives() -> None:
    print("test_no_false_positives")
    from siard_workflow.core.identifiers.magic_bytes import _detect, _is_legacy_text
    # (UTF-8-sjekken kan fra før ta en tilfeldig 16-byte verdi for tekst —
    # her testes den nye Windows-1252-sjekken)
    rnd = random.Random(42)
    hits = 0
    for _ in range(50000):
        n = rnd.choice((16, 16, 20, 32, 50, 64, 256, 389, 1000))
        b = bytes(rnd.getrandbits(8) for _ in range(n))
        hits += _is_legacy_text(b)
    _ok(hits == 0, f"tilfeldige data (16–1000 byte, 50 000 stk.): {hits} tatt for tekst")
    # tilfeldige bytes uten kontrolltegn — verste tilfelle for 16-byte hasher
    hits = 0
    for _ in range(50000):
        b = bytes(rnd.randint(0x20, 0xFF) for _ in range(16))
        hits += _is_legacy_text(b)
    _ok(hits < 25, f"16 byte uten kontrolltegn: {hits}/50 000 tatt for tekst")
    rec = b"\x03\x00\x22Navn\x00\x00\x00\x01" + bytes(240)
    _ok(_detect(rec)[0] == "bin", "binærpost med NUL → bin")
    _ok(_detect("Tekst med æøå\x01\x02 og kontrolltegn".encode("cp1252"))[0] == "bin",
        "tekst med kontrolltegn → bin")


def test_xml_nul() -> None:
    print("test_xml_nul")
    from siard_workflow.core.identifiers.magic_bytes import _detect
    _ok(_detect(b"<\x00\x00\x01\x00\x00")[0] == "bin", "«<» + NUL (6-byte post) → bin")
    _ok(_detect(b"<rot><a>1</a></rot>")[0] == "xml", "vanlig XML → xml")
    _ok(_detect(b"\xff\xfe" + "<rot>æ</rot>".encode("utf-16-le"))[0] == "xml",
        "UTF-16-XML med BOM → xml")


def test_siegfried_fallback() -> None:
    print("test_siegfried_fallback")
    from siard_workflow.core.identifiers.siegfried import SiegfriedIdentifier
    b = TEXTS[1].encode("cp1252")
    got = SiegfriedIdentifier._magic_fallback(("bin", "application/octet-stream", False), b, None)
    _ok(got[0] == "txt", "Siegfried «bin» → magic-bytes-fallback → txt")


def main() -> int:
    test_legacy_text()
    test_no_false_positives()
    test_xml_nul()
    test_siegfried_fallback()
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
