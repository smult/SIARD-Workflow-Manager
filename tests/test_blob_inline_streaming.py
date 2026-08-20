"""
Tester for minne-trygg, streaming inline-LOB-ekstraksjon i BLOB-konvertering.

Store tableX.xml (hundretusener av rader) sprengte tidligere minnet fordi
_extract_inline DOM-parset hele fila. Streaming-veien (_extract_inline_streaming)
behandler én <row> om gangen (iterparse) og skriver LOB-er + patchet XML rett til
disk med konstant minne.

Verifiserer:
  1. Streaming ekstraherer inline-LOB-er korrekt til disk og produserer gyldig,
     patchet XML med file=-referanser (og uten inline-tekst).
  2. Streaming er EKVIVALENT med DOM-veien (samme LOB-stier, samme innhold,
     samme file=-attributter per celle).
  3. _extract_inline delegerer til streaming for filer over terskelen (patched
     is None), og bruker DOM-veien for små filer (patched er bytes).
  4. Minnebruken i streaming er bundet (vokser ikke med radantall) — i motsetning
     til DOM som skalerer med filstørrelsen.

Kjør:  python -X utf8 tests/test_blob_inline_streaming.py
"""
from __future__ import annotations

import sys
import tracemalloc
import xml.etree.ElementTree as ET
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.operations.blob_convert_operation import BlobConvertOperation

_HEAD = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
         b'<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd">')
_TAIL = b'</table>'

_XML_STI  = "content/schema0/table1/table1.xml"
_TABLE_KEY = "schema0/table1"
_LOB_COLS = {_TABLE_KEY: {2: "content/schema0/table1/lob2"}}
_LOB_COL_MAP = _LOB_COLS[_TABLE_KEY]
_BASE_PATH = "content/schema0/table1"


def _noop(*a, **k):
    pass


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _make_inline_xml(nrows: int) -> bytes:
    """tableX.xml der c2 er en LOB-kolonne med inline-tekst (ingen file=)."""
    rows = b"".join(
        b'<row><c1>%d</c1><c2>Inline LOB innhold for rad %d med litt tekst</c2></row>'
        % (i, i) for i in range(1, nrows + 1))
    return _HEAD + rows + _TAIL


def _make_external_xml(nrows: int) -> bytes:
    """Alle LOB-er eksterne (file=) — ingen inline å ekstrahere."""
    rows = b"".join(
        b'<row><c1>%d</c1><c2 file="rec%d.bin" length="9"/></row>' % (i, i)
        for i in range(1, nrows + 1))
    return _HEAD + rows + _TAIL


def _cell_file_attrs(xml_bytes: bytes) -> list[str]:
    """Hent file=-verdier for c2-celler i rekkefølge (ns-agnostisk)."""
    root = ET.fromstring(xml_bytes)
    out = []
    for row in root:
        for cell in row:
            if cell.tag.split("}")[-1].lower() == "c2":
                out.append(cell.get("file") or "")
    return out


def test_streaming_basic(tmp: Path) -> None:
    print("test_streaming_basic")
    op = BlobConvertOperation()
    d = tmp / "content" / "schema0" / "table1"
    d.mkdir(parents=True)
    xml_path = d / "table1.xml"
    xml_path.write_bytes(_make_inline_xml(5))

    stats = {"inline_extracted": 0}
    stis, n = op._extract_inline_streaming(
        xml_path, _XML_STI, _TABLE_KEY, stats, _noop,
        _LOB_COL_MAP, tmp, base_path=_BASE_PATH)

    _ok(n == 5, f"5 inline-LOB-er ekstrahert (fikk {n})")
    _ok(len(stis) == 5, "5 LOB-stier returnert")
    # LOB-filer skrevet til disk med riktig innhold
    first = tmp / stis[0]
    _ok(first.exists() and b"Inline LOB innhold for rad 1" in first.read_bytes(),
        "LOB-fil skrevet til disk m/innhold")
    # Patchet XML: gyldig, har file=, ingen inline-tekst igjen
    patched = xml_path.read_bytes()
    parsed = ET.fromstring(patched)   # kaster hvis ugyldig
    _ok(parsed is not None, "patchet XML er gyldig")
    attrs = _cell_file_attrs(patched)
    _ok(all(a for a in attrs) and len(attrs) == 5,
        "alle c2-celler har file=-referanse")
    _ok(b"Inline LOB innhold" not in patched, "ingen inline-tekst igjen i XML")


def test_streaming_matches_dom(tmp: Path) -> None:
    print("test_streaming_matches_dom (ekvivalens)")
    op = BlobConvertOperation()
    xml_bytes = _make_inline_xml(8)

    # DOM-vei (ingen xml_path → DOM)
    stats_d = {"inline_extracted": 0}
    patched_dom, files_dom, n_dom = op._extract_inline(
        xml_bytes, _XML_STI, _TABLE_KEY, stats_d, _noop, lob_cols=_LOB_COLS)

    # Streaming-vei
    d = tmp / "content" / "schema0" / "table1"
    d.mkdir(parents=True)
    xml_path = d / "table1.xml"
    xml_path.write_bytes(xml_bytes)
    stats_s = {"inline_extracted": 0}
    stis_s, n_s = op._extract_inline_streaming(
        xml_path, _XML_STI, _TABLE_KEY, stats_s, _noop,
        _LOB_COL_MAP, tmp, base_path=_BASE_PATH)

    _ok(n_dom == n_s == 8, f"samme antall ekstrahert (DOM={n_dom}, stream={n_s})")
    _ok(set(files_dom.keys()) == set(stis_s), "samme sett av LOB-stier")
    # Samme innhold per LOB
    same_content = all((tmp / s).read_bytes() == files_dom[s] for s in stis_s)
    _ok(same_content, "identisk LOB-innhold DOM vs streaming")
    # Samme file=-attributter per celle
    _ok(_cell_file_attrs(patched_dom) == _cell_file_attrs(xml_path.read_bytes()),
        "identiske file=-attributter per celle")


def test_threshold_delegation(tmp: Path) -> None:
    print("test_threshold_delegation")
    op = BlobConvertOperation()
    d = tmp / "content" / "schema0" / "table1"
    d.mkdir(parents=True)
    xml_path = d / "table1.xml"
    xml_bytes = _make_inline_xml(50)
    xml_path.write_bytes(xml_bytes)

    # Liten fil, default terskel → DOM-vei (patched er bytes)
    stats = {"inline_extracted": 0}
    patched, files, n = op._extract_inline(
        xml_bytes, _XML_STI, _TABLE_KEY, stats, _noop, lob_cols=_LOB_COLS,
        xml_path=xml_path, extract_dir=tmp)
    _ok(patched is not None and isinstance(files, dict) and n == 50,
        "liten fil → DOM-vei (patched=bytes, files=dict)")

    # Tving lav terskel → streaming-vei (patched=None, files=liste)
    xml_path.write_bytes(xml_bytes)   # gjenopprett (DOM endret ikke disk, men vær trygg)
    op._STREAM_INLINE_THRESHOLD = 100   # bytes
    stats2 = {"inline_extracted": 0}
    patched2, files2, n2 = op._extract_inline(
        xml_bytes, _XML_STI, _TABLE_KEY, stats2, _noop, lob_cols=_LOB_COLS,
        xml_path=xml_path, extract_dir=tmp)
    _ok(patched2 is None and isinstance(files2, list) and n2 == 50,
        "over terskel → streaming-vei (patched=None, files=liste)")


def test_streaming_vs_dom_memory(tmp: Path) -> None:
    print("test_streaming_vs_dom_memory")
    op = BlobConvertOperation()
    # DOM-veien er O(n²) (_find_parent per element), så hold radantallet moderat;
    # minneforskjellen vs streaming er tydelig uansett.
    nrows = 4_000
    xml_bytes = _make_inline_xml(nrows)

    # DOM-vei: bygger hele treet + holder ALLE LOB-bytes i new_files-dict
    tracemalloc.start()
    _p, _files, _n = op._extract_inline(
        xml_bytes, _XML_STI, _TABLE_KEY, {"inline_extracted": 0}, _noop,
        lob_cols=_LOB_COLS)
    _, dom_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Streaming-vei: skriver LOB-er + XML til disk, holder kun sti-listen
    d = tmp / "content" / "schema0" / "table1"
    d.mkdir(parents=True)
    xml_path = d / "table1.xml"
    xml_path.write_bytes(xml_bytes)
    tracemalloc.start()
    stis, n = op._extract_inline_streaming(
        xml_path, _XML_STI, _TABLE_KEY, {"inline_extracted": 0}, _noop,
        _LOB_COL_MAP, tmp, base_path=_BASE_PATH)
    _, stream_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"    rows={nrows}  DOM peak={dom_peak//1024} KB  "
          f"stream peak={stream_peak//1024} KB  ({dom_peak/max(1,stream_peak):.1f}x)")
    _ok(n == nrows, "streaming ekstraherte alle rader")
    _ok(stream_peak < dom_peak * 0.6,
        f"streaming bruker vesentlig mindre minne enn DOM "
        f"(stream={stream_peak//1024} KB vs DOM={dom_peak//1024} KB)")


def main() -> None:
    import tempfile
    with tempfile.TemporaryDirectory() as _d:
        base = Path(_d)
        for name, fn in (("a", test_streaming_basic),
                         ("b", test_streaming_matches_dom),
                         ("c", test_threshold_delegation),
                         ("d", test_streaming_vs_dom_memory)):
            sub = base / name
            sub.mkdir()
            fn(sub)
    print("\nAlle streaming-inline-tester bestått ✓")


if __name__ == "__main__":
    main()
