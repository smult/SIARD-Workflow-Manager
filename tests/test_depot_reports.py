"""
Røyktest for depot-rapportene. Bygger alle fire rapportene med syntetiske data
og verifiserer at gyldige PDF-filer produseres.

Kjør:  python -X utf8 tests/test_depot_reports.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.operations import depot_reports as dr
from siard_workflow.operations.report_style import fmt_int, fmt_pct, human_size


def _fake_meta() -> dict:
    return {
        "db_name": "HsProDemo",
        "db_product": "Microsoft SQL Server 2019",
        "db_origin": "Demo kommune",
        "connection": "jdbc:sqlserver://demo",
        "db_user": "arkiv",
        "siard_version": "2.2",
        "description": "Helsejournaler 0–18 år (demo)",
        "producer_app": "Full Convert Pro 24.12",
        "archival_date": "2026-01-23",
        "data_origin_time_span": "2004-01-01 – 2023-02-11",
        "schema_count": 1,
        "table_count": 3,
        "row_count": 123456,
        "lob_table_count": 2,
        "lob_file_count": 42,
        "zip_entry_count": 5000,
        "file_size": 1024 * 1024 * 250,
        "content_extensions": ["pdf", "txt", "tif", "html"],
        "message_digest": "abc123def4567890",
        "message_digest_algo": "SHA-256",
        "schemas": [{
            "name": "schema0", "folder": "schema0", "description": "Hoved",
            "tables": [
                {"name": "KONTAKT", "folder": "table102",
                 "description": "Et oppmøte/konsultasjon", "rows": 100,
                 "columns": [
                     {"name": "id", "description": "Nøkkel"},
                     {"name": "navn", "description": ""},
                 ]},
                {"name": "DavItem", "folder": "table17",
                 "description": "", "rows": 200,
                 "columns": [{"name": "data", "description": ""}]},
                {"name": "TBDOK", "folder": "table167",
                 "description": "Timebok-dokumenter", "rows": 300,
                 "columns": [{"name": "doc", "description": "Dokument"}]},
            ],
        }],
    }


def _fake_csv_rows() -> list[dict]:
    rows = []
    # Konverterte PDF-er
    for i in range(8):
        rows.append({
            "fra_fil": f"content/schema0/table167/lob3/rec{i}.txt",
            "fra_storrelse": 500 * 1024, "fra_ext": "rtf",
            "til_fil": f"content/schema0/table167/lob3/rec{i}.pdf",
            "til_storrelse": 300 * 1024, "til_ext": "pdf",
            "kommentar": "Konvertert til PDF/A",
        })
    # Beholdte
    for i in range(5):
        rows.append({
            "fra_fil": f"content/schema0/table102/lob87/rec{i}.txt",
            "fra_storrelse": 1024, "fra_ext": "txt",
            "til_fil": f"content/schema0/table102/lob87/rec{i}.txt",
            "til_storrelse": 1024, "til_ext": "txt",
            "kommentar": "Beholdt originalformat",
        })
    # Feilede
    for i in range(3):
        rows.append({
            "fra_fil": f"content/schema0/table167/lob3/bad{i}.txt",
            "fra_storrelse": 976 * 1024, "fra_ext": "rtf",
            "til_fil": "", "til_storrelse": 0, "til_ext": "",
            "kommentar": "Conversion error: Traceback",
        })
    return rows


def _fake_errors() -> list[dict]:
    return [
        {"path": "content/schema0/table167/lob3/bad0.txt", "ext": "rtf",
         "msg": "Conversion error: Traceback (most recent call last)"},
        {"path": "content/schema0/table17/lob8/bad1.bin", "ext": "rtf",
         "msg": "Conversion timed out after 60 seconds"},
        {"path": "content/schema0/table167/lob3/bad2.txt", "ext": "rtf",
         "msg": "Conversion error: Traceback"},
    ]


def _fake_steps() -> list[dict]:
    return [
        {"id": "sha256", "label": "SHA-256 Sjekksum", "category": "Integritet",
         "success": True, "skipped": False, "elapsed": 1.2, "message": "ok"},
        {"id": "virus_scan", "label": "Virusskan", "category": "Sikkerhet",
         "success": True, "skipped": False, "elapsed": 30.0, "message": "rent"},
        {"id": "metadata_extract", "label": "Metadata-uttrekk", "category": "Metadata",
         "success": True, "skipped": False, "elapsed": 5.0, "message": "ok"},
        {"id": "blob_convert", "label": "BLOB Konverter til PDF/A", "category": "Innhold",
         "success": True, "skipped": False, "elapsed": 120.0, "message": "8 konvertert"},
    ]


def _assert_pdf(path: Path):
    assert path.exists(), f"Mangler: {path}"
    head = path.read_bytes()[:5]
    assert head.startswith(b"%PDF"), f"Ikke en PDF: {path} ({head!r})"
    assert path.stat().st_size > 1000, f"For liten PDF: {path}"
    print(f"  [ok] {path.name}  ({path.stat().st_size // 1024} KB)")


def main() -> int:
    # Enhetstest av formateringshjelpere
    assert fmt_int(472694) == "472 694", repr(fmt_int(472694))
    assert fmt_pct(703, 81312).replace(chr(160),chr(32)) == "0,9 %", repr(fmt_pct(703, 81312))
    assert fmt_pct(1, 0).replace(chr(160),chr(32)) == "0,0 %"
    assert "MB" in human_size(1024 * 1024 * 5)
    print("[ok] formateringshjelpere")

    meta = _fake_meta()
    csv_rows = _fake_csv_rows()
    errors = _fake_errors()
    blob = {"detected": 16, "converted": 8, "kept": 5, "failed": 3,
            "inline_extracted": 2, "xml_updated": 8}
    siard = Path("HsProDemo.siard")

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        print("Bygger rapporter:")

        p1 = out / "arkivinformasjon.pdf"
        dr.build_archive_info_report(meta, siard, p1)
        _assert_pdf(p1)

        p2 = out / "konvertering.pdf"
        dr.build_conversion_report(blob, csv_rows, errors, siard, p2, "HsProDemo")
        _assert_pdf(p2)

        p3 = out / "filorganisering.pdf"
        dr.build_file_organization_report(csv_rows, siard, p3, "HsProDemo",
                                          meta=meta)
        _assert_pdf(p3)

        p4 = out / "sammendrag.pdf"
        dr.build_processing_summary_report(_fake_steps(), {}, meta, blob,
                                           siard, p4, "HsProDemo")
        _assert_pdf(p4)

        # Tomt-tilfelle: ingen blob, ingen metadata-schemas
        p5 = out / "sammendrag_tom.pdf"
        dr.build_processing_summary_report([], {}, {}, {}, siard, p5, "tom")
        _assert_pdf(p5)

    print("\nALLE RAPPORTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
