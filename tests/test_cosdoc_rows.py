"""
Acos/CosDoc-flettingen fjerner datakilde-rader — <rows> i metadata.xml må følge med.

Bakgrunn (nov. 2026, DBPTK-validering av behandlet CosDoc-uttrekk): operasjonen
fletter dokumentpar (SeqNr 1 + 2) og fjerner datakilde-raden fra tableX.xml.
metadata.xml sto igjen med gammelt radantall (1 227 mot 1 019 rader), og
validatoren feilet på P_4.3-10. Operasjonen førte heller ingen PREMIS-hendelse.

Samme gjennomgang viste at datakilden ble slettet også når dokumentet ikke
lot seg flette (ingen flettefelt / ingen flettedata). Da gikk flettedataene
tapt. Nå fjernes datakilden bare når den faktisk er flettet inn.

Verifiserer:
  1. core/metadata_rows: riktig tabell (samme mappenavn i to schema),
     namespace-prefiks, resten av fila urørt
  2. pipeline-kjøring: flettet datakilde fjernet, <rows> oppdatert, andre
     tabeller urørt; datakilde som ikke er flettet beholdes (fil og rad),
     også når den deles med et dokument som ble flettet
  3. PREMIS: hendelse føres, teksten nevner fjernede rader og beholdte kilder
  4. dry-run endrer ingenting og føres ikke i PREMIS

LibreOffice og avkryptering er byttet ut med enkle stedfortredere; alle data
er syntetiske.

Kjør:  python -X utf8 tests/test_cosdoc_rows.py
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


MNS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
TNS = "http://www.bar.admin.ch/xmlns/siard/2/table.xsd"


def _col(name: str, typ: str, lob: str = "") -> str:
    lobf = f"<lobFolder>{lob}</lobFolder>" if lob else ""
    return f"<column><name>{name}</name>{lobf}<type>{typ}</type></column>"


META = f"""<?xml version="1.0" encoding="utf-8"?>
<siardArchive xmlns="{MNS}" version="2.1">
  <dbname>Test</dbname>
  <lobFolder>content</lobFolder>
  <schemas>
    <schema>
      <name>annet</name>
      <folder>schema0</folder>
      <tables>
        <table>
          <name>Annet</name>
          <folder>table0</folder>
          <columns>{_col("ID", "INTEGER")}</columns>
          <rows>5</rows>
        </table>
      </tables>
    </schema>
    <schema>
      <name>dbo</name>
      <folder>schema1</folder>
      <tables>
        <table>
          <name>Tst_ElFiler</name>
          <folder>table0</folder>
          <!-- kommentar som skal stå -->
          <columns>
            {_col("Tst_EefID", "INTEGER")}{_col("Tst_EveID", "INTEGER")}{_col("Tst_SeqNr", "INTEGER")}
            {_col("Tst_FilNavn", "VARCHAR(100)")}{_col("Tst_Size", "INTEGER")}
            {_col("Tst_ElFil", "BLOB", "schema1/table0/lob6")}
          </columns>
          <rows>9</rows>
        </table>
        <table>
          <name>Sist</name>
          <folder>table1</folder>
          <columns>{_col("ID", "INTEGER")}</columns>
          <rows>7</rows>
        </table>
      </tables>
    </schema>
  </schemas>
</siardArchive>
"""


def _row(eef: int, eve: int, seq: int, fname: str, rec: str, size: int) -> str:
    return (f"<row><c1>{eef}</c1><c2>{eve}</c2><c3>{seq}</c3><c4>{fname}</c4>"
            f"<c5>{size}</c5><c6 file=\"{rec}\" length=\"{size}\"/></row>\n")


def _make(root: Path) -> Path:
    (root / "header").mkdir(parents=True)
    (root / "header" / "metadata.xml").write_text(META, encoding="utf-8")
    tdir = root / "content" / "schema1" / "table0"
    lob = tdir / "lob6"
    lob.mkdir(parents=True)
    files = [(1, 10, 1, "brev.doc", "record0.bin"),        # flettes
             (2, 10, 2, "brev_D.doc", "record1.bin"),      # datakilde → fjernes
             (3, 11, 1, "skann.pdf", "record2.bin"),       # enkeltdokument, ikke Word
             (4, 12, 1, "notat.doc", "record3.bin"),       # enkeltdokument, Word
             (5, 13, 1, "ikkeflett.doc", "record4.bin"),   # lar seg ikke flette
             (6, 13, 2, "ikkeflett_D.doc", "record5.bin"), # datakilde → beholdes
             (7, 14, 1, "felles1.doc", "record6.bin"),     # flettes
             (8, 14, 1, "ikkeflett2.doc", "record7.bin"),  # lar seg ikke flette
             (9, 14, 2, "felles_D.doc", "record8.bin")]    # delt datakilde → beholdes
    body = ""
    for eef, eve, seq, fname, rec in files:
        data = f"syntetisk innhold {eef}".encode()
        (lob / rec).write_bytes(data)
        body += _row(eef, eve, seq, fname, rec, len(data))
    (tdir / "table0.xml").write_text(
        f'<?xml version="1.0" encoding="utf-8"?>\n'
        f'<table xmlns="{TNS}" version="2.1">\n{body}</table>', encoding="utf-8")
    return root


class _Log:
    def __init__(self):
        self.lines = []

    def log(self, msg, lvl="info"):
        self.lines.append((lvl, msg))


def _patch_external(c) -> None:
    """
    Bytt ut LibreOffice, avkryptering og mailmerge med stedfortredere:
    «konvertering» til .docx er en kopi, og flettingen lykkes for alle
    dokumenter unntatt dem som heter ikkeflett*.
    """
    def _copy(src, dst, _pw, _w):
        shutil.copy2(src, dst)
        return True

    def _batch(files, out_dir, _fmt, *_a, **_k):
        out = {}
        for f in files:
            d = Path(out_dir) / (Path(f).stem + ".docx")
            shutil.copy2(f, d)
            out[f] = d
        return out

    def _merge(main_docx, _data, merged_path, _w):
        if Path(main_docx).name.startswith("ikkeflett"):
            return False
        shutil.copy2(main_docx, merged_path)
        return True

    c._find_libreoffice = lambda _hint: "soffice"
    c._decrypt_file = _copy
    c._lo_convert_batch = _batch
    c._lo_convert = lambda *a, **k: None
    c._read_merge_data_from_doc_binary = lambda *a, **k: {"Navn": "Fiktiv Person"}
    c._read_merge_data_via_html = lambda *a, **k: {}
    c._patch_docx_datasource = lambda *a, **k: None
    c._perform_mailmerge = _merge


def _run(root: Path, **params):
    import siard_workflow.systemspecific_operations.cosdoc_operation as c
    from siard_workflow.core.context import WorkflowContext
    _patch_external(c)
    log = _Log()
    ctx = WorkflowContext(siard_path=root.parent / "dummy.siard", metadata={
        "file_logger": log, "stop_event": threading.Event(),
        "pause_event": threading.Event(), "temp_dir": str(root.parent)})
    ctx.extracted_path = root
    op = c.CosDocMailMergeOperation(**params)
    res = op.run(ctx)
    return op, res, ctx, log


def test_metadata_rows() -> None:
    print("\n1. core/metadata_rows")
    from siard_workflow.core.metadata_rows import count_rows, set_table_rows
    meta = META.encode("utf-8")
    out, old = set_table_rows(meta, "schema1", "table0", 3)
    _ok(old == 9, "gammelt antall returneres (9)")
    _ok(out.count(b"<rows>3</rows>") == 1 and b"<rows>5</rows>" in out
        and b"<rows>7</rows>" in out,
        "bare schema1/table0 endres — schema0/table0 og schema1/table1 urørt")
    _ok(out.replace(b"<rows>3</rows>", b"<rows>9</rows>") == meta,
        "resten av metadata.xml er byte-identisk (kommentar og formatering bevart)")
    out2, old2 = set_table_rows(meta, "schema9", "table0", 3)
    _ok(old2 is None and out2 == meta, "ukjent tabell: None og uendret")
    pref = (meta.replace(b"<rows>", b"<m:rows>").replace(b"</rows>", b"</m:rows>")
            .replace(b"<table>", b"<m:table>").replace(b"</table>", b"</m:table>")
            .replace(b"<schema>", b"<m:schema>").replace(b"</schema>", b"</m:schema>")
            .replace(b"<folder>", b"<m:folder>").replace(b"</folder>", b"</m:folder>"))
    out3, old3 = set_table_rows(pref, "schema1", "table0", 12)
    _ok(old3 == 9 and b"<m:rows>12</m:rows>" in out3, "namespace-prefiks håndteres")
    _ok(count_rows(b"<table><row><c1>1</c1></row>\n<row>\n</row><row/></table>") == 3,
        "count_rows teller <row>, <row/> og ikke <rows>")
    _ok(count_rows(b"<table></table>") == 0, "tom tabell gir 0")


def test_pipeline() -> None:
    print("\n2. Pipeline-kjøring: flettet datakilde fjernes, ikke-flettet beholdes")
    from siard_workflow.core.metadata_rows import count_rows
    with tempfile.TemporaryDirectory() as td:
        root = _make(Path(td) / "ut")
        op, res, ctx, log = _run(root)
        _ok(res.success, f"operasjonen fullfører ({res.message})")
        lob = root / "content/schema1/table0/lob6"
        xml = (root / "content/schema1/table0/table0.xml").read_bytes()
        meta = (root / "header/metadata.xml").read_bytes()
        d = res.data
        _ok((d.get("pairs_found"), d.get("pairs_merged"), d.get("merge_failed")) == (4, 2, 2),
            "4 par: 2 flettet, 2 lot seg ikke flette")
        _ok(count_rows(xml) == 8, "tableX.xml har 8 rader (bare én datakilde fjernet)")
        _ok(b"brev_D.doc" not in xml and not (lob / "record1.bin").exists(),
            "flettet datakilde: rad og fil er fjernet")
        _ok(b"ikkeflett_D.doc" in xml and (lob / "record5.bin").exists()
            and (lob / "record5.bin").read_bytes() == b"syntetisk innhold 6",
            "datakilde for dokument som ikke ble flettet: rad og fil står urørt")
        _ok(b"felles_D.doc" in xml and (lob / "record8.bin").exists(),
            "delt datakilde beholdes når ett av dokumentene ikke ble flettet")
        _ok(b'file="record5.bin"' in xml and b'file="record8.bin"' in xml,
            "beholdte datakilder har filreferansen sin i behold")
        _ok(meta.count(b"<rows>8</rows>") == 1, "<rows> for Tst_ElFiler er 8")
        _ok(b"<rows>5</rows>" in meta and b"<rows>7</rows>" in meta,
            "andre tabellers <rows> er urørt")
        _ok(b"<!-- kommentar som skal st" in meta, "kommentar i metadata.xml bevart")
        _ok((d.get("rows_before"), d.get("rows_after"), d.get("rows_removed")) == (9, 8, 1),
            "resultatet oppgir rader før/etter/fjernet (9/8/1)")
        _ok(d.get("data_sources_kept") == 2, "resultatet oppgir 2 beholdte datakilder")
        _ok(any("<rows> for Tst_ElFiler 9 → 8" in m for _l, m in log.lines),
            "loggen sier at <rows> er oppdatert")
        _ok(any(lvl == "warn" and "Beholder datakilden ikkeflett_D.doc" in m
                for lvl, m in log.lines),
            "loggen varsler om datakilden som beholdes")
        _ok(any("Beholdt 2 datakilde(r)" in m for _l, m in log.lines)
            and any("Slettet 1 datakilde-fil(er)" in m for _l, m in log.lines),
            "loggen oppsummerer slettet og beholdt")

        print("\n3. PREMIS")
        _ok(op.modifies_content and op.premis_event_type == "Migration",
            "operasjonen er merket innholdsendrende (Migration)")
        _ok(op.premis_should_record(res, ctx), "hendelsen føres")
        detail = op.premis_detail(res, ctx)
        _ok("1 datakilde-rad(er) fjernet fra Tst_ElFiler" in detail
            and "9 → 8 rader" in detail, "teksten nevner radene som er fjernet")
        _ok("2 datakilde(r) beholdt uendret" in detail,
            f"teksten nevner beholdte datakilder: {detail}")


def test_dry_run() -> None:
    print("\n4. Dry-run endrer ingenting")
    from siard_workflow.core.metadata_rows import count_rows
    with tempfile.TemporaryDirectory() as td:
        root = _make(Path(td) / "ut")
        before_meta = (root / "header/metadata.xml").read_bytes()
        before_xml = (root / "content/schema1/table0/table0.xml").read_bytes()
        op, res, ctx, _log = _run(root, dry_run=True)
        _ok(res.success, "dry-run fullfører")
        _ok((root / "header/metadata.xml").read_bytes() == before_meta,
            "metadata.xml uendret")
        _ok((root / "content/schema1/table0/table0.xml").read_bytes() == before_xml
            and count_rows(before_xml) == 9, "tableX.xml uendret (9 rader)")
        _ok(len(list((root / "content/schema1/table0/lob6").iterdir())) == 9,
            "ingen LOB-filer slettet")
        _ok(not op.premis_should_record(res, ctx), "dry-run føres ikke i PREMIS")


def main() -> int:
    test_metadata_rows()
    test_pipeline()
    test_dry_run()
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
