"""
Tester for LOB-referansekontrollen (DBPTK-validator A_M_5.6-1-2).

Bakgrunn (verifisert mot keeps/dbptk-developer master, siard_21/…/
MetadataColumnsValidator): validatoren slår opp "content/" + lobFolder + file
UTEN skilletegn; fallback er bare file, og starter den ikke med "content"
rapporteres «not found external lob». Basenavn + lobFolder uten avsluttende
'/' feiler derfor for HVER LOB selv om filene finnes.

Verifiserer:
  1. check_lob_refs klassifiserer: basenavn (fixable), avsluttende '/' (ok),
     full sti (ok), manglende fil (missing), ekstern referanse (external).
  2. Stikkprøve (max_refs_per_table) og scan_lob_ref_issues.
  3. XMLValidationOperation rapporterer A_M_5.6-1-2 per kolonne + oppsummering,
     og er grønn etter LobFolderFixOperation med DBPTK-validering (full sti).
  4. preflight: ENTEN KDRS Søk & Vis (standard, anbefalt) ELLER DBPTK-validering.

Kjør:  python -X utf8 tests/test_lob_ref_check.py
"""
from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.context import WorkflowContext
from siard_workflow.operations.lobfolder_fix_operation import (
    LobFolderFixOperation, check_lob_refs, scan_lob_ref_issues,
    dbptk_validator_lob_path, describe_lob_ref_column,
)
from siard_workflow.operations.standard_operations import XMLValidationOperation


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_NS_META = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
_NS_TAB  = "http://www.bar.admin.ch/xmlns/siard/2/table.xsd"


def _metadata(tables: dict[str, list[tuple[str, str, str | None]]]) -> bytes:
    tbls = ""
    for folder, cols in tables.items():
        c = "".join(
            f"<column><name>{n}</name>" + (f"<lobFolder>{lf}</lobFolder>" if lf else "")
            + f"<type>{t}</type><nullable>true</nullable></column>" for n, t, lf in cols)
        tbls += (f"<table><name>{folder}</name><folder>{folder}</folder>"
                 f"<columns>{c}</columns><rows>2</rows></table>")
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<siardArchive xmlns="{_NS_META}" version="2.1"><dbname>t</dbname>'
            f'<dataOriginTimespan>2024</dataOriginTimespan><lobFolder>content</lobFolder>'
            f'<schemas><schema><name>s</name><folder>schema0</folder>'
            f'<tables>{tbls}</tables></schema></schemas></siardArchive>\n').encode("utf-8")


def _table_xml(rows: list[str]) -> bytes:
    body = "\n".join(f"<row>{r}</row>" for r in rows)
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n<table xmlns="{_NS_TAB}" '
            f'version="2.1">\n{body}\n</table>\n').encode("utf-8")


def _build_zip(path: Path, files: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)


def _null_log():
    return {"file_logger": None, "progress_cb": None}


def _case_files() -> dict[str, bytes]:
    """
    table89 c4: lobFolder uten '/' + basenavn  → DBPTK feiler, filer finnes (fixable)
                rad 3 peker på fil som mangler                 (missing)
    table90 c2: lobFolder MED '/'              → DBPTK ok (sammensetting treffer)
    table91 c2: full content/-sti              → DBPTK ok
    table92 c2: ekstern referanse (..)         → external, ignoreres
    """
    return {
        "header/metadata.xml": _metadata({
            "table89": [("id", "INTEGER", None), ("a", "INTEGER", None),
                        ("b", "INTEGER", None), ("doc", "CLOB", "schema0/table89/lob4")],
            "table90": [("id", "INTEGER", None), ("doc", "CLOB", "schema0/table90/lob2/")],
            "table91": [("id", "INTEGER", None), ("doc", "CLOB", "schema0/table91/lob2")],
            "table92": [("id", "INTEGER", None), ("doc", "BLOB", "schema0/table92/lob2")],
        }),
        "content/schema0/table89/table89.xml": _table_xml([
            '<c1>1</c1><c4 file="record1.txt"/>',
            '<c1>2</c1><c4 file="record2.txt"/>',
            '<c1>3</c1><c4 file="record3.txt"/>',
        ]),
        "content/schema0/table89/lob4/record1.txt": b"a",
        "content/schema0/table89/lob4/record2.txt": b"b",
        "content/schema0/table90/table90.xml": _table_xml(['<c1>1</c1><c2 file="record1.txt"/>']),
        "content/schema0/table90/lob2/record1.txt": b"c",
        "content/schema0/table91/table91.xml": _table_xml(
            ['<c1>1</c1><c2 file="content/schema0/table91/lob2/record1.txt"/>']),
        "content/schema0/table91/lob2/record1.txt": b"d",
        "content/schema0/table92/table92.xml": _table_xml(['<c1>1</c1><c2 file="../ext/record1.bin"/>']),
    }


def test_check(tmp: Path) -> None:
    print("test_check")
    _ok(dbptk_validator_lob_path("schema0/table89/lob4", "record16.txt")
        == "content/schema0/table89/lob4record16.txt", "DBPTK-sammensetting uten skilletegn")
    _ok(dbptk_validator_lob_path("schema0/table89/lob4/", "record16.txt")
        == "content/schema0/table89/lob4/record16.txt", "avsluttende '/' gir gyldig sti")

    siard = tmp / "refs.siard"
    _build_zip(siard, _case_files())
    with zipfile.ZipFile(siard) as zf:
        lr = check_lob_refs(zf)
    _ok(lr["refs"] == 6 and lr["tables_scanned"] == 4, f"6 referanser i 4 tabeller ({lr['refs']}, {lr['tables_scanned']})")
    _ok(lr["dbptk_fail"] == 3 and lr["fixable"] == 2 and lr["missing"] == 1 and lr["external"] == 1,
        f"3 feiler i DBPTK: 2 rettbare, 1 mangler; 1 ekstern (fikk {lr})")
    by = {(c["table"], c["col"]): c for c in lr["columns"]}
    c89 = by[("table89", 4)]
    _ok(c89["example_ref"] == "record1.txt"
        and c89["example_full"] == "content/schema0/table89/lob4/record1.txt",
        "eksempel på rettbar referanse med full sti")
    _ok(c89["missing_examples"] == ["record3.txt"], "manglende fil listet")
    _ok(by[("table90", 2)]["dbptk_fail"] == 0, "avsluttende '/' på lobFolder → DBPTK slår opp")
    _ok(by[("table91", 2)]["dbptk_fail"] == 0, "full sti i file= → DBPTK slår opp")
    _ok(by[("table92", 2)]["external"] == 1 and by[("table92", 2)]["dbptk_fail"] == 0,
        "ekstern referanse telles som ekstern, ikke som feil")
    d = describe_lob_ref_column(c89)
    _ok("2 av 3" in d and "DBPTK-validering (full sti)" in d and "enten/eller" in d and "record3.txt" in d, f"rapportlinje: {d}")

    # Stikkprøve
    with zipfile.ZipFile(siard) as zf:
        lr2 = check_lob_refs(zf, max_refs_per_table=2)
    _ok(lr2["sampled"] and lr2["refs"] == 5 and lr2["fixable"] == 2 and lr2["missing"] == 0,
        f"stikkprøve på 2 per tabell: {lr2['refs']} referanser, rad 3 ikke lest")
    lr3 = scan_lob_ref_issues(siard)
    _ok(lr3.get("fixable") == 2, "scan_lob_ref_issues (preflight) finner rettbare")
    _ok(scan_lob_ref_issues(tmp / "finnes_ikke.siard") == {}, "lesefeil → tom dict")


def test_xml_validation_and_fix(tmp: Path) -> None:
    print("test_xml_validation_and_fix")
    siard = tmp / "refs.siard"
    res = XMLValidationOperation().run(WorkflowContext(siard_path=siard, metadata=_null_log()))
    am = [e for e in res.data["errors"] if e.startswith("A_M_5.6-1-2")]
    _ok(len(am) == 3, f"XML-validering: 1 kolonnelinje + 2 oppsummeringer (fikk {len(am)})")
    _ok(any("DBPTK-validering (full sti)" in e for e in am) and any("finnes ikke i arkivet" in e for e in am),
        "både rettbar og manglende oppsummert")

    op = LobFolderFixOperation()
    op.params["full_file_paths"] = True
    ctx = WorkflowContext(siard_path=siard, metadata=_null_log())
    rf = op.run(ctx)
    _ok(rf.success and rf.data.get("file_paths") == 3 and rf.data.get("file_unresolved") == 1
        and rf.data.get("fixes") == 1,
        f"lobfolder_fix: 3 referanser → full sti (table89 ×2, table90), 1 uløst (mangler), "
        f"1 metadata-fiks (avsluttende '/') ({rf.data})")
    out = Path(rf.data["output_path"])
    res2 = XMLValidationOperation().run(WorkflowContext(siard_path=out, metadata=_null_log()))
    am2 = [e for e in res2.data["errors"] if e.startswith("A_M_5.6-1-2")]
    _ok(len(am2) == 2 and all("finnes ikke i arkivet" in e or "mangler" in e for e in am2),
        f"etter retting gjenstår bare den manglende filen ({am2})")
    with zipfile.ZipFile(out) as zf:
        lr = check_lob_refs(zf)
    _ok(lr["fixable"] == 0 and lr["missing"] == 1 and lr["dbptk_fail"] == 1,
        "kontroll etter retting: 0 rettbare, 1 mangler")
    # table90 mistet avsluttende '/' i metadata, men fikk full sti i file= → fortsatt ok
    _ok(all(c["dbptk_fail"] == 0 for c in lr["columns"] if c["table"] == "table90"),
        "table90: fortsatt ok etter at avsluttende '/' ble fjernet (full sti skrevet)")

    op2 = XMLValidationOperation()
    op2.params["check_lob_refs"] = False
    res3 = op2.run(WorkflowContext(siard_path=siard, metadata=_null_log()))
    _ok(not any(e.startswith("A_M_5.6-1-2") for e in res3.data["errors"]), "sjekken kan slås av")


def test_ref_target_choice(tmp: Path) -> None:
    """Enten KDRS Søk & Vis (standard) eller DBPTK-validering — tydelig valg i preflight."""
    print("test_ref_target_choice")
    import customtkinter as ctk
    from gui.app import App
    import siard_workflow.operations.lobfolder_fix_operation as LF
    from siard_workflow.operations import UnpackSiardOperation, RepackSiardOperation
    _ok(LF.LobFolderFixOperation().params.get("ref_target") == LF.REF_KDRS
        and not LF.LobFolderFixOperation().use_full_paths(),
        "standard: KDRS Søk & Vis (filnavn beholdes)")
    _ok(LF.LobFolderFixOperation(ref_target=LF.REF_DBPTK).use_full_paths()
        and LF.LobFolderFixOperation(full_file_paths=True).use_full_paths(),
        "DBPTK-validering → full sti (også eldre eksplisitt full_file_paths=True)")

    root = ctk.CTk()
    root.withdraw()
    saved_scan = LF.scan_lob_ref_issues
    LF.scan_lob_ref_issues = lambda path: {
        "fixable": 555, "missing": 0, "sampled": True, "tables_scanned": 3,
        "columns": [{"fixable": 1, "schema": "schema0", "table": "table1", "col": 4,
                     "example_ref": "record1.txt",
                     "example_full": "content/schema0/table1/lob4/record1.txt"}]}
    inserted: list = []
    seen: list = []

    class _Panel:
        def insert_operation_before(self, op, before_id, silent=False):
            inserted.append((op, before_id))

        def move_operation_before(self, op, before_id, silent=False):
            inserted.append(("flyttet", before_id))

    def answer(a):
        def click():
            tops = [w for w in root.winfo_children()
                    if isinstance(w, ctk.CTkToplevel) and w.winfo_exists()]
            if not tops:
                root.after(150, click)
                return
            d = tops[0]
            seen.append(d.title())
            for w in d.winfo_children():
                for b in w.winfo_children():
                    try:
                        seen.append(str(b.cget("text")))
                    except Exception:
                        pass
            getattr(d, "_on_" + a)()
        root.after(400, click)
    try:
        root.siard_queue = [tmp / "x.siard"]
        root._pf_status = ""
        root.logs = []
        root._pf = lambda m: root.logs.append(m)
        root._log = lambda m, lvl="info": root.logs.append(m)
        root.workflow_panel = _Panel()
        pre = App._lob_fullpath_preflight.__get__(root)
        ops = [UnpackSiardOperation(), RepackSiardOperation()]

        answer("ja")
        _ok(pre(ops) is True and not inserted and "KDRS Søk & Vis (kun filnavn)" in root.logs[-1],
            "valg «KDRS Søk & Vis (anbefalt)» → ingen endring av file=")
        kd = seen.index("KDRS Søk & Vis (anbefalt)")
        db = seen.index("DBPTK-validering (full sti)")
        _ok(kd < db and "KDRS Søk & Vis eller DBPTK-validering" in seen[0]
            and any("KDRS Søk & Vis eller DBPTK-validering" in t for t in seen[1:kd]),
            "dialogen: tydelig enten/eller-overskrift, KDRS er første (uthevede) knapp")

        answer("nei")
        _ok(pre(ops) is True and inserted and inserted[-1][1] == "repack_siard"
            and inserted[-1][0].use_full_paths(),
            "valg «DBPTK-validering (full sti)» → «Korriger lobFolder» med full sti før «Pakk sammen»")

        inserted.clear()
        n_seen = len(seen)
        _ok(pre(ops + [LF.LobFolderFixOperation()]) is True and len(seen) == n_seen
            and not inserted and "filnavn beholdes" in root.logs[-1],
            "«Korriger lobFolder» med standardvalg i arbeidsflyten → ingen dialog, KDRS")
    finally:
        LF.scan_lob_ref_issues = saved_scan
        root.destroy()


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="lob_refs_") as td:
        tmp = Path(td)
        test_check(tmp)
        test_xml_validation_and_fix(tmp)
        test_ref_target_choice(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
