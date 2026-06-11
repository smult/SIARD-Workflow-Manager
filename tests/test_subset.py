"""
Test for referensielt konsistent datareduksjon (subset.py).

Bygger et syntetisk utpakket SIARD-tre med relasjonskjede
person → kontakt → kontaktlinje (+ liten oppslagstabell), kjører utvalget og
verifiserer hovedinvarianten: for HVER beholdt rad peker alle fremmednøkler på
en forelder-rad som også er beholdt.

Kjør:  python -X utf8 tests/test_subset.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.operations.anonymize_operation import read_tables, _table_xml_path
from siard_workflow.core.anonymize import subset as S


def _meta() -> str:
    def tbl(name, folder, cols, pk, rows, fks=""):
        cols_xml = "".join(
            f"<column><name>{n}</name><type>{t}</type></column>" for n, t in cols)
        pk_xml = (f"<primaryKey><name>pk_{name}</name>"
                  f"<column>{pk}</column></primaryKey>")
        return (f"<table><name>{name}</name><folder>{folder}</folder>"
                f"<columns>{cols_xml}</columns>{pk_xml}{fks}<rows>{rows}</rows></table>")

    fk_kontakt = ("<foreignKeys><foreignKey><name>fk_kp</name>"
                  "<referencedSchema>schema0</referencedSchema>"
                  "<referencedTable>person</referencedTable>"
                  "<reference><column>person_id</column><referenced>id</referenced>"
                  "</reference></foreignKey></foreignKeys>")
    fk_linje = ("<foreignKeys><foreignKey><name>fk_lk</name>"
                "<referencedSchema>schema0</referencedSchema>"
                "<referencedTable>kontakt</referencedTable>"
                "<reference><column>kontakt_id</column><referenced>id</referenced>"
                "</reference></foreignKey></foreignKeys>")
    tables = (
        tbl("person", "table0",
            [("id", "INTEGER"), ("navn", "VARCHAR(50)")], "id", 5)
        + tbl("kontakt", "table1",
              [("id", "INTEGER"), ("person_id", "INTEGER"), ("dato", "DATE")],
              "id", 10, fk_kontakt)
        + tbl("kontaktlinje", "table2",
              [("id", "INTEGER"), ("kontakt_id", "INTEGER"), ("tekst", "VARCHAR(99)")],
              "id", 20, fk_linje)
        + tbl("kode", "table3",
              [("id", "INTEGER"), ("verdi", "VARCHAR(20)")], "id", 3)
    )
    return ('<?xml version="1.0" encoding="utf-8"?>'
            '<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"'
            ' version="2.1"><dbname>DB</dbname><schemas><schema>'
            '<name>schema0</name><folder>schema0</folder><tables>'
            + tables + '</tables></schema></schemas></siardArchive>')


def _table(rows: list[str]) -> str:
    # SIARD-skrivere legger én <row> per linje — match det (radstreaming er
    # linjebasert i både subset.iter_rows og operasjonens omskriving).
    body = "\n".join(rows)
    return ('<?xml version="1.0" encoding="utf-8"?>\n'
            '<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd">\n'
            + body + "\n</table>\n")


def _build_tree(root: Path):
    (root / "header").mkdir(parents=True)
    (root / "header" / "metadata.xml").write_text(_meta(), encoding="utf-8")

    def write(folder, rows):
        d = root / "content" / "schema0" / folder
        d.mkdir(parents=True)
        (d / f"{folder}.xml").write_text(_table(rows), encoding="utf-8")

    # person 1..5
    write("table0", [f"<row><c1>{i}</c1><c2>Person {i}</c2></row>"
                     for i in range(1, 6)])
    # kontakt 1..10, person_id = ((i-1)%5)+1
    write("table1", [f"<row><c1>{i}</c1><c2>{((i-1)%5)+1}</c2><c3>2020-01-{i:02d}</c3></row>"
                     for i in range(1, 11)])
    # kontaktlinje 1..20, kontakt_id = ((i-1)%10)+1
    write("table2", [f"<row><c1>{i}</c1><c2>{((i-1)%10)+1}</c2><c3>linje {i}</c3></row>"
                     for i in range(1, 21)])
    # kode 1..3 (liten oppslagstabell)
    write("table3", [f"<row><c1>{i}</c1><c2>K{i}</c2></row>" for i in range(1, 4)])


def _e2e() -> None:
    """Ende-til-ende: kjør anonymiseringens _process_tree med subset på, og
    verifiser at OUTPUT-filene er redusert OG referensielt konsistente, og at
    metadata <rows> er oppdatert."""
    import threading
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation
    from siard_workflow.core.anonymize.fake_generators import MappingStore

    class _Ctx:
        def __init__(self, root):
            self.extracted_path = root
            self.siard_path = root / "x.siard"
            self.metadata = {}
            self.results = {}

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        _build_tree(root)
        meta_path = root / "header" / "metadata.xml"

        op = AnonymizeOperation(
            subset_enabled=True, subset_rows=2, subset_include_children=True,
            subset_table_mode="heuristic", use_ollama=False, show_preview=False,
            replace_lobs=False, dry_run=False)
        # Felt som ellers settes i run():
        op._ollama = None
        op._mapping = MappingStore()
        op._ollama_budget = 0
        op._budget_lock = threading.Lock()
        op._workers = 2

        logs = []
        def w(m, lvl="info"): logs.append((lvl, m))
        def progress(*a, **k): pass

        stats = op._process_tree(root, _Ctx(root), w, progress, dry_run=False)
        print("[info] e2e stats:", {k: stats[k] for k in stats
                                    if k.startswith("subset") or k == "tables"})

        # Les output på nytt
        tables = read_tables(meta_path)
        rels = S.read_relations(meta_path, tables)

        # Faktiske rader igjen per tabell
        kept_pk: dict = {}
        kept_count: dict = {}
        for tk, info in tables.items():
            xp = _table_xml_path(root, info)
            pkcols = rels[tk].pk_cols
            pks = set()
            n = 0
            for _pos, row in S.iter_rows(xp):
                n += 1
                if pkcols:
                    pks.add(tuple(row.get(c) for c in pkcols))
            kept_pk[tk] = pks
            kept_count[tk] = n

        print("[info] rader igjen:", {tables[tk]["table_name"]: kept_count[tk]
                                      for tk in tables})

        # Reduksjon faktisk skjedd (kontaktlinje 20 → mye færre)
        assert kept_count["schema0/table2"] < 20, kept_count
        # Ingen tom tabell
        for tk in tables:
            assert kept_count[tk] > 0, f"{tk} tom"

        # METADATA <rows> == faktisk antall i output
        import xml.etree.ElementTree as ET
        mroot = ET.parse(meta_path).getroot()
        def _loc(t): return t.split('}')[-1]
        meta_rows = {}
        for sch in mroot.iter():
            if _loc(sch.tag) != "schema":
                continue
            sf = next((c.text for c in sch if _loc(c.tag) == "folder"), "")
            for tb in sch.iter():
                if _loc(tb.tag) != "table":
                    continue
                tf = next((c.text for c in tb if _loc(c.tag) == "folder"), "")
                rv = next((c.text for c in tb if _loc(c.tag) == "rows"), None)
                meta_rows[f"{sf}/{tf}"] = int(rv) if rv else None
        for tk in tables:
            assert meta_rows.get(tk) == kept_count[tk], \
                f"metadata <rows> {meta_rows.get(tk)} != {kept_count[tk]} for {tk}"
        print("[ok] metadata <rows> oppdatert til faktisk antall")

        # HOVEDINVARIANT i OUTPUT: hver FK-verdi i en beholdt rad finnes som PK
        # blant beholdte foreldre-rader.
        violations = 0
        for tk, info in tables.items():
            rel = rels[tk]
            if not rel.fks:
                continue
            xp = _table_xml_path(root, info)
            for _pos, row in S.iter_rows(xp):
                for fk in rel.fks:
                    lv = tuple(row.get(c) for c in fk.cols)
                    if any(v is None for v in lv):
                        continue
                    if lv not in kept_pk.get(fk.parent_key, set()):
                        violations += 1
                        print(f"  BRUDD: {tables[tk]['table_name']} FK={lv} "
                              f"mangler forelder i {fk.parent_key}")
        assert violations == 0, f"{violations} referanse-brudd i output"
        print("[ok] OUTPUT referensielt konsistent (alle FK har forelder)")


def _e2e_lob() -> None:
    """Subset + LOB: foreldreløse LOB-filer (forkastede rader) skal slettes,
    beholdte rader sine LOB-filer skal bestå (og byttes til dummy)."""
    import threading
    from siard_workflow.operations.anonymize_operation import AnonymizeOperation
    from siard_workflow.core.anonymize.fake_generators import MappingStore

    class _Ctx:
        def __init__(self, root):
            self.extracted_path = root
            self.siard_path = root / "x.siard"
            self.metadata = {}
            self.results = {}

    meta = ('<?xml version="1.0" encoding="utf-8"?>'
            '<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"'
            ' version="2.1"><dbname>DB</dbname><schemas><schema>'
            '<name>schema0</name><folder>schema0</folder><tables>'
            '<table><name>person</name><folder>table0</folder><columns>'
            '<column><name>id</name><type>INTEGER</type></column>'
            '<column><name>navn</name><type>VARCHAR(50)</type></column>'
            '<column><name>dok</name><type>NBLOB</type>'
            '<lobFolder>schema0/table0/lob0</lobFolder></column>'
            '</columns><primaryKey><name>pk</name><column>id</column></primaryKey>'
            '<rows>5</rows></table></tables></schema></schemas></siardArchive>')

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "header").mkdir(parents=True)
        (root / "header" / "metadata.xml").write_text(meta, encoding="utf-8")
        t0 = root / "content" / "schema0" / "table0"
        t0.mkdir(parents=True)
        rows = "\n".join(
            f'<row><c1>{i}</c1><c2>Navn {i}</c2>'
            f'<c3 file="rec{i}.bin" length="5" digest="00" digestType="MD5"/></row>'
            for i in range(1, 6))
        (t0 / "table0.xml").write_text(
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<table xmlns="http://www.bar.admin.ch/xmlns/siard/2/table.xsd">\n'
            + rows + "\n</table>\n", encoding="utf-8")
        lob = t0 / "lob0"
        lob.mkdir()
        for i in range(1, 6):
            (lob / f"rec{i}.bin").write_bytes(b"DATA" + bytes([i]))

        op = AnonymizeOperation(
            subset_enabled=True, subset_rows=2, subset_table_mode="manual",
            subset_important_tables="person", use_ollama=False,
            show_preview=False, replace_lobs=True, dry_run=False)
        op._ollama = None
        op._mapping = MappingStore()
        op._ollama_budget = 0
        op._budget_lock = threading.Lock()
        op._workers = 1

        op._process_tree(root, _Ctx(root), lambda m, l="info": None,
                         lambda *a, **k: None, dry_run=False)

        remaining = sorted(p.name for p in lob.iterdir())
        # 5 rader → 2 beholdt (posisjon 0 og 2 = rec1 og rec3)
        assert remaining == ["rec1.bin", "rec3.bin"], remaining
        # Beholdte rader sine file=-ref skal fortsatt finnes i XML
        out_xml = (t0 / "table0.xml").read_text(encoding="utf-8")
        assert 'file="rec1.bin"' in out_xml and 'file="rec3.bin"' in out_xml
        assert 'file="rec2.bin"' not in out_xml, "forkastet rad fortsatt i XML"
        print(f"[ok] subset+LOB: foreldreløse slettet, beholdt {remaining}")


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        _build_tree(root)
        meta_path = root / "header" / "metadata.xml"

        tables = read_tables(meta_path)
        assert set(tables) == {"schema0/table0", "schema0/table1",
                               "schema0/table2", "schema0/table3"}, list(tables)

        rels = S.read_relations(meta_path, tables)
        # person har PK, ingen FK; kontakt har 1 FK → person
        assert rels["schema0/table0"].pk_cols == (1,)
        assert len(rels["schema0/table1"].fks) == 1
        fk = rels["schema0/table1"].fks[0]
        assert fk.parent_key == "schema0/table0" and fk.cols == (2,) and fk.parent_cols == (1,)
        assert rels["schema0/table2"].fks[0].parent_key == "schema0/table1"
        print("[ok] read_relations: PK/FK korrekt tolket")

        index = S.build_index(root, tables, rels, _table_xml_path)
        assert index.row_count == {"schema0/table0": 5, "schema0/table1": 10,
                                   "schema0/table2": 20, "schema0/table3": 3}, index.row_count
        print("[ok] build_index: radantall korrekt")

        plan = S.SubsetPlan(
            important={"schema0/table0"},   # person er viktig
            n_rows=2, include_children=True, child_cap=5,
            keep_full_threshold=5)          # kode (3 rader) beholdes helt
        keep, info = S.select_rows(index, rels, tables, plan)
        print("[info] per_table:", info["per_table"])

        # Ingen tabell tom (alle fikk minst et frø-utvalg)
        for tk in tables:
            assert keep[tk], f"tabell {tk} ble tom"
        # Liten oppslagstabell beholdt i sin helhet
        assert len(keep["schema0/table3"]) == 3, keep["schema0/table3"]

        # Barn-ekspansjon: viktige person-frø skal ha dratt med kontakt-rader
        assert len(keep["schema0/table1"]) >= 2

        # ── HOVEDINVARIANT: alle FK-er i beholdte rader peker på beholdt forelder
        violations = 0
        for tk in tables:
            rel = rels[tk]
            for pos in keep[tk]:
                row_fks = index.fk_local.get(tk, {}).get(pos, {})
                for fi, fk in enumerate(rel.fks):
                    lv = row_fks.get(fi)
                    if lv is None:
                        continue
                    parent_pos = index.fwd[fk.parent_key][tuple(fk.parent_cols)].get(lv)
                    if parent_pos is None or parent_pos not in keep[fk.parent_key]:
                        violations += 1
                        print(f"  BRUDD: {tk} rad {pos} FK {fk.name}={lv} "
                              f"→ forelder {fk.parent_key} ikke beholdt")
        assert violations == 0, f"{violations} referanse-brudd"
        print("[ok] referanseintegritet: alle FK peker på beholdte foreldre")

        # Reduksjon faktisk skjedd
        assert info["total_kept"] < info["total_original"], info
        print(f"[ok] redusert {info['total_original']} → {info['total_kept']} rader")

        # Heuristikk-anbefaling: person bør rangeres høyt (mest referert)
        rec = S.recommend_important_heuristic(tables, rels, index.row_count, top_k=2)
        assert "schema0/table0" in rec or "schema0/table1" in rec, rec
        print("[ok] heuristikk anbefaler sentrale tabeller:", rec)

    print("\n--- Ende-til-ende gjennom _process_tree ---")
    _e2e()
    _e2e_lob()

    print("\nALLE SUBSET-TESTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
