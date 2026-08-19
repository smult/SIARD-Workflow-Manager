"""
Tester for synkronisering av schema-navn-referanser i metadata.xml:
når et tomt <schema><name> fylles inn — eller et navn saneres — skal
referansene <foreignKey><referencedSchema> og <column><typeSchema> oppdateres
tilsvarende, slik at uttrekket henger logisk sammen.

Ren byte-inn/byte-ut (ingen ElementTree), som resten av siard_format.

Kjør:  python -X utf8 tests/test_schema_name_refs.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.siard_format import (
    apply_schema_name_fixes,
    apply_empty_schema_reference_fixes,
    apply_schema_reference_renames,
)


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


_HEAD = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
         b'<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd">\n')
_TAIL = b'</siardArchive>\n'


def _schema(name: bytes, folder: bytes, tables: bytes) -> bytes:
    return (b'<schema><name>' + name + b'</name><folder>' + folder
            + b'</folder><tables>' + tables + b'</tables></schema>')


def test_empty_single() -> None:
    print("test_empty_single (ett tomt schema)")
    table = (b'<table><name>kontakt</name>'
             b'<columns><column><name>c1</name>'
             b'<typeSchema></typeSchema><typeName>t</typeName></column></columns>'
             b'<foreignKeys>'
             b'<foreignKey><name>fk1</name>'
             b'<referencedSchema></referencedSchema><referencedTable>person</referencedTable>'
             b'<reference><column>pid</column><referenced>id</referenced></reference></foreignKey>'
             b'<foreignKey><name>fk2</name>'
             b'<referencedSchema/><referencedTable>x</referencedTable></foreignKey>'
             b'</foreignKeys></table>')
    meta = _HEAD + b'<schemas>' + _schema(b'', b'schema0', table) + b'</schemas>' + _TAIL

    d1 = apply_schema_name_fixes(meta, {1: "min_schema"})
    _ok(b'<name>min_schema</name>' in d1, "schema-navn fylt inn")

    d2, stats = apply_empty_schema_reference_fixes(d1, {1: "min_schema"})
    _ok(stats["updated"] == 3, f"3 referanser oppdatert (fikk {stats['updated']})")
    _ok(stats["unresolved"] == 0, "ingen uløste referanser")
    _ok(d2.count(b'<referencedSchema>min_schema</referencedSchema>') == 2,
        "begge referencedSchema (inkl. selvlukkende) fylt")
    _ok(b'<typeSchema>min_schema</typeSchema>' in d2, "typeSchema fylt")
    _ok(b'<referencedSchema></referencedSchema>' not in d2
        and b'<referencedSchema/>' not in d2
        and b'<typeSchema></typeSchema>' not in d2, "ingen tomme referanser igjen")
    _ok(b'<referencedTable>person</referencedTable>' in d2,
        "referencedTable urørt")


def test_empty_whitespace_variant() -> None:
    print("test_empty_whitespace_variant")
    table = (b'<table><name>t</name><foreignKeys><foreignKey>'
             b'<referencedSchema>   </referencedSchema><referencedTable>t</referencedTable>'
             b'</foreignKey></foreignKeys></table>')
    meta = _HEAD + b'<schemas>' + _schema(b'', b'schema0', table) + b'</schemas>' + _TAIL
    d, stats = apply_empty_schema_reference_fixes(meta, {1: "s0"})
    _ok(stats["updated"] == 1 and b'<referencedSchema>s0</referencedSchema>' in d,
        "whitespace-tom referencedSchema fylt")


def test_multi_intra_schema() -> None:
    print("test_multi_intra_schema (to navnløse schemas, intra-schema FK)")
    t_a = (b'<table><name>a</name><foreignKeys><foreignKey>'
           b'<referencedSchema></referencedSchema><referencedTable>a</referencedTable>'
           b'</foreignKey></foreignKeys></table>')
    t_b = (b'<table><name>b</name><foreignKeys><foreignKey>'
           b'<referencedSchema></referencedSchema><referencedTable>b</referencedTable>'
           b'</foreignKey></foreignKeys></table>')
    meta = (_HEAD + b'<schemas>'
            + _schema(b'', b'schema0', t_a)
            + _schema(b'', b'schema1', t_b)
            + b'</schemas>' + _TAIL)
    d, stats = apply_empty_schema_reference_fixes(meta, {1: "navnA", 2: "navnB"})
    _ok(b'<referencedSchema>navnA</referencedSchema>' in d, "schema #1-blokk → navnA")
    _ok(b'<referencedSchema>navnB</referencedSchema>' in d, "schema #2-blokk → navnB")
    _ok(stats["updated"] == 2 and stats["unresolved"] == 0,
        "begge blokker løst hver for seg")


def test_non_matching_untouched() -> None:
    print("test_non_matching_untouched")
    table = (b'<table><name>t</name><foreignKeys><foreignKey>'
             b'<referencedSchema>annet_schema</referencedSchema>'
             b'<referencedTable>t</referencedTable></foreignKey></foreignKeys></table>')
    meta = _HEAD + b'<schemas>' + _schema(b'', b'schema0', table) + b'</schemas>' + _TAIL
    # tomt schema #1 fylles, men referencedSchema har allerede en ikke-tom verdi
    d, stats = apply_empty_schema_reference_fixes(meta, {1: "s0"})
    _ok(b'<referencedSchema>annet_schema</referencedSchema>' in d,
        "ikke-tom, ikke-matchende referanse urørt")
    _ok(stats["updated"] == 0, "ingen tomme referanser å fylle")


def test_sanitize_rename() -> None:
    print("test_sanitize_rename (verdibytte ved sanering)")
    table = (b'<table><name>t</name>'
             b'<columns><column><name>c</name>'
             b'<typeSchema>Min Schema!</typeSchema><typeName>ty</typeName></column></columns>'
             b'<foreignKeys><foreignKey>'
             b'<referencedSchema>Min Schema!</referencedSchema>'
             b'<referencedTable>t</referencedTable></foreignKey></foreignKeys></table>')
    meta = _HEAD + b'<schemas>' + _schema(b'Min Schema!', b'schema0', table) + b'</schemas>' + _TAIL
    d, n = apply_schema_reference_renames(meta, {"Min Schema!": "schema1"})
    _ok(n == 2, f"2 referanser omdøpt (fikk {n})")
    _ok(b'<referencedSchema>schema1</referencedSchema>' in d, "referencedSchema omdøpt")
    _ok(b'<typeSchema>schema1</typeSchema>' in d, "typeSchema omdøpt")
    # case-uavhengig match
    d2, n2 = apply_schema_reference_renames(
        meta.replace(b'<referencedSchema>Min Schema!', b'<referencedSchema>min schema!'),
        {"Min Schema!": "schema1"})
    _ok(n2 >= 1, "case-uavhengig verdi-match")
    # no-op for ukjent navn
    d3, n3 = apply_schema_reference_renames(meta, {"Ukjent": "x"})
    _ok(n3 == 0 and d3 == meta, "ukjent navn → ingen endring")


def main() -> None:
    test_empty_single()
    test_empty_whitespace_variant()
    test_multi_intra_schema()
    test_non_matching_untouched()
    test_sanitize_rename()
    print("\nAlle schema-referanse-tester bestått ✓")


if __name__ == "__main__":
    main()
