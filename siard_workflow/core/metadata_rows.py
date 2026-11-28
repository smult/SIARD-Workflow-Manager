"""
Radantall i metadata.xml.

DBPTK-validatoren (P_4.3-10) krever at <rows> for en tabell i metadata.xml er
likt antall <row> i tableX.xml. En operasjon som fjerner eller legger til rader
må derfor oppdatere <rows> i samme omgang.

Byte-nivå erstatning, slik at formatering, namespaces og kommentarer i
metadata.xml bevares (samme framgangsmåte som lob_column_types.set_column_type).
"""
from __future__ import annotations

import re
from pathlib import Path

from .lob_column_types import _SCHEMA_BLOCK_RE, _TABLE_BLOCK_RE, _folder_of

_ROWS_RE = re.compile(
    rb"(<(?:[A-Za-z0-9_-]+:)?rows(?:\s[^>]*)?>)\s*(\d+)\s*"
    rb"(</(?:[A-Za-z0-9_-]+:)?rows\s*>)"
)
_ROW_RE = re.compile(rb"<(?:[A-Za-z0-9_-]+:)?row(?=[\s>/])")


def count_rows(table_xml: bytes) -> int:
    """Antall <row>-elementer i innholdet av en tableX.xml."""
    return len(_ROW_RE.findall(table_xml))


def set_table_rows(meta: bytes, schema_folder: str, table_folder: str,
                   rows: int) -> "tuple[bytes, int | None]":
    """
    Sett <rows> for tabellen `schema_folder`/`table_folder`.

    Returnerer (nye_bytes, gammelt_antall). gammelt_antall er None når
    tabellen eller <rows>-elementet ikke ble funnet (da er bytes uendret).
    """
    sfolder_b = schema_folder.encode("utf-8")
    tfolder_b = table_folder.encode("utf-8")
    new_b = str(int(rows)).encode("ascii")
    old: "list[int | None]" = [None]

    def _in_schema(sm: re.Match) -> bytes:
        if _folder_of(sm.group(2)) != sfolder_b:
            return sm.group(0)

        def _in_table(tm: re.Match) -> bytes:
            body = tm.group(2)
            if _folder_of(body) != tfolder_b:
                return tm.group(0)
            hits = list(_ROWS_RE.finditer(body))
            if not hits:
                return tm.group(0)
            rm = hits[-1]            # <rows> er tabellens siste element
            old[0] = int(rm.group(2))
            body = (body[:rm.start()] + rm.group(1) + new_b + rm.group(3)
                    + body[rm.end():])
            return tm.group(1) + body + tm.group(3)

        return (sm.group(1)
                + _TABLE_BLOCK_RE.sub(_in_table, sm.group(2))
                + sm.group(3))

    out = _SCHEMA_BLOCK_RE.sub(_in_schema, meta)
    return out, old[0]


def sync_table_rows(metadata_path: Path, schema_folder: str, table_folder: str,
                    rows: int) -> "int | None":
    """
    Skriv `rows` som <rows> for tabellen i metadata.xml på disk.

    Returnerer gammelt antall (None = ikke funnet). Fila skrives bare når
    antallet faktisk endres.
    """
    meta = Path(metadata_path).read_bytes()
    new_meta, old = set_table_rows(meta, schema_folder, table_folder, rows)
    if old is not None and old != int(rows):
        Path(metadata_path).write_bytes(new_meta)
    return old
