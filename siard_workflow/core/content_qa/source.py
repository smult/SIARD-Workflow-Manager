"""Read-only SIARD inventory access, supporting ZIP and extracted workspaces.

Parses the metadata catalog without assuming physical names for any application.
Table XML rows are iterated lazily so LOB contents are never loaded into memory.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import io
from pathlib import Path, PurePosixPath
import re
from typing import Iterator
import xml.etree.ElementTree as ET
import zipfile


def _local(tag: str) -> str:
    return tag.rsplit('}', 1)[-1].rsplit(':', 1)[-1]


def _child(parent, tag: str):
    return next((c for c in parent if _local(c.tag) == tag), None) if parent is not None else None


def _children(parent, tag: str) -> list:
    return [c for c in parent if _local(c.tag) == tag] if parent is not None else []


def _text(parent, tag: str) -> str:
    c = _child(parent, tag)
    return (c.text or '').strip() if c is not None else ''


def _folder(s: str) -> str:
    # Strictly prohibit escaping the SIARD source root or opening unrelated paths.
    normalized = str(s or '').replace('\\', '/').strip('/')
    parts = PurePosixPath(normalized).parts
    if not parts or any(part in ('..', '.') for part in parts) or ':' in normalized:
        raise ValueError(f'Ugyldig mappe i SIARD-metadata: {s!r}')
    return '/'.join(parts)


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str = ''
    nullable: str = ''
    description: str = ''


@dataclass(frozen=True)
class ForeignKey:
    name: str
    referenced_schema: str
    referenced_table: str
    references: tuple[tuple[str, str], ...]
    match_type: str = 'SIMPLE'
    description: str = ''


@dataclass(frozen=True)
class CandidateKey:
    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class Table:
    schema: str
    name: str
    schema_folder: str
    table_folder: str
    columns: tuple[Column, ...]
    declared_rows: int | None
    primary_key: tuple[str, ...] = ()
    description: str = ''
    foreign_keys: tuple[ForeignKey, ...] = ()
    candidate_keys: tuple[CandidateKey, ...] = ()

    @property
    def ident(self) -> str:
        return f'{self.schema}.{self.name}'

    @property
    def key(self) -> tuple[str, str]:
        return self.schema, self.name

    @property
    def xml_path(self) -> str:
        folder = _folder(self.table_folder)
        schema = _folder(self.schema_folder)
        # Metadata 'folder' can be nested, but SIARD table XML uses the
        # final folder component as its file stem.
        base = PurePosixPath(folder).name
        return f'content/{schema}/{folder}/{base}.xml'


def read_catalog(metadata_xml: bytes) -> list[Table]:
    if b'<!DOCTYPE' in metadata_xml.upper() or b'<!ENTITY' in metadata_xml.upper():
        raise ValueError('DTD/ENTITY er ikke tillatt i SIARD-metadata')
    root = ET.fromstring(metadata_xml)
    schemas = _child(root, 'schemas')
    if schemas is None:
        raise ValueError('SIARD-metadata mangler schemas')
    out: list[Table] = []
    seen: set[tuple[str, str]] = set()
    for schema in _children(schemas, 'schema'):
        schema_name = _text(schema, 'name')
        schema_folder = _text(schema, 'folder')
        if not schema_name or not schema_folder:
            raise ValueError('Schema mangler navn eller folder')
        for table in _children(_child(schema, 'tables'), 'table'):
            table_name = _text(table, 'name')
            table_folder = _text(table, 'folder')
            if not table_name or not table_folder:
                raise ValueError(f'Tabell i {schema_name} mangler navn eller folder')
            ident = (schema_name, table_name)
            if ident in seen:
                raise ValueError(f'Dobbel tabell i metadata: {schema_name}.{table_name}')
            seen.add(ident)
            rows = _text(table, 'rows')
            if rows:
                try:
                    declared = int(rows)
                    if declared < 0:
                        raise ValueError()
                except ValueError:
                    raise ValueError(f'Ugyldig rows-verdi for {schema_name}.{table_name}: {rows!r}') from None
            else:
                declared = None
            columns = tuple(Column(
                name=_text(c, 'name'),
                sql_type=_text(c, 'type') or _text(c, 'typeOriginal'),
                nullable=_text(c, 'nullable'),
                description=_text(c, 'description'),
            ) for c in _children(_child(table, 'columns'), 'column'))
            if not columns or not all(c.name for c in columns):
                raise ValueError(f'Kolonner mangler for {schema_name}.{table_name}')
            if len(set(c.name for c in columns)) != len(columns):
                raise ValueError(f'Doble kolonnenavn for {schema_name}.{table_name}')
            pk = _child(table, 'primaryKey')
            primary_key = tuple((c.text or '').strip() for c in _children(pk, 'column'))
            fkeys = []
            for fk in _children(_child(table, 'foreignKeys'), 'foreignKey'):
                refs = tuple((_text(r, 'column'), _text(r, 'referenced'))
                             for r in _children(fk, 'reference'))
                fkeys.append(ForeignKey(
                    name=_text(fk, 'name'),
                    referenced_schema=_text(fk, 'referencedSchema') or schema_name,
                    referenced_table=_text(fk, 'referencedTable'),
                    references=refs,
                    match_type=_text(fk, 'matchType') or 'SIMPLE',
                    description=_text(fk, 'description'),
                ))
            candidate_keys = tuple(CandidateKey(
                name=_text(ck, 'name'),
                columns=tuple((col.text or '').strip()
                              for col in _children(ck, 'column')),
            ) for ck in _children(_child(table, 'candidateKeys'), 'candidateKey'))
            out.append(Table(schema_name, table_name, schema_folder, table_folder,
                             columns, declared, primary_key, _text(table, 'description'),
                             tuple(fkeys), candidate_keys))
    if not out:
        raise ValueError('SIARD inneholder ingen tabeller i metadata')
    return out


class SiardSource:
    """Read a SIARD ZIP or extracted root, without changing either."""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._zip: zipfile.ZipFile | None = None
        self._names: dict[str, str] = {}

    def __enter__(self):
        if self.path.is_dir():
            self._root = self.path.resolve()
        elif self.path.is_file() and zipfile.is_zipfile(self.path):
            self._zip = zipfile.ZipFile(self.path, 'r')
            # A copied path case variant is not an additional SIARD object.
            self._names = {x.filename.replace('\\', '/'): x.filename
                           for x in self._zip.infolist() if not x.is_dir()}
        else:
            raise ValueError(f'Ikke en SIARD ZIP eller utpakket mappe: {self.path}')
        return self

    def __exit__(self, *_):
        if self._zip:
            self._zip.close()

    def exists(self, relative_path: str) -> bool:
        rel = _folder(relative_path)
        if self._zip:
            return rel in self._names
        p = (self._root / rel).resolve()
        return p.is_relative_to(self._root) and p.is_file()

    @contextmanager
    def open(self, relative_path: str):
        rel = _folder(relative_path)
        if self._zip:
            if rel not in self._names:
                raise FileNotFoundError(rel)
            with self._zip.open(self._names[rel]) as fh:
                yield fh
        else:
            file = (self._root / rel).resolve()
            if not file.is_relative_to(self._root):
                raise ValueError('Ulovlig filsti utenfor SIARD')
            with file.open('rb') as fh:
                yield fh

    def catalog(self) -> list[Table]:
        metadata = next((s for s in ('header/metadata.xml', 'metadata.xml')
                         if self.exists(s)), None)
        if metadata is None:
            raise FileNotFoundError('Finner ikke header/metadata.xml i SIARD')
        with self.open(metadata) as fh:
            return read_catalog(fh.read())

    def table_xml_path(self, table: Table) -> str | None:
        # SIARD requires <tableFolder>/<tableFolder>.xml. Do not silently
        # accept a different file just because it is the only XML in a folder.
        expected = table.xml_path
        return expected if self.exists(expected) else None

    def rows(self, table: Table) -> Iterator[tuple[int, dict[str, str | None]]]:
        rel = self.table_xml_path(table)
        if rel is None:
            raise FileNotFoundError(f'Tabell-XML mangler/er tvetydig: {table.ident} ({table.xml_path})')
        with self.open(rel) as fh:
            # Bound memory: clear the table root after each <row>.
            parser = ET.iterparse(fh, events=('start', 'end'))
            root = None
            row_n = 0
            depth = 0
            for event, elem in parser:
                if event == 'start':
                    depth += 1
                    if root is None:
                        root = elem
                    continue
                if depth != 2 or _local(elem.tag) != 'row':
                    depth -= 1
                    continue
                row_n += 1
                values: dict[str, str | None] = {}
                for col_node in elem:
                    hit = re.fullmatch(r'c([1-9]\d*)', _local(col_node.tag))
                    if hit:
                        i = int(hit.group(1)) - 1
                        if i < len(table.columns):
                            # Never load external LOB contents; attributes are not
                            # scalar values and must not become bogus keys.
                            values[table.columns[i].name] = (
                                col_node.text if col_node.text is not None else None)
                yield row_n, values
                if root is not None:
                    root.clear()
                depth -= 1
