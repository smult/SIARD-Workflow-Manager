"""Generic, JSON-driven SIARD decryption of LOBs in the extracted workspace.

Normal runs replace encrypted LOBs in the unpacked *working copy* and update
SIARD table XML references for subsequent RepackSiardOperation. Dry runs
never modify the workspace. No product-specific table/formula is hard-coded.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import html
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable


class RuleError(ValueError):
    """Invalid rule, ambiguous SIARD metadata, or unsafe path."""


@dataclass(frozen=True)
class TableInfo:
    name: str
    schema_folder: str
    table_folder: str
    columns: dict[str, int]
    lob_folders: dict[int, str]

    @property
    def xml_path_rel(self) -> Path:
        return Path("content") / self.schema_folder / self.table_folder / (self.table_folder + ".xml")

    def column_index(self, name: str) -> int:
        matches = [number for col, number in self.columns.items() if col.casefold() == name.casefold()]
        if len(matches) != 1:
            raise RuleError(f"Kolonnen '{name}' finnes ikke entydig i tabellen '{self.name}'.")
        return matches[0]


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _children(parent: ET.Element, name: str):
    return (child for child in parent if _local(child.tag) == name)


def _text(parent: ET.Element, name: str) -> str:
    return next(((x.text or "").strip() for x in _children(parent, name)), "")


def read_metadata_tables(root: Path) -> dict[str, TableInfo]:
    metadata = root / "header" / "metadata.xml"
    if not metadata.is_file():
        raise RuleError(f"Mangler metadata.xml: {metadata}")
    try:
        doc = ET.parse(metadata).getroot()
    except ET.ParseError as exc:
        raise RuleError(f"Ugyldig SIARD metadata.xml: {exc}") from exc
    found: dict[str, TableInfo] = {}
    for schemas_el in doc.iter():
        if _local(schemas_el.tag) != "schemas":
            continue
        for schema in _children(schemas_el, "schema"):
            sf = _text(schema, "folder")
            for tables_el in _children(schema, "tables"):
                for table in _children(tables_el, "table"):
                    name = _text(table, "name")
                    tf = _text(table, "folder")
                    cols: dict[str, int] = {}
                    lobs: dict[int, str] = {}
                    for columns_el in _children(table, "columns"):
                        for index, col in enumerate(_children(columns_el, "column"), start=1):
                            cols[_text(col, "name")] = index
                            lob = _text(col, "lobFolder")
                            if lob:
                                lobs[index] = lob
                    if not sf or not tf or not name or not cols:
                        raise RuleError("metadata.xml mangler navn, mappe eller kolonner i en tabell")
                    key = name.casefold()
                    if key in found:
                        raise RuleError(f"Tabellnavn '{name}' forekommer i flere schemaer; entydig oppslag kreves")
                    found[key] = TableInfo(name, sf, tf, cols, lobs)
    if not found:
        raise RuleError("Ingen tabeller i metadata.xml")
    return found


def iter_table_rows(xml_path: Path):
    """Yield (values, file references) per row using streaming XML parsing."""
    if not xml_path.is_file():
        raise RuleError(f"Mangler tabelldata: {xml_path}")
    try:
        parser = ET.iterparse(xml_path, events=("start", "end"))
        try:
            _, tree_root = next(parser)
        except StopIteration as exc:
            raise RuleError(f"Tom tabell-XML: {xml_path}") from exc
        for event, elem in parser:
            if event != "end" or _local(elem.tag) != "row":
                continue
            values: dict[int, str] = {}
            refs: dict[int, str] = {}
            for col in elem:
                name = _local(col.tag)
                if re.fullmatch(r"c\d+", name, flags=re.I):
                    index = int(name[1:])
                    values[index] = (col.text or "").strip()
                    if "file" in col.attrib:
                        refs[index] = col.attrib["file"].strip()
            yield values, refs
            tree_root.clear()
    except ET.ParseError as exc:
        raise RuleError(f"Ugyldig tabell-XML {xml_path.name}: {exc}") from exc


def read_rule(path: Path) -> dict:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuleError(f"Kan ikke lese regel-JSON '{path}': {exc}") from exc
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise RuleError("Regelsett krever 'version': 1")
    for section in ("source", "lookup", "formula"):
        if not isinstance(raw.get(section), dict):
            raise RuleError(f"Regelsett mangler objektet '{section}'")
    source, lookup, formula = (raw[k] for k in ("source", "lookup", "formula"))
    for field in ("table", "lob_column", "filename_column"):
        if not isinstance(source.get(field), str) or not source[field]:
            raise RuleError(f"Regelsett krever source.{field}")
    for field in ("table", "value_column"):
        if not isinstance(lookup.get(field), str) or not lookup[field]:
            raise RuleError(f"Regelsett krever lookup.{field}")
    if source.get("size_column") is not None and not isinstance(source["size_column"], str):
        raise RuleError("source.size_column maa vaere et kolonnenavn")
    joins = lookup.get("join")
    if not isinstance(joins, list) or not joins or any(
        not isinstance(item, dict) or not isinstance(item.get("source"), str)
        or not isinstance(item.get("target"), str) for item in joins
    ):
        raise RuleError("lookup.join krever minst ett par med source/target")
    ops = formula.get("operators", [])
    if not isinstance(ops, list):
        raise RuleError("formula.operators maa vaere en liste")
    if formula.get("use", "fallback_if_stored_empty") not in (
        "fallback_if_stored_empty", "fallback_if_stored_rejected"
    ):
        raise RuleError(
            "formula.use krever 'fallback_if_stored_empty' eller "
            "'fallback_if_stored_rejected'"
        )
    for op in ops:
        if isinstance(op, str):
            valid = op in ("basename", "stem", "reverse", "upper", "lower")
        else:
            valid = isinstance(op, dict) and len(op) == 1
            if valid:
                name, arg = next(iter(op.items()))
                if name in ("prepend", "append", "strip_suffix"):
                    valid = isinstance(arg, str)
                elif name == "replace":
                    valid = isinstance(arg, dict) and isinstance(arg.get("old"), str) and isinstance(arg.get("new"), str)
                else:
                    valid = False
        if not valid:
            raise RuleError(f"Ukjent eller utrygg operator: {op!r}")
    if formula.get("when_regex"):
        try:
            re.compile(formula["when_regex"])
        except (re.error, TypeError) as exc:
            raise RuleError(f"Ugyldig regex i formel: {exc}") from exc
    equals = source.get("where_equals", {})
    if not isinstance(equals, dict):
        raise RuleError("source.where_equals maa vaere et objekt")
    extensions = source.get("extensions", [".doc", ".docx"])
    if not isinstance(extensions, list) or any(not isinstance(ext, str) for ext in extensions):
        raise RuleError("source.extensions maa vaere en liste av filendelser")
    if not isinstance(formula.get("input_column"), str) or not formula["input_column"]:
        raise RuleError("formula.input_column maa angi en kildekolonne")
    return raw


def formula_password(filename: str, formula: dict) -> str:
    """Allow-listed string transformations; never evaluate arbitrary code."""
    regexp = formula.get("when_regex", "")
    if regexp:
        if not re.fullmatch(regexp, filename, flags=re.I):
            return ""
    value = filename
    for op in formula.get("operators", []):
        if op == "basename":
            value = value.replace("\\", "/").rsplit("/", 1)[-1]
        elif op == "stem":
            value = Path(value).stem
        elif op == "reverse":
            value = value[::-1]
        elif op == "lower":
            value = value.lower()
        elif op == "upper":
            value = value.upper()
        elif isinstance(op, dict):
            name, arg = next(iter(op.items()))
            if name == "prepend" and isinstance(arg, str):
                value = arg + value
            elif name == "append" and isinstance(arg, str):
                value += arg
            elif name == "strip_suffix" and isinstance(arg, str):
                if arg and value.lower().endswith(arg.lower()):
                    value = value[:-len(arg)]
            elif name == "replace" and isinstance(arg, dict) and isinstance(arg.get("old"), str) and isinstance(arg.get("new"), str):
                value = value.replace(arg["old"], arg["new"])
            else:
                raise RuleError(f"Ugyldig formeloperator {op!r}")
    return value


def _safe_child(root: Path, relative: str) -> Path:
    rel = PurePosixPath(relative.replace("\\", "/"))
    if (not relative or rel.is_absolute() or ".." in rel.parts
            or any(":" in piece for piece in rel.parts)):
        raise RuleError(f"Ugyldig referanse utenfor utpakket SIARD: {relative!r}")
    base = root.resolve()
    destination = (base / Path(*rel.parts)).resolve()
    if not destination.is_relative_to(base):
        raise RuleError(f"LOB-referanse peker utenfor utpakket SIARD: {relative!r}")
    return destination


def resolve_lob(root: Path, info: TableInfo, index: int, ref: str) -> Path:
    reference = ref.replace("\\", "/")
    parts = PurePosixPath(reference).parts
    if parts and parts[0].lower() == "content":
        return _safe_child(root, reference)
    folder = info.lob_folders.get(index, f"{info.schema_folder}/{info.table_folder}/lob{index}")
    folder_parts = PurePosixPath(folder.replace("\\", "/")).parts
    if folder_parts and folder_parts[0].lower() == "content":
        base = folder
    elif folder_parts and folder_parts[0].lower() == info.schema_folder.lower():
        base = "content/" + folder
    else:
        base = f"content/{info.schema_folder}/{info.table_folder}/{folder}"
    # Some producers store a relative path that already includes lob folder.
    if len(parts) > 1 and parts[0].lower() == info.schema_folder.lower():
        return _safe_child(root, "content/" + reference)
    if len(parts) > 1 and parts[0].lower() == "lob" + str(index):
        return _safe_child(root, f"content/{info.schema_folder}/{info.table_folder}/" + reference)
    return _safe_child(root, base.rstrip("/") + "/" + reference)


def _format_class(path: Path) -> str:
    with path.open("rb") as fh:
        head = fh.read(8)
    if head.startswith(b"%PDF-"):
        return "pdf"
    if head.startswith(bytes.fromhex("d0cf11e0a1b11ae1")):
        return "office"
    if head.startswith(b"PK\x03\x04"):
        return "zip"
    return "other"


def _decrypt_to_file(source: Path, password: str, target) -> str:
    """Return decrypted/not_encrypted; let caller handle crypto failures."""
    import msoffcrypto  # imported lazily so lookup can be unit-tested without pip
    with source.open("rb") as input_file:
        office = msoffcrypto.OfficeFile(input_file)
        if not office.is_encrypted():
            return "not_encrypted"
        office.load_key(password=password)
        office.decrypt(target)
    target.flush()
    target.seek(0)
    if target.read(8)[:4] not in (b"PK\x03\x04", bytes.fromhex("d0cf11e0")):
        raise ValueError("Dekryptert innhold har ikke gyldig Office-signatur")
    target.seek(0)
    return "decrypted"


def _new_stats() -> dict[str, int]:
    # The common WorkflowReportOperation displays the first 12 result values.
    # Put the same operational facts first as CosDoc: outcomes, failures,
    # and document counts. Lookup/formula diagnostics remain in the log.
    return {key: 0 for key in (
        "selected", "decrypted_saved", "decrypted_tested", "decrypt_failed",
        "not_encrypted", "blob_missing", "lookup_missing", "lookup_ambiguous",
        "password_missing", "formula_fallback_success", "formula_mismatch",
        "source_rows", "password_stored", "password_formula_only", "formula_match",
        "pdf_skipped", "format_skipped", "formula_fallback_attempted",
        "already_exists", "stopped",
    )}

def _patch_lob_metadata(
    xml_data: bytes,
    *,
    lob_index: int,
    changes: dict[str, dict],
    size_index: int | None = None,
) -> bytes:
    """Patch only changed row/cell bytes, retaining namespaces and formatting.

    The LOB *file* reference is unchanged; its length/digest refer to the
    decrypted bytes in the unpacked SIARD working directory. This is the same
    metadata convention used by the existing CosDoc operation (MD5).
    """
    row_pattern = re.compile(
        rb"<(?P<prefix>[A-Za-z_][\w.-]*:)?row(?=[\s>])[^>]*>.*?</(?:[A-Za-z_][\w.-]*:)?row\s*>",
        re.DOTALL,
    )
    cell_pattern = re.compile(
        rb"<(?P<prefix>[A-Za-z_][\w.-]*:)?c" + str(lob_index).encode('ascii')
        + rb"(?=[\s/>])(?P<attrs>[^<>]*?)(?P<close>/?)>",
        re.DOTALL,
    )
    file_pattern = re.compile(rb"(?<![\w:-])file\s*=\s*([\"'])(.*?)\1", re.DOTALL)
    updated_refs: set[str] = set()

    def update_cell(cell_match: re.Match[bytes]) -> bytes:
        attrs = cell_match.group('attrs')
        file_match = file_pattern.search(attrs)
        if not file_match:
            return cell_match.group(0)
        ref = html.unescape(file_match.group(2).decode('utf-8'))
        change = changes.get(ref)
        if change is None:
            return cell_match.group(0)
        updated_refs.add(ref)
        length = str(change['length']).encode('ascii')
        digest = change['digest'].encode('ascii')
        # SIARD length/digest/digestType refer to the new actual bytes.
        for key, value in ((b'length', length), (b'digestType', b'MD5'), (b'digest', digest)):
            pattern = re.compile(rb"(?<![\w:-])" + key + rb"\s*=\s*([\"']).*?\1", re.DOTALL)
            if pattern.search(attrs):
                attrs = pattern.sub(lambda m, k=key, v=value: k + b'=' + m.group(1) + v + m.group(1), attrs, count=1)
            else:
                attrs += b' ' + key + b'="' + value + b'"'
        return b'<' + (cell_match.group('prefix') or b'') + b'c' + str(lob_index).encode('ascii') + attrs + cell_match.group('close') + b'>'

    def update_row(match: re.Match[bytes]) -> bytes:
        row = match.group(0)
        refs_before = len(updated_refs)
        cell_ref_match = cell_pattern.search(row)
        if cell_ref_match is None:
            return row
        file_match = file_pattern.search(cell_ref_match.group('attrs'))
        if not file_match:
            return row
        ref = html.unescape(file_match.group(2).decode('utf-8'))
        if ref not in changes:
            return row
        row = cell_pattern.sub(update_cell, row, count=1)
        if size_index is not None:
            # Optional DB size field specified by name in the JSON rules.
            # Do not touch other rows or columns.
            tag = str(size_index).encode('ascii')
            size_pattern = re.compile(
                rb'(<(?:[A-Za-z_][\w.-]*:)?c' + tag + rb'(?=[\s>])[^>]*>)'
                rb'[^<]*(</(?:[A-Za-z_][\w.-]*:)?c' + tag + rb'\s*>)',
                re.DOTALL,
            )
            if size_pattern.search(row):
                row = size_pattern.sub(
                    lambda m: m.group(1) + str(changes[ref]['length']).encode('ascii') + m.group(2),
                    row, count=1,
                )
        return row

    new_xml = row_pattern.sub(update_row, xml_data)
    missing = set(changes) - updated_refs
    if missing:
        raise RuleError('Kunne ikke oppdatere LOB-referanser i table XML: ' + ', '.join(sorted(missing)[:5]))
    try:
        ET.fromstring(new_xml)
    except ET.ParseError as exc:
        raise RuleError('Oppdatert tabell-XML ble ugyldig: ' + str(exc)) from exc
    return new_xml


def _commit_decryption(
    extracted_dir: Path,
    table_xml: Path,
    changes: dict[str, dict],
    *,
    lob_index: int,
    size_index: int | None,
    stage_dir: Path,
) -> None:
    """Install LOB + metadata updates with rollback if a filesystem step fails.

    Staged files and backups are on the same volume as the extracted SIARD;
    os.replace is atomic per file on supported local filesystems.
    """
    xml_bytes = table_xml.read_bytes()
    xml_changed = _patch_lob_metadata(
        xml_bytes, lob_index=lob_index, changes=changes, size_index=size_index)
    staged_xml = stage_dir / 'table-updated.xml'
    staged_xml.write_bytes(xml_changed)
    backups: list[tuple[Path, Path]] = []
    xml_backup = stage_dir / 'table-original.xml'
    try:
        for index, change in enumerate(changes.values()):
            src = change['path']
            decrypted = change['staged']
            original = stage_dir / f'original-blob-{index:08d}.bak'
            os.replace(src, original)
            backups.append((src, original))
            os.replace(decrypted, src)
        os.replace(table_xml, xml_backup)
        try:
            os.replace(staged_xml, table_xml)
        except Exception:
            os.replace(xml_backup, table_xml)
            raise
    except Exception:
        # Restore source blobs and XML even when one replacement failed.
        for dest, backup in reversed(backups):
            if backup.exists():
                os.replace(backup, dest)
        if xml_backup.exists():
            os.replace(xml_backup, table_xml)
        raise


def execute_rule(
    extracted_dir: Path,
    rule: dict,
    *,
    only_ref: str = '',
    max_files: int = 0,
    dry_run: bool = True,
    output_dir: Path | None = None,
    log: Callable[[str, str], None] | None = None,
    stop: Callable[[], bool] | None = None,
    record_sink: Callable[[list[dict]], None] | None = None,
) -> dict[str, int]:
    """Decrypt SIARD LOBs by JSON rule. Commit only after all candidates are staged.

    A following RepackSiardOperation creates the new SIARD. When requested,
    record_sink receives a per-source-row audit trail *after* the commit has
    finished, so the log never reports a staged file as a committed result.
    output_dir is retained only for backward compatibility with old profiles.
    """
    root = Path(extracted_dir).expanduser().resolve()
    if not root.is_dir():
        raise RuleError(f'Utpakket SIARD-mappe finnes ikke: {root}')
    if max_files < 0:
        raise RuleError('Maks filer maa vaere 0 eller positiv')
    try:
        import msoffcrypto
    except ImportError as exc:
        raise RuleError('Mangler msoffcrypto-tool: python -m pip install msoffcrypto-tool') from exc

    info = read_metadata_tables(root)
    source_rule, lookup_rule, formula_rule = (rule[k] for k in ('source', 'lookup', 'formula'))
    source = info.get(source_rule['table'].casefold())
    lookup = info.get(lookup_rule['table'].casefold())
    if source is None or lookup is None:
        raise RuleError('Kildetabell eller oppslagstabell finnes ikke i metadata.xml')
    filename_i = source.column_index(source_rule['filename_column'])
    lob_i = source.column_index(source_rule['lob_column'])
    pw_i = lookup.column_index(lookup_rule['value_column'])
    formula_i = source.column_index(formula_rule.get('input_column') or source_rule['filename_column'])
    size_i = source.column_index(source_rule['size_column']) if source_rule.get('size_column') else None
    joins = [(source.column_index(p['source']), lookup.column_index(p['target']))
             for p in lookup_rule['join']]
    filters = [(source.column_index(k), str(v)) for k, v in source_rule.get('where_equals', {}).items()]
    allowed_exts = {str(ext).casefold() for ext in source_rule.get('extensions', ['.doc', '.docx'])}
    compare_formula = formula_rule.get('check_against_stored', True)
    fallback_policy = formula_rule.get('use', 'fallback_if_stored_empty')
    if fallback_policy not in ('fallback_if_stored_empty', 'fallback_if_stored_rejected'):
        raise RuleError(f'Ukjent formula.use: {fallback_policy!r}')
    target_ref = only_ref.strip().replace('\\', '/')
    writer = log or (lambda _msg, _level='info': None)

    lookup_index: dict[tuple[str, ...], list[str]] = {}
    for cells, _ in iter_table_rows(root / lookup.xml_path_rel):
        key = tuple(cells.get(k, '') for _, k in joins)
        if all(key):
            lookup_index.setdefault(key, []).append(cells.get(pw_i, ''))
    if not lookup_index:
        raise RuleError(f'Ingen oppslagsrader i {lookup.name}; kontroller regelsett')

    stats = _new_stats()
    records: list[dict] = []
    # A separate staging area is used for both the decrypted bytes and the
    # original blobs during commit; neither goes inside the SIARD ZIP.
    with tempfile.TemporaryDirectory(prefix='siard_decrypt_stage_', dir=root.parent) as stage_name:
        stage_dir = Path(stage_name)
        changes: dict[str, dict] = {}
        for values, refs in iter_table_rows(root / source.xml_path_rel):
            stats['source_rows'] += 1
            fname = values.get(filename_i, '')
            ref = refs.get(lob_i, '')
            if not ref:
                continue
            if target_ref and ref != target_ref and PurePosixPath(ref.replace('\\', '/')).name != PurePosixPath(target_ref).name:
                continue
            if filters and any(values.get(i, '') != val for i, val in filters):
                continue
            if allowed_exts and Path(fname).suffix.casefold() not in allowed_exts:
                continue
            if max_files and stats['selected'] >= max_files:
                break
            if stop and stop():
                stats['stopped'] += 1
                break
            stats['selected'] += 1
            rec = {
                'fil': ref,
                'opprinnelig_filnavn': fname,
                'plassering': f'{source.name}.{source_rule["lob_column"]}',
                'format': Path(fname).suffix.lstrip('.').lower(),
                'fra_storrelse': '',
                'til_storrelse': '',
                'status': '',
                'kategori': '',
                'arsak': '',
                'passordkilde': '',
                'formelavvik': '',
            }
            records.append(rec)

            def done(status: str, category: str = '', reason: str = '') -> None:
                rec['status'] = status
                rec['kategori'] = category
                rec['arsak'] = reason

            join_key = tuple(values.get(i, '') for i, _ in joins)
            candidates = lookup_index.get(join_key, [])
            if not candidates:
                stats['lookup_missing'] += 1
                done('feil', 'Passordoppslag mangler', 'Ingen samsvarende rad i oppslagstabell')
                continue
            if len(candidates) != 1:
                stats['lookup_ambiguous'] += 1
                done('feil', 'Tvetydig passordoppslag', f'{len(candidates)} samsvarende oppslagsrader')
                continue
            stored_pw = candidates[0].strip()
            calculated = formula_password(values.get(formula_i, ''), formula_rule)
            if stored_pw:
                stats['password_stored'] += 1
                if compare_formula and calculated:
                    is_match = stored_pw == calculated
                    stats['formula_match' if is_match else 'formula_mismatch'] += 1
                    rec['formelavvik'] = 'nei' if is_match else 'ja'
            elif calculated:
                stats['password_formula_only'] += 1
            else:
                stats['password_missing'] += 1
                done('feil', 'Passord mangler', 'Ingen lagret eller beregnet passordverdi')
                continue
            pw_options = []
            if stored_pw:
                pw_options.append(('lagret', stored_pw))
            if calculated and (not stored_pw or (fallback_policy == 'fallback_if_stored_rejected'
                                                and calculated != stored_pw)):
                pw_options.append(('beregnet', calculated))
            if not pw_options:
                stats['password_missing'] += 1
                done('feil', 'Passord mangler', 'Ingen brukbar passordkandidat')
                continue
            try:
                lob_file = resolve_lob(root, source, lob_i, ref)
            except RuleError as exc:
                stats['blob_missing'] += 1
                done('feil', 'Ugyldig LOB-referanse', str(exc))
                continue
            rec['fil'] = lob_file.relative_to(root).as_posix()
            if not lob_file.is_file():
                stats['blob_missing'] += 1
                done('feil', 'LOB-fil mangler', 'LOB-filen finnes ikke')
                continue
            try:
                rec['fra_storrelse'] = lob_file.stat().st_size
                rec['til_storrelse'] = rec['fra_storrelse']
                file_type = _format_class(lob_file)
            except OSError as exc:
                stats['decrypt_failed'] += 1
                done('feil', 'LOB kan ikke leses', f'{type(exc).__name__}: {exc}')
                continue
            if file_type == 'pdf':
                stats['pdf_skipped'] += 1
                done('beholdt', 'PDF', 'Allerede PDF – ingen dekryptering')
                continue
            if file_type not in ('office', 'zip'):
                stats['format_skipped'] += 1
                done('beholdt', 'Ukjent format', 'Ingen støttet Office-signatur')
                continue
            if ref in changes:
                stats['already_exists'] += 1
                done('beholdt', 'Duplikat referanse', 'LOB er allerede behandlet via en annen rad')
                continue
            try:
                with tempfile.TemporaryFile(mode='w+b') as scratch:
                    fallback_attempted = False
                    used_method = ''
                    result = ''
                    for method, candidate in pw_options:
                        scratch.seek(0)
                        scratch.truncate(0)
                        try:
                            result = _decrypt_to_file(lob_file, candidate, scratch)
                            used_method = method
                            break
                        except msoffcrypto.exceptions.InvalidKeyError:
                            if method == 'lagret' and len(pw_options) > 1:
                                fallback_attempted = True
                                stats['formula_fallback_attempted'] += 1
                                writer(f'  {ref}: lagret passord avvist; prøver beregnet passord', 'warn')
                                continue
                            raise
                    if result == 'not_encrypted':
                        stats['not_encrypted'] += 1
                        done('beholdt', 'Ikke kryptert', 'Dokumentet er allerede tilgjengelig uten passord')
                        continue
                    rec['passordkilde'] = 'beregnet (fallback)' if fallback_attempted else used_method
                    if fallback_attempted:
                        stats['formula_fallback_success'] += 1
                        writer(f'  {ref}: beregnet passord fungerte etter avvist lagret passord', 'info')
                    if dry_run:
                        stats['decrypted_tested'] += 1
                        done('testet', '', 'Dekryptering verifisert uten skriving')
                        writer(f'  Testet dekryptering: {ref}', 'info')
                    else:
                        staged = stage_dir / f'blob-{len(changes):08d}.dec'
                        md5 = hashlib.md5()
                        size = 0
                        scratch.seek(0)
                        with staged.open('wb') as out:
                            while True:
                                chunk = scratch.read(1024 * 1024)
                                if not chunk:
                                    break
                                out.write(chunk)
                                md5.update(chunk)
                                size += len(chunk)
                        rec['til_storrelse'] = size
                        done('klargjort', '', 'Venter på oppdatering av LOB og tabell-XML')
                        changes[ref] = {
                            'path': lob_file, 'staged': staged,
                            'length': size, 'digest': md5.hexdigest().upper(),
                        }
                        writer(f'  Dekryptert: {ref} ({size} byte)', 'ok')
            except msoffcrypto.exceptions.InvalidKeyError as exc:
                stats['decrypt_failed'] += 1
                done('feil', 'Passord avvist', f'{type(exc).__name__}: {exc}')
                writer(f'  {ref}: passord avvist ({type(exc).__name__}: {exc})', 'warn')
            except Exception as exc:
                stats['decrypt_failed'] += 1
                done('feil', 'Dekryptering feilet', f'{type(exc).__name__}: {exc}')
                writer(f'  {ref}: dekryptering feilet ({type(exc).__name__}: {exc})', 'warn')
            if stats['selected'] <= 15 or stats['selected'] % 250 == 0:
                writer(f'  Behandlet {stats["selected"]} utvalgte filer '
                       f'(test={stats["decrypted_tested"]}, klargjort={len(changes)})', 'info')

        if target_ref and not stats['selected']:
            raise RuleError(f'Ingen treff paa LOB-referansen {target_ref!r} og aktive filtre')
        if not dry_run and changes:
            if stats['stopped']:
                writer('  Avbrutt: ingen klargjorte dokumenter ble skrevet', 'warn')
                for rec in records:
                    if rec['status'] == 'klargjort':
                        done_rec = rec
                        done_rec['status'] = 'avbrutt'
                        done_rec['kategori'] = 'Avbrutt av bruker'
                        done_rec['arsak'] = 'Ingen endring skrevet til SIARD'
            else:
                _commit_decryption(
                    root, root / source.xml_path_rel, changes,
                    lob_index=lob_i, size_index=size_i, stage_dir=stage_dir,
                )
                stats['decrypted_saved'] = len(changes)
                for rec in records:
                    if rec['status'] == 'klargjort':
                        rec['status'] = 'dekryptert'
                        rec['arsak'] = 'Passord fjernet; LOB og tabell-XML oppdatert'
                writer(f'  Oppdaterte {len(changes)} LOB-filer og tabell-XML i arbeidskopien', 'ok')
    if record_sink is not None:
        record_sink(records)
    return stats
