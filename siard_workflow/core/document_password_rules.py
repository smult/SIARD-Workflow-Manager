"""Generic, JSON-driven SIARD document-password lookup and safe decryption.

No product-specific table, column or formula is hard-coded in this module.
Original SIARD/LOB/XML files are never modified. In test mode, decrypted
bytes are discarded when their temporary file is closed.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
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
    return {key: 0 for key in (
        "source_rows", "selected", "password_stored", "password_formula_only",
        "formula_match", "formula_mismatch", "password_missing", "lookup_missing",
        "lookup_ambiguous", "blob_missing", "pdf_skipped", "format_skipped",
        "not_encrypted", "decrypted_tested", "decrypted_saved", "decrypt_failed",
        "formula_fallback_attempted", "formula_fallback_success",
        "already_exists", "stopped",
    )}


def execute_rule(
    extracted_dir: Path,
    rule: dict,
    *,
    only_ref: str = "",
    max_files: int = 0,
    dry_run: bool = True,
    output_dir: Path | None = None,
    log: Callable[[str, str], None] | None = None,
    stop: Callable[[], bool] | None = None,
) -> dict[str, int]:
    root = Path(extracted_dir).expanduser().resolve()
    if not root.is_dir():
        raise RuleError(f"Utpakket SIARD-mappe finnes ikke: {root}")
    if not dry_run:
        if output_dir is None:
            raise RuleError("Velg egen utdatamappe for dekrypterte kopier")
        dest_root = Path(output_dir).expanduser().resolve()
        if dest_root == root or dest_root.is_relative_to(root):
            raise RuleError("Utdatamappen maa ligge UTENFOR den utpakkede SIARD-mappen")
        if any((parent / ".git").exists() for parent in (dest_root, *dest_root.parents)):
            raise RuleError("Utdatamappen maa ikke ligge i et Git-repository")
    else:
        dest_root = None
    if max_files < 0:
        raise RuleError("Maks filer maa vaere 0 eller positiv")
    if not dry_run:
        try:
            import msoffcrypto  # noqa: F401
        except ImportError as exc:
            raise RuleError("Mangler msoffcrypto-tool: python -m pip install msoffcrypto-tool") from exc
    info = read_metadata_tables(root)
    source_rule, lookup_rule, formula_rule = (rule[key] for key in ("source", "lookup", "formula"))
    source = info.get(source_rule["table"].casefold())
    lookup = info.get(lookup_rule["table"].casefold())
    if source is None or lookup is None:
        raise RuleError("Kildetabell eller oppslagstabell finnes ikke i metadata.xml")
    filename_i = source.column_index(source_rule["filename_column"])
    lob_i = source.column_index(source_rule["lob_column"])
    pw_i = lookup.column_index(lookup_rule["value_column"])
    formula_i = source.column_index(formula_rule.get("input_column") or source_rule["filename_column"])
    joins = [(source.column_index(p["source"]), lookup.column_index(p["target"]))
             for p in lookup_rule["join"]]
    filters = [(source.column_index(k), str(v))
               for k, v in source_rule.get("where_equals", {}).items()]
    allowed_exts = {str(ext).casefold() for ext in source_rule.get("extensions", [".doc", ".docx"])}
    from_formula = formula_rule.get("check_against_stored", True)
    # The original policy remains supported for existing external JSON rules.
    fallback_policy = formula_rule.get("use", "fallback_if_stored_empty")
    if fallback_policy not in ("fallback_if_stored_empty", "fallback_if_stored_rejected"):
        raise RuleError(f"Ukjent formula.use: {fallback_policy!r}")
    target_ref = only_ref.strip().replace("\\", "/")
    lookup_index: dict[tuple[str, ...], list[str]] = {}
    for cells, _ in iter_table_rows(root / lookup.xml_path_rel):
        key = tuple(cells.get(k, "") for _, k in joins)
        if all(key):
            lookup_index.setdefault(key, []).append(cells.get(pw_i, ""))
    if not lookup_index:
        raise RuleError(f"Ingen oppslagsrader i {lookup.name}; kontroller regelsett")
    stats = _new_stats()
    writer = log or (lambda _message, _level="info": None)
    if dry_run:
        try:
            import msoffcrypto  # noqa: F401
        except ImportError as exc:
            raise RuleError("Mangler msoffcrypto-tool for testdekryptering") from exc
    elif dest_root is not None:
        dest_root.mkdir(parents=True, exist_ok=True)
    for values, refs in iter_table_rows(root / source.xml_path_rel):
        stats["source_rows"] += 1
        fname = values.get(filename_i, "")
        ref = refs.get(lob_i, "")
        if not ref:
            continue
        if target_ref and ref != target_ref and PurePosixPath(ref.replace("\\", "/")).name != PurePosixPath(target_ref).name:
            continue
        if filters and any(values.get(i, "") != value for i, value in filters):
            continue
        if allowed_exts and Path(fname).suffix.casefold() not in allowed_exts:
            continue
        if max_files and stats["selected"] >= max_files:
            break
        if stop and stop():
            stats["stopped"] += 1
            break
        stats["selected"] += 1
        join_key = tuple(values.get(source_i, "") for source_i, _ in joins)
        candidates = lookup_index.get(join_key, [])
        if not candidates:
            stats["lookup_missing"] += 1
            continue
        if len(candidates) != 1:
            stats["lookup_ambiguous"] += 1
            continue
        stored_pw = candidates[0].strip()
        calculated = formula_password(values.get(formula_i, ""), formula_rule)
        if stored_pw:
            stats["password_stored"] += 1
            if from_formula and calculated:
                stats["formula_match" if stored_pw == calculated else "formula_mismatch"] += 1
        elif calculated:
            stats["password_formula_only"] += 1
        else:
            stats["password_missing"] += 1
        # Always try the database value first. A rule may opt in to retrying
        # the calculated value only after an explicit InvalidKeyError.
        password_candidates: list[tuple[str, str]] = []
        if stored_pw:
            password_candidates.append(("stored", stored_pw))
        if calculated and (
            not stored_pw or (
                fallback_policy == "fallback_if_stored_rejected"
                and calculated != stored_pw
            )
        ):
            password_candidates.append(("formula", calculated))
        if not password_candidates:
            continue
        try:
            lob_file = resolve_lob(root, source, lob_i, ref)
        except RuleError:
            stats["blob_missing"] += 1
            continue
        if not lob_file.is_file():
            stats["blob_missing"] += 1
            continue
        file_type = _format_class(lob_file)
        if file_type == "pdf":
            stats["pdf_skipped"] += 1
            continue
        if file_type not in ("office", "zip"):
            stats["format_skipped"] += 1
            continue
        try:
            with tempfile.TemporaryFile(mode="w+b") as scratch:
                fallback_attempted = False
                for method, candidate in password_candidates:
                    # A failed attempt may have written partial data; never
                    # append a successful retry to the failed first attempt.
                    scratch.seek(0)
                    scratch.truncate(0)
                    try:
                        result = _decrypt_to_file(lob_file, candidate, scratch)
                        break
                    except msoffcrypto.exceptions.InvalidKeyError:
                        if method == "stored" and len(password_candidates) > 1:
                            fallback_attempted = True
                            stats["formula_fallback_attempted"] += 1
                            writer(f"  {ref}: lagret passord avvist; prover beregnet passord", "warn")
                            continue
                        raise
                if result == "not_encrypted":
                    stats["not_encrypted"] += 1
                    continue
                if fallback_attempted:
                    stats["formula_fallback_success"] += 1
                    writer(f"  {ref}: beregnet passord fungerte etter avvist lagret passord", "info")
                if dry_run:
                    stats["decrypted_tested"] += 1
                else:
                    # Safe name; no original title or password in output path.
                    relative = lob_file.relative_to(root).as_posix()
                    tag = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:12]
                    ext = Path(fname).suffix.lower()
                    outfile = dest_root / f"{lob_file.stem}_{tag}{ext}"
                    try:
                        with outfile.open("xb") as output:
                            scratch.seek(0)
                            shutil.copyfileobj(scratch, output, length=1024 * 1024)
                    except FileExistsError:
                        stats["already_exists"] += 1
                        continue
                    except Exception:
                        outfile.unlink(missing_ok=True)
                        raise
                    stats["decrypted_saved"] += 1
        except Exception as exc:
            stats["decrypt_failed"] += 1
            if stats["decrypt_failed"] <= 15:
                writer(f"  {ref}: dekryptering feilet ({type(exc).__name__}: {exc})", "warn")
        if stats["selected"] <= 15 or stats["selected"] % 250 == 0:
            writer(f"  Behandlet {stats['selected']} utvalgte filer (OK test={stats['decrypted_tested']}, lagret={stats['decrypted_saved']})", "info")
    if target_ref and not stats["selected"]:
        raise RuleError(f"Ingen treff paa LOB-referansen '{target_ref}' og aktive filtre")
    return stats
