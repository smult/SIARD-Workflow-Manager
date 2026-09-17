"""siard_workflow/operations/lobfolder_fix_operation.py

LobFolderFixOperation
---------------------
Retter tre kjente lobFolder-inkompatibiliteter i SIARD-filer produsert av
SC Full Convert (SCFC), som gjør at LOBs ikke lar seg indeksere i DBPTK.

Se: https://github.com/keeps/dbptk-developer/issues/749

De tre problemene:
  1. Manglende <lobFolder>content</lobFolder> på database-nivå
     → DBPTK legger til './' foran alle LOB-stier
  2. Kolonne-nivå lobFolder har 'content/'-prefiks
     → stien dobles: './content/schema0/.../lob0/rec0.txt'
  3. Kolonne-nivå lobFolder har avsluttende '/'
     → doble skråstreker i stien: '...lob0//rec0.txt'

Opsjon ``full_file_paths`` (default av):
  DBPTK-VALIDATOREN (A_M_5.6-1-2, MetadataColumnsValidator) bygger stien
  ``"content/" + lobFolder + file`` UTEN skilletegn. Med kolonne-lobFolder
  ``schema0/table88/lob4`` og ``file="rec16.txt"`` blir det
  ``…lob4rec16.txt`` → finnes ikke → fallback til bare ``file`` → starter ikke
  med ``content`` → behandles som EKSTERN LOB → feil. Eneste layout som
  passerer både validatoren og importen (2.1 og 2.2) er full sti i
  attributtet: ``file="content/schema0/table88/lob4/record16.txt"``.

  MERK: KDRS Søk & Vis forventer bare basenavn i file= — opsjonen skal
  derfor kun brukes når DBPTK-validering er målet.
"""

from __future__ import annotations
import os
import re
import shutil
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext

_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")

_RE_COL_PREFIX     = re.compile(r"<lobFolder>/?content/", re.IGNORECASE)
_RE_TRAILING_SLASH = re.compile(r"/</lobFolder>", re.IGNORECASE)
_RE_DB_LOBFOLDER   = re.compile(r"<lobFolder>content</lobFolder>")
_RE_INSERT_AFTER   = re.compile(r"(</dataOriginTimespan>)", re.IGNORECASE)
_DB_LOBFOLDER_TAG  = "<lobFolder>content</lobFolder>"

# Kolonnetyper som kan ha eksterne LOB-filer
_RE_LOB_TYPE = re.compile(r"LOB|LARGE OBJECT|BINARY|XML", re.IGNORECASE)

# <cN … file="…"> i tableX.xml (én eller flere per linje)
_CELL_FILE_RE = re.compile(rb'<(c\d+)\b([^>]*?)\bfile="([^"]*)"')


# ── Offentlig hjelpefunksjon (brukes også av GUI-preflighten) ─────────────────

def scan_lobfolder_issues(siard_path: Path) -> list[str]:
    """
    Skann SIARD-filen for de tre SCFC→DBPTK lobFolder-inkompatibilitetene.
    Returnerer en liste med lesbar beskrivelse av hvert problem som ble funnet.
    Returnerer tom liste ved ingen problemer, eller ved feil under lesing.
    """
    try:
        with zipfile.ZipFile(siard_path, "r") as zf:
            name_lower = {n.lower(): n for n in zf.namelist()}
            meta_entry = next(
                (name_lower[c] for c in _METADATA_PATHS if c in name_lower), None)
            if not meta_entry:
                return []
            meta_bytes = zf.read(meta_entry)
    except Exception:
        return []

    try:
        meta_str = meta_bytes.decode("utf-8")
    except UnicodeDecodeError:
        meta_str = meta_bytes.decode("latin-1")

    issues: list[str] = []

    n1 = len(_RE_COL_PREFIX.findall(meta_str))
    if n1:
        issues.append(
            f"Kolonne-nivå <lobFolder> har 'content/'- eller '/content/'-prefiks "
            f"({n1} forekomst(er))")

    n2 = len(_RE_TRAILING_SLASH.findall(meta_str))
    if n2:
        issues.append(
            f"<lobFolder> har avsluttende '/' ({n2} forekomst(er))")

    if not _RE_DB_LOBFOLDER.search(meta_str):
        issues.append(
            "Mangler <lobFolder>content</lobFolder> på database-nivå")

    return issues


def dbptk_validator_lob_path(col_lob: str | None, ref: str) -> str:
    """
    Stien DBPTK-validatoren (A_M_5.6-1-2, MetadataColumnsValidator) slår opp:
    ``"content/" + lobFolder + file`` — UTEN skilletegn mellom mappe og fil.
    Med lobFolder ``schema0/table89/lob4`` og ``file="record16.txt"`` blir det
    ``content/schema0/table89/lob4record16.txt``, som ikke finnes. Fallback er
    bare ``file``; starter den ikke med ``content`` regnes LOB-en som ekstern og
    rapporteres «not found external lob». Passerer kun med avsluttende ``/`` på
    lobFolder (knekker 2.1-importen) eller full ``content/…``-sti i ``file=``.
    """
    return "content/" + (col_lob or "") + ref


def check_lob_refs(zf: zipfile.ZipFile, max_refs_per_table: int | None = None) -> dict:
    """
    Simuler A_M_5.6-1-2 for file=-referansene i alle tabeller med LOB-kolonner.

    Returnerer::

        {"refs": n, "dbptk_fail": n, "fixable": n, "missing": n, "external": n,
         "tables_scanned": n, "sampled": bool,
         "columns": [{"schema", "table", "col", "lob_folder", "refs",
                      "dbptk_fail", "fixable", "missing", "external",
                      "example_ref", "example_full", "missing_examples"}]}

    * dbptk_fail — referanser validatoren ikke finner med sin sammensetting
    * fixable    — av disse: filen finnes under content/… (løses med
                   «Full sti i file=» i LobFolderFixOperation)
    * missing    — filen finnes ikke i arkivet i det hele tatt
    * external   — eksterne/absolutte referanser (..-, /-, X:-, URI) — hoppes over

    ``max_refs_per_table`` begrenser skannet (preflight på store uttrekk);
    da settes ``sampled`` og tallene er nedre grenser.
    """
    name_lower = {n.lower(): n for n in zf.namelist()}
    entries    = set(zf.namelist())
    meta_entry = next((name_lower[c] for c in _METADATA_PATHS if c in name_lower), None)
    out = {"refs": 0, "dbptk_fail": 0, "fixable": 0, "missing": 0, "external": 0,
           "tables_scanned": 0, "sampled": False, "columns": []}
    if not meta_entry:
        return out
    meta_bytes = zf.read(meta_entry)
    try:
        meta_str = meta_bytes.decode("utf-8")
    except UnicodeDecodeError:
        meta_str = meta_bytes.decode("latin-1")
    lob_tables = _lob_tables_from_metadata(meta_str)
    exists = entries.__contains__

    for (sf, tf), cols in lob_tables.items():
        xml_name = name_lower.get(f"content/{sf}/{tf}/{tf}.xml".lower())
        if not xml_name:
            continue
        out["tables_scanned"] += 1
        per_col: dict[int, dict] = {}
        n_table = 0
        with zf.open(xml_name) as fh:
            for line in fh:
                if b"file=" not in line:
                    continue
                for m in _CELL_FILE_RE.finditer(line):
                    idx = int(m.group(1)[1:])
                    ref = m.group(3).decode("utf-8", "replace")
                    c = per_col.setdefault(idx, {
                        "schema": sf, "table": tf, "col": idx,
                        "lob_folder": cols.get(idx), "refs": 0, "dbptk_fail": 0,
                        "fixable": 0, "missing": 0, "external": 0,
                        "example_ref": None, "example_full": None,
                        "missing_examples": []})
                    c["refs"] += 1
                    n_table += 1
                    r = ref.strip().replace("\\", "/")
                    if not _is_rewritable(r) and not r.startswith("content/"):
                        c["external"] += 1
                        continue
                    concat = dbptk_validator_lob_path(cols.get(idx), ref)
                    if exists(concat) or (r.startswith("content/") and exists(r)):
                        continue
                    c["dbptk_fail"] += 1
                    full = _resolve_full_path(sf, tf, idx, ref, cols.get(idx), exists)
                    if full:
                        c["fixable"] += 1
                        if c["example_ref"] is None:
                            c["example_ref"], c["example_full"] = ref, full
                    else:
                        c["missing"] += 1
                        if len(c["missing_examples"]) < 3:
                            c["missing_examples"].append(ref)
                if max_refs_per_table and n_table >= max_refs_per_table:
                    out["sampled"] = True
                    break
        for c in per_col.values():
            out["columns"].append(c)
            for k in ("refs", "dbptk_fail", "fixable", "missing", "external"):
                out[k] += c[k]
    return out


def scan_lob_ref_issues(siard_path: Path, max_refs_per_table: int = 50) -> dict:
    """
    Preflight-skann (stikkprøve per tabell) for referanser DBPTK-validatoren
    ikke slår opp. Tom dict ved lesefeil.
    """
    try:
        with zipfile.ZipFile(siard_path, "r") as zf:
            return check_lob_refs(zf, max_refs_per_table=max_refs_per_table)
    except Exception:
        return {}


def describe_lob_ref_column(c: dict) -> str:
    """Én rapportlinje per kolonne for A_M_5.6-1-2."""
    where = f"{c['schema']}/{c['table']} c{c['col']}"
    parts = []
    if c["fixable"]:
        parts.append(
            f"{c['fixable']:,} av {c['refs']:,} LOB-referanser slås ikke opp av "
            f"DBPTK-validatoren (basenavn + lobFolder «{c['lob_folder'] or ''}» "
            f"uten '/'), men filene finnes — f.eks. file=\"{c['example_ref']}\" → "
            f"{c['example_full']}. Løses med «Korriger lobFolder» + «Full sti i "
            f"file=» (merk: KDRS Søk & Vis forventer basenavn)")
    if c["missing"]:
        ex = ", ".join(c["missing_examples"])
        parts.append(f"{c['missing']:,} LOB-fil(er) mangler i arkivet, f.eks. {ex}")
    return f"{where}: " + "; ".join(parts)


# ── Interne hjelpefunksjoner ──────────────────────────────────────────────────

def _fix_metadata(meta_str: str) -> tuple[str, list[str]]:
    """Applikér alle tre rettelsene. Returnerer (ny_tekst, endringsliste)."""
    changes: list[str] = []

    new_str, n1 = _RE_COL_PREFIX.subn("<lobFolder>", meta_str)
    if n1:
        changes.append(f"Fjernet 'content/'-prefiks fra {n1} lobFolder(s)")
        meta_str = new_str

    new_str, n2 = _RE_TRAILING_SLASH.subn("</lobFolder>", meta_str)
    if n2:
        changes.append(f"Fjernet avsluttende '/' fra {n2} lobFolder(s)")
        meta_str = new_str

    if not _RE_DB_LOBFOLDER.search(meta_str):
        if _RE_INSERT_AFTER.search(meta_str):
            meta_str = _RE_INSERT_AFTER.sub(
                r"\1\n\t" + _DB_LOBFOLDER_TAG, meta_str, count=1)
            changes.append(
                "Lagt til <lobFolder>content</lobFolder> på database-nivå")
        else:
            changes.append(
                "ADVARSEL: Fant ikke </dataOriginTimespan> — "
                "db-nivå lobFolder ikke lagt til")

    return meta_str, changes


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _lob_tables_from_metadata(meta_str: str
                              ) -> dict[tuple[str, str], dict[int, str | None]]:
    """
    Tabeller med LOB-kolonner fra metadata.xml:
        {(schema_folder, table_folder): {kolonneindeks (1-basert): lobFolder|None}}
    Kun kolonner med LOB-type eller eksplisitt lobFolder tas med.
    """
    result: dict[tuple[str, str], dict[int, str | None]] = {}
    try:
        root = ET.fromstring(meta_str.encode("utf-8"))
    except ET.ParseError:
        return result

    def _child_text(el, name):
        for ch in el:
            if _local(ch.tag) == name:
                return (ch.text or "").strip()
        return ""

    def _children(el, name):
        return [ch for ch in el if _local(ch.tag) == name]

    for schemas in _children(root, "schemas"):
        for schema in _children(schemas, "schema"):
            sf = _child_text(schema, "folder")
            for tables in _children(schema, "tables"):
                for table in _children(tables, "table"):
                    tf = _child_text(table, "folder")
                    if not sf or not tf:
                        continue
                    cols: dict[int, str | None] = {}
                    for columns in _children(table, "columns"):
                        for idx, col in enumerate(_children(columns, "column"), 1):
                            lf = _child_text(col, "lobFolder")
                            ty = _child_text(col, "type")
                            if lf or _RE_LOB_TYPE.search(ty):
                                cols[idx] = lf or None
                    if cols:
                        result[(sf, tf)] = cols
    return result


def _is_rewritable(ref: str) -> bool:
    """Sann for relative, interne referanser som ikke allerede er full sti."""
    ref = ref.strip().replace("\\", "/")
    if not ref or ref.startswith("content/"):
        return False
    # Eksternt fillager / absolutt sti / URI → rør ikke
    if (ref.startswith("..") or ref.startswith("/")
            or re.match(r"^[A-Za-z]:", ref) or "://" in ref):
        return False
    return True


def _resolve_full_path(schema_f: str, table_f: str, col_idx: int, ref: str,
                       col_lob: str | None,
                       exists: Callable[[str], bool]) -> str | None:
    """
    Full ``content/…``-sti for én file=-referanse, eller None hvis referansen
    skal stå urørt (allerede full, ekstern, eller ikke gjenfinnbar).
    """
    if not _is_rewritable(ref):
        return None
    ref = ref.strip().replace("\\", "/")

    base     = ref.rsplit("/", 1)[-1]
    dir_part = ref[:len(ref) - len(base)].strip("/")

    lf = (col_lob or "").strip().replace("\\", "/").strip("/")
    if lf.lower().startswith("content/"):
        lf = lf[len("content/"):]

    candidates: list[str] = []
    if lf:
        coldir = lf if "/" in lf else f"{schema_f}/{table_f}/{lf}"
        candidates.append(f"content/{coldir}/{base}")
        if dir_part:
            candidates.append(f"content/{coldir}/{ref}")
    if dir_part:
        candidates.append(f"content/{schema_f}/{table_f}/{ref}")
    candidates.append(f"content/{schema_f}/{table_f}/lob{col_idx}/{base}")

    for cand in candidates:
        if exists(cand):
            return cand
    return None


def _patch_table_stream(src, dst, schema_f: str, table_f: str,
                        col_lobs: dict[int, str | None],
                        exists: Callable[[str], bool],
                        stats: dict) -> int:
    """
    Les tableX.xml linje for linje fra ``src``, skriv om file=-referanser til
    full content/-sti og skriv til ``dst``. Returnerer antall endrede
    referanser. Ugjenfinnbare referanser telles i stats["file_unresolved"].
    """
    changed = 0
    cache: dict[tuple[int, str], str | None] = {}

    def _sub(m: re.Match) -> bytes:
        nonlocal changed
        tag = m.group(1).decode("ascii")
        try:
            col_idx = int(tag[1:])
        except ValueError:
            return m.group(0)
        try:
            ref = m.group(3).decode("utf-8")
        except UnicodeDecodeError:
            return m.group(0)
        key = (col_idx, ref)
        if key not in cache:
            cache[key] = _resolve_full_path(
                schema_f, table_f, col_idx, ref, col_lobs.get(col_idx), exists)
            if cache[key] is None and _is_rewritable(ref):
                stats["file_unresolved"] = stats.get("file_unresolved", 0) + 1
        new = cache[key]
        if new is None:
            return m.group(0)
        changed += 1
        return b'<' + m.group(1) + m.group(2) + b'file="' + new.encode("utf-8") + b'"'

    for line in src:
        if b'file="' in line:
            line = _CELL_FILE_RE.sub(_sub, line)
        dst.write(line)
    return changed


# ── Operasjonsklassen ─────────────────────────────────────────────────────────

class LobFolderFixOperation(BaseOperation):
    """
    Retter lobFolder-inkompatibiliteter mellom SCFC og DBPTK (issue #1).

    Pipeline-modus (ctx.extracted_path satt):
        Modifiserer header/metadata.xml (og tableX.xml ved full_file_paths)
        direkte på disk. Produserer ingen ny SIARD — RepackSiardOperation
        tar seg av det.

    Standalone-modus:
        Leser fra SIARD-zip, skriver korrigert kopi til <original>_lobfix.siard.
    """

    operation_id   = "lobfolder_fix"
    label          = "Korriger lobFolder (SCFC→DBPTK)"
    description    = (
        "Retter tre lobFolder-inkompatibiliteter i SCFC-produserte SIARD-filer "
        "som hindrer LOB-indeksering i DBPTK: legger til database-nivå "
        "<lobFolder>content</lobFolder>, fjerner 'content/'-prefiks fra "
        "kolonne-nivå lobFolder, og fjerner avsluttende '/'. Opsjon: skriv "
        "full content/-sti i file=-attributtene (DBPTK-validator A_M_5.6-1-2)."
    )
    category       = "Kompatibilitet"
    status         = 2
    produces_siard = True
    modifies_content = True
    premis_event_type  = "Adjustment"
    premis_event_label = "lobFolder-korreksjon"
    default_params: dict = {
        "full_file_paths": False,   # file="content/schemaN/tableM/lobK/recordN.ext"
    }

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and (
            result.data.get("fixes", 0) > 0
            or result.data.get("file_paths", 0) > 0)

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        full_paths = bool(self.params.get("full_file_paths", False))

        # ── Pipeline-modus ────────────────────────────────────────────────────
        if ctx.extracted_path and ctx.extracted_path.is_dir():
            root = ctx.extracted_path
            meta_path = root / "header" / "metadata.xml"
            if not meta_path.exists():
                meta_path = root / "metadata.xml"
            if not meta_path.exists():
                return self._fail("metadata.xml ikke funnet i utpakket mappe")

            raw = meta_path.read_bytes()
            try:
                meta_str = raw.decode("utf-8")
            except UnicodeDecodeError:
                meta_str = raw.decode("latin-1")

            meta_str, changes = _fix_metadata(meta_str)
            if changes:
                meta_path.write_bytes(meta_str.encode("utf-8"))
                w(f"  lobFolder rettet i metadata.xml: {'; '.join(changes)}", "ok")

            stats: dict = {}
            if full_paths:
                self._patch_tables_on_disk(root, meta_str, stats, w)

            n_paths = stats.get("file_paths", 0)
            if not changes and not n_paths:
                return self._ok({"fixes": 0, "file_paths": 0},
                                "Ingen lobFolder-problemer funnet")

            summary = "; ".join(changes + self._path_summary(stats))
            # Ingen output_path — RepackSiardOperation pakker ferdig SIARD
            return self._ok({"fixes": len(changes), **stats}, summary)

        # ── Standalone-modus ──────────────────────────────────────────────────
        siard_path = ctx.siard_path
        try:
            with zipfile.ZipFile(siard_path, "r") as zf:
                name_lower  = {n.lower(): n for n in zf.namelist()}
                meta_entry  = next(
                    (name_lower[c] for c in _METADATA_PATHS if c in name_lower),
                    None)
                if not meta_entry:
                    return self._fail("metadata.xml ikke funnet i SIARD-arkivet")
                meta_bytes  = zf.read(meta_entry)
                all_info    = zf.infolist()
                zip_names   = set(zf.namelist())
        except Exception as exc:
            return self._fail(f"Kunne ikke lese SIARD: {exc}")

        try:
            meta_str = meta_bytes.decode("utf-8")
        except UnicodeDecodeError:
            meta_str = meta_bytes.decode("latin-1")

        meta_str, changes = _fix_metadata(meta_str)
        if not changes and not full_paths:
            return self._ok({"fixes": 0, "file_paths": 0},
                            "Ingen lobFolder-problemer funnet")

        meta_bytes_new = meta_str.encode("utf-8")
        dst_path = siard_path.with_name(
            siard_path.stem + "_lobfix" + siard_path.suffix)

        lob_tables = _lob_tables_from_metadata(meta_str) if full_paths else {}
        table_xml_of = {f"content/{sf}/{tf}/{tf}.xml": (sf, tf)
                        for (sf, tf) in lob_tables}
        stats: dict = {}

        try:
            with zipfile.ZipFile(siard_path, "r") as zin, \
                 zipfile.ZipFile(dst_path, "w", zipfile.ZIP_DEFLATED,
                                 allowZip64=True) as zout:
                for item in all_info:
                    if item.filename == meta_entry:
                        zout.writestr(item, meta_bytes_new)
                        continue
                    key = table_xml_of.get(item.filename)
                    if key is not None:
                        sf, tf = key
                        n = self._patch_table_via_temp(
                            zin, zout, item, sf, tf, lob_tables[key],
                            lambda rel: rel in zip_names, stats, w)
                        if n:
                            w(f"  {item.filename}: {n:,} file=-referanser → "
                              f"full sti", "info")
                        continue
                    if item.is_dir():
                        zout.writestr(item, b"")
                        continue
                    with zin.open(item) as src, zout.open(item, "w") as dst:
                        shutil.copyfileobj(src, dst, 1024 * 1024)
        except Exception as exc:
            dst_path.unlink(missing_ok=True)
            return self._fail(f"Feil ved skriving av SIARD: {exc}")

        n_paths = stats.get("file_paths", 0)
        if not changes and not n_paths:
            dst_path.unlink(missing_ok=True)
            return self._ok({"fixes": 0, "file_paths": 0},
                            "Ingen lobFolder-problemer funnet")

        summary = "; ".join(changes + self._path_summary(stats))
        w(f"  Skrevet: {dst_path.name}  —  {summary}", "ok")
        return self._ok(
            {"fixes": len(changes), **stats, "output_path": str(dst_path)},
            summary)

    # ── full_file_paths-hjelpere ──────────────────────────────────────────────

    @staticmethod
    def _path_summary(stats: dict) -> list[str]:
        out = []
        if stats.get("file_paths"):
            out.append(f"{stats['file_paths']:,} file=-referanser skrevet "
                       f"som full content/-sti")
        if stats.get("file_unresolved"):
            out.append(f"ADVARSEL: {stats['file_unresolved']:,} file=-referanser "
                       f"kunne ikke gjenfinnes og er urørt")
        return out

    def _patch_tables_on_disk(self, root: Path, meta_str: str,
                              stats: dict, w) -> None:
        """Pipeline: skriv om file= i alle tableX.xml med LOB-kolonner."""
        lob_tables = _lob_tables_from_metadata(meta_str)
        if not lob_tables:
            w("  full_file_paths: ingen LOB-kolonner i metadata.xml", "info")
            return

        def _exists(rel: str) -> bool:
            return (root / rel).is_file()

        total = 0
        for (sf, tf), col_lobs in lob_tables.items():
            xml_path = root / "content" / sf / tf / f"{tf}.xml"
            if not xml_path.is_file():
                continue
            tmp = xml_path.with_suffix(".tmp_lobfix")
            try:
                with open(xml_path, "rb") as src, \
                     open(tmp, "wb", buffering=256 * 1024) as dst:
                    n = _patch_table_stream(src, dst, sf, tf, col_lobs,
                                            _exists, stats)
                if n:
                    tmp.replace(xml_path)
                    total += n
                    w(f"  {sf}/{tf}/{tf}.xml: {n:,} file=-referanser → "
                      f"full sti", "info")
                else:
                    tmp.unlink(missing_ok=True)
            except Exception as exc:
                tmp.unlink(missing_ok=True)
                w(f"  FEIL patch {tf}.xml: {exc}", "feil")
        stats["file_paths"] = total
        if stats.get("file_unresolved"):
            w(f"  ADVARSEL: {stats['file_unresolved']:,} file=-referanser "
              f"kunne ikke gjenfinnes — urørt", "warn")

    @staticmethod
    def _patch_table_via_temp(zin: zipfile.ZipFile, zout: zipfile.ZipFile,
                              item: zipfile.ZipInfo, sf: str, tf: str,
                              col_lobs: dict[int, str | None],
                              exists: Callable[[str], bool],
                              stats: dict, w) -> int:
        """Standalone: strøm én tableX.xml via temp-fil inn i ny zip."""
        fd, tmp_name = tempfile.mkstemp(suffix=".xml")
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            with zin.open(item) as src, open(tmp, "wb", buffering=256 * 1024) as dst:
                n = _patch_table_stream(src, dst, sf, tf, col_lobs, exists, stats)
            stats["file_paths"] = stats.get("file_paths", 0) + n
            info = zipfile.ZipInfo(item.filename, date_time=item.date_time)
            info.compress_type = zipfile.ZIP_DEFLATED
            with open(tmp, "rb") as src, zout.open(info, "w") as dst:
                shutil.copyfileobj(src, dst, 1024 * 1024)
            return n
        finally:
            tmp.unlink(missing_ok=True)
