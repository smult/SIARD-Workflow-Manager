"""siard_workflow/operations/siardmapper_operation.py

SiardMapperOperation
--------------------
Beriker SIARD-metadata (header/metadata.xml) med beskrivelser fra en
JSON-mal produsert av KDRS SIARDMapper-verktøyet.

Logikken er portet direkte fra SIARDMapper (core/matcher.py,
core/enricher.py, core/xml_parser.py) og kjøres headless uten GUI.

Krever: lxml>=4.9.0  (pip install lxml)

Parametere:
    json_template (str)  — absolutt sti til JSON-malfilen
    overwrite_existing (bool) — overstyr eksisterende beskrivelser (default: False)

Pipeline-modus (ctx.extracted_path satt):
    Modifiserer header/metadata.xml direkte på disk.
Standalone-modus:
    Skriver ny <original>_beriket.siard.
"""

from __future__ import annotations

import html as _html
import io
import json
import shutil
import threading
import xml.etree.ElementTree as _ET
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext

# ── SIARD-namespace ───────────────────────────────────────────────────────────

_SIARD_NS  = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"


def _decode(text: str) -> str:
    """Dekod HTML-entiteter iterativt (håndterer dobbel-enkoding som &amp;oslash;)."""
    if not text:
        return text
    result = text
    for _ in range(4):   # maks 4 pass dekoder selv 4-gangs enkoding
        decoded = _html.unescape(result)
        if decoded == result:
            break
        result = decoded
    return result.strip()
_NS        = {"s": _SIARD_NS}
_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")


def _tag(local: str) -> str:
    return f"{{{_SIARD_NS}}}{local}"


def _local_name(tag) -> str:
    if not isinstance(tag, str):
        return ""
    return tag.split("}", 1)[1] if "}" in tag else tag


# ── Datamodeller (Table / Column) ─────────────────────────────────────────────

@dataclass
class _Column:
    name: str
    description: Optional[str] = None

    @property
    def norm(self) -> str:
        return self.name.strip().lower()


@dataclass
class _Table:
    name: str
    schema_name: str = ""
    folder: Optional[str] = None
    description: Optional[str] = None
    columns: List[_Column] = field(default_factory=list)

    @property
    def norm(self) -> str:
        return self.name.strip().lower()


# ── XML-parsing av SIARD metadata ────────────────────────────────────────────

def _parse_metadata(xml_bytes: bytes) -> List[_Table]:
    from lxml import etree
    root = etree.fromstring(xml_bytes)
    tables: List[_Table] = []
    schemas_el = root.find("s:schemas", _NS)
    if schemas_el is None:
        return tables
    for schema_el in schemas_el.findall("s:schema", _NS):
        schema_name_el = schema_el.find("s:name", _NS)
        schema_name = (schema_name_el.text or "").strip() if schema_name_el is not None else ""
        tables_el = schema_el.find("s:tables", _NS)
        if tables_el is None:
            continue
        for table_el in tables_el.findall("s:table", _NS):
            name_el = table_el.find("s:name", _NS)
            if name_el is None or not name_el.text:
                continue
            desc_el   = table_el.find("s:description", _NS)
            folder_el = table_el.find("s:folder", _NS)
            tbl = _Table(
                name=name_el.text.strip(),
                schema_name=schema_name,
                folder=(folder_el.text or "").strip() if folder_el is not None else None,
                description=(desc_el.text or "").strip() if desc_el is not None else None,
            )
            cols_el = table_el.find("s:columns", _NS)
            if cols_el is not None:
                for col_el in cols_el.findall("s:column", _NS):
                    col_name_el = col_el.find("s:name", _NS)
                    if col_name_el is None or not col_name_el.text:
                        continue
                    col_desc_el = col_el.find("s:description", _NS)
                    tbl.columns.append(_Column(
                        name=col_name_el.text.strip(),
                        description=(col_desc_el.text or "").strip() if col_desc_el is not None else None,
                    ))
            tables.append(tbl)
    return tables


# ── JSON-mal-parsing ──────────────────────────────────────────────────────────

def _parse_json_template(path: Path) -> Dict[str, dict]:
    """Les JSON-malfilen og returner {normalisert_tabellnavn: oppføring}."""
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)

    def _extract_list(d: dict) -> list:
        for key in ("tables", "tabeller"):
            if key in d and isinstance(d[key], list):
                return d[key]
        schema = d.get("templateSchema")
        if isinstance(schema, dict):
            t = schema.get("tables")
            if isinstance(t, list):
                return t
        for v in d.values():
            if isinstance(v, dict) and "tables" in v and isinstance(v["tables"], list):
                return v["tables"]
        raise ValueError(
            "Fant ikke tabelliste i JSON-malen. "
            "Forventet nøkkel 'tables' på toppnivå eller under 'templateSchema'."
        )

    table_list = data if isinstance(data, list) else _extract_list(data)
    result: Dict[str, dict] = {}
    for entry in table_list:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", "").strip()
        if name:
            result[name.lower()] = entry
    return result


# ── Matcher ───────────────────────────────────────────────────────────────────

@dataclass
class _Match:
    table: _Table
    json_table_desc: Optional[str]
    col_descs: Dict[str, str]   # {normalized_col_name: description}


def _match(tables: List[_Table], json_lookup: Dict[str, dict]) -> List[_Match]:
    matches: List[_Match] = []
    for tbl in tables:
        entry = json_lookup.get(tbl.norm)
        col_descs: Dict[str, str] = {}
        if entry:
            for ce in entry.get("columns", []):
                col_name = ce.get("name", "").strip()
                col_desc = _decode(ce.get("description", ""))
                if col_name and col_desc:
                    col_descs[col_name.lower()] = col_desc
        tbl_desc_raw = (entry.get("description", "") or "") if entry else ""
        matches.append(_Match(
            table=tbl,
            json_table_desc=_decode(tbl_desc_raw) or None,
            col_descs=col_descs,
        ))
    return matches


# ── Enricher ──────────────────────────────────────────────────────────────────

def _find_or_insert(parent, local_name: str, after: List[str], before: List[str]):
    from lxml import etree
    existing = parent.find(f"s:{local_name}", _NS)
    if existing is not None:
        return existing
    new_el = etree.SubElement(parent, _tag(local_name))
    child_locals = [_local_name(c.tag) for c in parent]
    appended_idx = len(parent) - 1
    insert_pos = appended_idx
    last_after = next(
        (i for i in range(appended_idx - 1, -1, -1) if child_locals[i] in after), -1)
    first_before = next(
        (i for i, loc in enumerate(child_locals[:-1]) if loc in before), appended_idx)
    if last_after >= 0:
        insert_pos = last_after + 1
    elif first_before < appended_idx:
        insert_pos = first_before
    if insert_pos < appended_idx:
        parent.remove(new_el)
        parent.insert(insert_pos, new_el)
    return new_el


def _enrich(xml_bytes: bytes, matches: List[_Match], overwrite: bool) -> tuple[bytes, int, int]:
    """Returner (beriket_xml, n_tabeller, n_kolonner)."""
    from lxml import etree
    root = etree.fromstring(xml_bytes)
    match_map = {m.table.norm: m for m in matches}
    tables_done = 0
    cols_done = 0

    schemas_el = root.find("s:schemas", _NS)
    if schemas_el is None:
        return etree.tostring(root, encoding="UTF-8", xml_declaration=True,
                              pretty_print=True), 0, 0

    for schema_el in schemas_el.findall("s:schema", _NS):
        tables_el = schema_el.find("s:tables", _NS)
        if tables_el is None:
            continue
        for table_el in tables_el.findall("s:table", _NS):
            name_el = table_el.find("s:name", _NS)
            if name_el is None or not name_el.text:
                continue
            m = match_map.get(name_el.text.strip().lower())
            if m is None:
                continue

            # Tabellbeskrivelse
            if m.json_table_desc:
                desc_el = _find_or_insert(
                    table_el, "description",
                    after=["folder", "name"],
                    before=["columns", "primaryKey", "rows"],
                )
                if overwrite or not (desc_el.text or "").strip():
                    desc_el.text = m.json_table_desc
                    tables_done += 1

            # Kolonnebeskrivelser
            cols_el = table_el.find("s:columns", _NS)
            if cols_el is None or not m.col_descs:
                continue
            for col_el in cols_el.findall("s:column", _NS):
                col_name_el = col_el.find("s:name", _NS)
                if col_name_el is None or not col_name_el.text:
                    continue
                col_desc_txt = m.col_descs.get(col_name_el.text.strip().lower())
                if not col_desc_txt:
                    continue
                cdesc_el = _find_or_insert(
                    col_el, "description",
                    after=[
                        "name", "lobFolder", "encryptionParameter",
                        "defaultValue", "nullable", "type", "typeOriginal",
                        "typeSchema", "typeName", "typeFields", "mimeType",
                    ],
                    before=[],
                )
                if overwrite or not (cdesc_el.text or "").strip():
                    cdesc_el.text = col_desc_txt
                    cols_done += 1

    enriched = etree.tostring(root, encoding="UTF-8", xml_declaration=True,
                               pretty_print=True)
    return enriched, tables_done, cols_done


# ── Oppdater matches med brukerens redigeringer fra dialogen ─────────────────

_META_NS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"


def _remove_tables_from_metadata(xml_bytes: bytes,
                                  marked_names: set
                                  ) -> tuple[bytes, list[tuple[str, str]]]:
    """
    Fjern markerte tabeller fra metadata.xml.

    Returnerer (modifisert_xml_bytes, slettede_paths) der `slettede_paths` er
    en liste av (schema_folder, table_folder)-tupler som kalleren kan bruke
    for å slette tilhørende content/{schema}/{table}/-mapper.
    """
    if not marked_names:
        return xml_bytes, []

    ns = {"ns": _META_NS}
    _ET.register_namespace("", _META_NS)
    tree = _ET.parse(io.BytesIO(xml_bytes))
    root = tree.getroot()

    deleted: list[tuple[str, str]] = []
    for schema in root.findall("ns:schemas/ns:schema", ns):
        sch_folder_el = schema.find("ns:folder", ns)
        sch_folder    = (sch_folder_el.text or "").strip() \
                        if sch_folder_el is not None else ""
        tables_el = schema.find("ns:tables", ns)
        if tables_el is None:
            continue
        for tbl in list(tables_el):
            name_el = tbl.find("ns:name", ns)
            fold_el = tbl.find("ns:folder", ns)
            if name_el is None or fold_el is None:
                continue
            tname = (name_el.text or "").strip()
            if tname in marked_names:
                deleted.append((sch_folder, (fold_el.text or "").strip()))
                tables_el.remove(tbl)

    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    return buf.getvalue(), deleted


def _apply_dialog_edits(matches: List[_Match], edits: dict) -> List[_Match]:
    """
    Kombiner JSON-treff med manuelt redigerte beskrivelser fra dialogen.
    `edits` er {table_norm: {desc: str, cols: {col_norm: str}}}
    """
    for m in matches:
        edit = edits.get(m.table.norm, {})
        # Tabellbeskrivelse
        new_tbl_desc = (edit.get("desc") or "").strip()
        if new_tbl_desc:
            m.json_table_desc = new_tbl_desc
        # Kolonnebeskrivelser
        col_edits: Dict[str, str] = edit.get("cols", {})
        for col_norm, col_desc in col_edits.items():
            desc = (col_desc or "").strip()
            if desc:
                m.col_descs[col_norm] = desc
    return matches


# ── Operasjonsklassen ─────────────────────────────────────────────────────────

class SiardMapperOperation(BaseOperation):
    """
    Beriker SIARD-metadata med beskrivelser fra en JSON-mal (KDRS SIARDMapper).

    JSON-malen lages med det frittstående SIARDMapper-verktøyet, og operasjonen
    appliserer den automatisk i workflow-kjøringen.

    Pipeline-modus: modifiserer header/metadata.xml direkte på disk.
    Standalone:     skriver <original>_beriket.siard.
    """

    operation_id   = "siardmapper"
    label          = "Berik SIARD-metadata (SIARDMapper)"
    description    = (
        "Beriker header/metadata.xml med tabell- og kolonnebeskrivelser fra "
        "en JSON-mal produsert av KDRS SIARDMapper. Krever lxml>=4.9.0."
    )
    category       = "Metadata"
    status         = 2
    produces_siard = True
    default_params = {
        "json_template":       "",     # sti til JSON-malfilen
        "overwrite_existing":  False,  # True = overstyr beskrivelser som allerede finnes
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        # Sjekk lxml
        try:
            import lxml  # noqa: F401
        except ImportError:
            return self._fail(
                "lxml ikke installert. Kjør: pip install lxml>=4.9.0")

        # Valider parametere
        json_path = Path(self.params.get("json_template", "").strip())
        if not json_path or not json_path.exists():
            return self._fail(
                f"JSON-malfil ikke funnet: {json_path!r}. "
                "Angi gyldig sti i operasjonsparametrene.")

        overwrite = bool(self.params.get("overwrite_existing", False))

        # Les JSON-malen
        try:
            json_lookup = _parse_json_template(json_path)
        except Exception as exc:
            return self._fail(f"Kunne ikke lese JSON-mal: {exc}")
        w(f"  JSON-mal: {len(json_lookup)} tabell(er) lastet fra {json_path.name}", "info")

        # ── Bestem kilde for XML ──────────────────────────────────────────────
        pipeline = ctx.extracted_path and ctx.extracted_path.is_dir()
        siard_path = ctx.siard_path

        if pipeline:
            meta_path = ctx.extracted_path / "header" / "metadata.xml"
            if not meta_path.exists():
                meta_path = ctx.extracted_path / "metadata.xml"
            if not meta_path.exists():
                return self._fail("metadata.xml ikke funnet i utpakket mappe")
            xml_bytes = meta_path.read_bytes()
            all_info  = None
            meta_entry = None
        else:
            try:
                with zipfile.ZipFile(siard_path, "r") as zf:
                    name_lower = {n.lower(): n for n in zf.namelist()}
                    meta_entry = next(
                        (name_lower[c] for c in _METADATA_PATHS if c in name_lower), None)
                    if not meta_entry:
                        return self._fail("metadata.xml ikke funnet i SIARD-arkivet")
                    xml_bytes = zf.read(meta_entry)
                    all_info  = zf.infolist()
            except Exception as exc:
                return self._fail(f"Kunne ikke lese SIARD: {exc}")

        # ── Match og eventuell dialog ─────────────────────────────────────────
        try:
            tables  = _parse_metadata(xml_bytes)
            matches = _match(tables, json_lookup)
        except Exception as exc:
            return self._fail(f"Feil under match: {exc}")

        # Sjekk om treffet er 100 %
        n_tbl_tot = len(matches)
        n_tbl_hit = sum(1 for m in matches if m.json_table_desc)
        n_col_tot = sum(len(m.table.columns) for m in matches)
        n_col_hit = sum(
            sum(1 for c in m.table.columns if c.norm in m.col_descs)
            for m in matches)

        full_match = (n_tbl_hit == n_tbl_tot and n_col_hit == n_col_tot)

        if not full_match:
            w(f"  Delvis treff: {n_tbl_hit}/{n_tbl_tot} tabeller, "
              f"{n_col_hit}/{n_col_tot} kolonner", "warn")
            dialog_cb = ctx.metadata.get("siardmapper_dialog_cb")
            if dialog_cb:
                w("  Åpner redigeringsdialog for manglende beskrivelser …", "info")
                # Bygg suggestion-map: {col_norm: [desc, ...]}
                suggestion_map: Dict[str, List[str]] = {}
                for m in matches:
                    for c in m.table.columns:
                        d = m.col_descs.get(c.norm, "")
                        if d:
                            suggestion_map.setdefault(c.norm, []).append(d)
                updated = dialog_cb(
                    matches,
                    siard_path,
                    ctx.extracted_path,
                    json_path,
                    suggestion_map,
                )
                if updated is None:
                    return self._fail("Berikelse avbrutt av operatør")
                # Ny dialog-struktur har "edits" + "marked_for_deletion".
                # Eldre versjoner returnerte flat {tbl_norm: {...}} — håndter begge.
                if isinstance(updated, dict) and "edits" in updated:
                    edits_dict  = updated.get("edits") or {}
                    marked_list = updated.get("marked_for_deletion") or []
                else:
                    edits_dict  = updated
                    marked_list = []
                # Oppdater matches med brukerens redigeringer
                matches = _apply_dialog_edits(matches, edits_dict)
                # Lagre slette-markeringer til etter berikelse
                marked_names = {m.get("name") for m in marked_list if m.get("name")}
            else:
                w("  Ingen dialog tilgjengelig — fortsetter med tilgjengelige treff", "warn")
                marked_names: set = set()
        else:
            w(f"  100 % treff: {n_tbl_tot} tabeller, {n_col_tot} kolonner", "ok")
            marked_names: set = set()

        # ── Berik og skriv ────────────────────────────────────────────────────
        try:
            enriched, n_tbl, n_col = _enrich(xml_bytes, matches, overwrite)
        except Exception as exc:
            return self._fail(f"Feil under berikelse: {exc}")

        # ── Sanering: fjern tabeller markert for sletting ─────────────────────
        deleted_paths: list[tuple[str, str]] = []
        if marked_names:
            try:
                enriched, deleted_paths = _remove_tables_from_metadata(
                    enriched, marked_names)
                w(f"  Fjernet {len(deleted_paths)} tabell(er) fra metadata.xml",
                  "ok")
                for sch_folder, tbl_folder in deleted_paths:
                    w(f"    🗑  content/{sch_folder}/{tbl_folder}/", "info")
            except Exception as exc:
                return self._fail(f"Feil ved sanering av tabeller: {exc}")

        if n_tbl == 0 and n_col == 0 and not deleted_paths:
            w("  Ingen beskrivelser ble lagt til", "warn")
            return self._ok(
                {"tables_enriched": 0, "columns_enriched": 0,
                 "tables_deleted": 0},
                "Ingen endringer gjort")

        if pipeline:
            meta_path.write_bytes(enriched)
            # Slett content/{schema}/{table}/-mapper for markerte tabeller
            for sch_folder, tbl_folder in deleted_paths:
                target = ctx.extracted_path / "content" / sch_folder / tbl_folder
                if target.exists():
                    shutil.rmtree(target, ignore_errors=True)
            summary = (f"{n_tbl} tabell(er) beriket, {n_col} kolonne(r) beriket"
                       + (f", {len(deleted_paths)} tabell(er) slettet"
                          if deleted_paths else ""))
            w(f"  {summary}", "ok")
            return self._ok(
                {"tables_enriched": n_tbl, "columns_enriched": n_col,
                 "tables_deleted": len(deleted_paths)},
                summary)
        else:
            dst_path = siard_path.with_name(siard_path.stem + "_beriket" + siard_path.suffix)
            try:
                # Hopp over content/{schema}/{table}/-entries som tilhører
                # tabeller markert for sletting.
                skip_prefixes = tuple(
                    f"content/{s}/{t}/" for s, t in deleted_paths)
                buf = io.BytesIO()
                with zipfile.ZipFile(siard_path, "r") as zin, \
                     zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zout:
                    for item in all_info:
                        # Skip filer som tilhører slettede tabeller
                        if skip_prefixes and item.filename.startswith(skip_prefixes):
                            continue
                        data = enriched if item.filename == meta_entry else zin.read(item.filename)
                        zout.writestr(item, data)
                dst_path.write_bytes(buf.getvalue())
            except Exception as exc:
                return self._fail(f"Feil ved skriving av SIARD: {exc}")

            summary = (f"{n_tbl} tabell(er) beriket, {n_col} kolonne(r) beriket"
                       + (f", {len(deleted_paths)} tabell(er) slettet"
                          if deleted_paths else ""))
            w(f"  Skrevet: {dst_path.name}  —  {summary}", "ok")
            return self._ok(
                {"tables_enriched": n_tbl, "columns_enriched": n_col,
                 "tables_deleted": len(deleted_paths),
                 "output_path": str(dst_path)},
                summary)
