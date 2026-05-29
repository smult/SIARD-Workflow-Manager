"""
siard_workflow/operations/schema_selector_operation.py

SchemaSelectorOperation — la operatør velge hvilke schemas som skal være med
i resultat-SIARD. Hvis ett eller flere schemas ekskluderes, renses både
`header/metadata.xml` og `content/`-strukturen for de schemaene som ikke
velges.

Kjøremoduser:
  - Pipeline (etter Pakk ut SIARD): jobber direkte på utpakket extract_dir.
  - Standalone: pakker ut, viser dialog, renser, pakker inn ny SIARD.

Auto-add: UnpackSiardOperation tilbyr å legge til denne operasjonen rett
etter seg selv hvis utpakket SIARD inneholder mer enn ett schema.
"""
from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation


_NS = "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"
_NS_MAP = {"ns": _NS}


# ── Schema-leser ──────────────────────────────────────────────────────────────

def _list_schemas_from_xml(metadata_bytes: bytes) -> list[dict]:
    """
    Parser metadata.xml og returnerer liste av schemas med navn, folder
    og tabeller.  Brukes både fra pipeline (lest fra disk) og standalone
    (lest fra ZIP).
    """
    root = ET.parse(io := __import__("io").BytesIO(metadata_bytes)).getroot()
    schemas: list[dict] = []
    for schema_el in root.findall("ns:schemas/ns:schema", _NS_MAP):
        name_el   = schema_el.find("ns:name", _NS_MAP)
        folder_el = schema_el.find("ns:folder", _NS_MAP)
        if name_el is None or folder_el is None:
            continue
        tables: list[dict] = []
        for tbl in schema_el.findall("ns:tables/ns:table", _NS_MAP):
            tname_el  = tbl.find("ns:name",   _NS_MAP)
            tfold_el  = tbl.find("ns:folder", _NS_MAP)
            trows_el  = tbl.find("ns:rows",   _NS_MAP)
            if tname_el is None or tfold_el is None:
                continue
            try:
                rows = int((trows_el.text or "0").strip())
            except Exception:
                rows = 0
            tables.append({
                "name":   tname_el.text,
                "folder": tfold_el.text,
                "rows":   rows,
            })
        schemas.append({
            "name":   name_el.text,
            "folder": folder_el.text,
            "tables": tables,
        })
    return schemas


def _measure_schema_size(extract_dir: Path, folder: str) -> tuple[int, int]:
    """Returnerer (total_bytes, antall_filer) for et schemas content-mappe."""
    schema_dir = extract_dir / "content" / folder
    if not schema_dir.is_dir():
        return 0, 0
    total = 0
    n = 0
    for f in schema_dir.rglob("*"):
        if f.is_file():
            try:
                total += f.stat().st_size
                n += 1
            except Exception:
                pass
    return total, n


def list_schemas_with_size(extract_dir: Path) -> list[dict]:
    """
    Les metadata.xml fra utpakket SIARD og returner liste av
    {name, folder, tables[], size_bytes, file_count}.
    """
    metadata_path = extract_dir / "header" / "metadata.xml"
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata.xml ikke funnet: {metadata_path}")
    schemas = _list_schemas_from_xml(metadata_path.read_bytes())
    for s in schemas:
        size, n = _measure_schema_size(extract_dir, s["folder"])
        s["size_bytes"] = size
        s["file_count"] = n
    return schemas


def list_schemas_from_zip(siard_path: Path) -> list[dict]:
    """
    Les metadata.xml direkte fra SIARD-zip og returner schemas-info
    (uten filstørrelser — krever utpakking).
    """
    with zipfile.ZipFile(siard_path, "r", allowZip64=True) as zf:
        meta_name = next(
            (n for n in zf.namelist()
             if n.lower().endswith("header/metadata.xml")), None)
        if not meta_name:
            raise FileNotFoundError("metadata.xml ikke funnet i SIARD")
        schemas = _list_schemas_from_xml(zf.read(meta_name))
        # Beregn størrelse fra zip-entries
        for s in schemas:
            prefix = f"content/{s['folder']}/"
            total = 0
            n = 0
            for info in zf.infolist():
                if info.filename.startswith(prefix) and not info.is_dir():
                    total += info.file_size
                    n += 1
            s["size_bytes"] = total
            s["file_count"] = n
    return schemas


# ── Schema-rensing ────────────────────────────────────────────────────────────

def remove_schemas(extract_dir: Path,
                   schemas_to_remove: list[str]) -> dict:
    """
    Fjern oppgitte schemas (etter navn) fra utpakket SIARD:
      1. Fjern <schema>-noder fra header/metadata.xml
      2. Slett innholdsmappene content/{folder}/

    Returnerer dict med opprydningsstatistikk.
    """
    if not schemas_to_remove:
        return {"removed_schemas": 0, "deleted_folders": [],
                "bytes_freed": 0, "files_deleted": 0}

    metadata_path = extract_dir / "header" / "metadata.xml"
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata.xml ikke funnet: {metadata_path}")

    # Registrer namespace for å bevare ns0-prefiks ved skriving
    ET.register_namespace("", _NS)
    tree = ET.parse(metadata_path)
    root = tree.getroot()
    schemas_el = root.find("ns:schemas", _NS_MAP)
    if schemas_el is None:
        return {"removed_schemas": 0, "deleted_folders": [],
                "bytes_freed": 0, "files_deleted": 0}

    to_remove_lower = {s.strip().lower() for s in schemas_to_remove}
    deleted_folders: list[str] = []
    removed = 0

    # ET.Element doesn't have safe in-place iteration with remove —
    # bygg listen først
    for schema_el in list(schemas_el):
        name_el   = schema_el.find("ns:name",   _NS_MAP)
        folder_el = schema_el.find("ns:folder", _NS_MAP)
        if name_el is None or folder_el is None:
            continue
        if (name_el.text or "").strip().lower() in to_remove_lower:
            deleted_folders.append(folder_el.text or "")
            schemas_el.remove(schema_el)
            removed += 1

    if removed > 0:
        # Skriv tilbake metadata.xml
        tree.write(metadata_path, encoding="utf-8", xml_declaration=True)

    # Slett content-mapper og tell besparelser
    bytes_freed = 0
    files_deleted = 0
    for folder in deleted_folders:
        schema_dir = extract_dir / "content" / folder
        if schema_dir.is_dir():
            # Tell før sletting
            for f in schema_dir.rglob("*"):
                if f.is_file():
                    try:
                        bytes_freed += f.stat().st_size
                        files_deleted += 1
                    except Exception:
                        pass
            shutil.rmtree(schema_dir, ignore_errors=True)

    return {
        "removed_schemas":  removed,
        "deleted_folders":  deleted_folders,
        "bytes_freed":      bytes_freed,
        "files_deleted":    files_deleted,
    }


# ── Operasjon ─────────────────────────────────────────────────────────────────

class SchemaSelectorOperation(BaseOperation):
    """
    La operatør velge hvilke schemas som skal være med i resultat-SIARD.
    Schemas som ikke velges renses fra både metadata.xml og content/.
    """

    operation_id    = "schema_selector"
    label           = "Schema-velger"
    category        = "Innhold"
    status          = 2
    produces_siard  = True
    requires_unpack = True

    default_params = {
        "auto_select_all": False,   # hvis True: hopp over dialog (alle med)
        "dry_run":         False,
    }

    @property
    def description(self) -> str:
        return ("Viser dialog der operatør velger hvilke schemas som skal "
                "være med i resultatet. Schemas som ikke velges fjernes fra "
                "metadata.xml og content/.")

    # ── run ──────────────────────────────────────────────────────────────────

    def run(self, ctx) -> object:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg, lvl="info"):
            if log: log.log(msg, lvl)
            if pcb: pcb("log", msg=msg, level=lvl)

        def progress(event, **kw):
            if pcb: pcb(event, **kw)

        w("=" * 56)
        w("  SCHEMA-VELGER", "step")
        w("=" * 56)

        auto_all = bool(self.params.get("auto_select_all", False))
        dry_run  = bool(self.params.get("dry_run", False))

        # ── Pipeline-modus ───────────────────────────────────────────────────
        extract_dir = getattr(ctx, "extracted_path", None)
        if extract_dir and Path(extract_dir).is_dir():
            try:
                stats = self._process_filesystem(
                    Path(extract_dir), ctx, w, progress,
                    auto_all=auto_all, dry_run=dry_run)
            except Exception as exc:
                import traceback as _tb
                w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
                progress("finish", stats={})
                return self._fail(str(exc))
            self.produces_siard = False
            return self._ok(stats, self._summary_msg(stats))

        # ── Standalone-modus ─────────────────────────────────────────────────
        self.produces_siard = True
        src_path = ctx.siard_path
        suffix   = "_schema_renset"
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        c = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{c}" + src_path.suffix)
            c += 1

        try:
            stats = self._process_standalone(
                src_path, dst_path, ctx, w, progress,
                auto_all=auto_all, dry_run=dry_run)
        except Exception as exc:
            import traceback as _tb
            w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
            progress("finish", stats={})
            return self._fail(str(exc))

        if not dry_run:
            w(f"    Ny SIARD: {dst_path}", "ok")
        return self._ok(
            {**stats, "output_path": str(dst_path)},
            self._summary_msg(stats))

    # ── Pipeline-modus ───────────────────────────────────────────────────────

    def _process_filesystem(self, extract_dir: Path, ctx, w, progress,
                            *, auto_all: bool, dry_run: bool) -> dict:
        progress("phase", phase=1, total_phases=2,
                 label="Leser schemas fra metadata.xml")
        schemas = list_schemas_with_size(extract_dir)
        if not schemas:
            w("  Ingen schemas funnet i metadata.xml.", "info")
            return {"removed_schemas": 0, "kept_schemas": 0}

        w(f"  Fant {len(schemas)} schema(er):", "info")
        for s in schemas:
            w(f"    • {s['name']} (folder {s['folder']}): "
              f"{len(s['tables'])} tabell(er), {s['file_count']:,} filer, "
              f"{s['size_bytes']:,} bytes", "info")
        progress("phase_done")

        # Velg schemas
        if len(schemas) <= 1 or auto_all:
            keep_names = [s["name"] for s in schemas]
            w("  Beholder alle schemas (kun ett eller auto_select_all=True).",
              "info")
        else:
            keep_names = self._ask_user_for_schemas(ctx, schemas, w)
            if keep_names is None:
                # Brukeren avbrøt
                return self._fail_dict("Schema-valg avbrutt av bruker")

        remove_names = [s["name"] for s in schemas
                        if s["name"] not in keep_names]
        if not remove_names:
            w("  Alle schemas beholdes — ingen rensing nødvendig.", "ok")
            return {"removed_schemas": 0, "kept_schemas": len(schemas),
                    "bytes_freed": 0, "files_deleted": 0}

        progress("phase", phase=2, total_phases=2,
                 label=f"Fjerner {len(remove_names)} schema(er)")
        w(f"  Fjerner {len(remove_names)} schema(er): "
          f"{', '.join(remove_names)}", "warn")
        if dry_run:
            # Tørrkjøring: bare tell, ikke slett
            stats = {"removed_schemas": len(remove_names),
                     "deleted_folders": [s["folder"] for s in schemas
                                          if s["name"] in remove_names],
                     "bytes_freed": sum(s["size_bytes"] for s in schemas
                                         if s["name"] in remove_names),
                     "files_deleted": sum(s["file_count"] for s in schemas
                                           if s["name"] in remove_names)}
            w("  TØRKJØRING — ingen endringer skrevet.", "warn")
        else:
            stats = remove_schemas(extract_dir, remove_names)
        stats["kept_schemas"] = len(keep_names)
        progress("phase_done")
        return stats

    # ── Standalone-modus ─────────────────────────────────────────────────────

    def _process_standalone(self, src_path: Path, dst_path: Path,
                            ctx, w, progress,
                            *, auto_all: bool, dry_run: bool) -> dict:
        # Pakk ut til midlertidig mappe
        progress("phase", phase=1, total_phases=4,
                 label="Pakker ut SIARD")
        with tempfile.TemporaryDirectory(prefix="schema_sel_") as tmp_str:
            tmp = Path(tmp_str)
            w(f"  Pakker ut til {tmp} ...", "info")
            with zipfile.ZipFile(src_path, "r", allowZip64=True) as zin:
                zin.extractall(tmp)
            progress("phase_done")

            # Les schemas + velg
            progress("phase", phase=2, total_phases=4,
                     label="Leser schemas")
            schemas = list_schemas_with_size(tmp)
            w(f"  Fant {len(schemas)} schema(er)", "info")
            for s in schemas:
                w(f"    • {s['name']} (folder {s['folder']}): "
                  f"{len(s['tables'])} tabell(er), {s['file_count']:,} filer, "
                  f"{s['size_bytes']:,} bytes", "info")
            progress("phase_done")

            if len(schemas) <= 1 or auto_all:
                keep_names = [s["name"] for s in schemas]
                w("  Beholder alle schemas (kun ett eller auto_select_all=True).",
                  "info")
            else:
                keep_names = self._ask_user_for_schemas(ctx, schemas, w)
                if keep_names is None:
                    return self._fail_dict("Schema-valg avbrutt av bruker")

            remove_names = [s["name"] for s in schemas
                            if s["name"] not in keep_names]

            # Rens
            progress("phase", phase=3, total_phases=4,
                     label="Renser metadata.xml og content/")
            if remove_names and not dry_run:
                stats = remove_schemas(tmp, remove_names)
            else:
                stats = {"removed_schemas": len(remove_names),
                         "deleted_folders": [s["folder"] for s in schemas
                                              if s["name"] in remove_names],
                         "bytes_freed": sum(s["size_bytes"] for s in schemas
                                             if s["name"] in remove_names),
                         "files_deleted": sum(s["file_count"] for s in schemas
                                               if s["name"] in remove_names)}
            stats["kept_schemas"] = len(keep_names)
            progress("phase_done")

            # Pakk inn ny SIARD
            progress("phase", phase=4, total_phases=4,
                     label="Pakker ny SIARD")
            if not dry_run:
                from siard_workflow.core.siard_format import (
                    get_zip_compresslevel as _get_lvl,
                    get_smart_skip_enabled as _get_skip,
                    is_precompressed_bytes as _is_pre,
                )
                _level     = _get_lvl()
                _smartskip = _get_skip()
                _compress  = (zipfile.ZIP_STORED if _level == 0
                              else zipfile.ZIP_DEFLATED)
                _comp_lvl  = _level if _level > 0 else None
                w(f"  Pakker ny SIARD til {dst_path} (kompresjon nivå "
                  f"{_level}) ...", "info")
                with zipfile.ZipFile(dst_path, "w",
                                     compression=_compress,
                                     allowZip64=True,
                                     compresslevel=_comp_lvl) as zout:
                    for f in tmp.rglob("*"):
                        if f.is_file():
                            arc = str(f.relative_to(tmp))
                            if _level > 0 and _smartskip:
                                try:
                                    head = f.open("rb").read(16)
                                except Exception:
                                    head = b""
                                if head and _is_pre(head):
                                    zout.write(f, arc,
                                               compress_type=zipfile.ZIP_STORED)
                                    continue
                            zout.write(f, arc)
            progress("phase_done")
        return stats

    # ── Bruker-dialog (callback til GUI-tråden) ──────────────────────────────

    def _ask_user_for_schemas(self, ctx, schemas: list[dict],
                              w) -> "list[str] | None":
        """
        Be brukeren velge schemas via GUI.  Returnerer liste av navn å beholde,
        eller None hvis brukeren avbrøt.

        Bruker callback registrert i ctx.metadata["ask_schema_select_cb"].
        Hvis ingen callback finnes, beholdes alle schemas.
        """
        cb = ctx.metadata.get("ask_schema_select_cb")
        if cb is None:
            w("  Ingen GUI-callback registrert — beholder alle schemas.",
              "warn")
            return [s["name"] for s in schemas]
        try:
            result = cb(schemas)
        except Exception as exc:
            w(f"  Dialog feilet: {exc} — beholder alle schemas.", "warn")
            return [s["name"] for s in schemas]
        if result is None:
            return None
        return list(result)

    # ── Hjelpefunksjoner ─────────────────────────────────────────────────────

    @staticmethod
    def _summary_msg(stats: dict) -> str:
        return (f"{stats.get('removed_schemas', 0)} schema(er) fjernet, "
                f"{stats.get('kept_schemas', 0)} beholdt, "
                f"{stats.get('bytes_freed', 0):,} bytes spart")

    @staticmethod
    def _fail_dict(msg: str) -> dict:
        return {"removed_schemas": 0, "kept_schemas": 0, "error": msg}
