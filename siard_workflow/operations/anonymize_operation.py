"""
siard_workflow/operations/anonymize_operation.py

AnonymizeOperation — anonymiser personidentifiserende informasjon (PII) i et
SIARD-arkiv, og bytt ut BLOB/CLOB/filer med dummy-innhold.

Strategi (kolonne-først hybrid — se siard_workflow/core/anonymize):
  1. Klassifiser hver kolonne én gang (navn-heuristikk + verdisampling + valgfri
     lokal Ollama for tvetydige kolonner).
  2. Deterministisk per-type anonymisering av hele kolonnen (samme original →
     samme fake i hele arkivet, så fremmednøkler bevares).
  3. Regex/validering for strukturert PII (fnr, e-post, telefon, postnr) inne i
     fritekst — kun spennene erstattes, resten av teksten beholdes.
  4. LOB-filer byttes til dummy (Lorem Ipsum-PDF/RTF/tekst eller media-stub), og
     length/digest i tableX.xml oppdateres.

Kjøremoduser (som schema_selector / blob_convert):
  - Pipeline (etter «Pakk ut SIARD»): jobber på ctx.extracted_path, produces_siard=False.
  - Standalone: pakker ut → anonymiserer → pakker ny SIARD (_anonymisert).
"""
from __future__ import annotations

import re
import os
import html
import threading
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from xml.sax.saxutils import escape as _xml_escape

from siard_workflow.core.anonymize.fake_generators import lorem_ipsum

from siard_workflow.core.base_operation import BaseOperation
from siard_workflow.core.anonymize.pii_detect import (
    PiiType, ColumnClass, Span, classify_column, find_all_pii, find_name_spans,
    VALUE_TYPES, should_anonymize, is_excluded_field, is_ambiguous_name,
    is_valid_fnr, looks_like_person_name)

# Navnetyper som verifiseres mot innhold ved tvetydige kolonnenavn
_NAME_VALUE_TYPES = (PiiType.FULL_NAME, PiiType.FIRST_NAME, PiiType.LAST_NAME)
from siard_workflow.core.anonymize.fake_generators import MappingStore
from siard_workflow.core.anonymize import dummy_files


_NS_RE = re.compile(r"^\{[^}]+\}")
_LOB_TYPES = ("NCLOB", "CLOB", "NBLOB", "BLOB")
# Tekst-kolonnetyper som kan inneholde PII. Andre typer (BOOLEAN, INT, DATE,
# DOUBLE osv.) anonymiseres ALDRI — de kan ikke inneholde navn/adresse/fnr o.l.,
# og å endre dem ville bryte skjema-/XML-validering.
_TEXT_TYPES = ("VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "TEXT", "STRING", "CLOB")


def _is_text_type(col_type: str) -> bool:
    """True hvis kolonnetypen er tekst (kan inneholde PII)."""
    return any(t in (col_type or "").upper() for t in _TEXT_TYPES)
# Inline tekstcelle: <cN>tekst</cN>  (ikke selvlukkende fil-ref <cN .../>)
_CELL_RE = re.compile(rb"<c(\d+)>(.*?)</c\1>", re.DOTALL)


def _local(tag: str) -> str:
    return _NS_RE.sub("", tag)


def _child_text(el, local: str) -> str:
    for ch in el:
        if _local(ch.tag).lower() == local:
            return (ch.text or "").strip()
    return ""


# ── Metadata-leser (alle tabeller, ikke bare LOB-tabeller) ────────────────────

def read_tables(metadata_path: Path) -> "dict[str, dict]":
    """
    Les header/metadata.xml og returner per tabell:
      table_key ("schemaFolder/tableFolder") -> {
        schema_name, table_name, schema_folder, table_folder,
        columns: [{idx, name, type, lob_folder}],
        lob_cols: {idx: lob_folder_rel},          # rel under content/
        digest_cols: [idx], digesttype_cols: [idx],
      }
    Kolonneindeks (1-basert) tilsvarer <cN>-taggene i tableX.xml.
    """
    _DIGEST_KW     = ("digest", "md5", "sha1", "sha256", "checksum", "hash")
    _DIGESTTYPE_KW = ("digesttype", "hashtype", "checksumtype")

    tables: dict[str, dict] = {}
    if not metadata_path.exists():
        return tables
    try:
        root = ET.parse(metadata_path).getroot()
    except Exception:
        return tables

    for schema in root.iter():
        if _local(schema.tag).lower() != "schema":
            continue
        s_name   = _child_text(schema, "name")
        s_folder = _child_text(schema, "folder")
        for table in schema.iter():
            if _local(table.tag).lower() != "table":
                continue
            t_name   = _child_text(table, "name")
            t_folder = _child_text(table, "folder")
            cols: list[dict] = []
            idx = 0
            for col in table.iter():
                if _local(col.tag).lower() != "column":
                    continue
                idx += 1
                cols.append({
                    "idx":        idx,
                    "name":       _child_text(col, "name"),
                    "type":       _child_text(col, "type").upper(),
                    "lob_folder": _child_text(col, "lobfolder"),
                })
            if not cols or not s_folder or not t_folder:
                continue
            key = f"{s_folder}/{t_folder}"
            lob_cols = {c["idx"]: c["lob_folder"] for c in cols
                        if c["type"] in _LOB_TYPES and c["lob_folder"]}
            digest_cols = [c["idx"] for c in cols
                           if any(k in c["name"].lower() for k in _DIGEST_KW)]
            digesttype_cols = [c["idx"] for c in cols
                               if any(k in c["name"].lower() for k in _DIGESTTYPE_KW)]
            tables[key] = {
                "schema_name":     s_name,
                "table_name":      t_name,
                "table_desc":      _child_text(table, "description"),
                "schema_folder":   s_folder,
                "table_folder":    t_folder,
                "columns":         cols,
                "lob_cols":        lob_cols,
                "digest_cols":     digest_cols,
                "digesttype_cols": digesttype_cols,
            }
    return tables


def _table_xml_path(root: Path, info: dict) -> Path:
    tf = info["table_folder"]
    return root / "content" / info["schema_folder"] / tf / f"{tf}.xml"


# ── Celle-sampling og -omskriving ─────────────────────────────────────────────

def _decode_cell(raw: bytes) -> str:
    """Inner-tekst fra <cN>..</cN> → lesbar streng (XML-entiteter løst opp)."""
    return html.unescape(raw.decode("utf-8", errors="replace"))


def _apply_spans(text: str, spans: "list", mapping) -> str:
    """Erstatt PII-spenn i text med deterministiske fakes (via MappingStore),
    behold resten av teksten. Spenn må være ikke-overlappende og sortert."""
    out, pos = [], 0
    for sp in spans:
        if sp.start < pos:
            continue
        out.append(text[pos:sp.start])
        out.append(mapping.map(sp.pii_type, sp.text))
        pos = sp.end
    out.append(text[pos:])
    return "".join(out)


def sample_columns(xml_path: Path, max_samples: int = 50) -> "dict[int, list[str]]":
    """Stream tableX.xml og samle inntil max_samples ikke-tomme verdier per kolonne.
    (Beholdt for bakoverkompatibilitet/tester — klassifiseringen bruker nå
    collect_tail_rows for å unngå dummy-data i de første radene.)"""
    samples: dict[int, list[str]] = {}
    if not xml_path.exists():
        return samples
    try:
        with open(xml_path, "rb") as f:
            for line in f:
                for m in _CELL_RE.finditer(line):
                    idx = int(m.group(1))
                    val = _decode_cell(m.group(2)).strip()
                    if not val:
                        continue
                    bucket = samples.setdefault(idx, [])
                    if len(bucket) < max_samples:
                        bucket.append(val)
                if samples and all(len(v) >= max_samples for v in samples.values()) \
                        and len(samples) > 1:
                    break
    except Exception:
        pass
    return samples


def collect_tail_rows(xml_path: Path, window: int = 500) -> "list[dict[int, str]]":
    """Stream tableX.xml og behold de SISTE `window` radene (hver rad =
    {kolonneindeks: verdi}).

    De første radene i en tabell inneholder ofte test-/dummydata fra
    systemoppsett, så vi sampler fra halen for mer representative ekte data.
    """
    from collections import deque
    rows: "deque[dict[int, str]]" = deque(maxlen=window)
    if not xml_path.exists():
        return []
    in_row = False
    buf: list[bytes] = []
    try:
        with open(xml_path, "rb") as f:
            for line in f:
                if b"<row" in line:
                    in_row, buf = True, []
                if in_row:
                    buf.append(line)
                if in_row and b"</row>" in line:
                    in_row = False
                    blob = b"".join(buf)
                    row = {int(m.group(1)): _decode_cell(m.group(2))
                           for m in _CELL_RE.finditer(blob)}
                    if row:
                        rows.append(row)
    except Exception:
        pass
    return list(rows)


def column_samples_from_rows(rows: "list[dict[int, str]]",
                             max_per_col: int = 50) -> "dict[int, list[str]]":
    """Bygg per-kolonne verdiliste fra rad-utvalget (ikke-tomme verdier)."""
    samples: dict[int, list[str]] = {}
    for row in rows:
        for idx, val in row.items():
            if val and val.strip():
                bucket = samples.setdefault(idx, [])
                if len(bucket) < max_per_col:
                    bucket.append(val.strip())
    return samples


def spread_rows(rows: "list[dict[int, str]]", n: int = 5) -> "list[dict[int, str]]":
    """Velg n rader jevnt fordelt utover utvalget (ikke n påfølgende)."""
    if len(rows) <= n:
        return list(rows)
    step = len(rows) / n
    idxs = sorted({int(i * step) for i in range(n)})
    return [rows[i] for i in idxs]


# ── Operasjon ─────────────────────────────────────────────────────────────────

class AnonymizeOperation(BaseOperation):

    operation_id    = "anonymize"
    label           = "SIARD Anonymisering"
    category        = "Innhold"
    status          = 0          # verifisert ende-til-ende mot ekte SIARD
    produces_siard  = True
    requires_unpack = True
    modifies_content = True
    premis_event_type = "anonymisering"

    default_params = {
        "output_suffix":         "_anonymisert",
        "use_ollama":            True,
        "replace_lobs":          True,
        "replace_binary_media":  True,
        "show_preview":          True,   # vis forhåndsvisning før endringer skrives
        "preview_rows":          5,
        "analysis_rows":         20,    # antall eksempelrader Ollama vurderer per tabell
        "ollama_freetext_limit": 200,   # maks antall fritekstceller sendt til Ollama
        "dry_run":               False,
        # ── Datareduksjon (subset) ──────────────────────────────────────────
        # Når på: i stedet for å anonymisere HELE datasettet beholdes inntil
        # `subset_rows` rader fra hver tabell, pluss relaterte rader (foreldre
        # transitivt + utvalg av barn) slik at fremmednøkler henger sammen.
        "subset_enabled":          False,
        "subset_rows":             300,   # rader per tabell (frø)
        "subset_include_children": True,  # ta med barnerader fra viktige frø-rader
        "subset_table_mode":       "ollama",  # ollama | heuristic | manual
        "subset_important_tables": "",    # manuelt: komma-separerte tabellnavn
    }

    @property
    def description(self) -> str:
        return ("Finner personidentifiserende informasjon (fnr, navn, adresse, "
                "postnr, telefon, e-post) og erstatter med deterministiske "
                "fiktive verdier. BLOB/CLOB/filer byttes til dummy-innhold. "
                "Bruker valgfritt lokal Ollama for økt treffsikkerhet (aldri sky).")

    # ── run ────────────────────────────────────────────────────────────────────

    def run(self, ctx) -> object:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")
        io_lock = threading.Lock()   # trådsikker logging/progress under parallellkjøring

        def w(msg, lvl="info"):
            with io_lock:
                if log:
                    log.log(msg, lvl)
                if pcb:
                    pcb("log", msg=msg, level=lvl)

        def progress(event, **kw):
            with io_lock:
                if pcb:
                    pcb(event, **kw)

        w("=" * 56)
        w("  SIARD ANONYMISERING", "step")
        w("=" * 56)

        dry_run = bool(self.params.get("dry_run", False))

        # Bygg Ollama-klient hvis aktivert (degraderer hvis ikke tilgjengelig)
        self._ollama = self._build_ollama(w)
        self._ollama_budget = int(self.params.get("ollama_freetext_limit", 200) or 0)
        self._budget_lock = threading.Lock()
        self._mapping = MappingStore()
        self._workers = self._worker_count()

        # ── Pipeline-modus ───────────────────────────────────────────────────
        extract_dir = getattr(ctx, "extracted_path", None)
        if extract_dir and Path(extract_dir).is_dir():
            try:
                stats = self._process_tree(Path(extract_dir), ctx, w, progress,
                                           dry_run=dry_run)
            except _AbortedByUser as exc:
                w(f"  {exc}", "warn")
                progress("finish", stats={})
                return self._fail(str(exc))
            except Exception as exc:
                import traceback as _tb
                w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
                progress("finish", stats={})
                return self._fail(str(exc))
            self.produces_siard = False
            progress("finish", stats=stats)
            return self._ok(stats, self._summary_msg(stats))

        # ── Standalone-modus ─────────────────────────────────────────────────
        self.produces_siard = True
        src_path = ctx.siard_path
        suffix   = str(self.params.get("output_suffix", "_anonymisert"))
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        c = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{c}" + src_path.suffix)
            c += 1

        try:
            stats = self._process_standalone(src_path, dst_path, ctx, w, progress,
                                             dry_run=dry_run)
        except _AbortedByUser as exc:
            w(f"  {exc}", "warn")
            progress("finish", stats={})
            return self._fail(str(exc))
        except Exception as exc:
            import traceback as _tb
            w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
            progress("finish", stats={})
            return self._fail(str(exc))

        if not dry_run:
            w(f"    Ny SIARD: {dst_path}", "ok")
        progress("finish", stats=stats)
        return self._ok({**stats, "output_path": str(dst_path)},
                        self._summary_msg(stats))

    # ── Ollama-oppsett ─────────────────────────────────────────────────────────

    def _build_ollama(self, w):
        if not bool(self.params.get("use_ollama", True)):
            w("  Ollama: deaktivert i parametere — bruker regex/heuristikk.", "info")
            return None
        try:
            from settings import get_config as _cfg
            if not bool(_cfg("ollama_enabled", True)):
                w("  Ollama: deaktivert i innstillinger — bruker regex/heuristikk.",
                  "info")
                return None
            from siard_workflow.core.anonymize.ollama_client import OllamaClient
            client = OllamaClient(
                host=str(_cfg("ollama_host", "127.0.0.1")),
                port=int(_cfg("ollama_port", 11434) or 11434),
                model=str(_cfg("ollama_model", "")),
                timeout=int(_cfg("ollama_timeout", 30) or 30))
        except Exception as exc:
            w(f"  Ollama: kunne ikke initialiseres ({exc}) — bruker regex.", "warn")
            return None
        if not client.is_alive():
            w("  Ollama: ikke tilgjengelig lokalt — degraderer til regex/heuristikk.",
              "warn")
            return None
        model = client.pick_model() or "?"
        w(f"  Ollama: aktiv (modell: {model}) — øker treffsikkerhet lokalt.", "ok")
        return client

    # ── Felles prosessering på utpakket tre ──────────────────────────────────

    def _process_tree(self, root: Path, ctx, w, progress, *, dry_run: bool) -> dict:
        stop_ev = ctx.metadata.get("stop_event")
        metadata_path = root / "header" / "metadata.xml"
        if not metadata_path.exists():
            raise FileNotFoundError(f"metadata.xml ikke funnet: {metadata_path}")

        stats = {"tables": 0, "pii_columns": 0, "cells_anonymized": 0,
                 "freetext_cells": 0, "lobs_replaced": 0, "lob_columns": 0,
                 "mappings": 0}

        # Fase 1: les tabeller
        progress("phase", phase=1, total_phases=4, label="Leser metadata.xml")
        tables = read_tables(metadata_path)
        w(f"  Fant {len(tables)} tabell(er) i metadata.", "info")
        progress("phase_done")

        # ── Datareduksjon (subset): planlegg radutvalg med referanseintegritet ──
        subset_keep: "dict | None" = None
        subset_info: "dict | None" = None
        if bool(self.params.get("subset_enabled", False)):
            subset_keep, subset_info = self._plan_subset(
                root, metadata_path, tables, w, progress)

        # Fase 2: GRUNDIG FORANALYSE (Ollama vurderer ~10 rader per tabell)
        progress("phase", phase=2, total_phases=4,
                 label="Foranalyse: identifiserer persondata (Ollama)")
        plans, lob_plans = self._classify_all(root, tables, w, progress)
        stats["pii_columns"] = sum(
            1 for p in plans.values() if p["pii_type"] in VALUE_TYPES
            or p["pii_type"] == PiiType.FREE_TEXT)
        stats["lob_columns"] = len(lob_plans)
        progress("phase_done")

        # Fase 3: forhåndsvisning + bekreftelse
        summary = self._build_summary(plans, lob_plans)
        if subset_info is not None:
            summary["subset"] = subset_info
        show_preview = bool(self.params.get("show_preview", True))
        if dry_run:
            w("  TØRKJØRING — viser plan, skriver ingen endringer.", "warn")
            self._log_summary(summary, w)
            return {**stats, "dry_run": True}
        if show_preview:
            self._confirm_or_abort(ctx, summary, w)
        else:
            # Forhåndsvisning avskrudd → kjør endringene direkte etter identifisering
            w("  Forhåndsvisning avskrudd — kjører endringer direkte.", "info")
            self._log_summary(summary, w)

        # Fase 4: skriv om celler + bytt LOB-filer (parallelt per tabell)
        progress("phase", phase=3, total_phases=4, label="Anonymiserer celler")
        replace_lobs = bool(self.params.get("replace_lobs", True))
        n_tables = len(tables)

        def _do_table(key, info):
            xml_path = _table_xml_path(root, info)
            table_plans = {idx: p for (tk, idx), p in plans.items() if tk == key}
            keep_rows = subset_keep.get(key) if subset_keep is not None else None
            return self._rewrite_table(root, info, xml_path, table_plans,
                                       replace_lobs, w, keep_rows=keep_rows)

        done = 0
        kept_counts: dict = {}
        with ThreadPoolExecutor(max_workers=self._workers) as ex:
            futs = {ex.submit(_do_table, key, info): key
                    for key, info in tables.items()}
            for fut in as_completed(futs):
                key = futs[fut]
                cell_stats = fut.result()
                stats["tables"]           += 1
                stats["cells_anonymized"] += cell_stats["cells"]
                stats["freetext_cells"]   += cell_stats["freetext"]
                stats["lobs_replaced"]    += cell_stats["lobs"]
                if "kept_rows" in cell_stats:
                    kept_counts[key] = cell_stats["kept_rows"]
                done += 1
                progress("phase_progress", done=done, total=n_tables)
        progress("phase_done")

        # Subset: oppdater <rows> i metadata.xml til faktisk beholdt antall
        if subset_keep is not None and kept_counts:
            patched = self._patch_metadata_rows(metadata_path, tables, kept_counts, w)
            stats["subset_rows_kept"]   = sum(kept_counts.values())
            stats["subset_tables"]      = patched
            if subset_info:
                stats["subset_original_rows"] = subset_info.get("total_original", 0)

        stats["mappings"] = len(self._mapping)

        # Fase 4b: skriv rapport
        progress("phase", phase=4, total_phases=4, label="Skriver rapport")
        self._write_report(ctx, summary, stats, w)
        progress("phase_done")
        return stats

    # ── Parallellitet ──────────────────────────────────────────────────────────

    @staticmethod
    def _worker_count() -> int:
        """Antall arbeidertråder. I/O- og Ollama-bundet → flere enn CPU-kjerner.
        Leser config 'max_workers' hvis satt, ellers auto."""
        try:
            from settings import get_config
            n = int(get_config("max_workers", 0) or 0)
        except Exception:
            n = 0
        if n <= 0:
            n = min(16, (os.cpu_count() or 4) * 2)
        return max(1, n)

    # ── Datareduksjon (subset) ─────────────────────────────────────────────────

    def _plan_subset(self, root: Path, metadata_path: Path, tables: dict,
                     w, progress) -> "tuple[dict, dict]":
        """Planlegg referensielt radutvalg. Returnerer (keep, info)."""
        from siard_workflow.core.anonymize import subset as S
        w("  Datareduksjon (subset): planlegger referensielt utvalg ...", "step")
        relations = S.read_relations(metadata_path, tables)
        n_fk = sum(len(r.fks) for r in relations.values())
        n_pk = sum(1 for r in relations.values() if r.pk_cols)
        w(f"    {n_pk} tabell(er) med primærnøkkel, "
          f"{n_fk} fremmednøkkel-relasjon(er).", "info")
        if n_fk == 0:
            w("    Ingen fremmednøkler i metadata — utvalget blir per-tabell "
              "uten relasjonslukking.", "warn")

        index = S.build_index(root, tables, relations, _table_xml_path)

        # Kopitabeller (samme navn + _tmp/_kopi/dato) utelates helt.
        copies = S.detect_copy_tables(tables)
        if copies:
            cnames = ", ".join(sorted(tables[tk]["table_name"] for tk in copies))
            w(f"    Utelater {len(copies)} kopitabell(er): {cnames}", "info")

        mode = str(self.params.get("subset_table_mode", "ollama")).lower()
        important = self._select_important_tables(
            mode, tables, relations, index, w, exclude=copies)
        important -= copies
        names = ", ".join(sorted(tables[tk]["table_name"] for tk in important)) \
            or "(ingen)"
        w(f"    Viktige tabeller ({len(important)}): {names}", "info")

        n = int(self.params.get("subset_rows", 300) or 300)
        plan = S.SubsetPlan(
            important=important,
            n_rows=n,
            include_children=bool(self.params.get("subset_include_children", True)),
            child_cap=n,
            keep_full_threshold=n,   # tabeller ≤ n rader beholdes i sin helhet
            exclude=frozenset(copies),
        )
        keep, info = S.select_rows(index, relations, tables, plan, w)
        info["important_names"] = sorted(
            tables[tk]["table_name"] for tk in info.get("important", []) if tk in tables)
        info["mode"] = mode
        w(f"    Reduserer {info['total_original']:,} → "
          f"{info['total_kept']:,} rader.", "ok")
        return keep, info

    def _select_important_tables(self, mode: str, tables: dict, relations: dict,
                                 index, w, exclude: "set | None" = None) -> set:
        from siard_workflow.core.anonymize import subset as S
        exclude = exclude or set()
        name_to_key = {info["table_name"].lower(): tk
                       for tk, info in tables.items()}

        if mode == "manual":
            raw = str(self.params.get("subset_important_tables", "") or "")
            wanted = [s.strip() for s in raw.replace(";", ",").split(",") if s.strip()]
            chosen = {name_to_key[n.lower()] for n in wanted
                      if n.lower() in name_to_key}
            if chosen:
                return chosen
            w("    Subset: ingen gyldige manuelle tabellnavn — "
              "faller tilbake på heuristikk.", "warn")
            mode = "heuristic"

        if mode == "ollama" and self._ollama is not None:
            try:
                summary = [t for t in self._subset_schema_summary(
                    tables, relations, index)
                    if name_to_key.get(t["name"].lower()) not in exclude]
                rec = self._ollama.recommend_subset(summary)
                chosen = {name_to_key[n.lower()] for n in rec
                          if n.lower() in name_to_key}
                if chosen:
                    w(f"    Subset: Ollama foreslo {len(chosen)} sentrale "
                      f"tabell(er).", "ok")
                    return chosen
                w("    Subset: Ollama ga ingen brukbare forslag — "
                  "bruker heuristikk.", "warn")
            except Exception as exc:
                w(f"    Subset: Ollama-forslag feilet ({exc}) — "
                  f"bruker heuristikk.", "warn")

        return set(S.recommend_important_heuristic(
            tables, relations, index.row_count, exclude=exclude))

    @staticmethod
    def _subset_schema_summary(tables: dict, relations: dict, index) -> list:
        """Bygg tabell-oppsummering til Ollama: navn, beskrivelse, rader, ref-tall."""
        incoming = {tk: 0 for tk in tables}
        for tk, rel in relations.items():
            for fk in rel.fks:
                if fk.parent_key in incoming:
                    incoming[fk.parent_key] += 1
        out = []
        for tk, info in tables.items():
            out.append({
                "name":        info["table_name"],
                "description": info.get("table_desc", ""),
                "rows":        index.row_count.get(tk, 0),
                "refs_in":     incoming.get(tk, 0),
                "refs_out":    len(relations.get(tk).fks) if relations.get(tk) else 0,
            })
        return out

    @staticmethod
    def _patch_metadata_rows(metadata_path: Path, tables: dict,
                             kept_counts: dict, w) -> int:
        """Oppdater <rows>N</rows> i metadata.xml til beholdt antall per tabell.
        Returnerer antall oppdaterte tabeller."""
        try:
            tree = ET.parse(metadata_path)
        except Exception as exc:
            w(f"    Subset: kunne ikke lese metadata.xml for <rows>-oppdatering: "
              f"{exc}", "warn")
            return 0
        root_el = tree.getroot()
        m = re.match(r"\{([^}]+)\}", root_el.tag)
        if m:
            ET.register_namespace("", m.group(1))   # bevar default-namespace

        folder_keep = {}
        for key, cnt in kept_counts.items():
            info = tables.get(key)
            if info:
                folder_keep[(info["schema_folder"], info["table_folder"])] = cnt

        patched = 0
        for schema in root_el.iter():
            if _local(schema.tag).lower() != "schema":
                continue
            s_folder = _child_text(schema, "folder")
            for table in schema.iter():
                if _local(table.tag).lower() != "table":
                    continue
                t_folder = _child_text(table, "folder")
                cnt = folder_keep.get((s_folder, t_folder))
                if cnt is None:
                    continue
                for child in table:
                    if _local(child.tag).lower() == "rows":
                        child.text = str(cnt)
                        patched += 1
                        break
        if patched:
            try:
                tree.write(metadata_path, encoding="utf-8", xml_declaration=True)
            except Exception as exc:
                w(f"    Subset: skriving av metadata.xml feilet: {exc}", "warn")
                return 0
        return patched

    # ── Klassifisering (parallelt per tabell) ─────────────────────────────────

    def _classify_table(self, root: Path, key: str, info: dict):
        """Klassifiser én tabell. Returner (plans_subset, lob_subset, known_subset,
        log_lines). Trådsikker — ingen delt tilstand muteres her.

        Strategi:
          1. Sampler de SISTE radene (collect_tail_rows) — unngår dummy/test-data
             som ofte ligger først i tabellen.
          2. Lar lokal Ollama analysere ~5 representative rader + feltnavnene
             helhetlig for å foreslå hvilke felter som inneholder persondata.
          3. Kombinerer: navn-/verdi-heuristikk er primær (med strenge vakter);
             Ollama-forslaget fyller inn kolonner heuristikken markerer som OTHER.
        """
        plans: dict = {}
        lob_plans: dict = {}
        log_lines: list[tuple[str, str]] = []
        lob_cols = info["lob_cols"]

        rows = collect_tail_rows(_table_xml_path(root, info))
        samples = column_samples_from_rows(rows)

        # 1) Navn-/verdi-heuristikk (rask baseline) for alle vanlige kolonner.
        heur: dict[int, ColumnClass] = {}
        for col in info["columns"]:
            idx = col["idx"]
            if idx in lob_cols:
                continue
            if col["type"] in ("CLOB", "NCLOB") and not col["lob_folder"]:
                continue   # inline fritekst håndteres separat under
            # Ikke-tekstkolonner (BOOLEAN/INT/DATE/DOUBLE …): IKKE navnebasert
            # klassifisering (unngår at f.eks. «Personale» BOOLEAN blir navn).
            # Men personnummer kan ligge i et numerisk felt → verdibasert
            # fnr-deteksjon (gyldige 11-sifrede mod-11).
            if not _is_text_type(col["type"]):
                nvals = [v for v in samples.get(idx, []) if v and v.strip()]
                if nvals and sum(1 for v in nvals if is_valid_fnr(v)) / len(nvals) >= 0.6:
                    heur[idx] = ColumnClass(PiiType.FNR, "value")
                else:
                    heur[idx] = ColumnClass(PiiType.OTHER, "non-text")
                continue
            heur[idx] = classify_column(col["name"], samples.get(idx, []), ollama=None)

        # Finnes det tekstkolonner i det hele tatt? Da er tabellen verdt en
        # helhetlig Ollama-foranalyse.
        has_text_cols = any(
            _is_text_type(c["type"]) for c in info["columns"]
            if c["idx"] not in lob_cols)

        ollama_alive = False
        if self._ollama is not None and rows and has_text_cols:
            try:
                ollama_alive = bool(self._ollama.is_alive())
            except Exception:
                ollama_alive = False

        # 2) GRUNDIG FORANALYSE: la Ollama vurdere flere reelle eksempelrader (fra
        #    halen, ikke de første som ofte er dummy) for HVER tabell. Feltnavnene
        #    SKJULES (nøytrale etiketter) — vurderingen baseres KUN på verdiene, og
        #    KUN tekstkolonner sendes (numerisk fnr fanges av heuristikken).
        ollama_types: dict[str, str] = {}
        if ollama_alive:
            try:
                n_rows = int(self.params.get("analysis_rows", 20) or 20)
                cols_info = [(c["idx"], c["name"], c["type"])
                             for c in info["columns"]
                             if c["idx"] not in lob_cols and _is_text_type(c["type"])]
                ollama_types = self._ollama.analyze_table(
                    cols_info, spread_rows(rows, n_rows), info.get("table_name", ""))
            except Exception:
                ollama_types = {}
        ollama_lc = {k.lower(): v for k, v in ollama_types.items()}

        def _ollama_type(col_name: str) -> "PiiType | None":
            sug = ollama_types.get(col_name) or ollama_lc.get(col_name.lower())
            if not sug:
                return None
            try:
                pt = PiiType[sug]
            except KeyError:
                return None
            return pt if pt is not PiiType.OTHER else None

        # 3) Bygg planer. Regex/validering har autoritet på fnr/e-post; Ollama-
        #    foranalysen er autoritet på navn/sted (tvetydige tilfeller).
        for col in info["columns"]:
            idx, name = col["idx"], col["name"]
            if idx in lob_cols:
                folder = root / "content" / lob_cols[idx]
                n_files = (sum(1 for f in folder.rglob("*") if f.is_file())
                           if folder.is_dir() else 0)
                lob_plans[(key, idx)] = {"col_name": name,
                                         "lob_folder": lob_cols[idx],
                                         "n_files": n_files}
                plans[(key, idx)] = {"pii_type": PiiType.LOB, "source": "metadata",
                                     "col_name": name, "samples": []}
                continue
            if col["type"] in ("CLOB", "NCLOB") and not col["lob_folder"]:
                pt = (PiiType.OTHER if is_excluded_field(name)
                      else PiiType.FREE_TEXT)
                plans[(key, idx)] = {"pii_type": pt,
                                     "source": ("excluded" if pt is PiiType.OTHER
                                                else "metadata-inline"),
                                     "col_name": name,
                                     "samples": samples.get(idx, [])[:8]}
                if pt is PiiType.FREE_TEXT:
                    log_lines.append((f"    {info['table_name']}.{name}: "
                                      f"FREE_TEXT (inline CLOB)", "info"))
                continue

            cc = heur[idx]
            sug_pt = _ollama_type(name) if ollama_alive else None

            if is_excluded_field(name) or not _is_text_type(col["type"]):
                pass   # ekskludert eller ikke-tekst → behold heuristikk (OTHER/fnr-verdi)
            elif cc.pii_type in _NAME_VALUE_TYPES:
                # Navne-treff fra heuristikk. Tvetydige (NavnBM, FylkeNavn,
                # bar «Navn» …) MÅ bekreftes mot INNHOLDET; sterke nøkkelord
                # (Fornavn/Etternavn) stoles på.
                if is_ambiguous_name(name) and not self._is_name_column(samples.get(idx)):
                    cc = ColumnClass(PiiType.OTHER, "ikke-personnavn")
                    log_lines.append((f"    {info['table_name']}.{name}: "
                                      f"OTHER (innhold: ikke personnavn)", "info"))
            elif cc.pii_type is PiiType.OTHER and sug_pt is not None:
                # Ollama-foranalysen foreslår en type på en kolonne heuristikken
                # bommet på. Navneforslag bekreftes mot innhold (form + navne-
                # ordbok); sted/fnr/e-post godtas (strenge per-verdi-vakter
                # beskytter).
                if sug_pt in _NAME_VALUE_TYPES:
                    if self._is_name_column(samples.get(idx)):
                        cc = ColumnClass(sug_pt, "ollama-table")
                else:
                    cc = ColumnClass(sug_pt, "ollama-table")

            plans[(key, idx)] = {"pii_type": cc.pii_type, "source": cc.source,
                                 "col_name": name, "samples": samples.get(idx, [])[:8]}
            if cc.pii_type in VALUE_TYPES or cc.pii_type == PiiType.FREE_TEXT:
                log_lines.append((f"    {info['table_name']}.{name}: "
                                  f"{cc.pii_type.value} (kilde: {cc.source})", "info"))
        return plans, lob_plans, log_lines

    @staticmethod
    def _is_name_column(samples) -> bool:
        """Deterministisk avgjørelse: er dette en PERSONNAVN-kolonne?

        Krever at verdiene har navne-FORM (looks_like_person_name) OG at en andel
        gjenkjennes som kjente norske fornavn/etternavn (navneordbok). Dette er
        langt mer pålitelig enn LLM-en, som feilaktig bekrefter «J», «Høy»,
        «Klasseliste», «Sokna skole» som personnavn. Tom kolonne → False
        (ikke bekreftet → ikke anonymiser; unngår falske treff)."""
        from siard_workflow.core.anonymize.pii_detect import is_recognized_name
        vals = [v for v in (samples or []) if v and v.strip()]
        if not vals:
            return False
        shape = sum(1 for v in vals if looks_like_person_name(v)) / len(vals)
        if shape < 0.6:
            return False
        recog = sum(1 for v in vals if is_recognized_name(v)) / len(vals)
        return recog >= 0.25

    def _classify_all(self, root: Path, tables: dict, w, progress=None):
        """Foranalyse: klassifiser alle tabeller parallelt (Ollama + heuristikk)
        og slå sammen resultatene. Sender prosentvis framdrift per tabell."""
        plans: dict = {}
        lob_plans: dict = {}
        n = len(tables)
        done = 0
        with ThreadPoolExecutor(max_workers=self._workers) as ex:
            futs = [ex.submit(self._classify_table, root, key, info)
                    for key, info in tables.items()]
            for fut in as_completed(futs):
                p, lp, logs = fut.result()
                plans.update(p)
                lob_plans.update(lp)
                for msg, lvl in logs:
                    w(msg, lvl)
                done += 1
                if progress:
                    progress("phase_progress", done=done, total=n)
        return plans, lob_plans

    # ── Omskriving av én tabell (celler + LOB-filer + digest/length) ──────────

    def _rewrite_table(self, root: Path, info: dict, xml_path: Path,
                       table_plans: dict, replace_lobs: bool, w,
                       keep_rows: "set | None" = None) -> dict:
        out = {"cells": 0, "freetext": 0, "lobs": 0}
        if not xml_path.exists():
            return out
        subset = keep_rows is not None

        # Subset: hvilke LOB-filer refereres av BEHOLDTE rader? (resten er
        # foreldreløse og slettes — de hører til forkastede rader).
        kept_lob_refs: "set | None" = None
        if subset and info["lob_cols"]:
            kept_lob_refs = self._collect_kept_lob_refs(xml_path, keep_rows)

        # 1) Bytt LOB-filer på disk; bygg identitets-rename for digest/length-patch.
        #    Ved subset: slett foreldreløse LOB-filer i stedet for å bytte dem.
        ren_bytes: dict[bytes, bytes] = {}
        ren_paths: dict[bytes, Path] = {}
        if info["lob_cols"] and (replace_lobs or subset):
            for idx, lob_folder in info["lob_cols"].items():
                folder = root / "content" / lob_folder
                if not folder.is_dir():
                    continue
                for fp in folder.rglob("*"):   # rglob → fanger segmenterte LOB (seg0/…)
                    if not fp.is_file():
                        continue
                    # Subset: slett foreldreløse LOB-filer (kun direkte i lob-mappa
                    # for å ikke røre segmenterte under-mapper for beholdte rader).
                    if (subset and kept_lob_refs is not None
                            and fp.parent == folder
                            and fp.name not in kept_lob_refs):
                        try:
                            fp.unlink()
                        except Exception:
                            pass
                        continue
                    if not replace_lobs:
                        continue
                    try:
                        head = fp.open("rb").read(65536)
                    except Exception:
                        continue
                    kind, data = dummy_files.pick_dummy_for(
                        fp, head,
                        replace_binary_media=bool(
                            self.params.get("replace_binary_media", True)))
                    if kind == dummy_files.KIND_BIN and not data:
                        continue  # behold (media som ikke skal byttes)
                    try:
                        fp.write_bytes(data)
                    except Exception:
                        continue
                    bn = fp.name.encode("utf-8")
                    ren_bytes[bn] = bn          # samme navn → kun digest/length-refresh
                    ren_paths[bn] = fp
                    out["lobs"] += 1

        # Hvilke kolonner skal omskrives som verdi / fritekst
        value_cols = {idx: p["pii_type"] for idx, p in table_plans.items()
                      if p["pii_type"] in VALUE_TYPES}
        freetext_cols = {idx for idx, p in table_plans.items()
                         if p["pii_type"] == PiiType.FREE_TEXT}
        # Inline binær LOB (BLOB/NBLOB uten lobFolder) → byttes til hex av dummy
        inline_blob_cols = {c["idx"] for c in info["columns"]
                            if c["type"] in ("BLOB", "NBLOB") and not c["lob_folder"]}
        # Maks tegnlengde per kolonne (CHAR/VARCHAR/NCHAR/NVARCHAR(n)) — hard
        # grense slik at ingen fiktiv verdi kan bryte skjemaets kolonnelengde.
        col_maxlen: dict[int, int] = {}
        for c in info["columns"]:
            mlen = re.search(r"CHAR\((\d+)\)", c["type"])
            if mlen:
                col_maxlen[c["idx"]] = int(mlen.group(1))

        need_cell_rewrite = bool(value_cols or freetext_cols or inline_blob_cols)
        need_digest_patch = bool(ren_bytes)
        # Ved subset må vi alltid strømme om for å filtrere rader, selv uten PII.
        if not subset and not need_cell_rewrite and not need_digest_patch:
            return out

        from siard_workflow.operations.blob_convert_operation import (
            _patch_line_with_digest)

        def _rewrite_line(line: bytes) -> bytes:
            if need_cell_rewrite and b"<c" in line:
                line, nc, nf = self._rewrite_line_cells(
                    line, value_cols, freetext_cols, inline_blob_cols, col_maxlen)
                out["cells"]    += nc
                out["freetext"] += nf
            if need_digest_patch and (b"file=" in line or b"href=" in line):
                line, _ = _patch_line_with_digest(line, ren_bytes, ren_paths)
            return line

        tmp_path = xml_path.with_suffix(xml_path.suffix + ".anon_tmp")
        kept = 0
        try:
            with open(xml_path, "rb") as src, open(tmp_path, "wb") as dst:
                if not subset:
                    for line in src:
                        dst.write(_rewrite_line(line))
                else:
                    row_pos = -1
                    in_row = False
                    buf: list[bytes] = []
                    for line in src:
                        if b"<row" in line and not in_row:
                            in_row = True
                            row_pos += 1
                            buf = []
                        if in_row:
                            buf.append(line)
                            if b"</row>" in line:
                                in_row = False
                                if row_pos in keep_rows:
                                    kept += 1
                                    for bl in buf:
                                        dst.write(_rewrite_line(bl))
                                # forkastet rad → skrives ikke
                            continue
                        dst.write(line)   # header/footer-linjer
            tmp_path.replace(xml_path)
        except Exception as exc:
            w(f"    FEIL ved omskriving av {xml_path.name}: {exc}", "feil")
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
        if subset:
            out["kept_rows"] = kept
        return out

    @staticmethod
    def _collect_kept_lob_refs(xml_path: Path, keep_rows: set) -> set:
        """Returner sett av LOB-filnavn (basenavn) referert av BEHOLDTE rader."""
        refs: set = set()
        file_re = re.compile(rb'(?:file|href)="([^"]+)"')
        row_pos = -1
        in_row = False
        buf: list[bytes] = []
        try:
            with open(xml_path, "rb") as f:
                for line in f:
                    if b"<row" in line and not in_row:
                        in_row = True
                        row_pos += 1
                        buf = []
                    if in_row:
                        buf.append(line)
                        if b"</row>" in line:
                            in_row = False
                            if row_pos in keep_rows:
                                blob = b"".join(buf)
                                for m in file_re.finditer(blob):
                                    ref = m.group(1).decode("utf-8", "replace")
                                    base = ref.replace("\\", "/").rsplit("/", 1)[-1]
                                    if base:
                                        refs.add(base)
        except Exception:
            pass
        return refs

    def _rewrite_line_cells(self, line: bytes, value_cols: dict,
                            freetext_cols: set,
                            inline_blob_cols: "set | None" = None,
                            col_maxlen: "dict | None" = None
                            ) -> "tuple[bytes, int, int]":
        n_cells = 0
        n_free = 0
        inline_blob_cols = inline_blob_cols or set()
        col_maxlen = col_maxlen or {}

        def _emit(grp1: bytes, idx: int, text: str) -> bytes:
            # Hard lengdegrense → aldri bryt skjemaets CHAR(n)
            maxlen = col_maxlen.get(idx)
            if maxlen is not None and len(text) > maxlen:
                text = text[:maxlen]
            return (b"<c" + grp1 + b">" + _xml_escape(text).encode("utf-8")
                    + b"</c" + grp1 + b">")

        def _repl(m: "re.Match") -> bytes:
            nonlocal n_cells, n_free
            idx = int(m.group(1))
            inner = m.group(2)
            if idx in inline_blob_cols and inner.strip():
                # Inline binær LOB (hex) → hex av dummy-tekst
                n_cells += 1
                hex_dummy = dummy_files.dummy_text().hex().upper()
                maxlen = col_maxlen.get(idx)
                if maxlen is not None and len(hex_dummy) > maxlen:
                    hex_dummy = hex_dummy[:maxlen - (maxlen % 2)]   # behold gyldig hex
                return (b"<c" + m.group(1) + b">" + hex_dummy.encode("ascii")
                        + b"</c" + m.group(1) + b">")
            if idx in value_cols:
                original = _decode_cell(inner)
                # Per-verdi-vakt: endre kun verdier som faktisk matcher typen
                # (fnr=11 sifre, norsk telefon, aldri filnavn).
                if not should_anonymize(value_cols[idx], original):
                    return m.group(0)
                fake = self._mapping.map(value_cols[idx], original)
                n_cells += 1
                return _emit(m.group(1), idx, fake)
            if idx in freetext_cols:
                original = _decode_cell(inner)
                if not original.strip():
                    return m.group(0)
                # 1) Erstatt konkrete PII-spenn (personnavn via ordbok + fnr/e-post)
                #    deterministisk PÅ PLASS — behold resten av teksten.
                spans = self._freetext_spans(original)
                if spans:
                    new_text = _apply_spans(original, spans, self._mapping)
                    n_cells += 1
                    n_free += 1
                    return _emit(m.group(1), idx, new_text)
                # 2) Ingen eksplisitte spenn, men Ollama vurderer teksten som
                #    kontekstuelt identifiserende → hele feltet blir Lorem ipsum.
                if self._is_context_identifiable(original):
                    words = max(5, min(120, len(original.split()) or 8))
                    n_cells += 1
                    n_free += 1
                    return _emit(m.group(1), idx, lorem_ipsum(words, seed=original))
            return m.group(0)

        return _CELL_RE.sub(_repl, line), n_cells, n_free

    @staticmethod
    def _freetext_spans(text: str) -> "list[Span]":
        """Finn PII-spenn i fritekst som skal byttes på plass: personnavn (via
        navneordbok) + fødselsnummer + e-post. Telefon/postnr er utenfor omfang
        for fritekst. Overlapp fjernes (fnr/e-post har prioritet over navn)."""
        spans = [s for s in find_all_pii(text)
                 if s.pii_type in (PiiType.FNR, PiiType.EMAIL)]
        taken = [(s.start, s.end) for s in spans]
        for sp in find_name_spans(text):
            if not any(not (sp.end <= a or sp.start >= b) for a, b in taken):
                spans.append(sp)
                taken.append((sp.start, sp.end))
        spans.sort(key=lambda s: s.start)
        return spans

    def _is_context_identifiable(self, text: str) -> bool:
        """Sekundær sjekk: lokal Ollama vurderer om teksten KONTEKSTUELT
        identifiserer en privatperson (navn som ikke er i ordboka, unike
        detaljer). Cappet og trådsikkert. Uten Ollama → False."""
        if self._ollama is None or len(text) < 30:
            return False
        with self._budget_lock:
            if self._ollama_budget <= 0:
                return False
            self._ollama_budget -= 1
        try:
            return bool(self._ollama.judge_identifiable(text))
        except Exception:
            return False

    # ── Forhåndsvisning / bekreftelse ─────────────────────────────────────────

    def _build_summary(self, plans: dict, lob_plans: dict) -> dict:
        preview_rows = int(self.params.get("preview_rows", 5) or 5)
        columns = []
        for (key, idx), p in plans.items():
            pt = p["pii_type"]
            if pt not in VALUE_TYPES and pt != PiiType.FREE_TEXT:
                continue
            examples = []
            for val in p["samples"][:preview_rows]:
                if pt == PiiType.FREE_TEXT:
                    # Fritekst: personnavn/fnr/e-post byttes på plass (resten
                    # beholdes). Forhåndsvisningen viser regel-veien; Ollamas
                    # kontekstuelle vurdering skjer ved kjøring.
                    spans = self._freetext_spans(val) if val.strip() else []
                    after = _apply_spans(val, spans, self._mapping) if spans else val
                elif should_anonymize(pt, val):
                    after = self._mapping.map(pt, val)
                else:
                    after = val   # uendret (matcher ikke typen, f.eks. filnavn)
                examples.append({"before": val[:80], "after": after[:80]})
            columns.append({
                "table": key, "column": p["col_name"],
                "pii_type": pt.value, "source": p["source"],
                "examples": examples})
        lob_columns = [{
            "table": key, "column": v["col_name"],
            "lob_folder": v["lob_folder"], "n_files": v["n_files"]}
            for (key, idx), v in lob_plans.items()]
        return {
            "columns": columns,
            "lob_columns": lob_columns,
            "ollama_used": self._ollama is not None,
            "ollama_model": (getattr(self._ollama, "model", "") or "")
                            if self._ollama else "",
        }

    def _confirm_or_abort(self, ctx, summary: dict, w):
        cb = ctx.metadata.get("anonymize_preview_cb")
        if cb is None:
            w("  Ingen GUI-callback — fortsetter uten interaktiv bekreftelse "
              "(headless).", "warn")
            self._log_summary(summary, w)
            return
        try:
            ok = cb(summary)
        except Exception as exc:
            w(f"  Forhåndsvisning feilet ({exc}) — fortsetter uten bekreftelse.",
              "warn")
            return
        if ok is False:
            raise _AbortedByUser("Anonymisering avbrutt av bruker")

    def _log_summary(self, summary: dict, w):
        sub = summary.get("subset")
        if sub:
            w(f"  DATAREDUKSJON (subset): {sub.get('total_original', 0):,} → "
              f"{sub.get('total_kept', 0):,} rader "
              f"({sub.get('n_rows', 0)} per tabell, modus: {sub.get('mode','?')}).",
              "step")
            names = sub.get("important_names") or []
            if names:
                w(f"    Viktige tabeller: {', '.join(names[:15])}"
                  + (f" (+{len(names)-15})" if len(names) > 15 else ""), "info")
        w(f"  PII-kolonner: {len(summary['columns'])}, "
          f"LOB-kolonner: {len(summary['lob_columns'])}", "info")
        for col in summary["columns"][:20]:
            ex = col["examples"][0] if col["examples"] else None
            ex_txt = (f"  eks: {ex['before']!r} -> {ex['after']!r}" if ex else "")
            w(f"    • {col['column']} [{col['pii_type']}]{ex_txt}", "info")

    # ── Standalone (pakk ut → anonymiser → pakk ny SIARD) ─────────────────────

    def _process_standalone(self, src_path: Path, dst_path: Path, ctx, w,
                            progress, *, dry_run: bool) -> dict:
        with tempfile.TemporaryDirectory(prefix="anon_") as tmp_str:
            tmp = Path(tmp_str)
            w(f"  Pakker ut til {tmp} ...", "info")
            with zipfile.ZipFile(src_path, "r", allowZip64=True) as zin:
                orig_namelist = zin.namelist()
                zin.extractall(tmp)

            stats = self._process_tree(tmp, ctx, w, progress, dry_run=dry_run)
            if dry_run:
                return stats

            w(f"  Pakker ny SIARD til {dst_path} ...", "info")
            self._repack(tmp, dst_path, orig_namelist, w)
            return stats

    @staticmethod
    def _repack(tmp: Path, dst_path: Path, orig_namelist, w):
        from siard_workflow.core.siard_format import (
            get_zip_compresslevel as _get_lvl,
            get_smart_skip_enabled as _get_skip,
            is_precompressed_bytes as _is_pre,
        )
        level     = _get_lvl()
        smartskip = _get_skip()
        compress  = zipfile.ZIP_STORED if level == 0 else zipfile.ZIP_DEFLATED
        comp_lvl  = level if level > 0 else None
        # Bevar opprinnelig entry-rekkefølge der mulig (metadata/header først)
        on_disk = {str(f.relative_to(tmp)).replace("\\", "/"): f
                   for f in tmp.rglob("*") if f.is_file()}
        ordered = [n for n in orig_namelist if n in on_disk]
        ordered += [n for n in on_disk if n not in set(orig_namelist)]
        with zipfile.ZipFile(dst_path, "w", compression=compress,
                             allowZip64=True, compresslevel=comp_lvl) as zout:
            # Bevar katalogoppføringer fra originalen — særlig den tomme
            # header/siardversion/<v>/ som markerer SIARD-versjonen.
            for name in orig_namelist:
                if name.endswith("/"):
                    zout.writestr(zipfile.ZipInfo(name), b"")
            for arc in ordered:
                f = on_disk[arc]
                if level > 0 and smartskip:
                    try:
                        head = f.open("rb").read(16)
                    except Exception:
                        head = b""
                    if head and _is_pre(head):
                        zout.write(f, arc, compress_type=zipfile.ZIP_STORED)
                        continue
                zout.write(f, arc)

    # ── Rapport ────────────────────────────────────────────────────────────────

    def _write_report(self, ctx, summary: dict, stats: dict, w):
        log_dir = ctx.metadata.get("log_dir")
        if not log_dir:
            return
        try:
            import json
            stem = ctx.siard_path.stem if ctx.siard_path else "siard"
            path = Path(log_dir) / f"{stem}_anonymisering.json"
            report = {
                "summary": summary,
                "stats": stats,
                "mappings": [
                    {"type": t, "original": o, "fake": f}
                    for (t, o, f) in self._mapping.items()],
            }
            path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                            encoding="utf-8")
            w(f"  Anonymiseringsrapport: {path}", "ok")
        except Exception as exc:
            w(f"  Kunne ikke skrive rapport: {exc}", "warn")

    # ── Hjelpere ───────────────────────────────────────────────────────────────

    @staticmethod
    def _summary_msg(stats: dict) -> str:
        if stats.get("dry_run"):
            return (f"Tørkjøring: {stats.get('pii_columns', 0)} PII-kolonner, "
                    f"{stats.get('lob_columns', 0)} LOB-kolonner identifisert")
        msg = (f"{stats.get('cells_anonymized', 0)} celler anonymisert, "
               f"{stats.get('lobs_replaced', 0)} LOB-filer byttet, "
               f"{stats.get('mappings', 0)} unike erstatninger")
        if "subset_rows_kept" in stats:
            orig = stats.get("subset_original_rows", 0)
            msg += (f"  |  datasett redusert til {stats['subset_rows_kept']:,}"
                    + (f" av {orig:,}" if orig else "") + " rader")
        return msg


class _AbortedByUser(Exception):
    """Brukeren avbrøt anonymiseringen i forhåndsvisningen."""
