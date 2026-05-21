"""
siard_workflow/operations/xml_cleaner_operation.py

XmlCleanerOperation — rensning av tekstfelter i tableX.xml-filer.

Tilbyr avskruelig regler for rensing av paddet/redundant innhold i
tableX.xml.  Foreløpig én regel:

  - Padding-spaces: erstatt sekvenser av \\u0020 (SIARD-escaped space)
    med ett mellomrom i midten av tekstfelt, og fjern dem helt på
    slutten av tekstfelt (der de fungerer som padding).

Eksempler (\\u0020 vises i tableX.xml som de literale 6 tegnene \\,u,0,0,2,0):

  Før: <c13>Innkalling\\u0020\\u0020\\u0020\\u0020</c13>
  Etter: <c13>Innkalling</c13>

  Før: <c5>Hello\\u0020World</c5>
  Etter: <c5>Hello World</c5>

  Før: <c5>Hello\\u0020\\u0020\\u0020World</c5>
  Etter: <c5>Hello World</c5>

Operasjonen jobber på rå XML-bytes (regex-erstatning) for ytelse — siden
\\u0020 er ren ASCII kan det ikke kollidere med UTF-8-flerbytetegn eller
attributtinnhold (gyldig SIARD-XML har ikke literal "\\u0020" som
attributtverdi).
"""
from __future__ import annotations

import concurrent.futures
import os
import re
import shutil
import tempfile
import threading
import zipfile
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation


# ── Regex-mønstre ─────────────────────────────────────────────────────────────

# Trailing: ett eller flere   rett før lukke-tag </...>.
# Bruker bytes-regex så vi kan gjøre raskt søk i UTF-8-bytes uten dekoding.
_RE_TRAILING_PADDING = re.compile(rb"(?:\\u0020)+(?=</)")

# Inline: ett eller flere   et annet sted i tekstinnhold.
# Vi fanger generelt — kjøres ETTER trailing-fjerning så det som er igjen
# er kun "internt" innhold.
_RE_INLINE_PADDING = re.compile(rb"(?:\\u0020)+")


def _clean_padding_spaces(xml_bytes: bytes) -> tuple[bytes, int]:
    """
    Rens \\u0020-padding i tekstfelter.
    Returnerer (renset_bytes, antall_erstatninger).

    Strategi:
      1. Fjern trailing-sekvenser (rett før </tag>) helt.
      2. Kollaps gjenværende inline-sekvenser til ett vanlig mellomrom.
    """
    n_trailing = 0
    n_inline   = 0

    def _sub_trailing(_m):
        nonlocal n_trailing
        n_trailing += 1
        return b""

    def _sub_inline(_m):
        nonlocal n_inline
        n_inline += 1
        return b" "

    cleaned = _RE_TRAILING_PADDING.sub(_sub_trailing, xml_bytes)
    cleaned = _RE_INLINE_PADDING.sub(_sub_inline, cleaned)
    return cleaned, n_trailing + n_inline


# ── Filsystem-iterator ────────────────────────────────────────────────────────

def _iter_table_xmls(extract_dir: Path):
    """Yield (relativ_sti_str, absolutt_sti) for alle tableX.xml under content/."""
    content_dir = extract_dir / "content"
    if not content_dir.is_dir():
        return
    for table_xml in content_dir.rglob("*.xml"):
        # Forventet sti: content/{schema}/{table}/{table}.xml
        # (sjekker stem == folder for å unngå XSD-filer eller andre xml-er)
        if table_xml.stem == table_xml.parent.name:
            yield str(table_xml.relative_to(extract_dir)), table_xml


# ── Operasjon ─────────────────────────────────────────────────────────────────

class XmlCleanerOperation(BaseOperation):
    """
    Renser tableX.xml-filer i SIARD-arkiver for paddet/redundant innhold.
    Kjøres etter Pakk ut SIARD og før Pakk sammen SIARD.
    """

    operation_id    = "xml_cleaner"
    label           = "XML-renser"
    category        = "Innhold"
    status          = 2
    produces_siard  = True
    requires_unpack = True

    default_params = {
        "clean_padding_spaces": True,   #  -padding (SIARD-escape) renses
        "dry_run":              False,
    }

    @property
    def description(self) -> str:
        return ("Renser tekstfelter i tableX.xml: fjerner \\u0020-padding "
                "(SIARD-escaped mellomrom) på slutten av tekstfelter og "
                "kollapser inline-sekvenser til ett mellomrom.")

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
        w("  XML-RENSER", "step")
        w("=" * 56)

        clean_padding = bool(self.params.get("clean_padding_spaces", True))
        dry_run       = bool(self.params.get("dry_run", False))

        if not clean_padding:
            w("  Ingen rensereregler aktivert — operasjonen er en no-op.", "info")
            progress("finish", stats={})
            return self._ok({}, "Ingen aktive renseregler")

        stats: dict = {
            "tables_scanned":   0,
            "tables_modified":  0,
            "padding_removed":  0,   # totalt antall  -sekvenser erstattet
            "bytes_saved":      0,
        }

        # ── Pipeline-modus: jobb direkte på utpakket filsystem ───────────────
        extract_dir = getattr(ctx, "extracted_path", None)
        if extract_dir and extract_dir.is_dir():
            w(f"  Pipeline-modus: bruker utpakket mappe {extract_dir}", "info")
            try:
                self._process_filesystem(extract_dir, stats, w, progress,
                                         dry_run=dry_run)
            except Exception as exc:
                import traceback as _tb
                w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
                progress("finish", stats=stats)
                return self._fail(str(exc), stats)

            self._summary(w, stats, dry_run)
            progress("finish", stats=stats)
            self.produces_siard = False
            return self._ok(stats,
                            f"{stats['padding_removed']:,} padding-sekvenser fjernet"
                            f", {stats['bytes_saved']:,} bytes spart")

        # ── Standalone-modus: les inn SIARD, skriv ny SIARD ──────────────────
        self.produces_siard = True
        src_path = ctx.siard_path
        suffix   = "_xml_renset"
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        c = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{c}" + src_path.suffix)
            c += 1

        try:
            self._process_zip(src_path, dst_path, stats, w, progress,
                              dry_run=dry_run)
        except Exception as exc:
            import traceback as _tb
            w(f"  Feil: {exc}\n{_tb.format_exc()}", "feil")
            progress("finish", stats=stats)
            return self._fail(str(exc), stats)

        self._summary(w, stats, dry_run)
        if not dry_run:
            w(f"    Ny SIARD: {dst_path}", "ok")
        progress("finish", stats=stats)
        return self._ok(
            {**stats, "output_path": str(dst_path)},
            f"{stats['padding_removed']:,} padding-sekvenser fjernet"
            f", {stats['bytes_saved']:,} bytes spart")

    # ── Implementasjon ───────────────────────────────────────────────────────

    def _process_filesystem(self, extract_dir: Path, stats: dict,
                            w, progress, *, dry_run: bool) -> None:
        """
        Rens alle tableX.xml in-place i extract_dir, parallellisert.

        Hver tableX.xml prosesseres uavhengig (les bytes → regex → skriv
        bytes), så ThreadPoolExecutor er ideelt — disk-I/O og regex slipper
        begge GIL.
        """
        tables = list(_iter_table_xmls(extract_dir))
        if not tables:
            w("  Ingen tableX.xml-filer funnet under content/.", "info")
            return

        # Antall workers — hent fra global config, cap ved cpu_count og
        # antall filer for å unngå degenerert pool.
        try:
            from settings import get_config
            cfg_workers = int(get_config("max_workers", 4) or 4)
        except Exception:
            cfg_workers = 4
        max_w = max(1, min(cfg_workers, os.cpu_count() or 4, len(tables)))

        progress("phase", phase=1, total_phases=1,
                 label=f"Renser {len(tables)} tableX.xml-filer "
                       f"({max_w} parallelle)")
        w(f"  Starter parallell rensing av {len(tables)} tableX.xml-filer "
          f"på {max_w} worker-tråder ...", "info")

        # Workers leverer per-fil-resultater. Hovedtråden aggregerer stats
        # og logger — slik unngår vi lock-kontensjon på hver write.
        def _process_one(rel_sti: str, abs_path: Path):
            """Returnerer (rel_sti, n_repl, saved, error). 'modified' avledes."""
            try:
                src_bytes = abs_path.read_bytes()
                cleaned, n_repl = _clean_padding_spaces(src_bytes)
                if n_repl == 0:
                    return (rel_sti, 0, 0, None)
                saved = len(src_bytes) - len(cleaned)
                if not dry_run:
                    abs_path.write_bytes(cleaned)
                return (rel_sti, n_repl, saved, None)
            except Exception as exc:
                return (rel_sti, 0, 0, exc)

        n_done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as pool:
            futs = [pool.submit(_process_one, rs, ap) for rs, ap in tables]
            for fut in concurrent.futures.as_completed(futs):
                rel_sti, n_repl, saved, err = fut.result()
                n_done += 1
                stats["tables_scanned"] += 1
                if err is not None:
                    w(f"    [FEIL] {rel_sti}: {err}", "feil")
                elif n_repl > 0:
                    stats["tables_modified"] += 1
                    stats["padding_removed"] += n_repl
                    stats["bytes_saved"]     += saved
                    w(f"    {rel_sti}: {n_repl:,} sekvenser, "
                      f"{saved:,} bytes spart", "info")
                progress("phase_progress", done=n_done, total=len(tables))

        progress("phase_done")

    def _process_zip(self, src_path: Path, dst_path: Path, stats: dict,
                     w, progress, *, dry_run: bool) -> None:
        """Les SIARD-ZIP, skriv ny ZIP med rensede tableX.xml."""
        with zipfile.ZipFile(src_path, "r", allowZip64=True) as zin:
            # Identifiser alle tableX.xml (content/{schema}/{table}/{table}.xml)
            table_xml_names = set()
            for name in zin.namelist():
                parts = name.split("/")
                # content/schemaN/tableM/tableM.xml — siste to like stems
                if (len(parts) >= 3 and parts[0] == "content"
                        and parts[-1].endswith(".xml")):
                    stem_parent = parts[-2]
                    stem_file   = parts[-1][:-len(".xml")]
                    if stem_parent == stem_file:
                        table_xml_names.add(name)

            progress("phase", phase=1, total_phases=1,
                     label=f"Renser {len(table_xml_names)} tableX.xml-filer")

            if dry_run:
                # Bare tell og rapporter — ikke skriv ny SIARD
                for name in table_xml_names:
                    stats["tables_scanned"] += 1
                    src_bytes = zin.read(name)
                    cleaned, n_repl = _clean_padding_spaces(src_bytes)
                    if n_repl == 0:
                        continue
                    stats["tables_modified"] += 1
                    stats["padding_removed"] += n_repl
                    stats["bytes_saved"]     += len(src_bytes) - len(cleaned)
                    w(f"    {name}: {n_repl:,} sekvenser, "
                      f"{len(src_bytes) - len(cleaned):,} bytes (dry-run)",
                      "info")
                progress("phase_done")
                return

            # Skriv ny SIARD med rensede tableX.xml
            tmp_fd, tmp_name = tempfile.mkstemp(suffix=".siard")
            import os as _os
            _os.close(tmp_fd)
            tmp_path = Path(tmp_name)
            try:
                with zipfile.ZipFile(tmp_path, "w",
                                     compression=zipfile.ZIP_DEFLATED,
                                     allowZip64=True) as zout:
                    n_done = 0
                    for info in zin.infolist():
                        data = zin.read(info.filename)
                        if info.filename in table_xml_names:
                            stats["tables_scanned"] += 1
                            cleaned, n_repl = _clean_padding_spaces(data)
                            if n_repl > 0:
                                stats["tables_modified"] += 1
                                stats["padding_removed"] += n_repl
                                stats["bytes_saved"]     += len(data) - len(cleaned)
                                w(f"    {info.filename}: {n_repl:,} sekvenser, "
                                  f"{len(data) - len(cleaned):,} bytes spart",
                                  "info")
                                data = cleaned
                            n_done += 1
                            progress("phase_progress",
                                     done=n_done, total=len(table_xml_names))
                        # Skriv (med original compress_type for å bevare format)
                        zout.writestr(info, data)

                shutil.move(str(tmp_path), str(dst_path))
            finally:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass

            progress("phase_done")

    # ── Oppsummering ─────────────────────────────────────────────────────────

    def _summary(self, w, stats: dict, dry_run: bool) -> None:
        w("  OPPSUMMERING:", "step")
        labels = {
            "tables_scanned":  "Tabeller skannet",
            "tables_modified": "Tabeller endret",
            "padding_removed": "\\u0020-sekvenser fjernet",
            "bytes_saved":     "Bytes spart",
        }
        for k in ("tables_scanned", "tables_modified",
                  "padding_removed", "bytes_saved"):
            v = stats.get(k, 0)
            w(f"    {labels[k]:<28} {v:,}", "info")
        if dry_run:
            w("    (TØRKJØRING — ingen filer skrevet)", "warn")
        w("=" * 56)
