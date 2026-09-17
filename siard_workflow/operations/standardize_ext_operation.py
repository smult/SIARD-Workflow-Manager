"""siard_workflow/operations/standardize_ext_operation.py

StandardizeExtOperation
-----------------------
Omdøper alle LOB-filer i SIARD-strukturen som har ikke-standard filendelser
til .bin og oppdaterer tilhørende XML-referanser.

En "standard" endelse i SIARD-sammenheng er .bin (eller .txt for tekst-LOBs).
Filer som f.eks. heter record001.doc.pdf (etter ekstern konvertering) omdøpes
til record001.bin — stammen er alt frem til første punktum.

XML-kommentar legges på linjen: <!-- Filendelse endret fra .doc.pdf til .bin -->

Filnavn-standardisering (innstilling 'dbptk_lob_names', default på):
DBPTK-validatoren (P_4.2-3) krever at LOB-filer heter record[0-9]+.*.
Kjente stammer (recN, xrecN, LOBnnnn, lobN) omdøpes til recordN med uendret
endelse; ukjente stammer røres ikke. Ved navnekollisjon i samme lob-mappe
beholdes originalnavnet og det logges en advarsel. Navnebytte får ingen
XML-kommentar per linje (ville doblet størrelsen på store tabeller).

Ytelse (uttrekk med flere hundre tusen LOB-filer):
  * Katalogskanning med os.scandir som IKKE går inn i lob-mappene før
    planleggingen — et rglob over hele content/ stat'er hver eneste fil.
  * Omdøpingen planlegges per lob-mappe (kollisjonssjekk mot navn som finnes
    og navn som er planlagt) og utføres deretter i en tråd-pool: os.rename
    slipper GIL, så filsystemkallene overlapper.
  * XML-patchingen slår opp kommentaren for treffet direkte — den gamle
    varianten løp gjennom hele rename-kartet for hver treff-linje, som med
    N filer og M referanser ga N×M delstreng-søk (timer på store uttrekk).
  * Fremdrift rapporteres per fase (skanning, omdøping, XML) med bestemt
    progressbar, og per-fil-logging begrenses til de første linjene.

Gjør ingenting hvis både 'standardize_bin_ext' og 'dbptk_lob_names' er
False i globale innstillinger.
"""

from __future__ import annotations
import os
import re
import tempfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.lob_naming import (
    dbptk_names_enabled, dbptk_file_name, is_dbptk_lob_name,
)


_FILE_ATTR_RE = re.compile(
    rb'((?:file|fileName|href)=["\'])([^"\']+)(["\'])')
_LOB_DIR_RE = re.compile(r"lob\d+", re.IGNORECASE)

# Maks antall per-fil-linjer som logges (omdøpinger, kollisjoner, feil).
# Resten oppsummeres — 240 000 logglinjer gir ingen informasjon og kveler GUI-en.
_LOG_CAP = 40
# Antall omdøpinger per tråd-oppgave (færre futures, jevnere fremdrift)
_RENAME_CHUNK = 500
# Fremdrift meldes høyst så ofte (sekunder)
_PROGRESS_EVERY = 0.25


def _is_standard_lob_name(name: str) -> bool:
    """Returnerer True hvis filen allerede er .bin eller .txt (enkelt endelse)."""
    low = name.lower()
    return low.endswith(".bin") or low.endswith(".txt")


def _to_bin_name(name: str) -> str:
    """Konverter filnavn til <rotstamme>.bin — fjerner alle endelser."""
    return name.split(".")[0] + ".bin"


def _old_exts(name: str) -> str:
    """Returnerer endelse-del inkl. punktum, f.eks. '.doc.pdf' eller '.txt.rtf.pdf'."""
    parts = name.split(".", 1)
    return "." + parts[1] if len(parts) > 1 else ""


def _append_xml_comment_to_line(line: bytes, comment: str) -> bytes:
    """Legg XML-kommentar etter node-innholdet, før linjeskift."""
    stripped = line.rstrip(b"\r\n")
    newline  = line[len(stripped):]
    return stripped + f" <!-- {comment} -->".encode("utf-8") + newline


def _patch_xml_line(line: bytes,
                    rename_map_bytes: dict[bytes, tuple[bytes, str]]) -> bytes:
    """
    Erstatt filnavn-referanser i én XML-linje og legg til kommentar.
    rename_map_bytes: {old_basename_bytes: (new_basename_bytes, comment_str)}

    Kommentaren hentes fra det treffet som faktisk ble erstattet — O(1),
    uavhengig av størrelsen på rename-kartet.
    """
    comment_hit: list[str] = []

    def _replace(m: re.Match) -> bytes:
        attr_open  = m.group(1)
        ref        = m.group(2)
        attr_close = m.group(3)

        if b"/" in ref:
            dir_part, base_part = ref.rsplit(b"/", 1)
            dir_part += b"/"
        else:
            dir_part, base_part = b"", ref

        entry = rename_map_bytes.get(base_part)
        if entry is None:
            return m.group(0)
        new_base, comment = entry
        if comment and not comment_hit:
            comment_hit.append(comment)
        return attr_open + dir_part + new_base + attr_close

    new_line = _FILE_ATTR_RE.sub(_replace, line)
    if new_line == line:
        return line
    if comment_hit:
        new_line = _append_xml_comment_to_line(new_line, comment_hit[0])
    return new_line


def _worker_count() -> int:
    """Antall tråder for filsystemarbeid. Config 'max_workers' hvis satt, ellers auto."""
    try:
        from settings import get_config
        n = int(get_config("max_workers", 0) or 0)
    except Exception:
        n = 0
    if n <= 0:
        n = min(16, (os.cpu_count() or 4) * 2)
    return max(1, n)


def scan_content_tree(content_dir: Path) -> tuple[list[Path], list[Path]]:
    """
    Finn (lob-mapper, XML-filer utenfor lob-mapper) under content/.

    Bruker os.scandir og går IKKE inn i lob-mappene — det er der de
    hundretusener av filene ligger. metadata.xml hoppes over.
    """
    lob_dirs: list[Path] = []
    xml_files: list[Path] = []
    stack = [content_dir]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            if _LOB_DIR_RE.fullmatch(e.name):
                                lob_dirs.append(Path(e.path))
                            else:
                                stack.append(Path(e.path))
                        elif e.is_file(follow_symlinks=False):
                            low = e.name.lower()
                            if low.endswith(".xml") and low != "metadata.xml":
                                xml_files.append(Path(e.path))
                    except OSError:
                        continue
        except OSError:
            continue
    lob_dirs.sort()
    xml_files.sort()
    return lob_dirs, xml_files


def _plan_lob_dir(lob_dir: Path, ext_on: bool, names_on: bool
                  ) -> tuple[list[tuple[str, str, str, bool, bool]],
                             list[tuple[str, str]]]:
    """
    Planlegg omdøpinger i én lob-mappe.

    Returnerer (jobs, collisions) der job = (old, new, comment, ext_changed,
    name_changed). Kollisjonssjekken tar hensyn både til navn som finnes fra
    før og navn andre filer i mappa er planlagt omdøpt til. Et navn som
    finnes fra før regnes som opptatt selv om den filen selv skal omdøpes —
    da er ingen omdøping avhengig av rekkefølgen, og de kan kjøres parallelt.
    """
    try:
        with os.scandir(lob_dir) as it:
            names = [e.name for e in it if e.is_file(follow_symlinks=False)]
    except OSError:
        return [], []
    names.sort()
    taken = set(names)
    jobs: list[tuple[str, str, str, bool, bool]] = []
    collisions: list[tuple[str, str]] = []

    for old in names:
        new, comment, ext_changed = old, "", False
        if ext_on and not _is_standard_lob_name(old):
            new         = _to_bin_name(old)
            comment     = f"Filendelse endret fra {_old_exts(old)} til .bin"
            ext_changed = True
        name_changed = False
        if names_on and not is_dbptk_lob_name(new):
            cand = dbptk_file_name(new)
            if cand is not None:
                new          = cand
                name_changed = True
        if new == old:
            continue
        if new in taken:
            collisions.append((old, new))
            continue
        taken.add(new)
        jobs.append((old, new, comment, ext_changed, name_changed))
    return jobs, collisions


def _rename_chunk(chunk: list[tuple[Path, str, str]]
                  ) -> tuple[list[tuple[Path, str, str]], list[tuple[str, str]]]:
    """Utfør en bunt omdøpinger. Returnerer (vellykkede, [(old, feilmelding)])."""
    ok: list[tuple[Path, str, str]] = []
    errors: list[tuple[str, str]] = []
    for lob_dir, old, new in chunk:
        try:
            os.rename(lob_dir / old, lob_dir / new)
            ok.append((lob_dir, old, new))
        except OSError as exc:
            errors.append((old, str(exc)))
    return ok, errors


class _PhaseProgress:
    """Trådsikker, tidsstyrt fremdriftsmelding for én fase."""

    def __init__(self, progress, total: int):
        self._progress = progress
        self._total    = max(0, int(total))
        self._done     = 0
        self._last     = 0.0
        self._lock     = threading.Lock()
        self._progress("phase_progress", done=0, total=self._total)

    def add(self, n: int = 1, force: bool = False) -> None:
        with self._lock:
            self._done += n
            now = time.monotonic()
            if (force or self._done >= self._total
                    or now - self._last >= _PROGRESS_EVERY):
                self._last = now
                self._progress("phase_progress",
                               done=min(self._done, self._total) if self._total
                               else self._done,
                               total=self._total)


class StandardizeExtOperation(BaseOperation):
    """
    Standardiser alle LOB-filendelser til .bin i SIARD-strukturen.

    Gjennomgår alle lob-mapper og omdøper filer som ikke allerede heter
    <stamme>.bin (eller <stamme>.txt) til <stamme>.bin. Oppdaterer
    tilhørende XML-referanser i tableX.xml-filer med en XML-kommentar.
    Med 'dbptk_lob_names' standardiseres også stammen til recordN
    (krav i DBPTK-validatoren P_4.2-3).

    Støtter pipeline-modus (utpakket mappe) og standalone-modus.
    Gjør ingenting hvis både 'standardize_bin_ext' og 'dbptk_lob_names'
    er False i innstillinger.
    """

    operation_id   = "standardize_ext"
    label          = "Standardiser filendelser"
    category       = "Kompatibilitet"
    status         = 2
    produces_siard = False
    modifies_content = True
    premis_event_type  = "Adjustment"
    premis_event_label = "filnavn-standardisering"

    default_params: dict = {
        "output_suffix": "_stdext",
    }

    # Faser i _run_on_dir (pipeline). Standalone legger til utpakking og
    # pakking foran/bak.
    _DIR_PHASES = 3

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and (
            result.data.get("renamed", 0) > 0
            or result.data.get("renamed_names", 0) > 0)

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        def progress(event, **kw):
            if pcb:
                pcb(event, **kw)

        w("=" * 56)
        w("  STANDARDISER FILENDELSER", "step")
        w("=" * 56)

        # Sjekk innstillinger
        try:
            from settings import get_config
            ext_on = bool(get_config("standardize_bin_ext", True))
        except Exception:
            ext_on = True
        names_on = dbptk_names_enabled()
        if not ext_on and not names_on:
            w("  'Standardiser .bin' og 'Standardiser LOB-filnavn' er begge "
              "deaktivert i innstillinger — ingen endringer.", "info")
            return self._ok({}, "Deaktivert i innstillinger — ingen endringer")
        self._ext_on   = ext_on
        self._names_on = names_on
        w(f"  Filendelse → .bin: {'på' if ext_on else 'av'}   "
          f"Filnavn → recordN: {'på' if names_on else 'av'}   "
          f"Tråder: {_worker_count()}", "info")

        stop_ev  = ctx.metadata.get("stop_event",  threading.Event())

        # Pipeline-modus: utpakket mappe finnes allerede
        pre_dir = getattr(ctx, "extracted_path", None)
        if pre_dir is not None and pre_dir.is_dir():
            self.produces_siard = False
            t0 = time.monotonic()
            stats = self._run_on_dir(pre_dir, w, progress, stop_ev,
                                     phase_offset=0,
                                     total_phases=self._DIR_PHASES)
            msg = self._summary(stats)
            w(f"  Ferdig (pipeline, {time.monotonic() - t0:.1f} s): {msg}", "ok")
            return self._ok(stats, msg)

        # Standalone-modus: pakk ut, prosesser, repakk
        self.produces_siard = True
        src_path = ctx.siard_path
        suffix   = self.params.get("output_suffix", "_stdext")
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        PHASES   = self._DIR_PHASES + 2

        def phase(n, label):
            progress("phase", phase=n, total_phases=PHASES, label=label)

        # Sjekk at vi kan skrive til destinasjon
        try:
            with open(dst_path, "ab"):
                pass
            if dst_path.exists() and dst_path.stat().st_size == 0:
                try:
                    dst_path.unlink()
                except Exception:
                    pass
        except (PermissionError, OSError):
            return self._fail(f"Kan ikke skrive til: {dst_path}")

        td = ctx.metadata.get("temp_dir", "").strip() if hasattr(ctx, "metadata") else ""
        temp_root = Path(td) if td else src_path.parent
        t0 = time.monotonic()

        with tempfile.TemporaryDirectory(dir=temp_root,
                                         prefix="siard_stdext_") as _tmpdir:
            tmpdir = Path(_tmpdir)
            extract_dir = tmpdir / "extracted"
            extract_dir.mkdir()

            # ── Fase 1: Pakk ut ───────────────────────────────────────────────
            phase(1, "Pakker ut SIARD")
            w(f"  Pakker ut {src_path.name} ...", "info")
            try:
                with zipfile.ZipFile(src_path, "r", allowZip64=True) as zf:
                    orig_namelist = zf.namelist()
                    pp = _PhaseProgress(progress, len(orig_namelist))
                    for name in orig_namelist:
                        if stop_ev.is_set():
                            return self._fail("Stoppet av bruker")
                        zf.extract(name, extract_dir)
                        pp.add()
            except Exception as exc:
                return self._fail(f"Kan ikke pakke ut SIARD: {exc}")
            progress("phase_done")

            stats = self._run_on_dir(extract_dir, w, progress, stop_ev,
                                     phase_offset=1, total_phases=PHASES)
            if stop_ev.is_set():
                return self._fail("Stoppet av bruker", stats)

            # ── Siste fase: Repakk ────────────────────────────────────────────
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

            def _write_smart(zf, src_path, arc):
                if _level > 0 and _smartskip:
                    try:
                        with src_path.open("rb") as fh:
                            head = fh.read(16)
                    except Exception:
                        head = b""
                    if head and _is_pre(head):
                        zf.write(src_path, arc,
                                 compress_type=zipfile.ZIP_STORED)
                        return
                zf.write(src_path, arc)

            phase(PHASES, "Pakker ny SIARD")
            w(f"  Pakker ny SIARD: {dst_path.name} (kompresjon nivå "
              f"{_level}) ...", "info")
            try:
                orig_set = set(orig_namelist)
                # Filer som fikk nytt navn under prosessering
                new_files = [
                    f for f in extract_dir.rglob("*")
                    if f.is_file()
                    and str(f.relative_to(extract_dir)).replace("\\", "/")
                    not in orig_set]
                pp = _PhaseProgress(progress, len(orig_namelist) + len(new_files))
                with zipfile.ZipFile(dst_path, "w", _compress,
                                     allowZip64=True,
                                     compresslevel=_comp_lvl) as zf_out:
                    for orig_name in orig_namelist:
                        if stop_ev.is_set():
                            raise RuntimeError("Stoppet av bruker")
                        orig_p = extract_dir / orig_name
                        if orig_p.is_dir():
                            zf_out.writestr(
                                zipfile.ZipInfo(orig_name + "/"), b"")
                        elif orig_p.exists():
                            _write_smart(zf_out, orig_p, orig_name)
                        pp.add()
                    for f in new_files:
                        if stop_ev.is_set():
                            raise RuntimeError("Stoppet av bruker")
                        arc = str(f.relative_to(extract_dir)).replace("\\", "/")
                        _write_smart(zf_out, f, arc)
                        pp.add()
            except Exception as exc:
                dst_path.unlink(missing_ok=True)
                return self._fail(f"Pakking feilet: {exc}")
            progress("phase_done")

        ctx.siard_path = dst_path
        msg = f"{self._summary(stats)} → {dst_path.name}"
        w(f"  Ferdig ({time.monotonic() - t0:.1f} s): {msg}", "ok")
        return self._ok({**stats, "output_path": str(dst_path)}, msg)

    @staticmethod
    def _summary(stats: dict) -> str:
        parts = [f"{stats.get('renamed', 0)} fil(er) fikk .bin-endelse",
                 f"{stats.get('renamed_names', 0)} fil(er) omdøpt til recordN",
                 f"{stats.get('xml_updated', 0)} XML-referanser oppdatert"]
        if stats.get("name_collisions"):
            parts.append(f"{stats['name_collisions']} navnekollisjon(er) hoppet over")
        if stats.get("rename_errors"):
            parts.append(f"{stats['rename_errors']} omdøpingsfeil")
        return ", ".join(parts)

    def _run_on_dir(self, extract_dir: Path, w, progress, stop_ev,
                    phase_offset: int = 0, total_phases: int = 3) -> dict:
        """
        Kjør omdøping og XML-patching på utpakket katalog i tre faser:
          1. skann lob-mapper og planlegg omdøpinger (kollisjonssjekk)
          2. utfør omdøpingene i tråd-pool
          3. patch XML-referanser
        """
        stats = {"renamed": 0, "renamed_names": 0, "xml_updated": 0,
                 "name_collisions": 0}
        ext_on   = getattr(self, "_ext_on", True)
        names_on = getattr(self, "_names_on", dbptk_names_enabled())

        def phase(n, label):
            progress("phase", phase=phase_offset + n,
                     total_phases=total_phases, label=label)

        content_dir = extract_dir / "content"
        if not content_dir.exists():
            w("  Ingen content/-mappe funnet — ingenting å gjøre.", "info")
            for n in range(1, self._DIR_PHASES + 1):
                phase(n, "—")
                progress("phase_done")
            return stats

        # ── Fase 1: Skann og planlegg ─────────────────────────────────────────
        phase(1, "Skanner LOB-mapper")
        t0 = time.monotonic()
        lob_dirs, xml_files = scan_content_tree(content_dir)
        w(f"  {len(lob_dirs):,} lob-mapper og {len(xml_files):,} tabell-XML "
          f"funnet ({time.monotonic() - t0:.1f} s)", "info")

        jobs: list[tuple[Path, str, str]] = []            # (lob_dir, old, new)
        global_rename: dict[str, tuple[str, str]] = {}    # old → (new, comment)
        n_files = n_ext = n_name = 0
        n_logged = 0
        pp = _PhaseProgress(progress, len(lob_dirs))
        for lob_dir in lob_dirs:
            if stop_ev.is_set():
                break
            dir_jobs, collisions = _plan_lob_dir(lob_dir, ext_on, names_on)
            for old, new in collisions:
                stats["name_collisions"] += 1
                if n_logged < _LOG_CAP:
                    n_logged += 1
                    w(f"  ADVARSEL: {lob_dir.parent.name}/{lob_dir.name}/{old} "
                      f"→ {new} finnes allerede — beholder originalnavn", "warn")
            for old, new, comment, ext_changed, name_changed in dir_jobs:
                jobs.append((lob_dir, old, new))
                global_rename[old] = (new, comment)
                n_ext  += ext_changed
                n_name += name_changed
            pp.add()
        if stats["name_collisions"] > n_logged:
            w(f"  ... og {stats['name_collisions'] - n_logged:,} "
              f"navnekollisjon(er) til (ikke listet)", "warn")
        progress("phase_done")

        if stop_ev.is_set():
            return stats
        if not jobs:
            w("  Ingen filer trengte standardisering.", "info")
            for n in (2, 3):
                phase(n, "—")
                progress("phase_done")
            return stats

        w(f"  {len(jobs):,} fil(er) skal omdøpes "
          f"({n_ext:,} endelse → .bin, {n_name:,} navn → recordN)", "info")

        # ── Fase 2: Omdøp i tråd-pool ─────────────────────────────────────────
        phase(2, "Omdøper filer")
        t0 = time.monotonic()
        stats["rename_errors"] = 0
        renamed_ok: set[tuple[Path, str]] = set()
        pp = _PhaseProgress(progress, len(jobs))
        chunks = [jobs[i:i + _RENAME_CHUNK]
                  for i in range(0, len(jobs), _RENAME_CHUNK)]
        n_logged = 0
        with ThreadPoolExecutor(max_workers=_worker_count()) as pool:
            futures = []
            for ch in chunks:
                if stop_ev.is_set():
                    break
                futures.append(pool.submit(_rename_chunk, ch))
            for fut in as_completed(futures):
                ok, errors = fut.result()
                for lob_dir, old, new in ok:
                    renamed_ok.add((lob_dir, old))
                for old, err in errors:
                    stats["rename_errors"] += 1
                    if n_logged < _LOG_CAP:
                        n_logged += 1
                        w(f"  FEIL omdøp {old}: {err}", "feil")
                pp.add(len(ok) + len(errors))
        if stats["rename_errors"] > n_logged:
            w(f"  ... og {stats['rename_errors'] - n_logged:,} omdøpingsfeil til",
              "feil")

        # Tell opp og fjern mislykkede fra XML-kartet. Kartet er per basenavn:
        # feilet én fil, tas navnet ut kun hvis ingen annen fil med samme
        # basenavn lyktes (samme basenavn finnes typisk i mange lob-mapper).
        succeeded_names: set[str] = set()
        n_logged = 0
        for lob_dir, old, new in jobs:
            if (lob_dir, old) not in renamed_ok:
                continue
            succeeded_names.add(old)
            _new, comment = global_rename[old]
            ext_changed = bool(comment)
            if ext_changed:
                stats["renamed"] += 1
                if n_logged < _LOG_CAP:
                    n_logged += 1
                    w(f"  {old} → {new}", "info")
            # navnebytte: enten hele endringen, eller i tillegg til endelsen
            if new.split(".", 1)[0] != old.split(".", 1)[0]:
                stats["renamed_names"] += 1
        if stats["renamed"] > n_logged:
            w(f"  ... og {stats['renamed'] - n_logged:,} endelsesbytte(r) til "
              f"(ikke listet)", "info")
        for old in list(global_rename):
            if old not in succeeded_names:
                del global_rename[old]
        w(f"  {stats['renamed']:,} fil(er) fikk .bin-endelse, "
          f"{stats['renamed_names']:,} fil(er) omdøpt til recordN "
          f"({time.monotonic() - t0:.1f} s)", "ok")
        progress("phase_done")

        if stop_ev.is_set() or not global_rename:
            phase(3, "—")
            progress("phase_done")
            return stats

        # ── Fase 3: Patch tableX.xml ──────────────────────────────────────────
        phase(3, "Oppdaterer XML-referanser")
        t0 = time.monotonic()
        rename_map_bytes: dict[bytes, tuple[bytes, str]] = {
            k.encode("utf-8"): (v.encode("utf-8"), c)
            for k, (v, c) in global_rename.items()
        }
        sizes = {}
        for xf in xml_files:
            try:
                sizes[xf] = xf.stat().st_size
            except OSError:
                sizes[xf] = 0
        # Største først — gir jevnest utnyttelse av trådene
        xml_sorted = sorted(xml_files, key=lambda p: -sizes[p])
        pp = _PhaseProgress(progress, sum(sizes.values()))
        lock = threading.Lock()
        n_logged = 0

        def _one(xml_file: Path) -> int:
            if stop_ev.is_set():
                return 0
            return self._patch_xml_file(xml_file, rename_map_bytes, w,
                                        on_bytes=pp.add)

        # I/O overlapper mellom tråder; selve regex-arbeidet er GIL-bundet,
        # så gevinsten er størst med mange tabeller på treg disk.
        with ThreadPoolExecutor(max_workers=min(4, _worker_count())) as pool:
            for xml_file, n in zip(xml_sorted, pool.map(_one, xml_sorted)):
                with lock:
                    stats["xml_updated"] += n
                    if n and n_logged < _LOG_CAP:
                        n_logged += 1
                        w(f"  {xml_file.parent.name}/{xml_file.name}: "
                          f"{n:,} referanse(r)", "info")
        w(f"  {stats['xml_updated']:,} XML-referanser oppdatert "
          f"({time.monotonic() - t0:.1f} s).", "ok")
        progress("phase_done")
        return stats

    @staticmethod
    def _patch_xml_file(xml_file: Path,
                        rename_map_bytes: dict[bytes, tuple[bytes, str]],
                        w, on_bytes=None) -> int:
        """
        Patch én tableX.xml-fil linje for linje. Returnerer antall oppdateringer.
        on_bytes(n) kalles med antall leste bytes for fremdrift (ca. hver 8 MB).
        """
        tmp = xml_file.with_suffix(".tmp_stdext")
        updates = 0
        pending = 0
        REPORT  = 8 * 1024 * 1024
        try:
            with open(xml_file, "rb", buffering=1024 * 1024) as src, \
                 open(tmp, "wb", buffering=1024 * 1024) as dst:
                for line in src:
                    if b"file" in line or b"href" in line:
                        new_line = _patch_xml_line(line, rename_map_bytes)
                        if new_line is not line and new_line != line:
                            updates += 1
                            line = new_line
                    dst.write(line)
                    if on_bytes is not None:
                        pending += len(line)
                        if pending >= REPORT:
                            on_bytes(pending)
                            pending = 0
            if on_bytes is not None and pending:
                on_bytes(pending)
            tmp.replace(xml_file)
        except Exception as exc:
            tmp.unlink(missing_ok=True)
            w(f"  FEIL patch {xml_file.name}: {exc}", "feil")
            return 0
        return updates

    def count_non_standard_lob_files(self, extract_dir: Path) -> int:
        """Teller LOB-filer med ikke-standard endelse (ikke .bin/.txt)."""
        return count_non_standard_lob_files(extract_dir)[0]


def count_non_standard_lob_files(extract_dir: Path) -> tuple[int, int]:
    """
    (antall LOB-filer uten .bin/.txt-endelse,
     antall LOB-filer som ikke heter recordN.* men kan omdøpes til det)

    Skanner lob-mappene med os.scandir uten å stat'e hver fil.
    """
    n_ext = n_name = 0
    content = extract_dir / "content"
    if not content.exists():
        return 0, 0
    lob_dirs, _ = scan_content_tree(content)
    for lob_dir in lob_dirs:
        try:
            with os.scandir(lob_dir) as it:
                for e in it:
                    if not e.is_file(follow_symlinks=False):
                        continue
                    name = e.name
                    if not _is_standard_lob_name(name):
                        n_ext += 1
                    if not is_dbptk_lob_name(name) and dbptk_file_name(name):
                        n_name += 1
        except OSError:
            continue
    return n_ext, n_name
