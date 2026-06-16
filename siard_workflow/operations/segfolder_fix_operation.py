"""siard_workflow/operations/segfolder_fix_operation.py

SegFolderFixOperation
---------------------
Fjerner SIARD 2.2 LOB-segmentering (seg_N-mapper) slik at uttrekket blir
gyldig SIARD 2.1.

Bakgrunn
========
SIARD 2.2 innførte segmentering av LOB-mapper (kapittel 8 «Scalability
issues» i eCH-0165 v2.2): når antall filer eller samlet størrelse i en
LOB-mappe overstiger en implementasjonsavhengig grense, fordeles LOB-ene i
undermapper kalt ``seg_0``, ``seg_1``, ``seg_2`` osv. Segment-leddet legges
inn i ``file``-attributtet i tableX.xml — relativt til kolonnens
``lobFolder``:

    Northwind_lobs/s0_t2_c4/seg_0/t2_c4_r1.bin
    Northwind_lobs/s0_t2_c4/seg_1/t2_c4_r5.bin
    Northwind_lobs/s0_t2_c4/seg_2/t2_c4_r8.bin

    <c4 file="seg_0/t2_c4_r1.bin" length="10151" .../>
    <c4 file="seg_1/t2_c4_r5.bin" length="12131" .../>

SIARD 2.1 har ingen segmentering — alle LOB-filer ligger direkte i
kolonnemappen og refereres uten seg-ledd:

    <c4 file="t2_c4_r1.bin" length="10151" .../>

Når mål-versjonen er 2.1 må derfor seg_N-leddet fjernes:
  1. Alle filer i ``.../<kolonne>/seg_N/`` flyttes opp til ``.../<kolonne>/``
  2. De (nå tomme) seg_N-mappene slettes
  3. tableX.xml patches: ``seg_N/``-leddet strippes fra ``file=``/``href=``

Merknad om binær-splitting (eCH-0165 v2.2, 8.1.1): svært store LOB-er kan
være delt i ``_part001``, ``_part002`` … Dette er også en 2.2-funksjon som
ikke automatisk settes sammen her — operasjonen advarer hvis slike filer
oppdages, men flytter dem fortsatt ut av seg-mappen.
"""

from __future__ import annotations

import re
import shutil
import tempfile
import threading
import zipfile
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


# En seg-mappe heter seg_0, seg_1 … (eCH-0165 v2.2). Vi tolererer også den
# underscore-løse varianten seg0 for robusthet mot tredjepartsverktøy.
_SEG_NAME_RE = re.compile(r"^seg_?\d+$", re.IGNORECASE)
_SEG_NAME_RE_B = re.compile(rb"^seg_?\d+$", re.IGNORECASE)

# file=/href=/fileName= attributter i tableX.xml som peker på LOB-filer.
_FILE_ATTR_RE = re.compile(rb'((?:file|fileName|href)\s*=\s*["\'])([^"\']*)(["\'])')

# Splittede LOB-er (binær-deling) — kun til varsling.
_PART_SUFFIX_RE = re.compile(r"_part\d+$", re.IGNORECASE)


# ── Sti-hjelpere ──────────────────────────────────────────────────────────────

def _strip_seg_from_path(path: bytes) -> bytes:
    """Fjern alle seg_N-ledd fra en relativ sti (bytes). Bevarer øvrige ledd."""
    parts = path.split(b"/")
    kept = [p for p in parts if not _SEG_NAME_RE_B.match(p)]
    return b"/".join(kept)


# ── Offentlig hjelpefunksjon (brukes også av GUI-preflighten) ─────────────────

def scan_segfolder_issues(siard_path: Path) -> list[str]:
    """
    Skann SIARD-filen for SIARD 2.2 LOB-segmentering (seg_N-mapper).
    Returnerer en liste med lesbar beskrivelse av funnene, eller tom liste
    hvis ingen segmentering finnes (eller ved feil under lesing).
    """
    try:
        with zipfile.ZipFile(siard_path, "r") as zf:
            names = zf.namelist()
    except Exception:
        return []

    seg_dirs: set[str] = set()
    part_files = 0
    for name in names:
        comps = name.split("/")
        for i, c in enumerate(comps):
            if _SEG_NAME_RE.match(c):
                seg_dirs.add("/".join(comps[: i + 1]))
        base = comps[-1]
        if base and _PART_SUFFIX_RE.search(base):
            part_files += 1

    if not seg_dirs:
        return []

    issues = [
        f"LOB-segmentering (SIARD 2.2): {len(seg_dirs)} seg-mappe(r) funnet "
        f"— ikke gyldig i SIARD 2.1"
    ]
    if part_files:
        issues.append(
            f"Binær-splittede LOB-er (_partNNN): {part_files} fil(er) — "
            f"settes ikke automatisk sammen")
    return issues


# ── Operasjonsklassen ─────────────────────────────────────────────────────────

class SegFolderFixOperation(BaseOperation):
    """
    Fjerner SIARD 2.2 LOB-segmentering (seg_N-mapper) for å produsere gyldig
    SIARD 2.1.

    Kjører kun når mål-versjonen (siard_output_version) er 2.1 — SIARD 2.2
    tillater segmentering, så da er operasjonen et trygt no-op.

    Pipeline-modus (ctx.extracted_path satt):
        Flytter LOB-filer ut av seg_N-mapper på disk og patcher tableX.xml.
        Produserer ingen ny SIARD — RepackSiardOperation tar seg av det.

    Standalone-modus:
        Pakker ut, retter, og skriver korrigert kopi til <original>_segfix.siard.
    """

    operation_id   = "segfolder_fix"
    label          = "Fjern LOB-segmentering (SIARD 2.2→2.1)"
    description    = (
        "Fjerner SIARD 2.2 LOB-segmentering (seg_N-mapper) som ikke støttes av "
        "SIARD 2.1: flytter LOB-filer opp ut av seg-mappene og patcher "
        "file=-referansene i tableX.xml. Kjører kun når mål-versjon er 2.1."
    )
    category       = "Kompatibilitet"
    status         = 2
    produces_siard = False
    modifies_content = True
    premis_event_type = "LOB-segmentering fjernet"
    default_params: dict = {
        "output_suffix": "_segfix",
    }

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and result.data.get("files_moved", 0) > 0

    def premis_detail(self, result, ctx) -> str:
        d = result.data
        return (f"{d.get('files_moved', 0)} LOB-fil(er) flyttet ut av "
                f"{d.get('seg_removed', 0)} seg-mappe(r); "
                f"{d.get('xml_updated', 0)} XML-referanser oppdatert "
                f"(SIARD 2.2-segmentering fjernet for 2.1-kompatibilitet)")

    # ── Hovedinngang ─────────────────────────────────────────────────────────

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        # Kun relevant ved nedgradering til 2.1. SIARD 2.2 tillater seg-mapper.
        try:
            from siard_workflow.core.siard_format import get_target_siard_version
            target = get_target_siard_version()
        except Exception:
            target = "2.1"
        if target != "2.1":
            w(f"  Mål-versjon er {target} — SIARD 2.2 tillater segmentering, "
              f"ingen endring.", "info")
            return self._ok({"files_moved": 0, "seg_removed": 0,
                             "xml_updated": 0},
                            f"Mål-versjon {target} — segmentering beholdt")

        stop_ev = ctx.metadata.get("stop_event", threading.Event())

        # ── Pipeline-modus ────────────────────────────────────────────────────
        if ctx.extracted_path and ctx.extracted_path.is_dir():
            self.produces_siard = False
            stats = self._run_on_dir(ctx.extracted_path, w, stop_ev)
            if stats["files_moved"] == 0 and stats["seg_removed"] == 0:
                return self._ok(stats, "Ingen LOB-segmentering funnet")
            msg = (f"{stats['files_moved']} fil(er) flyttet ut av "
                   f"{stats['seg_removed']} seg-mappe(r), "
                   f"{stats['xml_updated']} XML-referanser oppdatert")
            w(f"  Ferdig (pipeline): {msg}", "ok")
            return self._ok(stats, msg)

        # ── Standalone-modus ──────────────────────────────────────────────────
        return self._run_standalone(ctx, w, stop_ev)

    # ── Disk-prosessering (delt av begge modi) ────────────────────────────────

    def _run_on_dir(self, extract_dir: Path, w, stop_ev) -> dict:
        """Flytt LOB-filer ut av seg-mapper og patch tableX.xml i utpakket mappe."""
        stats = {"files_moved": 0, "seg_removed": 0, "xml_updated": 0,
                 "collisions": 0, "part_files": 0}

        content_dir = extract_dir / "content"
        # Segmentering kan i prinsippet ligge både under content/ og i en
        # ekstern *_lobs-mappe som er pakket inn i arkivet. Skann hele treet.
        search_root = content_dir if content_dir.exists() else extract_dir

        # ── Steg 1: Finn alle seg-mapper (dypest først, så indre tømmes før ytre)
        seg_dirs = sorted(
            (d for d in search_root.rglob("*")
             if d.is_dir() and _SEG_NAME_RE.match(d.name)),
            key=lambda p: len(p.parts), reverse=True)

        if not seg_dirs:
            w("  Ingen seg-mapper funnet — ingenting å gjøre.", "info")
            return stats

        w(f"  {len(seg_dirs)} seg-mappe(r) funnet — fjerner segmentering.", "info")

        for seg_dir in seg_dirs:
            if stop_ev.is_set():
                break
            parent = seg_dir.parent
            moved_here = 0
            for child in list(seg_dir.iterdir()):
                if stop_ev.is_set():
                    break
                target = parent / child.name
                if _PART_SUFFIX_RE.search(child.name):
                    stats["part_files"] += 1
                if target.exists():
                    w(f"  [ADVARSEL] Navnekollisjon — hopper over: "
                      f"{child.name} (finnes allerede i {parent.name}/)", "warn")
                    stats["collisions"] += 1
                    continue
                try:
                    child.rename(target)
                    stats["files_moved"] += 1
                    moved_here += 1
                except OSError:
                    try:
                        shutil.move(str(child), str(target))
                        stats["files_moved"] += 1
                        moved_here += 1
                    except Exception as exc:
                        w(f"  FEIL flytting {child.name}: {exc}", "feil")
            # Slett seg-mappen hvis den nå er tom
            try:
                if not any(seg_dir.iterdir()):
                    seg_dir.rmdir()
                    stats["seg_removed"] += 1
                else:
                    w(f"  [ADVARSEL] {seg_dir.name} ikke tom — beholdes "
                      f"(kollisjoner?)", "warn")
            except OSError as exc:
                w(f"  FEIL sletting {seg_dir.name}: {exc}", "feil")

        if stats["part_files"]:
            w(f"  [ADVARSEL] {stats['part_files']} binær-splittet LOB-fil(er) "
              f"(_partNNN) oppdaget. SIARD 2.1 støtter ikke binær-splitting; "
              f"disse må settes sammen manuelt.", "warn")

        w(f"  {stats['files_moved']} fil(er) flyttet, "
          f"{stats['seg_removed']} seg-mappe(r) fjernet.", "ok")

        # ── Steg 2: Patch tableX.xml — fjern seg_N/-ledd fra file=/href= ───────
        for xml_file in search_root.rglob("*.xml"):
            if stop_ev.is_set():
                break
            if xml_file.name.lower() == "metadata.xml":
                continue
            stats["xml_updated"] += self._patch_xml_file(xml_file, w)

        w(f"  {stats['xml_updated']} XML-referanser oppdatert.", "ok")
        return stats

    @staticmethod
    def _patch_xml_file(xml_file: Path, w) -> int:
        """Strip seg_N/-ledd fra LOB-referanser i én tableX.xml. Returnerer antall."""
        updates = 0

        def _replace(m: re.Match) -> bytes:
            nonlocal updates
            attr_open, ref, attr_close = m.group(1), m.group(2), m.group(3)
            new_ref = _strip_seg_from_path(ref)
            if new_ref != ref:
                updates += 1
                return attr_open + new_ref + attr_close
            return m.group(0)

        tmp = xml_file.with_suffix(xml_file.suffix + ".tmp_segfix")
        try:
            with open(xml_file, "rb") as src, \
                 open(tmp, "wb", buffering=256 * 1024) as dst:
                for line in src:
                    if b"seg" in line and (b"file" in line or b"href" in line):
                        line = _FILE_ATTR_RE.sub(_replace, line)
                    dst.write(line)
            tmp.replace(xml_file)
        except Exception as exc:
            tmp.unlink(missing_ok=True)
            w(f"  FEIL patch {xml_file.name}: {exc}", "feil")
            return 0
        return updates

    # ── Standalone-modus (pakk ut → rett → pakk inn) ──────────────────────────

    def _run_standalone(self, ctx: WorkflowContext, w, stop_ev) -> OperationResult:
        self.produces_siard = True
        src_path = ctx.siard_path
        if not src_path or not src_path.exists():
            return self._fail(f"SIARD-fil ikke funnet: {src_path}")

        suffix   = (self.params.get("output_suffix") or "_segfix").strip()
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        counter  = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{counter}" + src_path.suffix)
            counter += 1

        td = (ctx.metadata.get("temp_dir", "").strip()
              if hasattr(ctx, "metadata") else "")
        temp_root = Path(td) if td and Path(td).is_dir() else src_path.parent

        with tempfile.TemporaryDirectory(dir=temp_root,
                                         prefix="siard_segfix_") as _tmpdir:
            extract_dir = Path(_tmpdir) / "extracted"
            extract_dir.mkdir()

            w(f"  Pakker ut {src_path.name} ...", "info")
            try:
                with zipfile.ZipFile(src_path, "r", allowZip64=True) as zf:
                    orig_namelist = zf.namelist()
                    for name in orig_namelist:
                        zf.extract(name, extract_dir)
            except Exception as exc:
                return self._fail(f"Kan ikke pakke ut SIARD: {exc}")

            stats = self._run_on_dir(extract_dir, w, stop_ev)
            if stats["files_moved"] == 0 and stats["seg_removed"] == 0:
                return self._ok(stats, "Ingen LOB-segmentering funnet")

            # Repakk — gjenbruk pakkelogikk/kompresjon fra siard_format
            from siard_workflow.core.siard_format import (
                get_zip_compresslevel, get_smart_skip_enabled,
                is_precompressed_bytes,
            )
            level     = get_zip_compresslevel()
            smartskip = get_smart_skip_enabled()
            compress  = zipfile.ZIP_STORED if level == 0 else zipfile.ZIP_DEFLATED
            comp_lvl  = level if level > 0 else None

            def _write_smart(zf, p: Path, arc: str) -> None:
                if level > 0 and smartskip:
                    try:
                        head = p.open("rb").read(16)
                    except Exception:
                        head = b""
                    if head and is_precompressed_bytes(head):
                        zf.write(p, arc, compress_type=zipfile.ZIP_STORED)
                        return
                zf.write(p, arc)

            # Filene som ble flyttet ut av seg-mapper har nye stier; de gamle
            # seg-stiene i orig_namelist finnes ikke lenger. Pakk derfor ut fra
            # filsystemet, ikke fra namelisten.
            w(f"  Pakker ny SIARD: {dst_path.name} (kompresjon nivå "
              f"{level}) ...", "info")
            try:
                with zipfile.ZipFile(dst_path, "w", compress,
                                     allowZip64=True,
                                     compresslevel=comp_lvl) as zf_out:
                    # Bevar tomme kataloginnganger fra originalen som fortsatt
                    # finnes på disk (seg-mapper er borte og hoppes over).
                    for name in orig_namelist:
                        if not name.endswith("/"):
                            continue
                        if any(_SEG_NAME_RE.match(c)
                               for c in name.strip("/").split("/")):
                            continue
                        if (extract_dir / name).is_dir():
                            zf_out.writestr(zipfile.ZipInfo(name), b"")
                    for f in sorted(extract_dir.rglob("*")):
                        if not f.is_file():
                            continue
                        arc = str(f.relative_to(extract_dir)).replace("\\", "/")
                        _write_smart(zf_out, f, arc)
            except Exception as exc:
                dst_path.unlink(missing_ok=True)
                return self._fail(f"Pakking feilet: {exc}")

        ctx.siard_path = dst_path
        msg = (f"{stats['files_moved']} fil(er) flyttet, "
               f"{stats['seg_removed']} seg-mappe(r) fjernet, "
               f"{stats['xml_updated']} XML-referanser → {dst_path.name}")
        w(f"  Ferdig: {msg}", "ok")
        return self._ok({**stats, "output_path": str(dst_path)}, msg)
