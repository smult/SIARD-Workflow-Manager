"""siard_workflow/operations/standardize_ext_operation.py

StandardizeExtOperation
-----------------------
Omdøper alle LOB-filer i SIARD-strukturen som har ikke-standard filendelser
til .bin og oppdaterer tilhørende XML-referanser.

En "standard" endelse i SIARD-sammenheng er .bin (eller .txt for tekst-LOBs).
Filer som f.eks. heter record001.doc.pdf (etter ekstern konvertering) omdøpes
til record001.bin — stammen er alt frem til første punktum.

XML-kommentar legges på linjen: <!-- Filendelse endret fra .doc.pdf til .bin -->

Gjør ingenting hvis 'standardize_bin_ext' er False i globale innstillinger.
"""

from __future__ import annotations
import io
import re
import shutil
import tempfile
import threading
import zipfile
from pathlib import Path, PurePosixPath

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


_FILE_ATTR_RE = re.compile(
    rb'((?:file|fileName|href)=["\'])([^"\']+)(["\'])')


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
    """
    def _replace(m: re.Match) -> bytes:
        attr_open = m.group(1)
        ref       = m.group(2)
        attr_close = m.group(3)

        # Sjekk om basenavn er i rename-map
        sep = b"/" if b"/" in ref else None
        if sep:
            dir_part  = ref.rsplit(b"/", 1)[0] + b"/"
            base_part = ref.rsplit(b"/", 1)[1]
        else:
            dir_part  = b""
            base_part = ref

        if base_part in rename_map_bytes:
            new_base, _comment = rename_map_bytes[base_part]
            return attr_open + dir_part + new_base + attr_close

        return m.group(0)

    new_line = _FILE_ATTR_RE.sub(_replace, line)
    if new_line == line:
        return line

    # Finn hvilken kommentar som gjelder (første treff)
    for old_b, (_, comment) in rename_map_bytes.items():
        if old_b in line:
            new_line = _append_xml_comment_to_line(new_line, comment)
            break
    return new_line


class StandardizeExtOperation(BaseOperation):
    """
    Standardiser alle LOB-filendelser til .bin i SIARD-strukturen.

    Gjennomgår alle lob-mapper og omdøper filer som ikke allerede heter
    <stamme>.bin (eller <stamme>.txt) til <stamme>.bin. Oppdaterer
    tilhørende XML-referanser i tableX.xml-filer med en XML-kommentar.

    Støtter pipeline-modus (utpakket mappe) og standalone-modus.
    Gjør ingenting hvis 'standardize_bin_ext' er False i innstillinger.
    """

    operation_id   = "standardize_ext"
    label          = "Standardiser filendelser"
    category       = "Kompatibilitet"
    status         = 2
    produces_siard = False

    default_params: dict = {
        "output_suffix": "_stdext",
    }

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

        # Sjekk innstilling
        try:
            from settings import get_config
            if not bool(get_config("standardize_bin_ext", True)):
                w("  'Standardiser .bin' er deaktivert i innstillinger — ingen endringer.", "info")
                return self._ok({}, "Deaktivert i innstillinger — ingen endringer")
        except Exception:
            pass

        stop_ev  = ctx.metadata.get("stop_event",  threading.Event())
        pause_ev = ctx.metadata.get("pause_event", threading.Event())

        # Pipeline-modus: utpakket mappe finnes allerede
        pre_dir = getattr(ctx, "extracted_path", None)
        if pre_dir is not None and pre_dir.is_dir():
            self.produces_siard = False
            stats = self._run_on_dir(pre_dir, w, progress, stop_ev)
            msg = (f"{stats['renamed']} fil(er) omdøpt, "
                   f"{stats['xml_updated']} XML-referanser oppdatert")
            w(f"  Ferdig (pipeline): {msg}", "ok")
            return self._ok(stats, msg)

        # Standalone-modus: pakk ut, prosesser, repakk
        self.produces_siard = True
        src_path = ctx.siard_path
        suffix   = self.params.get("output_suffix", "_stdext")
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)

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

        with tempfile.TemporaryDirectory(dir=temp_root,
                                         prefix="siard_stdext_") as _tmpdir:
            tmpdir = Path(_tmpdir)
            extract_dir = tmpdir / "extracted"
            extract_dir.mkdir()

            # Pakk ut
            w(f"  Pakker ut {src_path.name} ...", "info")
            try:
                with zipfile.ZipFile(src_path, "r", allowZip64=True) as zf:
                    orig_namelist = zf.namelist()
                    for name in orig_namelist:
                        zf.extract(name, extract_dir)
            except Exception as exc:
                return self._fail(f"Kan ikke pakke ut SIARD: {exc}")

            stats = self._run_on_dir(extract_dir, w, progress, stop_ev)

            # Repakk
            w(f"  Pakker ny SIARD: {dst_path.name} ...", "info")
            try:
                with zipfile.ZipFile(dst_path, "w",
                                     zipfile.ZIP_DEFLATED,
                                     allowZip64=True) as zf_out:
                    for orig_name in orig_namelist:
                        orig_p = extract_dir / orig_name
                        if orig_p.is_dir():
                            zf_out.writestr(
                                zipfile.ZipInfo(orig_name + "/"), b"")
                            continue
                        if orig_p.exists():
                            zf_out.write(orig_p, orig_name)
                    # Legg til filer som kom til under prosessering (endret navn)
                    for f in extract_dir.rglob("*"):
                        if not f.is_file():
                            continue
                        arc = str(f.relative_to(extract_dir)).replace("\\", "/")
                        if arc not in orig_namelist:
                            zf_out.write(f, arc)
            except Exception as exc:
                dst_path.unlink(missing_ok=True)
                return self._fail(f"Pakking feilet: {exc}")

        ctx.siard_path = dst_path
        msg = (f"{stats['renamed']} fil(er) omdøpt, "
               f"{stats['xml_updated']} XML-referanser oppdatert → {dst_path.name}")
        w(f"  Ferdig: {msg}", "ok")
        return self._ok({**stats, "output_path": str(dst_path)}, msg)

    def _run_on_dir(self, extract_dir: Path, w, progress, stop_ev) -> dict:
        """Kjørn omdøpings- og XML-patching på utpakket katalog."""
        stats = {"renamed": 0, "xml_updated": 0}

        content_dir = extract_dir / "content"
        if not content_dir.exists():
            w("  Ingen content/-mappe funnet — ingenting å gjøre.", "info")
            return stats

        # ── Steg 1: Finn og omdøp LOB-filer ──────────────────────────────────
        # rename_map: {old_basename: new_basename} — per LOB-mappe (kan ha kollisjoner)
        # Vi lagrer en global liste for XML-patching:
        # global_rename: {old_basename: (new_basename, comment)} — siste vinner ved kollisjoner
        global_rename: dict[str, tuple[str, str]] = {}

        lob_re = re.compile(r"lob\d+", re.IGNORECASE)
        for lob_dir in content_dir.rglob("*"):
            if stop_ev.is_set():
                break
            if not lob_dir.is_dir() or not lob_re.fullmatch(lob_dir.name):
                continue
            for f in list(lob_dir.iterdir()):
                if stop_ev.is_set():
                    break
                if not f.is_file():
                    continue
                if _is_standard_lob_name(f.name):
                    continue
                old_name  = f.name
                new_name  = _to_bin_name(old_name)
                old_exts  = _old_exts(old_name)
                comment   = f"Filendelse endret fra {old_exts} til .bin"
                new_path  = f.parent / new_name
                try:
                    f.rename(new_path)
                    stats["renamed"] += 1
                    global_rename[old_name] = (new_name, comment)
                    w(f"  {old_name} → {new_name}", "info")
                except Exception as exc:
                    w(f"  FEIL omdøp {old_name}: {exc}", "feil")

        if not global_rename:
            w("  Ingen filer trengte standardisering.", "info")
            return stats

        w(f"  {stats['renamed']} fil(er) omdøpt.", "ok")

        # ── Steg 2: Patch tableX.xml ──────────────────────────────────────────
        rename_map_bytes: dict[bytes, tuple[bytes, str]] = {
            k.encode("utf-8"): (v.encode("utf-8"), c)
            for k, (v, c) in global_rename.items()
        }

        for xml_file in content_dir.rglob("*.xml"):
            if stop_ev.is_set():
                break
            if xml_file.name.lower() == "metadata.xml":
                continue
            n = self._patch_xml_file(xml_file, rename_map_bytes, w)
            stats["xml_updated"] += n

        w(f"  {stats['xml_updated']} XML-referanser oppdatert.", "ok")
        return stats

    @staticmethod
    def _patch_xml_file(xml_file: Path,
                        rename_map_bytes: dict[bytes, tuple[bytes, str]],
                        w) -> int:
        """Patch én tableX.xml-fil linje for linje. Returnerer antall oppdateringer."""
        tmp = xml_file.with_suffix(".tmp_stdext")
        updates = 0
        try:
            with open(xml_file, "rb") as src, \
                 open(tmp, "wb", buffering=256 * 1024) as dst:
                for line in src:
                    if b"file" in line or b"href" in line:
                        new_line = _patch_xml_line(line, rename_map_bytes)
                        if new_line != line:
                            updates += 1
                            line = new_line
                    dst.write(line)
            tmp.replace(xml_file)
        except Exception as exc:
            tmp.unlink(missing_ok=True)
            w(f"  FEIL patch {xml_file.name}: {exc}", "feil")
            return 0
        return updates

    def count_non_standard_lob_files(self, extract_dir: Path) -> int:
        """Teller LOB-filer med ikke-standard endelse (ikke .bin/.txt)."""
        count = 0
        content = extract_dir / "content"
        if not content.exists():
            return 0
        lob_re = re.compile(r"lob\d+", re.IGNORECASE)
        for lob_dir in content.rglob("*"):
            if not lob_dir.is_dir() or not lob_re.fullmatch(lob_dir.name):
                continue
            for f in lob_dir.iterdir():
                if f.is_file() and not _is_standard_lob_name(f.name):
                    count += 1
        return count
