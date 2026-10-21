"""siard_workflow/core/zip_progress.py

Utpakking og innpakking av store SIARD-filer — med jevn fremdrift.

Fremdrift måles i BYTES, ikke antall filer: en SIARD har gjerne millioner av
små LOB-filer og noen få tableX.xml på flere GB. Telt i filer står fremdriften
stille i minutter mens en stor fil skrives, og hopper når de små suser forbi.

  * GUI: «phase_progress» hvert GUI_INTERVAL_S med tekst (filer, GB, MB/s)
    og anslag over gjenstående tid.
  * Kjørelogg: én linje per LOG_STEP_PCT %, og uansett minst hvert
    LOG_INTERVAL_S.
  * Store filer kopieres i blokker, med fremdrift underveis.
  * Utpakking i flere tråder — med millioner av små filer er det å opprette
    filene (ikke dekomprimeringen) som tar tid.
"""
from __future__ import annotations

import concurrent.futures
import os
import threading
import time
import zipfile
from pathlib import Path
from typing import Callable, Optional

GUI_INTERVAL_S = 0.5
LOG_STEP_PCT = 10
LOG_INTERVAL_S = 30 * 60
CHUNK = 4 << 20                  # blokkstørrelse ved kopiering av store filer
BIG_FILE = 8 << 20               # filer fra denne størrelsen strømmes i blokker
_WFLAGS = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)


def fmt_bytes(n: float) -> str:
    for unit, f in (("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10)):
        if n >= f:
            return f"{n / f:.1f} {unit}".replace(".", ",")
    return f"{int(n)} B"


def fmt_duration(sec: float) -> str:
    sec = max(0, int(sec))
    h, rest = divmod(sec, 3600)
    m, s = divmod(rest, 60)
    if h:
        return f"{h} t {m} min"
    return f"{m} min" if m else f"{s} s"


def _n(n: int) -> str:
    return f"{n:,}".replace(",", " ")


class ByteProgress:
    """Trådsikker fremdrift i bytes og filer, med GUI- og loggmeldinger."""

    def __init__(self, verb: str, n_files: int, total_bytes: int,
                 pcb: "Optional[Callable]" = None,
                 w: "Optional[Callable[[str, str], None]]" = None):
        self.verb = verb
        self.n_files = n_files
        self.total = max(1, total_bytes)
        self.pcb = pcb
        self.w = w
        self.files = 0
        self.bytes = 0
        self._lock = threading.Lock()
        self.t0 = time.monotonic()
        self._last_gui = 0.0
        self._last_log = self.t0
        self._next_pct = LOG_STEP_PCT

    def add(self, nbytes: int = 0, files: int = 0) -> None:
        with self._lock:
            self.bytes += nbytes
            self.files += files
            now = time.monotonic()
            gui = now - self._last_gui >= GUI_INTERVAL_S
            pct = self.bytes * 100 / self.total
            log = pct >= self._next_pct or now - self._last_log >= LOG_INTERVAL_S
            if gui:
                self._last_gui = now
            if log:
                self._last_log = now
                while self._next_pct <= pct:
                    self._next_pct += LOG_STEP_PCT
            snap = (self.files, self.bytes, now)
        if gui:
            self._emit_gui(*snap)
        if log and pct < 100:
            self._emit_log(*snap)

    def finish(self) -> None:
        snap = (self.files, self.bytes, time.monotonic())
        self._emit_gui(*snap)

    def _rate_eta(self, b: int, now: float) -> "tuple[str, str]":
        used = now - self.t0
        if used < 1 or not b:
            return "", ""
        rate = b / used
        rest = (self.total - b) / rate if rate else 0
        return (f"{fmt_bytes(rate)}/s",
                f"ca. {fmt_duration(rest)} igjen" if b < self.total else "")

    def text(self, files: int, b: int, now: float) -> str:
        rate, _eta = self._rate_eta(b, now)
        return (f"{self.verb}: {_n(files)}/{_n(self.n_files)} filer · "
                f"{fmt_bytes(b)} av {fmt_bytes(self.total)}"
                + (f" · {rate}" if rate else ""))

    def _emit_gui(self, files: int, b: int, now: float) -> None:
        if not self.pcb:
            return
        _rate, eta = self._rate_eta(b, now)
        try:
            self.pcb("phase_progress", done=min(1000, int(b * 1000 / self.total)),
                     total=1000, text=self.text(files, b, now), eta=eta)
        except Exception:
            pass

    def _emit_log(self, files: int, b: int, now: float) -> None:
        if not self.w:
            return
        rate, eta = self._rate_eta(b, now)
        self.w(f"    {b * 100 / self.total:.0f} % — {_n(files)}/{_n(self.n_files)} filer, "
               f"{fmt_bytes(b)} av {fmt_bytes(self.total)}"
               + (f", {rate}" if rate else "") + (f", {eta}" if eta else "")
               + f" ({fmt_duration(now - self.t0)} brukt)", "info")


# ── Utpakking ────────────────────────────────────────────────────────────────

def _target_path(name: str, dest: str) -> str:
    """Målsti for en ZIP-oppføring — samme rensing som ZipFile.extract."""
    arc = name.replace("/", os.path.sep)
    if os.path.altsep:
        arc = arc.replace(os.path.altsep, os.path.sep)
    arc = os.path.splitdrive(arc)[1]
    invalid = ("", os.path.curdir, os.path.pardir)
    arc = os.path.sep.join(x for x in arc.split(os.path.sep) if x not in invalid)
    if os.path.sep == "\\":
        arc = zipfile.ZipFile._sanitize_windows_name(arc, os.path.sep)
    return os.path.join(dest, arc)


def extract_all(zip_path: Path, dest: Path, progress_factory: Callable[[int, int], ByteProgress],
                workers: int = 4, stop: "Optional[Callable[[], bool]]" = None,
                warn: "Optional[Callable[[str], None]]" = None) -> "tuple[list[str], int]":
    """
    Pakk ut hele arkivet til `dest` i `workers` tråder (hver med egen
    ZipFile-instans). Returnerer (namelist, antall filer). progress_factory
    (n_filer, bytes_totalt) lager fremdriftsobjektet når innholdet er kjent.
    """
    dest_s = str(dest)
    with zipfile.ZipFile(zip_path, "r", allowZip64=True) as zf:
        infos = zf.infolist()
        names = [zi.filename for zi in infos]
    dirs: set = set()
    jobs: list = []
    for zi in infos:
        target = _target_path(zi.filename, dest_s)
        if zi.is_dir():
            dirs.add(target)
        else:
            dirs.add(os.path.dirname(target))
            jobs.append((zi, target))
    for d in sorted(dirs):                       # mapper først — ingen kappløp
        os.makedirs(d, exist_ok=True)
    prog = progress_factory(len(jobs), sum(zi.file_size for zi, _t in jobs))

    # Sammenhengende biter (leser ZIP-en sekvensielt per tråd), flere biter
    # enn tråder slik at én stor fil ikke holder igjen resten
    n_parts = max(1, min(len(jobs), workers * 16))
    size = -(-len(jobs) // n_parts) if jobs else 1
    parts = [jobs[i:i + size] for i in range(0, len(jobs), size)]

    def run(part) -> None:
        with zipfile.ZipFile(zip_path, "r", allowZip64=True) as z:
            for zi, target in part:
                if stop and stop():
                    return
                try:
                    if zi.file_size >= BIG_FILE:
                        with z.open(zi) as src, open(target, "wb") as dst:
                            while True:
                                chunk = src.read(CHUNK)
                                if not chunk:
                                    break
                                dst.write(chunk)
                                prog.add(len(chunk))
                    else:
                        # Små filer: lavnivå os.open/os.write (~10 % raskere enn
                        # filobjekter når hver fil koster ~0,4 ms i filsystemet)
                        data = z.read(zi)
                        fd = os.open(target, _WFLAGS, 0o666)
                        try:
                            view = memoryview(data)
                            while view:
                                view = view[os.write(fd, view):]
                        finally:
                            os.close(fd)
                        prog.add(len(data))
                    prog.add(files=1)
                except Exception as exc:
                    if warn:
                        warn(f"  [ADVARSEL] Kunne ikke pakke ut {zi.filename}: {exc}")
                    prog.add(files=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers),
                                               thread_name_prefix="utpakking") as pool:
        for _ in pool.map(run, parts):
            pass
    prog.finish()
    return names, len(jobs)


# ── Innpakking ───────────────────────────────────────────────────────────────

def scan_files(root: Path, pcb: "Optional[Callable]" = None) -> "list[tuple[Path, int]]":
    """
    Alle filer under `root` med størrelse, sortert som sorted(rglob) —
    os.scandir gir størrelsen fra mappelistingen (ingen ekstra stat på Windows).
    Viser fremdrift i GUI mens listen bygges.
    """
    out: list = []
    stack = [str(root)]
    last = 0.0
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        stack.append(e.path)
                    elif e.is_file(follow_symlinks=False):
                        try:
                            out.append((Path(e.path), e.stat().st_size))
                        except OSError:
                            out.append((Path(e.path), 0))
        except OSError:
            continue
        now = time.monotonic()
        if pcb and now - last >= GUI_INTERVAL_S:
            last = now
            try:
                pcb("phase_progress", done=0, total=0,
                    text=f"Finner filer: {_n(len(out))} …", eta="")
            except Exception:
                pass
    out.sort(key=lambda t: t[0])
    return out


def read_small(item: "tuple[Path, int]", limit: int = 1 << 20) -> "bytes | None":
    """Les en liten fil (forhåndslesing i tråd); None for større filer."""
    path, size = item
    if size >= limit:
        return None
    return path.read_bytes()
