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
import struct
import threading
import time
import zipfile
import zlib
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
    Pakk ut hele arkivet til `dest` i `workers` tråder. Returnerer (namelist,
    antall filer). progress_factory(n_filer, bytes_totalt) lager
    fremdriftsobjektet når innholdet er kjent.

    Innholdsfortegnelsen leses ÉN gang. Per oppføring beholdes bare
    posisjon, størrelser, metode, CRC og flagg i kompakte tall-arrayer (~32 byte),
    og trådene leser rådata direkte fra filposisjonen. (1.4.14 åpnet SIARD-fila
    på nytt for hver arbeidsbit — med 7,9 mill. oppføringer ble katalogen lest
    64 ganger og prosessen brukte 20 GB minne.)
    """
    from array import array
    dest_s = str(dest)
    names: list = []
    offs, csizes, sizes = array("Q"), array("Q"), array("Q")
    crcs, kinds = array("L"), array("L")         # kind = metode | flagg << 16
    idx = array("L")                             # oppføring → indeks i names
    dirs: set = set()
    # Uten søppeltømming mens katalogen leses: millioner av ZipInfo-objekter
    # utløser ellers gjentatte fulle gjennomganger av hele heapen (ingen
    # sykler skapes her, så det er trygt)
    import gc
    gc_was = gc.isenabled()
    gc.disable()
    try:
        _read_catalog(zip_path, dest_s, names, offs, csizes, sizes, crcs, kinds, idx, dirs)
    finally:
        if gc_was:
            gc.enable()
    for d in sorted(dirs):                       # mapper først — ingen kappløp
        os.makedirs(d, exist_ok=True)
    n_jobs = len(idx)
    prog = progress_factory(n_jobs, sum(sizes))
    return _extract_entries(zip_path, dest_s, names, offs, csizes, sizes, crcs, kinds,
                            idx, prog, workers, stop, warn)


def _read_catalog(zip_path, dest_s, names, offs, csizes, sizes, crcs, kinds, idx,
                  dirs) -> None:
    with zipfile.ZipFile(zip_path, "r", allowZip64=True) as zf:
        for zi in zf.infolist():
            names.append(zi.filename)
            target = _target_path(zi.filename, dest_s)
            if zi.is_dir():
                dirs.add(target)
                continue
            dirs.add(os.path.dirname(target))
            idx.append(len(names) - 1)
            offs.append(zi.header_offset)
            csizes.append(zi.compress_size)
            sizes.append(zi.file_size)
            crcs.append(zi.CRC & 0xFFFFFFFF)
            kinds.append((zi.compress_type & 0xFFFF) | (zi.flag_bits << 16))
        zf.filelist = []                          # frigi ZipInfo-objektene nå
        zf.NameToInfo = {}


def _extract_entries(zip_path, dest_s, names, offs, csizes, sizes, crcs, kinds, idx,
                     prog: ByteProgress, workers: int, stop, warn) -> "tuple[list[str], int]":
    n_jobs = len(idx)
    # Sammenhengende biter (sekvensiell lesing per tråd), flere biter enn
    # tråder slik at én stor fil ikke holder igjen resten
    n_parts = max(1, min(n_jobs, workers * 16))
    size = -(-n_jobs // n_parts) if n_jobs else 1
    parts = [(i, min(i + size, n_jobs)) for i in range(0, n_jobs, size)]
    fallback: list = []                          # krypterte/uvanlige metoder
    fb_lock = threading.Lock()

    def _warn(name: str, exc) -> None:
        if warn:
            warn(f"  [ADVARSEL] Kunne ikke pakke ut {name}: {exc}")

    def run(part) -> None:
        a, b = part
        with open(zip_path, "rb") as fh:
            for j in range(a, b):
                if stop and stop():
                    return
                name = names[idx[j]]
                method, flags = kinds[j] & 0xFFFF, kinds[j] >> 16
                if flags & 0x1 or method not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                    with fb_lock:
                        fallback.append(j)
                    continue
                target = _target_path(name, dest_s)
                try:
                    _extract_raw(fh, offs[j], csizes[j], sizes[j], method, crcs[j],
                                 target, prog)
                except Exception as exc:
                    _warn(name, exc)
                prog.add(files=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers),
                                               thread_name_prefix="utpakking") as pool:
        for _ in pool.map(run, parts):
            pass
    if fallback and not (stop and stop()):
        with zipfile.ZipFile(zip_path, "r", allowZip64=True) as zf:
            for j in fallback:
                name = names[idx[j]]
                try:
                    with zf.open(name) as src, open(_target_path(name, dest_s), "wb") as dst:
                        while True:
                            chunk = src.read(CHUNK)
                            if not chunk:
                                break
                            dst.write(chunk)
                            prog.add(len(chunk))
                except Exception as exc:
                    _warn(name, exc)
                prog.add(files=1)
    prog.finish()
    return names, n_jobs


def _extract_raw(fh, offset: int, csize: int, size: int, method: int, crc: int,
                 target: str, prog: ByteProgress) -> None:
    """Les én oppføring direkte fra filposisjonen (lokal header), dekomprimer og skriv."""
    fh.seek(offset)
    hdr = fh.read(30)
    if len(hdr) < 30 or hdr[:4] != b"PK\x03\x04":
        raise zipfile.BadZipFile("ugyldig lokal filheader")
    n, m = struct.unpack_from("<HH", hdr, 26)
    fh.seek(offset + 30 + n + m)
    if size >= BIG_FILE or csize >= BIG_FILE:
        dec = zlib.decompressobj(-15) if method == zipfile.ZIP_DEFLATED else None
        left, run_crc = csize, 0
        with open(target, "wb") as dst:
            while left > 0:
                raw = fh.read(min(CHUNK, left))
                if not raw:
                    raise EOFError("arkivet sluttet for tidlig")
                left -= len(raw)
                data = dec.decompress(raw) if dec else raw
                run_crc = zlib.crc32(data, run_crc)
                dst.write(data)
                prog.add(len(data))
            if dec:
                tail = dec.flush()
                run_crc = zlib.crc32(tail, run_crc)
                dst.write(tail)
                prog.add(len(tail))
        if run_crc & 0xFFFFFFFF != crc:
            raise zipfile.BadZipFile("feil CRC-32")
        return
    raw = fh.read(csize)
    data = zlib.decompress(raw, -15) if method == zipfile.ZIP_DEFLATED else raw
    if zlib.crc32(data) & 0xFFFFFFFF != crc:
        raise zipfile.BadZipFile("feil CRC-32")
    # Små filer: lavnivå os.open/os.write (~10 % raskere enn filobjekter når
    # hver fil koster ~0,4 ms i filsystemet)
    fd = os.open(target, _WFLAGS, 0o666)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
    finally:
        os.close(fd)
    prog.add(len(data))


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
