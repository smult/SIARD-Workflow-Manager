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


# ── Kopi av arkivet med endret metadata ──────────────────────────────────────
class RewriteCancelled(Exception):
    """Brukeren stoppet kjøringen mens den nye fila ble skrevet."""


class NotEnoughSpace(OSError):
    """For lite ledig plass til den nye fila."""


def _strip_zip64_extra(extra: bytes) -> bytes:
    """Fjern ZIP64-feltet (id 1) — det skrives på nytt ut fra størrelsene."""
    out, i = b"", 0
    while i + 4 <= len(extra):
        tag, ln = struct.unpack_from("<HH", extra, i)
        if tag != 1:
            out += extra[i:i + 4 + ln]
        i += 4 + ln
    return out


def _copy_entry_raw(raw, zi: zipfile.ZipInfo, zout: zipfile.ZipFile,
                    prog: "Optional[ByteProgress]", stop) -> None:
    """
    Kopier én oppføring uten å pakke den ut: de komprimerte bytene flyttes som
    de er, med ny lokal header. CRC og størrelser hentes fra innholdslista.
    """
    raw.seek(zi.header_offset)
    hdr = raw.read(30)
    if len(hdr) < 30 or hdr[:4] != b"PK\x03\x04":
        raise zipfile.BadZipFile(f"ugyldig lokal filheader for {zi.filename}")
    n, m = struct.unpack_from("<HH", hdr, 26)
    raw.seek(zi.header_offset + 30 + n + m)

    import copy
    ni = copy.copy(zi)
    ni.flag_bits &= ~0x08            # størrelsene står i headeren (ingen datadeskriptor)
    ni.extra = _strip_zip64_extra(zi.extra or b"")
    out = zout.fp
    out.seek(zout.start_dir)
    ni.header_offset = out.tell()
    zip64 = (ni.file_size > zipfile.ZIP64_LIMIT
             or ni.compress_size > zipfile.ZIP64_LIMIT)
    out.write(ni.FileHeader(zip64))
    left = zi.compress_size
    while left > 0:
        if stop is not None and stop():
            raise RewriteCancelled()
        chunk = raw.read(min(CHUNK, left))
        if not chunk:
            raise EOFError(f"arkivet sluttet for tidlig ({zi.filename})")
        out.write(chunk)
        left -= len(chunk)
        if prog:
            prog.add(len(chunk))
    zout.filelist.append(ni)
    zout.NameToInfo[ni.filename] = ni
    zout.start_dir = out.tell()
    zout._didModify = True
    if prog:
        prog.add(0, 1)


def rewrite_zip(src: Path, dst: Path, replace: "Optional[dict]" = None,
                skip: "Optional[Callable[[str], bool]]" = None, *,
                setup: "Optional[Callable]" = None,
                stop: "Optional[Callable[[], bool]]" = None) -> dict:
    """
    Skriv en kopi av zip-arkivet `src` til `dst`, der oppføringene i `replace`
    ({navn: bytes}) er byttet ut og de `skip(navn)` peker ut, er utelatt.

    Alt annet kopieres RÅTT og strømmet: ingenting pakkes ut og inn igjen, og
    ingenting holdes i minnet. (Før ble hele den nye fila bygget i minnet —
    umulig for store SIARD-filer, og uten livstegn underveis.)

    `setup(oppføringer, bytes_totalt)` kalles før noe skrives og kan gi
    `(fremdrift, før, etter)`: en ByteProgress og funksjoner som kalles med
    `(indeks, ZipInfo)` før og etter hver oppføring. Den kan også avbryte ved
    å kaste et unntak (f.eks. NotEnoughSpace).

    Fila skrives som `dst` + «.part» og får riktig navn når alt er skrevet;
    ved feil eller stopp slettes den halvferdige fila.
    """
    src, dst = Path(src), Path(dst)
    replace = dict(replace or {})
    part = dst.with_name(dst.name + ".part")
    stats = {"copied": 0, "replaced": 0, "skipped": 0, "entries": 0}
    try:
        with zipfile.ZipFile(src, "r") as zin:
            infos = zin.infolist()
            total = sum(i.compress_size for i in infos)
            prog = before = after = None
            if setup is not None:
                got = setup(infos, total)
                if got:
                    prog, before, after = got
            with open(src, "rb") as raw, \
                 zipfile.ZipFile(part, "w", zipfile.ZIP_DEFLATED,
                                 allowZip64=True) as zout:
                for idx, zi in enumerate(infos):
                    if stop is not None and stop():
                        raise RewriteCancelled()
                    if before:
                        before(idx, zi)
                    name = zi.filename
                    if skip is not None and skip(name):
                        stats["skipped"] += 1
                        if prog:
                            prog.add(zi.compress_size, 1)
                    elif name in replace:
                        ni = zipfile.ZipInfo(name, date_time=zi.date_time)
                        ni.compress_type = zipfile.ZIP_DEFLATED
                        ni.external_attr = zi.external_attr
                        ni.comment = zi.comment
                        zout.writestr(ni, replace[name])
                        stats["replaced"] += 1
                        if prog:
                            prog.add(zi.compress_size, 1)
                    else:
                        _copy_entry_raw(raw, zi, zout, prog, stop)
                        stats["copied"] += 1
                    if after:
                        after(idx, zi)
                stats["entries"] = len(zout.filelist)
            if prog:
                prog.finish()
        os.replace(part, dst)
    except BaseException:
        try:
            part.unlink()
        except OSError:
            pass
        raise
    return stats


def write_siard_copy(src: Path, dst: Path, replace: dict, *,
                     skip: "Optional[Callable[[str], bool]]" = None,
                     tables: "Optional[list]" = None,
                     pcb: "Optional[Callable]" = None,
                     w: "Optional[Callable[[str, str], None]]" = None,
                     stop: "Optional[Callable[[], bool]]" = None) -> dict:
    """
    Ny SIARD-fil med endret metadata.xml — for operasjoner som kjøres uten
    «Pakk ut SIARD». Sjekker ledig plass, viser fremdrift i bytes med anslått
    gjenstående tid, og melder fra for hver tabell som er ferdig skrevet.

    `tables`: [(skjemamappe, tabellmappe, tabellnavn)] fra metadata.xml.
    """
    import shutil
    src, dst = Path(src), Path(dst)
    say = w or (lambda _m, _l="info": None)
    names = {(str(s or "").lower(), str(t or "").lower()): n
             for s, t, n in (tables or [])}
    t0 = time.monotonic()

    def _key(name: str) -> "tuple | None":
        p = name.split("/")
        if len(p) >= 4 and p[0].lower() == "content":
            return p[1].lower(), p[2].lower()
        return None

    def _setup(infos, total):
        kept = [i for i in infos if not (skip and skip(i.filename))]
        need = (sum(i.compress_size for i in kept if i.filename not in replace)
                + sum(len(v) for v in replace.values()) + 150 * len(kept) + (1 << 20))
        try:
            free = shutil.disk_usage(dst.parent).free
        except OSError:
            free = None
        say(f"  Skriver ny SIARD-fil: {_n(len(kept))} filer, {fmt_bytes(need)}"
            + (f" (ledig på disken: {fmt_bytes(free)})" if free is not None else ""), "info")
        if free is not None and free < need * 1.01:
            raise NotEnoughSpace(
                f"For lite ledig plass i {dst.parent}: trenger {fmt_bytes(need)}, "
                f"ledig {fmt_bytes(free)}")
        say("  Innholdet kopieres uendret (uten ny komprimering); bare metadata.xml "
            "skrives på nytt", "info")

        last: dict = {}
        size: dict = {}
        for idx, i in enumerate(infos):
            k = _key(i.filename)
            if k is not None and not (skip and skip(i.filename)):
                last[k] = idx
                size[k] = size.get(k, 0) + i.compress_size
        n_tables = len(last)
        prog = ByteProgress("Skriver ny SIARD", len(infos), total, pcb, w)
        state = {"cur": None, "done": 0, "seen": 0}

        def before(idx, zi):
            k = _key(zi.filename)
            if k is not None and k != state["cur"] and k in last:
                state["cur"] = k
                state["seen"] += 1
                prog.verb = (f"Skriver ny SIARD — {names.get(k, k[1])} "
                             f"(tabell {min(state['seen'], n_tables)} av {n_tables})")

        def after(idx, zi):
            k = _key(zi.filename)
            if k is not None and last.get(k) == idx:
                state["done"] += 1
                say(f"  {state['done']}/{n_tables} tabeller skrevet — "
                    f"{names.get(k, k[1])} ({fmt_bytes(size[k])})", "info")

        return prog, before, after

    stats = rewrite_zip(src, dst, replace, skip, setup=_setup, stop=stop)
    stats["seconds"] = time.monotonic() - t0
    try:
        stats["bytes"] = dst.stat().st_size
    except OSError:
        stats["bytes"] = 0
    return stats


# ── Rask lesing av én oppføring ──────────────────────────────────────────────
def read_member_fast(zip_path: Path, names: "tuple | list",
                     max_size: int = 512 << 20) -> "tuple[str, bytes] | None":
    """
    Les én oppføring (første av `names` som finnes) fra et zip-arkiv UTEN å
    bygge hele innholdslista.

    zipfile.ZipFile lager ett Python-objekt per fil når arkivet åpnes — for en
    SIARD med millioner av LOB-filer tar det mange sekunder med ren
    Python-kjøring, som også gjør brukergrensesnittet tregt når det skjer i en
    bakgrunnstråd. Her letes filnavnet opp direkte i innholdslistas rå byte
    (lesing fra disk + bytes.find), og bare den ene oppføringen pakkes ut.

    Returnerer (navn, innhold), eller None når oppføringen ikke finnes eller
    arkivet har en form som ikke håndteres her — kall da zipfile i stedet.
    """
    try:
        with open(zip_path, "rb") as f:
            size = f.seek(0, 2)
            tail_len = min(size, 65536 + 22 + 20)
            f.seek(size - tail_len)
            tail = f.read(tail_len)
            i = tail.rfind(b"PK\x05\x06")
            if i < 0 or i + 22 > len(tail):
                return None
            _d, _cd, _nd, n_total, cd_size, cd_off = struct.unpack_from("<HHHHII", tail, i + 4)
            if n_total == 0xFFFF or cd_size == 0xFFFFFFFF or cd_off == 0xFFFFFFFF:
                # ZIP64: pekeren til den store sluttposten står rett foran
                j = i - 20
                if j < 0 or tail[j:j + 4] != b"PK\x06\x07":
                    return None
                z64_off = struct.unpack_from("<Q", tail, j + 8)[0]
                f.seek(z64_off)
                rec = f.read(56)
                if len(rec) < 56 or rec[:4] != b"PK\x06\x06":
                    return None
                cd_size, cd_off = struct.unpack_from("<QQ", rec, 40)
            if cd_off + cd_size > size:
                return None
            f.seek(cd_off)
            if f.read(4) != b"PK\x01\x02":
                return None                       # f.eks. data lagt foran arkivet

            for name in names:
                nb = name.encode("utf-8")
                pos = _find_cd_record(f, cd_off, cd_size, nb)
                if pos is None:
                    continue
                f.seek(pos)
                head = f.read(46)
                flags, method = struct.unpack_from("<HH", head, 8)
                crc, csize, usize = struct.unpack_from("<III", head, 16)
                fnlen, exlen = struct.unpack_from("<HH", head, 28)
                lho = struct.unpack_from("<I", head, 42)[0]
                f.seek(pos + 46 + fnlen)
                extra = f.read(exlen)
                # ZIP64-tillegget har verdiene som ikke fikk plass, i fast rekkefølge
                k = 0
                while k + 4 <= len(extra):
                    tag, ln = struct.unpack_from("<HH", extra, k)
                    if tag == 1:
                        q = k + 4
                        if usize == 0xFFFFFFFF:
                            usize = struct.unpack_from("<Q", extra, q)[0]
                            q += 8
                        if csize == 0xFFFFFFFF:
                            csize = struct.unpack_from("<Q", extra, q)[0]
                            q += 8
                        if lho == 0xFFFFFFFF:
                            lho = struct.unpack_from("<Q", extra, q)[0]
                        break
                    k += 4 + ln
                if flags & 0x01 or method not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                    return None                   # kryptert / annen komprimering
                if csize > max_size or usize > max_size:
                    return None
                f.seek(lho)
                lh = f.read(30)
                if len(lh) < 30 or lh[:4] != b"PK\x03\x04":
                    return None
                n, m = struct.unpack_from("<HH", lh, 26)
                f.seek(lho + 30 + n + m)
                raw = f.read(csize)
                if len(raw) != csize:
                    return None
                data = zlib.decompress(raw, -15) if method == zipfile.ZIP_DEFLATED else raw
                if len(data) != usize or zlib.crc32(data) & 0xFFFFFFFF != crc:
                    return None
                return name, data
    except (OSError, struct.error, zlib.error, ValueError):
        return None
    return None


def _find_cd_record(f, cd_off: int, cd_size: int, name: bytes) -> "int | None":
    """Filposisjonen til innholdslistas post for `name` (nøyaktig navn), eller None."""
    block = 8 << 20
    keep = 46 + len(name) + 8                 # overlapp, så et treff på blokkgrensa finnes
    pos, buf, base = cd_off, b"", cd_off
    end = cd_off + cd_size
    while pos < end:
        f.seek(pos)
        chunk = f.read(min(block, end - pos))
        if not chunk:
            break
        buf = buf[-keep:] + chunk
        base = pos + len(chunk) - len(buf)
        pos += len(chunk)
        at = buf.find(name)
        while at >= 0:
            start = at - 46
            if (start >= 0 and buf[start:start + 4] == b"PK\x01\x02"
                    and struct.unpack_from("<H", buf, start + 28)[0] == len(name)):
                return base + start
            at = buf.find(name, at + 1)
    return None


# ── Endring direkte i arkivet ────────────────────────────────────────────────
# Én oppføring (metadata.xml) byttes ut uten at arkivet kopieres: den nye
# versjonen skrives der innholdslista sto, og ny innholdsliste skrives etter.
# Tar sekunder uansett størrelse — men ORIGINALFILA endres. Derfor lagres den
# gamle innholdslista i en angrefil først; avbrytes skrivingen, legges den
# tilbake (med en gang, eller neste gang fila brukes), og fila er som før.
UNDO_SUFFIX = ".angre"


def undo_path(zip_path: Path) -> Path:
    zip_path = Path(zip_path)
    return zip_path.with_name(zip_path.name + UNDO_SUFFIX)


def restore_interrupted_update(zip_path: Path,
                               w: "Optional[Callable[[str, str], None]]" = None) -> bool:
    """
    Finnes det en angrefil etter en avbrutt direkte-endring, legges den gamle
    innholdslista tilbake og fila får opprinnelig størrelse. Returnerer True
    når fila ble gjenopprettet.
    """
    import hashlib
    import json
    zip_path = Path(zip_path)
    undo = undo_path(zip_path)
    if not undo.is_file():
        return False
    with open(undo, "rb") as u:
        head = json.loads(u.readline().decode("utf-8"))
        tail = u.read()
    if (len(tail) != head["tail_len"]
            or hashlib.sha256(tail).hexdigest() != head["sha256"]):
        raise zipfile.BadZipFile(
            f"Angrefila {undo.name} er skadet — {zip_path.name} kan ikke "
            f"gjenopprettes automatisk")
    with open(zip_path, "r+b") as f:
        f.seek(head["start_dir"])
        f.write(tail)
        f.truncate(head["size"])
        f.flush()
        os.fsync(f.fileno())
    undo.unlink()
    if w:
        w(f"  {zip_path.name} er gjenopprettet slik den var før en avbrutt "
          f"endring (angrefila er brukt og slettet)", "warn")
    return True


def update_member_in_place(zip_path: Path, name: str, data: bytes, *,
                           w: "Optional[Callable[[str, str], None]]" = None) -> dict:
    """
    Bytt ut oppføringen `name` i zip-arkivet med `data` — i selve fila.

    Ingen andre byte i arkivet flyttes eller leses. Den gamle versjonen av
    oppføringen blir liggende som ubrukte byte (den står ikke lenger i
    innholdslista). Feiler noe underveis, gjenopprettes fila fra angrefila.
    """
    import datetime
    import hashlib
    import json
    zip_path = Path(zip_path)
    restore_interrupted_update(zip_path, w)
    size = zip_path.stat().st_size
    with zipfile.ZipFile(zip_path, "r") as zf:
        start_dir = zf.start_dir
        n_before = len(zf.filelist)
        zf.getinfo(name)                       # KeyError hvis den ikke finnes
    with open(zip_path, "rb") as f:
        f.seek(start_dir)
        tail = f.read()                        # innholdslista (ca. 100 byte per fil)

    # 1. Angrefil — skrives ferdig og til disk FØR SIARD-fila røres
    undo = undo_path(zip_path)
    tmp = undo.with_name(undo.name + ".tmp")
    head = {"size": size, "start_dir": start_dir, "tail_len": len(tail),
            "sha256": hashlib.sha256(tail).hexdigest(), "member": name,
            "written": datetime.datetime.now().isoformat(timespec="seconds")}
    with open(tmp, "wb") as u:
        u.write(json.dumps(head).encode("utf-8") + b"\n")
        u.write(tail)
        u.flush()
        os.fsync(u.fileno())
    os.replace(tmp, undo)

    # 2. Skriv ny oppføring + ny innholdsliste, og kontroller resultatet
    try:
        with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            old = zf.getinfo(name)
            zf.filelist.remove(old)
            del zf.NameToInfo[name]
            ni = zipfile.ZipInfo(name, date_time=datetime.datetime.now().timetuple()[:6])
            ni.compress_type = zipfile.ZIP_DEFLATED
            ni.external_attr = old.external_attr
            zf.writestr(ni, data)
        with open(zip_path, "r+b") as f:
            os.fsync(f.fileno())
        with zipfile.ZipFile(zip_path, "r") as zf:
            if len(zf.filelist) != n_before or zf.read(name) != data:
                raise zipfile.BadZipFile("kontrollen etter skriving feilet")
    except BaseException:
        restore_interrupted_update(zip_path, w)
        raise
    undo.unlink()
    return {"entries": n_before, "size_before": size,
            "size_after": zip_path.stat().st_size, "dead_bytes": 0}


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


# ── Opprydding ───────────────────────────────────────────────────────────────

RMTREE_LOG_INTERVAL_S = 5 * 60


def rmtree_progress(root: Path, w: "Optional[Callable[[str, str], None]]" = None,
                    label: str = "Rydder", total: "int | None" = None,
                    pcb: "Optional[Callable]" = None) -> int:
    """
    shutil.rmtree med fremdrift: å slette millioner av filer tar over en time
    (antivirus), og uten meldinger ser det ut som om programmet har hengt.

    Kjørelogg: en startlinje, så én linje per LOG_STEP_PCT % (når `total` er
    kjent) og uansett minst hvert RMTREE_LOG_INTERVAL_S. Skrivebeskyttede
    filer slettes også. Returnerer antall slettede filer.
    """
    import stat as _stat
    root = Path(root)
    if not root.exists():
        return 0
    if w:
        w(f"  {label}: {root} …", "info")
    t0 = last_log = last_gui = time.monotonic()
    next_pct = LOG_STEP_PCT
    n = 0

    def _unlink(p: str) -> None:
        try:
            os.unlink(p)
        except PermissionError:
            try:
                os.chmod(p, _stat.S_IWRITE)
                os.unlink(p)
            except OSError:
                pass
        except OSError:
            pass

    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        for name in filenames:
            _unlink(os.path.join(dirpath, name))
            n += 1
        for name in dirnames:
            p = os.path.join(dirpath, name)
            try:
                if os.path.islink(p):
                    os.unlink(p)
                else:
                    os.rmdir(p)
            except OSError:
                pass
        now = time.monotonic()
        pct = int(n * 100 / total) if total else 0
        if w and (now - last_log >= RMTREE_LOG_INTERVAL_S
                  or (total and pct >= next_pct and pct < 100)):
            last_log = now
            el = now - t0
            if total:
                while next_pct <= pct:
                    next_pct += LOG_STEP_PCT
                eta = el * (total - n) / n if n else 0
                w(f"    {pct} % — {_n(n)}/{_n(total)} filer slettet, "
                  f"{fmt_duration(el)} brukt, ca. {fmt_duration(eta)} igjen", "info")
            else:
                w(f"    {_n(n)} filer slettet ({fmt_duration(el)} brukt)", "info")
        if pcb and now - last_gui >= GUI_INTERVAL_S:
            last_gui = now
            try:
                pcb("phase_progress", done=min(n, total or 0), total=total or 0,
                    text=f"{label}: {_n(n)} filer slettet", eta="")
            except Exception:
                pass
    try:
        os.rmdir(root)
    except OSError:
        pass
    if root.exists():
        import shutil
        shutil.rmtree(root, ignore_errors=True)
    if w and time.monotonic() - t0 >= 60:
        w(f"    {_n(n)} filer slettet på {fmt_duration(time.monotonic() - t0)}", "info")
    return n
