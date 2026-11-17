"""
siard_workflow/core/file_logger.py
Skriver logg til fil automatisk under workflow-kjøring.

Tidsstempler er HH:MM:SS. For at lange kjøringer (flere døgn) skal kunne
leses, skrives en datolinje hver gang døgnet skifter. Et livstegn
(HEARTBEAT_S, standard hver halvtime) skrives så lenge loggen er åpen:
hvilket steg som kjører, hvor lenge, siste loggmelding og minnebruk —
uavhengig av operasjonens egne fremdriftsmeldinger.
"""
from __future__ import annotations
import datetime
import logging
import re
import threading
import time
from pathlib import Path
from typing import Callable, Optional

HEARTBEAT_S = 30 * 60

_WEEKDAYS = ("mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag")


def resolve_log_dir(siard_path: Path, fallback: "Path | None" = None) -> Path:
    """
    Mappa kjøringens loggfiler skrives til: «Logg-mappe» fra Innstillinger
    (config «log_dir»), ellers `fallback`, ellers mappa SIARD-fila ligger i.
    """
    try:
        from settings import get_config
        cfg = str(get_config("log_dir", "") or "").strip()
    except Exception:
        cfg = ""
    if cfg:
        return Path(cfg)
    return Path(fallback) if fallback else Path(siard_path).parent


# ── Fremdriftslinjer ─────────────────────────────────────────────────────────
# Loggfila skal kunne leses fra topp til bunn. Fremdrift («Batch 12/1522»,
# «Detektert: 4 236/211 836 filer», «10 % — …») vises fortløpende i GUI-et, men
# i fila skrives bare første linje, hver fjerdedel og siste linje i en serie.
_RE_BATCH = re.compile(r"^(\s*)Batch (\d+)/(\d+): ")
_NUM = r"\d[\d   ,.]*?"
_RE_PROGRESS = re.compile(
    rf"({_NUM})\s*/\s*({_NUM})\s+(filer|linjer|rader|batcher)\b")
_RE_PCT = re.compile(r"\d+\s*%")


def _to_int(text: str) -> int:
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else 0


def _fmt_duration(sec: float) -> str:
    d, rest = divmod(int(sec), 86400)
    h, rest = divmod(rest, 3600)
    m = rest // 60
    if d:
        return f"{d} d {h} t {m} min"
    return f"{h} t {m} min" if h else f"{m} min"


class WorkflowFileLogger:
    """
    Kontekstmanager som logger til fil under en workflow-kjøring.

    Bruk:
        with WorkflowFileLogger(log_dir) as wfl:
            wfl.log("melding", "info")
            run = wf.execute(...)
    """

    # Operasjonene bruker både «success»/«error» og de norske «ok»/«feil»
    LEVELS = {"info": "INFO", "step": "STEP", "success": "OK  ", "ok": "OK  ",
              "warn": "WARN", "advarsel": "WARN", "error": "ERR ", "feil": "ERR ",
              "muted": "----"}
    MAX_NOTABLE = 200

    def __init__(self, log_dir: Path, siard_name: str = "",
                 heartbeat_s: float = HEARTBEAT_S,
                 on_heartbeat: "Optional[Callable[[str], None]]" = None,
                 header_info: "Optional[list]" = None,
                 thin_progress: bool = True):
        self.header_info = list(header_info or [])   # [(ledetekst, verdi), …]
        self.thin_progress = thin_progress
        self._prog: dict = {}        # serie → (sist ferdig, fjerdedel)
        self._series: dict = {}      # batch-serie → antall linjer sett
        self.suppressed = 0          # fremdriftslinjer som ikke ble skrevet
        self.warnings: list = []     # [(klokkeslett, melding)]
        self.errors: list = []
        self.n_warnings = 0
        self.n_errors = 0
        self.log_dir   = Path(log_dir)
        self.siard_name = siard_name
        self._path: Path | None = None
        self._fh = None
        self._lock = threading.Lock()
        self._day: "datetime.date | None" = None
        self._last_msg = ""
        self._last_time: "datetime.datetime | None" = None
        self._step = ""
        self._step_name = ""
        self._step_started = 0.0
        self._started = 0.0
        self._heartbeat_s = heartbeat_s
        self.on_heartbeat = on_heartbeat        # f.eks. ekko til GUI-loggen
        self._stop = threading.Event()
        self._hb_thread: "threading.Thread | None" = None

    def __enter__(self):
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = self.siard_name or "workflow"
        self._path = self.log_dir / f"{stem}_{ts}.log"
        self._fh = open(self._path, "w", encoding="utf-8")
        self._write_header()
        self._day = datetime.date.today()
        self._started = self._step_started = time.monotonic()
        if self._heartbeat_s and self._heartbeat_s > 0:
            self._hb_thread = threading.Thread(target=self._heartbeat_loop,
                                               name="logg-livstegn", daemon=True)
            self._hb_thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        with self._lock:
            if self._fh:
                self._write_line(f"\n[{self._ts()}] Logg avsluttet\n")
                self._fh.close()
                self._fh = None

    @property
    def log_path(self) -> Path | None:
        return self._path

    def set_step(self, label: str) -> None:
        """Steget som kjører nå (brukes i livstegnet)."""
        self._step = label
        self._step_name = label.split("] ", 1)[-1].strip()
        self._step_started = time.monotonic()

    def log(self, message: str, level: str = "info", raw: bool = False) -> None:
        """raw=True: skriv linja som den er (sammendraget bruker skillelinjer)."""
        if not self._fh:
            return
        tag = self.LEVELS.get(level, "INFO")
        if not raw and self._step:
            # Operasjonene skriver et eget banner («=====» / NAVN / «=====»)
            # rett etter stegoverskriften «[3] Navn» — det sier det samme
            bare = message.strip()
            if bare and set(bare) == {"="}:
                return
            if tag == "STEP" and bare.upper() == self._step_name.upper():
                return
        if message.strip():
            # Livstegnet viser siste melding — også fremdrift som ikke skrives
            self._last_msg = message.strip()
            self._last_time = datetime.datetime.now()
        # Flerlinjede meldinger (f.eks. stderr fra et eksternt program) blir
        # én linje, så hver linje i fila har klokkeslett og nivå
        if "\n" in message.strip("\n"):
            indent = message[:len(message) - len(message.lstrip())].replace("\n", "")
            message = indent + " ".join(
                p.strip() for p in message.splitlines() if p.strip())
        if self.thin_progress and tag == "INFO":
            message = self._thin(message)
            if message is None:
                self.suppressed += 1
                return
        if tag in ("WARN", "ERR "):
            self._note(tag, message)
        with self._lock:
            if not self._fh:
                return
            self._write_line(f"[{self._ts()}] [{tag}] {message}\n")

    def _note(self, tag: str, message: str) -> None:
        """Hold rede på advarsler og feil — til sluttsammendraget."""
        text = message.strip()
        if not text:
            return
        if tag == "WARN":
            self.n_warnings += 1
            bucket = self.warnings
        else:
            self.n_errors += 1
            bucket = self.errors
        if len(bucket) < self.MAX_NOTABLE:
            bucket.append((self._ts(), text))

    def _thin(self, message: str) -> "str | None":
        """
        Fremdriftslinje → None når den ikke skal i fila. Første linje, hver
        fjerdedel og siste linje i en serie skrives. Andre linjer urørt.
        """
        m = _RE_BATCH.match(message)
        if m:
            total = int(m.group(3))
            key = ("batch", total)
            seen = self._series.get(key, 0) + 1
            if seen > total:                 # ny serie med samme antall
                seen = 1
            self._series[key] = seen
            if total <= 8:
                return message
            if seen == total or (4 * seen) // total > (4 * (seen - 1)) // total:
                pct = 100 * seen // total
                return f"{m.group(1)}Batcher ferdig: {seen}/{total} ({pct} %)"
            return None
        m = _RE_PROGRESS.search(message)
        if not m:
            return message
        done, total = _to_int(m.group(1)), _to_int(m.group(2))
        if total <= 0:
            return message
        key = _RE_PCT.sub("", message[:m.start()]).strip() + "|" + m.group(3)
        prev = self._prog.get(key)
        bucket = min(4, (4 * done) // total)
        self._prog[key] = (done, bucket)
        if prev is None or done < prev[0]:   # første linje i (en ny) serie
            return message
        if done >= total and prev[0] < total:
            return message
        if bucket > prev[1] and done < total:
            return message
        return None

    def heartbeat(self) -> str:
        """Skriv ett livstegn nå og returner teksten."""
        now = datetime.datetime.now()
        parts = [f"Fortsatt i gang — totalt {_fmt_duration(time.monotonic() - self._started)}"]
        if self._step:
            parts.append(f"steg «{self._step}» i "
                         f"{_fmt_duration(time.monotonic() - self._step_started)}")
        if self._last_time:
            ago = _fmt_duration((now - self._last_time).total_seconds())
            parts.append(f"siste loggmelding {self._last_time:%d.%m %H:%M:%S} "
                         f"({ago} siden): «{self._last_msg[:160]}»")
        mem = _memory_text()
        if mem:
            parts.append(mem)
        msg = "LIVSTEGN: " + "; ".join(parts)
        with self._lock:
            if self._fh:
                self._write_line(f"[{self._ts()}] [----] {msg}\n")
        return msg

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self._heartbeat_s):
            try:
                msg = self.heartbeat()
                if self.on_heartbeat:
                    self.on_heartbeat(msg)
            except Exception:
                logging.getLogger(__name__).debug("livstegn feilet", exc_info=True)

    def _write_line(self, line: str) -> None:
        """Skriv (låst) — med datolinje først når døgnet har skiftet."""
        today = datetime.date.today()
        if self._day is not None and today != self._day:
            self._fh.write(f"---------- {_WEEKDAYS[today.weekday()]} "
                           f"{today:%d.%m.%Y} ----------\n")
        self._day = today
        self._fh.write(line)
        self._fh.flush()

    def _ts(self) -> str:
        return datetime.datetime.now().strftime("%H:%M:%S")

    def _write_header(self):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._fh.write("=" * 60 + "\n")
        self._fh.write(f"  SIARD Workflow Log\n")
        self._fh.write(f"  Fil:       {self.siard_name}\n")
        self._fh.write(f"  Startet:   {now}\n")
        for label, value in self.header_info:
            self._fh.write(f"  {(str(label) + ':'):<10} {value}\n")
        self._fh.write("=" * 60 + "\n\n")


def _memory_text() -> str:
    """«minne: prosess 1,2 GB, ledig 3,4 av 16,0 GB» (psutil), ellers tom."""
    try:
        import psutil
        rss = psutil.Process().memory_info().rss
        vm = psutil.virtual_memory()
        gb = lambda n: f"{n / 1024 ** 3:.1f}".replace(".", ",")
        return f"minne: prosess {gb(rss)} GB, ledig {gb(vm.available)} av {gb(vm.total)} GB"
    except Exception:
        return ""
