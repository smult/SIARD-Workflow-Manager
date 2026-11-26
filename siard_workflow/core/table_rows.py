"""
siard_workflow/core/table_rows.py

Bla i radene i en SIARD-tabell (content/schemaN/tableN/tableN.xml) uten å lese
hele fila inn i minnet.

Bakgrunn: eksempeldata i SIARDMapper-vinduet ble hentet ved å DOM-parse hele
tabellfila for hver visning. Det tok tid for store tabeller, og det gikk ikke
an å se annet enn de første radene.

TableRows gir sider med rader fra et vilkårlig sted i tabellen:
  - page_forward(start, limit)   rader fra og med `start`
  - page_backward(end, limit)    de siste radene før `end` (end=None = slutten)
  - key=...                      bare rader med ulikt innhold («Unike»)

Hurtigvei: radene telles ved å lete etter slutt-taggen (</row>) i rå bytes,
og XML-tolkingen starter først der siden begynner. Posisjoner som er funnet,
huskes, så videre blaing går raskt. Filer der dette ikke er trygt (CDATA,
annen koding enn UTF-8, uvanlig oppbygning) leses med vanlig strømtolking fra
starten — tregere, men riktig.
"""
from __future__ import annotations

import bisect
import itertools
import re
import sys
import threading
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, List, Optional, Tuple

_SKIP_CHUNK = 4 * 1024 * 1024      # byte-telling
_PARSE_CHUNK = 64 * 1024           # første blokk til XML-tolkeren (dobles opp til 1 MB)
_HEAD_MAX = 1024 * 1024            # så langt letes det etter første <row>
_ROW_RE = re.compile(rb"<((?:[A-Za-z_][\w.\-]*:)?row)(?=[\s/>])")
_CDATA = b"<![CDATA["


class Cancelled(Exception):
    """Forespørselen ble avbrutt (brukeren ba om noe annet imens)."""


class _NeedSlowPath(Exception):
    pass


@dataclass
class RowPage:
    rows: List[List[str]] = field(default_factory=list)
    numbers: List[int] = field(default_factory=list)   # 0-basert radnummer per rad
    scan_from: int = 0          # første rad som ble undersøkt
    scan_to: int = 0            # én forbi siste rad som ble undersøkt
    total: Optional[int] = None  # antall rader i tabellen, når det er kjent
    capped: bool = False        # søket etter unike rader stoppet ved grensen
    error: str = ""

    @property
    def at_start(self) -> bool:
        return self.scan_from <= 0

    @property
    def at_end(self) -> bool:
        return self.total is not None and self.scan_to >= self.total


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _cell_value(el) -> str:
    """Tekst i en celle. LOB-celler uten tekst vises med filreferansen."""
    if len(el) == 0:
        text = (el.text or "").strip()
    else:                                   # ARRAY/UDT: verdiene i underelementene
        text = " | ".join(t.strip() for t in el.itertext() if t and t.strip())
    if not text:
        ref = el.get("file")
        if ref:
            return f"→ {ref}"
    return text


class TableRows:
    """Sider med rader fra én tabellfil. `open_fn()` gir en binær, søkbar fil."""

    def __init__(self, open_fn: Callable[[], object], n_cols: int):
        self._open = open_fn
        self._n = n_cols
        self._lock = threading.Lock()
        self._prepared = False
        self._fast = True
        self._head = b""
        self._tok = b""
        self._marks_row: List[int] = []
        self._marks_pos: List[int] = []
        self.total: Optional[int] = None

    # ── Forberedelse ──────────────────────────────────────────────────────────

    def _prepare(self, f) -> None:
        if self._prepared:
            return
        f.seek(0)
        data = b""
        m = None
        while len(data) < _HEAD_MAX:
            chunk = f.read(64 * 1024)
            if not chunk:
                break
            data += chunk
            m = _ROW_RE.search(data)
            if m:
                break
        with self._lock:
            if self._prepared:
                return
            if m and _CDATA not in data[:m.start()]:
                self._head = data[:m.start()]
                self._tok = b"</" + m.group(1) + b">"
                self._marks_row = [0]
                self._marks_pos = [m.start()]
            else:
                self._fast = False          # tom tabell, UTF-16 eller uvanlig fil
            self._prepared = True

    def _use_slow_path(self) -> None:
        with self._lock:
            self._fast = False
            self._marks_row, self._marks_pos = [], []
            self.total = None

    def _add_mark(self, row: int, pos: int) -> None:
        with self._lock:
            i = bisect.bisect_left(self._marks_row, row)
            if i < len(self._marks_row) and self._marks_row[i] == row:
                return
            self._marks_row.insert(i, row)
            self._marks_pos.insert(i, pos)

    # ── Hopp til rad (teller slutt-tagger i rå bytes) ─────────────────────────

    def _skip_to(self, f, target: int, cancel, progress) -> Tuple[int, Optional[int]]:
        """
        Finn byte-posisjonen der rad `target` starter. Returnerer (rad, pos);
        pos er None når fila tok slutt før raden (da er self.total satt).
        """
        with self._lock:
            i = bisect.bisect_right(self._marks_row, target) - 1
            row, pos = self._marks_row[i], self._marks_pos[i]
            total = self.total
        if row == target:
            return row, pos
        if total is not None and target > total:
            return total, None
        tok, n_tok = self._tok, len(self._tok)
        f.seek(pos)
        tail = b""
        cd_tail = b""
        base = pos                           # posisjonen til data[0] i fila
        while True:
            if cancel and cancel():
                raise Cancelled()
            chunk = f.read(_SKIP_CHUNK)
            if not chunk:
                with self._lock:
                    self.total = row
                return row, None
            if _CDATA in chunk or _CDATA in cd_tail + chunk[:8]:
                raise _NeedSlowPath()        # </row> kan stå som tekst i CDATA
            cd_tail = chunk[-8:]
            data = tail + chunk
            cnt = data.count(tok)
            need = target - row
            if cnt >= need:
                idx = -n_tok
                for _ in range(need):
                    idx = data.find(tok, idx + n_tok)
                pos = base + idx + n_tok
                self._add_mark(target, pos)
                return target, pos
            if cnt:
                row += cnt
                self._add_mark(row, base + data.rfind(tok) + n_tok)
            tail = data[-(n_tok - 1):] if n_tok > 1 else b""
            base += len(data) - len(tail)
            if progress:
                progress(base)

    # ── Rader fra og med en gitt rad ──────────────────────────────────────────

    def _rows_once(self, f, start: int, cancel, progress) -> Iterator[Tuple[int, List[str]]]:
        parser = ET.XMLPullParser(events=("start", "end"))
        if self._fast:
            _row, pos = self._skip_to(f, start, cancel, progress)
            if pos is None:
                return
            f.seek(pos)
            try:
                parser.feed(self._head)
            except ET.ParseError:
                raise _NeedSlowPath()
            idx = start
        else:
            f.seek(0)
            idx = 0
        n = self._n
        depth = 0
        root = None
        chunk_size = _PARSE_CHUNK
        eof = False
        while not eof:
            if cancel and cancel():
                raise Cancelled()
            chunk = f.read(chunk_size)
            chunk_size = min(chunk_size * 2, 1024 * 1024)
            try:
                if chunk:
                    parser.feed(chunk)
                else:
                    eof = True
                    parser.close()
            except ET.ParseError:
                if self._fast:
                    raise _NeedSlowPath()
                eof = True                    # avkuttet/skadet fil: vis det som lot seg lese
            for ev, el in parser.read_events():
                if ev == "start":
                    depth += 1
                    if depth == 1:
                        root = el
                    continue
                depth -= 1
                if depth != 1 or _local(el.tag) != "row":
                    continue
                if idx >= start:
                    cells = [""] * n
                    for c in el:
                        ct = _local(c.tag)
                        if ct[:1] == "c" and ct[1:].isdigit():
                            ci = int(ct[1:]) - 1
                            if 0 <= ci < n:
                                cells[ci] = _cell_value(c)
                    yield idx, cells
                idx += 1
                if root is not None:
                    root.clear()
            if progress and not self._fast:
                try:
                    progress(f.tell())
                except Exception:
                    pass
        with self._lock:
            self.total = idx

    def _rows(self, f, start: int, cancel, progress) -> Iterator[Tuple[int, List[str]]]:
        """Rader (radnummer, celler) fra og med `start` til slutten av tabellen."""
        self._prepare(f)
        if self._fast:
            given = 0
            try:
                for item in self._rows_once(f, start, cancel, progress):
                    given += 1
                    yield item
                return
            except _NeedSlowPath:
                self._use_slow_path()
                start += given                # ikke gi de samme radene to ganger
        yield from self._rows_once(f, start, cancel, progress)

    # ── Offentlig ─────────────────────────────────────────────────────────────

    def count(self, cancel=None, progress=None) -> int:
        """Antall rader i tabellen (leser til slutten første gang)."""
        if self.total is not None:
            return self.total
        with self._open() as f:
            self._prepare(f)
            if self._fast:
                try:
                    self._skip_to(f, sys.maxsize, cancel, progress)
                except _NeedSlowPath:
                    self._use_slow_path()
            if self.total is None:
                for _ in self._rows_once(f, sys.maxsize, cancel, progress):
                    pass
        return self.total or 0

    def page_forward(self, start: int, limit: int, key=None, cancel=None,
                     progress=None, max_scan: int = 200_000,
                     max_seconds: float = 10.0) -> RowPage:
        """
        Inntil `limit` rader fra og med rad `start` (0-basert). Med `key`
        (celler → sammenligningsverdi) tas bare rader med en verdi som ikke
        alt er med; letingen stopper etter max_scan rader / max_seconds.
        """
        start = max(0, int(start))
        limit = max(1, int(limit))
        if self.total is not None and start >= self.total > 0:
            return self.page_backward(None, limit, key, cancel, progress,
                                      max_scan, max_seconds)
        page = RowPage(scan_from=start, scan_to=start)
        seen: set = set()
        t0 = time.monotonic()
        with self._open() as f:
            for idx, cells in self._rows(f, start, cancel, progress):
                if len(page.rows) >= limit:
                    break                     # det finnes flere rader etter siden
                page.scan_to = idx + 1
                if key is not None:
                    k = key(cells)
                    if k in seen:
                        if (idx + 1 - start >= max_scan
                                or time.monotonic() - t0 > max_seconds):
                            page.capped = True
                            break
                        continue
                    seen.add(k)
                page.rows.append(cells)
                page.numbers.append(idx)
        page.total = self.total
        if page.total is not None and start > page.total:
            page.scan_from = page.scan_to = page.total
        return page

    def page_backward(self, end: Optional[int], limit: int, key=None, cancel=None,
                      progress=None, max_scan: int = 200_000,
                      max_seconds: float = 10.0) -> RowPage:
        """
        De siste inntil `limit` radene før rad `end` (end=None: slutten av
        tabellen). Med `key`: de siste radene med ulik verdi, sett bakfra.
        """
        limit = max(1, int(limit))
        if end is None:
            end = self.count(cancel, progress)
        elif self.total is not None:
            end = min(int(end), self.total)
        if end <= 0:
            return self.page_forward(0, limit, key, cancel, progress,
                                     max_scan, max_seconds)
        if key is None:
            start = max(0, end - limit)
            return self.page_forward(start, end - start, None, cancel, progress)

        picked: List[Tuple[int, List[str]]] = []
        seen: set = set()
        block = max(limit * 20, 2000)
        hi = end
        scan_from = end
        capped = False
        t0 = time.monotonic()
        with self._open() as f:
            while hi > 0 and len(picked) < limit:
                lo = max(0, hi - block)
                rows = list(itertools.islice(
                    self._rows(f, lo, cancel, progress), hi - lo))
                for idx, cells in reversed(rows):
                    scan_from = idx
                    k = key(cells)
                    if k in seen:
                        continue
                    seen.add(k)
                    picked.append((idx, cells))
                    if len(picked) >= limit:
                        break
                else:
                    scan_from = lo
                    hi = lo
                    if hi > 0 and (end - hi >= max_scan
                                   or time.monotonic() - t0 > max_seconds):
                        capped = True
                        break
                    block = min(block * 2, 50_000)
        picked.reverse()
        return RowPage(rows=[c for _, c in picked], numbers=[i for i, _ in picked],
                       scan_from=scan_from, scan_to=end, total=self.total,
                       capped=capped)


# ── Finn og åpne tabellfila ───────────────────────────────────────────────────

class SiardZip:
    """Én åpen SIARD-fil delt mellom tabellene (innholdslista leses én gang)."""

    def __init__(self, siard_path):
        self._path = Path(siard_path) if siard_path else None
        self._lock = threading.Lock()
        self._zf: Optional[zipfile.ZipFile] = None
        self._index: Optional[dict] = None

    def _ensure(self) -> bool:
        with self._lock:
            if self._zf is not None:
                return True
            if not self._path or not self._path.is_file():
                return False
            zf = zipfile.ZipFile(self._path, "r", allowZip64=True)
            index: dict = {}
            for name in zf.namelist():
                parts = name.split("/")
                # content/<schema>/<tabell>/<fil>.xml
                if (len(parts) == 4 and parts[0] == "content"
                        and parts[3].lower().endswith(".xml")):
                    index.setdefault((parts[1].lower(), parts[2].lower()), name)
            self._zf, self._index = zf, index
            return True

    def find(self, schema_folder: str, folder: str) -> Optional[str]:
        if not self._ensure():
            return None
        folder = (folder or "").strip("/").lower()
        schema = (schema_folder or "").strip("/").lower()
        if schema and (schema, folder) in self._index:
            return self._index[(schema, folder)]
        for (_sch, tbl), name in self._index.items():
            if tbl == folder:
                return name
        return None

    def open(self, name: str):
        return self._zf.open(name, "r")

    def close(self) -> None:
        with self._lock:
            if self._zf is not None:
                try:
                    self._zf.close()
                except Exception:
                    pass
                self._zf = None
                self._index = None


def find_extracted_table(extracted_path, schema_folder: str, folder: str) -> Optional[Path]:
    """tableN.xml i en utpakket SIARD — uten å gå gjennom alle LOB-filene."""
    if not extracted_path:
        return None
    base = Path(extracted_path) / "content"
    clean = (folder or "").strip("/")
    if not clean or not base.is_dir():
        return None
    schema = (schema_folder or "").strip("/")
    if schema:
        p = base / schema / clean / f"{clean}.xml"
        if p.is_file():
            return p
    try:
        for pattern in (f"*/{clean}/{clean}.xml", f"*/{clean}/*.xml"):
            for p in sorted(base.glob(pattern)):
                if p.is_file():
                    return p
    except Exception:
        pass
    return None


def table_opener(folder: str, schema_folder: str, extracted_path,
                 siard_zip: Optional[SiardZip]) -> Callable[[], object]:
    """
    open_fn for TableRows: utpakket mappe først, ellers rett fra SIARD-fila.
    Stedet slås opp første gang fila åpnes (kan skje i en arbeidstråd).
    """
    found: dict = {}
    lock = threading.Lock()

    def _open():
        with lock:
            if not found:
                p = find_extracted_table(extracted_path, schema_folder, folder)
                if p is not None:
                    found["path"] = p
                else:
                    name = siard_zip.find(schema_folder, folder) if siard_zip else None
                    found["zip"] = name
        if found.get("path") is not None:
            return open(found["path"], "rb")
        if found.get("zip"):
            return siard_zip.open(found["zip"])
        raise FileNotFoundError(f"Fant ikke tabellfila for «{folder}»")

    return _open
