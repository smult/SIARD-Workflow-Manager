"""
siard_workflow/core/identifiers/siegfried.py
Siegfried/PRONOM-basert fildeteksjon.

Krever sf-binær (https://github.com/richardlehane/siegfried) installert.
Aktivert via config-nøkkelen `use_siegfried = True`.

Strategi:
  1. pre_scan(extract_dir): kjør `sf -multi 64 -json <dir>` én gang, cache resultat.
  2. identify(data|path): cache-oppslag når path er kjent; fallback til
     ad-hoc subprocess-kall ellers.
  3. is_encrypted: hybrid — bruk Siegfried-output, suppler med _detect_ole2_type
     for presis OLE2-kryptopodeteksjon (Siegfried's warning-felt er upresist
     for eldre Office-formater).
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Optional


# ── PRONOM PUID → kort ext-streng (matcher dagens semantikk) ─────────────────
#
# Tabellen mapper PRONOM-identifikatorer til samme korte ext-strenger som
# magic_bytes._detect() returnerer. Dette gjør at all nedstrøms-kode
# (_LO_CONVERTIBLE, _ARCHIVE_EXTS, ext-sammenligninger) fungerer uendret.

def _safe_unlink(p: Path, retries: int = 5, delay: float = 0.05) -> None:
    """
    Slett fil med retry — på Windows kan en nettopp avsluttet subprocess
    fortsatt holde lås kortvarig.
    """
    for _ in range(retries):
        try:
            p.unlink(missing_ok=True)
            return
        except (PermissionError, OSError):
            time.sleep(delay)
    # Siste forsøk — la unntak boble om vi fortsatt ikke får slettet
    try:
        p.unlink(missing_ok=True)
    except Exception:
        pass


_PUID_TO_EXT: dict[str, str] = {
    # Microsoft Word
    "fmt/37": "doc", "fmt/38": "doc", "fmt/39": "doc",
    "fmt/40": "doc", "fmt/609": "doc",
    "x-fmt/42": "doc", "x-fmt/43": "doc", "x-fmt/44": "doc", "x-fmt/45": "doc",
    "fmt/412": "docx", "fmt/523": "docx",
    # Microsoft Excel
    "fmt/55": "xls", "fmt/56": "xls", "fmt/57": "xls",
    "fmt/59": "xls", "fmt/61": "xls", "fmt/62": "xls",
    "fmt/175": "xls", "fmt/176": "xls", "fmt/177": "xls",
    "fmt/214": "xlsx", "fmt/445": "xlsx",
    # Microsoft PowerPoint
    "fmt/125": "ppt", "fmt/126": "ppt", "fmt/179": "ppt",
    "fmt/181": "ppt",
    "fmt/215": "pptx",
    # OpenDocument
    "fmt/136": "odt", "fmt/290": "odt", "fmt/291": "odt",
    "fmt/137": "ods", "fmt/294": "ods", "fmt/295": "ods",
    "fmt/138": "odp", "fmt/292": "odp", "fmt/293": "odp",
    "fmt/139": "odg", "fmt/296": "odg", "fmt/297": "odg",
    # PDF
    "fmt/14": "pdf", "fmt/15": "pdf", "fmt/16": "pdf",
    "fmt/17": "pdf", "fmt/18": "pdf", "fmt/19": "pdf", "fmt/20": "pdf",
    "fmt/95": "pdf", "fmt/276": "pdf",
    "fmt/354": "pdf",  # PDF/A-1a
    "fmt/476": "pdf", "fmt/477": "pdf", "fmt/478": "pdf",
    "fmt/479": "pdf", "fmt/480": "pdf", "fmt/481": "pdf",
    # RTF
    "fmt/45": "rtf", "fmt/50": "rtf", "fmt/52": "rtf",
    "fmt/53": "rtf", "fmt/355": "rtf", "fmt/969": "rtf",
    # WordPerfect
    "x-fmt/203": "wpd", "x-fmt/204": "wpd",
    "x-fmt/393": "wpd", "x-fmt/394": "wpd",
    "fmt/203": "wpd", "fmt/394": "wpd",
    # Bilder
    "fmt/11": "png", "fmt/12": "png", "fmt/13": "png",
    "fmt/41": "jpg", "fmt/42": "jpg", "fmt/43": "jpg", "fmt/44": "jpg",
    "fmt/645": "jpg",
    "fmt/3": "gif", "fmt/4": "gif",
    "fmt/115": "bmp", "fmt/116": "bmp", "fmt/117": "bmp",
    "x-fmt/25": "bmp",
    "fmt/152": "tiff", "fmt/153": "tiff", "fmt/154": "tiff",
    "fmt/155": "tiff", "fmt/156": "tiff",
    "fmt/353": "tiff", "fmt/387": "tiff",
    "x-fmt/399": "tiff",
    "fmt/92": "jp2", "fmt/93": "jp2",
    "fmt/91": "svg", "fmt/413": "svg", "fmt/414": "svg",
    "fmt/573": "webp",
    # Video
    "fmt/199": "mp4", "fmt/636": "mp4",
    "fmt/640": "mpg", "fmt/641": "mpg",
    "fmt/5": "avi", "fmt/648": "avi",
    "fmt/569": "m4v",
    # Arkiver
    "x-fmt/263": "zip", "fmt/289": "zip", "fmt/610": "zip",
    "x-fmt/266": "gz", "fmt/484": "7z",
    "x-fmt/265": "tar", "fmt/411": "rar",
    "fmt/410": "rar",
    # Tekst / markup
    "fmt/101": "xml", "fmt/801": "xml",
    "fmt/96": "html", "fmt/97": "html", "fmt/98": "html", "fmt/99": "html",
    "fmt/100": "html", "fmt/471": "html",
    "x-fmt/16": "txt", "x-fmt/21": "txt", "x-fmt/22": "txt",
    "fmt/111": "txt",
    # CSV
    "x-fmt/18": "csv", "fmt/800": "csv",
    # Lyd
    "fmt/134": "mp3", "fmt/141": "wav", "fmt/142": "flac",
    "fmt/138": "odp",  # konflikt fanget — odp har prioritet over flac
    "fmt/279": "flac",
    "fmt/203": "ogg",  # konflikt fanget — wpd har prioritet
    "fmt/541": "ogg", "fmt/667": "ogg",
    # E-post
    "fmt/278": "eml",
    "x-fmt/430": "msg", "fmt/950": "msg",
    # Geospatial / arkiv-spesifikt (norske formater)
    "fmt/700": "warc", "fmt/289": "warc",
    "fmt/802": "gml", "fmt/803": "gml",
    "fmt/824": "ifc", "fmt/825": "ifc",
    # MS Write (legacy)
    "x-fmt/393": "wri",
    # Generisk binær når ikke klassifisert
    # (puid "UNKNOWN" eller manglende match → "bin")
}


class SiegfriedIdentifier:
    """Siegfried/PRONOM-backend."""

    name = "siegfried"

    def __init__(self) -> None:
        from settings import get_config
        self.sf_exe: str = str(get_config("sf_executable", "") or "")
        self._cache: dict[str, tuple[str, str, bool]] = {}
        # Valgfri logger — settes utenfra (SiegfriedIdLogger). Hver
        # identifikasjon skrives som én rad: filnavn, ext, mime, PUID, ...
        self.logger = None
        # Sett av allerede loggede stier — unngå duplikater når samme fil
        # treffer både pre_scan-cache og senere identify(path=)-oppslag.
        self._logged_paths: set[str] = set()

    def set_logger(self, logger) -> None:
        """Sett logger som mottar én rad per identifisert fil."""
        self.logger = logger

    # ── Tilgjengelighet ──────────────────────────────────────────────────────

    def is_available(self) -> bool:
        if not self.sf_exe:
            return False
        if not Path(self.sf_exe).exists():
            return False
        return True

    # ── Forhåndsskann ────────────────────────────────────────────────────────

    # Windows CreateProcessW har 32k char-limit på kommandolinje. Med
    # romslig margin (~20k) og avg. ~150 chars per absolutt sti gir det
    # ~130 filer per batch. Bruker 100 for ekstra slingringsmonn.
    _SF_BATCH_SIZE = 100

    def pre_scan(self, root: Path,
                 files: Optional[list] = None,
                 max_workers: Optional[int] = None,
                 progress_cb: Optional[Callable[[int, int], None]] = None
                 ) -> None:
        """
        Fyll cache med Siegfried-resultater.

        Hvis `files` er gitt: batch-kall sf med eksplisitt fil-liste
        (anbefalt — unngår skann av irrelevante tableX.xml/metadata-filer).
        Hvis `files` er None: skann hele `root` rekursivt (fallback).

        max_workers: antall parallelle sf-prosesser (kun ved file-list-modus).
            None betyr at backenden velger selv (os.cpu_count()).
        progress_cb(done, total): kalles etter hver fullført batch.
        """
        if not self.is_available():
            return
        if files is not None:
            self._scan_file_list(files, max_workers=max_workers,
                                 progress_cb=progress_cb)
        else:
            self._scan_directory(Path(root))

    def _scan_file_list(self, files: list,
                        max_workers: Optional[int] = None,
                        progress_cb: Optional[Callable[[int, int], None]] = None
                        ) -> None:
        """Batch-kall til sf med eksplisitt fil-liste.

        - Path-prep: kun str-konvertering, ingen exists/resolve-syscall per fil.
          (Kalleren har allerede filsystem-validert listen.)
        - Subprocess-batcher kjøres parallelt i en ThreadPoolExecutor.
        - Cache-update beskyttes av lock.
        - Etter hver fullført batch kalles progress_cb(done, total).
        """
        # Ren str-konvertering — ingen syscalls per fil
        abs_paths = [str(Path(f)) for f in files if f]
        if not abs_paths:
            return

        # Del opp i batcher
        batches = [abs_paths[i:i + self._SF_BATCH_SIZE]
                   for i in range(0, len(abs_paths), self._SF_BATCH_SIZE)]
        n_total = len(batches)

        sf_exe = self.sf_exe
        cache_lock = threading.Lock()

        def _run(chunk: list[str]) -> "dict | None":
            try:
                proc = subprocess.run(
                    [sf_exe, "-multi", "64", "-json", *chunk],
                    capture_output=True, text=True, timeout=600,
                    encoding="utf-8", errors="replace",
                )
                if proc.returncode != 0 or not proc.stdout:
                    return None
                return json.loads(proc.stdout)
            except Exception:
                return None

        # max_workers=None → bruk cpu_count (clamp 4..16 som rimelig default)
        if max_workers is None or max_workers <= 0:
            max_workers = max(4, min(16, os.cpu_count() or 4))
        workers = max(1, min(int(max_workers), n_total))

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=workers) as pool:
            futs = {pool.submit(_run, c): i for i, c in enumerate(batches)}
            n_done = 0
            for fut in concurrent.futures.as_completed(futs):
                n_done += 1
                try:
                    result = fut.result()
                except Exception:
                    result = None
                if result:
                    with cache_lock:
                        self._ingest_sf_json(result)
                if progress_cb:
                    try:
                        progress_cb(n_done, n_total)
                    except Exception:
                        pass

    def _scan_directory(self, root: Path) -> None:
        """Rekursiv mappe-skann (fallback). Skanner ALT under root."""
        if not root.exists():
            return
        try:
            proc = subprocess.run(
                [self.sf_exe, "-multi", "64", "-json", str(root)],
                capture_output=True, text=True, timeout=1800,
                encoding="utf-8", errors="replace",
            )
            if proc.returncode != 0 or not proc.stdout:
                return
            self._ingest_sf_json(json.loads(proc.stdout))
        except Exception:
            return

    @staticmethod
    def _cache_key(path) -> str:
        """
        Normalisert cache-nøkkel — case-folding på Windows for å gjøre
        oppslag konsistente. INGEN resolve() (krever ikke syscall).
        Kalleren må sørge for at stien er absolutt før input til både
        pre_scan og identify (typisk via extract_dir / zip_sti).
        """
        return os.path.normcase(os.path.normpath(str(path)))

    def _ingest_sf_json(self, result: dict) -> None:
        """Felles parser av sf-JSON-output — fyller cache og logger."""
        for f in result.get("files", []):
            path = f.get("filename", "")
            if not path:
                continue
            matches = f.get("matches", [])
            ext, mime, enc = self._parse_match(matches)
            key = self._cache_key(path)
            self._cache[key] = (ext, mime, enc)
            self._log_match(path, ext, mime, matches)

    # ── Hovedfunksjon ────────────────────────────────────────────────────────

    def identify(self, data: Optional[bytes] = None,
                 path: Optional[Path] = None) -> tuple[str, str, bool]:
        # 1. Cache-oppslag når path er kjent
        if path is not None:
            cached = self._cache.get(self._cache_key(path))
            if cached is not None:
                return self._hybrid_encrypt(cached, data, path)

        # 2. Ad-hoc per-fil-kall — krever sf
        if not self.is_available():
            # Fallback: bruk magic-bytes-deteksjon hvis sf er utilgjengelig
            from siard_workflow.core.identifiers.magic_bytes import _detect
            if data is None and path is not None:
                try:
                    data = Path(path).read_bytes()[:65536]
                except Exception:
                    return ("bin", "application/octet-stream", False)
            return _detect(data or b"")

        # 3. Skriv data til temp-fil hvis kun bytes er gitt, ellers bruk path
        if path is not None and Path(path).exists():
            return self._sf_single(Path(path), data)
        if data is not None:
            # mkstemp() returnerer (fd, path) — vi MÅ lukke fd umiddelbart
            # ellers holder Windows låsen og unlink() feiler.
            fd, tmp_name = tempfile.mkstemp(prefix="sf_id_", suffix=".bin")
            os.close(fd)
            tmp = Path(tmp_name)
            try:
                tmp.write_bytes(data)
                return self._sf_single(tmp, data)
            finally:
                _safe_unlink(tmp)

        return ("bin", "application/octet-stream", False)

    # ── Subprocess-wrapper ──────────────────────────────────────────────────

    def _sf_single(self, path: Path,
                   data: Optional[bytes]) -> tuple[str, str, bool]:
        try:
            proc = subprocess.run(
                [self.sf_exe, "-json", str(path)],
                capture_output=True, text=True, timeout=30,
                encoding="utf-8", errors="replace",
            )
            if proc.returncode != 0 or not proc.stdout:
                return ("bin", "application/octet-stream", False)
            result = json.loads(proc.stdout)
        except Exception:
            return ("bin", "application/octet-stream", False)

        files = result.get("files", [])
        if not files:
            return ("bin", "application/octet-stream", False)
        matches = files[0].get("matches", [])
        base = self._parse_match(matches)
        self._log_match(str(path), base[0], base[1], matches)
        return self._hybrid_encrypt(base, data, path)

    # ── Logging-hjelpefunksjon ───────────────────────────────────────────────

    def _log_match(self, filename: str, ext: str, mime: str,
                   matches: list) -> None:
        """Skriv én linje til konfigurert logger (no-op hvis logger=None)."""
        if self.logger is None:
            return
        if filename in self._logged_paths:
            return
        self._logged_paths.add(filename)
        m = matches[0] if matches else {}
        try:
            self.logger.write(
                filename=filename,
                ext=ext,
                mime=mime,
                puid=m.get("id", "") or "",
                format_name=m.get("format", "") or "",
                basis=m.get("basis", "") or "",
                warning=m.get("warning", "") or "",
            )
        except Exception:
            pass

    # ── Resultat-parser ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_match(matches: list) -> tuple[str, str, bool]:
        if not matches:
            return ("bin", "application/octet-stream", False)
        m = matches[0]
        puid = m.get("id", "")
        ext = _PUID_TO_EXT.get(puid, "bin")
        mime = m.get("mime") or "application/octet-stream"
        warn = (m.get("warning") or "").lower()
        # Siegfried setter "encrypted" eller "password" i warning for noen formater
        enc = "encrypt" in warn or "password" in warn
        return (ext, mime, enc)

    # ── Hybrid kryptering-deteksjon ─────────────────────────────────────────

    @staticmethod
    def _hybrid_encrypt(base: tuple[str, str, bool],
                        data: Optional[bytes],
                        path: Optional[Path]) -> tuple[str, str, bool]:
        """
        For OLE2-formater (doc/xls/ppt + krypterte docx/xlsx/pptx) brukes
        magic_bytes._detect_ole2_type for presis kryptopodeteksjon.
        """
        ext, mime, enc = base
        if enc:
            return base
        if ext not in ("doc", "xls", "ppt", "docx", "xlsx", "pptx"):
            return base
        if data is None and path is not None:
            try:
                data = Path(path).read_bytes()[:65536]
            except Exception:
                return base
        if not data:
            return base
        # OLE2-deteksjon krever \xd0\xcf\x11\xe0-header
        if data[:4] != b"\xd0\xcf\x11\xe0":
            return base
        from siard_workflow.core.identifiers.magic_bytes import _detect_ole2_type
        ole = _detect_ole2_type(data)
        if ole is not None and ole[2]:
            return (ext, mime, True)
        return base
