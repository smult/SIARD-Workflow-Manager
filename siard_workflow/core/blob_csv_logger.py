"""
siard_workflow/core/blob_csv_logger.py
CSV-logg for BLOB-konverteringsdetaljer, og feil-logg for konverteringsfeil.

Format per linje (CSV):
  fra_fil, fra_størrelse, fra_ext, til_fil, til_størrelse, til_ext, kommentar
"""
from __future__ import annotations
import csv
import datetime
import re
from pathlib import Path


import threading as _threading

class BlobCsvLogger:
    """
    Skriver én CSV-rad per behandlet fil.
    Åpnes med __enter__ / __exit__ (kontekstmanager).
    """

    HEADER = [
        "fra_fil", "fra_storrelse", "fra_ext",
        "til_fil",  "til_storrelse",  "til_ext",
        "kommentar",
    ]

    def __init__(self, log_dir: Path, siard_name: str = ""):
        self.log_dir    = Path(log_dir)
        self.siard_name = siard_name
        self._path: Path | None = None
        self._fh   = None
        self._writer = None
        self._lock   = _threading.Lock()   # beskytt mot parallell skriving

    def __enter__(self) -> "BlobCsvLogger":
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = self.siard_name or "blob"
        self._path   = self.log_dir / f"{stem}_{ts}_blob_konvertering.csv"
        self._fh     = open(self._path, "w", encoding="utf-8-sig", newline="")
        self._writer = csv.writer(self._fh, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        self._writer.writerow(self.HEADER)
        self._fh.flush()
        return self

    def __exit__(self, *_):
        if self._fh:
            self._fh.close()

    @property
    def log_path(self) -> Path | None:
        return self._path

    def write(self,
              fra_fil:       str,
              fra_storrelse: int,
              fra_ext:       str,
              til_fil:       str,
              til_storrelse: int,
              til_ext:       str,
              kommentar:     str = "") -> None:
        if not self._writer:
            return
        with self._lock:
            self._writer.writerow([
                fra_fil, fra_storrelse, fra_ext,
                til_fil, til_storrelse, til_ext,
                kommentar,
            ])
            self._fh.flush()


class ConversionErrorLogger:
    """
    Samlet register over filer som IKKE ble konvertert som forventet, med
    kategori og årsak. Brukes av alle løp i BLOB-konverteringen (LibreOffice-
    batcher, e-post, formatoppgradering, arkiver, manglende referanser …).

    Filer (når log_dir er satt):
      {siard_stem}_{ts}_konvertering_feil.log   lesbar logg + oppsummering
      {siard_stem}_{ts}_konvertering_feil.csv   komplett liste (Excel, «;»)

    Uten log_dir holdes registeret bare i minnet (kjørerapporten bruker
    `summary()` uansett).

    Logglinje:  [TID]  filsti  |  ext  |  kategori  |  feilmelding
    """

    # Rekkefølgen er visningsrekkefølgen i logg og rapport
    CATEGORIES: dict = {
        "konverteringsfeil": "Konverteringsfeil",
        "tidsavbrudd":       "Tidsavbrudd under konvertering",
        "skadet":            "Skadet fil / kan ikke åpnes",
        "tom":               "Tom fil (0 byte)",
        "passordbeskyttet":  "Passordbeskyttet (ikke konvertert)",
        "mangler":           "Manglende fil (referert, finnes ikke)",
        "oppgradering":      "Formatoppgradering feilet",
    }
    CSV_HEADER = ["fil", "plassering", "format", "kategori", "arsak"]
    MAX_ENTRIES_IN_MEMORY = 20000

    _DAMAGED_HINTS = ("could not be loaded", "kunne ikke åpne", "kunne ikke leses",
                      "kunne ikke pakkes ut", "kunne ikke tolkes", "korrupt",
                      "skadet", "not an ole2", "olefileerror", "badzipfile",
                      "bad magic", "truncated", "unexpected end")

    def __init__(self, log_dir: "Path | None", siard_name: str = ""):
        self.log_dir    = Path(log_dir) if log_dir else None
        self.siard_name = siard_name
        self._path: Path | None = None
        self._csv_path: Path | None = None
        self._fh   = None
        self._csv_fh = None
        self._csv = None
        self._lock  = _threading.Lock()
        self._count = 0
        self._counts: dict = {}
        self._entries: list = []
        self._seen: set = set()
        self._root: Path | None = None
        self._names: "dict | None" = None

    # ── filer ────────────────────────────────────────────────────────────────

    def __enter__(self) -> "ConversionErrorLogger":
        if not self.log_dir:
            return self
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts         = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        stem       = self.siard_name or "blob"
        self._path = self.log_dir / f"{stem}_{ts}_konvertering_feil.log"
        self._csv_path = self.log_dir / f"{stem}_{ts}_konvertering_feil.csv"
        self._fh   = open(self._path, "w", encoding="utf-8", newline="")
        self._fh.write(
            f"# Konverteringsfeil — {stem} — {datetime.datetime.now()}\n"
            f"# Format: [TID]  filsti (relativ i SIARD)  |  ext  |  kategori  |  feilmelding\n"
            f"# Komplett liste i regneark: {self._csv_path.name}\n"
            f"# {'='*60}\n")
        self._fh.flush()
        self._csv_fh = open(self._csv_path, "w", encoding="utf-8-sig", newline="")
        self._csv = csv.writer(self._csv_fh, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        self._csv.writerow(self.CSV_HEADER)
        return self

    def __exit__(self, *_):
        with self._lock:
            fh, cfh = self._fh, self._csv_fh
            self._fh = self._csv_fh = self._csv = None
        if fh:
            try:
                if self._count == 0:
                    fh.close()
                    self._path.unlink(missing_ok=True)
                    self._path = None
                else:
                    fh.write(f"# {'='*60}\n# OPPSUMMERING — {self._count} fil(er):\n")
                    for key, label in self.CATEGORIES.items():
                        if self._counts.get(key):
                            fh.write(f"#   {label:<40} {self._counts[key]:>7,}\n")
                    fh.close()
            except Exception:
                pass
        if cfh:
            try:
                cfh.close()
                if self._count == 0:
                    self._csv_path.unlink(missing_ok=True)
                    self._csv_path = None
            except Exception:
                pass

    @property
    def log_path(self) -> Path | None:
        return self._path

    @property
    def csv_path(self) -> Path | None:
        return self._csv_path

    @property
    def count(self) -> int:
        return self._count

    # ── plassering (schema.tabell.kolonne) ───────────────────────────────────

    def attach_siard(self, root: Path) -> None:
        """Utpakket SIARD-mappe: gir tabell-/kolonnenavn i registeret."""
        self._root = Path(root)
        self._names = None

    def _name_map(self) -> dict:
        if self._root is None:
            return {}
        if self._names is not None:
            return self._names
        names: dict = {}
        try:
            import xml.etree.ElementTree as ET
            meta = self._root / "header" / "metadata.xml" if self._root else None
            if meta and meta.exists():
                def ln(el):
                    return el.tag.rsplit("}", 1)[-1]

                def child(el, name):
                    return next((c for c in el if ln(c) == name), None)

                def text(el, name):
                    c = child(el, name)
                    return (c.text or "").strip() if c is not None else ""

                root = ET.parse(meta).getroot()
                for sch in root.iter():
                    if ln(sch) != "schema":
                        continue
                    s_name, s_folder = text(sch, "name"), text(sch, "folder")
                    tables = child(sch, "tables")
                    for tab in (tables if tables is not None else []):
                        t_name, t_folder = text(tab, "name"), text(tab, "folder")
                        key = f"{s_folder}/{t_folder}"
                        names[key] = f"{s_name}.{t_name}" if s_name else t_name
                        cols = child(tab, "columns")
                        for col in (cols if cols is not None else []):
                            lf = text(col, "lobFolder").strip("/")
                            if lf:
                                last = lf.split("/")[-1]
                                names[f"{key}/{last}"] = f"{names[key]}.{text(col, 'name')}"
        except Exception:
            pass
        self._names = names
        return names

    def location(self, path: str) -> str:
        m = re.search(r"(schema[^/\\]+)[/\\](table[^/\\]+)(?:[/\\](lob[^/\\]+))?", path or "")
        if not m:
            return ""
        s, t, lob = m.groups()
        names = self._name_map()
        if lob and f"{s}/{t}/{lob}" in names:
            return names[f"{s}/{t}/{lob}"]
        return names.get(f"{s}/{t}", f"{s}/{t}" + (f"/{lob}" if lob else ""))

    # ── registrering ─────────────────────────────────────────────────────────

    @classmethod
    def classify(cls, error_msg: str) -> str:
        m = (error_msg or "").lower()
        if "tidsavbrudd" in m or "timeout" in m or "timed out" in m:
            return "tidsavbrudd"
        if any(h in m for h in cls._DAMAGED_HINTS):
            return "skadet"
        return "konverteringsfeil"

    def write(self, filename: str, ext: str, error_msg: str,
              category: "str | None" = None) -> None:
        """Registrer én fil som ikke ble konvertert (kategori utledes ellers)."""
        msg = (error_msg or "ukjent feil").replace("\n", " ").strip()
        cat = category if category in self.CATEGORIES else self.classify(msg)
        loc = self.location(filename)
        ts  = datetime.datetime.now().strftime("%H:%M:%S")
        with self._lock:
            key = (filename, cat)
            if key in self._seen:          # samme fil, samme kategori: én gang
                return
            self._seen.add(key)
            self._count += 1
            self._counts[cat] = self._counts.get(cat, 0) + 1
            if len(self._entries) < self.MAX_ENTRIES_IN_MEMORY:
                self._entries.append({"fil": filename, "plassering": loc,
                                      "format": ext, "kategori": cat, "arsak": msg})
            if self._fh:
                self._fh.write(f"[{ts}]  {filename:<60}  |  {ext:<8}  |  "
                               f"{cat:<17}  |  {msg}\n")
                self._fh.flush()
            if self._csv:
                self._csv.writerow([filename, loc, ext, self.CATEGORIES[cat], msg])
                self._csv_fh.flush()

    def summary(self, max_entries: int = 1000) -> dict:
        """Til kjørerapport/resultat: antall per kategori + (begrenset) liste."""
        with self._lock:
            return {
                "total":    self._count,
                "counts":   {k: self._counts[k] for k in self.CATEGORIES if self._counts.get(k)},
                "labels":   dict(self.CATEGORIES),
                "entries":  list(self._entries[:max_entries]),
                "truncated": max(0, self._count - min(max_entries, len(self._entries))),
                "log_path": str(self._path) if self._path else "",
                "csv_path": str(self._csv_path) if self._csv_path else "",
            }


class SiegfriedIdLogger:
    """
    Loggfører Siegfried/PRONOM-identifikasjon per fil.

    Navnmønster: {siard_stem}_{ts}_siegfried_identifikasjon.log
    CSV-format (semikolon-separert):
        filnavn ; ext ; mime ; PUID ; format ; basis ; warning
    """

    HEADER = ["filnavn", "ext", "mime", "puid", "format", "basis", "warning"]

    def __init__(self, log_dir: Path, siard_name: str = ""):
        self.log_dir    = Path(log_dir)
        self.siard_name = siard_name
        self._path: Path | None = None
        self._fh = None
        self._writer = None
        self._lock  = _threading.Lock()
        self._count = 0

    def __enter__(self) -> "SiegfriedIdLogger":
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = self.siard_name or "siard"
        self._path = self.log_dir / f"{stem}_{ts}_siegfried_identifikasjon.log"
        self._fh   = open(self._path, "w", encoding="utf-8-sig", newline="")
        self._writer = csv.writer(self._fh, delimiter=";",
                                   quoting=csv.QUOTE_MINIMAL)
        self._writer.writerow(self.HEADER)
        self._fh.flush()
        return self

    def __exit__(self, *_):
        if self._fh:
            if self._count == 0:
                try:
                    self._fh.close()
                    if self._path:
                        self._path.unlink(missing_ok=True)
                    self._path = None
                    return
                except Exception:
                    pass
            self._fh.close()

    @property
    def log_path(self) -> Path | None:
        return self._path

    @property
    def count(self) -> int:
        return self._count

    def write(self, filename: str, ext: str, mime: str,
              puid: str = "", format_name: str = "",
              basis: str = "", warning: str = "") -> None:
        if not self._writer:
            return
        # Normaliser linjeskift i lange feltverdier
        def _clean(s: str) -> str:
            return (s or "").replace("\n", " ").replace("\r", " ")
        with self._lock:
            self._writer.writerow([
                _clean(filename), _clean(ext), _clean(mime),
                _clean(puid), _clean(format_name),
                _clean(basis), _clean(warning),
            ])
            self._fh.flush()
            self._count += 1
