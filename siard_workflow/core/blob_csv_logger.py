"""
siard_workflow/core/blob_csv_logger.py
CSV-logg for BLOB-konverteringsdetaljer, og feil-logg for konverteringsfeil.

Format per linje (CSV):
  fra_fil, fra_størrelse, fra_ext, til_fil, til_størrelse, til_ext, kommentar
"""
from __future__ import annotations
import csv
import datetime
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
    Loggfører filer som feiler konvertering til egen .log-fil.
    Navnmønster: {siard_stem}_{ts}_konvertering_feil.log

    Format per linje:
      [TIDSSTEMPEL]  filnavn  |  original_ext  |  feilmelding
    """

    def __init__(self, log_dir: Path, siard_name: str = ""):
        self.log_dir    = Path(log_dir)
        self.siard_name = siard_name
        self._path: Path | None = None
        self._fh   = None
        self._lock  = _threading.Lock()
        self._count = 0

    def __enter__(self) -> "ConversionErrorLogger":
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts         = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        stem       = self.siard_name or "blob"
        self._path = self.log_dir / f"{stem}_{ts}_konvertering_feil.log"
        self._fh   = open(self._path, "w", encoding="utf-8", newline="")
        self._fh.write(
            f"# Konverteringsfeil — {stem} — {datetime.datetime.now()}\n"
            f"# Format: [TID]  filsti (relativ i SIARD)  |  ext  |  feilmelding\n"
            f"# {'='*60}\n")
        self._fh.flush()
        return self

    def __exit__(self, *_):
        if self._fh:
            if self._count == 0:
                # Ingen feil — slett tom fil
                try:
                    self._fh.close()
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

    def write(self, filename: str, ext: str, error_msg: str) -> None:
        """Loggfør én konverteringsfeil."""
        if not self._fh:
            return
        ts  = datetime.datetime.now().strftime("%H:%M:%S")
        msg = (error_msg or "ukjent feil").replace("\n", " ")
        line = f"[{ts}]  {filename:<60}  |  {ext:<8}  |  {msg}\n"
        with self._lock:
            self._fh.write(line)
            self._fh.flush()
            self._count += 1
