"""
siard_workflow/core/file_logger.py
Skriver logg til fil automatisk under workflow-kjøring.
"""
from __future__ import annotations
import datetime
import logging
from pathlib import Path


class WorkflowFileLogger:
    """
    Kontekstmanager som logger til fil under en workflow-kjøring.

    Bruk:
        with WorkflowFileLogger(log_dir) as wfl:
            wfl.log("melding", "info")
            run = wf.execute(...)
    """

    LEVELS = {"info": "INFO", "step": "STEP", "success": "OK  ",
              "warn": "WARN", "error": "ERR ", "muted": "----"}

    def __init__(self, log_dir: Path, siard_name: str = ""):
        self.log_dir   = Path(log_dir)
        self.siard_name = siard_name
        self._path: Path | None = None
        self._fh = None

    def __enter__(self):
        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = self.siard_name or "workflow"
        self._path = self.log_dir / f"{stem}_{ts}.log"
        self._fh = open(self._path, "w", encoding="utf-8")
        self._write_header()
        return self

    def __exit__(self, *_):
        if self._fh:
            self._fh.write(f"\n[{self._ts()}] Logg avsluttet\n")
            self._fh.close()

    @property
    def log_path(self) -> Path | None:
        return self._path

    def log(self, message: str, level: str = "info") -> None:
        if not self._fh:
            return
        tag = self.LEVELS.get(level, "INFO")
        line = f"[{self._ts()}] [{tag}] {message}\n"
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
        self._fh.write("=" * 60 + "\n\n")
