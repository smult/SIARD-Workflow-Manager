"""
siard_workflow/core/blob_progress.py
------------------------------------
Progresjonsfil for BLOB-konverteringen, så et avbrutt steg kan fortsette der
det stoppet — også etter flere avbrudd.

  {log_dir}/{stem}_blob_progress.tsv

  #siard-blob-progress<TAB>1<TAB><utpakket mappe><TAB><innstillinger (JSON)>
  D<TAB><zip_sti><TAB><til_fil><TAB><kommentar>          — fil ferdig behandlet
  R<TAB><opprinnelig filnavn><TAB><ny sti (relativ)><TAB><kommentar>
                                                          — konverteringsregister

Fila skrives fortløpende (én linje per fil, flush), og leses bare når den
hører til samme utpakkede mappe med samme navneinnstillinger — en ny utpakking
av originalen skal aldri hoppe over filer.

Tidligere ble ferdige filer lest fra CSV-loggen med feil skilletegn (CSV-en
skrives med «;», men ble lest med «,»), så ingenting ble hoppet over. Den
filen pekte dessuten bare på CSV-en fra første avbrudd.

Konverteringsregisteret («R») trengs når tableX.xml patches: en fil som ble
konvertert i en tidligere kjøring (record1.doc → record1.bin) har fått nytt
navn på disk, mens XML-en fortsatt peker på det gamle navnet.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path, PurePosixPath

PROGRESS_SUFFIX = "_blob_progress.tsv"
_MAGIC = "#siard-blob-progress"
_VERSION = "1"


def progress_path(log_dir, siard_stem: str) -> Path:
    return Path(log_dir) / f"{siard_stem}{PROGRESS_SUFFIX}"


def _clean(s) -> str:
    return str(s if s is not None else "").replace("\t", " ").replace("\r", " ") \
        .replace("\n", " ")


def _key(extract_dir) -> str:
    try:
        return str(Path(extract_dir).resolve())
    except Exception:
        return str(extract_dir)


class BlobProgress:
    """Les og skriv progresjonsfila for én BLOB-konvertering."""

    def __init__(self, path: Path, extract_dir: Path, settings: dict | None = None):
        self.path = Path(path)
        self.extract_dir = Path(extract_dir)
        self.settings = dict(settings or {})
        self._fh = None
        self._lock = threading.Lock()
        self.done: set[str] = set()
        # {orig_basename: (ny_sti_relativ, kommentar)}
        self.registry: dict[str, tuple[str, str]] = {}
        # [(zip_sti, til_fil)] i rekkefølge
        self.done_rows: list[tuple[str, str, str]] = []
        # Statistikk fra tidligere (avbrutte) kjøringer — én «S»-linje per kjøring
        self.prior_stats: list[dict] = []
        self.discarded_reason = ""

    # ── Lesing ────────────────────────────────────────────────────────────────

    def load(self) -> int:
        """Les eksisterende fil. Returnerer antall ferdige filer (0 = ny start)."""
        if not self.path.exists():
            return 0
        try:
            with open(self.path, encoding="utf-8", newline="") as f:
                head = f.readline().rstrip("\r\n").split("\t")
                if len(head) < 4 or head[0] != _MAGIC or head[1] != _VERSION:
                    self.discarded_reason = "ukjent format"
                    return 0
                if head[2] != _key(self.extract_dir):
                    self.discarded_reason = "hører til en annen utpakket mappe"
                    return 0
                try:
                    old = json.loads(head[3] or "{}")
                except Exception:
                    old = {}
                if old != self.settings:
                    self.discarded_reason = "innstillingene for LOB-navn er endret"
                    return 0
                for line in f:
                    parts = line.rstrip("\r\n").split("\t")
                    if parts[0] == "D" and len(parts) >= 2 and parts[1]:
                        til = parts[2] if len(parts) > 2 else ""
                        com = parts[3] if len(parts) > 3 else ""
                        if parts[1] not in self.done:
                            self.done_rows.append((parts[1], til, com))
                        self.done.add(parts[1])
                    elif parts[0] == "S" and len(parts) >= 2:
                        try:
                            self.prior_stats.append(json.loads(parts[1]))
                        except Exception:
                            pass
                    elif parts[0] == "R" and len(parts) >= 3 and parts[1]:
                        self.registry[parts[1]] = (parts[2],
                                                   parts[3] if len(parts) > 3 else "")
        except Exception as exc:
            self.discarded_reason = f"kunne ikke leses ({exc})"
            self.done.clear()
            self.done_rows.clear()
            self.prior_stats.clear()
            self.registry.clear()
            return 0
        return len(self.done)

    def resumed_refs(self) -> dict[str, list[tuple[str, str, Path, str]]]:
        """Filer behandlet i tidligere kjøringer, gruppert per tabell.

        {"schemaX/tableY": [(opprinnelig filnavn, opprinnelig zip_sti,
                             ny fil på disk, kommentar)]}
        Brukes når tableX.xml patches, så referanser til gamle navn rettes.
        """
        out: dict[str, list] = {}
        for zip_sti, til, _com in self.done_rows:
            p = PurePosixPath(zip_sti)
            parts = p.parts
            if len(parts) < 4:
                continue
            orig_name = p.name
            new_name = PurePosixPath(til).name if til else orig_name
            reg = self.registry.get(orig_name)
            if reg and reg[0]:
                new_path = self.extract_dir / reg[0]
                comment = reg[1]
            else:
                new_path = self.extract_dir / str(p.parent) / new_name
                comment = ""
            if not new_path.exists():
                continue
            if new_path.name == orig_name and not comment:
                continue   # uendret — ingenting å rette
            tk = f"{parts[1]}/{parts[2]}"
            out.setdefault(tk, []).append((orig_name, zip_sti, new_path, comment))
        return out

    # ── Skriving ──────────────────────────────────────────────────────────────

    def open(self, append: bool) -> "BlobProgress":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if append and self.path.exists():
            self._fh = open(self.path, "a", encoding="utf-8", newline="")
        else:
            self._fh = open(self.path, "w", encoding="utf-8", newline="")
            self._fh.write("\t".join([_MAGIC, _VERSION, _key(self.extract_dir),
                                      json.dumps(self.settings, sort_keys=True)]) + "\n")
            self._fh.flush()
        return self

    def record_done(self, zip_sti, til_fil="", kommentar="") -> None:
        if not self._fh or not zip_sti:
            return
        with self._lock:
            try:
                self._fh.write(f"D\t{_clean(zip_sti)}\t{_clean(til_fil)}\t"
                               f"{_clean(kommentar)[:200]}\n")
                self._fh.flush()
            except Exception:
                pass

    def record_registry(self, orig_basename, new_path, comment="") -> None:
        if not self._fh or not orig_basename:
            return
        try:
            rel = Path(new_path).relative_to(self.extract_dir).as_posix()
        except Exception:
            rel = str(new_path)
        with self._lock:
            try:
                self._fh.write(f"R\t{_clean(orig_basename)}\t{_clean(rel)}\t"
                               f"{_clean(comment)[:300]}\n")
                self._fh.flush()
            except Exception:
                pass

    def record_stats(self, stats: dict) -> None:
        """Tellinger for denne kjøringen (summeres med tidligere ved neste)."""
        if not self._fh:
            return
        data = {k: v for k, v in stats.items()
                if isinstance(v, (int, float)) and not isinstance(v, bool)}
        with self._lock:
            try:
                self._fh.write("S\t" + json.dumps(data, sort_keys=True) + "\n")
                self._fh.flush()
            except Exception:
                pass

    def total_stats(self, current: dict) -> dict:
        """Tellinger summert over tidligere kjøringer og denne."""
        tot: dict = {}
        for d in self.prior_stats + [current]:
            for k, v in d.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    tot[k] = tot.get(k, 0) + v
        return tot

    def close(self) -> None:
        with self._lock:
            if self._fh:
                try:
                    self._fh.close()
                except Exception:
                    pass
                self._fh = None

    def delete(self) -> None:
        self.close()
        try:
            self.path.unlink(missing_ok=True)
        except Exception:
            pass


class PersistentRegistry(dict):
    """Konverteringsregister som også skrives til progresjonsfila."""

    def __init__(self, progress: "BlobProgress | None"):
        super().__init__()
        self._progress = progress

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if self._progress is not None:
            try:
                new_path, comment = value
            except Exception:
                new_path, comment = value, ""
            self._progress.record_registry(key, new_path, comment)
