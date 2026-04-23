"""
siard_workflow/core/project_file.py
------------------------------------
Prosjektfil (.siardwf) for å lagre og gjenoppta en workflow-kjøring.

Format: JSON med versjonsnummer, kilde-SIARD-sti, og en liste over operasjoner
med status (pending / completed / failed / skipped) og evt. output-sti.

Brukes til:
  - Lagre workflow-oppsett (operasjoner + params) slik at det kan gjenbrukes
  - Checkpoint-er underveis i kjøring (skrives til disk etter hvert steg)
  - Gjenoppta en avbrutt kjøring: hoppe over steg som er 'completed'
"""
from __future__ import annotations

import datetime
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


# ── Versjon for filformat ─────────────────────────────────────────────────────
_FORMAT_VERSION = 1
_FILE_SUFFIX    = ".siardwf"


@dataclass
class OpCheckpoint:
    """Tilstand for én operasjon i prosjektfilen."""
    operation_id: str
    label:        str
    params:       dict
    status:       str        = "pending"   # pending | completed | failed | skipped
    output_siard: str | None = None        # absolutt sti til output-SIARD (hvis produces_siard)
    completed_at: str | None = None        # ISO-tidsstempel
    error_msg:    str | None = None        # kortet ned feilmelding
    # Ekstra context-data som må gjenopprettes ved hopp (f.eks. extracted_path
    # og original_namelist for unpack_siard, slik at repack_siard kan fullføre)
    ctx_data:     dict       = field(default_factory=dict)


@dataclass
class ProjectFile:
    """
    Representerer en .siardwf prosjektfil.

    Opprettes enten fra en kjørende workflow (from_ops) eller ved lasting fra
    disk (load). Etter lasting kan steg hoppes over i run-løkken ved å sjekke
    is_completed() / get_output_siard().
    """
    version:      int               = _FORMAT_VERSION
    source_siard: str               = ""
    created:      str               = ""
    updated:      str               = ""
    operations:   list[OpCheckpoint] = field(default_factory=list)

    # ── Fabrikkmetoder ────────────────────────────────────────────────────────

    @classmethod
    def from_ops(cls, source_siard: Path, ops: list) -> "ProjectFile":
        """Lag en ny prosjektfil fra en liste over operasjonsobjekter."""
        now = datetime.datetime.now().isoformat(timespec="seconds")
        checkpoints = [
            OpCheckpoint(
                operation_id=op.operation_id,
                label=op.label,
                params=dict(op.params),
            )
            for op in ops
        ]
        return cls(
            source_siard=str(source_siard),
            created=now,
            updated=now,
            operations=checkpoints,
        )

    @classmethod
    def load(cls, path: Path) -> "ProjectFile":
        """Last prosjektfil fra disk."""
        data = json.loads(path.read_text(encoding="utf-8"))
        ops = [OpCheckpoint(**op) for op in data.get("operations", [])]
        return cls(
            version=data.get("version", _FORMAT_VERSION),
            source_siard=data.get("source_siard", ""),
            created=data.get("created", ""),
            updated=data.get("updated", ""),
            operations=ops,
        )

    # ── Persistens ────────────────────────────────────────────────────────────

    def save(self, path: Path) -> None:
        """Skriv prosjektfil til disk (atomisk via temp-fil)."""
        self.updated = datetime.datetime.now().isoformat(timespec="seconds")
        data = {
            "version":      self.version,
            "source_siard": self.source_siard,
            "created":      self.created,
            "updated":      self.updated,
            "operations":   [asdict(op) for op in self.operations],
        }
        tmp = path.with_suffix(".siardwf.tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)

    # ── Statusoppdateringer (kalles fra run-løkken) ────────────────────────────

    def _find(self, op_id: str) -> OpCheckpoint | None:
        return next((o for o in self.operations if o.operation_id == op_id), None)

    def mark_completed(self, op_id: str, output_siard: Path | None = None,
                       ctx_data: dict | None = None) -> None:
        cp = self._find(op_id)
        if cp:
            cp.status       = "completed"
            cp.output_siard = str(output_siard) if output_siard else None
            cp.completed_at = datetime.datetime.now().isoformat(timespec="seconds")
            cp.error_msg    = None
            if ctx_data:
                cp.ctx_data = ctx_data

    def mark_failed(self, op_id: str, error_msg: str = "") -> None:
        cp = self._find(op_id)
        if cp:
            cp.status    = "failed"
            cp.error_msg = (error_msg or "")[:200]

    def mark_skipped(self, op_id: str) -> None:
        cp = self._find(op_id)
        if cp:
            cp.status = "skipped"

    # ── Spørringer ────────────────────────────────────────────────────────────

    def is_completed(self, op_id: str) -> bool:
        cp = self._find(op_id)
        return cp is not None and cp.status == "completed"

    def get_status(self, op_id: str) -> str:
        cp = self._find(op_id)
        return cp.status if cp else "pending"

    def get_output_siard(self, op_id: str) -> Path | None:
        cp = self._find(op_id)
        if cp and cp.output_siard:
            return Path(cp.output_siard)
        return None

    @property
    def completed_count(self) -> int:
        return sum(1 for op in self.operations if op.status == "completed")

    @property
    def pending_count(self) -> int:
        return sum(1 for op in self.operations if op.status in ("pending", "failed"))

    @property
    def is_fully_completed(self) -> bool:
        return all(op.status in ("completed", "skipped") for op in self.operations)
