"""
WorkflowContext
---------------
Bærer tilstand og resultater gjennom en workflow-kjøring.
Hver operasjon kan lese fra og skrive til konteksten.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class WorkflowContext:
    siard_path: Path
    results: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    flags: dict[str, bool] = field(default_factory=dict)

    # ── Hjelpere ────────────────────────────────────────────────────────────
    def set_result(self, key: str, value: Any) -> None:
        self.results[key] = value

    def get_result(self, key: str, default: Any = None) -> Any:
        return self.results.get(key, default)

    def set_flag(self, key: str, value: bool) -> None:
        self.flags[key] = value

    def get_flag(self, key: str, default: bool = False) -> bool:
        return self.flags.get(key, default)

    def summary(self) -> dict[str, Any]:
        return {
            "file": str(self.siard_path),
            "results": self.results,
            "flags": self.flags,
        }
