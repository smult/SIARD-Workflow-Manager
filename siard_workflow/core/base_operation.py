"""
BaseOperation
-------------
Abstrakt basisklasse for alle operasjoner i rammeverket.
Alle operasjoner arver fra denne og implementerer `run()`.
"""

from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Any

from .context import WorkflowContext

logger = logging.getLogger(__name__)


class OperationResult:
    """Innkapsler resultatet fra en enkelt operasjon."""

    def __init__(self, operation_id: str, success: bool, data: dict[str, Any] = None, message: str = ""):
        self.operation_id = operation_id
        self.success = success
        self.data = data or {}
        self.message = message

    def __repr__(self) -> str:
        status = "✓" if self.success else "✗"
        return f"[{status}] {self.operation_id}: {self.message}"


class BaseOperation(ABC):
    """
    Basisklasse for en SIARD-operasjon.

    Underklasser MÅ implementere:
        - operation_id  (klasseattributt, str)
        - label         (klasseattributt, str)
        - run(ctx)      (metode)

    Underklasser KAN overstyre:
        - description   (klasseattributt, str)
        - category      (klasseattributt, str)
        - default_params (klasseattributt, dict)
        - validate_params()
        - should_run(ctx)  – betinget kjøring basert på context-flagg
    """

    operation_id: str = ""
    label: str = ""
    description: str = ""
    category: str = "Generell"
    default_params: dict[str, Any] = {}

    def __init__(self, **params):
        # Prioritet: 1) eksplisitte params, 2) lagrede op_params, 3) config.json, 4) defaults
        merged = dict(self.default_params)
        try:
            import sys
            from pathlib import Path
            _root = Path(__file__).parent.parent.parent
            if str(_root) not in sys.path:
                sys.path.insert(0, str(_root))
            from settings import get_op_params, get_config

            # Last globale config-verdier for kjente nøkler
            _CONFIG_MAP = {
                "max_workers":   "max_workers",
                "lo_batch_size": "lo_batch_size",
                "lo_timeout":    "lo_timeout",
                "av_executable": "av_executable",
            }
            for param_key, config_key in _CONFIG_MAP.items():
                if param_key in merged:
                    val = get_config(config_key)
                    if val not in (None, "", 0) or config_key == "av_executable":
                        merged[param_key] = val

            # Lagrede op_params overstyrer config
            if self.operation_id:
                stored = get_op_params(self.operation_id, {})
                for k, v in stored.items():
                    if k in merged:
                        merged[k] = v
        except Exception:
            pass
        merged.update(params)   # eksplisitte params overstyrer alt
        self.params = merged
        self.validate_params()

    def validate_params(self) -> None:
        """Override for å validere parametere ved oppstart."""
        pass

    def should_run(self, ctx: WorkflowContext) -> bool:
        """
        Override for betinget kjøring.
        Standard: kjør alltid.
        Eks: return ctx.get_flag('has_blobs') == True
        """
        return True

    @abstractmethod
    def run(self, ctx: WorkflowContext) -> OperationResult:
        """Utfør operasjonen. Må implementeres av underklasse."""
        ...

    def _ok(self, data: dict = None, message: str = "") -> OperationResult:
        return OperationResult(self.operation_id, True, data or {}, message)

    def _fail(self, message: str, data: dict = None) -> OperationResult:
        return OperationResult(self.operation_id, False, data or {}, message)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} params={self.params}>"
