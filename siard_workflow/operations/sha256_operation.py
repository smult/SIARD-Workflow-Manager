"""
SHA256Operation
---------------
Kalkulerer SHA-256 sjekksum for hele SIARD-filen.
Resultatet lagres i konteksten under nøkkelen 'sha256'.
"""

from __future__ import annotations
import hashlib
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


class SHA256Operation(BaseOperation):
    """Beregner SHA-256 sjekksum for SIARD-filen."""

    operation_id = "sha256"
    label = "SHA-256 Sjekksum"
    description = "Kalkulerer SHA-256 sjekksum for hele SIARD-filen."
    category = "Integritet"
    default_params = {
        "chunk_size": 8192,          # bytes per lesechunk
        "save_to_file": False,       # skriv .sha256-fil ved siden av SIARD-filen
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        path: Path = ctx.siard_path
        sha = hashlib.sha256()
        chunk_size: int = self.params["chunk_size"]

        try:
            with open(path, "rb") as f:
                while chunk := f.read(chunk_size):
                    sha.update(chunk)
        except OSError as e:
            return self._fail(f"Kunne ikke lese filen: {e}")

        digest = sha.hexdigest()
        ctx.set_result("sha256", digest)

        if self.params["save_to_file"]:
            checksum_path = path.with_suffix(".sha256")
            checksum_path.write_text(f"{digest}  {path.name}\n")

        return self._ok(
            data={"sha256": digest, "file": str(path)},
            message=digest[:16] + "…",
        )
