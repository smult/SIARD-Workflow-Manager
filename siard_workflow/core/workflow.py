"""
Workflow
--------
En ordnet liste av operasjoner som kjøres sekvensielt på en SIARD-fil.
Støtter betinget kjøring (IF-operasjoner) via should_run() i hver operasjon.
"""

from __future__ import annotations
import logging
import time
from pathlib import Path
from typing import Iterator

from .base_operation import BaseOperation, OperationResult
from .context import WorkflowContext

logger = logging.getLogger(__name__)


class WorkflowExecutionError(Exception):
    pass


class WorkflowRun:
    """Samler alle resultater fra en enkelt workflow-kjøring."""

    def __init__(self, siard_path: Path):
        self.siard_path = siard_path
        self.results: list[OperationResult] = []
        self.skipped: list[str] = []
        self.start_time: float = 0.0
        self.end_time: float = 0.0

    @property
    def elapsed(self) -> float:
        return self.end_time - self.start_time

    @property
    def success(self) -> bool:
        return all(r.success for r in self.results)

    def print_summary(self) -> None:
        width = 60
        print("=" * width)
        print(f"  SIARD Workflow — {self.siard_path.name}")
        print(f"  Tid: {self.elapsed:.2f}s")
        print("=" * width)
        for r in self.results:
            icon = "✓" if r.success else "✗"
            print(f"  [{icon}] {r.operation_id:<30} {r.message}")
        if self.skipped:
            for s in self.skipped:
                print(f"  [–] {s:<30} (hoppet over)")
        print("=" * width)
        overall = "SUKSESS" if self.success else "FEIL"
        print(f"  Resultat: {overall}")
        print("=" * width)


class Workflow:
    """
    Bygger og kjører en pipeline av operasjoner.

    Bruk:
        wf = Workflow("MinWorkflow")
        wf.add(SHA256Operation())
        wf.add(BlobCheckOperation())
        result = wf.execute(Path("uttrekk.siard"))
    """

    def __init__(self, name: str = "Workflow", stop_on_error: bool = False):
        self.name = name
        self.stop_on_error = stop_on_error
        self._operations: list[BaseOperation] = []

    # ── Byggemetoder (fluent API) ────────────────────────────────────────────

    def add(self, operation: BaseOperation) -> "Workflow":
        """Legg til en operasjon bakerst i køen."""
        self._operations.append(operation)
        return self  # fluent

    def insert(self, index: int, operation: BaseOperation) -> "Workflow":
        """Sett inn en operasjon på en bestemt posisjon."""
        self._operations.insert(index, operation)
        return self

    def remove(self, operation_id: str) -> "Workflow":
        """Fjern en operasjon etter id."""
        self._operations = [o for o in self._operations if o.operation_id != operation_id]
        return self

    def __len__(self) -> int:
        return len(self._operations)

    def __iter__(self) -> Iterator[BaseOperation]:
        return iter(self._operations)

    def describe(self) -> None:
        """Print en oversikt over workflowen uten å kjøre den."""
        print(f"\nWorkflow: {self.name}  ({len(self._operations)} operasjoner)")
        for i, op in enumerate(self._operations, 1):
            print(f"  {i:>2}. [{op.category}] {op.label}")
            if op.params:
                for k, v in op.params.items():
                    print(f"       {k} = {v!r}")

    # ── Kjøring ─────────────────────────────────────────────────────────────

    def execute(self, siard_path: Path | str, verbose: bool = True) -> WorkflowRun:
        """
        Kjør alle operasjoner på SIARD-filen.

        Args:
            siard_path: Sti til .siard-filen.
            verbose:    Skriv fremgang til stdout.

        Returns:
            WorkflowRun med alle resultater.
        """
        siard_path = Path(siard_path)
        if not siard_path.exists():
            raise FileNotFoundError(f"SIARD-fil ikke funnet: {siard_path}")

        ctx = WorkflowContext(siard_path=siard_path)
        run = WorkflowRun(siard_path)
        run.start_time = time.time()

        if verbose:
            print(f"\n▶ Starter workflow «{self.name}» på {siard_path.name}")
            print(f"  {len(self._operations)} operasjoner planlagt\n")

        for op in self._operations:
            # Betinget kjøring
            if not op.should_run(ctx):
                if verbose:
                    print(f"  [–] {op.label} — hoppet over (should_run=False)")
                run.skipped.append(op.operation_id)
                continue

            if verbose:
                print(f"  ⬡  {op.label} ...", end="", flush=True)

            t0 = time.time()
            try:
                result = op.run(ctx)
                # Lagre resultat i kontekst så neste operasjon kan lese det
                ctx.set_result(op.operation_id, result.data)
            except Exception as exc:
                result = op._fail(str(exc))
                logger.exception("Operasjon %s kastet unntak", op.operation_id)

            elapsed = time.time() - t0
            run.results.append(result)

            if verbose:
                icon = "✓" if result.success else "✗"
                print(f"\r  [{icon}] {op.label:<40} ({elapsed:.2f}s)")
                if result.message:
                    print(f"       → {result.message}")

            if not result.success and self.stop_on_error:
                if verbose:
                    print(f"\n  ⚠ Stopper workflow pga. feil i {op.operation_id}")
                break

        run.end_time = time.time()

        if verbose:
            print()
            run.print_summary()

        return run
