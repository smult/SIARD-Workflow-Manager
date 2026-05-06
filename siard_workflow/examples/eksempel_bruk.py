"""
examples/eksempel_bruk.py
=========================
Viser de vanligste måtene å bruke siard_workflow-rammeverket på.
Kjør fra prosjektrot:  python examples/eksempel_bruk.py
"""

import sys
from pathlib import Path

# Legg til prosjektrot i path (unødvendig etter pip install)
sys.path.insert(0, str(Path(__file__).parent.parent))

from siard_workflow import create_manager
from siard_workflow.operations import (
    SHA256Operation,
    XMLValidationOperation,
    MetadataExtractOperation,
    ConditionalOperation,
    VirusScanOperation,
    UnpackSiardOperation,
    RepackSiardOperation,
    BlobConvertOperation,
    HexExtractOperation,
    WorkflowReportOperation,
)

# ─── 1. Kjør med innebygd profil ─────────────────────────────────────────────
def eksempel_profil(siard_fil: str):
    """Enkleste bruk: velg profil, kjør."""
    manager = create_manager()

    print("\n── Tilgjengelige profiler ──")
    for p in manager.list_profiles():
        print(f"  • {p}")

    result = manager.run_profile(siard_fil, profile="standardkjoring")
    print(f"\nSuksess: {result.success}")
    print(f"SHA-256: {result.results[0].data.get('sha256', '?')[:32]}…")


# ─── 2. Manuell pipeline-workflow ────────────────────────────────────────────
def eksempel_manuell_workflow(siard_fil: str):
    """
    Bygg en pipeline-workflow fra bunnen av.
    """
    from siard_workflow.core import Workflow

    wf = (Workflow("Manuell")
        .add(SHA256Operation(save_to_file=True))
        .add(UnpackSiardOperation())
        .add(HexExtractOperation())
        .add(BlobConvertOperation())
        .add(RepackSiardOperation())
        .add(MetadataExtractOperation())
        .add(WorkflowReportOperation())
    )

    wf.describe()
    run = wf.execute(siard_fil)
    run.print_summary()


# ─── 3. Egendefinert operasjon ────────────────────────────────────────────────
def eksempel_egendefinert_operasjon(siard_fil: str):
    """
    Vis hvordan man lager en ny operasjon og plugger den inn.
    """
    from siard_workflow.core import BaseOperation, OperationResult, WorkflowContext
    import zipfile

    class FilTellerOperation(BaseOperation):
        operation_id = "file_counter"
        label       = "Filteller"
        description = "Teller antall filer i SIARD-arkivet fordelt på type."
        category    = "Analyse"

        def run(self, ctx: WorkflowContext) -> OperationResult:
            from collections import Counter
            with zipfile.ZipFile(ctx.siard_path) as zf:
                extensions = Counter(
                    Path(n).suffix.lower() or "(ingen)"
                    for n in zf.namelist()
                    if not n.endswith("/")
                )
            ctx.set_result("file_counts", dict(extensions))
            details = ", ".join(f"{ext}: {n}" for ext, n in extensions.most_common())
            return self._ok(data=dict(extensions), message=details)

    manager = create_manager()
    wf = (manager
          .create_workflow(siard_fil, profile="standardkjoring")
          .add(FilTellerOperation()))

    wf.describe()
    manager.run(wf, siard_fil)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Bruk: python eksempel_bruk.py <sti/til/uttrekk.siard>")
        sys.exit(1)

    fil = sys.argv[1]
    print("=" * 60)
    print("EKSEMPEL 1: Innebygd profil")
    eksempel_profil(fil)

    print("\n" + "=" * 60)
    print("EKSEMPEL 2: Manuell pipeline-workflow")
    eksempel_manuell_workflow(fil)

    print("\n" + "=" * 60)
    print("EKSEMPEL 3: Egendefinert operasjon")
    eksempel_egendefinert_operasjon(fil)
