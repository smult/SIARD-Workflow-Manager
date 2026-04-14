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
    BlobCheckOperation,
    XMLValidationOperation,
    MetadataExtractOperation,
    ConditionalOperation,
    VirusScanOperation,
)

# ─── 1. Kjør med innebygd profil ─────────────────────────────────────────────
def eksempel_profil(siard_fil: str):
    """Enkleste bruk: velg profil, kjør."""
    manager = create_manager()

    print("\n── Tilgjengelige profiler ──")
    for p in manager.list_profiles():
        print(f"  • {p}")

    result = manager.run_profile(siard_fil, profile="standard")
    print(f"\nSuksess: {result.success}")
    print(f"SHA-256: {result.results[0].data.get('sha256', '?')[:32]}…")


# ─── 2. Profil + ekstra operasjon ────────────────────────────────────────────
def eksempel_profil_med_ekstra(siard_fil: str):
    """Bruk en profil som base og legg til en operasjon."""
    manager = create_manager()

    wf = manager.create_workflow(siard_fil, profile="standard")
    wf.add(BlobCheckOperation())   # legg til etter profil-ops

    wf.describe()
    manager.run(wf, siard_fil)


# ─── 3. Manuell workflow med IF-logikk ───────────────────────────────────────
def eksempel_manuell_workflow(siard_fil: str):
    """
    Bygg en workflow fra bunnen av.
    Virusskan kjøres BARE hvis BLOB-sjekken finner binærfiler.
    """
    manager = create_manager()
    wf = manager.create_workflow(siard_fil)  # ingen profil → tom workflow

    (wf
        .add(SHA256Operation(save_to_file=True))
        .add(BlobCheckOperation())
        .add(ConditionalOperation(
            operation=VirusScanOperation(),
            flag="has_blobs",
            run_when=True,      # kjøres kun hvis has_blobs == True
        ))
        .add(XMLValidationOperation(strict=False))
        .add(MetadataExtractOperation())
    )

    wf.describe()
    run = manager.run(wf, siard_fil)

    # Bruk resultater programmatisk
    for res in run.results:
        if res.operation_id == "blob_check":
            has_blobs = res.data.get("has_blobs", False)
            print(f"\nHar filen binærfiler? {'JA' if has_blobs else 'NEI'}")


# ─── 4. Egendefinert operasjon ────────────────────────────────────────────────
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
          .create_workflow(siard_fil, profile="quick")
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
    print("EKSEMPEL 2: Profil + ekstra operasjon")
    eksempel_profil_med_ekstra(fil)

    print("\n" + "=" * 60)
    print("EKSEMPEL 3: Manuell workflow med IF-logikk")
    eksempel_manuell_workflow(fil)

    print("\n" + "=" * 60)
    print("EKSEMPEL 4: Egendefinert operasjon")
    eksempel_egendefinert_operasjon(fil)
