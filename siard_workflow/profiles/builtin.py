"""
Innebygde profiler for vanlige SIARD-systemtyper.
Registreres automatisk i StandardProfiles-registeret.
"""

from __future__ import annotations

from siard_workflow.core.manager import BaseProfile
from siard_workflow.core.workflow import Workflow
from siard_workflow.operations import (
    SHA256Operation,
    BlobCheckOperation,
    XMLValidationOperation,
    MetadataExtractOperation,
    VirusScanOperation,
    ConditionalOperation,
)


class StandardProfile(BaseProfile):
    """
    Standard behandling for vanlige uttrekk uten binærfiler.
    SHA-256 → XML-validering → Metadata-uttrekk
    """
    name = "standard"
    description = "Grunnleggende behandling for vanlige SIARD-uttrekk"

    @classmethod
    def build(cls, workflow_name: str, stop_on_error: bool = False) -> Workflow:
        return (
            Workflow(name=workflow_name, stop_on_error=stop_on_error)
            .add(SHA256Operation())
            .add(XMLValidationOperation())
            .add(MetadataExtractOperation())
        )


class BlobProfile(BaseProfile):
    """
    Uttrekk som kan inneholde BLOB/CLOB.
    SHA-256 → BLOB-sjekk → [IF has_blobs] Virusskan → XML-validering → Metadata
    """
    name = "blob"
    description = "Uttrekk som kan inneholde binærfiler (BLOB/CLOB)"

    @classmethod
    def build(cls, workflow_name: str, stop_on_error: bool = False) -> Workflow:
        return (
            Workflow(name=workflow_name, stop_on_error=stop_on_error)
            .add(SHA256Operation())
            .add(BlobCheckOperation())
            .add(ConditionalOperation(VirusScanOperation(), flag="has_blobs", run_when=True))
            .add(XMLValidationOperation())
            .add(MetadataExtractOperation())
        )


class QuickProfile(BaseProfile):
    """
    Hurtigsjekksum — kun SHA-256 og XML-validering.
    """
    name = "quick"
    description = "Rask kontroll: kun sjekksum og XML-validering"

    @classmethod
    def build(cls, workflow_name: str, stop_on_error: bool = False) -> Workflow:
        return (
            Workflow(name=workflow_name, stop_on_error=stop_on_error)
            .add(SHA256Operation(save_to_file=True))
            .add(XMLValidationOperation())
        )


class FullProfile(BaseProfile):
    """
    Komplett behandling: alle operasjoner.
    """
    name = "full"
    description = "Full behandling med alle tilgjengelige operasjoner"

    @classmethod
    def build(cls, workflow_name: str, stop_on_error: bool = False) -> Workflow:
        return (
            Workflow(name=workflow_name, stop_on_error=stop_on_error)
            .add(SHA256Operation(save_to_file=True))
            .add(BlobCheckOperation())
            .add(ConditionalOperation(VirusScanOperation(), flag="has_blobs", run_when=True))
            .add(XMLValidationOperation(check_table_xsd=True))
            .add(MetadataExtractOperation())
        )


# ── Alle innebygde profiler ───────────────────────────────────────────────────
BUILTIN_PROFILES: list[type[BaseProfile]] = [
    StandardProfile,
    BlobProfile,
    QuickProfile,
    FullProfile,
]
