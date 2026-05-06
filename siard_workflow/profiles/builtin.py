"""
Innebygde profiler for vanlige SIARD-arbeidsflyter.
Registreres automatisk i StandardProfiles-registeret.
"""

from __future__ import annotations

from siard_workflow.core.manager import BaseProfile
from siard_workflow.core.workflow import Workflow
from siard_workflow.operations import (
    SHA256Operation,
    UnpackSiardOperation,
    RepackSiardOperation,
    HexExtractOperation,
    BlobConvertOperation,
    MetadataExtractOperation,
    WorkflowReportOperation,
    DiasPackageOperation,
)


class StandardkjøringProfile(BaseProfile):
    """
    Standard behandling: SHA-256 → pipeline → metadata-rapport → workflow-rapport.
    """
    name = "standardkjoring"
    description = "Standard behandling for SIARD-uttrekk"

    @classmethod
    def build(cls, workflow_name: str, stop_on_error: bool = False) -> Workflow:
        return (
            Workflow(name=workflow_name, stop_on_error=stop_on_error)
            .add(SHA256Operation())
            .add(UnpackSiardOperation())
            .add(HexExtractOperation())
            .add(BlobConvertOperation())
            .add(RepackSiardOperation())
            .add(MetadataExtractOperation())
            .add(WorkflowReportOperation())
        )


class StandardkjøringDIASProfile(BaseProfile):
    """
    Standard behandling med DIAS-pakking på slutten.
    """
    name = "standardkjoring_dias"
    description = "Standard behandling for SIARD-uttrekk med DIAS-pakking"

    @classmethod
    def build(cls, workflow_name: str, stop_on_error: bool = False) -> Workflow:
        return (
            Workflow(name=workflow_name, stop_on_error=stop_on_error)
            .add(SHA256Operation())
            .add(UnpackSiardOperation())
            .add(HexExtractOperation())
            .add(BlobConvertOperation())
            .add(RepackSiardOperation())
            .add(MetadataExtractOperation())
            .add(WorkflowReportOperation())
            .add(DiasPackageOperation())
        )


# ── Alle innebygde profiler ───────────────────────────────────────────────────
BUILTIN_PROFILES: list[type[BaseProfile]] = [
    StandardkjøringProfile,
    StandardkjøringDIASProfile,
]
