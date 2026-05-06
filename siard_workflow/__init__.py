"""
siard_workflow
==============
Rammeverk for behandling av SIARD-uttrekk.

Rask start::

    from siard_workflow import create_manager

    manager = create_manager()
    result = manager.run_profile("uttrekk.siard", profile="standardkjoring")

Eller manuell workflow::

    from siard_workflow import WorkflowManager
    from siard_workflow.operations import SHA256Operation, BlobConvertOperation

    manager = WorkflowManager()
    wf = manager.create_workflow("uttrekk.siard")
    wf.add(SHA256Operation()).add(BlobConvertOperation())
    result = manager.run(wf, "uttrekk.siard")
"""

from .core import (
    WorkflowContext,
    BaseOperation,
    OperationResult,
    Workflow,
    WorkflowRun,
    WorkflowManager,
    BaseProfile,
)
from .profiles import BUILTIN_PROFILES


def create_manager() -> WorkflowManager:
    """
    Opprett en WorkflowManager forhåndsregistrert med alle innebygde profiler.
    """
    manager = WorkflowManager()
    for profile_cls in BUILTIN_PROFILES:
        manager.register_profile(profile_cls.name, profile_cls)
    return manager


__all__ = [
    "WorkflowContext",
    "BaseOperation",
    "OperationResult",
    "Workflow",
    "WorkflowRun",
    "WorkflowManager",
    "BaseProfile",
    "create_manager",
]
