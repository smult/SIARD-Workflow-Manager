"""
WorkflowManager
---------------
Toppnivå-inngang til rammeverket.
Oppretter workflows fra profiler eller manuelt, og kjører dem.
"""

from __future__ import annotations
from pathlib import Path
from typing import Type

from .workflow import Workflow, WorkflowRun
from .base_operation import BaseOperation


class WorkflowManager:
    """
    Sentral manager for SIARD-workflow-behandling.

    Bruk:
        manager = WorkflowManager()
        manager.register_profile("standard", StandardProfile)

        wf = manager.create_workflow("uttrekk.siard", profile="standard")
        wf.add(EkstraOperasjon())   # valgfri utvidelse
        result = manager.run(wf)
    """

    def __init__(self):
        self._profiles: dict[str, "BaseProfile"] = {}

    # ── Profilregistrering ───────────────────────────────────────────────────

    def register_profile(self, name: str, profile: "BaseProfile") -> None:
        """Registrer en profil under et navn."""
        self._profiles[name] = profile

    def list_profiles(self) -> list[str]:
        return list(self._profiles.keys())

    # ── Workflow-fabrikk ─────────────────────────────────────────────────────

    def create_workflow(
        self,
        siard_path: Path | str,
        profile: str | None = None,
        stop_on_error: bool = False,
    ) -> Workflow:
        """
        Opprett en ny Workflow for en SIARD-fil.

        Args:
            siard_path:     Sti til .siard-filen.
            profile:        Navn på registrert profil (valgfri).
            stop_on_error:  Stopp ved første feil.

        Returns:
            Workflow klar til å kjøres eller modifiseres.
        """
        siard_path = Path(siard_path)
        name = siard_path.stem

        if profile:
            if profile not in self._profiles:
                raise KeyError(f"Ukjent profil: '{profile}'. Tilgjengelige: {self.list_profiles()}")
            wf = self._profiles[profile].build(name, stop_on_error)
        else:
            wf = Workflow(name=name, stop_on_error=stop_on_error)

        return wf

    # ── Kjøring ──────────────────────────────────────────────────────────────

    def run(self, workflow: Workflow, siard_path: Path | str, verbose: bool = True) -> WorkflowRun:
        """Kjør en ferdigbygd workflow på en SIARD-fil."""
        return workflow.execute(siard_path, verbose=verbose)

    def run_profile(
        self,
        siard_path: Path | str,
        profile: str,
        extra_ops: list[BaseOperation] | None = None,
        verbose: bool = True,
    ) -> WorkflowRun:
        """
        Snarvei: opprett workflow fra profil og kjør med én kommando.

        Args:
            siard_path:  Sti til .siard-filen.
            profile:     Profilnavn.
            extra_ops:   Ekstra operasjoner som legges til etter profilen.
            verbose:     Skriv fremgang til stdout.
        """
        wf = self.create_workflow(siard_path, profile=profile)
        for op in (extra_ops or []):
            wf.add(op)
        return self.run(wf, siard_path, verbose=verbose)


# ── Basisklasse for profiler ─────────────────────────────────────────────────

class BaseProfile:
    """
    En profil definerer en standard operasjonskjede for en systemtype.
    Underklasser implementerer `build()`.
    """

    name: str = ""
    description: str = ""

    @classmethod
    def build(cls, workflow_name: str, stop_on_error: bool = False) -> Workflow:
        raise NotImplementedError
