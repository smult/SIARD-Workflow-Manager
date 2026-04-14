# SIARD Workflow Manager

Rammeverk og GUI for behandling av SIARD-uttrekk.

## Krav
- Python 3.10+
- Windows / macOS / Linux

## Installasjon
    pip install -r requirements.txt

## Kjoer direkte
    python main.py eller start.bat

## Bygg EXE
    python bygg_exe.py
    # -> dist/SIARDWorkflowManager.exe

## Prosjektstruktur

    siard_manager/
      main.py                  # Inngangspunkt
      bygg_exe.py              # PyInstaller build-skript
      requirements.txt
      gui/
        app.py                 # Hovedvindu (App)
        workflow_panel.py      # Venstre: workflow-kø med rekkefølgekontroll
        operations_panel.py    # Høyre: operasjonspalett med parameterdialog
        log_panel.py           # Høyre: fargekoded kjørelogg
        styles.py              # Farger og fonter
      siard_workflow/          # Backend-rammeverket
        core/                  # Context, BaseOperation, Workflow, Manager
        operations/            # SHA256, BlobCheck, XML, Metadata, Virus, Conditional
        profiles/              # Standard, Blob, Quick, Full

## Legge til en ny operasjon

1. Lag klasse i siard_workflow/operations/ som arver BaseOperation
2. Implementer run(ctx) -> OperationResult
3. Legg til et OP_DEF-objekt i gui/operations_panel.py

Operasjonen vises da sammen med de andre i listen.
