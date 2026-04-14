# SIARD Workflow Manager

Rammeverk og GUI for behandling av SIARD-uttrekk.

Funksjonalitet omfatter blant annet:
- Valg av SIARD-filer og arbeidsmapper
- En del grunnleggende operasjoner for SIARD Workflow Manager, som for eksempel:
  - Sjekk av BLOB-er
  - Konvertering av BLOB-er
  - Ekstraksjon av hex-data
  - SHA256-hash av BLOB-er
  - Virus-skanning av BLOB-er (forutsatt at ClamAV er installert - forstatt noe ustabtil.. så ikke bruk denne funksjonen ennå)

Kjente utfordringer og begrensninger:
- Noen av operasjonene kan være ustabile, spesielt virus-skanning av BLOB
- Noen filformater er ikke støttet ennå, så det kan oppstå feil underveis. Alt skal logges, så det er mulig å se hva som skjer.
- Hvis det settes opp for mange workers, kan det oppstå problemer med ressursbruk og stabilitet.
  Det er derfor anbefalt å starte med et lavt antall workers og øke etterhvert hvis det fungerer stabilt.
  For egen maskin med 16 kjerner er 8 workers et godt utgangspunkt.
- Avhengig av menge ram i maskinen kan batch-størrelsen endre.
  Standard er 50, men kan hvis man har mye minne økers til 100 eller mer.
  Det er anbefalt å starte med 50 å øke etterhvert hvis det fungerer stabilt.
  Har man lite minne (<32GB) kan det være nødvendig å redusere batch-størrelsen til 25 eller mindre.

## Krav
- Python 3.10+
- Windows / macOS / Linux
- LibreOffice for dokumentkonvertering

## Installasjon
    pip install -r requirements.txt

## Kjør direkte
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
