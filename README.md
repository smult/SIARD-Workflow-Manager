# SIARD Workflow Manager

Rammeverk og GUI for behandling av SIARD-uttrekk.

PS: Brukes på eget ansvar! Utvikler har ikke ansvar for eventuelle feil og mangler 
som skulle oppstå, eller misbruk av programvaren. 

Ta kontakt hvis du har innspill, kritikk, ros, ønsker, kommentarer etc.

Funksjonalitet omfatter blant annet:

## Generell funksjonlitet
- Valg av SIARD-filer og arbeidsmapper
- En del grunnleggende operasjoner for SIARD Workflow Manager, som for eksempel:
  - Ut og innpakking av SIARD
  -- Versjonvalg mellom 2.1 og 2.2
  - Konvertering av BLOB-er - Konverterer (mer eller mindre) alt LibreOffice kan ta av formater.
  - Ekstraksjon av hex-data - Enkelte systemer har hatt HEX-kodet tekst/data i felter i databasen.
    Dette støttes ikke av FullConvert / Siard Suite / DBPTK. Konverterer dette og ekstraherer det 
    ut til filer hvis det er av en viss størrelse.
  - SHA256-hash av BLOB-er
  - Virus-skanning av BLOB-er - forutsatt at ClamAV er installert, eller et antivirus-program som takler 
    commandline - fortsatt noe ustabil.. så bruk med varsomhet
  
## Systemspesifikk funksjonlitet
  - CosDoc - Lagrer dokumenter som passordbeskyttede word-flette-filer. Modulen låser opp og fletter før eventuell konvertering

## Kjente utfordringer og begrensninger:
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
    Eventuelt install.bat

## Kjør direkte
    start.bat kjører opp programmet og sjekk av requirements. 
    Ønsker du å kjøre direkte uten requirements hver gang kan kommandoen "python .\main.py" brukes.

## Legge til en ny operasjon
1. Lag klasse i siard_workflow/operations/ som arver BaseOperation
2. Implementer run(ctx) -> OperationResult
3. Legg til et OP_DEF-objekt i gui/operations_panel.py

Operasjonen vises da sammen med de andre i listen.
