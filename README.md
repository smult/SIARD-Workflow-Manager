# SIARD Workflow Manager

Rammeverk og GUI for behandling av SIARD-uttrekk.

PS: Brukes på eget ansvar! Utvikler har ikke ansvar for eventuelle feil og mangler 
som skulle oppstå, eller misbruk av programvaren. 

Ta kontakt hvis du har innspill, kritikk, ros, ønsker, kommentarer etc.

Funksjonalitet omfatter blant annet:

## Generell funksjonalitet
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
- Bruken av Siegfried/PRONOM for filformatdeteksjon er bra, men litt treg. Bruk kan stilles inn i Innstillinger.
  Huk av for å bruke Siegfried, og velg sti til sf-applikasjon, eller trykk på knappen Installer for å laste ned   siste versjon av Siegfried fra GitHub.

## Krav
- Python 3.10+
- Windows / macOS / Linux
- LibreOffice for dokumentkonvertering
- Valgfritt: Ghostscript for PDF/A-normalisering av e-post (MSG/EML) og PDF-vedlegg.
  Installeres fra Innstillinger → Ghostscript (lastes ned fra Artifex på GitHub, ca. 65 MB,
  til din brukermappe), eller finnes automatisk hvis den allerede er installert.
  Uten Ghostscript brukes LibreOffice alene, og PDF-vedlegg tas med uten PDF/A-kontroll.

Merk om LibreOffice 26.2: enkelte installasjoner henger når samme LibreOffice-profil
brukes flere ganger. BLOB-konverteringen tester dette ved start og bruker i så fall ny
profil per kall (ca. 5 s ekstra per kall). Loggen sier fra når det skjer.

Lisenser: PyMuPDF og Ghostscript er AGPL (eller kommersiell lisens fra Artifex).
Greit for intern bruk; vurder lisensen før eventuell videredistribusjon.

### Sjekk av programvare og avhengigheter
Programmet sjekker selv hva som er installert (se `dependencies.py`):
- **Ved start (før vinduet åpnes):** Python-versjon, tkinter og påkrevde pakker
  (customtkinter). Mangler en påkrevd pakke, spør programmet om den skal installeres
  med pip. Svarer du nei, eller installasjonen feiler, får du kommandoen for å
  installere manuelt.
- **Etter oppstart:** Øvrige Python-pakker og eksterne programmer (LibreOffice,
  Ghostscript, Siegfried, antivirus og Ollama) sjekkes i bakgrunnen. Status skrives i
  loggen. Mangler noe som vanlig installasjon skal ha, vises en oversikt over hva som
  ikke virker, med knapper for å installere (pip, Ghostscript, Siegfried), peke ut
  LibreOffice eller åpne nedlastingssiden. «Ikke vis ved oppstart» gjelder til settet av
  mangler endrer seg.
- **Før kjøring (preflight «Programvare»):** Kun det operasjonene i arbeidsflyten
  trenger. Mangler noe som får en operasjon til å feile eller gi feil resultat, velger
  du mellom å installere, fortsette likevel eller stoppe. Mangler bare noe valgfritt,
  skrives det i loggen og kjøringen fortsetter.
- **Full oversikt:** Innstillinger → «Programvare og avhengigheter …».

| Komponent | Nivå | Uten den |
|---|---|---|
| customtkinter | påkrevd | programmet starter ikke |
| ReportLab | standard | ingen PDF-rapporter (Kjørerapport, Depotrapporter, Metadata-PDF) |
| lxml | standard | SIARD Mapper virker ikke |
| PyMuPDF, olefile | standard | e-post (MSG/EML) → PDF/A virker ikke; MSG behandles feilaktig som Word |
| msoffcrypto-tool, python-docx, docx-mailmerge2 | standard | CosDoc-fletting virker ikke |
| psutil | standard | enklere minneberegning for antall parallelle konverteringer |
| tkinterdnd2 | standard | ingen dra-og-slipp (Legg til-knappen virker) |
| LibreOffice | standard | Office-dokumenter, RTF, HTML m.m. konverteres ikke til PDF/A |
| Antivirus (Defender/ClamAV) | standard | Virusskanning virker ikke |
| Ghostscript | valgfri | e-post-PDF-er PDF/A-normaliseres ikke |
| Siegfried | valgfri | magic-bytes brukes i stedet for PRONOM |
| Ollama | valgfri | bare regelbasert PII-deteksjon i Anonymisering |
| py7zr / rarfile (+UnRAR) | valgfri | 7z-/RAR-arkiver i LOB-er pakkes ikke ut |
| python-magic-bin | valgfri | innebygd MIME-gjenkjenning i DIAS-pakke |

## Ferdig programpakke (for maskiner uten nett)
Programmet kan leveres som en ferdig pakke som ikke krever Python, pip eller nett:

    SIARDManager-<versjon>-win64-komplett.zip
      SIARDManager\SIARDManager.exe   programmet med Python og alle pakker innebygd
      SIARDManager\_internal\         biblioteker og ressursfiler
      SIARDManager\komponenter\       installasjonsfiler for LibreOffice, Ghostscript, Siegfried
      SIARDManager\LES MEG.txt

Pakk ut hele zip-filen til en skrivbar mappe og start `SIARDManager.exe`. Ved første
oppstart tilbys installasjon av LibreOffice, Ghostscript og Siegfried rett fra
`komponenter/`, uten nett og uten administratorrettigheter. De installeres i
`%LOCALAPPDATA%\SIARDManager`, og LibreOffice installeres med `msiexec /a`.
Innstillinger (`config.json`, `dias.json`) og loggen lagres ved siden av
`SIARDManager.exe`. Er mappa skrivebeskyttet, lagres de i
`%LOCALAPPDATA%\SIARDManager\data`. Er maskinen på nett, kan manglende komponenter
også lastes ned og installeres fra programmet.

Bygge pakken (Windows, med nett):

    packaging\bygg.bat                          alle komponenter (ca. 600 MB zip)
    packaging\bygg.bat --komponenter ingen      bare programmet
    packaging\bygg.bat --komponenter gs,sf      uten LibreOffice
    packaging\bygg.bat --lo-versjon 26.2.6      bestemt LibreOffice-versjon
    packaging\bygg.bat --publiser               bygg og last opp til GitHub-utgivelsen

Byggeskriptet (`packaging/build_release.py`) gjør dette:
- lager et eget byggemiljø (`build/venv`) med `requirements.txt`, de valgfrie
  tilleggene og PyInstaller;
- bygger `SIARDManager.exe` (`packaging/siard_manager.spec`);
- laster ned komponentene (verifisert med SHA256/SHA512, mellomlagret i
  `build/komponenter-cache`);
- selvtester pakken (`SIARDManager.exe --selftest resultat.json`);
- zipper resultatet til `dist/`: `SIARDManager-<versjon>-win64.zip` (bare
  programmet, brukes av automatisk oppdatering), `…-komplett.zip` (med
  komponenter) og `SHA256SUMS.txt`;
- med `--publiser`: laster opp filene til GitHub-utgivelsen med samme
  versjonsnummer (`gh` må være innlogget). Finnes utgivelsen ikke, lages et
  utkast som må publiseres på GitHub før programmene ser det.

Selvtesten kan også kjøres på en ny maskin for å kontrollere pakken.

### Automatisk oppdatering
Ved oppstart sjekker programmet GitHub for ny versjon. Har utgivelsen en
programpakke vedlagt, kan den lastes ned og installeres fra dialogen:
1. pakken lastes ned, sjekksummen kontrolleres og den nye versjonen selvtestes;
2. programmet spør om bekreftelse før noe endres;
3. programmet lukkes, programfilene byttes ut (innstillinger, `dias.json`,
   logger og `komponenter/` beholdes) og den nye versjonen startes. Feiler
   byttet, legges forrige versjon tilbake. Forløpet står i `oppdatering.log`.

Kjøres programmet fra kildekode i en git-kopi uten lokale endringer, tilbys
`git pull` i stedet. Maskiner uten nett oppdateres som før ved å pakke ut ny zip.

## Installasjon fra kildekode
    pip install -r requirements.txt
    Eventuelt install.bat
    Endre navn på config_example.json til config.json, hvis det er første gang programmet kjøres fra denne
    lokasjonen.

## Kjør direkte
    start.bat starter programmet uten konsollvindu (pythonw). Manglende pakker oppdages
    av programmet selv og installeres bare etter bekreftelse.
    Kommandoen "python .\main.py" starter programmet med konsoll (nyttig ved feilsøking).

## Legge til en ny operasjon
1. Lag klasse i siard_workflow/operations/ som arver BaseOperation
2. Implementer run(ctx) -> OperationResult
3. Legg til et OP_DEF-objekt i gui/operations_panel.py

Operasjonen vises da sammen med de andre i listen.
