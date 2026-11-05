# SIARD Workflow Manager – Generic Decrypt

**Status og erfaringer fra test 4. november 2026**  
**Repository:** https://github.com/IKAMR/SIARD-Workflow-Manager  
**Branch:** `feature/generic-decrypt`  
**Opphav:** https://github.com/smult/SIARD-Workflow-Manager

## 1. Hva vi utvikler

Tester en generisk operasjon for å låse opp passordbeskyttede dokumenter i SIARD-uttrekk. Den ligger under **Systemspesifikt** som **«Generisk: Lås opp dokumenter (JSON-regler)»** og er adskilt fra eksisterende CosDoc-/Barnevern-operasjon for opplåsing og mailmerge.

Operasjonen har støtte for:

- **JSON-regler** som beskriver aktuelle tabeller, koblingsfelt, passordfelt og beregning av alternativt passord. Uten valgt JSON-fil brukes medfølgende Acos Sosial-regel.
- Valg av **én LOB-fil**, maksimalt antall dokumenter og **Testkjøring**.
- Dekrypteringstest uten varig resultatfil, eller lagring av en opplåst dokumentkopi ved ordinær kjøring.
- Angivelse av en allerede utpakket SIARD-mappe for videre testing.

Operasjonen oppdaterer foreløpig ikke dokumentreferanser eller innhold i en ny SIARD-fil.

## 2. Testresultater – Acos Sosial `1567_005`

Regelsettet bruker `NEF_ELFILER`, `NVE_DOKVERSJON` og passordfeltet `nve_passord`. Alternativt passord beregnes fra dokumentfilnavnets stamme med regelen `R` + omvendt filstamme.

| Test | Resultat |
|---|---|
| `rec10.bin` – manuell åpning | Lyktes i LibreOffice med identifisert passord. |
| `rec10.bin` – Testkjøring | `decrypted_tested=1`, `decrypt_failed=0`. |
| `rec10.bin` – ordinær kjøring | `decrypted_saved=1`. Den lagrede kopien åpnet i LibreOffice uten passord. |
| `rec1520.bin` – ordinær dekrypteringsforsøk | Feilet med lagret passord (`InvalidKeyError: Failed to verify password`). |
| `rec1520.bin` – manuell kontroll | **Det beregnede passordet virket i LibreOffice.** Det lagrede passordet var ikke riktig for denne filen. |
| Større kjøring | Resultatet var ikke gjennomgått da testen ble avsluttet. |

### Passordretting på utviklingsbranchen

Den opprinnelige logikken valgte lagret passord dersom det fantes, og prøvde ikke den beregnede verdien etter en feil. Det forklarer feilen på `rec1520.bin`.

**Rettelsen er å prøve i denne rekkefølgen:**

1. Lagret passord fra SIARD.
2. Ved avvist passord: alternativt passord beregnet etter JSON-regelen.
3. Registrer hvilken metode som lykkes, eller feil dersom begge mislykkes.

JSON-regelverket støtter alternative oppsett. Rettelsen skal bevare gammel oppførsel for regelsett som bare ønsker fallback når lagret passord mangler. **En delta med passord-fallback er laget, men resultatet av en ny GUI-test mot `rec1520.bin` er ennå ikke bekreftet.**

## 3. Python-installasjon og oppstart – konkret erfaring

På testmaskin viste disse kommandoene ulike tolker:

| Kommando | Python |
|---|---|
| `py` | `C:\Program Files\Python314\python3.14t.exe` – free-threaded |
| `python` | `C:\Program Files\Python314\python.exe` – standard |

Det lokale `install_fix-1.bat` brukte `py`, mens manuell installasjon og senere oppstart brukte `python`. Pakker var dermed installert i ulike Python-miljøer, noe som forklarte de tilsynelatende motstridende meldingene om manglende avhengigheter.

Automatisk installasjon av PyMuPDF feilet med `metadata-generation-failed`. Manuell installasjon med ferdig Windows-pakke lyktes:

```cmd
python -m pip install --no-cache-dir --only-binary=:all: "PyMuPDF>=1.24,<2"
```

PyMuPDF 1.28.2 ble bekreftet installert. Etter kommandoene nedenfor startet SIARD Workflow Manager uten meldinger om manglende standardpakker:

```cmd
python -m pip install -r requirements.txt
python main.py
```

**Årsaken til den opprinnelige PyMuPDF-feilen er ikke endelig fastslått.** Det ble det laget en lokal `start_fix-1.bat` som bruker standard `pythonw.exe`. Dette endret ikke kollegaens `start.bat` i hovedrepoet.

### Aktuelle forbedringer i felles `main`

- Sikre at installasjon, programstart og automatisk pakkeinstallasjon bruker **samme Python-tolk**, og vis hvilken tolk som faktisk er valgt.
- Forbedre feilmeldingene fra pip. Dagens `dependencies.py` viser bare de siste åtte linjene ved feil; full logg bør kunne åpnes eller lagres.
- Vurder håndtering av ferdigkompilerte Windows-pakker når pip ellers forsøker en kildekodebygging. Dette må testes før det eventuelt gjøres til standard.

Dette er forslag til gjennomgang i felles kode, ikke endringer som allerede er gjennomført i `main`.

## 4. Profil, prosjektfil og gjenkjøring

Testene viste en praktisk forskjell mellom **arbeidsflytprofil** og **prosjektfil (`.siardwf`)**:

- Profilen lagrer oppsettet for arbeidsflyten.
- Prosjektfilen lagrer også hvilke steg som tidligere er fullført.
- En ny kjøring kan vise **SUKSESS**, selv om samtlige steg rapporteres som **«Allerede fullført – hoppet over»**.
- Den gule ↻-knappen nullstiller prosjektet fra første steg. Dermed gjentas også SHA-256, utpakking og andre steg ved testing av bare én ny LOB-fil.

Dette bidro til at en test var vanskelig å følge: Et skjermbilde viste fortsatt `rec10.bin`, selv om hensikten var å teste `rec1520.bin`.

**Forslag til videre diskusjon for `main`:** Tydelig skille mellom utførte og hoppede steg, kontroll ved endrede operasjonsparametere og mulighet for å kjøre ett valgt steg om igjen. Hvordan dette skal løses i arbeidsflytmotoren må avklares før implementering.

## 5. Neste kontroll

1. Verifiser passord-fallback i programmet mot `rec1520.bin` og kontroller at beregnet passord faktisk brukes etter feil på lagret passord.
2. Gå gjennom resultatene fra den større kjøringen før det trekkes konklusjoner om batch-dekryptering.
3. Diskuter videre endringer i prosjekt-/gjenkjøringslogikk separat fra passordrettingen.

**Status:** Generic Decrypt utvikles og testes på `feature/generic-decrypt`. Ingen sammenslåing med `main` er besluttet.
