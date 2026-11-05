# SIARD Workflow Manager – erfaringer og forbedringsforslag for `main`

**Dato:** 5. november 2026  
**Repository:** [IKAMR/SIARD-Workflow-Manager](https://github.com/IKAMR/SIARD-Workflow-Manager)  
**Bakgrunn:** Utprøving på `feature/generic-decrypt`

Dette notatet samler erfaringer som kan være relevante for den felles arbeidsflytmotoren og installasjonen. Det beskriver **ikke** Generic Decrypt-funksjonen; den er dokumentert i `README-GENERIC-DECRYPT.md`. Punktene nedenfor er forslag til vurdering, ikke vedtatte endringer i `main`.

## 1. Python-miljø, installasjon og oppstart

### Erfaringer

På en testmaskin pekte kommandoene på ulike Python-tolker:

| Kommando | Tolk |
|---|---|
| `py` | `C:\Program Files\Python314\python3.14t.exe` (free-threaded) |
| `python` | `C:\Program Files\Python314\python.exe` (ordinær Python 3.14) |

Et **lokalt** `install_fix-1.bat` brukte `py`, mens manuell installasjon og oppstart ble utført med `python`. Pakkene havnet dermed i ulike Python-miljøer, og programmet kunne melde om manglende avhengigheter selv om installasjonen tilsynelatende var utført. Dette er **ikke** dokumentasjon på at repositoryets ordinære `install.bat` bruker feil kommando; det bruker `python -m pip`.

Automatisk installasjon av PyMuPDF ga `metadata-generation-failed`. Følgende kommando lyktes på testmaskinen:

```cmd
python -m pip install --no-cache-dir --only-binary=:all: "PyMuPDF>=1.24,<2"
```

Årsaken til den opprinnelige PyMuPDF-feilen er ikke endelig avklart. Etter `python -m pip install -r requirements.txt` kunne programmet startes med `python main.py`. Et lokalt oppstartsskript ble brukt for å velge ordinær `pythonw.exe`; dette endret ikke felles `start.bat`.

### Forslag til `main`

1. **Entydig Python-tolk:** Installasjon, GUI-oppstart og automatisk pakkeinstallasjon må bruke samme tolk. Vurder prosjektlokalt virtuelt miljø, eller en tydelig valgt og kontrollert tolk.
2. **Bedre diagnostikk:** Vis Python-versjon og faktisk `sys.executable` ved avhengighetsproblemer; kontroller at `pythonw.exe` tilhører samme miljø som `python.exe`.
3. **Full pip-feillogg:** Behold komplett stdout/stderr og returkode ved installasjonsfeil. `dependencies.py` viser i dag bare de siste åtte feillinjene.
4. **Binærpakker på Windows:** Vurder et kontrollert alternativ for ferdigkompilerte pakker når pip forsøker kildekodebygging. Test kompatibilitet før dette eventuelt gjøres til standard; den lokale PyMuPDF-kommandoen ovenfor er ikke et generelt versjonskrav for `main`.

## 2. Arbeidsflytprofil, prosjektstatus og gjenkjøring

### Erfaringer

En **profil** lagrer operasjonene og innstillingene. En **prosjektfil** (`.siardwf`) lagrer i tillegg hvilke operasjoner som er gjennomført. Ved ny kjøring kunne tidligere fullførte steg bli hoppet over, selv når hensikten var å teste endrede parametere. Kjøringen kunne dermed avsluttes med «SUKSESS» uten at den aktuelle operasjonen ble utført på nytt.

Den eksisterende nullstillingsfunksjonen starter fra første steg. En liten endring i én operasjon kan derfor utløse unødvendig ny sjekksumkontroll, utpakking og etterfølgende arbeid. Testene viste også at parameterverdier i prosjektfil og profil kan være vanskelige å skille fra hverandre i GUI.

### Forslag til `main`

1. **Tydelig status:** Skill mellom «utført i denne kjøringen», «hoppet over – tidligere fullført» og «feilet» i logg og sluttoppsummering.
2. **Endrede parametere:** Avklar hvordan programmet skal oppdage parameterendringer og ugyldiggjøre relevante sjekkpunkter, uten at gammelt prosjektoppsett uventet overstyrer nye valg.
3. **Selektiv gjenkjøring:** Vurder «Kjør valgt operasjon på nytt» / «Kjør fra valgt steg», med korrekt håndtering av avhengigheter og senere steg.
4. **Bedre synlighet i GUI:** Gjør det enklere å se hvilken profil, prosjektfil og hvilke operative parametere som gjelder før kjøring.

## Videre håndtering

Disse to temaene bør vurderes separat fra `feature/generic-decrypt` og eventuelt tas som egne saker eller endringer i felles `main`. Dette notatet innebærer ingen endring av kode, versjonsnummer eller branch.
