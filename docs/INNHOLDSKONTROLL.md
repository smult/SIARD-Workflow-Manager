# Innholdskontroll (QA) - rammeverk og ePhorte-pilot

**Utviklingssprint:** `v1.4.22-ikamr-04-a2` (bygger videre på a1)  
**Gren:** `feature/innholdskontroll`  
**Status:** Rammeverk/pilot - ikke arkivfaglig ferdigkontrollert.

## Formål og avgrensning

Kontrollere antall og kvalitet på informasjonsbærere i et SIARD-uttrekk. Motoren er **generisk og skrivebeskyttet**. All faglig tolkning av ePhorte, Noark 4/5, barnevern, helse, skole osv. skal defineres i et JSON-malsett, ikke hardkodes i Python.

Dette er **ikke** en Noark 5-validering og erstatter ikke Arkade 5, DBPTK eller sammenligning med produksjonssystemet. Første sprint leverer faktisk kode for SIARD-opptellinger, databaserelasjoner i QA-mal og rapportering, **ikke** ferdig fysisk kartlegging av ePhorte-tabeller.

## Levert i a1

1. Leser `header/metadata.xml` fra `.siard` eller utpakket SIARD-mappe. Oppretter katalog over schemas, tabeller, kolonner, datatyper, beskrivelser, deklarerte radantall og primærnøkler.
2. Kan strømme alle `tableX.xml`-rader og kontrollere antall mot metadata. Kontrollerer også deklarerte primærnøkler for duplikater og NULL-verdier (valgbart). Leser ikke binære dokument-LOB-er.
3. Leser **versjonert JSON QA-profil** for faglige informasjonsbærere. En profil kan definere tabell, identifiserende nøkkel, statuskolonne, datokolonne og referansetall.
4. Kontrollerer unike nøkler, tomme nøkkelverdier og duplikater for definerte informasjonsbærere.
5. Kontrollerer manuelt kartlagte relasjoner mot primærnøkler, registrerer foreldreløse referanser og eventuelt påkrevde, men manglende fremmednøkler.
6. Regner statusfordelinger og årstall for eksplisitt valgte kolonner.
7. Skriver separate **PDF, JSON, tabell-CSV, informasjonsbærer-CSV og avviks-CSV** under kjøringens loggmappe. PDF-stilen bruker appens eksisterende `report_style.py`.
8. Integreres i arbeidsflyt/profiler/prosjekt via vanlig `BaseOperation`, `workflow_io.py` og fanen **Innhold**.

## Nytt i a2: automatisk kartlegging fra SIARD-metadata

Dette er en kartleggingsmotor i tillegg til QA-kontrollene i a1. Den leser bare
SIARD-katalogen (metadata.xml), og kan brukes uten å skanne tabellradene.

1. Parser deklarerte **fremmednøkler** (referert schema, tabell og kolonner) og
   kandidatnøkler fra metadata.xml i tillegg til primærnøkler.
2. Rangerer forslag til tabeller for de 13 ePhorte/Noark-begrepene etter
   **navnelikhet og metadata-beskrivelser**, med et kontrollert sett aliaser i
   standardprofilen. Kildetabellene blir aldri automatisk valgt.
3. Foreslår nøkkelfelt fra deklarert primærnøkkel/kandidatnøkkel, eller et
   eksplisitt merket navneforslag dersom ingen nøkkel er deklarert.
4. Foreslår mulige status-/datokolonner kun ut fra kolonnenavn.
5. Skiller mellom **deklarerte metadata-relasjoner** og **navnebaserte forslag**.
   Ingen av dem regnes som kontrollert referanseintegritet uten eksplisitt
   QA-mapping og dataskann.
6. Lager en egen kartleggingsseksjon i eksisterende PDF-kontrollrapport og
   egne eksportfiler (se under).

### Ny rapportering

I tillegg til de fem a1-utdataene produserer a2:

| Filendelse | Innhold |
|---|---|
| `_kartleggingsforslag.json` | Alle kandidater, poengscore, begrunnelser og kildefelt for manuell gjennomgang |
| `_kartleggingsforslag.csv` | Rangerte forslag per fagbegrep, med nøkkel-, status- og datofelt |
| `_fremmednokler.csv` | Fremmednøkler fra `metadata.xml` og om referert tabell/kolonner finnes i katalogen |
| `_relasjonsforslag.csv` | Mulige relasjoner basert på feltnavn som **ikke** er deklarerte fremmednøkler |

**VIKTIG:** `score` er en navnelikhetsrangering, ikke en sannsynlighet eller
arkivfaglig godkjenning. Forslag i CSV/JSON har status `UBEKREFTET`.
Ingen `source`, `key`, `status_column`, `date_column` eller `relations` settes
automatisk i din QA-mal. Godkjenn koblingene mot ePhortes fysiske modell,
Decom/KDRS-mal eller systemdokumentasjon før du aktiverer fagkontrollene.

### Første kartlegging på ePhorte

- I GUI: velg **Innholdskontroll** og sett **Tell og kontroller alle tabellrader**
  til **Nei**. Da lages kartleggingsforslag raskt fra metadata, uten radskann.
- Alternativt fra terminal i repositoryroten:

```bat
python -X utf8 -m siard_workflow.core.content_qa "D:\SIARD\ephorte.siard" --kartlegging --out "D:\SIARD\kontroll"
```

- Åpne `_kartleggingsforslag.csv`, `_fremmednokler.csv` og
  `_relasjonsforslag.csv`. Bekreft hvilke tabeller og nøkler som faktisk
  representerer **sak, journalpost og dokument**. Lag så en kopi av
  `ephorte_template.json` der `source` og `key` settes for bekreftede objekter.
- Etter godkjent mapping: slå på radskann og kjør den faktiske QA-kontrollen.
  Forvent `DELVIS_KONTROLLERT` så lenge andre fagbegreper ikke er kartlagt.

## Hva "kontrollert" betyr

| QA-status | Betydning | Arbeidsflyt-steg |
|---|---|---|
| `GODKJENT` | Alle definerte kontroller er kjørt og uten avvik | OK |
| `DELVIS_KONTROLLERT` | En eller flere tabeller, faglige objekter eller relasjoner er ikke kartlagt eller kontrollert | Feilstatus; andre rapportsteg fortsetter |
| `AVVIK` | Faktiske avvik i utførte kontroller | Feilstatus; andre rapportsteg fortsetter |

En fullført kjøring med en **ukartlagt ePhorte-mal** gir dermed aldri grønn godkjenning for informasjonsbærerne, selv om alle databaserader er telt uten tekniske feil.

## Hvorfor ePhorte-malen ikke har tabellnavn ennå

Noark har de konseptuelle nivåene arkiv, arkivdel, klasse, mappe, registrering, dokumentbeskrivelse, dokumentobjekt osv. Men tabeller og relasjoner i en ePhorte-produksjonsdatabase kan ha andre navn og avvike mellom versjoner. Det er derfor **ikke forsvarlig** å anta at tabell `SAK` eller `JOURNALPOST` finnes i SIARD.

Malsettet i `siard_workflow/rules/content_qa/ephorte_template.json` har 13 logiske informasjonsbærere, foreløpig med `"source": null`. Bruk SIARD-katalogen fra første QA-kjøring og eventuelt Decom-/KDRS Søk & Vis-maler og dokumentasjon for å fastsette korrekt tabell og nøkkel.

## Eksempel på kartlegging (fiktive tabellnavn!)

Legg inn `source` og `key` i kopien av JSON-malen for ditt **faktiske** uttrekk. Eksempel på **syntetisk** datamodell:

```json
{
  "format_version": 1,
  "id": "syntetisk-noark-demo",
  "label": "Kun et eksempel - ikke bruk ukritisk på ePhorte",
  "carriers": [
    {
      "id": "sak", "label": "Saker",
      "source": {"schema": "schema0", "table": "SAK"},
      "key": ["SAK_ID"], "status_column": "SAKSSTATUS",
      "date_column": "OPPRETTET", "expected_count": {"exact": 1200}
    },
    {
      "id": "jp", "label": "Journalposter",
      "source": {"schema": "schema0", "table": "JOURNALPOST"},
      "key": ["JP_ID"]
    }
  ],
  "relations": [
    {
      "id": "jp_til_sak", "child": "jp", "parent": "sak",
      "child_columns": ["SAK_ID"], "required": true
    }
  ]
}
```

`key` kan være en sammensatt liste over flere kolonner. En relasjon må ha like mange `child_columns` som foreldrens nøkkelfelter. `expected_count` støtter `exact`, `min` og `max` som ikke-negative heltall. Tomme valgbare fremmednøkler blir ikke feil. Sett `required: true` når feltet etter systemets modell er obligatorisk.

## Bruk i GUI

Under **Innhold** velges **Innholdskontroll (QA - informasjonsbærere)**:

- **QA-mal (JSON)**: Tom filsti bruker ePhorte-kartleggingsmalen. For en virkelig fagkontroll velger du din kartlagte profil.
- **Tell og kontroller alle tabellrader**: `Ja` (standard) strømmer alle tabeller. `Nei` gir katalog og `DELVIS_KONTROLLERT`.
- **Kontroller primærnøkler fra SIARD-metadata**: `Ja` (standard) kontrollerer eksisterende PK-definisjoner med en midlertidig diskindeks. Deaktivering gir `DELVIS_KONTROLLERT` for tabeller der PK-kontroll da utelates.

**Anbefalt arbeidsflyt:** Pakk ut SIARD → eventuelle endringer → Pakk sammen SIARD → **Innholdskontroll** → Kjørerapport (PDF) → eventuelle Depotrapporter. Operasjonen kan også brukes direkte uten utpakking hvis en ferdig SIARD-fil ligger i køen.

**Merk:** Et prosjekt med alle stegene markert fullført vil hoppe over kjøringen. Bruk et nytt prosjekt eller nullstill kontrollsteget når du skal analysere uttrekket på nytt.

## CLI

Fra repositoryroten:

```bat
python -X utf8 -m siard_workflow.core.content_qa "D:\SIARD\ephorte.siard" --out "D:\SIARD\kontroll"
python -X utf8 -m siard_workflow.core.content_qa "D:\SIARD\ephorte.siard" --profile "D:\malsett\ephorte_qa.json" --out "D:\SIARD\kontroll"
python -X utf8 -m siard_workflow.core.content_qa "D:\SIARD\ephorte.siard" --metadata-only
```

CLI-returkoder: `0=GODKJENT`, `1=program-/filfeil`, `2=DELVIS_KONTROLLERT`, `3=AVVIK`.

## Test

Kjør `test-innholdskontroll.bat` fra repositoryroten. BAT-filen kontrollerer at nødvendige repo-filer finnes, kontrollerer registrering i GUI og profillagring, kjører enhetstester og validerer Python-syntaks.

## Prinsipper for sikkerhet, skalerbarhet og sporbarhet

- **Skrivebeskyttet:** Ingen endring av den opprinnelige SIARD-filen eller utpakkede data.
- **Store datamengder:** XML-tabeller leses fortløpende, uten å laste alle rader i minnet. Nøkler og relasjoner sjekkes i en midlertidig SQLite-indeks som slettes ved fullføring/feil.
- **Ingen identifiserende verdier i rapportene:** Nøkkelverdier blir nøklede (HMAC) fingeravtrykk med tilfeldig nøkkel for hver kjøring; originalverdier og nøkler blir ikke skrevet til QA-rapport eller CSV. Statusfordelinger med svært mange distinkte verdier skjules.
- **Krav til faktisk kontroll:** Verifikasjon som ikke er utført rapporteres eksplisitt som `IKKE_KONTROLLERT`, ikke som `OK`.
- **Sammenlignbare kjøringer:** Tidsstemplede rapporter inneholder profil-ID og kontrollsum for QA-malen. Forventningstall kan registreres i profilen dersom de er dokumentert fra produksjon eller Noark-uttrekk.
- **Kontrollavgrensning:** Nøkkelverdier sammenlignes på den lagrede tekstrepresentasjonen. Full SQL-semantikk for kolonnens tegnsett/kollasjon og alle datatyper er ikke validert i a1. Dette er ikke en erstatning for SIARD/DBPTK-validitetstest.
- **Streng profilvalidering:** Ukjente eller feilstavede regel-felter avvises, fremfor å bli ignorert og dermed gi en tilsynelatende fullført kontroll.

## Ikke levert per a2 (videre sprint)

- Ferdig ePhorte-tabell-/feltkartlegging for en bestemt produksjons-SIARD.
- Direkte import av Decom- eller KDRS Søk & Vis-maler; formatene må først undersøkes på eksempler.
- Parsing og sammenligning mot et eksternt Noark 5-uttrekk (DWM/Documaster).
- Automatisk tolkning av alle statuskoder og formelle Noark-krav.
- Automatisk kontroll av dataverdier for alle deklarerte fremmednokler (a2 leser kun metadata for disse).
- Dokumentvisning og selve innholdsvurderingen av vedlegg og journalposter.

**Neste naturlige steg:** Kjør a2 i katalogmodus på ett reelt ePhorte-SIARD. Se gjennom kartleggingsforslagene og bekreft de faktiske koblingene for saker og journalposter. Deretter kan vi utvikle presise fagkontroller og sammenligning med Noark 5-uttrekket.
