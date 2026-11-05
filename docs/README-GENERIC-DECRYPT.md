# Generic Decrypt – SIARD Workflow Manager

**Utviklingsbranch:** `feature/generic-decrypt`

## Formål

**Generisk: Lås opp dokumenter (JSON-regler)** er en operasjon under **Systemspesifikt**. Den dekrypterer passordbeskyttede Office-dokumenter som ligger som LOB-er i et SIARD-uttrekk.

Operasjonen følger samme prinsipp for behandling av SIARD som den eksisterende Acos/CosDoc-operasjonen. Forskjellen er at tabeller, koblinger og passordregler beskrives i JSON i stedet for å være knyttet til én bestemt databasestruktur. Generic Decrypt utfører **ikke** mailmerge eller PDF/A-konvertering.

## Arbeidsflyt

Anbefalt rekkefølge i en pipeline:

1. **Pakk ut SIARD** til arbeidsmappe.
2. Utfør eventuelle nødvendige kompatibilitetsrettinger.
3. **Generisk dokumentopplåsing** leser LOB-referanser og passord fra SIARD-tabellene etter valgt JSON-regel.
4. Ved ordinær kjøring erstattes kryptert LOB med dekryptert innhold **i den utpakkede arbeidskopien**. Tilhørende `tableX.xml` oppdateres med riktig lengde og MD5-kontrollsum. Andre dokumenter beholdes uendret.
5. Utfør eventuelt **Berik SIARD-metadata**.
6. **Pakk sammen SIARD** oppretter en ny SIARD-fil fra arbeidskopien.
7. Kjør **Metadata-uttrekk**, **Kjørerapport** og eventuelle øvrige rapportoperasjoner.

Original-SIARD overskrives ikke av sammenpakkingsoperasjonen. Generic Decrypt lager ikke en separat mappe med dekrypterte dokumentkopier som ordinært resultat; resultatet følger med i den nye SIARD-filen.

**Testkjøring** forsøker faktisk dekryptering, men skriver ikke endringer i LOB-filene eller `tableX.xml`.

## Innstillinger i GUI

| Felt | Bruk |
|---|---|
| **Regel-JSON** | Velg JSON-fil med regler. Tomt felt bruker medfølgende Acos Sosial-eksempel. Andre systemer kan beskrives i egne JSON-regler uten endring av Python-koden. |
| **Utpakket SIARD-mappe (gjenbruk)** | Valgfri sti til eksisterende utpakket SIARD, med `header/metadata.xml` og `content/`. Ved normal pipeline brukes mappen fra **Pakk ut SIARD** i samme kjøring. |
| **LOB-fil** | Ett eksakt LOB-filnavn for målrettet test, eksempelvis `rec10.bin`. Tomt felt behandler alle dokumenter som passer JSON-regelen. Kommaseparerte lister, jokertegn og regulære uttrykk er ikke implementert. |
| **Maks dokumenter** | `0` betyr alle dokumenter som passer filteret; positivt tall begrenser antallet. |
| **Testkjøring (ikke skriv)** | Kontroller dekryptering uten å endre den utpakkede SIARD-strukturen. |

Kontroller parametere i en tidligere lagret **profil** eller **prosjektfil** før ny kjøring. Prosjektets fullført-status kan føre til at et steg hoppes over uten ny behandling. Knappen for å starte fra første steg nullstiller statusen i prosjektet, men innebærer at tidligere steg må kjøres på nytt.

## JSON-regelsett

Tabeller og kolonner finnes ved å lese `header/metadata.xml`. Kolonneposisjoner (`c1`, `c2`, ...) utledes fra SIARD-metadata; de er ikke hardkodet. LOB-stier leses fra kolonnens `file`-referanse og `lobFolder`.

### Kilder og oppslag

| JSON-felt | Betydning |
|---|---|
| `source.table` | Tabell med dokumenter og LOB-referanser. |
| `source.lob_column` / `source.filename_column` | Kolonner for LOB og opprinnelig dokumentfilnavn. |
| `source.where_equals` | Valgfrie filtre på kolonneverdier. |
| `source.extensions` | Valgfrie filendelser for dokumentutvalget. |
| `source.size_column` | Valgfri kolonne som inneholder dokumentstørrelsen. |
| `lookup.table` | Tabell der lagrede passord skal slås opp. |
| `lookup.value_column` | Kolonne med lagret passord. |
| `lookup.join` | Ett eller flere koblingspar mellom kildetabellen og oppslagstabellen. |
| `formula.input_column` | Kolonnen formelen anvendes på, typisk originalt dokumentfilnavn. |
| `formula.when_regex` | Valgfri betingelse for når formelen skal beregnes. |
| `formula.operators` | Rekkefølge av operasjoner som bygger passordet. |
| `formula.check_against_stored` | Sammenlign beregnet passord med lagret verdi for logging av avvik. |

### Passordprioritet

Lagret passord prøves først når det finnes. `formula.use` styrer hvordan et beregnet passord brukes:

- `fallback_if_stored_empty`: Bruk formelen når lagret passord mangler. Et avvist lagret passord utløser ikke et nytt forsøk.
- `fallback_if_stored_rejected`: Bruk formelen når lagret passord mangler **eller** når lagret passord blir avvist som ugyldig. Et vellykket lagret passord overstyres ikke. Samme passord prøves ikke to ganger.

Fallback ved avvist passord gjelder passordfeil, ikke vilkårlige fil- eller dekrypteringsfeil. Loggen registrerer blant annet `formula_mismatch`, `formula_fallback_attempted` og `formula_fallback_success`, uten å skrive passordverdier.

Støttede formeloperatorer er `basename`, `stem`, `reverse`, `upper`, `lower`, `prepend`, `append`, `strip_suffix` og `replace`. Operatorer med argumenter skrives for eksempel som `{"prepend": "R"}`, `{"append": "..."}`, `{"strip_suffix": "_D"}` eller `{"replace": {"old": "a", "new": "b"}}`. JSON-reglene kjører ikke vilkårlig Python-kode eller `eval`.

## Dekryptering og feilhåndtering

- Dekryptering av Office-filer skjer med `msoffcrypto-tool`.
- Ved ordinær kjøring oppdateres bare dokumenter som faktisk er dekryptert. Dokumenter som ikke kan behandles, blir stående uendret, og resultatet registreres i loggene.
- En feil på ett dokument skal ikke avbryte behandling av resten av dokumentutvalget.
- Allerede ukrypterte dokumenter og PDF-filer håndteres som egne resultatkategorier; de rapporteres ikke som vellykket dekryptert.
- Vellykket teknisk dekryptering er ikke det samme som verifisert dokumentvisning. Office-filer som bruker krypteringsmetoder verktøyet ikke støtter, kan fortsatt feile.
- **Pakk sammen SIARD** er et eget steg som må inngå dersom resultatet skal bli en ny SIARD-fil.

## Rapportering

Generic Decrypt skriver rapportfilene i samme loggområde som den ordinære arbeidsflyten. Filnavnene får kjøringens filnavn/tidsstempel som prefiks:

| Rapportfil | Innhold |
|---|---|
| `*_dokumentopplasing.csv` | Dokumentvis resultat: filreferanse, kilde, filtype, status, størrelse, passordmetode og eventuell årsak. |
| `*_dokumentopplasing_feil.csv` | Maskinlesbar liste over mislykkede dokumenter med fil, tabell/kolonne, kategori og årsak. |
| `*_dokumentopplasing_feil.log` | Lesbar feillogg. |
| `*_dokumentopplasing_rapport.pdf` | Egen dekrypteringsrapport med nøkkeltall, resultatfordeling, diagrammer og feiloversikt. |

CSV-filer bruker semikolon og UTF-8 med BOM, som de eksisterende konverteringsrapportene. PDF-en bruker tilsvarende stil som de etablerte rapportene. **Kjørerapport (PDF)** får også nøkkeltall fra dekrypteringen. Eksisterende metadata- og depotrapporter er separate operasjoner og endres ikke av Generic Decrypt.

Vesentlige telleverdier omfatter utvalgte dokumenter, dekrypterte og lagrede filer, feil, allerede ukrypterte filer, manglende LOB-er, treff/avvik i passordformel og vellykkede fallback-forsøk.

## Installasjon og testing

Bruk den eksisterende arbeidskopien på `feature/generic-decrypt`. Ved installasjon fra en delta-ZIP: pakk ut **direkte i repositoryroten**, behold katalogstrukturen og overskriv de berørte filene. Det kreves ikke et eget patch- eller installasjonsskript for deltaen.

Kjør `test-generic-decrypt.bat` og kontroller den faktiske sluttstatusen. Testsuiten for denne leveransen omfatter 27 syntetiske tester av passordregler, fallback, enkeltfeil, testkjøring, LOB- og XML-oppdatering, tilbakeføring, sammenpakking og CSV-/PDF-rapportering. Dette erstatter ikke praktisk verifikasjon på Windows mot reelle SIARD-filer.

Etter testkjøring: kontroller filutvalg og antallsgrense, kjør ordinær arbeidsflyt med **Pakk sammen SIARD**, og gjennomgå de produserte dokumentloggene og rapportene.

## Avgrensning

Generic Decrypt dekrypterer dokumenter og oppdaterer den utpakkede SIARD-arbeidskopien. Den utfører **ikke** mailmerge, Office-til-PDF/A-konvertering eller sammenpakking på egen hånd. Disse oppgavene tilhører andre operasjoner i arbeidsflyten.
