"""siard_workflow/operations/metadata_quality_operation.py

MetadataQualityOperation
------------------------
Retter to kjente innholdsfeil i ``header/metadata.xml`` fra filbaserte uttrekk
(typisk SC Full Convert mot Sybase Advantage / WIS o.l.):

  1. ``<dbname>`` inneholder tilkoblingsstien i stedet for databasenavnet::

         <dbname>Data.Q:\\Ikava\\WISFerdig\\Skole\\Flekkefjord historisk\\Data</dbname>

     SIARD-skjemaet sier «name of the archived database». Stien er hverken
     et navn eller noe DBPTK bruker — den lekker bare intern nettverksstruktur
     inn i et arkivobjekt som skal leve i mange tiår.

  2. ``<dataOriginTimespan>`` har omvendt datospenn og/eller lokalt datoformat::

         <dataOriginTimespan>31.7.2021 - 30.12.2013</dataOriginTimespan>

     Sluttdato før startdato er en ren innholdsfeil.

Hva dette IKKE er
=================
Ingen av disse hindrer innlasting i DBPTK, og de er ikke skjemabrudd:
``dbname`` og ``dataOriginTimespan`` er begge ``mandatoryString`` (``xs:string``
med ``minLength 1``) i den offisielle ``siard2-1-metadata.xsd``, og DBPTKs
validator (``MetadataDatabaseInfoValidator``, reglene M_5.1-1-2 og A_M_5.1-1-7)
sjekker kun at de ikke er tomme. DBPTK Desktop navngir dessuten Solr-collections
etter ``databaseUUID``, ikke etter ``dbname``. Dette er altså en
**metadatakvalitets**-operasjon, ikke en kompatibilitetsfiks.

``<connection>`` røres bevisst IKKE: SIARD-spesifikasjonen beskriver feltet som
«connection string used for archiving», og for en filbasert base ER stien den
korrekte tilkoblingen. Den er proveniens og skal bevares.
"""

from __future__ import annotations

import datetime
import io
import re
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape as _xml_escape, unescape as _xml_unescape

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext

_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")


# ── Tekstuttrekk fra metadata.xml (namespace-tolerant, byte-bevarende) ────────

def _tag_re(tag: str) -> re.Pattern:
    """Regex som fanger (åpningstagg, innhold, lukketagg) for et element."""
    return re.compile(
        r'(<(?:[A-Za-z0-9_-]+:)?' + tag + r'(?:\s[^>]*)?>)(.*?)'
        r'(</(?:[A-Za-z0-9_-]+:)?' + tag + r'\s*>)',
        re.DOTALL,
    )


_DBNAME_RE    = _tag_re("dbname")
_TIMESPAN_RE  = _tag_re("dataOriginTimespan")


def _get_value(pattern: re.Pattern, meta: str) -> str:
    """Les ut (avescapet) tekstverdi for første treff, eller tom streng."""
    m = pattern.search(meta)
    return _xml_unescape(m.group(2)).strip() if m else ""


def _set_value(pattern: re.Pattern, meta: str, new_value: str) -> str:
    """Sett ny tekstverdi på første treff. Bevarer tagger og attributter."""
    def _sub(m: re.Match) -> str:
        return m.group(1) + _xml_escape(new_value) + m.group(3)
    return pattern.sub(_sub, meta, count=1)


# ── 1. dbname med filsti ──────────────────────────────────────────────────────

# Start på en filsti: stasjonsbokstav (Q:\ eller Q:/) eller UNC (\\server\...)
_PATH_START_RE = re.compile(r'[A-Za-z]:[\\/]|\\\\')


def dbname_has_path(value: str) -> bool:
    """True hvis dbname-verdien inneholder en filsti."""
    return bool(_PATH_START_RE.search(value)) or "\\" in value


def derive_dbname(value: str) -> str:
    """
    Utled et databasenavn fra en dbname-verdi som inneholder en filsti.

    Strategi, i prioritert rekkefølge:
      1. Teksten FORAN stien — «Data.Q:\\Ikava\\…» → «Data»
         (Full Convert skriver «<navn>.<sti>»).
      2. Siste ikke-tomme sti-ledd — «Q:\\Ikava\\…\\Data» → «Data».
      3. Tom streng hvis ingenting brukbart finnes; da lar operasjonen
         feltet stå (dbname er obligatorisk og må aldri bli tomt).
    """
    m = _PATH_START_RE.search(value)
    if m:
        prefix = value[:m.start()].strip().rstrip(". \t-_")
        if prefix:
            return prefix
    # Fall tilbake til siste sti-ledd
    parts = [p.strip() for p in re.split(r'[\\/]+', value) if p.strip()]
    if parts:
        last = parts[-1]
        # Et rent stasjonsledd («Q:») er ikke et navn
        if not re.fullmatch(r'[A-Za-z]:', last):
            return last
    return ""


# ── 2. dataOriginTimespan ─────────────────────────────────────────────────────

# Skilletegn i et datospenn. Bindestrek krever omkringliggende mellomrom, ellers
# ville ISO-datoen «2013-12-30» blitt splittet i seg selv.
_RANGE_SPLIT_RE = re.compile(r'\s*[\u2013\u2014]\s*|\s+-\s+|\s+til\s+',
                             re.IGNORECASE)

# Godtatte datoformater. d.m.yyyy tolkes som norsk dag-først — det er
# konvensjonen i uttrekkene dette gjelder.
_DATE_FORMATS = (
    ("%Y-%m-%d", True),    # ISO — allerede riktig
    ("%d.%m.%Y", False),
    ("%d/%m/%Y", False),
    ("%d-%m-%Y", False),
)


def _parse_date(text: str) -> "tuple[datetime.date, bool] | None":
    """Returner (dato, er_allerede_iso) eller None hvis den ikke lar seg tolke."""
    text = text.strip()
    for fmt, is_iso in _DATE_FORMATS:
        try:
            return datetime.datetime.strptime(text, fmt).date(), is_iso
        except ValueError:
            continue
    return None


def parse_timespan(value: str) -> "tuple[datetime.date, datetime.date, bool] | None":
    """
    Tolk «<start> - <slutt>».

    Returnerer (start, slutt, begge_er_iso), eller None hvis verdien ikke er et
    tolkbart datospenn (da rører vi den ikke — feltet er fri tekst i SIARD).
    """
    parts = _RANGE_SPLIT_RE.split(value.strip())
    if len(parts) != 2:
        return None
    a = _parse_date(parts[0])
    b = _parse_date(parts[1])
    if a is None or b is None:
        return None
    return a[0], b[0], (a[1] and b[1])


def timespan_is_reversed(value: str) -> bool:
    """True hvis datospennet er tolkbart og sluttdato er før startdato."""
    parsed = parse_timespan(value)
    return parsed is not None and parsed[1] < parsed[0]


# ── Offentlig skannefunksjon (brukes av GUI-preflighten) ──────────────────────

def scan_metadata_quality(siard_path: Path) -> list[str]:
    """
    Skann metadata.xml for de to innholdsfeilene operasjonen retter.

    Rapporterer bevisst KUN harde feil — sti i dbname og omvendt datospenn —
    slik at preflight-dialogen ikke slår ut på hver eneste fil med lokalt
    datoformat. Selve operasjonen normaliserer i tillegg datoformatet når den
    først er lagt til.

    Returnerer tom liste ved ingen funn, eller ved feil under lesing.
    """
    try:
        with zipfile.ZipFile(siard_path, "r") as zf:
            name_lower = {n.lower(): n for n in zf.namelist()}
            meta_entry = next(
                (name_lower[c] for c in _METADATA_PATHS if c in name_lower), None)
            if not meta_entry:
                return []
            meta_bytes = zf.read(meta_entry)
    except Exception:
        return []

    meta_str, _ = _decode(meta_bytes)
    issues: list[str] = []

    dbname = _get_value(_DBNAME_RE, meta_str)
    if dbname and dbname_has_path(dbname):
        derived = derive_dbname(dbname)
        detail = f" → foreslått «{derived}»" if derived else ""
        issues.append(f"<dbname> inneholder filsti: «{dbname}»{detail}")

    timespan = _get_value(_TIMESPAN_RE, meta_str)
    if timespan and timespan_is_reversed(timespan):
        start, end, _ = parse_timespan(timespan)
        issues.append(
            f"<dataOriginTimespan> har sluttdato før startdato: «{timespan}» "
            f"(tolket {start.isoformat()} → {end.isoformat()})")

    return issues


# ── Felles retting ────────────────────────────────────────────────────────────

def _decode(raw: bytes) -> tuple[str, str]:
    """Dekod metadata-bytes. Returnerer (tekst, kodek) så vi kan skrive tilbake
    i SAMME koding — XML-deklarasjonen i fila skal fortsatt stemme."""
    try:
        return raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return raw.decode("latin-1"), "latin-1"


def _fix_metadata(meta_str: str,
                  dbname_override: str = "",
                  normalize_dates: bool = True) -> tuple[str, list[str]]:
    """Applikér rettelsene. Returnerer (ny_tekst, endringsliste)."""
    changes: list[str] = []

    # ── dbname ────────────────────────────────────────────────────────────────
    dbname = _get_value(_DBNAME_RE, meta_str)
    new_dbname = (dbname_override or "").strip()
    if not new_dbname and dbname and dbname_has_path(dbname):
        new_dbname = derive_dbname(dbname)
        if not new_dbname:
            changes.append(
                f"ADVARSEL: <dbname> «{dbname}» inneholder filsti, men lot seg "
                f"ikke utlede til et navn — feltet står urørt")
    if new_dbname and new_dbname != dbname:
        meta_str = _set_value(_DBNAME_RE, meta_str, new_dbname)
        changes.append(f"<dbname>: «{dbname}» → «{new_dbname}»")

    # ── dataOriginTimespan ────────────────────────────────────────────────────
    timespan = _get_value(_TIMESPAN_RE, meta_str)
    parsed = parse_timespan(timespan) if timespan else None
    if parsed:
        start, end, already_iso = parsed
        reversed_range = end < start
        if reversed_range:
            start, end = end, start
        if reversed_range or (normalize_dates and not already_iso):
            new_span = f"{start.isoformat()} - {end.isoformat()}"
            if new_span != timespan:
                meta_str = _set_value(_TIMESPAN_RE, meta_str, new_span)
                why = ("snudd omvendt datospenn" if reversed_range
                       else "normalisert til ISO-datoer")
                changes.append(
                    f"<dataOriginTimespan>: «{timespan}» → «{new_span}» ({why})")

    return meta_str, changes


# ── Operasjonsklassen ─────────────────────────────────────────────────────────

class MetadataQualityOperation(BaseOperation):
    """
    Retter innholdsfeil i metadata.xml: filsti i <dbname> og omvendt/lokalt
    formatert <dataOriginTimespan>.

    Pipeline-modus (ctx.extracted_path satt):
        Modifiserer header/metadata.xml direkte på disk. Produserer ingen ny
        SIARD — RepackSiardOperation tar seg av det.

    Standalone-modus:
        Leser fra SIARD-zip, skriver korrigert kopi til <original>_metafix.siard.
    """

    operation_id   = "metadata_quality"
    label          = "Rett metadata-kvalitet (dbname/datospenn)"
    description    = (
        "Retter to innholdsfeil i metadata.xml fra filbaserte uttrekk: filsti i "
        "<dbname> (f.eks. 'Data.Q:\\Ikava\\...\\Data' → 'Data') og "
        "<dataOriginTimespan> med sluttdato før startdato. Normaliserer også "
        "datospennet til ISO-datoer. <connection> røres ikke — den er proveniens."
    )
    category       = "Kompatibilitet"
    status         = 2
    produces_siard = True
    modifies_content = True
    premis_event_type  = "Adjustment"
    premis_event_label = "metadata-kvalitetskorreksjon"

    default_params = {
        "dbname":          "",     # tom = utled automatisk fra stien
        "normalize_dates": True,   # skriv datospenn som ISO (yyyy-mm-dd)
        "output_suffix":   "_metafix",
    }

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and result.data.get("fixes", 0) > 0

    def premis_detail(self, result, ctx) -> str:
        changes = result.data.get("changes") or []
        return "; ".join(changes) or "ingen metadata-endringer"

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        dbname_override = str(self.params.get("dbname") or "").strip()
        normalize_dates = bool(self.params.get("normalize_dates", True))

        # ── Pipeline-modus ────────────────────────────────────────────────────
        if ctx.extracted_path and ctx.extracted_path.is_dir():
            meta_path = ctx.extracted_path / "header" / "metadata.xml"
            if not meta_path.exists():
                meta_path = ctx.extracted_path / "metadata.xml"
            if not meta_path.exists():
                return self._fail("metadata.xml ikke funnet i utpakket mappe")

            meta_str, codec = _decode(meta_path.read_bytes())
            meta_str, changes = _fix_metadata(
                meta_str, dbname_override, normalize_dates)
            if not changes:
                return self._ok({"fixes": 0, "changes": []},
                                "Ingen metadata-kvalitetsproblemer funnet")

            meta_path.write_bytes(meta_str.encode(codec))
            for c in changes:
                w(f"  {c}", "ok")
            return self._ok({"fixes": len(changes), "changes": changes},
                            "; ".join(changes))

        # ── Standalone-modus ──────────────────────────────────────────────────
        siard_path = ctx.siard_path
        try:
            with zipfile.ZipFile(siard_path, "r") as zf:
                name_lower = {n.lower(): n for n in zf.namelist()}
                meta_entry = next(
                    (name_lower[c] for c in _METADATA_PATHS if c in name_lower),
                    None)
                if not meta_entry:
                    return self._fail("metadata.xml ikke funnet i SIARD-arkivet")
                meta_bytes = zf.read(meta_entry)
                all_info   = zf.infolist()
        except Exception as exc:
            return self._fail(f"Kunne ikke lese SIARD: {exc}")

        meta_str, codec = _decode(meta_bytes)
        meta_str, changes = _fix_metadata(
            meta_str, dbname_override, normalize_dates)
        if not changes:
            return self._ok({"fixes": 0, "changes": []},
                            "Ingen metadata-kvalitetsproblemer funnet")

        meta_bytes_new = meta_str.encode(codec)
        suffix = (self.params.get("output_suffix") or "_metafix").strip()
        dst_path = siard_path.with_name(
            siard_path.stem + suffix + siard_path.suffix)

        try:
            buf = io.BytesIO()
            with zipfile.ZipFile(siard_path, "r") as zin, \
                 zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED,
                                 allowZip64=True) as zout:
                for item in all_info:
                    data = (meta_bytes_new if item.filename == meta_entry
                            else zin.read(item.filename))
                    zout.writestr(item, data)
            dst_path.write_bytes(buf.getvalue())
        except Exception as exc:
            return self._fail(f"Feil ved skriving av SIARD: {exc}")

        for c in changes:
            w(f"  {c}", "ok")
        w(f"  Skrevet: {dst_path.name}", "ok")
        return self._ok(
            {"fixes": len(changes), "changes": changes,
             "output_path": str(dst_path)},
            "; ".join(changes))
