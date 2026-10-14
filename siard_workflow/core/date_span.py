"""siard_workflow/core/date_span.py

Uttrekkets datospenn (første og siste dato).

  * Oppgir metadata.xml et spenn i <dataOriginTimespan>, er det DET som gjelder
    — da estimeres ingenting (se metadata_span()). Spennet brukes også som
    periode (STARTDATE/ENDDATE) i DIAS-pakken (se period_for_dias()).
  * Bare når metadata.xml mangler et tolkbart spenn, ESTIMERES det ut fra
    datoverdiene. Målet er å treffe riktige fra–til-år, ikke eksakte datoer.
    Estimatet kan skrives inn i <dataOriginTimespan> (write_timespan()) — alltid
    med en XML-kommentar foran som sier at verdien er estimert. Kommentaren
    gjenkjennes senere (ESTIMATE_MARK), så spennet fortsatt merkes som estimat.

Fremgangsmåte for estimatet
  1. metadata.xml: finn datokolonner per tabell — DATE/TIMESTAMP, samt
     tekst- og tallkolonner med «dato/date» i navnet (dd.mm.åååå, åååå-mm-dd,
     ååååmmdd). Kolonner for fødselsdato o.l. utelates (ville dratt
     startåret langt bakover), det samme gjør fremtidsrettede kolonner
     (frist, utløp, gyldig til …).
  2. Bare relevante tabeller leses: tabeller med LOB-mappe (filer under
     content/schemaN/tableM/…) og journal-/sakstabeller (navn eller
     datokolonner med journal/sak). Finnes ingen slike, brukes alle tabeller.
  3. tableX.xml leses som bytes i blokker (raskt også for store tabeller):
     per rad hentes datoverdiene, og det noteres om raden peker på en LOB-fil
     (file=-attributt).
  4. Usannsynlige datoer (før MIN_YEAR eller etter arkiveringsdato/i dag)
     tas ikke med; heller ikke kjente plassholdere (1900-01-01, 1753-01-01 …).
  5. For hver tabell brukes én «hovedkolonne» (flest verdier, foretrekker navn
     som journaldato/dokumentdato/opprettet). I LOB-tabeller telles bare
     radene som peker på en LOB-fil; i journal-/sakstabeller alle radene.
  6. Fra–til-år uten enkeltstående feilregistreringer (se _span_years);
     første/siste dato innenfor disse årene rapporteres.

Et estimat merkes alltid slik i rapportene.
"""
from __future__ import annotations

import datetime as _dt
import re
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

NOTE = ("Estimert ut fra datoverdiene i uttrekket — ikke nødvendigvis 100 % korrekt. "
        "Bør kontrolleres mot kjent kunnskap om systemet.")
MIN_YEAR = 1850
TRIM = 0.0005                      # 0,05 % — ytterår med færre verdier regnes som avvik
NOTE_METADATA = ("Hentet fra <dataOriginTimespan> i metadata.xml — ikke estimert. "
                 "Brukes også som periode i DIAS-pakken.")
NOTE_METADATA_ESTIMATED = (
    "Hentet fra <dataOriginTimespan> i metadata.xml, men verdien ble ESTIMERT av "
    "SIARD Workflow Manager ut fra datoverdiene i uttrekket (se XML-kommentaren i "
    "metadata.xml) — ikke nødvendigvis 100 % korrekt.")
# Fast tekst i XML-kommentaren foran et estimert <dataOriginTimespan>
ESTIMATE_MARK = "dataOriginTimespan er estimert av SIARD Workflow Manager"

_DATE_TYPES = ("DATE", "TIMESTAMP", "DATETIME", "SMALLDATETIME")
_NAME_DATE = re.compile(r"dato|date|dt$|tidspunkt|timestamp|opprettet|registrert|created",
                        re.IGNORECASE)
_NAME_EXCLUDE = re.compile(
    r"f[oøö]d|birth|dob\b|f_?dato|fodt|født|alder|"          # fødsel
    r"frist|utl[oø]p|forfall|gyldig_?til|gyldigtil|valid_?to|expir|slutt_?plan|planlagt",
    re.IGNORECASE)
_NAME_PRIORITY = re.compile(
    r"journal|dok|brev|reg|opprett|created|mottatt|sendt|vedtak|sak|dato$|date$",
    re.IGNORECASE)
# Journal-/sakstabeller: tabellnavn (Noark 4: NOARKSAK/JOURNPOST, ePhorte o.l.:
# SA/JP) — kodetabeller som sakstype/saksstatus/saksbehandler tas ikke med
_TABLE_JS = re.compile(r"journ|(?:^|_|noark|arkiv)sak", re.IGNORECASE)
_TABLE_JS_EXACT = {"sa", "jp", "noarksak", "journpost"}
_TABLE_CODE = re.compile(r"(?:type|status|kode|code|beh\w*|part\w*|ansvar\w*|tilgang\w*)$",
                         re.IGNORECASE)
_COL_JS = re.compile(r"journ|^saks?_?(?:dato|aar|år|ar|date|year)", re.IGNORECASE)
_LOB_TYPES = ("BLOB", "CLOB", "NCLOB", "BINARY LARGE OBJECT", "CHARACTER LARGE OBJECT",
              "NATIONAL CHARACTER LARGE OBJECT")
_PLACEHOLDERS = {(1900, 1, 1), (1899, 12, 30), (1899, 12, 31), (1753, 1, 1), (1970, 1, 1)}

_RE_FILE = re.compile(rb'\sfile="')


@dataclass
class ColStat:
    table: str
    column: str
    col_type: str
    kind: str                      # «dato» | «tekst» | «tall»
    excluded: str = ""             # årsak når kolonnen ikke brukes
    n: int = 0
    n_lob: int = 0
    implausible: int = 0
    years: Counter = field(default_factory=Counter)
    years_lob: Counter = field(default_factory=Counter)
    first: "tuple | None" = None
    last: "tuple | None" = None
    first_lob: "tuple | None" = None
    last_lob: "tuple | None" = None
    # første/siste dato per år — gir eksakte datoer ved ytterårene i spennet
    ybounds: dict = field(default_factory=dict)
    ybounds_lob: dict = field(default_factory=dict)

    @staticmethod
    def _bound(d: dict, ymd: tuple) -> None:
        b = d.get(ymd[0])
        d[ymd[0]] = (ymd, ymd) if b is None else (min(b[0], ymd), max(b[1], ymd))

    def add(self, ymd: tuple, k: int = 1) -> None:
        """k forekomster av datoen (alle rader)."""
        self.n += k
        self.years[ymd[0]] += k
        self.first = ymd if self.first is None or ymd < self.first else self.first
        self.last = ymd if self.last is None or ymd > self.last else self.last
        self._bound(self.ybounds, ymd)

    def add_lob(self, ymd: tuple, k: int = 1) -> None:
        """k forekomster i rader med LOB-fil (i tillegg til add)."""
        self.n_lob += k
        self.years_lob[ymd[0]] += k
        self.first_lob = ymd if self.first_lob is None or ymd < self.first_lob else self.first_lob
        self.last_lob = ymd if self.last_lob is None or ymd > self.last_lob else self.last_lob
        self._bound(self.ybounds_lob, ymd)


# ── metadata.xml ─────────────────────────────────────────────────────────────

def _ln(el) -> str:
    return el.tag.rsplit("}", 1)[-1]


def _child(el, name):
    return next((c for c in el if _ln(c) == name), None)


def _text(el, name) -> str:
    c = _child(el, name)
    return (c.text or "").strip() if c is not None else ""


def _is_journal_sak(t_name: str, col_names: "list[str]") -> bool:
    n = t_name.strip().lower()
    if n in _TABLE_JS_EXACT:
        return True
    if _TABLE_JS.search(n) and not _TABLE_CODE.search(n):
        return True
    return any(_COL_JS.search(c) and not _NAME_EXCLUDE.search(c) for c in col_names)


def read_metadata(meta_bytes: bytes) -> "tuple[list[dict], dict]":
    """
    ([{label, xml, dir, columns:{idx: ColStat}, lob_declared, journal_sak}], info).
    Bare tabeller med datokolonner tas med. lob_declared: tabellen har en
    LOB-kolonne i metadata.xml — om den faktisk har en LOB-mappe med filer,
    avgjøres mot innholdet (se estimate()).
    """
    root = ET.fromstring(meta_bytes)
    info = {"archival_date": "", "timespan": "",
            "timespan_estimated": ESTIMATE_MARK.encode("utf-8") in meta_bytes}
    for el in root.iter():
        n = _ln(el)
        if n == "archivalDate" and not info["archival_date"]:
            info["archival_date"] = (el.text or "").strip()
        elif n == "dataOriginTimespan" and not info["timespan"]:
            info["timespan"] = (el.text or "").strip()
    tables = []
    for sch in root.iter():
        if _ln(sch) != "schema":
            continue
        s_name, s_folder = _text(sch, "name"), _text(sch, "folder")
        tabs = _child(sch, "tables")
        for tab in (tabs if tabs is not None else []):
            t_name, t_folder = _text(tab, "name"), _text(tab, "folder")
            cols = _child(tab, "columns")
            label = f"{s_name}.{t_name}" if s_name else t_name
            colmap: dict = {}
            has_lob = False
            col_names: list = []
            for i, col in enumerate(cols if cols is not None else [], 1):
                c_name, c_type = _text(col, "name"), _text(col, "type").upper()
                base = c_type.split("(")[0].strip()
                col_names.append(c_name)
                if _child(col, "lobFolder") is not None or base in _LOB_TYPES:
                    has_lob = True
                    continue
                if base.startswith(_DATE_TYPES) and not base.startswith("TIME "):
                    kind = "dato"
                elif _NAME_DATE.search(c_name) and ("CHAR" in base or "TEXT" in base):
                    kind = "tekst"
                elif _NAME_DATE.search(c_name) and base in (
                        "INTEGER", "INT", "BIGINT", "DECIMAL", "NUMERIC"):
                    kind = "tall"
                else:
                    continue
                cs = ColStat(label, c_name, c_type, kind)
                if _NAME_EXCLUDE.search(c_name):
                    cs.excluded = "navnet tyder på fødselsdato/frist — påvirker ikke estimatet"
                colmap[i] = cs
            if colmap:
                tables.append({"label": label,
                               "xml": f"content/{s_folder}/{t_folder}/{t_folder}.xml",
                               "dir": f"content/{s_folder}/{t_folder}",
                               "columns": colmap, "lob_declared": has_lob,
                               "journal_sak": _is_journal_sak(t_name, col_names)})
    return tables, info


# ── <dataOriginTimespan> ─────────────────────────────────────────────────────

_RX_SPAN_DATE = re.compile(
    r"(?<!\d)(?:(\d{4})-(\d{1,2})-(\d{1,2})|(\d{1,2})[./-](\d{1,2})[./-](\d{4}))(?!\d)")
_RX_YEAR = re.compile(r"(?<!\d)(1[89]\d{2}|20\d{2})(?!\d)")


def metadata_span(ts: str) -> "tuple[_dt.date, _dt.date] | None":
    """
    Tolk <dataOriginTimespan> (fri tekst) som (første, siste dato).
    Godtar ISO- og norske datoer (dd.mm.åååå, dd/mm/åååå, dd-mm-åååå) og rene
    årstall («2014 - 2020» → 2014-01-01 – 2020-12-31), også omvendt rekkefølge.
    Krever to ytterpunkter — én enkelt dato eller tekst uten datoer gir None.
    """
    points: list = []
    for m in _RX_SPAN_DATE.finditer(ts or ""):
        try:
            d = (_dt.date(int(m[1]), int(m[2]), int(m[3])) if m[1]
                 else _dt.date(int(m[6]), int(m[5]), int(m[4])))
        except ValueError:
            continue
        points.append((d, d))
    for y in _RX_YEAR.findall(_RX_SPAN_DATE.sub(" ", ts or "")):
        points.append((_dt.date(int(y), 1, 1), _dt.date(int(y), 12, 31)))
    if len(points) < 2:
        return None
    return min(p[0] for p in points), max(p[1] for p in points)


def _open_source(source: Path):
    """
    (metadata-bytes, opener(rel) → filobjekt|None, close(), has_lob_dir(tabellmappe))
    for mappe eller .siard. has_lob_dir: tabellmappa har undermapper med filer.
    """
    source = Path(source)
    if source.is_dir():
        def opener(rel):
            p = source / rel
            return open(p, "rb") if p.exists() else None

        def has_lob_dir(rel):
            d = source / rel
            try:
                return any(sub.is_dir() and any(f.is_file() for f in sub.rglob("*"))
                           for sub in d.iterdir())
            except OSError:
                return False
        return ((source / "header" / "metadata.xml").read_bytes(), opener, lambda: None,
                has_lob_dir)
    zf = zipfile.ZipFile(source)
    try:
        names = {n.lower(): n for n in zf.namelist()}
        meta_bytes = zf.read(names.get("header/metadata.xml", "header/metadata.xml"))
    except Exception:
        zf.close()
        raise

    def opener(rel):
        n = names.get(rel.lower())
        return zf.open(n) if n else None
    lob_dirs = {m[1] for m in (re.match(r"(content/[^/]+/[^/]+)/[^/]+/.*[^/]$", n)
                               for n in names) if m}
    return meta_bytes, opener, zf.close, lambda rel: rel.lower() in lob_dirs


def read_timespan(source: Path) -> str:
    """<dataOriginTimespan> fra metadata.xml i en mappe eller .siard-fil ("" ved feil)."""
    try:
        meta_bytes, _o, close, _l = _open_source(source)
        close()
        return read_metadata(meta_bytes)[1]["timespan"]
    except Exception:
        return ""


def period_for_dias(source: "Path | None", date_span_result: "dict | None" = None
                    ) -> "tuple[str, str, str] | None":
    """
    Periode (start, slutt, kilde) for DIAS-pakken: spennet i metadata.xml
    når det finnes, ellers et estimat fra «Estimer datospenn» i samme kjøring.
    """
    r = date_span_result or {}
    if r.get("found") and r.get("written_to_metadata"):
        return (_full_date(r["from_date"], True), _full_date(r["to_date"], False),
                "estimert fra uttrekket og skrevet til metadata.xml")
    ms = metadata_span(read_timespan(source)) if source else None
    if ms:
        return ms[0].isoformat(), ms[1].isoformat(), "metadata.xml <dataOriginTimespan>"
    if r.get("found") and r.get("source") == "estimat" and r.get("from_date"):
        return (_full_date(r["from_date"], True), _full_date(r["to_date"], False),
                "estimert fra uttrekket («Estimer datospenn»)")
    return None


# <dataOriginTimespan> (med eller uten navnerom-prefiks, også tomt element)
_RX_TS_ELEM = re.compile(
    r"(?P<indent>[ \t]*)<(?P<p>(?:[\w.-]+:)?)dataOriginTimespan(?:\s[^>]*)?"
    r"(?:/>|>(?P<val>.*?)</(?P=p)dataOriginTimespan\s*>)", re.S)
# Elementer <dataOriginTimespan> skal stå etter (SIARD 2: dataOwner) eller før
_RX_AFTER = re.compile(
    r"(?P<indent>[ \t]*)<(?P<p>(?:[\w.-]+:)?)dataOwner(?:\s[^>]*)?"
    r"(?:/>|>.*?</(?P=p)dataOwner\s*>)", re.S)
_RX_BEFORE = re.compile(
    r"(?P<indent>[ \t]*)<(?P<p>(?:[\w.-]+:)?)(?:lobFolder|producerApplication|archivalDate)\b")


def _comment_safe(text: str) -> str:
    """XML-kommentarer kan ikke inneholde «--» eller slutte med «-»."""
    text = re.sub(r"-{2,}", lambda m: "- " * (len(m[0]) - 1) + "-", text)
    return text.rstrip("-").replace("\n", " ")


def write_timespan(meta_bytes: bytes, res: dict,
                   today: "_dt.date | None" = None) -> "tuple[bytes, str, str] | None":
    """
    Skriv et estimert spenn inn i <dataOriginTimespan>, med en XML-kommentar
    foran som sier at verdien er estimert (og hva som eventuelt sto der før).
    Returnerer (nye metadata-bytes, ny verdi, kommentartekst), eller None når
    metadata.xml allerede oppgir et spenn, estimatet mangler, eller elementet
    ikke kan plasseres.
    """
    if not (res.get("found") and res.get("source") == "estimat"):
        return None
    try:
        text, codec = meta_bytes.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        text, codec = meta_bytes.decode("latin-1"), "latin-1"
    m = _RX_TS_ELEM.search(text)
    old = re.sub(r"\s+", " ", (m["val"] or "") if m else "").strip()
    if metadata_span(old):
        return None                                  # metadata.xml oppgir spennet selv
    value = f"{_full_date(res['from_date'], True)} - {_full_date(res['to_date'], False)}"
    today = today or _dt.date.today()
    comment = _comment_safe(
        f"{ESTIMATE_MARK} {today.isoformat()} ut fra datoverdiene i uttrekket "
        f"(grunnlag: {res.get('basis_text', '')}; sikkerhet: {res.get('confidence', '')}). "
        f"Estimat — ikke nødvendigvis 100 % korrekt."
        + (f" Opprinnelig verdi: «{old}»." if old else ""))
    if m:
        ind, pre = m["indent"], m["p"]
        new = (f"{ind}<!-- {comment} -->\n"
               f"{ind}<{pre}dataOriginTimespan>{value}</{pre}dataOriginTimespan>")
        text = text[:m.start()] + new + text[m.end():]
    else:
        a = _RX_AFTER.search(text)
        b = None if a else _RX_BEFORE.search(text)
        if not (a or b):
            return None
        ref = a or b
        ind, pre = ref["indent"], ref["p"]
        elem = (f"{ind}<!-- {comment} -->\n"
                f"{ind}<{pre}dataOriginTimespan>{value}</{pre}dataOriginTimespan>")
        if a:
            text = text[:a.end()] + "\n" + elem + text[a.end():]
        else:
            text = text[:b.start()] + elem + "\n" + text[b.start():]
    return text.encode(codec), value, comment


def _full_date(s: str, start: bool) -> str:
    """«2015» → 2015-01-01 / 2015-12-31; ISO-dato uendret."""
    s = (s or "").strip()
    if re.fullmatch(r"\d{4}", s):
        return f"{s}-01-01" if start else f"{s}-12-31"
    return s


def _parse_date(s: str) -> "_dt.date | None":
    m = re.match(r"\s*(\d{4})-(\d{2})-(\d{2})", s or "")
    if m:
        try:
            return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


# ── Skanning ─────────────────────────────────────────────────────────────────

def _scan_stream(read: Callable[[int], bytes], columns: dict, max_year: int) -> int:
    """
    Les en tableX.xml i blokker og registrer datoverdier. Returnerer antall rader.
    Hele blokker søkes med regex (ikke rad for rad) — radtilhørighet og om raden
    har en LOB-fil avgjøres med binærsøk i radslutt-posisjonene.
    """
    from bisect import bisect_left
    # Regex bare for denne tabellens datokolonner — ellers matches alle
    # datolignende verdier i alle kolonner, og Python-løkka blir flaskehalsen
    def _rx(kinds, tail: bytes):
        idx = sorted(i for i, c in columns.items() if c.kind in kinds)
        if not idx:
            return None
        alt = b"|".join(str(i).encode() for i in idx)
        return re.compile(rb"<c(" + alt + rb")>\s*" + tail)
    passes = [(r, o, k) for r, o, k in (
        (_rx(("dato", "tekst"), rb"(\d{4})-(\d{2})-(\d{2})"), (1, 2, 3), None),
        (_rx(("tekst",), rb"(\d{1,2})\.(\d{1,2})\.(\d{4})"), (3, 2, 1), "tekst"),
        (_rx(("tall",), rb"((?:1[89]|20)\d{2})(\d{2})(\d{2})\s*</c"), (1, 2, 3), "tall"),
    ) if r is not None]
    rows = 0
    buf = b""
    while True:
        chunk = read(8 << 20)
        if chunk:
            buf += chunk
            cut = buf.rfind(b"</row>")
            if cut < 0:
                continue
            block, buf = buf[:cut + 6], buf[cut + 6:]
        else:
            block, buf = buf, b""
        ends = [m.start() for m in re.finditer(rb"</row>", block)]
        if not chunk and block.strip() and (not ends or block.rstrip()[-6:] != b"</row>"):
            ends.append(len(block))              # siste rad uten </row> (avkortet fil)
        rows += len(ends)
        # Rader med LOB-fil: samles til en egen tekst som telles i tillegg
        lob_idx = sorted({bisect_left(ends, m.start()) for m in _RE_FILE.finditer(block)})
        lob_text = b"</row>".join(block[(ends[i - 1] if i else 0):ends[i]]
                                  for i in lob_idx if i < len(ends)) if lob_idx else b""
        for rx, order, kind in passes:
            # findall + Counter (C) — samme dato forekommer mange ganger; hver
            # unike dato tolkes og kontrolleres bare én gang
            for text, lob in ((block, False), (lob_text, True)):
                if not text:
                    continue
                for (col, a, b, c), k in Counter(rx.findall(text)).items():
                    cs = columns.get(int(col))
                    if cs is None or (kind and cs.kind != kind):
                        continue
                    parts = (a, b, c)
                    try:
                        y, mo, d = (int(parts[i - 1]) for i in order)
                        _dt.date(y, mo, d)
                    except ValueError:
                        if not lob:
                            cs.implausible += k
                        continue
                    if y < MIN_YEAR or y > max_year or (y, mo, d) in _PLACEHOLDERS:
                        if not lob:
                            cs.implausible += k
                        continue
                    if lob:
                        cs.add_lob((y, mo, d), k)
                    else:
                        cs.add((y, mo, d), k)
        if not chunk:
            break
    return rows


def _span_years(c: Counter, trim: float = TRIM) -> "tuple[int, int] | None":
    """
    Fra–til-år uten enkeltstående feilregistreringer. Et ytterår regnes som
    avvik når det har svært få verdier (under `trim` av alle, minst 2), eller
    ligger isolert (mer enn 2 år til neste år med data) og har under 1 %.
    Et kort første/siste år som henger sammen med resten, beholdes — det er
    typisk et delår (uttrekket starter i oktober o.l.).
    """
    total = sum(c.values())
    if not total:
        return None
    ys = sorted(y for y in c if c[y] > 0)
    small = max(2, total * trim)

    def outlier(y, neighbour):
        return c[y] < small or (abs(neighbour - y) > 2 and c[y] < total * 0.01)
    lo, hi = 0, len(ys) - 1
    while lo < hi and outlier(ys[lo], ys[lo + 1]):
        lo += 1
    while hi > lo and outlier(ys[hi], ys[hi - 1]):
        hi -= 1
    return ys[lo], ys[hi]


def from_metadata(info: dict, span: "tuple[_dt.date, _dt.date]") -> dict:
    """Resultat når metadata.xml oppgir spennet — ingen estimering."""
    a, b = span
    est = bool(info.get("timespan_estimated"))
    return {
        "found": True, "source": "metadata", "estimated": False,
        "metadata_estimated": est,
        "note": NOTE_METADATA_ESTIMATED if est else NOTE_METADATA,
        "from_year": a.year, "to_year": b.year,
        "from_date": a.isoformat(), "to_date": b.isoformat(),
        "metadata_timespan": info.get("timespan", ""),
        "archival_date": info.get("archival_date", ""),
        "basis_text": "metadata.xml <dataOriginTimespan>", "columns": [], "tables_scanned": [],
        "message": f"Datospenn fra metadata.xml: {a.year}–{b.year} "
                   f"({a.isoformat()} – {b.isoformat()})",
    }


def estimate(source: Path, progress: "Optional[Callable[[str], None]]" = None,
             stop: "Optional[Callable[[], bool]]" = None,
             trim: float = TRIM, today: "_dt.date | None" = None,
             use_metadata: bool = True) -> dict:
    """
    Datospennet for en utpakket SIARD-mappe eller en .siard-fil: fra metadata.xml
    når den oppgir et spenn (use_metadata), ellers estimert fra relevante tabeller.
    """
    meta_bytes, opener, close, has_lob_dir = _open_source(source)
    try:
        tables, info = read_metadata(meta_bytes)
        span = metadata_span(info["timespan"]) if use_metadata else None
        if span:
            return from_metadata(info, span)
        # LOB-tabell = tabell med LOB-mappe (filer) i uttrekket. CLOB-kolonner med
        # bare inline tekst gjør ikke en tabell til en dokumenttabell. Ligger
        # LOB-ene utenfor (eksternt fillager), brukes LOB-kolonnene i metadata.xml.
        dirs = {t["label"]: has_lob_dir(t["dir"]) for t in tables}
        for t in tables:
            t["lob"] = dirs[t["label"]] if any(dirs.values()) else t["lob_declared"]
        arch = _parse_date(info["archival_date"])
        today = today or _dt.date.today()
        max_year = min(arch.year if arch else today.year, today.year)
        relevant = [t for t in tables if t["lob"] or t["journal_sak"]]
        selection = "utvalgte" if relevant else "alle"
        todo = relevant or tables
        if progress:
            progress(f"{len(todo)} av {len(tables)} tabeller med datokolonner leses"
                     + (" (LOB- og journal-/sakstabeller)" if relevant else
                        " — fant ingen LOB- eller journal-/sakstabeller"))
        stats: list = []
        read: dict = {}
        n_rows = 0
        for i, t in enumerate(todo, 1):
            if stop and stop():
                break
            f = opener(t["xml"])
            if f is None:
                continue
            if progress:
                progress(f"[{i}/{len(todo)}] {t['label']} "
                         f"({len(t['columns'])} datokolonne(r))")
            with f:
                n_rows += _scan_stream(f.read, t["columns"], max_year)
            stats += list(t["columns"].values())
            read[t["label"]] = t
    finally:
        close()
    return summarize(stats, info, max_year, trim, n_rows, read, selection)


def _primary(cols: "list[ColStat]", lob: bool) -> "ColStat | None":
    usable = [c for c in cols if not c.excluded and (c.n_lob if lob else c.n)]
    if not usable:
        return None
    return max(usable, key=lambda c: ((c.n_lob if lob else c.n),
                                      1 if _NAME_PRIORITY.search(c.column) else 0))


def summarize(stats: "list[ColStat]", info: dict, max_year: int, trim: float = TRIM,
              n_rows: int = 0, tables: "dict | None" = None,
              selection: str = "alle") -> dict:
    """
    Slå sammen hovedkolonnene til et estimat. tables: label → tabellpost fra
    read_metadata (lob/journal_sak); selection «utvalgte» | «alle».
    """
    tables = tables or {}
    by_table: dict = {}
    for c in stats:
        by_table.setdefault(c.table, []).append(c)
    picks: list = []                                   # (kolonne, bruk LOB-rader)
    for label, cols in by_table.items():
        t = tables.get(label, {})
        pl = _primary(cols, True) if t.get("lob") else None
        if pl:                                         # LOB-tabell: rader med LOB-fil
            picks.append((pl, True))
            continue
        pa = _primary(cols, False) if (t.get("journal_sak") or selection == "alle") else None
        if pa:                                         # journal/sak (eller alle tabeller)
            picks.append((pa, False))
    years: Counter = Counter()
    for c, lob in picks:
        years.update(c.years_lob if lob else c.years)
    n_lob = sum(c.n_lob for c, lob in picks if lob)
    n_oth = sum(c.n for c, lob in picks if not lob)
    t_lob = sum(1 for _c, lob in picks if lob)
    t_oth = len(picks) - t_lob
    parts = []
    if t_lob:
        parts.append(f"datoer i {n_lob:,} rader med LOB-filer (dokumenter) i {t_lob} tabell(er)")
    if t_oth:
        parts.append(f"{n_oth:,} datoer i {t_oth} "
                     + ("journal-/sakstabell(er)" if selection == "utvalgte" else "tabell(er)"))
    basis_text = " og ".join(parts) or "ingen"
    if selection == "alle":
        basis_text += " — fant ingen LOB- eller journal-/sakstabeller, alle tabeller er lest"
    res = {
        "estimated": True, "source": "estimat", "note": NOTE, "basis": selection,
        "basis_text": basis_text, "rows_scanned": n_rows, "n_values": sum(years.values()),
        "metadata_timespan": info.get("timespan", ""),
        "archival_date": info.get("archival_date", ""), "max_year": max_year,
        "year_counts": {str(y): n for y, n in sorted(years.items())},
        "tables_scanned": [{"table": lbl, "why": _why(t)} for lbl, t in tables.items()],
    }
    span = _span_years(years, trim)
    if not span:
        res.update(found=False, message="Fant ingen brukbare datoverdier i uttrekket.")
        res["columns"] = _columns_out(stats, picks)
        return res
    fy, ty = span
    yb = [(c.ybounds_lob if lob else c.ybounds) for c, lob in picks]
    first = min((b[fy][0] for b in yb if fy in b), default=None)
    last = max((b[ty][1] for b in yb if ty in b), default=None)
    abs_first = min((d for d in ((c.first_lob if lob else c.first) for c, lob in picks) if d),
                    default=None)
    abs_last = max((d for d in ((c.last_lob if lob else c.last) for c, lob in picks) if d),
                   default=None)
    outside = sum(n for y, n in years.items() if y < fy or y > ty)
    n = res["n_values"]
    confidence = ("høy" if n >= 500 and selection == "utvalgte"
                  else "middels" if n >= 100 else "lav")
    res.update(
        found=True, from_year=fy, to_year=ty,
        from_date=_iso(first) or f"{fy}", to_date=_iso(last) or f"{ty}",
        absolute_first=_iso(abs_first), absolute_last=_iso(abs_last),
        outside_trim=outside, confidence=confidence,
        columns=_columns_out(stats, picks),
        message=f"Estimert datospenn {fy}–{ty} ({confidence} sikkerhet; grunnlag: {basis_text})",
    )
    return res


def _why(t: dict) -> str:
    return " + ".join(w for w, on in (("LOB-filer", t.get("lob")),
                                      ("journal/sak", t.get("journal_sak"))) if on) or "alle"


def _iso(d) -> str:
    return f"{d[0]:04d}-{d[1]:02d}-{d[2]:02d}" if d else ""


def _columns_out(stats: "list[ColStat]", picks: list, limit: int = 40) -> "list[dict]":
    used = {id(c): lob for c, lob in picks}
    out = []
    for c in sorted(stats, key=lambda c: (id(c) not in used, -c.n)):
        span = _span_years(c.years, TRIM)
        out.append({
            "table": c.table, "column": c.column, "type": c.col_type, "kind": c.kind,
            "n": c.n, "n_lob": c.n_lob, "implausible": c.implausible,
            "first": _iso(c.first), "last": _iso(c.last),
            "span": f"{span[0]}–{span[1]}" if span else "",
            "primary": id(c) in used, "lob_rows": used.get(id(c), False),
            "excluded": c.excluded,
        })
    return out[:limit]
