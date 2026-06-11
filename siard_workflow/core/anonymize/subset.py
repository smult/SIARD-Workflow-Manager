"""
siard_workflow/core/anonymize/subset.py
----------------------------------------
Referensielt konsistent datareduksjon («subsetting») for SIARD-arkiv.

Brukes av AnonymizeOperation når man vil lage en LITEN, anonymisert testutgave:
i stedet for å anonymisere HELE datasettet beholdes inntil N rader fra de
viktige tabellene, pluss de relaterte radene som trengs for at fremmednøkler
fortsatt henger sammen.

Algoritme (deterministisk — Ollama brukes kun til å foreslå hvilke tabeller som
er «viktige», ikke til radutvalget):

  1. Les primær-/fremmednøkler fra header/metadata.xml (read_relations).
  2. Indekser hver tableX.xml én gang (build_index): PK-verdi → radposisjon,
     FK-verdier per rad, og barn gruppert etter forelder-nøkkel.
  3. Velg rader (select_rows):
       • Frø: inntil N rader (jevnt fordelt) fra hver tabell — slik at INGEN
         tabell blir helt tom. Små oppslagstabeller beholdes i sin helhet.
       • Barn: fra hver viktig frø-rad tas inntil `child_cap` barnerader med
         (ett nivå) — gir sammenhengende kontekst (f.eks. journallinjer).
       • Foreldre (transitivt, obligatorisk): for HVER beholdt rad tas refererte
         forelder-rader med, til fikspunkt → ingen FK peker i tomme luften.
  4. Resultat: {table_key: set(row_pos)} som anonymiseringen filtrerer på.

Antakelse: fremmednøkler refererer parentens nøkkelkolonner (PK eller unik
nøkkel). Generell — indekserer faktisk refererte kolonnesett, ikke bare PK.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path


_NS_RE   = re.compile(r"^\{[^}]+\}")
_CELL_RE = re.compile(rb"<c(\d+)>(.*?)</c\1>", re.DOTALL)


def _local(tag: str) -> str:
    return _NS_RE.sub("", tag)


def _ctext(el, name: str) -> str:
    for ch in el:
        if _local(ch.tag).lower() == name.lower():
            return (ch.text or "").strip()
    return ""


def _decode(raw: bytes) -> str:
    import html
    return html.unescape(raw.decode("utf-8", errors="replace"))


# ─────────────────────────────────────────────────────────────────────────────
# Datastrukturer
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Fk:
    name: str
    cols: tuple                 # lokale kolonneindekser (1-basert)
    parent_key: str             # forelder-tabellens table_key
    parent_cols: tuple          # refererte kolonneindekser i forelderen (1-basert)


@dataclass
class Relation:
    pk_cols: tuple = ()         # PK-kolonneindekser (1-basert), kan være tom
    fks: list = field(default_factory=list)   # list[Fk]


@dataclass
class SubsetPlan:
    important: set              # set[table_key] — sentrale tabeller
    n_rows: int = 300           # rader per tabell (frø)
    include_children: bool = True
    child_cap: int = 0          # maks barnerader per forelder-rad (0 = n_rows)
    keep_full_threshold: int = 500   # tabeller ≤ dette beholdes i sin helhet
    max_keep_rows: int = 0      # global sikkerhetstak (0 = 50 × n_rows × antall tabeller)
    exclude: frozenset = frozenset()   # table_keys som utelates helt (f.eks. kopitabeller)


# ── Sentrale «entitets»-tabeller: prioriteres ved auto-valg av viktige tabeller ─
# Tabeller om personer/klienter/brukere/ansatte + journal/sak/dokument/melding/
# notat o.l. er nesten alltid de kritiske kjernetabellene i et fagsystem.
ENTITY_KEYWORDS = (
    "person", "klient", "client", "bruker", "user", "ansatt", "employee",
    "medarbeider", "kontakt", "contact", "elev", "pasient", "patient",
    "innbygger", "borger", "medlem", "member", "kunde", "customer", "barn",
    "individ", "deltaker", "soker", "mottaker", "avsender",
    "journal", "sak", "case", "dokument", "document", "melding", "message",
    "mail", "notat", "note", "vedtak", "vedlegg", "brev", "henvendelse",
    "soknad", "soeknad", "oppgave", "hendelse", "aktivitet", "behandling",
)


def _entity_score(table_name: str) -> int:
    nm = (table_name or "").lower()
    return sum(1 for kw in ENTITY_KEYWORDS if kw in nm)


# ─────────────────────────────────────────────────────────────────────────────
# 1) Les relasjoner (PK/FK) fra metadata.xml
# ─────────────────────────────────────────────────────────────────────────────

def read_relations(metadata_path: Path, tables: dict) -> "dict[str, Relation]":
    """Returnerer {table_key: Relation}. `tables` er resultatet av
    anonymize_operation.read_tables (gir folders, navn og kolonne-indekser)."""
    rels: dict[str, Relation] = {tk: Relation() for tk in tables}
    if not metadata_path.exists():
        return rels

    # Navn → table_key, og per-tabell kolonnenavn → indeks
    name_to_key: dict[tuple, str] = {}
    colidx: dict[str, dict[str, int]] = {}
    for tk, info in tables.items():
        name_to_key[(info["schema_name"].lower(), info["table_name"].lower())] = tk
        colidx[tk] = {c["name"].lower(): c["idx"] for c in info["columns"]}

    # folder → table_key (for å matche XML-tabell til riktig nøkkel)
    folder_to_key: dict[tuple, str] = {
        (info["schema_folder"], info["table_folder"]): tk
        for tk, info in tables.items()}

    try:
        root = ET.parse(metadata_path).getroot()
    except Exception:
        return rels

    for schema in root.iter():
        if _local(schema.tag).lower() != "schema":
            continue
        s_folder = _ctext(schema, "folder")
        for table in schema.iter():
            if _local(table.tag).lower() != "table":
                continue
            t_folder = _ctext(table, "folder")
            tk = folder_to_key.get((s_folder, t_folder))
            if tk is None:
                continue
            ci = colidx.get(tk, {})

            # Primærnøkkel
            pk_cols: list[int] = []
            for child in table:
                if _local(child.tag).lower() == "primarykey":
                    for c in child:
                        if _local(c.tag).lower() == "column" and c.text:
                            i = ci.get(c.text.strip().lower())
                            if i:
                                pk_cols.append(i)

            # Fremmednøkler
            fks: list[Fk] = []
            for wrapper in table:
                if _local(wrapper.tag).lower() != "foreignkeys":
                    continue
                for fk_el in wrapper:
                    if _local(fk_el.tag).lower() != "foreignkey":
                        continue
                    ref_schema = _ctext(fk_el, "referencedSchema")
                    ref_table  = _ctext(fk_el, "referencedTable")
                    parent_key = name_to_key.get(
                        (ref_schema.lower(), ref_table.lower()))
                    if parent_key is None:
                        continue
                    pci = colidx.get(parent_key, {})
                    local_cols: list[int] = []
                    parent_cols: list[int] = []
                    fk_name = _ctext(fk_el, "name")
                    for sub in fk_el:
                        if _local(sub.tag).lower() != "reference":
                            continue
                        lc = _ctext(sub, "column")
                        rc = _ctext(sub, "referenced")
                        li = ci.get(lc.lower())
                        ri = pci.get(rc.lower())
                        if li and ri:
                            local_cols.append(li)
                            parent_cols.append(ri)
                    if local_cols and len(local_cols) == len(parent_cols):
                        fks.append(Fk(fk_name, tuple(local_cols),
                                      parent_key, tuple(parent_cols)))

            rels[tk] = Relation(tuple(pk_cols), fks)
    return rels


# ─────────────────────────────────────────────────────────────────────────────
# 2) Indekser radene
# ─────────────────────────────────────────────────────────────────────────────

def iter_rows(xml_path: Path):
    """Yield (row_pos, {col_idx: value}) for hver <row> i tableX.xml.
    NULL-celler (utelatt <cN>) finnes ikke i dicten. Selvlukkende fil-ref-celler
    (<cN .../>) gir ingen verdi (LOB) og hoppes over her."""
    if not xml_path.exists():
        return
    in_row = False
    buf: list[bytes] = []
    pos = 0
    with open(xml_path, "rb") as f:
        for line in f:
            if b"<row" in line:
                in_row, buf = True, []
            if in_row:
                buf.append(line)
            if in_row and b"</row>" in line:
                in_row = False
                blob = b"".join(buf)
                row = {int(m.group(1)): _decode(m.group(2))
                       for m in _CELL_RE.finditer(blob)}
                yield pos, row
                pos += 1


@dataclass
class Index:
    row_count: dict          # table_key -> int
    pk_vals: dict            # table_key -> {row_pos: pk_tuple}
    fwd: dict                # table_key -> {colset: {value_tuple: row_pos}}
    rev: dict                # table_key -> {colset: {row_pos: value_tuple}}
    fk_local: dict           # table_key -> {row_pos: {fk_i: value_tuple}}
    children: dict           # parent_key -> list[(child_key, fk_i, pcs, {value: [pos]})]


def build_index(root: Path, tables: dict, relations: "dict[str, Relation]",
                table_xml_path, progress=None) -> Index:
    """Strøm hver tableX.xml én gang og bygg oppslags-indekser for utvalg.

    table_xml_path(root, info) -> Path (gjenbruker operasjonens hjelpefunksjon).
    """
    # Hvilke kolonnesett må indekseres med verdi↔pos på hver tabell?
    # = alle parent_cols-sett fra FK-er som peker TIL tabellen.
    index_colsets: dict[str, set] = {tk: set() for tk in tables}
    for tk, rel in relations.items():
        for fk in rel.fks:
            index_colsets.setdefault(fk.parent_key, set()).add(tuple(fk.parent_cols))

    row_count: dict = {}
    pk_vals: dict = {tk: {} for tk in tables}
    fwd: dict = {tk: {cs: {} for cs in index_colsets.get(tk, ())} for tk in tables}
    rev: dict = {tk: {cs: {} for cs in index_colsets.get(tk, ())} for tk in tables}
    fk_local: dict = {tk: {} for tk in tables}
    children: dict = {tk: [] for tk in tables}
    # midlertidig: parent_key -> (child_key, fk_i, pcs) -> {value: [pos]}
    child_groups: dict = {}

    n = len(tables)
    for ti, (tk, info) in enumerate(tables.items(), 1):
        rel = relations.get(tk, Relation())
        xml_path = table_xml_path(root, info)
        colsets = index_colsets.get(tk, ())
        cnt = 0
        for pos, row in iter_rows(xml_path):
            cnt = pos + 1
            # PK-verdi
            if rel.pk_cols:
                pv = tuple(row.get(c) for c in rel.pk_cols)
                if all(v is not None for v in pv):
                    pk_vals[tk][pos] = pv
            # Indekserte kolonnesett (forelder-side)
            for cs in colsets:
                val = tuple(row.get(c) for c in cs)
                if all(v is not None for v in val):
                    fwd[tk][cs][val] = pos
                    rev[tk][cs][pos] = val
            # FK-verdier (barn-side) + barn-gruppering
            for fi, fk in enumerate(rel.fks):
                lv = tuple(row.get(c) for c in fk.cols)
                if any(v is None for v in lv):
                    continue
                fk_local[tk].setdefault(pos, {})[fi] = lv
                gkey = (fk.parent_key, tk, fi, tuple(fk.parent_cols))
                grp = child_groups.setdefault(gkey, {})
                grp.setdefault(lv, []).append(pos)
        row_count[tk] = cnt
        if progress:
            progress(ti, n)

    # Bygg children-listen per forelder
    for (parent_key, child_key, fi, pcs), grp in child_groups.items():
        children.setdefault(parent_key, []).append((child_key, fi, pcs, grp))

    return Index(row_count, pk_vals, fwd, rev, fk_local, children)


# ─────────────────────────────────────────────────────────────────────────────
# 3) Velg rader
# ─────────────────────────────────────────────────────────────────────────────

def _spread(total: int, n: int) -> "list[int]":
    """n radposisjoner jevnt fordelt over [0, total)."""
    if total <= 0:
        return []
    if total <= n:
        return list(range(total))
    step = total / n
    return sorted({int(i * step) for i in range(n)})


def select_rows(index: Index, relations: "dict[str, Relation]",
                tables: dict, plan: SubsetPlan,
                w=None) -> "tuple[dict, dict]":
    """Returnerer (keep, info) der keep = {table_key: set(row_pos)} og info er
    en oppsummering for rapport/forhåndsvisning."""
    keep: dict[str, set] = {tk: set() for tk in tables}
    n = max(1, int(plan.n_rows))
    child_cap = plan.child_cap or n
    full_thr = plan.keep_full_threshold
    max_keep = plan.max_keep_rows or (50 * n * max(1, len(tables)))

    frontier: "deque[tuple]" = deque()
    total_kept = [0]
    capped = [False]

    def add(tk: str, pos: int) -> bool:
        s = keep.get(tk)
        if s is None or pos in s:
            return False
        s.add(pos)
        total_kept[0] += 1
        frontier.append((tk, pos))
        return True

    # ── Steg 1: frø — hver tabell får et utvalg (ingen tom tabell) ───────────
    seeded_important: dict[str, list] = {}
    exclude = plan.exclude or frozenset()
    for tk, info in tables.items():
        if tk in exclude:               # kopitabeller o.l. — ingen frø-rader
            continue
        total = index.row_count.get(tk, 0)
        if total <= 0:
            continue
        if total <= full_thr:
            picks = list(range(total))          # liten tabell → behold alt
        else:
            picks = _spread(total, n)           # viktig ELLER øvrig → utvalg
        for pos in picks:
            add(tk, pos)
        if tk in plan.important:
            seeded_important[tk] = picks

    # ── Steg 2: barn-ekspansjon fra viktige frø-rader (ett nivå, cappet) ─────
    if plan.include_children:
        for tk, picks in seeded_important.items():
            child_list = index.children.get(tk, [])
            if not child_list:
                continue
            for pos in picks:
                for (child_key, fi, pcs, grp) in child_list:
                    pval = index.rev.get(tk, {}).get(pcs, {}).get(pos)
                    if pval is None:
                        continue
                    for cpos in grp.get(pval, ())[:child_cap]:
                        add(child_key, cpos)
                        if total_kept[0] >= max_keep:
                            capped[0] = True
                            break
                    if capped[0]:
                        break
                if capped[0]:
                    break
            if capped[0]:
                break

    # ── Steg 3: forelder-lukking (transitivt, til fikspunkt) ─────────────────
    # Garanterer at HVER beholdt rad sine refererte foreldre også er med.
    while frontier:
        tk, pos = frontier.popleft()
        rel = relations.get(tk)
        if not rel or not rel.fks:
            continue
        row_fks = index.fk_local.get(tk, {}).get(pos)
        if not row_fks:
            continue
        for fi, fk in enumerate(rel.fks):
            lv = row_fks.get(fi)
            if lv is None:
                continue
            pcs = tuple(fk.parent_cols)
            parent_pos = index.fwd.get(fk.parent_key, {}).get(pcs, {}).get(lv)
            if parent_pos is not None:
                add(fk.parent_key, parent_pos)

    if capped[0] and w:
        w(f"  Subset: nådde sikkerhetstaket på {max_keep:,} rader — "
          f"barne-ekspansjon ble begrenset.", "warn")

    info = {
        "n_rows": n,
        "important": sorted(plan.important),
        "excluded": sorted(exclude),
        "per_table": {tk: len(keep[tk]) for tk in tables},
        "total_kept": total_kept[0],
        "total_original": sum(index.row_count.values()),
        "capped": capped[0],
    }
    return keep, info


# ─────────────────────────────────────────────────────────────────────────────
# Heuristisk anbefaling av viktige tabeller (fallback uten Ollama)
# ─────────────────────────────────────────────────────────────────────────────

def recommend_important_heuristic(tables: dict, relations: "dict[str, Relation]",
                                  row_counts: "dict | None" = None,
                                  top_k: int = 8,
                                  exclude: "set | None" = None) -> "list[str]":
    """Velg sentrale tabeller ut fra relasjonsgraf + radantall.

    Sentralitet = hvor mange andre tabeller som peker TIL tabellen (innkommende
    FK-er) — typiske «entitets»-tabeller (person, sak, kontakt) refereres mye.
    Bryt likhet med radantall. Ekskluder rene oppslags-/koblings-tabeller (få
    rader) når mulig.
    """
    incoming: dict[str, int] = {tk: 0 for tk in tables}
    outgoing: dict[str, int] = {tk: 0 for tk in tables}
    for tk, rel in relations.items():
        outgoing[tk] = len(rel.fks)
        for fk in rel.fks:
            if fk.parent_key in incoming:
                incoming[fk.parent_key] += 1

    rc = row_counts or {}
    exclude = exclude or frozenset()

    def score(tk: str) -> tuple:
        # Prioritert: (entitets-treff på navn, innkommende sentralitet, radantall).
        ent = _entity_score(tables[tk].get("table_name", ""))
        return (ent, incoming.get(tk, 0), rc.get(tk, 0))

    candidates = [tk for tk in tables if tk not in exclude]
    ranked = sorted(candidates, key=score, reverse=True)
    # Behold de som enten er entitets-tabeller ELLER har innkommende referanser.
    central = [tk for tk in ranked
               if _entity_score(tables[tk].get("table_name", "")) > 0
               or incoming.get(tk, 0) > 0]
    if not central:                      # ingen FK/entitetsnavn → største tabeller
        central = sorted(candidates, key=lambda tk: rc.get(tk, 0), reverse=True)
    return central[:top_k]


# ─────────────────────────────────────────────────────────────────────────────
# Kopitabell-deteksjon (utelates helt fra utvalget)
# ─────────────────────────────────────────────────────────────────────────────

# Suffiks som indikerer en kopi/midlertidig variant av en annen tabell.
_COPY_SUFFIX_RE = re.compile(
    r"(?:[ _\-]*(?:tmp|temp|copy|kopi|bak|backup|sikkerhetskopi|bup))+$",
    re.IGNORECASE)
# Dato-suffiks: _20230101, _230101, _2023, _2023_01, _2023_01_01, -2023-01 osv.
_DATE_SUFFIX_RE = re.compile(
    r"[ _\-]*(?:\d{4}[ _\-]?\d{2}[ _\-]?\d{2}|\d{6}|\d{4}(?:[ _\-]\d{2}){0,2})$")


def _strip_copy_suffix(name: str) -> str:
    n = name or ""
    for _ in range(6):
        prev = n
        n = _DATE_SUFFIX_RE.sub("", n)
        n = _COPY_SUFFIX_RE.sub("", n)
        if n == prev:
            break
    return n


def detect_copy_tables(tables: dict) -> set:
    """Returner table_keys for tabeller som ser ut som KOPIER av en annen tabell
    (samme navn + `_tmp`/`_kopi`/`_bak`/dato-suffiks). Slike utelates helt."""
    by_name: dict[str, list] = {}
    for tk, info in tables.items():
        by_name.setdefault((info.get("table_name") or "").lower(), []).append(tk)
    copies: set = set()
    for tk, info in tables.items():
        nm = (info.get("table_name") or "").lower()
        base = _strip_copy_suffix(nm).strip(" _-")
        if base and base != nm and base in by_name:
            copies.add(tk)
    return copies
