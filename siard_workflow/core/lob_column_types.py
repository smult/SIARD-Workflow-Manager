"""siard_workflow/core/lob_column_types.py

Konsistens mellom LOB-kolonnens deklarerte type og LOB-filenes endelse.

Bakgrunn
========
DBPTK avgjør BLOB kontra CLOB to steder — med to ULIKE kriterier:

  Ved import (``SIARD20ContentImportStrategy``) styrer FILENDELSEN::

      if (lobDir.endsWith(BLOB_EXTENSION))        // "bin" -> BLOB-gren
      else if (lobDir.endsWith(CLOB_EXTENSION))   // "txt" -> CLOB-gren

  I viseren (``ToolkitStructure2ViewerStructure.getCell``) styrer KOLONNETYPEN
  fra metadata.xml::

      if (columnType.getDbType().equals(ViewerType.dbTypes.CLOB))
          getCLOBValue(databasePath, binaryCell.getFile(), sharedZipFile)

Er en kolonne deklarert CLOB mens LOB-filene heter ``.bin``, tar importen
BLOB-grenen og lager en ``BinaryCell`` uten ``file``-verdi. Viseren ber så om
den som CLOB, ``zipFile.getEntry(...)`` gir ``null``, og innlastingen dør med::

    java.lang.NullPointerException: entry
        at java.util.zip.ZipFile.getInputStream(...)
        at ToolkitStructure2ViewerStructure.getCLOBValue(...)

Dette oppstår når LOB-innhold konverteres til et binærformat — typisk RTF/WPT
lagret i en MEMO-kolonne (``<type>CLOB</type>``, ``<typeOriginal>MEMO``) som
konverteres til PDF/A og dermed får ``.bin``-endelse, mens kolonnetypen blir
stående som CLOB. Kolonnen inneholder da ikke lenger tegndata.

Invarianten som håndheves
=========================
Har en LOB-kolonne minst én ``.bin``-referanse, skal den deklareres som
``BINARY LARGE OBJECT``, og ALLE kolonnens LOB-filer skal hete ``.bin``.

Blandede kolonner — noen celler konvertert, andre fortsatt ren tekst — kan ikke
beskrives av én kolonnetype. De gjøres derfor homogene ved at gjenværende
``.txt``-filer får ``.bin``-endelse. Ingen data går tapt, men DBPTK viser dem
som nedlasting i stedet for inline tekst.

``<typeOriginal>`` røres ikke: den beskriver kildesystemets type (f.eks. MEMO)
og endres ikke av en formatkonvertering.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

# SQL:2008-navnet vi skriver. DBPTKs SQL2008StandardDatatypeImporter godtar
# både dette og kortformen "BLOB"; langformen er den DBPTK selv skriver ut.
BINARY_LOB_TYPE = "BINARY LARGE OBJECT"

# Typenavn som betegner en TEGN-LOB (alle godtatt av samme importer).
_CHARACTER_LOB_TYPES = frozenset((
    "CHARACTER LARGE OBJECT", "CHAR LARGE OBJECT", "CLOB",
    "NATIONAL CHARACTER LARGE OBJECT", "NCHAR LARGE OBJECT", "NCLOB",
))
_BINARY_LOB_TYPES = frozenset(("BINARY LARGE OBJECT", "BLOB"))

# <cN ... file="..."> i tableX.xml — fanger kolonneindeks og filreferanse.
_CELL_FILE_RE = re.compile(
    rb"<c(?P<idx>[0-9]+)(?![0-9])[^>]*?"
    rb"(?:file|fileName|href)\s*=\s*(?P<q>[\"'])(?P<ref>[^\"']+)(?P=q)"
)

# Namespace-tolerante blokker i metadata.xml
def _block_re(tag: str) -> re.Pattern:
    t = tag.encode("ascii")
    return re.compile(
        rb"(<(?:[A-Za-z0-9_-]+:)?" + t + rb"(?:\s[^>]*)?>)(.*?)"
        rb"(</(?:[A-Za-z0-9_-]+:)?" + t + rb"\s*>)",
        re.DOTALL,
    )


_SCHEMA_BLOCK_RE = _block_re("schema")
_TABLE_BLOCK_RE = _block_re("table")
_COLUMN_BLOCK_RE = _block_re("column")
_TYPE_TAG_RE = _block_re("type")
_FOLDER_TAG_RE = re.compile(
    rb"<(?:[A-Za-z0-9_-]+:)?folder(?:\s[^>]*)?>(.*?)"
    rb"</(?:[A-Za-z0-9_-]+:)?folder\s*>",
    re.DOTALL,
)


def _base_type(type_text: str) -> str:
    """Typenavn uten lengdeangivelse: 'CLOB(1048576)' -> 'CLOB'."""
    return (type_text or "").strip().upper().split("(", 1)[0].strip()


def is_character_lob(type_text: str) -> bool:
    """True hvis <type>-verdien betegner en tegn-LOB (CLOB/NCLOB)."""
    return _base_type(type_text) in _CHARACTER_LOB_TYPES


def is_lob_type(type_text: str) -> bool:
    """True hvis <type>-verdien betegner en LOB i det hele tatt."""
    t = _base_type(type_text)
    return t in _CHARACTER_LOB_TYPES or t in _BINARY_LOB_TYPES


def scan_table_lob_extensions(table_xml: Path) -> dict[int, set[str]]:
    """
    Returner {kolonneindeks: {filendelser}} for en tableX.xml.

    Linjebasert strømming — tableX.xml kan være flere GB og skal aldri
    DOM-parses (jf. MemoryError-problemet i inline-LOB-uttrekket).
    """
    per_col: dict[int, set[str]] = {}
    with open(table_xml, "rb") as fh:
        for line in fh:
            for m in _CELL_FILE_RE.finditer(line):
                ref = m.group("ref").decode("utf-8", errors="replace")
                ext = ref.rsplit(".", 1)[-1].lower() if "." in ref else ""
                per_col.setdefault(int(m.group("idx")), set()).add(ext)
    return per_col


def rewrite_table_refs_to_bin(
        table_xml: Path,
        columns: set[int]) -> tuple[int, dict[int, list[tuple[str, str]]]]:
    """
    Skriv om .txt-referanser til .bin for `columns` i tableX.xml.

    Returnerer (antall_endret, {kolonneindeks: [(gammel_ref, ny_ref), ...]}).
    Referansene returneres PER KOLONNE og i sin helhet (ikke bare filnavnet):
    samme filnavn kan finnes i flere kolonners lobFolder, så omdøpingen på disk
    må bindes til den kolonnen referansen faktisk tilhører.

    Strømmer linjevis til en temp-fil som deretter erstatter originalen.
    """
    renames: dict[int, list[tuple[str, str]]] = {}
    changed = 0

    def _sub(m: re.Match) -> bytes:
        nonlocal changed
        idx = int(m.group("idx"))
        if idx not in columns:
            return m.group(0)
        ref = m.group("ref").decode("utf-8", errors="replace")
        if not ref.lower().endswith(".txt"):
            return m.group(0)
        new_ref = ref[: -len(".txt")] + ".bin"
        renames.setdefault(idx, []).append((ref, new_ref))
        changed += 1
        whole = m.group(0)
        start = m.start("ref") - m.start(0)
        end = m.end("ref") - m.start(0)
        return whole[:start] + new_ref.encode("utf-8") + whole[end:]

    tmp = table_xml.with_name(table_xml.name + ".lobtype.tmp")
    with open(table_xml, "rb") as src, open(tmp, "wb") as dst:
        for line in src:
            dst.write(_CELL_FILE_RE.sub(_sub, line))
    tmp.replace(table_xml)
    return changed, renames


def resolve_lob_dir(extract_dir: Path, db_lob_folder: str,
                    col_lob_folder: str, table_dir: Path) -> Path:
    """
    Finn mappa en kolonnes LOB-referanser er relative til.

    SIARD-oppslaget er <db-lobFolder>/<kolonne-lobFolder>/<file>. Vi prøver de
    variantene som forekommer i praksis og returnerer den første som finnes;
    ellers tabellmappa (der LOB-ene ligger når kolonnen ikke har lobFolder).
    """
    col = (col_lob_folder or "").strip().strip("/")
    db = (db_lob_folder or "").strip().strip("/")
    candidates = []
    if col:
        candidates.append(extract_dir / db / col if db else extract_dir / col)
        candidates.append(extract_dir / col)          # col inkluderer alt
        candidates.append(table_dir / col)            # relativt til tabellen
        candidates.append(table_dir / Path(col).name)  # kun siste ledd
    candidates.append(table_dir)
    for c in candidates:
        if c.is_dir():
            return c
    return table_dir


def _folder_of(block: bytes) -> bytes:
    """Les <folder>-verdien i en schema-/table-blokk."""
    fm = _FOLDER_TAG_RE.search(block)
    return fm.group(1).strip() if fm else b""


def set_column_type(meta: bytes, schema_folder: str, table_folder: str,
                    col_index: int, new_type: str) -> tuple[bytes, bool]:
    """
    Sett <type> for kolonne `col_index` (1-basert) i angitt schema/tabell.

    Byte-nivå erstatning som bevarer formatering, namespaces og <typeOriginal>.
    Returnerer (nye_bytes, ble_endret).
    """
    sfolder_b = schema_folder.encode("utf-8")
    tfolder_b = table_folder.encode("utf-8")
    new_type_b = new_type.encode("utf-8")
    hit = [False]

    def _in_schema(sm: re.Match) -> bytes:
        if _folder_of(sm.group(2)) != sfolder_b:
            return sm.group(0)

        def _in_table(tm: re.Match) -> bytes:
            if _folder_of(tm.group(2)) != tfolder_b:
                return tm.group(0)
            counter = [0]

            def _in_col(cm: re.Match) -> bytes:
                counter[0] += 1
                if counter[0] != col_index:
                    return cm.group(0)
                inner, n = _TYPE_TAG_RE.subn(
                    lambda t: t.group(1) + new_type_b + t.group(3),
                    cm.group(2), count=1)
                if n:
                    hit[0] = True
                return cm.group(1) + inner + cm.group(3)

            return (tm.group(1)
                    + _COLUMN_BLOCK_RE.sub(_in_col, tm.group(2))
                    + tm.group(3))

        return (sm.group(1)
                + _TABLE_BLOCK_RE.sub(_in_table, sm.group(2))
                + sm.group(3))

    out = _SCHEMA_BLOCK_RE.sub(_in_schema, meta)
    return out, hit[0]


def find_inconsistent_lob_columns(extract_dir: Path) -> list[dict]:
    """
    Finn tegn-LOB-kolonner som har minst én .bin-referanse.

    Returnerer liste av {schema, table, index, name, extensions}.
    """
    extract_dir = Path(extract_dir)
    meta_path = extract_dir / "header" / "metadata.xml"
    if not meta_path.exists():
        meta_path = extract_dir / "metadata.xml"
    if not meta_path.exists():
        return []

    raw = meta_path.read_bytes()
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return []

    m = re.search(rb'xmlns="([^"]+metadata\.xsd)"', raw)
    ns = {"n": m.group(1).decode()} if m else {"n": ""}

    def _txt(node, tag: str) -> str:
        e = node.find("n:" + tag, ns)
        return (e.text or "").strip() if e is not None and e.text else ""

    db_lob = _txt(root, "lobFolder")

    found: list[dict] = []
    for schema in root.findall(".//n:schema", ns):
        sfolder = _txt(schema, "folder")
        for table in schema.findall(".//n:table", ns):
            tfolder = _txt(table, "folder")
            txml = extract_dir / "content" / sfolder / tfolder / f"{tfolder}.xml"
            if not txml.exists():
                continue
            cols = table.findall("n:columns/n:column", ns)
            if not any(is_lob_type(_txt(c, "type")) for c in cols):
                continue
            per_col = scan_table_lob_extensions(txml)
            if not per_col:
                continue
            for idx, col in enumerate(cols, start=1):
                if not is_character_lob(_txt(col, "type")):
                    continue
                exts = per_col.get(idx)
                if not exts or "bin" not in exts:
                    continue
                found.append({
                    "schema": sfolder,
                    "table": tfolder,
                    "index": idx,
                    "name": _txt(col, "name"),
                    "type": _txt(col, "type"),
                    "extensions": sorted(exts),
                    "lob_folder": _txt(col, "lobFolder"),
                    "db_lob_folder": db_lob,
                })
    return found


def repair_dangling_lob_refs(extract_dir: Path, w=None) -> dict:
    """
    Rett file=-referanser som peker på en LOB-fil som ikke finnes.

    Bakgrunn: BLOB-konverteringen kan patche referansen i tableX.xml fra
    ``.txt`` til ``.bin`` selv om selve konverteringen ikke ble gjennomført.
    Da står referansen igjen og peker i tomme luften, mens originalfila ligger
    der uten referanse. DBPTK slår opp referansen med
    ``zipFile.getEntry(...)``, får ``null``, og innlastingen dør med
    ``NullPointerException: entry`` — samme symptom som typemismatchen, men
    en helt annen årsak.

    Reparasjonen er konservativ: en referanse rettes kun når det finnes
    NØYAKTIG ÉN fil med samme stamme (alt før første punktum) i kolonnens egen
    lobFolder. Referansen pekes da på den filen som faktisk finnes.

    Returnerer {"refs_repaired": n, "unresolved": n, "details": [...]}.
    """
    extract_dir = Path(extract_dir)
    stats = {"refs_repaired": 0, "unresolved": 0, "details": []}

    meta_path = extract_dir / "header" / "metadata.xml"
    if not meta_path.exists():
        meta_path = extract_dir / "metadata.xml"
    if not meta_path.exists():
        return stats

    raw = meta_path.read_bytes()
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return stats
    m = re.search(rb'xmlns="([^"]+metadata\.xsd)"', raw)
    ns = {"n": m.group(1).decode()} if m else {"n": ""}

    def _txt(node, tag: str) -> str:
        e = node.find("n:" + tag, ns)
        return (e.text or "").strip() if e is not None and e.text else ""

    db_lob = _txt(root, "lobFolder")

    for schema in root.findall(".//n:schema", ns):
        sfolder = _txt(schema, "folder")
        for table in schema.findall(".//n:table", ns):
            tfolder = _txt(table, "folder")
            tdir = extract_dir / "content" / sfolder / tfolder
            txml = tdir / f"{tfolder}.xml"
            if not txml.exists():
                continue
            cols = table.findall("n:columns/n:column", ns)

            # {kolonneindeks: {stamme: [filnavn]}} for kolonner med lobFolder
            col_files: dict[int, dict[str, list[str]]] = {}
            col_dirs: dict[int, Path] = {}
            for idx, col in enumerate(cols, start=1):
                if not is_lob_type(_txt(col, "type")):
                    continue
                lob_dir = resolve_lob_dir(
                    extract_dir, db_lob, _txt(col, "lobFolder"), tdir)
                if not lob_dir.is_dir():
                    continue
                by_stem: dict[str, list[str]] = {}
                for f in lob_dir.iterdir():
                    if f.is_file():
                        by_stem.setdefault(f.name.split(".", 1)[0],
                                           []).append(f.name)
                col_files[idx] = by_stem
                col_dirs[idx] = lob_dir
            if not col_files:
                continue

            fixes: dict[int, dict[str, str]] = {}
            for idx, by_stem in col_files.items():
                lob_dir = col_dirs[idx]
                for ext_set_ref in scan_table_lob_refs(txml, idx):
                    if (lob_dir / ext_set_ref).exists():
                        continue
                    stem = ext_set_ref.rsplit("/", 1)[-1].split(".", 1)[0]
                    cands = by_stem.get(stem, [])
                    if len(cands) == 1:
                        prefix = (ext_set_ref.rsplit("/", 1)[0] + "/"
                                  if "/" in ext_set_ref else "")
                        fixes.setdefault(idx, {})[ext_set_ref] = prefix + cands[0]
                    else:
                        stats["unresolved"] += 1
                        if w:
                            w(f"    ADVARSEL: {sfolder}/{tfolder} c{idx}: "
                              f"referansen «{ext_set_ref}» finnes ikke, og "
                              f"{len(cands)} kandidat(er) med samme stamme — "
                              f"ikke rettet", "warn")
            if not fixes:
                continue

            n = _apply_ref_fixes(txml, fixes)
            stats["refs_repaired"] += n
            for idx, mapping in fixes.items():
                detail = (f"{sfolder}/{tfolder} c{idx}: {len(mapping)} "
                          f"referanse(r) pekt tilbake på filen som finnes "
                          f"(f.eks. {next(iter(mapping.items()))[0]} → "
                          f"{next(iter(mapping.items()))[1]})")
                stats["details"].append(detail)
                if w:
                    w(f"    {detail}", "ok")

    if stats["refs_repaired"] and w:
        w(f"  MERK: {stats['refs_repaired']} LOB-fil(er) hadde patchet "
          f"XML-referanse uten at konverteringen faktisk ble utført — "
          f"innholdet er originalformatet, ikke PDF/A.", "warn")
    return stats


def scan_table_lob_refs(table_xml: Path, col_index: int) -> list[str]:
    """Returner alle file=-referanser for én kolonne i en tableX.xml."""
    refs: list[str] = []
    with open(table_xml, "rb") as fh:
        for line in fh:
            for m in _CELL_FILE_RE.finditer(line):
                if int(m.group("idx")) == col_index:
                    refs.append(m.group("ref").decode("utf-8", errors="replace"))
    return refs


def _apply_ref_fixes(table_xml: Path,
                     fixes: dict[int, dict[str, str]]) -> int:
    """Bytt ut file=-referanser i tableX.xml. Returnerer antall endret."""
    changed = 0

    def _sub(m: re.Match) -> bytes:
        nonlocal changed
        mapping = fixes.get(int(m.group("idx")))
        if not mapping:
            return m.group(0)
        ref = m.group("ref").decode("utf-8", errors="replace")
        new_ref = mapping.get(ref)
        if not new_ref:
            return m.group(0)
        changed += 1
        whole = m.group(0)
        start = m.start("ref") - m.start(0)
        end = m.end("ref") - m.start(0)
        return whole[:start] + new_ref.encode("utf-8") + whole[end:]

    tmp = table_xml.with_name(table_xml.name + ".reffix.tmp")
    with open(table_xml, "rb") as src, open(tmp, "wb") as dst:
        for line in src:
            dst.write(_CELL_FILE_RE.sub(_sub, line))
    tmp.replace(table_xml)
    return changed


def reconcile_lob_column_types(extract_dir: Path, w=None) -> dict:
    """
    Rett LOB-kolonner der deklarert type ikke stemmer med filendelsen.

    For hver tegn-LOB-kolonne med minst én .bin-referanse:
      1. gjenværende .txt-referanser i kolonnen skrives om til .bin, og filene
         på disk omdøpes tilsvarende (gjør kolonnen homogen)
      2. <type> settes til BINARY LARGE OBJECT (<typeOriginal> bevares)

    Returnerer {"columns_retyped": n, "files_renamed": n, "refs_rewritten": n,
                "details": [...]}.
    """
    extract_dir = Path(extract_dir)
    stats = {"columns_retyped": 0, "files_renamed": 0,
             "refs_rewritten": 0, "refs_repaired": 0,
             "unresolved_refs": 0, "details": []}

    # Hengende referanser først: en referanse som peker på en fil som ikke
    # finnes gir samme NPE i DBPTK som typemismatchen, og må rettes før vi
    # vurderer hvilke endelser kolonnen «egentlig» har.
    repair = repair_dangling_lob_refs(extract_dir, w)
    stats["refs_repaired"] = repair["refs_repaired"]
    stats["unresolved_refs"] = repair["unresolved"]
    stats["details"].extend(repair["details"])

    inconsistent = find_inconsistent_lob_columns(extract_dir)
    if not inconsistent:
        return stats

    meta_path = extract_dir / "header" / "metadata.xml"
    if not meta_path.exists():
        meta_path = extract_dir / "metadata.xml"

    # Grupper per tabell
    per_table: dict[tuple[str, str], list[dict]] = {}
    for c in inconsistent:
        per_table.setdefault((c["schema"], c["table"]), []).append(c)

    if w:
        w(f"  LOB-kolonnetyper: {len(inconsistent)} tegn-LOB-kolonne(r) "
          f"inneholder nå binærdata", "step")

    # ── 1. Gjør blandede kolonner homogene ────────────────────────────────────
    # Omdøpingen bindes til den enkelte kolonnens lobFolder: samme filnavn
    # (xrec2.txt) finnes typisk i FLERE kolonners lob-mapper, og en usikret
    # oppslagsrunde ville omdøpt en urørt tekstkolonnes fil.
    for (sfolder, tfolder), cols in per_table.items():
        by_index = {c["index"]: c for c in cols}
        mixed = {i for i, c in by_index.items() if "txt" in c["extensions"]}
        if not mixed:
            continue
        tdir = extract_dir / "content" / sfolder / tfolder
        txml = tdir / f"{tfolder}.xml"
        n_changed, renames = rewrite_table_refs_to_bin(txml, mixed)
        stats["refs_rewritten"] += n_changed
        for col_index, ref_pairs in renames.items():
            col = by_index[col_index]
            lob_dir = resolve_lob_dir(
                extract_dir, col["db_lob_folder"], col["lob_folder"], tdir)
            for old_ref, new_ref in ref_pairs:
                src = lob_dir / old_ref
                if not src.exists():
                    if w:
                        w(f"    ADVARSEL: fant ikke {src} — hopper over", "warn")
                    continue
                try:
                    src.rename(lob_dir / new_ref)
                    stats["files_renamed"] += 1
                except OSError as exc:
                    if w:
                        w(f"    FEIL omdøp {old_ref}: {exc}", "feil")
        if n_changed and w:
            w(f"    {sfolder}/{tfolder}: {n_changed} gjenværende "
              f".txt-referanse(r) → .bin (blandet kolonne)", "info")

    # ── 2. Sett <type> til BINARY LARGE OBJECT ────────────────────────────────
    meta = meta_path.read_bytes()
    for c in inconsistent:
        meta, ok = set_column_type(
            meta, c["schema"], c["table"], c["index"], BINARY_LOB_TYPE)
        if not ok:
            if w:
                w(f"    ADVARSEL: fant ikke <type> for "
                  f"{c['schema']}/{c['table']}.{c['name']}", "warn")
            continue
        stats["columns_retyped"] += 1
        detail = (f"{c['schema']}/{c['table']}.{c['name']}: "
                  f"{c['type']} → {BINARY_LOB_TYPE}")
        stats["details"].append(detail)
        if w:
            w(f"    {detail}", "ok")

    if stats["columns_retyped"]:
        meta_path.write_bytes(meta)
    return stats
