"""siard_workflow/core/external_lob.py

Håndtering av eksterne SIARD-fillager (ekstern ``lobFolder``).
=============================================================

SIARD-standarden tillater at LOB-filene ligger UTENFOR ``.siard``-arkivet, i en
søstermappe ved siden av arkivfilen. Dette signaliseres via et ``<lobFolder>``-
element på **database-nivå** i ``header/metadata.xml`` som peker ut av arkivet,
f.eks.::

    <lobFolder>..\\WinMed 2.siard_documents\\content</lobFolder>

Kolonne-nivå ``<lobFolder>`` (``schema0/table3/lob9``) og celle-referansene
(``<c9 file="seg0/rec1.bin" .../>``) er relative til denne basen. Fysisk sti::

    (siard_path / <db-lobFolder>).resolve() / <kolonne-lobFolder> / <celle-file=>

Dette modulen gir to symmetriske operasjoner som lar resten av verktøyet
behandle eksterne fillager på lik linje med interne:

* :func:`internalize` — kalles ved utpakking. Kopierer det eksterne
  ``content/…``-treet inn i den utpakkede mappen og skriver om db-nivå
  ``<lobFolder>`` til ``content``. Etterpå er arkivet internt og alle
  operasjoner (blob-konvertering, hex, SHA256, seg-fix …) virker uendret.

* :func:`externalize` — kalles ved innpakking når bruker velger «eksternt
  fillager». Flytter LOB-filene ut til en søstermappe, skriver om db-nivå
  ``<lobFolder>`` til å peke dit, og normaliserer celle-``file=`` slik at de er
  relative til kolonne-lobFolder.

Byte-nivå XML-patching følger samme mønster som
``segfolder_fix_operation.py`` for robusthet mot varierende innrykk/tegnsett.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Callable

# ── Regex (byte-nivå, tegnsett-uavhengig) ─────────────────────────────────────

# database-/kolonne-nivå <lobFolder>…</lobFolder>
_LOBFOLDER_RE = re.compile(rb"<lobFolder>(.*?)</lobFolder>", re.DOTALL | re.IGNORECASE)
# Første <schemas>/<schema> markerer slutten på database-nivå metadata.
_SCHEMAS_RE = re.compile(rb"<schemas>|<schema>", re.IGNORECASE)
# file=/href=/fileName= i tableX.xml
_FILE_ATTR_RE = re.compile(rb'((?:file|fileName|href)\s*=\s*["\'])([^"\']*)(["\'])')
# Ledende intern LOB-sti-prefiks: content/<schema>/<table>/lob<N>/
_LOB_PREFIX_RE = re.compile(rb"^content/[^/]+/[^/]+/lob\d+/", re.IGNORECASE)
# LOB-fil (relativ posix-sti) — brukes for å skille LOB-filer fra tabell-XML m.m.
_LOB_FILE_RE = re.compile(r"^content/[^/]+/[^/]+/lob\d+/", re.IGNORECASE)


Logger = Callable[..., None]


def _noop(*_a, **_k) -> None:
    pass


# ── Metadata-hjelpere ─────────────────────────────────────────────────────────

def _metadata_path(extract_dir: Path) -> Path | None:
    """Finn header/metadata.xml (eller metadata.xml) i utpakket mappe."""
    for cand in (extract_dir / "header" / "metadata.xml",
                 extract_dir / "metadata.xml"):
        if cand.exists():
            return cand
    return None


def read_db_lobfolder(meta_bytes: bytes) -> str | None:
    """
    Returner verdien av database-nivå ``<lobFolder>`` (den som står før
    ``<schemas>``), eller ``None`` hvis den ikke finnes.
    """
    m_schema = _SCHEMAS_RE.search(meta_bytes)
    limit = m_schema.start() if m_schema else len(meta_bytes)
    last = None
    for m in _LOBFOLDER_RE.finditer(meta_bytes, 0, limit):
        last = m
    if last is None:
        return None
    return last.group(1).decode("utf-8", "replace").strip()


def is_external_lobfolder(value: str | None) -> bool:
    """
    Sann hvis en ``<lobFolder>``-verdi peker på et eksternt lager. Interne
    verdier er tomme, ``content`` eller ``content/…``.
    """
    if not value:
        return False
    v = value.strip()
    if not v:
        return False
    if ".." in v or "\\" in v:
        return True
    # Windows-drev (C:) eller URI-skjema (file:) → eksternt
    if re.match(r"^[A-Za-z]:", v) or "://" in v:
        return True
    # Absolutt posix-sti
    if v.startswith("/"):
        return True
    # Alt annet (content, content/…, schemaX/…) tolkes som internt
    return False


def _set_db_lobfolder(meta_bytes: bytes, new_value: str) -> bytes:
    """
    Sett database-nivå ``<lobFolder>`` til ``new_value``. Erstatter eksisterende
    db-nivå-element hvis det finnes, ellers settes et nytt inn rett før
    ``<schemas>``. Kolonne-nivå ``<lobFolder>`` røres ikke.
    """
    new_el = b"<lobFolder>" + new_value.encode("utf-8") + b"</lobFolder>"
    m_schema = _SCHEMAS_RE.search(meta_bytes)
    limit = m_schema.start() if m_schema else len(meta_bytes)

    db_match = None
    for m in _LOBFOLDER_RE.finditer(meta_bytes, 0, limit):
        db_match = m
    if db_match is not None:
        return meta_bytes[: db_match.start()] + new_el + meta_bytes[db_match.end():]

    # Ingen db-nivå lobFolder — sett inn rett før <schemas> (behold innrykk).
    if m_schema is None:
        return meta_bytes  # uventet struktur; la den være
    ins = m_schema.start()
    line_start = meta_bytes.rfind(b"\n", 0, ins) + 1
    indent = meta_bytes[line_start:ins]
    insertion = new_el + b"\r\n" + indent
    return meta_bytes[:ins] + insertion + meta_bytes[ins:]


# ── Sti-oppløsning ────────────────────────────────────────────────────────────

def resolve_external_base(siard_path: Path, db_lobfolder: str) -> Path:
    """
    Løs opp ekstern base-mappe fra database-nivå ``<lobFolder>``.

    Verdien tolkes relativt til ``.siard``-filen behandlet som en katalog
    (DBPTK-modell): ``..\\X\\content`` → ``<siard-mappe>/X/content``.
    """
    norm = db_lobfolder.replace("\\", "/").strip()
    candidate = (siard_path / norm).resolve()
    if candidate.is_dir():
        return candidate
    # Fallback: prøv søstermappe basert på siste to ledd (…_documents/content)
    parts = [p for p in norm.split("/") if p not in ("", "..", ".")]
    if len(parts) >= 2:
        alt = (siard_path.parent / parts[-2] / parts[-1]).resolve()
        if alt.is_dir():
            return alt
    return candidate  # ikke-eksisterende — kaller sjekker .is_dir()


# ── Internalisering (ved utpakking) ───────────────────────────────────────────

def internalize(extract_dir: Path, siard_path: Path, w: Logger | None = None) -> dict:
    """
    Hvis arkivet bruker et eksternt db-nivå ``<lobFolder>``: kopier de eksterne
    LOB-filene inn i ``extract_dir/content/…`` og skriv om db-nivå
    ``<lobFolder>`` til ``content``. Etterpå er strukturen intern.

    Returnerer ``{"external_lobs_imported": n, "external_base": str|None}``.
    No-op (n=0) for interne arkiver.
    """
    w = w or _noop
    stats = {"external_lobs_imported": 0, "external_base": None}

    meta_path = _metadata_path(extract_dir)
    if meta_path is None:
        return stats

    meta_bytes = meta_path.read_bytes()
    db_lob = read_db_lobfolder(meta_bytes)
    if not is_external_lobfolder(db_lob):
        return stats

    ext_base = resolve_external_base(siard_path, db_lob)
    stats["external_base"] = str(ext_base)
    if not ext_base.is_dir():
        w(f"  [ADVARSEL] Eksternt fillager oppgitt i metadata "
          f"(<lobFolder>{db_lob}</lobFolder>) men mappen ble ikke funnet: "
          f"{ext_base}", "warn")
        return stats

    w(f"  Eksternt fillager oppdaget: {ext_base} — importerer inn i arkivet …",
      "info")

    content_dir = extract_dir / "content"
    content_dir.mkdir(parents=True, exist_ok=True)

    n = 0
    for src in ext_base.rglob("*"):
        if not src.is_file():
            continue
        rel = src.relative_to(ext_base)
        dst = content_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            # Intern fil finnes allerede (uventet) — behold intern, hopp over.
            continue
        try:
            shutil.copy2(src, dst)
            n += 1
        except Exception as exc:
            w(f"  FEIL kopiering {rel}: {exc}", "feil")

    # Skriv om db-nivå <lobFolder> → content (nå internt)
    new_meta = _set_db_lobfolder(meta_bytes, "content")
    if new_meta != meta_bytes:
        meta_path.write_bytes(new_meta)

    stats["external_lobs_imported"] = n
    w(f"  {n} ekstern(e) LOB-fil(er) importert; <lobFolder> satt til 'content'.",
      "ok")
    return stats


# ── Eksternalisering (ved innpakking) ─────────────────────────────────────────

def _iter_lob_files(extract_dir: Path):
    """Alle LOB-filer under content/*/*/lob*/… (relativ posix-sti + full sti)."""
    for f in extract_dir.rglob("*"):
        if not f.is_file():
            continue
        rel = f.relative_to(extract_dir).as_posix()
        if _LOB_FILE_RE.match(rel):
            yield rel, f


def _normalize_refs_in_tablexml(xml_file: Path) -> int:
    """
    Strip ledende ``content/<schema>/<table>/lob<N>/`` fra file=/href=-referanser
    slik at de blir relative til kolonnens lobFolder (ekstern base). Returnerer
    antall endrede referanser.
    """
    updates = 0

    def _replace(m: re.Match) -> bytes:
        nonlocal updates
        pre, ref, post = m.group(1), m.group(2), m.group(3)
        new_ref = _LOB_PREFIX_RE.sub(b"", ref)
        if new_ref != ref:
            updates += 1
            return pre + new_ref + post
        return m.group(0)

    tmp = xml_file.with_suffix(xml_file.suffix + ".tmp_extlob")
    try:
        with open(xml_file, "rb") as src, open(tmp, "wb", buffering=256 * 1024) as dst:
            for line in src:
                if b"content/" in line and (b"file" in line or b"href" in line):
                    line = _FILE_ATTR_RE.sub(_replace, line)
                dst.write(line)
        tmp.replace(xml_file)
    except Exception:
        tmp.unlink(missing_ok=True)
        return 0
    return updates


def externalize(extract_dir: Path, dst_path: Path, w: Logger | None = None) -> dict:
    """
    Flytt LOB-filene ut av den utpakkede mappen til en søstermappe ved siden av
    ``dst_path`` (``<dst-navn>_documents/content/…``), skriv om db-nivå
    ``<lobFolder>`` til å peke dit, og normaliser celle-``file=`` i tableX.xml.

    Kalles rett før ZIP-en skrives; siden filene flyttes ut av ``extract_dir``
    havner de heller ikke i ZIP-en.

    Returnerer ``{"lob_files_externalized": n, "external_documents_dir": str,
    "xml_normalized": m}``.
    """
    w = w or _noop
    stats = {"lob_files_externalized": 0, "external_documents_dir": None,
             "xml_normalized": 0}

    ext_root = dst_path.parent / (dst_path.name + "_documents")
    stats["external_documents_dir"] = str(ext_root)

    # 1. Flytt LOB-filer ut
    n = 0
    for rel, f in list(_iter_lob_files(extract_dir)):
        dst = ext_root / rel   # rel starter med 'content/…'
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(f), str(dst))
            n += 1
        except Exception as exc:
            w(f"  FEIL flytting {rel}: {exc}", "feil")
    stats["lob_files_externalized"] = n

    if n == 0:
        w("  Ingen LOB-filer å eksternalisere.", "info")
        return stats

    w(f"  {n} LOB-fil(er) flyttet til eksternt fillager: {ext_root}", "ok")

    # 2. Skriv om db-nivå <lobFolder> → ..\<dst-navn>_documents\content
    meta_path = _metadata_path(extract_dir)
    if meta_path is not None:
        ext_value = f"..\\{dst_path.name}_documents\\content"
        meta_bytes = meta_path.read_bytes()
        new_meta = _set_db_lobfolder(meta_bytes, ext_value)
        if new_meta != meta_bytes:
            meta_path.write_bytes(new_meta)
        w(f"  <lobFolder> satt til '{ext_value}'.", "info")

    # 3. Normaliser file=-referanser i tableX.xml (strip intern content-prefiks)
    content_dir = extract_dir / "content"
    m_total = 0
    if content_dir.is_dir():
        for xml_file in content_dir.rglob("*.xml"):
            if xml_file.name.lower() == "metadata.xml":
                continue
            m_total += _normalize_refs_in_tablexml(xml_file)
    stats["xml_normalized"] = m_total
    if m_total:
        w(f"  {m_total} file=-referanse(r) normalisert til ekstern lobFolder.",
          "info")

    return stats
