"""siard_workflow/operations/lobfolder_fix_operation.py

LobFolderFixOperation
---------------------
Retter tre kjente lobFolder-inkompatibiliteter i SIARD-filer produsert av
SC Full Convert (SCFC), som gjør at LOBs ikke lar seg indeksere i DBPTK.

Se: https://github.com/keeps/dbptk-developer/issues/749

De tre problemene:
  1. Manglende <lobFolder>content</lobFolder> på database-nivå
     → DBPTK legger til './' foran alle LOB-stier
  2. Kolonne-nivå lobFolder har 'content/'-prefiks
     → stien dobles: './content/schema0/.../lob0/rec0.txt'
  3. Kolonne-nivå lobFolder har avsluttende '/'
     → doble skråstreker i stien: '...lob0//rec0.txt'
"""

from __future__ import annotations
import io
import re
import zipfile
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext

_METADATA_PATHS = ("header/metadata.xml", "metadata.xml")

_RE_COL_PREFIX     = re.compile(r"<lobFolder>/?content/", re.IGNORECASE)
_RE_TRAILING_SLASH = re.compile(r"/</lobFolder>", re.IGNORECASE)
_RE_DB_LOBFOLDER   = re.compile(r"<lobFolder>content</lobFolder>")
_RE_INSERT_AFTER   = re.compile(r"(</dataOriginTimespan>)", re.IGNORECASE)
_DB_LOBFOLDER_TAG  = "<lobFolder>content</lobFolder>"


# ── Offentlig hjelpefunksjon (brukes også av GUI-preflighten) ─────────────────

def scan_lobfolder_issues(siard_path: Path) -> list[str]:
    """
    Skann SIARD-filen for de tre SCFC→DBPTK lobFolder-inkompatibilitetene.
    Returnerer en liste med lesbar beskrivelse av hvert problem som ble funnet.
    Returnerer tom liste ved ingen problemer, eller ved feil under lesing.
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

    try:
        meta_str = meta_bytes.decode("utf-8")
    except UnicodeDecodeError:
        meta_str = meta_bytes.decode("latin-1")

    issues: list[str] = []

    n1 = len(_RE_COL_PREFIX.findall(meta_str))
    if n1:
        issues.append(
            f"Kolonne-nivå <lobFolder> har 'content/'- eller '/content/'-prefiks "
            f"({n1} forekomst(er))")

    n2 = len(_RE_TRAILING_SLASH.findall(meta_str))
    if n2:
        issues.append(
            f"<lobFolder> har avsluttende '/' ({n2} forekomst(er))")

    if not _RE_DB_LOBFOLDER.search(meta_str):
        issues.append(
            "Mangler <lobFolder>content</lobFolder> på database-nivå")

    return issues


# ── Intern hjelpefunksjon ─────────────────────────────────────────────────────

def _fix_metadata(meta_str: str) -> tuple[str, list[str]]:
    """Applikér alle tre rettelsene. Returnerer (ny_tekst, endringsliste)."""
    changes: list[str] = []

    new_str, n1 = _RE_COL_PREFIX.subn("<lobFolder>", meta_str)
    if n1:
        changes.append(f"Fjernet 'content/'-prefiks fra {n1} lobFolder(s)")
        meta_str = new_str

    new_str, n2 = _RE_TRAILING_SLASH.subn("</lobFolder>", meta_str)
    if n2:
        changes.append(f"Fjernet avsluttende '/' fra {n2} lobFolder(s)")
        meta_str = new_str

    if not _RE_DB_LOBFOLDER.search(meta_str):
        if _RE_INSERT_AFTER.search(meta_str):
            meta_str = _RE_INSERT_AFTER.sub(
                r"\1\n\t" + _DB_LOBFOLDER_TAG, meta_str, count=1)
            changes.append(
                "Lagt til <lobFolder>content</lobFolder> på database-nivå")
        else:
            changes.append(
                "ADVARSEL: Fant ikke </dataOriginTimespan> — "
                "db-nivå lobFolder ikke lagt til")

    return meta_str, changes


# ── Operasjonsklassen ─────────────────────────────────────────────────────────

class LobFolderFixOperation(BaseOperation):
    """
    Retter lobFolder-inkompatibiliteter mellom SCFC og DBPTK (issue #1).

    Pipeline-modus (ctx.extracted_path satt):
        Modifiserer header/metadata.xml direkte på disk. Produserer ingen ny
        SIARD — RepackSiardOperation tar seg av det.

    Standalone-modus:
        Leser fra SIARD-zip, skriver korrigert kopi til <original>_lobfix.siard.
    """

    operation_id   = "lobfolder_fix"
    label          = "Korriger lobFolder (SCFC→DBPTK)"
    description    = (
        "Retter tre lobFolder-inkompatibiliteter i SCFC-produserte SIARD-filer "
        "som hindrer LOB-indeksering i DBPTK: legger til database-nivå "
        "<lobFolder>content</lobFolder>, fjerner 'content/'-prefiks fra "
        "kolonne-nivå lobFolder, og fjerner avsluttende '/'."
    )
    category       = "Kompatibilitet"
    status         = 1
    produces_siard = True
    default_params: dict = {}

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        # ── Pipeline-modus ────────────────────────────────────────────────────
        if ctx.extracted_path and ctx.extracted_path.is_dir():
            meta_path = ctx.extracted_path / "header" / "metadata.xml"
            if not meta_path.exists():
                meta_path = ctx.extracted_path / "metadata.xml"
            if not meta_path.exists():
                return self._fail("metadata.xml ikke funnet i utpakket mappe")

            raw = meta_path.read_bytes()
            try:
                meta_str = raw.decode("utf-8")
            except UnicodeDecodeError:
                meta_str = raw.decode("latin-1")

            meta_str, changes = _fix_metadata(meta_str)
            if not changes:
                return self._ok({"fixes": 0},
                                "Ingen lobFolder-problemer funnet")

            meta_path.write_bytes(meta_str.encode("utf-8"))
            summary = "; ".join(changes)
            w(f"  lobFolder rettet i metadata.xml: {summary}", "ok")
            # Ingen output_path — RepackSiardOperation pakker ferdig SIARD
            return self._ok({"fixes": len(changes)}, summary)

        # ── Standalone-modus ──────────────────────────────────────────────────
        siard_path = ctx.siard_path
        try:
            with zipfile.ZipFile(siard_path, "r") as zf:
                name_lower  = {n.lower(): n for n in zf.namelist()}
                meta_entry  = next(
                    (name_lower[c] for c in _METADATA_PATHS if c in name_lower),
                    None)
                if not meta_entry:
                    return self._fail("metadata.xml ikke funnet i SIARD-arkivet")
                meta_bytes  = zf.read(meta_entry)
                all_info    = zf.infolist()
        except Exception as exc:
            return self._fail(f"Kunne ikke lese SIARD: {exc}")

        try:
            meta_str = meta_bytes.decode("utf-8")
        except UnicodeDecodeError:
            meta_str = meta_bytes.decode("latin-1")

        meta_str, changes = _fix_metadata(meta_str)
        if not changes:
            return self._ok({"fixes": 0}, "Ingen lobFolder-problemer funnet")

        meta_bytes_new = meta_str.encode("utf-8")
        dst_path = siard_path.with_name(
            siard_path.stem + "_lobfix" + siard_path.suffix)

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

        summary = "; ".join(changes)
        w(f"  Skrevet: {dst_path.name}  —  {summary}", "ok")
        return self._ok(
            {"fixes": len(changes), "output_path": str(dst_path)},
            summary)
