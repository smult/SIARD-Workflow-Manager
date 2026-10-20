"""
siard_workflow/operations/pipeline_operations.py

UnpackSiardOperation  —  Pakker ut SIARD-arkivet til temp-mappe én gang
RepackSiardOperation  —  Pakker ferdig temp-mappe tilbake til ny .siard-fil

Disse to operasjonene danner rammen rundt en pipeline-arbeidsflyt der SIARD
kun pakkes ut og inn én gang, mens alle mellomliggende operasjoner (Virusskan,
HexExtract, BlobKonverter) jobber direkte på filsystemet.

Anbefalt rekkefølge:
    SHA256 → Pakk ut SIARD → Virusskan → HEX Inline Extract
           → BLOB Konverter → Pakk sammen SIARD
"""
from __future__ import annotations

import concurrent.futures
import datetime
import shutil
import tempfile
import time
import zipfile
from pathlib import Path, PurePosixPath

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.siard_format import (
    detect_siard_version, siard_version_transform,
    get_target_siard_version, is_siard_xml,
    detect_folder_siard_version, rewrite_siardversion_path,
)
from siard_workflow.core import external_lob
from siard_workflow.core.zip_progress import (
    BIG_FILE, CHUNK, ByteProgress, extract_all, fmt_bytes, fmt_duration,
    read_small, scan_files,
)


def _root_tag_end(head: bytes) -> int:
    """
    Posisjon rett etter rot-elementets starttag (etter XML-deklarasjon og
    kommentarer). Navnerom og versjon står her — dataradene etter røres ikke
    når en stor tableX.xml strømmes. Faller tilbake til siste linjeskift.
    """
    i = 0
    while True:
        i = head.find(b"<", i)
        if i < 0 or i + 1 >= len(head):
            break
        nxt = head[i + 1:i + 2]
        if nxt == b"?":
            j = head.find(b"?>", i)
            i = j + 2 if j >= 0 else len(head)
        elif head.startswith(b"<!--", i):
            j = head.find(b"-->", i)
            i = j + 3 if j >= 0 else len(head)
        elif nxt == b"!":
            j = head.find(b">", i)
            i = j + 1 if j >= 0 else len(head)
        else:
            j = head.find(b">", i)
            if j >= 0:
                return j + 1
            break
    return head.rfind(b"\n") + 1 or len(head)


def _zip_workers() -> int:
    """Tråder for utpakking/forhåndslesing — flere enn 4 gir ikke mer (diskbundet)."""
    try:
        from settings import get_config
        return max(1, min(4, int(get_config("max_workers", 4) or 4)))
    except Exception:
        return 4


# ─────────────────────────────────────────────────────────────────────────────

class UnpackSiardOperation(BaseOperation):
    """
    Pakker ut SIARD-arkivet til en midlertidig mappe.

    Setter ctx.extracted_path og lagrer den originale ZIP-navnlisten i
    ctx.results["unpack_siard"]["original_namelist"] slik at
    RepackSiardOperation kan gjenopprette kataloginnganger i ZIP-en.
    """

    operation_id   = "unpack_siard"
    label          = "Pakk ut SIARD"
    description    = (
        "Pakker ut SIARD-arkivet til en midlertidig mappe for pipeline-behandling. "
        "Bruk 'Pakk sammen SIARD' som siste operasjon for å lage ferdig arkiv."
    )
    category       = "Pipeline"
    status         = 2
    produces_siard = False
    requires_unpack = False  # er selve utpakkeren
    # Utpakkingen i seg selv endrer ikke innhold, men schema-navn-sanering gjør
    # det. Føres derfor som PREMIS-event kun når sanering faktisk skjedde.
    modifies_content = True
    premis_event_type  = "Adjustment"
    premis_event_label = "skjemanavn-sanering"

    default_params: dict = {}

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and (
            result.data.get("schemas_sanitized", 0) > 0
            or result.data.get("external_lobs_imported", 0) > 0)

    def premis_detail(self, result, ctx) -> str:
        parts = []
        n_san = result.data.get("schemas_sanitized", 0)
        if n_san:
            parts.append(f"{n_san} schema-navn med spesialtegn sanert til "
                         f"'schemaN' i metadata.xml ved utpakking")
        n_ext = result.data.get("external_lobs_imported", 0)
        if n_ext:
            parts.append(f"{n_ext} LOB-fil(er) importert fra eksternt fillager "
                         f"({result.data.get('external_base', '')}) og "
                         f"<lobFolder> satt til 'content'")
        return "; ".join(parts) or "ingen endringer ved utpakking"

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log: log.log(msg, lvl)
            if pcb: pcb("log", msg=msg, level=lvl)

        # Hopp over hvis allerede pakket ut (f.eks. ved gjenkjøring)
        if ctx.extracted_path and ctx.extracted_path.is_dir():
            w(f"  SIARD allerede utpakket: {ctx.extracted_path}", "info")
            return self._ok(
                {"extracted_path": str(ctx.extracted_path)},
                "Allerede utpakket — hopper over")

        siard_path = ctx.siard_path
        if not siard_path or not siard_path.exists():
            return self._fail(f"SIARD-fil ikke funnet: {siard_path}")

        # Velg temp-rot
        td = ctx.metadata.get("temp_dir", "").strip()
        temp_root = Path(td) if td and Path(td).is_dir() else None

        try:
            tmp = Path(tempfile.mkdtemp(prefix="siard_pipeline_", dir=temp_root))
        except Exception as exc:
            return self._fail(f"Kunne ikke opprette temp-mappe: {exc}")

        w(f"  Pakker ut {siard_path.name} → {tmp} ...", "info")
        t0 = datetime.datetime.now()

        # Fremdrift i bytes (jevn, også gjennom store tableX.xml), flere tråder
        stop_ev = ctx.metadata.get("stop_event")

        def _prog(n_files: int, n_bytes: int) -> ByteProgress:
            w(f"  {n_files:,} filer, {fmt_bytes(n_bytes)} utpakket størrelse", "info")
            return ByteProgress("Pakker ut", n_files, n_bytes, pcb, w)
        try:
            original_namelist, n_done = extract_all(
                siard_path, tmp, _prog, workers=_zip_workers(),
                stop=(stop_ev.is_set if stop_ev else None),
                warn=lambda m: w(m, "warn"))
        except Exception as exc:
            shutil.rmtree(tmp, ignore_errors=True)
            return self._fail(f"Kunne ikke åpne SIARD: {exc}")
        if stop_ev is not None and stop_ev.is_set():
            shutil.rmtree(tmp, ignore_errors=True)
            return self._fail("Utpakkingen ble stoppet")

        elapsed = (datetime.datetime.now() - t0).total_seconds()
        w(f"  Pakket ut {n_done:,} filer på {fmt_duration(elapsed)}", "ok")

        ctx.extracted_path = tmp
        data = {
            "extracted_path":    str(tmp),
            "original_namelist": original_namelist,
            "files_extracted":   n_done,
        }

        # ── Internaliser eksternt fillager (ekstern lobFolder) ─────────────────
        # SIARD kan lagre LOB-ene i en søstermappe (ekstern db-nivå <lobFolder>).
        # Kopier dem inn i den utpakkede mappen slik at ALLE nedstrøms-
        # operasjoner behandler dem på lik linje med interne LOB-er. Bruker kan
        # velge eksternt fillager igjen ved «Pakk sammen SIARD».
        try:
            ext_stats = external_lob.internalize(tmp, siard_path, w)
            data["external_lobs_imported"] = ext_stats["external_lobs_imported"]
            if ext_stats.get("external_base"):
                data["external_base"] = ext_stats["external_base"]
        except Exception as exc:
            w(f"  Advarsel: kunne ikke importere eksternt fillager: {exc}", "warn")

        # ── Sjekk for tomme <schema><name>-noder i metadata.xml ────────────────
        # Schema-navn er obligatorisk i SIARD. Hvis en eller flere er tomme,
        # spør operatør om å fylle inn (default = folder-navnet, typisk schemaN).
        try:
            self._check_empty_schema_names(tmp, ctx, w)
        except Exception as exc:
            w(f"  Advarsel: kunne ikke sjekke schema-navn: {exc}", "warn")

        # ── Saniter schema-navn med spesialtegn ────────────────────────────────
        # Schema-navn må være URL-/XML-sikre (kun A-Za-z0-9_.-). Navn med
        # spesialtegn (<, >, &, ", ', æøå, mellomrom osv.) byttes til schemaN
        # allerede ved utpakking, slik at ALLE arbeidsflyter får konsistente navn
        # — ikke bare de som bruker BLOB-konvertering.
        try:
            data["schemas_sanitized"] = self._sanitize_schema_names(tmp, w)
        except Exception as exc:
            w(f"  Advarsel: kunne ikke sanere schema-navn: {exc}", "warn")

        # ── Sjekk for ikke-standard LOB-filendelser og -filnavn ───────────────
        from siard_workflow.operations.standardize_ext_operation import (
            count_non_standard_lob_files as _count_lob)
        from siard_workflow.core.lob_naming import dbptk_names_enabled
        non_ext, non_name = _count_lob(tmp)
        if not dbptk_names_enabled():
            non_name = 0
        non_std = non_ext + non_name
        if non_ext > 0:
            w(f"  ADVARSEL: {non_ext} LOB-fil(er) med ikke-standard endelse "
              f"(ikke .bin/.txt) funnet. Kan gi utfordringer i KDRS Søk & Vis.",
              "warn")
        if non_name > 0:
            w(f"  ADVARSEL: {non_name} LOB-fil(er) heter ikke recordN.* "
              f"(krav i DBPTK-validatoren P_4.2-3).", "warn")
        if non_std > 0:
            ask_cb = ctx.metadata.get("ask_standardize_cb")
            if ask_cb:
                try:
                    if ask_cb(non_std):
                        from siard_workflow.operations.standardize_ext_operation \
                            import StandardizeExtOperation
                        ctx.metadata["_insert_after_current"] = StandardizeExtOperation()
                        w("  Legger til 'Standardiser filendelser' i arbeidsflyten.",
                          "ok")
                except Exception as exc:
                    w(f"  Advarsel: could not invoke ask_standardize_cb: {exc}", "warn")

        # ── Sjekk for flere schemas ───────────────────────────────────────────
        # Hvis utpakket SIARD har > 1 schema, tilby å legge til Schema-velger.
        # Operatøren kan da velge bort schemas som ikke skal være med i
        # resultatet.
        schema_count = self._count_schemas(tmp)
        if schema_count > 1:
            w(f"  {schema_count} schemas funnet i SIARD.", "info")
            ask_sch_cb = ctx.metadata.get("ask_schema_selector_cb")
            # Ikke spør hvis Schema-velger allerede er satt inn av forrige
            # callback (Standardiser-knappen)
            already_inserting = isinstance(
                ctx.metadata.get("_insert_after_current"), object) and \
                getattr(ctx.metadata.get("_insert_after_current"),
                        "operation_id", "") == "schema_selector"
            if ask_sch_cb and not already_inserting:
                try:
                    if ask_sch_cb(schema_count):
                        from siard_workflow.operations.schema_selector_operation \
                            import SchemaSelectorOperation
                        # Hvis vi allerede har lagt inn StandardizeExt for
                        # neste plass, må vi håndtere kjeden. Enkleste:
                        # legg Schema-velger først (rett etter Unpack), og
                        # la StandardizeExt komme etter den.
                        existing = ctx.metadata.get("_insert_after_current")
                        if existing is not None:
                            ctx.metadata["_insert_after_current"] = [
                                SchemaSelectorOperation(), existing]
                        else:
                            ctx.metadata["_insert_after_current"] = \
                                SchemaSelectorOperation()
                        w("  Legger til 'Schema-velger' i arbeidsflyten.",
                          "ok")
                except Exception as exc:
                    w(f"  Advarsel: could not invoke ask_schema_selector_cb: {exc}",
                      "warn")

        return self._ok(data, f"{n_done:,} filer pakket ut til {tmp}")

    @staticmethod
    def _count_non_standard_lob_files(extract_dir: Path) -> int:
        """Teller LOB-filer med ikke-standard endelse (ikke .bin/.txt)."""
        import re as _re
        count = 0
        content = extract_dir / "content"
        if not content.exists():
            return 0
        lob_re = _re.compile(r"lob\d+", _re.IGNORECASE)
        for lob_dir in content.rglob("*"):
            if not lob_dir.is_dir() or not lob_re.fullmatch(lob_dir.name):
                continue
            for f in lob_dir.iterdir():
                if f.is_file():
                    low = f.name.lower()
                    if not (low.endswith(".bin") or low.endswith(".txt")):
                        count += 1
        return count

    @staticmethod
    def _sanitize_schema_names(extract_dir: Path, w) -> int:
        """
        Saniter `<schema><name>`-verdier med spesialtegn i metadata.xml rett
        etter utpakking. Navn som ikke er URL-/XML-sikre (kun A-Za-z0-9_.-)
        byttes til 'schemaN' (1-basert indeks), og filen skrives på disk
        umiddelbart slik at alle nedstrøms-operasjoner og repakking ser det
        sanerte navnet.

        Tomme navn håndteres separat av _check_empty_schema_names (med dialog);
        denne tar kun ikke-tomme navn med ulovlige tegn.

        Returnerer antall sanerte schema-navn (0 hvis ingen endring).
        """
        from siard_workflow.core.siard_format import (
            list_all_schema_names, sanitize_metadata_schema_names,
            apply_schema_reference_renames, _SAFE_SCHEMA_NAME_RE,
        )
        metadata_path = extract_dir / "header" / "metadata.xml"
        if not metadata_path.exists():
            return 0
        data = metadata_path.read_bytes()
        before = list_all_schema_names(data)
        unsafe = [s for s in before
                  if s["name"].strip()
                  and not _SAFE_SCHEMA_NAME_RE.match(s["name"].strip())]
        if not unsafe:
            return 0
        w(f"  ADVARSEL: {len(unsafe)} schema-navn med spesialtegn funnet:", "warn")
        for s in unsafe:
            w(f"    • '{s['name']}' (folder: {s['folder']}) → schema{s['index']}",
              "warn")
        new_data = sanitize_metadata_schema_names(data)
        if new_data == data:
            return 0
        metadata_path.write_bytes(new_data)
        w(f"  Sanerte {len(unsafe)} schema-navn til 'schemaN' i metadata.xml.",
          "ok")

        # Hold schema-navn-referanser i synk: bygg gammelt→nytt navn ved å diffe
        # navnene før/etter sanering (per indeks), og oppdater
        # referencedSchema/typeSchema som pekte på det gamle navnet.
        after = {s["index"]: s["name"].strip()
                 for s in list_all_schema_names(new_data)}
        renames = {}
        for s in before:
            old = s["name"].strip()
            new = after.get(s["index"], "")
            if old and new and old != new:
                renames[old] = new
        ref_data, n_ref = apply_schema_reference_renames(new_data, renames)
        if n_ref:
            metadata_path.write_bytes(ref_data)
            w(f"  ✓ {n_ref} schema-referanse(r) "
              f"(referencedSchema/typeSchema) oppdatert til sanert navn", "ok")
        return len(unsafe)

    @staticmethod
    def _check_empty_schema_names(extract_dir: Path, ctx, w) -> None:
        """
        Sjekk metadata.xml for tomme `<schema><name>`-noder. Hvis funnet,
        spør operatør via callback om å fylle inn navn, og oppdater filen
        på disk umiddelbart.

        Schema-navn er obligatorisk i SIARD-spec. Konsumenter (KDRS Søk &
        Vis m.fl.) feiler hvis navnet er tomt. Det er heller ikke lov å
        bruke et navn som allerede er i bruk i samme SIARD — dialogen
        validerer dette aktivt, og fallback genererer unike defaults.
        """
        from siard_workflow.core.siard_format import (
            find_empty_schema_names, apply_schema_name_fixes,
            list_all_schema_names, apply_empty_schema_reference_fixes,
        )
        metadata_path = extract_dir / "header" / "metadata.xml"
        if not metadata_path.exists():
            return
        data = metadata_path.read_bytes()
        empties = find_empty_schema_names(data)
        if not empties:
            return

        # Hent navn på schemas som ALLEREDE har navn — dialog må unngå
        # kollisjon mot disse.
        all_names = list_all_schema_names(data)
        existing_names = {s["name"] for s in all_names
                          if s["name"] and s["name"].strip()}

        def _unique_default(folder: str, used: set) -> str:
            """Garanterer at default-navn ikke kolliderer med et eksisterende."""
            base = (folder or "schema").strip() or "schema"
            cand = base
            n = 1
            while cand in used:
                n += 1
                cand = f"{base}_{n}"
            return cand

        def _fallback_fixes() -> dict:
            """Default-fixes som garantert er unike på tvers av eksisterende navn."""
            used = set(existing_names)
            out: dict = {}
            for e in empties:
                name = _unique_default(e["folder"], used)
                used.add(name)
                out[e["index"]] = name
            return out

        w(f"  ADVARSEL: {len(empties)} schema(er) mangler navn i metadata.xml:",
          "warn")
        for e in empties:
            w(f"    • Schema #{e['index']} (folder: {e['folder']})", "warn")

        ask_cb = ctx.metadata.get("ask_fill_empty_schema_names_cb")
        if not ask_cb:
            w("  Ingen dialog — bruker folder-navn (unike defaults) som schema-navn.",
              "info")
            fixes = _fallback_fixes()
        else:
            try:
                fixes = ask_cb(empties, existing_names)
            except TypeError:
                # Bakoverkompatibilitet: eldre callback uten existing_names
                try:
                    fixes = ask_cb(empties)
                except Exception as exc:
                    w(f"  Dialog feilet: {exc} — bruker folder-navn.", "warn")
                    fixes = _fallback_fixes()
            except Exception as exc:
                w(f"  Dialog feilet: {exc} — bruker folder-navn.", "warn")
                fixes = _fallback_fixes()
            if fixes is None:
                w("  Operatør avbrøt — bruker folder-navn som default.", "warn")
                fixes = _fallback_fixes()
            else:
                # Operatør ga input — verifiser at det IKKE kolliderer.
                # Hvis dialog returnerer noe galt (programmer-feil), fall tilbake.
                proposed = set(fixes.values())
                if proposed & existing_names:
                    w("  Dialog returnerte navn som kolliderer med "
                      "eksisterende — bruker folder-navn.", "warn")
                    fixes = _fallback_fixes()
                elif len(proposed) != len(fixes):
                    w("  Dialog returnerte duplikater — bruker folder-navn.",
                      "warn")
                    fixes = _fallback_fixes()

        new_data = apply_schema_name_fixes(data, fixes)
        if new_data != data:
            metadata_path.write_bytes(new_data)
            for idx, name in sorted(fixes.items()):
                w(f"  ✓ Schema #{idx} → «{name}»", "ok")
        else:
            w("  Ingen schema-navn endret.", "info")

        # Hold referanser til de (tidligere tomme) schema-navnene i synk:
        # <foreignKey><referencedSchema> og <column><typeSchema> var tomme fordi
        # schema-navnet var tomt — de settes nå til det innfylte navnet.
        ref_data, ref_stats = apply_empty_schema_reference_fixes(new_data, fixes)
        if ref_data != new_data:
            metadata_path.write_bytes(ref_data)
            w(f"  ✓ {ref_stats['updated']} schema-referanse(r) "
              f"(referencedSchema/typeSchema) satt til nytt schema-navn", "ok")
        if ref_stats.get("unresolved"):
            w(f"  ADVARSEL: {ref_stats['unresolved']} tom schema-referanse "
              f"kunne ikke kobles entydig (flere navnløse schemas) — beholdt tom",
              "warn")

    @staticmethod
    def _count_schemas(extract_dir: Path) -> int:
        """Teller antall <schema> i header/metadata.xml. Returnerer 0 ved feil."""
        metadata_path = extract_dir / "header" / "metadata.xml"
        if not metadata_path.exists():
            return 0
        try:
            import xml.etree.ElementTree as _ET
            ns = {"ns": "http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd"}
            root = _ET.parse(metadata_path).getroot()
            return len(root.findall("ns:schemas/ns:schema", ns))
        except Exception:
            return 0


# ─────────────────────────────────────────────────────────────────────────────

class RepackSiardOperation(BaseOperation):
    """
    Pakker den midlertidige utpakkede mappen tilbake til en ny .siard-fil
    og rydder opp temp-mappen.

    Fordi operasjonen setter output_path i result.data, vil WorkflowContext
    automatisk oppdatere ctx.siard_path til den nye filen.
    """

    operation_id   = "repack_siard"
    label          = "Pakk sammen SIARD"
    description    = (
        "Pakker den utpakkede SIARD-strukturen til en ny .siard-fil og rydder "
        "temp-mappen. Bruk alltid etter 'Pakk ut SIARD' i pipeline-arbeidsflyten."
    )
    category       = "Pipeline"
    status         = 2
    produces_siard = True
    requires_unpack = False  # er selve sammenpackeren

    default_params = {
        "output_suffix":  "_konvertert",
        "keep_temp":      False,
        "compress_level": "",   # tom = bruk global config (siard_compress_level)
        "lob_storage":    "intern",   # "intern" = LOB-er i ZIP; "ekstern" = søstermappe
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def w(msg: str, lvl: str = "info") -> None:
            if log: log.log(msg, lvl)
            if pcb: pcb("log", msg=msg, level=lvl)

        extract_dir = ctx.extracted_path
        if not extract_dir or not extract_dir.is_dir():
            return self._fail(
                "Ingen utpakket SIARD-mappe funnet. "
                "Legg til 'Pakk ut SIARD' som første operasjon i workflowen.")

        # Hent original namelist (for å bevare ZIP-kataloginnganger)
        unpack_data   = ctx.get_result("unpack_siard") or {}
        orig_namelist: list[str] = unpack_data.get("original_namelist", [])

        # Bestem destinasjonsfil
        suffix   = (self.params.get("output_suffix") or "_konvertert").strip()
        src_path = ctx.siard_path
        # Anonymisert innhold skal alltid være synlig i filnavnet — et uttrekk
        # med fiktive personopplysninger må ikke kunne forveksles med originalen.
        if (ctx.get_result("anonymize") is not None
                and "anonymisert" not in suffix.lower()
                and "anonymisert" not in src_path.stem.lower()):
            suffix += "_anonymisert"
            w("  Anonymisering er med i arbeidsflyten → «_anonymisert» legges "
              "til i filnavnet", "info")
        dst_path = src_path.with_name(src_path.stem + suffix + src_path.suffix)
        counter  = 1
        while dst_path.exists():
            dst_path = src_path.with_name(
                src_path.stem + suffix + f"_{counter}" + src_path.suffix)
            counter += 1

        w(f"  Pakker sammen → {dst_path.name} ...", "info")

        # Versjonhåndtering
        target_version = get_target_siard_version()
        src_version    = "2.1"
        metadata_xml   = extract_dir / "header" / "metadata.xml"
        if metadata_xml.exists():
            try:
                src_version = detect_siard_version(metadata_xml.read_bytes())
            except Exception:
                pass
        w(f"  SIARD-versjon: {src_version} → {target_version}", "info")

        # ── Eksternt fillager (valgfritt) ──────────────────────────────────────
        # Hvis bruker valgte «ekstern»: flytt LOB-filene ut til en søstermappe
        # og skriv om <lobFolder>/file=-referanser før ZIP-en skrives. Filene er
        # da ute av extract_dir og havner derfor ikke i ZIP-en.
        lob_storage = (self.params.get("lob_storage") or "intern").strip().lower()
        ext_result: dict = {}
        if lob_storage == "ekstern":
            try:
                ext_result = external_lob.externalize(extract_dir, dst_path, w)
            except Exception as exc:
                return self._fail(f"Kunne ikke eksternalisere fillager: {exc}")

        # Kataloginnganger fra original ZIP
        orig_dir_entries = sorted(
            n for n in orig_namelist if n.endswith("/"))
        if lob_storage == "ekstern":
            # LOB-mappene ligger nå eksternt — ikke skriv tomme lob*/-innganger.
            import re as _re
            _lobdir = _re.compile(r"(^|/)lob\d+/", _re.IGNORECASE)
            orig_dir_entries = [n for n in orig_dir_entries
                                if not _lobdir.search(n)]

        # Versjonsmarkør-mappa header/siardversion/<x.y>/ må følge
        # mål-versjonen. Mappenavnet i original-ZIP-en er kilden til
        # sannhet — XML-innholdet kan allerede ha blitt normalisert til
        # generisk /2/-namespace av et tidligere steg i workflowen.
        folder_version = detect_folder_siard_version(
            orig_namelist, src_version)
        if folder_version != target_version:
            w(f"  Versjonsmarkør header/siardversion/: "
              f"{folder_version} → {target_version}", "info")

        def _ver_path(name: str) -> str:
            return rewrite_siardversion_path(name, target_version)

        w("  Finner filer …", "info")
        all_files = scan_files(extract_dir, pcb)          # [(sti, størrelse)]
        n_total   = len(all_files)
        n_written = 0
        t0        = datetime.datetime.now()
        total_bytes = sum(s for _p, s in all_files)
        w(f"  {n_total:,} filer, {fmt_bytes(total_bytes)}", "info")
        prog = ByteProgress("Pakker sammen", n_total, total_bytes, pcb, w)

        # Bestem kompresjonsnivå (per-instans override vinner over global config)
        from siard_workflow.core.siard_format import (
            get_zip_compresslevel, get_smart_skip_enabled, is_precompressed_bytes,
        )
        level      = get_zip_compresslevel(self.params.get("compress_level"))
        smart_skip = get_smart_skip_enabled()
        compression = zipfile.ZIP_STORED if level == 0 else zipfile.ZIP_DEFLATED
        compresslevel = level if level > 0 else None
        w(f"  ZIP-kompresjon: nivå {level}"
          + (" (smart-skip på)" if smart_skip and level > 0 else "")
          + (" (STORED)" if level == 0 else ""), "info")

        try:
            with zipfile.ZipFile(dst_path, "w", compression,
                                 allowZip64=True,
                                 compresslevel=compresslevel) as zf:

                # 1. Kataloginnganger fra original (f.eks. header/siardversion/2.1/)
                for dir_entry in orig_dir_entries:
                    dir_entry_out = _ver_path(dir_entry)
                    zf.writestr(zipfile.ZipInfo(dir_entry_out), b"")

                # 2. Alle filer fra filsystemet. Små filer leses på forhånd i
                #    tråder (mens ZIP-en skrives), store strømmes i blokker —
                #    ingen tableX.xml på flere GB leses inn i minnet.
                def _write(file_path: Path, size: int, data: "bytes | None") -> None:
                    arc_name = str(
                        file_path.relative_to(extract_dir)).replace("\\", "/")
                    arc_name = _ver_path(arc_name)
                    if data is None and size < BIG_FILE:
                        data = file_path.read_bytes()
                    if data is not None:
                        # SIARD XML-filer: transformer versjon
                        if is_siard_xml(arc_name):
                            try:
                                data = siard_version_transform(data, target_version)
                            except Exception:
                                pass
                        # Smart-skip: tving STORED for filer som ER allerede
                        # komprimerte (basert på magic-bytes, ikke filendelsen).
                        # Tekstlige `.bin`-filer (xml/rtf/txt) komprimeres normalt.
                        if (level > 0 and smart_skip
                                and is_precompressed_bytes(data[:16])):
                            zf.writestr(arc_name, data,
                                        compress_type=zipfile.ZIP_STORED)
                        else:
                            # Følg ZipFile-konstruktørens nivå
                            zf.writestr(arc_name, data)
                        prog.add(len(data), files=1)
                        return
                    # Stor fil: strøm i blokker. Versjonstransformasjonen gjelder
                    # navnerom/versjon i starten av fila (rot-element, XSD-hode)
                    with open(file_path, "rb") as fh:
                        head = fh.read(CHUNK)
                        if is_siard_xml(arc_name):
                            cut = _root_tag_end(head)
                            rest_of_head = head[cut:]
                            try:
                                head = siard_version_transform(head[:cut], target_version)
                            except Exception:
                                head = head[:cut]
                            head += rest_of_head
                        if level > 0 and smart_skip and is_precompressed_bytes(head[:16]):
                            zi = zipfile.ZipInfo(
                                arc_name, date_time=time.localtime(time.time())[:6])
                            zi.compress_type = zipfile.ZIP_STORED
                            target = zi
                        else:
                            target = arc_name
                        with zf.open(target, "w", force_zip64=True) as out:
                            out.write(head)
                            prog.add(len(head))
                            while True:
                                chunk = fh.read(CHUNK)
                                if not chunk:
                                    break
                                out.write(chunk)
                                prog.add(len(chunk))
                    prog.add(files=1)

                import itertools
                window = 256
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=_zip_workers(),
                        thread_name_prefix="forhandslesing") as pool:
                    it = iter(all_files)

                    def _submit():
                        return [(p, s, pool.submit(read_small, (p, s)))
                                for p, s in itertools.islice(it, window)]
                    cur = _submit()
                    while cur:
                        nxt = _submit()
                        for p, s, fut in cur:
                            _write(p, s, fut.result())
                            n_written += 1
                        cur = nxt
                prog.finish()

        except Exception as exc:
            return self._fail(f"Kunne ikke pakke sammen SIARD: {exc}")

        elapsed = (datetime.datetime.now() - t0).total_seconds()
        size_mb = dst_path.stat().st_size / 1_048_576
        w(f"  Skrevet {n_written:,} filer ({fmt_bytes(dst_path.stat().st_size)}) på "
          f"{fmt_duration(elapsed)}", "ok")
        w(f"  Ny SIARD: {dst_path}", "ok")

        # Rydd temp
        keep = bool(self.params.get("keep_temp", False))
        if keep:
            w(f"  Temp-mappe beholdt: {extract_dir}", "info")
        else:
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
                if not extract_dir.exists():
                    w(f"  Temp-mappe ryddet: {extract_dir}", "info")
                else:
                    w(f"  Advarsel: temp-mappe kunne ikke slettes: {extract_dir}",
                      "warn")
            except Exception as exc:
                w(f"  Advarsel: opprydding feilet: {exc}", "warn")

        ctx.extracted_path = None

        data = {
            "output_path":   str(dst_path),
            "files_written": n_written,
            "size_mb":       round(size_mb, 2),
            "lob_storage":   lob_storage,
        }
        if ext_result:
            data["external_documents_dir"] = ext_result.get("external_documents_dir")
            data["lob_files_externalized"] = ext_result.get("lob_files_externalized", 0)
            w(f"  Eksternt fillager: {ext_result.get('lob_files_externalized', 0)} "
              f"fil(er) → {ext_result.get('external_documents_dir')}", "ok")
        return self._ok(data, f"{n_written:,} filer → {dst_path.name}")
