"""
siard_workflow/operations/depot_reports.py
-------------------------------------------
Byggere for «depotrapportene» — KDRS SIARD Managers svar på rapportsettet et
depot lager ved mottak av en SIARD-deponering (jf. extras/-eksemplene fra IKA
Trøndelag). Presentasjonen følger «dagens rapport»-stil (report_style.py), mens
målepunktene speiler det eksterne rapportsettet, men beregnes fra dataene KDRS
SIARD Managers egne operasjoner faktisk produserer.

Rapporter:
  • build_archive_info_report      — Arkivinformasjon (fra metadata-uttrekk)
  • build_conversion_report        — Konverteringsrapport (fra BLOB-konvertering)
  • build_file_organization_report — Filorganisering (fra konverterings-CSV)
  • build_processing_summary_report— Behandlingssammendrag / godkjenning

Alle funksjoner krever reportlab (lastes lat via report_style.build_toolkit()).
"""
from __future__ import annotations

import csv
import datetime
from pathlib import Path

from siard_workflow.operations.report_style import (
    build_toolkit, fmt_int, fmt_pct, human_size,
)

# ─────────────────────────────────────────────────────────────────────────────
# Felles småhjelpere
# ─────────────────────────────────────────────────────────────────────────────

_NO_MONTHS = ["", "januar", "februar", "mars", "april", "mai", "juni", "juli",
              "august", "september", "oktober", "november", "desember"]


def _no_date(dt: datetime.datetime) -> str:
    return f"{dt.day}. {_NO_MONTHS[dt.month]} {dt.year}"


def _no_datetime(dt: datetime.datetime) -> str:
    return f"{_no_date(dt)} kl. {dt:%H:%M}"


_FILETYPE_NAMES = {
    "pdf": "PDF-dokument", "pdfa": "PDF-dokument",
    "txt": "Tekstfil", "text": "Tekstfil",
    "html": "Webside", "htm": "Webside",
    "csv": "Regnearksdata (CSV)",
    "json": "JSON-datafil",
    "xml": "XML-datafil",
    "tif": "TIFF-bilde", "tiff": "TIFF-bilde",
    "jpg": "JPEG-bilde", "jpeg": "JPEG-bilde", "png": "PNG-bilde",
    "gif": "Bilde", "bmp": "Bilde", "jp2": "Bilde",
    "rtf": "Rik tekstdokument (RTF)",
    "doc": "Word-dokument", "docx": "Word-dokument", "odt": "Tekstdokument",
    "xls": "Excel-regneark", "xlsx": "Excel-regneark", "ods": "Regneark",
    "ppt": "Presentasjon", "pptx": "Presentasjon", "odp": "Presentasjon",
    "bin": "Ukjent binærfil",
}


def _friendly_filetype(ext: str) -> str:
    ext = (ext or "").lower().lstrip(".")
    if not ext:
        return "Uten filendelse"
    return _FILETYPE_NAMES.get(ext, f".{ext}")


def _lob_folder(rel_path: str) -> str:
    """content/schema0/table17/lob8/rec1.bin → content/schema0/table17/lob8."""
    rel = (rel_path or "").replace("\\", "/").strip("/")
    if "/" in rel:
        return rel.rsplit("/", 1)[0]
    return rel or "(rot)"


def _classify_status(kommentar: str, til_ext: str) -> tuple[str, str]:
    """Returnerer (behandlingsstatus, kind) der kind ∈ ok/warn/fail/info."""
    k = (kommentar or "").lower()
    te = (til_ext or "").lower().lstrip(".")
    if any(w in k for w in ("feil", "error", "timeout", "timed out", "avbrudd")):
        return "Konvertering feilet", "fail"
    if "behold" in k:
        return "Godkjent arkivformat", "ok"
    if "tegnsett" in k or "utf-8" in k or "charset" in k:
        return "Tegnsett endret", "info"
    if "inline" in k:
        return "Inline uttrekk", "info"
    if "standardisert til .bin" in k:
        return "Standardisert til .bin", "info"
    if "konvert" in k or te in ("pdf", "pdfa"):
        return "Konvertert til PDF/A", "ok"
    # Utpakket container / endret filendelse til reelt format (f.eks. bz2 → xml):
    # nå et gyldig arkivformat med korrekt filendelse.
    if "endret filendelse" in k or "endret til" in k:
        return "Godkjent arkivformat", "ok"
    if k.strip():
        return kommentar.strip(), "info"
    return "Behandlet", "info"


def _read_conversion_csv(csv_path: Path) -> list[dict]:
    """Leser blob-konverterings-CSV (semikolon, utf-8-sig). Tom liste ved feil."""
    rows: list[dict] = []
    if not csv_path or not Path(csv_path).exists():
        return rows
    try:
        with open(csv_path, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh, delimiter=";")
            for r in reader:
                rows.append({
                    "fra_fil":       (r.get("fra_fil") or "").strip(),
                    "fra_storrelse": _to_int(r.get("fra_storrelse")),
                    "fra_ext":       (r.get("fra_ext") or "").strip().lstrip("."),
                    "til_fil":       (r.get("til_fil") or "").strip(),
                    "til_storrelse": _to_int(r.get("til_storrelse")),
                    "til_ext":       (r.get("til_ext") or "").strip().lstrip("."),
                    "kommentar":     (r.get("kommentar") or "").strip(),
                })
    except Exception:
        pass
    return rows


def _read_error_log(log_path: Path) -> list[dict]:
    """Parser konvertering_feil.log → [{path, ext, msg}]."""
    out: list[dict] = []
    if not log_path or not Path(log_path).exists():
        return out
    try:
        with open(log_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                # Format: [TID]  filsti  |  ext  |  feilmelding
                if "|" not in line:
                    continue
                head, _, rest = line.partition("|")
                ext, _, msg = rest.partition("|")
                # Fjern [TID]-prefiks fra head
                path = head.strip()
                if path.startswith("[") and "]" in path:
                    path = path.split("]", 1)[1].strip()
                out.append({
                    "path": path,
                    "ext":  ext.strip().lstrip("."),
                    "msg":  msg.strip(),
                })
    except Exception:
        pass
    return out


def _to_int(v) -> int:
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# 1) ARKIVINFORMASJON
# ─────────────────────────────────────────────────────────────────────────────

def build_archive_info_report(meta: dict, siard_path: Path, out_path: Path,
                              generated: datetime.datetime | None = None) -> None:
    tk = build_toolkit()
    now = generated or datetime.datetime.now()
    arc_name = meta.get("db_name") or siard_path.stem

    story = tk.cover(
        "ARKIVINFORMASJON",
        "KDRS SIARD Manager  —  Beskrivelse av deponert SIARD-arkiv",
        info_rows=[
            ("Arkiv:", arc_name),
            ("Fil:", siard_path.name),
            ("Rapport generert:", _no_datetime(now)),
        ])
    story.append(tk.para(
        "Denne rapporten oppsummerer de viktigste metadataene for "
        "SIARD-uttrekket: database- og systeminformasjon, statistikk over "
        "innholdet, samt hvor godt tabeller og kolonner er beskrevet."))

    # ── Database-informasjon ─────────────────────────────────────────────────
    story += tk.section("Database-informasjon")
    db_rows = [
        ("Databasenavn", meta.get("db_name")),
        ("Databaseprodukt", meta.get("db_product")),
        ("Opprinnelse", meta.get("db_origin")),
        ("Tilkobling", meta.get("connection")),
        ("Databasebruker", meta.get("db_user")),
        ("SIARD-versjon", meta.get("siard_version")),
    ]
    desc = (meta.get("description") or "").strip()
    if desc:
        db_rows.append(("Beskrivelse", desc))
    story.append(tk.kv_table([(k, v if v else "–") for k, v in db_rows]))

    # ── System og uttrekk ────────────────────────────────────────────────────
    story += tk.section("System og uttrekk")
    timespan = meta.get("data_origin_time_span")
    if not timespan and (meta.get("data_start") or meta.get("data_end")):
        timespan = f"{meta.get('data_start', '?')} – {meta.get('data_end', '?')}"
    story.append(tk.kv_table([
        ("Uttrekksverktøy", meta.get("producer_app") or "–"),
        ("Arkiveringsdato", meta.get("archival_date") or "–"),
        ("Bruksperiode", timespan or "–"),
    ]))

    # ── Arkivstatistikk ──────────────────────────────────────────────────────
    story += tk.section("Arkivstatistikk")
    exts = meta.get("content_extensions") or []
    ext_str = ", ".join(f".{e}" for e in exts[:25]) if exts else "–"
    if len(exts) > 25:
        ext_str += f"  (+{len(exts) - 25})"
    story.append(tk.kv_table([
        ("Antall skjemaer", fmt_int(meta.get("schema_count", 0))),
        ("Antall tabeller", fmt_int(meta.get("table_count", 0))),
        ("Totalt antall rader", fmt_int(meta.get("row_count", 0))),
        ("Tabeller med LOB-data", fmt_int(meta.get("lob_table_count", 0))),
        ("LOB-filer totalt", fmt_int(meta.get("lob_file_count", 0))),
        ("Antall ZIP-poster", fmt_int(meta.get("zip_entry_count", 0))),
        ("Filstørrelse", human_size(meta.get("file_size"))),
        ("Innholdsformater", ext_str),
    ]))

    # ── Integritet ───────────────────────────────────────────────────────────
    digest = (meta.get("message_digest") or "").strip()
    if digest:
        story += tk.section("Integritet")
        algo = meta.get("message_digest_algo") or ""
        story.append(tk.kv_table([
            ("Algoritme", algo or "–"),
            ("Meldingssammendrag", digest),
        ]))

    # ── Metadata-dekning ─────────────────────────────────────────────────────
    cov = _metadata_coverage(meta)
    if cov["tables_total"] or cov["cols_total"]:
        story += tk.section("Metadata-dekning")
        story.append(tk.para(
            "Tabeller og kolonner bør være beskrevet for at arkivet skal være "
            "lesbart for ettertiden. Oversikten viser hvor stor andel som har "
            "en beskrivelse."))
        cm = tk._rl["cm"]
        cw = [tk.inner_w - 5.4 * cm, 2.7 * cm, 2.7 * cm]
        rows = [
            ["Beskrevne tabeller", fmt_int(cov["tables_desc"]),
             fmt_pct(cov["tables_desc"], cov["tables_total"])],
            ["Ubeskrevne tabeller", fmt_int(cov["tables_total"] - cov["tables_desc"]),
             fmt_pct(cov["tables_total"] - cov["tables_desc"], cov["tables_total"])],
            ["Totalt tabeller", fmt_int(cov["tables_total"]), "100,0 %"],
            ["Beskrevne kolonner", fmt_int(cov["cols_desc"]),
             fmt_pct(cov["cols_desc"], cov["cols_total"])],
            ["Ubeskrevne kolonner", fmt_int(cov["cols_total"] - cov["cols_desc"]),
             fmt_pct(cov["cols_total"] - cov["cols_desc"], cov["cols_total"])],
            ["Totalt kolonner", fmt_int(cov["cols_total"]), "100,0 %"],
        ]
        row_bgs = {2: tk.C_SKIP_BG, 5: tk.C_SKIP_BG}
        story.append(tk.data_table(
            ["Metadata-dekning", "Antall", "Andel"], rows, cw,
            right_cols=[1, 2], row_bgs=row_bgs))

    tk.build(out_path, f"Arkivinformasjon — {arc_name}", story,
             footer_label=arc_name)


def _metadata_coverage(meta: dict) -> dict:
    tables_total = tables_desc = cols_total = cols_desc = 0
    for schema in meta.get("schemas", []) or []:
        for tbl in schema.get("tables", []) or []:
            tables_total += 1
            if (tbl.get("description") or "").strip():
                tables_desc += 1
            for col in tbl.get("columns", []) or []:
                cols_total += 1
                if (col.get("description") or "").strip():
                    cols_desc += 1
    return {
        "tables_total": tables_total, "tables_desc": tables_desc,
        "cols_total": cols_total, "cols_desc": cols_desc,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2) KONVERTERINGSRAPPORT
# ─────────────────────────────────────────────────────────────────────────────

def build_conversion_report(blob: dict, csv_rows: list[dict],
                            errors: list[dict], siard_path: Path,
                            out_path: Path, arc_name: str,
                            generated: datetime.datetime | None = None) -> None:
    tk = build_toolkit()
    now = generated or datetime.datetime.now()

    detected   = int(blob.get("detected", 0) or 0)
    converted  = int(blob.get("converted", 0) or 0)
    kept       = int(blob.get("kept", 0) or 0)
    failed     = int(blob.get("failed", 0) or 0)
    inline     = int(blob.get("inline_extracted", 0) or 0)
    attempted  = converted + failed
    ok_total   = converted + kept

    story = tk.cover(
        "KONVERTERINGSRAPPORT",
        "KDRS SIARD Manager  —  Konvertering av filer til arkivformat",
        info_rows=[
            ("Arkiv:", arc_name),
            ("Fil:", siard_path.name),
            ("Generert:", _no_datetime(now)),
        ])

    story.append(tk.para(
        "Konvertering er prosessen der filer transformeres til PDF/A og andre "
        "godkjente arkivformat for langtidslagring. Steget bruker LibreOffice "
        "for dokumenter og egne rutiner for øvrige formater. Feil kan skyldes "
        "ukjente eller korrupte kildefiler, svært store filer, manglende "
        "konverteringsverktøy eller funksjoner som ikke kan konverteres."))

    # ── 1. Konverteringssammendrag ──────────────────────────────────────────
    story.append(tk.subsection("1. Konverteringssammendrag"))
    cm = tk._rl["cm"]
    cw = [tk.inner_w - 4.0 * cm, 4.0 * cm]
    sum_rows = [
        ["Filer detektert for behandling", fmt_int(detected)],
        ["Konvertert til PDF/A", fmt_int(converted)],
        ["Beholdt i godkjent originalformat", fmt_int(kept)],
        ["Inline NBLOB/NCLOB hentet ut", fmt_int(inline)],
        ["Konvertering feilet", fmt_int(failed)],
        ["Suksessrate", fmt_pct(converted, attempted) if attempted else "–"],
        ["Feilrate", fmt_pct(failed, attempted) if attempted else "–"],
    ]
    story.append(tk.data_table(["Målepunkt", "Verdi"], sum_rows, cw,
                               right_cols=[1]))

    pie = tk.pie(["Konvertert (PDF/A)", "Beholdt", "Feilet"],
                 [float(converted), float(kept), float(failed)],
                 "Konverteringsutfall",
                 [tk.C_SUCCESS, tk.C_ACCENT, tk.C_ERROR])
    if pie is not None:
        story.append(tk.spacer(0.4))
        story.append(pie)
        story.append(tk.caption(
            "Grønn = konvertert til PDF/A  •  Blå = beholdt originalformat  "
            "•  Rød = feilet"))

    # Bygg feilliste — foretrekk feillogg, suppler med CSV-status
    failed_files = _collect_failed_files(errors, csv_rows)

    if failed and not failed_files:
        story.append(tk.note(
            f"{fmt_int(failed)} fil(er) feilet konvertering. Detaljert "
            "fil-liste er ikke tilgjengelig (ingen feillogg funnet).", "warn"))

    if failed_files:
        # ── 2. Feil etter filtype ────────────────────────────────────────────
        story.append(tk.subsection("2. Feil etter filtype"))
        by_ext: dict[str, int] = {}
        for f in failed_files:
            by_ext[f["ext"] or "(ukjent)"] = by_ext.get(f["ext"] or "(ukjent)", 0) + 1
        n_fail = len(failed_files)
        ext_rows = [[e, fmt_int(c), fmt_pct(c, n_fail)]
                    for e, c in sorted(by_ext.items(), key=lambda x: -x[1])]
        story.append(tk.data_table(
            ["Filtype", "Antall feil", "Andel"], ext_rows,
            [tk.inner_w - 7.0 * cm, 3.5 * cm, 3.5 * cm],
            header_bg=tk.C_ERROR, right_cols=[1, 2]))

        # ── 3. Feil etter årsak ──────────────────────────────────────────────
        story.append(tk.subsection("3. Feil etter årsak"))
        timeout = sum(1 for f in failed_files if _is_timeout(f["msg"]))
        other = n_fail - timeout
        cause_rows = []
        if other:
            cause_rows.append(["Andre feil", fmt_int(other), fmt_pct(other, n_fail)])
        if timeout:
            cause_rows.append(["Tidsavbrudd", fmt_int(timeout),
                               fmt_pct(timeout, n_fail)])
        story.append(tk.data_table(
            ["Feilkategori", "Antall feil", "Andel"], cause_rows,
            [tk.inner_w - 7.0 * cm, 3.5 * cm, 3.5 * cm],
            header_bg=tk.C_WARNING, right_cols=[1, 2]))

        # ── 4. Analyse etter filstørrelse ────────────────────────────────────
        size_map = {r["fra_fil"]: r["fra_storrelse"] for r in csv_rows}
        buckets = {"Små (<1 MB)": 0, "Middels (1–10 MB)": 0, "Store (>10 MB)": 0}
        known = 0
        for f in failed_files:
            sz = size_map.get(f["path"], 0)
            if not sz:
                continue
            known += 1
            mb = sz / (1024 * 1024)
            if mb < 1:
                buckets["Små (<1 MB)"] += 1
            elif mb <= 10:
                buckets["Middels (1–10 MB)"] += 1
            else:
                buckets["Store (>10 MB)"] += 1
        if known:
            story.append(tk.subsection("4. Analyse etter filstørrelse"))
            size_rows = [[k, fmt_int(v), fmt_pct(v, known)]
                         for k, v in buckets.items()]
            story.append(tk.data_table(
                ["Størrelse", "Antall feil", "Andel"], size_rows,
                [tk.inner_w - 7.0 * cm, 3.5 * cm, 3.5 * cm],
                header_bg=tk.C_ACCENT, right_cols=[1, 2]))

        # ── 5. Detaljert feiliste (gruppert etter mappe) ─────────────────────
        story.append(tk.spacer(0.3))
        story.append(tk.subsection("5. Detaljert feiliste"))
        story.append(tk.para(
            "Filene er gruppert etter plassering i arkivet for enkel "
            "lokalisering. Maks 15 filer vises per mappe."))
        by_folder: dict[str, list[dict]] = {}
        for f in failed_files:
            by_folder.setdefault(_lob_folder(f["path"]), []).append(f)
        size_map_local = size_map
        for folder, files in sorted(by_folder.items(),
                                    key=lambda x: -len(x[1])):
            story.append(tk.colored_heading(
                f"{folder}  ({fmt_int(len(files))} filer)", tk.C_ERROR))
            shown = files[:15]
            det_rows = []
            for f in shown:
                fname = f["path"].replace("\\", "/").rsplit("/", 1)[-1]
                sz = size_map_local.get(f["path"], 0)
                msg = f["msg"] or "Konverteringsfeil"
                if len(msg) > 60:
                    msg = msg[:57] + "…"
                det_rows.append([fname, f["ext"] or "–",
                                 human_size(sz) if sz else "–", msg])
            story.append(tk.data_table(
                ["Filnavn", "Type", "Størrelse", "Feilmelding"], det_rows,
                [4.5 * cm, 1.6 * cm, 2.2 * cm, tk.inner_w - 8.3 * cm],
                header_bg=tk.C_SKIP, font_size=8))
            if len(files) > 15:
                story.append(tk.caption(
                    f"… og {fmt_int(len(files) - 15)} flere filer i samme mappe"))

    # ── 6. Anbefalinger ──────────────────────────────────────────────────────
    story.append(tk.spacer(0.3))
    story.append(tk.subsection("6. Anbefalinger"))
    recs = []
    if failed_files and any(_is_timeout(f["msg"]) for f in failed_files):
        recs.append("Tidsavbrudd ble oppdaget. Vurder å øke timeout-verdien for "
                    "konverteringsjobber, spesielt for store eller komplekse filer.")
    if failed:
        recs.append("Gjennomgå filtype-statistikken i seksjon 2 for å avdekke "
                    "systematiske problemer med bestemte filformater.")
        recs.append("For kritiske filer som feilet, vurder manuell konvertering "
                    "med desktop-versjoner av verktøyene for å se om problemet er "
                    "reproduserbart.")
        recs.append("Dokumenter filer som ikke kan konverteres, inkludert årsak, "
                    "for fremtidig referanse.")
    else:
        recs.append("Ingen konverteringsfeil ble registrert — alle detekterte "
                    "filer ble konvertert eller beholdt i godkjent format.")
    story += tk.bullets(recs, numbered=True)

    tk.build(out_path, f"Konverteringsrapport — {arc_name}", story,
             footer_label=arc_name)


def _is_timeout(msg: str) -> bool:
    m = (msg or "").lower()
    return "timed out" in m or "timeout" in m or "avbrudd" in m


def _collect_failed_files(errors: list[dict], csv_rows: list[dict]) -> list[dict]:
    """Slår sammen feillogg og CSV-rader som er klassifisert som feilet."""
    seen = set()
    out: list[dict] = []
    for e in errors:
        key = e["path"]
        if key in seen:
            continue
        seen.add(key)
        out.append({"path": e["path"], "ext": e["ext"], "msg": e["msg"]})
    for r in csv_rows:
        status, kind = _classify_status(r["kommentar"], r["til_ext"])
        if kind != "fail":
            continue
        key = r["fra_fil"]
        if key in seen:
            continue
        seen.add(key)
        out.append({"path": r["fra_fil"], "ext": r["fra_ext"],
                    "msg": r["kommentar"] or "Konverteringsfeil"})
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 3) FILORGANISERING
# ─────────────────────────────────────────────────────────────────────────────

def build_file_organization_report(csv_rows: list[dict], siard_path: Path,
                                   out_path: Path, arc_name: str,
                                   meta: dict | None = None,
                                   generated: datetime.datetime | None = None) -> None:
    tk = build_toolkit()
    now = generated or datetime.datetime.now()

    # Bygg per-mappe-fordeling: folder -> {(status, filtype) -> antall}
    folders: dict[str, dict[tuple[str, str], int]] = {}
    statuses: set[str] = set()
    filetypes: set[str] = set()
    total_files = 0
    for r in csv_rows:
        folder = _lob_folder(r["fra_fil"])
        status, _ = _classify_status(r["kommentar"], r["til_ext"])
        ftype = _friendly_filetype(r["til_ext"] or r["fra_ext"])
        folders.setdefault(folder, {})
        key = (status, ftype)
        folders[folder][key] = folders[folder].get(key, 0) + 1
        statuses.add(status)
        filetypes.add(ftype)
        total_files += 1

    # Tabellbeskrivelser fra metadata (folder → beskrivelse) om tilgjengelig
    folder_desc = _folder_descriptions(meta) if meta else {}

    story = tk.cover(
        "FILORGANISERING",
        "KDRS SIARD Manager  —  Innholdsfiler gruppert etter tabell og status",
        info_rows=[
            ("Arkiv:", arc_name),
            ("Fil:", siard_path.name),
            ("Generert:", _no_datetime(now)),
        ])

    story.append(tk.para(
        "Rapporten viser innholdsfilene (LOB-filer) i arkivet, gruppert etter "
        "hvilken tabell de hører til og hvilken behandlingsstatus de fikk."))
    story += tk.bullets([
        "<b>Godkjent arkivformat:</b> filer som allerede var i gyldig arkivformat",
        "<b>Konvertert til PDF/A:</b> filer konvertert til PDF/A",
        "<b>Tegnsett endret:</b> tekstfiler der tegnsettet ble endret til UTF-8",
        "<b>Konvertering feilet:</b> filer der konverteringen ikke lyktes",
    ])

    story += tk.section("Sammendrag")
    story.append(tk.kv_table([
        ("Totalt antall innholdsfiler", fmt_int(total_files)),
        ("Datamapper/tabeller", fmt_int(len(folders))),
        ("Forskjellige behandlingsstatuser", fmt_int(len(statuses))),
        ("Forskjellige filtyper", fmt_int(len(filetypes))),
    ]))

    if not folders:
        story.append(tk.note(
            "Ingen LOB-filer ble registrert i konverteringssteget — "
            "ingen filorganisering å vise.", "info"))
        tk.build(out_path, f"Filorganisering — {arc_name}", story,
                 footer_label=arc_name)
        return

    cm = tk._rl["cm"]
    cw = [tk.inner_w - 9.0 * cm, 6.0 * cm, 3.0 * cm]
    for folder, dist in sorted(folders.items(), key=lambda x: -sum(x[1].values())):
        story.append(tk.spacer(0.3))
        story.append(tk.colored_heading(folder, tk.C_PRIMARY))
        desc = folder_desc.get(folder)
        if desc:
            story.append(tk.caption(desc))
        tot = sum(dist.values())
        rows = [[s, ft, fmt_int(c)]
                for (s, ft), c in sorted(dist.items(), key=lambda x: -x[1])]
        rows.append([f"Totalt {fmt_int(tot)} filer i denne mappen", "", ""])
        story.append(tk.data_table(
            ["Behandlingsstatus", "Filtype", "Antall filer"], rows, cw,
            header_bg=tk.C_PRIMARY, right_cols=[2], total_row=True))

    tk.build(out_path, f"Filorganisering — {arc_name}", story,
             footer_label=arc_name)


def _folder_descriptions(meta: dict) -> dict[str, str]:
    """Bygger {lob-mappe → tabellbeskrivelse} fra metadata (best-effort)."""
    out: dict[str, str] = {}
    for schema in meta.get("schemas", []) or []:
        sfolder = schema.get("folder", "")
        for tbl in schema.get("tables", []) or []:
            tfolder = tbl.get("folder", "")
            desc = (tbl.get("description") or tbl.get("name") or "").strip()
            if sfolder and tfolder and desc:
                # content/schemaX/tableY → prefiks; lobZ ukjent her, så lagre
                # tabell-prefikset slik at oppslag kan matche på startsWith
                out[f"content/{sfolder}/{tfolder}"] = desc
    # Utvid med direkte oppslag: lob-mappe arver tabellbeskrivelse
    return _PrefixDict(out)


class _PrefixDict(dict):
    """dict der .get(folder) matcher lengste nøkkel som er prefiks av folder."""
    def get(self, key, default=None):  # type: ignore[override]
        if key in self:
            return self[key]
        best = None
        for k in self:
            if key.startswith(k) and (best is None or len(k) > len(best)):
                best = k
        return self[best] if best is not None else default


# ─────────────────────────────────────────────────────────────────────────────
# 4) BEHANDLINGSSAMMENDRAG / GODKJENNING
# ─────────────────────────────────────────────────────────────────────────────

_TOOL_DESCRIPTIONS = {
    "sha256":          "Beregner SHA-256-kontrollsum for integritetssikring",
    "virus_scan":      "Skanner filene for virus og skadevare",
    "xml_validation":  "Validerer SIARD-XML mot skjema",
    "metadata_extract": "Leser ut database-metadata og lager metadatarapport",
    "blob_convert":    "Konverterer dokumenter til PDF/A med LibreOffice",
    "hex_extract":     "Henter ut HEX-kodede CLOB-felter",
    "xml_cleaner":     "Renser unødvendig utfylling (padding) i XML",
    "schema_selector": "Filtrerer bort uønskede skjemaer",
    "standardize_ext": "Standardiserer filendelser i LOB-mapper",
    "siardmapper":     "Beriker metadata med tabell-/kolonnebeskrivelser",
    "lobfolder_fix":   "Korrigerer lobFolder-referanser for kompatibilitet",
    "anonymize":       "Anonymiserer personidentifiserende data",
    "dias_package":    "Pakker arkivet til DIAS-format for deponering",
    "unpack_siard":    "Pakker ut SIARD-arkivet for behandling",
    "repack_siard":    "Pakker arkivet sammen igjen til SIARD",
    "standardize_bin_ext": "Standardiserer .bin-filendelser i LOB-mapper",
}


def build_processing_summary_report(step_results: list[dict], ctx_results: dict,
                                    meta: dict, blob: dict, siard_path: Path,
                                    out_path: Path, arc_name: str,
                                    generated: datetime.datetime | None = None) -> None:
    tk = build_toolkit()
    now = generated or datetime.datetime.now()

    # Status på tvers av steg
    run_steps = [s for s in step_results if not s.get("skipped")]
    fail_steps = [s for s in run_steps if not s.get("success")]
    converted = int(blob.get("converted", 0) or 0)
    kept      = int(blob.get("kept", 0) or 0)
    failed    = int(blob.get("failed", 0) or 0)
    has_blob  = bool(blob)
    archive_files = converted + kept + failed
    avvik = failed > 0 or bool(fail_steps)

    story = tk.cover(
        "BEHANDLINGSSAMMENDRAG",
        "KDRS SIARD Manager  —  Oppsummering av behandlet SIARD-deponering",
        info_rows=[
            ("Arkiv:", arc_name),
            ("Fil:", siard_path.name),
            ("Utstedt:", _no_date(now)),
        ])

    story.append(tk.para(
        "Dette dokumentet oppsummerer behandlingen av den deponerte "
        "SIARD-overføringen for langtidslagring. Formålet er å sikre at "
        "bevaring av elektronisk arkivmateriale skjer i henhold til gjeldende "
        "standarder og retningslinjer, jf. Riksarkivarens forskrift §5 om "
        "gyldige arkivformat."))

    # ── Samlet status ────────────────────────────────────────────────────────
    if avvik and not fail_steps:
        story.append(tk.status_box(
            "GODKJENT MED AVVIK",
            f"{fmt_int(failed)} fil(er) kunne ikke konverteres", "warn"))
    elif fail_steps:
        story.append(tk.status_box(
            "FEIL OPPDAGET",
            f"{len(fail_steps)} av {len(run_steps)} steg feilet", "fail"))
    else:
        story.append(tk.status_box(
            "GODKJENT",
            f"Alle {len(run_steps)} behandlingssteg fullført", "ok"))

    # ── Verktøy og metoder ───────────────────────────────────────────────────
    story += tk.section("Verktøy og metoder som er benyttet")
    cm = tk._rl["cm"]
    tool_rows = []
    for s in step_results:
        oid = s.get("id", "")
        desc = _TOOL_DESCRIPTIONS.get(oid, s.get("category", ""))
        st = ("Hoppet over" if s.get("skipped")
              else "OK" if s.get("success") else "FEIL")
        tool_rows.append([s.get("label", oid), desc, st])
    if tool_rows:
        story.append(tk.data_table(
            ["Verktøy/steg", "Beskrivelse", "Status"], tool_rows,
            [5.2 * cm, tk.inner_w - 8.2 * cm, 3.0 * cm],
            center_cols=[2]))

    # ── Nøkkelstatistikk ─────────────────────────────────────────────────────
    story += tk.section("Sammendrag")
    cw = [tk.inner_w - 9.0 * cm, 4.0 * cm, 5.0 * cm]
    stat_rows = []
    row_bgs: dict[int, object] = {}
    if meta:
        stat_rows.append(["Skjemaer / tabeller",
                          f"{fmt_int(meta.get('schema_count', 0))} / "
                          f"{fmt_int(meta.get('table_count', 0))}",
                          "Database-struktur"])
        stat_rows.append(["Totalt antall rader",
                          fmt_int(meta.get("row_count", 0)),
                          "Sum over alle tabeller"])
        stat_rows.append(["LOB-filer totalt",
                          fmt_int(meta.get("lob_file_count", 0)),
                          "Binærfiler i arkivet"])
    if has_blob:
        idx = len(stat_rows)
        stat_rows.append(["Konvertert til PDF/A", fmt_int(converted),
                          "Endret til arkivformat"])
        stat_rows.append(["Beholdt i arkivformat", fmt_int(kept),
                          "Opprinnelig gyldig format"])
        row_bgs[idx] = tk.C_SUCCESS_BG
        row_bgs[idx + 1] = tk.C_SUCCESS_BG
        if failed:
            stat_rows.append(["Feilede filer", fmt_int(failed),
                              "Konvertering mislyktes"])
            row_bgs[len(stat_rows) - 1] = tk.C_ERROR_BG
    stat_rows.append(["Behandlingsstatus",
                      "Godkjent med avvik" if avvik else "Godkjent", ""])
    if stat_rows:
        story.append(tk.data_table(
            ["Nøkkelstatistikk", "Verdi", "Beskrivelse"], stat_rows, cw,
            right_cols=[1], row_bgs=row_bgs))

    # ── Filstatus og behandlingsresultater ───────────────────────────────────
    if has_blob and archive_files:
        story += tk.section("Filstatus og behandlingsresultater")
        fs_rows = [
            ["Opprinnelig i arkivformat", fmt_int(kept),
             fmt_pct(kept, archive_files)],
            ["Nye i arkivformat (konvertert)", fmt_int(converted),
             fmt_pct(converted, archive_files)],
            ["Feilede filer", fmt_int(failed), fmt_pct(failed, archive_files)],
            ["Totalt behandlet", fmt_int(archive_files), "100,0 %"],
        ]
        story.append(tk.data_table(
            ["Status", "Antall filer", "Andel"], fs_rows,
            [tk.inner_w - 7.0 * cm, 3.5 * cm, 3.5 * cm],
            right_cols=[1, 2], total_row=True))

    # ── Signatur / utstedelse ────────────────────────────────────────────────
    story += tk.section("Utstedelse")
    verdict = "Godkjent med avvik" if avvik else "Godkjent"
    story.append(tk.kv_table([
        ("Dato", _no_date(now)),
        ("Behandlingsstatus", verdict),
        ("Behandlet av", "KDRS SIARD Manager (automatisert)"),
    ]))
    if failed:
        story.append(tk.note(
            f"Avvik: {fmt_int(failed)} fil(er) kunne ikke konverteres og "
            "arkiveres i opprinnelig format. Se konverteringsrapporten for "
            "detaljer.", "warn"))
    story.append(tk.spacer(0.3))
    story.append(tk.caption(
        "Denne rapporten er automatisk generert av KDRS SIARD Manager. "
        f"Rapport generert: {_no_datetime(now)}."))

    tk.build(out_path, f"Behandlingssammendrag — {arc_name}", story,
             footer_label=arc_name)
