"""
siard_workflow/core/run_summary.py
Sluttsammendraget i kjøreloggen: resultat, tid, stegene med status og
varighet, advarsler og feil, og filene kjøringen laget.

Sammendraget er det første en leser trenger — det skrives sist i loggen, der
man havner når fila åpnes for å se hvordan det gikk.
"""
from __future__ import annotations

from pathlib import Path

from siard_workflow.core import fmt

# Resultatnøkler som peker på filer kjøringen har laget → ledetekst
_OUTPUT_KEYS = (
    ("output_path", "Ny SIARD"),
    ("pdf_path",    "Metadata-rapport"),
    ("report_path", "Kjørerapport"),
    ("aic_path",    "DIAS-pakke (AIC)"),
)
MAX_NOTABLE = 15


_SKIP_PREFIX = "hoppet over — prosjektet er kjørt før"


def compact_preflight(lines: list) -> list:
    """
    Preflight-linjene [(nivå, melding)] til loggfila. Kontroller som hoppes
    over fordi prosjektet er kjørt før, gir ellers to like linjer hver («▸ Navn …»
    + «hoppet over — …») — de samles i én linje med navnene.
    """
    out: list = []
    skipped: list = []
    i = 0
    while i < len(lines):
        lvl, msg = lines[i]
        nxt = lines[i + 1][1] if i + 1 < len(lines) else ""
        if msg.strip().startswith("▸") and nxt.strip().startswith(_SKIP_PREFIX):
            skipped.append(msg.strip().lstrip("▸").strip().rstrip("…").strip())
            i += 2
            continue
        out.append((lvl, msg))
        i += 1
    if skipped:
        note = ("info", "  Hoppet over — prosjektet er kjørt før, svarene fra første "
                        "kjøring gjelder: " + ", ".join(skipped))
        # Rett etter innledningen (overskrift og forklaring), før øvrige kontroller
        pos = next((k for k, (_l, m) in enumerate(out) if m.strip().startswith("▸")),
                   len(out))
        out.insert(pos, note)
    return out


def failure_counts(results: dict) -> "tuple[int, int]":
    """(feilet, beholdt etter regelen) fra BLOB-konverteringens feiloversikt."""
    fs = ((results or {}).get("blob_convert") or {}).get("failure_summary") or {}
    if not isinstance(fs, dict) or not fs:
        return 0, 0
    kept_keys = set(fs.get("kept_keys") or ("passordbeskyttet", "signert"))
    counts = fs.get("counts") or {}
    kept = sum(n for k, n in counts.items() if k in kept_keys)
    failed = sum(n for k, n in counts.items() if k not in kept_keys)
    return failed, kept


def collect_outputs(results: dict, extra: "list | None" = None) -> list:
    """[(ledetekst, sti)] for filene kjøringen laget — uten duplikater."""
    out: list = []
    seen: set = set()

    def add(label, path) -> None:
        if not path:
            return
        key = str(path).lower()
        if key in seen:
            return
        seen.add(key)
        out.append((label, str(path)))

    for data in (results or {}).values():
        if not isinstance(data, dict):
            continue
        for key, label in _OUTPUT_KEYS:
            add(label, data.get(key))
        fs = data.get("failure_summary")
        if isinstance(fs, dict) and fs.get("total"):
            add("Ikke konverterte filer (regneark)", fs.get("csv_path"))
            add("Ikke konverterte filer (logg)", fs.get("log_path"))
    for label, path in (extra or []):
        add(label, path)
    return out


def _size_text(path: str) -> str:
    try:
        p = Path(path)
        if p.is_file():
            return f"  ({fmt.size(p.stat().st_size)})"
    except OSError:
        pass
    return ""


def overall_status(step_results: list, *, aborted: bool = False, held: int = 0,
                   failed_files: int = 0) -> "tuple[str, str]":
    """(nivå, tekst) for kjøringen samlet."""
    n_fail = sum(1 for s in step_results
                 if not s.get("skipped") and s.get("success") is False
                 and not s.get("interrupted"))
    if aborted:
        return "warn", "AVBRUTT — kjør arbeidsflyten igjen for å fortsette"
    if n_fail:
        return "error", f"FEIL — {n_fail} steg feilet"
    if held:
        return "warn", (f"FORELØPIG — {held} steg holdt igjen; kjør arbeidsflyten "
                        f"igjen når feilen er rettet")
    if failed_files:
        return "warn", (f"FULLFØRT MED AVVIK — {fmt.num(failed_files)} fil(er) kunne "
                        f"ikke konverteres og bør følges opp")
    return "success", "FULLFØRT uten feil"


def build_run_summary(step_results: list, *, total_s: float, wait_s: float = 0.0,
                      warnings: "list | None" = None, errors: "list | None" = None,
                      n_warnings: int = 0, n_errors: int = 0,
                      outputs: "list | None" = None, failed_files: int = 0,
                      kept_files: int = 0, held: int = 0,
                      aborted: bool = False) -> list:
    """Linjene i sluttsammendraget som [(nivå, tekst)]."""
    lines: list = [("info", "=" * 56), ("step", "SAMMENDRAG")]
    lvl, text = overall_status(step_results, aborted=aborted, held=held,
                               failed_files=failed_files)
    lines.append((lvl, f"  Resultat:  {text}"))
    tid = fmt.duration(total_s)
    if wait_s >= 60:
        tid += (f"  (herav {fmt.duration(wait_s)} venting på svar; "
                f"{fmt.duration(max(0.0, total_s - wait_s))} arbeid)")
    lines.append(("info", f"  Tid:       {tid}"))
    if failed_files or kept_files:
        lines.append(("info", f"  Filer:     {fmt.num(failed_files)} feilet, "
                              f"{fmt.num(kept_files)} beholdt uendret etter regelen "
                              f"(signert/passordbeskyttet)"))

    if step_results:
        lines.append(("info", "  Steg:"))
        width = min(46, max(len(str(s.get("label", ""))) for s in step_results))
        for s in step_results:
            label = str(s.get("label", ""))[:width]
            if s.get("skipped"):
                status, dur = "hoppet over", ""
                step_lvl = "muted"
            elif s.get("interrupted"):
                status, dur, step_lvl = "AVBRUTT", fmt.duration(s.get("elapsed", 0)), "warn"
            elif s.get("success"):
                status, dur, step_lvl = "OK", fmt.duration(s.get("elapsed", 0)), "info"
            else:
                status, dur, step_lvl = "FEIL", fmt.duration(s.get("elapsed", 0)), "error"
            nr = f"[{int(s.get('index', 0)) + 1}]"
            lines.append((step_lvl, f"    {nr:<5}{label:<{width}}  {status:<12}{dur}".rstrip()))

    if n_warnings or n_errors:
        lines.append(("info", f"  Advarsler: {fmt.num(n_warnings)}   Feil: {fmt.num(n_errors)}"))
        shown: list = []
        seen: set = set()
        for tag, items in (("FEIL", errors or []), ("ADVARSEL", warnings or [])):
            for ts, msg in items:
                if msg in seen:
                    continue
                seen.add(msg)
                shown.append((tag, ts, msg))
        for tag, ts, msg in shown[:MAX_NOTABLE]:
            lines.append(("info", f"    {ts}  {tag}: {msg[:300]}"))
        if len(shown) > MAX_NOTABLE:
            lines.append(("info", f"    … og {len(shown) - MAX_NOTABLE} til (se loggen over)"))

    if outputs:
        lines.append(("info", "  Filer laget:"))
        lw = max(len(label) for label, _ in outputs) + 1
        for label, path in outputs:
            lines.append(("info", f"    {(label + ':'):<{lw}} {path}{_size_text(path)}"))
    lines.append(("info", "=" * 56))
    return lines
