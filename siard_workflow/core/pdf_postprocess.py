"""siard_workflow/core/pdf_postprocess.py

Etterbehandling av PDF-filer med Ghostscript for mindre lagringsplass.

Gjelder tre grupper (opprinnelse):
  libreoffice  PDF/A laget av LibreOffice i samme kjøring
  system       eksisterende PDF-er fra fagsystemet (LOB-filer)
  arkiv        PDF-er inne i arkiver (zip) etter utpakking/ompakking
Signerte PDF-er røres ALDRI.

Bakgrunn (målt 2026-10-12 på 25 PDF/A-2b-filer fra LibreOffice 26.2): Ghostscript
skriver filene ~27 % mindre uten innholdstap — fontene lagres mer kompakt (uten
skjerm-«hinting»), like bilder lagres én gang, mindre ICC-profil og tettere
intern struktur.

Bilder komprimeres som standard IKKE med tap: JPEG/JPEG 2000 sendes uendret
gjennom, øvrige bilder lagres tapsfritt (Flate) — ingen nedsampling. Med
tilvalget «Tillat JPEG-omkoding av bilder» (lossless=False) bruker Ghostscript
sin standard, som kan kode bilder om til JPEG (større besparelse, tapsbeheftet;
målt 27 % mot 13 % tapsfritt på 25 filer). Den visuelle kontrollen gjelder
uansett.

Hver fil kontrolleres før den erstattes; ellers beholdes originalen:
  * signert / kryptert / innebygde filer / skjemafelt → hoppes over
  * Ghostscript feiler                                → beholdes
  * resultatet ikke PDF/A, annet sidetall, annen tekst
    eller synlig forskjell ved gjengivelse            → beholdes
  * besparelse under 2 %                              → beholdes
PDF-er som ikke var PDF/A fra før, blir PDF/A når de erstattes (del fra valgt
PDF/A-versjon); de som da ville blitt større, beholdes som de er.
"""
from __future__ import annotations

import concurrent.futures
import os
import shutil
import threading
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

MIN_SAVING = 0.02
ORIGIN_LABELS = {"libreoffice": "PDF/A fra LibreOffice (denne kjøringen)",
                 "system": "eksisterende PDF fra systemet",
                 "arkiv": "PDF inne i arkiv"}


@dataclass
class PdfItem:
    path: Path                  # frittstående PDF, eller zip-fila som inneholder den
    member: str = ""            # navn i zip-fila ("" = frittstående)
    origin: str = "system"

    @property
    def label(self) -> str:
        return f"{self.path.name}!{self.member}" if self.member else self.path.name

    def size(self) -> int:
        try:
            if self.member:
                with zipfile.ZipFile(self.path) as z:
                    return z.getinfo(self.member).file_size
            return self.path.stat().st_size
        except Exception:
            return 0

    def read_to(self, dst: Path) -> Path:
        if not self.member:
            return self.path
        dst.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(self.path) as z:
            dst.write_bytes(z.read(self.member))
        return dst


@dataclass
class OptResult:
    item: PdfItem
    before: int = 0
    after: int = 0
    out: "Path | None" = None       # optimalisert fil (temp) når ok
    ok: bool = False
    reason: str = ""                # hvorfor filen beholdes
    became_pdfa: bool = False       # var ikke PDF/A fra før

    @property
    def saved(self) -> int:
        return self.before - self.after if self.ok else 0


def _page_texts(path: Path) -> "tuple[int, list[str]]":
    import fitz
    with fitz.open(str(path)) as d:
        return d.page_count, [" ".join(p.get_text().split()) for p in d]


def _visually_equal(a: Path, b: Path, pages: int = 3) -> bool:
    """Gjengi de første sidene grovt (40 dpi, gråtoner) og sammenlign."""
    import fitz
    with fitz.open(str(a)) as da, fitz.open(str(b)) as db:
        for i in range(min(pages, da.page_count)):
            pa = da[i].get_pixmap(dpi=40, colorspace=fitz.csGRAY)
            pb = db[i].get_pixmap(dpi=40, colorspace=fitz.csGRAY)
            if (pa.width, pa.height) != (pb.width, pb.height):
                return False
            diffs = [abs(x - y) for x, y in zip(pa.samples, pb.samples)]
            if not diffs:
                continue
            if sum(diffs) / len(diffs) > 3 or \
                    sum(1 for d in diffs if d > 64) / len(diffs) > 0.005:
                return False
    return True


def optimize(item: PdfItem, out: Path, gs: Path, timeout: float = 300,
             default_part: int = 2, lossless: bool = True) -> OptResult:
    """Lag optimalisert kopi i `out` og kontroller den (originalen røres ikke)."""
    from siard_workflow.core import ghostscript as G
    from siard_workflow.core.file_integrity import is_signed_pdf
    from siard_workflow.core.msg_to_pdf import pdfa_identification
    r = OptResult(item=item)
    src = None
    try:
        src = item.read_to(out.with_name(out.stem + "_kilde.pdf"))
        r.before = src.stat().st_size
        if is_signed_pdf(src):
            r.reason = "signert PDF — røres ikke"
            return r
        import fitz
        with fitz.open(str(src)) as d:
            if d.needs_pass or d.is_encrypted:
                r.reason = "kryptert/passordbeskyttet"
                return r
            if d.embfile_count():
                r.reason = "har innebygde filer (ville gått tapt)"
                return r
            if d.is_form_pdf:
                r.reason = "har skjemafelt"
                return r
        ident = pdfa_identification(src)
        part = int(ident[0]) if ident else default_part
        r.became_pdfa = ident is None
        out.parent.mkdir(parents=True, exist_ok=True)
        ok, err = G.to_pdfa(src, out, part, gs, timeout, lossless_images=lossless)
        if not ok or not out.exists():
            r.reason = f"Ghostscript feilet ({(err or '')[:80]})"
            return r
        new_ident = pdfa_identification(out)
        if not new_ident or int(new_ident[0]) != part:
            r.reason = "resultatet ble ikke gyldig merket PDF/A"
            return r
        if _page_texts(src) != _page_texts(out):
            r.reason = "tekst eller sidetall avviker"
            return r
        if not _visually_equal(src, out):
            r.reason = "synlig forskjell ved gjengivelse"
            return r
        r.after = out.stat().st_size
        if r.after > r.before * (1 - MIN_SAVING):
            r.reason = ("ville blitt større som PDF/A" if r.after > r.before
                        else "ingen vesentlig besparelse")
            r.after = r.before
            return r
        r.ok, r.out = True, out
        return r
    except Exception as exc:
        r.reason = f"{type(exc).__name__}: {exc}"
        return r
    finally:
        if src is not None and item.member:
            try:
                src.unlink(missing_ok=True)
            except Exception:
                pass
        if not r.ok:
            try:
                Path(out).unlink(missing_ok=True)
            except Exception:
                pass


def pick_sample(items: "list[PdfItem]", n: int) -> "list[PdfItem]":
    """
    Representativt utvalg: minst én fil fra hver opprinnelse (når mulig), resten
    jevnt fordelt over filstørrelsene.
    """
    if len(items) <= n:
        return list(items)
    by_size = sorted(items, key=lambda it: it.size())
    picked: list = []
    for origin in ORIGIN_LABELS:
        group = [it for it in by_size if it.origin == origin]
        if group and len(picked) < n:
            picked.append(group[len(group) // 2])
    rest = [it for it in by_size if it not in picked]
    k = n - len(picked)
    if k > 0 and rest:
        step = (len(rest) - 1) / max(1, k - 1)
        for i in range(min(k, len(rest))):
            cand = rest[round(i * step)]
            if cand not in picked:
                picked.append(cand)
    return picked


def run_many(items: "list[PdfItem]", work_dir: Path, gs: Path, workers: int = 4,
             timeout: float = 300, stop_ev: "threading.Event | None" = None,
             progress: "Optional[Callable[[int, int], None]]" = None,
             default_part: int = 2, lossless: bool = True) -> "list[OptResult]":
    """Optimaliser parallelt til `work_dir` (originalene røres ikke)."""
    work_dir.mkdir(parents=True, exist_ok=True)
    results: list = []
    lock = threading.Lock()

    def one(i_it):
        i, it = i_it
        if stop_ev is not None and stop_ev.is_set():
            res = OptResult(item=it, before=it.size(), reason="avbrutt")
        else:
            res = optimize(it, work_dir / f"{i}.pdf", gs, timeout, default_part, lossless)
        with lock:
            results.append(res)
            if progress:
                progress(len(results), len(items))
        return res

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        list(pool.map(one, enumerate(items)))
    return results


def estimate(sample: "list[OptResult]", all_items: "list[PdfItem]",
             sample_seconds: float, workers: int) -> dict:
    """Besparelse for utvalget og beregnet besparelse for alle filene."""
    s_before = sum(r.before for r in sample)
    s_after = sum(r.after if r.ok else r.before for r in sample)
    ratio = (s_after / s_before) if s_before else 1.0
    total = sum(it.size() for it in all_items)
    per_file = sample_seconds * max(1, min(workers, len(sample))) / max(1, len(sample))
    origins: dict = {}
    for it in all_items:
        origins[it.origin] = origins.get(it.origin, 0) + 1
    skipped: dict = {}
    for r in sample:
        if not r.ok:
            skipped[r.reason] = skipped.get(r.reason, 0) + 1
    return {
        "n_sample": len(sample),
        "sample_before": s_before,
        "sample_after": s_after,
        "sample_saving_pct": (1 - ratio) * 100,
        "n_ok": sum(1 for r in sample if r.ok),
        "skipped": skipped,
        "n_total": len(all_items),
        "origins": origins,
        "origin_labels": dict(ORIGIN_LABELS),
        "total_before": total,
        "est_after": int(total * ratio),
        "est_saved": int(total * (1 - ratio)),
        "est_seconds": per_file * len(all_items) / max(1, workers),
    }


def _rewrite_zip(zip_path: Path, replacements: "dict[str, Path]") -> None:
    """Skriv zip-fila på nytt med erstattede medlemmer; resten byte for byte likt."""
    tmp = zip_path.with_name(zip_path.name + ".gs_tmp")
    with zipfile.ZipFile(zip_path) as zin, zipfile.ZipFile(tmp, "w") as zout:
        zout.comment = zin.comment
        for info in zin.infolist():
            if info.filename in replacements:
                ni = zipfile.ZipInfo(info.filename, date_time=info.date_time)
                ni.compress_type = zipfile.ZIP_DEFLATED
                ni.external_attr = info.external_attr
                zout.writestr(ni, replacements[info.filename].read_bytes())
            else:
                zout.writestr(info, zin.read(info.filename))
    os.replace(tmp, zip_path)


def apply(results: "list[OptResult]") -> "tuple[int, int, int]":
    """Erstatt originalene med kontrollerte, mindre filer. (antall, før, etter)."""
    n = before = after = 0
    per_zip: dict = {}
    for r in results:
        if not r.ok or r.out is None or not r.out.exists():
            continue
        if r.item.member:
            per_zip.setdefault(r.item.path, []).append(r)
            continue
        try:
            os.replace(r.out, r.item.path)
        except OSError:
            shutil.copy2(r.out, r.item.path)
            r.out.unlink(missing_ok=True)
        n += 1
        before += r.before
        after += r.after
    for zpath, rs in per_zip.items():
        try:
            _rewrite_zip(zpath, {r.item.member: r.out for r in rs})
        except Exception:
            for r in rs:
                r.ok, r.reason = False, "zip kunne ikke skrives på nytt"
            continue
        for r in rs:
            n += 1
            before += r.before
            after += r.after
    return n, before, after


def zip_pdf_members(zip_path: Path) -> "list[str]":
    """PDF-medlemmer på øverste nivå i en zip-fil (ikke nestede arkiver)."""
    try:
        with zipfile.ZipFile(zip_path) as z:
            out = []
            for info in z.infolist():
                if info.is_dir():
                    continue
                with z.open(info) as f:
                    if b"%PDF" in f.read(1024):
                        out.append(info.filename)
            return out
    except Exception:
        return []
