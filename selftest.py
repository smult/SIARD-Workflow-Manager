"""selftest.py

Selvtest av programpakken: `SIARDManager.exe --selftest [resultat.json]`
(eller `python main.py --selftest`). Kjøres av packaging/build_release.py
etter bygging, og kan kjøres på en ny maskin for å bekrefte at pakken er
komplett — uten nett.

Sjekker at alle Python-pakker kan importeres og faktisk virker, at alle
programmoduler lastes, at medfølgende ressursfiler finnes, at dra-og-slipp
(tkdnd) og underprosesser (ProcessPoolExecutor i pakket program) virker, og
at datamappa er skrivbar. Returkode 0 = alt OK.
"""
from __future__ import annotations

import json
import sys
import time
import traceback
from pathlib import Path


def _square(x: int) -> int:          # for ProcessPoolExecutor-testen (må kunne picklas)
    return x * x


def _modules() -> list[str]:
    import pkgutil
    names: list[str] = []
    for pkg_name in ("siard_workflow", "gui"):
        pkg = __import__(pkg_name)
        for m in pkgutil.walk_packages(pkg.__path__, prefix=pkg_name + "."):
            if ".examples" in m.name or m.name.endswith("app_backup"):
                continue
            names.append(m.name)
    return names


def run(out: "str | None" = None) -> int:
    results: list[dict] = []

    def check(name: str, fn) -> None:
        t0 = time.monotonic()
        try:
            detail = fn()
            results.append({"sjekk": name, "ok": True, "detalj": str(detail or ""),
                            "sek": round(time.monotonic() - t0, 2)})
        except Exception as exc:
            results.append({"sjekk": name, "ok": False,
                            "detalj": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc()[-1500:]})

    import app_paths
    import dependencies as D

    for p in D.PY_PACKAGES:
        check(f"pakke: {p.label}", lambda p=p: __import__(p.import_name) and
              (D.package_version(p) or "ok"))

    def _pdf():
        import fitz
        doc = fitz.open()
        doc.new_page().insert_text((72, 72), "Selvtest")
        data = doc.tobytes()
        return f"PyMuPDF {fitz.VersionBind}, {len(data)} byte"
    check("PyMuPDF lager PDF", _pdf)

    def _rl():
        import io
        from reportlab.platypus import SimpleDocTemplate, Paragraph
        from reportlab.lib.styles import getSampleStyleSheet
        b = io.BytesIO()
        SimpleDocTemplate(b).build([Paragraph("Selvtest æøå", getSampleStyleSheet()["Normal"])])
        return f"{len(b.getvalue())} byte"
    check("ReportLab lager PDF", _rl)

    def _lxml():
        from lxml import etree
        return etree.fromstring(b"<a><b/></a>").tag
    check("lxml parser XML", _lxml)

    def _docx():
        import docx
        return f"{len(docx.Document().paragraphs)} avsnitt (standardmal funnet)"
    check("python-docx standardmal", _docx)

    check("programmoduler", lambda: f"{len([__import__(m) for m in _modules()])} moduler lastet")

    def _res():
        need = [("siard_workflow", "sources", "kommunenummer.csv"),
                ("siard_workflow", "sources", "DIAS_PREMIS.xsd"),
                ("siard_workflow", "sources", "kildesystem_cache.json")]
        missing = [str(Path(*n)) for n in need if not app_paths.resource(*n).exists()]
        if missing:
            raise FileNotFoundError(", ".join(missing))
        return str(app_paths.bundle_dir())
    check("ressursfiler", _res)

    def _tk():
        import tkinter as tk
        import customtkinter  # noqa: F401  (temaer lastes ved import)
        from tkinterdnd2 import TkinterDnD
        root = tk.Tk()
        root.withdraw()
        try:
            ver = TkinterDnD._require(root)
        finally:
            root.destroy()
        return f"tkdnd {ver}"
    check("tkinter + dra-og-slipp", _tk)

    def _mp():
        import concurrent.futures as cf
        with cf.ProcessPoolExecutor(max_workers=1) as ex:
            return f"underprosess svarte {ex.submit(_square, 7).result(timeout=120)}"
    check("underprosesser (XML-rens)", _mp)

    def _data():
        d = app_paths.data_dir()
        probe = d / ".selvtest"
        probe.write_text("ok", "utf-8")
        probe.unlink()
        return str(d)
    check("datamappe skrivbar", _data)

    def _comp():
        from siard_workflow.core import components as C
        found = {k: str(C.bundled(k) or "") for k in C.KINDS}
        return ", ".join(f"{k}={'ja' if v else 'nei'}" for k, v in found.items())
    check("medfølgende komponenter", _comp)

    ok = all(r["ok"] for r in results)
    report = {"ok": ok, "frozen": app_paths.FROZEN, "python": sys.version.split()[0],
              "sjekker": results}
    text = json.dumps(report, ensure_ascii=False, indent=1)
    if out:
        Path(out).write_text(text, "utf-8")
    try:
        print(text)
    except Exception:
        pass
    return 0 if ok else 1
