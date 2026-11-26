"""
PREMIS-proveniensfila skal alltid med i DIAS-pakken.

Bakgrunn (nov. 2026): PREMIS-fila («Uttrekk_premis.xml») kom ikke med i
pakken. DIAS-pakkingen lette etter den ut fra navnet på RESULTAT-fila og
strippet bare ett suffiks av hver sort og ikke løpenummer:
«Uttrekk_konvertert_anonymisert.siard» ga «Uttrekk_konvertert_premis.xml», og
«Uttrekk_konvertert_1.siard» ga «Uttrekk_konvertert_1_premis.xml» — ingen av
dem finnes. Fila sto heller ikke i filtreet i DIAS-vinduet.

Verifiserer:
  1. grunnavnet utledes likt overalt (core/naming.py)
  2. PREMIS-fila ligger i tar-arkivet og i METS for resultatnavnene som
     feilet, også når logg-mappa er en annen enn SIARD-mappa
  3. kjøringens PREMIS-logger brukes direkte (egendefinert suffiks), og
     hendelser som ikke er skrevet ennå, skrives før pakking
  4. uten PREMIS-fil pakkes det som før, med en forklarende logglinje
  5. DIAS-vinduet: fila står i filtreet (eller som «skrives under kjøringen»)

Kjør:  python -X utf8 tests/test_dias_premis_included.py
"""
from __future__ import annotations

import os
import sys
import tarfile
import tempfile
import threading
import types
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_PREMIS_DEST = "administrative_metadata/repository_operations/"


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _siard(path: Path) -> Path:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("header/metadata.xml",
                   '<?xml version="1.0"?><siardArchive xmlns="http://www.bar.admin.ch/'
                   'xmlns/siard/2/metadata.xsd"><dbname>test</dbname><lobFolder>content'
                   '</lobFolder></siardArchive>')
        z.writestr("content/schema0/table0/table0.xml", "<table/>")
    return path


class _Op:
    operation_id = "blob_convert"
    premis_event_type = "Migration"
    premis_event_label = "formatkonvertering"

    def premis_detail(self, result, ctx):
        return "12 filer konvertert til PDF/A"


def _write_premis(log_dir: Path, original: Path) -> Path:
    """En PREMIS-fil slik en tidligere kjøring etterlater den."""
    from siard_workflow.core.premis_logger import PremisProvenanceLogger
    pl = PremisProvenanceLogger(log_dir, original)
    pl.record(_Op(), types.SimpleNamespace(success=True, message=""), None)
    out = pl.finalize(original, None)
    assert out is not None and out.exists()
    return out


def _pack(siard: Path, out_dir: Path, *, log_dir: "Path | None" = None,
          premis_logger=None, results: "dict | None" = None,
          extra_files: "list | None" = None) -> "tuple[object, list, list, str]":
    """Kjør DIAS-pakkingen. Returnerer (resultat, navn i tar, logglinjer, mets)."""
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations import dias_package_operation as D
    logs: list = []
    meta = {"file_logger": None, "stop_event": threading.Event(),
            "pause_event": threading.Event(),
            "log_dir": str(log_dir or siard.parent)}
    if premis_logger is not None:
        meta["premis_logger"] = premis_logger
    ctx = WorkflowContext(siard_path=siard, metadata=meta)
    for _k, _v in (results or {}).items():
        ctx.set_result(_k, _v)
    op = D.DiasPackageOperation()
    if extra_files is not None:
        import json
        op.params["extra_files"] = json.dumps(extra_files, ensure_ascii=False)
    for k in op._REQUIRED_META:
        op.params[k] = op.params.get(k) or "x"
    op.params["period_start"], op.params["period_end"] = "2010-01-01", "2015-12-31"
    op.params["output_dir"] = str(out_dir)
    orig_info = D.logger.info
    D.logger.info = lambda msg, *a: logs.append(str(msg) % a if a else str(msg))
    try:
        r = op.run(ctx)
    finally:
        D.logger.info = orig_info
    names: list = []
    mets = ""
    if r.success:
        tars = list(Path(r.data["aic_path"]).rglob("*.tar"))
        with tarfile.open(tars[0]) as t:
            names = t.getnames()
            mets = t.extractfile(
                next(n for n in names if n.endswith("/mets.xml"))).read().decode()
    return r, names, logs, mets


def _has(names: list, rel: str) -> bool:
    return any(n.endswith("/" + rel) for n in names)


def test_base_names() -> None:
    print("\n[1] Grunnavnet utledes likt overalt")
    from siard_workflow.core.naming import sidecar_base
    from siard_workflow.core.premis_logger import _base_name
    from gui.dias_dialog import _base_stem
    cases = {
        "Uttrekk.siard": "Uttrekk",
        "Uttrekk_konvertert.siard": "Uttrekk",
        "Uttrekk_hex_extracted_konvertert.siard": "Uttrekk",
        "Uttrekk_konvertert_anonymisert.siard": "Uttrekk",
        "Uttrekk_konvertert_1.siard": "Uttrekk",
        "Uttrekk_konvertert_anonymisert_12.siard": "Uttrekk",
        "Uttrekk_stdext_xsdfix_konvertert.siard": "Uttrekk",
        "WIS-Marnardal_2024.siard": "WIS-Marnardal_2024",
        "WIS-Marnardal_2024_konvertert_2.siard": "WIS-Marnardal_2024",
        "Arkiv_1.siard": "Arkiv_1",
        "_konvertert.siard": "_konvertert",
    }
    for name, want in cases.items():
        got = {sidecar_base(Path(name)), sidecar_base(name), _base_name(Path(name)),
               _base_stem(Path(name).stem)}
        _ok(got == {want}, f"{name} → {want}")


def test_packaged(tmp: Path) -> None:
    print("\n[2] PREMIS-fila kommer med for resultatnavnene som feilet")
    for i, result_name in enumerate(("Uttrekk_konvertert_anonymisert.siard",
                                     "Uttrekk_konvertert_1.siard",
                                     "Uttrekk_konvertert.siard")):
        d = tmp / f"a{i}"
        d.mkdir()
        _write_premis(d, d / "Uttrekk.siard")
        (d / "Uttrekk.sha256").write_text("abc  Uttrekk.siard\n", encoding="utf-8")
        r, names, logs, mets = _pack(_siard(d / result_name), d / "ut")
        _ok(r.success and _has(names, _PREMIS_DEST + "Uttrekk_premis.xml"),
            f"{result_name}: Uttrekk_premis.xml ligger i pakken")
        _ok("Uttrekk_premis.xml" in mets, "og er ført i METS")
        _ok(_has(names, "administrative_metadata/Uttrekk.sha256"),
            "SHA-256-fila (samme grunnavn) er også med")
        _ok(_has(names, "administrative_metadata/premis.xml"),
            "pakkens egen premis.xml er uendret på plass")

    d = tmp / "b"
    logdir = tmp / "b_logg"
    d.mkdir(); logdir.mkdir()
    _write_premis(logdir, d / "Uttrekk.siard")
    r, names, logs, mets = _pack(_siard(d / "Uttrekk_konvertert_1.siard"), d / "ut",
                                 log_dir=logdir)
    _ok(r.success and _has(names, _PREMIS_DEST + "Uttrekk_premis.xml"),
        "PREMIS-fila hentes fra logg-mappa når den er en annen enn SIARD-mappa")


def test_logger_direct(tmp: Path) -> None:
    print("\n[3] Kjøringens PREMIS-logger brukes direkte")
    from siard_workflow.core.premis_logger import PremisProvenanceLogger
    d = tmp / "c"
    logdir = tmp / "c_logg"
    d.mkdir(); logdir.mkdir()
    pl = PremisProvenanceLogger(logdir, d / "Uttrekk.siard")
    pl.record(_Op(), types.SimpleNamespace(success=True, message=""), None)
    _ok(pl.has_events() and not pl.out_path.exists(),
        "(hendelsen er registrert, men fila er ikke skrevet ennå)")
    # Egendefinert suffiks på resultatfila — kan ikke gjettes ut fra navnet
    r, names, logs, mets = _pack(_siard(d / "Uttrekk_ferdig_v2.siard"), d / "ut",
                                 log_dir=tmp / "finnes_ikke", premis_logger=pl)
    _ok(r.success and _has(names, _PREMIS_DEST + "Uttrekk_premis.xml"),
        "egendefinert suffiks: fila skrives og kommer med i pakken")
    with tarfile.open(next(Path(r.data["aic_path"]).rglob("*.tar"))) as t:
        body = t.extractfile(next(
            n for n in t.getnames() if n.endswith("Uttrekk_premis.xml"))).read().decode()
    _ok("12 filer konvertert til PDF/A" in body, "med hendelsene fra denne kjøringen")
    _ok(any("Auto-inkludert: Uttrekk_premis.xml" in m for m in logs),
        "loggen sier at fila er tatt med")


def test_without_premis(tmp: Path) -> None:
    print("\n[4] Uten PREMIS-fil")
    d = tmp / "d"
    d.mkdir()
    r, names, logs, mets = _pack(_siard(d / "Uttrekk.siard"), d / "ut")
    _ok(r.success and not any(n.endswith("_premis.xml") for n in names),
        "pakken lages som før")
    _ok(any("PREMIS-proveniens: ingen fil" in m for m in logs),
        "loggen forklarer hvorfor PREMIS-proveniens ikke er med")


def test_dialog_tree(tmp: Path) -> None:
    print("\n[5] DIAS-vinduet viser PREMIS-fila i filtreet")
    import settings
    import gui.dias_dialog as G
    d = tmp / "e"
    logdir = tmp / "e_logg"
    d.mkdir(); logdir.mkdir()
    siard = _siard(d / "Uttrekk.siard")
    orig_get = settings.get_config
    cfg = {"log_dir": "", "enable_premis_provenance": True}
    settings.get_config = lambda k, default=None: cfg.get(k, orig_get(k, default))
    try:
        _ok(not any(f["name"].endswith("_premis.xml") for f in G._discover_files(siard)),
            "(ingen PREMIS-fil ennå)")
        _write_premis(d, siard)
        hit = [f for f in G._discover_files(siard) if f["name"] == "Uttrekk_premis.xml"]
        _ok(len(hit) == 1 and hit[0]["dest"] == _PREMIS_DEST + "Uttrekk_premis.xml"
            and hit[0]["tag"] == "auto",
            "fila står i treet under repository_operations")
        (d / "Uttrekk_premis.xml").unlink()
        cfg["log_dir"] = str(logdir)
        _write_premis(logdir, siard)
        (logdir / "Uttrekk_20261126_090000.log").write_text("x", encoding="utf-8")
        found = {f["name"] for f in G._discover_files(siard)}
        _ok({"Uttrekk_premis.xml", "Uttrekk_20261126_090000.log"} <= found,
            "også når Logg-mappe er satt i Innstillinger (gjelder loggene også)")

        def _expected(ids: list) -> bool:
            return G.DiasParamDialog._premis_expected(
                types.SimpleNamespace(_upstream_op_ids=ids))

        _ok(_expected(["unpack_siard", "blob_convert", "repack_siard"]),
            "før kjøring: «skrives under kjøringen» når et steg endrer innholdet")
        _ok(not _expected(["sha256", "metadata_extract"]),
            "ikke når ingen steg endrer innholdet")
        cfg["enable_premis_provenance"] = False
        _ok(not _expected(["blob_convert"]), "ikke når PREMIS er slått av")
        _ok(G._PREMIS_PENDING["token"] == "provenance_premis"
            and G._PREMIS_PENDING["skip_if_suffix"] == "_premis.xml",
            "plassholderen løses til fila ved pakking, og vises ikke dobbelt")
    finally:
        settings.get_config = orig_get


def main() -> int:
    threading.Timer(240, lambda: os._exit(3)).start()
    with tempfile.TemporaryDirectory(prefix="diaspremis_") as td:
        tmp = Path(td)
        test_base_names()
        test_packaged(tmp)
        test_logger_direct(tmp)
        test_without_premis(tmp)
        test_dialog_tree(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    except BaseException:
        import traceback
        traceback.print_exc()
        rc = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)
