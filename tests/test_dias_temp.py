"""
DIAS-pakking:
  * temp-arbeid (SIP-struktur + tar) i prosjektets temp-mappe — ikke %TEMP%
  * SIARD-filen kopieres IKKE til temp; den legges rett i tar-arkivet fra
    kildestedet, og sjekksummen i METS/PREMIS gjelder samme fil

Før: hele SIARD-en ble først kopiert til %TEMP% (ofte C:), så lagt i tar —
dobbel datamengde.

Kjør:  python -X utf8 tests/test_dias_temp.py
"""
from __future__ import annotations

import sys
import tempfile
import threading
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


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


def _run(tmp: Path, temp_dir: "str | None") -> "tuple[object, list, list]":
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations import dias_package_operation as D
    created: list = []
    orig_mkdtemp = D.tempfile.mkdtemp
    orig_td = D.tempfile.TemporaryDirectory

    def mkdtemp(*a, **k):
        p = orig_mkdtemp(*a, **k); created.append(Path(p)); return p

    class TD(orig_td):
        def __init__(self, *a, **k):
            super().__init__(*a, **k); created.append(Path(self.name))

    D.tempfile.mkdtemp, D.tempfile.TemporaryDirectory = mkdtemp, TD
    try:
        siard = _siard(tmp / "arkiv.siard")
        logs: list = []
        meta = {"file_logger": None, "stop_event": threading.Event(),
                "pause_event": threading.Event(), "log_dir": str(tmp)}
        if temp_dir:
            meta["temp_dir"] = temp_dir
        ctx = WorkflowContext(siard_path=siard, metadata=meta)
        op = D.DiasPackageOperation()
        for k in op._REQUIRED_META:
            op.params[k] = op.params.get(k) or "x"
        op.params["output_dir"] = str(tmp / "ut")
        D.logger.info = lambda msg, *a: logs.append(str(msg))
        r = op.run(ctx)
    finally:
        D.tempfile.mkdtemp, D.tempfile.TemporaryDirectory = orig_mkdtemp, orig_td
    return r, created, logs


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="diastemp_") as td:
        tmp = Path(td)
        proj_temp = tmp / "prosjekt_temp"
        proj_temp.mkdir()
        print("test_project_temp")
        r, created, logs = _run(tmp, str(proj_temp))
        _ok(r.success, f"DIAS-pakke laget ({r.message})")
        _ok(len(created) == 1, f"bare én temp-mappe (arbeid/tar), ingen kopi av innholdet "
            f"({len(created)})")
        _ok(all(proj_temp in c.parents for c in created),
            "alle temp-mapper ligger i prosjektets temp-mappe: "
            + ", ".join(c.name for c in created))
        _ok(any("Temp for pakking" in m and "prosjektets temp-mappe" in m for m in logs),
            "loggen viser hvilken temp-mappe som brukes")
        _ok(not any(proj_temp.iterdir()), "temp-mappene ryddes etter pakking")
        aic = Path(r.data["aic_path"])
        tars = list(aic.rglob("*.tar"))
        _ok(aic.exists() and tars, "pakken ligger i utmappa med tar-arkiv")
        import hashlib, tarfile
        src = tmp / "arkiv.siard"
        with tarfile.open(tars[0]) as t:
            names = t.getnames()
            m = next(n for n in names if n.endswith("/content/arkiv.siard"))
            data = t.extractfile(m).read()
            mets = t.extractfile(next(n for n in names if n.endswith("/mets.xml"))).read().decode()
        _ok(data == src.read_bytes(), f"SIARD-en ligger uendret i tar-arkivet ({m})")
        _ok(hashlib.sha256(data).hexdigest() in mets.lower(),
            "METS har sjekksummen til SIARD-filen")

        print("test_auto_temp")
        tmp2 = tmp / "auto"
        tmp2.mkdir()
        r2, created2, logs2 = _run(tmp2, None)
        sys_temp = Path(tempfile.gettempdir()).resolve()
        _ok(r2.success, "uten prosjekt-temp: pakking ok")
        _ok(any("Temp for pakking" in m and "auto-valgt disk" in m for m in logs2),
            "uten prosjekt-temp: disk velges automatisk (som BLOB-konverteringen)")
        print("    auto-valgt:", sorted({str(c.parent) for c in created2}),
              "| system-temp:", sys_temp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
