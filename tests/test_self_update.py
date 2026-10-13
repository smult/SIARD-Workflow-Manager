"""
Automatisk oppdatering (core/self_update.py):

  * riktig programpakke/sjekksum velges fra GitHub-utgivelsen
  * bytte av programfiler (ekte PowerShell-skript): nye filer inn, brukerdata
    (config.json, dias.json, komponenter/, logger) beholdes, ingen *.old igjen
  * feil under byttet → forrige versjon gjenopprettes
  * nedlasting + sjekksum + utpakking + selvtest mot en lokal «GitHub»
    (bruker den bygde programpakken i build/dist når den finnes)
  * skadet nedlasting / pakke uten program avvises

Kjør:  python -X utf8 tests/test_self_update.py
"""
from __future__ import annotations

import hashlib
import http.server
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core import self_update as SU  # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_assets() -> None:
    print("test_assets")
    rel = {"tag_name": "1.4.11", "assets": [
        {"name": "SIARDManager-1.4.11-win64-komplett.zip"},
        {"name": "SIARDManager-1.4.11-win64.zip"}, {"name": "SHA256SUMS.txt"}]}
    pkg, sums = SU.find_assets(rel)
    _ok(pkg["name"] == "SIARDManager-1.4.11-win64.zip" and sums["name"] == "SHA256SUMS.txt",
        "programpakken uten komponenter velges, med sjekksumfil")
    _ok(SU.find_assets({"tag_name": "1.4.9", "assets": []}) == (None, None),
        "utgivelse uten vedlegg → ingen pakke")
    _ok(SU.expected_sha("abc123  SIARDManager-1.4.11-win64.zip\nfff  annen.zip\n",
                        "SIARDManager-1.4.11-win64.zip") == "abc123", "sjekksum leses fra SHA256SUMS")
    m, why = SU.mode()
    _ok(m in ("git", "manuell"), f"kildekodekjøring: modus «{m}» ({why or 'ren git-kopi'})")


def _run_swap(new: Path, dst: Path, log: Path, lock: "Path | None" = None) -> None:
    waiter = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1.5)"])
    script = new.parent / "bytt.ps1"
    script.write_text(SU.swap_script(new, dst, waiter.pid, log, relaunch=False), "utf-8-sig")
    fh = open(lock, "rb") if lock else None          # holder fila åpen → flytting feiler
    try:
        subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                        "-File", str(script)], timeout=120, capture_output=True)
    finally:
        if fh:
            fh.close()
    waiter.wait(10)


def _install(tmp: Path, name: str) -> "tuple[Path, Path]":
    dst = tmp / name / "SIARDManager æøå"            # norske tegn i stien
    (dst / "_internal").mkdir(parents=True)
    (dst / "_internal" / "gammel.txt").write_text("v1", "utf-8")
    (dst / "SIARDManager.exe").write_text("exe v1", "utf-8")
    (dst / "README.md").write_text("les v1", "utf-8")
    (dst / "config.json").write_text('{"min": "innstilling"}', "utf-8")
    (dst / "dias.json").write_text("{}", "utf-8")
    (dst / "komponenter").mkdir()
    (dst / "komponenter" / "LibreOffice.msi").write_text("stor", "utf-8")
    new = tmp / name / "w" / "ny" / "SIARDManager"
    (new / "_internal").mkdir(parents=True)
    (new / "_internal" / "ny.txt").write_text("v2", "utf-8")
    (new / "SIARDManager.exe").write_text("exe v2", "utf-8")
    (new / "README.md").write_text("les v2", "utf-8")
    return new, dst


def test_swap(tmp: Path) -> None:
    print("test_swap")
    if sys.platform != "win32":
        print("  (bare Windows)")
        return
    new, dst = _install(tmp, "ok")
    log = tmp / "ok" / "oppdatering.log"
    _run_swap(new, dst, log)
    _ok((dst / "SIARDManager.exe").read_text("utf-8") == "exe v2"
        and (dst / "_internal" / "ny.txt").exists()
        and not (dst / "_internal" / "gammel.txt").exists()
        and (dst / "README.md").read_text("utf-8") == "les v2",
        "programfilene er byttet ut (også _internal/)")
    _ok((dst / "config.json").read_text("utf-8") == '{"min": "innstilling"}'
        and (dst / "dias.json").exists() and (dst / "komponenter" / "LibreOffice.msi").exists(),
        "innstillinger og komponenter er beholdt")
    _ok(not list(dst.glob("*.old")), "ingen *.old-rester")
    _ok("Oppdatering installert" in log.read_text("utf-8"), "oppdateringsloggen viser resultatet")

    new2, dst2 = _install(tmp, "feil")
    log2 = tmp / "feil" / "oppdatering.log"
    _run_swap(new2, dst2, log2, lock=dst2 / "SIARDManager.exe")
    _ok((dst2 / "SIARDManager.exe").read_text("utf-8") == "exe v1"
        and (dst2 / "_internal" / "gammel.txt").exists()
        and (dst2 / "README.md").read_text("utf-8") == "les v1",
        "feil under byttet → forrige versjon er gjenopprettet")
    _ok(not list(dst2.glob("*.old")) and "gjenoppretter" in log2.read_text("utf-8"),
        "loggen sier at forrige versjon ble gjenopprettet")


class _Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


def _serve(folder: Path):
    handler = lambda *a, **k: _Quiet(*a, directory=str(folder), **k)  # noqa: E731
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def test_prepare(tmp: Path) -> None:
    print("test_prepare")
    web = tmp / "github"
    web.mkdir()
    built = _ROOT / "build" / "dist" / "SIARDManager"
    pkg = "SIARDManager-9.9.9-win64.zip"
    if (built / "SIARDManager.exe").exists():
        with zipfile.ZipFile(web / pkg, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as z:
            for p in built.rglob("*"):
                rel = p.relative_to(built)
                if p.is_file() and rel.parts[0] != "komponenter":
                    z.write(p, Path("SIARDManager") / rel)
        real = True
    else:
        with zipfile.ZipFile(web / pkg, "w") as z:
            z.writestr("SIARDManager/SIARDManager.exe", "x")
        real = False
    sha = hashlib.sha256((web / pkg).read_bytes()).hexdigest()
    (web / "SHA256SUMS.txt").write_text(f"{sha}  {pkg}\n", "utf-8")
    srv, base = _serve(web)
    try:
        rel = {"tag_name": "9.9.9", "assets": [
            {"name": pkg, "browser_download_url": f"{base}/{pkg}"},
            {"name": "SHA256SUMS.txt", "browser_download_url": f"{base}/SHA256SUMS.txt"}]}
        msgs: list = []
        if real:
            t0 = time.monotonic()
            app = SU.prepare_package(rel, tmp / "work", status=msgs.append)
            _ok((app / "SIARDManager.exe").exists() and (app / "_internal").is_dir(),
                f"ekte pakke lastet ned, kontrollert, pakket ut og selvtestet "
                f"({time.monotonic() - t0:.0f} s)")
            _ok(not (app / "config.json").exists() and not (app / "siard_manager.log").exists(),
                "selvtestens filer er ryddet bort fra den nye versjonen")
        else:
            try:
                SU.prepare_package(rel, tmp / "work")
                _ok(False, "pakke uten _internal/ skulle vært avvist")
            except RuntimeError as exc:
                _ok("mangler" in str(exc), f"pakke uten programfiler avvises ({exc})")
        (web / "SHA256SUMS.txt").write_text(f"{'0' * 64}  {pkg}\n", "utf-8")
        try:
            SU.prepare_package(rel, tmp / "work2")
            _ok(False, "feil sjekksum skulle vært avvist")
        except RuntimeError as exc:
            _ok("sjekksummen stemmer ikke" in str(exc), "skadet nedlasting (feil sjekksum) avvises")
    finally:
        srv.shutdown()


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="oppdatering_") as td:
        tmp = Path(td)
        test_assets()
        test_swap(tmp)
        test_prepare(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
