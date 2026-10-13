"""siard_workflow/core/self_update.py

Automatisk oppdatering av SIARD Workflow Manager fra GitHub-utgivelser.

Programpakke (SIARDManager.exe)
  1. Finn utgivelsens programpakke `SIARDManager-<ver>-win64.zip` (uten
     komponenter) og `SHA256SUMS.txt` (lastes opp av packaging/build_release.py
     --publiser).
  2. Last ned, kontroller SHA-256, pakk ut til en temp-mappe og kjør den nye
     `SIARDManager.exe --selftest` — feiler noe, røres ikke installasjonen.
  3. Etter brukerens bekreftelse: start et PowerShell-hjelpeskript og avslutt
     programmet. Skriptet venter til programmet er lukket, flytter gamle
     programfiler til side (*.old), kopierer inn de nye, starter den nye
     versjonen og rydder. Feiler kopieringen, legges de gamle filene tilbake.
     Innstillinger (config.json, dias.json), logger og komponenter/ beholdes.

Kildekode (start.bat / python main.py)
  Er programmappa en git-kopi uten lokale endringer: `git pull --ff-only`.
  Ellers: åpne nedlastingssiden (som før).

Maskiner uten nett: uendret — ny zip pakkes ut manuelt.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Optional

ProgressCb = Optional[Callable[[int, int], None]]

APP_EXE = "SIARDManager.exe"
SUMS_NAME = "SHA256SUMS.txt"
# Filer/mapper i programmappa som tilhører brukeren og ALDRI byttes ut
KEEP = ("config.json", "dias.json", "siard_manager.log", "siard_manager_krasj.log",
        "kildesystem_cache.json", "komponenter", "oppdatering.log")


def package_asset_name(version: str) -> str:
    return f"SIARDManager-{version.lstrip('vV')}-win64.zip"


def find_assets(release: dict) -> "tuple[dict | None, dict | None]":
    """(programpakke, sjekksumfil) fra en GitHub-utgivelse, eller None."""
    ver = str(release.get("tag_name", "")).lstrip("vV")
    assets = release.get("assets") or []
    pkg = next((a for a in assets if a.get("name") == package_asset_name(ver)), None)
    sums = next((a for a in assets if a.get("name") == SUMS_NAME), None)
    return pkg, sums


def install_dir() -> Path:
    try:
        from app_paths import install_dir as _i
        return _i()
    except Exception:
        return Path(sys.executable).resolve().parent


def _frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _writable(d: Path) -> bool:
    try:
        probe = d / ".oppdateringstest"
        probe.write_text("x", "utf-8")
        probe.unlink()
        return True
    except Exception:
        return False


def mode(release: "dict | None" = None) -> "tuple[str, str]":
    """
    Hvordan denne installasjonen kan oppdateres:
      ("pakke", "")   programpakken kan lastes ned og installeres
      ("git", "")     kildekode i en ren git-kopi: git pull
      ("manuell", årsak)
    """
    d = install_dir()
    if _frozen():
        if sys.platform != "win32":
            return "manuell", "automatisk installasjon støttes bare på Windows"
        if release is not None and find_assets(release)[0] is None:
            return "manuell", "utgivelsen har ingen programpakke vedlagt"
        if not _writable(d):
            return "manuell", f"programmappa er skrivebeskyttet ({d})"
        return "pakke", ""
    if (d / ".git").exists() and shutil.which("git"):
        try:
            r = subprocess.run(["git", "-C", str(d), "status", "--porcelain",
                                "--untracked-files=no"], capture_output=True, text=True,
                               timeout=30, **_hidden())
            if r.returncode == 0 and not r.stdout.strip():
                return "git", ""
            return "manuell", "git-kopien har lokale endringer"
        except Exception as exc:
            return "manuell", f"git kunne ikke kjøres ({exc})"
    return "manuell", "kildekodeinstallasjon uten git"


def _hidden() -> dict:
    try:
        from siard_workflow.core.subproc import hidden_kwargs
        return hidden_kwargs()
    except Exception:
        return {}


# ── Nedlasting og kontroll ───────────────────────────────────────────────────

def download(url: str, dest: Path, progress: ProgressCb = None,
             stop: "Callable[[], bool] | None" = None) -> Path:
    req = urllib.request.Request(url, headers={"User-Agent": "SIARD-Workflow-Manager-updater/2",
                                               "Accept": "application/octet-stream"})
    part = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(req, timeout=60) as r, open(part, "wb") as f:
        total = int(r.headers.get("Content-Length") or 0)
        done = 0
        while True:
            if stop and stop():
                raise RuntimeError("avbrutt")
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
            done += len(chunk)
            if progress:
                progress(done, total)
    part.replace(dest)
    return dest


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def expected_sha(sums_text: str, name: str) -> str:
    for line in sums_text.splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and parts[-1].lstrip("*") == name:
            return parts[0].lower()
    return ""


def stage(zip_path: Path, work: Path, selftest_timeout: int = 300) -> Path:
    """
    Pakk ut programpakken og kjør den nye versjonens selvtest. Returnerer
    mappa med SIARDManager.exe. Kaster RuntimeError ved feil.
    """
    out = work / "ny"
    shutil.rmtree(out, ignore_errors=True)
    with zipfile.ZipFile(zip_path) as z:
        for m in z.namelist():
            if m.startswith("/") or ".." in Path(m).parts:
                raise RuntimeError(f"ugyldig sti i pakken: {m}")
        z.extractall(out)
    app = next((p.parent for p in out.rglob(APP_EXE)), None)
    if app is None or not (app / "_internal").is_dir():
        raise RuntimeError(f"pakken mangler {APP_EXE} eller _internal/")
    if sys.platform == "win32":
        res = work / "selvtest.json"
        r = subprocess.run([str(app / APP_EXE), "--selftest", str(res)],
                           timeout=selftest_timeout, capture_output=True, **_hidden())
        try:
            ok = json.loads(res.read_text("utf-8")).get("ok")
        except Exception:
            ok = False
        if r.returncode != 0 or not ok:
            raise RuntimeError("den nye versjonens selvtest feilet — installasjonen avbrutt")
    for f in ("siard_manager.log", "siard_manager_krasj.log", "config.json"):
        (app / f).unlink(missing_ok=True)            # fra selvtesten
    return app


# ── Bytte av programfiler (etter at programmet er lukket) ────────────────────

def swap_script(new_app: Path, target: Path, pid: int, log: Path,
                relaunch: bool = True, cleanup: "Path | None" = None) -> str:
    """PowerShell-skript som bytter programfilene når prosessen `pid` er lukket."""
    def q(p) -> str:
        return "'" + str(p).replace("'", "''") + "'"
    keep = ", ".join(q(k) for k in KEEP)
    return f"""$ErrorActionPreference = 'Stop'
$new = {q(new_app)}; $dst = {q(target)}; $log = {q(log)}
$keep = @({keep})
function L($m) {{ Add-Content -LiteralPath $log -Value ("[" + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + "] " + $m) -Encoding UTF8 }}
L "Venter på at programmet (pid {pid}) lukkes ..."
try {{ Wait-Process -Id {pid} -Timeout 120 -ErrorAction SilentlyContinue }} catch {{}}
Start-Sleep -Milliseconds 800
$moved = @()
try {{
    foreach ($item in Get-ChildItem -LiteralPath $new -Force) {{
        if ($keep -contains $item.Name) {{ continue }}
        $t = Join-Path $dst $item.Name
        if (Test-Path -LiteralPath $t) {{
            $bak = "$t.old"
            if (Test-Path -LiteralPath $bak) {{ Remove-Item -LiteralPath $bak -Recurse -Force }}
            Move-Item -LiteralPath $t -Destination $bak
            $moved += ,@($t, $bak)
        }} else {{ $moved += ,@($t, $null) }}
        Copy-Item -LiteralPath $item.FullName -Destination $t -Recurse -Force
    }}
    foreach ($m in $moved) {{ if ($m[1]) {{ Remove-Item -LiteralPath $m[1] -Recurse -Force -ErrorAction SilentlyContinue }} }}
    L "Oppdatering installert."
}} catch {{
    L ("FEIL: " + $_.Exception.Message + " — gjenoppretter forrige versjon")
    foreach ($m in $moved) {{
        if (Test-Path -LiteralPath $m[0]) {{ Remove-Item -LiteralPath $m[0] -Recurse -Force -ErrorAction SilentlyContinue }}
        if ($m[1]) {{ Move-Item -LiteralPath $m[1] -Destination $m[0] -ErrorAction SilentlyContinue }}
    }}
}}
{"Start-Process -FilePath (Join-Path $dst '" + APP_EXE + "') -WorkingDirectory $dst" if relaunch else "# (ingen omstart)"}
{("Remove-Item -LiteralPath " + q(cleanup) + " -Recurse -Force -ErrorAction SilentlyContinue") if cleanup else ""}
"""


def launch_swap(new_app: Path, relaunch: bool = True) -> Path:
    """
    Skriv og start hjelpeskriptet (frakoblet, uten vindu). Programmet MÅ
    avslutte seg selv rett etterpå. Returnerer loggfila.
    """
    target = install_dir()
    try:
        from app_paths import data_dir
        log = data_dir() / "oppdatering.log"
    except Exception:
        log = target / "oppdatering.log"
    script = Path(tempfile.mkdtemp(prefix="siard_oppdatering_")) / "bytt.ps1"
    # UTF-8 med BOM: Windows PowerShell 5.1 leser ellers æøå i stier feil
    # Arbeidsmappa (nedlasting/utpakking) ryddes av skriptet etter byttet
    work = new_app.parent.parent if new_app.parent.name == "ny" else new_app.parent
    script.write_text(swap_script(new_app, target, os.getpid(), log, relaunch, work),
                      "utf-8-sig")
    flags = 0
    if sys.platform == "win32":
        flags = (getattr(subprocess, "CREATE_NO_WINDOW", 0)
                 | getattr(subprocess, "DETACHED_PROCESS", 0)
                 | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
    subprocess.Popen(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                      "-WindowStyle", "Hidden", "-File", str(script)],
                     creationflags=flags, close_fds=True)
    return log


def git_pull() -> "tuple[bool, str]":
    d = install_dir()
    r = subprocess.run(["git", "-C", str(d), "pull", "--ff-only"], capture_output=True,
                       text=True, timeout=300, **_hidden())
    msg = (r.stdout + r.stderr).strip()
    return r.returncode == 0, msg[-500:]


def prepare_package(release: dict, work: Path, progress: ProgressCb = None,
                    stop: "Callable[[], bool] | None" = None,
                    status: "Callable[[str], None] | None" = None) -> Path:
    """Last ned, kontroller og test programpakken. Returnerer klar programmappe."""
    def say(m):
        if status:
            status(m)
    pkg, sums = find_assets(release)
    if pkg is None:
        raise RuntimeError("utgivelsen har ingen programpakke vedlagt")
    work.mkdir(parents=True, exist_ok=True)
    say(f"Laster ned {pkg['name']} …")
    zpath = download(pkg["browser_download_url"], work / pkg["name"], progress, stop)
    if sums is not None:
        say("Kontrollerer sjekksum …")
        stext = download(sums["browser_download_url"], work / SUMS_NAME).read_text("utf-8")
        exp = expected_sha(stext, pkg["name"])
        if not exp:
            raise RuntimeError(f"{SUMS_NAME} mangler sjekksum for {pkg['name']}")
        if sha256(zpath) != exp:
            raise RuntimeError("sjekksummen stemmer ikke — nedlastingen er skadet")
    else:
        raise RuntimeError(f"utgivelsen mangler {SUMS_NAME} — kan ikke kontrollere pakken")
    say("Pakker ut og tester den nye versjonen …")
    app = stage(zpath, work)
    zpath.unlink(missing_ok=True)
    return app
