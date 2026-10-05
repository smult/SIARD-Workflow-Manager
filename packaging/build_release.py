"""packaging/build_release.py

Bygger en ferdig programpakke av SIARD Workflow Manager for Windows som kan
kjøres på maskiner UTEN nett og UTEN Python:

    dist/SIARDManager-<versjon>-win64[-komplett].zip
      SIARDManager/
        SIARDManager.exe            programmet (Python + alle pakker innebygd)
        _internal/                  Python, biblioteker, ressursfiler
        komponenter/                (valgfritt) installasjonsfiler for LibreOffice,
                                    Ghostscript og Siegfried — tilbys ved oppstart
        LES MEG.txt, README.md, LICENCE

Bruk (fra programmappa, på en maskin med nett):
    python packaging/build_release.py                  # med alle komponenter
    python packaging/build_release.py --komponenter ingen
    python packaging/build_release.py --komponenter gs,sf
    python packaging/build_release.py --lo-versjon 26.2.6

Byggemiljøet er et eget virtuelt miljø (build/venv) med requirements.txt +
valgfrie tillegg + PyInstaller, så pakken ikke avhenger av hva som tilfeldigvis
er installert på byggemaskinen. Nedlastede komponenter mellomlagres i
build/komponenter-cache. Pakken selvtestes (SIARDManager.exe --selftest) før
den zippes.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"
DIST = ROOT / "dist"
sys.path.insert(0, str(ROOT))

# Valgfrie tillegg som skal med i pakken (feiler én, bygges pakken uten den)
EXTRAS = ["py7zr>=0.20", "rarfile>=4.0", "python-magic-bin>=0.4.14"]
KIND_ALIASES = {"lo": "libreoffice", "libreoffice": "libreoffice",
                "gs": "ghostscript", "ghostscript": "ghostscript",
                "sf": "siegfried", "siegfried": "siegfried"}

LES_MEG = """SIARD Workflow Manager {versjon}
==============================================

Installasjon (ingen nett eller administratorrettigheter nødvendig)
  1. Pakk ut hele zip-filen til en mappe du har skrivetilgang til,
     f.eks. C:\\SIARDManager eller en mappe på D:. Bruk en kort sti — svært
     dype mapper kan gi feil ved utpakking (Windows' grense på 260 tegn).
  2. Start SIARDManager.exe.

Første oppstart
  Programmet sjekker selv hva som finnes på maskinen. Mangler LibreOffice,
  Ghostscript eller Siegfried, og installasjonsfilen ligger i komponenter/,
  tilbys installasjon rett fra pakken (installeres i
  %LOCALAPPDATA%\\SIARDManager). Oversikten finnes også under
  Innstillinger → «Programvare og avhengigheter …».
{komponenter}
Innstillinger
  config.json, dias.json og siard_manager.log lagres i samme mappe som
  SIARDManager.exe. Ved oppgradering: pakk ut ny versjon og kopier
  config.json og dias.json over fra den gamle mappa.

Kontroll av pakken
  SIARDManager.exe --selftest resultat.json   (returkode 0 = alt OK)
"""


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd: list, **kw) -> None:
    log("  $ " + " ".join(str(c) for c in cmd))
    subprocess.run([str(c) for c in cmd], check=True, **kw)


def venv_python(use_current: bool) -> Path:
    if use_current:
        return Path(sys.executable)
    venv = BUILD / "venv"
    py = venv / "Scripts" / "python.exe"
    if not py.exists():
        log(f"Lager byggemiljø {venv}")
        run([sys.executable, "-m", "venv", venv])
    run([py, "-m", "pip", "install", "--disable-pip-version-check", "-q", "-U", "pip",
         "pyinstaller==6.19.0"])
    # constraints.txt låser versjonene programmet er testet med (reproduserbart bygg)
    cons = ["-c", ROOT / "packaging" / "constraints.txt"]
    run([py, "-m", "pip", "install", "--disable-pip-version-check", "-q",
         "-r", ROOT / "requirements.txt", *cons])
    for extra in EXTRAS:
        try:
            run([py, "-m", "pip", "install", "--disable-pip-version-check", "-q", extra, *cons])
        except subprocess.CalledProcessError:
            log(f"  ADVARSEL: {extra} kunne ikke installeres — pakken bygges uten")
    return py


def build_exe(py: Path) -> Path:
    log("Bygger SIARDManager.exe med PyInstaller")
    out = BUILD / "dist"
    run([py, "-m", "PyInstaller", "--noconfirm", "--clean",
         "--distpath", out, "--workpath", BUILD / "work",
         ROOT / "packaging" / "siard_manager.spec"], cwd=ROOT)
    app = out / "SIARDManager"
    if not (app / "SIARDManager.exe").exists():
        raise SystemExit("PyInstaller laget ikke SIARDManager.exe")
    return app


def fetch_components(kinds: list[str], lo_version: str) -> list[Path]:
    from siard_workflow.core import components as C
    cache = BUILD / "komponenter-cache"
    files: list[Path] = []
    for kind in kinds:
        have = C.bundled(kind, cache)
        if have and (kind != "libreoffice" or not lo_version or lo_version in have.name):
            log(f"{C.LABELS[kind]}: bruker mellomlagret {have.name}")
            f = have
        else:
            log(f"{C.LABELS[kind]}: laster ned")
            f = C.download(kind, cache, lambda m: log("  " + m), lo_version=lo_version)
        files.append(f)
    return files


def selftest(app: Path) -> dict:
    log("Selvtest av pakken")
    out = BUILD / "selftest.json"
    out.unlink(missing_ok=True)
    r = subprocess.run([app / "SIARDManager.exe", "--selftest", out], timeout=600)
    if not out.exists():
        raise SystemExit(f"Selvtesten ga ikke resultat (returkode {r.returncode})")
    rep = json.loads(out.read_text("utf-8"))
    for c in rep["sjekker"]:
        log(f"  {'OK  ' if c['ok'] else 'FEIL'} {c['sjekk']}: {c['detalj'][:100]}")
    return rep


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--komponenter", default="alle",
                    help="alle | ingen | kommaliste av lo,gs,sf (standard: alle)")
    ap.add_argument("--lo-versjon", default="",
                    help="LibreOffice-versjon, f.eks. 26.2.6 (standard: siste modne)")
    ap.add_argument("--uten-venv", action="store_true",
                    help="bruk gjeldende Python i stedet for build/venv (raskere, mindre rent)")
    ap.add_argument("--ingen-zip", action="store_true", help="bare bygg mappa")
    args = ap.parse_args()
    if sys.platform != "win32":
        raise SystemExit("Pakken bygges på Windows.")

    from version import VERSION
    if args.komponenter.strip().lower() == "alle":
        kinds = ["libreoffice", "ghostscript", "siegfried"]
    elif args.komponenter.strip().lower() in ("", "ingen"):
        kinds = []
    else:
        kinds = []
        for k in args.komponenter.split(","):
            k = KIND_ALIASES.get(k.strip().lower())
            if not k:
                raise SystemExit(f"Ukjent komponent i --komponenter: {args.komponenter}")
            kinds.append(k)

    t0 = time.monotonic()
    log(f"SIARD Workflow Manager {VERSION} — komponenter: {', '.join(kinds) or 'ingen'}")
    py = venv_python(args.uten_venv)
    app = build_exe(py)

    # Komponenter + dokumentasjon
    comp_dir = app / "komponenter"
    shutil.rmtree(comp_dir, ignore_errors=True)
    comp_lines = ""
    if kinds:
        comp_dir.mkdir()
        from siard_workflow.core.identifiers.installer import SIG_DIRNAME
        for f in fetch_components(kinds, args.lo_versjon):
            shutil.copy2(f, comp_dir / f.name)
            sig = f.parent / SIG_DIRNAME
            if "siegfried" in f.name.lower() and sig.is_dir():
                shutil.copytree(sig, comp_dir / SIG_DIRNAME)
            comp_lines += f"    {f.name}\n"
        comp_lines = ("\nMedfølgende komponenter (komponenter/)\n" + comp_lines +
                      "  Mappa kan slettes etter installasjon for å spare plass.\n")
    (app / "LES MEG.txt").write_text(
        LES_MEG.format(versjon=VERSION, komponenter=comp_lines), "utf-8-sig")
    for f in ("README.md", "LICENCE"):
        shutil.copy2(ROOT / f, app / f)

    rep = selftest(app)
    # Filer selvtesten skrev i programmappa skal ikke med i pakken
    for f in ("siard_manager.log", "config.json", "dias.json", "kildesystem_cache.json"):
        (app / f).unlink(missing_ok=True)
    if not rep.get("ok"):
        raise SystemExit("Selvtesten feilet — pakken er ikke komplett (se over)")

    if args.ingen_zip:
        log(f"Ferdig: {app}  ({time.monotonic() - t0:.0f} s)")
        return 0
    DIST.mkdir(exist_ok=True)
    suffix = "-komplett" if set(kinds) == {"libreoffice", "ghostscript", "siegfried"} else \
        ("-" + "-".join(k[:2] for k in kinds) if kinds else "")
    zpath = DIST / f"SIARDManager-{VERSION}-win64{suffix}.zip"
    log(f"Pakker {zpath.name}")
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for p in sorted(app.rglob("*")):
            if p.is_file():
                z.write(p, Path("SIARDManager") / p.relative_to(app))
    log(f"Ferdig: {zpath} ({zpath.stat().st_size / 1e6:.0f} MB, "
        f"{time.monotonic() - t0:.0f} s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
