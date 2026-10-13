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
    ap.add_argument("--publiser", action="store_true",
                    help="last opp pakkene + SHA256SUMS.txt til GitHub-utgivelsen for "
                         "versjonen (gh CLI); finnes den ikke, lages et utkast")
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
    for f in ("siard_manager.log", "siard_manager_krasj.log", "config.json", "dias.json",
              "kildesystem_cache.json"):
        (app / f).unlink(missing_ok=True)
    if not rep.get("ok"):
        raise SystemExit("Selvtesten feilet — pakken er ikke komplett (se over)")

    if args.ingen_zip:
        log(f"Ferdig: {app}  ({time.monotonic() - t0:.0f} s)")
        return 0
    DIST.mkdir(exist_ok=True)
    zips: list[Path] = []
    # Programpakke uten komponenter — brukes av den automatiske oppdateringen
    zips.append(_zip_app(app, DIST / f"SIARDManager-{VERSION}-win64.zip",
                         exclude_top={"komponenter"}))
    if kinds:
        full = {"libreoffice", "ghostscript", "siegfried"}
        suffix = "-komplett" if set(kinds) == full else "-" + "-".join(k[:2] for k in kinds)
        zips.append(_zip_app(app, DIST / f"SIARDManager-{VERSION}-win64{suffix}.zip"))
    sums = DIST / "SHA256SUMS.txt"
    sums.write_text("".join(f"{_sha256(z)}  {z.name}\n" for z in zips), "utf-8")
    for z in zips:
        log(f"Ferdig: {z} ({z.stat().st_size / 1e6:.0f} MB)")
    log(f"Sjekksummer: {sums}  ({time.monotonic() - t0:.0f} s)")
    if args.publiser:
        publish(VERSION, zips + [sums])
    return 0


def _zip_app(app: Path, zpath: Path, exclude_top: "set | None" = None) -> Path:
    log(f"Pakker {zpath.name}")
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for p in sorted(app.rglob("*")):
            rel = p.relative_to(app)
            if p.is_file() and not (exclude_top and rel.parts[0] in exclude_top):
                z.write(p, Path("SIARDManager") / rel)
    return zpath


def _sha256(p: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def publish(version: str, files: "list[Path]") -> None:
    """Last opp til GitHub-utgivelsen `version` (gh CLI). Finnes den ikke, lages
    et UTKAST — programmene ser det ikke før det publiseres på GitHub."""
    gh = shutil.which("gh")
    if not gh:
        raise SystemExit("gh (GitHub CLI) finnes ikke — installer og kjør «gh auth login»")
    exists = subprocess.run([gh, "release", "view", version], capture_output=True,
                            cwd=ROOT).returncode == 0
    if not exists:
        log(f"Utgivelse {version} finnes ikke — lager utkast")
        run([gh, "release", "create", version, "--draft",
             "--title", f"SIARD Workflow Manager v{version}",
             "--notes", "Programpakke for Windows. Pakk ut hele zip-fila og start "
                        "SIARDManager.exe (se LES MEG.txt)."], cwd=ROOT)
    log(f"Laster opp {len(files)} fil(er) til utgivelse {version} …")
    run([gh, "release", "upload", version, "--clobber", *map(str, files)], cwd=ROOT)
    if not exists:
        log(f"Utkastet må publiseres på GitHub før programmene ser det: "
            f"gh release edit {version} --draft=false")
    log("Publisert.")


if __name__ == "__main__":
    sys.exit(main())
