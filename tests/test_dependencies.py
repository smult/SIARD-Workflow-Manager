"""
Tester for avhengighetssjekken (dependencies.py + gui/dependencies_dialog.py):
register i synk med requirements.txt, deteksjon av manglende pakker, hvilke
operasjoner som rammes, pip-kommando, og dialogen i alle tre moduser.

Kjør:  python -X utf8 tests/test_dependencies.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import dependencies as D   # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _st(key, level, ok, *, kind="pakke", in_use=True, ops=(), blocking=(), action="pip",
        spec=""):
    return D.DepStatus(key, kind, key, level, ok, in_use=in_use, detail="x", affects=f"{key}-funksjon",
                       ops=ops, blocking_ops=blocking, action=action, pip_spec=spec or f"{key}>=1")


class _Op:
    def __init__(self, oid, label=None, **params):
        self.operation_id, self.label, self.params = oid, label or oid, params


def test_registry_matches_requirements() -> None:
    print("test_registry_matches_requirements")
    req = (_ROOT / "requirements.txt").read_text("utf-8").lower()
    active = [ln.split("#")[0].strip() for ln in req.splitlines() if ln.strip() and not ln.startswith("#")]
    names = {ln.split(">")[0].split("=")[0].strip() for ln in active}
    for p in D.PY_PACKAGES:
        base = p.pip_spec.split(">")[0].strip().lower()
        if p.level in (D.REQUIRED, D.STANDARD):
            _ok(base in names, f"{p.label} ({p.level}) står i requirements.txt")
        else:
            _ok(base not in names and base in req, f"{p.label} (valgfri) nevnt som tillegg, ikke påkrevd")
    _ok(len({p.key for p in D.PY_PACKAGES}) == len(D.PY_PACKAGES), "unike nøkler")


def test_missing_detection() -> None:
    print("test_missing_detection")
    import importlib.util as iu
    orig = iu.find_spec
    iu.find_spec = lambda name, *a, **k: None if name == "lxml" else orig(name, *a, **k)
    try:
        miss = [p.key for p in D.missing_packages((D.STANDARD,))]
    finally:
        iu.find_spec = orig
    _ok("lxml" in miss and "reportlab" not in miss, f"manglende lxml oppdages ({miss})")
    _ok(D.missing_packages((D.REQUIRED,)) == [], "påkrevde pakker finnes i testmiljøet")


def test_impacts() -> None:
    print("test_impacts")
    statuses = [
        _st("reportlab", D.STANDARD, False, ops=("workflow_report", "metadata_extract"),
            blocking=("workflow_report",)),
        _st("ghostscript", D.OPTIONAL, False, kind="program", ops=("blob_convert",), action="component"),
        _st("py7zr", D.OPTIONAL, False, ops=("blob_convert",)),
        _st("siegfried", D.OPTIONAL, False, kind="program", in_use=False, ops=("blob_convert",)),
        _st("lxml", D.STANDARD, True, ops=("siardmapper",), blocking=("siardmapper",)),
        _st("libreoffice", D.STANDARD, False, kind="program", ops=("blob_convert",),
            blocking=("blob_convert",)),
    ]
    ops = [_Op("workflow_report", "Kjørerapport"), _Op("blob_convert"), _Op("siardmapper"),
           _Op("metadata_extract", generate_pdf=False)]
    imp = D.impacts_for_ops(ops, statuses, skip_keys=("libreoffice",))
    got = sorted((i.op_id, i.dep.key, i.blocking) for i in imp)
    _ok(got == [("blob_convert", "ghostscript", False), ("blob_convert", "py7zr", False),
                ("workflow_report", "reportlab", True)],
        f"riktige konsekvenser per operasjon ({got})")
    imp2 = D.impacts_for_ops([_Op("metadata_extract", generate_pdf=True)], statuses)
    _ok([i.dep.key for i in imp2] == ["reportlab"] and not imp2[0].blocking,
        "Metadata-uttrekk med PDF: ReportLab mangler → redusert (ikke blokkerende)")
    _ok(not any(i.dep.key == "siegfried" for i in imp), "avslått Siegfried regnes ikke som mangel")
    _ok(not any(i.dep.key == "libreoffice" for i in imp), "LibreOffice hoppes over (eget preflight-steg)")


def test_startup_problems_and_signature() -> None:
    print("test_startup_problems_and_signature")
    s = [_st("lxml", D.STANDARD, False), _st("py7zr", D.OPTIONAL, False),
         _st("ghostscript", D.OPTIONAL, False, kind="program"),
         _st("siegfried", D.OPTIONAL, False, kind="program", in_use=False),
         _st("reportlab", D.STANDARD, True)]
    keys = [x.key for x in D.startup_problems(s)]
    _ok(keys == ["lxml", "ghostscript"],
        f"oppstart: standard-mangler + påslått Ghostscript, ikke tillegg/avslått ({keys})")
    sig1 = D.signature(D.startup_problems(s))
    sig2 = D.signature(list(reversed(D.startup_problems(s))))
    _ok(sig1 == sig2 and sig1, "signatur uavhengig av rekkefølge")
    s[0].ok = True
    _ok(D.signature(D.startup_problems(s)) != sig1, "signatur endres når settet av mangler endres")
    _ok(D.signature([]) == "", "tom signatur når ingenting mangler")


def test_check_all_only_ops() -> None:
    print("test_check_all_only_ops")
    import time
    t0 = time.monotonic()
    st = D.check_all(only_ops={"sha256"})
    _ok(all(s.kind == "pakke" for s in st) and time.monotonic() - t0 < 3,
        "kun pakker når ingen operasjon bruker eksterne programmer")
    keys = {s.key for s in D.check_all(only_ops={"anonymize"}) if s.kind == "program"}
    _ok(keys == {"ollama"}, f"anonymisering: bare Ollama sjekkes ({keys})")


def test_pip_command() -> None:
    print("test_pip_command")
    import subprocess
    seen: dict = {}

    class R:
        returncode, stdout, stderr = 0, "", ""
    orig_run, orig_exe = subprocess.run, sys.executable
    subprocess.run = lambda cmd, **k: (seen.update(cmd=cmd, kw=k), R())[1]
    sys.executable = str(Path(orig_exe).with_name("pythonw.exe"))
    try:
        ok, _m = D.pip_install(["lxml>=4.9.0"])
    finally:
        subprocess.run, sys.executable = orig_run, orig_exe
    cmd = seen["cmd"]
    _ok(ok and cmd[1:4] == ["-m", "pip", "install"] and "lxml>=4.9.0" in cmd, f"pip-kommando ({cmd[1:]})")
    if Path(orig_exe).with_name("python.exe").exists():
        _ok(Path(cmd[0]).name.lower() == "python.exe", "pythonw erstattes med python.exe for pip")
    if sys.platform == "win32":
        _ok(seen["kw"].get("creationflags", 0) & subprocess.CREATE_NO_WINDOW, "pip uten konsollvindu")
    _ok('"lxml>=4.9.0"' in D.manual_pip_command(["lxml>=4.9.0"]), "manuell kommando siterer versjonskrav")


def test_real_statuses() -> None:
    print("test_real_statuses")
    st = D.check_all()
    keys = {s.key for s in st}
    _ok({"libreoffice", "ghostscript", "siegfried", "antivirus", "ollama"} <= keys,
        "alle eksterne programmer sjekkes")
    _ok(all(s.detail for s in st), "hver komponent har status-/detaljtekst")
    for s in st:
        print(f"      {'OK ' if s.ok else ('-- ' if not s.in_use else 'MANGLER')} "
              f"{s.label}: {s.detail[:70]}")


def test_gui() -> None:
    print("test_gui")
    import os, threading
    threading.Timer(90, lambda: os._exit(3)).start()
    import settings
    store: dict = {}
    settings.set_config = lambda k, v: store.__setitem__(k, v)
    import gui.app as A
    A.set_config = settings.set_config
    orig_startup = A.App._startup_dependency_check
    A.App._startup_dependency_check = lambda self: None   # testes eksplisitt under
    from gui.dependencies_dialog import DependenciesDialog
    app = A.App(); app.after(1500, app.quit); app.mainloop(); app.update()
    try:
        statuses = [
            _st("lxml", D.STANDARD, False, ops=("siardmapper",), blocking=("siardmapper",)),
            _st("py7zr", D.OPTIONAL, False, ops=("blob_convert",)),
            _st("libreoffice", D.STANDARD, False, kind="program", action="component",
                ops=("blob_convert",)),
            _st("reportlab", D.STANDARD, True),
        ]
        statuses[2].url = "https://example.invalid"
        statuses[2].component = "libreoffice"

        def texts(w):
            out = []
            for c in w.winfo_children():
                try:
                    out.append(str(c.cget("text")))
                except Exception:
                    pass
                out += texts(c)
            return out

        dlg = DependenciesDialog(app, statuses, mode="startup"); app.update()
        t = " ".join(texts(dlg))
        _ok("lxml" in t and "libreoffice" in t and "py7zr" not in t,
            "oppstart: viser standard-mangler, ikke valgfrie tillegg")
        _ok("Pek ut …" in t and "Last ned og installer" in t and "Installer" in t,
            "handlinger: Installer (pip), Last ned og installer / Pek ut (LibreOffice)")
        statuses[2].bundled = r"C:\pakke\komponenter\LibreOffice_26.2.6_Win_x86-64.msi"
        dlg2 = DependenciesDialog(app, statuses, mode="startup"); app.update()
        t2 = " ".join(texts(dlg2))
        _ok("Installer fra pakken" in t2 and "Last ned og installer" not in t2,
            "medfølgende installasjonsfil → «Installer fra pakken» (offline)")
        dlg2._lukk(); app.update()
        statuses[2].bundled = ""
        _ok(str(dlg._pip_all_btn.cget("state")) == "normal", "«Installer alle» aktiv når pip-pakker mangler")
        dlg._dont_var.set(True); dlg._lukk(); app.update()
        _ok(dlg.dont_show_again, "«ikke vis igjen» registreres")

        dlg = DependenciesDialog(app, statuses, mode="overview"); app.update()
        t = " ".join(texts(dlg))
        _ok("py7zr" in t and "reportlab" in t and "Brukes til:" in t, "oversikt: alle komponenter med formål")
        dlg._lukk(); app.update()

        imp = D.impacts_for_ops([_Op("siardmapper", "SIARD Mapper"), _Op("blob_convert")],
                                statuses, skip_keys=("libreoffice",))
        dlg = DependenciesDialog(app, statuses, mode="preflight", impacts=imp); app.update()
        _ok(dlg._continue_btn.cget("text") == "Fortsett likevel", "preflight: blokkerende mangel → «Fortsett likevel»")
        fixed = [D.DepStatus(**{**s.__dict__}) for s in statuses]
        fixed[0].ok = True
        dlg._finish(None, fixed, lambda _r: None); app.update()
        _ok(dlg._continue_btn.cget("text") == "Fortsett", "etter installasjon: «Fortsett»")
        dlg._fortsett(); app.update()
        _ok(dlg.result == "fortsett", "resultat «fortsett»")

        # Preflight-steget: bare valgfrie mangler → ingen dialog, fortsetter
        orig = D.check_all
        D.check_all = lambda include_tools=True, only_ops=None: statuses[1:]
        try:
            app._pf_status = None
            r = app._dependency_preflight([_Op("blob_convert")])
            _ok(r is True and "valgfri" in (app._pf_status or ""),
                f"kun valgfrie mangler: fortsetter uten dialog ({app._pf_status})")
            D.check_all = lambda include_tools=True, only_ops=None: statuses
            app.after(600, lambda: [w._stopp() for w in app.winfo_children()
                                    if isinstance(w, DependenciesDialog)])
            r = app._dependency_preflight([_Op("siardmapper", "SIARD Mapper")])
            _ok(r is False and "lxml" in (app._pf_status or ""),
                f"blokkerende mangel + Stopp → kjøringen startes ikke ({app._pf_status})")
            r = app._dependency_preflight([_Op("sha256")])
            _ok(r is True and "alt" in app._pf_status, "ingen relevante mangler → OK")
        finally:
            D.check_all = orig

        # Oppstartssjekken: viser dialog, lagrer «ikke vis igjen»-signatur
        D.check_all = lambda include_tools=True, only_ops=None: statuses
        try:
            app.after(900, lambda: [(w._dont_var.set(True), w._lukk()) for w in app.winfo_children()
                                    if isinstance(w, DependenciesDialog)])
            orig_startup(app)
            import time
            t0 = time.monotonic()
            while "deps_ack_signature" not in store and time.monotonic() - t0 < 10:
                app.update(); time.sleep(0.05)
            _ok(store.get("deps_ack_signature") == D.signature(D.startup_problems(statuses)),
                "oppstart: dialog vist og «ikke vis igjen» lagret for dette settet")
        finally:
            D.check_all = orig
    finally:
        app.destroy()


def test_components(tmp: Path) -> None:
    print("test_components")
    from siard_workflow.core import components as C
    html = ('<a href="25.8.7/">25.8.7/</a><a href="26.2.5/">x</a><a href="26.2.6/">x</a>'
            '<a href="26.8.0/">x</a><a href="26.8.1/">x</a>')
    _ok(C.pick_lo_version(html) == "26.2.6", "LibreOffice: høyeste modne versjon (ikke x.y.0/x.y.1)")
    _ok(C.pick_lo_version('<a href="27.2.0/">') == "27.2.0", "reserve: høyeste når ingen er moden")
    d = tmp / "komponenter"; d.mkdir()
    for n in ("LibreOffice_26.2.5_Win_x86-64.msi", "LibreOffice_26.2.6_Win_x86-64.msi",
              "gs10080w64.exe", "siegfried_1-11-4_win64.zip", "annet.txt"):
        (d / n).write_bytes(b"x")
    _ok(C.bundled("libreoffice", d).name == "LibreOffice_26.2.6_Win_x86-64.msi", "nyeste MSI velges")
    _ok(C.bundled("ghostscript", d).name == "gs10080w64.exe", "Ghostscript-installasjonsfil funnet")
    _ok(C.bundled("siegfried", d).name == "siegfried_1-11-4_win64.zip", "Siegfried-arkiv funnet")
    _ok(C.bundled("libreoffice", tmp / "finnes_ikke") is None, "ingen komponentmappe → None")
    # Status markeres med medfølgende fil, og tilbys ved oppstart selv om valgfri/avslått
    orig = C.components_dir
    C.components_dir = lambda: d
    try:
        st = D._mark_bundled(D.DepStatus("siegfried", "program", "Siegfried", D.OPTIONAL, False,
                                         in_use=False, action="component", component="siegfried"))
    finally:
        C.components_dir = orig
    _ok(st.bundled.endswith(".zip") and "uten nett" in st.note, "status: følger med pakken")
    _ok([s.key for s in D.startup_problems([st])] == ["siegfried"],
        "medfølgende, ikke installert komponent tilbys ved oppstart")


def test_frozen_packages() -> None:
    print("test_frozen_packages")
    orig = D.FROZEN
    D.FROZEN = True
    try:
        st = D.package_statuses()
    finally:
        D.FROZEN = orig
    _ok(all(s.action == "" for s in st), "pakket program: ingen pip-installasjon tilbys")
    import app_paths
    _ok(app_paths.data_dir() == app_paths.install_dir() and not app_paths.FROZEN,
        "kildekode: brukerdata i programmappa som før")


def main() -> int:
    test_registry_matches_requirements()
    test_missing_detection()
    test_impacts()
    test_startup_problems_and_signature()
    test_check_all_only_ops()
    test_pip_command()
    test_real_statuses()
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        test_components(Path(td))
    test_frozen_packages()
    test_gui()
    print("\nAlle tester OK")
    import os
    os._exit(0)


if __name__ == "__main__":
    main()
