"""
Lukking av programmet under en kjøring, garantert avslutning av prosessen,
pause/stopp i kjøreloggen og livstegnet, og Stopp-knappen.

Bakgrunn (2026-11-28): hovedvinduet ble lukket mens en kjøring sto på pause.
Prosessen levde videre uten vindu i 20 timer (arbeidstrådene ventet på
«▶ Fortsett», Python ventet på arbeidstrådene) og sendte livstegn hver
halvtime — og kjøreloggen sa «Fortsatt i gang». I tillegg forsvant
«⏹ Stopp» etter et ferdig BLOB-steg, så kjøringen ikke kunne stoppes.

Bare syntetiske data.  Kjør:  python -X utf8 tests/test_close_and_stop.py
"""
from __future__ import annotations

import queue
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from pathlib import Path
from types import SimpleNamespace

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


# ── Livstegnet: pause og stoppet ─────────────────────────────────────────────

def test_heartbeat_hold(tmp: Path) -> None:
    print("Livstegn i kjøreloggen")
    from siard_workflow.core.file_logger import WorkflowFileLogger
    with WorkflowFileLogger(tmp, "hold", heartbeat_s=0) as fl:
        fl.set_step("[8] BLOB Konverter til PDF/A")
        fl.log("  Konvertering feilet: record1.bin", "warn")
        _ok(fl.heartbeat().startswith("LIVSTEGN: Fortsatt i gang — totalt"),
            "uten pause: «Fortsatt i gang»")
        fl.set_hold("PÅ PAUSE (venter på «▶ Fortsett» i programmet)")
        hb = fl.heartbeat()
        _ok(hb.startswith("LIVSTEGN: PÅ PAUSE (venter på «▶ Fortsett» i programmet) siden ")
            and "Fortsatt i gang" not in hb and "; totalt " in hb
            and "steg «[8] BLOB Konverter til PDF/A»" in hb,
            f"på pause: livstegnet sier det ({hb[:70]}…)")
        fl.set_hold(None)
        _ok("Fortsatt i gang" in fl.heartbeat(), "pausen opphevet: «Fortsatt i gang» igjen")
        path = fl.log_path
    text = path.read_text(encoding="utf-8")
    _ok(text.count("LIVSTEGN: PÅ PAUSE") == 1, "pauselivstegnet står i loggfila")


def test_reporter_hold() -> None:
    print("Mobilvarsling")
    from siard_workflow.core.remote_notify import RunReporter

    sent: list = []
    hub = SimpleNamespace(
        notify=lambda title, text="", **kw: sent.append((title, text, kw)),
        set_status_provider=lambda fn: None, open_question=lambda: None)
    rep = RunReporter(hub, heartbeat=True)
    rep.file_start(0, 1, "Syntetisk.siard", 3)
    rep.step_start(1, 3, "BLOB Konverter til PDF/A")
    sent.clear()
    rep.heartbeat("LIVSTEGN: Fortsatt i gang")
    _ok(sent[-1][0] == "Livstegn: Syntetisk.siard", "vanlig livstegn: tittel «Livstegn»")
    rep.set_hold("På pause")
    rep.heartbeat("LIVSTEGN: PÅ PAUSE siden 28.11 13:11")
    _ok(sent[-1][0] == "På pause: Syntetisk.siard" and sent[-1][2]["priority"] == 1,
        "på pause: tittelen sier «På pause» (samme stille prioritet)")
    _ok("PÅ PAUSE siden " in rep.status_text(), "«Status nå» viser pausen")
    rep.set_hold(None)
    _ok("PÅ PAUSE" not in rep.status_text(), "pausen borte fra «Status nå» etter «Fortsett»")
    rep.closed(forced=False)
    _ok(sent[-1][0].startswith("Programmet er avsluttet") and "Prosjektet er lagret" in sent[-1][1],
        "varsel når programmet lukkes under kjøring")
    rep.closed(forced=True)
    _ok("rakk ikke å stoppe" in sent[-1][1], "… og når steget ikke rakk å stoppe")


# ── App: pause/stopp i kjøreloggen, venting som slipper ved lukking ──────────

class _Panel:
    def __init__(self):
        self.calls: list = []

    def __getattr__(self, name):
        return lambda *a, **k: self.calls.append((name, a))


def _fake_app(tmp: Path, with_log: bool = True):
    import gui.app as A
    from siard_workflow.core.file_logger import WorkflowFileLogger
    app = A.App.__new__(A.App)
    app._log_queue = queue.Queue()
    app._stop_event = threading.Event()
    app._pause_event = threading.Event()
    app._step_resume_event = threading.Event()
    app._abort_requested = False
    app._step_stopped = False
    app._closing = False
    app._run_thread = None
    app._remote_hub = None
    app._remote_rep = None
    app.progress_panel = _Panel()
    app.gui_log = []
    app._log = lambda msg, lvl="info": app.gui_log.append((lvl, msg))
    app._conv_ctx = None
    if with_log:
        fl = WorkflowFileLogger(tmp, f"app{len(list(tmp.glob('app*')))}", heartbeat_s=0)
        fl.__enter__()
        app._conv_ctx = SimpleNamespace(metadata={"file_logger": fl})
    return app


def _log_text(app) -> str:
    return app._conv_ctx.metadata["file_logger"].log_path.read_text(encoding="utf-8")


def test_pause_stop_logged(tmp: Path) -> None:
    print("Pause og stopp i kjøreloggen")
    app = _fake_app(tmp)
    fl = app._conv_ctx.metadata["file_logger"]
    app._conv_pause()
    _ok(app._pause_event.is_set() and "Satt på pause av operatøren" in _log_text(app),
        "«⏸ Pause» står i kjøreloggen")
    _ok(fl.heartbeat().startswith("LIVSTEGN: PÅ PAUSE"), "livstegnet vet om pausen")
    _ok(fl.n_warnings == 0, "operatørens valg teller ikke som advarsel i sammendraget")
    app._conv_pause()
    _ok(not app._pause_event.is_set() and "Pausen er opphevet" in _log_text(app)
        and "Fortsatt i gang" in fl.heartbeat(), "«▶ Fortsett» står i kjøreloggen")
    app._conv_pause()
    app._conv_stop()
    _ok(app._stop_event.is_set() and not app._pause_event.is_set()
        and "Stopp valgt av operatøren" in _log_text(app)
        and "Fortsatt i gang" in fl.heartbeat(),
        "«⏹ Stopp» opphever pausen og står i kjøreloggen")
    _ok(any(l == "warn" and "Stopp valgt" in m for l, m in app.gui_log),
        "… og i loggpanelet")
    fl.__exit__(None, None, None)


def test_wait_after_stop(tmp: Path) -> None:
    print("Venting etter stopp")
    app = _fake_app(tmp)
    fl = app._conv_ctx.metadata["file_logger"]

    # 1) «▶ Fortsett»
    res: list = []
    t = threading.Thread(target=lambda: res.append(app._wait_after_stop(2, "Steg C")),
                         daemon=True)
    app._stop_event.set()
    t.start()
    time.sleep(0.3)
    _ok(fl.heartbeat().startswith("LIVSTEGN: STOPPET (venter på «▶ Fortsett» eller «⏹ Avslutt»"),
        "stoppet: livstegnet sier at kjøringen venter på operatøren")
    _ok("Kjøringen står stoppet" in _log_text(app), "… og det står i kjøreloggen")
    app._conv_resume()
    t.join(3)
    _ok(res == [True] and not app._stop_event.is_set() and "Fortsatt i gang" in fl.heartbeat(),
        "«▶ Fortsett»: kjøringen gjenopptas")

    # 2) Programmet lukkes mens kjøringen står stoppet
    res.clear()
    app._stop_event.set()
    t = threading.Thread(target=lambda: res.append(app._wait_after_stop(2, "Steg C")),
                         daemon=True)
    t.start()
    time.sleep(0.2)
    app._request_stop_for_exit("Programmet avsluttes av operatøren — kjøringen stoppes")
    t.join(3)
    _ok(res == [False], "lukking mens kjøringen står stoppet → kjøringen avsluttes")

    # 3) Lukking som kommer mellom clear() og wait() slipper også gjennom
    res.clear()
    orig_clear = app._step_resume_event.clear

    def _clear_after_close():
        app._closing = True
        app._step_resume_event.set()
        orig_clear()                          # «Avslutt»-signalet nulles …
    app._closing = False
    app._step_resume_event.clear = _clear_after_close
    t = threading.Thread(target=lambda: res.append(app._wait_after_stop(2, "Steg C")),
                         daemon=True)
    t.start()
    t.join(3)
    _ok(res == [False], "… og ventingen stopper likevel (ingen evig venting)")

    # 4) Allerede under lukking: ingen venting
    t0 = time.monotonic()
    _ok(app._wait_after_stop(2, "Steg C") is False and time.monotonic() - t0 < 0.2,
        "under lukking: ingen venting på knapper som ikke finnes")
    fl.__exit__(None, None, None)


def test_request_stop_for_exit(tmp: Path) -> None:
    print("Lukking under kjøring")
    app = _fake_app(tmp)
    ctx = app._conv_ctx
    fl = ctx.metadata["file_logger"]
    app._conv_pause()                                  # kjøringen står på pause
    app._request_stop_for_exit("Programmet avsluttes av operatøren — kjøringen stoppes")
    _ok(app._closing and app._stop_event.is_set() and not app._pause_event.is_set()
        and ctx.metadata["stopped"] is True and ctx.metadata["paused"] is False
        and app._step_resume_event.is_set(),
        "pause oppheves, stopp settes, ventende kjøretråd slippes")
    _ok("Programmet avsluttes av operatøren" in _log_text(app), "årsaken står i kjøreloggen")
    sent: list = []
    app._remote_rep = SimpleNamespace(closed=lambda forced: sent.append(("closed", forced)))
    app._remote_hub = SimpleNamespace(stop=lambda flush_timeout=10.0: sent.append(("stop",)))
    app._final_cleanup(forced=True)
    text = fl.log_path.read_text(encoding="utf-8")
    _ok("avsluttet før steget rakk å stoppe" in text and "Logg avsluttet" in text
        and fl._fh is None, "tvungen avslutning: kjøreloggen lukkes med forklaring")
    _ok(sent == [("closed", True), ("stop",)] and app._remote_hub is None,
        "mobilen får beskjed, og varslingen stoppes")
    fl.log("etterpå", "info")                          # kjøretråden kan fortsatt logge
    _ok("etterpå" not in fl.log_path.read_text(encoding="utf-8"), "(skriving etter lukking er trygg)")


# ── Prosessen blir faktisk borte ─────────────────────────────────────────────

_HARNESS = r'''
import concurrent.futures, queue, sys, threading, time
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, {root!r})
mode, tmp = sys.argv[1], Path(sys.argv[2])
import gui.app as A
from siard_workflow.core.file_logger import WorkflowFileLogger

app = A.App.__new__(A.App)
app._log_queue = queue.Queue()
app._stop_event, app._pause_event = threading.Event(), threading.Event()
app._step_resume_event = threading.Event()
app._abort_requested = app._step_stopped = app._closing = False
app._remote_hub = app._remote_rep = None
app._CLOSE_GRACE_S = 4
fl = WorkflowFileLogger(tmp, mode, heartbeat_s=0)
fl.__enter__()
fl.set_step("[8] BLOB Konverter til PDF/A")
app._conv_ctx = SimpleNamespace(metadata={{"file_logger": fl}})
(tmp / (mode + ".path")).write_text(str(fl.log_path), encoding="utf-8")
stop_ev, pause_ev = app._stop_event, app._pause_event


def job(i):                       # som en retry-jobb i BLOB Konverter
    if stop_ev.is_set():
        return
    while pause_ev.is_set():
        if stop_ev.is_set():
            return
        time.sleep(0.05)
    time.sleep(0.05)


def run():
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(job, range(200)))
    if mode == "deaf":            # et steg som ikke bryr seg om stopp
        threading.Event().wait()
    if stop_ev.is_set():
        app._wait_after_stop(1, "BLOB Konverter til PDF/A", interrupted=True)
        fl.log("Kjøringen ble avsluttet. Prosjektet er lagret.", "warn")
    fl.__exit__(None, None, None)


threading.Thread(target=lambda: threading.Event().wait(), name="henger").start()  # ikke daemon
app._run_thread = threading.Thread(target=run, daemon=True)
app._run_thread.start()
time.sleep(0.3)
app._conv_pause = lambda: None
pause_ev.set()                    # «⏸ Pause»
time.sleep(0.3)
# … og så lukkes hovedvinduet: mainloop returnerer, main.py kaller dette
app.shutdown_after_mainloop()
'''


def _run_harness(tmp: Path, mode: str) -> "tuple[int, float, str]":
    script = tmp / "harness.py"
    script.write_text(textwrap.dedent(_HARNESS).format(root=str(_ROOT)), encoding="utf-8")
    t0 = time.monotonic()
    p = subprocess.run([sys.executable, "-X", "utf8", str(script), mode, str(tmp)],
                       capture_output=True, text=True, timeout=60)
    took = time.monotonic() - t0
    log = Path((tmp / f"{mode}.path").read_text(encoding="utf-8")).read_text(encoding="utf-8")
    if p.returncode != 0:
        print(p.stderr[-2000:])
    return p.returncode, took, log


def test_process_exits(tmp: Path) -> None:
    print("Prosessen avsluttes (underprosess: pause + lukket hovedvindu)")
    rc, took, log = _run_harness(tmp, "paused")
    _ok(rc == 0 and took < 20, f"kjøring på pause: prosessen ble borte ({took:.1f} s)")
    _ok("Hovedvinduet er lukket — kjøringen stoppes" in log
        and "Kjøringen ble avsluttet" in log and "Logg avsluttet" in log
        and "rakk å stoppe" not in log,
        "kjøringen stoppet ordentlig, og kjøreloggen ble avsluttet")

    rc, took, log = _run_harness(tmp, "deaf")
    _ok(rc == 0 and 3.5 < took < 30,
        f"steg som ikke stopper: prosessen avsluttes likevel etter fristen ({took:.1f} s)")
    _ok("avsluttet før steget rakk å stoppe" in log and "Logg avsluttet" in log,
        "… og kjøreloggen forklarer hva som skjedde")


def test_kill_child_tools(tmp: Path) -> None:
    print("Verktøyprosesser ryddes")
    if sys.platform != "win32":
        print("  (hoppet over — bare Windows)")
        return
    script = tmp / "kids.py"
    script.write_text(textwrap.dedent(r'''
        import os, subprocess, sys, time
        sys.path.insert(0, {root!r})
        from siard_workflow.core.proc_kill import kill_child_tools, TOOL_NAMES
        exe = os.path.basename(sys.executable).lower()
        kid = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        time.sleep(0.5)
        assert exe not in TOOL_NAMES
        n0 = kill_child_tools()                 # standardnavn: Python-barnet røres ikke
        time.sleep(0.3)
        alive_after_default = kid.poll() is None
        n1 = kill_child_tools({{exe}})            # bare for testen: regn python som verktøy
        kid.wait(10)
        print(n0, alive_after_default, n1, kid.returncode)
    ''').format(root=str(_ROOT)), encoding="utf-8")
    p = subprocess.run([sys.executable, "-X", "utf8", str(script)],
                       capture_output=True, text=True, timeout=60)
    if p.returncode != 0:
        print(p.stderr[-2000:])
    parts = p.stdout.split()
    _ok(p.returncode == 0 and parts[:2] == ["0", "True"],
        "prosesser som ikke er verktøy (f.eks. en PDF-leser) røres ikke")
    _ok(parts[2] == "1" and parts[3] != "0" and parts[3] != "None",
        "verktøyprosess startet av programmet avsluttes")


# ── Stopp-knappen ────────────────────────────────────────────────────────────

def test_stop_button() -> None:
    print("Stopp-knappen i fremdriftspanelet")
    import customtkinter as ctk
    from gui.progress_panel import ProgressPanel
    root = ctk.CTk()
    root.withdraw()
    try:
        calls: list = []
        p = ProgressPanel(root)
        p.grid(row=0, column=0, sticky="ew")
        p.set_callbacks(pause_cb=lambda: calls.append("pause"),
                        stop_cb=lambda: calls.append("stop"),
                        resume_cb=lambda: calls.append("resume"),
                        abort_cb=lambda: calls.append("abort"))

        def stop():
            return (bool(p._stop_btn.winfo_manager()), p._stop_btn.cget("state"),
                    p._stop_btn.cget("text"))

        def pause():
            return (bool(p._pause_btn.winfo_manager()), p._pause_btn.cget("state"),
                    p._pause_btn.cget("text"))

        p.show_simple("Pakk ut SIARD")
        _ok(stop() == (True, "normal", "⏹ Stopp"), "enkelt steg: «⏹ Stopp» synlig og klar")
        p.init_run(100)
        _ok(stop() == (True, "normal", "⏹ Stopp") and pause() == (True, "normal", "⏸ Pause"),
            "BLOB-steg: «⏸ Pause» og «⏹ Stopp»")
        p.finish({"converted": 90, "kept": 10, "failed": 0})
        _ok(stop()[0] and stop()[1] == "disabled", "steget ferdig: knappen står, men er av")

        # Feilen: etter et ferdig BLOB-steg kom «⏹ Stopp» aldri tilbake
        p.show_simple("Pakk sammen SIARD")
        _ok(stop() == (True, "normal", "⏹ Stopp"),
            "neste steg etter BLOB-steget: «⏹ Stopp» er tilbake")
        p.init_run(50)
        _ok(stop() == (True, "normal", "⏹ Stopp") and pause() == (True, "normal", "⏸ Pause"),
            "nytt BLOB-steg (f.eks. etter «Standardiser filendelser»): begge knappene")

        p.set_paused(True)
        _ok(pause()[2] == "▶ Fortsett" and stop() == (True, "normal", "⏹ Stopp"),
            "på pause: «▶ Fortsett» — og «⏹ Stopp» er fortsatt der")
        p._stop_btn.invoke()
        _ok(calls == ["stop"], "«⏹ Stopp» på pause kaller stopp")
        p.set_stopped()
        _ok(stop() == (True, "disabled", "⏳ Stopper …") and pause()[1:] == ("disabled", "⏸ Pause"),
            "etter «⏹ Stopp»: knappen viser «⏳ Stopper …» (forsvinner ikke)")
        p.set_step_stopped(True)
        _ok(stop() == (True, "normal", "▶ Fortsett") and pause() == (True, "normal", "⏹ Avslutt"),
            "steget har stoppet: «▶ Fortsett» og «⏹ Avslutt»")
        p._stop_btn.invoke()
        p._pause_btn.invoke()
        _ok(calls == ["stop", "resume", "abort"], "knappene kaller fortsett og avslutt")
        p.set_step_stopped(False)
        _ok(stop() == (True, "normal", "⏹ Stopp") and pause()[2] == "⏸ Pause",
            "etter «▶ Fortsett»: tilbake til «⏸ Pause» / «⏹ Stopp»")
        p.reset()
        _ok(stop()[0] and stop()[2] == "⏹ Stopp", "nullstilt for neste kjøring")
    finally:
        root.destroy()


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="close_stop_") as d:
        tmp = Path(d)
        test_heartbeat_hold(tmp)
        test_reporter_hold()
        test_pause_stop_logged(tmp)
        test_wait_after_stop(tmp)
        test_request_stop_for_exit(tmp)
        test_process_exits(tmp)
        test_kill_child_tools(tmp)
        test_stop_button()
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
