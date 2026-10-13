"""
Trygg avslutning av prosesstre (core/proc_kill.py).

Reell feil (2026-10-13, Bergen): programmet forsvant uten spor midt i en lang
BLOB-konvertering. Etter hver LibreOffice-batch ble «alle prosesser med
foreldre-ID == LibreOffice-PID» drept — etter at LibreOffice var avsluttet og
PID-en kunne være gjenbrukt. Programmet (pythonw, startet av start.bat sin
cmd.exe som avsluttes straks) kunne da bli regnet som «barn» og drept.

Kjør:  python -X utf8 tests/test_proc_kill.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_select_tree() -> None:
    print("test_select_tree")
    from siard_workflow.core.proc_kill import select_tree
    # LibreOffice: soffice.exe (100) opprettet t=1000, avsluttet t=2000,
    # med soffice.bin (101, t=1010).
    procs = [(101, 100),          # ekte barn
             (200, 100),          # pythonw: foreldre-ID 100 fra en DØD cmd.exe (t=10)
             (300, 100),          # ny prosess etter at 100 ble avsluttet (gjenbruk, t=2500)
             (102, 101)]          # barnebarn (t=1020)
    times = {101: (1010, None), 200: (10, None), 300: (2500, None), 102: (1020, None)}
    got = select_tree(100, (1000, 2000), procs, times, protected=set())
    _ok(got == [101, 102], f"bare ekte barn/barnebarn velges ({got})")
    _ok(200 not in got, "prosess opprettet FØR LibreOffice (PID-gjenbruk) drepes ikke")
    _ok(300 not in got, "prosess opprettet ETTER at LibreOffice avsluttet drepes ikke")
    got2 = select_tree(100, (1000, None), [(101, 100), (500, 100)],
                       {101: (1010, None), 500: (1500, None)}, protected={500})
    _ok(got2 == [101], "beskyttede prosesser (programmet selv) drepes aldri")


def test_real_tree() -> None:
    print("test_real_tree")
    from siard_workflow.core.proc_kill import kill_process_tree
    code = ("import subprocess, sys, time; "
            "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
            "time.sleep(60)")
    p = subprocess.Popen([sys.executable, "-c", code])
    time.sleep(1.5)
    import psutil
    kids = [c.pid for c in psutil.Process(p.pid).children(recursive=True)]
    _ok(len(kids) == 1, f"testprosess med ett barn startet ({kids})")
    kill_process_tree(p)
    time.sleep(1.0)
    _ok(p.poll() is not None, "rotprosessen er avsluttet")
    _ok(not any(psutil.pid_exists(k) and psutil.Process(k).is_running() for k in kids),
        "barneprosessen er avsluttet")
    # Avsluttet rot: ingen feil, ingen andre prosesser røres
    kill_process_tree(p)
    _ok(True, "kall på allerede avsluttet prosess er trygt")


def test_reused_pid_not_killed() -> None:
    """Som i Bergen: en eldre prosess (pythonw) har en «foreldre-ID» som nå
    tilhører en NYERE LibreOffice-prosess (PID-gjenbruk). Når LibreOffice-treet
    avsluttes, skal den eldre prosessen ikke drepes. PID-gjenbruk kan ikke
    framtvinges, så prosesslisten justeres slik Windows ville vist den — med
    ekte prosesser og ekte opprettelsestider."""
    print("test_reused_pid_not_killed")
    import psutil
    from siard_workflow.core import proc_kill as PK
    victim = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    time.sleep(0.3)
    lo = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])  # nyere
    time.sleep(0.3)
    orig = PK._snapshot
    PK._snapshot = lambda api: [(pid, lo.pid if pid == victim.pid else ppid)
                                for pid, ppid in orig(api)]
    try:
        PK.kill_process_tree(lo)
    finally:
        PK._snapshot = orig
    time.sleep(0.5)
    _ok(lo.poll() is not None, "LibreOffice-prosessen er avsluttet")
    _ok(victim.poll() is None and psutil.Process(victim.pid).is_running(),
        "eldre prosess med gjenbrukt foreldre-ID lever fortsatt (gammel kode drepte den)")
    victim.kill()


def test_never_self() -> None:
    print("test_never_self")
    from siard_workflow.core.proc_kill import kill_process_tree
    kill_process_tree(os.getpid())
    _ok(True, "programmet dreper aldri seg selv (kjører fortsatt)")
    if os.getppid():
        kill_process_tree(os.getppid())
        _ok(True, "programmets forelder/forfedre drepes aldri")


def main() -> int:
    test_select_tree()
    if sys.platform == "win32":
        test_real_tree()
        test_reused_pid_not_killed()
        test_never_self()
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
