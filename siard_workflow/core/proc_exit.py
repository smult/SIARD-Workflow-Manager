"""siard_workflow/core/proc_exit.py

Avslutt programmet slik at prosessen faktisk blir borte.

Når hovedvinduet er lukket, venter Python ellers på trådene i trådpuljene før
prosessen avsluttes. En tråd som venter på noe som aldri kommer — f.eks.
«▶ Fortsett» etter «⏸ Pause», når knappen ikke finnes lenger — holder da
prosessen i live uten vindu, og livstegnet fortsetter å gå (funnet 2026-11-28:
en kjøring sto slik i 20 timer). Her ventes det en kort, avgrenset tid, og så
avsluttes prosessen uansett.
"""
from __future__ import annotations

import os
import sys
import threading
import time


def exit_process(code: int = 0, grace: float = 3.0, kill_tools: bool = False) -> None:
    """
    Avslutt prosessen — returnerer aldri.

    grace:      så lenge ventes det til sammen på tråder som ikke er
                daemon-tråder (de gjør som regel noe som bør få bli ferdig).
    kill_tools: avslutt også LibreOffice/Ghostscript/Siegfried som programmet
                har startet (når en kjøring ble avbrutt av avslutningen).
    """
    deadline = time.monotonic() + max(0.0, grace)
    me = threading.current_thread()
    try:
        for t in threading.enumerate():
            if t is me or t.daemon:
                continue
            left = deadline - time.monotonic()
            if left <= 0:
                break
            t.join(left)
    except Exception:
        pass
    if kill_tools:
        try:
            from .proc_kill import kill_child_tools
            kill_child_tools()
        except Exception:
            pass
    for stream in (sys.stdout, sys.stderr):
        try:
            if stream is not None:
                stream.flush()
        except Exception:
            pass
    os._exit(code)
