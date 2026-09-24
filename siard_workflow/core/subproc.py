"""siard_workflow/core/subproc.py

Skjul konsollvinduer for eksterne kommandolinjeprogrammer på Windows.

Programmet kjøres normalt med `pythonw` (uten konsoll). Da åpner Windows et
nytt konsollvindu for HVERT kall til et konsollprogram — Ghostscript
(gswin64c.exe), Siegfried (sf.exe), taskkill, PowerShell osv. — som blinker
opp over skjermen. `hidden_kwargs()` gir argumentene til subprocess som
hindrer dette. Utdata via pipe påvirkes ikke.
"""
from __future__ import annotations

import subprocess
import sys


def hidden_kwargs() -> dict:
    """kwargs til subprocess.run/Popen/check_output: ingen konsollvindu (Windows)."""
    if sys.platform != "win32":
        return {}
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    si.wShowWindow = 0                      # SW_HIDE
    return {"creationflags": subprocess.CREATE_NO_WINDOW, "startupinfo": si}
