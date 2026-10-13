"""siard_workflow/core/proc_kill.py

Trygg avslutning av et prosesstre (LibreOffice: soffice.exe → soffice.bin).

Hvorfor egen modul (feil funnet 2026-10-13): Windows gjenbruker prosess-ID-er
raskt, og «foreldre-ID» (th32ParentProcessID) i en prosess peker bare på et tall
— også etter at forelderen er avsluttet og ID-en er gitt til en annen prosess.
Den gamle koden drepte alle prosesser med foreldre-ID == LibreOffice-PID etter
HVER batch, når LibreOffice allerede var avsluttet. Fikk en ny prosess samme
PID, ble feil prosesser drept — og siden programmet selv (pythonw) ble startet
av start.bat sin cmd.exe, som avsluttes med en gang, kunne programmet bli
«barn» av en LibreOffice-prosess som fikk cmd.exe sin gamle PID. Programmet
døde da momentant uten spor (etter 7 564 batcher i et reelt tilfelle).

Her regnes en prosess bare som barn når den er opprettet ETTER forelderen og
(hvis forelderen er avsluttet) FØR forelderen avsluttet. Programmet selv og
alle dets forfedre drepes aldri. Rotprosessen avsluttes via Popen-håndtaket
(peker på nøyaktig den prosessen, aldri en gjenbrukt PID).
"""
from __future__ import annotations

import os
import subprocess
import sys

_IS_WIN = sys.platform == "win32"


def select_tree(root_pid: int, root_times: "tuple[int, int | None]",
                procs: "list[tuple[int, int]]", times: dict,
                protected: "set[int]") -> "list[int]":
    """
    Etterkommere av root_pid (nærmeste først) blant `procs` [(pid, ppid)].
    times: {pid: (opprettet, avsluttet|None)}. En kandidat godtas som barn bare
    når opprettet ∈ [forelder.opprettet, forelder.avsluttet]. Ren funksjon
    (testbar uten ekte prosesser).
    """
    out: list = []
    queue = [(root_pid, root_times)]
    seen = {root_pid}
    while queue:
        pid, (created, exited) = queue.pop(0)
        for cpid, ppid in procs:
            if ppid != pid or cpid in seen or cpid in protected:
                continue
            ct = times.get(cpid)
            if not ct or ct[0] is None:
                continue
            if ct[0] < created or (exited is not None and ct[0] > exited):
                continue                      # PID-gjenbruk: ikke et ekte barn
            seen.add(cpid)
            out.append(cpid)
            queue.append((cpid, ct))
    return out


# ── Windows ──────────────────────────────────────────────────────────────────

def _win_api():
    import ctypes
    from ctypes import wintypes
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)

    class PROCESSENTRY32W(ctypes.Structure):
        _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD),
                    ("th32ProcessID", wintypes.DWORD),
                    ("th32DefaultHeapID", ctypes.c_size_t),
                    ("th32ModuleID", wintypes.DWORD), ("cntThreads", wintypes.DWORD),
                    ("th32ParentProcessID", wintypes.DWORD),
                    ("pcPriClassBase", ctypes.c_long), ("dwFlags", wintypes.DWORD),
                    ("szExeFile", ctypes.c_wchar * 260)]
    k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    k32.GetProcessTimes.argtypes = (wintypes.HANDLE,) + (ctypes.POINTER(wintypes.FILETIME),) * 4
    k32.TerminateProcess.argtypes = (wintypes.HANDLE, wintypes.UINT)
    k32.CloseHandle.argtypes = (wintypes.HANDLE,)
    k32.Process32FirstW.argtypes = (wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W))
    k32.Process32NextW.argtypes = (wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W))
    return ctypes, wintypes, k32, PROCESSENTRY32W


def _ft(ft) -> int:
    return (ft.dwHighDateTime << 32) | ft.dwLowDateTime


def _times_from_handle(api, h) -> "tuple[int, int | None] | None":
    ctypes, wintypes, k32, _ = api
    c, e, k, u = (wintypes.FILETIME() for _ in range(4))
    if not k32.GetProcessTimes(h, ctypes.byref(c), ctypes.byref(e),
                               ctypes.byref(k), ctypes.byref(u)):
        return None
    exited = _ft(e) or None          # 0 = kjører fortsatt
    return _ft(c), exited


def _times(api, pid: int) -> "tuple[int, int | None] | None":
    _c, _w, k32, _ = api
    h = k32.OpenProcess(0x1000, False, pid)          # QUERY_LIMITED_INFORMATION
    if not h:
        return None
    try:
        return _times_from_handle(api, h)
    finally:
        k32.CloseHandle(h)


def _snapshot(api) -> "list[tuple[int, int]]":
    ctypes, wintypes, k32, PE = api
    snap = k32.CreateToolhelp32Snapshot(0x2, 0)
    out: list = []
    if not snap or snap == wintypes.HANDLE(-1).value:
        return out
    try:
        pe = PE()
        pe.dwSize = ctypes.sizeof(PE)
        ok = k32.Process32FirstW(snap, ctypes.byref(pe))
        while ok:
            out.append((pe.th32ProcessID, pe.th32ParentProcessID))
            ok = k32.Process32NextW(snap, ctypes.byref(pe))
    finally:
        k32.CloseHandle(snap)
    return out


def _protected(api, procs, times) -> "set[int]":
    """Programmet selv og dets ekte forfedre — drepes aldri."""
    me = os.getpid()
    prot = {me, 0, 4}
    parent = dict(procs)
    cur = me
    for _ in range(64):
        p = parent.get(cur)
        if not p or p in prot:
            break
        tc, tp = times.get(cur), times.get(p)
        if not tc or not tp or tp[0] > tc[0]:
            break                     # foreldre-ID gjenbrukt — ikke en forfar
        prot.add(p)
        cur = p
    return prot


def _terminate_pid(api, pid: int, expect_created: int) -> None:
    _c, _w, k32, _ = api
    h = k32.OpenProcess(0x1000 | 0x0001, False, pid)  # QUERY_LIMITED | TERMINATE
    if not h:
        return
    try:
        t = _times_from_handle(api, h)
        if t and t[0] == expect_created and t[1] is None:   # samme prosess, kjører
            k32.TerminateProcess(h, 1)
    finally:
        k32.CloseHandle(h)


def kill_process_tree(proc: "subprocess.Popen | int") -> None:
    """
    Avslutt prosessen og dens EKTE etterkommere. `proc` bør være Popen-objektet
    (sikrest); med et rått PID antas det at prosessen fortsatt kjører.
    """
    if not _IS_WIN:
        import signal
        pid = proc.pid if isinstance(proc, subprocess.Popen) else int(proc)
        try:
            os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception:
            try:
                if isinstance(proc, subprocess.Popen):
                    proc.kill()
                else:
                    os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
        return
    try:
        api = _win_api()
    except Exception:
        return
    if isinstance(proc, subprocess.Popen):
        pid = proc.pid
        handle = getattr(proc, "_handle", None)
        root_t = _times_from_handle(api, int(handle)) if handle else None
    else:
        pid, handle = int(proc), None
        root_t = _times(api, pid)
    if not root_t:
        return
    procs = _snapshot(api)
    times = {p: t for p, _pp in procs if (t := _times(api, p))}
    prot = _protected(api, procs, times)
    if pid in prot:
        return
    kids = select_tree(pid, root_t, procs, times, prot)
    for k in reversed(kids):                     # dypeste først
        _terminate_pid(api, k, times[k][0])
    if root_t[1] is None:                        # roten kjører fortsatt
        if isinstance(proc, subprocess.Popen):
            try:
                proc.kill()                      # via håndtaket: nøyaktig denne
            except Exception:
                pass
        else:
            _terminate_pid(api, pid, root_t[0])
