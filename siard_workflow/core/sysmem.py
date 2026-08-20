"""
siard_workflow/core/sysmem.py — plattform-uavhengige systemminne-hjelpere.

Beste-innsats (psutil hvis installert, ellers OS-API) deteksjon av total og
ledig fysisk RAM, samt en anbefalt terskel for når inline-LOB-ekstraksjon skal
bytte fra DOM-parsing (rask, men ~5–10× filstørrelse i minnet) til streaming
(konstant minne). Ingen tredjeparts-avhengighet påkrevd.
"""
from __future__ import annotations

import os
import sys


def _psutil(attr: str) -> "int | None":
    try:
        import psutil  # type: ignore
        return int(getattr(psutil.virtual_memory(), attr))
    except Exception:
        return None


def _windows_mem() -> "tuple[int, int] | None":
    """(total, available) via GlobalMemoryStatusEx, eller None."""
    if not sys.platform.startswith("win"):
        return None
    try:
        import ctypes

        class _MEMSTAT(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        ms = _MEMSTAT()
        ms.dwLength = ctypes.sizeof(_MEMSTAT)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(ms)):
            return int(ms.ullTotalPhys), int(ms.ullAvailPhys)
    except Exception:
        pass
    return None


def total_memory_bytes() -> "int | None":
    """Total installert fysisk RAM (bytes), eller None hvis ukjent."""
    v = _psutil("total")
    if v:
        return v
    win = _windows_mem()
    if win:
        return win[0]
    try:
        return int(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    except (ValueError, OSError, AttributeError):
        return None


def available_memory_bytes() -> "int | None":
    """Ledig fysisk RAM (bytes), eller None hvis ukjent."""
    v = _psutil("available")
    if v:
        return v
    win = _windows_mem()
    if win:
        return win[1]
    try:
        return int(os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    except (ValueError, OSError, AttributeError):
        return None


# Grenser for auto-terskelen (MB)
AUTO_THRESHOLD_MIN_MB = 50
AUTO_THRESHOLD_MAX_MB = 500
_DEFAULT_THRESHOLD_MB = 50   # fallback når RAM ikke lar seg fastslå


def auto_stream_inline_threshold_mb() -> int:
    """
    Anbefalt DOM→streaming-terskel (MB) ut fra installert RAM.

    Under terskelen brukes DOM-parsing (som bruker ~5–10× filstørrelsen i
    minnet); over terskelen brukes streaming (konstant minne). Vi setter derfor
    terskelen til ~1 % av total RAM — da bruker DOM av en terskel-stor fil rundt
    10 % av RAM, med god margin til OS/LibreOffice/andre tråder. Klemt til
    [50, 500] MB.

    Eks: 8 GB→~80 MB, 16 GB→~160 MB, 32 GB→~320 MB, 64 GB→500 MB (tak).
    """
    total = total_memory_bytes()
    if not total:
        return _DEFAULT_THRESHOLD_MB
    mb = int(total / (100 * 1024 * 1024))   # ~1 % av total RAM, i MB
    return max(AUTO_THRESHOLD_MIN_MB, min(AUTO_THRESHOLD_MAX_MB, mb))
