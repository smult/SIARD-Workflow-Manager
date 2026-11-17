"""
siard_workflow/core/fmt.py
Felles visningsformat for kjørelogg og rapporter: antall med mellomrom som
tusenskille, størrelser og varigheter med norsk desimalkomma.
"""
from __future__ import annotations


def num(n) -> str:
    """211836 → «211 836»."""
    try:
        if isinstance(n, float) and not n.is_integer():
            return f"{n:,}".replace(",", " ").replace(".", ",")
        return f"{int(n):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(n)


def size(n_bytes) -> str:
    """34773785447 → «32,4 GB»."""
    try:
        n = float(n_bytes)
    except (TypeError, ValueError):
        return str(n_bytes)
    for unit, factor in (("TB", 1024 ** 4), ("GB", 1024 ** 3),
                         ("MB", 1024 ** 2), ("KB", 1024)):
        if n >= factor:
            return f"{n / factor:.1f} {unit}".replace(".", ",")
    return f"{int(n)} B"


def duration(sec) -> str:
    """5105.3 → «1 t 25 min», 272 → «4 min 32 s», 23.8 → «23,8 s»."""
    try:
        sec = max(0.0, float(sec))
    except (TypeError, ValueError):
        return str(sec)
    if sec < 60:
        return f"{sec:.1f} s".replace(".", ",") if sec < 10 or sec % 1 else f"{int(sec)} s"
    total = int(round(sec))
    d, rest = divmod(total, 86400)
    h, rest = divmod(rest, 3600)
    m, s = divmod(rest, 60)
    if d:
        return f"{d} d {h} t {m} min"
    if h:
        return f"{h} t {m:02d} min"
    return f"{m} min {s:02d} s"
