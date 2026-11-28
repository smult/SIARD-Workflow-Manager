"""siard_workflow/core/atomic_write.py

Bytt inn en fil i ett trinn.

Innholdet skrives til en midlertidig fil i SAMME mappe og får riktig navn først
når alt er skrevet. Blir programmet avbrutt underveis (krasj, strømbrudd, full
disk), står den gamle fila urørt — den kan aldri bli liggende halvskrevet.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

TMP_SUFFIX = ".tmp_write"


def replace_file(tmp, dst, tries: int = 5) -> None:
    """os.replace med noen nye forsøk — antivirus/indeksering kan holde målfila
    åpen et øyeblikk på Windows."""
    for i in range(tries):
        try:
            os.replace(tmp, dst)
            return
        except PermissionError:
            if i == tries - 1:
                raise
            time.sleep(0.2 * (i + 1))


def write_bytes_atomic(path, data: bytes) -> None:
    """Som Path.write_bytes, men den gamle fila står hel til den nye er ferdig."""
    path = Path(path)
    tmp = path.with_name(path.name + TMP_SUFFIX)
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        replace_file(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
