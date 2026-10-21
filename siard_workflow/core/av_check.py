"""siard_workflow/core/av_check.py

Hvorfor går utpakking av millioner av små filer så tregt? Hjelpefunksjoner
for forhåndskontrollen «Antivirus og temp-disk».

Målt på en arbeidsmaskin (okt. 2026): hver ny liten fil kostet ~0,4–0,5 ms i
filsystemet — ~20× normalt — fordi antivirus skanner hver fil som opprettes.
5 millioner filer ≈ 35–65 min bare på å opprette filene. Et unntak for
temp-mappa i antivirus gir typisk 3–10× raskere utpakking, konvertering og
sammenpakking. Mekanisk disk og korte 8.3-navn trekker i samme retning.

  zip_entry_count()   antall oppføringer i en ZIP/SIARD — leses fra sluttposten
                      (ZIP64), uten å lese hele katalogen (rask også ved 5 mill.)
  active_antivirus()  aktive antivirusprodukter (Windows Sikkerhetssenter)
  measure_file_rate() hvor mange små filer per sekund temp-mappa klarer nå
  volume_info()       mekanisk disk (HDD) og om 8.3-navn lages
"""
from __future__ import annotations

import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

MANY_FILES = 200_000             # varsle fra så mange filer i et uttrekk


def zip_entry_count(path: Path) -> "int | None":
    """Antall oppføringer fra ZIP-ens sluttpost (EOCD / ZIP64-EOCD), ellers None."""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            tail_len = min(size, 66_000)
            f.seek(size - tail_len)
            tail = f.read(tail_len)
            i = tail.rfind(b"PK\x05\x06")
            if i < 0 or i + 22 > len(tail):
                return None
            total = struct.unpack_from("<H", tail, i + 10)[0]
            if total != 0xFFFF:
                return total
            j = tail.rfind(b"PK\x06\x07", 0, i)          # ZIP64-lokator
            if j < 0:
                return None
            off = struct.unpack_from("<Q", tail, j + 8)[0]
            f.seek(off)
            rec = f.read(56)
            if rec[:4] != b"PK\x06\x06":
                return None
            return struct.unpack_from("<Q", rec, 32)[0]
    except OSError:
        return None


def _hidden() -> dict:
    try:
        from siard_workflow.core.subproc import hidden_kwargs
        return hidden_kwargs()
    except Exception:
        return {}


def active_antivirus(timeout: int = 15) -> "list[str]":
    """
    Navn på aktive antivirusprodukter (Windows Sikkerhetssenter). Tom liste
    utenfor Windows eller ved feil. productState: bit 12–15 = 1 → sanntid på.
    """
    if sys.platform != "win32":
        return []
    ps = ("Get-CimInstance -Namespace root/SecurityCenter2 -ClassName AntiVirusProduct "
          "-ErrorAction SilentlyContinue | Select-Object displayName, productState "
          "| ConvertTo-Json -Compress")
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                            "-Command", ps], capture_output=True, text=True,
                           timeout=timeout, **_hidden())
        data = json.loads(r.stdout or "[]")
    except Exception:
        return []
    if isinstance(data, dict):
        data = [data]
    out = []
    for p in data or []:
        try:
            state = int(p.get("productState", 0))
        except (TypeError, ValueError):
            continue
        if (state >> 12) & 0xF == 1 and p.get("displayName"):
            out.append(str(p["displayName"]))
    return out


def measure_file_rate(folder: "Path | None", n: int = 400) -> "float | None":
    """Små filer (1,5 KB) per sekund som kan opprettes i `folder` akkurat nå."""
    try:
        base = Path(tempfile.mkdtemp(prefix="siard_hastighet_",
                                     dir=str(folder) if folder else None))
    except OSError:
        return None
    data = b"x" * 1500
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
    try:
        t = time.perf_counter()
        for i in range(n):
            fd = os.open(str(base / f"record{i}.txt"), flags, 0o666)
            os.write(fd, data)
            os.close(fd)
        return n / max(1e-6, time.perf_counter() - t)
    except OSError:
        return None
    finally:
        shutil.rmtree(base, ignore_errors=True)


def volume_info(folder: "Path | None", timeout: int = 15) -> dict:
    """{'hdd': True/False/None, 'short_names': True/False/None} for disken til `folder`."""
    info: dict = {"hdd": None, "short_names": None}
    if sys.platform != "win32":
        return info
    folder = Path(folder) if folder else Path(tempfile.gettempdir())
    drive = folder.resolve().drive.rstrip(":")
    if len(drive) == 1:
        ps = (f"(Get-Partition -DriveLetter {drive} -ErrorAction SilentlyContinue | Get-Disk "
              f"| Get-PhysicalDisk -ErrorAction SilentlyContinue).MediaType")
        try:
            r = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                                "-Command", ps], capture_output=True, text=True,
                               timeout=timeout, **_hidden())
            media = (r.stdout or "").strip().upper()
            if media in ("HDD", "SSD", "SCM"):
                info["hdd"] = media == "HDD"
        except Exception:
            pass
    # 8.3: opprett en fil med langt navn og se om Windows lager et kort navn
    try:
        base = Path(tempfile.mkdtemp(prefix="siard_83_", dir=str(folder)))
        try:
            (base / "langtfilnavn_record123456.txt").write_bytes(b"x")
            r = subprocess.run(["cmd", "/c", "dir", "/x", str(base)], capture_output=True,
                               text=True, encoding="cp850", errors="replace",
                               timeout=timeout, **_hidden())
            info["short_names"] = "~" in (r.stdout or "").split("langtfilnavn")[0].rsplit("\n", 1)[-1]
        finally:
            shutil.rmtree(base, ignore_errors=True)
    except Exception:
        pass
    return info
