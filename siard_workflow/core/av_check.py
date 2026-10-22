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
                            "-Command", ps], capture_output=True, text=True, encoding="oem", errors="replace",
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
                                "-Command", ps], capture_output=True, text=True, encoding="oem", errors="replace",
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


# ── Filer per mappe (fra ZIP-katalogen) ──────────────────────────────────────

LARGE_FOLDER = 200_000           # 8.3-navn bremser merkbart fra slike mappestørrelser


def _central_directory(f) -> "tuple[int, int] | None":
    """(posisjon, størrelse) for ZIP-katalogen, også ZIP64."""
    f.seek(0, 2)
    size = f.tell()
    tail_len = min(size, 66_000)
    f.seek(size - tail_len)
    tail = f.read(tail_len)
    i = tail.rfind(b"PK\x05\x06")
    if i < 0 or i + 22 > len(tail):
        return None
    cd_size, cd_off = struct.unpack_from("<LL", tail, i + 12)
    if cd_off == 0xFFFFFFFF or cd_size == 0xFFFFFFFF:
        j = tail.rfind(b"PK\x06\x07", 0, i)
        if j < 0:
            return None
        f.seek(struct.unpack_from("<Q", tail, j + 8)[0])
        rec = f.read(56)
        if rec[:4] != b"PK\x06\x06":
            return None
        cd_size, cd_off = struct.unpack_from("<QQ", rec, 40)
    return cd_off, cd_size


def largest_folders(path: Path, top: int = 3) -> "list[tuple[str, int]]":
    """
    Mappene med flest filer i en ZIP/SIARD — leses rett fra katalogen (bare
    filnavnene), så det går raskt også med millioner av oppføringer.
    """
    from collections import Counter
    counts: Counter = Counter()
    try:
        with open(path, "rb") as f:
            loc = _central_directory(f)
            if not loc:
                return []
            off, total = loc
            f.seek(off)
            buf = b""
            left = total
            unpack = struct.unpack_from
            while left > 0 or len(buf) >= 46:
                if left > 0:
                    chunk = f.read(min(8 << 20, left))
                    left -= len(chunk)
                    buf += chunk
                pos, n = 0, len(buf)
                while pos + 46 <= n:
                    if buf[pos:pos + 4] != b"PK\x01\x02":
                        return [(d.decode("utf-8", "replace"), c)
                                for d, c in counts.most_common(top)]
                    ln, le, lc = unpack("<HHH", buf, pos + 28)
                    end = pos + 46 + ln + le + lc
                    if end > n:
                        break
                    name = buf[pos + 46:pos + 46 + ln]
                    if not name.endswith(b"/"):
                        counts[name[:name.rfind(b"/")]] += 1
                    pos = end
                buf = buf[pos:]
                if left <= 0 and pos == 0:
                    break
    except OSError:
        return []
    return [(d.decode("utf-8", "replace"), c) for d, c in counts.most_common(top)]


# ── 8.3-navn: status og avslåing (krever administrator) ──────────────────────

def short_name_policy() -> "int | None":
    """
    NtfsDisable8dot3NameCreation: 0 = på for alle disker, 1 = av for alle,
    2 = per disk, 3 = av unntatt systemdisken. None hvis ukjent.
    """
    if sys.platform != "win32":
        return None
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                            r"SYSTEM\CurrentControlSet\Control\FileSystem") as k:
            return int(winreg.QueryValueEx(k, "NtfsDisable8dot3NameCreation")[0])
    except Exception:
        return None


def disable_8dot3_commands(folder: "Path | None") -> "list[str]":
    """
    Kommandoene (administrator) som slår av 8.3-navn for nye filer på disken
    til `folder`. Per-disk-innstillingen gjelder bare når registerverdien er 2,
    så den settes først om nødvendig.
    """
    folder = Path(folder) if folder else Path(tempfile.gettempdir())
    drive = folder.resolve().drive or "C:"
    cmds = []
    if short_name_policy() != 2:
        cmds.append("fsutil 8dot3name set 2")
    cmds.append(f"fsutil 8dot3name set {drive} 1")
    return cmds


def run_elevated(commands: "list[str]", timeout: int = 900) -> "tuple[bool, str]":
    """
    Kjør kommandoene i et PowerShell-vindu som administrator (Windows spør om
    tillatelse). Vinduet viser resultatet og venter på Enter. Returnerer
    (startet_og_fullført, melding). Blokkerer — kall fra en tråd.
    """
    import base64
    if sys.platform != "win32":
        return False, "bare Windows"
    inner = ("$ErrorActionPreference = 'Continue'\n"
             "Write-Host 'Slår av korte 8.3-filnavn for nye filer (SIARD Workflow Manager)' "
             "-ForegroundColor Cyan\n"
             + "".join(f"Write-Host ''; Write-Host '> {c}' -ForegroundColor Yellow; {c}\n"
                       for c in commands)
             + "Write-Host ''; Write-Host 'Ferdig. Trykk Enter for å lukke vinduet.'\n"
               "[void](Read-Host)\n")
    b64 = base64.b64encode(inner.encode("utf-16-le")).decode("ascii")
    outer = ("try { Start-Process powershell -Verb RunAs -Wait -ArgumentList "
             f"'-NoProfile','-ExecutionPolicy','Bypass','-EncodedCommand','{b64}'; exit 0 }} "
             "catch { Write-Output $_.Exception.Message; exit 1 }")
    try:
        r = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                            "-Command", outer], capture_output=True, text=True, encoding="oem", errors="replace",
                           timeout=timeout, **_hidden())
    except subprocess.TimeoutExpired:
        return False, "tidsavbrudd"
    except Exception as exc:
        return False, str(exc)
    if r.returncode != 0:
        msg = (r.stdout or r.stderr or "").strip()
        return False, msg or "avbrutt (administratortilgang ble ikke gitt)"
    return True, ""
