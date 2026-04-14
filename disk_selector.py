"""
disk_selector.py  —  Finn beste tilkoblede disk for temp-mappe

Rangerer tilgjengelige disker etter egnethet:
  1. Disktype:  NVMe > SSD > ukjent > HDD > USB
  2. Systemdisk: ikke-systemdisk foretrekkes
  3. Ledig plass: mest ledig innen samme rangering

Windows: PowerShell Get-PhysicalDisk (MediaType + BusType).
Linux:   /sys/block/<dev>/queue/rotational
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


MIN_FREE_GB    = 20
MIN_FREE_BYTES = MIN_FREE_GB * 1024 ** 3

# Rangering — lavere tall = bedre
_TYPE_RANK = {"nvme": 0, "ssd": 1, "": 2, "hdd": 3, "usb": 4}


def get_disk_candidates(min_free_bytes: int = MIN_FREE_BYTES) -> list[dict]:
    """
    Returner liste over disker sortert etter egnethet.

    Hvert element har:
      path, free_bytes, total_bytes, is_system,
      disk_type  ("nvme"|"ssd"|"hdd"|"usb"|""),
      bus_type   ("NVMe"|"SATA"|"USB"|"SCSI"|""),
      label      — leselig streng for GUI og logg
    """
    if sys.platform == "win32":
        candidates = _windows_disks(min_free_bytes)
    else:
        candidates = _unix_disks(min_free_bytes)

    candidates.sort(key=lambda d: (
        _TYPE_RANK.get(d["disk_type"], 2),
        d["is_system"],
        -d["free_bytes"],
    ))
    return candidates


def best_temp_disk(min_free_bytes: int = MIN_FREE_BYTES,
                   siard_path: Path | None = None) -> Path:
    """Returner beste disk for temp-mappe — faller alltid tilbake."""
    for c in get_disk_candidates(min_free_bytes):
        return c["path"]
    if siard_path:
        root = _disk_root(siard_path)
        if shutil.disk_usage(root).free >= min_free_bytes:
            return root
    return Path.home()


def format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ── Windows ────────────────────────────────────────────────────────────────────

def _ps_disk_types() -> dict[str, tuple[str, str]]:
    """
    Kjør PowerShell Get-PhysicalDisk og returner
    {stasjonsbokstav: (disk_type, bus_type)}.
    Feiler stille og returnerer tom dict.
    """
    ps = (
        "Get-PhysicalDisk | ForEach-Object {"
        "  $d = $_;"
        "  try {"
        "    $parts = $d | Get-Disk | Get-Partition |"
        "      Where-Object { $_.DriveLetter };"
        "    foreach ($p in $parts) {"
        "      [PSCustomObject]@{"
        "        L=$p.DriveLetter; M=$d.MediaType; B=$d.BusType"
        "      }"
        "    }"
        "  } catch {}"
        "} | ConvertTo-Json -Compress"
    )
    try:
        raw = subprocess.check_output(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            text=True, timeout=10, stderr=subprocess.DEVNULL,
        ).strip()
        if not raw:
            return {}
        data = json.loads(raw)
        if isinstance(data, dict):
            data = [data]
    except Exception:
        return {}

    result: dict[str, tuple[str, str]] = {}
    for item in data:
        letter = str(item.get("L", "")).strip().upper()
        media  = str(item.get("M", "")).strip()   # SSD / HDD / Unspecified
        bus    = str(item.get("B", "")).strip()   # NVMe / SATA / USB / SCSI …
        if not letter:
            continue
        ml, bl = media.lower(), bus.lower()
        if bl == "nvme":
            dt = "nvme"
        elif ml == "ssd":
            dt = "ssd"
        elif ml == "hdd":
            dt = "hdd"
        elif bl == "usb":
            dt = "usb"
        else:
            dt = ""   # Unspecified — kan være SSD, rangeres mellom SSD og HDD
        result[letter] = (dt, bus)
    return result


def _type_label(disk_type: str, bus_type: str) -> str:
    if disk_type == "nvme":
        return "NVMe SSD"
    if disk_type == "ssd":
        return f"SSD ({bus_type})" if bus_type else "SSD"
    if disk_type == "hdd":
        return f"HDD ({bus_type})" if bus_type else "HDD"
    if disk_type == "usb":
        return "USB"
    return f"ukjent ({bus_type})" if bus_type else "ukjent type"


def _windows_disks(min_free_bytes: int) -> list[dict]:
    import string
    sys_drv    = os.environ.get("SystemDrive", "C:").upper().rstrip("\\")
    disk_types = _ps_disk_types()
    result     = []

    for letter in string.ascii_uppercase:
        root = Path(f"{letter}:\\")
        if not root.exists():
            continue
        try:
            usage = shutil.disk_usage(root)
        except (PermissionError, OSError):
            continue
        if usage.free < min_free_bytes:
            continue

        is_sys              = (letter.upper() + ":") == sys_drv
        disk_type, bus_type = disk_types.get(letter.upper(), ("", ""))
        tl                  = _type_label(disk_type, bus_type)

        label = f"{root}  —  {format_bytes(usage.free)} ledig  [{tl}]"
        if is_sys:
            label += "  [systemdisk]"

        result.append({
            "path":        root,
            "free_bytes":  usage.free,
            "total_bytes": usage.total,
            "is_system":   is_sys,
            "disk_type":   disk_type,
            "bus_type":    bus_type,
            "label":       label,
        })
    return result


# ── Linux / macOS ──────────────────────────────────────────────────────────────

def _linux_disk_type(dev_name: str) -> str:
    """Les /sys/block/<base>/queue/rotational — 0=SSD/NVMe, 1=HDD."""
    base = re.sub(r"p?\d+$", "", dev_name)   # nvme0n1p1 → nvme0n1, sda1 → sda
    if "nvme" in base.lower():
        return "nvme"
    try:
        rot = Path(f"/sys/block/{base}/queue/rotational").read_text().strip()
        return "ssd" if rot == "0" else "hdd"
    except Exception:
        return ""


def _unix_disks(min_free_bytes: int) -> list[dict]:
    mounts: list[tuple[Path, str]] = []
    try:
        with open("/proc/mounts") as f:
            skip = {"proc","sysfs","devtmpfs","tmpfs","cgroup","devpts",
                    "securityfs","debugfs","fusectl","configfs","binfmt_misc","squashfs"}
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[2] not in skip:
                    mounts.append((Path(parts[1]), parts[0]))
    except FileNotFoundError:
        try:
            out = subprocess.check_output(["df", "-P"], text=True)
            for line in out.splitlines()[1:]:
                p = line.split()
                if p:
                    mounts.append((Path(p[-1]), p[0]))
        except Exception:
            mounts = [(Path("/"), "")]

    seen:   set[str] = set()
    result: list[dict] = []
    sys_root = str(Path("/"))

    for mp, dev in mounts:
        if not mp.exists():
            continue
        try:
            usage  = shutil.disk_usage(mp)
            dev_id = str(os.stat(mp).st_dev)
        except (PermissionError, OSError):
            continue
        if dev_id in seen or usage.free < min_free_bytes:
            continue
        seen.add(dev_id)

        is_sys    = str(mp) == sys_root
        dev_name  = Path(dev).name if dev.startswith("/dev/") else ""
        disk_type = _linux_disk_type(dev_name) if dev_name else ""
        bus_type  = "NVMe" if disk_type == "nvme" else ""
        tl        = _type_label(disk_type, bus_type)

        label = f"{mp}  —  {format_bytes(usage.free)} ledig  [{tl}]"
        if is_sys:
            label += "  [systemdisk]"

        result.append({
            "path":        mp,
            "free_bytes":  usage.free,
            "total_bytes": usage.total,
            "is_system":   is_sys,
            "disk_type":   disk_type,
            "bus_type":    bus_type,
            "label":       label,
        })
    return result


# ── Felles ────────────────────────────────────────────────────────────────────

def _disk_root(path: Path) -> Path:
    if sys.platform == "win32":
        return Path(path.anchor)
    try:
        dev = os.stat(path).st_dev
        cur = path.resolve()
        while True:
            par = cur.parent
            if par == cur or os.stat(par).st_dev != dev:
                break
            cur = par
        return cur
    except OSError:
        return Path("/")
