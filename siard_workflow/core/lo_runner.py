"""siard_workflow/core/lo_runner.py

Felles kjøring av LibreOffice (soffice) for konverteringsløypene.

To problemer som håndteres her
==============================
1. **Gjenbruk av brukerprofil kan henge.** Verifisert 2026-09-24 med
   LibreOffice 26.2.1.2 på Windows: første `--headless --convert-to` med en ny
   profil (`-env:UserInstallation=…`) går på ~5 s, men ETHVERT senere kall med
   samme profil henger til tidsgrensen — også for en tekstfil på ett ord, uten
   låsefil eller gjenglemt prosess, og uavhengig av oppdateringssjekk.
   `health_check()` tester derfor to kall med samme profil. Henger det andre,
   settes `profile_reuse_ok() == False`, og `prepare_profile()` tømmer
   profilmappa før hvert kall (= ny profil per kall, ~5 s ekstra). På
   maskiner der gjenbruk virker, beholdes profilen (raskt).

2. **Tidsavbrudd etterlot `soffice.bin`.** `subprocess.run(timeout=…)` dreper
   bare startprosessen `soffice.exe`; selve LibreOffice (`soffice.bin`) ble
   gående og kunne blokkere senere kall. `run_lo()` dreper hele prosesstreet.
"""
from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path
from siard_workflow.core.subproc import hidden_kwargs

_state_lock = threading.Lock()
_reuse_ok: "bool | None" = None      # None = ikke testet (antas OK)
# Helsesjekk-resultat per soffice-sti for resten av programøkten: gjenbruks-
# testen koster opptil `reuse_timeout` sekunder på berørte maskiner.
_checked: dict[str, bool] = {}
# Installasjoner som er fullt helsesjekket (begge kall) i denne økten — da
# hopper senere sjekker (f.eks. BLOB-konverteringen etter preflight) over alt.
_verified: dict[str, "tuple[bool, str, bool]"] = {}

DOWNLOAD_URL = "https://www.libreoffice.org/download/download-libreoffice/"


def profile_reuse_ok() -> bool:
    """False hvis helsesjekken har påvist at gjenbruk av profil henger."""
    with _state_lock:
        return _reuse_ok is not False


def set_profile_reuse_ok(value: "bool | None") -> None:
    global _reuse_ok
    with _state_lock:
        _reuse_ok = value


def profile_url(profile_dir: Path) -> str:
    return Path(profile_dir).resolve().as_uri()


def prepare_profile(profile_dir: Path) -> Path:
    """
    Klargjør en profilmappe før et LibreOffice-kall. Når gjenbruk er påvist å
    henge, tømmes mappa slik at LibreOffice oppretter en ny profil.
    """
    profile_dir = Path(profile_dir)
    if not profile_reuse_ok() and profile_dir.exists():
        shutil.rmtree(profile_dir, ignore_errors=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    return profile_dir


def kill_tree(pid: int) -> None:
    """Drep en prosess og alle barneprosesser (soffice.exe → soffice.bin)."""
    try:
        if sys.platform == "win32":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=30, **hidden_kwargs())
        else:
            os.killpg(os.getpgid(pid), signal.SIGKILL)
    except Exception:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass


def run_lo(cmd: list[str], timeout: float) -> "tuple[int | None, bytes, bytes, bool]":
    """
    Kjør en LibreOffice-kommando. Returnerer (returkode, stdout, stderr,
    tidsavbrudd). Ved tidsavbrudd drepes hele prosesstreet.
    """
    kwargs: dict = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE,
                    **hidden_kwargs()}
    if sys.platform != "win32":
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(cmd, **kwargs)
    try:
        out, err = proc.communicate(timeout=timeout)
        return proc.returncode, out or b"", err or b"", False
    except subprocess.TimeoutExpired:
        kill_tree(proc.pid)
        try:
            out, err = proc.communicate(timeout=10)
        except Exception:
            out, err = b"", b""
        return None, out or b"", err or b"", True


def convert(lo_bin: str, src: Path, out_dir: Path, filter_str: str,
            profile_dir: Path, timeout: float,
            infilter: "str | None" = None) -> "tuple[bool, str]":
    """
    Konverter én fil. (True, "") når `out_dir/<stem>.<endelse>` ble laget,
    ellers (False, feilmelding).
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    prepare_profile(profile_dir)
    cmd = [lo_bin, f"-env:UserInstallation={profile_url(profile_dir)}",
           "--headless", "--norestore", "--nologo", "--nofirststartwizard"]
    if infilter:
        cmd.append(f"--infilter={infilter}")
    cmd += ["--convert-to", filter_str, "--outdir", str(out_dir), str(src)]
    try:
        rc, out, err, timed_out = run_lo(cmd, timeout)
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    target_ext = filter_str.split(":", 1)[0]
    result = out_dir / f"{Path(src).stem}.{target_ext}"
    if result.exists() and result.stat().st_size > 0:
        return True, ""
    if timed_out:
        return False, f"tidsavbrudd etter {int(timeout)} s"
    msg = (err or out).decode("utf-8", "replace").strip()
    return False, msg or f"ingen utfil (returkode {rc})"


def health_check(lo_bin: str, work_dir: Path, timeout: float = 120,
                 reuse_timeout: float = 45) -> "tuple[bool, str, bool]":
    """
    (fungerer, feilmelding, gjenbruk_ok). Konverterer en liten tekstfil to
    ganger med samme profil. Første kall må lykkes; henger eller feiler det
    andre, settes modus «ny profil per kall» (se modulens docstring).
    Resultatet av gjenbrukstesten huskes i økten OG i config.json per
    LibreOffice-installasjon (`install_key`) — testen kjøres på nytt først når
    LibreOffice installeres/oppgraderes. Første kall (fungerer LO?) kjøres alltid.
    """
    key = install_key(lo_bin) if lo_bin else ""
    with _state_lock:
        verified = _verified.get(key)
        cached = _checked.get(key)
    if verified is not None and verified[0]:
        set_profile_reuse_ok(verified[2])  # allerede sjekket i økten (preflight)
        return verified
    if cached is None:
        cached = _load_persisted(key)      # fra tidligere programøkt
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    src = work_dir / "lo_helsesjekk.txt"
    src.write_text("LibreOffice helsesjekk\n", "utf-8")
    prof = work_dir / "profil"
    set_profile_reuse_ok(None)          # første kall: ny profil uansett
    ok1, err1 = convert(lo_bin, src, work_dir / "o1", "pdf", prof, timeout)
    if not ok1:
        return False, err1, True
    if cached is not None:              # gjenbruk allerede testet (økt/config)
        set_profile_reuse_ok(cached)
        with _state_lock:
            _verified[key] = (True, "", cached)
        return True, "", cached
    ok2, _err2 = convert(lo_bin, src, work_dir / "o2", "pdf", prof, reuse_timeout)
    set_profile_reuse_ok(ok2)
    with _state_lock:
        _checked[key] = ok2
        _verified[key] = (True, "", ok2)
    _save_persisted(key, ok2)
    return True, "", ok2


def lo_version(lo_bin: str) -> str:
    """Versjonsnummer for soffice (Windows: filens versjonsressurs), ellers ""."""
    try:
        p = Path(lo_bin)
        if not p.is_file():
            w = shutil.which(str(lo_bin))
            p = Path(w) if w else p
        if sys.platform != "win32" or not p.is_file():
            return ""
        import ctypes
        from ctypes import wintypes
        ver = ctypes.windll.version
        size = ver.GetFileVersionInfoSizeW(str(p), None)
        if not size:
            return ""
        buf = ctypes.create_string_buffer(size)
        if not ver.GetFileVersionInfoW(str(p), 0, size, buf):
            return ""
        ptr = ctypes.c_void_p()
        ln = wintypes.UINT()
        if not ver.VerQueryValueW(buf, "\\", ctypes.byref(ptr), ctypes.byref(ln)):
            return ""
        class VS_FIXEDFILEINFO(ctypes.Structure):
            _fields_ = [(n, wintypes.DWORD) for n in (
                "sig", "struc", "fvMS", "fvLS", "pvMS", "pvLS", "mask", "flags",
                "os", "type", "subtype", "dateMS", "dateLS")]
        info = ctypes.cast(ptr, ctypes.POINTER(VS_FIXEDFILEINFO)).contents
        return (f"{info.pvMS >> 16}.{info.pvMS & 0xFFFF}."
                f"{info.pvLS >> 16}.{info.pvLS & 0xFFFF}")
    except Exception:
        return ""


def reuse_warning_acknowledged(lo_bin: str) -> bool:
    """True hvis brukeren har valgt «ikke vis igjen» for denne installasjonen."""
    try:
        from settings import get_config
        return install_key(lo_bin) in (get_config("lo_reuse_warning_ack", []) or [])
    except Exception:
        return False


def acknowledge_reuse_warning(lo_bin: str) -> None:
    try:
        from settings import get_config, set_config
        keys = [k for k in (get_config("lo_reuse_warning_ack", []) or [])
                if Path(k.split("|", 1)[0]).exists()]
        key = install_key(lo_bin)
        if key not in keys:
            keys.append(key)
        set_config("lo_reuse_warning_ack", keys)
    except Exception:
        pass


def install_key(lo_bin: str) -> str:
    """
    Nøkkel for én LibreOffice-installasjon: sti + størrelse og endringstid for
    soffice-filen. Endres ved ny installasjon/oppgradering, slik at
    gjenbrukstesten da kjøres på nytt.
    """
    try:
        p = Path(lo_bin).resolve()
        st = p.stat()
        return f"{p}|{st.st_size}|{int(st.st_mtime)}"
    except Exception:
        return str(lo_bin)


def _load_persisted(key: str) -> "bool | None":
    try:
        from settings import get_config
        cache = get_config("lo_profile_reuse_cache", {}) or {}
        v = cache.get(key)
        return bool(v) if isinstance(v, bool) else None
    except Exception:
        return None


def _save_persisted(key: str, value: bool) -> None:
    try:
        from settings import get_config, set_config
        cache = dict(get_config("lo_profile_reuse_cache", {}) or {})
        # Behold bare installasjoner som fortsatt finnes, og kun siste
        # installasjon per sti (en oppgradering erstatter den gamle nøkkelen)
        path = key.split("|", 1)[0]
        cache = {k: v for k, v in cache.items()
                 if Path(k.split("|", 1)[0]).exists() and k.split("|", 1)[0] != path}
        cache[key] = bool(value)
        set_config("lo_profile_reuse_cache", cache)
    except Exception:
        pass


def reset_session_cache(forget_persisted: bool = False) -> None:
    """Glem helsesjekk-resultater (økt; valgfritt også lagret i config)."""
    with _state_lock:
        _checked.clear()
        _verified.clear()
    set_profile_reuse_ok(None)
    if forget_persisted:
        try:
            from settings import set_config
            set_config("lo_profile_reuse_cache", {})
        except Exception:
            pass
