"""
siard_workflow/operations/virus_scan_operation.py

Virusskan via ClamAV daemon (clamd + clamdscan):
  1. Finn ClamAV-installasjon og database
  2. Kjor freshclam hvis database mangler
  3. Start clamd direkte (ikke via shell) uten Foreground-modus
  4. Vent til clamd svarer PONG pa TCP 127.0.0.1:3310
  5. Kjor clamdscan --multiscan via clamdscan.conf
  6. Analyser resultat, stopp clamd, rydd opp

Multiscan bruker alle CPU-kjerner (MaxThreads = os.cpu_count()).
"""
from __future__ import annotations
import datetime
import os
import socket
import subprocess
import sys
import shutil
import tempfile
import time
import zipfile
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


def _decode(proc) -> tuple:
    out = proc.stdout.decode("utf-8", errors="replace") if proc.stdout else ""
    err = proc.stderr.decode("utf-8", errors="replace") if proc.stderr else ""
    return out, err


class VirusScanOperation(BaseOperation):
    operation_id   = "virus_scan"
    label          = "Virusskan"
    description    = "Pakker ut SIARD og kjorer ClamAV-daemon (clamd+clamdscan) pa alle filer."
    category       = "Sikkerhet"
    status         = 0
    default_params = {"keep_temp": False}

    # ─── Finn ClamAV ──────────────────────────────────────────────────────────

    def _find_clamav_dir(self, w) -> Path | None:
        try:
            from settings import get_config
            av_exe = get_config("av_executable", "").strip()
            if av_exe:
                p = Path(av_exe)
                if p.is_file():  return p.parent
                if p.is_dir():   return p
        except Exception:
            pass
        for name in ("clamd", "clamd.exe", "clamdscan", "clamdscan.exe",
                     "clamscan", "clamscan.exe"):
            found = shutil.which(name)
            if found: return Path(found).parent
        if sys.platform == "win32":
            for base in (r"C:\Program Files\ClamAV",
                         r"C:\Program Files (x86)\ClamAV", r"C:\ClamAV"):
                if Path(base).is_dir(): return Path(base)
            for base in (r"C:\Program Files", r"C:\Program Files (x86)"):
                try:
                    for entry in Path(base).iterdir():
                        if "clamav" in entry.name.lower():
                            if (entry / "clamd.exe").exists() or \
                               (entry / "clamscan.exe").exists():
                                return entry
                except Exception:
                    pass
        w("  ClamAV ikke funnet.", "warn")
        w("  Last ned: https://www.clamav.net/downloads#otherversions", "warn")
        w("  Sett sti i Innstillinger -> Sti til AV-program", "warn")
        try:
            import tkinter as tk
            from tkinter import messagebox
            if tk._default_root:
                if messagebox.askyesno("ClamAV ikke funnet",
                                       "ClamAV ikke funnet.\nApne nedlastingsside?",
                                       icon="warning"):
                    import webbrowser
                    webbrowser.open("https://www.clamav.net/downloads#otherversions")
        except Exception:
            pass
        return None

    def _find_database(self, av_dir: Path) -> Path | None:
        pdata = os.environ.get("ProgramData", r"C:\ProgramData")
        candidates = [
            av_dir / "database", av_dir / "db", av_dir / "clamav-data",
            Path(pdata) / "ClamAV",
            Path(pdata) / "ClamAV" / "database",
            av_dir.parent / "database",
        ]
        for p in candidates:
            if not p.is_dir(): continue
            try:
                if any(f.suffix in (".cvd", ".cld", ".cdb") for f in p.iterdir()):
                    return p
            except Exception:
                pass
        return next((p for p in candidates if p.is_dir()), None)

    def _run_freshclam(self, av_dir: Path, w) -> bool:
        for name in ("freshclam.exe", "freshclam"):
            fc = av_dir / name
            if fc.exists():
                w(f"  Kjorer freshclam: {fc}", "info")
                try:
                    proc = subprocess.run(
                        [str(fc)], stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, timeout=180)
                    out, err = _decode(proc)
                    for line in (out + err).splitlines():
                        if line.strip(): w(f"  freshclam: {line}", "info")
                    return proc.returncode == 0
                except Exception as exc:
                    w(f"  freshclam feil: {exc}", "warn")
                    return False
        w("  freshclam ikke funnet.", "warn")
        return False

    # ─── clamd konfig og oppstart ──────────────────────────────────────────────

    def _write_clamd_conf(self, conf_path: Path, db_path: Path,
                          tmp_dir: Path, log_path: Path, n_threads: int) -> None:
        # IKKE Foreground -- clamd kjoerer som bakgrunnsprosess
        lines = [
            f"DatabaseDirectory {db_path}",
            f"TemporaryDirectory {tmp_dir}",
            f"MaxThreads {n_threads}",
            f"LogFile {log_path}",
            "LogVerbose no",
            "TCPSocket 3310",
            "TCPAddr 127.0.0.1",
            "ScanPE yes",
            "ScanOLE2 yes",
            "ScanHTML yes",
            "ScanArchive yes",
            "MaxScanSize 500M",
            "MaxFileSize 100M",
            "MaxRecursion 16",
            "MaxDirectoryRecursion 20",
        ]
        conf_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _write_clamdscan_conf(self, conf_path: Path) -> None:
        lines = ["TCPSocket 3310", "TCPAddr 127.0.0.1"]
        conf_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _start_clamd(self, av_dir: Path, conf_path: Path, w):
        for name in ("clamd.exe", "clamd"):
            exe = av_dir / name
            if exe.exists():
                w(f"  Starter clamd: {exe}", "info")
                try:
                    # Start direkte -- IKKE shell=True slik at vi faar riktig PID
                    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    if sys.platform == "win32":
                        flags |= getattr(subprocess, "DETACHED_PROCESS", 8)
                    proc = subprocess.Popen(
                        [str(exe), "--config-file", str(conf_path)],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        creationflags=flags if sys.platform == "win32" else 0)
                    return proc
                except Exception as exc:
                    w(f"  clamd oppstart feil: {exc}", "warn")
                    # Fallback: start via shell
                    try:
                        cmd_str = subprocess.list2cmdline(
                            [str(exe), "--config-file", str(conf_path)])
                        proc = subprocess.Popen(
                            cmd_str, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        return proc
                    except Exception as exc2:
                        w(f"  clamd shell-fallback feil: {exc2}", "warn")
                        return None
        w("  clamd.exe ikke funnet -- faller tilbake til clamscan.", "warn")
        return None

    def _wait_clamd_ready(self, timeout: int = 60, w=None) -> bool:
        """Vent til clamd svarer PONG pa TCP 127.0.0.1:3310."""
        deadline = time.monotonic() + timeout
        attempt  = 0
        while time.monotonic() < deadline:
            attempt += 1
            try:
                with socket.create_connection(("127.0.0.1", 3310), timeout=2) as s:
                    s.sendall(b"nPING\n")
                    resp = s.recv(32)
                    if b"PONG" in resp:
                        return True
            except (ConnectionRefusedError, OSError):
                pass
            except Exception:
                pass
            if w and attempt % 5 == 0:
                elapsed = int(time.monotonic() - (deadline - timeout))
                w(f"  Venter pa clamd ({elapsed}s) ...", "info")
            time.sleep(1)
        return False

    # ─── Logg ─────────────────────────────────────────────────────────────────

    def _write_av_log(self, log_dir: Path | None, siard_name: str,
                      log_lines: list) -> Path | None:
        if not log_dir: return None
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            path = log_dir / f"{siard_name}_{ts}_virusskan.log"
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"# Virusskan-rapport -- {siard_name}\n")
                f.write(f"# Tidspunkt: {datetime.datetime.now()}\n")
                f.write("# " + "="*60 + "\n\n")
                f.write("\n".join(log_lines) + "\n")
            return path
        except Exception:
            return None

    # ─── Hoved-run ────────────────────────────────────────────────────────────

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log         = ctx.metadata.get("file_logger")
        log_dir_raw = ctx.metadata.get("log_dir")
        log_dir     = Path(log_dir_raw) if log_dir_raw else None
        siard_name  = ctx.siard_path.stem if ctx.siard_path else "siard"

        def w(msg: str, lvl: str = "info") -> None:
            if log: log.log(msg, lvl)
            pcb = ctx.metadata.get("progress_cb")
            if pcb: pcb("log", msg=msg, level=lvl)

        try:
            from settings import get_config
            infected_rc = int(get_config("av_infected_rc", 1))
            av_timeout  = int(get_config("av_timeout", 600))
        except Exception:
            infected_rc, av_timeout = 1, 600

        n_threads = max(2, os.cpu_count() or 4)
        log_lines: list = []

        # Finn ClamAV
        av_dir = self._find_clamav_dir(w)
        if av_dir is None:
            return self._fail("ClamAV ikke funnet. Sett sti i Innstillinger.",
                              data={"log": []})
        w(f"  ClamAV-mappe: {av_dir}", "info")
        log_lines += [f"ClamAV-mappe    : {av_dir}",
                      f"CPU-kjerner     : {os.cpu_count()}",
                      f"Skannetraader   : {n_threads}"]

        # Finn/oppdater database
        db_path = self._find_database(av_dir)
        if db_path is None:
            w("  Database ikke funnet -- kjorer freshclam ...", "warn")
            self._run_freshclam(av_dir, w)
            db_path = self._find_database(av_dir)
        if db_path is None:
            msg = f"ClamAV-database ikke funnet i {av_dir}. Kjor freshclam manuelt."
            log_lines.append(f"FEIL: {msg}")
            self._write_av_log(log_dir, siard_name, log_lines)
            return self._fail(msg, data={"log": log_lines})
        w(f"  Database: {db_path}", "info")
        log_lines.append(f"Database        : {db_path}")

        # Temp-mappe fra config.json
        global_temp = ""
        try:
            from settings import get_config as _gc
            global_temp = _gc("global_temp_dir", "").strip()
        except Exception:
            pass
        if not global_temp:
            global_temp = (ctx.metadata.get("temp_dir", "").strip()
                           if hasattr(ctx, "metadata") else "")
        tmp_parent = Path(global_temp) if global_temp and Path(global_temp).is_dir() \
                     else None
        tmp_dir = Path(tempfile.mkdtemp(prefix="siard_av_", dir=tmp_parent))
        log_lines += [f"Temp-mappe      : {tmp_dir}",
                      f"SIARD-arkiv     : {ctx.siard_path}",
                      f"Tidspunkt start : {datetime.datetime.now()}", ""]

        clamd_proc = None
        try:
            # Pakk ut SIARD
            with zipfile.ZipFile(ctx.siard_path, "r") as zf:
                members = zf.namelist()
                w(f"  Pakker ut {len(members):,} filer ...", "info")
                log_lines.append(f"Pakker ut {len(members):,} filer ...")
                zf.extractall(tmp_dir)

            all_files = [f for f in tmp_dir.rglob("*") if f.is_file()]
            ext_count: dict = {}
            for f in all_files:
                ext = f.suffix.lower() or "(ingen)"
                ext_count[ext] = ext_count.get(ext, 0) + 1
            log_lines.append(f"Filer klar: {len(all_files):,}")
            log_lines.append("Fordeling per filtype:")
            for ext, cnt in sorted(ext_count.items(), key=lambda x: -x[1]):
                log_lines.append(f"  {ext:<14} {cnt:>6}")
            log_lines.append("")
            w(f"  Filer klar for skanning: {len(all_files):,}", "info")

            # Start clamd
            conf_path  = tmp_dir / "clamd.conf"
            clamd_log  = tmp_dir / "clamd.log"
            self._write_clamd_conf(conf_path, db_path, tmp_dir, clamd_log, n_threads)
            clamd_proc = self._start_clamd(av_dir, conf_path, w)
            use_clamd  = clamd_proc is not None

            if use_clamd:
                w("  Venter pa clamd (maks 60s) ...", "info")
                if self._wait_clamd_ready(timeout=60, w=w):
                    w("  clamd klar.", "ok")
                else:
                    w("  clamd svarte ikke -- faller tilbake til clamscan.", "warn")
                    use_clamd = False
                    try: clamd_proc.terminate()
                    except Exception: pass
                    clamd_proc = None

            # Bygg skannekommando
            scan_mode = ""
            if use_clamd:
                for name in ("clamdscan.exe", "clamdscan"):
                    scanner_exe = av_dir / name
                    if scanner_exe.exists(): break
                else:
                    scanner_exe = None

                if scanner_exe and scanner_exe.exists():
                    clamdscan_conf = tmp_dir / "clamdscan.conf"
                    self._write_clamdscan_conf(clamdscan_conf)
                    cmd = [str(scanner_exe),
                           "--config-file", str(clamdscan_conf),
                           "--multiscan",
                           "--infected",
                           "--no-summary",
                           str(tmp_dir)]
                    scan_mode = "clamdscan (multiscan)"
                else:
                    w("  clamdscan ikke funnet -- bruker clamscan.", "warn")
                    use_clamd = False

            if not use_clamd:
                for name in ("clamscan.exe", "clamscan"):
                    scanner_exe = av_dir / name
                    if scanner_exe.exists(): break
                cmd = [str(scanner_exe),
                       "--database", str(db_path),
                       f"--max-threads={n_threads}",
                       "--recursive", "--infected",
                       "--suppress-ok-results",
                       str(tmp_dir)]
                scan_mode = f"clamscan ({n_threads} traader)"

            log_lines += [f"Skannemetode    : {scan_mode}",
                          f"Kommando        : {' '.join(cmd)}", ""]
            w(f"  Skannemetode: {scan_mode}", "info")
            w(f"  Kommando: {' '.join(cmd)}", "info")
            w("  Skanner -- vennligst vent ...", "info")

            try:
                proc = subprocess.run(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    timeout=av_timeout)
                stdout_raw, stderr_raw = _decode(proc)
            except subprocess.TimeoutExpired:
                msg = f"Tidsavbrudd etter {av_timeout}s"
                log_lines.append(f"FEIL: {msg}")
                self._write_av_log(log_dir, siard_name, log_lines)
                return self._fail(msg, data={"log": log_lines})
            except (FileNotFoundError, PermissionError, OSError) as exc:
                # Prøv shell=True som siste utvei
                try:
                    proc = subprocess.run(
                        subprocess.list2cmdline(cmd), shell=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        timeout=av_timeout)
                    stdout_raw, stderr_raw = _decode(proc)
                except Exception as exc2:
                    msg = f"Skanningsfeil: {exc2}"
                    log_lines.append(f"FEIL: {msg}")
                    self._write_av_log(log_dir, siard_name, log_lines)
                    return self._fail(msg, data={"log": log_lines})

            stdout_lines = [l for l in stdout_raw.splitlines() if l.strip()]
            stderr_lines = [l for l in stderr_raw.splitlines() if l.strip()]

            if stdout_lines:
                log_lines += ["=== SKANNERAPPORT (stdout) ==="] + stdout_lines + [""]
            if stderr_lines:
                log_lines += ["=== FEILMELDINGER (stderr) ==="] + stderr_lines + [""]
            log_lines.append(f"Returkode: {proc.returncode}")
            log_lines.append(f"Tidspunkt slutt: {datetime.datetime.now()}")
            w(f"  Returkode: {proc.returncode}", "info")

            # ClamAV returkode 2 = intern feil (ikke virusfunn)
            if proc.returncode == 2:
                msg = ("ClamAV intern feil (returkode 2) -- ikke virusfunn. "
                       "Sjekk stderr i loggfilen.")
                log_lines += [f"\nADVARSEL: {msg}", "RESULTAT: FEIL VED SKANNING"]
                av_log_path = self._write_av_log(log_dir, siard_name, log_lines)
                if av_log_path: w(f"  AV-loggfil: {av_log_path}", "info")
                w(f"  [ADVARSEL] {msg}", "warn")
                for line in stderr_lines[:8]: w(f"  {line}", "warn")
                ctx.set_flag("virus_found", False)
                return self._fail(msg, data={"log": log_lines, "returncode": 2,
                                              "infected": False})

            infected = (proc.returncode == infected_rc)
            funn_linjer = [l for l in stdout_lines if any(
                kw in l.lower() for kw in ("found", "infected", "virus",
                                            "trojan", "malware", "threat"))]
            if funn_linjer:
                w("  === FUNN ===", "warn")
                for linje in funn_linjer[:20]: w(f"  {linje}", "warn")
            else:
                for linje in stdout_lines[-8:]: w(f"  {linje}", "info")

            log_lines += ["",
                f"RESULTAT: {'TRUSLER FUNNET' if infected else 'Ingen trusler funnet'}",
                f"Filer skannet: {len(all_files):,}"]
            av_log_path = self._write_av_log(log_dir, siard_name, log_lines)
            if av_log_path: w(f"  AV-loggfil: {av_log_path}", "info")

            ctx.set_flag("virus_found", infected)
            data = {"infected": infected, "returncode": proc.returncode,
                    "files_scanned": len(all_files), "ext_summary": ext_count,
                    "log": log_lines, "scan_mode": scan_mode,
                    "av_log_path": str(av_log_path) if av_log_path else ""}
            ctx.set_result("virus_scan_detail", data)

            if infected:
                msg = (f"TRUSLER FUNNET ({len(all_files):,} filer, "
                       f"{len(funn_linjer)} funn)")
                w(f"  {msg}", "warn")
                return self._fail(msg, data=data)

            msg = f"Ingen trusler funnet ({len(all_files):,} filer, {scan_mode})"
            w(f"  {msg}", "ok")
            return self._ok(data=data, message=msg)

        finally:
            if clamd_proc is not None:
                try:
                    clamd_proc.terminate()
                    clamd_proc.wait(timeout=5)
                    w("  clamd stoppet.", "info")
                except Exception:
                    try: clamd_proc.kill()
                    except Exception: pass
            if not self.params.get("keep_temp", False):
                shutil.rmtree(str(tmp_dir), ignore_errors=True)
            else:
                w(f"  Utpakket mappe beholdt: {tmp_dir}", "info")
