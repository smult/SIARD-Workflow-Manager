"""
siard_workflow/operations/virus_scan_operation.py

Generisk virusskan-operasjon.

Brukeren oppgir:
  av_executable   – full sti til AV-program (f.eks. C:\\AV\\scan.exe)
  av_args         – argumenter som liste, med {FILE} som plassholder for skannemålet
  av_infected_rc  – returkode som betyr "funn" (standard: 1)
  scan_target     – "file"   → send SIARD-fila direkte til AV
                    "folder" → pakk ut SIARD til temp-mappe, send mappen

Eksempler på av_args:
  Windows Defender: ["scan", "/ScanType:3", "/File:{FILE}"]
  clamscan:         ["--recursive", "--infected", "--suppress-ok-results", "{FILE}"]
  Sophos:           ["-f", "{FILE}"]
  F-Secure:         ["--scan", "{FILE}"]

Alle instillinger hentes fra config.json (via settings.py) men kan overstyres
per operasjon via params.
"""
from __future__ import annotations

import datetime
import os
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
import zipfile
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


# ─────────────────────────────────────────────────────────────────────────────
# Hjelpefunksjoner
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_args(args: list[str], target: str) -> list[str]:
    """Erstatter {FILE} i alle argumenter med faktisk sti."""
    return [a.replace("{FILE}", target) for a in args]


def _windows_defender_enabled() -> bool:
    """
    Sjekker om Windows Defender (MpCmdRun.exe) er tilgjengelig og aktivert
    ved å spørre WMI/PowerShell om tjenestestatus.
    Returnerer True kun hvis Defender er aktivert og klar til bruk.
    """
    try:
        result = subprocess.run(
            [
                "powershell", "-NonInteractive", "-NoProfile", "-Command",
                "(Get-MpComputerStatus).AntivirusEnabled",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        out = result.stdout.decode("utf-8", errors="replace").strip().lower()
        return out == "true"
    except Exception:
        return False


def _auto_detect_av() -> tuple[str, list[str], int | None] | tuple[None, None, None]:
    """
    Forsøker å finne et AV-program automatisk.
    Returnerer (exe_sti, args, infected_rc) eller (None, None, None).
    infected_rc er None hvis programmet bruker den konfigurerte standardverdien.

    Windows Defender MpCmdRun.exe returkoder:
      0  – ingen trusler funnet
      2  – trusler funnet (malware oppdaget)
      Andre – feil (f.eks. mangler adminrettigheter, tjenesten kjører ikke)
    """
    # Windows Defender — kun hvis exe finnes OG Defender faktisk er aktivert
    wd = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) \
         / "Windows Defender" / "MpCmdRun.exe"
    if wd.exists() and _windows_defender_enabled():
        return str(wd), ["scan", "/ScanType:3", "/File:{FILE}"], 2

    # clamscan på PATH
    for name in ("clamscan", "clamscan.exe"):
        found = shutil.which(name)
        if found:
            return found, ["--recursive", "--infected",
                           "--suppress-ok-results", "{FILE}"], 1

    # ClamAV i vanlige installasjonsplasser (Windows)
    for base in (r"C:\Program Files\ClamAV",
                 r"C:\Program Files (x86)\ClamAV", r"C:\ClamAV"):
        exe = Path(base) / "clamscan.exe"
        if exe.exists():
            return str(exe), ["--recursive", "--infected",
                              "--suppress-ok-results", "{FILE}"], 1

    return None, None, None


# ─────────────────────────────────────────────────────────────────────────────

class VirusScanOperation(BaseOperation):
    operation_id     = "virus_scan"
    label            = "Virusskan"
    description      = ("Kjører valgfritt antivirus mot SIARD-filen eller utpakket innhold. "
                        "Bruk {FILE} i argumenter som plassholder for skannemålet.")
    category         = "Sikkerhet"
    status           = 2
    halt_on_failure  = True   # trusler eller skannefeil stopper hele workflowen

    default_params = {
        "scan_target":  "file",   # "file" = SIARD-fila direkte | "folder" = pakk ut først
        "keep_temp":    False,    # behold utpakket mappe etter skanning
        # Tomme = hent fra config.json; kan overstyres per operasjon:
        "av_executable":  "",
        "av_args":        "",     # mellomromsseparerte args med {FILE}-plassholder
        "av_infected_rc": "",     # tom = hent fra config
    }

    # ─── Loggskriving ────────────────────────────────────────────────────────

    def _write_av_log(self, log_dir: Path | None, siard_name: str,
                      lines: list[str]) -> Path | None:
        if not log_dir:
            return None
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            path = log_dir / f"{siard_name}_{ts}_virusskan.log"
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"# Virusskan-rapport — {siard_name}\n")
                f.write(f"# Tidspunkt: {datetime.datetime.now()}\n")
                f.write("# " + "=" * 60 + "\n\n")
                f.write("\n".join(lines) + "\n")
            return path
        except Exception:
            return None

    # ─── Hoved-run ───────────────────────────────────────────────────────────

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log         = ctx.metadata.get("file_logger")
        log_dir_raw = ctx.metadata.get("log_dir")
        log_dir     = Path(log_dir_raw) if log_dir_raw else None
        siard_name  = ctx.siard_path.stem if ctx.siard_path else "siard"

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            pcb = ctx.metadata.get("progress_cb")
            if pcb:
                pcb("log", msg=msg, level=lvl)

        log_lines: list[str] = []

        # ── Hent innstillinger ────────────────────────────────────────────
        try:
            from settings import get_config
            cfg_exe         = get_config("av_executable", "").strip()
            cfg_args        = get_config("av_args", [])
            cfg_infected_rc = int(get_config("av_infected_rc", 1))
        except Exception:
            cfg_exe, cfg_args, cfg_infected_rc = "", [], 1

        # Param-overrides (tomme = bruk config)
        p_exe  = (self.params.get("av_executable") or "").strip()
        p_args = (self.params.get("av_args") or "").strip()
        p_rc   = self.params.get("av_infected_rc", "")

        av_exe      = p_exe or cfg_exe
        av_args_raw = p_args or (cfg_args if isinstance(cfg_args, str)
                                  else " ".join(cfg_args))
        try:
            infected_rc = int(p_rc) if str(p_rc).strip() else cfg_infected_rc
        except (ValueError, TypeError):
            infected_rc = cfg_infected_rc

        scan_target = self.params.get("scan_target", "file")
        # Pipeline-modus: hvis SIARD allerede er pakket ut, bruk alltid mappe-skanning
        if getattr(ctx, "extracted_path", None) and ctx.extracted_path.is_dir():
            if scan_target != "folder":
                w("  Pipeline-modus: bytter til mappe-skanning (extracted_path er satt)",
                  "info")
                scan_target = "folder"

        # ── Auto-detect hvis ingen AV oppgitt ────────────────────────────
        if not av_exe:
            auto_exe, auto_args, auto_infected_rc = _auto_detect_av()
            if auto_exe:
                av_exe = auto_exe
                if not av_args_raw:
                    av_args_raw = " ".join(auto_args)
                # Bruk AV-spesifikk infected_rc kun hvis ingen override er satt
                if auto_infected_rc is not None and not str(p_rc).strip() and cfg_infected_rc == 1:
                    infected_rc = auto_infected_rc
                w(f"  Auto-detektert AV: {av_exe}", "info")
            else:
                msg = (
                    "Ingen AV-program funnet eller konfigurert. "
                    "Sett 'av_executable' i Innstillinger → Generelt, "
                    "eller fyll inn 'AV-program' i operasjonsparametrene. "
                    "Eksempel args: --recursive {FILE}"
                )
                w(f"  {msg}", "warn")
                return self._fail(msg)

        if not Path(av_exe).exists() and not shutil.which(av_exe):
            msg = f"AV-program ikke funnet: {av_exe}"
            w(f"  {msg}", "warn")
            return self._fail(msg)

        # Bygg argumentliste
        try:
            av_args: list[str] = shlex.split(av_args_raw) if av_args_raw else []
        except ValueError as exc:
            msg = f"Ugyldig argumentstreng: {exc}"
            w(f"  {msg}", "warn")
            return self._fail(msg)

        # ── Bestem skannemål ─────────────────────────────────────────────
        tmp_dir: Path | None = None
        pipeline_dir = getattr(ctx, "extracted_path", None)

        if scan_target == "folder" and pipeline_dir and pipeline_dir.is_dir():
            # Pipeline-modus: bruk allerede utpakket mappe direkte
            target_path = str(pipeline_dir)
            all_files   = [f for f in pipeline_dir.rglob("*") if f.is_file()]
            w(f"  Bruker utpakket pipeline-mappe: {pipeline_dir}", "info")
            w(f"  {len(all_files):,} filer klar for skanning.", "info")
            log_lines.append(
                f"Skannemål       : pipeline-mappe ({len(all_files):,} filer)")

        elif scan_target == "folder":
            # Pakk ut SIARD til temp-mappe
            global_temp = ""
            try:
                from settings import get_config as _gc
                global_temp = _gc("global_temp_dir", "").strip()
            except Exception:
                pass
            tmp_parent = (Path(global_temp)
                          if global_temp and Path(global_temp).is_dir() else None)
            tmp_dir = Path(tempfile.mkdtemp(prefix="siard_av_", dir=tmp_parent))

            try:
                with zipfile.ZipFile(ctx.siard_path, "r") as zf:
                    members = zf.namelist()
                    w(f"  Pakker ut {len(members):,} filer til {tmp_dir} ...", "info")
                    zf.extractall(tmp_dir)
            except Exception as exc:
                if tmp_dir:
                    shutil.rmtree(str(tmp_dir), ignore_errors=True)
                return self._fail(f"Kunne ikke pakke ut SIARD: {exc}")

            target_path = str(tmp_dir)
            all_files = [f for f in tmp_dir.rglob("*") if f.is_file()]
            w(f"  {len(all_files):,} filer klar for skanning.", "info")
            log_lines.append(f"Skannemål       : utpakket mappe ({len(all_files):,} filer)")
        else:
            target_path = str(ctx.siard_path)
            all_files   = [ctx.siard_path]
            log_lines.append(f"Skannemål       : SIARD-fil direkte")

        # ── Bygg endelig kommando ─────────────────────────────────────────
        resolved_args = _resolve_args(av_args, target_path)
        cmd = [av_exe] + resolved_args

        # Hvis {FILE} ikke var i args, legg til skannemålet på slutten
        if "{FILE}" not in av_args_raw and target_path not in resolved_args:
            cmd.append(target_path)

        scan_start = datetime.datetime.now()
        log_lines += [
            f"AV-program      : {av_exe}",
            f"Kommando        : {subprocess.list2cmdline(cmd)}",
            f"Infisert-rc     : {infected_rc}",
            f"SIARD-arkiv     : {ctx.siard_path}",
            f"Tidspunkt start : {scan_start}",
            "",
        ]
        w(f"  AV-program: {av_exe}", "info")
        w(f"  Kommando: {subprocess.list2cmdline(cmd)}", "info")
        w("  Skanner — ingen tidsgrense, vennligst vent ...", "info")

        pcb       = ctx.metadata.get("progress_cb")
        stop_ev   = ctx.metadata.get("stop_event")

        # ── Kjør AV med live stdout-strømming ────────────────────────────
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except (FileNotFoundError, PermissionError, OSError) as exc:
            msg = f"Kunne ikke starte AV-program: {exc}"
            log_lines.append(f"FEIL: {msg}")
            self._write_av_log(log_dir, siard_name, log_lines)
            if tmp_dir and not self.params.get("keep_temp"):
                shutil.rmtree(str(tmp_dir), ignore_errors=True)
            return self._fail(msg, data={"log": log_lines})

        # Samle stderr i bakgrunnstråd for å unngå deadlock
        stderr_collected: list[str] = []

        def _read_stderr():
            if proc.stderr:
                for raw in proc.stderr:
                    line = raw.decode("utf-8", errors="replace").rstrip()
                    if line:
                        stderr_collected.append(line)

        stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
        stderr_thread.start()

        # Stream stdout linje for linje — gi tilbakemelding underveis
        stdout_lines: list[str] = []
        PROGRESS_INTERVAL = 5.0   # sekunder mellom fremdriftsmeldinger
        last_progress_t   = time.time()
        t_start           = time.time()
        aborted           = False

        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip()
            if not line:
                continue
            stdout_lines.append(line)

            # Vis alltid linjer med funn/feil umiddelbart
            line_lower = line.lower()
            if any(kw in line_lower for kw in (
                "found", "infected", "virus", "trojan", "malware",
                "threat", "funn", "trussel", "error", "failed",
            )):
                w(f"  ⚠  {line}", "warn")

            # Periodisk fremdriftsmelding
            now = time.time()
            if now - last_progress_t >= PROGRESS_INTERVAL:
                elapsed = int(now - t_start)
                m, s = divmod(elapsed, 60)
                tid  = f"{m}m {s:02d}s" if m else f"{s}s"
                w(f"  Skanner … {len(stdout_lines):,} linjer lest  ({tid} elapsed)",
                  "muted")
                if pcb:
                    pcb("scan_progress",
                        lines=len(stdout_lines), elapsed=elapsed)
                last_progress_t = now

            # Bruker trykket Stopp
            if stop_ev and stop_ev.is_set():
                proc.terminate()
                aborted = True
                break

        proc.wait()
        stderr_thread.join(timeout=10)

        if aborted:
            msg = "Skanning avbrutt av bruker"
            log_lines.append(f"AVBRUTT: {msg}")
            self._write_av_log(log_dir, siard_name, log_lines)
            if tmp_dir and not self.params.get("keep_temp"):
                shutil.rmtree(str(tmp_dir), ignore_errors=True)
            return self._fail(msg, data={"log": log_lines})

        stderr_lines = stderr_collected

        elapsed_total = int(time.time() - t_start)
        m, s = divmod(elapsed_total, 60)
        tid  = f"{m}m {s:02d}s" if m else f"{s}s"
        w(f"  Skanning fullført på {tid}  ({len(stdout_lines):,} linjer)", "info")

        if stdout_lines:
            log_lines += ["=== AV-OUTPUT (stdout) ==="] + stdout_lines + [""]
        if stderr_lines:
            log_lines += ["=== AV-FEILMELDINGER (stderr) ==="] + stderr_lines + [""]
        log_lines.append(f"Returkode       : {proc.returncode}")
        log_lines.append(f"Tidspunkt slutt : {datetime.datetime.now()}")

        w(f"  Returkode: {proc.returncode}", "info")

        # Sjekk om output inneholder feilindikatorer (gjelder særlig Windows Defender
        # som returnerer kode 2 for BÅDE "trusler funnet" og "skanningsfeil").
        all_output = ("\n".join(stdout_lines) + "\n".join(stderr_lines)).lower()
        error_in_output = any(kw in all_output for kw in (
            "failed with hr", "hr = 0x", "cmdtool: failed", "error",
        ))

        # Vis relevante linjer i GUI-loggen
        funn_linjer = [l for l in stdout_lines if any(
            kw in l.lower() for kw in
            ("found", "infected", "virus", "trojan", "malware", "threat",
             "infisert", "trussel", "funn", "detected")
        )]

        # En returkode lik infected_rc er bare et reelt funn hvis
        # output inneholder trusselnøkkelord ELLER ikke inneholder feilindikatorer.
        infected = (
            proc.returncode == infected_rc
            and (bool(funn_linjer) or not error_in_output)
        )
        scan_failed = (proc.returncode != 0 and not infected)
        if funn_linjer:
            w("  === FUNN ===", "warn")
            for linje in funn_linjer[:20]:
                w(f"  {linje}", "warn")
        else:
            for linje in stdout_lines[-10:]:
                w(f"  {linje}", "info")
        if stderr_lines:
            for linje in stderr_lines[:5]:
                w(f"  stderr: {linje}", "warn")

        if scan_failed:
            resultat_tekst = f"SKANNINGSFEIL (returkode {proc.returncode})"
        elif infected:
            resultat_tekst = "TRUSLER FUNNET"
        else:
            resultat_tekst = "Ingen trusler funnet"

        log_lines += [
            "",
            f"RESULTAT: {resultat_tekst}",
            f"Filer skannet   : {len(all_files):,}",
        ]

        av_log_path = self._write_av_log(log_dir, siard_name, log_lines)
        if av_log_path:
            w(f"  AV-loggfil: {av_log_path}", "info")

        # Rydd temp
        if tmp_dir:
            if self.params.get("keep_temp"):
                w(f"  Utpakket mappe beholdt: {tmp_dir}", "info")
            else:
                shutil.rmtree(str(tmp_dir), ignore_errors=True)

        ctx.set_flag("virus_found", infected)
        data = {
            "infected":      infected,
            "scan_failed":   scan_failed,
            "returncode":    proc.returncode,
            "files_scanned": len(all_files),
            "av_executable": av_exe,
            "scan_target":   scan_target,
            "log":           log_lines,
            "av_log_path":   str(av_log_path) if av_log_path else "",
        }
        ctx.set_result("virus_scan_detail", data)

        if scan_failed:
            msg = (
                f"Skanningen feilet (returkode {proc.returncode}). "
                "Mulige årsaker: manglende administratorrettigheter, "
                "AV-tjenesten kjører ikke, eller ugyldig skannemål."
            )
            w(f"  {msg}", "warn")
            return self._fail(msg, data=data)

        if infected:
            msg = (f"TRUSLER FUNNET ({len(all_files):,} filer skannet, "
                   f"{len(funn_linjer)} funn)")
            w(f"  {msg}", "warn")
            return self._fail(msg, data=data)

        msg = f"Ingen trusler funnet ({len(all_files):,} filer skannet)"
        w(f"  {msg}", "ok")
        return self._ok(data=data, message=msg)
