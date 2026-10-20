"""
gui/update_checker.py
---------------------
Sjekker GitHub Releases API for ny versjon av SIARD Workflow Manager.
Kjøres i bakgrunnstråd ved oppstart; viser dialog i GUI-tråden hvis ny versjon finnes.
"""
from __future__ import annotations

import re
import threading
import webbrowser
from typing import Callable

import customtkinter as ctk

from gui.styles import COLORS, FONTS

GITHUB_REPO     = "smult/SIARD-Workflow-Manager"
API_URL         = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
RELEASES_URL    = f"https://github.com/{GITHUB_REPO}/releases/latest"


# ─────────────────────────────────────────────────────────────────────────────
# Versjonssammenligning
# ─────────────────────────────────────────────────────────────────────────────

def _parse_version(v: str) -> tuple[int, ...]:
    """Konverterer '1.2.3' eller 'v1.2.3' til (1, 2, 3)."""
    v = v.lstrip("vV").strip()
    parts = re.split(r"[.\-]", v)
    result = []
    for p in parts:
        try:
            result.append(int(p))
        except ValueError:
            break
    return tuple(result) if result else (0,)


def _is_newer(remote: str, local: str) -> bool:
    return _parse_version(remote) > _parse_version(local)


# ─────────────────────────────────────────────────────────────────────────────
# Nettverksforespørsel (kjøres i bakgrunnstråd)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_latest() -> dict | None:
    """Henter siste release fra GitHub API. Returnerer dict eller None ved feil."""
    try:
        import urllib.request
        import json
        req = urllib.request.Request(
            API_URL,
            headers={"User-Agent": "SIARD-Workflow-Manager-updater/1.0"},
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Dialog
# ─────────────────────────────────────────────────────────────────────────────

class UpdateDialog(ctk.CTkToplevel):
    """
    Ny versjon: endringslogg + oppdatering.

    Programpakke (SIARDManager.exe): «Last ned og installer» laster ned,
    kontrollerer sjekksum og selvtester den nye versjonen, og ber deretter om
    bekreftelse før programmet lukkes og byttes ut (core/self_update.py).
    Kildekode i ren git-kopi: «Oppdater (git pull)». Ellers: nedlastingsside.
    """

    def __init__(self, parent, current_ver: str, new_ver: str,
                 release_name: str, body: str, download_url: str,
                 release: "dict | None" = None):
        super().__init__(parent)
        self.title("Ny versjon tilgjengelig")
        self.resizable(False, False)
        self.grab_set()
        self.lift()
        self.focus_force()
        self._app = parent
        self._download_url = download_url
        self._release = release or {}
        self._new_ver = new_ver
        self._busy = False
        self._cancel = False
        from siard_workflow.core import self_update as SU
        self._mode, self._mode_reason = SU.mode(self._release if release else None)

        W, H = 560, 600
        self.geometry(f"{W}x{H}")
        self.configure(fg_color=COLORS["surface"])

        header = ctk.CTkFrame(self, fg_color=COLORS.get("primary", "#1a3a6b"),
                              corner_radius=0, height=70)
        header.pack(fill="x")
        header.pack_propagate(False)
        ctk.CTkLabel(header, text="  Ny versjon tilgjengelig",
                     font=ctk.CTkFont(size=15, weight="bold"),
                     text_color="#ffffff", anchor="w").pack(side="left", padx=16, pady=16)

        ver_frame = ctk.CTkFrame(self, fg_color="transparent")
        ver_frame.pack(fill="x", padx=20, pady=(14, 4))
        ctk.CTkLabel(ver_frame, text=f"Installert versjon:  v{current_ver}",
                     font=ctk.CTkFont(size=12), text_color=COLORS["muted"],
                     anchor="w").pack(anchor="w")
        ctk.CTkLabel(ver_frame,
                     text=f"Ny versjon:              v{new_ver}  —  {release_name}",
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=COLORS.get("accent", "#4a9eff"),
                     anchor="w").pack(anchor="w", pady=(2, 0))

        ctk.CTkFrame(self, height=1, fg_color=COLORS.get("border", "#333355")).pack(
            fill="x", padx=20, pady=8)

        # Knapper og fremdrift nederst (pakkes før tekstboksen så de alltid vises)
        btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        btn_frame.pack(side="bottom", fill="x", padx=20, pady=(0, 16))
        self._status = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=11),
                                    text_color=COLORS["muted"], anchor="w", justify="left",
                                    wraplength=W - 40)
        self._status.pack(side="bottom", fill="x", padx=20, pady=(0, 4))
        self._bar = ctk.CTkProgressBar(self, height=10)
        self._bar.set(0)

        ctk.CTkLabel(self, text="Endringslogg:", font=ctk.CTkFont(size=11, weight="bold"),
                     text_color=COLORS["text"], anchor="w").pack(anchor="w", padx=20)
        text_box = ctk.CTkTextbox(self, height=220,
                                  font=ctk.CTkFont(size=11, family="Consolas"),
                                  fg_color=COLORS["bg"], border_color=COLORS["border"], border_width=1,
                                  text_color=COLORS["text"], wrap="word", corner_radius=6)
        text_box.pack(fill="both", expand=True, padx=20, pady=(4, 8))
        text_box.insert("0.0", body.strip() if body else "(ingen endringslogg tilgjengelig)")
        text_box.configure(state="disabled")

        self._buttons: list = []
        if self._mode == "pakke":
            self._main_btn = self._button(btn_frame, "Last ned og installer",
                                          self._start_package, primary=True)
        elif self._mode == "git":
            self._main_btn = self._button(btn_frame, "Oppdater (git pull)",
                                          self._start_git, primary=True)
        else:
            self._main_btn = self._button(btn_frame, "Last ned ny versjon",
                                          self._open_download, primary=True)
            if self._mode_reason:
                self._status.configure(text=f"Automatisk oppdatering ikke mulig: "
                                            f"{self._mode_reason}.")
        self._main_btn.pack(side="left")
        if self._mode != "manuell":
            b = self._button(btn_frame, "Nedlastingsside", self._open_download)
            b.pack(side="left", padx=8)
        self._close_btn = self._button(btn_frame, "Lukk", self._close)
        self._close_btn.pack(side="right")
        self.protocol("WM_DELETE_WINDOW", self._close)

        self.update_idletasks()
        px = parent.winfo_rootx() + (parent.winfo_width() - W) // 2
        py = parent.winfo_rooty() + (parent.winfo_height() - H) // 2
        self.geometry(f"{W}x{H}+{px}+{py}")

    # ── hjelpere ─────────────────────────────────────────────────────────────

    def _button(self, master, text, cmd, primary=False):
        kw = dict(text=text, command=cmd, height=36, font=ctk.CTkFont(size=12,
                  weight="bold" if primary else "normal"))
        if primary:
            kw.update(fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      text_color=COLORS["on_accent"])
        else:
            kw.update(fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      text_color=COLORS["btn_text"], border_width=1,
                      border_color=COLORS["btn_border"])
        b = ctk.CTkButton(master, **kw)
        self._buttons.append(b)
        return b

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        for b in self._buttons:
            if b is not self._close_btn:
                b.configure(state="disabled" if busy else "normal")
        self._close_btn.configure(text="Avbryt" if busy else "Lukk")

    def _say(self, msg: str, error: bool = False) -> None:
        self.after(0, lambda: self._status.configure(
            text=msg, text_color=COLORS["red"] if error else COLORS["muted"]))

    def _progress(self, done: int, total: int) -> None:
        def upd():
            if total:
                self._bar.set(done / total)
                self._status.configure(
                    text=f"Laster ned … {done / 1e6:.0f} av {total / 1e6:.0f} MB")
        self.after(0, upd)

    def _app_busy(self) -> bool:
        return bool(getattr(self._app, "_running", False)
                    or getattr(self._app, "_preflight_active", False))

    def _close(self) -> None:
        if self._busy:
            self._cancel = True
            self._say("Avbryter …")
            return
        self.destroy()

    def _open_download(self):
        webbrowser.open(self._download_url)
        self.destroy()

    # ── programpakke ─────────────────────────────────────────────────────────

    def _start_package(self) -> None:
        import tempfile
        from pathlib import Path
        from siard_workflow.core import self_update as SU
        self._set_busy(True)
        self._cancel = False
        self._bar.pack(side="bottom", fill="x", padx=20, pady=(0, 6), before=self._status)
        work = Path(tempfile.mkdtemp(prefix="siard_oppdatering_"))
        result: dict = {}

        def worker():
            try:
                result["app"] = SU.prepare_package(
                    self._release, work, progress=self._progress,
                    stop=lambda: self._cancel, status=self._say)
            except Exception as exc:
                result["err"] = str(exc)
            self.after(0, lambda: self._package_ready(result, work))

        threading.Thread(target=worker, daemon=True, name="oppdatering").start()

    def _package_ready(self, result: dict, work) -> None:
        import shutil
        from tkinter import messagebox
        from siard_workflow.core import self_update as SU
        self._set_busy(False)
        if "err" in result:
            shutil.rmtree(work, ignore_errors=True)
            self._say(f"Oppdateringen ble ikke installert: {result['err']}", error=True)
            return
        self._bar.set(1)
        if self._app_busy():
            shutil.rmtree(work, ignore_errors=True)
            self._say("En kjøring pågår — installer oppdateringen etter at den er ferdig "
                      "(start programmet på nytt, eller vent på neste sjekk).", error=True)
            return
        ok = messagebox.askyesno(
            "Installer ny versjon",
            f"Versjon {self._new_ver} er lastet ned, kontrollert og selvtestet.\n\n"
            "Installere nå? Programmet lukkes og starter på nytt med den nye "
            "versjonen. Innstillinger, logger og komponenter beholdes.\n\n"
            "(Nei = ingenting endres.)", parent=self)
        if not ok:
            shutil.rmtree(work, ignore_errors=True)
            self._say("Installasjonen ble avbrutt — ingenting er endret.")
            return
        self._say("Starter installasjonen …")
        self.update_idletasks()
        try:
            SU.launch_swap(result["app"])      # returnerer når skriptet kjører
        except Exception as exc:
            shutil.rmtree(work, ignore_errors=True)
            self._say(f"Kunne ikke starte installasjonen: {exc}", error=True)
            return
        self._say("Installerer — programmet lukkes og starter på nytt …")
        self.after(600, self._exit_app)

    def _exit_app(self) -> None:
        import os
        try:
            self._app.destroy()
        finally:
            os._exit(0)

    # ── kildekode (git) ──────────────────────────────────────────────────────

    def _start_git(self) -> None:
        from tkinter import messagebox
        from siard_workflow.core import self_update as SU
        if not messagebox.askyesno(
                "Oppdater fra git",
                f"Hente versjon {self._new_ver} med «git pull» i programmappa?\n\n"
                "Programmet startes på nytt etterpå.", parent=self):
            return
        self._set_busy(True)
        self._say("Henter oppdatering (git pull) …")
        result: dict = {}

        def worker():
            try:
                result["ok"], result["msg"] = SU.git_pull()
            except Exception as exc:
                result["ok"], result["msg"] = False, str(exc)
            self.after(0, lambda: self._git_done(result))

        threading.Thread(target=worker, daemon=True, name="git-pull").start()

    def _git_done(self, result: dict) -> None:
        from tkinter import messagebox
        self._set_busy(False)
        if not result.get("ok"):
            self._say(f"git pull feilet: {result.get('msg', '')}", error=True)
            return
        self._say("Oppdatert. " + result.get("msg", "")[-200:])
        if self._app_busy():
            self._say("Oppdatert — start programmet på nytt etter at kjøringen er ferdig.")
            return
        if messagebox.askyesno("Start på nytt", "Oppdateringen er hentet. Starte "
                               "programmet på nytt nå?", parent=self):
            restart = getattr(self._app, "_restart_app", None)
            if callable(restart):
                restart()


# ─────────────────────────────────────────────────────────────────────────────
# Offentlig API
# ─────────────────────────────────────────────────────────────────────────────

def check_for_updates(parent_widget, current_version: str,
                      silent_if_uptodate: bool = True) -> None:
    """
    Sjekker GitHub for ny versjon i bakgrunnen.
    Hvis ny versjon finnes, vises UpdateDialog i GUI-tråden.

    Parameters
    ----------
    parent_widget       : CTk-widget (brukes for dialog-plassering)
    current_version     : gjeldende versjon, f.eks. '1.1.9'
    silent_if_uptodate  : ikke vis noe hvis versjonen er oppdatert
    """
    def _worker():
        data = _fetch_latest()
        if not data:
            return

        tag     = data.get("tag_name", "")
        name    = data.get("name", tag)
        body    = data.get("body", "")
        html    = data.get("html_url", RELEASES_URL)

        if not _is_newer(tag, current_version):
            return   # already up to date

        # Planlegg dialog i GUI-tråden
        parent_widget.after(0, lambda: _show_dialog(
            parent_widget, current_version, tag.lstrip("vV"), name, body, html, data
        ))

    def _show_dialog(parent, cur, new_ver, rel_name, body, url, release):
        try:
            dlg = UpdateDialog(parent, cur, new_ver, rel_name, body, url, release)
            dlg.wait_window()
        except Exception:
            pass

    t = threading.Thread(target=_worker, daemon=True, name="update-checker")
    t.start()
