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
    """Modal dialog som viser info om ny versjon og tilbyr nedlasting."""

    def __init__(self, parent, current_ver: str, new_ver: str,
                 release_name: str, body: str, download_url: str):
        super().__init__(parent)
        self.title("Ny versjon tilgjengelig")
        self.resizable(False, False)
        self.grab_set()
        self.lift()
        self.focus_force()
        self._download_url = download_url

        W = 540
        self.geometry(f"{W}x520")
        self.configure(fg_color=COLORS.get("surface", "#1e1e2e"))

        # ── Overskrift ────────────────────────────────────────────────────
        header = ctk.CTkFrame(self, fg_color=COLORS.get("primary", "#1a3a6b"),
                              corner_radius=0, height=70)
        header.pack(fill="x")
        header.pack_propagate(False)

        ctk.CTkLabel(
            header,
            text="  Ny versjon tilgjengelig",
            font=ctk.CTkFont(size=15, weight="bold"),
            text_color="#ffffff",
            anchor="w",
        ).pack(side="left", padx=16, pady=16)

        # ── Versjonslinje ─────────────────────────────────────────────────
        ver_frame = ctk.CTkFrame(self, fg_color="transparent")
        ver_frame.pack(fill="x", padx=20, pady=(14, 4))

        ctk.CTkLabel(ver_frame, text=f"Installert versjon:  v{current_ver}",
                     font=ctk.CTkFont(size=12), text_color="#aaaaaa",
                     anchor="w").pack(anchor="w")
        ctk.CTkLabel(ver_frame,
                     text=f"Ny versjon:              v{new_ver}  —  {release_name}",
                     font=ctk.CTkFont(size=12, weight="bold"),
                     text_color=COLORS.get("accent", "#4a9eff"),
                     anchor="w").pack(anchor="w", pady=(2, 0))

        ctk.CTkFrame(self, height=1,
                     fg_color=COLORS.get("border", "#333355")).pack(fill="x",
                                                                     padx=20, pady=8)

        # ── Endringslogg ──────────────────────────────────────────────────
        ctk.CTkLabel(self, text="Endringslogg:",
                     font=ctk.CTkFont(size=11, weight="bold"),
                     text_color="#cccccc", anchor="w").pack(anchor="w", padx=20)

        text_box = ctk.CTkTextbox(
            self,
            height=260,
            font=ctk.CTkFont(size=11, family="Consolas"),
            fg_color=COLORS.get("bg", "#13131f"),
            text_color="#dddddd",
            wrap="word",
            corner_radius=6,
        )
        text_box.pack(fill="both", expand=True, padx=20, pady=(4, 8))
        text_box.insert("0.0", body.strip() if body else "(ingen endringslogg tilgjengelig)")
        text_box.configure(state="disabled")

        # ── Knapper ───────────────────────────────────────────────────────
        btn_frame = ctk.CTkFrame(self, fg_color="transparent")
        btn_frame.pack(fill="x", padx=20, pady=(0, 16))

        ctk.CTkButton(
            btn_frame,
            text="Last ned ny versjon",
            font=ctk.CTkFont(size=12, weight="bold"),
            width=200, height=36,
            command=self._open_download,
        ).pack(side="left")

        ctk.CTkButton(
            btn_frame,
            text="Lukk",
            font=ctk.CTkFont(size=12),
            width=100, height=36,
            fg_color="transparent",
            border_width=1,
            border_color=COLORS.get("border", "#333355"),
            command=self.destroy,
        ).pack(side="right")

        # Midtstill over foreldrevinduet
        self.update_idletasks()
        px = parent.winfo_rootx() + (parent.winfo_width()  - W)    // 2
        py = parent.winfo_rooty() + (parent.winfo_height() - 520) // 2
        self.geometry(f"{W}x520+{px}+{py}")

    def _open_download(self):
        webbrowser.open(self._download_url)
        self.destroy()


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
            parent_widget, current_version, tag.lstrip("vV"), name, body, html
        ))

    def _show_dialog(parent, cur, new_ver, rel_name, body, url):
        try:
            dlg = UpdateDialog(parent, cur, new_ver, rel_name, body, url)
            dlg.wait_window()
        except Exception:
            pass

    t = threading.Thread(target=_worker, daemon=True, name="update-checker")
    t.start()
