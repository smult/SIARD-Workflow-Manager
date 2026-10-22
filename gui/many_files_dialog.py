"""
gui/many_files_dialog.py

Forhåndskontrollens varsel ved uttrekk med svært mange filer: antivirus,
mekanisk disk og korte 8.3-filnavn (se siard_workflow/core/av_check.py).

8.3-delen viser status for temp-disken, kommandoen som slår 8.3-navn av, og
knapper for å kjøre den i et administrator-vindu (Windows spør om tillatelse)
eller kopiere den. Etter kjøringen sjekkes disken på nytt.

result: "ja" (fortsett) | "nei" (fortsett, ikke vis igjen) | "avbryt"
"""
from __future__ import annotations

import threading
from typing import Callable, Optional

import customtkinter as ctk

from gui.styles import COLORS, FONTS


class ManyFilesDialog(ctk.CTkToplevel):

    def __init__(self, parent, message: str, heading: str,
                 short_names: bool = False, drive: str = "",
                 commands: "Optional[list[str]]" = None,
                 run_elevated: "Optional[Callable[[list[str]], tuple]]" = None,
                 recheck: "Optional[Callable[[], Optional[bool]]]" = None):
        super().__init__(parent)
        self.result = "avbryt"
        self.title("Utpakking av mange filer")
        self.configure(fg_color=COLORS["bg"])
        self.resizable(False, False)
        self.grab_set()
        self.lift()
        self._commands = commands or []
        self._run_elevated = run_elevated
        self._recheck = recheck
        self._drive = drive
        w, wrap = 640, 600

        header = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        header.pack(side="top", fill="x")
        ctk.CTkLabel(header, text=f"  {heading}",
                     font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
                     text_color=COLORS["accent"], anchor="w").pack(side="left", padx=12, pady=10)

        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.pack(side="bottom", fill="x", padx=16, pady=14)
        btns.grid_columnconfigure((0, 1, 2), weight=1)
        ctk.CTkButton(btns, text="Fortsett", height=34,
                      fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                      text_color=COLORS["on_accent"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                      command=self._on_ja).grid(row=0, column=0, padx=(0, 6), sticky="ew")
        ctk.CTkButton(btns, text="Fortsett, ikke vis igjen", height=34,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
                      border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self._on_nei).grid(row=0, column=1, padx=3, sticky="ew")
        ctk.CTkButton(btns, text="Avbryt", height=34,
                      fg_color=COLORS["danger_bg"], hover_color=COLORS["danger_hover"],
                      text_color=COLORS["danger_text"], border_color=COLORS["danger_border"],
                      border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self._on_avbryt).grid(row=0, column=2, padx=(6, 0), sticky="ew")

        body = ctk.CTkScrollableFrame(self, fg_color="transparent")
        body.pack(side="top", fill="both", expand=True)
        msg = ctk.CTkLabel(body, text=message, wraplength=wrap, justify="left", anchor="nw",
                           font=ctk.CTkFont(family=FONTS["mono"], size=11),
                           text_color=COLORS["text"])
        msg.pack(padx=16, pady=(14, 6), fill="x", anchor="nw")

        self._status = None
        if short_names and self._commands:
            box = ctk.CTkFrame(body, fg_color=COLORS["panel"], corner_radius=8)
            box.pack(padx=16, pady=(4, 10), fill="x")
            ctk.CTkLabel(box, text=f"Korte 8.3-filnavn er PÅ for {drive}",
                         font=ctk.CTkFont(family=FONTS["mono"], size=12, weight="bold"),
                         text_color=COLORS["text"], anchor="w").pack(fill="x", padx=12, pady=(10, 2))
            ctk.CTkLabel(box, wraplength=wrap - 40, justify="left", anchor="w",
                         text=("Slå det av før utpakkingen — det gjelder nye filer, så "
                               "SIARD-filer som pakkes ut etterpå, får full hastighet. "
                               "Krever administrator. Kommando:"),
                         font=ctk.CTkFont(family=FONTS["mono"], size=11),
                         text_color=COLORS["text"]).pack(fill="x", padx=12)
            cmd = ctk.CTkTextbox(box, height=22 + 18 * len(self._commands), wrap="none",
                                 font=ctk.CTkFont(family=FONTS["mono"], size=12),
                                 fg_color=COLORS["bg"], text_color=COLORS["text"])
            cmd.insert("1.0", "\n".join(self._commands))
            cmd.configure(state="disabled")
            cmd.pack(fill="x", padx=12, pady=6)
            row = ctk.CTkFrame(box, fg_color="transparent")
            row.pack(fill="x", padx=12, pady=(0, 6))
            self._run_btn = ctk.CTkButton(
                row, text="Slå av 8.3-navn (administrator) …", height=30,
                fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                text_color=COLORS["on_accent"],
                font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                command=self._start_elevated)
            self._run_btn.pack(side="left")
            ctk.CTkButton(row, text="Kopier kommando", height=30,
                          fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                          text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
                          border_width=1, font=ctk.CTkFont(family=FONTS["mono"], size=11),
                          command=self._copy).pack(side="left", padx=8)
            self._status = ctk.CTkLabel(box, text="", anchor="w", wraplength=wrap - 40,
                                        justify="left",
                                        font=ctk.CTkFont(family=FONTS["mono"], size=11),
                                        text_color=COLORS["muted"])
            self._status.pack(fill="x", padx=12, pady=(0, 10))

        self.protocol("WM_DELETE_WINDOW", self._on_avbryt)
        self.update_idletasks()
        need = header.winfo_reqheight() + msg.winfo_reqheight() + btns.winfo_reqheight() + 80
        if self._status is not None:
            need += 230
        h = max(300, min(need, int(self.winfo_screenheight() * 0.85)))
        try:
            x = parent.winfo_x() + (parent.winfo_width() - w) // 2
            y = parent.winfo_y() + (parent.winfo_height() - h) // 2
        except Exception:
            x = y = 100
        self.geometry(f"{w}x{h}+{max(0, x)}+{max(0, y)}")

    # ── 8.3 ──────────────────────────────────────────────────────────────────

    def _say(self, text: str, color: str = "muted") -> None:
        if self._status is not None:
            self._status.configure(text=text, text_color=COLORS.get(color, COLORS["muted"]))

    def _copy(self) -> None:
        self.clipboard_clear()
        self.clipboard_append("\n".join(self._commands))
        self._say("Kommandoen er kopiert. Lim den inn i PowerShell eller ledetekst "
                  "startet som administrator.")

    def _start_elevated(self) -> None:
        if not self._run_elevated:
            return
        self._run_btn.configure(state="disabled")
        self._say("Windows spør om tillatelse — godkjenn, og lukk administrator-vinduet "
                  "når kommandoen er ferdig …", "text")

        box: dict = {}

        def work():
            ok, msg = self._run_elevated(self._commands)
            state = self._recheck() if (ok and self._recheck) else None
            box["res"] = (ok, msg, state)     # ingen Tk-kall fra tråden

        def poll():
            if not self.winfo_exists():
                return
            if "res" in box:
                self._done(*box["res"])
            else:
                self.after(200, poll)
        threading.Thread(target=work, daemon=True, name="8dot3").start()
        self.after(200, poll)

    def _done(self, ok: bool, msg: str, short_names_now: "Optional[bool]") -> None:
        if not self.winfo_exists():
            return
        self._run_btn.configure(state="normal")
        if not ok:
            self._say(f"Ikke utført: {msg}. Du kan kopiere kommandoen og kjøre den selv "
                      "som administrator.", "red")
        elif short_names_now is False:
            self._say(f"✓ Korte 8.3-filnavn er nå AV for {self._drive} — nye filer "
                      "opprettes uten.", "green")
            self._run_btn.configure(state="disabled")
        elif short_names_now is True:
            self._say(f"Kommandoen er kjørt, men {self._drive} lager fortsatt 8.3-navn. "
                      "Se meldingen i administrator-vinduet.", "red")
        else:
            self._say("Kommandoen er kjørt.", "text")

    # ── knapper ──────────────────────────────────────────────────────────────

    def _on_ja(self):
        self.result = "ja"
        self.destroy()

    def _on_nei(self):
        self.result = "nei"
        self.destroy()

    def _on_avbryt(self):
        self.result = "avbryt"
        self.destroy()
