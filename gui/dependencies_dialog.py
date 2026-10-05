"""gui/dependencies_dialog.py

Oversikt over programvare programmet bruker (Python-pakker og eksterne
programmer), hva som ikke virker når noe mangler, og installasjon med
bekreftelse. Se dependencies.py for registeret.

Moduser
  "startup"   — ved oppstart, kun det som mangler; «ikke vis igjen»-valg
  "overview"  — fra Innstillinger, alt med status
  "preflight" — før kjøring: hva arbeidsflyten mangler; result = "fortsett" | "stopp"
"""
from __future__ import annotations

import threading
import tkinter.messagebox as _mb

import customtkinter as ctk

import dependencies as deps
from gui.styles import COLORS, FONTS

_LEVEL_TEXT = {deps.REQUIRED: "påkrevd", deps.STANDARD: "standard", deps.OPTIONAL: "valgfri"}


def _font(size=11, bold=False):
    return ctk.CTkFont(family=FONTS["mono"], size=size, weight="bold" if bold else "normal")


class DependenciesDialog(ctk.CTkToplevel):

    def __init__(self, parent, statuses: "list[deps.DepStatus]", *, mode: str = "overview",
                 impacts: "list[deps.OpImpact] | None" = None, ops: "list | None" = None,
                 app=None, autocheck: bool = False):
        super().__init__(parent)
        self._app = app or parent
        self._mode = mode
        self._statuses = statuses
        self._impacts = impacts or []
        self._ops = ops or []
        self._busy = False
        self.result = "stopp" if mode == "preflight" else "lukk"
        self.dont_show_again = False

        titles = {"startup": "Manglende programvare",
                  "overview": "Programvare og avhengigheter",
                  "preflight": "Arbeidsflyten mangler programvare"}
        self.title(titles.get(mode, titles["overview"]))
        self.configure(fg_color=COLORS["bg"])
        self.transient(parent)
        self.grab_set()
        self.lift()

        header = ctk.CTkFrame(self, fg_color=COLORS["panel"], corner_radius=0)
        header.pack(side="top", fill="x")
        icon = "⚙" if mode == "overview" else "⚠"
        ctk.CTkLabel(header, text=f"  {icon}  {self.title()}", font=_font(13, True),
                     text_color=COLORS["text"] if mode == "overview" else COLORS["yellow"],
                     anchor="w").pack(side="left", padx=12, pady=10)

        # Knapper (nederst)
        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(side="bottom", fill="x", padx=16, pady=(6, 14))
        self._status_lbl = ctk.CTkLabel(self, text="", font=_font(11),
                                        text_color=COLORS["muted"], anchor="w",
                                        justify="left", wraplength=700)
        self._status_lbl.pack(side="bottom", fill="x", padx=18)
        self._dont_var = ctk.BooleanVar(value=False)
        if mode == "startup":
            ctk.CTkCheckBox(self, text="Ikke vis ved oppstart før noe endres "
                                       "(oversikten finnes under Innstillinger)",
                            variable=self._dont_var, font=_font(11),
                            text_color=COLORS["muted"]
                            ).pack(side="bottom", anchor="w", padx=18, pady=(0, 4))

        self._buttons: list = []
        if mode == "preflight":
            self._btn(btn_row, "Stopp kjøringen", self._stopp, primary=True).pack(
                side="left", padx=(0, 6))
            self._continue_btn = self._btn(btn_row, "Fortsett likevel", self._fortsett)
            self._continue_btn.pack(side="right")
        else:
            self._btn(btn_row, "Lukk", self._lukk, primary=True).pack(side="right")
            self._btn(btn_row, "Sjekk på nytt", self._recheck).pack(side="right", padx=6)
        self._pip_all_btn = self._btn(btn_row, "Installer alle manglende Python-pakker",
                                      self._install_all_pip)
        if not deps.FROZEN:              # pakket program: Python-pakkene følger med
            self._pip_all_btn.pack(side="left", padx=6)

        self._body = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self._body.pack(side="top", fill="both", expand=True, padx=4, pady=(6, 0))
        self._render()

        self.protocol("WM_DELETE_WINDOW", self._stopp if mode == "preflight" else self._lukk)
        self.bind("<Escape>", lambda _e: (self._stopp() if mode == "preflight" else self._lukk()))
        w = 780
        h = int(min(self.winfo_screenheight() * 0.85, 760 if mode == "overview" else 600))
        try:
            x = parent.winfo_x() + (parent.winfo_width() - w) // 2
            y = parent.winfo_y() + max(20, (parent.winfo_height() - h) // 2)
            self.geometry(f"{w}x{h}+{x}+{y}")
        except Exception:
            self.geometry(f"{w}x{h}")
        if autocheck:                    # programmer sjekkes i bakgrunnen (tar noen sekunder)
            self.after(50, self._recheck)

    # ── visning ──────────────────────────────────────────────────────────────

    def _btn(self, master, text, cmd, primary=False):
        if primary:
            b = ctk.CTkButton(master, text=text, command=cmd, height=32,
                              fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                              text_color=COLORS["on_accent"], font=_font(11, True))
        else:
            b = ctk.CTkButton(master, text=text, command=cmd, height=32,
                              fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                              text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
                              border_width=1, font=_font(11))
        self._buttons.append(b)
        return b

    def _visible(self) -> "list[deps.DepStatus]":
        if self._mode == "overview":
            return self._statuses
        if self._mode == "preflight":
            keys = {i.dep.key for i in self._impacts}
            return [s for s in self._statuses if s.key in keys]
        return deps.startup_problems(self._statuses)

    def _render(self) -> None:
        for c in self._body.winfo_children():
            c.destroy()
        self._buttons = [b for b in self._buttons if b.winfo_exists()]
        wrap = 690
        intro = {
            "startup": ("Noe av programvaren programmet bruker, mangler på denne maskinen. "
                        "Programmet kan brukes, men funksjonene som er nevnt nedenfor virker "
                        "ikke før det er installert."),
            "overview": ("Status for Python-pakker og eksterne programmer. «Standard» er del "
                         "av vanlig installasjon; «valgfri» gir ekstra funksjonalitet og har "
                         "reserveløsning."),
            "preflight": ("Operasjoner i arbeidsflyten trenger programvare som mangler. Du kan "
                          "installere nå, fortsette likevel eller stoppe kjøringen."),
        }[self._mode]
        ctk.CTkLabel(self._body, text=intro, font=_font(11), text_color=COLORS["text"],
                     wraplength=wrap, justify="left", anchor="w").pack(fill="x", padx=12,
                                                                       pady=(6, 8))
        if self._mode == "preflight":
            self._render_impacts(wrap)

        rows = self._visible()
        if not rows:
            ctk.CTkLabel(self._body, text="✓  Alt er på plass.", font=_font(12, True),
                         text_color=COLORS["green"], anchor="w").pack(fill="x", padx=12, pady=8)
        groups = (("Python-pakker", "pakke"), ("Eksterne programmer", "program"))
        for title, kind in groups:
            items = [s for s in rows if s.kind == kind]
            if not items:
                continue
            ctk.CTkLabel(self._body, text=title, font=_font(12, True),
                         text_color=COLORS["text_sub"], anchor="w").pack(fill="x", padx=12,
                                                                         pady=(10, 2))
            for s in items:
                self._render_row(s, wrap)

        missing_pip = [s for s in self._statuses if s.missing and s.action == "pip"
                       and (self._mode == "overview" or s in rows)]
        self._pip_all_btn.configure(state="normal" if missing_pip and not self._busy
                                    else "disabled")
        if self._mode == "preflight":
            still_blocking = any(i.blocking and self._current(i.dep.key).missing
                                 for i in self._impacts)
            self._continue_btn.configure(
                text="Fortsett likevel" if still_blocking else "Fortsett")

    def _render_impacts(self, wrap: int) -> None:
        box = ctk.CTkFrame(self._body, fg_color=COLORS["surface"], corner_radius=6)
        box.pack(fill="x", padx=12, pady=(0, 6))
        by_op: dict = {}
        for i in self._impacts:
            by_op.setdefault(i.op_label, []).append(i)
        for op_label, items in by_op.items():
            ctk.CTkLabel(box, text=f"«{op_label}»", font=_font(11, True),
                         text_color=COLORS["text"], anchor="w").pack(fill="x", padx=10,
                                                                     pady=(6, 0))
            for i in items:
                cur = self._current(i.dep.key)
                mark = "✓" if not cur.missing else ("✗" if i.blocking else "•")
                col = COLORS["green"] if not cur.missing else (
                    COLORS["red"] if i.blocking else COLORS["yellow"])
                txt = (f"  {mark} {i.dep.label}: "
                       + ("installert" if not cur.missing else
                          ("virker ikke / feil resultat" if i.blocking else "redusert funksjonalitet")))
                ctk.CTkLabel(box, text=txt, font=_font(11), text_color=col, anchor="w",
                             wraplength=wrap - 20, justify="left").pack(fill="x", padx=10)
        ctk.CTkFrame(box, height=6, fg_color="transparent").pack()

    def _render_row(self, s: "deps.DepStatus", wrap: int) -> None:
        row = ctk.CTkFrame(self._body, fg_color=COLORS["panel"], corner_radius=6)
        row.pack(fill="x", padx=12, pady=3)
        row.grid_columnconfigure(0, weight=1)
        if s.ok and s.in_use:
            mark, col = "✓", COLORS["green"]
        elif not s.in_use:
            mark, col = "–", COLORS["muted"]
        elif s.level == deps.OPTIONAL:
            mark, col = "•", COLORS["yellow"]
        else:
            mark, col = "✗", COLORS["red"]
        ctk.CTkLabel(row, text=f"{mark}  {s.label}   [{_LEVEL_TEXT.get(s.level, s.level)}]",
                     font=_font(11, True), text_color=col, anchor="w"
                     ).grid(row=0, column=0, sticky="w", padx=10, pady=(6, 0))
        ctk.CTkLabel(row, text=s.detail, font=_font(10), text_color=COLORS["muted"],
                     anchor="w", wraplength=wrap - 170, justify="left"
                     ).grid(row=1, column=0, sticky="w", padx=26)
        if s.missing or self._mode == "overview" or (s.bundled and not s.ok):
            txt = ("Uten: " if s.missing else "Brukes til: ") + s.affects
            if s.note:
                txt += f"\n{s.note}"
            ctk.CTkLabel(row, text=txt, font=_font(10), text_color=COLORS["text_sub"],
                         anchor="w", wraplength=wrap - 170, justify="left"
                         ).grid(row=2, column=0, sticky="w", padx=26, pady=(0, 6))
        actions = self._actions_for(s)
        if not actions:
            return
        acts = ctk.CTkFrame(row, fg_color="transparent")
        acts.grid(row=0, column=1, rowspan=3, sticky="e", padx=8, pady=6)
        for text, cmd in actions:
            b = self._btn(acts, text, cmd)
            b.configure(height=28, width=120)
            b.pack(side="top", pady=2)
            if self._busy:
                b.configure(state="disabled")

    def _actions_for(self, s: "deps.DepStatus") -> list:
        a: list = []
        if s.action == "pip" and s.missing and s.pip_spec:
            a.append(("Installer", lambda s=s: self._install_pip([s])))
        elif s.action == "component" and not s.ok:
            a.append(("Installer fra pakken" if s.bundled else "Last ned og installer",
                      lambda s=s: self._install_component(s)))
            if s.component == "libreoffice":
                a.append(("Pek ut …", self._browse_lo))
        elif s.action == "settings" and s.missing and self._mode != "overview":
            a.append(("Innstillinger", self._open_settings))
        if s.url and not s.ok and (s.action == "link" or not s.bundled):
            a.append(("Last ned", lambda u=s.url: self._open_url(u)))
        return a

    def _current(self, key: str) -> "deps.DepStatus":
        return next((s for s in self._statuses if s.key == key),
                    deps.DepStatus(key, "", key, deps.OPTIONAL, True))

    # ── handlinger ───────────────────────────────────────────────────────────

    def _set_busy(self, busy: bool, msg: str = "") -> None:
        self._busy = busy
        for b in self._buttons:
            try:
                if b.winfo_exists():
                    b.configure(state="disabled" if busy else "normal")
            except Exception:
                pass
        self._status_lbl.configure(text=msg, text_color=COLORS["muted"])

    def _run_bg(self, msg: str, work, done) -> None:
        """Kjør `work()` i bakgrunnen, deretter `done(resultat)` + ny sjekk i GUI-tråden."""
        self._set_busy(True, msg)

        def _t():
            try:
                res = work()
            except Exception as exc:
                res = exc
            try:
                statuses = deps.check_all()
            except Exception:
                statuses = self._statuses
            self.after(0, lambda: self._finish(res, statuses, done))

        threading.Thread(target=_t, daemon=True).start()

    def _finish(self, res, statuses, done) -> None:
        if not self.winfo_exists():
            return
        self._statuses = statuses
        self._set_busy(False, "")
        self._render()
        done(res)

    def _install_pip(self, items: "list[deps.DepStatus]") -> None:
        specs = [s.pip_spec for s in items]
        notes = "".join(f"\n\n{s.label}: {s.note}" for s in items if s.note)
        if not _mb.askyesno("Installer Python-pakker",
                            "Installere med pip (krever internett)?\n\n  "
                            + "\n  ".join(specs) + notes, parent=self):
            return

        def _done(res):
            ok, msg = res if isinstance(res, tuple) else (False, str(res))
            if ok:
                self._status_lbl.configure(text=f"Installert: {', '.join(specs)}",
                                           text_color=COLORS["green"])
            else:
                self._status_lbl.configure(text_color=COLORS["red"], text="Installasjon feilet")
                _mb.showerror("Installasjon feilet",
                              f"{msg}\n\nPrøv manuelt i et terminalvindu:\n"
                              f"{deps.manual_pip_command(specs)}", parent=self)

        self._run_bg(f"Installerer {', '.join(specs)} … (kan ta et minutt)",
                     lambda: deps.pip_install(specs), _done)

    def _install_all_pip(self) -> None:
        rows = self._visible()
        items = [s for s in self._statuses if s.missing and s.action == "pip"
                 and (self._mode == "overview" or s in rows)]
        if items:
            self._install_pip(items)

    _SIZES = {"libreoffice": "ca. 370 MB nedlasting, 1,5 GB installert",
              "ghostscript": "ca. 65 MB", "siegfried": "ca. 25 MB"}

    def _install_component(self, s: "deps.DepStatus") -> None:
        from siard_workflow.core import components as C
        name = C.LABELS.get(s.component, s.label)
        where = {"libreoffice": "%LOCALAPPDATA%\\SIARDManager\\libreoffice",
                 "ghostscript": "%LOCALAPPDATA%\\SIARDManager\\ghostscript",
                 "siegfried": "%LOCALAPPDATA%\\SIARDManager\\siegfried"}.get(s.component, "")
        if s.bundled:
            q = (f"Installere {name} fra programpakken?\n\n{s.bundled}\n\n"
                 f"Krever ikke nett eller administratorrettigheter. Installeres i {where}.")
        else:
            q = (f"Laste ned og installere {name} ({self._SIZES.get(s.component, '')})?\n\n"
                 f"Krever nett, men ikke administratorrettigheter. Installeres i {where}.")
        if s.component == "ghostscript":
            q += "\n\nGhostscript er AGPL-lisensiert."
        if s.component == "siegfried":
            q += "\n\nSiegfried blir valgt som filidentifisering (kan endres i Innstillinger)."
        if not _mb.askyesno(f"Installer {name}", q, parent=self):
            return
        if not s.bundled and not C.is_online():
            _mb.showerror("Ingen nettilgang",
                          f"Maskinen har ikke nettilgang, og installasjonsfilen for {name} "
                          "følger ikke med pakken.\n\nBruk en programpakke med komponenter, "
                          f"eller legg installasjonsfilen i {C.components_dir()}.", parent=self)
            return
        src = s.bundled or None
        self._run_tool_install(name, lambda p: C.install(s.component, p, source=src))

    def _run_tool_install(self, name: str, fn) -> None:
        def _progress(m):
            try:
                self.after(0, lambda: self._status_lbl.configure(text=str(m)))
            except Exception:
                pass

        def _done(res):
            if isinstance(res, Exception):
                self._status_lbl.configure(text_color=COLORS["red"],
                                           text=f"{name}: installasjon feilet")
                _mb.showerror("Installasjon feilet", f"{name}: {res}", parent=self)
            else:
                self._status_lbl.configure(text_color=COLORS["green"],
                                           text=f"{name} er installert: {res}")
                try:
                    from siard_workflow.core.file_identifier import reset_identifier
                    reset_identifier()
                except Exception:
                    pass

        self._run_bg(f"Installerer {name} …", lambda: fn(_progress), _done)

    def _browse_lo(self) -> None:
        try:
            self._app._prompt_for_libreoffice(ask=False)
        except Exception:
            pass
        self._recheck()

    def _open_settings(self) -> None:
        try:
            self._app._open_settings()
        except Exception:
            pass

    @staticmethod
    def _open_url(url: str) -> None:
        import webbrowser
        try:
            webbrowser.open(url)
        except Exception:
            pass

    def _recheck(self) -> None:
        self._run_bg("Sjekker på nytt …", lambda: None, lambda _r: None)

    # ── lukking ──────────────────────────────────────────────────────────────

    def _lukk(self) -> None:
        if self._busy:
            return
        self.dont_show_again = bool(self._dont_var.get())
        self.result = "lukk"
        self.grab_release()
        self.destroy()

    def _stopp(self) -> None:
        if self._busy:
            return
        self.result = "stopp"
        self.grab_release()
        self.destroy()

    def _fortsett(self) -> None:
        if self._busy:
            return
        self.result = "fortsett"
        self.grab_release()
        self.destroy()

    @property
    def statuses(self) -> "list[deps.DepStatus]":
        return self._statuses
