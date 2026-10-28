"""
gui/remote_setup_dialog.py — «Koble til mobil»

Veiviser for ikke-tekniske brukere: installer ntfy-appen, abonner på emnet,
og test med en ekte rundtur (mobilen trykker «Det virker», PC-en ser svaret).
Teams-delen sender en testmelding til Workflows-lenken.

Verdiene leses fra innstillingsvinduets felt (ikke lagret config), slik at
testen kan kjøres før «Lagre».
"""
from __future__ import annotations

import threading

import customtkinter as ctk

from gui.styles import COLORS, FONTS, LOG_COLORS

TEST_TIMEOUT_S = 120


class RemoteSetupDialog(ctk.CTkToplevel):
    def __init__(self, parent, vars_: dict):
        super().__init__(parent)
        self.title("Koble til mobil")
        self.configure(fg_color=COLORS["surface"])
        self.transient(parent)
        self.grab_set()
        self._vars = vars_
        self._closed = False
        self.protocol("WM_DELETE_WINDOW", self._close)
        self._build()
        self.geometry("680x640")
        self.minsize(600, 560)

    # Verdier fra innstillingsvinduet
    def _cfg(self, key: str, default=None):
        var = self._vars.get(key)
        if var is None:
            return default
        try:
            return var.get()
        except Exception:
            return default

    def _hub(self, teams_only: bool = False):
        from siard_workflow.core.remote_notify import build_hub
        values = {k: self._cfg(k) for k in ("ntfy_server", "ntfy_topic", "ntfy_token",
                                             "teams_webhook_url")}
        values["ntfy_enabled"] = (not teams_only) and bool(self._cfg("ntfy_enabled", True))
        if teams_only:
            values["ntfy_topic"] = ""
        else:
            values["teams_webhook_url"] = ""
        return build_hub(lambda k, d=None: values.get(k, d)
                         if values.get(k) is not None else d, force=True)

    # Oppsett
    def _build(self) -> None:
        f_head = ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold")
        f_text = ctk.CTkFont(family=FONTS["mono"], size=12)
        f_big = ctk.CTkFont(family=FONTS["mono"], size=18, weight="bold")
        f_small = ctk.CTkFont(family=FONTS["mono"], size=11)
        body = ctk.CTkScrollableFrame(self, fg_color=COLORS["panel"], corner_radius=8)
        body.pack(fill="both", expand=True, padx=14, pady=(14, 6))
        body.grid_columnconfigure(0, weight=1)
        row = 0

        def lbl(text, font=f_text, color=COLORS["text"], pady=(2, 2)):
            nonlocal row
            ctk.CTkLabel(body, text=text, font=font, text_color=color, justify="left",
                         wraplength=600, anchor="w").grid(
                row=row, column=0, padx=14, pady=pady, sticky="w")
            row += 1

        lbl("ntfy-appen: varsler og svar", f_head, COLORS["accent"], (10, 6))
        lbl("1.  Installer appen «ntfy» på mobilen (App Store eller Google Play, gratis).")
        lbl("2.  Åpne appen, trykk «+» (abonner på emne) og skriv inn dette emnet:",
            pady=(8, 2))
        topic_row = ctk.CTkFrame(body, fg_color="transparent")
        topic_row.grid(row=row, column=0, padx=30, pady=6, sticky="w")
        row += 1
        topic = self._cfg("ntfy_topic", "") or ""
        ctk.CTkLabel(topic_row, text=topic or "(emne mangler)", font=f_big,
                     text_color=COLORS["accent"]).pack(side="left")
        ctk.CTkButton(topic_row, text="Kopier", width=70, font=f_small,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
                      border_width=1, command=lambda: self._copy(topic)).pack(
            side="left", padx=(12, 0))
        server = (self._cfg("ntfy_server", "") or "https://ntfy.sh").rstrip("/")
        if server not in ("https://ntfy.sh", "http://ntfy.sh"):
            lbl(f"Velg «Bruk en annen server» i appen og skriv: {server}",
                f_small, COLORS["muted"])
        lbl("3.  Trykk «Send test». Mobilen får en melding med knappen «Det virker» "
            "— trykk på den.", pady=(8, 2))
        self._ntfy_btn = ctk.CTkButton(
            body, text="Send test", width=140,
            fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
            text_color=COLORS["on_accent"],
            font=ctk.CTkFont(family=FONTS["mono"], size=12, weight="bold"),
            command=self._test_ntfy)
        self._ntfy_btn.grid(row=row, column=0, padx=30, pady=6, sticky="w")
        row += 1
        self._ntfy_status = ctk.CTkLabel(body, text="", font=f_text, justify="left",
                                         wraplength=600, text_color=COLORS["muted"])
        self._ntfy_status.grid(row=row, column=0, padx=30, pady=(0, 8), sticky="w")
        row += 1

        lbl("Teams: bare varsler", f_head, COLORS["accent"], (16, 6))
        lbl("1.  I Teams: åpne «Arbeidsflyter» (Workflows) og velg malen «Send "
            "webhook-varsler til en chat» (eller «… til en kanal»).")
        lbl("2.  Velg chatten eller kanalen, fullfør, og kopier lenken som vises.",
            pady=(4, 2))
        lbl("3.  Lim lenken inn i feltet «Teams-lenke (Workflows)» i innstillingene, "
            "og trykk «Send test til Teams».", pady=(4, 2))
        self._teams_btn = ctk.CTkButton(
            body, text="Send test til Teams", width=180, font=f_small,
            fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
            text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
            border_width=1, command=self._test_teams)
        self._teams_btn.grid(row=row, column=0, padx=30, pady=6, sticky="w")
        row += 1
        self._teams_status = ctk.CTkLabel(body, text="", font=f_text, justify="left",
                                          wraplength=600, text_color=COLORS["muted"])
        self._teams_status.grid(row=row, column=0, padx=30, pady=(0, 8), sticky="w")
        row += 1

        lbl("Husk å slå på «Send varsler til mobil» og trykke «Lagre» i innstillingene.",
            f_small, COLORS["muted"], (12, 4))
        ctk.CTkButton(self, text="Lukk", width=100, font=f_small,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      text_color=COLORS["btn_text"], border_color=COLORS["btn_border"],
                      border_width=1, command=self._close).pack(
            side="right", padx=14, pady=(0, 12))

    # Handlinger
    def _copy(self, text: str) -> None:
        self.clipboard_clear()
        self.clipboard_append(text)

    def _set(self, label, text: str, color_key: str = "muted") -> None:
        if not self._closed:
            self.after(0, lambda: (not self._closed) and label.configure(
                text=text, text_color=LOG_COLORS.get(color_key, COLORS["muted"])))

    def _test_ntfy(self) -> None:
        hub = self._hub()
        if hub is None or not hub.can_ask:
            self._set(self._ntfy_status, "Fyll inn ntfy-emnet først (og slå på ntfy-appen).",
                      "warn")
            return
        self._ntfy_btn.configure(state="disabled")
        self._set(self._ntfy_status, "Sender … venter på svar fra mobilen (inntil 2 minutter).")

        def run():
            hub.start()
            t = hub.ask("Test fra SIARD Manager",
                        "Trykk «Det virker» for å bekrefte at mobilen er koblet til.",
                        [("ok", "Det virker")])
            ans = t.wait_answer(TEST_TIMEOUT_S)
            t.close("på mobil" if ans else "— testen ble avsluttet", "Det virker" if ans else "")
            hub.stop(5)
            if ans:
                self._set(self._ntfy_status, "✓ Mobilen er koblet til. Svar fra mobilen "
                          "kommer fram.", "success")
            elif "ntfy" in hub._failed:
                self._set(self._ntfy_status, "Kunne ikke sende. Sjekk nettforbindelsen og "
                          "serveradressen.", "error")
            else:
                self._set(self._ntfy_status, "Ingen svar innen 2 minutter. Sjekk at emnet i "
                          "appen er skrevet nøyaktig likt, og prøv igjen.", "warn")
            if not self._closed:
                self.after(0, lambda: (not self._closed)
                           and self._ntfy_btn.configure(state="normal"))

        threading.Thread(target=run, daemon=True).start()

    def _test_teams(self) -> None:
        hub = self._hub(teams_only=True)
        if hub is None:
            self._set(self._teams_status, "Lim inn Teams-lenken i innstillingene først.",
                      "warn")
            return
        self._set(self._teams_status, "Sender …")

        def run():
            hub.start()
            hub.notify("Test fra SIARD Manager",
                       "Varsler fra SIARD Workflow Manager kommer hit.", status_button=False)
            hub.stop(20)
            if "teams" in hub._failed:
                self._set(self._teams_status, "Kunne ikke sende. Sjekk at hele lenken er "
                          "kopiert, og at arbeidsflyten er slått på.", "error")
            else:
                self._set(self._teams_status, "✓ Sendt. Sjekk chatten/kanalen i Teams "
                          "(kan ta et halvt minutt).", "success")

        threading.Thread(target=run, daemon=True).start()

    def _close(self) -> None:
        self._closed = True
        self.destroy()
