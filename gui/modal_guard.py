"""
gui/modal_guard.py

Et minimert vindu gjenopprettes før en modal dialog vises over det.

Bakgrunn: er hovedvinduet minimert når et steg åpner en modal dialog, trekker
Windows dialogen tilbake sammen med vinduet den hører til (transient), mens
dialogen likevel får all inndata (grab). Hovedvinduet lar seg da ikke hente
opp fra oppgavelinja, og dialogen er usynlig — programmet står fast.

install() legger vakten på de tre stedene alle modale dialoger går gjennom:
  - wm_transient / transient   (dialogen knyttes til et vindu)
  - grab_set / grab_set_global (dialogen gjøres modal)
  - commondialog.Dialog.show   (meldingsbokser og fildialoger)
Nye dialoger trenger dermed ingen egen kode for dette.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import commondialog

_installed = False


def restore_window_chain(widget) -> bool:
    """
    Gjenopprett alle minimerte vinduer fra `widget` og opp til hovedvinduet.
    Returnerer True hvis noe ble gjenopprettet. Vinduer som er trukket tilbake
    med vilje (withdrawn) røres ikke.
    """
    tops = []
    seen: set = set()
    w = widget
    while w is not None:
        try:
            top = w.winfo_toplevel()
        except Exception:
            break
        if id(top) in seen:
            break
        seen.add(id(top))
        tops.append(top)
        w = getattr(top, "master", None)

    restored = False
    for top in reversed(tops):            # hovedvinduet først
        try:
            if top.wm_state() == "iconic":
                top.wm_deiconify()        # tilbake til forrige tilstand (også maksimert)
                restored = True
        except Exception:
            pass
    if restored:
        try:
            tops[-1].update_idletasks()
        except Exception:
            pass
    return restored


def install() -> None:
    """Legg vakten på tkinter (én gang per prosess)."""
    global _installed
    if _installed:
        return
    _installed = True

    _transient = tk.Wm.wm_transient

    def wm_transient(self, master=None):
        if master is not None:
            restore_window_chain(master)
        return _transient(self, master)

    tk.Wm.wm_transient = wm_transient
    tk.Wm.transient = wm_transient

    def _guard_grab(name: str) -> None:
        orig = getattr(tk.Misc, name)

        def grab(self):
            restored = restore_window_chain(self)
            result = orig(self)
            if restored:
                try:
                    self.lift()           # dialogen skal ligge over vinduet som kom opp
                except Exception:
                    pass
            return result

        grab.__name__ = name
        setattr(tk.Misc, name, grab)

    _guard_grab("grab_set")
    _guard_grab("grab_set_global")

    _show = commondialog.Dialog.show

    def show(self, **options):
        parent = (options.get("parent") or self.options.get("parent")
                  or self.master or getattr(tk, "_default_root", None))
        if parent is not None:
            restore_window_chain(parent)
        return _show(self, **options)

    commondialog.Dialog.show = show
