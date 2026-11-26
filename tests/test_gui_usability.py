"""
Brukervennlighet i GUI-et (nov. 2026).

Verifiserer:
  1. Skriftstørrelse fra tastaturet: Ctrl + / Ctrl − (også talltastaturet),
     Ctrl 0 = standard
  2. Minimert hovedvindu gjenopprettes før en modal dialog vises (ellers ble
     dialogen usynlig og hovedvinduet lot seg ikke hente opp fra oppgavelinja)
  3. SIARDMapper: beskrivelsene følger kolonnebredden (ikke kuttet ved
     55 tegn), og redigeringsfeltet følger cellen
  4. SIARDMapper: en tabell lukkes automatisk i det den BLIR ferdig, men ikke
     når man endrer i en tabell som alt var ferdig

Kjør:  python -X utf8 tests/test_gui_usability.py
"""
from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _pump(app, n: int = 12) -> None:
    for _ in range(n):
        app.update()
        time.sleep(0.03)


def test_font_keys(app) -> None:
    print("\n[1] Skriftstørrelse med Ctrl + / Ctrl − / Ctrl 0")
    import settings
    from gui.styles import FontRegistry

    saved: list = []
    orig_save = settings.save_config
    settings.save_config = lambda d: saved.append(dict(d))   # ikke rør config.json
    start = FontRegistry.current_offset()
    try:
        FontRegistry.scale(-start)                 # kjent utgangspunkt: 0
        app.focus_force()
        _pump(app, 3)
        # Talltastaturet gir «plus»/«minus» på Windows (KP_Add/KP_Subtract
        # finnes bare på Linux og kan ikke genereres her)
        for seq, want in (("<Control-plus>", 1), ("<Control-plus>", 2),
                          ("<Control-minus>", 1), ("<Control-minus>", 0),
                          ("<Control-minus>", -1), ("<Control-Key-0>", 0)):
            _ok(bool(app.bind_all(seq)), f"{seq} er bundet for hele programmet")
            # Tastetrykk leveres bare til vinduet med fokus; et annet program
            # kan ha tatt fokus imens — prøv igjen (ingen dobbelttelling, for
            # et tapt tastetrykk endrer ingenting)
            before = FontRegistry.current_offset()
            for _try in range(8):
                app.focus_force()
                _pump(app, 2)
                app.event_generate(seq)
                _pump(app, 2)
                if FontRegistry.current_offset() != before or want == before:
                    break
            _ok(FontRegistry.current_offset() == want,
                f"{seq} → størrelsesjustering {want:+d}")
        _ok(saved and saved[-1] == {"font_offset": 0},
            "valget lagres i innstillingene som med A−/A+-knappene")
    finally:
        FontRegistry.scale(start - FontRegistry.current_offset())
        settings.save_config = orig_save


def test_modal_guard(app) -> None:
    print("\n[2] Minimert hovedvindu gjenopprettes før modal dialog")
    import tkinter as tk
    from tkinter import commondialog
    import customtkinter as ctk
    from gui.modal_guard import restore_window_chain

    app.state("zoomed")
    _pump(app)

    def _minimize() -> None:
        app.iconify()
        _pump(app)
        _ok(app.state() == "iconic", "hovedvinduet er minimert")

    # a) dialog knyttet til hovedvinduet (transient) + grab — den vanlige formen
    _minimize()
    dlg = ctk.CTkToplevel(app)
    dlg.title("Spørsmål under kjøring")
    dlg.transient(app)
    dlg.grab_set()
    _pump(app)
    _ok(app.state() == "zoomed", "hovedvinduet er gjenopprettet (og fortsatt maksimert)")
    _ok(bool(dlg.winfo_viewable()), "dialogen er synlig")
    dlg.grab_release()
    dlg.destroy()
    _pump(app, 4)

    # b) dialog med bare grab (uten transient)
    _minimize()
    dlg = ctk.CTkToplevel(app)
    dlg.grab_set()
    _pump(app)
    _ok(app.state() != "iconic" and bool(dlg.winfo_viewable()),
        "grab alene gjenoppretter også")
    dlg.grab_release()
    dlg.destroy()
    _pump(app, 4)

    # c) meldingsbokser og fildialoger går gjennom commondialog.Dialog.show
    class _Probe(commondialog.Dialog):
        command = "list"                           # ufarlig Tcl-kommando

    _minimize()
    _Probe(parent=app).show()
    _pump(app)
    _ok(app.state() != "iconic", "meldingsboks/fildialog gjenoppretter hovedvinduet")

    # d) dialog fra en dialog: hele kjeden opp til hovedvinduet
    parent_dlg = ctk.CTkToplevel(app)
    _pump(app)
    _minimize()
    child = ctk.CTkToplevel(parent_dlg)
    child.transient(parent_dlg)
    child.grab_set()
    _pump(app)
    _ok(app.state() != "iconic", "underdialog gjenoppretter også hovedvinduet")
    child.grab_release()
    child.destroy()
    parent_dlg.destroy()
    _pump(app, 4)

    # e) vinduer som er trukket tilbake med vilje, røres ikke
    hidden = tk.Toplevel(app)
    hidden.withdraw()
    _pump(app, 3)
    _ok(restore_window_chain(hidden) is False and hidden.state() == "withdrawn",
        "skjult (withdrawn) vindu blir ikke vist")
    hidden.destroy()


def test_mapper(app) -> None:
    from gui.siardmapper_dialog import SiardMapperDialog, _ColItem, _TblItem

    long_desc = ("Entydig identifikator for klienten slik den ble tildelt ved "
                 "første registrering i fagsystemet, brukes i alle koblinger")
    klient = _TblItem("KLIENT", "klient", "Klienter", "table0", True, [
        _ColItem("KLI_ID", "kli_id", long_desc, 0),
        _ColItem("KLI_NAVN", "kli_navn", "", 1),
    ])
    sak = _TblItem("SAK", "sak", "Saker", "table1", True, [
        _ColItem("SAK_ID", "sak_id", "Løpenummer for saken", 0),
        _ColItem("SAK_TITTEL", "sak_tittel", long_desc, 1),
    ])
    dlg = SiardMapperDialog(app, [klient, sak], None, None)
    _pump(app, 20)
    tree = dlg._tree
    try:
        print("\n[3] SIARDMapper: tekst og redigeringsfelt følger kolonnebredden")
        shown = tree.set("C:klient:kli_id", "json_desc")
        _ok(len(long_desc) > 55 and shown == long_desc,
            f"hele beskrivelsen ligger i cellen ({len(shown)} tegn, ikke kuttet ved 55)")
        dlg._commit("C:sak:sak_tittel", long_desc + " — egen tekst")
        _ok(tree.set("C:sak:sak_tittel", "custom_desc") == long_desc + " — egen tekst",
            "egendefinert beskrivelse vises også i full lengde")
        dlg._commit("C:sak:sak_tittel", "linje 1\nlinje 2")
        _ok(tree.set("C:sak:sak_tittel", "custom_desc") == "linje 1 linje 2",
            "linjeskift vises som mellomrom i cellen")
        dlg._commit("C:sak:sak_tittel", "")

        tree.item("T:sak", open=True)
        _pump(app, 4)
        iid = "C:sak:sak_id"
        dlg._open_editor(iid)
        _pump(app, 6)
        entry = dlg._editor._entry
        _ok(entry.winfo_width() == tree.bbox(iid, "custom_desc")[2],
            f"redigeringsfeltet er like bredt som kolonnen ({entry.winfo_width()} px)")
        before = entry.winfo_width()
        tree.column("#0", width=120, minwidth=100)        # brukeren drar kolonnekanten
        tree.column("json_desc", width=150)
        _pump(app, 10)
        after = tree.bbox(iid, "custom_desc")[2]
        _ok(entry.winfo_width() == after and after != before,
            f"feltet følger kolonnen når bredden endres ({before} → {after} px)")
        _ok(entry.winfo_x() == tree.bbox(iid, "custom_desc")[0],
            "og står fortsatt i riktig kolonne")

        _ok(dlg._is_clipped("C:klient:kli_id", "#2", long_desc),
            "verktøytips vises når teksten ikke får plass i kolonnen")
        _ok(not dlg._is_clipped("C:sak:sak_id", "#3", "kort"),
            "ikke verktøytips når teksten får plass")
        dlg._editor.hide()
        tree.item("T:sak", open=False)

        print("\n[4] SIARDMapper: automatisk lukking av ferdige tabeller")
        _ok(bool(tree.item("T:klient", "open")) and not tree.item("T:sak", "open"),
            "ved åpning: ufullstendig tabell er åpen, ferdig tabell er lukket")

        dlg._commit("C:klient:kli_navn", "Klientens navn")
        _ok(not tree.item("T:klient", "open"),
            "tabellen lukkes i det siste felt blir beskrevet (som før)")

        # Brukeren åpner den ferdige tabellen for å rette noe
        tree.focus("T:klient")
        tree.item("T:klient", open=True)
        tree.event_generate("<<TreeviewOpen>>")
        _pump(app, 3)
        dlg._commit("C:klient:kli_navn", "Klientens fulle navn")
        _ok(bool(tree.item("T:klient", "open")),
            "endring i en ferdig tabell lukker den ikke")
        dlg._commit("C:klient:kli_navn", "")
        dlg._commit("C:klient:kli_navn", "Navn")
        _ok(bool(tree.item("T:klient", "open")),
            "heller ikke når et felt tømmes og fylles igjen")
        dlg._copy_json_to_custom("C:klient:kli_id", long_desc)
        _ok(bool(tree.item("T:klient", "open")),
            "heller ikke ved «kopier JSON-beskrivelse»")

        # Brukeren lukker selv — deretter gjelder vanlig oppførsel igjen
        tree.item("T:klient", open=False)
        tree.event_generate("<<TreeviewClose>>")
        _pump(app, 3)
        _ok("klient" not in dlg._keep_open, "lukket av brukeren → ikke lenger holdt åpen")

        # Ferdig tabell åpnet uten at åpne-hendelsen ble fanget (f.eks. etter
        # «Match på nytt»): endring skal fortsatt ikke lukke den
        tree.item("T:sak", open=True)
        _ok("sak" not in dlg._keep_open, "(tabellen er åpnet uten hendelse)")
        dlg._commit("C:sak:sak_id", "Saksnummer")
        _ok(bool(tree.item("T:sak", "open")),
            "endring i en tabell som var ferdig fra før, lukker den ikke")
    finally:
        try:
            dlg.grab_release()
        except Exception:
            pass
        dlg.destroy()
        _pump(app, 4)


def main() -> int:
    sys.argv = ["app"]
    threading.Timer(180, lambda: os._exit(3)).start()
    from gui.app import App
    # Oppstartssjekken av programvare kan åpne en modal dialog som blokkerer testen
    App._startup_dependency_check = lambda self: None
    app = App()
    _pump(app, 40)
    try:
        test_font_keys(app)
        test_modal_guard(app)
        test_mapper(app)
    finally:
        try:
            app.destroy()
        except Exception:
            pass
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    except BaseException:
        import traceback
        traceback.print_exc()
        rc = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)
