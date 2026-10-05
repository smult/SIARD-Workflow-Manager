import os
import sys
from pathlib import Path

# Pakket program (PyInstaller): underprosesser fra ProcessPoolExecutor
# (XML-rens) starter samme .exe — freeze_support() lar dem kjøre jobben sin i
# stedet for å starte et nytt GUI. Må kalles før alt annet. Uten pakke: no-op.
import multiprocessing
multiprocessing.freeze_support()

sys.path.insert(0, str(Path(__file__).parent))
import app_paths


def resource_path(relative):
    return app_paths.resource(relative)


# ── Konsoll-løs kjøring (pythonw.exe) ─────────────────────────────────────────
# start.bat starter GUI-et med pythonw.exe (uten konsollvindu) slik at
# konsollen/PowerShell ikke henger mens programmet kjører, og programmet ikke
# dør når konsollen lukkes. Under pythonw er sys.stdout/stderr = None, og enhver
# print() ville da krasje. Rut strømmene trygt til en loggfil, slik at
# eventuelle oppstartsfeil kan feilsøkes i ettertid.
def _redirect_std_streams() -> None:
    if sys.stdout is not None and sys.stderr is not None:
        return
    try:
        fh = open(app_paths.data_dir() / "siard_manager.log",
                  "a", encoding="utf-8", buffering=1)
    except Exception:
        fh = open(os.devnull, "w")
    if sys.stdout is None:
        sys.stdout = fh
    if sys.stderr is None:
        sys.stderr = fh


_redirect_std_streams()

# Selvtest av pakket program (brukes av packaging/build_release.py):
#   SIARDManager.exe --selftest resultat.json
# (__main__-vakt: underprosesser importerer main.py som __mp_main__ med samme argv)
if __name__ == "__main__" and len(sys.argv) >= 2 and sys.argv[1] == "--selftest":
    import selftest
    sys.exit(selftest.run(sys.argv[2] if len(sys.argv) > 2 else None))


# ── Avhengighetssjekk ved oppstart ───────────────────────────────────────────
# Kun det som trengs for å starte GUI-et sjekkes her (Python-versjon, tkinter,
# customtkinter). Øvrige pakker og eksterne programmer (LibreOffice,
# Ghostscript …) sjekkes etter oppstart i GUI-et — se dependencies.py.

_TITLE = "SIARD Workflow Manager"


def _fatal(msg: str) -> None:
    """Vis feilmelding uten tkinter (Windows-meldingsboks) og avslutt."""
    print(f"FEIL: {msg}")
    try:
        if sys.platform == "win32":
            import ctypes
            ctypes.windll.user32.MessageBoxW(None, msg, _TITLE, 0x10)
    except Exception:
        pass
    sys.exit(1)


def _ensure_dependencies() -> None:
    """
    Påkrevde pakker: spør brukeren før installasjon med pip. Uten bekreftelse
    (eller hvis installasjonen feiler) avsluttes programmet med beskjed om
    hvordan pakkene installeres manuelt.
    """
    import dependencies as deps

    if sys.version_info < deps.MIN_PYTHON:
        _fatal(f"Programmet krever Python {'.'.join(map(str, deps.MIN_PYTHON))} eller nyere.\n"
               f"Denne maskinen kjører Python {sys.version.split()[0]}.\n\n"
               "Installer en nyere Python fra https://www.python.org/")
    try:
        import tkinter as tk
        from tkinter import messagebox
    except Exception:
        _fatal("Python-installasjonen mangler tkinter (Tcl/Tk), som brukergrensesnittet "
               "trenger.\n\nInstaller Python på nytt fra https://www.python.org/ og kryss av "
               "for «tcl/tk and IDLE».")

    missing = deps.missing_packages((deps.REQUIRED,))
    if not missing:
        return
    if app_paths.FROZEN:
        # Pakket program: alle pakker skal følge med — pip finnes ikke her
        _fatal("Programpakken er ufullstendig — mangler: "
               + ", ".join(p.label for p in missing)
               + ".\n\nPakk ut hele zip-filen på nytt, eller hent en ny pakke.")
    specs = [p.pip_spec for p in missing]
    root = tk.Tk()
    root.withdraw()
    lines = "\n".join(f"  • {p.label} — {p.affects}" for p in missing)
    if not messagebox.askyesno(
            _TITLE,
            f"Følgende påkrevde Python-pakker mangler:\n\n{lines}\n\n"
            "Vil du installere dem nå (med pip, krever internett)?", parent=root):
        root.destroy()
        _fatal("Programmet kan ikke starte uten pakkene.\n\nInstaller manuelt med:\n"
               + deps.manual_pip_command(specs))

    # Enkel fremdriftsvisning mens pip kjører i bakgrunnen
    import threading
    win = tk.Toplevel(root)
    win.title(_TITLE)
    win.resizable(False, False)
    tk.Label(win, text=f"Installerer {', '.join(specs)} …\nDette kan ta et minutt.",
             justify="left", padx=24, pady=20).pack()
    win.update_idletasks()
    win.geometry(f"+{(win.winfo_screenwidth() - win.winfo_reqwidth()) // 2}"
                 f"+{(win.winfo_screenheight() - win.winfo_reqheight()) // 3}")
    result: dict = {}

    def _work():
        result["ok"], result["msg"] = deps.pip_install(specs)
        root.after(0, root.quit)

    threading.Thread(target=_work, daemon=True).start()
    root.mainloop()
    win.destroy()
    still = deps.missing_packages((deps.REQUIRED,))
    if not result.get("ok") or still:
        root.destroy()
        _fatal("Installasjonen feilet:\n\n" + (result.get("msg") or "ukjent feil")
               + "\n\nInstaller manuelt med:\n" + deps.manual_pip_command(specs))
    messagebox.showinfo(_TITLE, "Pakkene er installert. Programmet starter nå.", parent=root)
    root.destroy()


# ─────────────────────────────────────────────────────────────────────────────

_ensure_dependencies()

from gui.app import App


def main():
    try:
        app = App()
        app.mainloop()
    except Exception:
        # Uten konsoll (pythonw) er en uhåndtert feil usynlig — logg traceback
        # til fil og vis en enkel feilmelding slik at brukeren ikke står igjen
        # med et program som «bare forsvinner».
        import traceback
        traceback.print_exc()   # → siard_manager.log under pythonw
        try:
            import tkinter.messagebox as _mb
            _mb.showerror(
                "SIARD Workflow Manager",
                "Programmet kunne ikke starte.\n\n"
                f"Detaljer er skrevet til {app_paths.data_dir() / 'siard_manager.log'}.")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
