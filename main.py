import os
import sys
import subprocess
from pathlib import Path


def resource_path(relative):
    base = getattr(sys, "_MEIPASS", Path(__file__).parent)
    return Path(base) / relative


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
        fh = open(Path(__file__).parent / "siard_manager.log",
                  "a", encoding="utf-8", buffering=1)
    except Exception:
        fh = open(os.devnull, "w")
    if sys.stdout is None:
        sys.stdout = fh
    if sys.stderr is None:
        sys.stderr = fh


_redirect_std_streams()


# ── Avhengighetssjekk ved oppstart ───────────────────────────────────────────

_REQUIRED_PACKAGES = [
    # (import-navn, pip-pakkenavn)
    ("customtkinter", "customtkinter"),
    ("reportlab", "reportlab>=4.0"),
]


def _ensure_dependencies() -> None:
    """
    Sjekker at påkrevde Python-pakker er installert.
    Manglende pakker installeres automatisk via pip før GUI startes.
    """
    missing = []
    for import_name, pip_spec in _REQUIRED_PACKAGES:
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_spec)

    if not missing:
        return

    print(f"Installerer manglende pakker: {', '.join(missing)}")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", *missing],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print("Installasjon fullført.")
    except subprocess.CalledProcessError as e:
        print(f"ADVARSEL: Kunne ikke installere pakker automatisk: {e}")
        print(f"Kjør manuelt: pip install {' '.join(missing)}")


# ─────────────────────────────────────────────────────────────────────────────

sys.path.insert(0, str(Path(__file__).parent))

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
                "Detaljer er skrevet til siard_manager.log i programmappen.")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
