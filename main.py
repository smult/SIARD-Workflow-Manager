import sys
import subprocess
from pathlib import Path


def resource_path(relative):
    base = getattr(sys, "_MEIPASS", Path(__file__).parent)
    return Path(base) / relative


# ── Avhengighetssjekk ved oppstart ───────────────────────────────────────────

_REQUIRED_PACKAGES = [
    # (import-navn, pip-pakkenavn)
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
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
