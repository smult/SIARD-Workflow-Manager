import sys
from pathlib import Path

def resource_path(relative):
    base = getattr(sys, "_MEIPASS", Path(__file__).parent)
    return Path(base) / relative

sys.path.insert(0, str(Path(__file__).parent))
from gui.app import App

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
