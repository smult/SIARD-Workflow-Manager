# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller-oppskrift for SIARD Workflow Manager (Windows, mappe-pakke).

Bygges via packaging/build_release.py (anbefalt), eller direkte:
    python -m PyInstaller --noconfirm packaging/siard_manager.spec

Resultat: dist/SIARDManager/SIARDManager.exe + _internal/ med Python og alle
pakker. Kjører uten Python-installasjon og uten nett.
"""
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_all, collect_submodules

ROOT = Path(SPECPATH).resolve().parent
sys.path.insert(0, str(ROOT))
from version import VERSION  # noqa: E402

datas = [
    (str(ROOT / "siard_workflow" / "sources"), "siard_workflow/sources"),
    (str(ROOT / "config_example.json"), "."),
]
binaries = []
# Programmets egne moduler importeres mye «lat» (inne i funksjoner) — ta med alle
hiddenimports = (collect_submodules("siard_workflow") + collect_submodules("gui")
                 + ["app_paths", "dependencies", "settings", "version",
                    "disk_selector", "selftest"])

# Tredjepartspakker: moduler + datafiler (temaer, maler, fonter) + DLL-er
# (tkdnd, libmagic, MuPDF). Valgfrie tillegg tas med når de er installert i
# byggemiljøet.
for pkg in ("customtkinter", "tkinterdnd2", "reportlab", "pymupdf", "fitz", "lxml",
            "olefile", "msoffcrypto", "docx", "mailmerge", "psutil",
            "py7zr", "rarfile", "magic"):
    try:
        d, b, h = collect_all(pkg)
    except Exception:
        continue
    # lxml: ISO-Schematron-ressursene brukes ikke og gir unødig lange stier
    datas += [x for x in d if "isoschematron" not in x[0]]
    binaries += b
    hiddenimports += [m for m in h if "isoschematron" not in m]

a = Analysis(
    [str(ROOT / "main.py")],
    pathex=[str(ROOT)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    excludes=["tests", "testsiard", "weasyprint", "PyInstaller", "pytest",
              "lxml.isoschematron",   # ubrukt; lengste filstier i pakken
              "matplotlib", "IPython", "gui.app_backup", "siard_workflow.examples"],
    noarchive=False,
)
pyz = PYZ(a.pure)

# Windows-versjonsinfo (Egenskaper → Detaljer)
_v = tuple(int(x) for x in (VERSION.split(".") + ["0"] * 4)[:4])
try:
    from PyInstaller.utils.win32.versioninfo import (
        VSVersionInfo, FixedFileInfo, StringFileInfo, StringTable, StringStruct,
        VarFileInfo, VarStruct)
    version_info = VSVersionInfo(
        ffi=FixedFileInfo(filevers=_v, prodvers=_v),
        kids=[StringFileInfo([StringTable("041404B0", [
            StringStruct("CompanyName", "KDRS"),
            StringStruct("FileDescription", "SIARD Workflow Manager"),
            StringStruct("FileVersion", VERSION),
            StringStruct("ProductName", "SIARD Workflow Manager"),
            StringStruct("ProductVersion", VERSION),
            StringStruct("OriginalFilename", "SIARDManager.exe")])]),
            VarFileInfo([VarStruct("Translation", [0x0414, 1200])])])
except Exception:
    version_info = None

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="SIARDManager",
    console=False,          # GUI uten konsollvindu (som pythonw)
    upx=False,
    version=version_info,
)
coll = COLLECT(exe, a.binaries, a.datas, upx=False, name="SIARDManager")
