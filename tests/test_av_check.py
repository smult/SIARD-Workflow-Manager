"""
Forhåndskontroll «Antivirus og temp-disk (mange filer)» — core/av_check.py og
App._antivirus_preflight.

Verifiserer:
  1. zip_entry_count leser antall oppføringer fra sluttposten — vanlig ZIP og
     ZIP64 (> 65 535 oppføringer) — uten å lese hele katalogen
  2. measure_file_rate / active_antivirus / volume_info svarer (Windows)
  3. preflight: under terskelen → ingen dialog; aktivt antivirus + mange filer →
     dialog med antivirusnavn, målt hastighet, anslått tid og mappa som bør
     unntas; «Fortsett, ikke vis igjen» lagres; «Avbryt» stopper kjøringen

Kjør:  python -X utf8 tests/test_av_check.py
"""
from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core import av_check as AV   # noqa: E402


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def test_entry_count(tmp: Path) -> None:
    print("test_entry_count")
    small, big = tmp / "liten.siard", tmp / "stor.siard"
    with zipfile.ZipFile(small, "w") as zf:
        for i in range(123):
            zf.writestr(f"content/f{i}.txt", b"x")
    with zipfile.ZipFile(big, "w") as zf:
        for i in range(70_000):
            zf.writestr(f"content/f{i}.txt", b"")
    _ok(AV.zip_entry_count(small) == 123, "vanlig ZIP: 123 oppføringer")
    _ok(AV.zip_entry_count(big) == 70_000, "ZIP64 (> 65 535): 70 000 oppføringer")
    (tmp / "ikke.zip").write_bytes(b"ikke en zip")
    _ok(AV.zip_entry_count(tmp / "ikke.zip") is None, "ikke en ZIP → None")


def test_probes(tmp: Path) -> None:
    print("test_probes")
    rate = AV.measure_file_rate(tmp, n=100)
    _ok(rate and rate > 0 and not list(tmp.glob("siard_hastighet_*")),
        f"målt {rate:.0f} filer/s i temp-mappa, prøvefilene ryddet")
    if sys.platform == "win32":
        av = AV.active_antivirus()
        vol = AV.volume_info(tmp)
        _ok(isinstance(av, list) and set(vol) == {"hdd", "short_names"},
            f"antivirus {av}, disk {vol}")


class _Dialogs:
    """Klikker automatisk i forhåndskontrollens dialog og husker teksten."""
    def __init__(self, root, answer: str):
        self.root, self.answer, self.seen = root, answer, []

    def arm(self):
        import customtkinter as ctk

        def click():
            tops = [w for w in self.root.winfo_children()
                    if isinstance(w, ctk.CTkToplevel) and w.winfo_exists()]
            if not tops:
                self.root.after(150, click)
                return
            d = tops[0]
            self.seen.append(d.title())
            self.seen.extend(_texts(d))
            getattr(d, "_on_" + self.answer)()
        self.root.after(500, click)


def _texts(widget) -> list:
    out = []
    for w in widget.winfo_children():
        try:
            t = w.cget("text")
            if t:
                out.append(str(t))
        except Exception:
            pass
        out.extend(_texts(w))
    return out


def test_preflight(tmp: Path) -> None:
    print("test_preflight")
    import customtkinter as ctk
    import settings
    from gui.app import App
    root = ctk.CTk()
    root.withdraw()
    import gui.app as A
    saved = (AV.zip_entry_count, AV.active_antivirus, AV.volume_info, AV.measure_file_rate,
             settings.save_config, A.get_config)
    stored: dict = {}
    try:
        AV.active_antivirus = lambda timeout=15: ["Test-Antivirus"]
        AV.volume_info = lambda folder, timeout=15: {"hdd": True, "short_names": True}
        AV.measure_file_rate = lambda folder, n=400: 1500.0
        settings.save_config = lambda d: stored.update(d)

        class Op:
            def __init__(self, oid):
                self.operation_id = oid
        f = root
        f.siard_queue = [tmp / "uttrekk.siard"]
        f._global_temp_dir = tmp
        f._pf_status = ""
        f.logs = []
        f._pf = lambda m: f.logs.append(m)
        pre = App._antivirus_preflight.__get__(f)
        ops = [Op("unpack_siard"), Op("blob_convert")]

        AV.zip_entry_count = lambda p: 50_000
        _ok(pre(ops) is True and "ikke aktuelt (50 000 filer)" in f.logs[-1],
            "under terskelen: ingen dialog")

        AV.zip_entry_count = lambda p: 4_800_000
        A.get_config = lambda k, *a: False if k == "av_warning_dismissed" else None
        d = _Dialogs(root, "ja")
        d.arm()
        _ok(pre(ops) is True, "«Fortsett» → kjøringen fortsetter")
        text = " ".join(d.seen)
        _ok("Test-Antivirus" in text and "4 800 000 filer" in text
            and "1 500 nye filer per sekund" in text and "minst ca. 53 min" in text
            and str(tmp) in text and "mekanisk disk" in text and "8dot3name" in text,
            "varselet: antivirusnavn, antall filer, målt hastighet, anslått tid, "
            "mappa som bør unntas, HDD og 8.3")

        d = _Dialogs(root, "nei")
        d.arm()
        _ok(pre(ops) is True and stored.get("av_warning_dismissed") is True,
            "«Fortsett, ikke vis igjen» → lagret i config, kjøringen fortsetter")

        d = _Dialogs(root, "avbryt")
        d.arm()
        _ok(pre(ops) is False, "«Avbryt» → kjøringen startes ikke")

        A.get_config = lambda k, *a: True if k == "av_warning_dismissed" else None
        f.logs.clear()
        _ok(pre(ops) is True and "varsel slått av" in f.logs[-1],
            "slått av: ingen dialog, men funnet står i preflight-loggen")
        _ok(pre([Op("metadata_extract")]) is True, "uten utpakking: ikke aktuelt")
    finally:
        (AV.zip_entry_count, AV.active_antivirus, AV.volume_info, AV.measure_file_rate,
         settings.save_config, A.get_config) = saved
        root.destroy()


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="avsjekk_") as td:
        tmp = Path(td)
        test_entry_count(tmp)
        test_probes(tmp)
        test_preflight(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
