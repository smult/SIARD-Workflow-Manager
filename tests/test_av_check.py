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
    """Klikker automatisk i forhåndskontrollens dialog og husker tekst og dialog."""
    def __init__(self, root, answer: str, elevate: bool = False):
        self.root, self.answer, self.elevate = root, answer, elevate
        self.seen: list = []
        self.dialog = None
        self.status = ""

    def arm(self):
        import customtkinter as ctk

        def finish(d):
            if d._status is not None:
                self.status = d._status.cget("text")
            getattr(d, "_on_" + self.answer)()

        def wait_status(d, n=0):
            txt = d._status.cget("text") if d._status is not None else ""
            if ("AV for" in txt or "Ikke utført" in txt or n > 100):
                finish(d)
            else:
                self.root.after(100, lambda: wait_status(d, n + 1))

        def click():
            tops = [w for w in self.root.winfo_children()
                    if isinstance(w, ctk.CTkToplevel) and w.winfo_exists()]
            if not tops:
                self.root.after(150, click)
                return
            d = tops[0]
            self.dialog = d
            self.seen.append(d.title())
            self.seen.extend(_texts(d))
            if self.elevate:
                d._start_elevated()
                self.root.after(100, lambda: wait_status(d))
            else:
                finish(d)
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


def test_largest_folders(tmp: Path) -> None:
    print("test_largest_folders")
    z = tmp / "mapper.siard"
    with zipfile.ZipFile(z, "w") as zf:
        for i in range(70_000):                     # > 65 535 → ZIP64-katalog
            zf.writestr(f"content/schema0/table{1 if i % 7 else 2}/lob4/record{i}.txt", b"")
        zf.writestr(zipfile.ZipInfo("content/schema0/table1/lob4/"), b"")
    top = AV.largest_folders(z, top=2)
    _ok(top == [("content/schema0/table1/lob4", 60_000), ("content/schema0/table2/lob4", 10_000)],
        f"filer per mappe fra katalogen (mappeoppføringer telles ikke): {top}")
    cmds = AV.disable_8dot3_commands(tmp)
    drive = tmp.resolve().drive
    _ok(cmds[-1] == f"fsutil 8dot3name set {drive} 1"
        and (len(cmds) == 1) == (AV.short_name_policy() == 2),
        f"kommando for temp-disken (registerverdi {AV.short_name_policy()}): {cmds}")


def test_preflight(tmp: Path) -> None:
    print("test_preflight")
    import customtkinter as ctk
    import settings
    from gui.app import App
    root = ctk.CTk()
    root.withdraw()
    import gui.app as A
    saved = (AV.zip_entry_count, AV.active_antivirus, AV.volume_info, AV.measure_file_rate,
             AV.largest_folders, AV.run_elevated, settings.save_config, A.get_config)
    stored: dict = {}
    sn = {"on": True}
    try:
        AV.active_antivirus = lambda timeout=15: ["Test-Antivirus"]
        AV.volume_info = lambda folder, timeout=15: {"hdd": True, "short_names": sn["on"]}
        AV.measure_file_rate = lambda folder, n=400: 1500.0
        AV.largest_folders = lambda p, top=1: [("content/schema0/table12/lob4", 3_412_551)]
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
        drive = tmp.resolve().drive

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
            and str(tmp) in text and "mekanisk disk" in text,
            "varselet: antivirusnavn, antall filer, målt hastighet, anslått tid, "
            "mappa som bør unntas, HDD")
        _ok("content/schema0/table12/lob4 — 3 412 551 filer" in text
            and f"Korte 8.3-filnavn er PÅ for {drive}" in text and "VIKTIG" in text
            and d.dialog._commands and d.dialog._commands[-1] == f"fsutil 8dot3name set {drive} 1"
            and "Slå av 8.3-navn (administrator) …" in text and "Kopier kommando" in text,
            "8.3: største mappe, status for temp-disken, kommando, administrator- og kopier-knapp")

        # Knappen: administrator-vindu (simulert) → disken sjekkes på nytt
        calls: list = []

        def fake_elevated(cmds):
            calls.append(cmds)
            sn["on"] = False
            return True, ""
        AV.run_elevated = fake_elevated
        d = _Dialogs(root, "ja", elevate=True)
        d.arm()
        _ok(pre(ops) is True and calls and calls[0][-1].endswith("1")
            and "AV for" in d.status,
            f"«Slå av 8.3-navn» → kommandoene kjøres, ny sjekk: «{d.status}»")
        sn["on"] = True
        AV.run_elevated = lambda cmds: (False, "avbrutt (administratortilgang ble ikke gitt)")
        d = _Dialogs(root, "ja", elevate=True)
        d.arm()
        pre(ops)
        _ok("Ikke utført" in d.status and "kopiere" in d.status,
            "nektet administratortilgang → forklaring og tips om å kopiere kommandoen")

        d = _Dialogs(root, "nei")
        d.arm()
        _ok(pre(ops) is True and stored.get("av_warning_dismissed") is True,
            "«Fortsett, ikke vis igjen» → lagret i config, kjøringen fortsetter")

        d = _Dialogs(root, "avbryt")
        d.arm()
        _ok(pre(ops) is False, "«Avbryt» → kjøringen startes ikke")

        A.get_config = lambda k, *a: True if k == "av_warning_dismissed" else None
        d = _Dialogs(root, "ja")
        d.arm()
        _ok(pre(ops) is True and d.seen,
            "slått av, men 8.3 PÅ og stor mappe → vises likevel (kan rettes herfra)")
        sn["on"] = False
        f.logs.clear()
        _ok(pre(ops) is True and "varsel slått av" in f.logs[-1],
            "slått av og 8.3 av: ingen dialog, funnet står i preflight-loggen")

        # Uten antivirus/HDD: 8.3 PÅ + stor mappe er nok til å varsle
        A.get_config = lambda k, *a: False if k == "av_warning_dismissed" else None
        AV.active_antivirus = lambda timeout=15: []
        AV.volume_info = lambda folder, timeout=15: {"hdd": False, "short_names": True}
        d = _Dialogs(root, "ja")
        d.arm()
        _ok(pre(ops) is True and any("8.3-filnavn" in t for t in d.seen),
            "uten antivirus: 8.3 PÅ + mappe med 3,4 mill. filer → varsel om 8.3")
        AV.largest_folders = lambda p, top=1: [("content/schema0/table1/lob2", 5_000)]
        f.logs.clear()
        _ok(pre(ops) is True and "ikke funnet aktivt antivirus, HDD eller 8.3" in f.logs[-1],
            "uten antivirus og bare små mapper: ingen dialog")
        _ok(pre([Op("metadata_extract")]) is True, "uten utpakking: ikke aktuelt")
    finally:
        (AV.zip_entry_count, AV.active_antivirus, AV.volume_info, AV.measure_file_rate,
         AV.largest_folders, AV.run_elevated, settings.save_config, A.get_config) = saved
        root.destroy()


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="avsjekk_") as td:
        tmp = Path(td)
        test_entry_count(tmp)
        test_probes(tmp)
        test_largest_folders(tmp)
        test_preflight(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
