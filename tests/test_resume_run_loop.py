"""
Kjøreløkka (gui/app.py `_run_queue_thread`) med kjøreplan, over flere kjøringer.

Falske operasjoner (samme operation_id som de ekte) i en pipeline:
  Pakk ut → innholdssteg A → innholdssteg B → Pakk sammen → Kjørerapport

  Kjøring 1: A feiler → «Pakk sammen» holdes igjen, temp-mappa beholdes,
             B og rapporten kjøres (rapporten er foreløpig).
  Kjøring 2: A virker → bare A, «Pakk sammen» og rapporten kjøres; «Pakk ut»
             og B hoppes over (temp-mappa gjenbrukes).
  Kjøring 3: A tilbakestilt og avbrytes av bruker → status «interrupted»,
             «Avslutt» avslutter kjøringen; prosjektet er lagret.
  Kjøring 4: A fortsetter (ikke «Pakk ut» på nytt — temp-mappa finnes).

Kjør:  python -X utf8 tests/test_resume_run_loop.py
"""
from __future__ import annotations

import queue
import shutil
import sys
import tempfile
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


from siard_workflow.core.base_operation import BaseOperation  # noqa: E402

CALLS: list[str] = []
BEHAVIOUR: dict = {}


class FakeUnpack(BaseOperation):
    operation_id = "unpack_siard"
    label = "Pakk ut SIARD"
    category = "Pipeline"
    modifies_content = True

    def premis_should_record(self, result, ctx):
        return False

    def run(self, ctx):
        CALLS.append("unpack")
        d = Path(tempfile.mkdtemp(prefix="siard_pipeline_", dir=BEHAVIOUR["tmp"]))
        ctx.extracted_path = d
        return self._ok({"extracted_path": str(d), "original_namelist": ["header/"]})


class FakeContentA(BaseOperation):
    operation_id = "hex_extract"
    label = "Innhold A"
    category = "Innhold"
    requires_unpack = True
    modifies_content = True

    def premis_should_record(self, result, ctx):
        return False

    def run(self, ctx):
        CALLS.append("A")
        mode = BEHAVIOUR.get("A", "ok")
        if mode == "fail":
            return self._fail("A feilet")
        if mode == "interrupt":
            BEHAVIOUR["app"]._stop_event.set()
            return self._fail("avbrutt")
        (ctx.extracted_path / "a.txt").write_text("A")
        return self._ok({"n": 1}, "A ok")


class FakeContentB(BaseOperation):
    operation_id = "siardmapper"
    label = "Innhold B"
    category = "Metadata"
    modifies_content = True

    def premis_should_record(self, result, ctx):
        return False

    def run(self, ctx):
        CALLS.append("B")
        return self._ok({"tables_enriched": 3}, "B ok")


class FakeRepack(BaseOperation):
    operation_id = "repack_siard"
    label = "Pakk sammen SIARD"
    category = "Pipeline"
    produces_siard = True

    def run(self, ctx):
        CALLS.append("repack")
        out = ctx.siard_path.with_name(ctx.siard_path.stem + "_konvertert.siard")
        out.write_bytes(b"zip")
        shutil.rmtree(ctx.extracted_path, ignore_errors=True)   # som den ekte
        ctx.extracted_path = None
        return self._ok({"output_path": str(out)}, f"→ {out.name}")


class FakeReport(BaseOperation):
    operation_id = "workflow_report"
    label = "Kjørerapport (PDF)"
    category = "Rapport"

    def run(self, ctx):
        CALLS.append("report")
        BEHAVIOUR["report_held"] = list(ctx.metadata.get("held_steps") or [])
        return self._ok({"report_path": "x.pdf"}, "rapport")


def _make_app(tmp: Path):
    import gui.app as A
    app = A.App.__new__(A.App)
    app._log_queue = queue.Queue()
    app._stop_event = threading.Event()
    app._pause_event = threading.Event()
    app._step_resume_event = threading.Event()
    app._abort_requested = False
    app._step_stopped = False
    app._conv_ctx = None
    app._remote_hub = None
    app._remote_rep = None
    app._global_temp_dir = None
    app._output_dir_override = ""
    app._preflight_lines = []
    app._run_plan_bases = {}
    app._remote_start = lambda n: None
    app._remote_finish = lambda: None
    app._header_publish = lambda *a, **k: None
    app._header_apply_pending = lambda *a, **k: False
    app.after = lambda *a, **k: None
    return app


def _run(app, src, ops):
    from siard_workflow.core.project_file import ProjectFile
    pf_path = src.with_suffix(".siardwf")
    pf = ProjectFile.load(pf_path) if pf_path.exists() else None
    app._run_queue_thread([src], ops, pf, pf_path if pf else None)
    return ProjectFile.load(pf_path)


def main() -> None:
    print("test_resume_run_loop")
    import settings
    tmp = Path(tempfile.mkdtemp(prefix="resume_loop_"))
    orig_cfg = settings.get_config
    settings.get_config = lambda k, d=None: (str(tmp / "logs") if k == "log_dir"
                                             else False if k == "enable_premis_provenance"
                                             else orig_cfg(k, d))
    try:
        BEHAVIOUR.clear()
        BEHAVIOUR["tmp"] = str(tmp)
        src = tmp / "uttrekk.siard"
        src.write_bytes(b"zip")
        app = _make_app(tmp)
        BEHAVIOUR["app"] = app

        def ops():
            return [FakeUnpack(), FakeContentA(), FakeContentB(), FakeRepack(), FakeReport()]

        # ── Kjøring 1: A feiler ───────────────────────────────────────────────
        BEHAVIOUR["A"] = "fail"
        CALLS.clear()
        pf = _run(app, src, ops())
        st = [cp.status for cp in pf.operations]
        _ok(CALLS == ["unpack", "A", "B", "report"],
            f"kjøring 1: «Pakk sammen» kjøres ikke etter feil i A ({CALLS})")
        _ok(st == ["completed", "failed", "completed", "held", "completed"],
            f"kjøring 1: status {st}")
        _ok(BEHAVIOUR.get("report_held") == ["Pakk sammen SIARD"],
            "rapporten får vite hvilke steg som er holdt igjen")
        wd = Path(pf.workdir.get("path", ""))
        _ok(wd.is_dir(), "temp-mappa beholdes")
        _ok(len(pf.runs) == 1 and pf.runs[0]["result"] == "foreløpig",
            "kjøringen registreres som «foreløpig»")

        # ── Kjøring 2: A virker ───────────────────────────────────────────────
        BEHAVIOUR["A"] = "ok"
        CALLS.clear()
        pf = _run(app, src, ops())
        st = [cp.status for cp in pf.operations]
        _ok(CALLS == ["A", "repack", "report"],
            f"kjøring 2: bare A, «Pakk sammen» og rapporten ({CALLS})")
        _ok(all(s == "completed" for s in st), f"kjøring 2: alt fullført {st}")
        _ok(CALLS[0] == "A" and not wd.exists(),
            "A jobbet i samme temp-mappe som før (ryddet etter «Pakk sammen»)")
        _ok(pf.operations[1].attempt_no == 2, "A: forsøk 2")
        _ok(pf.operations[2].ctx_data.get("tables_enriched") == 3,
            "B sitt resultat ligger i prosjektfila (til rapporten)")

        # ── Kjøring 3: A tilbakestilt, avbrytes, «Avslutt» ────────────────────
        pf.reset(pf.operations[1])
        # Temp-mappa er borte etter «Pakk sammen»; bygg på siste resultat
        app._run_plan_bases = {str(src): "output"}
        pf.save(src.with_suffix(".siardwf"))
        BEHAVIOUR["A"] = "interrupt"
        app._abort_requested = True       # «Avslutt» er valgt

        orig_wait = app._wait_after_stop

        def _wait(i, label, interrupted=False):
            # Brukeren klikker «⏹ Avslutt» litt etter at kjøringen stoppet
            threading.Timer(0.2, app._step_resume_event.set).start()
            return orig_wait(i, label, interrupted)
        app._wait_after_stop = _wait
        CALLS.clear()
        pf = _run(app, src, ops())
        st = [cp.status for cp in pf.operations]
        _ok(CALLS == ["unpack", "A"], f"kjøring 3: stopper i A ({CALLS})")
        _ok(st[1] == "interrupted", f"kjøring 3: A er «avbrutt», ikke fullført ({st[1]})")
        _ok(pf.runs[-1]["result"] == "avbrutt", "kjøringen registreres som «avbrutt»")
        msgs = []
        while not app._log_queue.empty():
            it = app._log_queue.get()
            if it[0] == "log":
                msgs.append(it[1])
        _ok(any("Bygger på siste resultat" in m for m in msgs),
            "«Pakk ut» bygget på siste resultat (valgt grunnlag)")

        # ── Kjøring 4: fortsetter i samme temp-mappe ──────────────────────────
        app._stop_event.clear()
        app._abort_requested = False
        app._run_plan_bases = {}
        BEHAVIOUR["A"] = "ok"
        CALLS.clear()
        pf = _run(app, src, ops())
        _ok(CALLS == ["A", "repack", "report"],
            f"kjøring 4: «Pakk ut» og B hoppes over, A fortsetter ({CALLS})")
        _ok(all(cp.status == "completed" for cp in pf.operations), "kjøring 4: alt fullført")
        _ok(len(pf.runs) == 4 and [r["result"] for r in pf.runs]
            == ["foreløpig", "ok", "avbrutt", "ok"], "fire kjøringer i historikken")
        out = [p.name for p in tmp.glob("uttrekk_konvertert*.siard")]
        _ok(len(out) >= 1, f"resultat skrevet ({out})")
    finally:
        settings.get_config = orig_cfg
        shutil.rmtree(tmp, ignore_errors=True)
    print("\nAlle tester OK")


if __name__ == "__main__":
    main()
