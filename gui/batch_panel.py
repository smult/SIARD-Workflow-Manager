"""
gui/batch_panel.py
Batch-behandling: legg til flere SIARD-filer og kjør samme workflow på alle.
Apnes som et eget vindu fra hovedapplikasjonen.
"""
from __future__ import annotations
import threading
import queue
from pathlib import Path
from typing import Callable

import customtkinter as ctk
from gui.styles import COLORS, FONTS


class BatchFileRow(ctk.CTkFrame):
    STATES = {
        "venter":  ("–",  COLORS["muted"]),
        "kjører": ("⏳", COLORS["accent"]),
        "ok":      ("✓",  COLORS["green"]),
        "feil":    ("✗",  COLORS["red"]),
        "hoppet":  ("–",  COLORS["muted"]),
    }

    def __init__(self, parent, path: Path, on_remove: Callable):
        super().__init__(parent, fg_color=COLORS["panel"], corner_radius=6,
                         border_color=COLORS["border"], border_width=1)
        self.path = path
        self.grid_columnconfigure(1, weight=1)

        self._status_lbl = ctk.CTkLabel(self, text="–", width=20,
                                         font=ctk.CTkFont(family=FONTS["mono"], size=12),
                                         text_color=COLORS["muted"])
        self._status_lbl.grid(row=0, column=0, padx=(10, 4), pady=6)

        ctk.CTkLabel(self, text=path.name,
                     font=ctk.CTkFont(family=FONTS["mono"], size=11),
                     text_color=COLORS["text"], anchor="w").grid(
                         row=0, column=1, sticky="ew", padx=4)

        size_kb = path.stat().st_size / 1024 if path.exists() else 0
        ctk.CTkLabel(self, text=f"{size_kb:.0f} KB",
                     font=ctk.CTkFont(family=FONTS["mono"], size=10),
                     text_color=COLORS["muted"]).grid(row=0, column=2, padx=8)

        self._msg_lbl = ctk.CTkLabel(self, text="",
                                      font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                      text_color=COLORS["muted"])
        self._msg_lbl.grid(row=0, column=3, padx=4)

        ctk.CTkButton(self, text="✕", width=24, height=24, corner_radius=4,
                      fg_color="#2a1515", hover_color="#3d2020",
                      text_color=COLORS["red"],
                      font=ctk.CTkFont(size=11),
                      command=lambda: on_remove(self)).grid(row=0, column=4, padx=(4, 8))

    def set_state(self, state: str, msg: str = ""):
        icon, color = self.STATES.get(state, ("?", COLORS["muted"]))
        self._status_lbl.configure(text=icon, text_color=color)
        self._msg_lbl.configure(text=msg[:40])
        border = COLORS["accent"] if state == "kjører" else (
                 COLORS["green"]  if state == "ok"      else (
                 COLORS["red"]    if state == "feil"    else COLORS["border"]))
        self.configure(border_color=border)


class BatchWindow(ctk.CTkToplevel):
    """
    Frittstoende batch-vindu.
    Tar en workflow-fabrikk-funksjon (callable som returnerer en fersk Workflow)
    og kjorer den pa alle valgte filer.
    """

    def __init__(self, parent, get_workflow_ops: Callable, manager):
        super().__init__(parent)
        self.title("Batch-behandling")
        self.geometry("760x560")
        self.configure(fg_color=COLORS["bg"])
        self.grab_set()

        self._get_workflow_ops = get_workflow_ops
        self._manager = manager
        self._rows: list[BatchFileRow] = []
        self._q: queue.Queue = queue.Queue()
        self._running = False

        self._build()
        self._poll()

    def _build(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # Topplinje
        top = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=0, height=50)
        top.grid(row=0, column=0, sticky="ew")
        top.grid_columnconfigure(1, weight=1)
        top.grid_propagate(False)

        ctk.CTkLabel(top, text="BATCH-BEHANDLING",
                     font=ctk.CTkFont(family=FONTS["mono"], size=13, weight="bold"),
                     text_color=COLORS["accent"]).grid(row=0, column=0, padx=16, pady=12, sticky="w")

        btns = ctk.CTkFrame(top, fg_color="transparent")
        btns.grid(row=0, column=2, padx=12, pady=8)

        ctk.CTkButton(btns, text="+ Legg til filer", width=130, height=30,
                      fg_color=COLORS["btn"], hover_color=COLORS["btn_hover"],
                      font=ctk.CTkFont(family=FONTS["mono"], size=11),
                      command=self._add_files).pack(side="left", padx=(0, 6))

        self._run_btn = ctk.CTkButton(btns, text="▶ Kjor alle", width=120, height=30,
                                       fg_color=COLORS["accent"], hover_color=COLORS["accent_dim"],
                                       font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                                       command=self._run_all)
        self._run_btn.pack(side="left")

        # Filliste
        self._scroll = ctk.CTkScrollableFrame(self, fg_color=COLORS["surface"],
                                               corner_radius=8)
        self._scroll.grid(row=1, column=0, padx=12, pady=(8, 0), sticky="nsew")
        self._scroll.grid_columnconfigure(0, weight=1)

        self._empty_lbl = ctk.CTkLabel(self._scroll,
                                        text="Ingen filer lagt til\nKlikk '+ Legg til filer'",
                                        font=ctk.CTkFont(family=FONTS["mono"], size=11),
                                        text_color=COLORS["muted"])
        self._empty_lbl.grid(row=0, column=0, pady=40)

        # Statuslinje
        self._status_bar = ctk.CTkLabel(self, text="",
                                         font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                         text_color=COLORS["muted"])
        self._status_bar.grid(row=2, column=0, padx=16, pady=8, sticky="w")

        # Logg
        log_frame = ctk.CTkFrame(self, fg_color=COLORS["surface"], corner_radius=8, height=140)
        log_frame.grid(row=3, column=0, padx=12, pady=(0, 12), sticky="ew")
        log_frame.grid_columnconfigure(0, weight=1)
        log_frame.grid_propagate(False)

        ctk.CTkLabel(log_frame, text="LOGG",
                     font=ctk.CTkFont(family=FONTS["mono"], size=11, weight="bold"),
                     text_color=COLORS["muted"]).grid(row=0, column=0, padx=12, pady=(8, 2), sticky="w")

        self._log_box = ctk.CTkTextbox(log_frame,
                                        fg_color=COLORS["bg"],
                                        text_color=COLORS["text"],
                                        font=ctk.CTkFont(family=FONTS["mono"], size=10),
                                        corner_radius=6, state="disabled", height=90)
        self._log_box.grid(row=1, column=0, padx=10, pady=(0, 8), sticky="ew")
        for level, color in [("ok", COLORS["green"]), ("feil", COLORS["red"]),
                              ("step", COLORS["accent"]), ("info", COLORS["muted"])]:
            self._log_box.tag_config(level, foreground=color)

    # ── Filhåndtering ─────────────────────────────────────────────────────────

    def _add_files(self):
        from tkinter import filedialog
        paths = filedialog.askopenfilenames(
            title="Velg SIARD-filer",
            filetypes=[("SIARD-filer", "*.siard"), ("ZIP-filer", "*.zip"), ("Alle", "*.*")],
        )
        for p in paths:
            path = Path(p)
            if not any(r.path == path for r in self._rows):
                self._add_row(path)

    def _add_row(self, path: Path):
        self._empty_lbl.grid_remove()
        row = BatchFileRow(self._scroll, path, on_remove=self._remove_row)
        row.grid(row=len(self._rows), column=0, sticky="ew", pady=3, padx=2)
        self._rows.append(row)
        self._update_status()

    def _remove_row(self, row: BatchFileRow):
        if self._running:
            return
        self._rows.remove(row)
        row.destroy()
        for i, r in enumerate(self._rows):
            r.grid(row=i, column=0, sticky="ew", pady=3, padx=2)
        if not self._rows:
            self._empty_lbl.grid(row=0, column=0, pady=40)
        self._update_status()

    def _update_status(self):
        n = len(self._rows)
        done = sum(1 for r in self._rows if r._status_lbl.cget("text") in ("✓", "✗"))
        self._status_bar.configure(text=f"{n} filer totalt  |  {done} behandlet")

    # ── Batch-kjøring ─────────────────────────────────────────────────────────

    def _run_all(self):
        if self._running or not self._rows:
            return
        ops = self._get_workflow_ops()
        if not ops:
            self._log("Ingen operasjoner i workflow - legg til operasjoner først", "feil")
            return
        # Reset all rows
        for row in self._rows:
            row.set_state("venter")

        self._running = True
        self._run_btn.configure(state="disabled", text="Kjorer...")
        threading.Thread(target=self._batch_thread, args=(ops,), daemon=True).start()

    def _batch_thread(self, ops):
        import time
        from siard_workflow.core.workflow import Workflow
        from siard_workflow.core.context import WorkflowContext
        from siard_workflow.core.workflow import WorkflowRun
        from siard_workflow.core.file_logger import WorkflowFileLogger

        total = len(self._rows)
        for idx, row in enumerate(self._rows):
            self._q.put(("state", row, "kjører", ""))
            self._q.put(("log", f"[{idx+1}/{total}] {row.path.name}", "step"))

            wf = Workflow(name=row.path.stem)
            for op in ops:
                wf.add(op)

            log_dir = row.path.parent / "siard_logs"
            with WorkflowFileLogger(log_dir, row.path.stem) as wfl:
                ctx = WorkflowContext(siard_path=row.path)
                ctx.metadata["file_logger"] = wfl
                run = WorkflowRun(row.path)
                run.start_time = time.time()

                for op in wf:
                    if not op.should_run(ctx):
                        run.skipped.append(op.operation_id)
                        continue
                    try:
                        result = op.run(ctx)
                        ctx.set_result(op.operation_id, result.data)
                    except Exception as exc:
                        result = op._fail(str(exc))
                    run.results.append(result)
                    wfl.log(f"{op.operation_id}: {result.message}",
                            "ok" if result.success else "feil")

                run.end_time = time.time()

            ok = run.success
            msg = f"{run.elapsed:.1f}s"
            self._q.put(("state", row, "ok" if ok else "feil", msg))
            self._q.put(("log", f"  {'OK' if ok else 'FEIL'} — {row.path.name} ({msg})",
                         "ok" if ok else "feil"))

        self._q.put(("done", None, None, None))

    def _poll(self):
        try:
            while True:
                item = self._q.get_nowait()
                kind, *rest = item
                if kind == "state":
                    _, row, state, msg = item
                    row.set_state(state, msg)
                    self._update_status()
                elif kind == "log":
                    _, msg, level = item
                    self._append_log(msg, level)
                elif kind == "done":
                    self._running = False
                    self._run_btn.configure(state="normal", text="Kjor alle")
                    self._append_log("Batch fullfort", "ok")
        except queue.Empty:
            pass
        self.after(80, self._poll)

    def _append_log(self, msg: str, level: str = "info"):
        self._log_box.configure(state="normal")
        self._log_box.insert("end", msg + "\n", level)
        self._log_box.configure(state="disabled")
        self._log_box.see("end")
