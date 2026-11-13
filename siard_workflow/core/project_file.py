"""
siard_workflow/core/project_file.py
------------------------------------
Prosjektfil (.siardwf) for å lagre og gjenoppta en workflow-kjøring.

Format: JSON med versjonsnummer, kilde-SIARD-sti og en liste over operasjoner
med status og resultat. Versjon 2 lagrer i tillegg:

  - hele resultatet per steg (renset til JSON), melding, tidsbruk og kjøring
  - tidligere forsøk per steg (`attempts`)
  - alle kjøringer (`runs`) med loggfil og hvilke steg som kjørte
  - arbeidsmappa (temp) og hva den bygger på (`workdir`)
  - lagrede svar på spørsmål (`answers`), så de ikke stilles på nytt
  - filtyper (`format_counts`) og feillogger fra alle kjøringer (`failure_files`)

Brukes til:
  - Lagre workflow-oppsett (operasjoner + params) slik at det kan gjenbrukes
  - Checkpoint-er underveis i kjøring (skrives til disk etter hvert steg)
  - Gjenoppta en avbrutt kjøring: kjøreplanen (core/run_plan.py) bestemmer
    hvilke steg som hoppes over, kjøres eller må kjøres på nytt
  - Samlet kjørerapport på tvers av kjøringer

Versjon 1-filer lastes uendret (manglende felt får standardverdier).
"""
from __future__ import annotations

import datetime
import json
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path


# ── Versjon for filformat ─────────────────────────────────────────────────────
_FORMAT_VERSION = 2
_FILE_SUFFIX    = ".siardwf"

# Statuser et steg kan ha
STATUS_PENDING     = "pending"       # ikke kjørt (eller tilbakestilt)
STATUS_COMPLETED   = "completed"     # fullført OK
STATUS_FAILED      = "failed"        # feilet
STATUS_INTERRUPTED = "interrupted"   # stoppet av bruker midt i steget
STATUS_HELD        = "held"          # holdt igjen fordi et steg før feilet
STATUS_SKIPPED     = "skipped"       # vilkår ikke oppfylt
STATUS_USER_SKIP   = "user_skip"     # brukeren valgte «Hopp over i neste kjøring»

DONE_STATUSES = (STATUS_COMPLETED, STATUS_SKIPPED, STATUS_USER_SKIP)

_MAX_LIST     = 2000      # lengre lister kortes ned i lagret resultat
_MAX_ENTRIES  = 1000      # failure_summary.entries
_MAX_ATTEMPTS = 20


def now_iso() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


def sanitize_result(data, _depth: int = 0):
    """Gjør et operasjonsresultat lagringsbart som JSON.

    Path → str, set/tuple → liste, ukjente typer → str. Store lister kortes
    ned (original_namelist beholder bare katalogposter — det er alt «Pakk
    sammen» trenger, og lista kan ellers ha millioner av filnavn).
    """
    if _depth > 8:
        return str(data)
    if data is None or isinstance(data, (bool, int, float, str)):
        return data
    if isinstance(data, Path):
        return str(data)
    if isinstance(data, dict):
        out = {}
        for k, v in data.items():
            k = str(k)
            if k == "original_namelist" and isinstance(v, (list, tuple)):
                out[k] = [e for e in v if isinstance(e, str) and e.endswith("/")]
                continue
            if k == "entries" and isinstance(v, (list, tuple)):
                v = list(v)[:_MAX_ENTRIES]
            out[k] = sanitize_result(v, _depth + 1)
        return out
    if isinstance(data, (list, tuple, set, frozenset)):
        seq = sorted(data, key=str) if isinstance(data, (set, frozenset)) else list(data)
        return [sanitize_result(v, _depth + 1) for v in seq[:_MAX_LIST]]
    return str(data)


@dataclass
class OpCheckpoint:
    """Tilstand for én operasjon i prosjektfilen."""
    operation_id: str
    label:        str
    params:       dict
    status:       str        = STATUS_PENDING
    output_siard: str | None = None        # absolutt sti til output-SIARD (hvis produces_siard)
    completed_at: str | None = None        # ISO-tidsstempel
    error_msg:    str | None = None        # kortet ned feilmelding
    # Ekstra context-data som må gjenopprettes ved hopp (f.eks. extracted_path
    # og original_namelist for unpack_siard, slik at repack_siard kan fullføre).
    # Fra versjon 2: hele resultatet (renset), ikke bare skalarer og lister.
    ctx_data:     dict       = field(default_factory=dict)
    # ── Versjon 2 ─────────────────────────────────────────────────────────────
    message:      str        = ""          # stegets melding (også ved OK)
    elapsed:      float      = 0.0         # sekunder i siste forsøk
    started_at:   str | None = None
    run_id:       str | None = None        # kjøringen siste forsøk hørte til
    log_file:     str | None = None        # kjøreloggen for siste forsøk
    attempts:     list       = field(default_factory=list)   # tidligere forsøk
    # Satt når brukeren ba om å kjøre steget på nytt («reset») — hvorfor
    reset_reason: str | None = None

    @classmethod
    def from_dict(cls, d: dict) -> "OpCheckpoint":
        names = {f.name for f in fields(cls)}
        kw = {k: v for k, v in d.items() if k in names}
        kw.setdefault("params", {})
        kw.setdefault("label", kw.get("operation_id", ""))
        return cls(**kw)

    def _archive_attempt(self) -> None:
        """Legg nåværende (avsluttede) forsøk i historikken."""
        if self.status == STATUS_PENDING and not self.run_id:
            return
        self.attempts.append({
            "run_id":   self.run_id,
            "status":   self.status,
            "message":  (self.message or self.error_msg or "")[:300],
            "elapsed":  round(float(self.elapsed or 0.0), 1),
            "ended":    self.completed_at,
            "log_file": self.log_file,
        })
        del self.attempts[:-_MAX_ATTEMPTS]

    @property
    def attempt_no(self) -> int:
        """Nummeret på siste forsøk (1 = første gang steget kjørte)."""
        return len(self.attempts) + (1 if self.run_id else 0)


@dataclass
class ProjectFile:
    """
    Representerer en .siardwf prosjektfil.

    Opprettes enten fra en kjørende workflow (from_ops) eller ved lasting fra
    disk (load). Run-løkka bruker `operations[i]` posisjonelt (samme rekkefølge
    som arbeidsflyten — se gui/app.py `_sync_checkpoints_to_ops`), så samme
    operasjon kan forekomme flere ganger (f.eks. SHA-256 først og sist).
    """
    version:       int               = _FORMAT_VERSION
    source_siard:  str               = ""
    created:       str               = ""
    updated:       str               = ""
    operations:    list[OpCheckpoint] = field(default_factory=list)
    # ── Versjon 2 ─────────────────────────────────────────────────────────────
    runs:          list = field(default_factory=list)
    workdir:       dict = field(default_factory=dict)
    answers:       dict = field(default_factory=dict)
    format_counts: dict = field(default_factory=dict)
    failure_files: list = field(default_factory=list)

    # ── Fabrikkmetoder ────────────────────────────────────────────────────────

    @classmethod
    def from_ops(cls, source_siard: Path, ops: list) -> "ProjectFile":
        """Lag en ny prosjektfil fra en liste over operasjonsobjekter."""
        now = now_iso()
        checkpoints = [
            OpCheckpoint(
                operation_id=op.operation_id,
                label=op.label,
                params=dict(op.params),
            )
            for op in ops
        ]
        return cls(
            source_siard=str(source_siard),
            created=now,
            updated=now,
            operations=checkpoints,
        )

    @classmethod
    def load(cls, path: Path) -> "ProjectFile":
        """Last prosjektfil fra disk (versjon 1 og 2)."""
        data = json.loads(path.read_text(encoding="utf-8"))
        ops = [OpCheckpoint.from_dict(op) for op in data.get("operations", [])]
        return cls(
            version=_FORMAT_VERSION,
            source_siard=data.get("source_siard", ""),
            created=data.get("created", ""),
            updated=data.get("updated", ""),
            operations=ops,
            runs=list(data.get("runs") or []),
            workdir=dict(data.get("workdir") or {}),
            answers=dict(data.get("answers") or {}),
            format_counts=dict(data.get("format_counts") or {}),
            failure_files=list(data.get("failure_files") or []),
        )

    # ── Persistens ────────────────────────────────────────────────────────────

    def save(self, path: Path) -> None:
        """Skriv prosjektfil til disk (atomisk via temp-fil)."""
        self.updated = now_iso()
        data = {
            "version":       _FORMAT_VERSION,
            "source_siard":  self.source_siard,
            "created":       self.created,
            "updated":       self.updated,
            "operations":    [asdict(op) for op in self.operations],
            "runs":          self.runs,
            "workdir":       self.workdir,
            "answers":       self.answers,
            "format_counts": self.format_counts,
            "failure_files": self.failure_files,
        }
        tmp = path.with_suffix(".siardwf.tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str),
                       encoding="utf-8")
        tmp.replace(path)

    # ── Oppslag ───────────────────────────────────────────────────────────────

    def _find(self, op_id: str) -> OpCheckpoint | None:
        return next((o for o in self.operations if o.operation_id == op_id), None)

    def at(self, index: int, op_id: str | None = None) -> OpCheckpoint | None:
        """Checkpoint på posisjon `index` (faller tilbake til op_id-oppslag)."""
        if 0 <= index < len(self.operations):
            cp = self.operations[index]
            if op_id is None or cp.operation_id == op_id:
                return cp
        return self._find(op_id) if op_id else None

    # ── Statusoppdateringer (kalles fra run-løkken) ────────────────────────────

    def _resolve(self, op: "str | OpCheckpoint") -> OpCheckpoint | None:
        return op if isinstance(op, OpCheckpoint) else self._find(op)

    def mark_started(self, op, run_id: str | None = None,
                     log_file: str | None = None) -> None:
        cp = self._resolve(op)
        if cp:
            cp._archive_attempt()
            cp.status       = STATUS_PENDING
            cp.started_at   = now_iso()
            cp.completed_at = None
            cp.run_id       = run_id
            cp.log_file     = log_file
            cp.message      = ""
            cp.error_msg    = None
            cp.elapsed      = 0.0
            cp.reset_reason = None

    def mark_completed(self, op, output_siard: Path | None = None,
                       ctx_data: dict | None = None, *, message: str = "",
                       elapsed: float = 0.0) -> None:
        cp = self._resolve(op)
        if cp:
            cp.status       = STATUS_COMPLETED
            cp.output_siard = str(output_siard) if output_siard else None
            cp.completed_at = now_iso()
            cp.error_msg    = None
            cp.message      = message or cp.message
            cp.elapsed      = float(elapsed or cp.elapsed or 0.0)
            if ctx_data is not None:
                cp.ctx_data = ctx_data

    def mark_failed(self, op, error_msg: str = "", *, elapsed: float = 0.0,
                    ctx_data: dict | None = None,
                    interrupted: bool = False) -> None:
        cp = self._resolve(op)
        if cp:
            cp.status       = STATUS_INTERRUPTED if interrupted else STATUS_FAILED
            cp.error_msg    = (error_msg or "")[:200]
            cp.message      = error_msg or ""
            cp.completed_at = now_iso()
            cp.elapsed      = float(elapsed or 0.0)
            if ctx_data is not None:
                cp.ctx_data = ctx_data

    def mark_held(self, op, reason: str = "") -> None:
        """Holdt igjen i denne kjøringen. Et tidligere resultat er utdatert
        (innholdet er endret) — det legges i historikken."""
        cp = self._resolve(op)
        if cp:
            if cp.status == STATUS_COMPLETED:
                cp._archive_attempt()
                cp.run_id = None
            cp.status       = STATUS_HELD
            cp.message      = reason
            cp.completed_at = None

    def mark_skipped(self, op) -> None:
        cp = self._resolve(op)
        if cp:
            cp.status = STATUS_SKIPPED

    def reset(self, op, reason: str = "tilbakestilt av bruker") -> None:
        """Steget skal kjøres på nytt neste gang (historikken beholdes)."""
        cp = self._resolve(op)
        if cp:
            cp._archive_attempt()
            cp.status       = STATUS_PENDING
            cp.run_id       = None
            cp.completed_at = None
            cp.message      = ""
            cp.error_msg    = None
            cp.reset_reason = reason

    # ── Kjøringer ─────────────────────────────────────────────────────────────

    def start_run(self, log_file: str | None = None) -> str:
        run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        # Unik også om to kjøringer starter samme sekund
        existing = {r.get("run_id") for r in self.runs}
        base, n = run_id, 1
        while run_id in existing:
            n += 1
            run_id = f"{base}_{n}"
        self.runs.append({"run_id": run_id, "started": now_iso(), "ended": None,
                          "log_file": log_file, "steps_run": [], "result": None})
        return run_id

    def get_run(self, run_id: str | None) -> dict | None:
        return next((r for r in self.runs if r.get("run_id") == run_id), None)

    def note_step_run(self, run_id: str, label: str, status: str) -> None:
        r = self.get_run(run_id)
        if r is not None:
            r.setdefault("steps_run", []).append({"label": label, "status": status})

    def end_run(self, run_id: str, result: str) -> None:
        r = self.get_run(run_id)
        if r is not None:
            r["ended"]  = now_iso()
            r["result"] = result

    def add_failure_files(self, run_id: str, **paths) -> None:
        entry = {"run_id": run_id, **{k: str(v) for k, v in paths.items() if v}}
        if len(entry) > 1 and entry not in self.failure_files:
            self.failure_files.append(entry)

    def current_failure_files(self, key: str) -> list[str]:
        """Gjeldende logger av en type (blob_csv / error_log / error_csv) fra
        alle kjøringer siden BLOB-konverteringen sist startet fra begynnelsen."""
        out = []
        for e in self.failure_files:
            if e.get("superseded") or not e.get(key):
                continue
            if e[key] not in out and Path(e[key]).exists():
                out.append(e[key])
        return out

    # ── Lagrede svar ──────────────────────────────────────────────────────────

    def get_answer(self, key: str, fingerprint: str = ""):
        a = self.answers.get(key)
        if not isinstance(a, dict):
            return None
        if fingerprint and a.get("fingerprint") and a.get("fingerprint") != fingerprint:
            return None
        return a.get("value")

    def set_answer(self, key: str, value, fingerprint: str = "") -> None:
        self.answers[key] = {"value": sanitize_result(value), "at": now_iso(),
                             "fingerprint": fingerprint}

    def forget_answers(self, prefix: str = "") -> int:
        keys = [k for k in self.answers if k.startswith(prefix)]
        for k in keys:
            del self.answers[k]
        return len(keys)

    # ── Spørringer ────────────────────────────────────────────────────────────

    def is_completed(self, op_id: str) -> bool:
        cp = self._find(op_id)
        return cp is not None and cp.status == STATUS_COMPLETED

    def get_status(self, op_id: str) -> str:
        cp = self._find(op_id)
        return cp.status if cp else STATUS_PENDING

    def get_output_siard(self, op_id: str) -> Path | None:
        cp = self._find(op_id)
        if cp and cp.output_siard:
            return Path(cp.output_siard)
        return None

    @property
    def completed_count(self) -> int:
        return sum(1 for op in self.operations if op.status == STATUS_COMPLETED)

    @property
    def pending_count(self) -> int:
        return sum(1 for op in self.operations if op.status not in DONE_STATUSES)

    @property
    def is_fully_completed(self) -> bool:
        return all(op.status in DONE_STATUSES for op in self.operations)

    @property
    def has_history(self) -> bool:
        """Er noe kjørt før (fullført, feilet, avbrutt …)?"""
        return any(op.status != STATUS_PENDING or op.run_id or op.attempts
                   for op in self.operations)


def source_fingerprint(path) -> str:
    """Størrelse + endringstid for kilde-SIARD — endres fila, gjelder ikke svarene."""
    try:
        st = Path(path).stat()
        return f"{st.st_size}:{int(st.st_mtime)}"
    except Exception:
        return ""
