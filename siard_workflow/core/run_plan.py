"""
siard_workflow/core/run_plan.py
-------------------------------
Kjøreplan for en arbeidsflyt som har vært kjørt før (prosjektfil .siardwf).

Tidligere ble alle steg etter det første som måtte kjøres, kjørt på nytt. Nå
bestemmes det per steg ut fra hva steget gjør:

  source   — før «Pakk ut»: leser original-SIARD (SHA-256, virus …)
  unpack   — «Pakk ut SIARD»
  content  — endrer innholdet i den utpakkede mappa (BLOB, HEX, SIARDMapper …)
  inspect  — mellom «Pakk ut» og «Pakk sammen», men endrer ikke innhold
  package  — «Pakk sammen SIARD»
  derived  — etter «Pakk sammen»: leser resultatet/loggene (SHA-256, rapport,
             DIAS, depotrapporter …)
  chain    — arbeidsflyt uten «Pakk ut»/«Pakk sammen» (hvert steg lager ny
             SIARD) — her gjelder den gamle regelen: alt etter første steg som
             må kjøres, kjøres

Regler (pipeline):
  - Fullførte steg hoppes over; resultatet hentes fra prosjektfila.
  - Feilet, avbrutt, holdt igjen, tilbakestilt eller nytt steg kjøres.
  - Kjøres et innholdssteg, må «Pakk sammen» og alle steg etter den kjøres på
    nytt (innholdet er endret). Innholdssteg som avhenger av et steg som
    kjøres på nytt (f.eks. BLOB etter HEX), kjøres også.
  - Finnes temp-mappa fortsatt, brukes den. Er den borte, velges grunnlag:
      «original» — pakk ut original-SIARD og kjør alle innholdssteg på nytt
      «output»   — pakk ut siste resultat (_konvertert.siard) og kjør bare
                   de stegene som må kjøres
    Forslaget er «original» når et steg som må ha uberørte data kjøres
    (BLOB, anonymisering, HEX …), ellers «output».

Under kjøringen (gui/app.py): feiler eller avbrytes et innholdssteg, holdes
«Pakk sammen» og stegene som avhenger av den igjen (`held_on_content_failure`),
og temp-mappa beholdes — neste kjøring tar bare det som gjenstår.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from siard_workflow.core.project_file import (
    DONE_STATUSES, STATUS_USER_SKIP, OpCheckpoint)

# Steg som leser resultatet/loggene uansett hvor de står i arbeidsflyten
_DERIVED_IDS = {"workflow_report", "depot_reports", "dias_package", "metadata_extract"}

# Steg som bør kjøre på uberørte data når de kjøres på nytt
_ORIGINAL_BASE_IDS = {"blob_convert", "anonymize", "hex_extract", "standardize_ext"}
_SYSTEM_CATEGORY = "Systemspesifikt"

# Innholdssteg som bygger på resultatet av andre innholdssteg (samme regler
# som rekkefølgekontrollen i workflow_validator.py). Kjøres et av stegene i
# settet på nytt, må steget kjøres på nytt også.
_CONTENT_DEPENDS: dict[str, set[str]] = {
    "blob_convert": {"hex_extract", "standardize_ext", "segfolder_fix",
                     "lobfolder_fix", _SYSTEM_CATEGORY},
    "hex_extract":  {"standardize_ext", "segfolder_fix", _SYSTEM_CATEGORY},
}

# Holdes ikke igjen når et innholdssteg feiler — rapporten skal fortelle hva
# som skjedde (merkes «foreløpig»)
_NEVER_HELD = {"workflow_report"}

ACTION_SKIP      = "skip"        # fullført før — hoppes over
ACTION_RUN       = "run"         # ikke fullført (ny, feilet, avbrutt, tilbakestilt)
ACTION_RERUN     = "rerun"       # fullført før, men må kjøres på nytt
ACTION_USER_SKIP = "user_skip"   # brukeren har valgt å hoppe over steget

BASE_ORIGINAL = "original"
BASE_OUTPUT   = "output"


@dataclass
class StepPlan:
    index:  int
    op_id:  str
    label:  str
    kind:   str
    action: str
    reason: str = ""
    status: str = ""            # status i prosjektfila før kjøringen

    @property
    def runs(self) -> bool:
        return self.action in (ACTION_RUN, ACTION_RERUN)


@dataclass
class RunPlan:
    steps:            list[StepPlan] = field(default_factory=list)
    mode:             str = "pipeline"      # pipeline | chain
    workdir_ok:       bool = False          # temp-mappa fra forrige kjøring finnes
    base:             str | None = None     # valgt grunnlag når temp-mappa må lages på nytt
    base_suggestion:  str | None = None
    base_reason:      str = ""
    base_choice:      bool = False          # brukeren bør få velge grunnlag
    output_siard:     str | None = None     # siste «Pakk sammen»-resultat
    held_on_content_failure: list[int] = field(default_factory=list)

    @property
    def has_history_skips(self) -> bool:
        return any(s.action in (ACTION_SKIP, ACTION_USER_SKIP) for s in self.steps)

    @property
    def run_indices(self) -> list[int]:
        return [s.index for s in self.steps if s.runs]

    @property
    def nothing_to_do(self) -> bool:
        return not self.run_indices

    def step(self, index: int) -> StepPlan | None:
        return self.steps[index] if 0 <= index < len(self.steps) else None

    def lines(self) -> list[str]:
        """Planen som tekst (logg/dialog)."""
        icon = {ACTION_SKIP: "✓", ACTION_RUN: "▶", ACTION_RERUN: "↻",
                ACTION_USER_SKIP: "–"}
        return [f"{icon.get(s.action, '?')} [{s.index + 1}] {s.label}"
                + (f" — {s.reason}" if s.reason else "") for s in self.steps]


# ── Klassifisering ────────────────────────────────────────────────────────────

def _cat(op) -> str:
    return str(getattr(op, "category", "") or "")


def step_kinds(ops: list) -> tuple[list[str], str]:
    """Klassifiser stegene. Returnerer (kinds, mode)."""
    ids = [getattr(op, "operation_id", "") for op in ops]
    u = ids.index("unpack_siard") if "unpack_siard" in ids else None
    r = None
    if u is not None:
        r = next((i for i in range(u + 1, len(ids)) if ids[i] == "repack_siard"), None)
    if u is None:
        return ["chain"] * len(ops), "chain"
    end = r if r is not None else len(ops)
    kinds: list[str] = []
    for i, op in enumerate(ops):
        oid = ids[i]
        if i == u:
            kinds.append("unpack")
        elif i == r:
            kinds.append("package")
        elif i < u:
            kinds.append("source")
        elif oid in _DERIVED_IDS or (r is not None and i > r):
            kinds.append("derived")
        elif u < i < end:
            content = (getattr(op, "modifies_content", False)
                       or getattr(op, "requires_unpack", False)
                       or _cat(op) == _SYSTEM_CATEGORY)
            kinds.append("content" if content else "inspect")
        else:
            kinds.append("derived")
    return kinds, "pipeline"


def rerun_base_for(op) -> str:
    """Hvilket grunnlag steget helst skal kjøre på når det kjøres på nytt."""
    if (getattr(op, "operation_id", "") in _ORIGINAL_BASE_IDS
            or _cat(op) == _SYSTEM_CATEGORY):
        return BASE_ORIGINAL
    return BASE_OUTPUT


def _depends(op_after, op_before) -> bool:
    deps = _CONTENT_DEPENDS.get(getattr(op_after, "operation_id", ""), set())
    return (getattr(op_before, "operation_id", "") in deps
            or (_SYSTEM_CATEGORY in deps and _cat(op_before) == _SYSTEM_CATEGORY))


# ── Planen ────────────────────────────────────────────────────────────────────

def _status(cp: "OpCheckpoint | None") -> str:
    return cp.status if cp else "pending"


def _undone_reason(cp: "OpCheckpoint | None") -> str:
    st = _status(cp)
    if cp is not None and cp.reset_reason:
        return f"kjøres på nytt ({cp.reset_reason})"
    return {
        "failed":      "feilet forrige gang",
        "interrupted": "ble avbrutt — fortsetter",
        "held":        "ble holdt igjen forrige gang",
    }.get(st, "")


def build_run_plan(ops: list, checkpoints: "list[OpCheckpoint] | None",
                   workdir_ok: bool = False, output_siard: "str | None" = None,
                   base: "str | None" = None) -> RunPlan:
    """Lag kjøreplan.

    ops:          operasjonene i arbeidsflyten (rekkefølge = kjørerekkefølge)
    checkpoints:  prosjektfilas checkpoints, posisjonelt synket mot ops
                  (None = ny kjøring: alt kjøres)
    workdir_ok:   temp-mappa fra «Pakk ut» finnes fortsatt
    output_siard: siste «Pakk sammen»-resultat (hvis det finnes på disk)
    base:         valgt grunnlag (None = bruk forslaget)
    """
    kinds, mode = step_kinds(ops)
    cps = list(checkpoints or [])
    cps += [None] * (len(ops) - len(cps))
    plan = RunPlan(mode=mode, workdir_ok=workdir_ok, output_siard=output_siard)

    def mk(i, action, reason=""):
        op = ops[i]
        return StepPlan(i, getattr(op, "operation_id", ""), getattr(op, "label", ""),
                        kinds[i], action, reason, _status(cps[i]))

    user_skip = [_status(cp) == STATUS_USER_SKIP for cp in cps]
    done = [_status(cp) in DONE_STATUSES for cp in cps]

    if mode == "chain":
        first = next((i for i in range(len(ops)) if not done[i]), None)
        for i in range(len(ops)):
            if user_skip[i]:
                plan.steps.append(mk(i, ACTION_USER_SKIP, "hoppes over (valgt)"))
            elif first is None or i < first:
                plan.steps.append(mk(i, ACTION_SKIP, _done_reason(cps[i])))
            elif not done[i]:
                plan.steps.append(mk(i, ACTION_RUN, _undone_reason(cps[i])))
            else:
                plan.steps.append(mk(i, ACTION_RERUN,
                                     f"bygger på «{ops[first].label}», som kjøres"))
        return plan

    u = kinds.index("unpack")
    r = kinds.index("package") if "package" in kinds else None
    content = [i for i, k in enumerate(kinds) if k == "content"]
    actions: dict[int, tuple[str, str]] = {}

    # 1. Innholdssteg som ikke er fullført + de som avhenger av dem
    need_content: dict[int, str] = {}
    for i in content:
        if user_skip[i]:
            continue
        if not done[i]:
            need_content[i] = ""
    for j in content:
        if j in need_content or user_skip[j]:
            continue
        dep = next((k for k in sorted(need_content) if k < j and _depends(ops[j], ops[k])), None)
        if dep is not None:
            need_content[j] = f"bygger på «{ops[dep].label}», som kjøres på nytt"

    unpack_needed = not done[u] and not user_skip[u]
    package_needed = r is not None and not done[r] and not user_skip[r]
    content_work = bool(need_content) or unpack_needed
    needs_workdir = content_work or package_needed

    base_used = None
    if needs_workdir:
        if unpack_needed:
            base_used = BASE_ORIGINAL
            plan.base_reason = "«Pakk ut» er ikke fullført"
        elif workdir_ok:
            base_used = None   # temp-mappa gjenbrukes
        else:
            out_ok = bool(output_siard) and Path(output_siard).exists()
            if not out_ok:
                base_used = BASE_ORIGINAL
                plan.base_reason = ("temp-mappa fra forrige kjøring er borte og det "
                                    "finnes ingen sammenpakket SIARD å bygge på")
            else:
                originals = [ops[i].label for i in need_content
                             if rerun_base_for(ops[i]) == BASE_ORIGINAL]
                plan.base_suggestion = BASE_ORIGINAL if originals else BASE_OUTPUT
                plan.base_reason = (
                    f"{', '.join(originals)} bør kjøre på uberørte data"
                    if originals else
                    "stegene som kjøres, tåler å bygge på siste resultat")
                plan.base_choice = True
                base_used = base or plan.base_suggestion
    plan.base = base_used

    # 2. Grunnlag «original»: alle innholdssteg kjøres på nytt
    if base_used == BASE_ORIGINAL:
        actions[u] = (ACTION_RUN if not done[u] else ACTION_RERUN,
                      "pakker ut original-SIARD på nytt" if done[u]
                      else _undone_reason(cps[u]))
        for i in content:
            if user_skip[i]:
                continue
            if i in need_content:
                actions[i] = (ACTION_RUN if not done[i] else ACTION_RERUN,
                              need_content[i] or _undone_reason(cps[i]))
            else:
                actions[i] = (ACTION_RERUN, "ny utpakking fra original — må kjøres igjen")
    elif base_used == BASE_OUTPUT:
        actions[u] = (ACTION_RERUN, "pakker ut siste resultat "
                      f"({Path(output_siard).name})" if output_siard else "")
        for i, why in need_content.items():
            actions[i] = (ACTION_RUN if not done[i] else ACTION_RERUN,
                          why or _undone_reason(cps[i]))
    else:
        # temp-mappa gjenbrukes (eller trengs ikke)
        for i, why in need_content.items():
            actions[i] = (ACTION_RUN if not done[i] else ACTION_RERUN,
                          why or _undone_reason(cps[i]))

    # 3. «Pakk sammen» og alt som bygger på resultatet
    if r is not None and not user_skip[r] and (content_work or package_needed
                                                 or base_used is not None):
        if done[r]:
            actions[r] = (ACTION_RERUN, "innholdet endres — må pakkes sammen på nytt")
        else:
            actions[r] = (ACTION_RUN, _undone_reason(cps[r]))
    repacking = r is not None and r in actions
    for i, k in enumerate(kinds):
        if i in actions or user_skip[i]:
            continue
        if k == "derived" and repacking and (r is None or i > r or
                                              getattr(ops[i], "operation_id", "")
                                              in _DERIVED_IDS):
            actions[i] = (ACTION_RERUN if done[i] else ACTION_RUN,
                          "resultatet endres — kjøres på nytt" if done[i]
                          else _undone_reason(cps[i]))
        elif not done[i]:
            actions[i] = (ACTION_RUN, _undone_reason(cps[i]))

    for i in range(len(ops)):
        if user_skip[i]:
            plan.steps.append(mk(i, ACTION_USER_SKIP, "hoppes over (valgt)"))
        elif i in actions:
            plan.steps.append(mk(i, *actions[i]))
        else:
            plan.steps.append(mk(i, ACTION_SKIP, _done_reason(cps[i])))

    # Holdes igjen dersom et innholdssteg feiler/avbrytes under kjøringen
    if r is not None:
        plan.held_on_content_failure = [
            i for i, k in enumerate(kinds)
            if (i == r or (k == "derived" and (i > r or getattr(ops[i], "operation_id", "")
                                                in _DERIVED_IDS)))
            and getattr(ops[i], "operation_id", "") not in _NEVER_HELD]
    return plan


def _done_reason(cp: "OpCheckpoint | None") -> str:
    if cp is None:
        return ""
    when = _fmt_when(cp.completed_at)
    st = cp.status
    if st == "skipped":
        return "hoppet over forrige gang (vilkår ikke oppfylt)"
    return f"fullført {when}".strip() if when else "fullført"


def _fmt_when(iso: "str | None") -> str:
    if not iso:
        return ""
    try:
        import datetime
        d = datetime.datetime.fromisoformat(iso)
        return d.strftime("%d.%m %H:%M")
    except Exception:
        return str(iso)


def held_after_unpack_failure(plan: RunPlan) -> list[int]:
    """Feiler «Pakk ut», kan ingen innholdssteg kjøre — hold dem og resten igjen."""
    return [s.index for s in plan.steps
            if s.kind in ("content", "inspect", "package", "derived")
            and s.op_id not in _NEVER_HELD]
