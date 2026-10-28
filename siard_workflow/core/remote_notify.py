"""
siard_workflow/core/remote_notify.py
-------------------------------------
Varsler til mobil og svar fra mobil mens en workflow kjører.

Kanaler:
  ntfy   — gratis app (iPhone/Android). Programmet publiserer til et emne
           (ntfy.sh eller egen server); spørsmål får knapper som sender svaret
           til «<emne>-svar», som programmet lytter på. «Status nå» besvares
           med en statusmelding. Bare utgående HTTPS fra PC-en.
  Teams  — Workflows-webhook (erstatter Office 365-connectorene som Microsoft
           slo av i mai 2026). Bare varsler; spørsmål vises med teksten.

Meldingene er tekniske (filnavn, steg, spørsmålstekst). «Skjul filnavn»
erstatter SIARD-filnavnet med «fil k av n».

Alt nettverksarbeid skjer i egne tråder — en treg eller utilgjengelig tjeneste
skal aldri stoppe eller felle en kjøring. Bare stdlib (urllib).
"""
from __future__ import annotations

import json
import logging
import os
import queue
import secrets
import threading
import time
import urllib.parse
import urllib.request
from typing import Callable, Optional

logger = logging.getLogger(__name__)

MAX_MSG_BYTES = 3800            # ntfy: meldinger over 4 096 byte blir vedlegg
_UA = "SIARD-Workflow-Manager"
_SEND_TIMEOUT = 15
_STREAM_TIMEOUT = 120           # ntfy sender keepalive ca. hvert 45. s
_RETRY_S = 2.0                  # økende ventetid ved brudd (2, 4, 6 … maks 30 s)
_POLL_S = 10.0                  # intervall for reserve-avspørring
STATUS_WORDS = ("status", "status?", "status nå", "status na")


# Tegn som ikke kan forveksles når emnet skrives av på mobilen (uten l/1/i,
# o/0, b/6, g/9, q/9, s/5, z/2, u/v)
_TOPIC_CHARS = "acdefhjkmnprtwxy3478"


def new_topic() -> str:
    """Tilfeldig, vanskelig å gjette emnenavn — emnet er «passordet» på ntfy.sh."""
    return "siard-" + "".join(secrets.choice(_TOPIC_CHARS) for _ in range(12))


def trim_text(text: str, limit: int = MAX_MSG_BYTES) -> str:
    b = (text or "").encode("utf-8")
    if len(b) <= limit:
        return text or ""
    return b[:limit - 8].decode("utf-8", "ignore").rstrip() + "\n…"


def _fmt_secs(s: float) -> str:
    s = int(max(0, s))
    h, rest = divmod(s, 3600)
    m = rest // 60
    return f"{h} t {m} min" if h else f"{m} min" if m else f"{s} s"


# ── Kanaler ───────────────────────────────────────────────────────────────────

class NtfyChannel:
    """Publiser til et ntfy-emne og lytt på svar-emnet."""
    name = "ntfy"
    two_way = True

    def __init__(self, server: str, topic: str, token: str = ""):
        self.server = (server or "https://ntfy.sh").rstrip("/")
        self.topic = topic
        self.token = token or ""

    @property
    def reply_topic(self) -> str:
        return f"{self.topic}-svar"

    def _headers(self, content_json: bool = False) -> dict:
        h = {"User-Agent": _UA}
        if content_json:
            h["Content-Type"] = "application/json"
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def payload(self, msg: dict) -> dict:
        """JSON-publisering (unngår koding av æøå i HTTP-hoder)."""
        p = {"topic": self.topic,
             "title": msg["title"],
             "message": trim_text(msg["text"]),
             "priority": int(msg.get("priority", 3)),
             "tags": list(msg.get("tags", ()))}
        actions = []
        for label, body in msg.get("actions", ())[:3]:
            a = {"action": "http", "label": label[:40],
                 "url": f"{self.server}/{self.reply_topic}",
                 "method": "POST", "body": body, "clear": True}
            if self.token:
                a["headers"] = {"Authorization": f"Bearer {self.token}"}
            actions.append(a)
        if actions:
            p["actions"] = actions
        return p

    def send(self, msg: dict) -> None:
        req = urllib.request.Request(
            self.server + "/", data=json.dumps(self.payload(msg)).encode("utf-8"),
            headers=self._headers(True), method="POST")
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as r:
            r.read()

    def listen(self, since: str, on_message: Callable[[str], None],
               stop: threading.Event) -> None:
        """
        Les svar-emnet som en strøm (én åpen forbindelse). Ved brudd: koble til
        igjen fra siste mottatte melding (since=<id>), med økende ventetid. Får
        strømmen ikke kontakt flere ganger på rad (f.eks. en proxy som holder
        igjen strømmen), brukes korte avspørringer (poll=1) hvert 10. sekund.
        """
        last = since
        failures = 0
        while not stop.is_set():
            poll = failures >= 3
            url = (f"{self.server}/{self.reply_topic}/json?since="
                   f"{urllib.parse.quote(str(last))}" + ("&poll=1" if poll else ""))
            try:
                req = urllib.request.Request(url, headers=self._headers())
                with urllib.request.urlopen(
                        req, timeout=_SEND_TIMEOUT if poll else _STREAM_TIMEOUT) as r:
                    if not poll:
                        failures = 0
                    for raw in r:
                        if stop.is_set():
                            return
                        try:
                            ev = json.loads(raw.decode("utf-8"))
                        except ValueError:
                            continue
                        if ev.get("event") == "message":
                            last = ev.get("id") or last
                            try:
                                on_message(ev.get("message", ""))
                            except Exception:
                                logger.exception("ntfy: feil i svarbehandling")
                if poll:
                    stop.wait(_POLL_S)
            except Exception as exc:
                failures += 1
                logger.info("ntfy-lytter: %s (forsøk %d)", exc, failures)
                stop.wait(_POLL_S if poll else min(30.0, _RETRY_S * failures))


class TeamsChannel:
    """Teams via Workflows-webhook: Adaptive Card i en chat eller kanal."""
    name = "teams"
    two_way = False

    def __init__(self, url: str):
        self.url = url.strip()

    @staticmethod
    def payload(msg: dict) -> dict:
        prio = int(msg.get("priority", 3))
        body = [{"type": "TextBlock", "text": msg["title"], "weight": "Bolder",
                 "size": "Medium", "wrap": True,
                 "color": "Attention" if prio >= 4 else "Default"}]
        # Én TextBlock per linje — Teams slår ellers sammen enkle linjeskift
        for line in trim_text(msg["text"], 12000).splitlines()[:60]:
            body.append({"type": "TextBlock", "text": line or " ", "wrap": True,
                         "spacing": "None"})
        if msg.get("question"):
            body.append({"type": "TextBlock", "wrap": True, "isSubtle": True,
                         "text": "Svar på PC-en, eller med knappene i ntfy-appen."})
        card = {"type": "AdaptiveCard", "version": "1.4",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "body": body}
        return {"type": "message", "attachments": [
            {"contentType": "application/vnd.microsoft.card.adaptive",
             "contentUrl": None, "content": card}]}

    def send(self, msg: dict) -> None:
        req = urllib.request.Request(
            self.url, data=json.dumps(self.payload(msg)).encode("utf-8"),
            headers={"User-Agent": _UA, "Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as r:
            r.read()


# ── Spørsmål ──────────────────────────────────────────────────────────────────

class Ticket:
    """Et åpent spørsmål. Svaret kommer fra lytter-tråden (mobil)."""

    def __init__(self, hub: "RemoteHub", qid: str, title: str, choices: list):
        self.hub = hub
        self.qid = qid
        self.title = title
        self.choices = dict(choices)
        self.answer: Optional[str] = None
        self._ev = threading.Event()
        self._closed = False

    def deliver(self, key: str) -> bool:
        if key not in self.choices or self._ev.is_set():
            return False
        self.answer = key
        self._ev.set()
        return True

    def poll(self) -> Optional[str]:
        return self.answer

    def wait_answer(self, timeout: float) -> Optional[str]:
        return self.answer if self._ev.wait(timeout) else None

    def close(self, by: str = "på PC-en", label: str = "") -> None:
        if not self._closed:
            self._closed = True
            self.hub._close_ticket(self, by, label)


# ── Hub ───────────────────────────────────────────────────────────────────────

def machine_name() -> str:
    """Maskinnavnet (COMPUTERNAME) — vises i meldingene så de kan skilles fra hverandre."""
    try:
        import socket
        return (os.environ.get("COMPUTERNAME") or socket.gethostname() or "").strip()
    except Exception:
        return ""


class RemoteHub:
    def __init__(self, channels: list, log: "Callable[[str, str], None] | None" = None,
                 origin: str = ""):
        self.channels = list(channels)
        self._log = log
        self.origin = origin              # maskinnavn foran tittelen: «[ARKSOR-01] …»
        self._q: "queue.Queue[dict | None]" = queue.Queue()
        self._tickets: dict = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list = []
        self._failed: set = set()
        self._status_provider: "Callable[[], str] | None" = None
        self.started_at = time.time()

    # Egenskaper
    @property
    def can_ask(self) -> bool:
        return any(c.two_way for c in self.channels)

    def set_status_provider(self, fn: "Callable[[], str]") -> None:
        self._status_provider = fn

    # Livsløp
    def start(self) -> "RemoteHub":
        self.started_at = time.time()
        t = threading.Thread(target=self._sender_loop, name="remote-send", daemon=True)
        t.start()
        self._threads.append(t)
        since = str(int(self.started_at) - 2)
        for ch in self.channels:
            if ch.two_way:
                lt = threading.Thread(target=ch.listen, args=(since, self._on_reply, self._stop),
                                      name=f"remote-listen-{ch.name}", daemon=True)
                lt.start()
                self._threads.append(lt)
        return self

    def stop(self, flush_timeout: float = 10.0) -> None:
        """Send det som ligger i køen (inntil flush_timeout), og stopp trådene."""
        self._q.put(None)
        for t in self._threads[:1]:
            t.join(flush_timeout)
        self._stop.set()

    # Sending
    def notify(self, title: str, text: str = "", priority: int = 3, tags=(),
               status_button: bool = True, _actions=None, _question: bool = False) -> None:
        actions = list(_actions or [])
        if status_button and len(actions) < 3 and self.can_ask:
            actions.append(("Status nå", "status"))
        if self.origin:
            title = f"[{self.origin}] {title}"
        self._q.put({"title": title, "text": text, "priority": priority,
                     "tags": tuple(tags), "actions": actions, "question": _question})

    def ask(self, title: str, text: str, choices: list) -> Ticket:
        """Send et spørsmål med knapper (maks 3 valg). Svaret kommer i Ticket."""
        qid = secrets.token_hex(4)
        t = Ticket(self, qid, title, choices)
        with self._lock:
            self._tickets[qid] = t
        actions = [(label, f"q={qid}&a={key}") for key, label in choices[:3]]
        self.notify(f"Venter på svar: {title}", text, priority=4, tags=("question",),
                    status_button=False, _actions=actions, _question=True)
        return t

    def _close_ticket(self, t: Ticket, by: str, label: str) -> None:
        with self._lock:
            self._tickets.pop(t.qid, None)
        self.notify(f"Besvart: {t.title}",
                    (f"«{label}» " if label else "") + f"(svar gitt {by})",
                    priority=2, tags=("white_check_mark",), status_button=False)

    def open_question(self) -> Optional[str]:
        with self._lock:
            return next((t.title for t in self._tickets.values()), None)

    def _on_reply(self, body: str) -> None:
        body = (body or "").strip()
        if body.lower() in STATUS_WORDS:
            self.send_status()
            return
        q = urllib.parse.parse_qs(body)
        qid, key = (q.get("q") or [""])[0], (q.get("a") or [""])[0]
        with self._lock:
            t = self._tickets.get(qid)
        if t is not None:
            t.deliver(key)

    def send_status(self) -> None:
        try:
            text = self._status_provider() if self._status_provider else ""
        except Exception as exc:
            text = f"(status utilgjengelig: {exc})"
        self.notify("Status", text or "Ingen jobb kjører.", priority=3,
                    tags=("information_source",))

    def _sender_loop(self) -> None:
        while True:
            msg = self._q.get()
            if msg is None:
                return
            for ch in self.channels:
                m = msg if ch.two_way else {**msg, "actions": ()}
                try:
                    ch.send(m)
                except Exception as exc:
                    if ch.name not in self._failed:
                        self._failed.add(ch.name)
                        txt = (f"  Mobilvarsling ({ch.name}) feilet: {exc} — "
                               "jobben fortsetter uten varsler herfra")
                        logger.warning(txt)
                        if self._log:
                            try:
                                self._log(txt, "warn")
                            except Exception:
                                pass


def build_hub(get_config: Callable, log=None, force: bool = False) -> "RemoteHub | None":
    """Hub fra innstillingene, eller None når varsling er av / ikke satt opp."""
    if not force and not get_config("notify_enabled", False):
        return None
    channels: list = []
    topic = (get_config("ntfy_topic", "") or "").strip()
    if get_config("ntfy_enabled", True) and topic:
        channels.append(NtfyChannel(get_config("ntfy_server", "https://ntfy.sh") or "",
                                    topic, get_config("ntfy_token", "") or ""))
    url = (get_config("teams_webhook_url", "") or "").strip()
    if url:
        channels.append(TeamsChannel(url))
    return RemoteHub(channels, log=log, origin=machine_name()) if channels else None


# ── Rapportering fra en kjøring ───────────────────────────────────────────────

class RunReporter:
    """
    Holder rede på hvor kjøringen er (for «Status nå») og sender varsler ved
    start, feil, ferdig og livstegn. Alle metoder er trygge å kalle fra
    arbeidstråden og kaster aldri.
    """

    def __init__(self, hub: RemoteHub, hide_filenames: bool = False,
                 each_step: bool = False, heartbeat: bool = True):
        self.hub = hub
        self.hide = hide_filenames
        self.each_step = each_step
        self.heartbeat_on = heartbeat
        self.n_files = 0
        self.file_idx = 0
        self.file_name = ""
        self.file_t0 = 0.0
        self.step_idx = 0
        self.step_total = 0
        self.step_label = ""
        self.step_t0 = 0.0
        self.phase: dict = {}
        self.last_log = ""
        self.queue_t0 = time.time()
        self.failed_steps: list = []
        hub.set_status_provider(self.status_text)

    # Navn
    def _name(self, name: str = "") -> str:
        name = name or self.file_name
        if self.hide or not name:
            return f"fil {self.file_idx + 1} av {self.n_files}"
        return name

    def _scrub(self, text: str) -> str:
        if self.hide and self.file_name:
            stem = self.file_name.rsplit(".", 1)[0]
            text = text.replace(self.file_name, self._name()).replace(stem, self._name())
        return text

    def _safe(fn):                                              # noqa: N805
        def wrap(self, *a, **k):
            try:
                return fn(self, *a, **k)
            except Exception:
                logger.exception("RunReporter.%s", fn.__name__)
        return wrap

    # Hendelser
    @_safe
    def queue_start(self, n_files: int) -> None:
        self.n_files = n_files
        self.queue_t0 = time.time()

    @_safe
    def file_start(self, idx: int, total: int, name: str, n_steps: int) -> None:
        self.file_idx, self.n_files, self.file_name = idx, total, name
        self.file_t0 = time.time()
        self.step_total = n_steps
        self.failed_steps = []
        self.hub.notify(f"Startet: {self._name()}",
                        f"Fil {idx + 1} av {total} · {n_steps} steg",
                        priority=2, tags=("arrow_forward",))

    @_safe
    def step_start(self, idx: int, total: int, label: str) -> None:
        self.step_idx, self.step_total, self.step_label = idx, total, label
        self.step_t0 = time.time()
        self.phase = {}

    @_safe
    def step_done(self, idx: int, label: str, success: bool, message: str,
                  elapsed: float) -> None:
        if not success:
            self.failed_steps.append(label)
            self.hub.notify(f"Steg feilet: {label}",
                            self._scrub(f"{self._name()} · steg {idx + 1} av "
                                        f"{self.step_total}\n{message}"),
                            priority=4, tags=("warning",))
        elif self.each_step:
            self.hub.notify(f"Ferdig: {label}",
                            f"{self._name()} · steg {idx + 1} av {self.step_total} · "
                            f"{_fmt_secs(elapsed)}", priority=2, tags=("heavy_check_mark",))

    @_safe
    def halted(self, label: str, message: str) -> None:
        self.hub.notify(f"Stoppet: {self._name()}",
                        self._scrub(f"«{label}» feilet, og arbeidsflyten er stoppet "
                                    f"for denne filen.\n{message}"),
                        priority=5, tags=("rotating_light",))

    @_safe
    def file_done(self, idx: int, total: int, success: bool) -> None:
        dur = _fmt_secs(time.time() - self.file_t0)
        if success:
            title, prio, tag = f"Ferdig: {self._name()}", 3, "white_check_mark"
            text = f"Fil {idx + 1} av {total} fullført på {dur}."
        else:
            title, prio, tag = f"Ferdig med feil: {self._name()}", 4, "x"
            text = (f"Fil {idx + 1} av {total} · {dur}\nFeilet: "
                    + ", ".join(self.failed_steps or ["(se loggen)"]))
        self.hub.notify(title, text, priority=prio, tags=(tag,))

    @_safe
    def queue_done(self, stopped: bool = False) -> None:
        if self.n_files > 1 or stopped:
            dur = _fmt_secs(time.time() - self.queue_t0)
            self.hub.notify("Kjøringen er stoppet" if stopped else "Alle filer er ferdige",
                            f"{self.n_files} fil(er) · {dur}", priority=3,
                            tags=("stop_sign" if stopped else "tada",), status_button=False)

    @_safe
    def heartbeat(self, text: str) -> None:
        if self.heartbeat_on:
            self.hub.notify(f"Livstegn: {self._name()}", self._scrub(text),
                            priority=1, tags=("hourglass",))

    @_safe
    def progress(self, event: str, kw: dict) -> None:
        if event == "log":
            msg = (kw.get("msg") or "").strip()
            if msg:
                self.last_log = msg
        elif event == "phase_progress":
            self.phase = {k: kw.get(k) for k in ("done", "total", "label", "text", "eta")}

    # Status
    def status_text(self) -> str:
        if not self.file_name:
            return "Ingen jobb kjører."
        lines = [f"{self._name()} (fil {self.file_idx + 1} av {self.n_files})",
                 f"Steg {self.step_idx + 1} av {self.step_total}: {self.step_label}",
                 f"Tid i steget: {_fmt_secs(time.time() - self.step_t0)}"
                 f" · totalt {_fmt_secs(time.time() - self.queue_t0)}"]
        ph = self.phase
        if ph.get("total"):
            pct = 100.0 * (ph.get("done") or 0) / ph["total"]
            lines.append(f"Fremdrift: {pct:.0f} % ({ph.get('done'):,} av {ph['total']:,})"
                         .replace(",", " ")
                         + (f" · {ph['eta']}" if ph.get("eta") else ""))
            if ph.get("text"):
                lines.append(str(ph["text"]))
        waiting = self.hub.open_question()
        if waiting:
            lines.append(f"VENTER PÅ SVAR: {waiting}")
        if self.last_log:
            lines.append(f"Siste logglinje: {self.last_log[:200]}")
        return self._scrub("\n".join(lines))
