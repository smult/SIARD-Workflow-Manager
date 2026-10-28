"""
Varsling til mobil og svar fra mobil (core/remote_notify + gui/app).

Kjører mot en lokal, falsk ntfy-/Teams-server (http.server i en tråd) —
ingenting sendes ut av maskinen.

Verifiserer:
  1. ntfy: JSON-publisering (tittel/tekst med æøå, prioritet, «Status nå»-knapp
     mot <emne>-svar), lange tekster kuttes
  2. spørsmål → knapp på mobilen → svaret kommer fram (strøm); ukjent qid og
     meldinger fra før start ignoreres; reserve-avspørring når strømmen svikter
  3. «status» fra mobilen → statusmelding fra RunReporter
  4. Teams: Adaptive Card, én linje per TextBlock, ingen knapper; bare Teams →
     kan ikke spørre
  5. server nede → ingen unntak, én loggmelding
  6. RunReporter: «Skjul filnavn», feilet steg (prioritet 4), fil ferdig
  7. build_hub fra innstillinger
  8. GUI: et ja/nei-vindu lukkes av svar fra mobilen; lokalt svar gir
     «Besvart … på PC-en»

Kjør:  python -X utf8 tests/test_remote_notify.py
"""
from __future__ import annotations

import json
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


def _until(fn, timeout=8.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = fn()
        if v:
            return v
        time.sleep(0.05)
    return None


# ── Falsk ntfy/Teams-server ──────────────────────────────────────────────────

class FakeServer:
    def __init__(self):
        self.published: list = []        # JSON fra POST /
        self.topics: dict = {}           # topic -> [{id, time, message}]
        self.teams: list = []
        self.stream_broken = False
        self.n = 0
        self.lock = threading.Lock()
        srv = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                path = self.path.strip("/")
                if path == "":
                    srv.published.append(json.loads(body.decode("utf-8")))
                elif path == "teams":
                    srv.teams.append(json.loads(body.decode("utf-8")))
                else:
                    srv.add(path, body.decode("utf-8"))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"{}")

            def do_GET(self):
                u = urllib.parse.urlparse(self.path)
                topic = u.path.strip("/").rsplit("/json", 1)[0]
                q = urllib.parse.parse_qs(u.query)
                since = (q.get("since") or ["all"])[0]
                poll = "poll" in q
                if not poll and srv.stream_broken:
                    self.send_response(502)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.end_headers()
                sent = set()
                t0 = time.time()
                try:
                    while True:
                        for m in srv.after(topic, since):
                            if m["id"] in sent:
                                continue
                            sent.add(m["id"])
                            self.wfile.write((json.dumps({"event": "message", **m}) + "\n")
                                             .encode("utf-8"))
                            self.wfile.flush()
                        if poll or time.time() - t0 > 20:
                            return
                        time.sleep(0.05)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    return

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.httpd.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def add(self, topic: str, message: str, t: "float | None" = None) -> None:
        with self.lock:
            self.n += 1
            self.topics.setdefault(topic, []).append(
                {"id": f"m{self.n}", "time": int(t or time.time()), "message": message})

    def after(self, topic: str, since: str) -> list:
        with self.lock:
            msgs = list(self.topics.get(topic, []))
        if since == "all":
            return msgs
        if since.isdigit():
            return [m for m in msgs if m["time"] >= int(since)]
        ids = [m["id"] for m in msgs]
        return msgs[ids.index(since) + 1:] if since in ids else msgs

    def phone_press(self, msg: dict, label: str) -> None:
        """Som når brukeren trykker en knapp i ntfy-appen."""
        a = next(a for a in msg["actions"] if a["label"] == label)
        req = urllib.request.Request(a["url"], data=a["body"].encode("utf-8"), method="POST")
        urllib.request.urlopen(req, timeout=5).read()

    def stop(self):
        self.httpd.shutdown()


# ── Tester ───────────────────────────────────────────────────────────────────

def test_publish(srv: FakeServer) -> None:
    print("test_publish")
    from siard_workflow.core.remote_notify import NtfyChannel, RemoteHub
    hub = RemoteHub([NtfyChannel(srv.url, "siard-test")]).start()
    hub.notify("Ferdig: Ålesund_æøå.siard", "Fil 1 av 1 fullført", priority=4,
               tags=("white_check_mark",))
    hub.notify("Lang", "x" * 10000)
    m = _until(lambda: len(srv.published) >= 2 and srv.published)
    hub.stop()
    p = m[0]
    _ok(p["topic"] == "siard-test" and p["title"] == "Ferdig: Ålesund_æøå.siard"
        and p["priority"] == 4, "JSON-publisering med æøå og prioritet")
    a = p["actions"][0]
    _ok(a["label"] == "Status nå" and a["url"] == f"{srv.url}/siard-test-svar"
        and a["body"] == "status" and a["clear"] is True,
        "«Status nå»-knapp sender til <emne>-svar")
    _ok(len(m[1]["message"].encode("utf-8")) < 4096, "lange tekster kuttes under 4 096 byte")
    srv.published.clear()
    hub2 = RemoteHub([NtfyChannel(srv.url, "siard-test")], origin="ARKSOR-01").start()
    hub2.notify("Ferdig: Extens VAF.siard", "")
    m2 = _until(lambda: srv.published and srv.published[0])
    hub2.stop()
    _ok(m2["title"] == "[ARKSOR-01] Ferdig: Extens VAF.siard",
        "maskinnavnet står foran tittelen")


def test_question_roundtrip(srv: FakeServer) -> None:
    print("test_question_roundtrip")
    from siard_workflow.core.remote_notify import NtfyChannel, RemoteHub
    srv.published.clear()
    srv.add("siard-q-svar", "q=gammel&a=ja", t=time.time() - 3600)   # fra før start
    hub = RemoteHub([NtfyChannel(srv.url, "siard-q")]).start()
    t = hub.ask("Datospenn funnet", "Estimert 2014–2020", [("ja", "Ja"), ("nei", "Nei")])
    q = _until(lambda: next((p for p in srv.published if p["title"].startswith("Venter")),
                            None))
    _ok(q is not None and [a["label"] for a in q["actions"]] == ["Ja", "Nei"]
        and q["priority"] == 4, "spørsmålet har knapper og høy prioritet")
    _ok(hub.open_question() == "Datospenn funnet", "åpent spørsmål vises i status")
    srv.add("siard-q-svar", "q=feil-id&a=ja")
    time.sleep(0.4)
    _ok(t.poll() is None, "ukjent qid og meldinger fra før start ignoreres")
    srv.phone_press(q, "Nei")
    _ok(t.wait_answer(5) == "nei", "knappetrykk på mobilen gir svaret «nei»")
    t.close("på mobil", "Nei")
    done = _until(lambda: next((p for p in srv.published if p["title"].startswith("Besvart")),
                               None))
    _ok(done is not None and "svar gitt på mobil" in done["message"],
        "«Besvart»-melding etter svaret")
    hub.stop()


def test_poll_fallback(srv: FakeServer) -> None:
    print("test_poll_fallback")
    import siard_workflow.core.remote_notify as R
    old = (R._RETRY_S, R._POLL_S)
    R._RETRY_S, R._POLL_S = 0.05, 0.1
    srv.stream_broken = True
    try:
        srv.published.clear()
        hub = R.RemoteHub([R.NtfyChannel(srv.url, "siard-p")]).start()
        t = hub.ask("Test", "x", [("ok", "OK")])
        q = _until(lambda: next((p for p in srv.published if p.get("actions")), None))
        srv.phone_press(q, "OK")
        _ok(t.wait_answer(8) == "ok", "strømmen svikter → svaret hentes ved avspørring")
        hub.stop()
    finally:
        srv.stream_broken = False
        R._RETRY_S, R._POLL_S = old


def test_status_command(srv: FakeServer) -> None:
    print("test_status_command")
    from siard_workflow.core.remote_notify import NtfyChannel, RemoteHub, RunReporter
    srv.published.clear()
    hub = RemoteHub([NtfyChannel(srv.url, "siard-s")]).start()
    rep = RunReporter(hub)
    rep.queue_start(2)
    rep.file_start(0, 2, "Profil_Kristiansand.siard", 5)
    rep.step_start(2, 5, "BLOB-konvertering")
    rep.progress("phase_progress", {"done": 1500, "total": 6000, "eta": "ca. 2 t igjen"})
    rep.progress("log", {"msg": "Konverterer batch 30"})
    srv.add("siard-s-svar", "status")
    st = _until(lambda: next((p for p in srv.published if p["title"] == "Status"), None))
    _ok(st is not None and "Steg 3 av 5: BLOB-konvertering" in st["message"]
        and "25 %" in st["message"] and "ca. 2 t igjen" in st["message"]
        and "Profil_Kristiansand.siard" in st["message"],
        "«status» fra mobilen gir fil, steg, fremdrift og ETA")
    hub.stop()


def test_teams(srv: FakeServer) -> None:
    print("test_teams")
    from siard_workflow.core.remote_notify import RemoteHub, TeamsChannel
    hub = RemoteHub([TeamsChannel(srv.url + "/teams")]).start()
    _ok(not hub.can_ask, "bare Teams: kan ikke spørre (bare varsler)")
    hub.notify("Steg feilet: DIAS-pakking", "linje 1\nlinje 2", priority=4)
    m = _until(lambda: srv.teams and srv.teams[0])
    hub.stop()
    att = m["attachments"][0]
    body = att["content"]["body"]
    _ok(m["type"] == "message"
        and att["contentType"] == "application/vnd.microsoft.card.adaptive"
        and body[0]["text"] == "Steg feilet: DIAS-pakking" and body[0]["color"] == "Attention"
        and [b["text"] for b in body[1:3]] == ["linje 1", "linje 2"]
        and "actions" not in att["content"],
        "Adaptive Card med én TextBlock per linje, ingen knapper")


def test_server_down() -> None:
    print("test_server_down")
    from siard_workflow.core.remote_notify import NtfyChannel, RemoteHub, TeamsChannel
    logs: list = []
    hub = RemoteHub([NtfyChannel("http://127.0.0.1:9", "x"),
                     TeamsChannel("http://127.0.0.1:9/teams")],
                    log=lambda m, l: logs.append(m)).start()
    for i in range(3):
        hub.notify(f"melding {i}", "")
    hub.stop(15)
    _ok(len(logs) == 2 and all("feilet" in m for m in logs),
        f"server nede: ingen unntak, én loggmelding per kanal ({len(logs)})")


def test_reporter(srv: FakeServer) -> None:
    print("test_reporter")
    from siard_workflow.core.remote_notify import NtfyChannel, RemoteHub, RunReporter
    srv.published.clear()
    hub = RemoteHub([NtfyChannel(srv.url, "siard-r")]).start()
    rep = RunReporter(hub, hide_filenames=True)
    rep.queue_start(2)
    rep.file_start(1, 2, "HemmeligKommune.siard", 4)
    rep.step_start(0, 4, "Pakk ut SIARD")
    rep.step_done(0, "Pakk ut SIARD", False, "Kan ikke lese HemmeligKommune.siard", 3.0)
    rep.heartbeat("LIVSTEGN: HemmeligKommune 2 t")
    rep.file_done(1, 2, False)
    msgs = _until(lambda: len(srv.published) >= 4 and list(srv.published))
    hub.stop()
    allt = json.dumps(msgs, ensure_ascii=False)
    _ok("HemmeligKommune" not in allt and "fil 2 av 2" in allt,
        "«Skjul filnavn»: filnavnet erstattes med «fil 2 av 2»")
    fail = next(p for p in msgs if p["title"].startswith("Steg feilet"))
    _ok(fail["priority"] == 4, "feilet steg varsles med prioritet 4")
    _ok(any(p["title"].startswith("Ferdig med feil") and "Pakk ut SIARD" in p["message"]
            for p in msgs), "fil ferdig med feil lister feilede steg")
    _ok(any(p["title"].startswith("Livstegn") and p["priority"] == 1 for p in msgs),
        "livstegn sendes stille (prioritet 1)")


def test_build_hub() -> None:
    print("test_build_hub")
    from siard_workflow.core.remote_notify import build_hub, new_topic
    cfg = {"notify_enabled": False, "ntfy_topic": "siard-a"}
    g = lambda k, d=None: cfg.get(k, d)
    _ok(build_hub(g) is None, "av → ingen hub")
    cfg["notify_enabled"] = True
    _ok([c.name for c in build_hub(g).channels] == ["ntfy"], "på + emne → ntfy")
    from siard_workflow.core.remote_notify import machine_name
    _ok(machine_name() and build_hub(g).origin == machine_name(),
        f"hub fra innstillingene merker meldingene med maskinnavnet ({machine_name()})")
    cfg["teams_webhook_url"] = "https://x.example/wf"
    _ok([c.name for c in build_hub(g).channels] == ["ntfy", "teams"], "+ Teams-lenke → begge")
    cfg.update(ntfy_topic="", teams_webhook_url="")
    _ok(build_hub(g) is None, "på, men ingenting satt opp → ingen hub")
    t = new_topic()
    _ok(t.startswith("siard-") and len(t) == 18 and t != new_topic(), "tilfeldig emnenavn")
    many = "".join(new_topic()[6:] for _ in range(300))
    _ok(not set(many) & set("l1io0b6g9q5sz2uv"),
        "emnet har ingen tegn som kan forveksles ved avskrift (l/1, o/0 …)")


def test_gui_remote_answer(srv: FakeServer) -> None:
    print("test_gui_remote_answer")
    import queue
    import customtkinter as ctk
    from gui.app import App
    from siard_workflow.core.remote_notify import NtfyChannel, RemoteHub
    srv.published.clear()
    root = ctk.CTk()
    root.geometry("800x600+0+0")
    root._log_queue = queue.Queue()
    root._log = lambda m, lvl="info": None
    for name in ("_remote_wait", "_apply_remote", "_ask_yes_no"):
        setattr(root, name, getattr(App, name).__get__(root))
    root._answer_suggestion = App._answer_suggestion
    root._remote_hub = RemoteHub([NtfyChannel(srv.url, "siard-g")]).start()
    results: list = []

    def run_once(answer_from):
        th = threading.Thread(target=lambda: results.append(
            root._ask_yes_no("Flere schemas funnet", "Legge til Schema-velger?",
                             "Ja, legg til", "Nei")))
        th.start()
        if answer_from == "mobil":
            def press():
                q = next((p for p in srv.published
                          if p["title"] == "Venter på svar: Flere schemas funnet"
                          and p not in seen), None)
                if q is None:
                    root.after(100, press)
                    return
                seen.append(q)
                threading.Thread(target=srv.phone_press, args=(q, "Ja, legg til"),
                                 daemon=True).start()
            root.after(300, press)
        else:
            def click():
                tops = [w for w in root.winfo_children()
                        if isinstance(w, ctk.CTkToplevel) and w.winfo_exists()]
                if not tops:
                    root.after(100, click)
                    return
                tops[0]._on_nei()
            root.after(800, click)

        def poll():
            if th.is_alive():
                root.after(100, poll)
            else:
                root.quit()
        root.after(100, poll)
        root.mainloop()

    seen: list = []
    try:
        run_once("mobil")
        _ok(results[-1] is True and not [w for w in root.winfo_children()
                                         if isinstance(w, ctk.CTkToplevel)
                                         and w.winfo_exists()],
            "svar fra mobilen lukker vinduet med «Ja»")
        run_once("pc")
        _ok(results[-1] is False, "lokalt svar på PC-en virker som før")
        besvart = _until(lambda: (lambda b: len(b) >= 2 and b)(
            [p["message"] for p in srv.published
             if p["title"] == "Besvart: Flere schemas funnet"]))
        _ok(besvart and any("svar gitt på mobil" in m for m in besvart)
            and any("svar gitt på PC-en" in m for m in besvart),
            "mobilen får «Besvart» både for mobil- og PC-svar")
        logs = []
        while not root._log_queue.empty():
            logs.append(root._log_queue.get())
        _ok(any("Svar fra mobil" in str(x) for x in logs), "mobilsvaret logges")
    finally:
        root._remote_hub.stop()
        root.destroy()


def main() -> int:
    srv = FakeServer()
    try:
        test_publish(srv)
        test_question_roundtrip(srv)
        test_poll_fallback(srv)
        test_status_command(srv)
        test_teams(srv)
        test_server_down()
        test_reporter(srv)
        test_build_hub()
        test_gui_remote_answer(srv)
    finally:
        srv.stop()
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
