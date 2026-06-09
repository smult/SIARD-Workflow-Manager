"""
siard_workflow/core/anonymize/ollama_client.py

Valgfri LOKAL Ollama-klient for å øke treffsikkerheten ved PII-deteksjon.

VIKTIG — personvern: denne klienten snakker KUN med en lokal Ollama-instans
(http://127.0.0.1:11434 som standard). Ingen data sendes til skytjenester. Hvis
Ollama ikke kjører, degraderer anonymiseringen til ren regex/heuristikk.

Bruker kun stdlib `urllib.request` (samme mønster som gui/update_checker.py og
core/identifiers/installer.py) — ingen ekstra avhengigheter.

Ollama-endepunkter:
  GET  /api/tags       — liveness + liste over installerte modeller
  POST /api/generate   — prompt → svar (stream=false)
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request

# Foretrukne modeller i prioritert rekkefølge hvis ingen er konfigurert.
# Gemma er liten/rask og egner seg godt til klassifisering lokalt.
_PREFERRED_MODELS = ("gemma3:4b", "gemma3", "gemma2", "gemma", "llama3.2",
                     "llama3.1", "llama3", "mistral", "qwen2.5", "phi3")

# Fast vokabular modellen MÅ svare innenfor (matcher PiiType-navn). Omfang:
# personnavn, personnummer, e-post og stedsangivelser ned på stedsnivå
# (adresse/postnr/sted). Telefon er bevisst utelatt (anonymiseres ikke).
_CLASSIFY_VOCAB = (
    "FNR", "FIRST_NAME", "LAST_NAME", "FULL_NAME", "ADDRESS", "POSTNR",
    "CITY", "EMAIL", "FREE_TEXT", "OTHER",
)


class OllamaClient:
    """Tynn klient mot en lokal Ollama. Alle nettverksfeil svelges og gir
    degradering (is_alive() == False eller tomt resultat)."""

    def __init__(self, host: str = "127.0.0.1", port: int = 11434,
                 model: str = "", timeout: int = 30):
        self.host = (host or "127.0.0.1").strip()
        self.port = int(port or 11434)
        self.model = (model or "").strip()
        self.timeout = int(timeout or 30)
        self._base = f"http://{self.host}:{self.port}"
        self._alive_cache: "bool | None" = None
        self._resolved_model: "str | None" = None

    # ── liveness / modellvalg ─────────────────────────────────────────────────

    def _get_json(self, path: str, timeout: "int | None" = None):
        req = urllib.request.Request(
            self._base + path,
            headers={"Accept": "application/json",
                     "User-Agent": "SIARD-Manager-anon/1.0"})
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def is_alive(self) -> bool:
        """True hvis lokal Ollama svarer. Caches per instans (rask kort timeout)."""
        if self._alive_cache is not None:
            return self._alive_cache
        try:
            self._get_json("/api/tags", timeout=min(3, self.timeout))
            self._alive_cache = True
        except Exception:
            self._alive_cache = False
        return self._alive_cache

    def list_models(self) -> "list[str]":
        try:
            data = self._get_json("/api/tags", timeout=min(3, self.timeout))
            return [m.get("name", "") for m in data.get("models", [])
                    if m.get("name")]
        except Exception:
            return []

    def pick_model(self, preferred: str = "") -> "str | None":
        """Velg modell: eksplisitt konfigurert → foretrukket → første tilgjengelige."""
        if self._resolved_model is not None:
            return self._resolved_model or None
        available = self.list_models()
        if not available:
            self._resolved_model = ""
            return None

        def _match(want: str) -> "str | None":
            want = (want or "").strip().lower()
            if not want:
                return None
            for a in available:
                al = a.lower()
                if al == want or al.split(":")[0] == want or al.startswith(want):
                    return a
            return None

        chosen = _match(preferred) or _match(self.model)
        if not chosen:
            for pref in _PREFERRED_MODELS:
                chosen = _match(pref)
                if chosen:
                    break
        chosen = chosen or available[0]
        self._resolved_model = chosen
        self.model = chosen
        return chosen

    # ── generering ────────────────────────────────────────────────────────────

    def _generate(self, prompt: str, *, fmt: "str | None" = None,
                  num_predict: int = 64) -> str:
        model = self.pick_model()
        if not model:
            return ""
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0, "num_predict": num_predict},
        }
        if fmt:
            payload["format"] = fmt
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._base + "/api/generate", data=data,
            headers={"Content-Type": "application/json",
                     "User-Agent": "SIARD-Manager-anon/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            return (result.get("response") or "").strip()
        except Exception:
            return ""

    # ── oppgaver ──────────────────────────────────────────────────────────────

    def classify_column(self, col_name: str, samples: "list[str]") -> str:
        """Klassifiser en kolonne til ett ord i _CLASSIFY_VOCAB. Tomt ved feil."""
        sample_txt = " | ".join(s[:60] for s in (samples or [])[:8])
        prompt = (
            "Du klassifiserer en databasekolonne for anonymisering. "
            "Svar med KUN ETT ORD fra denne listen, ingenting annet:\n"
            f"{', '.join(_CLASSIFY_VOCAB)}.\n\n"
            f"Kolonnenavn: {col_name}\n"
            f"Eksempelverdier: {sample_txt}\n\n"
            "Hvilken type personinformasjon inneholder kolonnen? Svar med ett ord:")
        ans = self._generate(prompt, num_predict=8).upper()
        for token in _CLASSIFY_VOCAB:
            if token in ans:
                return token
        return ""

    def analyze_table(self, columns: "list[tuple]", rows: "list[dict]",
                      table_name: str = "") -> "dict[str, str]":
        """Helhetlig tabellanalyse (foranalyse): gitt kolonner (idx, navn, type)
        og noen EKSEMPELRADER, avgjør hvilke kolonner som skal anonymiseres.

        Omfang — KUN disse kategoriene:
          FULL_NAME/FIRST_NAME/LAST_NAME = personnavn
          FNR                            = personnummer (11 sifre)
          EMAIL                          = e-postadresse
          ADDRESS/POSTNR/CITY            = stedsangivelse ned på stedsnivå
        Alt annet (telefon, koder, beløp, datoer, titler, fag-/stedsnavn på
        region-/landnivå, beskrivelser osv.) → utelates.

        Returnerer {kolonnenavn: TYPE} der TYPE ∈ _CLASSIFY_VOCAB (OTHER utelates).
        """
        if not columns or not rows:
            return {}
        col_line = ", ".join(f"{name}({typ})" for _idx, name, typ in columns)
        row_lines = []
        for ri, row in enumerate(rows, 1):
            cells = []
            for idx, name, _typ in columns:
                v = (row.get(idx) or "").strip()
                if v:
                    cells.append(f"{name}={v[:40]}")
            if cells:
                row_lines.append(f"Rad {ri}: " + "; ".join(cells))
        if not row_lines:
            return {}
        ctx = f"Tabell «{table_name}».\n" if table_name else ""
        prompt = (
            "Du gjør en foranalyse av en databasetabell for anonymisering. Bruk "
            "BÅDE kolonnenavnene OG de faktiske eksempelradene.\n" + ctx +
            "Finn KUN kolonner som inneholder:\n"
            "- personnavn (navn på enkeltpersoner)\n"
            "- personnummer/fødselsnummer (11 sifre)\n"
            "- e-postadresse\n"
            "- stedsangivelse ned på stedsnivå (gateadresse, postnummer, "
            "poststed/by/tettsted — IKKE fylke, region eller land)\n\n"
            f"Kolonner: {col_line}\n\n" + "\n".join(row_lines) + "\n\n"
            "Svar KUN med JSON: {kolonnenavn: TYPE}. TYPE må være én av: "
            f"{', '.join(t for t in _CLASSIFY_VOCAB if t != 'FREE_TEXT')}. "
            "Ta KUN med kolonner som faktisk inneholder slike data (utelat alt "
            "annet, inkludert telefon, titler, koder, beløp og datoer).")
        ans = self._generate(prompt, fmt="json", num_predict=512)
        if not ans:
            return {}
        try:
            parsed = json.loads(ans)
        except Exception:
            return {}
        out: dict[str, str] = {}
        if isinstance(parsed, dict):
            for k, v in parsed.items():
                t = str(v).upper().strip()
                if t in _CLASSIFY_VOCAB and t != "OTHER":
                    out[str(k)] = t
        return out

    def verify_person_names(self, samples: "list[str]") -> bool:
        """Avgjør om VERDIENE er navn på enkeltpersoner — basert KUN på verdiene.

        VIKTIG: kolonnenavnet utelates med vilje. Modellen anker feilaktig på ord
        som «Navn» i kolonnenavnet og svarer «person» selv for fag-/typenavn
        (f.eks. NavnBM = «Matematikk»). Med bare verdiene svarer den korrekt.

        Returnerer False ved tydelig «ANNET». PERSON eller uklart → True (behold
        som personnavn → anonymiser, sikreste standard for personvern)."""
        sample_txt = "\n".join(f"- {s[:60]}" for s in (samples or [])[:12])
        if not sample_txt.strip():
            return True
        prompt = (
            "Her er noen verdier fra én databasekolonne:\n" + sample_txt + "\n\n"
            "Er disse verdiene navn på ENKELTPERSONER (menneskers fornavn og/eller "
            "etternavn), eller er de noe annet — f.eks. fagnavn, kategorier, "
            "institusjons-/skjematyper, stedsnavn, titler eller beskrivelser?\n"
            "Svar KUN med ett ord: PERSON eller ANNET.")
        ans = self._generate(prompt, num_predict=4).strip().upper()
        return not ans.startswith("ANNET")

    # Bakoverkompatibelt alias (kolonnenavn/tabellnavn ignoreres nå)
    def verify_person_name_column(self, col_name: str = "",
                                  samples: "list[str] | None" = None,
                                  table_name: str = "") -> bool:
        return self.verify_person_names(samples or [])

    def judge_identifiable(self, text: str) -> bool:
        """Vurder om en fritekst gjør en KONKRET person identifiserbar
        (navn, relasjoner, unike detaljer). Returnerer True/False. Ved feil
        eller uklart svar: False (kaller faller da tilbake på regex/heuristikk)."""
        snippet = (text or "")[:1500]
        if not snippet.strip():
            return False
        prompt = (
            "Vurder om teksten under DIREKTE identifiserer en konkret "
            "privatperson som er REGISTRERT i systemet (f.eks. klient, elev, "
            "innbygger, pasient, bruker) — typisk fullt navn kombinert med "
            "personlige opplysninger, fødselsnummer, adresse e.l.\n"
            "Stillingstitler, roller, faguttrykk, organisasjons-/systeminfo, "
            "eller navn på ANSATTE/saksbehandlere alene regnes IKKE som direkte "
            "identifiserende.\n"
            "Svar KUN med ett ord: JA eller NEI.\n\nTEKST:\n" + snippet)
        ans = self._generate(prompt, num_predict=4).strip().upper()
        return ans.startswith("JA") or ans.startswith("YES")

    def find_pii_spans(self, text: str) -> "list[dict]":
        """Be modellen returnere PII-fraser i fritekst som JSON-liste.

        Returnerer liste av {"text": str, "type": <vokabular>}. Tom ved feil.
        Operasjonen lokaliserer selv frasene i teksten (modellen gir ikke
        pålitelige indekser).
        """
        snippet = (text or "")[:2000]
        if not snippet.strip():
            return []
        prompt = (
            "Finn alle personidentifiserende fraser i teksten under "
            "(navn, adresser, fødselsnummer, telefon, e-post). "
            'Svar KUN med en JSON-liste på formen '
            '[{"text":"...","type":"FULL_NAME|ADDRESS|FNR|PHONE|EMAIL"}]. '
            "Tom liste hvis ingen.\n\nTEKST:\n" + snippet)
        ans = self._generate(prompt, fmt="json", num_predict=512)
        if not ans:
            return []
        try:
            parsed = json.loads(ans)
        except Exception:
            return []
        if isinstance(parsed, dict):
            parsed = parsed.get("items") or parsed.get("results") or []
        out: list[dict] = []
        if isinstance(parsed, list):
            for it in parsed:
                if isinstance(it, dict) and it.get("text"):
                    typ = str(it.get("type", "FULL_NAME")).upper()
                    if typ not in _CLASSIFY_VOCAB:
                        typ = "FULL_NAME"
                    out.append({"text": str(it["text"]), "type": typ})
        return out
