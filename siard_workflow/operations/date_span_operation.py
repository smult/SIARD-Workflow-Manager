"""siard_workflow/operations/date_span_operation.py

DateSpanOperation — uttrekkets første og siste dato (fra–til).

Oppgir metadata.xml et spenn i <dataOriginTimespan>, brukes det (ingen
estimering). Ellers estimeres spennet ut fra datoverdiene i LOB- og
journal-/sakstabellene. Se core/date_span.py for metoden.

Et estimat skrives inn i <dataOriginTimespan> (param write_metadata), med en
XML-kommentar foran som sier at verdien er estimert:
  * pipeline-modus: header/metadata.xml i den utpakkede mappa endres på stedet
  * frittstående: ny kopi <original>_datospenn.siard (legg operasjonen mellom
    «Pakk ut SIARD» og «Pakk sammen SIARD» for å slippe kopien)

Resultatet vises i kjørerapporten og i metadata-rapporten (legg operasjonen
før «Metadata-uttrekk»/«Kjørerapport»), et estimat alltid merket som estimat.
DIAS-pakken bruker samme spenn som periode.
"""
from __future__ import annotations

import shutil
import threading
import zipfile
from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext


class DateSpanOperation(BaseOperation):
    operation_id = "date_span"
    label = "Estimer datospenn (fra–til)"
    description = (
        "Finner uttrekkets første og siste dato. Oppgir metadata.xml et datospenn "
        "(dataOriginTimespan), brukes det. Ellers estimeres spennet ut fra datoene i "
        "LOB-tabeller (dokumenter) og journal-/sakstabeller — merket som estimat i "
        "kjørerapporten og metadata-rapporten — og skrives inn i metadata.xml med en "
        "XML-kommentar om at verdien er estimert. Spennet brukes også som periode i "
        "DIAS-pakken."
    )
    category = "Innhold"
    status = 1
    # Endrer bare metadata.xml, og bare når et estimat skrives inn
    modifies_content = True
    produces_siard = False          # settes True når en frittstående kopi skrives
    premis_event_type = "Adjustment"
    premis_event_label = "estimert datospenn i metadata.xml"

    default_params = {
        "trim_percent": 0.05,    # ytterår med under denne andelen (%) av datoene er avvik
        "write_metadata": True,  # skriv estimatet inn i <dataOriginTimespan>
    }

    def premis_should_record(self, result, ctx) -> bool:
        return bool(result.success) and bool((result.data or {}).get("written_to_metadata"))

    def premis_detail(self, result, ctx) -> str:
        d = result.data or {}
        return (f"<dataOriginTimespan> satt til «{d.get('written_value', '')}», estimert ut "
                f"fra datoverdiene i uttrekket ({d.get('basis_text', '')}); XML-kommentar "
                f"om at verdien er estimert er lagt inn foran elementet")

    def run(self, ctx: WorkflowContext) -> OperationResult:
        from siard_workflow.core import date_span as DS
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")
        stop_ev = ctx.metadata.get("stop_event") or threading.Event()
        self.produces_siard = False

        def w(msg: str, lvl: str = "info") -> None:
            if log:
                log.log(msg, lvl)
            if pcb:
                pcb("log", msg=msg, level=lvl)

        pipeline = bool(ctx.extracted_path and ctx.extracted_path.is_dir())
        src = ctx.extracted_path if pipeline else ctx.siard_path
        if not src or not src.exists():
            return self._fail("Fant ikke SIARD-fil eller utpakket mappe")
        try:
            raw = str(self.params.get("trim_percent", 0.05)).replace(",", ".")  # «0,05» godtas
            trim = max(0.0, min(10.0, float(raw))) / 100
        except (TypeError, ValueError):
            trim = DS.TRIM
        w("  Leser datospenn …", "info")
        try:
            res = DS.estimate(src, progress=lambda m: w(f"    {m}", "info"),
                              stop=stop_ev.is_set, trim=trim)
        except Exception as exc:
            return self._fail(f"Kunne ikke estimere datospenn: {exc}")

        if res.get("source") == "metadata":
            w(f"  DATOSPENN FRA metadata.xml: {res['from_date']} – {res['to_date']}  "
              f"(«{res['metadata_timespan']}»)", "ok")
            if res.get("metadata_estimated"):
                w("    Verdien ble estimert av SIARD Workflow Manager ved en tidligere "
                  "kjøring (se XML-kommentar i metadata.xml) — ikke nødvendigvis 100 % "
                  "korrekt.", "warn")
            else:
                w("    metadata.xml oppgir spennet — det brukes, ingen estimering. "
                  "Brukes også som periode i DIAS-pakken.", "info")
            return self._ok(res, res["message"])
        if res.get("metadata_timespan"):
            w(f"    metadata.xml <dataOriginTimespan> «{res['metadata_timespan']}» er ikke "
              "et tolkbart datospenn — estimerer fra uttrekket", "warn")
        else:
            w("    metadata.xml oppgir ikke datospenn — estimerer fra uttrekket", "info")
        if not res.get("found"):
            w(f"  {res.get('message')}", "warn")
            return self._ok(res, res.get("message", "Ingen datoer funnet"))
        w(f"  ESTIMERT DATOSPENN: {res['from_year']}–{res['to_year']}  "
          f"({res['from_date']} – {res['to_date']})", "ok")
        w(f"    Grunnlag: {res['basis_text']}; sikkerhet: {res['confidence']}", "info")
        ts = res.get("tables_scanned", [])
        if ts:
            w("    Tabeller lest: " + ", ".join(f"{t['table']} ({t['why']})" for t in ts[:8])
              + (f" … (+{len(ts) - 8})" if len(ts) > 8 else ""), "info")
        if res.get("outside_trim"):
            w(f"    {res['outside_trim']:,} datoverdier utenfor spennet regnes som avvik "
              f"(ytterpunkter {res['absolute_first']} – {res['absolute_last']})", "info")
        excluded = [c for c in res.get("columns", []) if c.get("excluded")]
        if excluded:
            w("    Ikke brukt: " + ", ".join(f"{c['table']}.{c['column']}" for c in excluded[:6])
              + (" …" if len(excluded) > 6 else "") + " (fødselsdato/frist o.l.)", "info")
        w(f"    Brukes som periode i DIAS-pakken: {DS._full_date(res['from_date'], True)} – "
          f"{DS._full_date(res['to_date'], False)}", "info")
        w(f"    {DS.NOTE}", "info")

        if self.params.get("write_metadata", True) not in (False, "False", "false", 0, "0"):
            try:
                self._write_metadata(DS, ctx, pipeline, res, w)
            except Exception as exc:
                w(f"  [ADVARSEL] Kunne ikke skrive estimatet til metadata.xml: {exc}", "warn")
        return self._ok(res, f"Estimert datospenn {res['from_year']}–{res['to_year']} "
                             f"({res['confidence']} sikkerhet)"
                        + ("; skrevet til metadata.xml" if res.get("written_to_metadata")
                           else ""))

    def _write_metadata(self, DS, ctx: WorkflowContext, pipeline: bool, res: dict, w) -> None:
        """Skriv estimatet inn i <dataOriginTimespan> med en XML-kommentar foran."""
        if pipeline:
            meta_path = ctx.extracted_path / "header" / "metadata.xml"
            out = DS.write_timespan(meta_path.read_bytes(), res)
            if not out:
                w("  [ADVARSEL] Fant ikke plass til <dataOriginTimespan> i metadata.xml "
                  "— ikke skrevet", "warn")
                return
            meta_path.write_bytes(out[0])
            dst = None
        else:
            src = ctx.siard_path
            with zipfile.ZipFile(src) as zin:
                names = {n.lower(): n for n in zin.namelist()}
                entry = names.get("header/metadata.xml")
                if not entry:
                    raise FileNotFoundError("header/metadata.xml finnes ikke")
                out = DS.write_timespan(zin.read(entry), res)
                if not out:
                    w("  [ADVARSEL] Fant ikke plass til <dataOriginTimespan> i metadata.xml "
                      "— ikke skrevet", "warn")
                    return
                dst = src.with_name(src.stem + "_datospenn" + src.suffix)
                n = 1
                while dst.exists():
                    dst = src.with_name(f"{src.stem}_datospenn_{n}{src.suffix}")
                    n += 1
                w(f"  Skriver kopi med oppdatert metadata.xml: {dst.name} "
                  "(legg operasjonen mellom «Pakk ut» og «Pakk sammen» for å slippe kopien)",
                  "info")
                try:
                    with zipfile.ZipFile(dst, "w", allowZip64=True) as zout:
                        for info in zin.infolist():
                            if info.filename == entry:
                                zout.writestr(info, out[0])
                                continue
                            with zin.open(info) as fi, zout.open(info, "w",
                                                                 force_zip64=True) as fo:
                                shutil.copyfileobj(fi, fo, 8 << 20)
                except BaseException:
                    Path(dst).unlink(missing_ok=True)
                    raise
            self.produces_siard = True
            res["output_path"] = str(dst)
        res["written_to_metadata"] = True
        res["written_value"] = out[1]
        w(f"  Skrevet til metadata.xml: <dataOriginTimespan>{out[1]}</dataOriginTimespan>, "
          "med XML-kommentar om at verdien er estimert", "ok")
