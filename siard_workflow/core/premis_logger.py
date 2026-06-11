"""
siard_workflow/core/premis_logger.py
PREMIS-proveniens for SIARD-bearbeiding.

Akkumulerer én PREMIS-event per innholdsendrende operasjon i workflowen og
skriver en samlet proveniensfil ({base}_premis.xml) ved siden av SIARD-fila.
Fila beskriver hva som ble gjort med uttrekket fra original til ferdig pakke,
og auto-inkluderes i DIAS-pakka av DiasPackageOperation.

Bruker arkivverkets DIAS_PREMIS v2.0-navnerom — samme som
DiasPackageOperation genererer for selve pakka.
"""
from __future__ import annotations
import datetime
import logging
from pathlib import Path
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

PREMIS_NS = "http://arkivverket.no/standarder/PREMIS"
XSI_NS    = "http://www.w3.org/2001/XMLSchema-instance"
XLINK_NS  = "http://www.w3.org/1999/xlink"

# Suffikser som strippes for å finne uttrekkets «base»-navn — samme liste som
# DiasPackageOperation bruker, slik at premis-fila matcher de andre sidefilene.
_SUFFIXES = ("_konvertert", "_hex_extracted", "_cosdoc", "_blob", "_dias")

ET.register_namespace("premis", PREMIS_NS)
ET.register_namespace("xsi", XSI_NS)
ET.register_namespace("xlink", XLINK_NS)


def _p(tag: str) -> str:
    return f"{{{PREMIS_NS}}}{tag}"


def _base_name(siard_path: Path) -> str:
    base = siard_path.stem
    changed = True
    while changed:
        changed = False
        for suf in _SUFFIXES:
            if base.lower().endswith(suf.lower()):
                base = base[: -len(suf)]
                changed = True
    return base


class PremisProvenanceLogger:
    """
    Samler PREMIS-events under en workflow-kjøring og skriver dem til fil.

    Bruk:
        pl = PremisProvenanceLogger(log_dir, siard_path)
        ...
        pl.record(op, result, ctx)     # for hver endrende operasjon
        ...
        pl.finalize(ctx.siard_path, ctx)   # skriver {base}_premis.xml
    """

    def __init__(self, log_dir, siard_path, agent_version: str = ""):
        self.log_dir   = Path(log_dir)
        self.base      = _base_name(Path(siard_path))
        self.agent_id  = f"SIARD Manager v{agent_version}" if agent_version else "SIARD Manager"
        self._events: list[dict] = []
        self._path: Path | None = None

    @property
    def out_path(self) -> Path:
        return self.log_dir / f"{self.base}_premis.xml"

    @property
    def path(self) -> Path | None:
        return self._path

    def has_events(self) -> bool:
        return bool(self._events)

    def _ts(self) -> str:
        # ISO 8601 med tidssone-offset (lokal tid).
        return datetime.datetime.now().astimezone().isoformat(timespec="seconds")

    def record(self, op, result, ctx) -> None:
        """
        Registrer én PREMIS-event for en innholdsendrende operasjon.
        Speiler også én info-linje i kjøreloggen hvis en file_logger finnes.
        """
        try:
            event_type = getattr(op, "premis_event_type", "") or getattr(op, "label", "") \
                or getattr(op, "operation_id", "operasjon")
            try:
                detail = op.premis_detail(result, ctx)
            except Exception:
                detail = getattr(result, "message", "") or ""
            self._events.append({
                "type":    event_type,
                "op_id":   getattr(op, "operation_id", ""),
                "datetime": self._ts(),
                "detail":  detail or "",
                "success": bool(getattr(result, "success", True)),
            })
            fl = (ctx.metadata or {}).get("file_logger") if ctx else None
            if fl:
                status = "OK" if getattr(result, "success", True) else "FEIL"
                fl.log(f"  PREMIS: {event_type} ({status}) — {detail or '–'}", "info")
        except Exception:
            logger.exception("Kunne ikke registrere PREMIS-event for %r", op)

    def finalize(self, siard_path, ctx=None) -> "Path | None":
        """
        Skriv den akkumulerte proveniensen til {base}_premis.xml.
        Returnerer stien, eller None hvis ingen events ble registrert.
        """
        if not self._events:
            return None
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            root = self._build_tree(Path(siard_path), ctx)
            tree = ET.ElementTree(root)
            try:
                ET.indent(tree, space="  ")
            except Exception:
                pass  # ET.indent finnes fra Python 3.9
            out = self.out_path
            tree.write(out, encoding="utf-8", xml_declaration=True)
            self._path = out
            logger.info("PREMIS-proveniens skrevet: %s (%d hendelser)",
                        out, len(self._events))
            return out
        except Exception:
            logger.exception("Kunne ikke skrive PREMIS-proveniensfil")
            return None

    # ── XML-bygging ───────────────────────────────────────────────────────────

    def _build_tree(self, siard_path: Path, ctx) -> ET.Element:
        obj_id = siard_path.name
        root = ET.Element(_p("premis"), {
            f"{{{XSI_NS}}}schemaLocation":
                f"{PREMIS_NS} http://schema.arkivverket.no/PREMIS/v2.0/DIAS_PREMIS.xsd",
            "version": "2.0",
        })

        # ── premis:object (selve uttrekket) ───────────────────────────────────
        obj = ET.SubElement(root, _p("object"))
        obj.set(f"{{{XSI_NS}}}type", "premis:file")
        oid = ET.SubElement(obj, _p("objectIdentifier"))
        ET.SubElement(oid, _p("objectIdentifierType")).text = "NO/RA"
        ET.SubElement(oid, _p("objectIdentifierValue")).text = obj_id
        chars = ET.SubElement(obj, _p("objectCharacteristics"))
        ET.SubElement(chars, _p("compositionLevel")).text = "0"

        sha = (ctx.results.get("sha256") if (ctx and ctx.results) else None)
        if sha:
            fixity = ET.SubElement(chars, _p("fixity"))
            ET.SubElement(fixity, _p("messageDigestAlgorithm")).text = "SHA-256"
            ET.SubElement(fixity, _p("messageDigest")).text = str(sha)
            ET.SubElement(fixity, _p("messageDigestOriginator")).text = self.agent_id
        fmt = ET.SubElement(chars, _p("format"))
        fmt_des = ET.SubElement(fmt, _p("formatDesignation"))
        ET.SubElement(fmt_des, _p("formatName")).text = "SIARD"

        # ── premis:event (én per endrende operasjon) ──────────────────────────
        for i, ev in enumerate(self._events, 1):
            e = ET.SubElement(root, _p("event"))
            eid = ET.SubElement(e, _p("eventIdentifier"))
            ET.SubElement(eid, _p("eventIdentifierType")).text = "SIARD-Manager"
            ET.SubElement(eid, _p("eventIdentifierValue")).text = str(i)
            ET.SubElement(e, _p("eventType")).text = ev["type"]
            ET.SubElement(e, _p("eventDateTime")).text = ev["datetime"]
            if ev["detail"]:
                ET.SubElement(e, _p("eventDetail")).text = ev["detail"]
            outcome_inf = ET.SubElement(e, _p("eventOutcomeInformation"))
            # DIAS-konvensjon: 0 = suksess, 1 = feil
            ET.SubElement(outcome_inf, _p("eventOutcome")).text = "0" if ev["success"] else "1"
            lai = ET.SubElement(e, _p("linkingAgentIdentifier"))
            ET.SubElement(lai, _p("linkingAgentIdentifierType")).text = "SIARD-Manager"
            ET.SubElement(lai, _p("linkingAgentIdentifierValue")).text = self.agent_id
            loi = ET.SubElement(e, _p("linkingObjectIdentifier"))
            ET.SubElement(loi, _p("linkingObjectIdentifierType")).text = "NO/RA"
            ET.SubElement(loi, _p("linkingObjectIdentifierValue")).text = obj_id

        # ── premis:agent (SIARD Manager) ──────────────────────────────────────
        agent = ET.SubElement(root, _p("agent"))
        aid = ET.SubElement(agent, _p("agentIdentifier"))
        ET.SubElement(aid, _p("agentIdentifierType")).text = "SIARD-Manager"
        ET.SubElement(aid, _p("agentIdentifierValue")).text = self.agent_id
        ET.SubElement(agent, _p("agentName")).text = "SIARD Manager"
        ET.SubElement(agent, _p("agentType")).text = "software"

        return root
