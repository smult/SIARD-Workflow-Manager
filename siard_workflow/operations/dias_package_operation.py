"""
siard_workflow/operations/dias_package_operation.py
----------------------------------------------------
DiasPackageOperation — Pakker en ferdig behandlet SIARD-fil inn i et
DIAS/SIP-pakkeformat (METS + PREMIS) klar for langtidsbevaring i ESSArch.

Basert på ET-Producer (https://github.com/KDRS-SA/ET-producer).
Kjernefunksjonaliteten er portert til headless Python uten GUI-avhengigheter.

Valgfritt: python-magic (pip install python-magic-bin) for nøyaktig MIME-type
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import tarfile
import tempfile
from datetime import datetime
from pathlib import Path
from uuid import uuid1

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext

logger = logging.getLogger(__name__)

# Programmappen er tre nivåer opp fra denne filen
_PROGRAM_DIR = Path(__file__).parent.parent.parent
_DIAS_JSON   = _PROGRAM_DIR / "dias.json"

# Alle kjente feltnøkler med hardkodede fallback-verdier
_DIAS_KEYS: dict[str, str] = {
    "submission_agreement":         "",
    "label":                        "",
    "system":                       "",
    "system_version":               "",
    "archivist_type":               "SIARD",
    "period_start":                 "",
    "period_end":                   "",
    "owner_org":                    "",
    "archivist_org":                "",
    "submitter_org":                "",
    "submitter_person":             "",
    "producer_org":                 "",
    "producer_person":              "",
    "producer_software":            "SIARD Workflow Manager",
    "creator":                      "",
    "preserver":                    "",
    "descriptive_metadata_path":    "",
    "administrative_metadata_path": "",
    "output_dir":                   "",
    "extra_files":                  "[]",  # JSON: [{src, dest}, ...]
}


def _load_dias_json() -> dict[str, str]:
    """Leser dias.json fra programmappen. Manglende nøkler hentes fra _DIAS_KEYS."""
    base = dict(_DIAS_KEYS)
    try:
        if _DIAS_JSON.exists():
            data = json.loads(_DIAS_JSON.read_text(encoding="utf-8"))
            for k in _DIAS_KEYS:
                if k in data and isinstance(data[k], str):
                    base[k] = data[k]
    except Exception:
        logger.warning("Kunne ikke lese %s — bruker hardkodede standardverdier", _DIAS_JSON)
    return base


def read_meta_from_mets(mets_path) -> dict:
    """
    Les metadata fra en eksisterende METS-fil (info.xml, mets.xml eller annet navn).

    Returnerer dict med nøkler fra _DIAS_KEYS der verdier finnes i filen.
    Kaster ValueError ved ugyldig XML.
    """
    import xml.etree.ElementTree as ET
    _NS = "http://www.loc.gov/METS/"

    def _tag(local):
        return f"{{{_NS}}}{local}"

    def _name(agent_el):
        n = agent_el.find(_tag("name"))
        return (n.text or "").strip() if n is not None else ""

    try:
        root = ET.parse(str(mets_path)).getroot()
    except ET.ParseError as exc:
        raise ValueError(f"Ugyldig XML: {exc}") from exc

    result: dict = {}

    label = root.get("LABEL", "")
    if label:
        result["label"] = label

    hdr = root.find(_tag("metsHdr"))
    if hdr is None:
        return result

    for ar in hdr.findall(_tag("altRecordID")):
        val = (ar.text or "").strip()
        t = ar.get("TYPE", "")
        if t == "SUBMISSIONAGREEMENT":
            result["submission_agreement"] = val
        elif t == "STARTDATE":
            result["period_start"] = val
        elif t == "ENDDATE":
            result["period_end"] = val

    sw_archivists: list[str] = []
    for agent in hdr.findall(_tag("agent")):
        typ       = agent.get("TYPE", "")
        role      = agent.get("ROLE", "")
        otherrole = agent.get("OTHERROLE", "")
        othertype = agent.get("OTHERTYPE", "")
        name = _name(agent)

        if typ == "ORGANIZATION" and role == "ARCHIVIST":
            result["archivist_org"] = name
        elif typ == "OTHER" and othertype == "SOFTWARE" and role == "ARCHIVIST":
            sw_archivists.append(name)
        elif typ == "ORGANIZATION" and role == "CREATOR":
            result["creator"] = name
        elif role == "OTHER" and otherrole == "PRODUCER":
            if typ == "ORGANIZATION":
                result["producer_org"] = name
            elif typ == "INDIVIDUAL":
                result["producer_person"] = name
            elif typ == "OTHER" and othertype == "SOFTWARE":
                result["producer_software"] = name
        elif role == "OTHER" and otherrole == "SUBMITTER":
            if typ == "ORGANIZATION":
                result["submitter_org"] = name
            elif typ == "INDIVIDUAL":
                result["submitter_person"] = name
        elif typ == "ORGANIZATION" and role == "IPOWNER":
            result["owner_org"] = name
        elif typ == "ORGANIZATION" and role == "PRESERVATION":
            result["preserver"] = name

    for key, val in zip(("system", "system_version", "archivist_type"), sw_archivists):
        result[key] = val

    return result


def _resolve_pending(token: str, siard_path: Path, ctx) -> "Path | None":
    """
    Løs opp [[pending:{token}]] til reell filsti, eller None hvis ikke funnet.
    Brukes av DiasPackageOperation.run() rett før ekstra-filer kopieres.
    """
    base = siard_path.stem
    _suffixes = ("_konvertert", "_hex_extracted", "_cosdoc", "_blob", "_dias")
    changed = True
    while changed:
        changed = False
        for suf in _suffixes:
            if base.lower().endswith(suf.lower()):
                base = base[: -len(suf)]
                changed = True

    parent = siard_path.parent
    meta   = ctx.metadata if ctx and ctx.metadata else {}
    log_dir_str = meta.get("log_dir", "")
    log_dir = Path(log_dir_str) if log_dir_str else None

    def _newest(*dirs, pattern: str) -> "Path | None":
        candidates: list[Path] = []
        for d in dirs:
            if d and d.is_dir():
                candidates.extend(d.glob(pattern))
        return max(candidates, key=lambda p: p.stat().st_mtime, default=None)

    if token == "workflow_log":
        fl = meta.get("file_logger")
        if fl and hasattr(fl, "log_path"):
            lp = Path(fl.log_path)
            if lp.exists():
                return lp
        return _newest(parent, log_dir, pattern=f"{base}_*.log")

    if token == "blob_csv":
        return _newest(parent, log_dir, pattern=f"{base}_*_blob_konvertering.csv")

    if token == "konvertering_feil":
        return _newest(parent, log_dir, pattern=f"{base}_*_konvertering_feil.log")

    if token == "sha256":
        p = parent / f"{base}.sha256"
        return p if p.exists() else None

    return None


# ─────────────────────────────────────────────────────────────────────────────

class DiasPackageOperation(BaseOperation):
    """Pakker SIARD-fil inn i DIAS/SIP-format (METS + PREMIS) for ESSArch."""

    operation_id    = "dias_package"
    label           = "DIAS-pakking (SIP/AIC)"
    description     = (
        "Pakker den ferdigbehandlede SIARD-filen inn i et DIAS-pakkeformat "
        "i henhold til METS- og DIAS_PREMIS-standardene, klar for innsending "
        "til langtidsbevaringsplatform (ESSArch). Produserer en AIC-mappe med "
        "SIP, mets.xml, premis.xml, log.xml og komprimert tar-arkiv."
    )
    category        = "Pakking"
    status          = 1
    produces_siard  = False
    requires_unpack = False

    default_params = _load_dias_json()

    _REQUIRED_META = [
        "label", "system", "system_version", "submission_agreement",
        "archivist_type", "period_start", "period_end",
        "owner_org", "archivist_org", "submitter_org", "submitter_person",
        "producer_org", "producer_person", "producer_software",
        "creator", "preserver",
    ]

    def run(self, ctx: WorkflowContext) -> OperationResult:
        siard_path = ctx.siard_path
        if not siard_path or not siard_path.exists():
            return self._fail("Ingen SIARD-fil i kontekst.")

        missing = [k for k in self._REQUIRED_META if not str(self.params.get(k, "")).strip()]
        if missing:
            return self._fail(f"Manglende obligatoriske parametere: {', '.join(missing)}")

        out_root = Path(self.params.get("output_dir") or siard_path.parent)
        out_root.mkdir(parents=True, exist_ok=True)

        # Løs opp [[pending:*]]-tokens i extra_files til reelle filstier
        try:
            raw_list = json.loads(self.params.get("extra_files", "[]") or "[]")
        except Exception:
            raw_list = []
        resolved_list = []
        for ef in raw_list:
            src = ef.get("src", "")
            if src.startswith("[[pending:") and src.endswith("]]"):
                token = src[10:-2]
                real  = _resolve_pending(token, siard_path, ctx)
                if real:
                    dest_dir = ef.get("dest", "").rstrip("/")
                    resolved_list.append({"src": str(real),
                                          "dest": f"{dest_dir}/{real.name}"})
                    logger.info("Pending token '%s' løst til: %s", token, real)
                else:
                    logger.warning("Pending token '%s' — fil ikke funnet, hopper over", token)
            else:
                resolved_list.append(ef)
        meta = {**self.params, "extra_files": json.dumps(resolved_list, ensure_ascii=False)}

        # Lag en midlertidig content-mappe med kun SIARD-filen
        with tempfile.TemporaryDirectory() as tmp_content:
            content_dir = Path(tmp_content) / siard_path.stem
            content_dir.mkdir()
            shutil.copy2(siard_path, content_dir / siard_path.name)

            try:
                aic_path = _build_dias_package(
                    content_path = content_dir,
                    out_root     = out_root,
                    meta         = meta,
                    log_fn       = lambda msg: logger.info(msg),
                )
            except Exception as exc:
                logger.exception("DIAS-pakking feilet")
                return self._fail(f"DIAS-pakking feilet: {exc}")

        return self._ok(
            data={"aic_path": str(aic_path)},
            message=f"DIAS-pakke opprettet: {aic_path.name}")


# ─────────────────────────────────────────────────────────────────────────────
# Kjernefunksjoner (portert fra ET-Producer, GUI-avhengigheter fjernet)
# ─────────────────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02:00")


def _gather_file_info(directory: str, prefix: str, log_fn) -> dict:
    """SHA-256, MIME-type, størrelse og endringstidspunkt for alle filer i mappen."""
    log_fn(f"Beregner sjekksummer for {prefix}...")
    try:
        import magic as _magic
        _use_magic = True
    except ImportError:
        _use_magic = False

    info_dict = {}
    for root, _, files in os.walk(directory):
        for file in files:
            sha  = hashlib.sha256()
            full = os.path.join(root, file)
            mime = "application/octet-stream"
            with open(full, "rb") as f:
                chunk = f.read(4_000_000)
                if _use_magic and chunk:
                    try:
                        mime = _magic.from_buffer(chunk, mime=True)
                    except Exception:
                        pass
                while chunk:
                    sha.update(chunk)
                    chunk = f.read(4_000_000)
            rel = os.path.relpath(root, directory).replace("\\", "/")
            key = f"{prefix}/{rel}/{file}" if rel != "." else f"{prefix}/{file}"
            key = key.replace("\\", "/")
            mtime = datetime.fromtimestamp(os.path.getmtime(full)).strftime("%Y-%m-%dT%H:%M:%S+02:00")
            info_dict[key] = [sha.hexdigest(), mime, os.stat(full).st_size, mtime]
    return info_dict


def _pack_sip(sip_dir: str, sip_id: str, content_path: str, log_fn):
    log_fn("Komprimerer SIP til tar-arkiv...")
    sip_dir_path = Path(sip_dir)
    sip_dir_parent = sip_dir_path.parent  # {tmp_out}/{sip_id}/content/

    with tarfile.open(f"{sip_dir}.tar", "w") as tar:
        # SIP-innholdet: arkiveres som {sip_id}/xxx
        for file in sip_dir_path.rglob("*"):
            if file.is_file():
                arcname = str(file.relative_to(sip_dir_parent)).replace("\\", "/")
                tar.add(str(file), arcname=arcname)
        # Content-filer: arkiveres som {sip_id}/content/xxx
        for file in Path(content_path).rglob("*"):
            if file.is_file():
                rel = str(file.relative_to(Path(content_path))).replace("\\", "/")
                tar.add(str(file), arcname=f"{sip_id}/content/{rel}")

    shutil.rmtree(sip_dir_path)


def _write_sip_log(path: str, sip_id: str, create_date: str, meta: dict):
    xml = f"""<?xml version='1.0' encoding='UTF-8'?>
<premis:premis xmlns:premis="http://arkivverket.no/standarder/PREMIS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://arkivverket.no/standarder/PREMIS http://schema.arkivverket.no/PREMIS/v2.0/DIAS_PREMIS.xsd" version="2.0">
  <premis:object xsi:type="premis:file">
    <premis:objectIdentifier>
      <premis:objectIdentifierType>NO/RA</premis:objectIdentifierType>
      <premis:objectIdentifierValue>{sip_id}</premis:objectIdentifierValue>
    </premis:objectIdentifier>
    <premis:preservationLevel>
      <premis:preservationLevelValue>full</premis:preservationLevelValue>
    </premis:preservationLevel>
    <premis:significantProperties>
      <premis:significantPropertiesType>aic_object</premis:significantPropertiesType>
      <premis:significantPropertiesValue></premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>createdate</premis:significantPropertiesType>
      <premis:significantPropertiesValue>{create_date}</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>archivist_organization</premis:significantPropertiesType>
      <premis:significantPropertiesValue>{meta["archivist_org"]}</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>label</premis:significantPropertiesType>
      <premis:significantPropertiesValue>{meta["label"]}</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>iptype</premis:significantPropertiesType>
      <premis:significantPropertiesValue>SIP</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:objectCharacteristics>
      <premis:compositionLevel>0</premis:compositionLevel>
      <premis:format>
        <premis:formatDesignation>
          <premis:formatName>tar</premis:formatName>
        </premis:formatDesignation>
      </premis:format>
    </premis:objectCharacteristics>
    <premis:storage>
      <premis:storageMedium>Preservation platform ESSArch</premis:storageMedium>
    </premis:storage>
    <premis:relationship>
      <premis:relationshipType>structural</premis:relationshipType>
      <premis:relationshipSubType>is part of</premis:relationshipSubType>
      <premis:relatedObjectIdentification>
        <premis:relatedObjectIdentifierType>NO/RA</premis:relatedObjectIdentifierType>
        <premis:relatedObjectIdentifierValue></premis:relatedObjectIdentifierValue>
      </premis:relatedObjectIdentification>
    </premis:relationship>
  </premis:object>
  <premis:event>
    <premis:eventIdentifier>
      <premis:eventIdentifierType>NO/RA</premis:eventIdentifierType>
      <premis:eventIdentifierValue>{uuid1()}</premis:eventIdentifierValue>
    </premis:eventIdentifier>
    <premis:eventType>10000</premis:eventType>
    <premis:eventDateTime>{create_date}</premis:eventDateTime>
    <premis:eventDetail>Log circular created</premis:eventDetail>
    <premis:eventOutcomeInformation>
      <premis:eventOutcome>0</premis:eventOutcome>
      <premis:eventOutcomeDetail>
        <premis:eventOutcomeDetailNote>Success to create logfile</premis:eventOutcomeDetailNote>
      </premis:eventOutcomeDetail>
    </premis:eventOutcomeInformation>
    <premis:linkingObjectIdentifier>
      <premis:linkingObjectIdentifierType>NO/RA</premis:linkingObjectIdentifierType>
      <premis:linkingObjectIdentifierValue>{sip_id}</premis:linkingObjectIdentifierValue>
    </premis:linkingObjectIdentifier>
  </premis:event>
</premis:premis>"""
    Path(path).write_text(xml, encoding="utf-8")


def _write_sip_premis(path: str, sip_id: str, info_dict: dict):
    skip = {f"{sip_id}/mets.xml", f"{sip_id}/administrative_metadata/premis.xml"}
    lines = [
        "<?xml version='1.0' encoding='UTF-8'?>",
        ' <premis:premis xmlns:premis="http://arkivverket.no/standarder/PREMIS"'
        ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
        ' xmlns:xlink="http://www.w3.org/1999/xlink"'
        ' xsi:schemaLocation="http://arkivverket.no/standarder/PREMIS'
        ' http://schema.arkivverket.no/PREMIS/v2.0/DIAS_PREMIS.xsd" version="2.0">',
        f'  <premis:object xsi:type="premis:file">',
        f'    <premis:objectIdentifier>',
        f'      <premis:objectIdentifierType>NO/RA</premis:objectIdentifierType>',
        f'      <premis:objectIdentifierValue>{sip_id}</premis:objectIdentifierValue>',
        f'    </premis:objectIdentifier>',
        f'    <premis:preservationLevel>',
        f'      <premis:preservationLevelValue>full</premis:preservationLevelValue>',
        f'    </premis:preservationLevel>',
        f'    <premis:objectCharacteristics>',
        f'      <premis:compositionLevel>0</premis:compositionLevel>',
        f'      <premis:format><premis:formatDesignation>',
        f'        <premis:formatName>tar</premis:formatName>',
        f'      </premis:formatDesignation></premis:format>',
        f'    </premis:objectCharacteristics>',
        f'    <premis:storage>',
        f'      <premis:storageMedium>ESSArch Tools</premis:storageMedium>',
        f'    </premis:storage>',
        f'  </premis:object>',
    ]
    for path_key, info in info_dict.items():
        if path_key in skip:
            continue
        ext = os.path.splitext(path_key)[1][1:]
        lines.append(
            f'  <premis:object xsi:type="premis:file">\n'
            f'    <premis:objectIdentifier>\n'
            f'      <premis:objectIdentifierType>NO/RA</premis:objectIdentifierType>\n'
            f'      <premis:objectIdentifierValue>{path_key}</premis:objectIdentifierValue>\n'
            f'    </premis:objectIdentifier>\n'
            f'    <premis:objectCharacteristics>\n'
            f'      <premis:compositionLevel>0</premis:compositionLevel>\n'
            f'      <premis:fixity>\n'
            f'        <premis:messageDigestAlgorithm>SHA-256</premis:messageDigestAlgorithm>\n'
            f'        <premis:messageDigest>{info[0]}</premis:messageDigest>\n'
            f'        <premis:messageDigestOriginator>ESSArch</premis:messageDigestOriginator>\n'
            f'      </premis:fixity>\n'
            f'      <premis:size>{info[2]}</premis:size>\n'
            f'      <premis:format><premis:formatDesignation>\n'
            f'        <premis:formatName>{ext}</premis:formatName>\n'
            f'      </premis:formatDesignation></premis:format>\n'
            f'    </premis:objectCharacteristics>\n'
            f'    <premis:storage>\n'
            f'      <premis:contentLocation>\n'
            f'        <premis:contentLocationType>SIP</premis:contentLocationType>\n'
            f'        <premis:contentLocationValue>{sip_id}</premis:contentLocationValue>\n'
            f'      </premis:contentLocation>\n'
            f'    </premis:storage>\n'
            f'    <premis:relationship>\n'
            f'      <premis:relationshipType>structural</premis:relationshipType>\n'
            f'      <premis:relationshipSubType>is part of</premis:relationshipSubType>\n'
            f'      <premis:relatedObjectIdentification>\n'
            f'        <premis:relatedObjectIdentifierType>NO/RA</premis:relatedObjectIdentifierType>\n'
            f'        <premis:relatedObjectIdentifierValue>{sip_id}</premis:relatedObjectIdentifierValue>\n'
            f'      </premis:relatedObjectIdentification>\n'
            f'    </premis:relationship>\n'
            f'  </premis:object>'
        )
    lines.append(
        '  <premis:agent>\n'
        '    <premis:agentIdentifier>\n'
        '      <premis:agentIdentifierType>NO/RA</premis:agentIdentifierType>\n'
        '      <premis:agentIdentifierValue>ESSArch</premis:agentIdentifierValue>\n'
        '    </premis:agentIdentifier>\n'
        '    <premis:agentName>ESSArch Tools</premis:agentName>\n'
        '    <premis:agentType>software</premis:agentType>\n'
        '  </premis:agent>\n'
        '</premis:premis>'
    )
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def _sha256_file(path: str) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(4_000_000)
            if not chunk:
                break
            sha.update(chunk)
    return sha.hexdigest()


def _write_sip_mets(mets_path: str, premis_path: str, sip_id: str,
                    creation_date: str, info_dict: dict, meta: dict):
    premis_sha   = _sha256_file(premis_path)
    premis_size  = os.stat(premis_path).st_size
    premis_mtime = datetime.fromtimestamp(os.path.getmtime(premis_path)).strftime("%Y-%m-%dT%H:%M:%S+02:00")
    id_list      = [f"ID{uuid1()}"]

    skip_rel = {"file:administrative_metadata/premis.xml", "file:mets.xml"}

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<mets:mets xmlns:mets="http://www.loc.gov/METS/"'
        f' xmlns:xlink="http://www.w3.org/1999/xlink"'
        f' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
        f' xsi:schemaLocation="http://www.loc.gov/METS/ http://schema.arkivverket.no/METS/mets.xsd"'
        f' PROFILE="http://xml.ra.se/METS/RA_METS_eARD.xml"'
        f' LABEL="{meta["label"]}" TYPE="SIP" ID="ID{uuid1()}" OBJID="UUID:{sip_id}">',
        f'    <mets:metsHdr CREATEDATE="{creation_date}" RECORDSTATUS="NEW">',
        f'        <mets:agent TYPE="ORGANIZATION" ROLE="ARCHIVIST"><mets:name>{meta["archivist_org"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="ARCHIVIST"><mets:name>{meta["system"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="ARCHIVIST"><mets:name>{meta["system_version"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="ARCHIVIST"><mets:name>{meta["archivist_type"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="ORGANIZATION" ROLE="CREATOR"><mets:name>{meta["creator"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="ORGANIZATION" ROLE="OTHER" OTHERROLE="PRODUCER"><mets:name>{meta["producer_org"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="INDIVIDUAL" ROLE="OTHER" OTHERROLE="PRODUCER"><mets:name>{meta["producer_person"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="OTHER" OTHERROLE="PRODUCER"><mets:name>{meta["producer_software"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="ORGANIZATION" ROLE="OTHER" OTHERROLE="SUBMITTER"><mets:name>{meta["submitter_org"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="INDIVIDUAL" ROLE="OTHER" OTHERROLE="SUBMITTER"><mets:name>{meta["submitter_person"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="ORGANIZATION" ROLE="IPOWNER"><mets:name>{meta["owner_org"]}</mets:name></mets:agent>',
        f'        <mets:agent TYPE="ORGANIZATION" ROLE="PRESERVATION"><mets:name>{meta["preserver"]}</mets:name></mets:agent>',
        f'        <mets:altRecordID TYPE="SUBMISSIONAGREEMENT">{meta["submission_agreement"]}</mets:altRecordID>',
        f'        <mets:altRecordID TYPE="STARTDATE">{meta["period_start"]}</mets:altRecordID>',
        f'        <mets:altRecordID TYPE="ENDDATE">{meta["period_end"]}</mets:altRecordID>',
        f'        <mets:metsDocumentID>mets.xml</mets:metsDocumentID>',
        f'    </mets:metsHdr>',
        f'    <mets:amdSec ID="amdSec001">',
        f'        <mets:digiprovMD ID="digiprovMD001">',
        f'            <mets:mdRef MIMETYPE="text/xml" CHECKSUMTYPE="SHA-256" CHECKSUM="{premis_sha}"'
        f' MDTYPE="PREMIS" xlink:href="file:administrative_metadata/premis.xml" LOCTYPE="URL"'
        f' CREATED="{premis_mtime}" xlink:type="simple" ID="{id_list[-1]}" SIZE="{premis_size}"/>',
        f'        </mets:digiprovMD>',
        f'    </mets:amdSec>',
        f'    <mets:fileSec>',
        f'        <mets:fileGrp ID="fgrp001" USE="FILES">',
    ]

    for path_key, info in info_dict.items():
        rel = "file:" + path_key.removeprefix(f"{sip_id}/")
        if rel in skip_rel:
            continue
        id_list.append(f"ID{uuid1()}")
        lines.append(
            f'            <mets:file MIMETYPE="{info[1]}" CHECKSUMTYPE="SHA-256"'
            f' CREATED="{info[3]}" CHECKSUM="{info[0]}" USE="Datafile"'
            f' ID="{id_list[-1]}" SIZE="{info[2]}">'
            f'<mets:FLocat xlink:href="{rel}" LOCTYPE="URL" xlink:type="simple"/>'
            f'</mets:file>'
        )

    lines += [
        f'        </mets:fileGrp>',
        f'    </mets:fileSec>',
        f'    <mets:structMap>',
        f'        <mets:div LABEL="Package">',
        f'            <mets:div ADMID="amdSec001" LABEL="Content Description">',
        f'                <mets:fptr FILEID="{id_list.pop(0)}"/>',
        f'            </mets:div>',
        f'            <mets:div ADMID="amdSec001" LABEL="Datafiles">',
    ]
    while id_list:
        lines.append(f'                <mets:fptr FILEID="{id_list.pop(0)}"/>')
    lines += [
        f'            </mets:div>',
        f'        </mets:div>',
        f'    </mets:structMap>',
        f'</mets:mets>',
    ]
    Path(mets_path).write_text("\n".join(lines), encoding="utf-8")


def _write_sip_info(info_path: str, tar_path: str, sip_id: str,
                    creation_date: str, meta: dict):
    extra_id = f"ID{uuid1()}"
    tar_sha   = _sha256_file(tar_path)
    tar_size  = os.stat(tar_path).st_size
    tar_mtime = datetime.fromtimestamp(os.path.getmtime(tar_path)).strftime("%Y-%m-%dT%H:%M:%S+02:00")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<mets:mets xmlns:mets="http://www.loc.gov/METS/" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/METS/ http://schema.arkivverket.no/METS/info.xsd" PROFILE="http://xml.ra.se/METS/RA_METS_eARD.xml" LABEL="{meta["label"]}" TYPE="SIP" ID="ID{uuid1()}" OBJID="UUID:{sip_id}">
    <mets:metsHdr CREATEDATE="{creation_date}" RECORDSTATUS="NEW">
        <mets:agent TYPE="ORGANIZATION" ROLE="ARCHIVIST"><mets:name>{meta["archivist_org"]}</mets:name></mets:agent>
        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="ARCHIVIST"><mets:name>{meta["system"]}</mets:name></mets:agent>
        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="ARCHIVIST"><mets:name>{meta["system_version"]}</mets:name></mets:agent>
        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="ARCHIVIST"><mets:name>{meta["archivist_type"]}</mets:name></mets:agent>
        <mets:agent TYPE="ORGANIZATION" ROLE="CREATOR"><mets:name>{meta["creator"]}</mets:name></mets:agent>
        <mets:agent TYPE="ORGANIZATION" ROLE="OTHER" OTHERROLE="PRODUCER"><mets:name>{meta["producer_org"]}</mets:name></mets:agent>
        <mets:agent TYPE="INDIVIDUAL" ROLE="OTHER" OTHERROLE="PRODUCER"><mets:name>{meta["producer_person"]}</mets:name></mets:agent>
        <mets:agent TYPE="OTHER" OTHERTYPE="SOFTWARE" ROLE="OTHER" OTHERROLE="PRODUCER"><mets:name>{meta["producer_software"]}</mets:name></mets:agent>
        <mets:agent TYPE="ORGANIZATION" ROLE="OTHER" OTHERROLE="SUBMITTER"><mets:name>{meta["submitter_org"]}</mets:name></mets:agent>
        <mets:agent TYPE="INDIVIDUAL" ROLE="OTHER" OTHERROLE="SUBMITTER"><mets:name>{meta["submitter_person"]}</mets:name></mets:agent>
        <mets:agent TYPE="ORGANIZATION" ROLE="IPOWNER"><mets:name>{meta["owner_org"]}</mets:name></mets:agent>
        <mets:agent TYPE="ORGANIZATION" ROLE="PRESERVATION"><mets:name>{meta["preserver"]}</mets:name></mets:agent>
        <mets:altRecordID TYPE="SUBMISSIONAGREEMENT">{meta["submission_agreement"]}</mets:altRecordID>
        <mets:altRecordID TYPE="STARTDATE">{meta["period_start"]}</mets:altRecordID>
        <mets:altRecordID TYPE="ENDDATE">{meta["period_end"]}</mets:altRecordID>
        <mets:metsDocumentID>info.xml</mets:metsDocumentID>
    </mets:metsHdr>
    <mets:fileSec>
        <mets:fileGrp ID="fgrp001" USE="FILES">
            <mets:file MIMETYPE="application/x-tar" CHECKSUMTYPE="SHA-256" CREATED="{tar_mtime}" CHECKSUM="{tar_sha}" USE="Datafile" ID="{extra_id}" SIZE="{tar_size}">
                <mets:FLocat xlink:href="file:{os.path.basename(tar_path)}" LOCTYPE="URL" xlink:type="simple"/>
            </mets:file>
        </mets:fileGrp>
    </mets:fileSec>
    <mets:structMap>
        <mets:div LABEL="Package">
            <mets:div LABEL="Content Description"/>
            <mets:div LABEL="Datafiles">
                <mets:fptr FILEID="{extra_id}"/>
            </mets:div>
        </mets:div>
    </mets:structMap>
</mets:mets>"""
    Path(info_path).write_text(xml, encoding="utf-8")


def _write_aic_log(path: str, aic_id: str, sip_id: str, create_date: str, meta: dict):
    xml = f"""<?xml version='1.0' encoding='UTF-8'?>
<premis:premis xmlns:premis="http://arkivverket.no/standarder/PREMIS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://arkivverket.no/standarder/PREMIS http://schema.arkivverket.no/PREMIS/v2.0/DIAS_PREMIS.xsd" version="2.0">
  <premis:object xsi:type="premis:file">
    <premis:objectIdentifier>
      <premis:objectIdentifierType>NO/RA</premis:objectIdentifierType>
      <premis:objectIdentifierValue>{sip_id}</premis:objectIdentifierValue>
    </premis:objectIdentifier>
    <premis:preservationLevel>
      <premis:preservationLevelValue>full</premis:preservationLevelValue>
    </premis:preservationLevel>
    <premis:significantProperties>
      <premis:significantPropertiesType>aic_object</premis:significantPropertiesType>
      <premis:significantPropertiesValue>{aic_id}</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>createdate</premis:significantPropertiesType>
      <premis:significantPropertiesValue>{create_date}</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>archivist_organization</premis:significantPropertiesType>
      <premis:significantPropertiesValue>{meta["archivist_org"]}</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>label</premis:significantPropertiesType>
      <premis:significantPropertiesValue>{meta["label"]}</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:significantProperties>
      <premis:significantPropertiesType>iptype</premis:significantPropertiesType>
      <premis:significantPropertiesValue>SIP</premis:significantPropertiesValue>
    </premis:significantProperties>
    <premis:objectCharacteristics>
      <premis:compositionLevel>0</premis:compositionLevel>
      <premis:format>
        <premis:formatDesignation>
          <premis:formatName>tar</premis:formatName>
        </premis:formatDesignation>
      </premis:format>
    </premis:objectCharacteristics>
    <premis:storage>
      <premis:storageMedium>Preservation platform ESSArch</premis:storageMedium>
    </premis:storage>
    <premis:relationship>
      <premis:relationshipType>structural</premis:relationshipType>
      <premis:relationshipSubType>is part of</premis:relationshipSubType>
      <premis:relatedObjectIdentification>
        <premis:relatedObjectIdentifierType>NO/RA</premis:relatedObjectIdentifierType>
        <premis:relatedObjectIdentifierValue>{aic_id}</premis:relatedObjectIdentifierValue>
      </premis:relatedObjectIdentification>
    </premis:relationship>
  </premis:object>
  <premis:event>
    <premis:eventIdentifier>
      <premis:eventIdentifierType>NO/RA</premis:eventIdentifierType>
      <premis:eventIdentifierValue>{uuid1()}</premis:eventIdentifierValue>
    </premis:eventIdentifier>
    <premis:eventType>20000</premis:eventType>
    <premis:eventDateTime>{create_date}</premis:eventDateTime>
    <premis:eventDetail>Created log circular</premis:eventDetail>
    <premis:eventOutcomeInformation>
      <premis:eventOutcome>0</premis:eventOutcome>
      <premis:eventOutcomeDetail>
        <premis:eventOutcomeDetailNote>Success to create logfile</premis:eventOutcomeDetailNote>
      </premis:eventOutcomeDetail>
    </premis:eventOutcomeInformation>
    <premis:linkingObjectIdentifier>
      <premis:linkingObjectIdentifierType>NO/RA</premis:linkingObjectIdentifierType>
      <premis:linkingObjectIdentifierValue>{sip_id}</premis:linkingObjectIdentifierValue>
    </premis:linkingObjectIdentifier>
  </premis:event>
</premis:premis>"""
    Path(path).write_text(xml, encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────

def _build_dias_package(
    content_path: Path,
    out_root:     Path,
    meta:         dict,
    log_fn,
) -> Path:
    """
    Bygger full DIAS AIC/SIP-pakke og returnerer stien til AIC-mappen.

    Mappestruktur som opprettes:
        {aic_id}/
          info.xml
          {sip_id}/
            log.xml
            administrative_metadata/repository_operations/
            descriptive_metadata/
            content/
              {sip_id}/        ← SIP-innhold (premis, mets, log, content/)
                {sip_id}.tar   ← komprimert innhold
    """
    log_fn("Bygger mappestruktur...")
    sip_id      = str(uuid1())
    tmp_out     = out_root / f"_dias_tmp_{sip_id}"
    tarfile_dir = tmp_out / sip_id / "content" / sip_id

    (tmp_out / sip_id / "administrative_metadata" / "repository_operations").mkdir(parents=True)
    (tmp_out / sip_id / "descriptive_metadata").mkdir(parents=True)
    (tarfile_dir / "administrative_metadata").mkdir(parents=True)
    (tarfile_dir / "descriptive_metadata").mkdir(parents=True)
    (tarfile_dir / "content").mkdir(parents=True)

    # Kopier ekstra filer (logg, SHA256, rapport, prosjektfil etc.) FØR
    # _gather_file_info slik at de inkluderes automatisk i METS/PREMIS
    extra_files_raw = meta.get("extra_files", "[]")
    try:
        extra_files = json.loads(extra_files_raw) if isinstance(extra_files_raw, str) else []
    except Exception:
        extra_files = []
    for ef in extra_files:
        try:
            src  = Path(ef["src"])
            dest = tarfile_dir / ef["dest"]
            if src.exists():
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
                log_fn(f"  Ekstra fil inkludert: {ef['dest']}")
            else:
                log_fn(f"  Advarsel: Ekstra fil ikke funnet: {src}")
        except Exception as exc:
            log_fn(f"  Feil ved kopiering av ekstra fil: {exc}")

    # Kopier valgfrie metadata-mapper
    desc_path = meta.get("descriptive_metadata_path", "").strip()
    if desc_path and Path(desc_path).is_dir():
        shutil.copytree(desc_path, tarfile_dir / "descriptive_metadata", dirs_exist_ok=True)

    adm_path = meta.get("administrative_metadata_path", "").strip()
    if adm_path and Path(adm_path).is_dir():
        shutil.copytree(adm_path, tarfile_dir / "administrative_metadata", dirs_exist_ok=True)

    # Zone 1 — ETP (SIP-bygging)
    now = _ts()
    log_fn("Skriver SIP log.xml...")
    _write_sip_log(str(tarfile_dir / "log.xml"), sip_id, now, meta)

    info_dict = _gather_file_info(str(tarfile_dir), sip_id, log_fn)
    info_dict.update(_gather_file_info(str(content_path), f"{sip_id}/content", log_fn))

    log_fn("Skriver premis.xml...")
    premis_path = str(tarfile_dir / "administrative_metadata" / "premis.xml")
    _write_sip_premis(premis_path, sip_id, info_dict)

    log_fn("Skriver mets.xml...")
    _write_sip_mets(str(tarfile_dir / "mets.xml"), premis_path, sip_id, now, info_dict, meta)

    _pack_sip(str(tarfile_dir), sip_id, str(content_path), log_fn)

    log_fn("Skriver info.xml...")
    _write_sip_info(
        str(tmp_out / "info.xml"),
        f"{tarfile_dir}.tar",
        sip_id, now, meta,
    )

    # Zone 2 — ETA (AIC-omslag)
    log_fn("Oppretter AIC-struktur...")
    aic_id   = str(uuid1())
    aic_path = out_root / aic_id
    tmp_out.rename(aic_path)

    log_fn("Skriver AIC log.xml...")
    _write_aic_log(str(aic_path / sip_id / "log.xml"), aic_id, sip_id, _ts(), meta)

    log_fn(f"Ferdig! AIC-mappe: {aic_path.name}")
    return aic_path
