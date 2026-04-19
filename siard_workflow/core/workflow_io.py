"""
siard_workflow/core/workflow_io.py
Importer og eksporter workflow-konfigurasjon som JSON.
"""
from __future__ import annotations
import json
from pathlib import Path
from .workflow import Workflow


def _op_map() -> dict:
    from siard_workflow.operations import (
        SHA256Operation, BlobCheckOperation,
        XMLValidationOperation, MetadataExtractOperation,
        VirusScanOperation, ConditionalOperation,
        UnpackSiardOperation, RepackSiardOperation,
        WorkflowReportOperation,
    )
    from siard_workflow.operations.blob_convert_operation import BlobConvertOperation
    from siard_workflow.operations.hex_extract_operation import HexExtractOperation
    return {
        "sha256":            SHA256Operation,
        "blob_check":        BlobCheckOperation,
        "xml_validation":    XMLValidationOperation,
        "metadata_extract":  MetadataExtractOperation,
        "virus_scan":        VirusScanOperation,
        "blob_convert":      BlobConvertOperation,
        "hex_extract":       HexExtractOperation,
        "unpack_siard":      UnpackSiardOperation,
        "repack_siard":      RepackSiardOperation,
        "workflow_report":   WorkflowReportOperation,
    }


def ops_to_dict(ops: list) -> list:
    """Serialiser en liste operasjoner til JSON-kompatibel liste."""
    result = []
    for op in ops:
        entry = {
            "operation_id": op.operation_id,
            "params": {k: v for k, v in op.params.items()
                       if not k.startswith("_")},
        }
        if hasattr(op, "_inner"):
            entry["inner"] = {
                "operation_id": op._inner.operation_id,
                "params": op._inner.params.copy(),
            }
            entry["flag"]     = op.params.get("flag", "")
            entry["run_when"] = op.params.get("run_when", True)
        result.append(entry)
    return result


def ops_from_dict(ops_data: list) -> list:
    """Gjenoppbygg en liste operasjoner fra serialisert liste."""
    om = _op_map()
    from siard_workflow.operations import ConditionalOperation
    ops = []
    for entry in ops_data:
        op_id = entry["operation_id"]
        if op_id.startswith("if_") or "inner" in entry:
            inner_id  = entry.get("inner", {}).get("operation_id", "")
            inner_cls = om.get(inner_id)
            if not inner_cls:
                continue
            inner_op = inner_cls(**entry.get("inner", {}).get("params", {}))
            op = ConditionalOperation(
                inner_op,
                flag=entry.get("flag", ""),
                run_when=entry.get("run_when", True),
            )
        else:
            cls = om.get(op_id)
            if not cls:
                continue
            op = cls(**entry.get("params", {}))
        ops.append(op)
    return ops


def workflow_to_dict(wf: Workflow) -> dict:
    """Serialiser en workflow til en JSON-kompatibel dict."""
    return {
        "name": wf.name,
        "stop_on_error": wf.stop_on_error,
        "operations": ops_to_dict(list(wf)),
    }


def workflow_to_json(wf: Workflow, path: Path) -> None:
    data = workflow_to_dict(wf)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def workflow_from_dict(data: dict) -> Workflow:
    """Gjenoppbygg en Workflow fra en serialisert dict."""
    wf = Workflow(name=data.get("name", "Importert"),
                  stop_on_error=data.get("stop_on_error", False))
    for op in ops_from_dict(data.get("operations", [])):
        wf.add(op)
    return wf


def workflow_from_json(path: Path) -> Workflow:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return workflow_from_dict(data)
