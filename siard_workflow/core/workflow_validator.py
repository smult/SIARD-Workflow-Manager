"""
siard_workflow/core/workflow_validator.py
Rekkefølgevalidering for workflow-operasjoner.

Regler gjelder kun når begge de refererte operasjonene er tilstede i workflowen.
"""
from __future__ import annotations

_BEFORE_HEX_BLOB_IDS = {"standardize_ext"}  # + kategori "Systemspesifikt"


def _idxs(ops: list, op_id: str) -> list[int]:
    return [i for i, o in enumerate(ops) if o.operation_id == op_id]


def _before_hex_blob_idxs(ops: list) -> list[int]:
    """Indekser for operasjoner som må komme før hex_extract og blob_convert."""
    return [
        i for i, o in enumerate(ops)
        if getattr(o, "category", "") == "Systemspesifikt"
        or o.operation_id in _BEFORE_HEX_BLOB_IDS
    ]


def validate_workflow(ops: list) -> list[tuple]:
    """
    Returnerer liste av (op, melding) for hvert rekkefølgebrudd.
    Betingelser gjelder kun når begge operasjonene er tilstede i workflowen.
    """
    violations: list[tuple] = []
    n = len(ops)

    for idx, op in enumerate(ops):
        oid = op.operation_id
        msg = None

        if oid == "sha256":
            if idx != 0 and ops[idx - 1].operation_id != "repack_siard":
                msg = ("SHA256 skal være første operasjon, "
                       "eller plassert rett etter 'Pakk sammen SIARD'")

        elif oid == "unpack_siard":
            if idx > 1:
                msg = "'Pakk ut SIARD' skal være operasjon nr. 1 eller 2"

        elif oid == "virus_scan":
            u = _idxs(ops, "unpack_siard")
            if u and idx <= max(u):
                msg = "Virussjekk skal ligge etter 'Pakk ut SIARD'"

        elif getattr(op, "category", "") == "Systemspesifikt" \
                or oid in _BEFORE_HEX_BLOB_IDS:
            u = _idxs(ops, "unpack_siard")
            h = _idxs(ops, "hex_extract")
            b = _idxs(ops, "blob_convert")
            lbl = getattr(op, "label", oid)
            if u and idx <= max(u):
                msg = f"'{lbl}' skal ligge etter 'Pakk ut SIARD'"
            elif h and idx >= min(h):
                msg = f"'{lbl}' skal ligge før 'HEX-uttrekk'"
            elif b and idx >= min(b):
                msg = f"'{lbl}' skal ligge før 'Blob-konvertering'"

        elif oid == "hex_extract":
            u = _idxs(ops, "unpack_siard")
            s = _before_hex_blob_idxs(ops)
            if u and idx <= max(u):
                msg = "HEX-uttrekk skal ligge etter 'Pakk ut SIARD'"
            elif s and idx <= max(s):
                msg = "HEX-uttrekk skal ligge etter alle systemspesifikke operasjoner"

        elif oid == "blob_convert":
            u = _idxs(ops, "unpack_siard")
            s = _before_hex_blob_idxs(ops)
            h = _idxs(ops, "hex_extract")
            if u and idx <= max(u):
                msg = "Blob-konvertering skal ligge etter 'Pakk ut SIARD'"
            elif s and idx <= max(s):
                msg = "Blob-konvertering skal ligge etter alle systemspesifikke operasjoner"
            elif h and idx <= max(h):
                msg = "Blob-konvertering skal ligge etter 'HEX-uttrekk'"

        elif oid == "xml_cleaner":
            # Jobber direkte på utpakkede tableX.xml-filer i pipeline-modus.
            # Må derfor ligge mellom 'Pakk ut SIARD' og 'Pakk sammen SIARD'
            # når begge er tilstede.
            u = _idxs(ops, "unpack_siard")
            r = _idxs(ops, "repack_siard")
            if u and idx <= max(u):
                msg = "XML-renser skal ligge etter 'Pakk ut SIARD'"
            elif r and idx >= min(r):
                msg = "XML-renser skal ligge før 'Pakk sammen SIARD'"

        elif oid == "schema_selector":
            # Renser metadata.xml og content/-mapper i utpakket SIARD.
            # Må ligge mellom 'Pakk ut SIARD' og 'Pakk sammen SIARD' når begge
            # er tilstede. Bør ligge tidlig — før Blob Convert og HEX Extract
            # for å unngå unødvendig arbeid på schemas som skal fjernes.
            u = _idxs(ops, "unpack_siard")
            r = _idxs(ops, "repack_siard")
            if u and idx <= max(u):
                msg = "Schema-velger skal ligge etter 'Pakk ut SIARD'"
            elif r and idx >= min(r):
                msg = "Schema-velger skal ligge før 'Pakk sammen SIARD'"

        elif oid == "metadata_extract":
            r = _idxs(ops, "repack_siard")
            if r and idx <= max(r):
                msg = "Metadata-uttrekk skal ligge etter 'Pakk sammen SIARD'"

        elif oid == "workflow_report":
            r = _idxs(ops, "repack_siard")
            m = _idxs(ops, "metadata_extract")
            d = _idxs(ops, "dias_package")
            if r and idx <= max(r):
                msg = "Kjørerapport skal ligge etter 'Pakk sammen SIARD'"
            elif m and idx <= max(m):
                msg = "Kjørerapport skal ligge etter 'Metadata-uttrekk'"
            elif d and idx >= min(d):
                msg = "Kjørerapport skal ligge før 'DIAS-pakking'"

        elif oid == "dias_package":
            if idx != n - 1:
                msg = "'DIAS-pakking' skal alltid være siste operasjon"

        if msg:
            violations.append((op, msg))

    return violations
