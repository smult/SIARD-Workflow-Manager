"""Experimental SIARD operation: password lookup and safe Office decryption.

Integrates with the existing pipeline extracted_path, or takes an explicit
existing unpacked folder to support reruns without extracting the SIARD again.
No file inside the unpacked SIARD is changed by this pilot operation.
"""
from __future__ import annotations

from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.document_password_rules import (
    RuleError, execute_rule, read_rule,
)


class GenericDocumentDecryptOperation(BaseOperation):
    operation_id = "generic_document_decrypt"
    label = "Generisk dokumentopplaasning (JSON-regler)"
    description = (
        "Generisk oppslag av dokumentpassord via SIARD-tabeller og konfigurert "
        "formel. Testkjoering dekrypterer bare til en midlertidig fil. "
        "Vanlig kjoering lagrer opplaaste KOPIER utenfor SIARD. "
        "Eksisterende utpakket mappe kan gjenbrukes mellom kjoeringer."
    )
    category = "Systemspesifikt"
    status = 2  # Visible with the default status filter; experimental by label/docs.
    halt_on_failure = True
    produces_siard = False
    modifies_content = False
    requires_unpack = False  # Either pipeline ctx or explicit reused folder is valid.

    default_params = {
        "rule_file": "",        # empty = sample Acos Sosial rule bundled with source
        "unpacked_dir": "",     # empty = ctx.extracted_path (current pipeline)
        "lob_ref": "",          # e.g. rec10.bin; empty = all matching rows
        "max_files": 10,       # 0 = all
        "dry_run": True,
        "output_dir": "",       # mandatory when dry_run=False
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get("file_logger")
        pcb = ctx.metadata.get("progress_cb")

        def write(message: str, level: str = "info") -> None:
            if log:
                log.log(message, level)
            if pcb:
                pcb("log", msg=message, level=level)

        try:
            explicit_dir = str(self.params.get("unpacked_dir", "") or "").strip()
            reused = Path(explicit_dir).expanduser() if explicit_dir else None
            from_pipeline = getattr(ctx, "extracted_path", None)
            if reused:
                directory = reused
                write(f"  Gjenbruker oppgitt utpakket SIARD-mappe: {directory}")
            elif from_pipeline is not None and from_pipeline.is_dir():
                directory = from_pipeline
                write(f"  Bruker allerede utpakket pipeline-mappe: {directory}")
            else:
                return self._fail(
                    "Ingen utpakket SIARD-mappe. Bruk Pakk ut SIARD foerst, "
                    "eller oppgi en BEHOLDT mappe i feltet 'Utpakket SIARD-mappe'."
                )

            default_rule = (Path(__file__).resolve().parents[1]
                            / "rules" / "acos_sosial_s1021.json")
            rule_file = str(self.params.get("rule_file", "") or "").strip()
            rule_path = Path(rule_file).expanduser() if rule_file else default_rule
            rule = read_rule(rule_path)
            dry_run = bool(self.params.get("dry_run", True))
            max_files = int(self.params.get("max_files", 10) or 0)
            lob_ref = str(self.params.get("lob_ref", "") or "").strip()
            target_str = str(self.params.get("output_dir", "") or "").strip()
            target = Path(target_str).expanduser() if target_str else None
            stop_event = ctx.metadata.get("stop_event")

            write(f"  Regelsett: {rule.get('name', rule_path.name)}")
            write(f"  Modus: {'TEST (ingen varig lagring)' if dry_run else 'LAGRE DEKRYPTERTE KOPIER'}")
            write(f"  Omfang: {lob_ref or 'alle'}; maks filer: {max_files or 'alle'}")
            data = execute_rule(
                directory, rule,
                only_ref=lob_ref, max_files=max_files, dry_run=dry_run,
                output_dir=target, log=write,
                stop=(stop_event.is_set if stop_event else None),
            )
            data["dry_run"] = dry_run
            data["rule_file"] = str(rule_path)
            data["unpacked_dir"] = str(directory)
            if not dry_run and target is not None:
                data["output_dir"] = str(target)
            ctx.set_result(self.operation_id, data)

            write("  OPPSUMMERING: " + ", ".join(
                f"{key}={data[key]}" for key in (
                    "selected", "password_stored", "password_formula_only",
                    "formula_match", "formula_mismatch", "decrypted_tested",
                    "decrypted_saved", "formula_fallback_attempted",
                    "formula_fallback_success", "pdf_skipped", "not_encrypted",
                    "blob_missing", "decrypt_failed", "lookup_ambiguous",
                )))
            if data.get("stopped"):
                return self._fail("Test/steg avbrutt av bruker", data=data)
            if not data.get("selected"):
                return self._fail("Ingen dokumenter passet til regel og filtre", data=data)
            if data.get("decrypt_failed") or data.get("lookup_ambiguous"):
                return self._fail("Noen dokumenter kunne ikke behandles (se logg)", data=data)
            n = data["decrypted_tested"] if dry_run else data["decrypted_saved"]
            return self._ok(data, f"{'Testet' if dry_run else 'Lagret'}: {n} dokumenter")
        except (RuleError, OSError, ValueError) as exc:
            write(f"  Generisk dokumentopplaasning: {exc}", "warn")
            return self._fail(str(exc))
