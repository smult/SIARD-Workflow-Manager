"""Generic, JSON-driven SIARD document decryption.

Follows the same unpack -> process LOBs / update table XML -> repack workflow
as CosDoc/Barnevern. The only system-specific information comes from rules.
Uses the existing WorkflowFileLogger, PREMIS and report pipeline, and emits
per-document CSV / error CSV / error log / PDF reports alongside the run log.
"""
from __future__ import annotations

from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.document_password_rules import (
    RuleError, execute_rule, read_rule,
)
from siard_workflow.core.generic_decrypt_reporting import write_decrypt_reports


class GenericDocumentDecryptOperation(BaseOperation):
    operation_id = 'generic_document_decrypt'
    label = 'Generisk dokumentopplåsing (JSON-regler)'
    description = (
        'Dekrypterer passordbeskyttede dokumenter etter JSON-regler. '
        'Samme pipeline som Acos/CosDoc: skriver LOB-er og oppdatert table XML '
        'i SIARD-arbeidskopien, deretter Pakk sammen SIARD. '
        'Loggfører resultat per dokument i CSV, egen feilliste og PDF-rapport.'
    )
    category = 'Systemspesifikt'
    status = 2
    produces_siard = False
    requires_unpack = True
    modifies_content = True
    halt_on_failure = False

    default_params = {
        'rule_file': '',
        'unpacked_dir': '',
        'lob_ref': '',
        'max_files': 0,
        'dry_run': False,
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get('file_logger')
        progress_cb = ctx.metadata.get('progress_cb')

        def write(message: str, level: str = 'info') -> None:
            if log:
                log.log(message, 'success' if level == 'ok' else level)
            if progress_cb:
                progress_cb('log', msg=message, level=level)

        try:
            existing = str(self.params.get('unpacked_dir', '') or '').strip()
            reused = Path(existing).expanduser() if existing else None
            pipeline = getattr(ctx, 'extracted_path', None)
            if pipeline is not None and pipeline.is_dir():
                directory = pipeline
                if reused and reused.resolve() != directory.resolve():
                    write('  Bruker aktiv pipeline-mappe; gjenbruksmappe er ignorert', 'warn')
            elif reused and reused.is_dir():
                directory = reused
                ctx.extracted_path = reused
            else:
                return self._fail(
                    'Ingen utpakket SIARD-mappe. Legg Pakk ut SIARD før '
                    'dokumentopplåsingen i arbeidsflyten.'
                )
            write(f'  Arbeidsmappe: {directory}', 'info')

            default_rule = (Path(__file__).resolve().parents[1]
                            / 'rules' / 'acos_sosial_s1021.json')
            selected_rule = str(self.params.get('rule_file', '') or '').strip()
            rule_path = Path(selected_rule).expanduser() if selected_rule else default_rule
            rule = read_rule(rule_path)
            name = str(rule.get('name', rule_path.name))
            dry_run = bool(self.params.get('dry_run', False))
            max_files = int(self.params.get('max_files', 0) or 0)
            ref = str(self.params.get('lob_ref', '') or '').strip()
            stop_event = ctx.metadata.get('stop_event')
            write(f'  Regelsett: {name}', 'info')
            write(f'  Modus: {"Testkjøring (ingen endring)" if dry_run else "Oppdater utpakket SIARD + sammenpakking"}', 'info')
            write(f'  Omfang: {ref or "alle"}; maks filer: {max_files or "alle"}', 'info')

            records: list[dict] = []
            data = execute_rule(
                directory, rule,
                only_ref=ref, max_files=max_files, dry_run=dry_run,
                log=write, stop=(stop_event.is_set if stop_event else None),
                record_sink=lambda items: records.extend(items),
            )
            data['dry_run'] = dry_run
            data['rule_file'] = str(rule_path)
            data['unpacked_dir'] = str(directory)
            data['rule_name'] = name
            if not records and data.get('selected'):
                # An unexpected missing audit trail must never be silently
                # presented as a completed run.
                return self._fail('Manglende dokumentlogg fra dekryptering', data=data)

            log_dir = Path(ctx.metadata.get('log_dir') or ctx.siard_path.parent)
            file_log_path = getattr(log, 'log_path', None)
            reports = write_decrypt_reports(
                log_dir, ctx.siard_path.stem, records, data, name,
                dry_run=dry_run, log_file=file_log_path,
            )
            data['document_log_csv'] = reports['csv']
            data['error_log_csv'] = reports['errors_csv']
            data['error_log_path'] = reports['errors_log']
            data['document_report_pdf'] = reports['pdf']
            ctx.set_result(self.operation_id, data)
            write(f'  Dokumentlogg (CSV): {Path(reports["csv"]).name}', 'ok')
            write(f'  Dekrypteringsfeil (CSV): {Path(reports["errors_csv"]).name}', 'ok')
            write(f'  Feillogg: {Path(reports["errors_log"]).name}', 'ok')
            write(f'  Dokumentrapport (PDF): {Path(reports["pdf"]).name}', 'ok')

            count = data['decrypted_tested'] if dry_run else data['decrypted_saved']
            failed = sum(int(data.get(key, 0)) for key in (
                'decrypt_failed', 'lookup_ambiguous', 'lookup_missing',
                'blob_missing', 'password_missing',
            ))
            write('  OPPSUMMERING: '
                  f'valgt={data["selected"]}, '
                  f'passordfunnet={data["password_stored"]}, '
                  f'dekryptert={count}, '
                  f'beholdt_ukryptert={data["not_encrypted"]}, '
                  f'passord_fallback={data["formula_fallback_success"]}, '
                  f'feil={failed}', 'info')
            if data.get('stopped'):
                return self._fail('Operasjonen ble avbrutt; dokumentlogg skrevet', data)
            if not dry_run and count:
                write('  Arbeidskopi oppdatert: bruk Pakk sammen SIARD for ny SIARD-fil', 'ok')
            summary = (f'{"Testet" if dry_run else "Dekryptert i SIARD-arbeidskopien"}: '
                       f'{count} dokument(er), {failed} feil; CSV + PDF lagret')
            if failed:
                # The run may continue to repack/report because
                # halt_on_failure=False. The overall status must still show
                # genuine decryption failures (unlike retained plain files).
                return self._fail(summary, data)
            return self._ok(data, summary)
        except (RuleError, OSError, ValueError, ImportError) as exc:
            write(f'  Generisk dokumentopplåsing: {exc}', 'warn')
            return self._fail(str(exc))

    def premis_should_record(self, result: OperationResult, ctx: WorkflowContext) -> bool:
        return (not bool(self.params.get('dry_run', False))
                and bool(result.data.get('decrypted_saved', 0)))
