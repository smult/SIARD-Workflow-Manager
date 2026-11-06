"""Workflow operation for read-only SIARD content QA."""
from __future__ import annotations

from pathlib import Path

from siard_workflow.core.base_operation import BaseOperation, OperationResult
from siard_workflow.core.context import WorkflowContext
from siard_workflow.core.content_qa import load_profile, run_content_qa
from siard_workflow.core.content_qa.reporting import write_qa_reports


class ContentQAOperation(BaseOperation):
    operation_id = 'content_qa'
    label = 'Innholdskontroll (QA / informasjonsbærere)'
    description = (
        'Skrivebeskyttet analyse av SIARD-tabeller, radantall og faglige '
        'informasjonsbærere etter JSON-mal. Dokumenterer avvik, manglende '
        'kartlegging og relasjonsfeil. Lager JSON, CSV og PDF. '
        'Foreslår tabell-/nøkkelkoblinger og leser deklarerte fremmednøkler '
        'fra SIARD-metadata uten å godkjenne forslagene automatisk. '
        'Kan kjøres etter Pakk sammen SIARD, før Kjørerapport.'
    )
    category = 'Innhold'
    status = 1  # Pilot/beta, not a complete validated QA product.
    produces_siard = False
    requires_unpack = False
    modifies_content = False
    halt_on_failure = False

    default_params = {
        'profile_file': '',
        'scan_rows': True,
        'check_metadata_pk': True,
    }

    def run(self, ctx: WorkflowContext) -> OperationResult:
        log = ctx.metadata.get('file_logger')
        progress_cb = ctx.metadata.get('progress_cb')

        def message(msg: str, level: str = 'info'):
            if log:
                log.log(msg, 'success' if level == 'ok' else level)
            if progress_cb:
                progress_cb('log', msg=msg, level=level)

        try:
            source = Path(ctx.siard_path)
            profile_path = str(self.params.get('profile_file') or '').strip()
            profile = load_profile(profile_path)
            scan = bool(self.params.get('scan_rows', True))
            stop_event = ctx.metadata.get('stop_event')
            message(f'  Innholdskontroll: {source.name}; profil: {profile["id"]}')
            result = run_content_qa(
                source, profile, scan_rows=scan,
                check_metadata_pk=bool(self.params.get('check_metadata_pk', True)),
                on_progress=lambda info: message('  QA: ' + info),
                should_stop=stop_event.is_set if stop_event else None,
                temp_dir=ctx.metadata.get('temp_dir'),
            )
            output_dir = Path(ctx.metadata.get('log_dir') or source.parent)
            paths = write_qa_reports(result, output_dir, source.stem)
            summary = result['summary']
            data = {
                'qa_status': result['status'],
                'qa_tables': summary['tables'],
                'qa_tables_verified': summary['tables_verified'],
                'qa_tables_with_errors': summary['tables_with_errors'],
                'qa_pk_verified': summary['pk_verified'],
                'qa_pk_with_errors': summary['pk_with_errors'],
                'qa_carriers': summary['carriers'],
                'qa_carriers_verified': summary['carriers_verified'],
                'qa_carriers_unverified': summary['carriers_unverified'],
                'qa_carriers_with_errors': summary['carriers_with_errors'],
                'qa_relations_verified': summary['relations_verified'],
                'qa_profile_id': result['profile_id'],
                'qa_report_pdf': paths['pdf'],
                'qa_report_json': paths['json'],
                'qa_tables_csv': paths['tables_csv'],
                'qa_carriers_csv': paths['carriers_csv'],
                'qa_findings_csv': paths['findings_csv'],
                'qa_mapping_csv': paths['mapping_csv'],
                'qa_foreign_keys_csv': paths['foreign_keys_csv'],
                'qa_relation_proposals_csv': paths['relation_proposals_csv'],
                'qa_mapping_json': paths['mapping_json'],
                'qa_discovery_carriers_with_candidates': summary['discovery_carriers_with_candidates'],
                'qa_discovery_declared_foreign_keys': summary['discovery_declared_foreign_keys'],
                'qa_discovery_relation_proposals': summary['discovery_relation_proposals'],
            }
            message(f'  QA-rapport: {Path(paths["pdf"]).name}', 'ok')
            message(f'  Kartlegging: {summary["discovery_carriers_with_candidates"]}/'
                    f'{summary["carriers"]} fagbegreper med tabellforslag; '
                    f'{summary["discovery_declared_foreign_keys"]} fremmednøkler fra metadata; '
                    f'{summary["discovery_relation_proposals"]} navnebaserte relasjonsforslag')
            message(f'  QA-status: {result["status"]}; '
                    f'{summary["tables_verified"]}/{summary["tables"]} tabeller; '
                    f'{summary["carriers_verified"]}/{summary["carriers"]} informasjonsbærere',
                    'warn' if result['status'] != 'GODKJENT' else 'ok')
            verdict = (f'QA {result["status"]}: {summary["tables_verified"]}/'
                       f'{summary["tables"]} tabeller, '
                       f'{summary["carriers_verified"]}/{summary["carriers"]} informasjonsbærere')
            if result['status'] != 'GODKJENT':
                # An incomplete check must not yield an overall 'GODKJENT'
                # workflow verdict. halt_on_failure=False permits subsequent
                # reporting to run and document the incomplete/failed checks.
                return self._fail(verdict + '; se PDF/CSV', data)
            return self._ok(data, verdict + '; se PDF/CSV')
        except Exception as exc:
            # Preserve failure information instead of showing a green step if
            # a table, a temporary index or the PDF report could not be built.
            detail = f'{type(exc).__name__}: {exc}'
            message('  QA kunne ikke fullføres: ' + detail, 'warn')
            return self._fail('QA kunne ikke fullføres: ' + detail)
