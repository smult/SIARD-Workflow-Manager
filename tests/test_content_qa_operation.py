"""Operation/CLI integration tests against synthetic SIARD archives."""
import json
from pathlib import Path
import tempfile
import unittest

from siard_workflow.core.context import WorkflowContext
from siard_workflow.operations.content_qa_operation import ContentQAOperation
from siard_workflow.core.content_qa.__main__ import main as qa_main
from test_content_qa import example_profile, synthetic_siard


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.archive = synthetic_siard(self.root)
        self.log_dir = self.root / 'logs'

    def test_mapped_quality_report_ok(self):
        profile = self.root / 'mapped.json'
        profile.write_text(json.dumps(example_profile()), encoding='utf-8')
        ctx = WorkflowContext(siard_path=self.archive)
        ctx.metadata['log_dir'] = str(self.log_dir)
        op = ContentQAOperation(profile_file=str(profile), scan_rows=True)
        before = self.archive.read_bytes()
        result = op.run(ctx)
        self.assertTrue(result.success, result.message)
        self.assertEqual(result.data['qa_status'], 'GODKJENT')
        self.assertEqual(result.data['qa_tables_verified'], 2)
        self.assertEqual(result.data['qa_carriers_verified'], 2)
        self.assertEqual(before, self.archive.read_bytes())
        self.assertTrue(Path(result.data['qa_report_pdf']).is_file())

    def test_unmapped_profile_is_not_green(self):
        ctx = WorkflowContext(siard_path=self.archive)
        ctx.metadata['log_dir'] = str(self.log_dir)
        op = ContentQAOperation()
        result = op.run(ctx)
        self.assertFalse(result.success, result.message)
        self.assertEqual(result.data['qa_status'], 'DELVIS_KONTROLLERT')
        self.assertEqual(result.data['qa_carriers_verified'], 0)
        self.assertTrue(Path(result.data['qa_report_pdf']).is_file())

    def test_missing_archive_produces_failure(self):
        ctx = WorkflowContext(siard_path=self.root / 'missing.siard')
        result = ContentQAOperation().run(ctx)
        self.assertFalse(result.success)

    def test_cli_mapped_exit_zero(self):
        profile = self.root / 'mapped.json'
        profile.write_text(json.dumps(example_profile()), encoding='utf-8')
        code = qa_main([str(self.archive), '--profile', str(profile),
                        '--out', str(self.log_dir)])
        self.assertEqual(code, 0)

    def test_cli_default_is_partial(self):
        code = qa_main([str(self.archive), '--out', str(self.log_dir)])
        self.assertEqual(code, 2)


if __name__ == '__main__':
    unittest.main()
