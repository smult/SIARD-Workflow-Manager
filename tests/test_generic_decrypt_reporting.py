"""Report regression tests: original BLOB-conversion report layout/CSV conventions."""
from __future__ import annotations

import csv
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from siard_workflow.core.generic_decrypt_reporting import (
    CSV_HEADER, ERROR_HEADER, write_decrypt_reports,
)


class DecryptionReportTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        self.addCleanup(self.t.cleanup)
        self.root = Path(self.t.name)
        self.records = [
            dict(fil='content/schema0/table0/lob3/rec1.bin', opprinnelig_filnavn='1234.doc',
                 plassering='MyTable.MyBlob', format='doc', fra_storrelse=123,
                 til_storrelse=110, status='dekryptert', kategori='',
                 arsak='Passord fjernet; LOB og tabell-XML oppdatert',
                 passordkilde='lagret', formelavvik='nei'),
            dict(fil='content/schema0/table0/lob3/rec2.bin', opprinnelig_filnavn='3456.doc',
                 plassering='MyTable.MyBlob', format='doc', fra_storrelse=123,
                 til_storrelse=123, status='feil', kategori='Passord avvist',
                 arsak='InvalidKeyError: Failed to verify password',
                 passordkilde='', formelavvik='ja'),
            dict(fil='content/schema0/table0/lob3/rec3.bin', opprinnelig_filnavn='5678.doc',
                 plassering='MyTable.MyBlob', format='doc', fra_storrelse=90,
                 til_storrelse=90, status='beholdt', kategori='Ikke kryptert',
                 arsak='Dokumentet er allerede tilgjengelig uten passord',
                 passordkilde='', formelavvik=''),
        ]
        self.stats = dict(selected=3, decrypted_saved=1, decrypted_tested=0,
                          not_encrypted=1, decrypt_failed=1, password_stored=2,
                          formula_mismatch=1, formula_fallback_success=0,
                          pdf_skipped=0, format_skipped=0, already_exists=0)

    def test_all_report_variants_and_standard_csv_headers(self):
        paths = write_decrypt_reports(self.root, 'demo', self.records, self.stats,
                                     'test rules', dry_run=False,
                                     log_file=self.root / 'demo_20261105_025500.log')
        for p in paths.values():
            self.assertTrue(Path(p).is_file(), p)
            self.assertIn('20261105_025500', Path(p).name)
        with open(paths['csv'], encoding='utf-8-sig', newline='') as fh:
            rows = list(csv.DictReader(fh, delimiter=';'))
        self.assertEqual(list(CSV_HEADER), list(rows[0].keys()))
        self.assertEqual(3, len(rows))
        self.assertEqual('dekryptert', rows[0]['status'])
        self.assertEqual('Passord fjernet; LOB og tabell-XML oppdatert', rows[0]['kommentar'])
        with open(paths['errors_csv'], encoding='utf-8-sig', newline='') as fh:
            errors = list(csv.DictReader(fh, delimiter=';'))
        self.assertEqual(list(ERROR_HEADER), list(errors[0].keys()))
        self.assertEqual(1, len(errors))
        self.assertIn('rec2.bin', errors[0]['fil'])
        self.assertIn('Passord avvist', Path(paths['errors_log']).read_text(encoding='utf8'))
        self.assertGreater(Path(paths['pdf']).stat().st_size, 3000)
        content = Path(paths['pdf']).read_bytes()
        self.assertTrue(content.startswith(b'%PDF-'))

    def test_no_secrets_in_csv_and_pdf(self):
        records = [dict(self.records[0], arsak='Oppdatert', passordkilde='beregnet (fallback)')]
        paths = write_decrypt_reports(self.root, 'demo', records,
                                     dict(self.stats, selected=1, decrypted_saved=1,
                                          decrypt_failed=0, not_encrypted=0),
                                     'my rules', dry_run=False)
        for key in ('csv', 'errors_csv', 'errors_log'):
            text = Path(paths[key]).read_text(encoding='utf-8-sig')
            self.assertNotIn('R4321', text)
            self.assertNotIn('DEMO_OVERRIDE', text)

    def test_empty_error_csv_contains_header(self):
        paths = write_decrypt_reports(self.root, 'demo', self.records[:1],
                                     dict(self.stats, selected=1, decrypt_failed=0),
                                     'rules', dry_run=True)
        with open(paths['errors_csv'], encoding='utf-8-sig', newline='') as fh:
            data = list(csv.reader(fh, delimiter=';'))
        self.assertEqual(1, len(data))
        self.assertEqual(list(ERROR_HEADER), data[0])


if __name__ == '__main__':
    unittest.main()
