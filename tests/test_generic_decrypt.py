"""Synthetic tests: no real archive documents or credentials are distributed."""
from __future__ import annotations

import json
import os
from unittest.mock import patch
import hashlib
import zipfile
import xml.etree.ElementTree as ET
import sys
import tempfile
import types
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
from siard_workflow.core.document_password_rules import (
    RuleError, execute_rule, formula_password, read_rule, resolve_lob,
    read_metadata_tables,
)

RULE = _ROOT / "siard_workflow" / "rules" / "acos_sosial_s1021.json"
CFB = bytes.fromhex("d0cf11e0a1b11ae1")


class FakeInvalidKeyError(Exception):
    """Test stand-in for msoffcrypto.exceptions.InvalidKeyError."""


class FakeOfficeFile:
    """Stub for test isolation; not a real Office encryption implementation."""

    attempts: list[tuple[str, str]] = []
    non_key_error_for: str = ""
    probe_error_for: str = ""
    force_reject_for: str = ""
    reject_after_partial_write: bool = False

    def __init__(self, fh):
        self.handle = fh
        self.ref = fh.name
        self.password = None

    def is_encrypted(self):
        if Path(self.ref).name == FakeOfficeFile.probe_error_for:
            raise RuntimeError('Simulated Office encryption probe failure')
        return self.handle.read(64).startswith(CFB + b"SECRET-FIXTURE-ENCRYPTED")

    def load_key(self, password):
        name = Path(self.ref).name
        FakeOfficeFile.attempts.append((name, password))
        if name == FakeOfficeFile.non_key_error_for:
            raise RuntimeError("Cannot read encryption metadata")
        if name == FakeOfficeFile.force_reject_for:
            raise FakeInvalidKeyError("Failed to verify password")
        accepted = {
            "rec10.bin": "R4321",
            "rec1520.bin": "R2111",  # formula is valid, stored value is not
            "rec55.bin": "R6789",
        }
        if password != accepted.get(name):
            if not (FakeOfficeFile.reject_after_partial_write and
                    name == "rec1520.bin" and password == "DEMO_OVERRIDE"):
                raise FakeInvalidKeyError("Failed to verify password")
        self.password = password

    def decrypt(self, fh):
        if not self.password:
            raise ValueError("key not loaded")
        if (FakeOfficeFile.reject_after_partial_write and
                Path(self.ref).name == "rec1520.bin" and
                self.password == "DEMO_OVERRIDE"):
            fh.write(b"PARTIAL-INVALID-DATA")
            raise FakeInvalidKeyError("Failed to verify password")
        fh.write(CFB + b"PLAIN-TEST-ONLY")


class GenericDecryptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="generic_test_")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "siard_extract"
        self.root.mkdir()
        (self.root / "header").mkdir()
        tables = {
            "NEF_ELFILER": ("table186", ["Nef_DokID", "Nef_Versjon", "Nef_Variant", "Nef_SeqNr",
                                          "Nef_Filnavn", "Nef_Size", "Nef_ElFil", "nef_filsti", "Nef_Kryptert"]),
            "NVE_DOKVERSJON": ("table180", ["Nve_DokID", "Nve_Versjon", "Nve_Variant"]
                                          + [f"demo_field_{i}" for i in range(4, 15)]
                                          + ["nve_passord"]),
        }
        xml_tables = []
        for name, (folder, columns) in tables.items():
            cxml = "".join(
                f"<column><name>{name_}</name>{'<lobFolder>schema0/table186/lob7</lobFolder>' if name_ == 'Nef_ElFil' else ''}</column>"
                for name_ in columns
            )
            xml_tables.append(f"<table><name>{name}</name><folder>{folder}</folder><columns>{cxml}</columns></table>")
            (self.root / "content" / "schema0" / folder).mkdir(parents=True)
        metadata = "<archive xmlns='http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd'>" + (
            "<schemas><schema><name>dbo</name><folder>schema0</folder><tables>" + "".join(xml_tables)
            + "</tables></schema></schemas></archive>"
        )
        (self.root / "header" / "metadata.xml").write_text(metadata, encoding="utf-8")
        fpath = self.root / "content" / "schema0" / "table186"
        lobs = fpath / "lob7"
        lobs.mkdir()
        for blob in ("rec10.bin", "rec11.bin", "rec1520.bin", "rec55.bin", "rec90.bin"):
            (lobs / blob).write_bytes(CFB + b"SECRET-FIXTURE-ENCRYPTED")
        (lobs / "rec100.bin").write_bytes(b"%PDF-1.7\nconverted")
        cases = [
            ("6", "1", "P", "1", "1234.doc", "rec10.bin"),
            ("6", "1", "P", "2", "1234_D.doc", "rec11.bin"),
            ("7", "1", "P", "1", "1112.doc", "rec1520.bin"),
            ("8", "1", "P", "1", "9876.doc", "rec55.bin"),
            ("9", "1", "P", "1", "4444.doc", "rec90.bin"),
            ("10", "1", "P", "1", "7777.doc", "rec100.bin"),
        ]
        rows = []
        for dokid, versjon, variant, seq, name, blob in cases:
            rows.append(f"<row><c1>{dokid}</c1><c2>{versjon}</c2><c3>{variant}</c3>"
                        f"<c4>{seq}</c4><c5>{name}</c5><c6>100</c6>"
                        f"<c7 file='{blob}' length='100' /></row>")
        (fpath / "table186.xml").write_text("<table>" + "".join(rows) + "</table>", encoding="utf-8")
        pw_rows = [("6", "R4321"), ("7", "DEMO_OVERRIDE"), ("8", ""),
                   ("9", ""), ("10", "R7777")]
        lookup_rows = [f"<row><c1>{did}</c1><c2>1</c2><c3>P</c3><c15>{pw}</c15></row>"
                       for did, pw in pw_rows]
        (self.root / "content" / "schema0" / "table180" / "table180.xml").write_text(
            "<table>" + "".join(lookup_rows) + "</table>", encoding="utf-8")
        self._old_crypto = sys.modules.get("msoffcrypto")
        FakeOfficeFile.attempts = []
        FakeOfficeFile.non_key_error_for = ""
        FakeOfficeFile.probe_error_for = ""
        FakeOfficeFile.force_reject_for = ""
        FakeOfficeFile.reject_after_partial_write = False
        sys.modules["msoffcrypto"] = types.SimpleNamespace(
            OfficeFile=FakeOfficeFile,
            exceptions=types.SimpleNamespace(InvalidKeyError=FakeInvalidKeyError),
        )
        self.addCleanup(self.restore_crypto)
        self.rule = read_rule(RULE)

    def restore_crypto(self):
        if self._old_crypto is None:
            sys.modules.pop("msoffcrypto", None)
        else:
            sys.modules["msoffcrypto"] = self._old_crypto

    def _add_document(self, docid: str, name: str, blob: str, payload: bytes):
        """Add one original SIARD row to exercise missing-password cases."""
        blob_path = self.root / 'content/schema0/table186/lob7' / blob
        blob_path.write_bytes(payload)
        table = self.root / 'content/schema0/table186/table186.xml'
        xml = table.read_text(encoding='utf-8')
        row = (f"<row><c1>{docid}</c1><c2>1</c2><c3>P</c3><c4>1</c4>"
               f"<c5>{name}</c5><c6>{len(payload)}</c6>"
               f"<c7 file='{blob}' length='{len(payload)}' /></row>")
        table.write_text(xml.replace('</table>', row + '</table>'), encoding='utf-8')
        return blob_path

    def test_plain_doc_without_password_is_retained_without_error(self):
        """The 1,935 *_P.doc files must not be rejected solely for no password."""
        blob = self._add_document('8', '4331_1_P.doc', 'rec200.bin',
                                  CFB + b'PLAIN-WORD-DOCUMENT')
        original = blob.read_bytes()
        rows = []
        stats = execute_rule(self.root, self.rule, only_ref='rec200.bin',
                             dry_run=False, record_sink=rows.extend)
        self.assertEqual(1, stats['selected'])
        self.assertEqual(1, stats['not_encrypted'])
        self.assertEqual(0, stats['password_missing'])
        self.assertEqual(0, stats['decrypt_failed'])
        self.assertEqual(0, stats['decrypted_saved'])
        self.assertEqual('beholdt', rows[0]['status'])
        self.assertEqual('Ikke kryptert', rows[0]['kategori'])
        self.assertTrue(rows[0]['fil'].startswith('content/'))
        self.assertEqual(original, blob.read_bytes())
        from siard_workflow.core.generic_decrypt_reporting import write_decrypt_reports
        import csv
        reports = write_decrypt_reports(Path(self.temp.name), 'demo', rows, stats,
                                         'test rules', dry_run=False)
        with open(reports['csv'], encoding='utf-8-sig', newline='') as fh:
            line = next(csv.DictReader(fh, delimiter=';'))
        self.assertEqual('beholdt', line['status'])
        self.assertIn('tilgjengelig uten passord', line['kommentar'])
        with open(reports['errors_csv'], encoding='utf-8-sig', newline='') as fh:
            self.assertEqual([], list(csv.DictReader(fh, delimiter=';')))
        self.assertTrue(Path(reports['pdf']).read_bytes().startswith(b'%PDF-'))

    def test_plain_doc_with_missing_lookup_still_retained(self):
        blob = self._add_document('9999', '9001_1_P.doc', 'rec201.bin',
                                  CFB + b'PLAIN-DOC')
        rows = []
        stats = execute_rule(self.root, self.rule, only_ref='rec201.bin',
                             record_sink=rows.extend)
        self.assertEqual(1, stats['not_encrypted'])
        self.assertEqual(0, stats['lookup_missing'])
        self.assertEqual('beholdt', rows[0]['status'])
        self.assertEqual(blob.read_bytes(), CFB + b'PLAIN-DOC')

    def test_plain_doc_with_ambiguous_lookup_still_retained(self):
        self._add_document('6', '9002_1_P.doc', 'rec202.bin', CFB + b'PLAIN-DOC')
        pw_xml = self.root / 'content/schema0/table180/table180.xml'
        data = pw_xml.read_text(encoding='utf-8')
        pw_xml.write_text(data.replace('</table>',
                           '<row><c1>6</c1><c2>1</c2><c3>P</c3><c15>bad</c15></row></table>'),
                           encoding='utf-8')
        rows = []
        stats = execute_rule(self.root, self.rule, only_ref='rec202.bin',
                             record_sink=rows.extend)
        self.assertEqual(1, stats['not_encrypted'])
        self.assertEqual(0, stats['lookup_ambiguous'])
        self.assertEqual('beholdt', rows[0]['status'])

    def test_plain_ooxml_without_password_is_retained(self):
        self._add_document('8', '4331_1_P.docx', 'rec203.bin',
                           b'PK\x03\x04' + b'PLAIN-OFFICE-ZIP')
        rows = []
        stats = execute_rule(self.root, self.rule, only_ref='rec203.bin',
                             record_sink=rows.extend)
        self.assertEqual(1, stats['not_encrypted'])
        self.assertEqual(0, stats['password_missing'])
        self.assertEqual('beholdt', rows[0]['status'])

    def test_encrypted_doc_without_password_is_a_real_error(self):
        original = CFB + b'SECRET-FIXTURE-ENCRYPTED'
        blob = self._add_document('8', '4331_1_P.doc', 'rec204.bin', original)
        rows = []
        stats = execute_rule(self.root, self.rule, only_ref='rec204.bin',
                             dry_run=False, record_sink=rows.extend)
        self.assertEqual(1, stats['selected'])
        self.assertEqual(0, stats['not_encrypted'])
        self.assertEqual(1, stats['password_missing'])
        self.assertEqual(0, stats['decrypted_saved'])
        self.assertEqual('feil', rows[0]['status'])
        self.assertEqual('Passord mangler', rows[0]['kategori'])
        self.assertIn('Kryptert Office-dokument', rows[0]['arsak'])
        self.assertTrue(rows[0]['fil'].startswith('content/'))
        self.assertEqual(original, blob.read_bytes())

    def test_encrypted_doc_missing_lookup_is_a_real_error(self):
        self._add_document('9999', '4332_1_P.doc', 'rec205.bin',
                           CFB + b'SECRET-FIXTURE-ENCRYPTED')
        rows = []
        stats = execute_rule(self.root, self.rule, only_ref='rec205.bin',
                             record_sink=rows.extend)
        self.assertEqual(1, stats['lookup_missing'])
        self.assertEqual(0, stats['not_encrypted'])
        self.assertEqual('feil', rows[0]['status'])
        self.assertEqual('Passordoppslag mangler', rows[0]['kategori'])

    def test_failed_encryption_probe_is_not_misclassified_as_plain(self):
        self._add_document('8', '4333_1_P.doc', 'rec206.bin', CFB + b'PLAIN')
        FakeOfficeFile.probe_error_for = 'rec206.bin'
        rows = []
        stats = execute_rule(self.root, self.rule, only_ref='rec206.bin',
                             record_sink=rows.extend)
        self.assertEqual(1, stats['decrypt_failed'])
        self.assertEqual(0, stats['not_encrypted'])
        self.assertEqual('feil', rows[0]['status'])
        self.assertEqual('Krypteringskontroll feilet', rows[0]['kategori'])

    def test_mixed_batch_reports_encrypted_and_plain_separately(self):
        self._add_document('8', '4334_1_P.doc', 'rec207.bin', CFB + b'PLAIN-DOC')
        rows = []
        stats = execute_rule(self.root, self.rule, max_files=0,
                             dry_run=False, record_sink=rows.extend)
        self.assertEqual(6, stats['selected'])
        self.assertEqual(1, stats['not_encrypted'])
        self.assertEqual(3, stats['decrypted_saved'])
        self.assertEqual(1, stats['decrypt_failed'])  # existing encrypted rec90
        self.assertEqual(0, stats['password_missing'])
        self.assertEqual(6, len(rows))
        self.assertEqual(3, sum(r['status'] == 'dekryptert' for r in rows))
        self.assertEqual(1, sum(r['status'] == 'beholdt' and r['kategori'] == 'Ikke kryptert'
                                for r in rows))
        self.assertEqual(1, sum(r['status'] == 'feil' for r in rows))

    def test_formula_and_exception(self):
        formula = self.rule["formula"]
        self.assertEqual("R4321", formula_password("1234.doc", formula))
        self.assertEqual("", formula_password("1234_D.doc", formula))
        self.assertEqual("R2111", formula_password("1112.doc", formula))

    def test_metadata_and_ref_lookup(self):
        table = read_metadata_tables(self.root)["nef_elfiler"]
        path = resolve_lob(self.root, table, table.column_index("nef_elfil"), "rec10.bin")
        self.assertEqual("rec10.bin", path.name)
        self.assertTrue(path.is_file())

    def test_real_dry_run_on_one_file(self):
        original = (self.root / "content/schema0/table186/lob7/rec10.bin").read_bytes()
        results = execute_rule(self.root, self.rule, only_ref="rec10.bin", max_files=1)
        self.assertEqual(1, results["decrypted_tested"])
        self.assertEqual(1, results["formula_match"])
        self.assertEqual(1, results["password_stored"])
        self.assertEqual(0, results["formula_fallback_attempted"])
        self.assertEqual([("rec10.bin", "R4321")], FakeOfficeFile.attempts)
        self.assertEqual(original, (self.root / "content/schema0/table186/lob7/rec10.bin").read_bytes())
        self.assertEqual([], list((Path(self.temp.name)).rglob("*decrypted*.doc")))

    def test_rejected_stored_password_retries_formula(self):
        messages: list[str] = []
        results = execute_rule(
            self.root, self.rule, only_ref="rec1520.bin",
            log=lambda message, level: messages.append(message),
        )
        self.assertEqual(1, results["decrypted_tested"])
        self.assertEqual(1, results["formula_mismatch"])
        self.assertEqual(1, results["formula_fallback_attempted"])
        self.assertEqual(1, results["formula_fallback_success"])
        self.assertEqual(0, results["decrypt_failed"])
        self.assertEqual(
            [("rec1520.bin", "DEMO_OVERRIDE"), ("rec1520.bin", "R2111")],
            FakeOfficeFile.attempts,
        )
        self.assertNotIn("DEMO_OVERRIDE", " ".join(messages))
        self.assertNotIn("R2111", " ".join(messages))

    def test_fallback_after_failed_decrypt_replaces_lob_and_metadata(self):
        FakeOfficeFile.reject_after_partial_write = True
        blob = self.root / 'content/schema0/table186/lob7/rec1520.bin'
        original = blob.read_bytes()
        result = execute_rule(self.root, self.rule, only_ref='rec1520.bin', dry_run=False)
        self.assertEqual(1, result['formula_fallback_attempted'])
        self.assertEqual(1, result['formula_fallback_success'])
        self.assertEqual(1, result['decrypted_saved'])
        self.assertEqual(0, result['decrypt_failed'])
        self.assertNotEqual(original, blob.read_bytes())
        self.assertEqual(CFB + b'PLAIN-TEST-ONLY', blob.read_bytes())
        self.assertEqual([], list(Path(self.temp.name).rglob('*.tmp')))
        xml = (self.root / 'content/schema0/table186/table186.xml').read_text()
        self.assertIn('file="rec1520.bin"', xml.replace("'", '"'))
        self.assertIn('digestType="MD5"', xml)
        self.assertIn('digest="' + hashlib.md5(blob.read_bytes()).hexdigest().upper() + '"', xml)

    def test_two_rejected_passwords_count_as_one_document_failure(self):
        FakeOfficeFile.force_reject_for = "rec1520.bin"
        result = execute_rule(self.root, self.rule, only_ref="rec1520.bin")
        self.assertEqual(1, result["selected"])
        self.assertEqual(1, result["formula_fallback_attempted"])
        self.assertEqual(0, result["formula_fallback_success"])
        self.assertEqual(1, result["decrypt_failed"])
        self.assertEqual(
            [("rec1520.bin", "DEMO_OVERRIDE"), ("rec1520.bin", "R2111")],
            FakeOfficeFile.attempts,
        )

    def test_original_json_policy_does_not_retry_rejected_password(self):
        old_rule = json.loads(json.dumps(self.rule))
        old_rule["formula"]["use"] = "fallback_if_stored_empty"
        result = execute_rule(self.root, old_rule, only_ref="rec1520.bin")
        self.assertEqual(1, result["formula_mismatch"])
        self.assertEqual(0, result["decrypted_tested"])
        self.assertEqual(0, result["formula_fallback_attempted"])
        self.assertEqual(1, result["decrypt_failed"])
        self.assertEqual([("rec1520.bin", "DEMO_OVERRIDE")], FakeOfficeFile.attempts)

    def test_non_key_errors_must_not_trigger_formula_retry(self):
        FakeOfficeFile.non_key_error_for = "rec1520.bin"
        result = execute_rule(self.root, self.rule, only_ref="rec1520.bin")
        self.assertEqual(1, result["decrypt_failed"])
        self.assertEqual(0, result["formula_fallback_attempted"])
        self.assertEqual([("rec1520.bin", "DEMO_OVERRIDE")], FakeOfficeFile.attempts)

    def test_same_stored_and_calculated_password_not_retried(self):
        FakeOfficeFile.non_key_error_for = "rec10.bin"
        result = execute_rule(self.root, self.rule, only_ref="rec10.bin")
        self.assertEqual(1, result["decrypt_failed"])
        self.assertEqual(0, result["formula_fallback_attempted"])
        self.assertEqual([("rec10.bin", "R4321")], FakeOfficeFile.attempts)

    def test_fallback_when_db_empty(self):
        results = execute_rule(self.root, self.rule, only_ref="rec55.bin")
        self.assertEqual(1, results["decrypted_tested"])
        self.assertEqual(1, results["password_formula_only"])

    def test_seq2_omitted_and_pdf_skipped(self):
        results = execute_rule(self.root, self.rule, max_files=0)
        self.assertEqual(5, results["selected"])
        self.assertEqual(1, results["pdf_skipped"])
        self.assertEqual(3, results["decrypted_tested"])
        self.assertEqual(1, results["decrypt_failed"])
        self.assertEqual(0, results["formula_mismatch"] - 1)

    def test_live_replaces_lob_and_updates_size_and_digest(self):
        blob = self.root / 'content/schema0/table186/lob7/rec10.bin'
        original = blob.read_bytes()
        table_xml = self.root / 'content/schema0/table186/table186.xml'
        source_xml = table_xml.read_bytes()
        self.assertIn(b"length='100'", source_xml)
        result = execute_rule(self.root, self.rule, only_ref='rec10.bin', dry_run=False)
        self.assertEqual(1, result['decrypted_saved'])
        self.assertEqual(0, result['decrypt_failed'])
        self.assertEqual(CFB + b'PLAIN-TEST-ONLY', blob.read_bytes())
        self.assertNotEqual(original, blob.read_bytes())
        doc = ET.parse(table_xml).getroot()
        rows = list(doc)
        cell = rows[0].find('c7')
        self.assertEqual('rec10.bin', cell.get('file'))
        self.assertEqual(str(len(blob.read_bytes())), cell.get('length'))
        self.assertEqual('MD5', cell.get('digestType'))
        self.assertEqual(hashlib.md5(blob.read_bytes()).hexdigest().upper(), cell.get('digest'))
        self.assertEqual(str(len(blob.read_bytes())), rows[0].find('c6').text)
        self.assertEqual('100', rows[1].find('c6').text)
        # Re-running after a successful in-place update does not decrypt again.
        second = execute_rule(self.root, self.rule, only_ref='rec10.bin', dry_run=False)
        self.assertEqual(1, second['not_encrypted'])
        self.assertEqual(0, second['decrypted_saved'])

    def test_repacked_archive_contains_decrypted_lob_and_consistent_xml(self):
        # Synthetic ZIP round-trip of the same files used by the SIARD repacker.
        original = Path(self.temp.name) / 'original.siard'
        with zipfile.ZipFile(original, 'w', zipfile.ZIP_DEFLATED) as z:
            for p in self.root.rglob('*'):
                if p.is_file():
                    z.write(p, p.relative_to(self.root).as_posix())
        with zipfile.ZipFile(original) as z:
            encrypted = z.read('content/schema0/table186/lob7/rec10.bin')
        execute_rule(self.root, self.rule, only_ref='rec10.bin', dry_run=False)
        repacked = Path(self.temp.name) / 'result.siard'
        with zipfile.ZipFile(repacked, 'w', zipfile.ZIP_DEFLATED) as z:
            for p in self.root.rglob('*'):
                if p.is_file():
                    z.write(p, p.relative_to(self.root).as_posix())
        with zipfile.ZipFile(repacked) as z:
            decrypted = z.read('content/schema0/table186/lob7/rec10.bin')
            xml = ET.fromstring(z.read('content/schema0/table186/table186.xml'))
        self.assertNotEqual(encrypted, decrypted)
        self.assertEqual(CFB + b'PLAIN-TEST-ONLY', decrypted)
        self.assertEqual(str(len(decrypted)), list(xml)[0].find('c7').get('length'))
        self.assertEqual('MD5', list(xml)[0].find('c7').get('digestType'))
        with zipfile.ZipFile(original) as z:
            self.assertEqual(encrypted, z.read('content/schema0/table186/lob7/rec10.bin'))

    def test_batch_commits_valid_and_keeps_failed_documents(self):
        successful = self.root / 'content/schema0/table186/lob7/rec10.bin'
        failed = self.root / 'content/schema0/table186/lob7/rec90.bin'
        original_successful = successful.read_bytes()
        original_failed = failed.read_bytes()
        table_xml = self.root / 'content/schema0/table186/table186.xml'
        result = execute_rule(self.root, self.rule, max_files=0, dry_run=False)
        self.assertEqual(1, result['decrypt_failed'])
        self.assertEqual(3, result['decrypted_saved'])
        self.assertEqual(5, result['selected'])
        self.assertNotEqual(original_successful, successful.read_bytes())
        self.assertEqual(original_failed, failed.read_bytes())
        doc = ET.parse(table_xml).getroot()
        rows = list(doc)
        self.assertEqual(str(len(successful.read_bytes())), rows[0].find('c7').get('length'))
        self.assertEqual('100', rows[4].find('c7').get('length'))

    def test_report_first_twelve_fields_include_outcomes(self):
        from siard_workflow.core.document_password_rules import _new_stats
        keys = list(_new_stats())[:12]
        for name in ('decrypted_saved', 'decrypted_tested', 'decrypt_failed',
                     'selected', 'formula_mismatch', 'formula_fallback_success'):
            self.assertIn(name, keys)

    def test_per_document_audit_matches_committed_batch(self):
        records = []
        result = execute_rule(
            self.root, self.rule, max_files=0, dry_run=False,
            record_sink=records.extend,
        )
        self.assertEqual(result['selected'], len(records))
        self.assertEqual(result['decrypted_saved'],
                         sum(1 for row in records if row['status'] == 'dekryptert'))
        self.assertEqual(result['decrypt_failed'],
                         sum(1 for row in records if row['status'] == 'feil'))
        self.assertEqual(result['pdf_skipped'],
                         sum(1 for row in records if row['kategori'] == 'PDF'))
        paths = [row['fil'] for row in records]
        self.assertTrue(all(path.startswith('content/') for path in paths))
        by_name = {Path(row['fil']).name: row for row in records}
        self.assertEqual('beregnet (fallback)', by_name['rec1520.bin']['passordkilde'])
        self.assertEqual('ja', by_name['rec1520.bin']['formelavvik'])
        self.assertEqual('R4321' in repr(records), False)
        self.assertNotIn('DEMO_OVERRIDE', repr(records))

    def test_per_document_audit_dry_run_reports_tested_not_written(self):
        records = []
        blob = self.root / 'content/schema0/table186/lob7/rec10.bin'
        before = blob.read_bytes()
        result = execute_rule(self.root, self.rule, only_ref='rec10.bin',
                              dry_run=True, record_sink=records.extend)
        self.assertEqual(1, result['decrypted_tested'])
        self.assertEqual('testet', records[0]['status'])
        self.assertEqual(before, blob.read_bytes())

    def test_commit_xml_failure_rolls_back_lob(self):
        blob = self.root / 'content/schema0/table186/lob7/rec10.bin'
        table_xml = self.root / 'content/schema0/table186/table186.xml'
        original_blob, original_xml = blob.read_bytes(), table_xml.read_bytes()
        real_replace = os.replace
        failure_injections = []
        def fail_xml_commit(src, dst):
            # execute_rule() normalizes the workspace with Path.resolve().
            # On Windows the resulting destination may be canonically spelled
            # differently from the test's original path, even when identical.
            # Match the unique staged XML file and the destination filename.
            if (Path(src).name == 'table-updated.xml'
                    and Path(dst).name == table_xml.name):
                failure_injections.append(Path(dst).name)
                raise OSError('Simulated XML commit failure')
            return real_replace(src, dst)
        with patch('siard_workflow.core.document_password_rules.os.replace', side_effect=fail_xml_commit):
            with self.assertRaisesRegex(OSError, 'Simulated XML commit failure'):
                execute_rule(self.root, self.rule, only_ref='rec10.bin', dry_run=False)
        self.assertEqual(['table186.xml'], failure_injections)
        self.assertEqual(original_blob, blob.read_bytes())
        self.assertEqual(original_xml, table_xml.read_bytes())

    def test_dry_run_does_not_change_lob_or_xml(self):
        blob = self.root / 'content/schema0/table186/lob7/rec1520.bin'
        table_xml = self.root / 'content/schema0/table186/table186.xml'
        before_blob, before_xml = blob.read_bytes(), table_xml.read_bytes()
        result = execute_rule(self.root, self.rule, only_ref='rec1520.bin', dry_run=True)
        self.assertEqual(1, result['formula_fallback_success'])
        self.assertEqual(1, result['decrypted_tested'])
        self.assertEqual(before_blob, blob.read_bytes())
        self.assertEqual(before_xml, table_xml.read_bytes())

    def test_reject_missing_reference(self):
        with self.assertRaises(RuleError):
            execute_rule(self.root, self.rule, only_ref="not_here.bin")

    def test_reject_unsafe_lob_path(self):
        table = read_metadata_tables(self.root)["nef_elfiler"]
        with self.assertRaises(RuleError):
            resolve_lob(self.root, table, table.column_index("Nef_ElFil"), "../../private/secret.bin")

    def test_bad_rules_not_evaluated(self):
        invalid = json.loads(RULE.read_text(encoding="utf-8"))
        invalid["formula"]["operators"].append({"eval": "print('bad')"})
        path = Path(self.temp.name) / "bad.json"
        path.write_text(json.dumps(invalid), encoding="utf-8")
        with self.assertRaises(RuleError):
            read_rule(path)

    def test_original_json_policy_remains_valid(self):
        old_rule = json.loads(json.dumps(self.rule))
        old_rule["formula"]["use"] = "fallback_if_stored_empty"
        path = Path(self.temp.name) / "old_rule.json"
        path.write_text(json.dumps(old_rule), encoding="utf-8")
        self.assertEqual("fallback_if_stored_empty", read_rule(path)["formula"]["use"])

    def test_unknown_fallback_policy_rejected(self):
        invalid = json.loads(json.dumps(self.rule))
        invalid["formula"]["use"] = "guess_all_passwords"
        path = Path(self.temp.name) / "invalid_rule.json"
        path.write_text(json.dumps(invalid), encoding="utf-8")
        with self.assertRaises(RuleError):
            read_rule(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
