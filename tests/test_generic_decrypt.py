"""Synthetic tests: no real archive documents or credentials are distributed."""
from __future__ import annotations

import json
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
    force_reject_for: str = ""
    reject_after_partial_write: bool = False

    def __init__(self, fh):
        self.handle = fh
        self.ref = fh.name
        self.password = None

    def is_encrypted(self):
        return self.handle.read(8) == CFB

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

    def test_fallback_after_failed_decrypt_clears_partial_output(self):
        FakeOfficeFile.reject_after_partial_write = True
        output = Path(self.temp.name) / "formula_output"
        result = execute_rule(
            self.root, self.rule, only_ref="rec1520.bin",
            dry_run=False, output_dir=output,
        )
        self.assertEqual(1, result["formula_fallback_attempted"])
        self.assertEqual(1, result["formula_fallback_success"])
        self.assertEqual(1, result["decrypted_saved"])
        self.assertEqual(0, result["decrypt_failed"])
        copied = list(output.glob("rec1520_*.doc"))
        self.assertEqual(1, len(copied))
        self.assertEqual(CFB + b"PLAIN-TEST-ONLY", copied[0].read_bytes())

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

    def test_live_writes_copies_not_source(self):
        output = Path(self.temp.name) / "unlocked"
        original = (self.root / "content/schema0/table186/lob7/rec10.bin").read_bytes()
        result = execute_rule(self.root, self.rule, only_ref="rec10.bin", dry_run=False, output_dir=output)
        self.assertEqual(1, result["decrypted_saved"])
        copied = list(output.glob("rec10_*.doc"))
        self.assertEqual(1, len(copied))
        self.assertEqual(CFB + b"PLAIN-TEST-ONLY", copied[0].read_bytes())
        self.assertEqual(original, (self.root / "content/schema0/table186/lob7/rec10.bin").read_bytes())
        again = execute_rule(self.root, self.rule, only_ref="rec10.bin", dry_run=False, output_dir=output)
        self.assertEqual(1, again["already_exists"])
        self.assertEqual(0, again["decrypted_saved"])

    def test_reject_output_inside_source(self):
        with self.assertRaises(RuleError):
            execute_rule(self.root, self.rule, only_ref="rec10.bin", dry_run=False, output_dir=self.root / "myfiles")

    def test_reject_output_inside_git_repo(self):
        target = Path(self.temp.name) / "repo"
        (target / ".git").mkdir(parents=True)
        with self.assertRaises(RuleError):
            execute_rule(self.root, self.rule, only_ref="rec10.bin", dry_run=False,
                         output_dir=target / "decrypted")

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
