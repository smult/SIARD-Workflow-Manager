"""Read-only content QA tests. No real-world/sensitive archive data is used."""
from __future__ import annotations

from copy import deepcopy
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

from siard_workflow.core.content_qa import (ProfileError, SiardSource,
                                            load_profile, run_content_qa, validate_profile)
from siard_workflow.core.content_qa.reporting import _csv_safe, write_qa_reports


SCHEMA_NS = 'http://www.bar.admin.ch/xmlns/siard/2/metadata.xsd'
TABLE_NS = 'http://www.bar.admin.ch/xmlns/siard/2/table.xsd'


def example_profile():
    return {
        'format_version': 1,
        'id': 'test-arkiv',
        'label': 'Synthetic Noark model',
        'carriers': [
            {'id': 'sak', 'label': 'Saker',
             'source': {'schema': 'schema0', 'table': 'SAK'},
             'key': ['SAK_ID'], 'status_column': 'STATUS',
             'expected_count': {'exact': 2}},
            {'id': 'jp', 'label': 'Journalposter',
             'source': {'schema': 'schema0', 'table': 'JOURNALPOST'},
             'key': ['JP_ID'], 'date_column': 'JOURNAL_DATO'},
        ],
        'relations': [
            {'id': 'jp_sak', 'child': 'jp', 'parent': 'sak',
             'child_columns': ['SAK_ID'], 'required': True},
        ],
    }


def synthetic_siard(tmp: Path, *, orphan=False, duplicate=False, nullkey=False,
                    wrong_declared=False, missing_xml=False, alternate_folder_file=False,
                    error_xml=False, zip_it=True, large_count=0,
                    null_fk=False, nested_row=False) -> Path:
    """Create tiny SIARD with two related tables. Does not touch real archives."""
    root = tmp / 'extracted'
    (root / 'header').mkdir(parents=True, exist_ok=True)
    (root / 'content/schema0/table0').mkdir(parents=True, exist_ok=True)
    (root / 'content/schema0/table1').mkdir(parents=True, exist_ok=True)
    ET.register_namespace('', SCHEMA_NS)
    md = ET.Element(f'{{{SCHEMA_NS}}}siardArchive')
    schemas = ET.SubElement(md, f'{{{SCHEMA_NS}}}schemas')
    sch = ET.SubElement(schemas, f'{{{SCHEMA_NS}}}schema')
    ET.SubElement(sch, f'{{{SCHEMA_NS}}}name').text = 'schema0'
    ET.SubElement(sch, f'{{{SCHEMA_NS}}}folder').text = 'schema0'
    tables = ET.SubElement(sch, f'{{{SCHEMA_NS}}}tables')
    spec = [('SAK', 'table0', 2,
             ['SAK_ID', 'STATUS', 'OPPRETTET'], ['SAK_ID']),
            ('JOURNALPOST', 'table1', 2 if wrong_declared else 3 + large_count,
             ['JP_ID', 'SAK_ID', 'JOURNAL_DATO'], ['JP_ID'])]
    for name, folder, count, columns, keys in spec:
        t = ET.SubElement(tables, f'{{{SCHEMA_NS}}}table')
        ET.SubElement(t, f'{{{SCHEMA_NS}}}name').text = name
        ET.SubElement(t, f'{{{SCHEMA_NS}}}folder').text = folder
        ET.SubElement(t, f'{{{SCHEMA_NS}}}rows').text = str(count)
        cols = ET.SubElement(t, f'{{{SCHEMA_NS}}}columns')
        for cname in columns:
            c = ET.SubElement(cols, f'{{{SCHEMA_NS}}}column')
            ET.SubElement(c, f'{{{SCHEMA_NS}}}name').text = cname
            ET.SubElement(c, f'{{{SCHEMA_NS}}}type').text = 'VARCHAR(40)'
        pk = ET.SubElement(t, f'{{{SCHEMA_NS}}}primaryKey')
        for key in keys:
            ET.SubElement(pk, f'{{{SCHEMA_NS}}}column').text = key
    (root / 'header/metadata.xml').write_bytes(ET.tostring(md, encoding='utf-8', xml_declaration=True))
    ET.register_namespace('', TABLE_NS)
    sak = [('1', 'A', '2004-02-03'), ('2', 'U', '2005-02-03')]
    jp = [('10', '1', '2004-03-01'),
          ('10' if duplicate else '11', '2', '2006-04-04'),
          (None if nullkey else '12', None if null_fk else ('999' if orphan else '1'), '2007-05-05')]
    jp.extend((str(i + 20), '1', '2008-05-05') for i in range(large_count))
    for folder, records in (('table0', sak), ('table1', jp)):
        t = ET.Element(f'{{{TABLE_NS}}}table')
        for values in records:
            row = ET.SubElement(t, f'{{{TABLE_NS}}}row')
            for i, v in enumerate(values, 1):
                c = ET.SubElement(row, f'{{{TABLE_NS}}}c{i}')
                if v is not None:
                    c.text = v
                if nested_row and folder == 'table0' and i == 3:
                    ET.SubElement(c, f'{{{TABLE_NS}}}row').text = 'nested XML, not a real table row'
        fname = 'other.xml' if folder == 'table0' and alternate_folder_file else folder + '.xml'
        if not (missing_xml and folder == 'table1'):
            target = root / 'content/schema0' / folder / fname
            target.write_bytes(ET.tostring(t, encoding='utf-8', xml_declaration=True)
                               if not (error_xml and folder == 'table1') else b'<invalid')
    if not zip_it:
        return root
    zpath = tmp / 'sample.siard'
    with zipfile.ZipFile(zpath, 'w', zipfile.ZIP_DEFLATED) as zf:
        for f in root.rglob('*'):
            if f.is_file():
                zf.write(f, f.relative_to(root).as_posix())
    return zpath


class ProfileTests(unittest.TestCase):
    def test_default_template_has_unmapped_ephorte_concepts(self):
        p = load_profile()
        self.assertIn('ephorte', p['id'])
        self.assertGreaterEqual(len(p['carriers']), 8)
        self.assertTrue(all(c['source'] is None for c in p['carriers']))

    def test_wrong_format(self):
        p = example_profile()
        p['format_version'] = 0
        with self.assertRaises(ProfileError):
            validate_profile(p)

    def test_duplicate_carrier(self):
        p = example_profile()
        p['carriers'].append(deepcopy(p['carriers'][0]))
        with self.assertRaises(ProfileError):
            validate_profile(p)

    def test_unknown_relation(self):
        p = example_profile()
        p['relations'][0]['parent'] = 'not_exists'
        with self.assertRaises(ProfileError):
            validate_profile(p)

    def test_noninteger_expected_rejected(self):
        p = example_profile()
        p['carriers'][0]['expected_count']['exact'] = True
        with self.assertRaises(ProfileError):
            validate_profile(p)

    def test_unknown_rule_expression_is_rejected(self):
        p = example_profile()
        p['arbitrary_python'] = '__import__("os").system("false")'
        with self.assertRaises(ProfileError):
            validate_profile(p)

    def test_typo_in_status_field_name_rejected(self):
        p = example_profile()
        p['carriers'][0]['status_colunm'] = 'STATUS'
        with self.assertRaises(ProfileError):
            validate_profile(p)

    def test_true_is_not_supported_profile_version(self):
        p = example_profile()
        p['format_version'] = True
        with self.assertRaises(ProfileError):
            validate_profile(p)


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.root = Path(self.td.name)

    def test_good_zip(self):
        path = synthetic_siard(self.root)
        digest = hashlib.sha256(path.read_bytes()).digest()
        result = run_content_qa(path, example_profile())
        self.assertEqual(result['status'], 'GODKJENT')
        self.assertEqual(result['summary']['tables_verified'], 2)
        self.assertEqual(result['summary']['carriers_verified'], 2)
        self.assertEqual(result['summary']['relations_verified'], 1)
        self.assertEqual(result['summary']['actual_rows_total'], 5)
        self.assertEqual(result['carriers'][0]['status_distribution'], {'A': 1, 'U': 1})
        self.assertEqual(result['carriers'][1]['year_distribution'], {'2004': 1, '2006': 1, '2007': 1})
        self.assertEqual(hashlib.sha256(path.read_bytes()).digest(), digest)

    def test_good_directory(self):
        path = synthetic_siard(self.root, zip_it=False)
        result = run_content_qa(path, example_profile())
        self.assertEqual(result['status'], 'GODKJENT')

    def test_referenced_key_missing(self):
        result = run_content_qa(synthetic_siard(self.root, orphan=True), example_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertEqual(result['relations'][0]['orphans'], 1)
        self.assertTrue(any(f['check_id'] == 'relation_orphan' for f in result['findings']))

    def test_duplicate_unique_key(self):
        result = run_content_qa(synthetic_siard(self.root, duplicate=True), example_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertEqual(result['carriers'][1]['duplicate_key'], 1)
        self.assertEqual(result['carriers'][1]['unique'], 2)

    def test_missing_key(self):
        result = run_content_qa(synthetic_siard(self.root, nullkey=True), example_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertEqual(result['carriers'][1]['missing_key'], 1)

    def test_bad_metadata_row_count(self):
        result = run_content_qa(synthetic_siard(self.root, wrong_declared=True), example_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertTrue(any(f['check_id'] == 'table_count_mismatch' for f in result['findings']))

    def test_missing_table_xml(self):
        result = run_content_qa(synthetic_siard(self.root, missing_xml=True), example_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertIsNone(result['catalog'][1]['actual_rows'])
        self.assertEqual(result['carriers'][1]['status'], 'IKKE_KONTROLLERT')
        self.assertEqual(result['relations'][0]['status'], 'IKKE_KONTROLLERT')

    def test_broken_table_xml(self):
        result = run_content_qa(synthetic_siard(self.root, error_xml=True), example_profile())
        self.assertEqual(result['status'], 'AVVIK')

    def test_nonstandard_table_xml_name_is_not_silently_accepted(self):
        result = run_content_qa(synthetic_siard(self.root, alternate_folder_file=True), example_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertEqual(result['catalog'][0]['status'], 'AVVIK')

    def test_no_scan_must_never_claim_pass(self):
        result = run_content_qa(synthetic_siard(self.root), example_profile(), scan_rows=False)
        self.assertEqual(result['status'], 'DELVIS_KONTROLLERT')
        self.assertEqual(result['summary']['tables_verified'], 0)
        self.assertEqual(result['summary']['actual_rows_total'], 0)

    def test_template_without_mappings_never_approved(self):
        result = run_content_qa(synthetic_siard(self.root), load_profile())
        self.assertEqual(result['status'], 'DELVIS_KONTROLLERT')
        self.assertEqual(result['summary']['tables_verified'], 2)
        self.assertEqual(result['summary']['carriers_verified'], 0)

    def test_missing_physical_table_mapping_unverified(self):
        p = example_profile()
        p['carriers'][0]['source']['table'] = 'DOES_NOT_EXIST'
        result = run_content_qa(synthetic_siard(self.root), p)
        self.assertEqual(result['status'], 'DELVIS_KONTROLLERT')
        self.assertEqual(result['carriers'][0]['status'], 'IKKE_KONTROLLERT')

    def test_missing_column_mapping_unverified(self):
        p = example_profile()
        p['carriers'][0]['key'] = ['DOES_NOT_EXIST']
        result = run_content_qa(synthetic_siard(self.root), p)
        self.assertEqual(result['carriers'][0]['status'], 'IKKE_KONTROLLERT')
        self.assertEqual(result['status'], 'DELVIS_KONTROLLERT')

    def test_row_metadata_only_is_not_full_qa(self):
        p = {'format_version': 1, 'id': 'meta', 'label': 'Meta', 'carriers': []}
        result = run_content_qa(synthetic_siard(self.root), p)
        self.assertEqual(result['status'], 'DELVIS_KONTROLLERT')

    def test_expected_count_mismatch(self):
        p = example_profile()
        p['carriers'][0]['expected_count'] = {'exact': 5}
        result = run_content_qa(synthetic_siard(self.root), p)
        self.assertEqual(result['status'], 'AVVIK')
        self.assertTrue(any(f['check_id'] == 'expected_count' for f in result['findings']))

    def test_optional_null_relation(self):
        # Optional null is not an orphan, provided a key exists; this field
        # test needs a dedicated child FK null independently of the child PK.
        p = example_profile()
        p['relations'][0]['required'] = False
        result = run_content_qa(synthetic_siard(self.root), p)
        self.assertEqual(result['relations'][0]['missing_reference'], 0)

    def test_stop_does_not_create_success(self):
        from siard_workflow.core.content_qa.engine import QAStopped
        with self.assertRaises(QAStopped):
            run_content_qa(synthetic_siard(self.root), example_profile(), should_stop=lambda: True)

    def test_temp_index_deleted(self):
        siard = synthetic_siard(self.root)
        temp_root = self.root / 'temp'
        temp_root.mkdir()
        run_content_qa(siard, example_profile(), temp_dir=temp_root)
        self.assertEqual(list(temp_root.iterdir()), [])

    def test_no_data_values_in_json(self):
        result = run_content_qa(synthetic_siard(self.root), example_profile())
        dumped = json.dumps(result, ensure_ascii=False)
        self.assertNotIn('2004-03-01', dumped)
        self.assertNotIn('"1"', dumped)

    def test_zip_traversal_rejected(self):
        from siard_workflow.core.content_qa.source import _folder
        with self.assertRaises(ValueError):
            _folder('../outside')

    def test_synthetic_2000_records_streamed(self):
        path = synthetic_siard(self.root, large_count=2000)
        result = run_content_qa(path, example_profile())
        self.assertEqual(result['summary']['actual_rows_total'], 2005)
        self.assertEqual(result['catalog'][1]['actual_rows'], 2003)
        self.assertEqual(result['status'], 'GODKJENT')

    def test_metadata_primary_key_duplicate_even_unmapped(self):
        path = synthetic_siard(self.root, duplicate=True)
        result = run_content_qa(path, load_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertEqual(result['catalog'][1]['pk_duplicates'], 1)
        self.assertTrue(any(f['check_id'] == 'metadata_pk_duplicate' for f in result['findings']))

    def test_metadata_primary_key_check_opt_out(self):
        path = synthetic_siard(self.root, duplicate=True)
        result = run_content_qa(path, load_profile(), check_metadata_pk=False)
        self.assertEqual(result['status'], 'DELVIS_KONTROLLERT')
        self.assertFalse(any(f['check_id'] == 'metadata_pk_duplicate' for f in result['findings']))

    def test_metadata_primary_key_null(self):
        path = synthetic_siard(self.root, nullkey=True)
        result = run_content_qa(path, load_profile())
        self.assertEqual(result['catalog'][1]['pk_missing'], 1)
        self.assertEqual(result['status'], 'AVVIK')

    def test_null_required_relation(self):
        path = synthetic_siard(self.root, null_fk=True)
        result = run_content_qa(path, example_profile())
        self.assertEqual(result['status'], 'AVVIK')
        self.assertEqual(result['relations'][0]['missing_reference'], 1)

    def test_null_optional_relation(self):
        path = synthetic_siard(self.root, null_fk=True)
        p = example_profile()
        p['relations'][0]['required'] = False
        result = run_content_qa(path, p)
        self.assertEqual(result['status'], 'GODKJENT')
        self.assertEqual(result['relations'][0]['missing_reference'], 0)

    def test_nested_xml_row_does_not_inflate_count(self):
        path = synthetic_siard(self.root, nested_row=True)
        result = run_content_qa(path, example_profile())
        self.assertEqual(result['status'], 'GODKJENT')
        self.assertEqual(result['summary']['actual_rows_total'], 5)

    def test_many_status_values_are_hidden_and_bounded(self):
        from siard_workflow.core.content_qa.engine import _add_distribution, _limit_distribution
        from collections import Counter
        counter = Counter()
        for i in range(500):
            _add_distribution(counter, 'sensitive-value-' + str(i))
        self.assertEqual(_limit_distribution(counter),
                         {'(mange forskjellige verdier - fordeling skjult)': 500})

    def test_invalid_date_is_not_grouped_as_valid_year(self):
        from siard_workflow.core.content_qa.engine import _classify_date
        self.assertEqual(_classify_date('2006-99-99'), '(ugyldig/ukjent dato)')
        self.assertEqual(_classify_date('2006-01-01T00:00:00'), '2006')



class ReportTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.root = Path(self.td.name)

    def test_dotted_archive_stem_preserves_timestamp(self):
        siard = synthetic_siard(self.root)
        result = run_content_qa(siard, example_profile())
        paths = write_qa_reports(result, self.root / 'out', 'ephorte-v1.2')
        self.assertIn('ephorte-v1.2_', Path(paths['json']).name)
        self.assertTrue(Path(paths['json']).exists())
        self.assertTrue(Path(paths['pdf']).exists())

    def test_identical_second_reports_never_overwritten(self):
        siard = synthetic_siard(self.root)
        result = run_content_qa(siard, example_profile())
        a = write_qa_reports(result, self.root / 'out', 'same')
        first = Path(a['json']).read_bytes()
        b = write_qa_reports(result, self.root / 'out', 'same')
        self.assertNotEqual(a['json'], b['json'])
        self.assertEqual(first, Path(a['json']).read_bytes())

    def test_csv_formula_injection_protection(self):
        self.assertEqual(_csv_safe('=SUM(1,1)'), "'=SUM(1,1)")
        self.assertEqual(_csv_safe('+cmd'), "'+cmd")

    def test_written_reports_csv_and_pdf(self):
        siard = synthetic_siard(self.root)
        result = run_content_qa(siard, example_profile())
        paths = write_qa_reports(result, self.root / 'out', siard.stem)
        self.assertEqual(len(paths), 9)
        for p in paths.values():
            self.assertTrue(Path(p).is_file(), p)
            self.assertGreater(Path(p).stat().st_size, 0)
        with Path(paths['tables_csv']).open(encoding='utf-8-sig', newline='') as f:
            items = list(csv.DictReader(f, delimiter=';'))
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]['actual_rows'], '2')
        self.assertEqual(json.loads(Path(paths['json']).read_text('utf-8'))['status'], 'GODKJENT')
        self.assertTrue(Path(paths['pdf']).read_bytes().startswith(b'%PDF'))

    def test_error_and_partial_report_pdf(self):
        siard = synthetic_siard(self.root, orphan=True)
        result = run_content_qa(siard, example_profile())
        a = write_qa_reports(result, self.root / 'out', 'erroneous')
        self.assertTrue(Path(a['pdf']).is_file())
        partial = run_content_qa(siard, load_profile())
        b = write_qa_reports(partial, self.root / 'out', 'unmapped')
        self.assertTrue(Path(b['pdf']).is_file())


if __name__ == '__main__':
    unittest.main()
