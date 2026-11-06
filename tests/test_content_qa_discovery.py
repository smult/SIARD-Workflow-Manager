"""Metadata-driven mapping suggestions: synthetic archives only."""
from __future__ import annotations

from copy import deepcopy
import csv
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

from test_content_qa import SCHEMA_NS, synthetic_siard, example_profile
from siard_workflow.core.content_qa import SiardSource, load_profile, run_content_qa
from siard_workflow.core.content_qa.discovery import discover_catalog
from siard_workflow.core.content_qa.profile import ProfileError, validate_profile
from siard_workflow.core.content_qa.reporting import write_qa_reports


def with_declared_fk(root: Path, *, missing_target=False, missing_column=False,
                     zip_it=False) -> Path:
    directory = synthetic_siard(root, zip_it=False)
    metadata = directory / 'header' / 'metadata.xml'
    md = ET.parse(metadata)
    ns = {'s': SCHEMA_NS}
    tables = md.findall('.//s:table', ns)
    assert len(tables) == 2
    parent = tables[0]
    ck_list = ET.SubElement(parent, f'{{{SCHEMA_NS}}}candidateKeys')
    ck = ET.SubElement(ck_list, f'{{{SCHEMA_NS}}}candidateKey')
    ET.SubElement(ck, f'{{{SCHEMA_NS}}}name').text = 'SAK_UNIQUE'
    ET.SubElement(ck, f'{{{SCHEMA_NS}}}column').text = 'SAK_ID'
    child = tables[1]
    fks = ET.SubElement(child, f'{{{SCHEMA_NS}}}foreignKeys')
    fk = ET.SubElement(fks, f'{{{SCHEMA_NS}}}foreignKey')
    ET.SubElement(fk, f'{{{SCHEMA_NS}}}name').text = 'FK_JP_SAK'
    ET.SubElement(fk, f'{{{SCHEMA_NS}}}referencedSchema').text = 'schema0'
    ET.SubElement(fk, f'{{{SCHEMA_NS}}}referencedTable').text = (
        'NOT_EXISTS' if missing_target else 'SAK')
    ref = ET.SubElement(fk, f'{{{SCHEMA_NS}}}reference')
    ET.SubElement(ref, f'{{{SCHEMA_NS}}}column').text = (
        'NOT_A_COLUMN' if missing_column else 'SAK_ID')
    ET.SubElement(ref, f'{{{SCHEMA_NS}}}referenced').text = 'SAK_ID'
    ET.SubElement(fk, f'{{{SCHEMA_NS}}}matchType').text = 'SIMPLE'
    md.write(metadata, encoding='utf-8', xml_declaration=True)
    if not zip_it:
        return directory
    out = root / 'with_fk.siard'
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file in directory.rglob('*'):
            if file.is_file():
                zf.write(file, file.relative_to(directory).as_posix())
    return out


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)

    def test_source_reads_declared_keys(self):
        path = with_declared_fk(self.tmp)
        with SiardSource(path) as source:
            tables = source.catalog()
        self.assertEqual(len(tables[0].candidate_keys), 1)
        self.assertEqual(tables[0].candidate_keys[0].columns, ('SAK_ID',))
        self.assertEqual(len(tables[1].foreign_keys), 1)
        self.assertEqual(tables[1].foreign_keys[0].name, 'FK_JP_SAK')
        self.assertEqual(tables[1].foreign_keys[0].references, (('SAK_ID', 'SAK_ID'),))

    def test_default_ephorte_template_proposes_noark_concepts_without_mappings(self):
        source = synthetic_siard(self.tmp)
        with SiardSource(source) as stream:
            result = discover_catalog(stream.catalog(), load_profile())
        candidates = {entry['carrier_id']: entry for entry in result['carriers']}
        sak = candidates['mappe']['candidates'][0]
        jp = candidates['registrering']['candidates'][0]
        self.assertEqual(sak['table'], 'SAK')
        self.assertEqual(jp['table'], 'JOURNALPOST')
        self.assertEqual(sak['key']['columns'], ['SAK_ID'])
        self.assertEqual(sak['key']['basis'], 'DEKLARERT_PRIMAERNOEKKEL')
        self.assertTrue(all(entry['review_status'] == 'UBEKREFTET'
                            for entry in result['carriers']))
        self.assertTrue(all(c['source'] is None for c in load_profile()['carriers']))

    def test_declared_fk_is_separate_from_heuristics(self):
        with SiardSource(with_declared_fk(self.tmp, zip_it=True)) as source:
            discovery = discover_catalog(source.catalog(), load_profile())
        declared = discovery['declared_relations']
        self.assertEqual(len(declared), 1)
        self.assertEqual(declared[0]['status'], 'METADATA_LOEST')
        self.assertEqual(declared[0]['child_columns'], ['SAK_ID'])
        self.assertEqual(declared[0]['parent_columns'], ['SAK_ID'])
        self.assertFalse(any(r['child_table'] == 'JOURNALPOST' and
                             r['parent_table'] == 'SAK' and
                             r['child_columns'] == ['SAK_ID']
                             for r in discovery['relation_proposals']))

    def test_missing_fk_target_marked_review_no_fabrication(self):
        with SiardSource(with_declared_fk(self.tmp, missing_target=True)) as source:
            discovery = discover_catalog(source.catalog(), load_profile())
        self.assertEqual(discovery['declared_relations'][0]['status'], 'KREVER_GJENNOMGANG')
        self.assertIn('finnes ikke', discovery['declared_relations'][0]['note'])

    def test_invalid_fk_column_not_reported_resolved(self):
        with SiardSource(with_declared_fk(self.tmp, missing_column=True)) as source:
            discovery = discover_catalog(source.catalog(), load_profile())
        self.assertEqual(discovery['declared_relations'][0]['status'], 'KREVER_GJENNOMGANG')

    def test_name_based_fk_not_treated_as_validated(self):
        with SiardSource(synthetic_siard(self.tmp)) as source:
            discovery = discover_catalog(source.catalog(), load_profile())
        proposals = discovery['relation_proposals']
        self.assertTrue(any(r['child_table'] == 'JOURNALPOST' and
                            r['parent_table'] == 'SAK' and
                            r['child_columns'] == ['SAK_ID'] for r in proposals))
        self.assertTrue(all(r['origin'] == 'NAVNELIKHET_IKKE_VERIFISERT' and
                            r['status'] == 'FORSLAG' for r in proposals))

    def test_metadata_only_has_proposals_but_no_false_approval(self):
        result = run_content_qa(synthetic_siard(self.tmp), load_profile(), scan_rows=False)
        self.assertEqual(result['status'], 'DELVIS_KONTROLLERT')
        self.assertGreaterEqual(result['summary']['discovery_carriers_with_candidates'], 2)
        self.assertEqual(result['summary']['tables_verified'], 0)
        self.assertEqual(result['discovery']['review_required'], True)

    def test_existing_mapped_profile_remains_approved(self):
        result = run_content_qa(synthetic_siard(self.tmp), example_profile())
        self.assertEqual(result['status'], 'GODKJENT')
        self.assertTrue(result['discovery']['review_required'])
        self.assertEqual(result['summary']['carriers_verified'], 2)

    def test_alias_validation_rejects_strings_and_code(self):
        profile = example_profile()
        profile['carriers'][0]['aliases'] = '__import__("os").system("echo bad")'
        with self.assertRaises(ProfileError):
            validate_profile(profile)
        profile['carriers'][0]['aliases'] = ['SAK', 9]
        with self.assertRaises(ProfileError):
            validate_profile(profile)

    def test_custom_alias_can_find_nonliteral_physical_name(self):
        profile = example_profile()
        profile['carriers'][0]['aliases'] = ['SAK']
        with SiardSource(synthetic_siard(self.tmp)) as stream:
            mapping = discover_catalog(stream.catalog(), profile)
        self.assertEqual(mapping['carriers'][0]['candidates'][0]['table'], 'SAK')

    def test_does_not_export_archive_record_values_in_suggestions(self):
        result = run_content_qa(synthetic_siard(self.tmp), load_profile())
        encoded = json.dumps(result['discovery'], ensure_ascii=False)
        self.assertNotIn('2004-03-01', encoded)
        self.assertNotIn('2006-04-04', encoded)

    def test_short_journalpost_alias_exact_match(self):
        from siard_workflow.core.content_qa.source import Table, Column
        table = Table('schema0', 'JP', 'schema0', 'table0',
                      (Column('JP_ID'),), 9, primary_key=('JP_ID',))
        suggestions = discover_catalog([table], load_profile())
        journalpost = next(c for c in suggestions['carriers']
                           if c['carrier_id'] == 'registrering')
        self.assertEqual(journalpost['candidates'][0]['table'], 'JP')

    def test_candidate_key_as_proposed_key_when_pk_absent(self):
        from siard_workflow.core.content_qa.source import Table, Column, CandidateKey
        table = Table('schema0', 'SAK', 'schema0', 'table0',
                      (Column('SAK_ID'),), 9, primary_key=(),
                      candidate_keys=(CandidateKey('UNIQ_SAK', ('SAK_ID',)),))
        suggestions = discover_catalog([table], load_profile())
        sak = next(c for c in suggestions['carriers'] if c['carrier_id'] == 'mappe')
        self.assertEqual(sak['candidates'][0]['key']['columns'], ['SAK_ID'])
        self.assertEqual(sak['candidates'][0]['key']['basis'],
                         'DEKLARERT_KANDIDATNOEKKEL')

    def test_generic_id_is_not_inferred_as_fk(self):
        from siard_workflow.core.content_qa.source import Table, Column
        parent = Table('schema0', 'SAK', 'schema0', 'table0',
                       (Column('ID'),), 2, primary_key=('ID',))
        child = Table('schema0', 'JOURNALPOST', 'schema0', 'table1',
                      (Column('ID'), Column('JP_ID')), 3, primary_key=('JP_ID',))
        suggestions = discover_catalog([parent, child], load_profile())
        self.assertFalse(any(r['child_columns'] == ['ID'] for r in
                             suggestions['relation_proposals']))

    def test_reporting_all_discovery_files(self):
        result = run_content_qa(with_declared_fk(self.tmp), load_profile(), scan_rows=False)
        paths = write_qa_reports(result, self.tmp / 'reports', 'ephorte.qa')
        self.assertEqual(len(paths), 9)
        self.assertTrue(all(Path(path).is_file() for path in paths.values()))
        self.assertTrue(Path(paths['pdf']).read_bytes().startswith(b'%PDF'))
        self.assertIn('ephorte.qa_', Path(paths['mapping_csv']).name)
        with Path(paths['mapping_csv']).open(encoding='utf-8-sig', newline='') as fh:
            rows = list(csv.DictReader(fh, delimiter=';'))
        self.assertTrue(any(row['carrier_id'] == 'mappe' and row['table'] == 'SAK'
                            for row in rows))
        with Path(paths['foreign_keys_csv']).open(encoding='utf-8-sig', newline='') as fh:
            rows = list(csv.DictReader(fh, delimiter=';'))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['name'], 'FK_JP_SAK')
        self.assertTrue(json.loads(Path(paths['mapping_json']).read_text(encoding='utf-8'))
                        ['review_required'])


if __name__ == '__main__':
    unittest.main()
