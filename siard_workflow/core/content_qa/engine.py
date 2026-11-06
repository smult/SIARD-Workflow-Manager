"""Generic, read-only SIARD QA engine.

- Catalog/row counts are independent of any application.
- Domain carriers and links are declared by a versioned JSON profile.
- All rows are streamed. Unique identifiers are HMAC fingerprints kept in a
  temporary SQLite database and removed at the end of the run.
- No raw archival record values are persisted in findings or reports.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, date
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from typing import Callable

from .profile import validate_profile
from .source import SiardSource, Table
from .discovery import discover_catalog


class QAStopped(InterruptedError):
    pass


def _fingerprint(secret: bytes, values: tuple[str, ...]) -> bytes:
    # HMAC with an ephemeral per-run key: IDs cannot be correlated across runs
    # and the on-disk SQLite work database contains no plaintext identifiers.
    buf = json.dumps(values, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return hmac.new(secret, buf, hashlib.sha256).digest()


def _columns_ok(names: list[str] | tuple[str, ...], table: Table) -> bool:
    existing = {c.name for c in table.columns}
    return all(name in existing for name in names)


def _add_distribution(counter: Counter, value: str | None, max_distinct: int = 40) -> None:
    # Bounded memory, and prevent accidentally exporting free-text/person data
    # if an inappropriate field was mapped as a status column.
    name = value if value is not None else '(tom)'
    if len(name) > 100:
        name = '(lang tekst - verdi skjult)'
    hidden = '(mange forskjellige verdier - fordeling skjult)'
    if hidden in counter:
        counter[hidden] += 1
    elif name in counter or len(counter) < max_distinct:
        counter[name] += 1
    else:
        total = sum(counter.values()) + 1
        counter.clear()
        counter[hidden] = total


def _limit_distribution(values: Counter) -> dict[str, int]:
    return dict(sorted(values.items(), key=lambda x: (-x[1], x[0])))


def _classify_date(text: str) -> str:
    value = text[:10]
    try:
        if len(value) == 4 and value.isdigit():
            year = int(value)
            if 1600 <= year <= 2100:
                return value
        if len(value) == 10:
            year = date.fromisoformat(value).year
            if 1600 <= year <= 2100:
                return str(year)
    except ValueError:
        pass
    return '(ugyldig/ukjent dato)'


def run_content_qa(
    source_path: str | Path,
    profile: dict,
    *,
    scan_rows: bool = True,
    check_metadata_pk: bool = True,
    on_progress: Callable[[str], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
    temp_dir: str | Path | None = None,
) -> dict:
    """Return plain-JSON QA result without changing the SIARD source.

    Status: GODKJENT only if all configured carriers and controls were really
    verified; AVVIK for actual integrity violations; DELVIS otherwise.
    """
    profile = validate_profile(profile)
    source_path = Path(source_path)
    progress = on_progress or (lambda _msg: None)
    stop = should_stop or (lambda: False)
    secret = os.urandom(32)
    findings: list[dict] = []
    catalog: list[dict] = []
    carriers: list[dict] = []
    relations: list[dict] = []

    def finding(code: str, scope: str, status: str, count: int, detail: str, severity='info'):
        findings.append({
            'check_id': code, 'scope': scope, 'status': status,
            'count': count, 'severity': severity, 'detail': detail,
        })

    with SiardSource(source_path) as source:
        tables = source.catalog()
        progress(f'SIARD-katalog lest: {len(tables)} tabeller')
        discovery = discover_catalog(tables, profile)
        progress(f'Kartleggingsforslag: '
                 f'{discovery["summary"]["carriers_with_candidates"]}/'
                 f'{discovery["summary"]["carriers"]} informasjonsbaerere med kandidater; '
                 f'{discovery["summary"]["declared_foreign_keys"]} deklarerte fremmednokler')
        table_map = {t.key: t for t in tables}
        by_table: dict[tuple[str, str], list[dict]] = defaultdict(list)
        carriers_by_id = {c['id']: c for c in profile['carriers']}
        carrier_map = {}
        missing_carriers = 0
        for c in profile['carriers']:
            entity: dict = {'id': c['id'], 'label': c['label'], 'table': '',
                            'status': 'IKKE_KONTROLLERT', 'rows': None,
                            'unique': None, 'missing_key': None, 'duplicate_key': None,
                            'status_distribution': {}, 'year_distribution': {},
                            'notes': ''}
            carriers.append(entity)
            carrier_map[c['id']] = entity
            mapping = c.get('source')
            if mapping is None:
                missing_carriers += 1
                entity['notes'] = 'Ingen fysisk tabell kartlagt i QA-profilen'
                finding('carrier_unmapped', c['id'], 'IKKE_KONTROLLERT', 0, entity['notes'])
                continue
            key = (mapping['schema'], mapping['table'])
            entity['table'] = '.'.join(key)
            table = table_map.get(key)
            if table is None:
                missing_carriers += 1
                entity['notes'] = 'Tabellen finnes ikke i SIARD-metadata'
                finding('carrier_table_missing', c['id'], 'IKKE_KONTROLLERT', 0, entity['notes'], 'warning')
                continue
            relevant = list(c.get('key') or [])
            relevant += [c[f] for f in ('status_column', 'date_column') if c.get(f)]
            if not _columns_ok(relevant, table):
                missing_carriers += 1
                entity['notes'] = 'Én eller flere kartlagte kolonner finnes ikke i SIARD-tabellen'
                finding('carrier_column_missing', c['id'], 'IKKE_KONTROLLERT', 0, entity['notes'], 'warning')
                continue
            if not scan_rows:
                missing_carriers += 1
                entity['notes'] = 'Rader ble ikke lest (scan_rows=false)'
                finding('carrier_not_scanned', c['id'], 'IKKE_KONTROLLERT', 0, entity['notes'])
                continue
            by_table[key].append(c)

        relation_by_child: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for rel in profile.get('relations', []):
            report = {'id': rel['id'], 'child': rel['child'], 'parent': rel['parent'],
                      'status': 'IKKE_KONTROLLERT', 'references': None,
                      'orphans': None, 'missing_reference': None, 'notes': ''}
            relations.append(report)
            parent = carriers_by_id[rel['parent']]
            child = carriers_by_id[rel['child']]
            pc = list(parent.get('key') or [])
            cc = list(rel['child_columns'])
            parent_src = parent.get('source')
            child_src = child.get('source')
            if not parent_src or not child_src or not pc or len(pc) != len(cc):
                report['notes'] = 'Kartlegging eller nøkkeldefinisjon mangler'
            else:
                pt = table_map.get((parent_src['schema'], parent_src['table']))
                ct = table_map.get((child_src['schema'], child_src['table']))
                if not pt or not ct or not _columns_ok(pc, pt) or not _columns_ok(cc, ct):
                    report['notes'] = 'Relasjon peker til ukjent tabell/kolonne'
                elif (parent['id'] not in [m['id'] for m in by_table.get(pt.key, [])] or
                      child['id'] not in [m['id'] for m in by_table.get(ct.key, [])]):
                    report['notes'] = 'Relasjon krever lesing av begge informasjonsbærere'
                else:
                    relation_by_child[ct.key].append(rel)
                    report['status'] = 'VENTER'
            if report['status'] == 'IKKE_KONTROLLERT':
                finding('relation_unverified', rel['id'], 'IKKE_KONTROLLERT', 0, report['notes'])

        td = Path(temp_dir) if temp_dir else None
        if td is not None and not td.is_dir():
            td = None
        with tempfile.TemporaryDirectory(prefix='siard_content_qa_', dir=td) as tmp:
            db = sqlite3.connect(str(Path(tmp) / 'qa_index.sqlite3'))
            try:
                db.execute('PRAGMA journal_mode=OFF')
                db.execute('PRAGMA synchronous=OFF')
                db.execute('CREATE TABLE unique_keys (entity TEXT NOT NULL, key_hash BLOB NOT NULL, '
                           'PRIMARY KEY(entity, key_hash)) WITHOUT ROWID')
                db.execute('CREATE TABLE references_ (relation TEXT NOT NULL, key_hash BLOB NOT NULL)')
                tracked_relations = {r['id']: r for r in relations}
                stats = {c['id']: {'total': 0, 'unique': 0, 'null': 0, 'duplicates': 0,
                                   'status': Counter(), 'year': Counter()}
                         for c in profile['carriers']}
                rel_counts = {r['id']: {'references': 0, 'missing': 0} for r in profile.get('relations', [])}
                for t in tables:
                    if stop():
                        raise QAStopped('Innholdskontrollen ble avbrutt av operatøren')
                    t_report = {
                        'schema': t.schema, 'table': t.name,
                        'description': t.description,
                        'columns': [dict(name=c.name, sql_type=c.sql_type,
                                         nullable=c.nullable, description=c.description)
                                    for c in t.columns],
                        'primary_key': list(t.primary_key),
                        'candidate_keys': [{'name': ck.name, 'columns': list(ck.columns)}
                                           for ck in t.candidate_keys],
                        'foreign_key_count': len(t.foreign_keys),
                        'metadata_rows': t.declared_rows,
                        'actual_rows': None, 'status': 'IKKE_KONTROLLERT',
                        'pk_status': 'IKKE_KONTROLLERT' if t.primary_key else 'IKKE_DEFINERT',
                        'pk_missing': None, 'pk_duplicates': None,
                    }
                    catalog.append(t_report)
                    if not scan_rows:
                        continue
                    progress(f'Kontrollerer {t.ident}')
                    actual = 0
                    pk_null = 0
                    pk_dupe = 0
                    pk_ident = 'metadata::' + t.ident
                    pk_cols = list(t.primary_key)
                    can_check_pk = (check_metadata_pk and pk_cols and _columns_ok(pk_cols, t))
                    if check_metadata_pk and pk_cols and not can_check_pk:
                        finding('metadata_pk_invalid', t.ident, 'AVVIK', 1,
                                'Deklarert primærnøkkel viser til kolonne som mangler', 'error')
                        t_report['pk_status'] = 'AVVIK'
                    try:
                        for row_n, row in source.rows(t):
                            actual += 1
                            if actual % 1000 == 0 and stop():
                                raise QAStopped('Innholdskontrollen ble avbrutt av operatøren')
                            if can_check_pk:
                                pk_values = tuple(row.get(col) for col in pk_cols)
                                if any(v is None for v in pk_values):
                                    pk_null += 1
                                else:
                                    cur = db.execute(
                                        'INSERT OR IGNORE INTO unique_keys(entity,key_hash) VALUES (?,?)',
                                        (pk_ident, _fingerprint(secret, pk_values)))
                                    if not cur.rowcount:
                                        pk_dupe += 1
                            for c in by_table.get(t.key, []):
                                st = stats[c['id']]
                                st['total'] += 1
                                keys = list(c.get('key') or [])
                                if keys:
                                    raw_key = tuple(row.get(name) for name in keys)
                                    if any(v is None for v in raw_key):
                                        st['null'] += 1
                                    else:
                                        fp = _fingerprint(secret, raw_key)
                                        cursor = db.execute(
                                            'INSERT OR IGNORE INTO unique_keys(entity,key_hash) VALUES(?, ?)',
                                            (c['id'], fp))
                                        if cursor.rowcount:
                                            st['unique'] += 1
                                        else:
                                            st['duplicates'] += 1
                                if c.get('status_column'):
                                    s = row.get(c['status_column'])
                                    _add_distribution(st['status'], s)
                                if c.get('date_column'):
                                    dt = row.get(c['date_column'])
                                    _add_distribution(st['year'], _classify_date(dt) if dt else '(tom)')
                            for rel in relation_by_child.get(t.key, []):
                                values = tuple(row.get(name) for name in rel['child_columns'])
                                st = rel_counts[rel['id']]
                                if any(v is None for v in values):
                                    if rel.get('required', False):
                                        st['missing'] += 1
                                else:
                                    db.execute('INSERT INTO references_(relation,key_hash) VALUES(?,?)',
                                               (rel['id'], _fingerprint(secret, values)))
                                    st['references'] += 1
                    except QAStopped:
                        raise
                    except (OSError, ValueError, EOFError, ET.ParseError, zipfile.BadZipFile) as exc:
                        # The table is NOT verified if its XML is malformed or
                        # absent. Do not report partially counted rows as valid.
                        t_report['status'] = 'AVVIK'
                        t_report['note'] = f'Tabell-XML kunne ikke leses: {type(exc).__name__}'
                        finding('table_unreadable', t.ident, 'AVVIK', 1,
                                t_report['note'], 'error')
                        continue
                    t_report['actual_rows'] = actual
                    if can_check_pk:
                        t_report['pk_missing'] = pk_null
                        t_report['pk_duplicates'] = pk_dupe
                        t_report['pk_status'] = 'AVVIK' if pk_null or pk_dupe else 'OK'
                        if pk_null:
                            finding('metadata_pk_null', t.ident, 'AVVIK', pk_null,
                                    'Tom verdi i deklarert primærnøkkel', 'error')
                        if pk_dupe:
                            finding('metadata_pk_duplicate', t.ident, 'AVVIK', pk_dupe,
                                    'Duplikat av deklarert primærnøkkel', 'error')
                    t_report['status'] = ('OK' if t.declared_rows == actual
                                          else 'IKKE_KONTROLLERT' if t.declared_rows is None else 'AVVIK')
                    if t_report['pk_status'] == 'AVVIK':
                        t_report['status'] = 'AVVIK'
                    if t.declared_rows is None:
                        finding('table_count_not_declared', t.ident, 'IKKE_KONTROLLERT', 0,
                                'Antall rader er ikke oppgitt i metadata.xml')
                    elif t.declared_rows != actual:
                        finding('table_count_mismatch', t.ident, 'AVVIK',
                                abs(t.declared_rows - actual),
                                f'Metadata: {t.declared_rows}; faktisk: {actual}', 'error')

                db.commit()
                # Unique and status checks only on completely scanned tables.
                verified_tables = {(t['schema'], t['table']) for t in catalog
                                   if t['actual_rows'] is not None}
                for c in profile['carriers']:
                    entity = carrier_map[c['id']]
                    mapping = c.get('source')
                    if not mapping or (mapping['schema'], mapping['table']) not in verified_tables:
                        if not entity['notes']:
                            entity['notes'] = 'Tabell-XML ble ikke kontrollert'
                            finding('carrier_scan_incomplete', c['id'], 'IKKE_KONTROLLERT', 0,
                                    entity['notes'])
                        continue
                    if c not in by_table[(mapping['schema'], mapping['table'])]:
                        continue
                    st = stats[c['id']]
                    entity['rows'] = st['total']
                    entity['status'] = 'OK'
                    if c.get('key'):
                        entity['unique'] = st['unique']
                        entity['missing_key'] = st['null']
                        entity['duplicate_key'] = st['duplicates']
                        for typ, n in (('missing_key', st['null']), ('duplicate_key', st['duplicates'])):
                            if n:
                                finding(typ, c['id'], 'AVVIK', n,
                                        'Manglende nøkkelverdi' if typ == 'missing_key'
                                        else 'Gjentatt verdi for definert nøkkel', 'error')
                                entity['status'] = 'AVVIK'
                    else:
                        entity['notes'] = 'Antall rader telt, men unike informasjonsbærere ikke verifisert (key mangler)'
                        entity['status'] = 'IKKE_KONTROLLERT'
                        finding('carrier_key_unmapped', c['id'], 'IKKE_KONTROLLERT', 0,
                                entity['notes'])
                    entity['status_distribution'] = _limit_distribution(st['status'])
                    entity['year_distribution'] = _limit_distribution(st['year'])
                    expected = c.get('expected_count')
                    if expected:
                        # Compare unique carriers where a key is declared.
                        count = entity['unique'] if c.get('key') else entity['rows']
                        if count is not None and any((
                            count != expected['exact'] if 'exact' in expected else False,
                            count < expected['min'] if 'min' in expected else False,
                            count > expected['max'] if 'max' in expected else False,
                        )):
                            finding('expected_count', c['id'], 'AVVIK', abs(count - expected.get('exact', count)),
                                    f'Tellingsavvik mot forventning: {expected}, observert: {count}', 'error')
                            entity['status'] = 'AVVIK'
                for rel in profile.get('relations', []):
                    report = tracked_relations[rel['id']]
                    if report['status'] != 'VENTER':
                        continue
                    parent = carrier_map[rel['parent']]
                    child = carrier_map[rel['child']]
                    if parent['rows'] is None or child['rows'] is None:
                        report['status'] = 'IKKE_KONTROLLERT'
                        report['notes'] = 'En av tabellene mangler gyldig dataskann'
                        finding('relation_scan_incomplete', rel['id'], 'IKKE_KONTROLLERT', 0,
                                report['notes'])
                        continue
                    # SQL is exclusively on ephemeral keyed fingerprints; none
                    # of the input database values enters a SQL expression.
                    bad = db.execute(
                        'SELECT count(*) FROM references_ r WHERE r.relation=? '
                        'AND NOT EXISTS (SELECT 1 FROM unique_keys k '
                        'WHERE k.entity=? AND k.key_hash=r.key_hash)',
                        (rel['id'], rel['parent'])).fetchone()[0]
                    st = rel_counts[rel['id']]
                    report['references'] = st['references']
                    report['orphans'] = bad
                    report['missing_reference'] = st['missing']
                    report['status'] = 'AVVIK' if bad or st['missing'] else 'OK'
                    if bad:
                        finding('relation_orphan', rel['id'], 'AVVIK', bad,
                                'Referanse uten tilhørende informasjonsbærer', 'error')
                    if st['missing']:
                        finding('relation_required_missing', rel['id'], 'AVVIK', st['missing'],
                                'Obligatorisk fremmednøkkel mangler', 'error')
            finally:
                db.close()

    bad = sum(1 for x in findings if x['status'] == 'AVVIK')
    not_checked = sum(1 for x in catalog if x['status'] == 'IKKE_KONTROLLERT')
    not_checked += sum(1 for x in catalog if x['pk_status'] == 'IKKE_KONTROLLERT')
    not_checked += sum(1 for x in carriers if x['status'] == 'IKKE_KONTROLLERT')
    not_checked += sum(1 for x in relations if x['status'] == 'IKKE_KONTROLLERT')
    # No system-specific carrier mappings = discovery only, never full approval.
    if not profile['carriers']:
        not_checked += 1
        finding('domain_scope_missing', profile['id'], 'IKKE_KONTROLLERT', 0,
                'Ingen faglige informasjonsbærere definert i profilen')
    status = 'AVVIK' if bad or any(x['status'] == 'AVVIK' for x in catalog + carriers + relations) else (
        'DELVIS_KONTROLLERT' if not_checked else 'GODKJENT')
    result = {
        'format_version': 1,
        'created_at': datetime.now().astimezone().isoformat(timespec='seconds'),
        'source_name': source_path.name,
        'profile_id': profile['id'],
        'profile_label': profile['label'],
        'profile_sha256': hashlib.sha256(json.dumps(profile, ensure_ascii=False,
                                      sort_keys=True).encode('utf-8')).hexdigest(),
        'scan_rows': scan_rows, 'check_metadata_pk': check_metadata_pk, 'status': status,
        'summary': {
            'discovery_carriers_with_candidates': discovery['summary']['carriers_with_candidates'],
            'discovery_declared_foreign_keys': discovery['summary']['declared_foreign_keys'],
            'discovery_declared_foreign_keys_resolved': discovery['summary']['declared_foreign_keys_resolved'],
            'discovery_relation_proposals': discovery['summary']['name_based_relation_proposals'],
            'tables': len(catalog),
            'tables_verified': sum(1 for x in catalog if x['status'] == 'OK'),
            'tables_unverified': sum(1 for x in catalog if x['status'] == 'IKKE_KONTROLLERT'),
            'tables_with_errors': sum(1 for x in catalog if x['status'] == 'AVVIK'),
            'pk_defined': sum(1 for x in catalog if x['primary_key']),
            'pk_verified': sum(1 for x in catalog if x['pk_status'] == 'OK'),
            'pk_unverified': sum(1 for x in catalog if x['pk_status'] == 'IKKE_KONTROLLERT'),
            'pk_with_errors': sum(1 for x in catalog if x['pk_status'] == 'AVVIK'),
            'metadata_rows_total': sum(x['metadata_rows'] or 0 for x in catalog),
            'actual_rows_total': sum(x['actual_rows'] or 0 for x in catalog),
            'carriers': len(carriers),
            'carriers_verified': sum(1 for x in carriers if x['status'] == 'OK'),
            'carriers_unverified': sum(1 for x in carriers if x['status'] == 'IKKE_KONTROLLERT'),
            'carriers_with_errors': sum(1 for x in carriers if x['status'] == 'AVVIK'),
            'relations': len(relations),
            'relations_verified': sum(1 for x in relations if x['status'] == 'OK'),
            'relations_unverified': sum(1 for x in relations if x['status'] == 'IKKE_KONTROLLERT'),
            'relations_with_errors': sum(1 for x in relations if x['status'] == 'AVVIK'),
            'finding_groups': bad,
        },
        'catalog': catalog, 'carriers': carriers, 'relations': relations,
        'findings': findings, 'discovery': discovery,
    }
    progress(f'QA ferdig: {status}; {len(tables)} tabeller, '
             f'{result["summary"]["carriers_verified"]}/{len(carriers)} faglige informasjonsbærere kontrollert')
    return result
