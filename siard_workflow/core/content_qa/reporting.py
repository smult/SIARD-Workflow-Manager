"""CSV, JSON and PDF reports using the existing SIARD depot-report style.

Only aggregate statistics and metadata are exported. No record values, person
identifiers, full source paths or temporary index keys are written to reports.
"""
from __future__ import annotations

import csv
from datetime import datetime
import json
from pathlib import Path
from xml.sax.saxutils import escape


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open('w', encoding='utf-8-sig', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, delimiter=';',
                                extrasaction='ignore', lineterminator='\n')
        writer.writeheader()
        for row in rows:
            # Avoid Excel interpreting user-supplied metadata labels as formulas.
            writer.writerow({col: _csv_safe(row.get(col)) for col in columns})


def _csv_safe(v):
    if v is None:
        return ''
    if isinstance(v, (dict, list)):
        v = json.dumps(v, ensure_ascii=False, sort_keys=True)
    if isinstance(v, str) and v.lstrip().startswith(('=', '+', '-', '@')):
        return "'" + v
    return v


def write_qa_reports(result: dict, output_dir: str | Path, stem: str) -> dict[str, str]:
    """Persist QA results, then produce a PDF with shared report styling.

    Files are timestamped and never overwrite prior checks. Existing SIARD is
    not modified. A PDF error is surfaced so it cannot silently be marked OK.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    # The SIARD stem is a local file basename, not a directory or a report path.
    safe_stem = ''.join(c if c.isalnum() or c in '._-' else '_' for c in Path(stem).name)
    base = output_dir / f'{safe_stem}_{ts}_innholdskontroll'
    # Multiple profiles or reruns in the same second must never overwrite
    # evidence files from a preceding check.
    candidate = base
    index = 2
    def files_for(path: Path) -> dict[str, Path]:
        # Build extensions by concatenation: Path.with_suffix would discard
        # any part after a dot in a legitimate SIARD file stem.
        return {
            'json': Path(f'{path}.json'),
            'tables_csv': Path(f'{path}_tabeller.csv'),
            'carriers_csv': Path(f'{path}_informasjonsbaerere.csv'),
            'findings_csv': Path(f'{path}_avvik.csv'),
            'mapping_json': Path(f'{path}_kartleggingsforslag.json'),
            'mapping_csv': Path(f'{path}_kartleggingsforslag.csv'),
            'foreign_keys_csv': Path(f'{path}_fremmednokler.csv'),
            'relation_proposals_csv': Path(f'{path}_relasjonsforslag.csv'),
            'pdf': Path(f'{path}.pdf'),
        }
    paths = files_for(candidate)
    while any(path.exists() for path in paths.values()):
        candidate = output_dir / f'{safe_stem}_{ts}_innholdskontroll_{index:02d}'
        index += 1
        paths = files_for(candidate)
    paths['json'].write_text(json.dumps(result, ensure_ascii=False,
                                        indent=2) + '\n', encoding='utf-8')
    _write_csv(paths['tables_csv'],
               ['schema', 'table', 'metadata_rows', 'actual_rows', 'status', 'primary_key', 'pk_status', 'pk_missing', 'pk_duplicates', 'candidate_keys', 'foreign_key_count'],
               [{**x, 'primary_key': ', '.join(x.get('primary_key', []))}
                for x in result['catalog']])
    _write_csv(paths['carriers_csv'],
               ['id', 'label', 'table', 'status', 'rows', 'unique',
                'missing_key', 'duplicate_key', 'notes', 'status_distribution', 'year_distribution'],
               result['carriers'])
    _write_csv(paths['findings_csv'],
               ['check_id', 'scope', 'status', 'count', 'severity', 'detail'],
               result['findings'])
    # Discovery remains a separate, explicit proposal artifact. It MUST NOT
    # become a validated QA profile automatically.
    discovery = result.get('discovery') or {}
    paths['mapping_json'].write_text(json.dumps(discovery, ensure_ascii=False,
                                                indent=2) + '\n', encoding='utf-8')
    mapped_rows = []
    for carrier in discovery.get('carriers', []):
        for proposal in carrier.get('candidates', []):
            mapped_rows.append({
                'carrier_id': carrier['carrier_id'],
                'carrier_label': carrier['carrier_label'],
                'review_status': carrier['review_status'],
                'schema': proposal['schema'], 'table': proposal['table'],
                'score': proposal['score'], 'strength': proposal['strength'],
                'reason': proposal['reason'],
                'declared_rows': proposal['declared_rows'],
                'key_columns': proposal['key']['columns'],
                'key_basis': proposal['key']['basis'],
                'status_columns': proposal['status_columns'],
                'date_columns': proposal['date_columns'],
            })
        if not carrier.get('candidates'):
            mapped_rows.append({
                'carrier_id': carrier['carrier_id'],
                'carrier_label': carrier['carrier_label'],
                'review_status': 'INGEN_KANDIDAT',
            })
    _write_csv(paths['mapping_csv'],
               ['carrier_id', 'carrier_label', 'review_status', 'schema', 'table',
                'score', 'strength', 'reason', 'declared_rows', 'key_columns',
                'key_basis', 'status_columns', 'date_columns'], mapped_rows)
    _write_csv(paths['foreign_keys_csv'],
               ['name', 'origin', 'child_schema', 'child_table', 'child_columns',
                'parent_schema', 'parent_table', 'parent_columns', 'match_type',
                'status', 'note'], discovery.get('declared_relations', []))
    _write_csv(paths['relation_proposals_csv'],
               ['origin', 'child_schema', 'child_table', 'child_columns',
                'parent_schema', 'parent_table', 'parent_columns', 'status',
                'note'], discovery.get('relation_proposals', []))
    _pdf_qa(result, paths['pdf'])
    return {k: str(p) for k, p in paths.items()}


def _pdf_qa(result: dict, output: Path) -> None:
    # This is the SAME toolkit used by the existing depot reports.
    from siard_workflow.operations.report_style import build_toolkit, fmt_int

    tk = build_toolkit()
    E = lambda s: escape(str(s))
    s = result['summary']
    status = result['status']
    verdict = {'GODKJENT': 'GODKJENT', 'DELVIS_KONTROLLERT': 'DELVIS KONTROLLERT',
               'AVVIK': 'AVVIK OPPDAGET'}[status]
    kind = {'GODKJENT': 'ok', 'DELVIS_KONTROLLERT': 'warn', 'AVVIK': 'fail'}[status]
    story = tk.cover('INNHOLDSKONTROLL',
                     'SIARD Workflow Manager - kontroll av informasjonsbærere',
                     [('SIARD-uttrekk', E(result['source_name'])),
                      ('QA-mal', E(result['profile_label'])),
                      ('Mal-ID', E(result['profile_id'])),
                      ('Kjørt', E(result['created_at'].replace('T', ' ')[:19])),
                      ('Modus', 'Skrivebeskyttet, full radskann' if result['scan_rows']
                       else 'Skrivebeskyttet, kun katalog')])
    story += tk.section('Samlet vurdering')
    story.append(tk.status_box(
        verdict,
        f"{s['tables_verified']}/{s['tables']} tabeller. "
        f"{s['carriers_verified']}/{s['carriers']} fagobjekter.", kind))
    story.append(tk.spacer(0.25))
    story.append(tk.kv_table([
        ('Tabeller i SIARD', fmt_int(s['tables'])),
        ('Metadata-rader (sum)', fmt_int(s['metadata_rows_total'])),
        ('Leste datarader (sum)', fmt_int(s['actual_rows_total']) if result['scan_rows'] else 'Ikke lest'),
        ('Verifiserte tabeller', fmt_int(s['tables_verified'])),
        ('Tabeller med avvik', fmt_int(s['tables_with_errors'])),
        ('Deklarerte primærnøkler', fmt_int(s['pk_defined'])),
        ('Primærnøkler kontrollert', fmt_int(s['pk_verified'])),
        ('Primærnøkler med avvik', fmt_int(s['pk_with_errors'])),
        ('Informasjonsbaerere', fmt_int(s['carriers'])),
        ('Kartlegging gjenstår', fmt_int(s['carriers_unverified'])),
        ('Relasjoner kontrollert', fmt_int(s['relations_verified'])),
        ('Fagbegreper med kartleggingsforslag', fmt_int(s.get('discovery_carriers_with_candidates', 0))),
        ('Fremmednøkler i metadata', fmt_int(s.get('discovery_declared_foreign_keys', 0))),
        ('Navnebaserte relasjonsforslag', fmt_int(s.get('discovery_relation_proposals', 0))),
    ]))
    if status == 'DELVIS_KONTROLLERT':
        story.append(tk.note('Dette er IKKE en faglig godkjenning: tabeller, '
                             'informasjonsbærere eller relasjoner er ikke kartlagt/kontrollert.',
                             'warn'))
    if status == 'AVVIK':
        story.append(tk.note('Avvik er dokumentert i avviks-CSV. '
                             'En fullfort arbeidsflyt betyr ikke at innholdet er godkjent.', 'fail'))

    # Discovery is NOT the same as a completed Noark/content check. Keep it
    # visually separate from the verified QA sections.
    discovery = result.get('discovery') or {}
    if discovery:
        story += tk.section('Automatisk kartlegging - forslag til gjennomgang')
        story.append(tk.note(
            'Navnelikhet og metadata-fremmednokler gir kun kartleggingsforslag. '
            'Ingen forslag aktiveres automatisk som en faglig QA-kontroll. '
            'Knytt tabeller og felt til QA-malen etter kontroll mot systemmodell.', 'warn'))
        dsum = discovery.get('summary') or {}
        story.append(tk.kv_table([
            ('Fagbegreper med kandidater', fmt_int(dsum.get('carriers_with_candidates', 0))),
            ('Fagbegreper uten kandidater', fmt_int(dsum.get('carriers_without_candidates', 0))),
            ('Deklarerte fremmednøkler', fmt_int(dsum.get('declared_foreign_keys', 0))),
            ('Metadata-relasjoner med tabelltreff', fmt_int(dsum.get('declared_foreign_keys_resolved', 0))),
            ('Mulige relasjoner ut fra feltnavn', fmt_int(dsum.get('name_based_relation_proposals', 0))),
        ]))
        rows = []
        for item in discovery.get('carriers', []):
            for cand in item.get('candidates', [])[:3]:
                rows.append([E(item['carrier_label']), E(cand['schema'] + '.' + cand['table']),
                             str(cand['score']), E(', '.join(cand['key']['columns']) or '-'),
                             'UBEKREFTET'])
            if not item.get('candidates'):
                rows.append([E(item['carrier_label']), '-', '-', '-', 'INGEN KANDIDAT'])
        if rows:
            story.append(tk.data_table(['Fagbegrep', 'Tabellforslag', 'Treff', 'Nokkel', 'Vurdering'],
                                       rows, [tk.inner_w*k for k in (.22,.30,.10,.22,.16)],
                                       font_size=7.2, right_cols=[2]))
        declared = discovery.get('declared_relations', [])
        if declared:
            story.append(tk.subsection('Fremmednøkler deklarert i SIARD-metadata'))
            rel_rows = [[E(r['child_schema'] + '.' + r['child_table']),
                         E(', '.join(r['child_columns'])),
                         E(r['parent_schema'] + '.' + r['parent_table']),
                         E(r['status'].replace('_', ' '))] for r in declared[:30]]
            story.append(tk.data_table(['Fra tabell', 'Kolonner', 'Til tabell', 'Metadata'],
                                       rel_rows, [tk.inner_w*k for k in (.32,.23,.31,.14)],
                                       font_size=7.0))
            if len(declared) > 30:
                story.append(tk.note('Kun de 30 første metadata-relasjonene vises her; '
                                     'hele listen finnes i fremmednokler.csv.', 'info'))
        proposed = discovery.get('relation_proposals', [])
        if proposed:
            story.append(tk.subsection('Navnebaserte relasjonsforslag (ikke kontrollert)'))
            rows = [[E(r['child_schema'] + '.' + r['child_table']),
                     E(', '.join(r['child_columns'])),
                     E(r['parent_schema'] + '.' + r['parent_table'])]
                    for r in proposed[:20]]
            story.append(tk.data_table(['Fra tabell', 'Mulig fremmednøkkel', 'Til tabell'],
                                       rows, [tk.inner_w*k for k in (.37,.26,.37)],
                                       font_size=7.0))
            if len(proposed) > 20:
                story.append(tk.note('Kun 20 av relasjonsforslagene vises; '
                                     'se relasjonsforslag.csv.', 'info'))

    story += tk.section('Informasjonsbaerere og opptellinger')
    if result['carriers']:
        rows = []
        for c in result['carriers']:
            n = '-' if c['rows'] is None else fmt_int(c['rows'])
            unq = '-' if c['unique'] is None else fmt_int(c['unique'])
            rows.append([E(c['label']), E(c['table'] or '(ikke kartlagt)'), n, unq,
                         E(c['status'].replace('_', ' '))])
        story.append(tk.data_table(['Informasjonsbaerer', 'SIARD-tabell', 'Rader', 'Unike', 'Status'],
                                   rows, [tk.inner_w * k for k in (.24,.27,.12,.12,.25)],
                                   font_size=7.5, right_cols=[2,3]))
    else:
        story.append(tk.note('Ingen faglige informasjonsbærere definert i profilen.', 'warn'))

    distributions = [c for c in result['carriers'] if c['status_distribution'] or c['year_distribution']]
    if distributions:
        story += tk.section('Status- og årsfordelinger')
        for c in distributions:
            story.append(tk.subsection(E(c['label'])))
            for label, values in (('Status', c['status_distribution']),
                                  ('År', c['year_distribution'])):
                if values:
                    rows = [[E(v), fmt_int(n)] for v, n in list(values.items())[:40]]
                    story.append(tk.data_table([label, 'Antall'], rows,
                                               [tk.inner_w*.7, tk.inner_w*.3],
                                               right_cols=[1], font_size=8))

    story += tk.section('Databasetabeller og radkontroll')
    t_rows = [[E(t['schema']), E(t['table']),
               '-' if t['metadata_rows'] is None else fmt_int(t['metadata_rows']),
               '-' if t['actual_rows'] is None else fmt_int(t['actual_rows']),
               E(t['status'].replace('_', ' '))] for t in result['catalog']]
    story.append(tk.data_table(['Schema','Tabell','Metadata','Faktisk','Status'], t_rows,
                               [tk.inner_w * k for k in (.16,.34,.16,.16,.18)],
                               font_size=7.5, right_cols=[2,3]))

    story += tk.section('Kontroller og avvik')
    if result['findings']:
        f_rows = [[E(f['check_id']), E(f['scope']), E(f['status'].replace('_', ' ')),
                   fmt_int(f['count']), E(f['detail'])] for f in result['findings']]
        story.append(tk.data_table(['Kontroll','Område','Resultat','Antall','Forklaring'],
                                   f_rows, [tk.inner_w * k for k in (.16,.17,.18,.10,.39)],
                                   font_size=7.0, right_cols=[3]))
    else:
        story.append(tk.note('Ingen avvik registrert i utførte kontroller.', 'ok'))

    story += tk.section('Sporbarhet og avgrensninger')
    story.append(tk.para('Alle kildefiler er lest skrivebeskyttet. Ingen tabell- eller '
                         'dokumentverdier er lagret i denne rapporten. Unike nøkler '
                         'kontrolleres med midlertidige, nøklede fingeravtrykk og slettes '
                         'etter kjøringen. Resultatfilene kan sammenholdes med '
                         'kontrollens JSON- og CSV-grunnlag.'))
    story.append(tk.para('Antall rader i en tabell er ikke nødvendigvis antall unike saker, '
                         'journalposter eller dokumenter. Faglige antall krever '
                         'bekreftet kartlegging i QA-profilen. Manglende kartlegging '
                         'rapporteres som IKKE KONTROLLERT.'))
    tk.build(output, 'Innholdskontroll', story,
             footer_label=result['source_name'], author='SIARD Workflow Manager')
