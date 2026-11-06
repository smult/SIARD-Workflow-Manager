"""Metadata-only proposals for system-specific SIARD content mappings.

Discovery is NOT a QA approval and does not populate source/key fields in a
validated profile. Physical table names and exported descriptions are hints,
not proof that a table represents a Noark information carrier.

Only metadata (table/column/constraint names and declared row counts) is
examined; archival record values and LOBs are not read by this module.
"""
from __future__ import annotations

import re
import unicodedata

from .source import Table, ForeignKey


_COMMON_PREFIXES = ('dbo', 'tbl', 'tab', 'table')
_GENERIC_KEYS = frozenset({'id', 'pk', 'key', 'kode', 'code', 'nummer', 'nr'})


def _normal(value: str) -> str:
    """Unicode/case/underscore-insensitive comparison, never SQL syntax."""
    value = unicodedata.normalize('NFKC', str(value)).casefold()
    return ''.join(ch for ch in value if ch.isalnum())


def _table_stem(value: str) -> str:
    text = str(value)
    # Strip one traditional naming prefix, not arbitrary leading letters.
    for prefix in _COMMON_PREFIXES:
        if re.match(r'^' + prefix + r'[_ .-]', text, flags=re.IGNORECASE):
            text = text[len(prefix) + 1:]
            break
    return _normal(text)


def _tokens(value: str) -> set[str]:
    text = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', str(value))
    return set(re.findall(r'[^\W_]+', text.casefold(), flags=re.UNICODE))


def _column_proposals(table: Table) -> dict:
    """Column-name hints only. Never assert uniqueness or semantic meaning."""
    cols = [c.name for c in table.columns]
    all_cols = set(cols)
    primary = list(table.primary_key)
    if primary and set(primary) <= all_cols:
        key = {'columns': primary, 'basis': 'DEKLARERT_PRIMAERNOEKKEL'}
    else:
        unique = next((list(ck.columns) for ck in table.candidate_keys
                       if ck.columns and set(ck.columns) <= all_cols), None)
        if unique:
            key = {'columns': unique, 'basis': 'DEKLARERT_KANDIDATNOEKKEL'}
        else:
            stem = _table_stem(table.name)
            guessed = [name for name in cols
                       if _normal(name) in (stem + 'id', stem + 'nr', stem + 'nummer')]
            if not guessed:
                guessed = [name for name in cols if _normal(name) == 'id']
            key = {'columns': guessed[:1], 'basis': 'NAVNEFORSLAG_IKKE_VERIFISERT'
                   if guessed else 'INGEN_NOEKKEL_FUNNET'}
    status_cols = [name for name in cols if any(word in _normal(name)
                   for word in ('status', 'tilstand', 'fase', 'gradering'))]
    date_cols = [name for name in cols if any(word in _normal(name)
                 for word in ('dato', 'date', 'opprettet', 'registrert', 'avsluttet',
                              'jourfoert', 'journalfoert'))]
    return {'key': key, 'status_columns': status_cols[:8],
            'date_columns': date_cols[:8]}


def _score(table: Table, carrier: dict) -> tuple[int, str]:
    aliases = [carrier['label'], carrier['id'], *(carrier.get('aliases') or [])]
    name = _table_stem(table.name)
    name_tokens = _tokens(table.name)
    description = _normal(table.description)
    best = (0, '')
    for alias in aliases:
        a = _normal(alias)
        if not a or len(a) < 2:
            continue
        score = 0
        reason = ''
        if name == a:
            score, reason = 92, 'Tabellnavnet tilsvarer et begrep i QA-malen'
        elif len(a) >= 5 and (name.startswith(a) or name.endswith(a)):
            score, reason = 65, 'Tabellnavnet inneholder et QA-begrep'
        elif len(a) >= 4 and a in name_tokens:
            score, reason = 58, 'Et ord i tabellnavnet tilsvarer et QA-begrep'
        elif len(a) >= 5 and a in description:
            score, reason = 36, 'Beskrivelsen i SIARD-metadata nevner et QA-begrep'
        if score > best[0]:
            best = (score, reason)
    # A declared key adds useful evidence of structure, not domain meaning.
    if best[0] and table.primary_key:
        return min(99, best[0] + 3), best[1] + '; primaernokkel er deklarert'
    return best


def _resolve_foreign_key(tables: dict[tuple[str, str], Table],
                         child: Table, fk: ForeignKey) -> tuple[Table | None, str]:
    schema = fk.referenced_schema or child.schema
    name = fk.referenced_table
    target = tables.get((schema, name))
    if target is None and name.count('.') == 1:
        # SIARD permits schema.table notation for referencedTable.
        s, t = name.split('.', 1)
        target = tables.get((s.strip('"'), t.strip('"')))
    if target is None:
        return None, 'Referert tabell finnes ikke med oppgitt schema/tabellnavn'
    child_columns = {c.name for c in child.columns}
    parent_columns = {c.name for c in target.columns}
    if not fk.references or any(not a or not b or a not in child_columns
                                or b not in parent_columns for a, b in fk.references):
        return target, 'En eller flere deklarerte referansekolonner mangler'
    return target, ''


def _declared_relations(tables: list[Table]) -> list[dict]:
    table_map = {t.key: t for t in tables}
    out = []
    for t in tables:
        for fk in t.foreign_keys:
            target, issue = _resolve_foreign_key(table_map, t, fk)
            out.append({
                'origin': 'DEKLARERT_SIARD_METADATA', 'name': fk.name,
                'child_schema': t.schema, 'child_table': t.name,
                'child_columns': [a for a, _ in fk.references],
                'parent_schema': target.schema if target else fk.referenced_schema,
                'parent_table': target.name if target else fk.referenced_table,
                'parent_columns': [b for _, b in fk.references],
                'match_type': fk.match_type,
                'status': 'METADATA_LOEST' if not issue else 'KREVER_GJENNOMGANG',
                'note': issue or 'Relasjonen er deklarert i SIARD, men dataverdier er ikke kontrollert av kartleggingen',
            })
    return out


def _inferred_relations(tables: list[Table], declared: list[dict],
                        limit: int = 1500) -> list[dict]:
    """Suggest child->parent links based on column names and declared PKs.

    A same-named generic ID never generates a suggestion. Nothing from here
    enters integrity checks until an operator maps it in a QA profile.
    """
    target_index: dict[str, list[tuple[Table, str]]] = {}
    for parent in tables:
        if len(parent.primary_key) != 1:
            continue
        pk = parent.primary_key[0]
        if pk not in {c.name for c in parent.columns}:
            continue
        stem = _table_stem(parent.name)
        pk_norm = _normal(pk)
        candidates = {stem + 'id', stem + 'nr', stem + 'nummer'}
        if pk_norm not in _GENERIC_KEYS and len(pk_norm) >= 5:
            candidates.add(pk_norm)
        for term in candidates:
            target_index.setdefault(term, []).append((parent, pk))

    existing = {(r['child_schema'], r['child_table'],
                 tuple(r['child_columns']), r['parent_schema'], r['parent_table'])
                for r in declared}
    out = []
    seen = set()
    for child in tables:
        for col in child.columns:
            for parent, pk in target_index.get(_normal(col.name), []):
                if parent.key == child.key:
                    continue
                sig = (child.schema, child.name, (col.name,), parent.schema, parent.name)
                if sig in seen or sig in existing:
                    continue
                seen.add(sig)
                out.append({
                    'origin': 'NAVNELIKHET_IKKE_VERIFISERT',
                    'child_schema': child.schema, 'child_table': child.name,
                    'child_columns': [col.name],
                    'parent_schema': parent.schema, 'parent_table': parent.name,
                    'parent_columns': [pk],
                    'status': 'FORSLAG',
                    'note': 'Feltnavn tilsvarer mulig fremmednokkel; kontroller modell og data for bruk',
                })
                if len(out) >= limit:
                    return out
    return out


def discover_catalog(tables: list[Table], profile: dict, *, max_candidates: int = 5) -> dict:
    """Produce conservative, review-only mapping suggestions from metadata."""
    suggestions = []
    for carrier in profile['carriers']:
        candidates = []
        for t in tables:
            score, reason = _score(t, carrier)
            if score < 35:
                continue
            columns = _column_proposals(t)
            candidates.append({
                'schema': t.schema, 'table': t.name, 'score': score,
                'strength': 'STERKT_NAVNETREFF' if score >= 90 else (
                    'MULIG_NAVNETREFF' if score >= 55 else 'BESKRIVELSESTREFF'),
                'reason': reason,
                'declared_rows': t.declared_rows,
                'key': columns['key'],
                'status_columns': columns['status_columns'],
                'date_columns': columns['date_columns'],
            })
        candidates.sort(key=lambda c: (-c['score'], c['schema'], c['table']))
        suggestions.append({
            'carrier_id': carrier['id'], 'carrier_label': carrier['label'],
            'already_mapped': bool(carrier.get('source')),
            'review_status': 'UBEKREFTET',
            'candidates': candidates[:max_candidates],
            'candidates_total': len(candidates),
        })
    declared = _declared_relations(tables)
    inferred = _inferred_relations(tables, declared)
    summary = {
        'tables': len(tables),
        'carriers': len(suggestions),
        'carriers_with_candidates': sum(bool(c['candidates']) for c in suggestions),
        'carriers_without_candidates': sum(not c['candidates'] for c in suggestions),
        'declared_foreign_keys': len(declared),
        'declared_foreign_keys_resolved': sum(r['status'] == 'METADATA_LOEST' for r in declared),
        'declared_foreign_keys_needing_review': sum(r['status'] != 'METADATA_LOEST' for r in declared),
        'name_based_relation_proposals': len(inferred),
    }
    return {
        'method': 'METADATA_NAVNETREFF_IKKE_FAGLIG_KONTROLL',
        'review_required': True,
        'summary': summary,
        'carriers': suggestions,
        'declared_relations': declared,
        'relation_proposals': inferred,
    }
