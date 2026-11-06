"""Versioned, data-only QA profiles (no SQL execution or Python expressions)."""
from __future__ import annotations

import json
from pathlib import Path


class ProfileError(ValueError):
    pass


def _known_fields(data: dict, allowed: set[str], scope: str) -> None:
    unknown = set(data).difference(allowed)
    if unknown:
        raise ProfileError(f'{scope}: ukjente felt i QA-malen: {", ".join(sorted(unknown))}')


def _string(v, what: str, required: bool = True) -> str:
    if not isinstance(v, str) or (required and not v.strip()):
        raise ProfileError(f'{what} må være tekst' + (' og ikke tom' if required else ''))
    return v.strip()


def load_profile(path: str | Path | None = None) -> dict:
    if path:
        p = Path(path).expanduser()
    else:
        p = Path(__file__).resolve().parents[2] / 'rules' / 'content_qa' / 'ephorte_template.json'
    try:
        raw = json.loads(p.read_text(encoding='utf-8-sig'))
    except (OSError, ValueError) as exc:
        raise ProfileError(f'Kunne ikke lese QA-profil {p}: {exc}') from exc
    return validate_profile(raw)


def validate_profile(profile: dict) -> dict:
    if not isinstance(profile, dict):
        raise ProfileError('QA-mal må være et JSON-objekt')
    _known_fields(profile, {'format_version','id','label','system_hint','description',
                            'carriers','relations'}, 'profil')
    if type(profile.get('format_version')) is not int or profile.get('format_version') != 1:
        raise ProfileError('Ukjent QA-malformat; forventer format_version = 1')
    _string(profile.get('id'), 'id')
    _string(profile.get('label'), 'label')
    if not isinstance(profile.get('carriers'), list):
        raise ProfileError('carriers må være en liste')
    if not isinstance(profile.get('relations', []), list):
        raise ProfileError('relations må være en liste')
    seen = set()
    for item in profile['carriers']:
        if not isinstance(item, dict):
            raise ProfileError('Hver informasjonsbærer må være et JSON-objekt')
        _known_fields(item, {'id','label','description','aliases','source','key','status_column',
                             'date_column','expected_count'}, 'carrier')
        name = _string(item.get('id'), 'carrier.id')
        _string(item.get('label'), 'carrier.label')
        if name in seen:
            raise ProfileError(f'Duplikat carrier.id: {name}')
        seen.add(name)
        aliases = item.get('aliases', [])
        if not isinstance(aliases, list) or len(aliases) > 30 or any(
                not isinstance(a, str) or not a.strip() or len(a) > 100 for a in aliases):
            raise ProfileError(f'{name}.aliases maa vaere en liste med inntil 30 korte navn')
        source = item.get('source')
        if source is not None:
            if not isinstance(source, dict):
                raise ProfileError(f'{name}.source må være objekt eller null')
            _known_fields(source, {'schema','table'}, f'{name}.source')
            _string(source.get('schema'), f'{name}.source.schema')
            _string(source.get('table'), f'{name}.source.table')
        for field in ('key',):
            keys = item.get(field, [])
            if not isinstance(keys, list) or any(not isinstance(v, str) or not v for v in keys):
                raise ProfileError(f'{name}.{field} må være en liste med kolonnenavn')
            if len(set(keys)) != len(keys):
                raise ProfileError(f'{name}.{field} inneholder duplikater')
        for field in ('status_column', 'date_column'):
            if item.get(field) is not None:
                _string(item[field], f'{name}.{field}')
        ex = item.get('expected_count')
        if ex is not None:
            if not isinstance(ex, dict) or not ex or not set(ex).issubset({'exact','min','max'}):
                raise ProfileError(f'{name}.expected_count må ha exact, min eller max')
            if any(type(v) is not int or v < 0 for v in ex.values()):
                raise ProfileError(f'{name}.expected_count må ha ikke-negative heltall')
            if 'min' in ex and 'max' in ex and ex['min'] > ex['max']:
                raise ProfileError(f'{name}.expected_count: min > max')
    rel_ids = set()
    for relation in profile.get('relations', []):
        if not isinstance(relation, dict):
            raise ProfileError('relations må inneholde JSON-objekter')
        _known_fields(relation, {'id','child','parent','child_columns','required',
                                 'description'}, 'relation')
        rel_id = _string(relation.get('id'), 'relation.id')
        if rel_id in rel_ids:
            raise ProfileError(f'Duplikat relation.id: {rel_id}')
        rel_ids.add(rel_id)
        child = _string(relation.get('child'), f'{rel_id}.child')
        parent = _string(relation.get('parent'), f'{rel_id}.parent')
        if child not in seen or parent not in seen:
            raise ProfileError(f'{rel_id} peker til ukjent informasjonsbærer')
        cols = relation.get('child_columns')
        if not isinstance(cols, list) or not cols or any(not isinstance(c, str) or not c for c in cols):
            raise ProfileError(f'{rel_id}.child_columns må inneholde minst ett kolonnenavn')
        if 'required' in relation and type(relation['required']) is not bool:
            raise ProfileError(f'{rel_id}.required må være true/false')
    return profile
