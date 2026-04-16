"""
siard_workflow/core/siard_format.py  —  SIARD-formatversjonshåndtering

Støtter versjondeteksjon og namespace-transformasjon mellom SIARD 2.1 og 2.2.

SIARD 2.1 namespace-base: http://www.bar.admin.ch/xmlns/siard/2.1/
SIARD 2.2 namespace-base: http://www.bar.admin.ch/xmlns/siard/2.2/

Noen eldre arkiver bruker generisk siard/2/ som håndteres som 2.1.
"""
from __future__ import annotations

import re


# Basis-URI som er felles for alle SIARD 2.x namespace-strenger
_NS_BASE = b"www.bar.admin.ch/xmlns/siard/"

# XML-filer som inneholder SIARD-namespaces og skal transformeres
_SIARD_XML_SUFFIXES = (
    "metadata.xml",
    ".xsd",
    # table-XML-er identifiseres på filnavn-mønster, ikke endelse
)


def detect_siard_version(metadata_bytes: bytes) -> str:
    """
    Detekter SIARD-versjon fra metadata.xml-innhold.

    Sjekker namespace-URI-er (2.1/2.2) og version-attributt.
    Returnerer '2.1' som standard hvis verken 2.1 eller 2.2 gjenkjennes.
    """
    if _NS_BASE + b"2.2/" in metadata_bytes:
        return "2.2"
    if _NS_BASE + b"2.1/" in metadata_bytes:
        return "2.1"
    # Fallback: sjekk version-attributt direkte
    m = re.search(rb'version="(2\.\d+)"', metadata_bytes)
    if m:
        ver = m.group(1).decode(errors="replace")
        if ver in ("2.1", "2.2"):
            return ver
    return "2.1"


def siard_version_transform(data: bytes, to_ver: str) -> bytes:
    """
    Transformer SIARD namespace-URI-er og version-attributt til ønsket versjon.

    Per SIARD-standarden skal xmlns og xsi:schemaLocation bruke den generiske
    /siard/2/-URIen uavhengig av versjon:
      - http://www.bar.admin.ch/xmlns/siard/2/

    Versjonerte URI-er (/2.1/ og /2.2/) erstattes derfor med den generiske /2/-URIen.
    Den generiske /2/-URIen forblir uendret (allerede korrekt).

    version-attributtet i rot-elementet settes til to_ver ("2.1" eller "2.2").

    Returnerer data uendret hvis to_ver ikke er '2.1' eller '2.2'.
    """
    if to_ver not in ("2.1", "2.2"):
        return data

    dst = to_ver.encode()

    # Erstatt versjonerte namespace-URI-er med den generiske /2/-URIen
    # (SIARD-standarden krever generisk URI for xmlns og xsi:schemaLocation)
    data = data.replace(_NS_BASE + b"2.1/", _NS_BASE + b"2/")
    data = data.replace(_NS_BASE + b"2.2/", _NS_BASE + b"2/")
    # Generisk _NS_BASE + b"2/" forblir uendret — allerede korrekt

    # Erstatt version-attributt i rot-elementet med ønsket versjon
    data = data.replace(b'version="2.1"', b'version="' + dst + b'"')
    data = data.replace(b'version="2.2"', b'version="' + dst + b'"')

    return data


def get_target_siard_version() -> str:
    """
    Les ønsket eksport-versjon fra config.json.
    Returnerer '2.1' som standard.
    """
    try:
        from settings import get_config
        ver = str(get_config("siard_output_version") or "2.1").strip()
        if ver in ("2.1", "2.2"):
            return ver
    except Exception:
        pass
    return "2.1"


# Tegn som er tillatt i et schema-navn uten HTML- eller URL-koding
_SAFE_SCHEMA_NAME_RE = re.compile(r'^[A-Za-z0-9_.\-]+$')


def sanitize_metadata_schema_names(data: bytes) -> bytes:
    """
    Saniterer <schemas><schema><name>-verdier i metadata.xml.

    Dersom et schema-navn inneholder HTML-usikre tegn (<, >, &, ", ')
    eller tegn som ikke er URL-safe (kun A-Za-z0-9_.- tillatt),
    erstattes verdien med 'schemaN' (der N er 1-basert indeks).

    Bruker byte-nivå regex-erstatning for å unngå XML-re-serialisering
    og bevare all eksisterende formatering, namespaces og attributter.
    """
    # Finn <schemas>-blokken (med mulig namespace-prefiks)
    sm = re.search(rb'<(?:[A-Za-z0-9_-]+:)?schemas(?:\s[^>]*)?>', data)
    if not sm:
        return data
    em = re.search(rb'</(?:[A-Za-z0-9_-]+:)?schemas\s*>', data[sm.end():])
    if not em:
        return data

    pre          = data[:sm.end()]
    schemas_body = data[sm.end(): sm.end() + em.start()]
    post         = data[sm.end() + em.start():]

    counter = [0]
    changed = [False]

    _schema_block = re.compile(
        rb'(<(?:[A-Za-z0-9_-]+:)?schema(?:\s[^>]*)?>)(.*?)'
        rb'(</(?:[A-Za-z0-9_-]+:)?schema\s*>)',
        re.DOTALL,
    )
    _name_tag = re.compile(
        rb'(<(?:[A-Za-z0-9_-]+:)?name(?:\s[^>]*)?>)(.*?)'
        rb'(</(?:[A-Za-z0-9_-]+:)?name\s*>)',
        re.DOTALL,
    )

    def _fix_schema(m: re.Match) -> bytes:
        counter[0] += 1
        inner = m.group(2)
        nm = _name_tag.search(inner)
        if nm:
            val = nm.group(2).decode("utf-8", errors="replace").strip()
            if not _SAFE_SCHEMA_NAME_RE.match(val):
                replacement = f"schema{counter[0]}".encode()
                inner = inner[: nm.start(2)] + replacement + inner[nm.end(2) :]
                changed[0] = True
        return m.group(1) + inner + m.group(3)

    new_body = _schema_block.sub(_fix_schema, schemas_body)

    if not changed[0]:
        return data
    return pre + new_body + post


def is_siard_xml(arc_name: str) -> bool:
    """
    Returner True hvis arc_name er en XML-fil som typisk inneholder
    SIARD namespace-deklarasjoner og skal transformeres.

    Inkluderer:
      - header/metadata.xml
      - header/*.xsd
      - content/.../tableN.xml
    """
    name_lower = arc_name.lower()
    return name_lower.endswith(".xml") or name_lower.endswith(".xsd")
