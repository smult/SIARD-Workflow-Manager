"""
Tester for fullstendig omskriving av SIARD-versjonsreferanser ved nedgradering
2.2 → 2.1 (og oppgradering 2.1 → 2.2).

Bakgrunn: namespace-URI og version-attributtet var ikke de eneste stedene
versjonen sto. header/metadata.xsd låser version-attributtet via en
`versionType`-simpleType:

    <xs:simpleType name="versionType">
      <xs:annotation>
        <xs:documentation>versionType is constrained to "2.2" …</xs:documentation>
      </xs:annotation>
      <xs:restriction base="xs:string">
        <xs:whiteSpace value="collapse" />
        <xs:enumeration value="2.2" />
      </xs:restriction>
    </xs:simpleType>

Blir denne stående på "2.2" mens metadata.xml skrives med version="2.1",
validerer ikke uttrekket mot sin egen XSD. I tillegg må versjonsmarkør-mappa
header/siardversion/<x.y>/ følge mål-versjonen.

Kjør:  python -X utf8 tests/test_version_refs.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from siard_workflow.core.siard_format import (
    siard_version_transform,
    detect_siard_version,
    detect_folder_siard_version,
    rewrite_siardversion_path,
)


def _ok(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ✓ {msg}")


# ── Testdata: utdrag av en ekte SIARD 2.2 header/metadata.xsd ─────────────────

_XSD_22 = b'''<?xml version="1.0" encoding="UTF-8"?>
<!-- SIARD 2.2 metadata schema (eCH-0165 V2.2) -->
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns="http://www.bar.admin.ch/xmlns/siard/2.2/metadata.xsd"
           targetNamespace="http://www.bar.admin.ch/xmlns/siard/2.2/metadata.xsd"
           elementFormDefault="qualified" version="2.2">
\t<xs:simpleType name="versionType">
\t\t<xs:annotation>
\t\t\t<xs:documentation>versionType is constrained to "2.2" for conformity with this XML schema</xs:documentation>
\t\t</xs:annotation>
\t\t<xs:restriction base="xs:string">
\t\t\t<xs:whiteSpace value="collapse" />
\t\t\t<xs:enumeration value="2.2" />
\t\t</xs:restriction>
\t</xs:simpleType>
\t<xs:attribute name="version" type="versionType" fixed="2.2"/>
\t<xs:simpleType name="unrelatedType">
\t\t<xs:annotation>
\t\t\t<xs:documentation>Kodeverk, se kapittel 2.2 i intern instruks</xs:documentation>
\t\t</xs:annotation>
\t\t<xs:restriction base="xs:string">
\t\t\t<xs:enumeration value="2.2" />
\t\t</xs:restriction>
\t</xs:simpleType>
</xs:schema>
'''

_META_22 = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
            b'<siardArchive xmlns="http://www.bar.admin.ch/xmlns/siard/2.2/metadata.xsd"'
            b' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
            b' xsi:schemaLocation="http://www.bar.admin.ch/xmlns/siard/2.2/metadata.xsd'
            b' metadata.xsd" version="2.2">\n'
            b'  <dbname>Northwind</dbname>\n'
            b'</siardArchive>\n')


def test_xsd_versiontype_downgrade() -> None:
    print("test_xsd_versiontype_downgrade (metadata.xsd 2.2 → 2.1)")
    out = siard_version_transform(_XSD_22, "2.1")

    _ok(b'<xs:enumeration value="2.1" />\n\t\t</xs:restriction>' in out
        or b'value="2.1"' in out.split(b'name="versionType"')[1],
        "versionType: enumeration satt til 2.1")
    _ok(b'versionType is constrained to "2.1"' in out,
        "versionType: dokumentasjon satt til 2.1")
    _ok(b'fixed="2.1"' in out, 'version-attributt: fixed="2.1"')
    _ok(b'version="2.1"' in out and b'version="2.2"' not in out,
        "xs:schema version-attributt satt til 2.1")
    _ok(b"eCH-0165 V2.1" in out, "kommentar om SIARD-versjon oppdatert")
    _ok(b'siard/2/metadata.xsd' in out and b'siard/2.2/' not in out,
        "namespace normalisert til generisk /2/")

    # Urelatert simpleType og urelatert dokumentasjon skal IKKE røres
    tail = out.split(b'name="unrelatedType"')[1]
    _ok(b'<xs:enumeration value="2.2" />' in tail,
        "urelatert simpleType uberørt")
    _ok(b"kapittel 2.2 i intern instruks" in out,
        "dokumentasjon uten versjonskontekst uberørt")


def test_metadata_downgrade() -> None:
    print("test_metadata_downgrade (metadata.xml 2.2 → 2.1)")
    out = siard_version_transform(_META_22, "2.1")
    _ok(b'version="2.1"' in out, "version-attributt satt til 2.1")
    _ok(b"siard/2.2/" not in out, "ingen 2.2-namespace igjen")
    _ok(detect_siard_version(out) == "2.1", "detect_siard_version() gir 2.1")
    _ok(b"<dbname>Northwind</dbname>" in out, "innhold bevart")


def test_upgrade_and_noop() -> None:
    print("test_upgrade_and_noop")
    up = siard_version_transform(_XSD_22.replace(b"2.2", b"2.1"), "2.2")
    _ok(b'versionType is constrained to "2.2"' in up,
        "oppgradering 2.1 → 2.2 skriver om versionType")
    _ok(siard_version_transform(_META_22, "3.0") == _META_22,
        "ukjent målversjon → data uendret")
    # Idempotens: transformasjon to ganger gir samme resultat
    once = siard_version_transform(_XSD_22, "2.1")
    _ok(siard_version_transform(once, "2.1") == once, "idempotent")


def test_multiline_version_attribute_decl() -> None:
    print("test_multiline_version_attribute_decl")
    xsd = (b'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">\n'
           b'  <xs:attribute\n'
           b'      name="version"\n'
           b'      type="xs:string"\n'
           b'      default="2.2"/>\n'
           b'</xs:schema>\n')
    out = siard_version_transform(xsd, "2.1")
    _ok(b'default="2.1"' in out, "default= på flerlinjes version-deklarasjon")


def test_version_token_boundaries() -> None:
    print("test_version_token_boundaries")
    # Setningsavsluttende «2.2.» skal treffes …
    doc = (b'<xs:schema><xs:annotation><xs:documentation>Structure of the meta'
           b' data in the SIARD format 2.2.</xs:documentation></xs:annotation>'
           b'</xs:schema>')
    _ok(b"SIARD format 2.1." in siard_version_transform(doc, "2.1"),
        "«… format 2.2.» (punktum til slutt) skrives om")

    # … men xs:schema-revisjonen 2.2.1.7 er IKKE SIARD-versjonen og skal stå
    rev = (b'<xs:schema version="2.2.1.7" id="metadata">'
           b'<xs:annotation><xs:documentation>SIARD 2.2 schema, revision '
           b'2.2.1.7</xs:documentation></xs:annotation></xs:schema>')
    out = siard_version_transform(rev, "2.1")
    _ok(b'version="2.2.1.7"' in out, "xs:schema-revisjon 2.2.1.7 uberørt")
    _ok(b"revision 2.2.1.7" in out, "revisjonsnummer i dokumentasjon uberørt")
    _ok(b"SIARD 2.1 schema" in out, "selve versjonsomtalen skrives likevel om")

    # Versjonstall som er del av et større tall skal ikke treffes
    _ok(b"12.2" in siard_version_transform(
            b'<!-- siard note: 12.2 -->', "2.1"), "«12.2» uberørt")


def test_future_version_comment_untouched() -> None:
    print("test_future_version_comment_untouched (ekte 2.1-XSD)")
    # Den offisielle SIARD 2.1-XSD-en har denne kommentaren INNE i versionType.
    # «2.2» er en henvisning til en framtidig versjon — ikke filens egen versjon.
    xsd = (b'<xs:simpleType name="versionType">\n'
           b'  <xs:annotation>\n'
           b'    <xs:documentation>versionType is constrained to "2.1" for '
           b'conformity with this XML schema</xs:documentation>\n'
           b'  </xs:annotation>\n'
           b'  <xs:restriction base="xs:string">\n'
           b'    <xs:whiteSpace value="collapse"/>\n'
           b'    <xs:enumeration value="2.1"/>\n'
           b'    <!--  to be extended later with\n'
           b'    <xs.enumeration value="2.2"/>\n'
           b'    etc. -->\n'
           b'  </xs:restriction>\n'
           b'</xs:simpleType>\n')
    out = siard_version_transform(xsd, "2.1")
    _ok(out == xsd, "ekte 2.1-XSD: 2.1 → 2.1 er en ren no-op")
    _ok(b'<xs.enumeration value="2.2"/>' in out,
        "kommentar om framtidig versjon bevart")


def test_siardversion_folder() -> None:
    print("test_siardversion_folder (header/siardversion/<x.y>/)")
    namelist = ["header/", "header/siardversion/", "header/siardversion/2.2/",
                "header/metadata.xml", "content/schema0/table1/table1.xml"]
    _ok(detect_folder_siard_version(namelist) == "2.2",
        "mappeversjon lest fra namelist")
    _ok(detect_folder_siard_version([], "2.1") == "2.1",
        "fallback når markør-mappa mangler")

    _ok(rewrite_siardversion_path("header/siardversion/2.2/", "2.1")
        == "header/siardversion/2.1/", "markør-mappa skrevet om")
    _ok(rewrite_siardversion_path("header/siardversion/", "2.1")
        == "header/siardversion/", "foreldremappa uberørt")
    _ok(rewrite_siardversion_path("header/metadata.xml", "2.1")
        == "header/metadata.xml", "andre header-filer uberørt")
    # Presisjon: kun versjonsleddet — ikke andre forekomster i stien
    _ok(rewrite_siardversion_path(
            "content/schema0/table2.2/t.xml", "2.1")
        == "content/schema0/table2.2/t.xml", "innholdsstier uberørt")
    _ok(rewrite_siardversion_path("header/siardversion/2.1/", "2.1")
        == "header/siardversion/2.1/", "allerede riktig versjon → uendret")


def main() -> None:
    test_xsd_versiontype_downgrade()
    test_metadata_downgrade()
    test_upgrade_and_noop()
    test_multiline_version_attribute_decl()
    test_version_token_boundaries()
    test_future_version_comment_untouched()
    test_siardversion_folder()
    print("\nAlle versjonsreferanse-tester bestått ✓")


if __name__ == "__main__":
    main()
