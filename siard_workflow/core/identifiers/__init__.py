"""
siard_workflow.core.identifiers — pluggbare fildeteksjonsbackends.

Eksisterende kontrakt: identify() returnerer (ext, mime, is_encrypted).

Backends:
  - magic_bytes : header/magic-byte-deteksjon (default)
  - siegfried   : PRONOM-basert via ekstern sf-binær (opt-in)
"""
