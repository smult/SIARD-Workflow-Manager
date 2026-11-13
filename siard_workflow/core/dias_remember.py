"""
siard_workflow/core/dias_remember.py
------------------------------------
Felt i DIAS-pakkingen som huskes til neste kjøring.

Brukeren krysser av «Husk» til høyre for hvert felt i DIAS-dialogen. Avkryssede
felt og verdiene deres lagres i programmets innstillinger (config.json,
uavhengig av uttrekk og kildesystem) og fylles inn i nye DIAS-steg. Felt uten
avkrysning huskes ikke på tvers av uttrekk — de ligger bare i prosjektfila
(.siardwf) for uttrekket de ble fylt ut for.

Erstatter at alle feltene ble lagret i op_params og dermed fulgte med til
neste uttrekk (Submission Agreement, pakketittel, periode … fra forrige
uttrekk dukket opp igjen).
"""
from __future__ import annotations

# Standardutvalg før brukeren har valgt selv: felt som normalt er like for
# alle uttrekk hos samme arkivinstitusjon
DEFAULT_FIELDS: tuple[str, ...] = (
    "archivist_type", "creator", "preserver", "confirm_before_packing",
)


def remembered_fields() -> list[str]:
    """Feltnøklene som er krysset av for «Husk»."""
    try:
        from settings import get_config
        fields = get_config("dias_remember_fields")
    except Exception:
        fields = None
    if fields is None:
        return list(DEFAULT_FIELDS)
    return [str(f) for f in fields]


def remembered_values() -> dict:
    """{feltnøkkel: verdi} for avkryssede felt som har en lagret verdi."""
    try:
        from settings import get_config, load_settings
        fields = get_config("dias_remember_fields")
        values = dict(get_config("dias_remembered_values") or {})
        if fields is None and not values:
            # Første gang: hent standardfeltene fra de tidligere lagrede
            # DIAS-innstillingene (op_params), så ingenting går tapt
            old = load_settings().get("op_params", {}).get("dias_package", {})
            values = {k: old[k] for k in DEFAULT_FIELDS if k in old}
    except Exception:
        return {}
    keep = set(remembered_fields())
    return {k: v for k, v in values.items() if k in keep}


def save_remembered(fields, values: dict) -> dict:
    """
    Lagre hvilke felt som huskes, og verdiene deres (bare avkryssede felt).
    Returnerer de lagrede verdiene.
    """
    fields = [str(f) for f in fields]
    stored = {k: values[k] for k in fields if k in values}
    from settings import save_config
    save_config({"dias_remember_fields": fields, "dias_remembered_values": stored})
    return stored
