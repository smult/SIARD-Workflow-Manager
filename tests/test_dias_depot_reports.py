"""
Depotrapportene skal med i DIAS-pakken når de er laget.

Bakgrunn (nov. 2026): «Depotrapporter (PDF)» skriver rapportene til
undermappa «depotrapporter» med tidsstempel i navnet
(«Uttrekk_konvertert_arkivinformasjon_20261126_101500.pdf»). DIAS-pakkingen
lette aldri der, så rapportene kom ikke med i pakken uten at de ble lagt til
for hånd.

Verifiserer:
  1. filnavnene operasjonen faktisk lager, gjenkjennes
  2. rapportene fra denne kjøringen ligger i tar-arkivet og i METS
  3. laget i en tidligere kjøring: nyeste sett fra disk — ikke blandet med
     eldre sett, og ikke rapporter for andre uttrekk i samme mappe
  4. plassholderen fra DIAS-vinduet løses til hele settet; eldre versjoner i
     fillista tas ut; ingen dobbeltføring
  5. uten depotrapporter pakkes det som før
  6. DIAS-vinduet: rapportene står i filtreet

Kjør:  python -X utf8 tests/test_dias_depot_reports.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import types
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(Path(__file__).resolve().parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from test_dias_premis_included import _has, _ok, _pack, _siard  # noqa: E402

_DEST = "administrative_metadata/repository_operations/"
_KINDS = ("arkivinformasjon", "konverteringsrapport", "filorganisering",
          "behandlingssammendrag")


def _reports(folder: Path, stem: str, ts: str, kinds=_KINDS) -> "list[Path]":
    folder.mkdir(parents=True, exist_ok=True)
    out = []
    for k in kinds:
        p = folder / f"{stem}_{k}_{ts}.pdf"
        p.write_bytes(b"%PDF-1.4\n% " + k.encode() + b" " + ts.encode() + b"\n")
        out.append(p)
    return out


def _in_pkg(names: list) -> "list[str]":
    """Depotrapportene i pakken (filnavn)."""
    from siard_workflow.operations.dias_package_operation import depot_report_kind
    return sorted(Path(n).name for n in names if depot_report_kind(n))


def test_real_names(tmp: Path) -> None:
    print("\n[1] Filnavnene operasjonen lager, gjenkjennes")
    from siard_workflow.core.context import WorkflowContext
    from siard_workflow.operations.depot_reports_operation import DepotReportsOperation
    from siard_workflow.operations.dias_package_operation import (
        depot_report_files, depot_report_kind)
    d = tmp / "ekte"
    d.mkdir()
    siard = _siard(d / "Uttrekk_konvertert_anonymisert.siard")
    ctx = WorkflowContext(siard_path=siard, metadata={
        "log_dir": str(d), "step_results": []})
    op = DepotReportsOperation()
    op.params["open_folder"] = False
    r = op.run(ctx)
    _ok(r.success and r.data["report_paths"], f"depotrapporter laget ({r.message})")
    for p in r.data["report_paths"]:
        _ok(depot_report_kind(p, "Uttrekk") in _KINDS,
            f"{Path(p).name} gjenkjennes som depotrapport for «Uttrekk»")
    _ok(Path(r.data["report_dir"]).name == "depotrapporter",
        "rapportene ligger i undermappa «depotrapporter»")
    ctx.set_result("depot_reports", r.data)
    _ok([str(p) for p in depot_report_files(siard, ctx)] == r.data["report_paths"],
        "DIAS-pakkingen får nøyaktig disse filene fra kjøringen")
    _ok(sorted(str(p) for p in depot_report_files(d / "Uttrekk.siard"))
        == sorted(r.data["report_paths"]),
        "og finner de samme på disk ut fra originalfila (uten kjøringsdata)")
    _ok(not depot_report_kind("Uttrekk_workflow_rapport_20261126_101500.pdf")
        and not depot_report_kind("Annet_arkivinformasjon_20261126_101500.pdf", "Uttrekk"),
        "kjørerapporten og andre uttrekks rapporter er ikke depotrapporter for dette")


def test_this_run(tmp: Path) -> None:
    print("\n[2] Laget i denne kjøringen")
    d = tmp / "a"
    d.mkdir()
    siard = _siard(d / "Uttrekk_konvertert.siard")
    reps = _reports(d / "egen_mappe", "Uttrekk_konvertert", "20261126_101500")
    r, names, logs, mets = _pack(siard, d / "ut", results={"depot_reports": {
        "report_dir": str(d / "egen_mappe"), "report_paths": [str(p) for p in reps]}})
    _ok(r.success, f"DIAS-pakke laget ({r.message})")
    for p in reps:
        _ok(_has(names, _DEST + p.name) and p.name in mets,
            f"{p.name.split('_')[2]}: i pakken og i METS")
    _ok(sum("Auto-inkludert" in m and "20261126_101500" in m for m in logs) == 4,
        "loggen sier at de fire rapportene er tatt med")


def test_from_disk(tmp: Path) -> None:
    print("\n[3] Laget i en tidligere kjøring")
    d = tmp / "b"
    d.mkdir()
    siard = _siard(d / "Uttrekk_konvertert_1.siard")
    _reports(d / "depotrapporter", "Uttrekk_konvertert", "20261120_080000")
    new = _reports(d / "depotrapporter", "Uttrekk_konvertert_1", "20261126_101500",
                   kinds=("arkivinformasjon", "behandlingssammendrag"))
    _reports(d / "depotrapporter", "Annet_konvertert", "20261127_120000")
    r, names, logs, mets = _pack(siard, d / "ut")
    _ok(r.success and _in_pkg(names) == sorted(p.name for p in new),
        "nyeste sett er med — ikke eldre sett og ikke rapporter for andre uttrekk: "
        + ", ".join(n.split("_")[-3] for n in _in_pkg(names)))

    d2 = tmp / "b2"
    d2.mkdir()
    beside = _reports(d2, "Uttrekk", "20261126_101500", kinds=("behandlingssammendrag",))
    r, names, logs, mets = _pack(_siard(d2 / "Uttrekk.siard"), d2 / "ut")
    _ok(r.success and _in_pkg(names) == [beside[0].name],
        "også når rapportene ligger i SIARD-mappa (ingen undermappe)")

    # Kjøringsdata peker på filer som er flyttet → nyeste sett på disk
    r, names, logs, mets = _pack(siard, d / "ut2", results={"depot_reports": {
        "report_dir": str(d / "depotrapporter"),
        "report_paths": [str(d / "depotrapporter" / "flyttet.pdf")]}})
    _ok(r.success and _in_pkg(names) == sorted(p.name for p in new),
        "filene fra kjøringen er borte → nyeste sett på disk brukes")


def test_placeholder_and_stale(tmp: Path) -> None:
    print("\n[4] Plassholder fra DIAS-vinduet og eldre versjoner i fillista")
    d = tmp / "c"
    d.mkdir()
    siard = _siard(d / "Uttrekk_konvertert.siard")
    old = _reports(d / "depotrapporter", "Uttrekk_konvertert", "20261120_080000")
    new = _reports(d / "depotrapporter", "Uttrekk_konvertert", "20261126_101500")
    own = d / "Notat fra arkivaren.pdf"
    own.write_bytes(b"%PDF-1.4\n")
    extra = [{"src": "[[pending:depot_reports]]", "dest": _DEST},
             {"src": str(old[0]), "dest": _DEST + old[0].name},      # fra forrige åpning
             {"src": str(new[1]), "dest": _DEST + new[1].name},      # alt valgt i vinduet
             {"src": str(own), "dest": _DEST + own.name}]
    r, names, logs, mets = _pack(siard, d / "ut", extra_files=extra, results={
        "depot_reports": {"report_dir": str(d / "depotrapporter"),
                          "report_paths": [str(p) for p in new]}})
    _ok(r.success and _in_pkg(names) == sorted(p.name for p in new),
        "nøyaktig de fire nye rapportene — én av hver, ingen dobbelt")
    _ok(any("Eldre depotrapport utelatt" in m and old[0].name in m for m in logs),
        "eldre versjon er tatt ut, og loggen sier det")
    _ok(_has(names, _DEST + own.name), "filer brukeren selv la til, er urørt")


def test_none(tmp: Path) -> None:
    print("\n[5] Uten depotrapporter")
    d = tmp / "d"
    d.mkdir()
    r, names, logs, mets = _pack(_siard(d / "Uttrekk.siard"), d / "ut")
    _ok(r.success and not _in_pkg(names), "pakken lages som før")
    _ok(not any("depotrapport" in m.lower() for m in logs), "uten støy i loggen")


def test_dialog(tmp: Path) -> None:
    print("\n[6] DIAS-vinduet")
    import gui.dias_dialog as G
    d = tmp / "e"
    d.mkdir()
    siard = _siard(d / "Uttrekk.siard")
    _reports(d / "depotrapporter", "Uttrekk_konvertert", "20261120_080000")
    new = _reports(d / "depotrapporter", "Uttrekk_konvertert", "20261126_101500")
    found = [f for f in G._discover_files(siard) if "depotrapporter" in f["src"]]
    _ok(sorted(f["name"] for f in found) == sorted(p.name for p in new)
        and all(f["dest"] == _DEST + f["name"] and f["tag"] == "auto" for f in found),
        "nyeste sett står i filtreet under repository_operations")
    _ok(not [f for f in G._discover_files(siard, depot=False)
             if "depotrapporter" in f["src"]],
        "skal rapportene lages på nytt i arbeidsflyten, vises ikke de gamle")
    pend = G._PENDING_BY_OP.get("depot_reports") or []
    _ok(len(pend) == 1 and pend[0]["token"] == "depot_reports"
        and pend[0]["folder_id"] == "repo_ops",
        "da står «Depotrapporter (PDF) – lages under kjøringen» i treet i stedet")

    def _show(runtime: bool, ids: list) -> bool:
        return G.DiasParamDialog._show_depot(
            types.SimpleNamespace(_runtime=runtime, _upstream_op_ids=ids))

    _ok(_show(False, ["sha256"]) and not _show(False, ["blob_convert", "depot_reports"])
        and _show(True, ["blob_convert", "depot_reports"]),
        "når pakkingen starter (kontrollvinduet), vises de nylagde rapportene")


def main() -> int:
    threading.Timer(240, lambda: os._exit(3)).start()
    with tempfile.TemporaryDirectory(prefix="diasdepot_") as td:
        tmp = Path(td)
        test_real_names(tmp)
        test_this_run(tmp)
        test_from_disk(tmp)
        test_placeholder_and_stale(tmp)
        test_none(tmp)
        test_dialog(tmp)
    print("\nAlle tester OK")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    except BaseException:
        import traceback
        traceback.print_exc()
        rc = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)
