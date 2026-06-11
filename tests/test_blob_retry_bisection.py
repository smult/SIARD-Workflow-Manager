"""
Kontrakt-test for halverende retry (bisection) i BLOB-konverteringen.

Den ekte retry-løkka i _convert_all kaller LibreOffice og kan ikke kjøres uten
LO + reelle filer. Denne testen modellerer SAMME løkke-kontrakt for å sikre:
  • størrelsen halveres hver runde (batch_size/2 → … → 1),
  • filer som lykkes tas ut fortløpende (ingen ny runde for dem),
  • kun i siste runde (størrelse 1) markeres gjenværende filer som endelig feilet,
  • langt færre LO-kall enn å ta alt enkeltvis,
  • løkka terminerer alltid.

Kjør:  python -X utf8 tests/test_blob_retry_bisection.py
"""
from __future__ import annotations


def _bisection_retry(items, batch_size, bad, run_lo_chunk):
    """Faithful kopi av kontrollflyten i _convert_all sin retry-løkke.

    run_lo_chunk(batch) -> (converted_items, failed_items) modellerer at LO
    avbryter HELE batchen hvis den inneholder en «vanskelig» fil (alle i batchen
    feiler), ellers konverteres alt.
    """
    converted: list = []
    failed: list = []
    rounds = 0
    lo_calls = 0

    pending = list(items)
    size = max(1, batch_size // 2)
    while pending:
        final = size <= 1
        rounds += 1
        rbatches = [pending[i:i + size] for i in range(0, len(pending), size)]
        still: list = []
        for batch in rbatches:
            lo_calls += 1
            ok, bad_in_batch = run_lo_chunk(batch)
            converted.extend(ok)
            if final:
                failed.extend(bad_in_batch)     # endelig feilet
            else:
                still.extend(bad_in_batch)       # ny runde
        pending = still
        if final:
            break
        size = max(1, size // 2)
    return converted, failed, rounds, lo_calls


def _make_lo(bad: set):
    def run_lo_chunk(batch):
        # LO avbryter hele batchen hvis den inneholder en vanskelig fil
        if any(it in bad for it in batch):
            return [], list(batch)
        return list(batch), []
    return run_lo_chunk


def main() -> int:
    # 50 filer, 1 vanskelig fil midt i
    items = list(range(50))
    bad = {23}
    conv, fail, rounds, calls = _bisection_retry(items, 50, bad, _make_lo(bad))
    assert sorted(conv) == [i for i in items if i not in bad], "gode ikke konvertert"
    assert fail == [23], fail
    assert rounds <= 7, rounds                 # ~log2(50)+1
    assert calls < len(items), (calls, len(items))   # langt færre enn enkeltvis (50)
    print(f"[ok] 1 vanskelig av 50: {len(conv)} konvertert, {fail} feilet, "
          f"{rounds} runder, {calls} LO-kall (vs 50 enkeltvis)")

    # Flere vanskelige filer spredt utover
    items = list(range(64))
    bad = {5, 30, 47, 60}
    conv, fail, rounds, calls = _bisection_retry(items, 64, bad, _make_lo(bad))
    assert sorted(conv) == [i for i in items if i not in bad]
    assert sorted(fail) == sorted(bad), fail
    assert calls < len(items), (calls, len(items))
    print(f"[ok] 4 vanskelige av 64: {len(conv)} konvertert, {len(fail)} feilet, "
          f"{rounds} runder, {calls} LO-kall (vs 64 enkeltvis)")

    # Ingen vanskelige → alt i FØRSTE runde, ingen feil
    items = list(range(40))
    conv, fail, rounds, calls = _bisection_retry(items, 40, set(), _make_lo(set()))
    assert sorted(conv) == items and fail == []
    assert rounds == 1, rounds
    print(f"[ok] 0 vanskelige av 40: alt konvertert i {rounds} runde, {calls} kall")

    # Alt vanskelig → alle ender som feilet, løkka terminerer
    items = list(range(8))
    bad = set(items)
    conv, fail, rounds, calls = _bisection_retry(items, 8, bad, _make_lo(bad))
    assert conv == [] and sorted(fail) == items
    print(f"[ok] alt vanskelig: terminerer, {len(fail)} feilet i {rounds} runder")

    # Liten batch (size→1 med en gang) oppfører seg som enkeltvis
    items = list(range(3))
    bad = {1}
    conv, fail, rounds, calls = _bisection_retry(items, 1, bad, _make_lo(bad))
    assert sorted(conv) == [0, 2] and fail == [1]
    print(f"[ok] batch_size=1: enkeltvis-fallback fungerer ({rounds} runde)")

    print("\nALLE RETRY-BISECTION-TESTER OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
