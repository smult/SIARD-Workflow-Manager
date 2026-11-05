# IKAMR experimental generic document decryption

Target branch: `feature/generic-decrypt` of `IKAMR/SIARD-Workflow-Manager`.
Base source reviewed: commit `7898caa54128225a63646275f381f628fe0b8847`.

## Goal and evidence

The supplied notes describe the Acos Sosial 1567_005 (1999-2019) extract.
The existing CosDoc job expects Eef_EefID/Eef_EveID, whereas this extract
uses NEF_ELFILER with Nef_DokID/Nef_Versjon/Nef_Variant. The related table
NVE_DOKVERSJON contains nve_passord. Prior investigation found 2,887 failed
Word conversions with stored passwords; 2,886 password values reportedly
matched a filename-derived formula, and one exception did not.

The bundled example rule contains NO real passwords, document content or
personal information. This release was not exercised on the actual SIARD data.

## Installasjon: Pakk ut direkte (ingen installasjonsskript)

Denne ZIP-filen inneholder **komplette** Python-filer, ikke patcher.
Den er laget for IKAMR-branchen `feature/generic-decrypt` basert paa commit
`7898caa54128225a63646275f381f628fe0b8847`.

1. Kjoer `git status` og kontroller at du ikke har ulagrede endringer.
2. Kontroller at du staar paa `feature/generic-decrypt`.
3. Pakk ZIP-filen ut **direkte i repo-roten** med mapper bevart.
   Godta overskriving av de tre eksisterende Python-filene.
4. Kjoer `test-generic-decrypt.bat` (eller `python tests/test_generic_decrypt.py`).
5. Start med `start.bat` eller `python main.py`.
6. Velg **Systemspesifikt** og den nye generiske operasjonen.

Det skal **ikke** kjoeres `apply_delta.py`, `git apply`, `pip install` eller
andre installasjons-/patchskript for aa legge inn denne deltaen.
Applikasjonens ordinære avhengigheter installeres slik som foer.

Deltaen endrer tre eksisterende filer: `gui/operations_panel.py`,
`siard_workflow/core/workflow_io.py` og
`siard_workflow/systemspecific_operations/__init__.py`.
`dependencies.py` er ikke endret; `msoffcrypto-tool` er allerede deklarert
som standardavhengighet for den eksisterende CosDoc-operasjonen.

## GUI card

**Generisk: Las opp dokumenter (JSON-regler)** in **Systemspesifikt**:

- `Regel-JSON`: empty means bundled `siard_workflow/rules/acos_sosial_s1021.json`.
  Browse to a custom JSON rule for another system, no Python change required.
- `Utpakket SIARD-mappe`: choose a preserved extracted directory (with
  `header/metadata.xml` and `content/`). Empty uses `ctx.extracted_path`
  from an earlier **Pakk ut SIARD** step in the *same* workflow run.
- `LOB-fil`: start with `rec10.bin`. Empty means all rows passing the filters.
- `Maks dokumenter`: start with `1`; `0` means all matching documents.
- `Testkjoring`: ON by default. Attempts real Office password decryption,
  then discards decrypted temporary bytes. The SIARD is never modified.
- `Utdatamappe`: required only when test mode is OFF. Must be outside both
  the extracted SIARD directory and any Git repository. A new decrypted
  copy is created; an existing copy is never overwritten.

## Recommended debug workflow for 1567_005

1. Use a **fresh extraction of the original SIARD**, not a copy already
   modified by previous BLOB/PDF conversion steps. Preserve its path.
   The existing pipeline `Pakk ut SIARD` + `Pakk sammen SIARD` with
   `Behold temp-mappe` enabled can leave an extracted working directory.
2. To run the new operation without unpacking again, create a workflow with
   *only the new generic card*. Enter the preserved folder path explicitly.
3. Select `rec10.bin`, `max_files=1`, `Testkjoring=ON`. The log must report
   a stored password, formula match and `decrypted_tested=1`.
4. Test the known formula exception `rec1520.bin` next. It should report
   `formula_mismatch=1`, try the **stored** value first, and on
   `InvalidKeyError` retry the calculated formula value. The expected result
   for this documented exception is `formula_fallback_success=1`.
5. Once a sample works, clear `LOB-fil` and increase the limit (first 10,
   then 100, then 0). Examine counts for unsupported, missing, unencrypted,
   PDF, and failed files.
6. With test mode OFF and a secure external output directory, decrypted
   Office *copies* are saved for manual opening and further verification.
7. Rerun indefinitely against the same preserved extraction. No second ZIP
   extraction is needed for the new operation.

**Important:** `Pakk ut SIARD` still unpacks on new independent workflow runs.
This delta does not change that global behavior. Explicit `Utpakket SIARD-mappe`
is the reuse mechanism for the new operation. Do not put a modified workspace
through a new pipeline without reviewing the state of earlier operations.

## Rule JSON schema (version 1)

Source and lookup tables and columns are resolved dynamically using
`header/metadata.xml`; XML `c1`, `c2`, etc. are derived from actual column
order, not hard-coded. LOBs are resolved from the column's `file` attribute
and `<lobFolder>`, not by guessing an ID from `recNNN.bin`.

- `source.table`, `lob_column`, `filename_column`
- `source.where_equals`: mapping of column names to equality filters
- `source.extensions`: list of filename extensions
- `lookup.table`, `value_column`, `join`: arbitrary number of named source-
  target column pairs
- `formula.input_column`, `when_regex`, `operators`
- `formula.use`: `fallback_if_stored_empty` (original policy) or
  `fallback_if_stored_rejected` (try the stored value first, then retry the
  formula value **only** if msoffcrypto raises `InvalidKeyError`); both use the
  calculated value when the stored value is blank. A successful stored value
  is never replaced, and identical stored/calculated values are not retried.
- `formula.check_against_stored`: compare generated and stored values

Supported formula operators: `basename`, `stem`, `reverse`, `upper`, `lower`,
`{"prepend": "R"}`, `{"append": "..."}`, `{"strip_suffix": "..."}` and
`{"replace": {"old": "...", "new": "..."}}`. Arbitrary Python execution or
`eval` in rule files is intentionally disallowed.

The stored database password always gets the first attempt. Formula values are
fallback candidates according to `formula.use`. For the included Acos Sosial
rule, `fallback_if_stored_rejected` supports the manually verified exception
`rec1520.bin`: a rejected stored password is followed by the calculated value.
The log records `formula_fallback_attempted` and `formula_fallback_success`,
never the password text. A formula match does not prove that the password
opens a document; only a successful decryption attempt establishes that.

## Explicit limitations in this first delta

- Office decryption uses `msoffcrypto-tool`. Some legacy or custom encryption
  schemes may not be supported. A failed decryption is recorded as failure;
  it is not silently copied as a successful result.
- In test mode, decrypted bytes go into a temporary file and are removed
  automatically. The operating system may use disk-backed temp storage.
  Ensure the machine's configured TEMP location satisfies your security policy.
- A decrypted Office signature is verified, but this does not establish that
  the document renders correctly. Open sample outputs manually in Word or LO.
- This pilot **does not replace LOBs in SIARD, update XML digest/length fields,
  perform mail merge, convert to PDF/A, or repack the decrypted copies**.
  Those steps should only be connected once source-specific verification is
  complete. This is deliberate to preserve original archive evidence.
- Source files that already contain PDF (`%PDF-`) are skipped despite their
  legacy `.doc` filename, because the notes describe metadata inconsistency
  following previous conversions.
- No passwords are printed or stored in results, CSV, log, JSON rule, or Git.
  The chosen output directory may contain sensitive material: do not commit it.
- The ZIP was tested using synthetic SIARD metadata/table/LOB fixtures, a
  mock Office crypto adapter, idempotent patcher tests, and Python syntax
  checks. It was not tested on the user's Windows GUI or real encrypted docs.

## Other screenshot observations (out of scope)

- PyMuPDF metadata-generation failure is a separate Python/package install
  issue, not required for this decryption pilot. Full pip logs would be needed
  to diagnose it.
- Ghostscript is optional for the described PDF/A pipeline.
- ESET CLI was configured in the notes, but program dependency checks may
  still show 'Defender/ClamAV missing': that checker needs its own update.

## Git

Do not put SIARD, extracted XML/LOBs, decrypted documents or password reports
in Git. After local testing: inspect `git diff`, then commit only the code,
rule preset, tests and documentation onto `feature/generic-decrypt`.
