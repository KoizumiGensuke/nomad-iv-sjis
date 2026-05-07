# nomad-iv-sjis (fixed)

Minimal NOMAD plugin for reading CP932 / Shift-JIS IV CSV files.

## What changed in this fixed version

- The CSV reading logic was moved into the schema class `IVData.normalize(self, archive, logger)`.
- The separate plugin normalizer entry point was removed.
- This matches the intended flow for a schema-driven entry that reads `data_file` and fills `voltage` / `current`.

## Expected CSV structure

- lines 1-5: metadata
- line 6: header `V(V),I(A)`
- line 7+: numeric data

## Quick test

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

Place your CSV file next to `tests/data/test_iv.archive.yaml`, then run:

```bash
nomad parse tests/data/test_iv.archive.yaml > normalized.txt 2> parse.log
```

Then confirm that `normalized.txt` contains `voltage` and `current`.
