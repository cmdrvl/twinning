# committed-row snapshot fixture

This fixture pins the committed-state byte surface for snapshot freeze,
restore, and re-freeze.

The row order in `relations.json` is intentionally not primary-key order. The
snapshot layer must canonicalize rows before hashing or producing committed
state bytes.
