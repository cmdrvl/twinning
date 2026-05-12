# differential fixtures

This subtree holds the real-Postgres parity corpora and the first run_once
parity ledger fixture.

- `read_corpus/` and `write_corpus/` pin the declared read/write semantics.
- `parity_ledger/` runs a child process through the twinning pgwire listener and
  records reference-vs-twin observations in a ledger shape.
