# differential fixtures

This subtree holds the real-Postgres parity corpora, the first run_once parity
ledger fixture, and the prototype twin-pair migration proof fixture.

- `read_corpus/` and `write_corpus/` pin the declared read/write semantics.
- `parity_ledger/` runs a child process through the twinning pgwire listener and
  records reference-vs-twin observations in a ledger shape.
- `twin_pair_migration_proof/` restores two declared committed-state snapshots
  and records an interface-equivalence proof report with both endpoint
  identities.
