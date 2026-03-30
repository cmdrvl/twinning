# write_corpus

Checked-in fixture set for the declared write differential corpus.

Contents:

- `schema.sql` defines the minimal tenant/deal schema used by the write corpus
- `corpus.json` defines seed SQL, the exercised statement, and the expected
  outcome class for each write case

Current scope:

- declared `INSERT`
- primary-key `ON CONFLICT ... DO UPDATE`
- unique and foreign-key SQLSTATE cases
- explicit refusal classification for out-of-subset `INSERT ... SELECT`

The live twin-vs-Postgres executor is still a later lane. This fixture contract
exists now so corpus drift is caught before that runner lands.
