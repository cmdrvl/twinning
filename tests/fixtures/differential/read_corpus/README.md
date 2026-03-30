# read_corpus

Checked-in fixture set for the declared read differential corpus.

Contents:

- `schema.sql` defines the minimal tenant/deal schema used by the read corpus
- `corpus.json` defines seed SQL, the exercised query, and the expected
  rowset or classification for each read case

Current scope:

- declared `select_by_pk`
- declared `select_filtered_scan`
- declared `select_is_null`
- explicit refusal classification for out-of-subset locking reads
- explicit `skip` classification for read/query shapes that exist in the wider
  controlled vocabulary but are not yet in the manifest-backed differential
  subset

The live twin-vs-Postgres executor is still a later lane. This fixture contract
exists now so read/query corpus drift is caught before that runner lands.
