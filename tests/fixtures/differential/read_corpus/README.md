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
- declared `select_in_list` for non-null literal lists
- declared `select_between`
- declared `aggregate_basic_group_by`
- explicit refusal classification for IN-list NULL members, type-mismatched
  BETWEEN, and out-of-subset locking reads
- explicit refusal classification for empty IN lists, HAVING, windows, and
  subqueries

The live twin-vs-Postgres executor is still a later lane. This fixture contract
exists now so read/query corpus drift is caught before that runner lands.
