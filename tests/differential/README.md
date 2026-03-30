# differential corpora

These modules are the checked-in differential corpus harness for the
real-Postgres parity lane named in the plan.

Current state:

- read and write corpus entry tests both run by default against the landed
  twin-side normalization and kernel surfaces
- the shared runner still carries `TWINNING_DIFF_POSTGRES_URL` so a future
  live-target comparison path can reuse the same checked-in corpora
- fixture directories are checked in and exercised so layout drift shows up
  early

The purpose of this layout is to keep the checked-in corpus contract active now
instead of letting the differential suite become an afterthought.
