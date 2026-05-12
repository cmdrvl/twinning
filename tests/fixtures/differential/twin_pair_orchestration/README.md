# twin-pair orchestration fixture

Prototype production-orchestration manifest fixture for twin-pair migration
proof.

The checked-in fixture keeps the manifest parser contract active, including a
schema bootstrap with a load script. The production runner currently supports
restore-backed endpoints and refuses schema bootstraps with load scripts until
live materialization is implemented.

The referenced replay manifest is translated Postgres-compatible. This fixture
does not claim Oracle/TNS support; non-Postgres endpoint specs are parser
refusals.
