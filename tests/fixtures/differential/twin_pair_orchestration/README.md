# twin-pair orchestration fixture

Prototype production-orchestration manifest fixture for twin-pair migration
proof.

The checked-in fixture keeps the manifest parser and orchestration contract
active, including a schema bootstrap with a deterministic SQL load script. The
production runner supports restore-backed endpoints and schema endpoints whose
load scripts stay within the declared Postgres mutation subset.

The referenced replay manifest is translated Postgres-compatible. This fixture
does not claim Oracle/TNS support; non-Postgres endpoint specs are parser
refusals.
