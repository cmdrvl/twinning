# twin-pair orchestration fixture

Prototype production-orchestration manifest fixture for twin-pair migration
proof.

The manifest is intentionally parsed and validated only. It does not boot
twins, replay queries, read target evidence artifacts, or seal bundles.

The referenced replay manifest is translated Postgres-compatible. This fixture
does not claim Oracle/TNS support; non-Postgres endpoint specs are parser
refusals.
