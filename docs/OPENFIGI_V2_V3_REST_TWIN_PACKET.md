# OpenFIGI v2/v3 REST Twin Packet

This packet implements the generic REST/OpenAPI support needed to run separate
OpenFIGI v2 and v3 twins from the current public schema while preserving existing
Twinning behavior.

Verified contract snapshot:

- Source URL: `https://api.openfigi.com/schema`
- SHA-256: `d83fbc4ad3053c23684ec9c9b24e667d61ef1022e1d98456252f8cba3159d520`
- OpenAPI: `3.0.0`
- `info.version`: `2.0.0`
- Server URL: `https://api.openfigi.com/{basePath}`
- `basePath` enum: `v3`, `v2`
- Paths in the schema are unversioned: `/mapping`, `/filter`, `/search`,
  `/mapping/values/{key}`

Implemented support:

- `POST /mapping` accepts top-level `array<object>` JSON request bodies.
- `POST /filter` and `POST /search` accept object request bodies with array
  interval fields such as `contractSize`.
- `GET /mapping/values/{key}` uses a hidden `key` selector column for seeded
  value-list rows while returning only the public `values` array field.
- `/v2/*` and `/v3/*` mount from the OpenAPI `basePath` server variable when
  `--server-variable basePath=v2` or `--server-variable basePath=v3` is selected.
- Deterministic `x-twinning.response-stubs` can model OpenFIGI batch mapping
  responses for local contract tests. This fixes the generic mutation fallback
  where `POST /v3/mapping` accepted the request shape but returned
  `{"data":null,"warning":null}` because the REST kernel cannot infer FIGI
  provider semantics from `idType`/`idValue`.
- `twinning port` can start separate v2 and v3 twins from the same schema via
  `--from-server-variable basePath=v2 --to-server-variable basePath=v3` and
  compares mounted routes by their unversioned logical OpenAPI path.

Response-stub fixture:

- `tests/fixtures/rest/openfigi_v2_v3/response-stub-schema.yaml` is a small
  OpenFIGI-shaped spec variant for contract tests. It keeps response semantics
  deterministic and local; it does not call OpenFIGI and does not require a real
  API key.
- The fixture matches `POST /v3/mapping` with body
  `[{"idType":"ID_CUSIP","idValue":"037833100"}]` and header
  `X-OPENFIGI-APIKEY: test-token`, then returns a top-level OpenFIGI batch array:

```json
[
  {
    "data": [
      {
        "figi": "BBG000B9XRY4",
        "compositeFIGI": "BBG000B9XRY4",
        "ticker": "AAPL",
        "name": "APPLE INC",
        "securityType": "Common Stock",
        "exchCode": "US"
      }
    ]
  }
]
```

Local proof command shape:

```bash
twinning --json rest \
  --spec tests/fixtures/rest/openfigi_v2_v3/response-stub-schema.yaml \
  --server-variable basePath=v3 \
  --auth-mode shape \
  --run '<local client command using $TWIN_BASE_URL/v3/mapping>'
```

Use separate spec variants or overlays for success, no-match, per-item error,
malformed response, and ambiguity scenarios rather than adding runtime scenario
state to the REST twin.

Compatibility invariants:

- Existing REST exact-path behavior remains the default unless server-variable
  mounting is explicitly selected.
- Existing `--base-prefix` and `x-twinning` routing behavior remains compatible.
- Existing REST report fields remain stable; version identity fields must be
  additive.
- Response stubs run after route matching, auth shape enforcement, and chaos
  injection, and before generic normalize/kernel/encode fallback.
- Existing scalar result serialization for null, boolean, integer, and text
  remains stable.
- JSON and array values remain unsupported in path/query predicates unless a
  later bead defines exact semantics.
- Unsupported shapes outside the implemented subset continue to return explicit
  `unsupported_shape` refusals.
- REST changes must not introduce imports from Postgres/MCP/Snowflake protocol
  modules, and non-REST feature combinations must remain testable.

Implementation lanes:

- Versioning: `src/cli.rs`, `src/config.rs`, `crates/twinning-rest/src/config.rs`,
  `spec.rs`, `policy.rs`, `routes.rs`, `topology.rs`, `listener.rs`, `report.rs`,
  `src/port.rs`.
- Values: `crates/twinning-kernel/src/ir.rs`, `result.rs`, `kernel/value.rs`,
  `kernel/coerce.rs`, `kernel/read.rs`, `kernel/mutation.rs`, `kernel/predicate.rs`,
  `kernel/constraints.rs`, plus REST normalize/encode adapters.
- Route shapes: `crates/twinning-rest/src/routes.rs`, `normalize.rs`,
  `encode.rs`, `seed.rs`, and REST tests.
- Conformance/goldens: `tests/fixtures/rest`, `tests/rest`, REST reports, and
  OpenFIGI canary fixtures.

Current focused coverage:

- `tests/rest/openfigi_v2_v3.rs` verifies schema hash, server-variable
  mounting, executable route kinds, array-body normalization, value-list seeding,
  and key-filtered reads.
- `tests/rest/response_stubs.rs` verifies response-stub parsing, mounted path
  matching, auth-before-stub, chaos-before-stub, body mismatch fallthrough, and
  OpenFIGI-shaped `/v3/mapping` responses.
- `tests/port.rs` verifies separate OpenFIGI v2/v3 port twins compare as the
  same logical operations after selected server-variable mount prefixes are
  stripped.

Minimum gates:

```bash
cargo fmt --check
cargo test --workspace --no-default-features --features rest
cargo test --workspace --no-default-features --features rest,postgres
cargo test --workspace --no-default-features --features all
cargo clippy --workspace --all-targets --features all -- -D warnings
```
