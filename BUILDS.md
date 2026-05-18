# Build Size Guardrails

`bd-11kc` adds CI coverage for the four supported binary feature sets. CI runs
each row in an isolated Cargo target directory, builds before testing so
integration tests use the matching `CARGO_BIN_EXE_twinning`, and uploads the
release binary size as a workflow artifact.

Reference local run on 2026-05-17, macOS arm64, release profile:

| Feature set | Command | Release size bytes |
| --- | --- | ---: |
| `rest` | `cargo build --release --no-default-features --features rest` | 4,250,620 |
| `postgres` | `cargo build --release --no-default-features --features postgres` | 2,316,688 |
| `rest+postgres` | `cargo build --release --no-default-features --features rest,postgres` | 4,419,156 |
| `all` | `cargo build --release --features all` | 4,419,156 |

The `all` feature is currently equivalent to `rest+postgres`.
