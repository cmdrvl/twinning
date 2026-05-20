# Seed JSONL v0

Seed JSONL is a deterministic artifact pair for moving a twin past empty state.
The exported contract is meant to be handed to an external LLM agent, which
fills a separate seed-data JSONL file. `twinning` does not generate mock data,
call a model, sample existing data, or apply faker logic.

## Contract lines

Each contract line is one JSON object:

```json
{"version":"twinning.seed-contract.v0","kind":"target","twin":"postgres","target_kind":"relation","target":"public.users","fields":[{"name":"id","type":"integer","required":true,"nullable":false}]}
```

The shared envelope fields are:

- `version`: `twinning.seed-contract.v0`
- `kind`: `target`
- `twin`: protocol adapter name, such as `postgres`, `rest`, `snowflake`, or `mcp`
- `target_kind`: protocol-specific target class, such as `relation`, `resource`, `table`, or `tool`
- `target`: protocol-specific target identifier
- `fields`: deterministic field metadata for seedable targets
- `seedable` and `reason`: optional markers for unsupported targets
- `payload`: optional protocol-specific metadata

## Data lines

Each filled seed row is one JSON object:

```json
{"version":"twinning.seed-data.v0","kind":"row","twin":"postgres","target_kind":"relation","target":"public.users","row":{"id":1,"name":"Ada"}}
```

V1 treats seed data as complete supplied state. Protocol adapters may impose
stricter rules, but the shared contract does not apply defaults, invent missing
fields, merge sources, or interpret protocol-specific types.

## Postgres v1 behavior

Postgres supports the first concrete seed path:

```bash
twinning postgres --schema schema.sql \
  --export-seed-contract out/seed-contract.jsonl --json

twinning postgres --schema schema.sql \
  --seed out/seed-data.jsonl --snapshot out/seeded.twin --json
```

The contract emits one `target` line per schema relation. Each field is required
in v1 because the importer treats a seed row as complete supplied state;
nullable fields may use JSON `null`. Defaults are exported as metadata only and
are not evaluated during import.

Filled seed rows are imported in file order through the existing Postgres
mutation and constraint path. That means type coercion, NOT NULL, primary key,
unique, foreign key, and CHECK behavior match the live kernel. Imported rows
become committed state before bootstrap finalization, `--run` listener startup,
or `--serve` listener startup.

Unsupported v1 compositions are explicit refusals:

- `--export-seed-contract` with `--restore`
- `--seed` with `--restore`
- `--seed` with `--materialize-source-url`
