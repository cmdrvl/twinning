# Lineage-Preserving Scenario Factory

## One-line promise

Turn a cataloged use case into a repeatable, lineage-rich dummy data product
bundle: declared Postgres schema, plausible sample rows, a frozen `twinning`
snapshot, an app/API contract, and evidence showing exactly how the scenario was
produced.

This plan is intentionally separate from
[PLAN_TWINNING.md](./PLAN_TWINNING.md). The scenario factory is a consumer and
orchestrator around `twinning`; it must not widen `twinning`'s core runtime
scope.

---

## Why this exists

Many product, sales, support, and agent-evaluation workflows need a realistic
demo or test data backend without pointing at production:

- a dynamic map layer backed by location rows
- an operational dashboard backed by sample entities
- a CRUD workflow backed by plausible customer/account data
- an extractor or API demo backed by a deterministic catalog slice

Static JSON fixtures are enough for screenshots. They are weak when the app
needs to query, filter, validate, mutate, reset, or preserve provenance.

The scenario factory exists to make those bundles mechanically repeatable:

```text
catalog use case
  -> scenario manifest
  -> schema + declaration + dummy/sample rows
  -> twinning report + snapshot
  -> app/API contract
  -> lineage bundle
  -> optional pack seal
```

The resulting bundle is not a production deployment. It is a deterministic,
inspectable scenario that real app code can run against.

---

## Boundary with `twinning`

`twinning` remains focused on:

- protocol-faithful Postgres runtime behavior for the declared subset
- schema/declaration validation
- materialized committed state
- deterministic `twinning.v0` reports
- deterministic `twinning.snapshot.v0` snapshots
- restore/reset behavior
- embedded verify execution over committed state

The scenario factory owns:

- selecting a cataloged use case
- deciding which tables and fields support a scenario
- generating or sampling dummy rows
- recording lineage
- packaging app/API configuration
- producing scenario bundles
- handing artifacts to `pack` when sealing is needed

The scenario factory must not:

- add map, dashboard, or app-specific semantics to `twinning`
- require `twinning` to emulate PostGIS, tiles, map engines, or web APIs
- put prompt histories, operator notes, or app config into the snapshot hash
- use `twinning` as a truth oracle or policy engine
- bypass `verify`, `benchmark`, `assess`, or `pack` ownership boundaries

---

## Initial use case: Postgres-backed map layer

The first concrete use case is a dummy data backend for a dynamic map web app.

This does **not** mean twinning a map engine. The map app and API stay outside
`twinning`. `twinning` only sees ordinary Postgres tables, for example:

```sql
CREATE TABLE public.locations (
    location_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata_json TEXT
);

CREATE TABLE public.location_events (
    event_id TEXT PRIMARY KEY,
    location_id TEXT NOT NULL REFERENCES public.locations (location_id),
    occurred_at TEXT NOT NULL,
    severity TEXT NOT NULL,
    summary TEXT NOT NULL
);
```

The app/API layer may translate those rows into GeoJSON, layer filters, styling,
or viewport responses. That translation belongs to the scenario factory or app
adapter, not to `twinning`.

### Map-layer query wedge

The first map scenario should stay inside the current/future declared Postgres
subset:

- point lookup by ID
- filtered scan by category/status
- latitude and longitude `BETWEEN` predicates
- `IS NULL` checks for optional fields
- `COUNT(*)` by simple filters if supported by the current canary subset
- explicit refusal for joins, PostGIS functions, geospatial operators, and
  unsupported reflection

If a query shape is not in the `twinning` canary manifest, the scenario factory
must either avoid it or classify it as an expected refusal/SKIP in its own
scenario canaries.

---

## Artifact model

The factory should produce first-class artifacts around `twinning` artifacts.

### `scenario.manifest.v0`

Input contract for building a scenario.

Required fields:

| Field | Meaning |
|------|---------|
| `version` | `scenario.manifest.v0` |
| `scenario_id` | Stable scenario identity |
| `catalog_use_case_id` | Source catalog/use-case identity |
| `scenario_kind` | e.g. `map_layer`, `dashboard`, `crud_demo` |
| `engine` | `postgres` for the first wedge |
| `schema` | Path or generated schema identity |
| `declaration` | Optional `twinning.catalog-declaration.v0` identity |
| `layer_bindings` | App-facing layer/table/field mapping |
| `dummy_data_policy` | Generator or sampler policy |
| `row_count_targets` | Per-table target row counts |
| `privacy_policy` | Synthetic-only, sampled, redacted, or mixed |
| `verify_artifact` | Optional compiled verify artifact |
| `outputs` | Paths for report, snapshot, bundle, and lineage |

### `scenario.lineage.v0`

Production-history artifact. It records how the scenario state was produced.

Required lineage chain:

```text
catalog_use_case
  -> scenario_manifest
  -> schema/declaration
  -> generator_or_sampler
  -> generated_or_sampled_rows
  -> twinning_report
  -> twinning_snapshot
  -> app_api_contract
  -> app_bundle
```

Representative fields:

- `lineage_id`
- `scenario_id`
- `catalog_use_case_hash`
- `manifest_hash`
- `schema_hash`
- `declaration_hash`
- `generator_name`
- `generator_version`
- `generator_config_hash`
- `seed`
- `source_materialization`
- `row_artifact_hashes`
- `twinning_report_hash`
- `twinning_snapshot_hash`
- `api_contract_hash`
- `app_bundle_hash`
- `created_by`
- `created_at`
- `warnings`

### `scenario.bundle.v0`

Portable bundle manifest for consumers.

Required fields:

| Field | Meaning |
|------|---------|
| `version` | `scenario.bundle.v0` |
| `scenario_id` | Stable scenario identity |
| `kind` | Scenario kind |
| `manifest` | Manifest artifact identity |
| `lineage` | Lineage artifact identity |
| `schema` | Schema artifact identity |
| `declaration` | Optional declaration identity |
| `twinning_report` | `twinning.v0` report identity |
| `twinning_snapshot` | `twinning.snapshot.v0` identity |
| `api_contract` | App/API contract identity |
| `app_config` | App configuration identity |
| `verification` | Optional verify report identity |
| `seal` | Optional pack/evidence identity |

### Snapshot hash rule

The scenario lineage artifact must not change the `twinning.snapshot.v0` hash
unless it changes committed-state meaning.

`twinning.snapshot.v0` hash surface:

- schema/catalog identity
- declaration identity if attached
- verify artifact identity if attached
- committed relation contents
- deterministic relation ordering and row counts

`scenario.lineage.v0` hash surface:

- catalog identity
- generator/sampler config
- prompts or recipes if used
- source identities
- app/API contract identity
- twinning report/snapshot identities
- bundle identity
- operator metadata

This separation keeps `twinning` deterministic while preserving rich provenance
around it.

---

## Repository placement

The long-term implementation should live outside `twinning`, for example:

```text
cmdrvl/
  twinning/
    docs/PLAN_TWINNING.md
    docs/PLAN_LINEAGE_SCENARIO_FACTORY.md   # this consumer plan only

  scenario-factory/
    src/
    schemas/
      scenario.manifest.v0.schema.json
      scenario.lineage.v0.schema.json
      scenario.bundle.v0.schema.json
    fixtures/
      map_layer/
    templates/
      map_api/
      map_app/
    docs/
      PLAN_SCENARIO_FACTORY.md

  catalog/
  verify/
  pack/
```

This file may remain in `twinning/docs` as a boundary note and seed plan, but
the code, schemas, templates, and generators should move into a sibling
scenario-factory project once implementation starts.

---

## Ownership table

| Concern | Owner | Notes |
|------|-------|-------|
| Use-case meaning | catalog | Scenario factory references catalog identity; it does not redefine truth. |
| Dummy/sample row decisions | scenario factory | Generation and sampling policy live outside `twinning`. |
| Schema validation | twinning | Normalized Postgres DDL/catalog bootstrap. |
| Catalog subset identity | twinning + declaration layer | Reuse `twinning.catalog-declaration.v0` where appropriate. |
| Runtime behavior | twinning | Protocol/session/kernel behavior only. |
| Snapshot/report | twinning | Existing artifact contracts. |
| Constraint semantics | verify | Scenario factory can request verify but not reinterpret it. |
| App/API contract | scenario factory | Map API, GeoJSON shape, app config, layer metadata. |
| Evidence seal | pack | Optional final archival boundary. |

---

## Command sketch

The factory CLI should orchestrate existing tools instead of adding scenario
commands to `twinning`.

```bash
# Build a deterministic dummy scenario bundle.
scenario-factory build \
  --manifest scenarios/ohio-retail-map.scenario.json \
  --bundle out/ohio-retail-map.bundle.json

# Explain the lineage graph for a built scenario.
scenario-factory lineage \
  --bundle out/ohio-retail-map.bundle.json \
  --json

# Run the scenario's app/API canaries.
scenario-factory test \
  --bundle out/ohio-retail-map.bundle.json \
  --json
```

Internal orchestration may call:

```bash
cargo run --manifest-path ../twinning/Cargo.toml -- postgres \
  --schema out/schema.sql \
  --declaration out/declaration.json \
  --verify out/constraints.verify.json \
  --report out/twinning-report.json \
  --snapshot out/twinning-snapshot.twin \
  --json
```

For source-backed scenarios, the factory may use `--materialize-source-url`
against a disposable or redacted source database. Production data handling must
be explicit in the scenario privacy policy.

---

## Invariants

### S01. Twinning stays focused

No scenario-factory feature may require broadening `twinning` beyond its
canary-defined Postgres subset. If the scenario needs unsupported query shapes,
the scenario must adapt, add an explicit canary to `twinning`, or classify the
shape as unsupported.

### S02. Lineage is explicit

Every scenario bundle must include a lineage artifact that links catalog input,
generation/materialization decisions, `twinning` artifacts, and app/API
contracts.

### S03. Snapshot determinism is preserved

Changing prompts, operator notes, app config, or lineage metadata must not
change the `twinning.snapshot.v0` hash unless committed relation contents or
snapshot semantic inputs change.

### S04. Dummy data policy is auditable

Every row artifact must declare whether it is synthetic, sampled, redacted,
or mixed. Mixed policies must identify which fields are generated vs sampled.

### S05. App contracts are separate

Map layer config, GeoJSON response shape, styling, clustering, and frontend
state live outside `twinning`.

### S06. Verify remains raw

If a scenario uses `verify`, the scenario bundle records raw verify artifact
identities and reports. It does not convert them into product scores.

### S07. Bundles are replayable

Given a scenario bundle and referenced artifacts, an operator should be able to
restore or rebuild the same dummy data product state and explain how it was
made.

---

## Non-goals

This plan does not propose:

- a map engine inside `twinning`
- PostGIS emulation in the v0 center
- tile serving, clustering, routing, or geocoding inside `twinning`
- a production hosting path
- a new truth/policy engine
- adding scenario commands to the `twinning` CLI
- changing the `twinning.snapshot.v0` equality surface for lineage metadata
- replacing catalog, verify, benchmark, assess, or pack

---

## Phased implementation

### Phase 0: Plan and fixture

Deliverables:

- this plan
- one hand-written `scenario.manifest.v0` example for a map layer
- one generated-schema fixture
- one lineage artifact example
- one bundle artifact example

Gate:

- artifacts are self-contained and do not require changes to `twinning`

### Phase 1: Static scenario builder

Deliverables:

- standalone `scenario-factory build` prototype
- JSON schema validation for manifest, lineage, and bundle
- deterministic dummy row generator with seed support
- call into `twinning` bootstrap/snapshot path
- emit lineage and bundle artifacts

Gate:

- same manifest + same seed produces identical row artifacts and identical
  `twinning.snapshot.v0` hash

### Phase 2: Map-layer canary

Deliverables:

- map-layer fixture with `locations` and optional related tables
- API contract fixture for GeoJSON-like output
- canary queries limited to the `twinning` supported subset
- explicit unsupported-shape cases for geospatial functions and joins

Gate:

- scenario canary passes without changing `twinning` runtime scope

### Phase 3: Source-backed sample materialization

Deliverables:

- privacy policy enforcement for sampled/redacted rows
- source identity hashing
- materialization report linkage
- row-level or field-level generation/sampling annotations where needed

Gate:

- lineage can explain every field origin category without exposing production
  secrets in dummy bundles

### Phase 4: Evidence and reuse

Deliverables:

- optional `pack` sealing handoff
- scenario search/index metadata
- reusable app/API templates
- scenario diffing by bundle and snapshot identity

Gate:

- previously built scenarios can be searched by catalog use case, schema hash,
  snapshot hash, app template, and lineage inputs

---

## First map-layer fixture shape

Example `scenario.manifest.v0`:

```json
{
  "version": "scenario.manifest.v0",
  "scenario_id": "ohio-retail-locations-demo",
  "catalog_use_case_id": "catalog://demo/retail-locations",
  "scenario_kind": "map_layer",
  "engine": "postgres",
  "schema": "out/ohio-retail-locations/schema.sql",
  "declaration": "out/ohio-retail-locations/declaration.json",
  "layer_bindings": [
    {
      "layer_id": "locations",
      "table": "public.locations",
      "id_column": "location_id",
      "label_column": "name",
      "latitude_column": "latitude",
      "longitude_column": "longitude",
      "filter_columns": ["category", "status"]
    }
  ],
  "dummy_data_policy": {
    "kind": "synthetic",
    "generator": "deterministic_seeded_v0",
    "seed": "ohio-retail-locations-demo-v0"
  },
  "row_count_targets": {
    "public.locations": 250,
    "public.location_events": 1000
  },
  "privacy_policy": "synthetic_only",
  "outputs": {
    "report": "out/ohio-retail-locations/twinning-report.json",
    "snapshot": "out/ohio-retail-locations/twinning-snapshot.twin",
    "lineage": "out/ohio-retail-locations/scenario-lineage.json",
    "bundle": "out/ohio-retail-locations/scenario-bundle.json"
  }
}
```

---

## Open questions

1. Should the scenario-factory project live as a standalone repo, or inside a
   broader CMD+RVL factory repo?
2. Should lineage artifacts use a generic CMD+RVL lineage schema rather than a
   scenario-specific one?
3. Should the first app/API template be generated code, a static fixture, or a
   separately versioned reusable package?
4. How much row-level provenance is required for purely synthetic data?
5. Should scenario bundles be sealed by default, or only for externally shared
   demos?

---

## Plan alignment summary

This use case is aligned with the `twinning` plan if it remains a consumer:

- `twinning` materializes and snapshots declared Postgres state
- the scenario factory owns use-case selection, dummy rows, app contracts, and
  lineage
- rich provenance lives in sibling artifacts, not inside the snapshot equality
  surface
- any new runtime behavior must still enter `twinning` through the normal
  canary/differential process

The scenario factory can become a valuable downstream product without pulling
`twinning` away from its current Postgres tournament wedge.
