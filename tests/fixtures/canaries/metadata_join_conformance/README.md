# metadata_join_conformance canary

This fixture pins the first catalog-introspection and join-boundary lane.

- `information_schema_public_base_tables` is the exact supported metadata
  query for declared public base tables.
- `select_inner_join_eq_refusal` keeps joins explicitly refused until the plan
  grows a real joined-read IR and executor path.
