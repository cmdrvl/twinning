# doctor detector fixtures

These fixtures back the read-only `twinning doctor` detector catalog. They
freeze the failure signals that must exist before any future `doctor --fix`
surface is exposed.

The detector catalog is intentionally non-mutating. Future fixers must add a
verbatim backup, explicit inverse, and regression fixture before they can be
advertised as available.
