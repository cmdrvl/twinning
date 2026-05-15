# storage fixtures

The storage suite enforces the tournament-mode budget table from
`docs/PLAN_TWINNING.md`.

Timing and overlay-size metrics are required in CI and fail the gate when any
sample crosses its red line. RSS metrics are enforced when the local platform
can report resident memory through `ps -o rss=`. When that probe is unavailable,
the test records an `unavailable_platform` metric with a reason so the skip is
explicit rather than a warning.
