#!/usr/bin/env python3
"""Pinned representative extractor canary target for twinning.

This path is frozen by `bd-2te`. The later live-canary wiring bead runs this
file unchanged and is responsible for supplying a real twin DSN plus the
runtime-specific execution harness.
"""

from __future__ import annotations

import json
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent
FIXTURE_PATH = FIXTURE_DIR / "fixture.json"
INPUT_CORPUS_PATH = FIXTURE_DIR / "input_rows.json"


def load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def load_input_corpus() -> dict:
    return json.loads(INPUT_CORPUS_PATH.read_text())


def main() -> int:
    fixture = load_fixture()
    input_corpus = load_input_corpus()
    print(
        json.dumps(
            {
                "entrypoint": fixture["entrypoint"],
                "write_shapes": fixture["write_shapes"],
                "read_shapes": fixture["read_shapes"],
                "mutation_case_names": [
                    case["name"] for case in input_corpus["mutation_cases"]
                ],
                "read_case_names": [case["name"] for case in input_corpus["read_cases"]],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
