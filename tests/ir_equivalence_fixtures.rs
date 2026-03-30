#![forbid(unsafe_code)]

#[path = "support.rs"]
mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

use serde::Deserialize;
use serde_json::Value;
use twinning::ir::Operation;

const FIXTURE_VERSION: &str = "twinning.ir-equivalence.fixture.v0";

#[derive(Debug, Deserialize)]
struct IrEquivalenceFixture {
    version: String,
    client: String,
    supported_cases: Vec<SupportedCase>,
    refusal_cases: Vec<RefusalCase>,
}

#[derive(Debug, Deserialize)]
struct SupportedCase {
    case_id: String,
    equivalence_group: String,
    covers_shape: String,
    surface: Value,
    expected_ir: Operation,
}

#[derive(Debug, Deserialize)]
struct RefusalCase {
    case_id: String,
    near_miss_shape: String,
    surface: Value,
    expected_ir: Operation,
}

fn load_fixture(canary_id: &str) -> IrEquivalenceFixture {
    let path = support::fixture_dir(canary_id).join("ir_equivalence.json");
    serde_json::from_str(&fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("read IR equivalence fixture `{}`: {error}", path.display())
    }))
    .unwrap_or_else(|error| panic!("parse IR equivalence fixture `{}`: {error}", path.display()))
}

fn supported_shape_set(canary_id: &str) -> BTreeSet<String> {
    let canary = support::canary_by_id(canary_id);
    canary
        .session_shapes
        .iter()
        .chain(canary.write_shapes.iter())
        .chain(canary.read_shapes.iter())
        .cloned()
        .collect()
}

fn client_fixture_map() -> BTreeMap<&'static str, IrEquivalenceFixture> {
    BTreeMap::from([
        ("psql_smoke", load_fixture("psql_smoke")),
        ("psycopg2_params", load_fixture("psycopg2_params")),
        ("sqlalchemy_core", load_fixture("sqlalchemy_core")),
    ])
}

#[test]
fn ir_equivalence_fixtures_align_with_manifest_and_fixture_contract() {
    for (canary_id, fixture) in client_fixture_map() {
        let manifest = support::canary_by_id(canary_id);
        let declared_shapes = supported_shape_set(canary_id);

        assert_eq!(fixture.version, FIXTURE_VERSION);
        assert_eq!(fixture.client, manifest.client);
        assert!(
            !fixture.supported_cases.is_empty(),
            "{canary_id} should pin at least one supported equivalence case"
        );
        assert!(
            !fixture.refusal_cases.is_empty(),
            "{canary_id} should pin at least one refusal near-miss case"
        );

        for case in &fixture.supported_cases {
            assert!(
                declared_shapes.contains(&case.covers_shape),
                "{canary_id} case `{}` covers undeclared shape `{}`",
                case.case_id,
                case.covers_shape
            );
            assert!(
                case.surface.is_object(),
                "{canary_id} case `{}` should pin an object-valued client surface",
                case.case_id
            );
        }

        for case in &fixture.refusal_cases {
            assert!(
                !declared_shapes.contains(&case.near_miss_shape),
                "{canary_id} refusal `{}` should stay outside the declared subset",
                case.case_id
            );
            assert!(
                case.surface.is_object(),
                "{canary_id} refusal `{}` should pin an object-valued client surface",
                case.case_id
            );
        }
    }
}

#[test]
fn shared_equivalence_groups_keep_identical_expected_ir_across_clients() {
    let fixtures = client_fixture_map();
    let mut groups: BTreeMap<String, Vec<(String, Value)>> = BTreeMap::new();

    for fixture in fixtures.values() {
        for case in &fixture.supported_cases {
            groups
                .entry(case.equivalence_group.clone())
                .or_default()
                .push((
                    fixture.client.clone(),
                    serde_json::to_value(&case.expected_ir).expect("serialize expected IR"),
                ));
        }
    }

    let expected_groups = BTreeMap::from([
        (
            "tx_begin",
            BTreeSet::from([
                String::from("psql"),
                String::from("psycopg2"),
                String::from("sqlalchemy_core"),
            ]),
        ),
        (
            "tx_commit",
            BTreeSet::from([String::from("psycopg2"), String::from("sqlalchemy_core")]),
        ),
        (
            "insert_values",
            BTreeSet::from([String::from("psycopg2"), String::from("sqlalchemy_core")]),
        ),
        (
            "select_by_pk",
            BTreeSet::from([String::from("psycopg2"), String::from("sqlalchemy_core")]),
        ),
    ]);

    for (group, expected_clients) in expected_groups {
        let cases = groups
            .get(group)
            .unwrap_or_else(|| panic!("missing shared equivalence group `{group}`"));
        let actual_clients: BTreeSet<String> =
            cases.iter().map(|(client, _)| client.clone()).collect();
        assert_eq!(
            actual_clients, expected_clients,
            "unexpected client set for group `{group}`"
        );

        let canonical = &cases[0].1;
        for (client, value) in &cases[1..] {
            assert_eq!(
                value, canonical,
                "group `{group}` should normalize identically across clients; `{client}` drifted"
            );
        }
    }
}

#[test]
fn refusal_near_miss_fixtures_stay_explicit_and_normalized() {
    for (canary_id, fixture) in client_fixture_map() {
        for case in fixture.refusal_cases {
            let refusal = match case.expected_ir {
                Operation::Refusal(refusal) => refusal,
                other => panic!(
                    "{canary_id} refusal `{}` should pin `Operation::Refusal`, got `{}`",
                    case.case_id,
                    serde_json::to_string(&other).expect("serialize unexpected operation")
                ),
            };

            assert_eq!(
                refusal.code, "unsupported_shape",
                "{canary_id} refusal `{}` should use the controlled unsupported-shape code",
                case.case_id
            );
            assert_eq!(
                refusal.detail.get("shape"),
                Some(&case.near_miss_shape),
                "{canary_id} refusal `{}` should preserve the refused shape token",
                case.case_id
            );
        }
    }
}
