use std::{collections::BTreeSet, fs};

use serde_json::Value;
use twinning::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    kernel::{storage::TableStorage, value::KernelValue},
    protocol::postgres::{
        extended_execute::{ExecuteRequest, ExtendedExecuteState},
        extended_parse::{BindRequest, ExtendedParseState, ParseRequest},
        session::SessionLoop,
        simple_query::dispatch_simple_query,
    },
};

use super::assertions::{
    assert_required_sqlstates, assert_sqlstate, assert_unsupported_refusal_sqlstate,
};

const TEXT_OID: u32 = 25;

#[test]
fn psycopg2_fixture_covers_declared_mutation_subset() {
    let fixture_dir = crate::support::canary_fixture_dir_for_test(stringify!(psycopg2_params));
    let fixture_path = fixture_dir.join("ir_equivalence.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(&fixture_path).expect("read psycopg2 ir equivalence fixture"),
    )
    .expect("parse psycopg2 ir equivalence fixture");
    let manifest = crate::support::canary_by_id(stringify!(psycopg2_params));

    let covered_shapes: BTreeSet<String> = fixture["supported_cases"]
        .as_array()
        .expect("supported_cases array")
        .iter()
        .map(|case| {
            case["covers_shape"]
                .as_str()
                .unwrap_or_else(|| {
                    panic!(
                        "covers_shape should be a string in `{}`",
                        fixture_path.display()
                    )
                })
                .to_owned()
        })
        .collect();
    let refusal_shapes: BTreeSet<String> = fixture["refusal_cases"]
        .as_array()
        .expect("refusal_cases array")
        .iter()
        .map(|case| {
            case["near_miss_shape"]
                .as_str()
                .unwrap_or_else(|| {
                    panic!(
                        "near_miss_shape should be a string in `{}`",
                        fixture_path.display()
                    )
                })
                .to_owned()
        })
        .collect();

    let declared_write_shapes: BTreeSet<String> = manifest.write_shapes.iter().cloned().collect();

    assert_eq!(
        covered_shapes.intersection(&declared_write_shapes).count(),
        declared_write_shapes.len(),
        "fixture should pin every declared psycopg2 mutation shape"
    );
    assert!(
        covered_shapes.contains("select_by_pk"),
        "fixture should pin the declared parameterized point-read companion shape"
    );
    assert!(
        refusal_shapes.contains("savepoint"),
        "fixture should preserve the explicit psycopg2 savepoint refusal near miss"
    );
}

#[test]
fn psycopg2_params() {
    let fixture_dir = crate::support::canary_fixture_dir_for_test(stringify!(psycopg2_params));
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for psycopg2_params"
    );

    let manifest = crate::support::canary_by_id(stringify!(psycopg2_params));
    let (catalog, mut backend) = deals_backend();
    let mut parse_state = ExtendedParseState::new();
    let mut execute_state = ExtendedExecuteState::new();
    let mut session = SessionLoop::new();

    let begin = dispatch_simple_query(&mut session, "psycopg2-1", "BEGIN");
    assert_eq!(decode_command_complete(&begin.frames[0]), "BEGIN");
    assert_eq!(
        decode_ready_status(begin.frames.last().expect("ready frame")),
        b'T'
    );

    assert_eq!(
        parse_state.process_parse(
            "psycopg2-1",
            ParseRequest {
                statement_name: String::from("insert_returning"),
                sql: String::from(
                    "INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2) RETURNING deal_name",
                ),
                param_types: vec![String::from("text"), String::from("text")],
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("insert_returning_portal"),
            statement_name: String::from("insert_returning"),
            params: vec![Some(String::from("deal-2")), Some(String::from("Beta"))],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );

    let insert_frames = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("insert_returning_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(
        decode_row_description(&insert_frames[0]),
        vec![(String::from("deal_name"), TEXT_OID)]
    );
    assert_eq!(
        decode_data_row(&insert_frames[1]),
        vec![Some(String::from("Beta"))]
    );
    assert_eq!(decode_command_complete(&insert_frames[2]), "INSERT 0 1");
    assert_eq!(
        decode_ready_status(&execute_state.process_sync(&mut session).frames[0]),
        b'T'
    );

    assert_eq!(
        parse_state.process_parse(
            "psycopg2-1",
            ParseRequest {
                statement_name: String::from("select_by_pk"),
                sql: String::from(
                    "SELECT deal_id, deal_name FROM public.deals WHERE deal_id = $1",
                ),
                param_types: vec![String::from("text")],
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("select_by_pk_portal"),
            statement_name: String::from("select_by_pk"),
            params: vec![Some(String::from("deal-1"))],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );

    let select_frames = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("select_by_pk_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(
        decode_row_description(&select_frames[0]),
        vec![
            (String::from("deal_id"), TEXT_OID),
            (String::from("deal_name"), TEXT_OID),
        ]
    );
    assert_eq!(
        decode_data_row(&select_frames[1]),
        vec![Some(String::from("deal-1")), Some(String::from("Alpha"))]
    );
    assert_eq!(decode_command_complete(&select_frames[2]), "SELECT 1");
    assert_eq!(
        decode_ready_status(&execute_state.process_sync(&mut session).frames[0]),
        b'T'
    );

    assert_eq!(
        parse_state.process_parse(
            "psycopg2-1",
            ParseRequest {
                statement_name: String::from("upsert_pk"),
                sql: String::from(
                    "INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2) ON CONFLICT (deal_id) DO UPDATE SET deal_name = EXCLUDED.deal_name RETURNING deal_name",
                ),
                param_types: vec![String::from("text"), String::from("text")],
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("upsert_pk_portal"),
            statement_name: String::from("upsert_pk"),
            params: vec![
                Some(String::from("deal-1")),
                Some(String::from("Alpha Updated")),
            ],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );

    let upsert_frames = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("upsert_pk_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(
        decode_row_description(&upsert_frames[0]),
        vec![(String::from("deal_name"), TEXT_OID)]
    );
    assert_eq!(
        decode_data_row(&upsert_frames[1]),
        vec![Some(String::from("Alpha Updated"))]
    );
    assert_eq!(decode_command_complete(&upsert_frames[2]), "INSERT 0 1");
    assert_eq!(
        decode_ready_status(&execute_state.process_sync(&mut session).frames[0]),
        b'T'
    );
    assert_eq!(
        backend
            .visible_table("public.deals")
            .expect("visible deals")
            .rows()
            .find(|row| row.values[0] == KernelValue::Text(String::from("deal-1")))
            .expect("upserted deal row")
            .values,
        vec![
            KernelValue::Text(String::from("deal-1")),
            KernelValue::Text(String::from("Alpha Updated")),
        ]
    );

    let commit = dispatch_simple_query(&mut session, "psycopg2-1", "COMMIT");
    assert_eq!(decode_command_complete(&commit.frames[0]), "COMMIT");
    assert_eq!(
        decode_ready_status(commit.frames.last().expect("ready frame")),
        b'I'
    );

    assert_eq!(
        parse_state.process_parse(
            "psycopg2-1",
            ParseRequest {
                statement_name: String::from("insert_duplicate"),
                sql: String::from("INSERT INTO public.deals (deal_id, deal_name) VALUES ($1, $2)"),
                param_types: vec![String::from("text"), String::from("text")],
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("insert_duplicate_portal"),
            statement_name: String::from("insert_duplicate"),
            params: vec![
                Some(String::from("deal-1")),
                Some(String::from("Collision")),
            ],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );

    let duplicate_frames = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("insert_duplicate_portal"),
            max_rows: 0,
        },
    );
    let duplicate_sqlstate = decode_error_field(&duplicate_frames[0], b'C');
    assert_sqlstate(
        duplicate_sqlstate.as_deref(),
        "23505",
        "psycopg2_params unique violation",
    );
    assert_eq!(
        decode_ready_status(&execute_state.process_sync(&mut session).frames[0]),
        b'I'
    );
    assert_required_sqlstates(
        duplicate_sqlstate.iter().map(String::as_str),
        manifest.required_sqlstates.iter().map(String::as_str),
        "psycopg2_params required sqlstates",
    );

    let savepoint = dispatch_simple_query(&mut session, "psycopg2-1", "SAVEPOINT before_retry");
    assert_unsupported_refusal_sqlstate(
        decode_error_field(&savepoint.frames[0], b'C').as_deref(),
        "psycopg2_params savepoint refusal",
    );
    assert_eq!(
        decode_ready_status(savepoint.frames.last().expect("ready frame")),
        b'I'
    );
}

fn deals_backend() -> (Catalog, BaseSnapshotBackend) {
    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            deal_name TEXT NOT NULL
        );
        "#,
    )
    .expect("schema should parse");

    let mut deals = TableStorage::new(
        catalog
            .table("public.deals")
            .expect("deals table should exist"),
    )
    .expect("deals storage should build");
    deals
        .insert_row(vec![
            KernelValue::Text(String::from("deal-1")),
            KernelValue::Text(String::from("Alpha")),
        ])
        .expect("insert seed row");

    let backend = BaseSnapshotBackend::new([deals]).expect("build backend");
    (catalog, backend)
}

fn parse_complete_frame() -> Vec<u8> {
    vec![b'1', 0, 0, 0, 4]
}

fn bind_complete_frame() -> Vec<u8> {
    vec![b'2', 0, 0, 0, 4]
}

fn decode_command_complete(frame: &[u8]) -> String {
    assert_eq!(frame[0], b'C');
    String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("command tag")
}

fn decode_ready_status(frame: &[u8]) -> u8 {
    assert_eq!(frame[0], b'Z');
    frame[5]
}

fn decode_row_description(frame: &[u8]) -> Vec<(String, u32)> {
    assert_eq!(frame[0], b'T');
    let body = &frame[5..];
    let field_count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cursor = 2usize;
    let mut fields = Vec::with_capacity(field_count);

    for _ in 0..field_count {
        let name_end = body[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .expect("field name terminator");
        let name = String::from_utf8(body[cursor..cursor + name_end].to_vec()).expect("field name");
        cursor += name_end + 1;
        cursor += 4;
        cursor += 2;
        let type_oid = u32::from_be_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]);
        cursor += 4;
        cursor += 2;
        cursor += 4;
        cursor += 2;
        fields.push((name, type_oid));
    }

    fields
}

fn decode_data_row(frame: &[u8]) -> Vec<Option<String>> {
    assert_eq!(frame[0], b'D');
    let body = &frame[5..];
    let field_count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cursor = 2usize;
    let mut values = Vec::with_capacity(field_count);

    for _ in 0..field_count {
        let length = i32::from_be_bytes([
            body[cursor],
            body[cursor + 1],
            body[cursor + 2],
            body[cursor + 3],
        ]);
        cursor += 4;
        if length < 0 {
            values.push(None);
            continue;
        }

        let length = length as usize;
        values.push(Some(
            String::from_utf8(body[cursor..cursor + length].to_vec()).expect("value"),
        ));
        cursor += length;
    }

    values
}

fn decode_error_field(frame: &[u8], field_type: u8) -> Option<String> {
    assert_eq!(frame[0], b'E');

    let mut cursor = 5usize;
    while cursor < frame.len() {
        let current_field_type = frame[cursor];
        cursor += 1;
        if current_field_type == 0 {
            return None;
        }

        let value_end = frame[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .expect("field terminator");
        let value =
            String::from_utf8(frame[cursor..cursor + value_end].to_vec()).expect("field value");
        cursor += value_end + 1;

        if current_field_type == field_type {
            return Some(value);
        }
    }

    None
}
