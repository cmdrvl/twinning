use std::{collections::BTreeSet, fs};

use serde_json::Value;
use twinning::{
    backend::BaseSnapshotBackend,
    catalog::parse_postgres_schema,
    kernel::storage::TableStorage,
    protocol::postgres::{
        extended_execute::{DescribeTarget, ExecuteRequest, ExtendedExecuteState},
        extended_parse::{BindRequest, ExtendedParseState, ParseRequest},
        session::SessionLoop,
    },
};

use super::assertions::assert_unsupported_refusal_sqlstate;

const TEXT_OID: u32 = 25;
const PUBLIC_BASE_TABLES_SQL: &str = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name";
const JOIN_SQL: &str = "SELECT deals.deal_id, tenants.tenant_name FROM public.deals JOIN public.tenants ON deals.tenant_id = tenants.tenant_id WHERE deals.deal_id = $1";

#[test]
fn metadata_join_fixture_covers_declared_manifest_shapes() {
    let fixture_dir =
        crate::support::canary_fixture_dir_for_test(stringify!(metadata_join_conformance));
    let fixture_path = fixture_dir.join("ir_equivalence.json");
    let fixture: Value = serde_json::from_str(
        &fs::read_to_string(&fixture_path).expect("read metadata/join fixture"),
    )
    .expect("parse metadata/join fixture");
    let manifest = crate::support::canary_by_id(stringify!(metadata_join_conformance));

    let supported_shapes = fixture_shapes(&fixture, "supported_cases", "covers_shape");
    let refusal_shapes = fixture_shapes(&fixture, "refusal_cases", "near_miss_shape");
    let manifest_read_shapes = manifest
        .read_shapes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    assert_eq!(
        supported_shapes
            .union(&refusal_shapes)
            .cloned()
            .collect::<BTreeSet<_>>(),
        manifest_read_shapes,
        "metadata/join fixture should pin every declared conformance read shape"
    );
    assert!(
        supported_shapes.contains("information_schema_public_base_tables"),
        "fixture should prove the exact supported information_schema shape"
    );
    assert!(
        refusal_shapes.contains("select_inner_join_eq_refusal"),
        "fixture should preserve the explicit join refusal shape"
    );
}

#[test]
fn metadata_join_conformance() {
    let fixture_dir =
        crate::support::canary_fixture_dir_for_test(stringify!(metadata_join_conformance));
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for metadata_join_conformance"
    );

    let (catalog, mut backend) = conformance_backend();
    let mut parse_state = ExtendedParseState::new();
    let mut execute_state = ExtendedExecuteState::new();
    let mut session = SessionLoop::new();

    assert_eq!(
        parse_state.process_parse(
            "metadata-join-1",
            ParseRequest {
                statement_name: String::from("public_base_tables"),
                sql: String::from(PUBLIC_BASE_TABLES_SQL),
                param_types: Vec::new(),
            },
        ),
        vec![parse_complete_frame()]
    );
    let metadata_describe = execute_state.process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Statement(String::from("public_base_tables")),
    );
    assert_eq!(
        decode_row_description(&metadata_describe[1]),
        vec![(String::from("table_name"), TEXT_OID)]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("public_base_tables_portal"),
            statement_name: String::from("public_base_tables"),
            params: Vec::new(),
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );
    let metadata_frames = execute_state.process_execute(
        &catalog,
        &mut backend,
        &parse_state,
        ExecuteRequest {
            portal_name: String::from("public_base_tables_portal"),
            max_rows: 0,
        },
    );
    assert_eq!(
        decode_row_description(&metadata_frames[0]),
        vec![(String::from("table_name"), TEXT_OID)]
    );
    assert_eq!(
        decode_data_row(&metadata_frames[1]),
        vec![Some(String::from("deals"))]
    );
    assert_eq!(
        decode_data_row(&metadata_frames[2]),
        vec![Some(String::from("tenants"))]
    );
    assert_eq!(decode_command_complete(&metadata_frames[3]), "SELECT 2");
    assert_eq!(
        decode_ready_status(&execute_state.process_sync(&mut session).frames[0]),
        b'I'
    );

    assert_eq!(
        parse_state.process_parse(
            "metadata-join-1",
            ParseRequest {
                statement_name: String::from("inner_join"),
                sql: String::from(JOIN_SQL),
                param_types: vec![String::from("text")],
            },
        ),
        vec![parse_complete_frame()]
    );
    assert_eq!(
        parse_state.process_bind(BindRequest {
            portal_name: String::from("inner_join_portal"),
            statement_name: String::from("inner_join"),
            params: vec![Some(String::from("deal-001"))],
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()]
    );
    let join_refusal = execute_state.process_describe(
        &catalog,
        &parse_state,
        DescribeTarget::Portal(String::from("inner_join_portal")),
    );
    assert_unsupported_refusal_sqlstate(
        Some(decode_error_sqlstate(&join_refusal[0]).as_str()),
        "metadata_join_conformance join refusal",
    );
    assert_eq!(
        decode_error_field(&join_refusal[0], b'V').as_deref(),
        Some("unsupported_shape")
    );
    assert!(
        decode_error_field(&join_refusal[0], b'D')
            .expect("refusal detail")
            .contains("shape=select_join")
    );
}

fn fixture_shapes(fixture: &Value, section: &str, shape_key: &str) -> BTreeSet<String> {
    fixture[section]
        .as_array()
        .unwrap_or_else(|| panic!("{section} should be an array"))
        .iter()
        .map(|case| {
            case[shape_key]
                .as_str()
                .unwrap_or_else(|| panic!("{shape_key} should be a string"))
                .to_owned()
        })
        .collect()
}

fn conformance_backend() -> (twinning::catalog::Catalog, BaseSnapshotBackend) {
    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.tenants (
            tenant_id TEXT PRIMARY KEY,
            tenant_name TEXT NOT NULL
        );

        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL REFERENCES public.tenants(tenant_id),
            deal_name TEXT NOT NULL
        );
        "#,
    )
    .expect("schema should parse");
    let storages = catalog
        .tables
        .iter()
        .map(TableStorage::new)
        .collect::<Result<Vec<_>, _>>()
        .expect("table storage should build");
    let backend = BaseSnapshotBackend::new(storages).expect("backend should build");
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
        if length == -1 {
            values.push(None);
            continue;
        }

        let length = length as usize;
        values.push(Some(
            String::from_utf8(body[cursor..cursor + length].to_vec()).expect("field value"),
        ));
        cursor += length;
    }

    values
}

fn decode_ready_status(frame: &[u8]) -> u8 {
    assert_eq!(frame[0], b'Z');
    frame[5]
}

fn decode_error_sqlstate(frame: &[u8]) -> String {
    decode_error_field(frame, b'C').expect("error SQLSTATE")
}

fn decode_error_field(frame: &[u8], wanted: u8) -> Option<String> {
    assert_eq!(frame[0], b'E');
    let body = &frame[5..];
    let mut cursor = 0usize;

    while body[cursor] != 0 {
        let code = body[cursor];
        cursor += 1;
        let value_end = body[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .expect("value terminator");
        let value =
            String::from_utf8(body[cursor..cursor + value_end].to_vec()).expect("field value");
        cursor += value_end + 1;
        if code == wanted {
            return Some(value);
        }
    }

    None
}
