use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::Duration,
};

use serde::Deserialize;
use serde_json::Value;
use twinning::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    ir::{MutationKind, MutationOp, ScalarValue},
    kernel::{mutation::execute_insert, storage::TableStorage, value::KernelValue},
    protocol::postgres::{
        extended_execute::{ExecuteRequest, ExtendedExecuteState},
        extended_parse::{
            BindRequest, ExtendedParseState, ParseRequest, bind_complete_frame,
            parse_complete_frame,
        },
        listener::PgwireListener,
        session::SessionLoop,
        simple_query::dispatch_simple_query,
    },
    result::KernelResult,
};

use super::assertions::{assert_required_sqlstates, assert_sqlstate};

fn string_list(value: &Value, field: &str) -> Result<Vec<String>, String> {
    let items = value[field]
        .as_array()
        .ok_or_else(|| format!("`{field}` should be an array"))?;

    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("`{field}` entries should be strings"))
        })
        .collect()
}

#[test]
fn extractor_canary_fixture_is_pinned_and_manifest_aligned() {
    let fixture_dir = crate::support::canary_fixture_dir_for_test(stringify!(extractor_canary));
    let pinned_fixture_path = fixture_dir.join("fixture.json");
    let input_corpus_path = fixture_dir.join("input_rows.json");
    let entrypoint_path = fixture_dir.join("representative_extractor.py");

    let pinned_fixture: Value = serde_json::from_str(
        &fs::read_to_string(&pinned_fixture_path).expect("read extractor canary fixture"),
    )
    .expect("parse extractor canary fixture");
    let input_corpus: Value = serde_json::from_str(
        &fs::read_to_string(&input_corpus_path).expect("read extractor canary input corpus"),
    )
    .expect("parse extractor canary input corpus");
    let manifest = crate::support::canary_by_id(stringify!(extractor_canary));

    assert_eq!(
        pinned_fixture["version"],
        Value::String("twinning.extractor-canary.fixture.v0".to_owned())
    );
    assert_eq!(
        pinned_fixture["entrypoint"],
        Value::String(
            "tests/fixtures/canaries/extractor_canary/representative_extractor.py".to_owned()
        )
    );
    assert_eq!(
        pinned_fixture["input_corpus"],
        Value::String("tests/fixtures/canaries/extractor_canary/input_rows.json".to_owned())
    );
    assert_eq!(
        pinned_fixture["client"],
        Value::String(manifest.client.clone())
    );
    assert_eq!(
        pinned_fixture["unsupported_policy"],
        Value::String(manifest.unsupported_policy.clone())
    );
    assert_eq!(
        string_list(&pinned_fixture, "session_shapes").expect("session_shapes should be strings"),
        manifest.session_shapes
    );
    assert_eq!(
        string_list(&pinned_fixture, "write_shapes").expect("write_shapes should be strings"),
        manifest.write_shapes
    );
    assert_eq!(
        string_list(&pinned_fixture, "read_shapes").expect("read_shapes should be strings"),
        manifest.read_shapes
    );
    assert_eq!(
        string_list(&pinned_fixture, "required_sqlstates")
            .expect("required_sqlstates should be strings"),
        manifest.required_sqlstates
    );

    assert!(
        entrypoint_path.exists(),
        "missing pinned extractor entrypoint"
    );
    assert!(
        input_corpus_path.exists(),
        "missing deterministic input corpus"
    );
    assert_eq!(
        input_corpus["version"],
        Value::String("twinning.extractor-canary.input.v0".to_owned())
    );
    assert_eq!(
        input_corpus["mutation_cases"]
            .as_array()
            .expect("mutation_cases array")
            .len(),
        7
    );
    assert_eq!(
        input_corpus["read_cases"]
            .as_array()
            .expect("read_cases array")
            .len(),
        2
    );
}

#[test]
fn extractor_canary() {
    let fixture_dir = crate::support::canary_fixture_dir_for_test(stringify!(extractor_canary));
    assert!(
        fixture_dir.exists(),
        "fixture dir should exist for extractor_canary"
    );

    let fixture = load_fixture(&fixture_dir);
    let input_corpus = load_input_corpus(&fixture_dir);
    let extractor_output = run_extractor_entrypoint(fixture.entrypoint_path());
    let manifest = crate::support::canary_by_id(stringify!(extractor_canary));

    assert_eq!(fixture.version, "twinning.extractor-canary.fixture.v0");
    assert_eq!(input_corpus.version, "twinning.extractor-canary.input.v0");
    assert_eq!(
        fixture.input_corpus,
        String::from("tests/fixtures/canaries/extractor_canary/input_rows.json")
    );
    assert_eq!(
        extractor_output.entrypoint, fixture.entrypoint,
        "extractor_canary entrypoint drifted from the pinned fixture"
    );
    assert_eq!(
        extractor_output.write_shapes, fixture.write_shapes,
        "extractor_canary write-shape declaration drifted from the pinned fixture"
    );
    assert_eq!(
        extractor_output.read_shapes, fixture.read_shapes,
        "extractor_canary read-shape declaration drifted from the pinned fixture"
    );
    assert_eq!(
        extractor_output.mutation_case_names,
        input_corpus.mutation_case_names(),
        "extractor_canary mutation-case order drifted from the pinned input corpus"
    );
    assert_eq!(
        extractor_output.read_case_names,
        input_corpus.read_case_names(),
        "extractor_canary read-case order drifted from the pinned input corpus"
    );
    assert_eq!(
        fixture.client, manifest.client,
        "extractor_canary client drifted from the manifest"
    );

    assert_startup_parameter_status_baseline(fixture.client.as_str());

    let protocol_catalog = protocol_mutation_catalog();
    let rich_catalog = rich_catalog();
    let mut mutation_backend = protocol_mutation_backend(&protocol_catalog);
    let mut parse_state = ExtendedParseState::new();
    let mut execute_state = ExtendedExecuteState::new();
    let mut session = SessionLoop::new();
    let mut observed_sqlstates = Vec::new();
    let mut mutation_context = ExecutionContext {
        session_id: "extractor-canary-write",
        session: &mut session,
        parse_state: &mut parse_state,
        execute_state: &mut execute_state,
        catalog: &protocol_catalog,
        backend: &mut mutation_backend,
    };

    let begin = mutation_context.simple_query("BEGIN");
    assert_eq!(decode_command_complete(&begin.frames[0]), "BEGIN");
    assert_eq!(
        decode_ready_status(begin.frames.last().expect("ready frame")),
        b'T'
    );

    for case_name in &extractor_output.mutation_case_names {
        let case = input_corpus.mutation_case(case_name);
        match case.name.as_str() {
            "insert_seed_deal" | "upsert_existing_pk" | "upsert_existing_unique" => {
                let execution = execute_statement(
                    &mut mutation_context,
                    case.name.as_str(),
                    protocol_mutation_sql_for_shape(case.shape.as_str()),
                    protocol_mutation_param_types(),
                    protocol_row_params(&case.row),
                );
                let first_frame = execution.frames.first().expect("command complete");
                assert_eq!(
                    first_frame[0],
                    b'C',
                    "extractor_canary success case `{}` returned tag `{}` instead of command complete; sqlstate={:?}; message={:?}",
                    case.name,
                    first_frame[0] as char,
                    decode_error_field(first_frame, b'C'),
                    decode_error_field(first_frame, b'M')
                );
                assert_eq!(
                    decode_command_complete(first_frame),
                    "INSERT 0 1",
                    "extractor_canary success case `{}` should stay in the declared INSERT/UPSERT lane",
                    case.name
                );
                assert_eq!(
                    execution.ready_status, b'T',
                    "extractor_canary success case `{}` should keep the session in-transaction until COMMIT",
                    case.name
                );
            }
            "reject_missing_required_name" | "reject_duplicate_unique_key" => {
                let execution = execute_statement(
                    &mut mutation_context,
                    case.name.as_str(),
                    protocol_mutation_sql_for_shape(case.shape.as_str()),
                    protocol_mutation_param_types(),
                    protocol_row_params(&case.row),
                );
                let expected = case
                    .expect_sqlstate
                    .as_deref()
                    .expect("protocol refusal case should declare SQLSTATE");
                let actual =
                    decode_error_field(execution.frames.first().expect("error frame"), b'C')
                        .expect("SQLSTATE field");
                assert_sqlstate(
                    Some(actual.as_str()),
                    expected,
                    format!("extractor_canary `{}`", case.name).as_str(),
                );
                observed_sqlstates.push(actual);
                assert_eq!(
                    execution.ready_status, b'E',
                    "extractor_canary refusal case `{}` should leave the session failed until ROLLBACK",
                    case.name
                );
                let rollback = mutation_context.simple_query("ROLLBACK");
                assert_eq!(decode_command_complete(&rollback.frames[0]), "ROLLBACK");
                assert_eq!(
                    decode_ready_status(rollback.frames.last().expect("ready frame")),
                    b'I'
                );

                let begin_again = mutation_context.simple_query("BEGIN");
                assert_eq!(decode_command_complete(&begin_again.frames[0]), "BEGIN");
                assert_eq!(
                    decode_ready_status(begin_again.frames.last().expect("ready frame")),
                    b'T'
                );
            }
            "reject_missing_parent_tenant" => observed_sqlstates.push(run_kernel_failure_probe(
                format!("extractor_canary `{}`", case.name).as_str(),
                &rich_catalog,
                &case.row,
                "23503",
            )),
            "reject_bad_status_and_amount_text" => {
                observed_sqlstates.push(run_kernel_failure_probe_allowing(
                    format!("extractor_canary `{}`", case.name).as_str(),
                    &rich_catalog,
                    &case.row,
                    &["23514", "22P02"],
                ));
            }
            other => panic!("unexpected extractor mutation case `{other}`"),
        }
    }

    let combined_failure = input_corpus.mutation_case("reject_bad_status_and_amount_text");
    observed_sqlstates.push(run_kernel_failure_probe(
        "extractor_canary bad_status focused probe",
        &rich_catalog,
        &DealInputRow {
            amount_text: String::from("100.00"),
            ..combined_failure.row.clone()
        },
        "23514",
    ));
    observed_sqlstates.push(run_kernel_failure_probe(
        "extractor_canary bad_amount focused probe",
        &rich_catalog,
        &DealInputRow {
            status: Some(String::from("open")),
            ..combined_failure.row.clone()
        },
        "22P02",
    ));

    let commit = mutation_context.simple_query("COMMIT");
    assert_eq!(decode_command_complete(&commit.frames[0]), "COMMIT");
    assert_eq!(
        decode_ready_status(commit.frames.last().expect("ready frame")),
        b'I'
    );

    let visible_deals = mutation_backend
        .visible_table("public.deals")
        .expect("visible deals table");
    let visible_rows = visible_deals
        .rows()
        .map(|row| row.values.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        visible_rows,
        vec![vec![
            KernelValue::Text(String::from("deal-002")),
            KernelValue::Text(String::from("alpha-001")),
            KernelValue::Text(String::from("Alpha Unique Rewrite")),
        ]],
        "extractor_canary successful write lane drifted from the pinned UPSERT subset"
    );

    assert_required_sqlstates(
        observed_sqlstates.iter().map(String::as_str),
        manifest.required_sqlstates.iter().map(String::as_str),
        "extractor_canary required sqlstates",
    );

    let mut read_backend = read_backend(&rich_catalog);
    let mut read_parse_state = ExtendedParseState::new();
    let mut read_execute_state = ExtendedExecuteState::new();
    let mut read_session = SessionLoop::new();
    let mut read_context = ExecutionContext {
        session_id: "extractor-canary-read",
        session: &mut read_session,
        parse_state: &mut read_parse_state,
        execute_state: &mut read_execute_state,
        catalog: &rich_catalog,
        backend: &mut read_backend,
    };

    for case_name in &extractor_output.read_case_names {
        let case = input_corpus.read_case(case_name);
        let (sql, param_types, params) = read_query_for_case(case);
        let execution = execute_statement(
            &mut read_context,
            case.name.as_str(),
            sql,
            param_types,
            params,
        );

        assert_eq!(
            decode_row_description(execution.frames.as_slice()),
            vec![String::from("deal_id")],
            "extractor_canary read case `{}` projected columns drifted from the declared subset",
            case.name
        );
        assert_eq!(
            decode_data_rows(execution.frames.as_slice()),
            case.expected_deal_ids
                .iter()
                .map(|deal_id| vec![Some(deal_id.clone())])
                .collect::<Vec<_>>(),
            "extractor_canary read case `{}` rowset drifted from the pinned input corpus",
            case.name
        );
        assert_eq!(
            decode_command_complete(last_command_complete(execution.frames.as_slice())),
            format!("SELECT {}", case.expected_deal_ids.len()),
            "extractor_canary read case `{}` command tag drifted from the declared row count",
            case.name
        );
        assert_eq!(
            execution.ready_status, b'I',
            "extractor_canary read case `{}` should leave the session idle",
            case.name
        );
    }
}

#[derive(Debug, Deserialize)]
struct ExtractorFixture {
    version: String,
    entrypoint: String,
    input_corpus: String,
    client: String,
    write_shapes: Vec<String>,
    read_shapes: Vec<String>,
}

impl ExtractorFixture {
    fn entrypoint_path(&self) -> PathBuf {
        crate::support::repo_root().join(&self.entrypoint)
    }
}

#[derive(Debug, Deserialize)]
struct InputCorpus {
    version: String,
    mutation_cases: Vec<MutationCase>,
    read_cases: Vec<ReadCase>,
}

impl InputCorpus {
    fn mutation_case_names(&self) -> Vec<String> {
        self.mutation_cases
            .iter()
            .map(|case| case.name.clone())
            .collect()
    }

    fn read_case_names(&self) -> Vec<String> {
        self.read_cases
            .iter()
            .map(|case| case.name.clone())
            .collect()
    }

    fn mutation_case(&self, case_name: &str) -> &MutationCase {
        self.mutation_cases
            .iter()
            .find(|case| case.name == case_name)
            .unwrap_or_else(|| panic!("missing extractor mutation case `{case_name}`"))
    }

    fn read_case(&self, case_name: &str) -> &ReadCase {
        self.read_cases
            .iter()
            .find(|case| case.name == case_name)
            .unwrap_or_else(|| panic!("missing extractor read case `{case_name}`"))
    }
}

#[derive(Debug, Deserialize)]
struct MutationCase {
    name: String,
    shape: String,
    expect_sqlstate: Option<String>,
    row: DealInputRow,
}

#[derive(Debug, Clone, Deserialize)]
struct DealInputRow {
    tenant_id: String,
    deal_id: String,
    external_key: String,
    deal_name: Option<String>,
    status: Option<String>,
    amount_text: String,
}

#[derive(Debug, Deserialize)]
struct ReadCase {
    name: String,
    shape: String,
    predicate: ReadPredicate,
    expected_deal_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ReadPredicate {
    column: String,
    operator: String,
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExtractorOutput {
    entrypoint: String,
    write_shapes: Vec<String>,
    read_shapes: Vec<String>,
    mutation_case_names: Vec<String>,
    read_case_names: Vec<String>,
}

struct StatementExecution {
    frames: Vec<Vec<u8>>,
    ready_status: u8,
}

struct ExecutionContext<'a> {
    session_id: &'a str,
    session: &'a mut SessionLoop,
    parse_state: &'a mut ExtendedParseState,
    execute_state: &'a mut ExtendedExecuteState,
    catalog: &'a Catalog,
    backend: &'a mut BaseSnapshotBackend,
}

impl ExecutionContext<'_> {
    fn simple_query(&mut self, sql: &str) -> twinning::protocol::postgres::session::SessionCycle {
        dispatch_simple_query(self.session, self.session_id, sql)
    }
}

fn load_fixture(fixture_dir: &Path) -> ExtractorFixture {
    let fixture_path = fixture_dir.join("fixture.json");
    serde_json::from_str(&fs::read_to_string(&fixture_path).expect("read extractor fixture"))
        .expect("parse extractor fixture")
}

fn load_input_corpus(fixture_dir: &Path) -> InputCorpus {
    let input_corpus_path = fixture_dir.join("input_rows.json");
    serde_json::from_str(
        &fs::read_to_string(&input_corpus_path).expect("read extractor input corpus"),
    )
    .expect("parse extractor input corpus")
}

fn run_extractor_entrypoint(entrypoint: PathBuf) -> ExtractorOutput {
    let output = Command::new("python3")
        .arg(&entrypoint)
        .current_dir(crate::support::repo_root())
        .output()
        .unwrap_or_else(|error| panic!("run `{}`: {error}", entrypoint.display()));

    assert!(
        output.status.success(),
        "extractor entrypoint `{}` failed: stdout={}; stderr={}",
        entrypoint.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("parse extractor stdout as JSON")
}

fn assert_startup_parameter_status_baseline(application_name: &str) {
    let listener = PgwireListener::bind("127.0.0.1", 0).expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let handle = thread::spawn(move || listener.accept("extractor-canary-startup"));

    let mut client = TcpStream::connect(addr).expect("connect");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set timeout");

    write_startup_packet(
        &mut client,
        &[
            ("user", "postgres"),
            ("database", "postgres"),
            ("application_name", application_name),
        ],
    );
    let startup_frames = read_until_ready(&mut client).expect("startup frames");
    assert!(startup_frames.iter().any(|frame| frame[0] == b'R'));
    assert_eq!(
        decode_parameter_statuses(startup_frames.as_slice()),
        BTreeMap::from([
            (String::from("DateStyle"), String::from("ISO, MDY")),
            (
                String::from("application_name"),
                application_name.to_owned(),
            ),
            (String::from("client_encoding"), String::from("UTF8")),
            (String::from("integer_datetimes"), String::from("on")),
            (String::from("server_encoding"), String::from("UTF8")),
            (String::from("server_version"), String::from("16.0")),
            (
                String::from("standard_conforming_strings"),
                String::from("on"),
            ),
        ]),
        "extractor_canary startup parameter-status baseline drifted from the declared pgwire subset"
    );
    assert_eq!(
        decode_ready_status(startup_frames.last().expect("ready frame")),
        b'I'
    );

    write_terminate_message(&mut client);
    handle
        .join()
        .expect("listener thread")
        .expect("serve startup connection");
}

fn protocol_mutation_catalog() -> Catalog {
    parse_postgres_schema(
        r#"
        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            external_key TEXT NOT NULL,
            deal_name TEXT NOT NULL,
            CONSTRAINT deals_external_key_key UNIQUE (external_key)
        );
        "#,
    )
    .expect("protocol mutation schema should parse")
}

fn rich_catalog() -> Catalog {
    parse_postgres_schema(
        r#"
        CREATE TABLE public.tenants (
            tenant_id TEXT PRIMARY KEY
        );

        CREATE TABLE public.deals (
            tenant_id TEXT REFERENCES public.tenants (tenant_id),
            deal_id TEXT PRIMARY KEY,
            external_key TEXT,
            deal_name TEXT NOT NULL,
            status TEXT CHECK (status IN ('open', 'closed')),
            amount NUMERIC CHECK (amount >= 0),
            CONSTRAINT deals_external_key_key UNIQUE (external_key)
        );
        "#,
    )
    .expect("rich extractor schema should parse")
}

fn protocol_mutation_backend(catalog: &Catalog) -> BaseSnapshotBackend {
    let deals = TableStorage::new(
        catalog
            .table("public.deals")
            .expect("deals table should exist"),
    )
    .expect("deals storage should build");

    BaseSnapshotBackend::new([deals]).expect("build protocol mutation backend")
}

fn constraint_backend(catalog: &Catalog) -> BaseSnapshotBackend {
    let mut tenants = TableStorage::new(
        catalog
            .table("public.tenants")
            .expect("tenants table should exist"),
    )
    .expect("tenant storage should build");
    tenants
        .insert_row(vec![KernelValue::Text(String::from("tenant-a"))])
        .expect("insert tenant-a");

    let deals = TableStorage::new(
        catalog
            .table("public.deals")
            .expect("deals table should exist"),
    )
    .expect("deals storage should build");

    BaseSnapshotBackend::new([tenants, deals]).expect("build constraint backend")
}

fn read_backend(catalog: &Catalog) -> BaseSnapshotBackend {
    let mut tenants = TableStorage::new(
        catalog
            .table("public.tenants")
            .expect("tenants table should exist"),
    )
    .expect("tenant storage should build");
    for tenant_id in ["tenant-a", "tenant-b"] {
        tenants
            .insert_row(vec![KernelValue::Text(tenant_id.to_owned())])
            .unwrap_or_else(|error| panic!("insert tenant `{tenant_id}`: {error}"));
    }

    let mut deals = TableStorage::new(
        catalog
            .table("public.deals")
            .expect("deals table should exist"),
    )
    .expect("deals storage should build");
    for row in [
        vec![
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("alpha-001")),
            KernelValue::Text(String::from("Alpha")),
            KernelValue::Null,
            KernelValue::Numeric(String::from("100.00")),
        ],
        vec![
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("deal-002")),
            KernelValue::Text(String::from("alpha-002")),
            KernelValue::Text(String::from("Beta")),
            KernelValue::Text(String::from("open")),
            KernelValue::Numeric(String::from("110.00")),
        ],
        vec![
            KernelValue::Text(String::from("tenant-b")),
            KernelValue::Text(String::from("deal-999")),
            KernelValue::Text(String::from("beta-999")),
            KernelValue::Text(String::from("Gamma")),
            KernelValue::Text(String::from("closed")),
            KernelValue::Numeric(String::from("95.00")),
        ],
    ] {
        deals
            .insert_row(row)
            .expect("insert committed extractor read row");
    }

    BaseSnapshotBackend::new([tenants, deals]).expect("build read backend")
}

fn protocol_mutation_sql_for_shape(shape: &str) -> &'static str {
    match shape {
        "insert_values" => {
            "INSERT INTO public.deals (deal_id, external_key, deal_name) VALUES ($1, $2, $3)"
        }
        "upsert_pk" => {
            "INSERT INTO public.deals (deal_id, external_key, deal_name) VALUES ($1, $2, $3) ON CONFLICT (deal_id) DO UPDATE SET external_key = EXCLUDED.external_key, deal_name = EXCLUDED.deal_name"
        }
        "upsert_unique" => {
            "INSERT INTO public.deals (deal_id, external_key, deal_name) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT deals_external_key_key DO UPDATE SET deal_id = EXCLUDED.deal_id, deal_name = EXCLUDED.deal_name"
        }
        other => panic!("unexpected extractor mutation shape `{other}`"),
    }
}

fn protocol_mutation_param_types() -> &'static [&'static str] {
    &["text", "text", "text"]
}

fn protocol_row_params(row: &DealInputRow) -> Vec<Option<String>> {
    vec![
        Some(row.deal_id.clone()),
        Some(row.external_key.clone()),
        row.deal_name.clone(),
    ]
}

fn read_query_for_case(
    case: &ReadCase,
) -> (&'static str, &'static [&'static str], Vec<Option<String>>) {
    match (
        case.shape.as_str(),
        case.predicate.column.as_str(),
        case.predicate.operator.as_str(),
    ) {
        ("select_filtered_scan", "tenant_id", "eq") => (
            "SELECT deal_id FROM public.deals WHERE tenant_id = $1 LIMIT 50",
            &["text"],
            vec![Some(case.predicate.value.clone().expect(
                "select_filtered_scan should declare a predicate value",
            ))],
        ),
        ("select_is_null", "status", "is_null") => (
            "SELECT deal_id FROM public.deals WHERE status IS NULL",
            &[],
            Vec::new(),
        ),
        other => panic!("unexpected extractor read shape/predicate combination: {other:?}"),
    }
}

fn execute_statement(
    context: &mut ExecutionContext<'_>,
    statement_name: &str,
    sql: &str,
    param_types: &[&str],
    params: Vec<Option<String>>,
) -> StatementExecution {
    assert_eq!(
        context.parse_state.process_parse(
            context.session_id,
            ParseRequest {
                statement_name: statement_name.to_owned(),
                sql: sql.to_owned(),
                param_types: param_types
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect(),
            },
        ),
        vec![parse_complete_frame()],
        "extractor_canary parse refused `{statement_name}` unexpectedly"
    );
    assert_eq!(
        context.parse_state.process_bind(BindRequest {
            portal_name: format!("{statement_name}_portal"),
            statement_name: statement_name.to_owned(),
            params,
            parameter_formats: Vec::new(),
            result_formats: Vec::new(),
        }),
        vec![bind_complete_frame()],
        "extractor_canary bind refused `{statement_name}` unexpectedly"
    );

    let frames = context.execute_state.process_execute(
        context.catalog,
        context.backend,
        context.parse_state,
        ExecuteRequest {
            portal_name: format!("{statement_name}_portal"),
            max_rows: 0,
        },
    );
    let sync = context.execute_state.process_sync(context.session);

    StatementExecution {
        frames,
        ready_status: decode_ready_status(sync.frames.first().expect("ready frame")),
    }
}

fn run_kernel_failure_probe(
    context: &str,
    catalog: &Catalog,
    row: &DealInputRow,
    expected_sqlstate: &str,
) -> String {
    let mut backend = constraint_backend(catalog);
    let result = execute_insert(
        catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("extractor-canary-kernel"),
            table: String::from("public.deals"),
            kind: MutationKind::Insert,
            columns: vec![
                String::from("tenant_id"),
                String::from("deal_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(row.tenant_id.clone()),
                ScalarValue::Text(row.deal_id.clone()),
                ScalarValue::Text(row.external_key.clone()),
                option_to_scalar(&row.deal_name),
                option_to_scalar(&row.status),
                ScalarValue::Text(row.amount_text.clone()),
            ]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: None,
            returning: Vec::new(),
        },
    );
    let KernelResult::Refusal(refusal) = result else {
        panic!("{context} should refuse, got {result:?}");
    };
    assert_sqlstate(Some(refusal.sqlstate.as_str()), expected_sqlstate, context);

    refusal.sqlstate
}

fn run_kernel_failure_probe_allowing(
    context: &str,
    catalog: &Catalog,
    row: &DealInputRow,
    allowed_sqlstates: &[&str],
) -> String {
    let mut backend = constraint_backend(catalog);
    let result = execute_insert(
        catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("extractor-canary-kernel"),
            table: String::from("public.deals"),
            kind: MutationKind::Insert,
            columns: vec![
                String::from("tenant_id"),
                String::from("deal_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(row.tenant_id.clone()),
                ScalarValue::Text(row.deal_id.clone()),
                ScalarValue::Text(row.external_key.clone()),
                option_to_scalar(&row.deal_name),
                option_to_scalar(&row.status),
                ScalarValue::Text(row.amount_text.clone()),
            ]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: None,
            returning: Vec::new(),
        },
    );
    let KernelResult::Refusal(refusal) = result else {
        panic!("{context} should refuse, got {result:?}");
    };
    assert!(
        allowed_sqlstates.contains(&refusal.sqlstate.as_str()),
        "{context}: expected one of [{}], got `{}`",
        allowed_sqlstates.join(", "),
        refusal.sqlstate
    );

    refusal.sqlstate
}

fn option_to_scalar(value: &Option<String>) -> ScalarValue {
    match value {
        Some(value) => ScalarValue::Text(value.clone()),
        None => ScalarValue::Null,
    }
}

fn write_startup_packet(stream: &mut TcpStream, params: &[(&str, &str)]) {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608_u32.to_be_bytes());
    for (name, value) in params {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);

    let mut frame = Vec::new();
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame).expect("write startup");
    stream.flush().expect("flush startup");
}

fn write_terminate_message(stream: &mut TcpStream) {
    let mut frame = Vec::with_capacity(5);
    frame.push(b'X');
    frame.extend_from_slice(&4_i32.to_be_bytes());
    stream.write_all(&frame).expect("write terminate");
    stream.flush().expect("flush terminate");
}

fn read_until_ready(stream: &mut TcpStream) -> std::io::Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    loop {
        let frame = read_backend_frame(stream)?;
        let done = frame[0] == b'Z';
        frames.push(frame);
        if done {
            return Ok(frames);
        }
    }
}

fn read_backend_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag)?;
    let mut length_bytes = [0_u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = i32::from_be_bytes(length_bytes);
    let mut body = vec![0_u8; length as usize - 4];
    stream.read_exact(&mut body)?;

    let mut frame = Vec::with_capacity(length as usize + 1);
    frame.push(tag[0]);
    frame.extend_from_slice(&length_bytes);
    frame.extend_from_slice(&body);
    Ok(frame)
}

fn decode_parameter_statuses(frames: &[Vec<u8>]) -> BTreeMap<String, String> {
    frames
        .iter()
        .filter(|frame| frame[0] == b'S')
        .map(|frame| {
            let body = &frame[5..frame.len() - 1];
            let separator = body
                .iter()
                .position(|byte| *byte == 0)
                .expect("parameter status separator");
            let name = String::from_utf8(body[..separator].to_vec()).expect("status name");
            let value = String::from_utf8(body[separator + 1..].to_vec()).expect("status value");
            (name, value)
        })
        .collect()
}

fn decode_row_description(frames: &[Vec<u8>]) -> Vec<String> {
    let frame = frames
        .iter()
        .find(|frame| frame[0] == b'T')
        .expect("row description frame");
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
        cursor += 4;
        cursor += 2;
        cursor += 4;
        cursor += 2;
        fields.push(name);
    }

    fields
}

fn decode_data_rows(frames: &[Vec<u8>]) -> Vec<Vec<Option<String>>> {
    frames
        .iter()
        .filter(|frame| frame[0] == b'D')
        .map(|frame| {
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
                    String::from_utf8(body[cursor..cursor + length].to_vec()).expect("field value"),
                ));
                cursor += length;
            }

            values
        })
        .collect()
}

fn last_command_complete(frames: &[Vec<u8>]) -> &[u8] {
    frames
        .iter()
        .rev()
        .find(|frame| frame[0] == b'C')
        .expect("command complete frame")
}

fn decode_command_complete(frame: &[u8]) -> String {
    assert_eq!(frame[0], b'C');
    String::from_utf8(frame[5..frame.len() - 1].to_vec()).expect("command tag")
}

fn decode_ready_status(frame: &[u8]) -> u8 {
    assert_eq!(frame[0], b'Z');
    frame[5]
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
