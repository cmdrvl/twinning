#![forbid(unsafe_code)]

pub use twinning::{catalog, kernel};

#[path = "../src/kernel/constraints.rs"]
mod constraints;

use twinning::{
    catalog::{Catalog, TableCatalog, parse_postgres_schema},
    kernel::{storage::TableStorage, value::KernelValue},
};

#[test]
fn valid_insert_row_passes_constraint_checks() {
    let fixture = deals_fixture();

    constraints::enforce_insert_constraints(
        fixture.deals_table(),
        &fixture.deals_storage,
        &[
            KernelValue::Text(String::from("deal-100")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("unique-100")),
            KernelValue::Text(String::from("Valid Deal")),
            KernelValue::Null,
            KernelValue::Integer(100),
        ],
        |name| fixture.lookup_table(name),
    )
    .expect("valid row should satisfy declared constraints");
}

#[test]
fn not_null_violation_maps_to_23502() {
    let fixture = deals_fixture();

    let error = constraints::enforce_insert_constraints(
        fixture.deals_table(),
        &fixture.deals_storage,
        &[
            KernelValue::Text(String::from("deal-101")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("unique-101")),
            KernelValue::Null,
            KernelValue::Text(String::from("open")),
            KernelValue::Integer(100),
        ],
        |name| fixture.lookup_table(name),
    )
    .expect_err("missing required deal_name should violate not-null");

    assert_eq!(error.sqlstate(), constraints::NOT_NULL_VIOLATION_SQLSTATE);
}

#[test]
fn foreign_key_violation_maps_to_23503() {
    let fixture = deals_fixture();

    let error = constraints::enforce_insert_constraints(
        fixture.deals_table(),
        &fixture.deals_storage,
        &[
            KernelValue::Text(String::from("deal-102")),
            KernelValue::Text(String::from("tenant-missing")),
            KernelValue::Text(String::from("unique-102")),
            KernelValue::Text(String::from("Broken Parent")),
            KernelValue::Text(String::from("open")),
            KernelValue::Integer(100),
        ],
        |name| fixture.lookup_table(name),
    )
    .expect_err("missing tenant should violate foreign key");

    assert_eq!(
        error.sqlstate(),
        constraints::FOREIGN_KEY_VIOLATION_SQLSTATE
    );
}

#[test]
fn unique_violation_maps_to_23505() {
    let fixture = deals_fixture();

    let error = constraints::enforce_insert_constraints(
        fixture.deals_table(),
        &fixture.deals_storage,
        &[
            KernelValue::Text(String::from("deal-103")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-001")),
            KernelValue::Text(String::from("Duplicate Unique")),
            KernelValue::Text(String::from("open")),
            KernelValue::Integer(100),
        ],
        |name| fixture.lookup_table(name),
    )
    .expect_err("duplicate external key should violate unique");

    assert_eq!(error.sqlstate(), constraints::UNIQUE_VIOLATION_SQLSTATE);
}

#[test]
fn check_violation_maps_to_23514() {
    let fixture = deals_fixture();

    let error = constraints::enforce_insert_constraints(
        fixture.deals_table(),
        &fixture.deals_storage,
        &[
            KernelValue::Text(String::from("deal-104")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("unique-104")),
            KernelValue::Text(String::from("Bad Status")),
            KernelValue::Text(String::from("pending")),
            KernelValue::Integer(-5),
        ],
        |name| fixture.lookup_table(name),
    )
    .expect_err("bad status and negative amount should violate checks");

    assert_eq!(error.sqlstate(), constraints::CHECK_VIOLATION_SQLSTATE);
}

struct DealsFixture {
    catalog: Catalog,
    tenants_storage: TableStorage,
    deals_storage: TableStorage,
}

impl DealsFixture {
    fn deals_table(&self) -> &TableCatalog {
        self.catalog.table("public.deals").expect("deals table")
    }

    fn lookup_table(&self, name: &str) -> Option<&TableStorage> {
        match name {
            "public.tenants" => Some(&self.tenants_storage),
            "public.deals" => Some(&self.deals_storage),
            _ => None,
        }
    }
}

fn deals_fixture() -> DealsFixture {
    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.tenants (
            tenant_id TEXT PRIMARY KEY
        );

        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT REFERENCES public.tenants (tenant_id),
            external_key TEXT UNIQUE,
            deal_name TEXT NOT NULL,
            status TEXT CHECK (status IN ('open', 'closed')),
            amount INTEGER CHECK (amount >= 0)
        );
        "#,
    )
    .expect("schema should parse");

    let mut tenants_storage =
        TableStorage::new(catalog.table("public.tenants").expect("tenants table"))
            .expect("tenant storage should build");
    tenants_storage
        .insert_row(vec![KernelValue::Text(String::from("tenant-a"))])
        .expect("seed tenant");

    let mut deals_storage = TableStorage::new(catalog.table("public.deals").expect("deals table"))
        .expect("deal storage should build");
    deals_storage
        .insert_row(vec![
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-001")),
            KernelValue::Text(String::from("Alpha")),
            KernelValue::Null,
            KernelValue::Integer(100),
        ])
        .expect("seed deal");

    DealsFixture {
        catalog,
        tenants_storage,
        deals_storage,
    }
}
