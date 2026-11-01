"""Exercise the M0c psycopg2 and SQLAlchemy Core cases against live twinning."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

import psycopg2
import sqlalchemy
from sqlalchemy import Column, MetaData, Numeric, String, Table, bindparam, create_engine, insert, select


def emit(case: str, sqlstate: str | None, rows: list[list[object]], **extra: object) -> None:
    record = {"case": case, "sqlstate": sqlstate, "rows": rows, **extra}
    print(json.dumps(record, sort_keys=True), flush=True)


def observe_psycopg2(
    case: str,
    dsn: str,
    sql: str,
    params: tuple[object, ...],
    *,
    expected_sqlstate: str | None = None,
    connection_mode: str,
) -> tuple[str | None, list[list[object]], int | None]:
    sqlstate: str | None = None
    error_detail: str | None = None
    rows: list[list[object]] = []
    row_count: int | None = None
    connection = None
    try:
        connection = psycopg2.connect(dsn)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                if cursor.description is not None:
                    rows = [list(row) for row in cursor.fetchall()]
                row_count = cursor.rowcount
    except psycopg2.Error as error:
        sqlstate = error.pgcode
        error_detail = error.diag.message_primary or str(error).strip()
    finally:
        if connection is not None:
            connection.close()

    emit(
        case,
        sqlstate,
        rows,
        expected_sqlstate=expected_sqlstate,
        connection_mode=connection_mode,
        row_count=row_count,
        error_detail=error_detail,
    )
    return sqlstate, rows, row_count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    uv_version = subprocess.run(
        ["uv", "--version"], check=True, stdout=subprocess.PIPE, text=True
    ).stdout.strip()
    print(
        "VERSIONS: "
        + json.dumps(
            {
                "psycopg2": psycopg2.__version__,
                "sqlalchemy": sqlalchemy.__version__,
                "uv": uv_version,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    d10_dsn = (
        f"postgresql://u@127.0.0.1:{args.port}/db"
        "?sslmode=disable&gssencmode=disable"
    )
    default_ssl_dsn = f"postgresql://u@127.0.0.1:{args.port}/db"
    failures: list[str] = []

    psycopg_cases = [
        (
            "psycopg2_valid_insert",
            "INSERT INTO public.deals (deal_id, deal_name) VALUES (%s, %s)",
            ("psycopg2-valid-1", "Alpha"),
            None,
            "D10",
            d10_dsn,
        ),
        (
            "psycopg2_check_violation",
            "INSERT INTO public.deals (deal_id, deal_name, amount) VALUES (%s, %s, %s)",
            ("psycopg2-check-1", "Negative", -1),
            "23514",
            "D10",
            d10_dsn,
        ),
        (
            "psycopg2_duplicate_primary_key",
            "INSERT INTO public.deals (deal_id, deal_name) VALUES (%s, %s)",
            ("psycopg2-valid-1", "Duplicate"),
            "23505",
            "D10",
            d10_dsn,
        ),
        (
            "psycopg2_select_by_pk",
            "SELECT deal_id, deal_name FROM public.deals WHERE deal_id = %s",
            ("psycopg2-valid-1",),
            None,
            "D10",
            d10_dsn,
        ),
        (
            "psycopg2_default_sslmode_select_by_pk",
            "SELECT deal_id, deal_name FROM public.deals WHERE deal_id = %s",
            ("psycopg2-valid-1",),
            None,
            "libpq_default_sslmode",
            default_ssl_dsn,
        ),
    ]
    for case, sql, params, expected, connection_mode, dsn in psycopg_cases:
        actual, rows, _ = observe_psycopg2(
            case,
            dsn,
            sql,
            params,
            expected_sqlstate=expected,
            connection_mode=connection_mode,
        )
        if actual != expected:
            failures.append(f"{case}: expected SQLSTATE {expected!r}, observed {actual!r}")
        if case.endswith("select_by_pk") and rows != [["psycopg2-valid-1", "Alpha"]]:
            failures.append(f"{case}: unexpected rows {rows!r}")

    sqlalchemy_dsn = d10_dsn.replace("postgresql://", "postgresql+psycopg2://", 1)
    engine = create_engine(sqlalchemy_dsn)
    deals = Table(
        "deals",
        MetaData(),
        Column("deal_id", String, primary_key=True),
        Column("deal_name", String, nullable=False),
        Column("amount", Numeric),
        schema="public",
    )
    try:
        with engine.begin() as connection:
            result = connection.execute(
                insert(deals).values(deal_id="sqlalchemy-valid-1", deal_name="Beta")
            )
            emit(
                "sqlalchemy_core_insert",
                None,
                [],
                expected_sqlstate=None,
                connection_mode="D10",
                row_count=result.rowcount,
            )
            if result.rowcount != 1:
                failures.append(
                    f"sqlalchemy_core_insert: expected row_count 1, observed {result.rowcount!r}"
                )
    except sqlalchemy.exc.SQLAlchemyError as error:
        original = getattr(error, "orig", None)
        sqlstate = getattr(original, "pgcode", None)
        emit(
            "sqlalchemy_core_insert",
            sqlstate,
            [],
            expected_sqlstate=None,
            connection_mode="D10",
            row_count=None,
            error_detail=getattr(error.orig, "pgerror", str(error)).strip(),
        )
        failures.append(f"sqlalchemy_core_insert: unexpected SQLSTATE {sqlstate!r}")

    try:
        with engine.connect() as connection:
            result = connection.execute(
                select(deals.c.deal_id, deals.c.deal_name).where(
                    deals.c.deal_id == bindparam("deal_id")
                ),
                {"deal_id": "sqlalchemy-valid-1"},
            )
            rows = [list(row) for row in result.fetchall()]
        emit(
            "sqlalchemy_core_select_by_pk",
            None,
            rows,
            expected_sqlstate=None,
            connection_mode="D10",
        )
        if rows != [["sqlalchemy-valid-1", "Beta"]]:
            failures.append(f"sqlalchemy_core_select_by_pk: unexpected rows {rows!r}")
    except sqlalchemy.exc.SQLAlchemyError as error:
        original = getattr(error, "orig", None)
        sqlstate = getattr(original, "pgcode", None)
        emit(
            "sqlalchemy_core_select_by_pk",
            sqlstate,
            [],
            expected_sqlstate=None,
            connection_mode="D10",
        )
        failures.append(f"sqlalchemy_core_select_by_pk: unexpected SQLSTATE {sqlstate!r}")
    finally:
        engine.dispose()

    for failure in failures:
        print(f"DRIVER_FAILURE: {failure}", file=sys.stderr, flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
