"""Run the two psycopg2 operations shown by the MCP demo."""

from __future__ import annotations

import sys

import psycopg2


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: driver.py <connection-string>")

    connection = psycopg2.connect(sys.argv[1])
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO public.deals (deal_id, deal_name) VALUES (%s, %s)",
                ("demo-1", "Example deal"),
            )
        connection.commit()
        print("driver: valid parameterized insert succeeded", flush=True)

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO public.deals (deal_id, deal_name, amount) "
                    "VALUES (%s, %s, %s)",
                    ("demo-2", "Negative amount", -1),
                )
        except psycopg2.Error as error:
            if error.pgcode != "23514":
                raise RuntimeError(
                    f"expected CHECK SQLSTATE 23514, got {error.pgcode!r}"
                ) from error
            connection.rollback()
            print("driver: CHECK violation returned SQLSTATE 23514", flush=True)
        else:
            raise RuntimeError("expected the negative amount to violate CHECK")
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
