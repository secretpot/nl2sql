from os import sep
from typing import (
    Any,
    Sequence,
    FrozenSet
)
from sqlalchemy import (
    Table,
    Engine,
    Connection,
    func,
    text,
    select
)
from nl2sql.tools.database.card.stringifier import (
    stringify_value
)

from nl2sql.utils.path import fpd


def _random_order_expr(conn: Connection) -> Any:
    try:
        dialect = getattr(conn, "engine", None)
        dialect_name = dialect.dialect.name if dialect else conn.dialect.name
    except Exception:
        dialect_name = "default"

    dialect_name = (dialect_name or "default").lower()

    if dialect_name in ("postgresql", "sqlite", "duckdb", "redshift", "snowflake", "trino"):
        return func.random()
    elif dialect_name in ("mysql", "mariadb"):
        return func.rand()
    elif dialect_name in ("mssql", "sqlserver"):
        return func.newid()  # ORDER BY NEWID()
    elif dialect_name in ("oracle",):
        return func.dbms_random.right()
    else:
        return func.random()


def sample_column_examples(
        conn: Connection,
        table: Table,
        col_name: str,
        limit: int = 0,
        random: bool = False
) -> list[str]:
    if limit <= 0:
        return []

    col = table.c[col_name]
    rand_expr = _random_order_expr(conn)

    stmt = (
        select(col)
        .where(col.isnot(None))
        .order_by(rand_expr)
        .limit(limit)
    ) if random else (
        select(col)
        .where(col.isnot(None))
        .limit(limit)
    )

    try:
        rows = conn.execute(stmt).fetchall()
        return [stringify_value(r[0]) for r in rows if r and r[0] is not None]
    except Exception:
        return []
