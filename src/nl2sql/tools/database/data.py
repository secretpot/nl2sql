import records
from pydantic import BaseModel
from typing import (
    Any,
    Mapping,
    Optional,
    Sequence,
)
from sqlalchemy import (
    Table,
    Connection,
    func,
    select,
    inspect,
    create_engine
)

from sqlalchemy.sql import sqltypes


class AmbiguousResult(BaseModel):
    sql: str
    results: list
    is_ambiguous: bool
    error: Optional[str] = None


def sample_table(conn: Connection, table: Table, limit: int = 3) -> Sequence:
    dialect = conn.engine.dialect.name
    if dialect not in ('postgresql', 'mysql'):
        raise NotImplementedError(f"Unsupported dialect: {dialect}")
    rand_func = func.random if dialect == 'postgresql' else func.rand
    stmt = select(table).order_by(rand_func()).limit(limit)
    result = conn.execute(stmt).fetchall()
    return result


def execute_sql(
        db: records.Database,
        sql: str,
        fmt: Optional[str] = "markdown",
        params: Optional[Mapping[str, Any]] = None,
) -> str | list[dict] | dict:
    data = db.query(sql, **(params or {}))
    if fmt == "markdown":
        return str(data.dataset)
    else:
        return data.as_dict()


def find_ambiguous_entities(
        db: records.Database,
        table: str,
        model: type[BaseModel],
        keyword: Any,
        *,
        display_cols: Sequence[str] | None = None,
        schema: str | None = None,
) -> AmbiguousResult:
    qualified_table = f"{schema}.{table}" if schema else table
    # Extract model fields
    if issubclass(model, BaseModel):
        fields_map = getattr(model, "model_fields", {}) or {}
    else:
        return AmbiguousResult(
            sql="",
            results=[],
            is_ambiguous=False,
            error="`model` must be a Pydantic BaseModel (class or instance).",
        )
    candidate_names = set(fields_map.keys())
    if not candidate_names:
        return AmbiguousResult(
            sql="",
            results=[],
            is_ambiguous=False,
            error="Model has no fields.",
        )

    # Extract table columns that have string type
    inspector = inspect(create_engine(db.db_url))
    columns = inspector.get_columns(table, schema=schema)
    string_cols = [
        col["name"]
        for col in columns
        if col["name"] in candidate_names and isinstance(col.get("type"), sqltypes.String)
    ]
    if not string_cols:
        return AmbiguousResult(
            sql="",
            results=[],
            is_ambiguous=False,
            error="No available columns among model fields.",
        )

    # Build case-insensitive equality OR conditions
    kw_str = "" if keyword is None else str(keyword)
    binds: dict[str, Any] = {}
    clauses: list[str] = []
    for col in string_cols:
        pname = f"p_{col}"
        clauses.append(f"UPPER({col}) = UPPER(:{pname})")
        binds[pname] = kw_str

    where_clause = " OR ".join(clauses)
    display_clause = ", ".join(display_cols) if display_cols else "*"
    sql_text = f"SELECT {display_clause} FROM {qualified_table} WHERE {where_clause}"

    try:
        results = execute_sql(db, sql_text, fmt="dict", params=binds)
        return AmbiguousResult(
            sql=sql_text,
            results=results,
            is_ambiguous=len(results) > 1,
        )
    except Exception as exc:
        return AmbiguousResult(
            sql=sql_text,
            results=[],
            is_ambiguous=False,
            error=str(exc),
        )
