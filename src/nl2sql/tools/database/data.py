import records
from pydantic import BaseModel
from typing import (
    Optional,
    Sequence
)
from sqlalchemy import (
    Table,
    Connection,
    func,
    select,
)


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
        fmt: Optional[str] = "markdown"
) -> str | list[dict] | dict:
    data = db.query(sql)
    if fmt == "markdown":
        return str(data.dataset)
    else:
        return data.as_dict()


def find_ambiguous_entities(
        db: records.Database,
        keyword: str,
        table: str,
        ambiguous_at: list[str],
        display_columns: list[str] = None
) -> AmbiguousResult:
    display_columns = display_columns or ["*"]
    params = [f"UPPER({column}) LIKE '%{keyword.upper()}%'" for column in ambiguous_at]
    sql = f"SELECT {','.join(display_columns)} FROM {table} WHERE {' OR '.join(params)}"

    try:
        results = execute_sql(db, sql, "dict")
        return AmbiguousResult(is_ambiguous=len(results) > 1, results=results, sql=sql)
    except Exception as e:
        return AmbiguousResult(is_ambiguous=False, results=[], error=str(e), sql=sql)

