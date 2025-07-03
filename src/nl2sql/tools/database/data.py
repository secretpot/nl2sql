import records
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
    if len(data) == 1:
        return dict(data[0])
    else:
        return list(map(lambda x: dict(x), data))
