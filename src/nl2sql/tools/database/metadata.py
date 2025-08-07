from os.path import sep
from typing import Any
from os import sep
from pydantic import (
    BaseModel,
    PrivateAttr
)
from sqlalchemy import (
    MetaData,
    Inspector,
    Connection,
    text,
    inspect
)

from nl2sql.utils.path import fpd
from nl2sql.utils.strings import read_file_to_str
from nl2sql.tools.database.data import sample_table


class Metadata(BaseModel):
    table_name: str
    description: str
    ddl: str
    samples: list[str]
    _doc: str = PrivateAttr("")

    def model_post_init(self, context: Any, /) -> None:
        header = f"-- {self.table_name}: {self.description}"
        footer = f"-- Example Values:\n{'\n'.join(self.samples)}" if self.samples else ""
        self._doc = f"{header}\n{self.ddl}\n{footer}"

    def __str__(self) -> str:
        return self._doc

    @property
    def doc(self):
        return self._doc

    @classmethod
    def query(cls, conn: Connection, table_name: str, schema: str = None, sample_limit: int = 3) -> "Metadata":
        inspector = inspect(conn.engine)
        metadata = MetaData()
        metadata.reflect(bind=inspector.engine)
        table = metadata.tables.get(table_name)
        comment = inspector.get_table_comment(table_name).get("text") or ""
        ddl = _query_ddl_with_inline_comment(conn, inspector, table_name, schema)
        samples = sample_table(conn, table, limit=sample_limit)
        table_context = cls(
            table_name=table_name,
            description=comment,
            ddl=ddl,
            samples=map(str, map(tuple, samples))
        )
        return table_context

    @classmethod
    def query_or_default(
            cls,
            conn: Connection,
            table_name: str,
            schema: str = None,
            sample_limit: int = 3
    ) -> "Metadata":
        try:
            return cls.query(conn, table_name, schema, sample_limit)
        except Exception as e:
            return cls(
                table_name=table_name,
                description=f"Can't get schema info for table {table_name}: {e}",
                ddl="",
                samples=[]
            )


def _build_ddl_string(
        table_name: str,
        columns: list,
        pk_info: dict,
        fk_info: list,
        comments: dict
) -> str:
    ddl_lines = [f"CREATE TABLE {table_name} ("]

    for col in columns:
        name = col["name"]
        col_type = str(col["type"])
        nullable = col.get("nullable", True)
        default = col.get("default", None)
        comment = comments.get(name, "")

        line = f"    {name} {col_type}"
        if default is not None:
            line += f" DEFAULT {default}"
        if not nullable:
            line += " NOT NULL"
        line += ","
        if comment:
            line += f" -- {comment}"
        ddl_lines.append(line)

    if pk_info and pk_info.get("constrained_columns"):
        ddl_lines.append(
            f"    PRIMARY KEY ({', '.join(pk_info['constrained_columns'])}),"
        )

    for fk in fk_info:
        ddl_lines.append(
            f"    FOREIGN KEY ({', '.join(fk['constrained_columns'])}) "
            f"REFERENCES {fk['referred_table']} ({', '.join(fk['referred_columns'])}),"
        )

    if ddl_lines[-1].endswith(','):
        ddl_lines[-1] = ddl_lines[-1][:-1]

    ddl_lines.append(");")
    return "\n".join(ddl_lines)


def _generate_postgres_ddl(
        conn: Connection, inspector: Inspector,
        table_name: str, schema: str = None
) -> str:
    columns = inspector.get_columns(table_name, schema)
    pk_info = inspector.get_pk_constraint(table_name, schema)
    fk_info = inspector.get_foreign_keys(table_name, schema)

    query = text(read_file_to_str(f"{fpd(__file__, 2)}{sep}resources{sep}sqls{sep}ddl_postgres.sql"))
    result = conn.execute(query, {"table_name": table_name, "schema": schema or "public"})

    comments = {name: comment for name, comment in result if comment}

    return _build_ddl_string(table_name, columns, pk_info, fk_info, comments)


def _generate_mysql_ddl(
        conn: Connection, inspector: Inspector,
        table_name: str, schema: str = None
) -> str:
    columns = inspector.get_columns(table_name, schema)
    pk_info = inspector.get_pk_constraint(table_name, schema)
    fk_info = inspector.get_foreign_keys(table_name, schema)

    query = text(read_file_to_str(f"{fpd(__file__, 4)}{sep}resources{sep}sqls{sep}ddl_mysql.sql"))
    result = conn.execute(query, {"table": table_name})

    comments = {name: comment for name, comment in result if comment}

    return _build_ddl_string(table_name, columns, pk_info, fk_info, comments)


def _query_ddl_with_inline_comment(
        conn: Connection, inspector: Inspector,
        table_name: str, schema: str = None
) -> str:
    dialect = inspector.dialect.name

    if dialect == 'postgresql':
        return _generate_postgres_ddl(conn, inspector, table_name, schema)
    elif dialect == 'mysql':
        return _generate_mysql_ddl(conn, inspector, table_name, schema)
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect}")

