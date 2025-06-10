from os.path import sep
from typing import Sequence
from pydantic import BaseModel
from sqlalchemy import func, text, select, inspect, MetaData, Connection, Inspector, Table

from nl2sql.utils.path import fpd
from nl2sql.utils.strings import read_file_to_str


class TableContext(BaseModel):
    table_name: str
    description: str
    ddl: str
    samples: list[str]

    def __str__(self) -> str:
        header = f"-- {self.table_name}: {self.description}"
        footer = f"-- Example Values:\n{'\n'.join(self.samples)}" if self.samples else ""
        return f"{header}\n{self.ddl}\n{footer}"

    @classmethod
    def query(cls, conn: Connection, table_name: str, schema: str = None, sample_limit: int = 3) -> "TableContext":
        inspector = inspect(conn.engine)
        metadata = MetaData()
        metadata.reflect(bind=inspector.engine)
        table = metadata.tables.get(table_name)
        comment = inspector.get_table_comment(table_name).get("text") or ""
        ddl = cls.query_ddl_with_inline_comment(conn, inspector, table_name, schema)
        samples = cls.sample_table(conn, table, limit=sample_limit)
        table_context = cls(
            table_name=table_name,
            description=comment,
            ddl=ddl,
            samples=map(str, map(tuple, samples))
        )
        return table_context

    @classmethod
    def query_ddl_with_inline_comment(
            cls,
            conn: Connection, inspector: Inspector,
            table_name: str, schema: str = None
    ) -> str:
        dialect = inspector.dialect.name

        if dialect == 'postgresql':
            return cls._generate_postgres_ddl(conn, inspector, table_name, schema)
        elif dialect == 'mysql':
            return cls._generate_mysql_ddl(conn, inspector, table_name, schema)
        else:
            raise NotImplementedError(f"Unsupported dialect: {dialect}")

    @classmethod
    def _generate_postgres_ddl(
            cls,
            conn: Connection, inspector: Inspector,
            table_name: str, schema: str = None
    ) -> str:
        columns = inspector.get_columns(table_name, schema)
        pk_info = inspector.get_pk_constraint(table_name, schema)
        fk_info = inspector.get_foreign_keys(table_name, schema)

        query = text(read_file_to_str(f"{fpd(__file__)}{sep}resources{sep}sqls{sep}ddl_postgres.sql"))
        result = conn.execute(query, {"table_name": table_name, "schema": schema or "public"})

        comments = {name: comment for name, comment in result if comment}

        return cls._build_ddl_string(table_name, columns, pk_info, fk_info, comments)

    @classmethod
    def _generate_mysql_ddl(
            cls,
            conn: Connection, inspector: Inspector,
            table_name: str, schema: str = None
    ) -> str:
        columns = inspector.get_columns(table_name, schema)
        pk_info = inspector.get_pk_constraint(table_name, schema)
        fk_info = inspector.get_foreign_keys(table_name, schema)

        query = text(read_file_to_str(f"{fpd(__file__, 4)}{sep}resources{sep}sqls{sep}ddl_mysql.sql"))
        result = conn.execute(query, {"table": table_name})

        comments = {name: comment for name, comment in result if comment}

        return cls._build_ddl_string(table_name, columns, pk_info, fk_info, comments)

    @staticmethod
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

    @classmethod
    def sample_table(cls, db: Connection, table: Table, limit: int = 3) -> Sequence:
        dialect = db.engine.dialect.name
        if dialect not in ('postgresql', 'mysql'):
            raise NotImplementedError(f"Unsupported dialect: {dialect}")
        rand_func = func.random if dialect == 'postgresql' else func.rand
        stmt = select(table).order_by(rand_func()).limit(limit)
        result = db.execute(stmt).fetchall()
        return result
