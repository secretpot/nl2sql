from pydantic import (
    BaseModel,
    Field
)
from sqlalchemy import (
    Engine,
    inspect
)
from nl2sql.tools.database.card.parser import parse_table_metadata


class Column(BaseModel):
    name: str
    type_: str = Field(alias="type")
    nullable: bool
    comment: str = ""
    synonyms: list[str] = []
    semantic_type: str | None = None
    fk: str | None = None
    enums: list[str] | None = None
    pattern: str | None = None
    value_map: dict[str, str] | None = {}
    samples: list[str] = []


class JoinPath(BaseModel):
    from_: str = Field(alias="from")
    to: str
    type_: str = Field(alias="type")
    cardinality: str | None = None
    confidence: float = Field(1.0, ge=0.0, le=1.0)

    def __eq__(self, other: "JoinPath") -> bool:
        return self.from_ == other.from_ and self.to == other.to and self.type_ == other.type_


class TableMeta(BaseModel):
    """
    For internal usage only.
    """

    class Index(BaseModel):
        name: str
        columns: list[str]

    class Relation(BaseModel):
        type_: str = Field(alias="type")
        from_: str = Field(alias="from")
        to: str
        join: str
        cardinality: str | None = None

        def to_join_path(self) -> JoinPath:
            return JoinPath(**{
                "from": self.from_,
                "to": self.to,
                "type": self.join,
                "cardinality": self.cardinality
            })

    table: str
    comment: str = ""
    primary_key: list[str]
    indexes: list[Index]
    relations: list[Relation]
    columns: list[Column]

    def build_join_graph(self) -> list[JoinPath]:
        return list(map(lambda x: x.to_join_path(), self.relations))

    def to_llm_card(self, alias: str | None = None):
        return TableCard(
            name=self.table,
            alias=alias,
            comment=self.comment,
            primary_key=self.primary_key,
            columns=self.columns,
        )


class TableCard(BaseModel):
    name: str
    alias: str | None = None
    comment: str = ""
    primary_key: list[str]
    columns: list[Column]


class DatabasePack(BaseModel):
    """
    For llm context building.
    """

    tables: list[TableCard]
    known_joins: list[JoinPath] | None = []

    @classmethod
    def from_table_cards(cls, table_cards: list[TableMeta]):
        graph = []
        [graph.extend(x.build_join_graph()) for x in table_cards]
        return cls(
            tables=list(map(lambda x: x.to_llm_card(), table_cards)),
            known_joins=graph
        )

    @classmethod
    def from_database(
            cls,
            engine: Engine,
            tables: list[str] | None = None,
            aliases: dict[str, str] | list[str] | None = None,
            extra_joins: list[JoinPath] | None = None,
            db_schema: str | None = None,
            max_samples: int = 3
    ) -> "DatabasePack":
        if not tables:
            return cls(
                tables=[]
            )
        inspector = inspect(engine)
        cards = map(lambda x: TableMeta(**parse_table_metadata(engine, inspector, x, db_schema, max_samples)), tables)

        pack = cls.from_table_cards(list(cards))
        if isinstance(aliases, list):
            [pack.add_alias(table, alias) for table, alias in zip(tables, aliases)]
        elif isinstance(aliases, dict):
            [pack.add_alias(table, aliases.get(table)) for table in tables]
        for path in extra_joins or []:
            pack.add_join_path(path)
        return pack

    def add_alias(self, table_name: str, alias: str):
        for table in self.tables:
            if table.name == table_name and table.alias is None:
                table.alias = alias

    def add_join_path(self, path: JoinPath):
        if path not in self.known_joins:
            self.known_joins.append(path)
