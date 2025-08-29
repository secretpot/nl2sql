from typing import (
    Union,
    Literal
)
from pydantic import (
    Field,
    BaseModel
)


class Evidence(BaseModel):
    reason: str = Field(max_length=50)
    original_text: str = Field(max_length=50)
    sources: list[Literal["user_query", "hard_filter", "soft_hint", "table_cards"]]


class Value(BaseModel):
    evidence: Evidence


class LiteralValue(Value):
    type_: Literal[
        "int",
        "float",
        "str",
        "bool",
        "date",
        "time",
        "timestamp",
        "decimal",
        "null"
    ] = Field(..., alias="type")
    is_ref: bool = False
    value: Union[int, float, str, bool, None]


class ColumnValue(Value):
    type_: Literal["column"] = Field("column", alias="type")
    table: str
    column: str


class FunctionValue(Value):
    type_: Literal["fn"] = Field("fn", alias="type")
    name: Literal[
        "NOW",
        "DATE_TRUNC",
        "DATE_SUB",
        "DATE_ADD",
        "COALESCE",
        "CONCAT",
        "ROUND",
        "LOWER",
        "UPPER",
        "ABS",
        "GREATEST",
        "LEAST"
    ]
    args: list["ScalarValue"]


class AggregationValue(Value):
    type_: Literal["agg"] = Field("agg", alias="type")
    name: Literal["count", "sum", "avg", "min", "max"]
    arg: Union[LiteralValue, ColumnValue, FunctionValue, "ExpressionValue"] | None = None
    distinct: bool = False


class ExpressionValue(Value):
    type_: Literal["expr"] = Field("expr", alias="type")
    left: Union[LiteralValue, ColumnValue, FunctionValue, AggregationValue]
    op: Literal["+", "-", "*", "/"]
    right: Union[LiteralValue, ColumnValue, FunctionValue, AggregationValue]


SequenceValue = list[Union[LiteralValue, ColumnValue, FunctionValue, AggregationValue]]
ScalarValue = Union[
    LiteralValue,
    ColumnValue,
    FunctionValue,
    AggregationValue,
    ExpressionValue
]


class Clause(BaseModel):
    left: Union[LiteralValue, ColumnValue, FunctionValue, AggregationValue, ExpressionValue, None]
    op: Literal[
        "=",
        "!=",
        ">",
        "<",
        ">=",
        "<=",
        "IN",
        "NOT IN",
        "BETWEEN",
        "LIKE",
        "NOT LIKE",
        "ILIKE",
        "NOT ILIKE",
        "IS",
        "IS NOT"
    ]
    right: Union[
        LiteralValue, ColumnValue, FunctionValue, AggregationValue, ExpressionValue, SequenceValue, None
    ]


class ClauseGroup(BaseModel):
    logic: Literal["AND", "OR"] | None = None  # clauses之间的逻辑
    not_: bool = Field(False, alias="not")
    clauses: list[Union[Clause, "ClauseGroup"]]


class Table(BaseModel):
    name: str
    alias: str | None = None


class Join(BaseModel):
    left: Table
    right: Table
    type_: Literal["inner", "left"] = Field(alias="type")
    on: ClauseGroup


class Select(BaseModel):
    expr: ScalarValue
    alias: str | None = None


class Order(BaseModel):
    expr: ScalarValue
    type_: Literal["asc", "desc"] = Field(alias="type")


class IR(BaseModel):
    select: list[Select]
    distinct: bool = False
    tables: list[Table]
    joins: list[Join] = []
    where: ClauseGroup | None = None
    group_by: list[Union[LiteralValue, ColumnValue, FunctionValue, ExpressionValue, int]] = []
    order_by: list[Order] = []
    having: ClauseGroup | None = None
    limit: int | None = None
    offset: int | None = None
    need_clarification: bool = False
    questions: list[str] = []
