from typing import Literal
from pydantic import BaseModel


class Restriction(BaseModel):
    left: str
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
    right: str


class UserQuery(BaseModel):
    question: str
    hard_filters: list[Restriction] | None = []
    select_whitelist: list[str] | None = []
    references_mapping: dict[str, str] | None = {}





