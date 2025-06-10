import json
from pydantic import BaseModel


class Config(BaseModel):
    class Fmt(BaseModel):
        ai_uri_examples: list[str]
        ollama_uri: str
        openai_uri: str

    class Text2SQL(BaseModel):
        class Postgresql(BaseModel):
            question: str
            tables: list[str]

        class Mysql(BaseModel):
            question: str
            tables: list[str]

        class Optimize(BaseModel):
            sql: str
            question: str
            problem: str
            tables: list[str]

        postgresql: Postgresql
        mysql: Mysql
        optimize: Optimize

    class Timeout(BaseModel):
        timeout: int
        question: str
        tables: list[str]

    db_uri: str
    llm_uri: str
    milvus_uri: str
    collection_name: str
    embedding_uri: str

    fmt: Fmt

    text2sql: Text2SQL
    timeout: Timeout


context = Config(**json.load(open("test.json")))
