import abc

import openai
import pymilvus
import sqlalchemy
from os import sep

from nl2sql.utils.path import fpd
from nl2sql.utils.strings import read_file_to_str
from nl2sql.tools.database.metadata import Metadata
from nl2sql.tools.database.vector import query_sql_references_by_similar_question

from typing import (
    Any,
    Optional,
    Iterable
)
from pydantic import (
    BaseModel,
    PrivateAttr
)


class NL2SQLResult(BaseModel):
    question: str
    tables: list[str]
    prompt: str
    sql: Optional[str] = ""

    def __str__(self):
        s = "=" * 37
        question = f"Question: {self.question}"
        tables = f"Tables: {self.tables}"
        sql = f"SQL: {self.sql}"
        return f"{s}\n{question}\n{tables}\n{sql}\n{s}\n"


class Text2SQLBase(BaseModel, abc.ABC, metaclass=abc.ABCMeta):
    db_uri: str
    openai_baseurl: str
    openai_apikey: Optional[str] = ""
    llm_model: str

    milvus_uri: Optional[str] = ""
    collection_name: Optional[str] = ""
    embedding_model: Optional[str] = ""

    text2sql_prompt: Optional[str] = ""

    _sqlalchemy_engine: sqlalchemy.Engine = PrivateAttr()
    _openai_service: openai.AsyncOpenAI = PrivateAttr()
    _milvus_client: pymilvus.MilvusClient = PrivateAttr()

    def model_post_init(self, context: Any, /) -> None:
        # connect to database
        self._sqlalchemy_engine = sqlalchemy.create_engine(self.db_uri)
        self._sqlalchemy_engine.connect().close()
        self._milvus_client = pymilvus.MilvusClient(self.milvus_uri) if self.milvus_uri else None
        if self._milvus_client:
            if self.collection_name and self.collection_name not in self._milvus_client.list_collections():
                raise RuntimeError(f"Collection {self.collection_name} not found in Milvus server.")
        # connect to llm
        self._openai_service = openai.AsyncOpenAI(
            base_url=self.openai_baseurl,
            api_key=self.openai_apikey
        )

        # load prompt
        default_text2sql_prompt_file = f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}text2sql_assembly.md"
        self.text2sql_prompt = self.text2sql_prompt or read_file_to_str(default_text2sql_prompt_file)

    @property
    def sqlalchemy_engine(self):
        return self._sqlalchemy_engine

    @property
    def milvus_client(self):
        return self._milvus_client

    @property
    def is_references_enabled(self):
        return self._milvus_client and self._openai_service and self.collection_name and self.embedding_model

    def query_tables_metadata(
            self,
            tables: Iterable[str] = None,
            db_schema: str = None,
            sample_limit: int = 3,
    ) -> list[Metadata]:
        tables = tables or sqlalchemy.inspect(self._sqlalchemy_engine).get_table_names()
        with self._sqlalchemy_engine.connect() as db:
            return list(map(lambda x: Metadata.query_or_default(db, x, db_schema, sample_limit), tables))

    async def query_similar_questions(self, question: str, limit: int = 3, tags: Iterable[str] = None) -> dict:
        if self.is_references_enabled:
            references = query_sql_references_by_similar_question(
                question,
                self._milvus_client, self.collection_name,
                self._openai_service, self.embedding_model,
                limit, tags
            )
        else:
            references = {}
        return references

    @abc.abstractmethod
    async def generate(
            self,
            question: str,
            tables: list[str] = None,
            db_schema: str = None,
            sample_limit: int = 3,
            ref_limit: int = 3,
            tags: Iterable[str] = None,
            **kwargs
    ) -> NL2SQLResult:
        raise NotImplementedError
