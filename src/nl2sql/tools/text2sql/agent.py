import openai
import agents
import pymilvus
import records
import sqlalchemy

from os import sep

from nl2sql.utils.path import fpd
from nl2sql.utils.strings import read_file_to_str
from nl2sql.tools.database.data import execute_sql
from nl2sql.tools.database.metadata import Metadata as TableContext

from typing import (
    Any,
    Optional,
    Iterable,
    Callable,
    Coroutine
)
from pydantic import (
    BaseModel,
    PrivateAttr
)


class AgentResult(BaseModel):
    query: str
    tables: list[str]
    prompt: str

    final_sql: Optional[str] = ""
    intermediate_sql: Optional[str] = ""
    request_more_info: Optional[str] = ""

    def __str__(self):
        s = "=" * 37
        query = f"Query: {self.query}"
        tables = f"Tables: {self.tables}"
        sql = f"SQL: {self.final_sql}"
        intermediate_sql = f"Intermediate SQL: {self.intermediate_sql}"
        request_more_info = f"Request: {self.request_more_info}"
        return f"{s}\n{query}\n{tables}\n{intermediate_sql}\n{sql}\n{request_more_info}\n{s}\n"


class Text2SQLAgent(BaseModel):
    db_uri: str
    openai_baseurl: str
    openai_apikey: Optional[str] = ""
    llm_model: str

    milvus_uri: Optional[str] = ""
    collection_name: Optional[str] = ""
    embedding_model: Optional[str] = ""
    text2sql_prompt: Optional[str] = ""
    context_prompt: Optional[str] = ""

    _sqlalchemy_engine: sqlalchemy.Engine = PrivateAttr()
    _openai_service: openai.AsyncOpenAI = PrivateAttr()
    _milvus_client: pymilvus.MilvusClient = PrivateAttr()
    _agent: agents.Agent = PrivateAttr()

    def model_post_init(self, context: Any, /) -> None:
        # connect to database
        self._sqlalchemy_engine = sqlalchemy.create_engine(self.db_uri)
        self._sqlalchemy_engine.connect().close()
        self._milvus_client = pymilvus.MilvusClient(self.milvus_uri) if self.milvus_uri else None
        if self.collection_name and self.collection_name not in self._milvus_client.list_collections():
            raise RuntimeError(f"Collection {self.collection_name} not found in Milvus server.")
        # connect to llm
        self._openai_service = openai.AsyncOpenAI(
            base_url=self.openai_baseurl,
            api_key=self.openai_apikey
        )

        # load prompt
        default_text2sql_prompt_file = f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}text2sql_agent.md"
        self.text2sql_prompt = self.text2sql_prompt or read_file_to_str(default_text2sql_prompt_file)
        default_context_prompt_file = f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}context.md"
        self.context_prompt = self.context_prompt or read_file_to_str(default_context_prompt_file)

        # create agent
        agents.set_tracing_disabled(True)
        self._agent: Optional[agents.Agent] = None

    @property
    def agent(self):
        return self._agent

    @property
    def sqlalchemy_engine(self):
        return self._sqlalchemy_engine

    @property
    def milvus_client(self):
        return self._milvus_client

    def query_table_context(
            self,
            tables: Iterable[str] = None,
            db_schema: str = None,
            sample_limit: int = 3,
    ) -> list[TableContext]:

        tables = tables or sqlalchemy.inspect(self._sqlalchemy_engine).get_table_names()
        results = []
        with self._sqlalchemy_engine.connect() as db:
            for table in tables:
                try:
                    results.append(TableContext.query(db, table, db_schema, sample_limit))
                except Exception as e:
                    results.append(TableContext(
                        table_name=table,
                        description=f"Can't get schema info for table {table}: {e}",
                        ddl="",
                        samples=[]
                    ))
        return results

    async def query_sql_references(self, query: str, limit: int = 3, tags: Iterable[str] = None) -> dict:
        if self._milvus_client and self._openai_service and self.collection_name and self.embedding_model:
            embedding = (await self._openai_service.embeddings.create(
                input=query,
                model=self.embedding_model
            )).data[0].embedding
            expr = ""
            if tags:
                expr = f"ARRAY_LENGTH(tags) == 0 || ARRAY_CONTAINS_ANY(tags, {tags})"
            searched = self._milvus_client.search(
                self.collection_name, [embedding], output_fields=["query", "sql"], limit=limit,
                filter=expr
            )
            if len(searched) > 0:
                references = map(lambda x: x["entity"], searched[0])
                references = {x["query"]: x["sql"] for x in references}
            else:
                references = {}
        else:
            references = {}
        return references

    def tool_search_table_metadata(self, create_tool: bool = True) -> (
            agents.FunctionTool | Callable[[Iterable[str], str, int], Coroutine]
    ):
        async def search_table_metadata(
                tables: Optional[Iterable[str]] = None,
                db_schema: Optional[str] = None,
                sample_limit: Optional[int] = 3,
        ) -> str:
            """
            Search DDLs with inline comments and sample values for the given tables.

            Args:
                tables (list[str], optional): List of table names, use all tables if None.
                db_schema (str, optional): Database schema, use `public` if None.
                sample_limit (int, optional): Limit for sample values. Defaults to 3.
            """
            return "\n".join(map(lambda x: str(x), self.query_table_context(tables, db_schema, sample_limit)))

        if create_tool:
            return agents.function_tool()(search_table_metadata)
        return search_table_metadata

    def tool_search_sql_references(self, create_tool: bool = True) -> (
            agents.FunctionTool | Callable[[str, int], Coroutine]
    ):
        async def search_sql_references(
                question: str,
                ref_limit: Optional[int] = 3,
                tags: Optional[Iterable[str]] = None
        ) -> str:
            """
            Search the SQL references most relevant to the query.

            Args:
                question (str): User's question.
                ref_limit (int, optional): Limit for references. Defaults to 3.
                tags (list[str], optional): List of tags, use all tags if None.
            """
            references = await self.query_sql_references(question, ref_limit, tags)
            refs = list(map(lambda x: f"Query: {x[0]}\nSQL: {x[1]}\n", references.items()))
            refs_context = f"{"\n".join(refs)}" if len(refs) > 0 else ""
            return refs_context

        if create_tool:
            return agents.function_tool()(search_sql_references)
        return search_sql_references

    def tool_execute_sql(self, create_tool: bool = True) -> (
            agents.FunctionTool | Callable[[str, str], Coroutine]
    ):

        async def sql_executor(
                sql: str,
                fmt: Optional[str] = "markdown"
        ) -> str | list[dict] | dict:
            """
            Execute the SQL and return the result.

            Args:
                sql (str): SQL string that can be executed directly.
                fmt (str, optional): Format of the result. Defaults to "markdown". Supported formats: "markdown", "json"
            """
            return execute_sql(db, sql, fmt)

        db = records.Database(self.db_uri)

        if create_tool:
            return agents.function_tool()(sql_executor)
        return sql_executor

    def initialize(
            self,
            name: str = "Text3SQL",
            instructions: Optional[str] = None,
            tools: Optional[Iterable[agents.Tool]] = None
    ) -> agents.Agent:
        self._agent = agents.Agent(
            name=name,
            model=agents.OpenAIChatCompletionsModel(
                model=self.llm_model,
                openai_client=self._openai_service
            ),
            model_settings=agents.ModelSettings(
                parallel_tool_calls=True
            ),
            instructions=instructions or self.text2sql_prompt.format(
                dialect=self._sqlalchemy_engine.dialect.name,
                db_context="\n",
                sql_references="\n"
            ),
            tools=tools or [
                self.tool_search_table_metadata(),
                self.tool_search_sql_references(),
                self.tool_execute_sql(),
            ],
        )
        return self._agent

    async def generate(
            self,
            query: str, tables: list[str] = None,
            context_prompt: str = None,
            db_schema: str = None, sample_limit: int = 3,
            ref_limit: int = 3, tags: Iterable[str] = None
    ) -> AgentResult:
        if self._agent is None:
            raise ValueError("Agent is not initialized, call create_agent() first.")

        if context_prompt is None:
            context_prompt = self.context_prompt.format(
                tables=tables,
                db_schema=db_schema,
                sample_limit=sample_limit,
                ref_limit=ref_limit,
                tags=tags
            )

        return AgentResult(
            query=query,
            tables=tables or [],
            prompt=context_prompt,
            final_sql=(await agents.Runner.run(
                self._agent,
                [
                    {"role": "system", "content": context_prompt},
                    {"role": "user", "content": query}
                ]
            )).final_output
        )
