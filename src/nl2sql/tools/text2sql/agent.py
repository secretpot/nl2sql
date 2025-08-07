import json
import agents
import records

from os import sep
from deprecated import deprecated

from nl2sql.utils.path import fpd
from nl2sql.utils.strings import read_file_to_str
from nl2sql.tools.database.data import execute_sql
from nl2sql.tools.text2sql.base import NL2SQLResult, Text2SQLBase

from typing import (
    Any,
    Optional,
    Iterable,
    Callable,
    Coroutine
)
from pydantic import (
    PrivateAttr
)


class AgentResult(NL2SQLResult):
    request_more_info: Optional[str] = ""
    fail_reason: Optional[str] = ""

    def __str__(self):
        s = "=" * 37
        question = f"Question: {self.question}"
        tables = f"Tables: {self.tables}"
        sql = f"SQL: {self.sql}"
        request_more_info = f"Request: {self.request_more_info}"
        fail_reason = f"Fail Reason: {self.fail_reason}"
        return f"{s}\n{question}\n{tables}\n{sql}\n{request_more_info}\n{fail_reason}\n{s}\n"


@deprecated(
    reason="Agent as a tool is hard to build a simple nl2sql flow, "
           "try request more info to build a sql by Text2SQLAssembly.",
    version="1.0.0"
)
class Text2SQLAgent(Text2SQLBase):
    context_prompt: Optional[str] = ""

    _agent: agents.Agent = PrivateAttr()

    def model_post_init(self, context: Any, /) -> None:
        # load prompt
        default_text2sql_prompt_file = f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}text2sql_agent.md"
        self.text2sql_prompt = self.text2sql_prompt or read_file_to_str(default_text2sql_prompt_file)
        default_context_prompt_file = f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}context.md"
        self.context_prompt = self.context_prompt or read_file_to_str(default_context_prompt_file)
        super().model_post_init(context)

        # create agent
        agents.set_tracing_disabled(True)
        self._agent: Optional[agents.Agent] = agents.Agent(
            name="simple-nl2sql-agent",
            model=agents.OpenAIChatCompletionsModel(
                model=self.llm_model,
                openai_client=self._openai_service
            ),
            model_settings=agents.ModelSettings(
                parallel_tool_calls=True
            ),
            instructions=self.text2sql_prompt.format(
                dialect=self._sqlalchemy_engine.dialect.name,
                db_context="\n",
                sql_references="\n"
            ),
            tools=[
                self.tool_search_tables_metadata(),
                self.tool_search_sql_references(),
                self.tool_execute_sql(),
            ],
        )

    @property
    def agent(self):
        return self._agent

    def tool_search_tables_metadata(self, create_tool: bool = True) -> (
            agents.FunctionTool | Callable[[Iterable[str], str, int], Coroutine]
    ):
        async def search_tables_metadata(
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
            return "\n".join(map(str, self.query_tables_metadata(tables, db_schema, sample_limit)))

        if create_tool:
            return agents.function_tool()(search_tables_metadata)
        return search_tables_metadata

    def tool_search_sql_references(self, create_tool: bool = True) -> (
            agents.FunctionTool | Callable[[str, int], Coroutine]
    ):
        async def search_sql_references(
                question: str,
                ref_limit: Optional[int] = 3,
                tags: Optional[Iterable[str]] = None
        ) -> str:
            """
            Search the SQL references most relevant to the user's question.

            Args:
                question (str): User's question.
                ref_limit (int, optional): Limit for references. Defaults to 3.
                tags (list[str], optional): List of tags, use all tags if None.
            """
            references = await self.query_similar_questions(question, ref_limit, tags)
            refs = list(map(lambda x: f"Question: {x[0]}\nSQL: {x[1]}\n", references.items()))
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

    async def generate(
            self,
            question: str,
            tables: list[str] = None,
            context_prompt: str = None,
            db_schema: str = None,
            sample_limit: int = 3,
            ref_limit: int = 3,
            tags: Iterable[str] = None
    ) -> AgentResult:
        if context_prompt is None:
            context_prompt = self.context_prompt.format(
                tables=tables,
                db_schema=db_schema,
                sample_limit=sample_limit,
                ref_limit=ref_limit,
                tags=tags
            )

        answer = (await agents.Runner.run(
            self._agent,
            [
                {"role": "system", "content": context_prompt},
                {"role": "user", "content": question}
            ]
        ))
        try:
            answer = json.loads(answer.final_output)

            if isinstance(answer, dict):
                return AgentResult(
                    question=question,
                    tables=tables or [],
                    prompt=context_prompt,
                    sql=answer.get("final_sql"),
                    request_more_info=answer.get("request_more_information"),
                    fail_reason=answer.get("fail_reason")
                )
            else:
                return AgentResult(
                    question=question,
                    tables=tables or [],
                    prompt=context_prompt,
                    sql="",
                    request_more_info=None,
                    fail_reason=f"Answer is not a standard answer:\n `{answer}`"
                )
        except json.JSONDecodeError as e:
            return AgentResult(
                question=question,
                tables=tables or [],
                prompt=context_prompt,
                sql="",
                request_more_info=None,
                fail_reason=f"Answer is not a valid JSON object:\n `{answer}`"
            )
