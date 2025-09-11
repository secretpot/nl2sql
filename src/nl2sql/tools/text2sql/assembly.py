from os import sep
from records import Database

from nl2sql.tools.text2sql.base import NL2SQLResult, Text2SQLBase
from nl2sql.tools.database.data import execute_sql
from nl2sql.utils.path import fpd
from nl2sql.utils.strings import read_file_to_str

from typing import (
    Any,
    Iterable,
    Optional
)


class Text2SQLAssembly(Text2SQLBase):
    verify_prompt: Optional[str] = ""

    def model_post_init(self, context: Any, /) -> None:
        # load prompt
        default_text2sql_prompt_file = f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}text2sql_assembly.md"
        self.text2sql_prompt = self.text2sql_prompt or read_file_to_str(default_text2sql_prompt_file)
        default_verify_prompt_file = f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}verify.md"
        self.verify_prompt = self.verify_prompt or read_file_to_str(default_verify_prompt_file)
        super().model_post_init(context)

    async def generate(
            self,
            question: str,
            tables: list[str] = None,
            columns: list[str] = None,
            expressions: list[str] = None,
            db_schema: str = None,
            sample_limit: int = 3,
            ref_limit: int = 3,
            tags: Iterable[str] = None,
            max_verification: int = 2,
    ) -> NL2SQLResult:
        tables_metadata = self.query_tables_metadata(tables, db_schema, sample_limit)
        db_ctxt = "\n\n".join(map(str, tables_metadata))
        required_columns = "\n".join(map(lambda x: f"- {x}", columns or []))
        cols_ctxt = f"# Required Columns\n{required_columns}" if columns else ""
        expressions = "\n".join(map(lambda x: f"- {x}", expressions or []))
        expr_ctxt = f"# Predicates References\n{expressions}" if expressions else ""
        references = await self.query_similar_questions(question, ref_limit, tags)
        refs = list(map(lambda x: f"Question: {x[0]}\nSQL: {x[1]}\n", references.items()))
        similar_ctxt = f"# Similar Question&SQL References\n{"\n".join(refs)}" if len(refs) > 0 else ""

        system_prompt = self.text2sql_prompt.format(
            dialect=self._sqlalchemy_engine.dialect.name,
            db_ctxt=db_ctxt,
            cols_ctxt=cols_ctxt,
            expr_ctxt=expr_ctxt,
            similar_ctxt=similar_ctxt
        )
        messages = [
            {
                "role": "system",
                "content": system_prompt
            }, {
                "role": "user",
                "content": question
            }
        ]
        sql = (await self._openai_service.chat.completions.create(
            model=self.llm_model,
            messages=messages,
        )).choices[0].message.content

        for i in range(max_verification):
            suggestions = await self.verify(question, sql)
            if len(suggestions) < 5 and suggestions.upper().find("OK") != -1:
                break
            sql = (await self._openai_service.chat.completions.create(
                model=self.llm_model,
                messages=[
                    *messages,
                    {
                        "role": "assistant",
                        "content": sql
                    }, {
                        "role": "user",
                        "content": suggestions
                    }
                ],
            )).choices[0].message.content

        return NL2SQLResult(
            question=question,
            tables=tables,
            prompt=system_prompt,
            sql=sql
        )

    async def verify(
            self,
            question: str,
            sql: str
    ) -> str:
        with Database(self.db_uri) as db:
            results = execute_sql(db, sql)

        system_prompt = self.verify_prompt.format(
            dialect=self._sqlalchemy_engine.dialect.name,
            results=results,
            question=question
        )

        return (await self._openai_service.chat.completions.create(
            model=self.llm_model,
            messages=[{
                "role": "system",
                "content": system_prompt
            }, {
                "role": "user",
                "content": sql
            }],
        )).choices[0].message.content
