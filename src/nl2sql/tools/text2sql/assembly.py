from nl2sql.tools.text2sql.base import NL2SQLResult, Text2SQLBase

from typing import (
    Iterable
)


class Text2SQLAssembly(Text2SQLBase):
    async def generate(
            self,
            question: str,
            tables: list[str] = None,
            columns: list[str] = None,
            expressions: list[str] = None,
            db_schema: str = None,
            sample_limit: int = 3,
            ref_limit: int = 3,
            tags: Iterable[str] = None
    ) -> NL2SQLResult:
        tables_metadata = "\n".join(map(str, self.query_tables_metadata(tables, db_schema, sample_limit)))
        references = await self.query_similar_questions(question, ref_limit, tags)
        refs = list(map(lambda x: f"Question: {x[0]}\nSQL: {x[1]}\n", references.items()))
        similar_context = f"# Similar References\n{"\n".join(refs)}" if len(refs) > 0 else ""
        system_prompt = self.text2sql_prompt.format(
            dialect=self._sqlalchemy_engine.dialect.name,
            db_ctxt=tables_metadata,
            cols="\n".join(map(lambda x: f"- {x}", columns or [])),
            references="\n".join(map(lambda x: f"- {x}", expressions or [])),
            similar=similar_context
        )

        return NL2SQLResult(
            question=question,
            tables=tables,
            prompt=system_prompt,
            sql=(await self._openai_service.chat.completions.create(
                model=self.llm_model,
                messages=[{
                    "role": "system",
                    "content": system_prompt
                }, {
                    "role": "user",
                    "content": question
                }],
            )).choices[0].message.content
        )
