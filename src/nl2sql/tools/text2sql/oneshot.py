import pymilvus
import sqlalchemy
from os import sep
from pydantic import BaseModel
from deprecated import deprecated
from langchain_ollama import OllamaEmbeddings, ChatOllama
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage

from nl2sql.utils.path import fpd
from nl2sql.utils.strings import read_file_to_str
from nl2sql.utils.ai import parse_ai_uri, APIType
from nl2sql.tools.database.metadata import Metadata as TableContext


class PromptInfo(BaseModel):
    tables: list[str]
    db_context: str
    ref_req: str
    refs_context: str


class SQLResult(BaseModel):
    query: str
    tables: list[str]
    prompt: str
    sql: str

    def __str__(self):
        s = "=" * 37
        return f"{s}\nQuery: {self.query}\nTables: {self.tables}\nSQL: {self.sql}\n\nPrompt:\n{self.prompt[:]}\n{s}\n"


@deprecated(
    reason="Generate SQL simply in onestep is not a good idea, "
           "try request more info to build a sql by Text2SQLAssembly.",
    version="0.5.0"
)
class Text2SQL:
    def __init__(self, db_uri: str, llm_uri: str,
                 milvus_uri: str = None, collection_name: str = None, embedding_uri: str = None):
        self._db_uri: str = db_uri
        self._llm_uri: str = llm_uri
        self._milvus_uri: str = milvus_uri
        self._collection_name: str = collection_name
        self._embedding_uri: str = embedding_uri
        self._text2sql_prompt: str = read_file_to_str(
            f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}text2sql_oneshot.md"
        )
        self._ref_req_prompt: str = read_file_to_str(
            f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}references.md"
        )
        self._optimize_prompt: str = read_file_to_str(
            f"{fpd(__file__, 2)}{sep}resources{sep}prompts{sep}text2sql{sep}optimizesql.md"
        )

        # create engine
        self._engine = sqlalchemy.create_engine(self._db_uri)
        self._engine.connect().close()
        self._milvus_client = pymilvus.MilvusClient(self._milvus_uri) if self._milvus_uri else None
        self._llm_api = parse_ai_uri(llm_uri)
        self._embedding_api = parse_ai_uri(embedding_uri) if embedding_uri else None
        self._llm_model = None
        self._embed_model = None

        # create ai api object
        if self._llm_api.api_type is APIType.OLLAMA:
            self._llm_model = ChatOllama(
                model=self._llm_api.model,
                base_url=self._llm_api.api_uri,
                api_key=self._llm_api.api_key
            )
        elif self._llm_api.api_type is APIType.OPENAI:
            self._llm_model = ChatOpenAI(
                model_name=self._llm_api.model,
                openai_api_base=self._llm_api.api_uri,
                openai_api_key=self._llm_api.api_key
            )
        else:
            raise NotImplementedError(f"Unsupported LLM API type: {self._llm_api.api_type}")
        if self._embedding_api is not None:
            if self._embedding_api.api_type is APIType.OLLAMA:
                self._embed_model = OllamaEmbeddings(
                    model=self._embedding_api.model,
                    base_url=self._embedding_api.api_uri
                )
            elif self._embedding_api.api_type is APIType.OPENAI:
                self._embed_model = OpenAIEmbeddings(
                    name=self._embedding_api.model,
                    base_url=self._embedding_api.api_uri,
                    api_key=self._embedding_api.api_key,
                )
            else:
                self._embed_model = None
        else:
            self._embed_model = None

    def generate(self, query: str, tables: list[str] = None) -> SQLResult:
        prompt_info = self.query_prompt_info(query, tables)
        system_prompt = self._text2sql_prompt.format(
            dialect=self._engine.dialect.name,
            db_ctxt=prompt_info.db_context,
            ref_req=prompt_info.ref_req,
            references=prompt_info.refs_context,
        )

        return SQLResult(
            query=query,
            tables=prompt_info.tables,
            prompt=system_prompt,
            sql=self._llm_model.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=query)
            ]).content
        )

    def optimize(self, sql: str, query: str, problem: str, tables: list[str] = None) -> SQLResult:
        prompt_info = self.query_prompt_info(query, tables)
        system_prompt = self._optimize_prompt.format(
            sql=sql,
            query=query,
            dialect=self._engine.dialect.name,
            db_ctxt=prompt_info.db_context,
            ref_req=prompt_info.ref_req,
            references=prompt_info.refs_context,
        )

        return SQLResult(
            query=problem,
            tables=prompt_info.tables,
            prompt=system_prompt,
            sql=self._llm_model.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=problem)
            ]).content
        )

    def query_prompt_info(
            self,
            query: str, tables: list[str] = None,
            sample_limit: int = 3, ref_limit: int = 3
    ) -> PromptInfo:
        tables = tables or sqlalchemy.inspect(self._engine).get_table_names()

        references = self.query_sql_references(query, ref_limit)
        refs = list(map(lambda x: f"Query: {x[0]}\nSQL: {x[1]}\n", references.items()))
        refs_context = f"# References\n{"\n".join(refs)}" if len(refs) > 0 else ""

        schema_info = {}
        errs = {}
        with self._engine.connect() as db:
            metadata = sqlalchemy.MetaData()
            metadata.reflect(bind=self._engine)
            for table_name in tables:
                try:
                    schema_info[table_name] = TableContext.query(db, table_name, sample_limit=sample_limit)
                except Exception as e:
                    errs[table_name] = f"Can't get schema info for table {table_name}: {e}"
        db_ctxt = "\n".join(map(lambda x: str(x), schema_info.values()))
        ref_req = self._ref_req_prompt if references else ""
        return PromptInfo(
            tables=tables,
            db_context=db_ctxt,
            ref_req=ref_req,
            refs_context=refs_context
        )

    def query_sql_references(self, query: str, limit: int = 3) -> dict:
        if self._milvus_client and self._embedding_api and self._collection_name:

            if self._embed_model:
                embedding = self._embed_model.embed_query(query)
                searched = self._milvus_client.search(
                    self._collection_name, [embedding], output_fields=["query", "sql"], limit=limit
                )
                if len(searched) > 0:
                    references = map(lambda x: x["entity"], searched[0])
                    references = {x["query"]: x["sql"] for x in references}
                else:
                    references = {}
            else:
                references = {}
        else:
            references = {}
        return references
