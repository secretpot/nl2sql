import asyncio

from .config import context
from nl2sql.tools.text2sql import Text2SQL, Text2SQLAgent


def test_postgres():
    worker = Text2SQL(
        db_uri=context.postgres_uri,
        llm_uri=context.llm_uri,
        milvus_uri=context.milvus_uri,
        collection_name=context.collection_name,
        embedding_uri=context.embedding_uri
    )
    for case in context.text2sql.postgresql:
        res = worker.generate(case.question, case.tables)
        print(res, file=open("logs/postgresql.log", "a+"))
        assert len(res.sql) > 0


def test_mysql():
    worker = Text2SQL(
        db_uri=context.mysql_uri,
        llm_uri=context.llm_uri,
        milvus_uri=context.milvus_uri,
        collection_name=context.collection_name,
        embedding_uri=context.embedding_uri
    )
    for case in context.text2sql.mysql:
        res = worker.generate(case.question, case.tables)
        print(res, file=open("logs/mysql.log", "a+"))
        assert len(res.sql) > 0


def test_optimize():
    worker = Text2SQL(
        db_uri=context.postgres_uri,
        llm_uri=context.llm_uri,
        milvus_uri=context.milvus_uri,
        collection_name=context.collection_name,
        embedding_uri=context.embedding_uri
    )
    res = worker.optimize(
        context.text2sql.optimize.sql,
        context.text2sql.optimize.question,
        context.text2sql.optimize.problem,
        context.text2sql.optimize.tables
    )
    print(res, file=open("logs/optimize.log", "a+"))
    assert len(res.sql) > 0


def test_agent():
    agent = Text2SQLAgent(
        db_uri=context.postgres_uri,
        openai_baseurl=context.openai_baseurl,
        openai_apikey=context.openai_apikey,
        llm_model=context.llm_model,
        milvus_uri=context.milvus_uri,
        collection_name=context.collection_name,
        embedding_model="bge-m3",
    )
    agent.initialize()
    for case in context.text2sql.postgresql:
        res = asyncio.run(agent.generate(case.question, case.tables))
        print(res, file=open("logs/agent.log", "a+"))
        assert len(res.final_sql) > 0
