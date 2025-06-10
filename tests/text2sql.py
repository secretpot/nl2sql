from .config import context
from nl2sql.tools.text2sql import Text2SQL


def test_postgres():
    worker = Text2SQL(
        db_uri=context.postgres_uri,
        llm_uri=context.llm_uri,
        milvus_uri=context.milvus_uri,
        collection_name=context.collection_name,
        embedding_uri=context.embedding_uri
    )
    res = worker.generate(context.text2sql.postgresql.question, context.text2sql.postgresql.tables)
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
    res = worker.generate(context.text2sql.mysql.question, context.text2sql.mysql.tables)
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
