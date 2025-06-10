import time

from .config import context

from src.nl2sql.tools.text2sql import Text2SQL


def test_timeout():
    worker = Text2SQL(
        db_uri=context.db_uri,
        llm_uri=context.llm_uri,
        milvus_uri=context.milvus_uri,
        collection_name=context.collection_name,
        embedding_uri=context.embedding_uri
    )
    time.sleep(context.timeout.timeout)
    res = worker.generate(context.timeout.question, context.timeout.tables)
    print(res, file=open("logs/timeout.log", "a+"))
    assert len(res.sql) > 0
