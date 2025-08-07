import openai
import pymilvus
from typing import (
    Iterable
)


async def query_sql_references_by_similar_question(
        question: str,
        milvus_client: pymilvus.MilvusClient,
        collection_name: str,
        openai_service: openai.AsyncOpenAI,
        embedding_model: str,
        limit: int = 3,
        tags: Iterable[str] = None
) -> dict:
    embedding = (await openai_service.embeddings.create(
        input=question,
        model=embedding_model
    )).data[0].embedding
    expr = f"ARRAY_LENGTH(tags) == 0 || ARRAY_CONTAINS_ANY(tags, {tags})" if tags else ""
    searched = milvus_client.search(
        collection_name,
        [embedding],
        output_fields=["query", "sql"],
        limit=limit,
        filter=expr
    )
    if len(searched) > 0:
        references = map(lambda x: x["entity"], searched[0])
        references = {x["query"]: x["sql"] for x in references}
    else:
        references = {}

    return references
