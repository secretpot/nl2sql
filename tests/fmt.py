from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI

from .config import context
from nl2sql.utils.ai import parse_ai_uri


def test_ai_uri():
    samples = context.fmt.ai_uri_examples
    for s in samples:
        print(parse_ai_uri(s))

    api = parse_ai_uri(context.fmt.openai_uri)

    llm = ChatOpenAI(
        model_name=api.model,
        openai_api_base=api.api_uri,
        openai_api_key=api.api_key
    )
    print(llm.invoke("hi").content)

    api = parse_ai_uri(context.fmt.ollama_uri)

    llm = ChatOllama(
        model=api.model,
        base_url=api.api_uri,
        api_key=api.api_key
    )
    print(llm.invoke("hi").content)
