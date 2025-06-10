import re
from enum import Enum
from pydantic import BaseModel
from typing import Optional


class ModelType(Enum):
    LLM = "LLM"
    EMBEDDING = "EMBEDDING"


class APIType(Enum):
    OPENAI = "OPENAI"
    OLLAMA = "OLLAMA"


class AiApi(BaseModel):
    model_type: ModelType
    api_type: APIType
    api_uri: str
    model: str
    api_key: Optional[str] = ""


def parse_ai_uri(uri: str) -> AiApi:
    """
    Parse URIs of format:
      model_type+api_type://model[:model_tag]@[api_key]@api_uri
    """
    enum_regex = r"[^+]+"
    # Pattern to match api_uri: either schema://... or ip:port[/path]
    api_uri_regex = r"(?:[a-zA-Z]+://)?[^@\s]+"
    pattern = re.compile(
        rf"^(?P<model_type>{enum_regex})\+"
        rf"(?P<api_type>{enum_regex})://"
        rf"(?P<model>[^:@]+)"
        rf"(?::(?P<model_tag>[^@]+))?"
        rf"(?:@(?P<api_key>[^@]+))?"
        rf"@(?P<api_uri>{api_uri_regex})$"
    )
    match = pattern.match(uri)
    if not match:
        expected = (
            "model_type+api_type://model[:model_tag]@[api_key]@"
            "<schema://url_or_ip:port>"
        )
        raise ValueError(f"Invalid AI API URI: {uri}, expected {expected}")

    mt = match.group("model_type").upper()
    at = match.group("api_type").upper()
    model_type = ModelType(mt)
    api_type = APIType(at)

    name = match.group("model")
    raw_tag = match.group("model_tag")
    api_key = match.group("api_key") or ""
    uri_part = match.group("api_uri")

    # Default tag only for OLLAMA
    if api_type == APIType.OLLAMA:
        tag = raw_tag or "latest"
    else:
        tag = raw_tag or None

    # Construct model string
    model_full = f"{name}:{tag}" if tag else name

    return AiApi(
        model_type=model_type,
        api_type=api_type,
        api_uri=uri_part,
        model=model_full,
        api_key=api_key
    )
