import datetime as _dt
import re
from decimal import Decimal
from typing import (
    Any
)


def to_type_str(col_type: Any) -> str:
    try:
        return str(col_type).lower()
    except Exception:
        return col_type.__class__.__name__.lower()


def stringify_value(v: Any) -> str:
    try:
        if v is None:
            return ""
        if isinstance(v, (int, float, str, bool)):
            return str(v)
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, (_dt.date, _dt.datetime, _dt.time)):
            return v.isoformat(sep=" ")
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                return v.decode("utf-8", errors="ignore")
            except Exception:
                return repr(v)
        return str(v)
    except Exception:
        return str(v)


def split_items(text: str) -> list[str]:
    # split by normal separators, but keep english space(support multi-word)
    if not text:
        return []
    items = re.split(r"[,\uFF0C;\uFF1B\u3001/|]+", text)
    # remove leading/trailing whitespace and empty string
    return [it.strip() for it in items if it and it.strip()]


def normalize_header(name: str) -> str:
    name = name.strip().lower()
    name = name.replace(" ", "").replace("-", "").replace("_", "")
    if name in ("synonmys", "synonym", "synonyms"):
        return "synonyms"
    if name in ("comment", "comments", "coment"):
        return "comment"
    if name in ("semantictype", "semantic_type", "semantictypes"):
        return "semantic_type"
    if name in ("enum", "enums"):
        return "enums"
    if name in ("pattern",):
        return "pattern"
    if name in ("value_map", "value_mapping", "vmap", "mapping"):
        return "value_map"
    return name


def normalize_punctuation(s: str) -> str:
    # 常见全角/箭头等转半角 & 统一
    s = (s.replace('，', ',')
         .replace('、', ',')
         .replace('；', ';')
         .replace('：', ':')
         .replace('＝', '=')
         .replace('→', '->')
         .replace('⇒', '->')
         .replace('。', '.'))
    return s


def strip_wrappers(s: str) -> str:
    return s.strip().strip("[]{}()“”\"'‘’").strip()
