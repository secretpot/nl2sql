import re
import json
from os import sep
from typing import (
    Any,
    Sequence,
    FrozenSet
)
from sqlalchemy import (
    Engine,
    Table,
    MetaData,
    text
)

from nl2sql.utils.path import fpd

from nl2sql.tools.database.card.stringifier import (
    split_items,
    to_type_str,
    strip_wrappers,
    normalize_header,
    normalize_punctuation
)
from nl2sql.tools.database.card.data import sample_column_examples

SECTION_HEADER_RE = re.compile(
    r'^\s*#?\s*(comment|synonyms?|(value[\s_-]*|v)map(ping)?|synonmys|semantic[\s_-]*type|enums|pattern)\s*:?$',
    re.IGNORECASE
)
ALLOWED_SEMANTIC_TYPES = {
    "id", "fk", "datetime", "date", "enum", "amount", "number", "text", "bool", "category"
}


def _mssql_enrich_comments(engine: Engine, schema: str | None, table_name: str,
                           table_comment: str | None,
                           columns_info: list[dict[str, Any]]) -> tuple[str | None, list[dict[str, Any]]]:
    """Notes Read Compensation for SQL Server (sys.extended_properties)."""
    if engine.dialect.name != "mssql":
        return table_comment, columns_info

    # try to get table comment when it is empty
    try:
        with engine.connect() as conn:
            if not table_comment:
                sql_path = sep.join([fpd(__file__, 2), "resources", "sqls", "tb_cmt_mssql.sql"])
                with open(sql_path, "r", encoding="utf-8") as f:
                    tbl_sql = text(f.read())
                    row = conn.execute(tbl_sql, {"tname": table_name, "sname": schema}).fetchone()
                    if row and row[0]:
                        table_comment = str(row[0])
            sql_path = sep.join([fpd(__file__, 2), "resources", "sqls", "col_cmt_mssql.sql"])
            with open(sql_path, "r", encoding="utf-8") as f:
                col_sql = text(f.read())
                rows = conn.execute(col_sql, {"tname": table_name, "sname": schema}).fetchall()
                col_comment_map = {str(r[0]): (str(r[1]) if r[1] is not None else None) for r in rows}
                # Merge into columns_info
                for col in columns_info:
                    name = col.get("name")
                    if name in col_comment_map and not col.get("comment"):
                        col["comment"] = col_comment_map[name]
    except Exception:
        pass

    return table_comment, columns_info


def _collect_unique_sets(inspector, table_name: str, schema: str | None) -> set[FrozenSet[str]]:
    """Collects the unique column set of the table: primary key, unique constraint, unique index."""
    uniq: set[FrozenSet[str]] = set()
    try:
        pk = inspector.get_pk_constraint(table_name, schema=schema) or {}
        pk_cols = pk.get("constrained_columns") or []
        if pk_cols:
            uniq.add(frozenset(pk_cols))
    except Exception:
        pass
    try:
        for uq in inspector.get_unique_constraints(table_name, schema=schema) or []:
            cols = uq.get("column_names") or []
            if cols:
                uniq.add(frozenset(cols))
    except Exception:
        pass
    try:
        for idx in inspector.get_indexes(table_name, schema=schema) or []:
            if idx.get("unique"):
                cols = idx.get("column_names") or []
                if cols:
                    uniq.add(frozenset(cols))
    except Exception:
        pass
    return uniq


def _is_unique_set(unique_sets: set[FrozenSet[str]], cols: Sequence[str]) -> bool:
    """Whether the given column sequence matches a unique set(order is not important)."""
    return bool(cols) and frozenset(cols) in unique_sets


def parse_enums(txt: str, limit: int = 30) -> list[str] | None:
    """
    Parse enumeration definitions (supports only two forms):

    1. JSON array: ["paid","cancelled","refunded"]
    2. Comma/Chinese comma/period/semicolon/vertical bar/slash/newline separated list of pure values
    Returns: list[str]; returns None if parsing fails.
    """
    if not txt:
        return None
    t = txt.strip()
    # 1. JSON Array
    if t.startswith("[") and t.endswith("]"):
        try:
            data = json.loads(t)
            if isinstance(data, list):
                values = []
                for x in data:
                    v = str(x).strip()
                    if v:
                        values.append(v)
                out = list(dict.fromkeys(values))
                return out[:limit] if out else None
        except json.JSONDecodeError:
            pass

    # 2. Line/delimiter splitting
    candidates: list[str] = []
    for line in t.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        parts = re.split(r"[,\uFF0C;\uFF1B\u3001/|]+", line) if any(
            ch in line for ch in [",", "，", "、", ";", "；", "/", "|"]
        ) else [line]
        for p in parts:
            v = p.strip()
            if not v:
                continue
            # remove tokens with ":"
            if ":" in v or "：" in v:
                candidates.append(re.split(r"[:：]", v)[0].strip())
            candidates.append(v)

    if not candidates:
        return None

    return list(dict.fromkeys(candidates))[:limit]


def _merge_put(d: dict[str, str], k: str, v: str, overwrite: bool = False) -> None:
    if not k or not v:
        return
    if k in d and not overwrite:
        return
    d[k] = v


def _parse_pairs_block(block: str, limit: int, overwrite: bool) -> dict[str, str]:
    if not block or not block.strip():
        return {}
    s = normalize_punctuation(block)
    s = re.sub(r"\s*(->|=>|=|：)\s*", ":", s)
    tokens = re.split(r"[\n,;|/\t]+", s)
    mapping: dict[str, str] = {}
    for tok in tokens:
        tok = strip_wrappers(tok)
        if not tok:
            continue
        if ":" in tok:
            k, v = tok.split(":", 1)
        else:
            parts = tok.split()
            if len(parts) >= 2:
                k, v = parts[0], " ".join(parts[1:])
            else:
                continue
        k, v = strip_wrappers(k), strip_wrappers(v)
        if not k or not v:
            continue
        _merge_put(mapping, k, v, overwrite)
        if len(mapping) >= limit:
            break
    return mapping


def _try_parse_json_map(t: str, limit: int, overwrite: bool = False) -> dict[str, str] | None:
    tt = t.lstrip()
    if not (tt.startswith("{") or tt.startswith("[")):
        return None
    try:
        data = json.loads(t)
    except json.JSONDecodeError:
        return None

    mapping: dict[str, str] = {}

    if isinstance(data, dict):
        for k, v in data.items():
            ks = strip_wrappers(str(k))
            vs = strip_wrappers("" if v is None else str(v))
            if not ks or not vs:
                continue
            _merge_put(mapping, ks, vs, overwrite)
            if len(mapping) >= limit:
                break
        return mapping or None

    if isinstance(data, list):
        for item in data:
            k = v = None
            # array
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                k, v = str(item[0]), str(item[1])
            # single key dict or dict with explicit field
            elif isinstance(item, dict):
                if len(item) == 1:
                    (k, v), = item.items()
                    k, v = str(k), str(v)
                else:
                    for a, b in [("key", "value"), ("k", "v"), ("from", "to"), ("label", "code"), ("name", "id")]:
                        if a in item and b in item:
                            k, v = str(item[a]), str(item[b])
                            break
            if k is None or v is None:
                continue
            ks, vs = strip_wrappers(k), strip_wrappers(v)
            if not ks or not vs:
                continue
            _merge_put(mapping, ks, vs, overwrite)
            if len(mapping) >= limit:
                break
        return mapping or None

    return None


def parse_value_map(
        txt: str,
        limit: int = 30,
        overwrite: bool = False,
) -> dict[str, str] | None:
    """
    Parse value map definitions (supports only two forms):

    1. JSON:
       - {"男":"F","女":"M","others":"OTHER"}
       - [["男","F"],["女","M"]]
       - [{"男":"F"},{"女":"M"}]
       - [{"key":"男","value":"F"}, {"k":"女","v":"M"}]
    2. Comments/Long Text (Error Tolerance):
       - Parses only the '# value_map' section (falls back to parsing the entire section if not found)
       - Supports -> / => / = / : as key-value separators
       - Supports Chinese/English commas, semicolons, vertical bars, slashes, newlines, and tabs as pair separators
       - Automatically cleans up full-width punctuation and quotes/brackets
    Returns: dict[str, str]; returns None if parsing fails.
    """
    if not txt or not txt.strip():
        return None
    t = txt.strip()

    m = _try_parse_json_map(t, limit=limit, overwrite=overwrite)
    if m:
        return m

    mapping = _parse_pairs_block(txt, limit=limit, overwrite=overwrite)
    return mapping if mapping else None


def parse_comment_block(raw: str | None, max_enums: int = 30) -> dict[str, Any]:
    """
    Parse column comments, supporting the following sections: comment/synonyms/semantic_type/enums/pattern
    If the comment is empty or the corresponding section is not provided,
    the corresponding field will be missing(enums, pattern) or empty(synonyms).
    """
    result: dict[str, Any] = {
        "comment": "",
        "synonyms": [],
    }
    if not raw:
        return result

    lines = raw.splitlines()
    sections: dict[str, list[str]] = {}
    current: str | None = None
    saw_any_header = False

    for line in lines:
        m = SECTION_HEADER_RE.match(line)
        if m:
            saw_any_header = True
            header = normalize_header(m.group(1))
            current = header
            sections.setdefault(current, [])
            continue
        # non-header
        if current is None:
            # considered as comment if no header is encountered
            sections.setdefault("comment", [])
            sections["comment"].append(line)
        else:
            sections.setdefault(current, [])
            sections[current].append(line)

    if not saw_any_header:
        sections = {"comment": lines}

    # comment
    comment_text = "\n".join(sections.get("comment", [])).strip()
    result["comment"] = comment_text

    # synonyms
    syns = []
    if synonyms := sections.get("synonyms", []):
        for ss in map(split_items, synonyms):
            syns.extend(ss)
    result["synonyms"] = list(dict.fromkeys(syns))

    # semantic_type
    if "semantic_type" in sections:
        st_text = " ".join(sections["semantic_type"]).strip().lower()
        st_text = re.split(r"\s+", st_text)[0] if st_text else ""
        if st_text in ALLOWED_SEMANTIC_TYPES:
            result["semantic_type"] = st_text
        else:
            pass

    # value map
    if "value_map" in sections:
        vm_text = "\n".join(sections["value_map"]).strip()
        value_map = parse_value_map(vm_text)
        if value_map is not None:
            result["value_map"] = value_map

    # enums
    if "enums" in sections:
        enum_text = "\n".join(sections["enums"]).strip()
        enums_parsed = parse_enums(enum_text, max_enums)
        if enums_parsed is not None:
            result["enums"] = enums_parsed  # list[str]

    # pattern
    if "pattern" in sections:
        pat_text = "\n".join(sections["pattern"]).strip()
        # keep non-empty pattern (do not validate regex, avoid misjudgment)
        if pat_text:
            # keep the first line
            first_line = next((ln.strip() for ln in pat_text.splitlines() if ln.strip()), "")
            if first_line:
                result["pattern"] = first_line

    return result


def parse_table_metadata(
        engine: Engine,
        inspector,
        table_name: str,
        schema: str | None,
        max_samples: int = 0
) -> dict[str, Any]:
    table_obj = Table(table_name, MetaData(), autoload_with=engine, schema=schema)

    # primary key
    pk_cols = []
    try:
        pk_info = inspector.get_pk_constraint(table_name, schema=schema) or {}
        pk_cols = pk_info.get("constrained_columns") or []
    except Exception:
        pass

    # index
    idx_list: list[dict[str, Any]] = []
    try:
        raw_indexes = inspector.get_indexes(table_name, schema=schema) or []
        for idx in raw_indexes:
            cols = idx.get("column_names") or []
            idx_list.append({"name": idx.get("name"), "columns": cols})
    except Exception:
        pass

    # get columns info
    try:
        columns_info = inspector.get_columns(table_name, schema=schema) or []
    except Exception:
        columns_info = []

    # nullable
    nullable_map = {c.get("name"): bool(c.get("nullable", True)) for c in columns_info}

    # gather unique sets of the left table in the join
    from_unique_sets = _collect_unique_sets(inspector, table_name, schema)

    # foreign key
    fk_map: dict[str, tuple[str, str]] = {}
    relations: list[dict[str, Any]] = []
    try:
        fks = inspector.get_foreign_keys(table_name, schema=schema) or []
        for fk in fks:
            from_cols = fk.get("constrained_columns") or []
            ref_table = fk.get("referred_table")
            ref_schema = fk.get("referred_schema")
            to_cols = fk.get("referred_columns") or []

            if not ref_table or not from_cols or not to_cols:
                continue

            rel_obj = {}

            # join type
            is_optional = any(nullable_map.get(c, True) for c in from_cols)
            join_type = "left" if is_optional else "inner"

            # cardinality
            to_unique_sets = _collect_unique_sets(inspector, ref_table, ref_schema)
            to_is_unique = _is_unique_set(to_unique_sets, to_cols)  # Usually True (referenced keys should be unique)
            from_is_unique = _is_unique_set(from_unique_sets, from_cols)

            if to_is_unique and from_is_unique:
                cardinality = "1:1"
            elif to_is_unique:
                cardinality = "m:1"
            else:
                cardinality = None

            if cardinality:
                rel_obj["cardinality"] = cardinality

            # foreign keys mapping
            for c, rc in zip(from_cols, to_cols):
                fk_map[c] = (f"{ref_schema + '.' if ref_schema else ''}{ref_table}", rc)

            # relations
            from_expr = (
                f"{table_name}.{from_cols[0]}"
                if len(from_cols) == 1 else f"{table_name}.(" + ", ".join(from_cols) + ")"
            )
            to_expr = (
                f"{ref_table}.{to_cols[0]}"
                if len(to_cols) == 1 else f"{ref_table}.(" + ", ".join(to_cols) + ")"
            )

            rel_obj.update({
                "type": "fk",
                "from": from_expr,
                "to": to_expr,
                "join": join_type,
            })

            relations.append(rel_obj)
    except Exception:
        pass

    # table comment
    tbl_comment = None
    try:
        tc = inspector.get_table_comment(table_name, schema=schema) or {}
        tbl_comment = tc.get("text") or None
    except Exception:
        pass

    # columns info
    try:
        columns_info = inspector.get_columns(table_name, schema=schema) or []
    except Exception:
        columns_info = []

    # SQL Server comment compensation
    tbl_comment, columns_info = _mssql_enrich_comments(engine, schema, table_name, tbl_comment, columns_info)

    if tbl_comment and tbl_comment.strip():
        table_comment_text = tbl_comment
    else:
        table_comment_text = ""

    result: dict[str, Any] = {
        "table": table_name,
        "comment": table_comment_text,
        "primary_key": pk_cols,
        "indexes": idx_list,
        "relations": relations,
        "columns": [],
    }

    # parse comment column by column
    with engine.connect() as conn:
        for col in columns_info:
            name = col.get("name")
            typ = col.get("type")
            nullable = bool(col.get("nullable", True))
            comment_raw = col.get("comment")  # may be None

            parsed = parse_comment_block(comment_raw)

            col_obj: dict[str, Any] = {
                "name": name,
                "type": to_type_str(typ),
                "nullable": nullable,
                "comment": parsed.get("comment", ""),
                "synonyms": parsed.get("synonyms", []),
            }
            if st := parsed.get("semantic_type", None):
                col_obj["semantic_type"] = st
            if vm := parsed.get("value_map", None):
                col_obj["value_map"] = vm

            # fk info
            if name in fk_map:
                rt, rc = fk_map[name]
                rt_clean = rt.split(".")[-1]
                col_obj["fk"] = f"{rt_clean}.{rc}"
            else:
                col_obj["fk"] = None

            # samples
            samples = sample_column_examples(conn, table_obj, name, limit=max_samples)
            if samples:
                col_obj["samples"] = samples

            # enums
            if "enums" in parsed and parsed["enums"] is not None:
                col_obj["enums"] = parsed["enums"]

            # pattern
            if "pattern" in parsed and parsed["pattern"]:
                col_obj["pattern"] = parsed["pattern"]

            result["columns"].append(col_obj)

    return result
