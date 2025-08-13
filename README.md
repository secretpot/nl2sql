# NL2SQL
A simple implementation of NL2SQL.

## Requirements
- Python 3.12+
- Postgresql / Mysql
- LLM API
- *Milvus 2.5.0+*
- *Embedding API*

## Definition
### LLM URI
A db-uri-style string that provides information for calling the model API.
#### Format
```plaintext
<model_type>+<api_type>://<model>[:<model_tag>]@[<api_key>]@<url>
```
#### Supported Model Type
- llm
- embedding
#### Supported API Type
- ollama
- openai
#### Example
llm+ollama://qwen2.5:32b@localhost:11434
llm+openai://gpt-3.5-turbo:32b@your_token@http://localhost:11434/v1
### SQL References
Optional metadata stored in Milvus.
Provide some sql references for problems similar to the current problem.
#### Format
Must contain fields: `query`, `sql` and `tags`.
Vector field will be matched the embedding of query.


## Usage
> assembly sql generating
```python
import asyncio
from nl2sql.tools.text2sql import Text2SQLAssembly
text2sql = Text2SQLAssembly(
    db_uri="postgresql+psycopg2://postgres:123456@localhost:5432/test",
    openai_baseurl="http://localhost:11434/v1",
    openai_apikey="your_token",
    llm_model="qwen2.5:0.5b",
    embedding_model="bge-m3",
    milvus_uri="http://read:123456@localhost:19530",
    collection_name="sql_references",
)
# request or get information in context engineering
# using more information to generate SQL exactly
asyncio.run(text2sql.generate(
    "公司的设备清单", 
    ["assets", "users", "projects"], 
    columns=["id", "name", "asset_type"],  # optional
    expressions=["order by id", "limit 10"],  # optional
))
```
> _@Deprecated(0.5.0)_ generate sql
```python
from nl2sql.tools.text2sql import Text2SQL
worker = Text2SQL(
    db_uri="postgresql+psycopg2://postgres:123456@localhost:5432/test",
    llm_uri="llm+ollama://qwen2.5:32b@localhost:11434",
    milvus_uri="http://read:123456@localhost:19530",
    collection_name="sql_references",
    embedding_uri="embedding+ollama://bge-m3@localhost:11434"
)
sql = worker.generate("公司的设备清单", ["assets", "users", "projects"]).sql
```
> _@Deprecated(0.5.0)_ optimize sql
```python
from nl2sql.tools.text2sql import Text2SQL

worker = Text2SQL(
    db_uri="postgresql+psycopg2://postgres:123456@localhost:5432/test",
    llm_uri="llm+ollama://qwen2.5:32b@localhost:11434",
    milvus_uri="http://read:123456@localhost:19530",
    collection_name="sql_references",
    embedding_uri="embedding+ollama://bge-m3@localhost:11434"
)
sql = worker.optimize("select * from users", "所有用户的用户名", "有多余的字段", ["users", "projects"]).sql
```
> _@Deprecated(1.0.0)_ agent for sql generating
```python
import asyncio
from nl2sql.tools.text2sql import Text2SQLAgent

agent = Text2SQLAgent(
    db_uri="postgresql+psycopg2://postgres:123456@localhost:5432/test",
    openai_baseurl="http://localhost:11434/v1",
    openai_apikey="your_token",
    llm_model="qwen2.5:0.5b",
    embedding_model="bge-m3",
    milvus_uri="http://read:123456@localhost:19530",
    collection_name="sql_references",
)
# no longer need to specify name, instructions, tools
asyncio.run(agent.generate("公司的设备清单", ["assets", "users", "projects"]))
```
> Check if the entity is ambiguous
```python
from pydantic import BaseModel
from nl2sql.tools.text2sql import Text2SQLAssembly


class User(BaseModel):
    email: str
    full_name: str


text2sql = Text2SQLAssembly(
    db_uri="postgresql+psycopg2://postgres:123456@localhost:5432/test",
    openai_baseurl="http://localhost:11434/v1",
    openai_apikey="your_token",
    llm_model="qwen2.5:0.5b",
    embedding_model="bge-m3",
    milvus_uri="http://read:123456@localhost:19530",
    collection_name="sql_references",
)
result = text2sql.is_entity_ambiguous(
    User, "users", "test",
    display_cols=["email", "full_name", "id"]
)
```
