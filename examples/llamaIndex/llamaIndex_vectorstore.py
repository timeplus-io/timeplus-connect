"""Timeplus vector store.

An index that is built on top of an existing Timeplus cluster.

Rewrite from https://github.com/run-llama/llama_index/blob/main/llama-index-integrations/vector_stores/llama-index-vector-stores-clickhouse/llama_index/vector_stores/clickhouse/base.py 

"""

import importlib
import json
import logging
import re
from typing import Any, Dict, List, Optional, cast

from llama_index.core.bridge.pydantic import PrivateAttr
from llama_index.core.schema import (
    BaseNode,
    MetadataMode,
    NodeRelationship,
    RelatedNodeInfo,
    TextNode,
)
from llama_index.core.utils import iter_batch
from llama_index.core.vector_stores.types import (
    VectorStoreQuery,
    VectorStoreQueryMode,
    VectorStoreQueryResult,
    BasePydanticVectorStore,
)
from llama_index.core import Settings


logger = logging.getLogger(__name__)


def _default_tokenizer(text: str) -> List[str]:
    """Default tokenizer."""
    tokens = re.split(r"[ \n]", text)  # split by space or newline
    result = []
    for token in tokens:
        if token.strip() == "":
            continue
        result.append(token.strip())
    return result


def escape_str(value: str) -> str:
    BS = "\\"
    must_escape = (BS, "'")
    return (
        "".join(f"{BS}{c}" if c in must_escape else c for c in value) if value else ""
    )


def format_list_to_string(lst: List) -> str:
    return "[" + ",".join(str(item) for item in lst) + "]"


DISTANCE_MAPPING = {
    "l2": "l2_distance",
    "cosine": "cosine_distance",
    "dot": "dot_product",
}


class TimeplusSettings:
    """Timeplus Client Configuration.

    Args:
        stream (str): Stream name to operate on.
        database (str): Database name to find the stream.
        engine (str): Engine. Options are "Mutable Stream". Default is "Mutable Stream".
    """

    def __init__(
        self,
        stream: str,
        database: str,
        engine: str,
        batch_size: int,
        metric: str,
        **kwargs: Any,
    ) -> None:
        self.stream = stream
        self.database = database
        self.engine = engine
        self.batch_size = batch_size
        self.metric = metric

    def build_query_statement(
        self,
        query_embed: List[float],
        where_str: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> str:
        query_embed_str = format_list_to_string(query_embed)
        where_str = f"WHERE {where_str}" if where_str else ""
        distance = DISTANCE_MAPPING[self.metric]
        return f"""
            SELECT id, doc_id, text, node_info, metadata,
            {distance}(vector, {query_embed_str}) AS score
            FROM table({self.database}.{self.stream}) {where_str}
            ORDER BY score ASC
            LIMIT {limit}
            """


class TimeplusVectorStore(BasePydanticVectorStore):
    """Timeplus Vector Store.
    In this vector store, embeddings and docs are stored within an existing
    Timeplus cluster.
    During query time, the index uses Timeplus to query for the top
    k most similar nodes.

    Args:
        timeplus_client (httpclient): timeplus-connect httpclient of
            an existing timeplus cluster.
        stream (str, optional): The name of the timeplus timeplus
            where data will be stored. Defaults to "llama_index".
        database (str, optional): The name of the Timeplus database
            where data will be stored. Defaults to "default".
        batch_size (int, optional): the size of documents to insert. Defaults to 1000.

    Examples:

        ```python
        import timeplus_connect

        # initialize client
        client = timeplus_connect.get_client(
            host="localhost",
            port=8123,
            username="default",
            password="",
        )

        vector_store = TimeplusVectorStore(timeplus_client=client)
        ```
    """

    stores_text: bool = True
    flat_metadata: bool = False
    _table_existed: bool = PrivateAttr(default=False)
    _client: Any = PrivateAttr()
    _config: Any = PrivateAttr()
    _dim: Any = PrivateAttr()
    _column_config: Any = PrivateAttr()
    _column_names: List[str] = PrivateAttr()
    _column_type_names: List[str] = PrivateAttr()
    metadata_column: str = "metadata"
    AMPLIFY_RATIO_LE5: int = 100
    AMPLIFY_RATIO_GT5: int = 20
    AMPLIFY_RATIO_GT50: int = 10

    def __init__(
        self,
        timeplus_client: Optional[Any] = None,
        stream: str = "llama_index",
        database: str = "default",
        engine: str = "Stream",
        metric: str = "cosine",
        index_type: str = "NONE",
        batch_size: int = 1000,
        **kwargs: Any,
    ) -> None:
        """Initialize params."""
        import_err_msg = """
            `timeplus_connect` package not found,
            please run `pip install timeplus_connect`
        """
        timeplus_connect_spec = importlib.util.find_spec(
            "timeplus_connect.driver.httpclient"
        )
        if timeplus_connect_spec is None:
            raise ImportError(import_err_msg)

        if timeplus_client is None:
            raise ValueError("Missing Timeplus client!")
        client = timeplus_client
        config = TimeplusSettings(
            stream=stream,
            database=database,
            engine=engine,
            metric=metric,
            batch_size=batch_size,
            **kwargs,
        )

        # schema column name, type, and construct format method
        column_config: Dict = {
            "id": {"type": "string", "extract_func": lambda x: x.node_id},
            "doc_id": {"type": "string", "extract_func": lambda x: x.ref_doc_id},
            "text": {
                "type": "string",
                "extract_func": lambda x: escape_str(
                    x.get_content(metadata_mode=MetadataMode.NONE) or ""
                ),
            },
            "vector": {
                "type": "array(float32)",
                "extract_func": lambda x: x.get_embedding(),
            },
            "node_info": {
                "type": "tuple(start nullable(uint64), end nullable(uint64))",
                "extract_func": lambda x: x.get_node_info(),
            },
            "metadata": {
                "type": "string",
                "extract_func": lambda x: json.dumps(x.metadata),
            },
        }
        column_names = list(column_config.keys())
        column_type_names = [
            column_config[column_name]["type"] for column_name in column_names
        ]

        super().__init__(
            timeplus_client=timeplus_client,
            stream=stream,
            database=database,
            engine=engine,
            batch_size=batch_size
        )
        self._client = client
        self._config = config
        self._column_config = column_config
        self._column_names = column_names
        self._column_type_names = column_type_names
        dimension = len(Settings.embed_model.get_query_embedding("try this out"))
        self.create_table(dimension)

    @property
    def client(self) -> Any:
        """Get client."""
        return self._client

    def create_table(self, dimension: int) -> None:
        schema_ = f"""
            CREATE MUTABLE STREAM IF NOT EXISTS {self._config.database}.{self._config.stream}(
                {",".join([f'{k} {v["type"]}' for k, v in self._column_config.items()])}
            )
            PRIMARY KEY (id, doc_id)
            """
        self._dim = dimension
        self._client.command(schema_)
        self._table_existed = True

    def _upload_batch(
        self,
        batch: List[BaseNode],
    ) -> None:
        _data = []
        # we assume all rows have all columns
        for idx, item in enumerate(batch):
            _row = []
            for column_name in self._column_names:
                _row.append(self._column_config[column_name]["extract_func"](item))
            _data.append(_row)

        self._client.insert(
            f"{self._config.database}.{self._config.stream}",
            data=_data,
            column_names=self._column_names,
            column_type_names=self._column_type_names,
        )


    def _append_meta_filter_condition(
        self, where_str: Optional[str], exact_match_filter: list
    ) -> str:
        
        filter_str = " AND ".join(
            f"json_extract_string("
            f"{self.metadata_column}, '{filter_item.key}') "
            f"= '{filter_item.value}'"
            for filter_item in exact_match_filter
        )
        if where_str is None:
            where_str = filter_str
        else:
            where_str = f"{where_str} AND " + filter_str
        return where_str

    def add(
        self,
        nodes: List[BaseNode],
        **add_kwargs: Any,
    ) -> List[str]:
        """Add nodes to index.

        Args:
            nodes: List[BaseNode]: list of nodes with embeddings
        """
        if not nodes:
            return []

        if not self._table_existed:
            self.create_table(len(nodes[0].get_embedding()))

        for batch in iter_batch(nodes, self._config.batch_size):
            self._upload_batch(batch=batch)

        return [result.node_id for result in nodes]

    def delete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
        """
        Delete nodes using with ref_doc_id.

        Args:
            ref_doc_id (str): The doc_id of the document to delete.
        """
        self._client.command(
            f"DELETE FROM {self._config.database}.{self._config.stream} WHERE doc_id='{ref_doc_id}'"
        )

    def drop(self) -> None:
        """Drop ClickHouse table."""
        self._client.command(
            f"DROP STREAM IF EXISTS {self._config.database}.{self._config.stream}"
        )

    def query(
        self, query: VectorStoreQuery, where: Optional[str] = None, **kwargs: Any
    ) -> VectorStoreQueryResult:
        """Query index for top k most similar nodes.

        Args:
            query (VectorStoreQuery): query
            where (str): additional where filter
        """
        query_embedding = cast(List[float], query.query_embedding)
        where_str = where
        if query.doc_ids:
            if where_str is not None:
                where_str = f"{where_str} AND {f'doc_id IN {format_list_to_string(query.doc_ids)}'}"
            else:
                where_str = f"doc_id IN {format_list_to_string(query.doc_ids)}"

        # TODO: Support other filter types
        if query.filters is not None and len(query.filters.legacy_filters()) > 0:
            where_str = self._append_meta_filter_condition(
                where_str, query.filters.legacy_filters()
            )

        # build query sql
        if query.mode == VectorStoreQueryMode.DEFAULT:
            query_statement = self._config.build_query_statement(
                query_embed=query_embedding,
                where_str=where_str,
                limit=query.similarity_top_k,
            )
        else:
            raise ValueError(f"query mode {query.mode!s} not supported")
        nodes = []
        ids = []
        similarities = []
        #print(f"query: {query_statement}")
        response = self._client.query(query_statement)
        #print(f"response: {response}")
        column_names = response.column_names
        #print(f"column_names: {column_names}")
        #print(f"column_names: {response.result_columns}")
        
        id_idx = column_names.index("id")
        text_idx = column_names.index("text")
        metadata_idx = column_names.index("metadata")
        node_info_idx = column_names.index("node_info")
        score_idx = column_names.index("score")
        for r in response.result_rows:
            start_char_idx = None
            end_char_idx = None

            if isinstance(r[node_info_idx], dict):
                start_char_idx = r[node_info_idx].get("start", None)
                end_char_idx = r[node_info_idx].get("end", None)
            node = TextNode(
                id_=r[id_idx],
                text=r[text_idx],
                metadata=json.loads(r[metadata_idx]),
                start_char_idx=start_char_idx,
                end_char_idx=end_char_idx,
                relationships={
                    NodeRelationship.SOURCE: RelatedNodeInfo(node_id=r[id_idx])
                },
            )

            nodes.append(node)
            similarities.append(r[score_idx])
            ids.append(r[id_idx])
        return VectorStoreQueryResult(nodes=nodes, similarities=similarities, ids=ids)
    

# test code
import os
import timeplus_connect

from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, StorageContext
from llama_index.core.vector_stores.types import ExactMatchFilter, MetadataFilters


timeplus_host = os.getenv("TIMEPLUS_HOST") or "localhost"
timeplus_user = os.getenv("TIMEPLUS_USER") or "proton"
timeplus_password = os.getenv("TIMEPLUS_PASSWORD") or "timeplus@t+"

client = timeplus_connect.get_client(
            host=timeplus_host,
            port=8123,
            username=timeplus_user,
            password=timeplus_password,
        )

# Load documents and build index
documents = SimpleDirectoryReader(
    "./data"
).load_data()

storage_context = StorageContext.from_defaults(
    vector_store=TimeplusVectorStore(timeplus_client=client)
)

index = VectorStoreIndex.from_documents(
    documents, storage_context=storage_context
)

'''
# Query the index
query_engine = index.as_query_engine()
response = query_engine.query("What did the author do growing up?")

print(response)
'''

# Query the index with filters
query_engine = index.as_query_engine(
    similarity_top_k=3,
    vector_store_query_mode="default",
    filters=MetadataFilters(
        filters=[
            ExactMatchFilter(key="file_name", value="paul_graham_essay.txt"),
        ]
    ),
    alpha=None,
    doc_ids=None,
)
response = query_engine.query("what did the author do growing up?")
print(response)

