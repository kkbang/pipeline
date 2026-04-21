import os
from pathlib import Path

from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import bulk as opensearch_bulk

from worker.common.config import settings


class OpenSearchStore:
    def __init__(self, base_dir: str | Path | None = None) -> None:
        configured_dir = os.getenv("LOCAL_DATA_DIR", "").strip()
        if base_dir is None:
            if configured_dir:
                base_dir = configured_dir
            else:
                base_dir = Path(__file__).resolve().parents[2] / "local_data"
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        http_auth = None
        if settings.opensearch_user and settings.opensearch_password:
            http_auth = (settings.opensearch_user, settings.opensearch_password)

        self.client = OpenSearch(
            hosts=[{"host": settings.opensearch_host, "port": settings.opensearch_port}],
            http_auth=http_auth,
            use_ssl=settings.opensearch_use_ssl,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            timeout=settings.request_timeout_seconds,
        )
        self._ensured_indices: set[str] = set()

    def _ensure_index(self, index_name: str) -> None:
        if index_name in self._ensured_indices:
            return
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name)
        self._ensured_indices.add(index_name)

    def refresh_index(self, collection_name: str) -> None:
        self._ensure_index(collection_name)
        self.client.indices.refresh(index=collection_name)

    def get_document(self, collection_name: str, doc_id: str) -> dict:
        self._ensure_index(collection_name)
        try:
            result = self.client.get(index=collection_name, id=doc_id)
        except NotFoundError:
            return {}
        return {
            "_id": result.get("_id", doc_id),
            "_source": result.get("_source", {}),
        }

    def upsert_document(
        self,
        collection_name: str,
        doc_id: str,
        body: dict,
        *,
        refresh: bool = True,
    ) -> None:
        self._ensure_index(collection_name)
        self.client.update(
            index=collection_name,
            id=doc_id,
            body={"doc": body, "doc_as_upsert": True},
            refresh=refresh,
        )

    def bulk_upsert_documents(
        self,
        collection_name: str,
        documents: list[tuple[str, dict]],
        *,
        refresh: bool = False,
        chunk_size: int = 500,
    ) -> None:
        if not documents:
            return
        self._ensure_index(collection_name)
        safe_chunk_size = max(1, chunk_size)
        actions = (
            {
                "_op_type": "update",
                "_index": collection_name,
                "_id": doc_id,
                "doc": body,
                "doc_as_upsert": True,
            }
            for doc_id, body in documents
        )
        opensearch_bulk(
            self.client,
            actions,
            chunk_size=safe_chunk_size,
            refresh=refresh,
            raise_on_error=True,
        )

    def update_document(
        self,
        collection_name: str,
        doc_id: str,
        body: dict,
        *,
        refresh: bool = True,
    ) -> None:
        self._ensure_index(collection_name)
        existing = self.get_document(collection_name, doc_id)
        if not existing:
            raise KeyError(f"Document '{doc_id}' does not exist in collection '{collection_name}'")
        self.client.update(
            index=collection_name,
            id=doc_id,
            body={"doc": body},
            refresh=refresh,
        )

    def replace_document(
        self,
        collection_name: str,
        doc_id: str,
        source: dict,
        *,
        refresh: bool = True,
    ) -> None:
        self._ensure_index(collection_name)
        self.client.index(
            index=collection_name,
            id=doc_id,
            body=source,
            refresh=refresh,
        )

    def bulk_index_documents(
        self,
        collection_name: str,
        documents: list[tuple[str, dict]],
        *,
        refresh: bool = False,
        chunk_size: int = 500,
    ) -> None:
        if not documents:
            return
        self._ensure_index(collection_name)
        safe_chunk_size = max(1, chunk_size)
        actions = (
            {
                "_op_type": "index",
                "_index": collection_name,
                "_id": doc_id,
                "_source": source,
            }
            for doc_id, source in documents
        )
        opensearch_bulk(
            self.client,
            actions,
            chunk_size=safe_chunk_size,
            refresh=refresh,
            raise_on_error=True,
        )

    def delete_document(
        self,
        collection_name: str,
        doc_id: str,
        *,
        refresh: bool = True,
    ) -> None:
        self._ensure_index(collection_name)
        try:
            self.client.delete(index=collection_name, id=doc_id, refresh=refresh)
        except NotFoundError:
            return

    def delete_documents_by_field(
        self,
        collection_name: str,
        field_name: str,
        value: str,
        *,
        refresh: bool = True,
    ) -> int:
        self._ensure_index(collection_name)
        result = self.client.delete_by_query(
            index=collection_name,
            body={
                "query": {
                    "bool": {
                        "should": [
                            {"term": {f"{field_name}.keyword": value}},
                            {"term": {field_name: value}},
                        ],
                        "minimum_should_match": 1,
                    }
                }
            },
            conflicts="proceed",
            refresh=refresh,
        )
        return int(result.get("deleted") or 0)

    def count_documents(
        self,
        collection_name: str,
        query: dict | None = None,
    ) -> int:
        self._ensure_index(collection_name)
        result = self.client.count(
            index=collection_name,
            body={"query": query or {"match_all": {}}},
        )
        return int(result.get("count") or 0)

    def iterate_documents_by_query(
        self,
        collection_name: str,
        query: dict | None = None,
        *,
        size: int = 1000,
        sort: list[dict] | None = None,
        source_includes: list[str] | None = None,
    ):
        self._ensure_index(collection_name)
        safe_size = max(1, size)
        resolved_sort = sort or [{"_id": "asc"}]
        search_after = None

        while True:
            body = {
                "size": safe_size,
                "sort": resolved_sort,
                "query": query or {"match_all": {}},
            }
            if source_includes:
                body["_source"] = source_includes
            if search_after is not None:
                body["search_after"] = search_after

            result = self.client.search(index=collection_name, body=body)
            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                return

            for hit in hits:
                yield hit

            search_after = hits[-1].get("sort")
            if not search_after:
                return

    def list_documents(
        self,
        collection_name: str,
        size: int = 1000,
    ) -> list[dict]:
        self._ensure_index(collection_name)
        result = self.client.search(
            index=collection_name,
            body={
                "size": size,
                "sort": [{"_id": "asc"}],
                "query": {"match_all": {}},
            },
        )
        return result.get("hits", {}).get("hits", [])

    def find_documents_by_field(
        self,
        collection_name: str,
        field_name: str,
        value: str,
        size: int = 1000,
    ) -> list[dict]:
        self._ensure_index(collection_name)
        result = self.client.search(
            index=collection_name,
            body={
                "size": size,
                "sort": [{"_id": "asc"}],
                "query": {
                    "bool": {
                        "should": [
                            {"term": {f"{field_name}.keyword": value}},
                            {"term": {field_name: value}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
            },
        )
        return result.get("hits", {}).get("hits", [])

    def find_documents_by_status(
        self,
        collection_name: str,
        status: str,
        size: int = 1000,
    ) -> list[dict]:
        return self.find_documents_by_field(
            collection_name=collection_name,
            field_name="status",
            value=status,
            size=size,
        )
