import os
from pathlib import Path

from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError

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

    def _ensure_index(self, index_name: str) -> None:
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name)

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

    def upsert_document(self, collection_name: str, doc_id: str, body: dict) -> None:
        self._ensure_index(collection_name)
        self.client.update(
            index=collection_name,
            id=doc_id,
            body={"doc": body, "doc_as_upsert": True},
            refresh=True,
        )

    def update_document(self, collection_name: str, doc_id: str, body: dict) -> None:
        self._ensure_index(collection_name)
        existing = self.get_document(collection_name, doc_id)
        if not existing:
            raise KeyError(f"Document '{doc_id}' does not exist in collection '{collection_name}'")
        self.client.update(
            index=collection_name,
            id=doc_id,
            body={"doc": body},
            refresh=True,
        )

    def replace_document(self, collection_name: str, doc_id: str, source: dict) -> None:
        self._ensure_index(collection_name)
        self.client.index(
            index=collection_name,
            id=doc_id,
            body=source,
            refresh=True,
        )

    def delete_document(self, collection_name: str, doc_id: str) -> None:
        self._ensure_index(collection_name)
        try:
            self.client.delete(index=collection_name, id=doc_id, refresh=True)
        except NotFoundError:
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
