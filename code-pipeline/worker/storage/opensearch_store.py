from opensearchpy import OpenSearch
from worker.common.config import settings


class OpenSearchStore:
    def __init__(self) -> None:
        self.client = OpenSearch(
            hosts=[{
                "host": settings.opensearch_host,
                "port": settings.opensearch_port,
            }],
            http_auth=(settings.opensearch_user, settings.opensearch_password),
            use_ssl=settings.opensearch_use_ssl,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )

    def index(self, index_name: str, body: dict, doc_id: str | None = None) -> None:
        self.client.index(index=index_name, id=doc_id, body=body, refresh=True)

    def upsert(self, index_name: str, doc_id: str, body: dict) -> None:
        self.client.update(
            index=index_name,
            id=doc_id,
            body={
                "doc": body,
                "doc_as_upsert": True,
            },
            refresh=True,
        )

    def update(self, index_name: str, doc_id: str, body: dict) -> None:
        self.client.update(
            index=index_name,
            id=doc_id,
            body={"doc": body},
            refresh=True,
        )

    def search_by_status(self, index_name: str, status: str, size: int = 1000) -> list[dict]:
        result = self.client.search(
            index=index_name,
            body={
                "size": size,
                "query": {
                    "term": {
                        "status.keyword": status
                    }
                }
            }
        )
        return result["hits"]["hits"]