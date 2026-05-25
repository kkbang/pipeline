from threading import Lock
from typing import Any

from worker.retrieval.chunk_retrieval_service import _clean_text


class RepoDocumentCache:
    def __init__(self) -> None:
        self._lock = Lock()
        self._docs: dict[str, dict[str, Any] | None] = {}

    def get_many(
        self,
        *,
        store: Any,
        collection_name: str,
        doc_ids: list[str],
    ) -> dict[str, dict[str, Any] | None]:
        normalized_doc_ids = list(
            dict.fromkeys(_clean_text(doc_id) for doc_id in doc_ids if _clean_text(doc_id))
        )
        if not normalized_doc_ids:
            return {}

        with self._lock:
            missing_doc_ids = [doc_id for doc_id in normalized_doc_ids if doc_id not in self._docs]

        if missing_doc_ids:
            multi_get = getattr(store, "multi_get_documents", None)
            fetched_docs: dict[str, dict[str, Any] | None]
            if callable(multi_get):
                response = multi_get(collection_name, missing_doc_ids) or {}
                fetched_docs = {
                    _clean_text(doc_id): (doc if isinstance(doc, dict) else None)
                    for doc_id, doc in dict(response).items()
                    if _clean_text(doc_id)
                }
            else:
                fetched_docs = {}
                for doc_id in missing_doc_ids:
                    doc = store.get_document(collection_name, doc_id)
                    fetched_docs[doc_id] = doc if isinstance(doc, dict) else None

            with self._lock:
                for doc_id in missing_doc_ids:
                    self._docs[doc_id] = fetched_docs.get(doc_id)

        with self._lock:
            return {doc_id: self._docs.get(doc_id) for doc_id in normalized_doc_ids}
