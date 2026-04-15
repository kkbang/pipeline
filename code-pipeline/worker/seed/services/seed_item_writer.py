import re
from hashlib import sha1

from worker.storage.opensearch_store import OpenSearchStore


def safe_path_fragment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")


def stable_digest(value: str) -> str:
    return sha1(value.encode("utf-8")).hexdigest()[:12]


def write_seed_item(
    store: OpenSearchStore,
    *,
    doc_id: str,
    source_type: str,
    source_name: str,
    source_item_id: str,
    raw_metadata: dict,
    candidate_repo_urls: list[str],
    extra_body: dict | None = None,
) -> None:
    body = {
        "source_type": source_type,
        "source_name": source_name,
        "source_item_id": source_item_id,
        "raw_metadata": raw_metadata,
        "candidate_repo_urls": candidate_repo_urls,
        "status": "ingested",
    }
    if extra_body:
        body.update(extra_body)

    store.upsert_document(
        collection_name="seed_item_index",
        doc_id=doc_id,
        body=body,
    )
