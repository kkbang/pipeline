import re
from datetime import datetime, timezone
from hashlib import sha1

from worker.storage.opensearch_store import OpenSearchStore


def safe_path_fragment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")


def stable_digest(value: str) -> str:
    return sha1(value.encode("utf-8")).hexdigest()[:12]


def _normalized_urls(urls: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for url in urls:
        if not isinstance(url, str):
            continue
        candidate = url.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return sorted(normalized)


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
    # NOTE:
    # We intentionally do not persist `raw_metadata` into seed_item_index.
    # `raw_metadata` contained highly dynamic nested structures by registry/source,
    # which caused:
    # - mapper_parsing_exception (object vs concrete value conflicts)
    # - field explosion (index.mapping.total_fields.limit exceeded)
    #
    # Seed normalization only relies on candidate_repo_urls, so omitting
    # raw_metadata here keeps pipeline behavior while stabilizing mappings.
    _ = raw_metadata

    base_body = {
        "source_type": source_type,
        "source_name": source_name,
        "source_item_id": source_item_id,
        "candidate_repo_urls": candidate_repo_urls,
    }
    if extra_body:
        base_body.update(extra_body)

    now_iso = datetime.now(timezone.utc).isoformat()
    existing = store.get_document("seed_item_index", doc_id)
    if not existing:
        store.upsert_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                **base_body,
                "status": "ingested",
                "first_seen_at": now_iso,
                "last_seen_at": now_iso,
                "last_ingested_at": now_iso,
            },
        )
        return

    current = existing.get("_source", {})
    has_changed = (
        current.get("source_type") != source_type
        or current.get("source_name") != source_name
        or current.get("source_item_id") != source_item_id
        or _normalized_urls(current.get("candidate_repo_urls") or [])
        != _normalized_urls(candidate_repo_urls)
    )
    existing_status = str(current.get("status") or "").strip() or "ingested"
    next_status = "ingested" if has_changed else existing_status

    update_body = {
        **base_body,
        "status": next_status,
        "first_seen_at": current.get("first_seen_at") or now_iso,
        "last_seen_at": now_iso,
    }
    if has_changed:
        update_body["last_ingested_at"] = now_iso
    elif current.get("last_ingested_at"):
        update_body["last_ingested_at"] = current.get("last_ingested_at")

    store.upsert_document(
        collection_name="seed_item_index",
        doc_id=doc_id,
        body=update_body,
    )
