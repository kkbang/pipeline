import hashlib
import logging

from worker.common.config import settings
from worker.seed.resolvers.github_url_canonicalizer import canonicalize_github_repo_url
from worker.storage.opensearch_store import OpenSearchStore

logger = logging.getLogger(__name__)
BULK_FLUSH_SIZE = max(100, settings.opensearch_bulk_flush_docs)


def _status_query(status: str) -> dict:
    return {
        "bool": {
            "should": [
                {"term": {"status.keyword": status}},
                {"term": {"status": status}},
            ],
            "minimum_should_match": 1,
        }
    }


def _iter_seed_docs_by_status(store: OpenSearchStore, status: str):
    yield from store.iterate_documents_by_query(
        collection_name="seed_item_index",
        query=_status_query(status),
        size=1000,
        sort=[{"_id": "asc"}],
    )


def _load_candidate_urls(source: dict) -> list[str]:
    candidate_urls = source.get("candidate_repo_urls")
    if isinstance(candidate_urls, list):
        return [url for url in candidate_urls if isinstance(url, str) and url.strip()]

    return []


def _repo_id(owner: str, repo: str) -> str:
    return f"github:{owner.lower()}/{repo.lower()}"


def _load_registered_repo_ids(
    store: OpenSearchStore,
) -> set[str]:
    repo_ids: set[str] = set()
    for hit in store.iterate_documents_by_query(
        collection_name="repo_registry_index",
        size=1000,
        sort=[{"_id": "asc"}],
        source_includes=["owner", "repo_name"],
    ):
        source = hit.get("_source") or {}
        owner = str(source.get("owner") or "").strip().lower()
        repo = str(source.get("repo_name") or "").strip().lower()
        if owner and repo:
            repo_ids.add(_repo_id(owner, repo))
            continue

        doc_id = str(hit.get("_id") or "").strip().lower()
        if doc_id.startswith("github:"):
            repo_ids.add(doc_id)

    return repo_ids


def _flush_seed_updates(store: OpenSearchStore, pending_updates: list[tuple[str, dict]]) -> None:
    if not pending_updates:
        return
    store.bulk_upsert_documents(
        collection_name="seed_item_index",
        documents=pending_updates,
        refresh=False,
        chunk_size=BULK_FLUSH_SIZE,
    )
    pending_updates.clear()


def _value_shard_index(value: str, shard_count: int) -> int:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % shard_count


def _is_seed_doc_in_shard(doc_id: str, shard_index: int, shard_count: int) -> bool:
    if shard_count <= 1:
        return True
    return _value_shard_index(doc_id, shard_count) == (shard_index % shard_count)


def run_seed_normalization_for_shard(
    shard_index: int,
    *,
    shard_count: int | None = None,
) -> dict:
    resolved_shard_count = (
        shard_count
        if isinstance(shard_count, int) and shard_count > 0
        else max(1, settings.seed_normalize_parallelism)
    )
    normalized_shard_index = shard_index % resolved_shard_count

    store = OpenSearchStore()
    registered_repo_ids = _load_registered_repo_ids(store)
    pending_updates: list[tuple[str, dict]] = []

    scanned_count = 0
    processed_count = 0
    normalized_count = 0
    already_registered_count = 0
    invalid_count = 0

    for hit in _iter_seed_docs_by_status(store, "ingested"):
        scanned_count += 1
        doc_id = hit["_id"]
        if not _is_seed_doc_in_shard(doc_id, normalized_shard_index, resolved_shard_count):
            continue

        processed_count += 1
        source = hit["_source"]

        candidate_urls = _load_candidate_urls(source)

        selected_url = None
        owner = None
        repo = None
        canonical_url = None

        for url in candidate_urls:
            result = canonicalize_github_repo_url(url)
            if result:
                owner, repo, canonical_url = result
                selected_url = url
                break

        if not canonical_url:
            invalid_count += 1
            pending_updates.append(
                (
                    doc_id,
                    {
                        "status": "invalid",
                        "reason": "non_github_or_invalid_repository_url",
                    },
                )
            )
            if len(pending_updates) >= BULK_FLUSH_SIZE:
                _flush_seed_updates(store, pending_updates)
            continue

        normalized_owner = str(owner or "").strip().lower()
        normalized_repo = str(repo or "").strip().lower()
        normalized_canonical_url = f"https://github.com/{normalized_owner}/{normalized_repo}"
        normalized_repo_id = _repo_id(normalized_owner, normalized_repo)

        if normalized_repo_id in registered_repo_ids:
            already_registered_count += 1
            pending_updates.append(
                (
                    doc_id,
                    {
                        "status": "already_registered",
                        "reason": "repo_already_registered",
                        "selected_repository_url": selected_url,
                        "owner": normalized_owner,
                        "repo": normalized_repo,
                        "canonical_repo_url": normalized_canonical_url,
                    },
                )
            )
            if len(pending_updates) >= BULK_FLUSH_SIZE:
                _flush_seed_updates(store, pending_updates)
            continue

        normalized_count += 1
        pending_updates.append(
            (
                doc_id,
                {
                    "status": "normalized",
                    "selected_repository_url": selected_url,
                    "owner": normalized_owner,
                    "repo": normalized_repo,
                    "canonical_repo_url": normalized_canonical_url,
                },
            )
        )
        if len(pending_updates) >= BULK_FLUSH_SIZE:
            _flush_seed_updates(store, pending_updates)

    _flush_seed_updates(store, pending_updates)
    if processed_count > 0:
        store.refresh_index("seed_item_index")

    logger.info(
        (
            "Seed normalization shard completed: shard=%s/%s scanned=%s processed=%s "
            "normalized=%s already_registered=%s invalid=%s"
        ),
        normalized_shard_index,
        resolved_shard_count,
        scanned_count,
        processed_count,
        normalized_count,
        already_registered_count,
        invalid_count,
    )
    return {
        "stage": "seed_normalization",
        "shard_index": normalized_shard_index,
        "shard_count": resolved_shard_count,
        "scanned_count": scanned_count,
        "processed_count": processed_count,
        "normalized_count": normalized_count,
        "already_registered_count": already_registered_count,
        "invalid_count": invalid_count,
    }


def run_seed_normalization() -> None:
    run_seed_normalization_for_shard(0, shard_count=1)
