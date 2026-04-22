import asyncio
import hashlib
import httpx
import logging
from datetime import datetime, timezone
from typing import Iterable, TypeVar

from worker.common.config import settings
from worker.seed.adapters.github import (
    GitHubRepoMetadataAdapter,
    RepoMetadataFetchResult,
)
from worker.seed.qualifiers.repo_qualifier import qualify_repo
from worker.storage.opensearch_store import OpenSearchStore

logger = logging.getLogger(__name__)

QUALIFICATION_FAILURE_COLLECTION = "seed_qualification_failure_index"
BULK_FLUSH_SIZE = max(100, settings.opensearch_bulk_flush_docs)
T = TypeVar("T")


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


def _collect_source_types(hits: list[dict]) -> list[str]:
    return sorted(
        {
            hit["_source"].get("source_type")
            for hit in hits
            if hit.get("_source", {}).get("source_type")
        }
    )


def _failure_doc_id(owner: str, repo: str) -> str:
    return f"github:{owner}/{repo}"


def _build_qualification_failure_document(
    owner: str,
    repo: str,
    hits: list[dict],
    reason: str,
    error: Exception | None = None,
    status_code: int | None = None,
) -> tuple[str, dict]:
    source = hits[0]["_source"]
    repo_doc_id = _failure_doc_id(owner, repo)
    body = {
        "owner": owner,
        "repo": repo,
        "canonical_repo_url": source["canonical_repo_url"],
        "status": "qualification_failed",
        "reason": reason,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "group_size": len(hits),
        "source_types": _collect_source_types(hits),
        "seed_doc_ids": [hit["_id"] for hit in hits],
    }

    if status_code is not None:
        body["error_status_code"] = status_code

    if error is not None:
        body["error_message"] = str(error)

    return repo_doc_id, body


def _value_shard_index(value: str, shard_count: int) -> int:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % shard_count


def _is_repo_in_shard(owner: str, repo: str, shard_index: int, shard_count: int) -> bool:
    if shard_count <= 1:
        return True
    return _value_shard_index(_repo_id(owner, repo), shard_count) == (shard_index % shard_count)


def _group_by_repo(
    seed_docs: Iterable[dict],
    *,
    shard_index: int,
    shard_count: int,
) -> tuple[dict[tuple[str, str], list[dict]], int]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    scanned_count = 0
    for hit in seed_docs:
        scanned_count += 1
        source = hit.get("_source", {})
        owner = str(source.get("owner") or "").strip().lower()
        repo = str(source.get("repo") or "").strip().lower()
        if not owner or not repo:
            continue
        if not _is_repo_in_shard(owner, repo, shard_index, shard_count):
            continue

        repo_key = (owner, repo)

        if repo_key not in grouped:
            grouped[repo_key] = []

        grouped[repo_key].append(hit)

    return grouped, scanned_count


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


def _append_group_status_updates(
    pending_updates: list[tuple[str, dict]],
    hits: list[dict],
    *,
    status: str,
    reason: str | None = None,
) -> None:
    body = {"status": status}
    if reason is not None:
        body["reason"] = reason

    for group_hit in hits:
        pending_updates.append((group_hit["_id"], dict(body)))


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


def _flush_repo_registry_updates(
    store: OpenSearchStore,
    pending_updates: list[tuple[str, dict]],
) -> None:
    if not pending_updates:
        return
    store.bulk_upsert_documents(
        collection_name="repo_registry_index",
        documents=pending_updates,
        refresh=False,
        chunk_size=BULK_FLUSH_SIZE,
    )
    pending_updates.clear()


def _flush_failure_updates(
    store: OpenSearchStore,
    pending_updates: list[tuple[str, dict]],
) -> None:
    if not pending_updates:
        return
    store.bulk_upsert_documents(
        collection_name=QUALIFICATION_FAILURE_COLLECTION,
        documents=pending_updates,
        refresh=False,
        chunk_size=BULK_FLUSH_SIZE,
    )
    pending_updates.clear()


def _flush_failure_deletes(
    store: OpenSearchStore,
    pending_doc_ids: list[str],
) -> None:
    if not pending_doc_ids:
        return
    store.bulk_delete_documents(
        collection_name=QUALIFICATION_FAILURE_COLLECTION,
        doc_ids=pending_doc_ids,
        refresh=False,
        chunk_size=BULK_FLUSH_SIZE,
    )
    pending_doc_ids.clear()


def _iter_chunked(items: list[T], chunk_size: int) -> Iterable[list[T]]:
    safe_chunk_size = max(1, chunk_size)
    for start in range(0, len(items), safe_chunk_size):
        yield items[start : start + safe_chunk_size]


def _build_repo_registry_body(
    source: dict,
    hits: list[dict],
    repo_metadata: dict,
) -> dict:
    license_info = repo_metadata.get("license") or {}
    parent = repo_metadata.get("parent") or {}
    topics = repo_metadata.get("topics") or []
    if not isinstance(topics, list):
        topics = []

    return {
        "canonical_repo_url": source["canonical_repo_url"],
        "owner": source["owner"].lower(),
        "repo_name": source["repo"].lower(),
        "default_branch": repo_metadata.get("default_branch"),
        "hosting_platform": "github",
        "crawl_status": "scheduled",
        "discovery_source_count": len(hits),
        "source_types": _collect_source_types(hits),
        "repo_description": repo_metadata.get("description"),
        "repo_homepage": repo_metadata.get("homepage"),
        "repo_language": repo_metadata.get("language"),
        "repo_topics": [topic for topic in topics if isinstance(topic, str) and topic.strip()],
        "repo_license_spdx": license_info.get("spdx_id"),
        "repo_license_name": license_info.get("name"),
        "repo_visibility": repo_metadata.get("visibility"),
        "repo_owner_type": ((repo_metadata.get("owner") or {}).get("type")),
        "repo_stars": repo_metadata.get("stargazers_count"),
        "repo_forks_count": repo_metadata.get("forks_count"),
        "repo_watchers_count": repo_metadata.get("watchers_count"),
        "repo_open_issues_count": repo_metadata.get("open_issues_count"),
        "repo_size_kb": repo_metadata.get("size"),
        "repo_is_fork": repo_metadata.get("fork") is True,
        "repo_parent_full_name": str(parent.get("full_name") or "").strip() or None,
        "repo_created_at": repo_metadata.get("created_at"),
        "repo_updated_at": repo_metadata.get("updated_at"),
        "repo_pushed_at": repo_metadata.get("pushed_at"),
    }


def _fetch_repo_metadata_results(
    metadata_adapter: GitHubRepoMetadataAdapter,
    repo_keys: list[tuple[str, str]],
) -> dict[tuple[str, str], RepoMetadataFetchResult]:
    # 현재 서비스 함수는 동기 함수라서 async 배치를 여기서 실행
    if not repo_keys:
        return {}
    return asyncio.run(metadata_adapter.fetch_repo_metadata_batch(repo_keys))


def run_seed_qualification_for_shard(
    shard_index: int,
    *,
    shard_count: int | None = None,
) -> dict:
    resolved_shard_count = (
        shard_count
        if isinstance(shard_count, int) and shard_count > 0
        else max(1, settings.seed_qualify_parallelism)
    )
    normalized_shard_index = shard_index % resolved_shard_count

    store = OpenSearchStore()
    metadata_adapter = GitHubRepoMetadataAdapter()
    grouped_seed_docs, scanned_seed_docs = _group_by_repo(
        _iter_seed_docs_by_status(store, "normalized"),
        shard_index=normalized_shard_index,
        shard_count=resolved_shard_count,
    )
    registered_repo_ids = _load_registered_repo_ids(store)
    pending_seed_updates: list[tuple[str, dict]] = []
    pending_repo_registry_updates: list[tuple[str, dict]] = []
    pending_failure_updates: list[tuple[str, dict]] = []
    pending_failure_deletes: list[str] = []

    touched_seed_item_index = False
    touched_repo_registry_index = False
    touched_failure_index = False

    existing_repo_groups: dict[tuple[str, str], list[dict]] = {}
    to_qualify_groups: dict[tuple[str, str], list[dict]] = {}
    for repo_key, hits in grouped_seed_docs.items():
        owner, repo = repo_key
        if _repo_id(owner, repo) in registered_repo_ids:
            existing_repo_groups[repo_key] = hits
        else:
            to_qualify_groups[repo_key] = hits

    if existing_repo_groups:
        logger.info(
            "Seed qualification skipped already-registered repos: repo_count=%s seed_doc_count=%s",
            len(existing_repo_groups),
            sum(len(hits) for hits in existing_repo_groups.values()),
        )

    for (owner, repo), hits in existing_repo_groups.items():
        pending_failure_deletes.append(_failure_doc_id(owner, repo))
        touched_failure_index = True
        if len(pending_failure_deletes) >= BULK_FLUSH_SIZE:
            _flush_failure_deletes(store, pending_failure_deletes)
        _append_group_status_updates(
            pending_seed_updates,
            hits,
            status="already_registered",
            reason="repo_already_registered",
        )
        touched_seed_item_index = True
        if len(pending_seed_updates) >= BULK_FLUSH_SIZE:
            _flush_seed_updates(store, pending_seed_updates)

    if not to_qualify_groups:
        _flush_seed_updates(store, pending_seed_updates)
        _flush_repo_registry_updates(store, pending_repo_registry_updates)
        _flush_failure_updates(store, pending_failure_updates)
        _flush_failure_deletes(store, pending_failure_deletes)
        if touched_seed_item_index:
            store.refresh_index("seed_item_index")
        if touched_repo_registry_index:
            store.refresh_index("repo_registry_index")
        if touched_failure_index:
            store.refresh_index(QUALIFICATION_FAILURE_COLLECTION)
        return {
            "stage": "seed_qualification",
            "shard_index": normalized_shard_index,
            "shard_count": resolved_shard_count,
            "scanned_seed_docs": scanned_seed_docs,
            "repo_group_count": len(grouped_seed_docs),
            "existing_repo_group_count": len(existing_repo_groups),
            "to_qualify_group_count": 0,
            "registered_group_count": 0,
            "rejected_group_count": 0,
            "qualification_failed_group_count": 0,
        }

    to_qualify_items = list(to_qualify_groups.items())

    registered_group_count = 0
    rejected_group_count = 0
    qualification_failed_group_count = 0

    # 메타데이터 fetch 및 OpenSearch 쓰기를 chunk 단위로 반복해
    # "끝에 한 번에 쓰기" 대신 중간중간 밀어 넣는다.
    for chunk_items in _iter_chunked(to_qualify_items, BULK_FLUSH_SIZE):
        fetch_results = _fetch_repo_metadata_results(
            metadata_adapter,
            [repo_key for repo_key, _ in chunk_items],
        )

        for (owner, repo), hits in chunk_items:
            source = hits[0]["_source"]
            repo_doc_id = _repo_id(owner, repo)
            fetch_result = fetch_results[(owner, repo)]
            exc = fetch_result.error

            if isinstance(exc, httpx.HTTPStatusError):
                status_code = exc.response.status_code if exc.response is not None else None
                reason = "github_rate_limited" if status_code == 403 else "github_api_error"
                logger.warning(
                    "GitHub qualification failed for %s/%s: reason=%s status_code=%s group_size=%s error=%s",
                    owner,
                    repo,
                    reason,
                    status_code,
                    len(hits),
                    str(exc),
                )
                pending_failure_updates.append(
                    _build_qualification_failure_document(
                        owner=owner,
                        repo=repo,
                        hits=hits,
                        reason=reason,
                        error=exc,
                        status_code=status_code,
                    )
                )
                touched_failure_index = True
                if len(pending_failure_updates) >= BULK_FLUSH_SIZE:
                    _flush_failure_updates(store, pending_failure_updates)
                _append_group_status_updates(
                    pending_seed_updates,
                    hits,
                    status="qualification_failed",
                    reason=reason,
                )
                qualification_failed_group_count += 1
                touched_seed_item_index = True
                if len(pending_seed_updates) >= BULK_FLUSH_SIZE:
                    _flush_seed_updates(store, pending_seed_updates)
                continue

            if isinstance(exc, httpx.RequestError):
                logger.warning(
                    "GitHub request failed for %s/%s: group_size=%s error=%s",
                    owner,
                    repo,
                    len(hits),
                    str(exc),
                )
                pending_failure_updates.append(
                    _build_qualification_failure_document(
                        owner=owner,
                        repo=repo,
                        hits=hits,
                        reason="github_request_failed",
                        error=exc,
                    )
                )
                touched_failure_index = True
                if len(pending_failure_updates) >= BULK_FLUSH_SIZE:
                    _flush_failure_updates(store, pending_failure_updates)
                _append_group_status_updates(
                    pending_seed_updates,
                    hits,
                    status="qualification_failed",
                    reason="github_request_failed",
                )
                qualification_failed_group_count += 1
                touched_seed_item_index = True
                if len(pending_seed_updates) >= BULK_FLUSH_SIZE:
                    _flush_seed_updates(store, pending_seed_updates)
                continue

            if exc is not None:
                logger.warning(
                    "Unexpected GitHub qualification failure for %s/%s: group_size=%s error=%s",
                    owner,
                    repo,
                    len(hits),
                    str(exc),
                )
                pending_failure_updates.append(
                    _build_qualification_failure_document(
                        owner=owner,
                        repo=repo,
                        hits=hits,
                        reason="github_unknown_error",
                        error=exc,
                    )
                )
                touched_failure_index = True
                if len(pending_failure_updates) >= BULK_FLUSH_SIZE:
                    _flush_failure_updates(store, pending_failure_updates)
                _append_group_status_updates(
                    pending_seed_updates,
                    hits,
                    status="qualification_failed",
                    reason="github_unknown_error",
                )
                qualification_failed_group_count += 1
                touched_seed_item_index = True
                if len(pending_seed_updates) >= BULK_FLUSH_SIZE:
                    _flush_seed_updates(store, pending_seed_updates)
                continue

            repo_metadata = fetch_result.metadata
            is_eligible, reason, _score = qualify_repo(repo_metadata)

            if not is_eligible:
                pending_failure_deletes.append(_failure_doc_id(owner, repo))
                touched_failure_index = True
                if len(pending_failure_deletes) >= BULK_FLUSH_SIZE:
                    _flush_failure_deletes(store, pending_failure_deletes)
                _append_group_status_updates(
                    pending_seed_updates,
                    hits,
                    status="rejected",
                    reason=reason,
                )
                rejected_group_count += 1
                touched_seed_item_index = True
                if len(pending_seed_updates) >= BULK_FLUSH_SIZE:
                    _flush_seed_updates(store, pending_seed_updates)
                continue

            pending_failure_deletes.append(_failure_doc_id(owner, repo))
            touched_failure_index = True
            if len(pending_failure_deletes) >= BULK_FLUSH_SIZE:
                _flush_failure_deletes(store, pending_failure_deletes)

            repo_body = _build_repo_registry_body(
                source=source,
                hits=hits,
                repo_metadata=repo_metadata,
            )
            pending_repo_registry_updates.append((repo_doc_id, repo_body))
            touched_repo_registry_index = True
            if len(pending_repo_registry_updates) >= BULK_FLUSH_SIZE:
                _flush_repo_registry_updates(store, pending_repo_registry_updates)

            _append_group_status_updates(
                pending_seed_updates,
                hits,
                status="registered",
            )
            registered_group_count += 1
            touched_seed_item_index = True
            if len(pending_seed_updates) >= BULK_FLUSH_SIZE:
                _flush_seed_updates(store, pending_seed_updates)

        # chunk 경계에서 남은 pending을 밀어내어 long tail을 줄인다.
        _flush_seed_updates(store, pending_seed_updates)
        _flush_repo_registry_updates(store, pending_repo_registry_updates)
        _flush_failure_updates(store, pending_failure_updates)
        _flush_failure_deletes(store, pending_failure_deletes)

    _flush_seed_updates(store, pending_seed_updates)
    _flush_repo_registry_updates(store, pending_repo_registry_updates)
    _flush_failure_updates(store, pending_failure_updates)
    _flush_failure_deletes(store, pending_failure_deletes)

    if touched_seed_item_index:
        store.refresh_index("seed_item_index")
    if touched_repo_registry_index:
        store.refresh_index("repo_registry_index")
    if touched_failure_index:
        store.refresh_index(QUALIFICATION_FAILURE_COLLECTION)

    logger.info(
        (
            "Seed qualification shard completed: shard=%s/%s scanned_seed_docs=%s "
            "repo_group_count=%s existing_group_count=%s to_qualify_group_count=%s "
            "registered_group_count=%s rejected_group_count=%s failed_group_count=%s"
        ),
        normalized_shard_index,
        resolved_shard_count,
        scanned_seed_docs,
        len(grouped_seed_docs),
        len(existing_repo_groups),
        len(to_qualify_groups),
        registered_group_count,
        rejected_group_count,
        qualification_failed_group_count,
    )
    return {
        "stage": "seed_qualification",
        "shard_index": normalized_shard_index,
        "shard_count": resolved_shard_count,
        "scanned_seed_docs": scanned_seed_docs,
        "repo_group_count": len(grouped_seed_docs),
        "existing_repo_group_count": len(existing_repo_groups),
        "to_qualify_group_count": len(to_qualify_groups),
        "registered_group_count": registered_group_count,
        "rejected_group_count": rejected_group_count,
        "qualification_failed_group_count": qualification_failed_group_count,
    }


def run_seed_qualification() -> None:
    run_seed_qualification_for_shard(0, shard_count=1)
