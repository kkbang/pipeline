import asyncio
import httpx
import logging
from datetime import datetime, timezone

from worker.seed.adapters.github import (
    GitHubRepoMetadataAdapter,
    RepoMetadataFetchResult,
)
from worker.seed.qualifiers.repo_qualifier import qualify_repo
from worker.storage.local_json_store import LocalJsonStore

# from worker.storage.opensearch_store import OpenSearchStore

logger = logging.getLogger(__name__)

QUALIFICATION_FAILURE_COLLECTION = "seed_qualification_failure_index"


def _collect_source_types(hits: list[dict]) -> list[str]:
    return sorted(
        {
            hit["_source"].get("source_type")
            for hit in hits
            if hit.get("_source", {}).get("source_type")
        }
    )


def _record_qualification_failure(
    store: LocalJsonStore,
    owner: str,
    repo: str,
    hits: list[dict],
    reason: str,
    error: Exception | None = None,
    status_code: int | None = None,
) -> None:
    source = hits[0]["_source"]
    repo_doc_id = f"github:{owner}/{repo}"
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

    store.upsert_document(
        collection_name=QUALIFICATION_FAILURE_COLLECTION,
        doc_id=repo_doc_id,
        body=body,
    )


def _clear_qualification_failure(
    store: LocalJsonStore,
    owner: str,
    repo: str,
) -> None:
    store.delete_document(
        collection_name=QUALIFICATION_FAILURE_COLLECTION,
        doc_id=f"github:{owner}/{repo}",
    )


def _group_by_repo(seed_docs: list[dict]) -> dict[tuple[str, str], list[dict]]:
    grouped = {}
    for hit in seed_docs:
        source = hit["_source"]
        owner = source["owner"].lower()
        repo = source["repo"].lower()
        repo_key = (owner, repo)

        if repo_key not in grouped:
            grouped[repo_key] = []

        grouped[repo_key].append(hit)

    return grouped


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


def run_seed_qualification() -> None:
    store = LocalJsonStore()
    # os = OpenSearchStore()
    metadata_adapter = GitHubRepoMetadataAdapter()
    seed_docs = store.find_documents_by_status(
        collection_name="seed_item_index",
        status="normalized",
    )
    grouped_seed_docs = _group_by_repo(seed_docs)

    # GitHub 메타데이터 병렬로 수집
    fetch_results = _fetch_repo_metadata_results(
        metadata_adapter,
        list(grouped_seed_docs.keys()),
    )

    # 저장/업데이트는 그대로 순차 처리해서 충돌 가능성을 줄임
    for (owner, repo), hits in grouped_seed_docs.items():
        source = hits[0]["_source"]
        repo_doc_id = f"github:{owner}/{repo}"
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
            _record_qualification_failure(
                store=store,
                owner=owner,
                repo=repo,
                hits=hits,
                reason=reason,
                error=exc,
                status_code=status_code,
            )
            for group_hit in hits:
                store.update_document(
                    collection_name="seed_item_index",
                    doc_id=group_hit["_id"],
                    body={
                        "status": "qualification_failed",
                        "reason": reason,
                    },
                )
            continue

        if isinstance(exc, httpx.RequestError):
            logger.warning(
                "GitHub request failed for %s/%s: group_size=%s error=%s",
                owner,
                repo,
                len(hits),
                str(exc),
            )
            _record_qualification_failure(
                store=store,
                owner=owner,
                repo=repo,
                hits=hits,
                reason="github_request_failed",
                error=exc,
            )
            for group_hit in hits:
                store.update_document(
                    collection_name="seed_item_index",
                    doc_id=group_hit["_id"],
                    body={
                        "status": "qualification_failed",
                        "reason": "github_request_failed",
                    },
                )
            continue

        if exc is not None:
            logger.warning(
                "Unexpected GitHub qualification failure for %s/%s: group_size=%s error=%s",
                owner,
                repo,
                len(hits),
                str(exc),
            )
            _record_qualification_failure(
                store=store,
                owner=owner,
                repo=repo,
                hits=hits,
                reason="github_unknown_error",
                error=exc,
            )
            for group_hit in hits:
                store.update_document(
                    collection_name="seed_item_index",
                    doc_id=group_hit["_id"],
                    body={
                        "status": "qualification_failed",
                        "reason": "github_unknown_error",
                    },
                )
            continue

        repo_metadata = fetch_result.metadata
        is_eligible, reason, _score = qualify_repo(repo_metadata)

        if not is_eligible:
            _clear_qualification_failure(store, owner, repo)
            for group_hit in hits:
                store.update_document(
                    collection_name="seed_item_index",
                    doc_id=group_hit["_id"],
                    body={
                        "status": "rejected",
                        "reason": reason,
                    },
                )
            continue

        _clear_qualification_failure(store, owner, repo)

        repo_body = _build_repo_registry_body(
            source=source,
            hits=hits,
            repo_metadata=repo_metadata,
        )

        store.upsert_document(
            collection_name="repo_registry_index",
            doc_id=repo_doc_id,
            body=repo_body,
        )

        for group_hit in hits:
            store.update_document(
                collection_name="seed_item_index",
                doc_id=group_hit["_id"],
                body={
                    "status": "registered",
                },
            )
