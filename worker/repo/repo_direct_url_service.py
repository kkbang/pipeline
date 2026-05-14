from typing import Any

from worker.repo.code_chunk_document import CODE_CHUNK_INDEX_ALIAS
from worker.repo.repo_chunk_service import run_repo_code_chunking_for_repo
from worker.repo.repo_crawl_service import run_repo_snapshot_download_for_repo
from worker.repo.repo_file_extract_service import run_repo_file_extraction_for_repo
from worker.repo.repo_snapshot_cleanup import (
    FINAL_SNAPSHOT_CLEANUP_STATUSES,
    prune_local_snapshot_artifacts,
)
from worker.repo.repo_snapshot_local_paths import resolve_snapshot_root_path
from worker.repo.repo_validation_service import run_repo_processing_validation_for_repo
from worker.seed.resolvers.github_url_canonicalizer import canonicalize_github_repo_url
from worker.storage.opensearch_store import OpenSearchStore


REPO_REGISTRY_INDEX = "repo_registry_index"


def _repo_id(owner: str, repo_name: str) -> str:
    return f"github:{owner.lower()}/{repo_name.lower()}"


def _field_term_query(field_name: str, value: str) -> dict[str, Any]:
    return {
        "bool": {
            "should": [
                {"term": {f"{field_name}.keyword": value}},
                {"term": {field_name: value}},
            ],
            "minimum_should_match": 1,
        }
    }


def _ensure_local_snapshot_cleanup(
    *,
    repo_id: str,
    owner: str,
    repo_name: str,
    store: OpenSearchStore,
    refresh_writes: bool,
) -> dict[str, Any]:
    repo_doc = store.get_document(REPO_REGISTRY_INDEX, repo_id)
    repo_source = dict((repo_doc or {}).get("_source", {})) if isinstance(repo_doc, dict) else {}
    cleanup_status = str(repo_source.get("snapshot_local_cleanup_status") or "").strip()
    cleanup_error = repo_source.get("snapshot_local_cleanup_error")

    if cleanup_status not in FINAL_SNAPSHOT_CLEANUP_STATUSES:
        pruned_any, cleanup_error = prune_local_snapshot_artifacts(
            base_dir=store.base_dir,
            source={
                "owner": owner,
                "repo_name": repo_name,
            },
        )
        if pruned_any:
            cleanup_status = "pruned"
        elif cleanup_error == "snapshot_extract_dir_missing,snapshot_download_path_missing":
            cleanup_status = "missing_confirmed"
        else:
            cleanup_status = "skipped"
        store.upsert_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=repo_id,
            body={
                "snapshot_local_cleanup_status": cleanup_status,
                "snapshot_local_cleanup_error": cleanup_error,
            },
            refresh=refresh_writes,
        )

    snapshot_root_path = resolve_snapshot_root_path(
        store.base_dir,
        {
            "owner": owner,
            "repo_name": repo_name,
        },
    )
    return {
        "status": cleanup_status,
        "error": cleanup_error,
        "snapshot_root_exists": snapshot_root_path.exists(),
    }


def register_github_repo_url(
    repo_url: str,
    *,
    store: OpenSearchStore | None = None,
    refresh_writes: bool = False,
) -> dict[str, Any]:
    resolved_store = store or OpenSearchStore()
    canonicalized = canonicalize_github_repo_url(repo_url)
    if canonicalized is None:
        raise ValueError(f"unsupported GitHub repository URL: {repo_url}")

    owner, repo_name, canonical_repo_url = canonicalized
    repo_id = _repo_id(owner, repo_name)
    existing_doc = resolved_store.get_document(REPO_REGISTRY_INDEX, repo_id)
    existing_source = existing_doc.get("_source") if isinstance(existing_doc, dict) else {}
    created = not isinstance(existing_source, dict) or not existing_source

    resolved_store.upsert_document(
        collection_name=REPO_REGISTRY_INDEX,
        doc_id=repo_id,
        body={
            "canonical_repo_url": canonical_repo_url,
            "owner": owner.lower(),
            "repo_name": repo_name.lower(),
            "hosting_platform": "github",
            "crawl_status": "scheduled",
            "source_types": ["direct_input"],
        },
        refresh=refresh_writes,
    )
    return {
        "repo_id": repo_id,
        "owner": owner.lower(),
        "repo_name": repo_name.lower(),
        "canonical_repo_url": canonical_repo_url,
        "created": created,
    }


def process_github_repo_url(
    repo_url: str,
    *,
    store: OpenSearchStore | None = None,
    refresh_writes: bool = False,
    run_validation: bool = True,
) -> dict[str, Any]:
    resolved_store = store or OpenSearchStore()
    registration = register_github_repo_url(
        repo_url,
        store=resolved_store,
        refresh_writes=refresh_writes,
    )
    repo_id = str(registration["repo_id"])

    stage_results = [
        run_repo_snapshot_download_for_repo(
            repo_id,
            store=resolved_store,
            refresh_writes=refresh_writes,
        ),
        run_repo_file_extraction_for_repo(
            repo_id,
            store=resolved_store,
            refresh_writes=refresh_writes,
        ),
        run_repo_code_chunking_for_repo(
            repo_id,
            store=resolved_store,
            refresh_writes=refresh_writes,
        ),
    ]
    if run_validation:
        stage_results.append(
            run_repo_processing_validation_for_repo(
                repo_id,
                store=resolved_store,
                refresh_writes=refresh_writes,
            )
        )

    local_snapshot_cleanup = _ensure_local_snapshot_cleanup(
        repo_id=repo_id,
        owner=str(registration["owner"]),
        repo_name=str(registration["repo_name"]),
        store=resolved_store,
        refresh_writes=refresh_writes,
    )
    repo_doc = resolved_store.get_document(REPO_REGISTRY_INDEX, repo_id)
    repo_source = dict((repo_doc or {}).get("_source", {})) if isinstance(repo_doc, dict) else {}
    chunk_doc_count = resolved_store.count_documents(
        collection_name=CODE_CHUNK_INDEX_ALIAS,
        query=_field_term_query("repo_id", repo_id),
    )
    return {
        "repo_id": repo_id,
        "canonical_repo_url": registration["canonical_repo_url"],
        "registration": registration,
        "chunk_doc_count": chunk_doc_count,
        "local_snapshot_cleanup": local_snapshot_cleanup,
        "repo_status": {
            "crawl_status": repo_source.get("crawl_status"),
            "file_extract_status": repo_source.get("file_extract_status"),
            "chunk_status": repo_source.get("chunk_status"),
            "validation_status": repo_source.get("validation_status"),
        },
        "stages": stage_results,
    }
