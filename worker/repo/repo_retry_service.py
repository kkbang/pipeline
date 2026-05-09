from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.repo_crawl_service import run_repo_snapshot_download_for_repo
from worker.repo.repo_file_extract_service import run_repo_file_extraction_for_repo
from worker.repo.repo_pipeline_manifest_service import repo_id_shard_index
from worker.repo.repo_retry_state import (
    REPO_PROCESSING_RETRY_STAGE_CRAWL,
    REPO_PROCESSING_RETRY_STAGE_EXTRACT,
    clear_repo_processing_retry_state,
    iter_retry_candidate_docs,
    mark_repo_processing_retry_pending,
    mark_repo_processing_retrying,
    refresh_repo_processing_retry_candidates,
)
from worker.storage.opensearch_store import OpenSearchStore


REPO_REGISTRY_INDEX = "repo_registry_index"


def _normalized_repo_ids(
    repo_ids: list[str],
    *,
    shard_index: int,
    shard_count: int,
    batch_limit: int | None,
) -> list[str]:
    unique_repo_ids = sorted({repo_id for repo_id in repo_ids if isinstance(repo_id, str) and repo_id.strip()})
    normalized_shard_index = shard_index % shard_count
    selected_repo_ids = [
        repo_id
        for repo_id in unique_repo_ids
        if repo_id_shard_index(repo_id, shard_count) == normalized_shard_index
    ]
    if batch_limit is None or batch_limit <= 0:
        return selected_repo_ids
    return selected_repo_ids[:batch_limit]


def list_repo_ids_for_processing_retry(
    *,
    store: OpenSearchStore,
    shard_index: int,
    shard_count: int,
    batch_size: int | None = None,
) -> list[str]:
    refresh_repo_processing_retry_candidates(store=store, refresh_writes=False)
    repo_ids = [
        hit.get("_id")
        for hit in iter_retry_candidate_docs(store)
        if isinstance(hit.get("_id"), str) and str(hit.get("_id")).strip()
    ]
    return _normalized_repo_ids(
        repo_ids,
        shard_index=shard_index,
        shard_count=shard_count,
        batch_limit=batch_size,
    )


def run_repo_processing_retry_for_repo(
    repo_id: str,
    *,
    store: OpenSearchStore | None = None,
    refresh_writes: bool = False,
) -> dict:
    local_store = store or OpenSearchStore()
    repo_doc = local_store.get_document(collection_name=REPO_REGISTRY_INDEX, doc_id=repo_id)
    if not repo_doc or not repo_doc.get("_source"):
        return {
            "repo_id": repo_id,
            "stage": "retry",
            "stage_status": "skipped",
            "reason": "repo_not_found",
        }

    source = dict(repo_doc.get("_source", {}))
    retry_stage = str(source.get("processing_retry_stage") or "").strip()
    if retry_stage not in {REPO_PROCESSING_RETRY_STAGE_CRAWL, REPO_PROCESSING_RETRY_STAGE_EXTRACT}:
        return {
            "repo_id": repo_id,
            "stage": "retry",
            "stage_status": "skipped",
            "reason": "retry_stage_not_supported",
        }

    retrying_source = mark_repo_processing_retrying(
        source,
        stage=retry_stage,
        started_at=datetime.now(timezone.utc).isoformat(),
    )
    local_store.replace_document(
        collection_name=REPO_REGISTRY_INDEX,
        doc_id=repo_id,
        source=retrying_source,
        refresh=refresh_writes,
    )

    if retry_stage == REPO_PROCESSING_RETRY_STAGE_CRAWL:
        crawl_result = run_repo_snapshot_download_for_repo(
            repo_id,
            store=local_store,
            refresh_writes=refresh_writes,
        )
        crawl_status = str(crawl_result.get("stage_status") or "")
        if crawl_status == "downloaded":
            result = run_repo_file_extraction_for_repo(
                repo_id,
                store=local_store,
                refresh_writes=refresh_writes,
            )
        else:
            result = crawl_result
    else:
        result = run_repo_file_extraction_for_repo(
            repo_id,
            store=local_store,
            refresh_writes=refresh_writes,
        )

    latest_doc = local_store.get_document(collection_name=REPO_REGISTRY_INDEX, doc_id=repo_id)
    latest_source = dict(latest_doc.get("_source", {})) if latest_doc else {}
    latest_retry_state = str(latest_source.get("processing_retry_state") or "").strip()
    latest_retry_stage = str(latest_source.get("processing_retry_stage") or "").strip()
    result_status = str(result.get("stage_status") or "")

    if latest_retry_state == "retrying" and latest_retry_stage == retry_stage:
        if result_status in {"downloaded", "extracted"}:
            local_store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=repo_id,
                source=clear_repo_processing_retry_state(latest_source),
                refresh=refresh_writes,
            )
        else:
            pending_source = mark_repo_processing_retry_pending(
                latest_source,
                stage=retry_stage,
                reason=str(
                    result.get("error_message")
                    or result.get("reason")
                    or result_status
                    or f"{retry_stage}_retry_incomplete"
                ),
                marked_at=datetime.now(timezone.utc).isoformat(),
            )
            local_store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=repo_id,
                source=pending_source,
                refresh=refresh_writes,
            )

    return {
        "repo_id": repo_id,
        "stage": "retry",
        "retry_stage": retry_stage,
        **result,
    }


def run_repo_processing_retry_for_shard(
    shard_index: int,
    *,
    shard_count: int | None = None,
    batch_size: int | None = None,
) -> dict:
    resolved_shard_count = (
        shard_count
        if isinstance(shard_count, int) and shard_count > 0
        else max(1, settings.repo_pipeline_parallelism)
    )
    effective_batch_size = (
        batch_size if isinstance(batch_size, int) and batch_size > 0 else settings.repo_retry_repo_limit
    )
    store = OpenSearchStore()
    repo_ids = list_repo_ids_for_processing_retry(
        store=store,
        shard_index=shard_index,
        shard_count=resolved_shard_count,
        batch_size=effective_batch_size,
    )

    processed_count = 0
    completed_count = 0
    failed_count = 0
    skipped_count = 0
    for repo_id in repo_ids:
        processed_count += 1
        result = run_repo_processing_retry_for_repo(
            repo_id,
            store=store,
            refresh_writes=False,
        )
        stage_status = str(result.get("stage_status") or "")
        if stage_status in {"downloaded", "extracted"}:
            completed_count += 1
        elif stage_status == "skipped":
            skipped_count += 1
        else:
            failed_count += 1

    return {
        "stage": "retry",
        "shard_index": shard_index,
        "shard_count": resolved_shard_count,
        "processed_count": processed_count,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
    }
