from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.common.opensearch_queries import build_keyword_or_term_query
from worker.repo.common.time_utils import is_stale_timestamp, utcnow_iso
from worker.repo.snapshot.repo_snapshot_local_paths import strip_local_snapshot_fields
from worker.storage.opensearch_store import OpenSearchStore


REPO_REGISTRY_INDEX = "repo_registry_index"

REPO_PROCESSING_RETRY_STATE_PENDING = "retry_pending"
REPO_PROCESSING_RETRY_STATE_RETRYING = "retrying"

REPO_PROCESSING_RETRY_STAGE_CRAWL = "crawl"
REPO_PROCESSING_RETRY_STAGE_EXTRACT = "extract"

_RETRY_STATE_FIELDS = (
    "processing_retry_state",
    "processing_retry_stage",
    "processing_retry_reason",
    "processing_retry_marked_at",
    "processing_retry_started_at",
    "processing_retry_last_at",
    "processing_retry_attempt_count",
)


def clear_repo_processing_retry_state(source: dict) -> dict:
    cleaned = strip_local_snapshot_fields(dict(source))
    for field_name in _RETRY_STATE_FIELDS:
        cleaned.pop(field_name, None)
    return cleaned


def mark_repo_processing_retry_pending(
    source: dict,
    *,
    stage: str,
    reason: str,
    marked_at: str | None = None,
) -> dict:
    updated = strip_local_snapshot_fields(dict(source))
    timestamp = marked_at or _utcnow_iso()
    updated["processing_retry_state"] = REPO_PROCESSING_RETRY_STATE_PENDING
    updated["processing_retry_stage"] = stage
    updated["processing_retry_reason"] = reason
    updated["processing_retry_marked_at"] = timestamp
    updated["processing_retry_last_at"] = timestamp
    updated.pop("processing_retry_started_at", None)
    updated["processing_retry_attempt_count"] = int(updated.get("processing_retry_attempt_count") or 0)
    return updated


def mark_repo_processing_retrying(
    source: dict,
    *,
    stage: str,
    started_at: str | None = None,
) -> dict:
    updated = strip_local_snapshot_fields(dict(source))
    timestamp = started_at or _utcnow_iso()
    updated["processing_retry_state"] = REPO_PROCESSING_RETRY_STATE_RETRYING
    updated["processing_retry_stage"] = stage
    updated["processing_retry_started_at"] = timestamp
    updated["processing_retry_last_at"] = timestamp
    updated["processing_retry_attempt_count"] = int(updated.get("processing_retry_attempt_count") or 0) + 1
    if not str(updated.get("processing_retry_marked_at") or "").strip():
        updated["processing_retry_marked_at"] = timestamp
    return updated


def is_repo_processing_retry_managed(source: dict, *, stage: str | None = None) -> bool:
    retry_state = str(source.get("processing_retry_state") or "").strip()
    if retry_state not in {
        REPO_PROCESSING_RETRY_STATE_PENDING,
        REPO_PROCESSING_RETRY_STATE_RETRYING,
    }:
        return False
    if stage is None:
        return True
    return str(source.get("processing_retry_stage") or "").strip() == stage


def iter_retry_candidate_docs(
    store: OpenSearchStore,
    *,
    stage: str | None = None,
):
    for hit in store.iterate_documents_by_query(
        collection_name=REPO_REGISTRY_INDEX,
        query=build_keyword_or_term_query("processing_retry_state", REPO_PROCESSING_RETRY_STATE_PENDING),
        size=1000,
        sort=[{"_id": "asc"}],
    ):
        source = hit.get("_source", {})
        if stage is not None and str(source.get("processing_retry_stage") or "").strip() != stage:
            continue
        yield hit


def refresh_repo_processing_retry_candidates(
    *,
    store: OpenSearchStore | None = None,
    refresh_writes: bool = False,
) -> dict[str, int]:
    local_store = store or OpenSearchStore()
    stale_after_seconds = max(1, int(settings.repo_retry_stale_after_seconds))
    now = datetime.now(timezone.utc)
    counts = {
        "crawl_failed_marked": 0,
        "downloading_stale_marked": 0,
        "downloaded_not_extracted_marked": 0,
        "extract_failed_marked": 0,
        "extracting_stale_marked": 0,
    }

    for hit in local_store.iterate_documents_by_query(
        collection_name=REPO_REGISTRY_INDEX,
        query=build_keyword_or_term_query("crawl_status", "crawl_failed"),
        size=1000,
        sort=[{"_id": "asc"}],
    ):
        doc_id = hit.get("_id")
        source = dict(hit.get("_source", {}))
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue
        if is_repo_processing_retry_managed(source, stage=REPO_PROCESSING_RETRY_STAGE_CRAWL):
            continue
        updated_source = mark_repo_processing_retry_pending(
            source,
            stage=REPO_PROCESSING_RETRY_STAGE_CRAWL,
            reason=str(source.get("crawl_error_message") or "crawl_failed"),
            marked_at=now.isoformat(),
        )
        local_store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=updated_source,
            refresh=refresh_writes,
        )
        counts["crawl_failed_marked"] += 1

    for hit in local_store.iterate_documents_by_query(
        collection_name=REPO_REGISTRY_INDEX,
        query=build_keyword_or_term_query("crawl_status", "downloading"),
        size=1000,
        sort=[{"_id": "asc"}],
    ):
        doc_id = hit.get("_id")
        source = dict(hit.get("_source", {}))
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue
        if is_repo_processing_retry_managed(source, stage=REPO_PROCESSING_RETRY_STAGE_CRAWL):
            continue
        if not _is_stale_timestamp(
            source.get("crawl_started_at"),
            now=now,
            stale_after_seconds=stale_after_seconds,
        ):
            continue
        failed_source = strip_local_snapshot_fields(source)
        failed_source["crawl_status"] = "crawl_failed"
        failed_source["crawl_finished_at"] = now.isoformat()
        failed_source["crawl_error_message"] = (
            str(source.get("crawl_error_message") or "").strip() or "stale_downloading_lease_expired"
        )
        updated_source = mark_repo_processing_retry_pending(
            failed_source,
            stage=REPO_PROCESSING_RETRY_STAGE_CRAWL,
            reason=str(failed_source.get("crawl_error_message") or "stale_downloading_lease_expired"),
            marked_at=now.isoformat(),
        )
        local_store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=updated_source,
            refresh=refresh_writes,
        )
        counts["downloading_stale_marked"] += 1

    for hit in local_store.iterate_documents_by_query(
        collection_name=REPO_REGISTRY_INDEX,
        query=build_keyword_or_term_query("crawl_status", "downloaded"),
        size=1000,
        sort=[{"_id": "asc"}],
    ):
        doc_id = hit.get("_id")
        source = dict(hit.get("_source", {}))
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue
        if is_repo_processing_retry_managed(source, stage=REPO_PROCESSING_RETRY_STAGE_EXTRACT):
            continue
        file_extract_status = str(source.get("file_extract_status") or "").strip()
        if file_extract_status in {"extracted", "extract_failed", "extracting"}:
            continue
        if not _is_stale_timestamp(
            source.get("crawl_finished_at") or source.get("crawled_at"),
            now=now,
            stale_after_seconds=stale_after_seconds,
        ):
            continue
        updated_source = mark_repo_processing_retry_pending(
            source,
            stage=REPO_PROCESSING_RETRY_STAGE_EXTRACT,
            reason="downloaded_not_extracted_stale",
            marked_at=now.isoformat(),
        )
        local_store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=updated_source,
            refresh=refresh_writes,
        )
        counts["downloaded_not_extracted_marked"] += 1

    for hit in local_store.iterate_documents_by_query(
        collection_name=REPO_REGISTRY_INDEX,
        query=build_keyword_or_term_query("file_extract_status", "extract_failed"),
        size=1000,
        sort=[{"_id": "asc"}],
    ):
        doc_id = hit.get("_id")
        source = dict(hit.get("_source", {}))
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue
        if is_repo_processing_retry_managed(source, stage=REPO_PROCESSING_RETRY_STAGE_EXTRACT):
            continue
        updated_source = mark_repo_processing_retry_pending(
            source,
            stage=REPO_PROCESSING_RETRY_STAGE_EXTRACT,
            reason=str(source.get("file_extract_error_message") or "extract_failed"),
            marked_at=now.isoformat(),
        )
        local_store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=updated_source,
            refresh=refresh_writes,
        )
        counts["extract_failed_marked"] += 1

    for hit in local_store.iterate_documents_by_query(
        collection_name=REPO_REGISTRY_INDEX,
        query=build_keyword_or_term_query("file_extract_status", "extracting"),
        size=1000,
        sort=[{"_id": "asc"}],
    ):
        doc_id = hit.get("_id")
        source = dict(hit.get("_source", {}))
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue
        if is_repo_processing_retry_managed(source, stage=REPO_PROCESSING_RETRY_STAGE_EXTRACT):
            continue
        if not _is_stale_timestamp(
            source.get("file_extract_started_at"),
            now=now,
            stale_after_seconds=stale_after_seconds,
        ):
            continue
        failed_source = strip_local_snapshot_fields(source)
        failed_source["file_extract_status"] = "extract_failed"
        failed_source["file_extract_finished_at"] = now.isoformat()
        failed_source["file_extract_error_message"] = (
            str(source.get("file_extract_error_message") or "").strip()
            or "stale_extracting_lease_expired"
        )
        updated_source = mark_repo_processing_retry_pending(
            failed_source,
            stage=REPO_PROCESSING_RETRY_STAGE_EXTRACT,
            reason=str(failed_source.get("file_extract_error_message") or "stale_extracting_lease_expired"),
            marked_at=now.isoformat(),
        )
        local_store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=updated_source,
            refresh=refresh_writes,
        )
        counts["extracting_stale_marked"] += 1

    return counts
