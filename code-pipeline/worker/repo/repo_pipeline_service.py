from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.repo_chunk_service import run_repo_code_chunking_for_repo
from worker.repo.repo_file_extract_service import run_repo_file_extraction_for_repo
from worker.repo.repo_validation_service import run_repo_processing_validation_for_repo
from worker.storage.opensearch_store import OpenSearchStore


REPO_REGISTRY_INDEX = "repo_registry_index"


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _has_newer_chunk_than_validation(source: dict) -> bool:
    chunk_finished_at = _parse_datetime(source.get("chunk_finished_at"))
    validation_checked_at = _parse_datetime(source.get("validation_checked_at"))
    if chunk_finished_at is None:
        return False
    if validation_checked_at is None:
        return True
    return chunk_finished_at > validation_checked_at


def _needs_repo_pipeline_processing(source: dict) -> bool:
    if source.get("crawl_status") != "downloaded":
        return False

    if source.get("file_extract_status") != "extracted":
        return True

    if source.get("chunk_status") != "chunked":
        return True

    if source.get("validation_status") != "validated":
        return True

    return _has_newer_chunk_than_validation(source)


def list_repo_ids_for_repo_pipeline() -> list[str]:
    store = OpenSearchStore()
    downloaded_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="crawl_status",
        value="downloaded",
        size=max(1, settings.repo_pipeline_batch_size),
    )

    repo_ids = []
    for hit in downloaded_docs:
        doc_id = hit.get("_id")
        if not isinstance(doc_id, str) or not doc_id.strip():
            continue

        source = hit.get("_source", {})
        if _needs_repo_pipeline_processing(source):
            repo_ids.append(doc_id)

    return sorted(set(repo_ids))


def run_repo_pipeline_for_repo(
    repo_id: str,
    *,
    store: OpenSearchStore | None = None,
    refresh_writes: bool = True,
) -> dict:
    if store is None:
        store = OpenSearchStore()
    started_at = datetime.now(timezone.utc).isoformat()
    pipeline_result = {
        "repo_id": repo_id,
        "started_at": started_at,
        "pipeline_status": "running",
        "stages": {},
    }

    extract_result = run_repo_file_extraction_for_repo(
        repo_id,
        store=store,
        refresh_writes=refresh_writes,
    )
    pipeline_result["stages"]["extract"] = extract_result
    extract_stage_status = extract_result.get("stage_status")
    if extract_stage_status not in {"extracted"}:
        if extract_stage_status == "skipped" and extract_result.get("reason") == "already_extracted":
            pass
        else:
            pipeline_result["pipeline_status"] = "failed_extract"
            pipeline_result["finished_at"] = datetime.now(timezone.utc).isoformat()
            return pipeline_result

    chunk_result = run_repo_code_chunking_for_repo(
        repo_id,
        store=store,
        refresh_writes=refresh_writes,
    )
    pipeline_result["stages"]["chunk"] = chunk_result
    chunk_stage_status = chunk_result.get("stage_status")
    if chunk_stage_status not in {"chunked", "chunk_failed"}:
        if chunk_stage_status == "skipped" and chunk_result.get("reason") == "already_chunking":
            pipeline_result["pipeline_status"] = "skipped_chunking_in_progress"
        else:
            pipeline_result["pipeline_status"] = "failed_chunk"
        pipeline_result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return pipeline_result

    validation_result = run_repo_processing_validation_for_repo(
        repo_id,
        store=store,
        refresh_writes=refresh_writes,
    )
    pipeline_result["stages"]["validation"] = validation_result

    validation_stage_status = validation_result.get("stage_status")
    if validation_stage_status == "validated":
        pipeline_result["pipeline_status"] = "validated"
    elif validation_stage_status == "validation_failed":
        pipeline_result["pipeline_status"] = "validation_failed"
    else:
        pipeline_result["pipeline_status"] = "validation_skipped"

    pipeline_result["finished_at"] = datetime.now(timezone.utc).isoformat()
    return pipeline_result
