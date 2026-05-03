from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.repo_pipeline_manifest_service import (
    CRAWL_DOWNLOADED_STAGE,
    EXTRACT_READY_STAGE,
    CHUNK_READY_STAGE,
    CHUNK_COMPLETED_STAGE,
    iter_stage_manifest_entries,
    repo_id_shard_index,
    resolve_pipeline_base_dir,
)
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


def _is_stale(started_at: object, now: datetime) -> bool:
    parsed_started_at = _parse_datetime(started_at)
    if parsed_started_at is None:
        return True

    elapsed_seconds = (now - parsed_started_at).total_seconds()
    return elapsed_seconds >= settings.repo_crawl_lease_seconds


def _has_newer_chunk_than_validation(source: dict) -> bool:
    chunk_finished_at = _parse_datetime(source.get("chunk_finished_at"))
    validation_checked_at = _parse_datetime(source.get("validation_checked_at"))
    if chunk_finished_at is None:
        return False
    if validation_checked_at is None:
        return True
    return chunk_finished_at > validation_checked_at


def _field_term_query(field_name: str, value: str) -> dict:
    return {
        "bool": {
            "should": [
                {"term": {f"{field_name}.keyword": value}},
                {"term": {field_name: value}},
            ],
            "minimum_should_match": 1,
        }
    }


def _matches_batch_id(source: dict, field_name: str, batch_id: str | None) -> bool:
    if not batch_id:
        return True
    return str(source.get(field_name) or "").strip() == batch_id


def _iter_repo_docs_by_field(store: OpenSearchStore, field_name: str, value: str):
    yield from store.iterate_documents_by_query(
        collection_name=REPO_REGISTRY_INDEX,
        query=_field_term_query(field_name, value),
        size=1000,
        sort=[{"_id": "asc"}],
    )


def _resolve_batch_limit(batch_size: int | None) -> int | None:
    if not isinstance(batch_size, int):
        return None
    if batch_size <= 0:
        return None
    return batch_size


def _normalized_repo_ids(
    repo_ids: list[str],
    batch_limit: int | None,
    *,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> list[str]:
    unique_repo_ids = sorted({repo_id for repo_id in repo_ids if isinstance(repo_id, str) and repo_id.strip()})
    if isinstance(shard_index, int) and isinstance(shard_count, int) and shard_count > 0:
        normalized_shard_index = shard_index % shard_count
        unique_repo_ids = [
            repo_id
            for repo_id in unique_repo_ids
            if repo_id_shard_index(repo_id, shard_count) == normalized_shard_index
        ]
    if batch_limit is None:
        return unique_repo_ids
    return unique_repo_ids[:batch_limit]


def _repo_ids_from_manifest(
    *,
    batch_id: str | None,
    stage_name: str,
    batch_limit: int | None,
    shard_index: int | None,
    shard_count: int | None,
) -> list[str]:
    repo_ids = []
    resolved_base_dir = resolve_pipeline_base_dir()
    for entry in iter_stage_manifest_entries(
        base_dir=resolved_base_dir,
        batch_id=batch_id,
        stage_name=stage_name,
        shard_index=shard_index,
    ) or []:
        repo_id = entry.get("repo_id")
        if isinstance(repo_id, str) and repo_id.strip():
            repo_ids.append(repo_id)
    # Manifest files are already stage-specific work assignments. Re-applying
    # hash partitioning here would undo planner-driven rebalancing.
    return _normalized_repo_ids(repo_ids, batch_limit)


def list_repo_ids_for_extraction(
    batch_size: int | None = None,
    *,
    batch_id: str | None = None,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> list[str]:
    batch_limit = _resolve_batch_limit(batch_size)
    if batch_id:
        return _repo_ids_from_manifest(
            batch_id=batch_id,
            stage_name=CRAWL_DOWNLOADED_STAGE,
            batch_limit=batch_limit,
            shard_index=shard_index,
            shard_count=shard_count,
        )

    store = OpenSearchStore()
    now = datetime.now(timezone.utc)

    selected_repo_ids = []
    for hit in _iter_repo_docs_by_field(store, "crawl_status", "downloaded"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if not _matches_batch_id(source, "crawl_batch_id", batch_id):
            continue
        if source.get("file_extract_status") == "extracted":
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    for hit in _iter_repo_docs_by_field(store, "file_extract_status", "extracting"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("crawl_status") != "downloaded":
            continue
        if not _matches_batch_id(source, "crawl_batch_id", batch_id):
            continue
        if not _is_stale(source.get("file_extract_started_at"), now):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    return _normalized_repo_ids(
        selected_repo_ids,
        batch_limit,
        shard_index=shard_index,
        shard_count=shard_count,
    )


def list_repo_ids_for_chunking(
    batch_size: int | None = None,
    *,
    batch_id: str | None = None,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> list[str]:
    batch_limit = _resolve_batch_limit(batch_size)
    if batch_id:
        repo_ids = _repo_ids_from_manifest(
            batch_id=batch_id,
            stage_name=CHUNK_READY_STAGE,
            batch_limit=batch_limit,
            shard_index=shard_index,
            shard_count=shard_count,
        )
        if repo_ids:
            return repo_ids
        return _repo_ids_from_manifest(
            batch_id=batch_id,
            stage_name=EXTRACT_READY_STAGE,
            batch_limit=batch_limit,
            shard_index=shard_index,
            shard_count=shard_count,
        )

    store = OpenSearchStore()
    now = datetime.now(timezone.utc)

    selected_repo_ids = []
    for hit in _iter_repo_docs_by_field(store, "file_extract_status", "extracted"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if not _matches_batch_id(source, "file_extract_batch_id", batch_id):
            continue
        if source.get("chunk_status") == "chunked":
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    for hit in _iter_repo_docs_by_field(store, "chunk_status", "chunking"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("file_extract_status") != "extracted":
            continue
        if not _matches_batch_id(source, "file_extract_batch_id", batch_id):
            continue
        if not _is_stale(source.get("chunk_started_at"), now):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    return _normalized_repo_ids(
        selected_repo_ids,
        batch_limit,
        shard_index=shard_index,
        shard_count=shard_count,
    )


def list_repo_ids_for_validation(
    batch_size: int | None = None,
    *,
    batch_id: str | None = None,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> list[str]:
    batch_limit = _resolve_batch_limit(batch_size)
    if batch_id:
        return _repo_ids_from_manifest(
            batch_id=batch_id,
            stage_name=CHUNK_COMPLETED_STAGE,
            batch_limit=batch_limit,
            shard_index=shard_index,
            shard_count=shard_count,
        )

    store = OpenSearchStore()
    now = datetime.now(timezone.utc)

    selected_repo_ids = []
    for hit in _iter_repo_docs_by_field(store, "chunk_status", "chunked"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if not _matches_batch_id(source, "chunk_batch_id", batch_id):
            continue
        if source.get("validation_status") == "validated" and not _has_newer_chunk_than_validation(source):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    for hit in _iter_repo_docs_by_field(store, "chunk_status", "chunk_failed"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if not _matches_batch_id(source, "chunk_batch_id", batch_id):
            continue
        if source.get("validation_status") == "validated" and not _has_newer_chunk_than_validation(source):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    for hit in _iter_repo_docs_by_field(store, "validation_status", "validating"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("chunk_status") not in {"chunked", "chunk_failed"}:
            continue
        if not _matches_batch_id(source, "chunk_batch_id", batch_id):
            continue
        if not _is_stale(source.get("validation_started_at"), now):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    return _normalized_repo_ids(
        selected_repo_ids,
        batch_limit,
        shard_index=shard_index,
        shard_count=shard_count,
    )
