import logging
import hashlib
import time
from datetime import datetime, timezone

from worker.common.config import settings
from worker.storage.opensearch_store import OpenSearchStore


REPO_REGISTRY_INDEX = "repo_registry_index"
logger = logging.getLogger(__name__)


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


def _field_terms_query(field_name: str, values: list[str]) -> dict:
    normalized_values = [str(value).strip() for value in values if str(value).strip()]
    return {
        "bool": {
            "should": [_field_term_query(field_name, value) for value in normalized_values],
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


def _repo_id_shard_index(repo_id: str, shard_count: int) -> int:
    digest = hashlib.sha1(repo_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % shard_count


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
            if _repo_id_shard_index(repo_id, shard_count) == normalized_shard_index
        ]
    if batch_limit is None:
        return unique_repo_ids
    return unique_repo_ids[:batch_limit]


def _batch_visibility_query(
    *,
    status_field: str,
    status_values: list[str],
    batch_field: str,
    batch_id: str,
) -> dict:
    return {
        "bool": {
            "must": [
                _field_terms_query(status_field, status_values),
                _field_term_query(batch_field, batch_id),
            ]
        }
    }


def _wait_for_batch_visibility(
    *,
    store: OpenSearchStore,
    stage_name: str,
    batch_id: str | None,
    status_field: str,
    status_values: list[str],
    batch_field: str,
) -> None:
    normalized_batch_id = str(batch_id or "").strip()
    if not normalized_batch_id:
        return

    timeout_seconds = max(0.0, float(settings.repo_stage_visibility_wait_seconds))
    poll_interval_seconds = max(
        0.1,
        float(settings.repo_stage_visibility_poll_interval_seconds),
    )
    if timeout_seconds <= 0:
        return

    query = _batch_visibility_query(
        status_field=status_field,
        status_values=status_values,
        batch_field=batch_field,
        batch_id=normalized_batch_id,
    )
    if store.count_documents(REPO_REGISTRY_INDEX, query=query) > 0:
        return

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        sleep_seconds = min(poll_interval_seconds, max(0.0, deadline - time.monotonic()))
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        if store.count_documents(REPO_REGISTRY_INDEX, query=query) > 0:
            logger.info(
                "Batch became visible for stage=%s batch_id=%s after polling",
                stage_name,
                normalized_batch_id,
            )
            return

    logger.info(
        "Batch visibility polling timed out for stage=%s batch_id=%s",
        stage_name,
        normalized_batch_id,
    )


def list_repo_ids_for_extraction(
    batch_size: int | None = None,
    *,
    batch_id: str | None = None,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> list[str]:
    store = OpenSearchStore()
    batch_limit = _resolve_batch_limit(batch_size)
    now = datetime.now(timezone.utc)
    _wait_for_batch_visibility(
        store=store,
        stage_name="extract",
        batch_id=batch_id,
        status_field="crawl_status",
        status_values=["downloaded"],
        batch_field="crawl_batch_id",
    )

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
    store = OpenSearchStore()
    batch_limit = _resolve_batch_limit(batch_size)
    now = datetime.now(timezone.utc)
    _wait_for_batch_visibility(
        store=store,
        stage_name="chunk",
        batch_id=batch_id,
        status_field="file_extract_status",
        status_values=["extracted"],
        batch_field="file_extract_batch_id",
    )

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
    store = OpenSearchStore()
    batch_limit = _resolve_batch_limit(batch_size)
    now = datetime.now(timezone.utc)
    _wait_for_batch_visibility(
        store=store,
        stage_name="validation",
        batch_id=batch_id,
        status_field="chunk_status",
        status_values=["chunked", "chunk_failed"],
        batch_field="chunk_batch_id",
    )

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
