from datetime import datetime, timezone

from worker.common.config import settings
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


def _resolve_batch_size(batch_size: int | None) -> int:
    if isinstance(batch_size, int) and batch_size > 0:
        return batch_size
    return max(1, settings.repo_pipeline_batch_size)


def _normalized_repo_ids(repo_ids: list[str], batch_size: int) -> list[str]:
    unique_repo_ids = sorted({repo_id for repo_id in repo_ids if isinstance(repo_id, str) and repo_id.strip()})
    return unique_repo_ids[:batch_size]


def list_repo_ids_for_extraction(batch_size: int | None = None) -> list[str]:
    store = OpenSearchStore()
    safe_batch_size = _resolve_batch_size(batch_size)
    now = datetime.now(timezone.utc)

    downloaded_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="crawl_status",
        value="downloaded",
        size=safe_batch_size,
    )
    extracting_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="file_extract_status",
        value="extracting",
        size=safe_batch_size,
    )

    selected_repo_ids = []
    for hit in downloaded_docs:
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("file_extract_status") == "extracted":
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    for hit in extracting_docs:
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("crawl_status") != "downloaded":
            continue
        if not _is_stale(source.get("file_extract_started_at"), now):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    return _normalized_repo_ids(selected_repo_ids, safe_batch_size)


def list_repo_ids_for_chunking(batch_size: int | None = None) -> list[str]:
    store = OpenSearchStore()
    safe_batch_size = _resolve_batch_size(batch_size)
    now = datetime.now(timezone.utc)

    extracted_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="file_extract_status",
        value="extracted",
        size=safe_batch_size,
    )
    chunking_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="chunk_status",
        value="chunking",
        size=safe_batch_size,
    )

    selected_repo_ids = []
    for hit in extracted_docs:
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("chunk_status") == "chunked":
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    for hit in chunking_docs:
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("file_extract_status") != "extracted":
            continue
        if not _is_stale(source.get("chunk_started_at"), now):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    return _normalized_repo_ids(selected_repo_ids, safe_batch_size)


def list_repo_ids_for_validation(batch_size: int | None = None) -> list[str]:
    store = OpenSearchStore()
    safe_batch_size = _resolve_batch_size(batch_size)
    now = datetime.now(timezone.utc)

    chunked_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="chunk_status",
        value="chunked",
        size=safe_batch_size,
    )
    chunk_failed_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="chunk_status",
        value="chunk_failed",
        size=safe_batch_size,
    )
    validating_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="validation_status",
        value="validating",
        size=safe_batch_size,
    )

    selected_repo_ids = []
    for hit in [*chunked_docs, *chunk_failed_docs]:
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("validation_status") == "validated" and not _has_newer_chunk_than_validation(source):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    for hit in validating_docs:
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if source.get("chunk_status") not in {"chunked", "chunk_failed"}:
            continue
        if not _is_stale(source.get("validation_started_at"), now):
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    return _normalized_repo_ids(selected_repo_ids, safe_batch_size)
