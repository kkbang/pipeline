from __future__ import annotations

from typing import Any

from scripts.backfill_code_chunk_embeddings import (
    CODE_CHUNK_INDEX,
    CODE_CHUNK_EMBEDDING_INDEX,
    EMBEDDING_BACKFILL_ALLOWED_LANGUAGES,
    EMBEDDING_BACKFILL_SCAN_SIZE,
    EMBEDDING_BACKFILL_SOURCE_FIELDS,
    EMBEDDING_BACKFILL_WRITE_BATCH_SIZE,
    EMBEDDING_BACKFILL_WRITE_BUFFER_SIZE,
    OpenSearchHttpStore,
    backfill_code_chunk_embeddings,
    _build_embedding_backfill_query,
    _filter_missing_embedding_docs,
    _is_eligible_source_chunk,
)
from worker.common.config import settings


_NULL_STRINGS = {"", "none", "null"}
_TRUE_STRINGS = {"1", "true", "yes", "y", "on"}
_FALSE_STRINGS = {"0", "false", "no", "n", "off"}


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _NULL_STRINGS:
        return None
    return text


def _normalize_optional_int(value: Any) -> int | None:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return None
    return int(normalized)


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return False
    lowered = normalized.lower()
    if lowered in _TRUE_STRINGS:
        return True
    if lowered in _FALSE_STRINGS:
        return False
    return bool(value)


def is_code_chunk_embedding_backfill_enabled() -> bool:
    return bool(
        settings.code_chunk_embedding_enabled
        and settings.code_chunk_embedding_endpoint
        and settings.code_chunk_embedding_model
    )


def run_code_chunk_embedding_backfill(
    *,
    repo_id: str | None = None,
    limit: int | None = None,
    scan_size: int | None = None,
    write_batch_size: int | None = None,
    write_buffer_size: int | None = None,
    force_reembed: bool = False,
    refresh_writes: bool = False,
    skip_writes: bool = False,
) -> dict[str, Any]:
    normalized_repo_id = _normalize_optional_text(repo_id)
    normalized_limit = _normalize_optional_int(limit)
    normalized_scan_size = _normalize_optional_int(scan_size)
    normalized_write_batch_size = _normalize_optional_int(write_batch_size)
    normalized_write_buffer_size = _normalize_optional_int(write_buffer_size)
    normalized_force_reembed = _normalize_bool(force_reembed)
    normalized_refresh_writes = _normalize_bool(refresh_writes)
    normalized_skip_writes = _normalize_bool(skip_writes)

    stats = backfill_code_chunk_embeddings(
        repo_id=normalized_repo_id,
        scan_size=max(
            1,
            int(
                normalized_scan_size
                or settings.code_chunk_embedding_backfill_scan_size
                or EMBEDDING_BACKFILL_SCAN_SIZE
            ),
        ),
        write_batch_size=max(
            1,
            int(
                normalized_write_batch_size
                or settings.code_chunk_embedding_backfill_write_batch_size
                or EMBEDDING_BACKFILL_WRITE_BATCH_SIZE
            ),
        ),
        write_buffer_size=max(
            1,
            int(
                normalized_write_buffer_size
                or settings.code_chunk_embedding_backfill_write_buffer_size
                or EMBEDDING_BACKFILL_WRITE_BUFFER_SIZE
            ),
        ),
        limit=max(
            0,
            int(
                normalized_limit
                if normalized_limit is not None
                else settings.code_chunk_embedding_backfill_run_limit
            ),
        ),
        force_reembed=normalized_force_reembed,
        refresh_writes=normalized_refresh_writes,
        skip_writes=normalized_skip_writes,
    )
    return {
        "repo_id": normalized_repo_id,
        "source_index": CODE_CHUNK_INDEX,
        "embedding_index": CODE_CHUNK_EMBEDDING_INDEX,
        "languages": list(EMBEDDING_BACKFILL_ALLOWED_LANGUAGES),
        "force_reembed": normalized_force_reembed,
        "refresh_writes": normalized_refresh_writes,
        "skip_writes": normalized_skip_writes,
        "run_limit": max(
            0,
            int(
                normalized_limit
                if normalized_limit is not None
                else settings.code_chunk_embedding_backfill_run_limit
            ),
        ),
        **stats.as_dict(),
    }


def has_pending_code_chunk_embedding_backfill_work(
    *,
    repo_id: str | None = None,
    force_reembed: bool = False,
    skip_writes: bool = False,
    store: Any | None = None,
) -> bool:
    normalized_repo_id = _normalize_optional_text(repo_id)
    normalized_force_reembed = _normalize_bool(force_reembed)
    normalized_skip_writes = _normalize_bool(skip_writes)

    if normalized_skip_writes:
        return False
    if normalized_force_reembed:
        return False

    resolved_store = store or OpenSearchHttpStore()
    scan_size = max(
        1,
        int(settings.code_chunk_embedding_backfill_scan_size or EMBEDDING_BACKFILL_SCAN_SIZE),
    )

    for language in EMBEDDING_BACKFILL_ALLOWED_LANGUAGES:
        query = _build_embedding_backfill_query(
            repo_id=normalized_repo_id,
            force_reembed=False,
            language=language,
        )
        candidate_batch: list[tuple[str, dict[str, Any]]] = []
        for hit in resolved_store.iterate_documents_by_query(
            collection_name=CODE_CHUNK_INDEX,
            query=query,
            size=scan_size,
            source_includes=EMBEDDING_BACKFILL_SOURCE_FIELDS,
        ):
            doc_id = str(hit.get("_id") or "").strip()
            if not doc_id:
                continue
            source = dict(hit.get("_source", {}))
            if not _is_eligible_source_chunk(source):
                continue
            candidate_batch.append((doc_id, source))
            if len(candidate_batch) < scan_size:
                continue
            if _filter_missing_embedding_docs(resolved_store, candidate_batch):
                return True
            candidate_batch.clear()

        if candidate_batch and _filter_missing_embedding_docs(resolved_store, candidate_batch):
            return True

    return False
