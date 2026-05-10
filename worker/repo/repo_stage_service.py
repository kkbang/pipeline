from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.repo_chunk_phase import (
    CHUNK_PHASE_LIGHT,
    CHUNK_PHASE_WHALE,
    is_whale_repo,
    normalize_chunk_phase,
    parse_non_negative_int,
)
from worker.repo.repo_pipeline_manifest_service import (
    CRAWL_DOWNLOADED_STAGE,
    EXTRACT_READY_STAGE,
    CHUNK_READY_STAGE,
    CHUNK_COMPLETED_STAGE,
    iter_stage_manifest_entries,
    repo_id_shard_index,
    resolve_pipeline_base_dir,
)
from worker.repo.repo_retry_state import is_repo_processing_retry_managed
from worker.repo.repo_snapshot_cleanup import needs_snapshot_cleanup_retry
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


def _chunk_phase_from_manifest_entry(entry: dict) -> str:
    if not isinstance(entry, dict):
        return CHUNK_PHASE_LIGHT

    normalized_phase = normalize_chunk_phase(entry.get("chunk_phase"), default="")
    if normalized_phase:
        return normalized_phase
    if bool(entry.get("is_whale_repo")):
        return CHUNK_PHASE_WHALE
    return CHUNK_PHASE_LIGHT


def _chunk_phase_for_repo_source(source: dict) -> str:
    total_code_bytes = parse_non_negative_int(
        source.get("file_extract_total_code_bytes") or source.get("chunk_repo_total_code_bytes")
    )
    code_file_count = parse_non_negative_int(
        source.get("file_extract_code_files_count") or source.get("chunk_code_files_count")
    )
    if is_whale_repo(total_code_bytes=total_code_bytes, code_file_count=code_file_count):
        return CHUNK_PHASE_WHALE
    return CHUNK_PHASE_LIGHT


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
    phase: str | None = None,
) -> list[str]:
    repo_ids = []
    normalized_phase = normalize_chunk_phase(phase, default="") if phase is not None else ""
    resolved_base_dir = resolve_pipeline_base_dir()
    for entry in iter_stage_manifest_entries(
        base_dir=resolved_base_dir,
        batch_id=batch_id,
        stage_name=stage_name,
        shard_index=shard_index,
    ) or []:
        if normalized_phase and stage_name == CHUNK_READY_STAGE:
            if _chunk_phase_from_manifest_entry(entry) != normalized_phase:
                continue
        elif normalized_phase == CHUNK_PHASE_WHALE and stage_name == EXTRACT_READY_STAGE:
            continue
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
    selected_repo_ids = []
    for hit in _iter_repo_docs_by_field(store, "crawl_status", "downloaded"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if not _matches_batch_id(source, "crawl_batch_id", batch_id):
            continue
        if source.get("file_extract_status") in {"extracted", "extracting", "extract_failed"}:
            continue
        if is_repo_processing_retry_managed(source):
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
    phase: str | None = None,
) -> list[str]:
    batch_limit = _resolve_batch_limit(batch_size)
    normalized_phase = normalize_chunk_phase(phase) if phase is not None else ""
    if batch_id:
        repo_ids = _repo_ids_from_manifest(
            batch_id=batch_id,
            stage_name=CHUNK_READY_STAGE,
            batch_limit=batch_limit,
            shard_index=shard_index,
            shard_count=shard_count,
            phase=normalized_phase,
        )
        if not repo_ids:
            repo_ids = _repo_ids_from_manifest(
                batch_id=batch_id,
                stage_name=EXTRACT_READY_STAGE,
                batch_limit=batch_limit,
                shard_index=shard_index,
                shard_count=shard_count,
                phase=normalized_phase,
            )

        store = OpenSearchStore()
        normalized_shard_index = None
        if isinstance(shard_index, int) and isinstance(shard_count, int) and shard_count > 0:
            normalized_shard_index = shard_index % shard_count
        for chunk_status in ("chunked", "chunk_failed"):
            for hit in _iter_repo_docs_by_field(store, "chunk_status", chunk_status):
                doc_id = hit.get("_id")
                source = hit.get("_source", {})
                if not (
                    _matches_batch_id(source, "chunk_batch_id", batch_id)
                    or _matches_batch_id(source, "file_extract_batch_id", batch_id)
                    or _matches_batch_id(source, "crawl_batch_id", batch_id)
                ):
                    continue
                if not needs_snapshot_cleanup_retry(source):
                    continue
                if (
                    normalized_shard_index is not None
                    and isinstance(doc_id, str)
                    and doc_id.strip()
                    and repo_id_shard_index(doc_id, shard_count) != normalized_shard_index
                ):
                    continue
                if isinstance(doc_id, str) and doc_id.strip():
                    repo_ids.append(doc_id)

        # Chunk manifests are already shard-specific assignments. Re-applying
        # hash partitioning here drops repos that were planner-assigned to this
        # shard but whose repo_id hashes elsewhere.
        return _normalized_repo_ids(repo_ids, batch_limit)

    store = OpenSearchStore()
    now = datetime.now(timezone.utc)

    selected_repo_ids = []
    for hit in _iter_repo_docs_by_field(store, "file_extract_status", "extracted"):
        doc_id = hit.get("_id")
        source = hit.get("_source", {})
        if not _matches_batch_id(source, "file_extract_batch_id", batch_id):
            continue
        if source.get("chunk_status") in {"chunked", "chunk_failed"}:
            if needs_snapshot_cleanup_retry(source):
                if isinstance(doc_id, str) and doc_id.strip():
                    selected_repo_ids.append(doc_id)
            continue
        if normalized_phase and _chunk_phase_for_repo_source(source) != normalized_phase:
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
        if normalized_phase and _chunk_phase_for_repo_source(source) != normalized_phase:
            continue
        if isinstance(doc_id, str) and doc_id.strip():
            selected_repo_ids.append(doc_id)

    return _normalized_repo_ids(
        selected_repo_ids,
        batch_limit,
        shard_index=shard_index,
        shard_count=shard_count,
    )


def has_pending_chunk_work(
    *,
    batch_id: str | None = None,
    phase: str = CHUNK_PHASE_LIGHT,
    require_light_phase_cleared: bool = False,
) -> bool:
    normalized_phase = normalize_chunk_phase(phase)
    if batch_id:
        return bool(
            _repo_ids_from_manifest(
                batch_id=batch_id,
                stage_name=CHUNK_READY_STAGE,
                batch_limit=1,
                shard_index=None,
                shard_count=None,
                phase=normalized_phase,
            )
        )

    if require_light_phase_cleared and normalized_phase == CHUNK_PHASE_WHALE:
        if list_repo_ids_for_chunking(batch_size=1, batch_id=None, phase=CHUNK_PHASE_LIGHT):
            return False

    return bool(
        list_repo_ids_for_chunking(
            batch_size=1,
            batch_id=None,
            phase=normalized_phase,
        )
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
