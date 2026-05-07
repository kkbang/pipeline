from datetime import datetime, timezone

from worker.repo.repo_snapshot_local_paths import strip_local_snapshot_fields


_RESETTABLE_PREFIXES = (
    "file_extract_",
    "chunk_",
    "validation_",
    "snapshot_local_cleanup_",
)
_MISSING_SNAPSHOT_ROOT_PREFIX = "snapshot_root_path is missing or invalid:"


def reset_downstream_processing_fields(source: dict) -> dict:
    cleaned = {}
    for key, value in strip_local_snapshot_fields(source).items():
        if key.startswith(_RESETTABLE_PREFIXES):
            continue
        cleaned[key] = value
    return cleaned


def is_missing_snapshot_root_error(exc: BaseException | None) -> bool:
    return isinstance(exc, FileNotFoundError) and _MISSING_SNAPSHOT_ROOT_PREFIX in str(exc)


def build_crawl_retry_source(
    source: dict,
    *,
    error_message: str,
    finished_at: str | None = None,
) -> dict:
    retry_source = reset_downstream_processing_fields(source)
    retry_source["crawl_status"] = "crawl_failed"
    retry_source["crawl_started_at"] = None
    retry_source["crawl_finished_at"] = finished_at or datetime.now(timezone.utc).isoformat()
    retry_source["crawl_error_message"] = error_message
    return retry_source
