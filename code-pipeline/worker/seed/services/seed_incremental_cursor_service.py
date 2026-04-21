from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from worker.seed.services.seed_item_writer import safe_path_fragment, stable_digest
from worker.storage.opensearch_store import OpenSearchStore


CURSOR_COLLECTION = "seed_source_cursor_index"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value or not isinstance(raw_value, str):
        return None
    value = raw_value.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_date(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).date().isoformat()


def build_cursor_doc_id(source_type: str, source_name: str, source_key: str) -> str:
    normalized = f"{source_type}:{source_name}:{source_key}".strip()
    return f"cursor:{safe_path_fragment(source_type)}:{safe_path_fragment(source_name)}:{stable_digest(normalized)}"


def load_cursor(
    store: OpenSearchStore,
    *,
    source_type: str,
    source_name: str,
    source_key: str,
) -> dict:
    doc_id = build_cursor_doc_id(source_type, source_name, source_key)
    hit = store.get_document(CURSOR_COLLECTION, doc_id)
    return hit.get("_source", {}) if hit else {}


def should_skip_by_min_interval(
    cursor: dict,
    *,
    min_interval_minutes: int,
    now: datetime | None = None,
) -> bool:
    if min_interval_minutes <= 0:
        return False
    now = now or _utc_now()
    last_success_at = _parse_iso_datetime(cursor.get("last_success_at"))
    if last_success_at is None:
        return False
    elapsed = now - last_success_at
    return elapsed < timedelta(minutes=max(0, min_interval_minutes))


def build_incremental_pushed_filter_date(
    cursor: dict,
    *,
    overlap_days: int,
) -> str | None:
    candidates = [
        _parse_iso_datetime(cursor.get("last_max_pushed_at")),
        _parse_iso_datetime(cursor.get("last_max_updated_at")),
        _parse_iso_datetime(cursor.get("last_success_at")),
    ]
    anchor = next((candidate for candidate in candidates if candidate is not None), None)
    if anchor is None:
        return None
    adjusted = anchor - timedelta(days=max(0, overlap_days))
    return _format_date(adjusted)


def extract_max_timestamp(values: Iterable[str | None]) -> str | None:
    max_dt: datetime | None = None
    for raw_value in values:
        parsed = _parse_iso_datetime(raw_value)
        if parsed is None:
            continue
        if max_dt is None or parsed > max_dt:
            max_dt = parsed
    return max_dt.isoformat() if max_dt is not None else None


@dataclass(slots=True)
class CursorUpdatePayload:
    source_type: str
    source_name: str
    source_key: str
    request_label: str
    request_query: str
    emitted_count: int
    status: str
    last_max_pushed_at: str | None = None
    last_max_updated_at: str | None = None
    error_message: str | None = None


def upsert_cursor(store: OpenSearchStore, payload: CursorUpdatePayload) -> None:
    doc_id = build_cursor_doc_id(
        payload.source_type,
        payload.source_name,
        payload.source_key,
    )
    now_iso = _utc_now().isoformat()
    body = {
        "source_type": payload.source_type,
        "source_name": payload.source_name,
        "source_key": payload.source_key,
        "request_label": payload.request_label,
        "request_query": payload.request_query,
        "last_emitted_count": payload.emitted_count,
        "last_status": payload.status,
        "last_attempt_at": now_iso,
        "last_error_message": payload.error_message,
    }
    if payload.status == "success":
        body.update(
            {
                "last_success_at": now_iso,
                "last_max_pushed_at": payload.last_max_pushed_at,
                "last_max_updated_at": payload.last_max_updated_at,
            }
        )

    store.upsert_document(
        collection_name=CURSOR_COLLECTION,
        doc_id=doc_id,
        body=body,
    )
