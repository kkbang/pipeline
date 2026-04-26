import logging
from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.repo_pipeline_manifest_service import (
    CHUNK_READY_STAGE,
    EXTRACT_READY_STAGE,
    iter_stage_manifest_entries,
    write_stage_manifest,
)
from worker.storage.opensearch_store import OpenSearchStore


logger = logging.getLogger(__name__)

REPO_REGISTRY_INDEX = "repo_registry_index"


def _parse_non_negative_int(value: object) -> int:
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _is_whale_repo(total_code_bytes: int) -> bool:
    return total_code_bytes >= max(1, int(settings.repo_chunk_whale_repo_min_code_bytes))


def _load_extracted_entries(store: OpenSearchStore, batch_id: str) -> list[dict]:
    entries_by_repo: dict[str, dict] = {}
    for entry in iter_stage_manifest_entries(
        base_dir=store.base_dir,
        batch_id=batch_id,
        stage_name=EXTRACT_READY_STAGE,
    ) or []:
        repo_id = str(entry.get("repo_id") or "").strip()
        if not repo_id:
            continue
        if str(entry.get("stage_status") or "") != "extracted":
            continue
        entries_by_repo[repo_id] = dict(entry)
    return list(entries_by_repo.values())


def _weight_for_entry(store: OpenSearchStore, entry: dict) -> tuple[int, int]:
    total_code_bytes = _parse_non_negative_int(entry.get("total_code_bytes"))
    code_files_indexed = _parse_non_negative_int(entry.get("code_files_indexed"))
    if total_code_bytes > 0:
        return total_code_bytes, code_files_indexed

    repo_id = str(entry.get("repo_id") or "").strip()
    if not repo_id:
        return 0, code_files_indexed

    repo_doc = store.get_document(REPO_REGISTRY_INDEX, repo_id)
    source = repo_doc.get("_source", {}) if isinstance(repo_doc, dict) else {}
    total_code_bytes = _parse_non_negative_int(source.get("file_extract_total_code_bytes"))
    if code_files_indexed <= 0:
        code_files_indexed = _parse_non_negative_int(source.get("file_extract_code_files_count"))
    return total_code_bytes, code_files_indexed


def plan_chunk_shards_for_batch(
    *,
    batch_id: str | None,
    shard_count: int | None = None,
) -> dict:
    normalized_batch_id = str(batch_id or "").strip()
    resolved_shard_count = max(
        1,
        shard_count if isinstance(shard_count, int) and shard_count > 0 else settings.repo_pipeline_parallelism,
    )
    store = OpenSearchStore()
    if not normalized_batch_id:
        return {
            "stage": "chunk_plan",
            "batch_id": normalized_batch_id,
            "shard_count": resolved_shard_count,
            "planned_repo_count": 0,
            "reason": "missing_batch_id",
        }

    extracted_entries = _load_extracted_entries(store, normalized_batch_id)
    planned_at = datetime.now(timezone.utc).isoformat()

    weighted_entries: list[dict] = []
    for entry in extracted_entries:
        repo_id = str(entry.get("repo_id") or "").strip()
        if not repo_id:
            continue
        total_code_bytes, code_files_indexed = _weight_for_entry(store, entry)
        weighted_entries.append(
            {
                **entry,
                "batch_id": normalized_batch_id,
                "total_code_bytes": total_code_bytes,
                "code_files_indexed": code_files_indexed,
                "planning_weight": total_code_bytes,
                "is_whale_repo": _is_whale_repo(total_code_bytes),
                "chunk_execution_mode": (
                    "file_parallel_whale"
                    if _is_whale_repo(total_code_bytes)
                    else "repo_sequential"
                ),
                "planned_at": planned_at,
            }
        )

    weighted_entries.sort(
        key=lambda item: (
            -_parse_non_negative_int(item.get("planning_weight")),
            str(item.get("repo_id") or ""),
        )
    )

    shard_entries: dict[int, list[dict]] = {index: [] for index in range(resolved_shard_count)}
    shard_weights: list[int] = [0 for _ in range(resolved_shard_count)]

    for entry in weighted_entries:
        target_shard = min(
            range(resolved_shard_count),
            key=lambda index: (shard_weights[index], len(shard_entries[index]), index),
        )
        shard_weights[target_shard] += _parse_non_negative_int(entry.get("planning_weight"))
        shard_entries[target_shard].append(
            {
                **entry,
                "planned_shard_index": target_shard,
            }
        )

    write_stage_manifest(
        base_dir=store.base_dir,
        batch_id=normalized_batch_id,
        stage_name=CHUNK_READY_STAGE,
        entries=[entry for entries in shard_entries.values() for entry in entries],
    )
    for shard_index in range(resolved_shard_count):
        write_stage_manifest(
            base_dir=store.base_dir,
            batch_id=normalized_batch_id,
            stage_name=CHUNK_READY_STAGE,
            entries=shard_entries[shard_index],
            shard_index=shard_index,
        )

    logger.info(
        "Chunk shard planning complete: batch_id=%s repo_count=%s shard_count=%s max_shard_weight=%s min_shard_weight=%s",
        normalized_batch_id,
        len(weighted_entries),
        resolved_shard_count,
        max(shard_weights) if shard_weights else 0,
        min(shard_weights) if shard_weights else 0,
    )
    return {
        "stage": "chunk_plan",
        "batch_id": normalized_batch_id,
        "shard_count": resolved_shard_count,
        "planned_repo_count": len(weighted_entries),
        "max_shard_weight": max(shard_weights) if shard_weights else 0,
        "min_shard_weight": min(shard_weights) if shard_weights else 0,
        "total_weight": sum(shard_weights),
    }
