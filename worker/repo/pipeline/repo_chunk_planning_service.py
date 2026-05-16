import logging
from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.chunking.repo_chunk_phase import (
    CHUNK_PHASE_LIGHT,
    CHUNK_PHASE_WHALE,
    parse_non_negative_int,
    resolve_chunk_phase_for_repo,
)
from worker.repo.pipeline.repo_pipeline_manifest_service import (
    CHUNK_READY_STAGE,
    EXTRACT_READY_STAGE,
    iter_stage_manifest_entries,
    write_stage_manifest,
)
from worker.storage.opensearch_store import OpenSearchStore


logger = logging.getLogger(__name__)

REPO_REGISTRY_INDEX = "repo_registry_index"


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
    total_code_bytes = parse_non_negative_int(entry.get("total_code_bytes"))
    code_files_indexed = parse_non_negative_int(entry.get("code_files_indexed"))
    if total_code_bytes > 0:
        return total_code_bytes, code_files_indexed

    repo_id = str(entry.get("repo_id") or "").strip()
    if not repo_id:
        return 0, code_files_indexed

    repo_doc = store.get_document(REPO_REGISTRY_INDEX, repo_id)
    source = repo_doc.get("_source", {}) if isinstance(repo_doc, dict) else {}
    total_code_bytes = parse_non_negative_int(source.get("file_extract_total_code_bytes"))
    if code_files_indexed <= 0:
        code_files_indexed = parse_non_negative_int(source.get("file_extract_code_files_count"))
    return total_code_bytes, code_files_indexed


def _assign_entries_to_shards(entries: list[dict], shard_count: int) -> tuple[dict[int, list[dict]], list[int]]:
    shard_entries: dict[int, list[dict]] = {index: [] for index in range(shard_count)}
    shard_weights: list[int] = [0 for _ in range(shard_count)]

    for entry in entries:
        target_shard = min(
            range(shard_count),
            key=lambda index: (shard_weights[index], len(shard_entries[index]), index),
        )
        shard_weights[target_shard] += parse_non_negative_int(entry.get("planning_weight"))
        shard_entries[target_shard].append(
            {
                **entry,
                "planned_shard_index": target_shard,
            }
        )

    return shard_entries, shard_weights


def plan_chunk_shards_for_batch(
    *,
    batch_id: str | None,
    shard_count: int | None = None,
    whale_shard_count: int | None = None,
) -> dict:
    normalized_batch_id = str(batch_id or "").strip()
    resolved_light_shard_count = max(
        1,
        shard_count if isinstance(shard_count, int) and shard_count > 0 else settings.repo_pipeline_parallelism,
    )
    resolved_whale_shard_count = max(
        1,
        (
            whale_shard_count
            if isinstance(whale_shard_count, int) and whale_shard_count > 0
            else settings.repo_chunk_whale_phase_parallelism
        ),
    )
    store = OpenSearchStore()
    if not normalized_batch_id:
        return {
            "stage": "chunk_plan",
            "batch_id": normalized_batch_id,
            "light_shard_count": resolved_light_shard_count,
            "whale_shard_count": resolved_whale_shard_count,
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
        chunk_phase = resolve_chunk_phase_for_repo(
            total_code_bytes=total_code_bytes,
            code_file_count=code_files_indexed,
        )
        weighted_entries.append(
            {
                **entry,
                "batch_id": normalized_batch_id,
                "total_code_bytes": total_code_bytes,
                "code_files_indexed": code_files_indexed,
                "planning_weight": total_code_bytes,
                "is_whale_repo": chunk_phase == CHUNK_PHASE_WHALE,
                "chunk_phase": chunk_phase,
                "chunk_execution_mode": "file_parallel_whale" if chunk_phase == CHUNK_PHASE_WHALE else "repo_sequential",
                "planned_at": planned_at,
            }
        )

    weighted_entries.sort(
        key=lambda item: (
            -parse_non_negative_int(item.get("planning_weight")),
            str(item.get("repo_id") or ""),
        )
    )

    light_entries = [entry for entry in weighted_entries if entry.get("chunk_phase") == CHUNK_PHASE_LIGHT]
    whale_entries = [entry for entry in weighted_entries if entry.get("chunk_phase") == CHUNK_PHASE_WHALE]
    light_shard_entries, light_shard_weights = _assign_entries_to_shards(light_entries, resolved_light_shard_count)
    whale_shard_entries, whale_shard_weights = _assign_entries_to_shards(whale_entries, resolved_whale_shard_count)
    shard_entries: dict[int, list[dict]] = {}
    for shard_index in range(max(resolved_light_shard_count, resolved_whale_shard_count)):
        shard_entries[shard_index] = [
            *light_shard_entries.get(shard_index, []),
            *whale_shard_entries.get(shard_index, []),
        ]

    write_stage_manifest(
        base_dir=store.base_dir,
        batch_id=normalized_batch_id,
        stage_name=CHUNK_READY_STAGE,
        entries=[entry for entries in shard_entries.values() for entry in entries],
    )
    for shard_index in range(max(resolved_light_shard_count, resolved_whale_shard_count)):
        write_stage_manifest(
            base_dir=store.base_dir,
            batch_id=normalized_batch_id,
            stage_name=CHUNK_READY_STAGE,
            entries=shard_entries[shard_index],
            shard_index=shard_index,
        )

    logger.info(
        (
            "Chunk shard planning complete: batch_id=%s repo_count=%s "
            "light_repo_count=%s whale_repo_count=%s light_shard_count=%s whale_shard_count=%s"
        ),
        normalized_batch_id,
        len(weighted_entries),
        len(light_entries),
        len(whale_entries),
        resolved_light_shard_count,
        resolved_whale_shard_count,
    )
    return {
        "stage": "chunk_plan",
        "batch_id": normalized_batch_id,
        "light_shard_count": resolved_light_shard_count,
        "whale_shard_count": resolved_whale_shard_count,
        "planned_repo_count": len(weighted_entries),
        "light_repo_count": len(light_entries),
        "whale_repo_count": len(whale_entries),
        "light_max_shard_weight": max(light_shard_weights) if light_shard_weights else 0,
        "light_min_shard_weight": min(light_shard_weights) if light_shard_weights else 0,
        "whale_max_shard_weight": max(whale_shard_weights) if whale_shard_weights else 0,
        "whale_min_shard_weight": min(whale_shard_weights) if whale_shard_weights else 0,
        "total_weight": sum(light_shard_weights) + sum(whale_shard_weights),
    }
