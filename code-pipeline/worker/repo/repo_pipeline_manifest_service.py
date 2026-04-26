import json
import os
import re
from pathlib import Path


REPO_PIPELINE_BATCH_DIR = "repo_pipeline_batches"
CRAWL_DOWNLOADED_STAGE = "crawl_downloaded"
EXTRACT_READY_STAGE = "extract_ready"
CHUNK_READY_STAGE = "chunk_ready"
CHUNK_COMPLETED_STAGE = "chunk_completed"
VALIDATION_COMPLETED_STAGE = "validation_completed"


def resolve_pipeline_base_dir(base_dir: str | Path | None = None) -> Path:
    if base_dir is not None:
        return Path(base_dir)

    configured_dir = os.getenv("LOCAL_DATA_DIR", "").strip()
    if configured_dir:
        return Path(configured_dir)

    return Path(__file__).resolve().parents[2] / "local_data"


def _safe_batch_fragment(batch_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", str(batch_id or "").strip())
    return normalized.strip("_") or "unknown_batch"


def _stage_dir(base_dir: Path, batch_id: str, stage_name: str) -> Path:
    return base_dir / REPO_PIPELINE_BATCH_DIR / _safe_batch_fragment(batch_id) / stage_name


def _stage_file_name(shard_index: int | None) -> str:
    if isinstance(shard_index, int) and shard_index >= 0:
        return f"shard_{shard_index:03d}.jsonl"
    return "all.jsonl"


def repo_id_shard_index(repo_id: str, shard_count: int) -> int:
    import hashlib

    digest = hashlib.sha1(repo_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % shard_count


def write_stage_manifest(
    *,
    base_dir: Path,
    batch_id: str | None,
    stage_name: str,
    entries: list[dict],
    shard_index: int | None = None,
) -> str | None:
    normalized_batch_id = str(batch_id or "").strip()
    if not normalized_batch_id:
        return None

    target_dir = _stage_dir(base_dir, normalized_batch_id, stage_name)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / _stage_file_name(shard_index)
    temp_path = target_path.with_suffix(target_path.suffix + ".tmp")

    with temp_path.open("w", encoding="utf-8") as output:
        for entry in entries:
            output.write(json.dumps(entry, ensure_ascii=True, sort_keys=True))
            output.write("\n")

    temp_path.replace(target_path)
    return str(target_path)


def iter_stage_manifest_entries(
    *,
    base_dir: Path | None = None,
    batch_id: str | None,
    stage_name: str,
    shard_index: int | None = None,
):
    normalized_batch_id = str(batch_id or "").strip()
    if not normalized_batch_id:
        return

    resolved_base_dir = resolve_pipeline_base_dir(base_dir)
    target_dir = _stage_dir(resolved_base_dir, normalized_batch_id, stage_name)
    if not target_dir.exists() or not target_dir.is_dir():
        return

    if isinstance(shard_index, int) and shard_index >= 0:
        candidate_path = target_dir / _stage_file_name(shard_index)
        if candidate_path.exists() and candidate_path.is_file():
            manifest_paths = [candidate_path]
        else:
            fallback_path = target_dir / _stage_file_name(None)
            manifest_paths = [fallback_path] if fallback_path.exists() and fallback_path.is_file() else []
    else:
        manifest_paths = sorted(target_dir.glob("*.jsonl"))

    for manifest_path in manifest_paths:
        with manifest_path.open("r", encoding="utf-8") as input_file:
            for line in input_file:
                payload = line.strip()
                if not payload:
                    continue
                yield json.loads(payload)
