import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from worker.common.config import settings
from worker.repo.repo_stage_service import list_repo_ids_for_chunking
from worker.storage.opensearch_store import OpenSearchStore


logger = logging.getLogger(__name__)

REPO_REGISTRY_INDEX = "repo_registry_index"
REPO_FILE_INDEX = "repo_file_index"
REPO_CHUNK_INDEX = "repo_chunk_index"


@dataclass(slots=True)
class RepoChunkStats:
    code_files_seen: int = 0
    chunk_docs_created: int = 0
    total_lines_seen: int = 0
    chunked_lines_total: int = 0
    skipped_missing_files: int = 0
    skipped_non_utf8_files: int = 0
    deleted_previous_docs: int = 0


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


def _is_stale_chunking(source: dict, now: datetime) -> bool:
    if source.get("chunk_status") != "chunking":
        return False

    started_at = _parse_datetime(source.get("chunk_started_at"))
    if started_at is None:
        return True

    elapsed_seconds = (now - started_at).total_seconds()
    return elapsed_seconds >= settings.repo_crawl_lease_seconds


def _load_repo_docs_for_chunking(store: OpenSearchStore, now: datetime) -> list[dict]:
    extracted_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="file_extract_status",
        value="extracted",
        size=max(1, settings.repo_chunk_repo_limit),
    )
    chunking_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="chunk_status",
        value="chunking",
        size=max(1, settings.repo_chunk_repo_limit),
    )

    repo_docs_by_id = {}
    for hit in extracted_docs:
        source = hit.get("_source", {})
        if source.get("chunk_status") == "chunked":
            continue
        repo_docs_by_id[hit["_id"]] = hit

    stale_docs = []
    for hit in chunking_docs:
        source = hit.get("_source", {})
        if source.get("file_extract_status") != "extracted":
            continue
        if _is_stale_chunking(source, now):
            repo_docs_by_id[hit["_id"]] = hit
            stale_docs.append(hit)

    if stale_docs:
        logger.warning(
            "Reclaiming stale repo chunking leases: count=%s lease_seconds=%s",
            len(stale_docs),
            settings.repo_crawl_lease_seconds,
        )

    return list(repo_docs_by_id.values())


def _claim_repo_docs_for_chunking(
    store: OpenSearchStore,
    repo_docs: list[dict],
    started_at: str,
) -> dict[str, dict]:
    claimed_docs = {}
    for hit in repo_docs:
        doc_id = hit["_id"]
        source = dict(hit.get("_source", {}))
        attempt_count = int(source.get("chunk_attempt_count") or 0) + 1
        claimed_source = {
            **source,
            "chunk_status": "chunking",
            "chunk_started_at": started_at,
            "chunk_finished_at": None,
            "chunk_error_message": None,
            "chunk_attempt_count": attempt_count,
            "chunk_total_count": None,
            "chunk_code_files_count": None,
            "chunk_total_lines_seen": None,
            "chunked_lines_total": None,
            "chunk_skipped_missing_files": None,
            "chunk_skipped_non_utf8_files": None,
            "chunk_deleted_previous_docs": None,
            "chunk_max_lines": None,
            "chunk_overlap_lines": None,
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=claimed_source,
        )
        claimed_docs[doc_id] = {"_id": doc_id, "_source": claimed_source}
    return claimed_docs


def _chunk_parameters() -> tuple[int, int]:
    max_lines = max(1, int(settings.repo_chunk_max_lines))
    overlap_lines = max(0, int(settings.repo_chunk_overlap_lines))
    if overlap_lines >= max_lines:
        overlap_lines = max(0, max_lines - 1)
    return max_lines, overlap_lines


def _build_chunk_doc_id(
    repo_id: str,
    file_path: str,
    line_start: int,
    line_end: int,
    chunk_index: int,
) -> str:
    digest_input = f"{repo_id}:{file_path}:{line_start}:{line_end}:{chunk_index}"
    digest = hashlib.sha1(digest_input.encode("utf-8")).hexdigest()
    return f"{repo_id}:chunk:{digest}"


def _iterate_line_chunks(lines: list[str], max_lines: int, overlap_lines: int):
    if not lines:
        return

    start_index = 0
    total_lines = len(lines)
    while start_index < total_lines:
        end_index = min(start_index + max_lines, total_lines)
        chunk_lines = lines[start_index:end_index]
        if chunk_lines:
            yield (
                start_index + 1,
                end_index,
                "\n".join(chunk_lines),
            )

        if end_index >= total_lines:
            return

        if overlap_lines <= 0:
            start_index = end_index
        else:
            start_index = max(0, end_index - overlap_lines)


def _iter_code_file_docs(store: OpenSearchStore, repo_id: str):
    query = {
        "bool": {
            "must": [
                {
                    "bool": {
                        "should": [
                            {"term": {"repo_id.keyword": repo_id}},
                            {"term": {"repo_id": repo_id}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                {"term": {"is_code_file": True}},
            ]
        }
    }
    yield from store.iterate_documents_by_query(
        collection_name=REPO_FILE_INDEX,
        query=query,
        size=1000,
        sort=[{"_id": "asc"}],
    )


def _chunk_single_repo(
    store: OpenSearchStore,
    *,
    repo_id: str,
    source: dict,
    chunked_at: str,
) -> RepoChunkStats:
    snapshot_root = Path(str(source.get("snapshot_root_path") or "")).resolve()
    if not snapshot_root.exists() or not snapshot_root.is_dir():
        raise FileNotFoundError(f"snapshot_root_path is missing or invalid: {snapshot_root}")

    max_lines, overlap_lines = _chunk_parameters()
    deleted_docs = store.delete_documents_by_field(
        collection_name=REPO_CHUNK_INDEX,
        field_name="repo_id",
        value=repo_id,
        refresh=True,
    )

    owner = str(source.get("owner") or "").strip().lower()
    repo_name = str(source.get("repo_name") or "").strip().lower()
    canonical_repo_url = source.get("canonical_repo_url")
    snapshot_ref = source.get("snapshot_ref")
    bulk_flush_docs = max(1, int(settings.opensearch_bulk_flush_docs))

    stats = RepoChunkStats(deleted_previous_docs=deleted_docs)
    pending_docs: list[tuple[str, dict]] = []
    for file_hit in _iter_code_file_docs(store, repo_id):
        file_source = file_hit.get("_source", {})
        relative_path = str(file_source.get("file_path") or "").strip()
        if not relative_path:
            continue

        stats.code_files_seen += 1
        absolute_path = snapshot_root / relative_path
        if not absolute_path.exists() or not absolute_path.is_file():
            stats.skipped_missing_files += 1
            continue

        try:
            content = absolute_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            stats.skipped_non_utf8_files += 1
            continue

        lines = content.splitlines()
        stats.total_lines_seen += len(lines)
        language = file_source.get("language")
        chunk_index = 0
        for line_start, line_end, chunk_text in _iterate_line_chunks(
            lines,
            max_lines=max_lines,
            overlap_lines=overlap_lines,
        ):
            if not chunk_text.strip():
                continue

            chunk_index += 1
            line_count = line_end - line_start + 1
            stats.chunk_docs_created += 1
            stats.chunked_lines_total += line_count

            pending_docs.append(
                (
                    _build_chunk_doc_id(
                        repo_id=repo_id,
                        file_path=relative_path,
                        line_start=line_start,
                        line_end=line_end,
                        chunk_index=chunk_index,
                    ),
                    {
                        "repo_id": repo_id,
                        "owner": owner,
                        "repo_name": repo_name,
                        "canonical_repo_url": canonical_repo_url,
                        "snapshot_ref": snapshot_ref,
                        "file_path": relative_path,
                        "language": language,
                        "chunk_index": chunk_index,
                        "line_start": line_start,
                        "line_end": line_end,
                        "line_count": line_count,
                        "chunk_text": chunk_text,
                        "chunk_char_count": len(chunk_text),
                        "chunk_sha1": hashlib.sha1(chunk_text.encode("utf-8")).hexdigest(),
                        "chunked_at": chunked_at,
                    },
                )
            )
            if len(pending_docs) >= bulk_flush_docs:
                store.bulk_upsert_documents(
                    collection_name=REPO_CHUNK_INDEX,
                    documents=pending_docs,
                    refresh=False,
                    chunk_size=bulk_flush_docs,
                )
                pending_docs.clear()

    if pending_docs:
        store.bulk_upsert_documents(
            collection_name=REPO_CHUNK_INDEX,
            documents=pending_docs,
            refresh=False,
            chunk_size=bulk_flush_docs,
        )

    store.refresh_index(REPO_CHUNK_INDEX)
    return stats


def _load_repo_doc_for_chunking(store: OpenSearchStore, repo_id: str) -> dict | None:
    repo_doc = store.get_document(collection_name=REPO_REGISTRY_INDEX, doc_id=repo_id)
    if not repo_doc:
        return None
    if not repo_doc.get("_source"):
        return None
    return repo_doc


def run_repo_code_chunking_for_repo(
    repo_id: str,
    *,
    store: OpenSearchStore | None = None,
) -> dict:
    if store is None:
        store = OpenSearchStore()

    chunk_started_at = datetime.now(timezone.utc).isoformat()
    max_lines, overlap_lines = _chunk_parameters()
    try:
        repo_doc = _load_repo_doc_for_chunking(store, repo_id)
        if repo_doc is None:
            return {
                "repo_id": repo_id,
                "stage": "chunk",
                "stage_status": "skipped",
                "reason": "repo_not_found",
            }

        source = dict(repo_doc.get("_source", {}))
        if source.get("file_extract_status") != "extracted":
            return {
                "repo_id": repo_id,
                "stage": "chunk",
                "stage_status": "skipped",
                "reason": "file_extract_not_extracted",
            }

        if source.get("chunk_status") == "chunked":
            return {
                "repo_id": repo_id,
                "stage": "chunk",
                "stage_status": "chunked",
                "reason": "already_chunked",
            }

        if source.get("chunk_status") == "chunking":
            if _is_stale_chunking(source, datetime.now(timezone.utc)):
                logger.warning("Reclaiming stale chunking lease for repo_id=%s", repo_id)
            else:
                return {
                    "repo_id": repo_id,
                    "stage": "chunk",
                    "stage_status": "skipped",
                    "reason": "already_chunking",
                }

        claimed_docs = _claim_repo_docs_for_chunking(
            store=store,
            repo_docs=[repo_doc],
            started_at=chunk_started_at,
        )
        claimed_source = dict(claimed_docs[repo_id]["_source"])
        chunk_finished_at = datetime.now(timezone.utc).isoformat()
        stats = _chunk_single_repo(
            store,
            repo_id=repo_id,
            source=claimed_source,
            chunked_at=chunk_finished_at,
        )

        chunk_error_message = None
        chunk_status = "chunked"
        if stats.chunk_docs_created == 0:
            chunk_status = "chunk_failed"
            chunk_error_message = "no_chunks_created"

        updated_source = {
            **claimed_source,
            "chunk_status": chunk_status,
            "chunk_finished_at": chunk_finished_at,
            "chunk_error_message": chunk_error_message,
            "chunk_total_count": stats.chunk_docs_created,
            "chunk_code_files_count": stats.code_files_seen,
            "chunk_total_lines_seen": stats.total_lines_seen,
            "chunked_lines_total": stats.chunked_lines_total,
            "chunk_skipped_missing_files": stats.skipped_missing_files,
            "chunk_skipped_non_utf8_files": stats.skipped_non_utf8_files,
            "chunk_deleted_previous_docs": stats.deleted_previous_docs,
            "chunk_max_lines": max_lines,
            "chunk_overlap_lines": overlap_lines,
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=repo_id,
            source=updated_source,
        )

        return {
            "repo_id": repo_id,
            "stage": "chunk",
            "stage_status": chunk_status,
            "chunk_total_count": stats.chunk_docs_created,
            "code_files_seen": stats.code_files_seen,
            "error_message": chunk_error_message,
        }
    except Exception as exc:  # noqa: BLE001 - repo 단위 파이프라인 실패를 상위로 전달하기 위함
        logger.warning("Code chunking failed for repo_id=%s error=%s", repo_id, str(exc))
        repo_doc = _load_repo_doc_for_chunking(store, repo_id) or {"_source": {}}
        failed_source = {
            **dict(repo_doc.get("_source", {})),
            "chunk_status": "chunk_failed",
            "chunk_finished_at": datetime.now(timezone.utc).isoformat(),
            "chunk_error_message": str(exc),
            "chunk_max_lines": max_lines,
            "chunk_overlap_lines": overlap_lines,
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=repo_id,
            source=failed_source,
        )
        return {
            "repo_id": repo_id,
            "stage": "chunk",
            "stage_status": "chunk_failed",
            "error_message": str(exc),
        }


def run_repo_code_chunking_for_shard(
    shard_index: int,
    *,
    shard_count: int | None = None,
    batch_size: int | None = None,
) -> dict:
    resolved_shard_count = (
        shard_count
        if isinstance(shard_count, int) and shard_count > 0
        else max(1, settings.repo_pipeline_parallelism)
    )
    store = OpenSearchStore()
    repo_ids = list_repo_ids_for_chunking(
        batch_size=batch_size,
        shard_index=shard_index,
        shard_count=resolved_shard_count,
    )

    processed_count = 0
    chunked_count = 0
    failed_count = 0
    skipped_count = 0
    for repo_id in repo_ids:
        processed_count += 1
        result = run_repo_code_chunking_for_repo(repo_id, store=store)
        stage_status = str(result.get("stage_status") or "")
        if stage_status == "chunked":
            chunked_count += 1
        elif stage_status == "chunk_failed":
            failed_count += 1
        else:
            skipped_count += 1

    return {
        "stage": "chunk",
        "shard_index": shard_index,
        "shard_count": resolved_shard_count,
        "processed_count": processed_count,
        "chunked_count": chunked_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
    }


def run_repo_code_chunking() -> None:
    store = OpenSearchStore()
    chunk_started_at = datetime.now(timezone.utc)
    repo_docs = _load_repo_docs_for_chunking(store, chunk_started_at)
    if not repo_docs:
        return

    claimed_docs = _claim_repo_docs_for_chunking(
        store=store,
        repo_docs=repo_docs,
        started_at=chunk_started_at.isoformat(),
    )

    max_lines, overlap_lines = _chunk_parameters()
    for doc_id, hit in claimed_docs.items():
        source = dict(hit.get("_source", {}))
        chunk_finished_at = datetime.now(timezone.utc).isoformat()
        try:
            stats = _chunk_single_repo(
                store,
                repo_id=doc_id,
                source=source,
                chunked_at=chunk_finished_at,
            )

            chunk_error_message = None
            chunk_status = "chunked"
            if stats.chunk_docs_created == 0:
                chunk_status = "chunk_failed"
                chunk_error_message = "no_chunks_created"

            updated_source = {
                **source,
                "chunk_status": chunk_status,
                "chunk_finished_at": chunk_finished_at,
                "chunk_error_message": chunk_error_message,
                "chunk_total_count": stats.chunk_docs_created,
                "chunk_code_files_count": stats.code_files_seen,
                "chunk_total_lines_seen": stats.total_lines_seen,
                "chunked_lines_total": stats.chunked_lines_total,
                "chunk_skipped_missing_files": stats.skipped_missing_files,
                "chunk_skipped_non_utf8_files": stats.skipped_non_utf8_files,
                "chunk_deleted_previous_docs": stats.deleted_previous_docs,
                "chunk_max_lines": max_lines,
                "chunk_overlap_lines": overlap_lines,
            }
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=doc_id,
                source=updated_source,
            )
            claimed_docs[doc_id]["_source"] = updated_source
        except Exception as exc:  # noqa: BLE001 - repo별 실패를 이어서 처리해야 함
            logger.warning("Code chunking failed for repo_id=%s error=%s", doc_id, str(exc))
            failed_source = {
                **source,
                "chunk_status": "chunk_failed",
                "chunk_finished_at": chunk_finished_at,
                "chunk_error_message": str(exc),
                "chunk_max_lines": max_lines,
                "chunk_overlap_lines": overlap_lines,
            }
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=doc_id,
                source=failed_source,
            )
            claimed_docs[doc_id]["_source"] = failed_source
