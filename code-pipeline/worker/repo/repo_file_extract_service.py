import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from worker.common.config import settings
from worker.repo.repo_stage_service import list_repo_ids_for_extraction
from worker.storage.opensearch_store import OpenSearchStore


logger = logging.getLogger(__name__)

REPO_FILE_INDEX = "repo_file_index"
REPO_REGISTRY_INDEX = "repo_registry_index"

IGNORED_DIRECTORIES = {
    ".git",
    ".hg",
    ".svn",
    ".idea",
    ".vscode",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "node_modules",
    "vendor",
    "dist",
    "build",
    "target",
    ".venv",
    "venv",
}

CODE_LANGUAGE_BY_EXTENSION = {
    ".py": "python",
    ".js": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".java": "java",
    ".go": "go",
    ".rs": "rust",
    ".c": "c",
    ".h": "c",
    ".cc": "cpp",
    ".cpp": "cpp",
    ".cxx": "cpp",
    ".cs": "csharp",
    ".php": "php",
    ".rb": "ruby",
    ".swift": "swift",
    ".kt": "kotlin",
    ".kts": "kotlin",
    ".scala": "scala",
    ".sh": "shell",
    ".bash": "shell",
    ".zsh": "shell",
    ".sql": "sql",
}

SPECIAL_CODE_FILENAMES = {
    "dockerfile": "dockerfile",
    "makefile": "makefile",
    "cmakelists.txt": "cmake",
    "build.gradle": "gradle",
}

LICENSE_FILENAMES = {
    "license",
    "license.txt",
    "license.md",
    "copying",
    "copying.txt",
    "copying.md",
    "notice",
    "notice.txt",
    "notice.md",
}

MARKUP_EXTENSIONS = {".md", ".rst", ".txt"}
CONFIG_EXTENSIONS = {
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".env",
}

@dataclass(slots=True)
class RepoFileExtractStats:
    total_files_seen: int = 0
    text_files_indexed: int = 0
    code_files_indexed: int = 0
    skipped_large_files: int = 0
    skipped_binary_files: int = 0
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


def _is_stale_extracting(source: dict, now: datetime) -> bool:
    if source.get("file_extract_status") != "extracting":
        return False

    started_at = _parse_datetime(source.get("file_extract_started_at"))
    if started_at is None:
        return True

    elapsed_seconds = (now - started_at).total_seconds()
    return elapsed_seconds >= settings.repo_crawl_lease_seconds


def _normalize_repo_identity(source: dict) -> tuple[str, str]:
    owner = str(source.get("owner") or "").strip().lower()
    repo = str(source.get("repo_name") or "").strip().lower()
    return owner, repo


def _load_repo_docs_for_extraction(store: OpenSearchStore, now: datetime) -> list[dict]:
    downloaded_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="crawl_status",
        value="downloaded",
        size=max(1, settings.repo_file_extract_repo_limit),
    )
    extracting_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="file_extract_status",
        value="extracting",
        size=max(1, settings.repo_file_extract_repo_limit),
    )

    repo_docs_by_id = {}
    for hit in downloaded_docs:
        source = hit.get("_source", {})
        if source.get("file_extract_status") == "extracted":
            continue
        repo_docs_by_id[hit["_id"]] = hit

    stale_docs = []
    for hit in extracting_docs:
        source = hit.get("_source", {})
        if source.get("crawl_status") != "downloaded":
            continue
        if _is_stale_extracting(source, now):
            repo_docs_by_id[hit["_id"]] = hit
            stale_docs.append(hit)

    if stale_docs:
        logger.warning(
            "Reclaiming stale repo extraction leases: count=%s lease_seconds=%s",
            len(stale_docs),
            settings.repo_crawl_lease_seconds,
        )

    return list(repo_docs_by_id.values())


def _claim_repo_docs_for_extraction(
    store: OpenSearchStore,
    repo_docs: list[dict],
    started_at: str,
    *,
    refresh_writes: bool = True,
) -> dict[str, dict]:
    claimed_docs = {}
    for hit in repo_docs:
        doc_id = hit["_id"]
        source = dict(hit.get("_source", {}))
        attempt_count = int(source.get("file_extract_attempt_count") or 0) + 1
        claimed_source = {
            **source,
            "file_extract_status": "extracting",
            "file_extract_started_at": started_at,
            "file_extract_finished_at": None,
            "file_extract_error_message": None,
            "file_extract_attempt_count": attempt_count,
            "file_extract_total_files_seen": None,
            "file_extract_text_files_count": None,
            "file_extract_code_files_count": None,
            "file_extract_skipped_large_files": None,
            "file_extract_skipped_binary_files": None,
            "file_extract_skipped_non_utf8_files": None,
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=claimed_source,
            refresh=refresh_writes,
        )
        claimed_docs[doc_id] = {"_id": doc_id, "_source": claimed_source}
    return claimed_docs


def _iter_snapshot_files(snapshot_root: Path):
    for current_dir, dir_names, file_names in os.walk(snapshot_root, topdown=True):
        dir_names[:] = [name for name in dir_names if name not in IGNORED_DIRECTORIES]
        base_dir = Path(current_dir)
        for file_name in file_names:
            yield base_dir / file_name


def _is_binary_bytes(raw_bytes: bytes) -> bool:
    return b"\x00" in raw_bytes


def _classify_file(file_path: str) -> tuple[str, bool, bool, str]:
    file_name = PurePosixPath(file_path).name.lower()
    extension = PurePosixPath(file_path).suffix.lower()

    if file_name in LICENSE_FILENAMES or file_name.startswith("license"):
        return "license", False, True, "license"

    if file_name in SPECIAL_CODE_FILENAMES:
        return SPECIAL_CODE_FILENAMES[file_name], True, False, "code"

    if extension in CODE_LANGUAGE_BY_EXTENSION:
        return CODE_LANGUAGE_BY_EXTENSION[extension], True, False, "code"

    if extension in CONFIG_EXTENSIONS:
        return "config", False, False, "config"

    if extension in MARKUP_EXTENSIONS:
        return "document", False, False, "document"

    return "unknown", False, False, "text"


def _build_repo_file_doc_id(repo_id: str, file_path: str) -> str:
    path_digest = hashlib.sha1(file_path.encode("utf-8")).hexdigest()
    return f"{repo_id}:file:{path_digest}"


def _max_file_size_bytes() -> int:
    return max(1, int(settings.repo_file_extract_max_file_size_bytes))


def _extract_repo_files(
    store: OpenSearchStore,
    *,
    repo_id: str,
    source: dict,
    extracted_at: str,
    refresh_writes: bool = True,
) -> RepoFileExtractStats:
    snapshot_root = Path(str(source.get("snapshot_root_path") or "")).resolve()
    if not snapshot_root.exists() or not snapshot_root.is_dir():
        raise FileNotFoundError(f"snapshot_root_path is missing or invalid: {snapshot_root}")

    max_file_size_bytes = _max_file_size_bytes()
    deleted_docs = store.delete_documents_by_field(
        collection_name=REPO_FILE_INDEX,
        field_name="repo_id",
        value=repo_id,
        refresh=refresh_writes,
    )

    owner, repo = _normalize_repo_identity(source)
    snapshot_ref = str(source.get("snapshot_ref") or "").strip() or None
    canonical_repo_url = source.get("canonical_repo_url")
    bulk_flush_docs = max(1, int(settings.opensearch_bulk_flush_docs))

    stats = RepoFileExtractStats(deleted_previous_docs=deleted_docs)
    pending_docs: list[tuple[str, dict]] = []
    for absolute_path in _iter_snapshot_files(snapshot_root):
        if not absolute_path.is_file():
            continue

        stats.total_files_seen += 1
        size_bytes = absolute_path.stat().st_size
        if size_bytes > max_file_size_bytes:
            stats.skipped_large_files += 1
            continue

        raw_bytes = absolute_path.read_bytes()
        if _is_binary_bytes(raw_bytes):
            stats.skipped_binary_files += 1
            continue

        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            stats.skipped_non_utf8_files += 1
            continue

        relative_path = absolute_path.relative_to(snapshot_root).as_posix()
        language, is_code_file, is_license_file, file_category = _classify_file(relative_path)
        if is_code_file:
            stats.code_files_indexed += 1

        stats.text_files_indexed += 1
        line_count = text.count("\n") + (1 if text else 0)
        pending_docs.append(
            (
                _build_repo_file_doc_id(repo_id, relative_path),
                {
                    "repo_id": repo_id,
                    "owner": owner,
                    "repo_name": repo,
                    "canonical_repo_url": canonical_repo_url,
                    "snapshot_ref": snapshot_ref,
                    "snapshot_root_path": str(snapshot_root),
                    "file_path": relative_path,
                    "file_name": PurePosixPath(relative_path).name,
                    "file_extension": PurePosixPath(relative_path).suffix.lower(),
                    "language": language,
                    "file_category": file_category,
                    "is_code_file": is_code_file,
                    "is_license_file": is_license_file,
                    "file_size_bytes": size_bytes,
                    "line_count": line_count,
                    "content_sha1": hashlib.sha1(raw_bytes).hexdigest(),
                    "extracted_at": extracted_at,
                },
            )
        )
        if len(pending_docs) >= bulk_flush_docs:
            store.bulk_index_documents(
                collection_name=REPO_FILE_INDEX,
                documents=pending_docs,
                refresh=False,
                chunk_size=bulk_flush_docs,
            )
            pending_docs.clear()

    if pending_docs:
        store.bulk_index_documents(
            collection_name=REPO_FILE_INDEX,
            documents=pending_docs,
            refresh=False,
            chunk_size=bulk_flush_docs,
        )

    if refresh_writes:
        store.refresh_index(REPO_FILE_INDEX)
    return stats


def _load_repo_doc_for_extraction(store: OpenSearchStore, repo_id: str) -> dict | None:
    repo_doc = store.get_document(collection_name=REPO_REGISTRY_INDEX, doc_id=repo_id)
    if not repo_doc:
        return None
    if not repo_doc.get("_source"):
        return None
    return repo_doc


def run_repo_file_extraction_for_repo(
    repo_id: str,
    *,
    store: OpenSearchStore | None = None,
    refresh_writes: bool = True,
) -> dict:
    if store is None:
        store = OpenSearchStore()

    extraction_started_at = datetime.now(timezone.utc).isoformat()
    try:
        repo_doc = _load_repo_doc_for_extraction(store, repo_id)
        if repo_doc is None:
            return {
                "repo_id": repo_id,
                "stage": "extract",
                "stage_status": "skipped",
                "reason": "repo_not_found",
            }

        source = dict(repo_doc.get("_source", {}))
        if source.get("crawl_status") != "downloaded":
            return {
                "repo_id": repo_id,
                "stage": "extract",
                "stage_status": "skipped",
                "reason": "crawl_not_downloaded",
            }

        if source.get("file_extract_status") == "extracted":
            return {
                "repo_id": repo_id,
                "stage": "extract",
                "stage_status": "extracted",
                "reason": "already_extracted",
            }

        if source.get("file_extract_status") == "extracting":
            if _is_stale_extracting(source, datetime.now(timezone.utc)):
                logger.warning("Reclaiming stale extraction lease for repo_id=%s", repo_id)
            else:
                return {
                    "repo_id": repo_id,
                    "stage": "extract",
                    "stage_status": "skipped",
                    "reason": "already_extracting",
                }

        claimed_docs = _claim_repo_docs_for_extraction(
            store=store,
            repo_docs=[repo_doc],
            started_at=extraction_started_at,
            refresh_writes=refresh_writes,
        )
        claimed_source = dict(claimed_docs[repo_id]["_source"])
        extract_finished_at = datetime.now(timezone.utc).isoformat()

        stats = _extract_repo_files(
            store,
            repo_id=repo_id,
            source=claimed_source,
            extracted_at=extract_finished_at,
            refresh_writes=refresh_writes,
        )

        validation_error = None
        final_status = "extracted"
        if stats.code_files_indexed == 0:
            validation_error = "no_code_files_extracted"
            final_status = "extract_failed"

        updated_source = {
            **claimed_source,
            "file_extract_status": final_status,
            "file_extract_finished_at": extract_finished_at,
            "file_extract_error_message": validation_error,
            "file_extract_total_files_seen": stats.total_files_seen,
            "file_extract_text_files_count": stats.text_files_indexed,
            "file_extract_code_files_count": stats.code_files_indexed,
            "file_extract_skipped_large_files": stats.skipped_large_files,
            "file_extract_skipped_binary_files": stats.skipped_binary_files,
            "file_extract_skipped_non_utf8_files": stats.skipped_non_utf8_files,
            "file_extract_deleted_previous_docs": stats.deleted_previous_docs,
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=repo_id,
            source=updated_source,
            refresh=refresh_writes,
        )

        return {
            "repo_id": repo_id,
            "stage": "extract",
            "stage_status": final_status,
            "code_files_indexed": stats.code_files_indexed,
            "text_files_indexed": stats.text_files_indexed,
            "total_files_seen": stats.total_files_seen,
            "error_message": validation_error,
        }
    except Exception as exc:  # noqa: BLE001 - repo 단위 파이프라인 실패를 상위로 전달하기 위함
        logger.warning("File extraction failed for repo_id=%s error=%s", repo_id, str(exc))
        if store is not None:
            repo_doc = _load_repo_doc_for_extraction(store, repo_id) or {"_source": {}}
            failed_source = {
                **dict(repo_doc.get("_source", {})),
                "file_extract_status": "extract_failed",
                "file_extract_finished_at": datetime.now(timezone.utc).isoformat(),
                "file_extract_error_message": str(exc),
            }
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=repo_id,
                source=failed_source,
                refresh=refresh_writes,
            )
        return {
            "repo_id": repo_id,
            "stage": "extract",
            "stage_status": "extract_failed",
            "error_message": str(exc),
        }


def run_repo_file_extraction_for_shard(
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
    repo_ids = list_repo_ids_for_extraction(
        batch_size=batch_size,
        shard_index=shard_index,
        shard_count=resolved_shard_count,
    )

    processed_count = 0
    extracted_count = 0
    failed_count = 0
    skipped_count = 0
    for repo_id in repo_ids:
        processed_count += 1
        result = run_repo_file_extraction_for_repo(
            repo_id,
            store=store,
            refresh_writes=False,
        )
        stage_status = str(result.get("stage_status") or "")
        if stage_status == "extracted":
            extracted_count += 1
        elif stage_status == "extract_failed":
            failed_count += 1
        else:
            skipped_count += 1

    if processed_count > 0:
        store.refresh_index(REPO_FILE_INDEX)
        store.refresh_index(REPO_REGISTRY_INDEX)

    return {
        "stage": "extract",
        "shard_index": shard_index,
        "shard_count": resolved_shard_count,
        "processed_count": processed_count,
        "extracted_count": extracted_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
    }


def run_repo_file_extraction() -> None:
    store = OpenSearchStore()
    extraction_started_at = datetime.now(timezone.utc)
    repo_docs = _load_repo_docs_for_extraction(store, extraction_started_at)
    if not repo_docs:
        return

    claimed_docs = _claim_repo_docs_for_extraction(
        store=store,
        repo_docs=repo_docs,
        started_at=extraction_started_at.isoformat(),
        refresh_writes=False,
    )

    for doc_id, hit in claimed_docs.items():
        source = dict(hit.get("_source", {}))
        extract_finished_at = datetime.now(timezone.utc).isoformat()
        try:
            stats = _extract_repo_files(
                store,
                repo_id=doc_id,
                source=source,
                extracted_at=extract_finished_at,
                refresh_writes=False,
            )

            validation_error = None
            final_status = "extracted"
            if stats.code_files_indexed == 0:
                validation_error = "no_code_files_extracted"
                final_status = "extract_failed"

            updated_source = {
                **source,
                "file_extract_status": final_status,
                "file_extract_finished_at": extract_finished_at,
                "file_extract_error_message": validation_error,
                "file_extract_total_files_seen": stats.total_files_seen,
                "file_extract_text_files_count": stats.text_files_indexed,
                "file_extract_code_files_count": stats.code_files_indexed,
                "file_extract_skipped_large_files": stats.skipped_large_files,
                "file_extract_skipped_binary_files": stats.skipped_binary_files,
                "file_extract_skipped_non_utf8_files": stats.skipped_non_utf8_files,
                "file_extract_deleted_previous_docs": stats.deleted_previous_docs,
            }
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=doc_id,
                source=updated_source,
                refresh=False,
            )
            claimed_docs[doc_id]["_source"] = updated_source
        except Exception as exc:  # noqa: BLE001 - repo별 실패를 계속 진행하기 위함
            logger.warning("File extraction failed for repo_id=%s error=%s", doc_id, str(exc))
            failed_source = {
                **source,
                "file_extract_status": "extract_failed",
                "file_extract_finished_at": extract_finished_at,
                "file_extract_error_message": str(exc),
            }
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=doc_id,
                source=failed_source,
                refresh=False,
            )
            claimed_docs[doc_id]["_source"] = failed_source

    store.refresh_index(REPO_FILE_INDEX)
    store.refresh_index(REPO_REGISTRY_INDEX)
