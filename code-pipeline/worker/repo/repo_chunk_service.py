import hashlib
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from worker.common.config import settings
from worker.repo.repo_pipeline_manifest_service import (
    CHUNK_COMPLETED_STAGE,
    write_stage_manifest,
)
from worker.repo.repo_snapshot_local_paths import (
    resolve_snapshot_download_path,
    resolve_snapshot_extract_dir,
    resolve_snapshot_root_path,
    strip_local_snapshot_fields,
)
from worker.repo.repo_stage_service import list_repo_ids_for_chunking
from worker.storage.opensearch_store import OpenSearchStore

try:
    from tree_sitter import Node, Parser, Tree
    from tree_sitter_languages import get_parser
except ImportError:  # pragma: no cover - optional dependency fallback
    Node = Any
    Parser = Any
    Tree = Any
    get_parser = None


logger = logging.getLogger(__name__)

REPO_REGISTRY_INDEX = "repo_registry_index"
REPO_FILE_INDEX = "repo_file_index"
REPO_CHUNK_INDEX = "repo_chunk_index"

LANGUAGE_TO_PARSER_KEY = {
    "python": "python",
    "javascript": "javascript",
    "typescript": "typescript",
    "java": "java",
    "go": "go",
    "rust": "rust",
    "c": "c",
    "cpp": "cpp",
    "csharp": "c_sharp",
    "php": "php",
    "ruby": "ruby",
    "swift": "swift",
    "kotlin": "kotlin",
    "scala": "scala",
    "shell": "bash",
    "sql": "sql",
}

FUNCTION_NODE_TYPES_BY_LANGUAGE = {
    "python": {
        "function_definition",
        "decorated_definition",
    },
    "javascript": {
        "function_declaration",
        "generator_function_declaration",
        "method_definition",
        "arrow_function",
    },
    "typescript": {
        "function_declaration",
        "generator_function_declaration",
        "method_definition",
        "arrow_function",
    },
    "java": {
        "method_declaration",
        "constructor_declaration",
    },
    "go": {
        "function_declaration",
        "method_declaration",
    },
    "rust": {
        "function_item",
    },
    "c": {
        "function_definition",
    },
    "cpp": {
        "function_definition",
    },
    "csharp": {
        "method_declaration",
        "constructor_declaration",
        "local_function_statement",
    },
    "php": {
        "function_definition",
        "method_declaration",
    },
    "ruby": {
        "method",
    },
    "swift": {
        "function_declaration",
        "initializer_declaration",
        "deinitializer_declaration",
    },
    "kotlin": {
        "function_declaration",
        "secondary_constructor",
    },
    "scala": {
        "function_definition",
    },
    "shell": {
        "function_definition",
    },
    "sql": {
        "create_function_statement",
        "create_procedure_statement",
    },
}

_PARSER_CACHE: dict[str, Parser | None] = {}


@dataclass(slots=True)
class RepoChunkStats:
    code_files_seen: int = 0
    chunk_docs_created: int = 0
    total_lines_seen: int = 0
    chunked_lines_total: int = 0
    function_chunks_created: int = 0
    line_window_chunks_created: int = 0
    files_chunked_with_functions: int = 0
    files_chunked_with_line_window: int = 0
    parser_unavailable_files: int = 0
    parser_failed_files: int = 0
    skipped_missing_files: int = 0
    skipped_non_utf8_files: int = 0
    deleted_previous_docs: int = 0


@dataclass(frozen=True, slots=True)
class FunctionSpan:
    line_start: int
    line_end: int
    node_type: str


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
    *,
    refresh_writes: bool = False,
) -> dict[str, dict]:
    claimed_docs = {}
    for hit in repo_docs:
        doc_id = hit["_id"]
        source = strip_local_snapshot_fields(dict(hit.get("_source", {})))
        attempt_count = int(source.get("chunk_attempt_count") or 0) + 1
        claimed_source = {
            **source,
            "chunk_batch_id": source.get("file_extract_batch_id") or source.get("crawl_batch_id"),
            "chunk_status": "chunking",
            "chunk_started_at": started_at,
            "chunk_finished_at": None,
            "chunk_error_message": None,
            "chunk_attempt_count": attempt_count,
            "chunk_total_count": None,
            "chunk_code_files_count": None,
            "chunk_total_lines_seen": None,
            "chunked_lines_total": None,
            "chunk_function_chunks_count": None,
            "chunk_line_window_chunks_count": None,
            "chunk_function_chunked_files_count": None,
            "chunk_line_window_chunked_files_count": None,
            "chunk_parser_unavailable_files": None,
            "chunk_parser_failed_files": None,
            "chunk_skipped_missing_files": None,
            "chunk_skipped_non_utf8_files": None,
            "chunk_deleted_previous_docs": None,
            "chunk_max_lines": None,
            "chunk_overlap_lines": None,
            "chunk_strategy": None,
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=claimed_source,
            refresh=refresh_writes,
        )
        claimed_docs[doc_id] = {"_id": doc_id, "_source": claimed_source}
    return claimed_docs


def _chunk_parameters() -> tuple[int, int]:
    max_lines = max(1, int(settings.repo_chunk_max_lines))
    overlap_lines = max(0, int(settings.repo_chunk_overlap_lines))
    if overlap_lines >= max_lines:
        overlap_lines = max(0, max_lines - 1)
    return max_lines, overlap_lines


def _is_path_within(path: Path, base_dir: Path) -> bool:
    try:
        path.relative_to(base_dir)
        return True
    except ValueError:
        return False


def _rmtree_force_writable(
    func: Callable[..., object],
    path: str,
    exc_info: tuple[type[BaseException], BaseException, object],
) -> None:
    target = Path(path)
    try:
        if target.exists():
            os.chmod(target, 0o700 if target.is_dir() else 0o600)
        parent = target.parent
        if parent.exists():
            os.chmod(parent, 0o700)
        func(path)
    except Exception:
        raise exc_info[1]


def _unlink_force_writable(target: Path) -> None:
    if target.exists():
        os.chmod(target, 0o600)
    parent = target.parent
    if parent.exists():
        os.chmod(parent, 0o700)
    target.unlink()


def _prune_local_snapshot_artifacts(*, base_dir: Path, source: dict) -> tuple[bool, str | None]:
    resolved_base_dir = base_dir.resolve()
    pruned_any = False
    missing_artifacts: list[str] = []

    extract_dir = resolve_snapshot_extract_dir(base_dir, source).resolve()
    if not _is_path_within(extract_dir, resolved_base_dir):
        raise ValueError(
            f"Refusing to prune snapshot_extract_dir outside local data dir: {extract_dir}"
        )
    if extract_dir.exists() and not extract_dir.is_dir():
        raise ValueError(f"snapshot_extract_dir is not a directory: {extract_dir}")
    if extract_dir.exists():
        shutil.rmtree(extract_dir, onerror=_rmtree_force_writable)
        pruned_any = True
    else:
        missing_artifacts.append("snapshot_extract_dir_missing")

    download_path = resolve_snapshot_download_path(base_dir, source).resolve()
    if not _is_path_within(download_path, resolved_base_dir):
        raise ValueError(
            f"Refusing to prune snapshot_download_path outside local data dir: {download_path}"
        )
    if download_path.exists() and not download_path.is_file():
        raise ValueError(f"snapshot_download_path is not a file: {download_path}")
    if download_path.exists():
        _unlink_force_writable(download_path)
        pruned_any = True
    else:
        missing_artifacts.append("snapshot_download_path_missing")

    cleanup_error = ",".join(missing_artifacts) if missing_artifacts else None
    return pruned_any, cleanup_error


def _maybe_prune_local_snapshot_artifacts(
    *,
    store: OpenSearchStore,
    repo_id: str,
    source: dict,
    chunk_status: str,
) -> tuple[dict, str | None, str | None, str | None]:
    updated_source = dict(source)
    if chunk_status != "chunked":
        return updated_source, None, None, None

    cleanup_finished_at = datetime.now(timezone.utc).isoformat()
    try:
        pruned, cleanup_error = _prune_local_snapshot_artifacts(
            base_dir=store.base_dir,
            source=source,
        )
    except Exception as exc:  # noqa: BLE001 - cleanup 실패가 chunk 성공 자체를 뒤집지 않도록 함
        logger.warning(
            "Local snapshot cleanup failed for repo_id=%s error=%s",
            repo_id,
            str(exc),
        )
        return updated_source, "cleanup_failed", cleanup_finished_at, str(exc)

    cleanup_status = "pruned" if pruned else "skipped"
    updated_source.pop("snapshot_extract_dir", None)
    updated_source.pop("snapshot_root_path", None)
    updated_source.pop("snapshot_download_path", None)

    return updated_source, cleanup_status, cleanup_finished_at, cleanup_error


def _best_effort_replace_repo_chunk_doc(
    *,
    store: OpenSearchStore,
    doc_id: str,
    source: dict,
    refresh: bool,
) -> bool:
    try:
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=source,
            refresh=refresh,
        )
    except Exception as exc:  # noqa: BLE001 - 원본 실패를 덮는 2차 실패는 경고만 남김
        logger.warning(
            "Best-effort repo chunk status write failed for repo_id=%s error=%s",
            doc_id,
            str(exc),
        )
        return False
    return True


def _resolve_parser_key(language: object) -> str | None:
    normalized = str(language or "").strip().lower()
    if not normalized:
        return None
    return LANGUAGE_TO_PARSER_KEY.get(normalized)


def _get_tree_sitter_parser(language: object) -> Parser | None:
    parser_key = _resolve_parser_key(language)
    if parser_key is None:
        return None

    if parser_key in _PARSER_CACHE:
        return _PARSER_CACHE[parser_key]

    if get_parser is None:
        _PARSER_CACHE[parser_key] = None
        return None

    try:
        parser = get_parser(parser_key)
    except Exception:  # noqa: BLE001 - unsupported parser는 라인 윈도우 fallback
        parser = None

    _PARSER_CACHE[parser_key] = parser
    return parser


def _is_function_like_node(language: str, node: Node) -> bool:
    node_type = str(node.type or "")
    if not node_type:
        return False

    if not node.is_named:
        return False

    if language == "python" and node_type == "function_definition":
        parent = node.parent
        if parent is not None and str(parent.type or "") == "decorated_definition":
            return False

    function_node_types = FUNCTION_NODE_TYPES_BY_LANGUAGE.get(language, set())
    if node_type in function_node_types:
        return True

    # 언어별 매핑이 없거나 누락된 노드 타입은 이름 기반으로 보수적으로 허용.
    lowered = node_type.lower()
    if "function" in lowered or "method" in lowered or "constructor" in lowered:
        return True

    return False


def _collect_function_spans(language: object, tree: Tree) -> list[FunctionSpan]:
    normalized_language = str(language or "").strip().lower()
    if not normalized_language:
        return []

    root = tree.root_node
    stack: list[Node] = [root]
    spans: list[FunctionSpan] = []
    seen_ranges: set[tuple[int, int]] = set()

    while stack:
        node = stack.pop()
        children = list(node.children)
        if children:
            stack.extend(reversed(children))

        if not _is_function_like_node(normalized_language, node):
            continue

        line_start = int(node.start_point[0]) + 1
        line_end = int(node.end_point[0]) + 1
        if line_start <= 0 or line_end < line_start:
            continue

        range_key = (line_start, line_end)
        if range_key in seen_ranges:
            continue
        seen_ranges.add(range_key)
        spans.append(
            FunctionSpan(
                line_start=line_start,
                line_end=line_end,
                node_type=str(node.type or ""),
            )
        )

    spans.sort(key=lambda span: (span.line_start, span.line_end, span.node_type))
    return spans


def _iterate_line_chunks_in_range(
    *,
    lines: list[str],
    line_start: int,
    line_end: int,
    max_lines: int,
    overlap_lines: int,
):
    bounded_start = max(1, line_start)
    bounded_end = min(len(lines), line_end)
    if bounded_end < bounded_start:
        return

    sub_lines = lines[bounded_start - 1 : bounded_end]
    for start, end, chunk_text in _iterate_line_chunks(
        sub_lines,
        max_lines=max_lines,
        overlap_lines=overlap_lines,
    ):
        yield (
            bounded_start + start - 1,
            bounded_start + end - 1,
            chunk_text,
        )


def _iterate_chunks_for_file(
    *,
    content: str,
    lines: list[str],
    language: object,
    max_lines: int,
    overlap_lines: int,
):
    parser = _get_tree_sitter_parser(language)
    if parser is None:
        yield from (
            (line_start, line_end, chunk_text, "line_window", None, "parser_unavailable")
            for line_start, line_end, chunk_text in _iterate_line_chunks(
                lines,
                max_lines=max_lines,
                overlap_lines=overlap_lines,
            )
        )
        return

    try:
        tree = parser.parse(content.encode("utf-8"))
    except Exception:  # noqa: BLE001 - parse 실패 시 라인 윈도우 fallback
        yield from (
            (line_start, line_end, chunk_text, "line_window", None, "parser_failed")
            for line_start, line_end, chunk_text in _iterate_line_chunks(
                lines,
                max_lines=max_lines,
                overlap_lines=overlap_lines,
            )
        )
        return

    function_spans = _collect_function_spans(language, tree)
    if not function_spans:
        yield from (
            (line_start, line_end, chunk_text, "line_window", None, "no_function_nodes")
            for line_start, line_end, chunk_text in _iterate_line_chunks(
                lines,
                max_lines=max_lines,
                overlap_lines=overlap_lines,
            )
        )
        return

    for span in function_spans:
        span_line_count = span.line_end - span.line_start + 1
        if span_line_count <= max_lines:
            chunk_text = "\n".join(lines[span.line_start - 1 : span.line_end])
            yield (
                span.line_start,
                span.line_end,
                chunk_text,
                "function",
                span.node_type,
                None,
            )
            continue

        for line_start, line_end, chunk_text in _iterate_line_chunks_in_range(
            lines=lines,
            line_start=span.line_start,
            line_end=span.line_end,
            max_lines=max_lines,
            overlap_lines=overlap_lines,
        ):
            yield (
                line_start,
                line_end,
                chunk_text,
                "function_window",
                span.node_type,
                None,
            )


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


def _repo_id_query(repo_id: str) -> dict:
    return {
        "bool": {
            "should": [
                {"term": {"repo_id.keyword": repo_id}},
                {"term": {"repo_id": repo_id}},
            ],
            "minimum_should_match": 1,
        }
    }


def _iter_existing_repo_chunk_doc_ids(store: OpenSearchStore, repo_id: str):
    yield from (
        hit["_id"]
        for hit in store.iterate_documents_by_query(
            collection_name=REPO_CHUNK_INDEX,
            query=_repo_id_query(repo_id),
            size=1000,
            sort=[
                {"file_path.keyword": {"order": "asc", "unmapped_type": "keyword"}},
                {"line_start": {"order": "asc", "unmapped_type": "long"}},
                {"line_end": {"order": "asc", "unmapped_type": "long"}},
                {"chunk_index": {"order": "asc", "unmapped_type": "long"}},
            ],
            source_includes=[],
        )
    )


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
        # `_id` 정렬은 fielddata 메모리를 크게 사용해 circuit breaker를 유발할 수 있다.
        # repo 단위 query에서는 file_path가 사실상 유일 키이므로 doc_values 기반 정렬을 사용한다.
        sort=[{"file_path.keyword": {"order": "asc", "unmapped_type": "keyword"}}],
        source_includes=["file_path", "language"],
    )


def _chunk_single_repo(
    store: OpenSearchStore,
    *,
    repo_id: str,
    source: dict,
    chunked_at: str,
    refresh_writes: bool = False,
) -> RepoChunkStats:
    snapshot_root = resolve_snapshot_root_path(store.base_dir, source).resolve()
    if not snapshot_root.exists() or not snapshot_root.is_dir():
        raise FileNotFoundError(f"snapshot_root_path is missing or invalid: {snapshot_root}")

    max_lines, overlap_lines = _chunk_parameters()
    stale_doc_ids = set(_iter_existing_repo_chunk_doc_ids(store, repo_id))

    owner = str(source.get("owner") or "").strip().lower()
    repo_name = str(source.get("repo_name") or "").strip().lower()
    canonical_repo_url = source.get("canonical_repo_url")
    snapshot_ref = source.get("snapshot_ref")
    bulk_flush_docs = max(1, int(settings.opensearch_bulk_flush_docs))

    stats = RepoChunkStats()
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

        file_has_function_chunk = False
        file_has_line_window_chunk = False
        file_parser_unavailable = False
        file_parser_failed = False
        for (
            line_start,
            line_end,
            chunk_text,
            chunk_strategy,
            function_node_type,
            fallback_reason,
        ) in _iterate_chunks_for_file(
            content=content,
            lines=lines,
            language=language,
            max_lines=max_lines,
            overlap_lines=overlap_lines,
        ):
            if not chunk_text.strip():
                continue

            chunk_index += 1
            line_count = line_end - line_start + 1
            stats.chunk_docs_created += 1
            stats.chunked_lines_total += line_count
            if chunk_strategy in {"function", "function_window"}:
                file_has_function_chunk = True
                stats.function_chunks_created += 1
            else:
                file_has_line_window_chunk = True
                stats.line_window_chunks_created += 1
                if fallback_reason == "parser_unavailable":
                    file_parser_unavailable = True
                elif fallback_reason == "parser_failed":
                    file_parser_failed = True

            chunk_doc_id = _build_chunk_doc_id(
                repo_id=repo_id,
                file_path=relative_path,
                line_start=line_start,
                line_end=line_end,
                chunk_index=chunk_index,
            )
            stale_doc_ids.discard(chunk_doc_id)
            pending_docs.append(
                (
                    chunk_doc_id,
                    {
                        "repo_id": repo_id,
                        "owner": owner,
                        "repo_name": repo_name,
                        "canonical_repo_url": canonical_repo_url,
                        "snapshot_ref": snapshot_ref,
                        "file_path": relative_path,
                        "language": language,
                        "chunk_index": chunk_index,
                        "chunk_strategy": chunk_strategy,
                        "function_node_type": function_node_type,
                        "fallback_reason": fallback_reason,
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
                store.bulk_index_documents(
                    collection_name=REPO_CHUNK_INDEX,
                    documents=pending_docs,
                    refresh=False,
                    chunk_size=bulk_flush_docs,
                )
                pending_docs.clear()

        if file_has_function_chunk:
            stats.files_chunked_with_functions += 1
        if file_has_line_window_chunk:
            stats.files_chunked_with_line_window += 1
        if file_parser_unavailable:
            stats.parser_unavailable_files += 1
        if file_parser_failed:
            stats.parser_failed_files += 1

    if pending_docs:
        store.bulk_index_documents(
            collection_name=REPO_CHUNK_INDEX,
            documents=pending_docs,
            refresh=False,
            chunk_size=bulk_flush_docs,
        )

    if stale_doc_ids:
        store.bulk_delete_documents(
            collection_name=REPO_CHUNK_INDEX,
            doc_ids=list(stale_doc_ids),
            refresh=False,
            chunk_size=bulk_flush_docs,
        )
        stats.deleted_previous_docs = len(stale_doc_ids)

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
    refresh_writes: bool = False,
) -> dict:
    if store is None:
        store = OpenSearchStore()

    chunk_started_at = datetime.now(timezone.utc).isoformat()
    max_lines, overlap_lines = _chunk_parameters()
    current_source: dict = {}
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
        current_source = dict(source)
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
            refresh_writes=refresh_writes,
        )
        claimed_source = dict(claimed_docs[repo_id]["_source"])
        current_source = dict(claimed_source)
        chunk_finished_at = datetime.now(timezone.utc).isoformat()
        stats = _chunk_single_repo(
            store,
            repo_id=repo_id,
            source=claimed_source,
            chunked_at=chunk_finished_at,
            refresh_writes=refresh_writes,
        )

        chunk_error_message = None
        chunk_status = "chunked"
        if stats.chunk_docs_created == 0:
            chunk_status = "chunk_failed"
            chunk_error_message = "no_chunks_created"

        updated_source = {
            **strip_local_snapshot_fields(claimed_source),
            "chunk_status": chunk_status,
            "chunk_finished_at": chunk_finished_at,
            "chunk_error_message": chunk_error_message,
            "chunk_total_count": stats.chunk_docs_created,
            "chunk_code_files_count": stats.code_files_seen,
            "chunk_total_lines_seen": stats.total_lines_seen,
            "chunked_lines_total": stats.chunked_lines_total,
            "chunk_function_chunks_count": stats.function_chunks_created,
            "chunk_line_window_chunks_count": stats.line_window_chunks_created,
            "chunk_function_chunked_files_count": stats.files_chunked_with_functions,
            "chunk_line_window_chunked_files_count": stats.files_chunked_with_line_window,
            "chunk_parser_unavailable_files": stats.parser_unavailable_files,
            "chunk_parser_failed_files": stats.parser_failed_files,
            "chunk_skipped_missing_files": stats.skipped_missing_files,
            "chunk_skipped_non_utf8_files": stats.skipped_non_utf8_files,
            "chunk_deleted_previous_docs": stats.deleted_previous_docs,
            "chunk_max_lines": max_lines,
            "chunk_overlap_lines": overlap_lines,
            "chunk_strategy": "function_first_with_fallback",
        }
        updated_source, cleanup_status, cleanup_finished_at, cleanup_error = (
            _maybe_prune_local_snapshot_artifacts(
                store=store,
                repo_id=repo_id,
                source=updated_source,
                chunk_status=chunk_status,
            )
        )
        updated_source["snapshot_local_cleanup_status"] = cleanup_status
        updated_source["snapshot_local_cleanup_at"] = cleanup_finished_at
        updated_source["snapshot_local_cleanup_error"] = cleanup_error
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=repo_id,
            source=updated_source,
            refresh=refresh_writes,
        )

        return {
            "repo_id": repo_id,
            "stage": "chunk",
            "stage_status": chunk_status,
            "chunk_total_count": stats.chunk_docs_created,
            "function_chunks_count": stats.function_chunks_created,
            "line_window_chunks_count": stats.line_window_chunks_created,
            "code_files_seen": stats.code_files_seen,
            "error_message": chunk_error_message,
            "snapshot_cleanup_status": cleanup_status,
            "snapshot_cleanup_error": cleanup_error,
        }
    except Exception as exc:  # noqa: BLE001 - repo 단위 파이프라인 실패를 상위로 전달하기 위함
        logger.warning("Code chunking failed for repo_id=%s error=%s", repo_id, str(exc))
        failed_source = {
            **strip_local_snapshot_fields(dict(current_source)),
            "chunk_status": "chunk_failed",
            "chunk_finished_at": datetime.now(timezone.utc).isoformat(),
            "chunk_error_message": str(exc),
            "chunk_max_lines": max_lines,
            "chunk_overlap_lines": overlap_lines,
            "chunk_strategy": "function_first_with_fallback",
        }
        _best_effort_replace_repo_chunk_doc(
            store=store,
            doc_id=repo_id,
            source=failed_source,
            refresh=refresh_writes,
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
    batch_id: str | None = None,
) -> dict:
    resolved_shard_count = (
        shard_count
        if isinstance(shard_count, int) and shard_count > 0
        else max(1, settings.repo_pipeline_parallelism)
    )
    store = OpenSearchStore()
    repo_ids = list_repo_ids_for_chunking(
        batch_size=batch_size,
        batch_id=batch_id,
        shard_index=shard_index,
        shard_count=resolved_shard_count,
    )

    processed_count = 0
    chunked_count = 0
    failed_count = 0
    skipped_count = 0
    manifest_entries: list[dict] = []
    for repo_id in repo_ids:
        processed_count += 1
        result = run_repo_code_chunking_for_repo(
            repo_id,
            store=store,
            refresh_writes=False,
        )
        stage_status = str(result.get("stage_status") or "")
        if stage_status == "chunked":
            chunked_count += 1
            manifest_entries.append(
                {
                    "batch_id": batch_id,
                    "repo_id": repo_id,
                    "stage_status": stage_status,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "shard_index": shard_index,
                }
            )
        elif stage_status == "chunk_failed":
            failed_count += 1
            manifest_entries.append(
                {
                    "batch_id": batch_id,
                    "repo_id": repo_id,
                    "stage_status": stage_status,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                    "shard_index": shard_index,
                }
            )
        else:
            skipped_count += 1

    write_stage_manifest(
        base_dir=store.base_dir,
        batch_id=batch_id,
        stage_name=CHUNK_COMPLETED_STAGE,
        entries=manifest_entries,
        shard_index=shard_index,
    )

    return {
        "stage": "chunk",
        "shard_index": shard_index,
        "shard_count": resolved_shard_count,
        "processed_count": processed_count,
        "chunked_count": chunked_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
        "manifest_count": len(manifest_entries),
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
        refresh_writes=False,
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
                refresh_writes=False,
            )

            chunk_error_message = None
            chunk_status = "chunked"
            if stats.chunk_docs_created == 0:
                chunk_status = "chunk_failed"
                chunk_error_message = "no_chunks_created"

            updated_source = {
                **strip_local_snapshot_fields(source),
                "chunk_status": chunk_status,
                "chunk_finished_at": chunk_finished_at,
                "chunk_error_message": chunk_error_message,
                "chunk_total_count": stats.chunk_docs_created,
                "chunk_code_files_count": stats.code_files_seen,
                "chunk_total_lines_seen": stats.total_lines_seen,
                "chunked_lines_total": stats.chunked_lines_total,
                "chunk_function_chunks_count": stats.function_chunks_created,
                "chunk_line_window_chunks_count": stats.line_window_chunks_created,
                "chunk_function_chunked_files_count": stats.files_chunked_with_functions,
                "chunk_line_window_chunked_files_count": stats.files_chunked_with_line_window,
                "chunk_parser_unavailable_files": stats.parser_unavailable_files,
                "chunk_parser_failed_files": stats.parser_failed_files,
                "chunk_skipped_missing_files": stats.skipped_missing_files,
                "chunk_skipped_non_utf8_files": stats.skipped_non_utf8_files,
                "chunk_deleted_previous_docs": stats.deleted_previous_docs,
                "chunk_max_lines": max_lines,
                "chunk_overlap_lines": overlap_lines,
                "chunk_strategy": "function_first_with_fallback",
            }
            updated_source, cleanup_status, cleanup_finished_at, cleanup_error = (
                _maybe_prune_local_snapshot_artifacts(
                    store=store,
                    repo_id=doc_id,
                    source=updated_source,
                    chunk_status=chunk_status,
                )
            )
            updated_source["snapshot_local_cleanup_status"] = cleanup_status
            updated_source["snapshot_local_cleanup_at"] = cleanup_finished_at
            updated_source["snapshot_local_cleanup_error"] = cleanup_error
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=doc_id,
                source=updated_source,
                refresh=False,
            )
            claimed_docs[doc_id]["_source"] = updated_source
        except Exception as exc:  # noqa: BLE001 - repo별 실패를 이어서 처리해야 함
            logger.warning("Code chunking failed for repo_id=%s error=%s", doc_id, str(exc))
            failed_source = {
                **strip_local_snapshot_fields(source),
                "chunk_status": "chunk_failed",
                "chunk_finished_at": chunk_finished_at,
                "chunk_error_message": str(exc),
                "chunk_max_lines": max_lines,
                "chunk_overlap_lines": overlap_lines,
                "chunk_strategy": "function_first_with_fallback",
            }
            _best_effort_replace_repo_chunk_doc(
                store=store,
                doc_id=doc_id,
                source=failed_source,
                refresh=False,
            )
            claimed_docs[doc_id]["_source"] = failed_source
