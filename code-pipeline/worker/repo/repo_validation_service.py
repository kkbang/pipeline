import logging
from datetime import datetime, timezone

from worker.common.config import settings
from worker.repo.repo_stage_service import list_repo_ids_for_validation
from worker.storage.opensearch_store import OpenSearchStore


logger = logging.getLogger(__name__)

REPO_REGISTRY_INDEX = "repo_registry_index"
REPO_FILE_INDEX = "repo_file_index"
REPO_CHUNK_INDEX = "repo_chunk_index"
REPO_VALIDATION_INDEX = "repo_processing_validation_index"


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


def _is_stale_validating(source: dict, now: datetime) -> bool:
    if source.get("validation_status") != "validating":
        return False

    started_at = _parse_datetime(source.get("validation_started_at"))
    if started_at is None:
        return True

    elapsed_seconds = (now - started_at).total_seconds()
    return elapsed_seconds >= settings.repo_crawl_lease_seconds


def _has_newer_chunk_result_than_validation(source: dict) -> bool:
    chunk_finished_at = _parse_datetime(source.get("chunk_finished_at"))
    validation_checked_at = _parse_datetime(source.get("validation_checked_at"))
    if chunk_finished_at is None:
        return False
    if validation_checked_at is None:
        return True
    return chunk_finished_at > validation_checked_at


def _load_repo_docs_for_validation(store: OpenSearchStore, now: datetime) -> list[dict]:
    chunked_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="chunk_status",
        value="chunked",
        size=max(1, settings.repo_validation_repo_limit),
    )
    chunk_failed_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="chunk_status",
        value="chunk_failed",
        size=max(1, settings.repo_validation_repo_limit),
    )
    validating_docs = store.find_documents_by_field(
        collection_name=REPO_REGISTRY_INDEX,
        field_name="validation_status",
        value="validating",
        size=max(1, settings.repo_validation_repo_limit),
    )

    repo_docs_by_id = {}
    for hit in [*chunked_docs, *chunk_failed_docs]:
        source = hit.get("_source", {})
        if source.get("validation_status") == "validated" and not _has_newer_chunk_result_than_validation(
            source
        ):
            continue
        repo_docs_by_id[hit["_id"]] = hit

    stale_docs = []
    for hit in validating_docs:
        source = hit.get("_source", {})
        if source.get("chunk_status") not in {"chunked", "chunk_failed"}:
            continue
        if _is_stale_validating(source, now):
            repo_docs_by_id[hit["_id"]] = hit
            stale_docs.append(hit)

    if stale_docs:
        logger.warning(
            "Reclaiming stale repo validation leases: count=%s lease_seconds=%s",
            len(stale_docs),
            settings.repo_crawl_lease_seconds,
        )

    return list(repo_docs_by_id.values())


def _claim_repo_docs_for_validation(
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
        attempt_count = int(source.get("validation_attempt_count") or 0) + 1
        claimed_source = {
            **source,
            "validation_status": "validating",
            "validation_started_at": started_at,
            "validation_checked_at": None,
            "validation_error_message": None,
            "validation_attempt_count": attempt_count,
            "validation_failed_rules": [],
            "validation_warning_rules": [],
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=doc_id,
            source=claimed_source,
            refresh=refresh_writes,
        )
        claimed_docs[doc_id] = {"_id": doc_id, "_source": claimed_source}
    return claimed_docs


def _repo_id_query(repo_id: str, extra_must: list[dict] | None = None) -> dict:
    must_clauses = [
        {
            "bool": {
                "should": [
                    {"term": {"repo_id.keyword": repo_id}},
                    {"term": {"repo_id": repo_id}},
                ],
                "minimum_should_match": 1,
            }
        }
    ]
    if extra_must:
        must_clauses.extend(extra_must)

    return {"bool": {"must": must_clauses}}


def _repo_chunk_query(
    repo_id: str,
    *,
    extra_must: list[dict] | None = None,
    extra_must_not: list[dict] | None = None,
) -> dict:
    query = _repo_id_query(repo_id, extra_must=extra_must)
    if extra_must_not:
        query["bool"]["must_not"] = extra_must_not
    return query


def _validate_single_repo(store: OpenSearchStore, repo_id: str, source: dict) -> tuple[str, list[str], list[str], dict]:
    failed_rules = []
    warning_rules = []

    file_docs_count = store.count_documents(
        collection_name=REPO_FILE_INDEX,
        query=_repo_id_query(repo_id),
    )
    code_file_docs_count = store.count_documents(
        collection_name=REPO_FILE_INDEX,
        query=_repo_id_query(repo_id, extra_must=[{"term": {"is_code_file": True}}]),
    )
    chunk_docs_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_id_query(repo_id),
    )
    chunk_docs_with_strategy_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[{"exists": {"field": "chunk_strategy"}}],
        ),
    )
    function_chunk_docs_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[{"term": {"chunk_strategy": "function"}}],
        ),
    )
    function_window_chunk_docs_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[{"term": {"chunk_strategy": "function_window"}}],
        ),
    )
    line_window_chunk_docs_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[{"term": {"chunk_strategy": "line_window"}}],
        ),
    )

    function_chunks_total = function_chunk_docs_count + function_window_chunk_docs_count
    recognized_chunk_docs_count = function_chunks_total + line_window_chunk_docs_count
    chunk_docs_missing_strategy_count = max(0, chunk_docs_count - chunk_docs_with_strategy_count)
    unknown_strategy_chunk_docs_count = max(
        0,
        chunk_docs_with_strategy_count - recognized_chunk_docs_count,
    )

    line_window_parser_unavailable_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[
                {"term": {"chunk_strategy": "line_window"}},
                {"term": {"fallback_reason": "parser_unavailable"}},
            ],
        ),
    )
    line_window_parser_failed_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[
                {"term": {"chunk_strategy": "line_window"}},
                {"term": {"fallback_reason": "parser_failed"}},
            ],
        ),
    )
    line_window_no_function_nodes_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[
                {"term": {"chunk_strategy": "line_window"}},
                {"term": {"fallback_reason": "no_function_nodes"}},
            ],
        ),
    )

    line_window_missing_fallback_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[{"term": {"chunk_strategy": "line_window"}}],
            extra_must_not=[{"exists": {"field": "fallback_reason"}}],
        ),
    )
    function_chunks_missing_node_type_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[
                {
                    "bool": {
                        "should": [
                            {"term": {"chunk_strategy": "function"}},
                            {"term": {"chunk_strategy": "function_window"}},
                        ],
                        "minimum_should_match": 1,
                    }
                }
            ],
            extra_must_not=[{"exists": {"field": "function_node_type"}}],
        ),
    )
    function_chunks_with_fallback_reason_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[
                {
                    "bool": {
                        "should": [
                            {"term": {"chunk_strategy": "function"}},
                            {"term": {"chunk_strategy": "function_window"}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                {"exists": {"field": "fallback_reason"}},
            ],
        ),
    )
    line_window_chunks_with_node_type_count = store.count_documents(
        collection_name=REPO_CHUNK_INDEX,
        query=_repo_chunk_query(
            repo_id,
            extra_must=[
                {"term": {"chunk_strategy": "line_window"}},
                {"exists": {"field": "function_node_type"}},
            ],
        ),
    )

    extract_status = str(source.get("file_extract_status") or "")
    chunk_status = str(source.get("chunk_status") or "")
    repo_chunk_strategy = str(source.get("chunk_strategy") or "")
    expected_text_files = source.get("file_extract_text_files_count")
    expected_code_files = source.get("file_extract_code_files_count")
    expected_chunk_count = source.get("chunk_total_count")
    expected_function_chunk_count = source.get("chunk_function_chunks_count")
    expected_line_window_chunk_count = source.get("chunk_line_window_chunks_count")

    if extract_status != "extracted":
        failed_rules.append("file_extract_not_extracted")
    if chunk_status != "chunked":
        failed_rules.append("chunk_not_chunked")
    if file_docs_count <= 0:
        failed_rules.append("no_extracted_file_docs")
    if code_file_docs_count <= 0:
        failed_rules.append("no_extracted_code_file_docs")
    if chunk_docs_count <= 0:
        failed_rules.append("no_chunk_docs")
    if chunk_status == "chunked":
        if chunk_docs_missing_strategy_count > 0:
            failed_rules.append("chunk_docs_missing_strategy")
        if unknown_strategy_chunk_docs_count > 0:
            failed_rules.append("chunk_docs_unknown_strategy")
        if line_window_missing_fallback_count > 0:
            failed_rules.append("line_window_missing_fallback_reason")
        if function_chunks_missing_node_type_count > 0:
            failed_rules.append("function_chunks_missing_node_type")
        if function_chunks_with_fallback_reason_count > 0:
            failed_rules.append("function_chunks_have_fallback_reason")
        if line_window_chunks_with_node_type_count > 0:
            failed_rules.append("line_window_chunks_have_function_node_type")

    if isinstance(expected_text_files, int) and expected_text_files > 0 and file_docs_count != expected_text_files:
        warning_rules.append("text_file_count_mismatch")
    if isinstance(expected_code_files, int) and expected_code_files > 0 and code_file_docs_count != expected_code_files:
        warning_rules.append("code_file_count_mismatch")
    if isinstance(expected_chunk_count, int) and expected_chunk_count > 0 and chunk_docs_count != expected_chunk_count:
        warning_rules.append("chunk_count_mismatch")
    if (
        isinstance(expected_function_chunk_count, int)
        and expected_function_chunk_count >= 0
        and function_chunks_total != expected_function_chunk_count
    ):
        warning_rules.append("function_chunk_count_mismatch")
    if (
        isinstance(expected_line_window_chunk_count, int)
        and expected_line_window_chunk_count >= 0
        and line_window_chunk_docs_count != expected_line_window_chunk_count
    ):
        warning_rules.append("line_window_chunk_count_mismatch")
    if repo_chunk_strategy and repo_chunk_strategy != "function_first_with_fallback":
        warning_rules.append("unexpected_repo_chunk_strategy")

    status = "validated" if not failed_rules else "validation_failed"
    metrics = {
        "file_extract_status": extract_status,
        "chunk_status": chunk_status,
        "repo_chunk_strategy": repo_chunk_strategy,
        "file_docs_count": file_docs_count,
        "code_file_docs_count": code_file_docs_count,
        "chunk_docs_count": chunk_docs_count,
        "chunk_docs_with_strategy_count": chunk_docs_with_strategy_count,
        "chunk_docs_missing_strategy_count": chunk_docs_missing_strategy_count,
        "chunk_docs_unknown_strategy_count": unknown_strategy_chunk_docs_count,
        "function_chunk_docs_count": function_chunk_docs_count,
        "function_window_chunk_docs_count": function_window_chunk_docs_count,
        "function_chunks_total": function_chunks_total,
        "line_window_chunk_docs_count": line_window_chunk_docs_count,
        "line_window_parser_unavailable_count": line_window_parser_unavailable_count,
        "line_window_parser_failed_count": line_window_parser_failed_count,
        "line_window_no_function_nodes_count": line_window_no_function_nodes_count,
        "line_window_missing_fallback_count": line_window_missing_fallback_count,
        "function_chunks_missing_node_type_count": function_chunks_missing_node_type_count,
        "function_chunks_with_fallback_reason_count": function_chunks_with_fallback_reason_count,
        "line_window_chunks_with_node_type_count": line_window_chunks_with_node_type_count,
        "expected_text_files": expected_text_files,
        "expected_code_files": expected_code_files,
        "expected_chunk_count": expected_chunk_count,
        "expected_function_chunk_count": expected_function_chunk_count,
        "expected_line_window_chunk_count": expected_line_window_chunk_count,
    }
    return status, failed_rules, warning_rules, metrics


def _load_repo_doc_for_validation(store: OpenSearchStore, repo_id: str) -> dict | None:
    repo_doc = store.get_document(collection_name=REPO_REGISTRY_INDEX, doc_id=repo_id)
    if not repo_doc:
        return None
    if not repo_doc.get("_source"):
        return None
    return repo_doc


def run_repo_processing_validation_for_repo(
    repo_id: str,
    *,
    store: OpenSearchStore | None = None,
    refresh_writes: bool = True,
) -> dict:
    if store is None:
        store = OpenSearchStore()

    validation_started_at = datetime.now(timezone.utc).isoformat()
    try:
        repo_doc = _load_repo_doc_for_validation(store, repo_id)
        if repo_doc is None:
            return {
                "repo_id": repo_id,
                "stage": "validation",
                "stage_status": "skipped",
                "reason": "repo_not_found",
            }

        source = dict(repo_doc.get("_source", {}))
        if source.get("chunk_status") not in {"chunked", "chunk_failed"}:
            return {
                "repo_id": repo_id,
                "stage": "validation",
                "stage_status": "skipped",
                "reason": "chunk_not_finished",
            }

        if source.get("validation_status") == "validated" and not _has_newer_chunk_result_than_validation(source):
            return {
                "repo_id": repo_id,
                "stage": "validation",
                "stage_status": "validated",
                "reason": "already_validated",
            }

        if source.get("validation_status") == "validating":
            if _is_stale_validating(source, datetime.now(timezone.utc)):
                logger.warning("Reclaiming stale validation lease for repo_id=%s", repo_id)
            else:
                return {
                    "repo_id": repo_id,
                    "stage": "validation",
                    "stage_status": "skipped",
                    "reason": "already_validating",
                }

        claimed_docs = _claim_repo_docs_for_validation(
            store=store,
            repo_docs=[repo_doc],
            started_at=validation_started_at,
            refresh_writes=refresh_writes,
        )
        claimed_source = dict(claimed_docs[repo_id]["_source"])
        checked_at = datetime.now(timezone.utc).isoformat()
        status, failed_rules, warning_rules, metrics = _validate_single_repo(
            store=store,
            repo_id=repo_id,
            source=claimed_source,
        )

        store.upsert_document(
            collection_name=REPO_VALIDATION_INDEX,
            doc_id=repo_id,
            body={
                "repo_id": repo_id,
                "validation_status": status,
                "failed_rules": failed_rules,
                "warning_rules": warning_rules,
                "metrics": metrics,
                "checked_at": checked_at,
            },
            refresh=refresh_writes,
        )

        updated_source = {
            **claimed_source,
            "validation_status": status,
            "validation_checked_at": checked_at,
            "validation_error_message": None,
            "validation_failed_rules": failed_rules,
            "validation_warning_rules": warning_rules,
            "validation_file_docs_count": metrics["file_docs_count"],
            "validation_code_file_docs_count": metrics["code_file_docs_count"],
            "validation_chunk_docs_count": metrics["chunk_docs_count"],
            "validation_chunk_docs_missing_strategy_count": metrics["chunk_docs_missing_strategy_count"],
            "validation_chunk_docs_unknown_strategy_count": metrics["chunk_docs_unknown_strategy_count"],
            "validation_function_chunks_total": metrics["function_chunks_total"],
            "validation_line_window_chunk_docs_count": metrics["line_window_chunk_docs_count"],
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=repo_id,
            source=updated_source,
            refresh=refresh_writes,
        )

        return {
            "repo_id": repo_id,
            "stage": "validation",
            "stage_status": status,
            "failed_rules": failed_rules,
            "warning_rules": warning_rules,
        }
    except Exception as exc:  # noqa: BLE001 - repo 단위 파이프라인 실패를 상위로 전달하기 위함
        logger.warning("Validation failed for repo_id=%s error=%s", repo_id, str(exc))
        checked_at = datetime.now(timezone.utc).isoformat()
        store.upsert_document(
            collection_name=REPO_VALIDATION_INDEX,
            doc_id=repo_id,
            body={
                "repo_id": repo_id,
                "validation_status": "validation_failed",
                "failed_rules": ["validation_runtime_error"],
                "warning_rules": [],
                "checked_at": checked_at,
                "error_message": str(exc),
            },
            refresh=refresh_writes,
        )
        repo_doc = _load_repo_doc_for_validation(store, repo_id) or {"_source": {}}
        failed_source = {
            **dict(repo_doc.get("_source", {})),
            "validation_status": "validation_failed",
            "validation_checked_at": checked_at,
            "validation_error_message": str(exc),
            "validation_failed_rules": ["validation_runtime_error"],
            "validation_warning_rules": [],
        }
        store.replace_document(
            collection_name=REPO_REGISTRY_INDEX,
            doc_id=repo_id,
            source=failed_source,
            refresh=refresh_writes,
        )
        return {
            "repo_id": repo_id,
            "stage": "validation",
            "stage_status": "validation_failed",
            "failed_rules": ["validation_runtime_error"],
            "error_message": str(exc),
        }


def run_repo_processing_validation_for_shard(
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
    repo_ids = list_repo_ids_for_validation(
        batch_size=batch_size,
        shard_index=shard_index,
        shard_count=resolved_shard_count,
    )

    processed_count = 0
    validated_count = 0
    failed_count = 0
    skipped_count = 0
    for repo_id in repo_ids:
        processed_count += 1
        result = run_repo_processing_validation_for_repo(
            repo_id,
            store=store,
            refresh_writes=False,
        )
        stage_status = str(result.get("stage_status") or "")
        if stage_status == "validated":
            validated_count += 1
        elif stage_status == "validation_failed":
            failed_count += 1
        else:
            skipped_count += 1

    if processed_count > 0:
        store.refresh_index(REPO_VALIDATION_INDEX)
        store.refresh_index(REPO_REGISTRY_INDEX)

    return {
        "stage": "validation",
        "shard_index": shard_index,
        "shard_count": resolved_shard_count,
        "processed_count": processed_count,
        "validated_count": validated_count,
        "failed_count": failed_count,
        "skipped_count": skipped_count,
    }


def run_repo_processing_validation() -> None:
    store = OpenSearchStore()
    validation_started_at = datetime.now(timezone.utc)
    repo_docs = _load_repo_docs_for_validation(store, validation_started_at)
    if not repo_docs:
        return

    claimed_docs = _claim_repo_docs_for_validation(
        store=store,
        repo_docs=repo_docs,
        started_at=validation_started_at.isoformat(),
    )

    for doc_id, hit in claimed_docs.items():
        source = dict(hit.get("_source", {}))
        checked_at = datetime.now(timezone.utc).isoformat()
        try:
            status, failed_rules, warning_rules, metrics = _validate_single_repo(
                store=store,
                repo_id=doc_id,
                source=source,
            )

            store.upsert_document(
                collection_name=REPO_VALIDATION_INDEX,
                doc_id=doc_id,
                body={
                    "repo_id": doc_id,
                    "validation_status": status,
                    "failed_rules": failed_rules,
                    "warning_rules": warning_rules,
                    "metrics": metrics,
                    "checked_at": checked_at,
                },
            )

            updated_source = {
                **source,
                "validation_status": status,
                "validation_checked_at": checked_at,
                "validation_error_message": None,
                "validation_failed_rules": failed_rules,
                "validation_warning_rules": warning_rules,
                "validation_file_docs_count": metrics["file_docs_count"],
                "validation_code_file_docs_count": metrics["code_file_docs_count"],
                "validation_chunk_docs_count": metrics["chunk_docs_count"],
                "validation_chunk_docs_missing_strategy_count": metrics["chunk_docs_missing_strategy_count"],
                "validation_chunk_docs_unknown_strategy_count": metrics["chunk_docs_unknown_strategy_count"],
                "validation_function_chunks_total": metrics["function_chunks_total"],
                "validation_line_window_chunk_docs_count": metrics["line_window_chunk_docs_count"],
            }
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=doc_id,
                source=updated_source,
            )
            claimed_docs[doc_id]["_source"] = updated_source
        except Exception as exc:  # noqa: BLE001 - repo별 실패를 이어서 처리해야 함
            logger.warning("Validation failed for repo_id=%s error=%s", doc_id, str(exc))
            store.upsert_document(
                collection_name=REPO_VALIDATION_INDEX,
                doc_id=doc_id,
                body={
                    "repo_id": doc_id,
                    "validation_status": "validation_failed",
                    "failed_rules": ["validation_runtime_error"],
                    "warning_rules": [],
                    "checked_at": checked_at,
                    "error_message": str(exc),
                },
            )
            failed_source = {
                **source,
                "validation_status": "validation_failed",
                "validation_checked_at": checked_at,
                "validation_error_message": str(exc),
                "validation_failed_rules": ["validation_runtime_error"],
                "validation_warning_rules": [],
            }
            store.replace_document(
                collection_name=REPO_REGISTRY_INDEX,
                doc_id=doc_id,
                source=failed_source,
            )
            claimed_docs[doc_id]["_source"] = failed_source
