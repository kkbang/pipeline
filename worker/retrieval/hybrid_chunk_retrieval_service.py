from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Mapping

from worker.repo.code_chunk_document import CODE_CHUNK_INDEX_ALIAS, VALID_CHUNK_VALIDATION_STATUSES
from worker.repo.code_chunk_embedding_service import get_code_chunk_embedding_client
from worker.retrieval.chunk_retrieval_service import (
    MAX_EVIDENCE_PER_CANDIDATE,
    REPO_REGISTRY_INDEX,
    RETRIEVAL_SEARCH_SOURCE_FIELDS,
    RetrievalEvidence,
    _build_match_analysis,
    _build_source_repo_summary,
    _build_variant_search_body,
    _clean_text,
    _multi_get_documents,
    _multi_search_documents,
    _normalized_variant_score,
    _strongest_evidence_type,
    _variant_name_from_id,
)
from worker.retrieval.query_expansion_service import build_query_bundle

try:
    from worker.storage.opensearch_store import OpenSearchStore
except ModuleNotFoundError:  # pragma: no cover - optional in script/http-only envs
    OpenSearchStore = Any  # type: ignore[misc,assignment]


DEFAULT_HYBRID_RETRIEVAL_VERSION = "hybrid_retrieval_v1"
DEFAULT_RULE_BASED_TOP_K = 50
DEFAULT_RULE_BASED_PER_VARIANT_K = 20
DEFAULT_KNN_TOP_K = 50
DEFAULT_MERGED_TOP_K = 100
HYBRID_SOURCE_FIELDS = [
    "chunk_id",
    "repo_id",
    "language",
    "file_path",
    "chunk_type",
    "symbol_type",
    "symbol_name",
    "raw_code",
    "normalized_code",
    "anonymized_code",
    "raw_hash",
    "normalized_hash",
    "anonymized_hash",
    "call_tokens",
    "identifier_tokens",
    "operator_tokens",
    "control_flow_tags",
    "structure_signature",
    "ast_node_sequence",
    "validation_status",
    "anonymized_embedding",
]


@dataclass(frozen=True, slots=True)
class HybridRetrievalResult:
    retrieval_version: str
    source_chunk_id: str
    source_repo_id: str
    rule_based_status: dict[str, Any]
    knn_status: dict[str, Any]
    rule_based_candidates: tuple[dict[str, Any], ...]
    knn_candidates: tuple[dict[str, Any], ...]
    merged_candidates: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "retrieval_version": self.retrieval_version,
            "source_chunk_id": self.source_chunk_id,
            "source_repo_id": self.source_repo_id,
            "rule_based_status": dict(self.rule_based_status),
            "knn_status": dict(self.knn_status),
            "rule_based_candidate_count": len(self.rule_based_candidates),
            "knn_candidate_count": len(self.knn_candidates),
            "merged_candidate_count": len(self.merged_candidates),
            "rule_based_candidates": [dict(candidate) for candidate in self.rule_based_candidates],
            "knn_candidates": [dict(candidate) for candidate in self.knn_candidates],
            "merged_candidates": [dict(candidate) for candidate in self.merged_candidates],
        }


def _build_knn_search_body(
    *,
    source_doc: Mapping[str, Any],
    query_vector: list[float],
    top_k: int,
    include_same_repo: bool,
) -> dict[str, Any]:
    filter_clauses: list[dict[str, Any]] = [
        {"terms": {"validation_status": list(VALID_CHUNK_VALIDATION_STATUSES)}},
    ]
    language = _clean_text(source_doc.get("language"))
    if language:
        filter_clauses.append({"term": {"language": language}})

    must_not_clauses: list[dict[str, Any]] = []
    source_chunk_id = _clean_text(source_doc.get("chunk_id"))
    if source_chunk_id:
        must_not_clauses.append({"term": {"chunk_id": source_chunk_id}})
    source_repo_id = _clean_text(source_doc.get("repo_id"))
    if not include_same_repo and source_repo_id:
        must_not_clauses.append({"term": {"repo_id": source_repo_id}})

    query: dict[str, Any] = {
        "bool": {
            "filter": filter_clauses,
            "must": [
                {
                    "knn": {
                        "anonymized_embedding": {
                            "vector": query_vector,
                            "k": max(1, int(top_k)),
                        }
                    }
                }
            ],
        }
    }
    if must_not_clauses:
        query["bool"]["must_not"] = must_not_clauses

    return {
        "size": max(1, int(top_k)),
        "track_total_hits": False,
        "_source": RETRIEVAL_SEARCH_SOURCE_FIELDS,
        "query": query,
    }


def _ensure_query_embedding(source_doc: Mapping[str, Any]) -> tuple[dict[str, Any], list[float] | None]:
    source_with_embedding = dict(source_doc)
    existing_embedding = source_with_embedding.get("anonymized_embedding")
    if isinstance(existing_embedding, list) and existing_embedding:
        return source_with_embedding, [float(value) for value in existing_embedding]

    embedding_client = get_code_chunk_embedding_client()
    if not embedding_client.enabled:
        return source_with_embedding, None

    temp_docs = [("__query__", source_with_embedding)]
    embedding_client.enrich_documents(temp_docs)
    resolved_embedding = source_with_embedding.get("anonymized_embedding")
    if not isinstance(resolved_embedding, list) or not resolved_embedding:
        return source_with_embedding, None
    return source_with_embedding, [float(value) for value in resolved_embedding]


def retrieve_rule_based_candidates(
    source_doc: Mapping[str, Any],
    *,
    store: OpenSearchStore | None = None,
    top_k: int = DEFAULT_RULE_BASED_TOP_K,
    per_variant_k: int = DEFAULT_RULE_BASED_PER_VARIANT_K,
    include_same_repo: bool = False,
) -> dict[str, Any]:
    resolved_store = store or OpenSearchStore()
    bundle = build_query_bundle(source_doc)
    variant_bodies = [
        _build_variant_search_body(
            variant=variant,
            bundle=bundle,
            per_variant_k=per_variant_k,
            include_same_repo=include_same_repo,
        )
        for variant in bundle.variants
    ]
    responses = _multi_search_documents(
        store=resolved_store,
        collection_name=CODE_CHUNK_INDEX_ALIAS,
        bodies=variant_bodies,
    )

    merged_candidates: dict[str, dict[str, Any]] = {}
    for variant, response in zip(bundle.variants, responses, strict=True):
        hits = response.get("hits", {}).get("hits", [])
        max_score = max((float(hit.get("_score") or 0.0) for hit in hits), default=0.0)
        for rank, hit in enumerate(hits, start=1):
            candidate_source = dict(hit.get("_source", {}))
            candidate_chunk_id = _clean_text(hit.get("_id")) or _clean_text(candidate_source.get("chunk_id"))
            if not candidate_chunk_id or candidate_chunk_id == bundle.source_chunk_id:
                continue

            raw_score = float(hit.get("_score") or 0.0)
            normalized_score = _normalized_variant_score(raw_score, max_score, rank - 1, variant.priority)
            if normalized_score <= 0:
                continue

            evidence = RetrievalEvidence(
                variant_id=variant.variant_id,
                variant_name=_variant_name_from_id(variant.variant_id),
                family=variant.family,
                raw_score=raw_score,
                normalized_score=normalized_score,
                rank=rank,
            )
            payload = merged_candidates.setdefault(
                candidate_chunk_id,
                {
                    "source": candidate_source,
                    "aggregate_score": 0.0,
                    "evidences": [],
                },
            )
            payload["aggregate_score"] += normalized_score
            if len(payload["evidences"]) < MAX_EVIDENCE_PER_CANDIDATE:
                payload["evidences"].append(evidence)

    ranked_items = sorted(
        merged_candidates.items(),
        key=lambda item: (-float(item[1]["aggregate_score"]), -len(item[1]["evidences"]), item[0]),
    )[: max(1, int(top_k))]
    repo_docs = _multi_get_documents(
        store=resolved_store,
        collection_name=REPO_REGISTRY_INDEX,
        doc_ids=[_clean_text(payload["source"].get("repo_id")) for _chunk_id, payload in ranked_items],
    )

    candidates: list[dict[str, Any]] = []
    for rank, (chunk_id, payload) in enumerate(ranked_items, start=1):
        candidate_source = dict(payload["source"])
        evidences = sorted(
            payload["evidences"],
            key=lambda item: (-item.normalized_score, item.rank, item.variant_id),
        )
        strongest_evidence_type, _ = _strongest_evidence_type(evidences)
        repo_doc = repo_docs.get(_clean_text(candidate_source.get("repo_id")))
        candidates.append(
            {
                "chunk_id": chunk_id,
                "repo_id": _clean_text(candidate_source.get("repo_id")),
                "language": _clean_text(candidate_source.get("language")).lower(),
                "file_path": _clean_text(candidate_source.get("file_path")),
                "chunk_type": _clean_text(candidate_source.get("chunk_type")),
                "symbol_name": _clean_text(candidate_source.get("symbol_name")),
                "source_repo": _build_source_repo_summary(candidate_source, repo_doc),
                "source": candidate_source,
                "rule_based": {
                    "rank": rank,
                    "aggregate_score": round(float(payload["aggregate_score"]), 6),
                    "evidence_count": len(evidences),
                    "strongest_evidence_type": strongest_evidence_type,
                    "evidences": [evidence.as_dict() for evidence in evidences],
                    "match_analysis": _build_match_analysis(
                        source_doc=source_doc,
                        candidate_source=candidate_source,
                        evidences=evidences,
                        aggregate_score=float(payload["aggregate_score"]),
                    ),
                },
            }
        )

    return {
        "status": "ok",
        "source_chunk_id": bundle.source_chunk_id,
        "source_repo_id": bundle.source_repo_id,
        "candidates": candidates,
    }


def retrieve_knn_candidates(
    source_doc: Mapping[str, Any],
    *,
    store: OpenSearchStore | None = None,
    top_k: int = DEFAULT_KNN_TOP_K,
    include_same_repo: bool = False,
) -> dict[str, Any]:
    resolved_store = store or OpenSearchStore()
    source_with_embedding, query_vector = _ensure_query_embedding(source_doc)
    if not query_vector:
        return {
            "status": "skipped",
            "reason": "query_embedding_unavailable",
            "source_chunk_id": _clean_text(source_doc.get("chunk_id")),
            "source_repo_id": _clean_text(source_doc.get("repo_id")),
            "candidates": [],
        }

    response = resolved_store.search_documents(
        CODE_CHUNK_INDEX_ALIAS,
        _build_knn_search_body(
            source_doc=source_with_embedding,
            query_vector=query_vector,
            top_k=top_k,
            include_same_repo=include_same_repo,
        ),
    )
    hits = response.get("hits", {}).get("hits", [])
    repo_docs = _multi_get_documents(
        store=resolved_store,
        collection_name=REPO_REGISTRY_INDEX,
        doc_ids=[_clean_text((hit.get("_source") or {}).get("repo_id")) for hit in hits],
    )

    candidates: list[dict[str, Any]] = []
    for rank, hit in enumerate(hits, start=1):
        candidate_source = dict(hit.get("_source", {}))
        chunk_id = _clean_text(hit.get("_id")) or _clean_text(candidate_source.get("chunk_id"))
        if not chunk_id:
            continue
        raw_score = float(hit.get("_score") or 0.0)
        repo_doc = repo_docs.get(_clean_text(candidate_source.get("repo_id")))
        candidates.append(
            {
                "chunk_id": chunk_id,
                "repo_id": _clean_text(candidate_source.get("repo_id")),
                "language": _clean_text(candidate_source.get("language")).lower(),
                "file_path": _clean_text(candidate_source.get("file_path")),
                "chunk_type": _clean_text(candidate_source.get("chunk_type")),
                "symbol_name": _clean_text(candidate_source.get("symbol_name")),
                "source_repo": _build_source_repo_summary(candidate_source, repo_doc),
                "source": candidate_source,
                "knn": {
                    "rank": rank,
                    "score": round(raw_score, 6),
                    "match_analysis": _build_match_analysis(
                        source_doc=source_with_embedding,
                        candidate_source=candidate_source,
                        evidences=[],
                        aggregate_score=raw_score,
                    ),
                },
            }
        )

    return {
        "status": "ok",
        "source_chunk_id": _clean_text(source_doc.get("chunk_id")),
        "source_repo_id": _clean_text(source_doc.get("repo_id")),
        "candidates": candidates,
    }


def _merge_hybrid_candidates(
    *,
    rule_based_candidates: list[dict[str, Any]],
    knn_candidates: list[dict[str, Any]],
    top_k: int,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    ordered_chunk_ids: list[str] = []

    def _upsert(candidate: dict[str, Any], source_name: str) -> None:
        chunk_id = _clean_text(candidate.get("chunk_id"))
        if not chunk_id:
            return
        payload = merged.get(chunk_id)
        if payload is None:
            payload = {
                "chunk_id": chunk_id,
                "repo_id": _clean_text(candidate.get("repo_id")),
                "language": _clean_text(candidate.get("language")).lower(),
                "file_path": _clean_text(candidate.get("file_path")),
                "chunk_type": _clean_text(candidate.get("chunk_type")),
                "symbol_name": _clean_text(candidate.get("symbol_name")),
                "retrieval_sources": [],
                "source_repo": dict(candidate.get("source_repo") or {}),
                "source": dict(candidate.get("source") or {}),
                "rule_based": None,
                "knn": None,
            }
            merged[chunk_id] = payload
            ordered_chunk_ids.append(chunk_id)

        retrieval_sources = payload["retrieval_sources"]
        if source_name not in retrieval_sources:
            retrieval_sources.append(source_name)
        if source_name == "rule_based":
            payload["rule_based"] = dict(candidate.get("rule_based") or {})
        elif source_name == "knn":
            payload["knn"] = dict(candidate.get("knn") or {})

    for candidate in rule_based_candidates:
        _upsert(candidate, "rule_based")
    for candidate in knn_candidates:
        _upsert(candidate, "knn")

    return [merged[chunk_id] for chunk_id in ordered_chunk_ids[: max(1, int(top_k))]]


def retrieve_hybrid_candidates(
    source_doc: Mapping[str, Any],
    *,
    store: OpenSearchStore | None = None,
    rule_based_top_k: int = DEFAULT_RULE_BASED_TOP_K,
    per_variant_k: int = DEFAULT_RULE_BASED_PER_VARIANT_K,
    knn_top_k: int = DEFAULT_KNN_TOP_K,
    merged_top_k: int = DEFAULT_MERGED_TOP_K,
    include_same_repo: bool = False,
) -> HybridRetrievalResult:
    resolved_store = store or OpenSearchStore()
    with ThreadPoolExecutor(max_workers=2) as executor:
        rule_future = executor.submit(
            retrieve_rule_based_candidates,
            source_doc,
            store=resolved_store,
            top_k=rule_based_top_k,
            per_variant_k=per_variant_k,
            include_same_repo=include_same_repo,
        )
        knn_future = executor.submit(
            retrieve_knn_candidates,
            source_doc,
            store=resolved_store,
            top_k=knn_top_k,
            include_same_repo=include_same_repo,
        )
        rule_based_result = rule_future.result()
        knn_result = knn_future.result()

    rule_based_candidates = list(rule_based_result.get("candidates") or [])
    knn_candidates = list(knn_result.get("candidates") or [])
    merged_candidates = _merge_hybrid_candidates(
        rule_based_candidates=rule_based_candidates,
        knn_candidates=knn_candidates,
        top_k=merged_top_k,
    )
    return HybridRetrievalResult(
        retrieval_version=DEFAULT_HYBRID_RETRIEVAL_VERSION,
        source_chunk_id=_clean_text(source_doc.get("chunk_id")),
        source_repo_id=_clean_text(source_doc.get("repo_id")),
        rule_based_status={
            "status": rule_based_result.get("status"),
            "reason": rule_based_result.get("reason"),
        },
        knn_status={
            "status": knn_result.get("status"),
            "reason": knn_result.get("reason"),
        },
        rule_based_candidates=tuple(rule_based_candidates),
        knn_candidates=tuple(knn_candidates),
        merged_candidates=tuple(merged_candidates),
    )


def retrieve_hybrid_candidates_by_chunk_id(
    chunk_id: str,
    *,
    store: OpenSearchStore | None = None,
    rule_based_top_k: int = DEFAULT_RULE_BASED_TOP_K,
    per_variant_k: int = DEFAULT_RULE_BASED_PER_VARIANT_K,
    knn_top_k: int = DEFAULT_KNN_TOP_K,
    merged_top_k: int = DEFAULT_MERGED_TOP_K,
    include_same_repo: bool = False,
) -> HybridRetrievalResult:
    resolved_store = store or OpenSearchStore()
    source_doc = resolved_store.get_document(CODE_CHUNK_INDEX_ALIAS, chunk_id)
    source = source_doc.get("_source") if isinstance(source_doc, dict) else None
    if not isinstance(source, dict) or not source:
        raise KeyError(f"Chunk '{chunk_id}' was not found in '{CODE_CHUNK_INDEX_ALIAS}'")
    source_with_id = {"chunk_id": chunk_id, **source}
    return retrieve_hybrid_candidates(
        source_with_id,
        store=resolved_store,
        rule_based_top_k=rule_based_top_k,
        per_variant_k=per_variant_k,
        knn_top_k=knn_top_k,
        merged_top_k=merged_top_k,
        include_same_repo=include_same_repo,
    )


def _field_term_query(field_name: str, value: str) -> dict[str, Any]:
    return {
        "bool": {
            "should": [
                {"term": {f"{field_name}.keyword": value}},
                {"term": {field_name: value}},
            ],
            "minimum_should_match": 1,
        }
    }


def list_repo_chunk_sources(
    repo_id: str,
    *,
    store: OpenSearchStore | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    resolved_store = store or OpenSearchStore()
    query = {
        "bool": {
            "must": [
                _field_term_query("repo_id", repo_id),
                {"terms": {"validation_status": list(VALID_CHUNK_VALIDATION_STATUSES)}},
            ]
        }
    }
    source_chunks: list[dict[str, Any]] = []
    for hit in resolved_store.iterate_documents_by_query(
        collection_name=CODE_CHUNK_INDEX_ALIAS,
        query=query,
        size=500,
        sort=[{"_id": "asc"}],
        source_includes=HYBRID_SOURCE_FIELDS,
    ):
        source = dict(hit.get("_source", {}))
        chunk_id = _clean_text(hit.get("_id")) or _clean_text(source.get("chunk_id"))
        if not chunk_id:
            continue
        source_chunks.append({"chunk_id": chunk_id, **source})
        if isinstance(limit, int) and limit > 0 and len(source_chunks) >= limit:
            break
    return source_chunks


def retrieve_hybrid_candidates_for_repo(
    repo_id: str,
    *,
    store: OpenSearchStore | None = None,
    source_chunk_limit: int | None = None,
    rule_based_top_k: int = DEFAULT_RULE_BASED_TOP_K,
    per_variant_k: int = DEFAULT_RULE_BASED_PER_VARIANT_K,
    knn_top_k: int = DEFAULT_KNN_TOP_K,
    merged_top_k: int = DEFAULT_MERGED_TOP_K,
    include_same_repo: bool = False,
) -> dict[str, Any]:
    resolved_store = store or OpenSearchStore()
    source_chunks = list_repo_chunk_sources(
        repo_id,
        store=resolved_store,
        limit=source_chunk_limit,
    )
    chunk_results: list[dict[str, Any]] = []
    for source_chunk in source_chunks:
        hybrid_result = retrieve_hybrid_candidates(
            source_chunk,
            store=resolved_store,
            rule_based_top_k=rule_based_top_k,
            per_variant_k=per_variant_k,
            knn_top_k=knn_top_k,
            merged_top_k=merged_top_k,
            include_same_repo=include_same_repo,
        )
        chunk_results.append(
            {
                "source_chunk_id": source_chunk["chunk_id"],
                "file_path": _clean_text(source_chunk.get("file_path")),
                "symbol_name": _clean_text(source_chunk.get("symbol_name")),
                "result": hybrid_result,
            }
        )

    return {
        "retrieval_version": DEFAULT_HYBRID_RETRIEVAL_VERSION,
        "repo_id": repo_id,
        "source_chunk_count": len(source_chunks),
        "chunk_results": chunk_results,
    }


def find_repo_chunk(
    *,
    repo_id: str,
    file_path: str,
    symbol_name: str = "",
    store: OpenSearchStore | None = None,
) -> dict[str, Any] | None:
    resolved_store = store or OpenSearchStore()
    must_clauses: list[dict[str, Any]] = [
        {
            "bool": {
                "should": [
                    {"term": {"repo_id.keyword": repo_id}},
                    {"term": {"repo_id": repo_id}},
                ],
                "minimum_should_match": 1,
            }
        },
        {
            "bool": {
                "should": [
                    {"term": {"file_path.keyword": file_path}},
                    {"term": {"file_path": file_path}},
                ],
                "minimum_should_match": 1,
            }
        },
        {"terms": {"validation_status": list(VALID_CHUNK_VALIDATION_STATUSES)}},
    ]
    if _clean_text(symbol_name):
        must_clauses.append(
            {
                "bool": {
                    "should": [
                        {"term": {"symbol_name.keyword": symbol_name}},
                        {"term": {"symbol_name": symbol_name}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )

    response = resolved_store.search_documents(
        CODE_CHUNK_INDEX_ALIAS,
        {
            "size": 1,
            "sort": [{"_id": "asc"}],
            "_source": ["chunk_id", "repo_id", "file_path", "symbol_name"],
            "query": {"bool": {"must": must_clauses}},
        },
    )
    hits = response.get("hits", {}).get("hits", [])
    if not hits:
        return None
    hit = hits[0]
    return {
        "chunk_id": _clean_text(hit.get("_id")) or _clean_text((hit.get("_source") or {}).get("chunk_id")),
        "repo_id": _clean_text((hit.get("_source") or {}).get("repo_id")),
        "file_path": _clean_text((hit.get("_source") or {}).get("file_path")),
        "symbol_name": _clean_text((hit.get("_source") or {}).get("symbol_name")),
    }
