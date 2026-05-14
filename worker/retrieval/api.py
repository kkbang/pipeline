import logging
from typing import Any

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel, Field
except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency guard
    missing_name = getattr(exc, "name", "") or "dependency"
    raise RuntimeError(
        "FastAPI API requires additional runtime dependencies. "
        f"Missing module: {missing_name}. "
        "Install with: python3 -m pip install fastapi uvicorn"
    ) from exc

from worker.repo.local_query_repo_service import prepare_local_query_repo
from worker.retrieval.hybrid_chunk_retrieval_service import retrieve_hybrid_candidates_for_source_chunks
from worker.storage.opensearch_store import OpenSearchStore


logger = logging.getLogger(__name__)
CODE_CHUNK_INDEX = "code_chunk_index"


app = FastAPI(
    title="Hybrid Code Chunk Retrieval API",
    version="1.0.0",
    description="Process a direct GitHub repository URL, then run rule-based + kNN hybrid retrieval for its chunks.",
)


class HybridRepoRetrieveRequest(BaseModel):
    repo_url: str = Field(..., min_length=1, description="GitHub repository URL.")
    source_chunk_limit: int = Field(
        default=20,
        ge=1,
        le=500,
        description="Maximum number of source chunks from the query repo to run retrieval for.",
    )
    rule_based_top_k: int = Field(default=50, ge=1, le=200)
    per_variant_k: int = Field(default=20, ge=1, le=100)
    knn_top_k: int = Field(default=50, ge=1, le=200)
    merged_top_k: int = Field(default=100, ge=1, le=500)
    include_same_repo: bool = Field(default=False)
    skip_validation: bool = Field(default=False)


def _exception_detail(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return f"{type(exc).__name__}: {message}"
    return type(exc).__name__


def _compact_repo_processing(process_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "repo_id": process_result.get("repo_id"),
        "canonical_repo_url": process_result.get("canonical_repo_url"),
        "source_chunk_count": process_result.get("source_chunk_count"),
        "local_snapshot_cleanup": process_result.get("local_snapshot_cleanup"),
    }


def _compact_chunk_for_rerank(chunk: dict[str, Any]) -> dict[str, Any]:
    return {
        "chunk_id": chunk.get("chunk_id"),
        "repo_id": chunk.get("repo_id"),
        "language": chunk.get("language"),
        "file_path": chunk.get("file_path"),
        "chunk_type": chunk.get("chunk_type"),
        "symbol_name": chunk.get("symbol_name"),
        "raw_code": chunk.get("raw_code"),
        "anonymized_code": chunk.get("anonymized_code"),
    }


def _compact_candidate(candidate: dict[str, Any], candidate_source: dict[str, Any] | None = None) -> dict[str, Any]:
    retrieval_sources = candidate.get("retrieval_sources")
    if not retrieval_sources:
        if candidate.get("rule_based") and candidate.get("knn"):
            retrieval_sources = ["rule_based", "knn"]
        elif candidate.get("rule_based"):
            retrieval_sources = ["rule_based"]
        elif candidate.get("knn"):
            retrieval_sources = ["knn"]
    payload = {
        "chunk_id": candidate.get("chunk_id"),
        "repo_id": candidate.get("repo_id"),
        "file_path": candidate.get("file_path"),
        "symbol_name": candidate.get("symbol_name"),
        "retrieval_sources": retrieval_sources,
        "candidate_chunk": _compact_chunk_for_rerank(candidate_source or dict(candidate.get("source") or {})),
    }
    if candidate.get("rule_based"):
        payload["rule_based"] = {
            "rank": (candidate.get("rule_based") or {}).get("rank"),
            "aggregate_score": (candidate.get("rule_based") or {}).get("aggregate_score"),
            "evidence_count": (candidate.get("rule_based") or {}).get("evidence_count"),
            "strongest_evidence_type": (candidate.get("rule_based") or {}).get("strongest_evidence_type"),
            "match_analysis": {
                "ranking_score": ((candidate.get("rule_based") or {}).get("match_analysis") or {}).get("ranking_score"),
                "call_token_overlap_count": ((candidate.get("rule_based") or {}).get("match_analysis") or {}).get("call_token_overlap_count"),
                "identifier_term_overlap_count": ((candidate.get("rule_based") or {}).get("match_analysis") or {}).get("identifier_term_overlap_count"),
                "operator_token_overlap_count": ((candidate.get("rule_based") or {}).get("match_analysis") or {}).get("operator_token_overlap_count"),
                "domain_alignment_terms": ((candidate.get("rule_based") or {}).get("match_analysis") or {}).get("domain_alignment_terms"),
            },
        }
    if candidate.get("knn"):
        payload["knn"] = {
            "rank": (candidate.get("knn") or {}).get("rank"),
            "score": (candidate.get("knn") or {}).get("score"),
            "match_analysis": {
                "ranking_score": ((candidate.get("knn") or {}).get("match_analysis") or {}).get("ranking_score"),
                "call_token_overlap_count": ((candidate.get("knn") or {}).get("match_analysis") or {}).get("call_token_overlap_count"),
                "identifier_term_overlap_count": ((candidate.get("knn") or {}).get("match_analysis") or {}).get("identifier_term_overlap_count"),
                "operator_token_overlap_count": ((candidate.get("knn") or {}).get("match_analysis") or {}).get("operator_token_overlap_count"),
                "domain_alignment_terms": ((candidate.get("knn") or {}).get("match_analysis") or {}).get("domain_alignment_terms"),
            },
        }
    return payload


def _build_candidate_source_lookup(store: OpenSearchStore, retrieval_result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    chunk_ids: list[str] = []
    for chunk_result in retrieval_result.get("chunk_results") or []:
        result = chunk_result.get("result")
        merged_candidates = getattr(result, "merged_candidates", ())
        for candidate in merged_candidates:
            chunk_id = str(candidate.get("chunk_id") or "").strip()
            if chunk_id:
                chunk_ids.append(chunk_id)

    docs = store.multi_get_documents(CODE_CHUNK_INDEX, chunk_ids)
    return {
        chunk_id: {"chunk_id": chunk_id, **dict((doc or {}).get("_source", {}))}
        for chunk_id, doc in docs.items()
    }


def _compact_repo_hybrid_payload(
    process_result: dict[str, Any],
    retrieval_result: dict[str, Any],
    *,
    candidate_source_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "repo_processing": _compact_repo_processing(process_result),
        "retrieval_version": retrieval_result.get("retrieval_version"),
        "repo_id": retrieval_result.get("repo_id"),
        "source_chunk_count": retrieval_result.get("source_chunk_count"),
        "chunk_results": [
            {
                "source_chunk_id": chunk_result.get("source_chunk_id"),
                "file_path": chunk_result.get("file_path"),
                "symbol_name": chunk_result.get("symbol_name"),
                "source_chunk": _compact_chunk_for_rerank(
                    next(
                        (
                            source_chunk
                            for source_chunk in process_result.get("source_chunks") or []
                            if source_chunk.get("chunk_id") == chunk_result.get("source_chunk_id")
                        ),
                        {},
                    )
                ),
                "merged_candidate_count": len(chunk_result["result"].merged_candidates),
                "merged_candidates": [
                    _compact_candidate(
                        candidate,
                        candidate_source_lookup.get(str(candidate.get("chunk_id") or "").strip()),
                    )
                    for candidate in chunk_result["result"].merged_candidates
                ],
            }
            for chunk_result in (retrieval_result.get("chunk_results") or [])
        ],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/retrieve/hybrid/by-repo-url")
def retrieve_hybrid_by_repo_url(request: HybridRepoRetrieveRequest) -> dict[str, Any]:
    try:
        process_result = prepare_local_query_repo(
            request.repo_url,
            source_chunk_limit=request.source_chunk_limit,
            precompute_embeddings=True,
        )
        store = OpenSearchStore()
        retrieval_result = retrieve_hybrid_candidates_for_source_chunks(
            list(process_result["source_chunks"]),
            store=store,
            rule_based_top_k=request.rule_based_top_k,
            per_variant_k=request.per_variant_k,
            knn_top_k=request.knn_top_k,
            merged_top_k=request.merged_top_k,
            include_same_repo=request.include_same_repo,
        )
        candidate_source_lookup = _build_candidate_source_lookup(store, retrieval_result)
        return _compact_repo_hybrid_payload(
            process_result,
            retrieval_result,
            candidate_source_lookup=candidate_source_lookup,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime integration path
        logger.exception(
            "Hybrid retrieval API failed for repo_url=%s source_chunk_limit=%s",
            request.repo_url,
            request.source_chunk_limit,
        )
        raise HTTPException(status_code=500, detail=_exception_detail(exc)) from exc
