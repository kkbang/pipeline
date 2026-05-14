from worker.retrieval.query_expansion_service import (
    DEFAULT_QUERY_EXPANSION_VERSION,
    QueryBundle,
    QueryVariant,
    build_query_bundle,
)

try:
    from worker.retrieval.chunk_retrieval_service import (
        DEFAULT_RETRIEVAL_VERSION,
        RetrievalCandidate,
        RetrievalEvidence,
        RetrievalResult,
        retrieve_similar_chunks,
        retrieve_similar_chunks_by_chunk_id,
    )
except ModuleNotFoundError:  # pragma: no cover - optional dependency for lighter test envs
    DEFAULT_RETRIEVAL_VERSION = "retrieval_v1"

try:
    from worker.retrieval.hybrid_chunk_retrieval_service import (
        DEFAULT_HYBRID_RETRIEVAL_VERSION,
        HybridRetrievalResult,
        find_repo_chunk,
        list_repo_chunk_sources,
        retrieve_hybrid_candidates,
        retrieve_hybrid_candidates_by_chunk_id,
        retrieve_hybrid_candidates_for_repo,
        retrieve_hybrid_candidates_for_source_chunks,
    )
except ModuleNotFoundError:  # pragma: no cover - optional dependency for lighter test envs
    DEFAULT_HYBRID_RETRIEVAL_VERSION = "hybrid_retrieval_v1"

__all__ = [
    "DEFAULT_RETRIEVAL_VERSION",
    "DEFAULT_HYBRID_RETRIEVAL_VERSION",
    "DEFAULT_QUERY_EXPANSION_VERSION",
    "QueryBundle",
    "QueryVariant",
    "build_query_bundle",
]

if "retrieve_similar_chunks" in globals():
    __all__.extend(
        [
            "RetrievalCandidate",
            "RetrievalEvidence",
            "RetrievalResult",
            "retrieve_similar_chunks",
            "retrieve_similar_chunks_by_chunk_id",
        ]
    )

if "retrieve_hybrid_candidates" in globals():
    __all__.extend(
        [
            "HybridRetrievalResult",
            "find_repo_chunk",
            "list_repo_chunk_sources",
            "retrieve_hybrid_candidates",
            "retrieve_hybrid_candidates_by_chunk_id",
            "retrieve_hybrid_candidates_for_repo",
            "retrieve_hybrid_candidates_for_source_chunks",
        ]
    )
