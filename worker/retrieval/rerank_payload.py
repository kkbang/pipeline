from typing import Any, Mapping


def compact_chunk_for_rerank(chunk: Mapping[str, Any]) -> dict[str, Any]:
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


def infer_retrieval_sources(candidate: Mapping[str, Any]) -> list[str]:
    retrieval_sources = candidate.get("retrieval_sources")
    if isinstance(retrieval_sources, list) and retrieval_sources:
        return [str(source_name) for source_name in retrieval_sources]
    if candidate.get("rule_based") and candidate.get("knn"):
        return ["rule_based", "knn"]
    if candidate.get("rule_based"):
        return ["rule_based"]
    if candidate.get("knn"):
        return ["knn"]
    return []


def compact_candidate_for_rerank(
    candidate: Mapping[str, Any],
    *,
    candidate_source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_source = candidate_source or dict(candidate.get("source") or {})
    return {
        "chunk_id": candidate.get("chunk_id"),
        "repo_id": candidate.get("repo_id"),
        "file_path": candidate.get("file_path"),
        "symbol_name": candidate.get("symbol_name"),
        "retrieval_sources": infer_retrieval_sources(candidate),
        "candidate_chunk": compact_chunk_for_rerank(resolved_source),
    }
