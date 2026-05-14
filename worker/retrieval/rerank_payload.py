from typing import Any, Mapping


def compact_chunk_for_rerank(chunk: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "chunk_id": chunk.get("chunk_id"),
        "repo_id": chunk.get("repo_id"),
        "language": chunk.get("language"),
        "file_path": chunk.get("file_path"),
        "chunk_type": chunk.get("chunk_type"),
        "symbol_name": chunk.get("symbol_name"),
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
    payload = {
        "chunk_id": candidate.get("chunk_id"),
        "repo_id": candidate.get("repo_id"),
        "file_path": candidate.get("file_path"),
        "symbol_name": candidate.get("symbol_name"),
        "retrieval_sources": infer_retrieval_sources(candidate),
        "candidate_chunk": compact_chunk_for_rerank(resolved_source),
    }
    source_repo = candidate.get("source_repo")
    if isinstance(source_repo, Mapping):
        payload["candidate_repo"] = {
            "repo_url": source_repo.get("repo_url"),
            "license_spdx": source_repo.get("license_spdx"),
            "license_name": source_repo.get("license_name"),
        }
    hybrid = candidate.get("hybrid")
    if isinstance(hybrid, Mapping):
        payload["hybrid"] = {
            "score": hybrid.get("score"),
            "source_count": hybrid.get("source_count"),
            "rule_based_scaled_score": hybrid.get("rule_based_scaled_score"),
            "knn_scaled_score": hybrid.get("knn_scaled_score"),
        }
    license_review = candidate.get("license_review")
    if isinstance(license_review, Mapping):
        payload["license_review"] = {
            "risk_score": license_review.get("risk_score"),
            "risk_level": license_review.get("risk_level"),
            "strongest_evidence_type": license_review.get("strongest_evidence_type"),
            "license_spdx": license_review.get("license_spdx"),
        }
    return payload
