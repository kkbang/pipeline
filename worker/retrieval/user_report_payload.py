from collections.abc import Mapping, Sequence
from typing import Any


USER_VISIBLE_RISK_LEVELS = ("critical", "high", "medium")
DEFAULT_MAX_CANDIDATES_PER_SOURCE = 3
DEFAULT_REASON_LIMIT = 3
_RISK_LEVEL_RANK = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
    "same_repo": 4,
}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _compact_dict(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if value not in (None, "", [], {})
    }


def _result_attr(result: Any, name: str, default: Any) -> Any:
    if hasattr(result, name):
        return getattr(result, name)
    if isinstance(result, Mapping):
        return result.get(name, default)
    return default


def _user_visible_reasons(reasons: Sequence[Any], *, limit: int = DEFAULT_REASON_LIMIT) -> list[str]:
    visible_reasons: list[str] = []
    for reason in reasons:
        text = _clean_text(reason)
        if not text:
            continue
        if text.startswith("Hybrid similarity score after scaling:"):
            continue
        visible_reasons.append(text)
        if len(visible_reasons) >= limit:
            break
    return visible_reasons


def _user_candidate_payload(candidate: Mapping[str, Any]) -> dict[str, Any]:
    source_repo = dict(candidate.get("source_repo") or {})
    review = dict(candidate.get("license_review") or {})
    return {
        "repository": _compact_dict(
            {
                "repo_url": _clean_text(source_repo.get("repo_url")),
                "license_spdx": _clean_text(source_repo.get("license_spdx")),
                "license_name": _clean_text(source_repo.get("license_name")),
            }
        ),
        "location": _compact_dict(
            {
                "file_path": _clean_text(candidate.get("file_path")),
                "symbol_name": _clean_text(candidate.get("symbol_name")),
            }
        ),
        "risk": _compact_dict(
            {
                "level": _clean_text(review.get("risk_level")),
                "score": review.get("risk_score"),
            }
        ),
        "matched_by": [str(source_name) for source_name in list(candidate.get("retrieval_sources") or [])],
        "match_signal": _clean_text(review.get("strongest_evidence_type")),
        "why": _user_visible_reasons(list(review.get("reasons") or [])),
    }


def _user_visible_candidates(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(candidate)
        for candidate in candidates
        if _clean_text((candidate.get("license_review") or {}).get("risk_level")) in USER_VISIBLE_RISK_LEVELS
    ]


def _build_limitations(chunk_results: Sequence[Mapping[str, Any]]) -> list[str]:
    total_chunk_count = len(chunk_results)
    if total_chunk_count == 0:
        return []

    knn_skipped = 0
    knn_skip_reasons: set[str] = set()
    for chunk_result in chunk_results:
        result = chunk_result.get("result")
        knn_status = dict(_result_attr(result, "knn_status", {}) or {})
        if _clean_text(knn_status.get("status")) == "ok":
            continue
        knn_skipped += 1
        reason = _clean_text(knn_status.get("reason")) or _clean_text(knn_status.get("status"))
        if reason:
            knn_skip_reasons.add(reason)

    if knn_skipped == 0:
        return []

    if knn_skip_reasons == {"query_embedding_unavailable"}:
        if knn_skipped == total_chunk_count:
            return [
                "Embedding-based kNN retrieval was unavailable for all analyzed source chunks, so findings are based on rule-based matching only."
            ]
        return [
            f"Embedding-based kNN retrieval was unavailable for {knn_skipped} of {total_chunk_count} analyzed source chunks."
        ]

    reason_text = ", ".join(sorted(knn_skip_reasons)) or "unknown"
    return [
        f"Embedding-based kNN retrieval was unavailable for {knn_skipped} of {total_chunk_count} analyzed source chunks ({reason_text})."
    ]


def build_user_facing_hybrid_result_payload(
    result: Any,
    *,
    source_chunk: Mapping[str, Any] | None = None,
    max_candidates: int = DEFAULT_MAX_CANDIDATES_PER_SOURCE,
) -> dict[str, Any]:
    merged_candidates = list(_result_attr(result, "merged_candidates", ()) or ())
    review_candidates = _user_visible_candidates(merged_candidates)
    displayed_candidates = review_candidates[: max(1, int(max_candidates))]
    low_risk_count = max(0, len(merged_candidates) - len(review_candidates))

    review_counts = {"critical": 0, "high": 0, "medium": 0}
    for candidate in review_candidates:
        risk_level = _clean_text((candidate.get("license_review") or {}).get("risk_level"))
        if risk_level in review_counts:
            review_counts[risk_level] += 1

    payload = {
        "source": _compact_dict(
            {
                "file_path": _clean_text((source_chunk or {}).get("file_path")),
                "symbol_name": _clean_text((source_chunk or {}).get("symbol_name")),
            }
        ),
        "summary": _compact_dict(
            {
                "critical": review_counts["critical"],
                "high": review_counts["high"],
                "medium": review_counts["medium"],
                "suppressed_low_risk": low_risk_count,
            }
        ),
        "candidates": [_user_candidate_payload(candidate) for candidate in displayed_candidates],
    }
    additional_review_candidate_count = max(0, len(review_candidates) - len(displayed_candidates))
    if additional_review_candidate_count > 0:
        payload["summary"]["additional_review_candidates"] = additional_review_candidate_count
    return payload


def build_user_facing_repo_hybrid_payload(
    process_result: Mapping[str, Any],
    retrieval_result: Mapping[str, Any],
    *,
    max_candidates_per_source: int = DEFAULT_MAX_CANDIDATES_PER_SOURCE,
) -> dict[str, Any]:
    raw_chunk_results = list(retrieval_result.get("chunk_results") or [])
    findings: list[dict[str, Any]] = []
    review_counts = {"critical": 0, "high": 0, "medium": 0}
    suppressed_low_risk_count = 0

    for chunk_result in raw_chunk_results:
        result = chunk_result.get("result")
        merged_candidates = list(_result_attr(result, "merged_candidates", ()) or ())
        review_candidates = _user_visible_candidates(merged_candidates)
        suppressed_low_risk_count += max(0, len(merged_candidates) - len(review_candidates))
        if not review_candidates:
            continue

        for candidate in review_candidates:
            risk_level = _clean_text((candidate.get("license_review") or {}).get("risk_level"))
            if risk_level in review_counts:
                review_counts[risk_level] += 1

        displayed_candidates = review_candidates[: max(1, int(max_candidates_per_source))]
        top_review = dict((displayed_candidates[0].get("license_review") or {}))
        findings.append(
            {
                "source": _compact_dict(
                    {
                        "file_path": _clean_text(chunk_result.get("file_path")),
                        "symbol_name": _clean_text(chunk_result.get("symbol_name")),
                    }
                ),
                "top_risk": _compact_dict(
                    {
                        "level": _clean_text(top_review.get("risk_level")),
                        "score": top_review.get("risk_score"),
                    }
                ),
                "review_candidate_count": len(review_candidates),
                "additional_review_candidates": max(0, len(review_candidates) - len(displayed_candidates)),
                "candidates": [_user_candidate_payload(candidate) for candidate in displayed_candidates],
            }
        )

    findings.sort(
        key=lambda item: (
            _RISK_LEVEL_RANK.get(_clean_text((item.get("top_risk") or {}).get("level")), 99),
            -float((item.get("top_risk") or {}).get("score") or 0.0),
            -int(item.get("review_candidate_count") or 0),
            _clean_text((item.get("source") or {}).get("file_path")),
            _clean_text((item.get("source") or {}).get("symbol_name")),
        )
    )

    summary = {
        "critical": review_counts["critical"],
        "high": review_counts["high"],
        "medium": review_counts["medium"],
        "sources_with_findings": len(findings),
        "suppressed_low_risk": suppressed_low_risk_count,
    }

    payload = {
        "repo_id": retrieval_result.get("repo_id") or process_result.get("repo_id"),
        "repo_url": process_result.get("canonical_repo_url"),
        "analyzed_source_count": retrieval_result.get("source_chunk_count") or len(raw_chunk_results),
        "summary": summary,
        "findings": findings,
    }
    limitations = _build_limitations(raw_chunk_results)
    if limitations:
        payload["limitations"] = limitations
    return payload
