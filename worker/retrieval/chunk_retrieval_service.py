import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping

from worker.repo.code_chunk_document import CODE_CHUNK_INDEX_ALIAS, VALID_CHUNK_VALIDATION_STATUSES
from worker.retrieval.query_expansion_service import QueryBundle, QueryVariant, build_query_bundle

try:
    from worker.storage.opensearch_store import OpenSearchStore
except ModuleNotFoundError:  # pragma: no cover - optional in script/http-only envs
    OpenSearchStore = Any  # type: ignore[misc,assignment]


DEFAULT_RETRIEVAL_VERSION = "retrieval_v1"
DEFAULT_PER_VARIANT_K = 10
DEFAULT_TOP_K = 20
DEFAULT_INCLUDE_SAME_REPO = False
DEFAULT_INCLUDE_LOW_CONFIDENCE = False
MAX_EVIDENCE_PER_CANDIDATE = 8
REPO_REGISTRY_INDEX = "repo_registry_index"
GENERIC_CALL_TOKENS = {"this.setdata", "setdata"}
GENERIC_IDENTIFIER_TERMS = {
    "on",
    "handler",
    "this",
    "data",
    "event",
    "option",
    "value",
    "currenttarget",
    "dataset",
    "properties",
    "popup",
    "close",
    "open",
    "x",
}
INTERACTION_DOMAIN_KEYWORDS = {
    "touch",
    "touches",
    "changed",
    "target",
    "touchstart",
    "touchmove",
    "touchend",
    "swipe",
    "slide",
    "gesture",
    "drag",
    "threshold",
    "page",
    "pagex",
    "pagey",
    "client",
    "changedtouches",
    "targettouches",
    "clientx",
    "clienty",
    "start",
    "end",
    "move",
    "left",
    "right",
    "open",
    "close",
}
HIGH_SIGNAL_DOMAIN_TERMS = {
    "touch",
    "touches",
    "touchstart",
    "touchmove",
    "touchend",
    "swipe",
    "slide",
    "gesture",
    "drag",
    "threshold",
    "page",
    "pagex",
    "pagey",
    "changedtouches",
    "targettouches",
    "clientx",
    "clienty",
    "start",
    "end",
}
COMPOUND_DOMAIN_KEYWORDS = {
    "touchstart",
    "touchmove",
    "touchend",
    "changedtouches",
    "targettouches",
    "pagex",
    "pagey",
    "clientx",
    "clienty",
}
RETRIEVAL_SEARCH_SOURCE_FIELDS = [
    "chunk_id",
    "repo_id",
    "language",
    "file_path",
    "chunk_type",
    "symbol_name",
    "identifier_tokens",
    "call_tokens",
    "operator_tokens",
    "raw_hash",
    "normalized_hash",
    "anonymized_hash",
]


@dataclass(frozen=True, slots=True)
class RetrievalEvidence:
    variant_id: str
    variant_name: str
    family: str
    raw_score: float
    normalized_score: float
    rank: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "variant_name": self.variant_name,
            "family": self.family,
            "raw_score": self.raw_score,
            "normalized_score": self.normalized_score,
            "rank": self.rank,
        }


@dataclass(frozen=True, slots=True)
class RetrievalCandidate:
    chunk_id: str
    repo_id: str
    language: str
    file_path: str
    chunk_type: str
    symbol_name: str
    aggregate_score: float
    evidence_count: int
    evidences: tuple[RetrievalEvidence, ...]
    source_repo: dict[str, Any]
    license_review: dict[str, Any]
    match_analysis: dict[str, Any]
    source: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "repo_id": self.repo_id,
            "language": self.language,
            "file_path": self.file_path,
            "chunk_type": self.chunk_type,
            "symbol_name": self.symbol_name,
            "aggregate_score": self.aggregate_score,
            "evidence_count": self.evidence_count,
            "evidences": [evidence.as_dict() for evidence in self.evidences],
            "source_repo": dict(self.source_repo),
            "license_review": dict(self.license_review),
            "match_analysis": dict(self.match_analysis),
            "source": dict(self.source),
        }


@dataclass(frozen=True, slots=True)
class RetrievalResult:
    retrieval_version: str
    query_bundle: QueryBundle
    candidate_count: int
    license_review_summary: dict[str, Any]
    candidates: tuple[RetrievalCandidate, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "retrieval_version": self.retrieval_version,
            "query_bundle": self.query_bundle.as_dict(),
            "candidate_count": self.candidate_count,
            "license_review_summary": dict(self.license_review_summary),
            "candidates": [candidate.as_dict() for candidate in self.candidates],
        }


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _variant_name_from_id(variant_id: str) -> str:
    normalized = _clean_text(variant_id)
    if not normalized:
        return ""
    return normalized.rsplit(":", 1)[-1]


def _build_variant_search_body(
    *,
    variant: QueryVariant,
    bundle: QueryBundle,
    per_variant_k: int,
    include_same_repo: bool,
) -> dict[str, Any]:
    filter_clauses: list[dict[str, Any]] = [
        {"terms": {"validation_status": list(VALID_CHUNK_VALIDATION_STATUSES)}},
    ]
    if bundle.language:
        filter_clauses.append({"term": {"language": bundle.language}})

    must_not_clauses: list[dict[str, Any]] = []
    if bundle.source_chunk_id:
        must_not_clauses.append({"term": {"chunk_id": bundle.source_chunk_id}})
    if not include_same_repo and bundle.source_repo_id:
        must_not_clauses.append({"term": {"repo_id": bundle.source_repo_id}})

    query_clause = _build_variant_query_clause(variant)
    body: dict[str, Any] = {
        "size": max(1, int(per_variant_k)),
        "track_total_hits": False,
        "_source": RETRIEVAL_SEARCH_SOURCE_FIELDS,
        "query": {
            "bool": {
                "filter": filter_clauses,
                "must": [query_clause],
            }
        },
    }
    if must_not_clauses:
        body["query"]["bool"]["must_not"] = must_not_clauses
    return body


def _build_variant_query_clause(variant: QueryVariant) -> dict[str, Any]:
    if variant.family == "exact":
        return _build_exact_query_clause(variant)
    if variant.family == "normalized":
        return _build_normalized_query_clause(variant)
    if variant.family == "structural":
        return _build_structural_query_clause(variant)
    raise ValueError(f"unsupported query family: {variant.family}")


def _build_exact_query_clause(variant: QueryVariant) -> dict[str, Any]:
    if variant.query_payload:
        if "raw_hash" in variant.query_payload:
            return {"term": {"raw_hash": variant.query_payload["raw_hash"]}}
    if variant.query_text:
        return {
            "bool": {
                "should": [
                    {"match_phrase": {"raw_code": {"query": variant.query_text, "boost": 4.0, "slop": 0}}},
                    {
                        "match": {
                            "raw_code": {
                                "query": variant.query_text,
                                "operator": "or",
                                "minimum_should_match": "60%",
                                "boost": 1.5,
                            }
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        }
    raise ValueError("exact query variant is missing payload and text")


def _build_normalized_query_clause(variant: QueryVariant) -> dict[str, Any]:
    if variant.query_payload:
        if "normalized_hash" in variant.query_payload:
            return {"term": {"normalized_hash": variant.query_payload["normalized_hash"]}}
        if "anonymized_hash" in variant.query_payload:
            return {"term": {"anonymized_hash": variant.query_payload["anonymized_hash"]}}

    field_name = "normalized_code"
    if "anonymize_identifiers" in variant.normalization_steps:
        field_name = "anonymized_code"
    if variant.query_text:
        return {
            "bool": {
                "should": [
                    {
                        "match_phrase": {
                            field_name: {
                                "query": variant.query_text,
                                "boost": 3.5,
                                "slop": 0,
                            }
                        }
                    },
                    {
                        "match": {
                            field_name: {
                                "query": variant.query_text,
                                "operator": "or",
                                "minimum_should_match": "55%",
                                "boost": 1.5,
                            }
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        }
    raise ValueError("normalized query variant is missing payload and text")


def _build_structural_query_clause(variant: QueryVariant) -> dict[str, Any]:
    payload = variant.query_payload or {}
    should_clauses: list[dict[str, Any]] = []

    chunk_type = _clean_text(payload.get("chunk_type"))
    if chunk_type:
        should_clauses.append({"term": {"chunk_type": {"value": chunk_type, "boost": 1.0}}})

    symbol_type = _clean_text(payload.get("symbol_type"))
    if symbol_type:
        should_clauses.append({"term": {"symbol_type": {"value": symbol_type, "boost": 1.0}}})

    structure_signature = _clean_text(payload.get("structure_signature"))
    if structure_signature:
        should_clauses.append(
            {"term": {"structure_signature": {"value": structure_signature, "boost": 4.0}}}
        )

    should_clauses.extend(_build_keyword_overlap_clauses("control_flow_tags", payload.get("control_flow_tags"), 1.0))
    should_clauses.extend(_build_keyword_overlap_clauses("call_tokens", payload.get("call_tokens"), 2.0))
    should_clauses.extend(_build_keyword_overlap_clauses("identifier_tokens", payload.get("identifier_tokens"), 1.5))
    should_clauses.extend(_build_keyword_overlap_clauses("operator_tokens", payload.get("operator_tokens"), 0.75))
    should_clauses.extend(_build_keyword_overlap_clauses("ast_node_sequence", payload.get("ast_node_sequence"), 0.5))

    if not should_clauses:
        return {"match_none": {}}

    return {
        "bool": {
            "should": should_clauses,
            "minimum_should_match": 1,
        }
    }


def _build_keyword_overlap_clauses(field_name: str, values: object, boost: float) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    clauses: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_value in values:
        value = _clean_text(raw_value)
        if not value or value in seen:
            continue
        seen.add(value)
        clauses.append({"term": {field_name: {"value": value, "boost": boost}}})
    return clauses


def _normalized_variant_score(raw_score: float, max_score: float, rank: int, priority: int) -> float:
    if raw_score <= 0 or max_score <= 0:
        return 0.0
    score_ratio = raw_score / max_score
    rank_ratio = 1.0 / max(1, rank + 1)
    priority_weight = max(1, priority) / 100.0
    return ((score_ratio * 0.7) + (rank_ratio * 0.3)) * priority_weight


def _normalize_token(value: object) -> str:
    return _clean_text(value).lower()


def _split_identifier_terms(value: object) -> set[str]:
    raw = _clean_text(value)
    if not raw:
        return set()
    parts = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+", raw.replace(".", "_").replace("-", "_"))
    terms = {_normalize_token(part) for part in parts if _normalize_token(part)}
    if not terms and raw:
        terms = {_normalize_token(raw)}
    return {term for term in terms if term and term not in GENERIC_IDENTIFIER_TERMS}


def _extract_identifier_term_set(source: Mapping[str, Any]) -> set[str]:
    terms: set[str] = set()
    for value in source.get("identifier_tokens") or []:
        terms.update(_split_identifier_terms(value))
    terms.update(_split_identifier_terms(source.get("symbol_name")))
    return terms


def _extract_call_token_set(source: Mapping[str, Any], *, include_generic: bool) -> set[str]:
    tokens = {
        _normalize_token(value)
        for value in (source.get("call_tokens") or [])
        if _normalize_token(value)
    }
    if include_generic:
        return tokens
    return {token for token in tokens if token not in GENERIC_CALL_TOKENS}


def _extract_operator_token_set(source: Mapping[str, Any]) -> set[str]:
    return {
        _normalize_token(value)
        for value in (source.get("operator_tokens") or [])
        if _normalize_token(value)
    }


def _extract_domain_terms(source: Mapping[str, Any]) -> set[str]:
    terms = _extract_identifier_term_set(source)
    raw_fragments = list(source.get("identifier_tokens") or [])
    raw_fragments.extend(source.get("call_tokens") or [])
    raw_fragments.append(source.get("symbol_name"))

    detected_terms = {term for term in terms if term in INTERACTION_DOMAIN_KEYWORDS}
    for raw_fragment in raw_fragments:
        normalized_fragment = re.sub(r"[^a-z0-9]+", "", _normalize_token(raw_fragment))
        if not normalized_fragment:
            continue
        for keyword in COMPOUND_DOMAIN_KEYWORDS:
            if keyword in normalized_fragment:
                detected_terms.add(keyword)
    return detected_terms


def _build_match_analysis(
    *,
    source_doc: Mapping[str, Any],
    candidate_source: Mapping[str, Any],
    evidences: list[RetrievalEvidence],
    aggregate_score: float,
) -> dict[str, Any]:
    source_calls = _extract_call_token_set(source_doc, include_generic=False)
    candidate_calls = _extract_call_token_set(candidate_source, include_generic=False)
    filtered_call_overlap = source_calls & candidate_calls

    source_all_calls = _extract_call_token_set(source_doc, include_generic=True)
    candidate_all_calls = _extract_call_token_set(candidate_source, include_generic=True)
    boilerplate_call_overlap = (source_all_calls & candidate_all_calls) & GENERIC_CALL_TOKENS

    source_identifier_terms = _extract_identifier_term_set(source_doc)
    candidate_identifier_terms = _extract_identifier_term_set(candidate_source)
    identifier_overlap = source_identifier_terms & candidate_identifier_terms

    source_operator_tokens = _extract_operator_token_set(source_doc)
    candidate_operator_tokens = _extract_operator_token_set(candidate_source)
    operator_overlap = source_operator_tokens & candidate_operator_tokens

    source_domain_terms = _extract_domain_terms(source_doc)
    candidate_domain_terms = _extract_domain_terms(candidate_source)
    domain_alignment_terms = source_domain_terms & candidate_domain_terms
    high_signal_domain_terms = domain_alignment_terms & HIGH_SIGNAL_DOMAIN_TERMS

    support_category_count = sum(
        1
        for overlap_count in (
            len(filtered_call_overlap),
            len(identifier_overlap),
            len(operator_overlap),
        )
        if overlap_count > 0
    )

    strongest_evidence_type, _ = _strongest_evidence_type(evidences)
    domain_bonus = min(len(domain_alignment_terms), 5) * 0.04
    high_signal_domain_bonus = min(len(high_signal_domain_terms), 4) * 0.09
    call_bonus = min(len(filtered_call_overlap), 3) * 0.04
    identifier_bonus = min(len(identifier_overlap), 5) * 0.03
    operator_bonus = min(len(operator_overlap), 6) * 0.04
    boilerplate_penalty = 0.12 if boilerplate_call_overlap and not filtered_call_overlap else 0.0
    anonymized_only_penalty = (
        0.12 if strongest_evidence_type == "anonymized_code_match" and support_category_count < 2 else 0.0
    )
    weak_semantic_penalty = 0.14 if strongest_evidence_type == "anonymized_code_match" and not domain_alignment_terms else 0.0
    low_signal_penalty = (
        0.08
        if not domain_alignment_terms and not filtered_call_overlap and len(identifier_overlap) <= 1
        else 0.0
    )

    ranking_score = aggregate_score + domain_bonus + high_signal_domain_bonus + call_bonus
    ranking_score += identifier_bonus + operator_bonus
    ranking_score -= boilerplate_penalty + anonymized_only_penalty + weak_semantic_penalty + low_signal_penalty

    return {
        "ranking_score": round(ranking_score, 6),
        "support_category_count": support_category_count,
        "call_token_overlap_count": len(filtered_call_overlap),
        "identifier_term_overlap_count": len(identifier_overlap),
        "operator_token_overlap_count": len(operator_overlap),
        "boilerplate_call_overlap_count": len(boilerplate_call_overlap),
        "domain_alignment_terms": sorted(domain_alignment_terms),
        "high_signal_domain_terms": sorted(high_signal_domain_terms),
        "filtered_call_overlap": sorted(filtered_call_overlap),
        "identifier_term_overlap": sorted(identifier_overlap),
        "operator_token_overlap": sorted(operator_overlap),
    }


def _build_cluster_key(candidate_source: Mapping[str, Any]) -> str:
    for field_name in ("raw_hash", "normalized_hash", "anonymized_hash"):
        field_value = _clean_text(candidate_source.get(field_name))
        if field_value:
            return f"{field_name}:{field_value}"
    return f"chunk:{_clean_text(candidate_source.get('chunk_id'))}"


def _strongest_evidence_type(evidences: list[RetrievalEvidence]) -> tuple[str, float]:
    ranked_types = {
        ("exact", "raw_hash"): ("raw_hash_match", 0.99),
        ("normalized", "normalized_hash"): ("normalized_hash_match", 0.95),
        ("normalized", "anonymized_hash"): ("anonymized_hash_match", 0.92),
        ("exact", "raw_code"): ("raw_code_match", 0.56),
        ("normalized", "normalized_code"): ("normalized_code_match", 0.6),
        ("normalized", "anonymized_code"): ("anonymized_code_match", 0.58),
        ("structural", "summary"): ("structural_similarity", 0.22),
    }
    strongest_type = "unknown"
    strongest_score = 0.0
    for evidence in evidences:
        evidence_type, base_score = ranked_types.get(
            (evidence.family, evidence.variant_name),
            ("unknown", 0.25),
        )
        if base_score > strongest_score:
            strongest_type = evidence_type
            strongest_score = base_score
    return strongest_type, strongest_score


def _build_source_repo_summary(candidate_source: Mapping[str, Any], repo_doc: Mapping[str, Any] | None) -> dict[str, Any]:
    repo_source = dict((repo_doc or {}).get("_source", {})) if isinstance(repo_doc, dict) else {}
    return {
        "repo_id": _clean_text(candidate_source.get("repo_id")),
        "repo_url": _clean_text(candidate_source.get("repo_url")) or _clean_text(repo_source.get("canonical_repo_url")),
        "owner": _clean_text(candidate_source.get("owner")) or _clean_text(repo_source.get("owner")),
        "repo_name": _clean_text(candidate_source.get("repo_name")) or _clean_text(repo_source.get("repo_name")),
        "license_spdx": _clean_text(repo_source.get("repo_license_spdx")),
        "license_name": _clean_text(repo_source.get("repo_license_name")),
        "visibility": _clean_text(repo_source.get("repo_visibility")),
        "is_fork": bool(repo_source.get("repo_is_fork")),
    }


def _build_license_review(
    *,
    candidate_source: Mapping[str, Any],
    source_repo_id: str,
    aggregate_score: float,
    evidences: list[RetrievalEvidence],
    repo_doc: Mapping[str, Any] | None,
    match_analysis: Mapping[str, Any],
) -> dict[str, Any]:
    strongest_evidence_type, base_score = _strongest_evidence_type(evidences)
    same_repo = _clean_text(candidate_source.get("repo_id")) == source_repo_id and bool(source_repo_id)
    evidence_bonus = min(len(evidences), 4) * 0.03
    aggregate_bonus = min(max(aggregate_score, 0.0), 1.0) * 0.1
    risk_score = min(1.0, base_score + evidence_bonus + aggregate_bonus)
    reasons: list[str] = []
    lexical_evidence_types = {"raw_code_match", "normalized_code_match", "anonymized_code_match"}

    if same_repo:
        return {
            "risk_score": 0.0,
            "risk_level": "same_repo",
            "strongest_evidence_type": strongest_evidence_type,
            "same_repo": True,
            "reasons": ["Matched chunk is from the same repository as the query source."],
        }

    if strongest_evidence_type == "raw_hash_match":
        reasons.append("Exact raw hash match indicates near-certain copied code.")
    elif strongest_evidence_type in {"normalized_hash_match", "anonymized_hash_match"}:
        reasons.append("Hash-level match survives normalization/anonymization, indicating strong code reuse.")
    elif strongest_evidence_type in lexical_evidence_types:
        reasons.append("Lexical match indicates a high-overlap candidate that should be reviewed.")
    else:
        reasons.append("Structural similarity indicates a weaker reuse candidate that still merits review.")

    repo_source = dict((repo_doc or {}).get("_source", {})) if isinstance(repo_doc, dict) else {}
    license_spdx = _clean_text(repo_source.get("repo_license_spdx"))
    license_name = _clean_text(repo_source.get("repo_license_name"))
    if license_spdx or license_name:
        reasons.append(f"Source repository license metadata: {license_spdx or license_name}.")
    else:
        reasons.append("Source repository license metadata is missing, so manual review is required.")

    if strongest_evidence_type == "anonymized_code_match" and match_analysis["support_category_count"] < 2:
        risk_score = min(risk_score, 0.55)
        reasons.append(
            "Anonymized-code-only match lacks enough call/operator/identifier overlap, so confidence is capped."
        )

    if strongest_evidence_type == "structural_similarity":
        risk_score = min(risk_score, 0.35)

    domain_alignment_terms = list(match_analysis.get("domain_alignment_terms") or [])
    high_signal_domain_terms = list(match_analysis.get("high_signal_domain_terms") or [])
    call_overlap_count = int(match_analysis.get("call_token_overlap_count") or 0)
    operator_overlap_count = int(match_analysis.get("operator_token_overlap_count") or 0)
    identifier_overlap_count = int(match_analysis.get("identifier_term_overlap_count") or 0)
    ranking_score = float(match_analysis.get("ranking_score") or 0.0)
    medium_support = bool(domain_alignment_terms) or (
        operator_overlap_count >= 4 and identifier_overlap_count >= 3
    )

    if strongest_evidence_type in lexical_evidence_types:
        semantic_bonus = min(len(domain_alignment_terms), 3) * 0.02
        semantic_bonus += min(len(high_signal_domain_terms), 4) * 0.025
        semantic_bonus += min(operator_overlap_count, 6) * 0.008
        semantic_bonus += min(identifier_overlap_count, 4) * 0.004
        semantic_bonus += min(call_overlap_count, 2) * 0.02
        semantic_bonus += min(max(ranking_score - aggregate_score, 0.0), 1.0) * 0.12
        risk_score = min(1.0, risk_score + semantic_bonus)

    if domain_alignment_terms:
        reasons.append(
            "Interaction-domain terms matched: " + ", ".join(domain_alignment_terms) + "."
        )
    elif strongest_evidence_type in lexical_evidence_types:
        reasons.append("No interaction-domain match was found, so lexical similarity alone is treated conservatively.")

    if (
        strongest_evidence_type in lexical_evidence_types
        and not medium_support
    ):
        risk_score = min(risk_score, 0.55)
        reasons.append(
            "Candidate lacks enough domain overlap or combined operator/identifier overlap for medium risk."
        )

    if strongest_evidence_type in lexical_evidence_types and call_overlap_count == 0:
        no_call_cap = 0.68
        no_call_cap += min(len(high_signal_domain_terms), 4) * 0.015
        no_call_cap += min(operator_overlap_count, 6) * 0.005
        no_call_cap += min(identifier_overlap_count, 4) * 0.003
        risk_score = min(risk_score, min(no_call_cap, 0.79))
        reasons.append("No direct call-token overlap was found, so risk is capped below high.")

    if risk_score >= 0.95:
        risk_level = "critical"
    elif risk_score >= 0.8:
        risk_level = "high"
    elif risk_score >= 0.6:
        risk_level = "medium"
    else:
        risk_level = "low"

    return {
        "risk_score": round(risk_score, 4),
        "risk_level": risk_level,
        "strongest_evidence_type": strongest_evidence_type,
        "same_repo": False,
        "reasons": reasons,
    }


def _should_keep_candidate(
    *,
    candidate_source: Mapping[str, Any],
    license_review: Mapping[str, Any],
    match_analysis: Mapping[str, Any],
    include_same_repo: bool,
    include_low_confidence: bool,
) -> bool:
    if not include_same_repo and bool(license_review.get("same_repo")):
        return False

    risk_level = _clean_text(license_review.get("risk_level"))
    evidence_type = _clean_text(license_review.get("strongest_evidence_type"))
    if not include_low_confidence:
        if risk_level in {"low", "same_repo"}:
            return False
        if evidence_type == "structural_similarity":
            return False
        if (
            evidence_type == "anonymized_code_match"
            and int(match_analysis.get("support_category_count") or 0) < 2
        ):
            return False
        if (
            int(match_analysis.get("boilerplate_call_overlap_count") or 0) > 0
            and int(match_analysis.get("call_token_overlap_count") or 0) == 0
            and int(match_analysis.get("identifier_term_overlap_count") or 0) == 0
            and not list(match_analysis.get("domain_alignment_terms") or [])
        ):
            return False

    file_path = _clean_text(candidate_source.get("file_path")).lower()
    symbol_name = _clean_text(candidate_source.get("symbol_name"))
    if not include_low_confidence and (
        ".min.js" in file_path
        or file_path.endswith(".min.js")
        or (len(symbol_name) <= 1 and "javascript" == _clean_text(candidate_source.get("language")).lower())
    ):
        return False

    return True


def _summarize_license_review(candidates: list[RetrievalCandidate]) -> dict[str, Any]:
    counts = {
        "critical": 0,
        "high": 0,
        "medium": 0,
        "low": 0,
        "same_repo": 0,
    }
    for candidate in candidates:
        level = _clean_text(candidate.license_review.get("risk_level"))
        if level in counts:
            counts[level] += 1
    return counts


def _multi_search_documents(
    *,
    store: OpenSearchStore,
    collection_name: str,
    bodies: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not bodies:
        return []

    multi_search = getattr(store, "multi_search_documents", None)
    if callable(multi_search):
        responses = [dict(response or {}) for response in multi_search(collection_name, bodies)]
        if len(responses) < len(bodies):
            responses.extend({} for _ in range(len(bodies) - len(responses)))
        return responses[: len(bodies)]

    return [
        dict(
            store.search_documents(
                collection_name=collection_name,
                body=body,
            )
            or {}
        )
        for body in bodies
    ]


def _multi_get_documents(
    *,
    store: OpenSearchStore,
    collection_name: str,
    doc_ids: list[str],
) -> dict[str, dict[str, Any] | None]:
    unique_doc_ids = [doc_id for doc_id in dict.fromkeys(doc_ids) if _clean_text(doc_id)]
    if not unique_doc_ids:
        return {}

    multi_get = getattr(store, "multi_get_documents", None)
    if callable(multi_get):
        response = multi_get(collection_name, unique_doc_ids) or {}
        return {
            _clean_text(doc_id): (doc if isinstance(doc, dict) else None)
            for doc_id, doc in dict(response).items()
            if _clean_text(doc_id)
        }

    docs: dict[str, dict[str, Any] | None] = {}
    for doc_id in unique_doc_ids:
        doc = store.get_document(collection_name, doc_id)
        docs[doc_id] = doc if isinstance(doc, dict) else None
    return docs


def retrieve_similar_chunks(
    source_doc: Mapping[str, Any],
    *,
    store: OpenSearchStore | None = None,
    top_k: int = DEFAULT_TOP_K,
    per_variant_k: int = DEFAULT_PER_VARIANT_K,
    include_same_repo: bool = DEFAULT_INCLUDE_SAME_REPO,
    include_low_confidence: bool = DEFAULT_INCLUDE_LOW_CONFIDENCE,
) -> RetrievalResult:
    resolved_store = store or OpenSearchStore()
    bundle = build_query_bundle(source_doc)
    merged_candidates: dict[str, dict[str, Any]] = {}
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

    for variant, response in zip(bundle.variants, responses):
        hits = response.get("hits", {}).get("hits", [])
        max_score = max((float(hit.get("_score") or 0.0) for hit in hits), default=0.0)

        for rank, hit in enumerate(hits):
            candidate_source = dict(hit.get("_source", {}))
            candidate_chunk_id = _clean_text(hit.get("_id")) or _clean_text(candidate_source.get("chunk_id"))
            if not candidate_chunk_id:
                continue
            if bundle.source_chunk_id and candidate_chunk_id == bundle.source_chunk_id:
                continue

            raw_score = float(hit.get("_score") or 0.0)
            normalized_score = _normalized_variant_score(raw_score, max_score, rank, variant.priority)
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
            candidate_entry = merged_candidates.setdefault(
                candidate_chunk_id,
                {
                    "source": candidate_source,
                    "aggregate_score": 0.0,
                    "evidences": [],
                },
            )
            candidate_entry["aggregate_score"] += normalized_score
            if len(candidate_entry["evidences"]) < MAX_EVIDENCE_PER_CANDIDATE:
                candidate_entry["evidences"].append(evidence)

    ranked_candidates: list[RetrievalCandidate] = []
    for chunk_id, payload in merged_candidates.items():
        candidate_source = dict(payload["source"])
        evidences = sorted(
            payload["evidences"],
            key=lambda item: (-item.normalized_score, item.rank, item.variant_id),
        )
        aggregate_score = float(payload["aggregate_score"])
        match_analysis = _build_match_analysis(
            source_doc=source_doc,
            candidate_source=candidate_source,
            evidences=evidences,
            aggregate_score=aggregate_score,
        )
        source_repo = _build_source_repo_summary(candidate_source, None)
        license_review = _build_license_review(
            candidate_source=candidate_source,
            source_repo_id=bundle.source_repo_id,
            aggregate_score=aggregate_score,
            evidences=evidences,
            repo_doc=None,
            match_analysis=match_analysis,
        )
        if not _should_keep_candidate(
            candidate_source=candidate_source,
            license_review=license_review,
            match_analysis=match_analysis,
            include_same_repo=include_same_repo,
            include_low_confidence=include_low_confidence,
        ):
            continue
        ranked_candidates.append(
            RetrievalCandidate(
                chunk_id=chunk_id,
                repo_id=_clean_text(candidate_source.get("repo_id")),
                language=_clean_text(candidate_source.get("language")).lower(),
                file_path=_clean_text(candidate_source.get("file_path")),
                chunk_type=_clean_text(candidate_source.get("chunk_type")),
                symbol_name=_clean_text(candidate_source.get("symbol_name")),
                aggregate_score=aggregate_score,
                evidence_count=len(evidences),
                evidences=tuple(evidences),
                source_repo=source_repo,
                license_review=license_review,
                match_analysis=match_analysis,
                source=candidate_source,
            )
        )

    cluster_sizes = Counter(_build_cluster_key(candidate.source) for candidate in ranked_candidates)
    ranked_candidates.sort(
        key=lambda candidate: (
            candidate.license_review.get("risk_level") != "critical",
            candidate.license_review.get("risk_level") != "high",
            candidate.license_review.get("risk_level") != "medium",
            -float(candidate.match_analysis.get("ranking_score") or 0.0),
            -candidate.evidence_count,
            candidate.chunk_id,
        )
    )

    deduped_candidates: list[RetrievalCandidate] = []
    seen_cluster_keys: set[str] = set()
    for candidate in ranked_candidates:
        cluster_key = _build_cluster_key(candidate.source)
        if cluster_key in seen_cluster_keys:
            continue
        seen_cluster_keys.add(cluster_key)
        match_analysis = dict(candidate.match_analysis)
        match_analysis["cluster_key"] = cluster_key
        match_analysis["cluster_size"] = int(cluster_sizes.get(cluster_key, 1))
        deduped_candidates.append(
            RetrievalCandidate(
                chunk_id=candidate.chunk_id,
                repo_id=candidate.repo_id,
                language=candidate.language,
                file_path=candidate.file_path,
                chunk_type=candidate.chunk_type,
                symbol_name=candidate.symbol_name,
                aggregate_score=candidate.aggregate_score,
                evidence_count=candidate.evidence_count,
                evidences=candidate.evidences,
                source_repo=candidate.source_repo,
                license_review=candidate.license_review,
                match_analysis=match_analysis,
                source=candidate.source,
            )
        )

    trimmed_candidates = deduped_candidates[: max(1, int(top_k))]
    repo_registry_docs = _multi_get_documents(
        store=resolved_store,
        collection_name=REPO_REGISTRY_INDEX,
        doc_ids=[candidate.repo_id for candidate in trimmed_candidates],
    )
    enriched_candidates: list[RetrievalCandidate] = []
    for candidate in trimmed_candidates:
        repo_doc = repo_registry_docs.get(candidate.repo_id)
        source_repo = _build_source_repo_summary(candidate.source, repo_doc)
        license_review = _build_license_review(
            candidate_source=candidate.source,
            source_repo_id=bundle.source_repo_id,
            aggregate_score=candidate.aggregate_score,
            evidences=list(candidate.evidences),
            repo_doc=repo_doc,
            match_analysis=candidate.match_analysis,
        )
        enriched_candidates.append(
            RetrievalCandidate(
                chunk_id=candidate.chunk_id,
                repo_id=candidate.repo_id,
                language=candidate.language,
                file_path=candidate.file_path,
                chunk_type=candidate.chunk_type,
                symbol_name=candidate.symbol_name,
                aggregate_score=candidate.aggregate_score,
                evidence_count=candidate.evidence_count,
                evidences=candidate.evidences,
                source_repo=source_repo,
                license_review=license_review,
                match_analysis=candidate.match_analysis,
                source=candidate.source,
            )
        )

    final_candidates = tuple(enriched_candidates)
    return RetrievalResult(
        retrieval_version=DEFAULT_RETRIEVAL_VERSION,
        query_bundle=bundle,
        candidate_count=len(final_candidates),
        license_review_summary=_summarize_license_review(list(final_candidates)),
        candidates=final_candidates,
    )


def retrieve_similar_chunks_by_chunk_id(
    chunk_id: str,
    *,
    store: OpenSearchStore | None = None,
    top_k: int = DEFAULT_TOP_K,
    per_variant_k: int = DEFAULT_PER_VARIANT_K,
    include_same_repo: bool = DEFAULT_INCLUDE_SAME_REPO,
    include_low_confidence: bool = DEFAULT_INCLUDE_LOW_CONFIDENCE,
) -> RetrievalResult:
    resolved_store = store or OpenSearchStore()
    source_doc = resolved_store.get_document(CODE_CHUNK_INDEX_ALIAS, chunk_id)
    source = source_doc.get("_source") if isinstance(source_doc, dict) else None
    if not isinstance(source, dict) or not source:
        raise KeyError(f"Chunk '{chunk_id}' was not found in '{CODE_CHUNK_INDEX_ALIAS}'")
    source_with_id = {
        "chunk_id": chunk_id,
        **source,
    }
    return retrieve_similar_chunks(
        source_with_id,
        store=resolved_store,
        top_k=top_k,
        per_variant_k=per_variant_k,
        include_same_repo=include_same_repo,
        include_low_confidence=include_low_confidence,
    )
