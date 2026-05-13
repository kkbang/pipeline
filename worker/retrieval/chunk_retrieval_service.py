from dataclasses import dataclass
from typing import Any, Mapping

from worker.repo.code_chunk_document import CODE_CHUNK_INDEX_ALIAS, VALID_CHUNK_VALIDATION_STATUSES
from worker.retrieval.query_expansion_service import QueryBundle, QueryVariant, build_query_bundle
from worker.storage.opensearch_store import OpenSearchStore


DEFAULT_RETRIEVAL_VERSION = "retrieval_v1"
DEFAULT_PER_VARIANT_K = 10
DEFAULT_TOP_K = 20
MAX_EVIDENCE_PER_CANDIDATE = 8
REPO_REGISTRY_INDEX = "repo_registry_index"
RETRIEVAL_SOURCE_FIELDS = [
    "chunk_id",
    "repo_id",
    "repo_url",
    "owner",
    "repo_name",
    "language",
    "file_path",
    "chunk_type",
    "start_line",
    "end_line",
    "symbol_type",
    "symbol_name",
    "raw_code",
    "normalized_code",
    "anonymized_code",
    "identifier_tokens",
    "call_tokens",
    "operator_tokens",
    "control_flow_tags",
    "structure_signature",
    "ast_node_sequence",
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
) -> dict[str, Any]:
    filter_clauses: list[dict[str, Any]] = [
        {"terms": {"validation_status": list(VALID_CHUNK_VALIDATION_STATUSES)}},
    ]
    if bundle.language:
        filter_clauses.append({"term": {"language": bundle.language}})

    must_not_clauses: list[dict[str, Any]] = []
    if bundle.source_chunk_id:
        must_not_clauses.append({"term": {"chunk_id": bundle.source_chunk_id}})

    query_clause = _build_variant_query_clause(variant)
    body: dict[str, Any] = {
        "size": max(1, int(per_variant_k)),
        "_source": RETRIEVAL_SOURCE_FIELDS,
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
                    {"match_phrase": {"raw_code": {"query": variant.query_text, "boost": 4.0}}},
                    {"match": {"raw_code": {"query": variant.query_text, "boost": 1.0}}},
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
                    { "match_phrase": {field_name: {"query": variant.query_text, "boost": 3.0}} },
                    { "match": {field_name: {"query": variant.query_text, "boost": 1.0}} },
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


def _strongest_evidence_type(evidences: list[RetrievalEvidence]) -> tuple[str, float]:
    ranked_types = {
        ("exact", "raw_hash"): ("raw_hash_match", 0.99),
        ("normalized", "normalized_hash"): ("normalized_hash_match", 0.95),
        ("normalized", "anonymized_hash"): ("anonymized_hash_match", 0.92),
        ("exact", "raw_code"): ("raw_code_phrase_match", 0.82),
        ("normalized", "normalized_code"): ("normalized_code_match", 0.74),
        ("normalized", "anonymized_code"): ("anonymized_code_match", 0.7),
        ("structural", "structural_summary"): ("structural_similarity", 0.42),
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
) -> dict[str, Any]:
    strongest_evidence_type, base_score = _strongest_evidence_type(evidences)
    same_repo = _clean_text(candidate_source.get("repo_id")) == source_repo_id and bool(source_repo_id)
    evidence_bonus = min(len(evidences), 4) * 0.03
    aggregate_bonus = min(max(aggregate_score, 0.0), 1.0) * 0.1
    risk_score = min(1.0, base_score + evidence_bonus + aggregate_bonus)
    reasons: list[str] = []

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
    elif strongest_evidence_type in {"raw_code_phrase_match", "normalized_code_match", "anonymized_code_match"}:
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


def retrieve_similar_chunks(
    source_doc: Mapping[str, Any],
    *,
    store: OpenSearchStore | None = None,
    top_k: int = DEFAULT_TOP_K,
    per_variant_k: int = DEFAULT_PER_VARIANT_K,
) -> RetrievalResult:
    resolved_store = store or OpenSearchStore()
    bundle = build_query_bundle(source_doc)
    merged_candidates: dict[str, dict[str, Any]] = {}
    repo_registry_cache: dict[str, dict[str, Any] | None] = {}

    for variant in bundle.variants:
        body = _build_variant_search_body(
            variant=variant,
            bundle=bundle,
            per_variant_k=per_variant_k,
        )
        response = resolved_store.search_documents(
            collection_name=CODE_CHUNK_INDEX_ALIAS,
            body=body,
        )
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
        candidate_repo_id = _clean_text(candidate_source.get("repo_id"))
        if candidate_repo_id not in repo_registry_cache:
            repo_doc = resolved_store.get_document(REPO_REGISTRY_INDEX, candidate_repo_id) if candidate_repo_id else {}
            repo_registry_cache[candidate_repo_id] = repo_doc if isinstance(repo_doc, dict) else None
        repo_doc = repo_registry_cache.get(candidate_repo_id)
        source_repo = _build_source_repo_summary(candidate_source, repo_doc)
        license_review = _build_license_review(
            candidate_source=candidate_source,
            source_repo_id=bundle.source_repo_id,
            aggregate_score=float(payload["aggregate_score"]),
            evidences=evidences,
            repo_doc=repo_doc,
        )
        ranked_candidates.append(
            RetrievalCandidate(
                chunk_id=chunk_id,
                repo_id=_clean_text(candidate_source.get("repo_id")),
                language=_clean_text(candidate_source.get("language")).lower(),
                file_path=_clean_text(candidate_source.get("file_path")),
                chunk_type=_clean_text(candidate_source.get("chunk_type")),
                symbol_name=_clean_text(candidate_source.get("symbol_name")),
                aggregate_score=float(payload["aggregate_score"]),
                evidence_count=len(evidences),
                evidences=tuple(evidences),
                source_repo=source_repo,
                license_review=license_review,
                source=candidate_source,
            )
        )

    ranked_candidates.sort(
        key=lambda candidate: (
            candidate.license_review.get("risk_level") != "critical",
            candidate.license_review.get("risk_level") != "high",
            candidate.license_review.get("risk_level") != "medium",
            -candidate.aggregate_score,
            -candidate.evidence_count,
            candidate.chunk_id,
        )
    )

    trimmed_candidates = tuple(ranked_candidates[: max(1, int(top_k))])
    return RetrievalResult(
        retrieval_version=DEFAULT_RETRIEVAL_VERSION,
        query_bundle=bundle,
        candidate_count=len(trimmed_candidates),
        license_review_summary=_summarize_license_review(list(trimmed_candidates)),
        candidates=trimmed_candidates,
    )


def retrieve_similar_chunks_by_chunk_id(
    chunk_id: str,
    *,
    store: OpenSearchStore | None = None,
    top_k: int = DEFAULT_TOP_K,
    per_variant_k: int = DEFAULT_PER_VARIANT_K,
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
    )
