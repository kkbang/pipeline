from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Mapping

from worker.repo.chunking.code_chunk_document import CODE_CHUNK_INDEX_ALIAS, VALID_CHUNK_VALIDATION_STATUSES
from worker.repo.chunking.code_chunk_embedding_service import get_code_chunk_embedding_client
from worker.common.config import settings
from worker.retrieval.chunk_retrieval_service import (
    MAX_EVIDENCE_PER_CANDIDATE,
    REPO_REGISTRY_INDEX,
    RetrievalEvidence,
    _build_license_review,
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
from worker.retrieval.repo_doc_cache import RepoDocumentCache
from worker.retrieval.query_expansion_service import build_query_bundle
from worker.retrieval.source_chunk_selection import select_source_chunks

try:
    from worker.storage.opensearch_store import OpenSearchStore
except ModuleNotFoundError:  # pragma: no cover - optional in script/http-only envs
    OpenSearchStore = Any  # type: ignore[misc,assignment]


DEFAULT_HYBRID_RETRIEVAL_VERSION = "hybrid_retrieval_v1"
DEFAULT_RULE_BASED_TOP_K = 50
DEFAULT_RULE_BASED_PER_VARIANT_K = 20
DEFAULT_KNN_TOP_K = 50
DEFAULT_MERGED_TOP_K = 100
CODE_CHUNK_EMBEDDING_INDEX = (
    f"{settings.code_chunk_embedding_index_alias}_{settings.code_chunk_embedding_index_version}"
)
STRONG_COPYLEFT_LICENSE_MARKERS = ("AGPL", "GPL", "SSPL", "BUSL", "CPAL")
WEAK_COPYLEFT_LICENSE_MARKERS = ("LGPL", "MPL", "EPL", "CDDL")
NOTICE_LICENSE_MARKERS = ("APACHE", "MIT", "BSD", "ISC", "ARTISTIC", "ZLIB")
PUBLIC_DOMAIN_LICENSE_MARKERS = ("UNLICENSE", "CC0", "0BSD")
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
KNN_EMBEDDING_SOURCE_FIELDS = [
    "chunk_id",
    "repo_id",
    "language",
    "file_path",
    "chunk_type",
    "symbol_name",
]


@dataclass(frozen=True, slots=True)
class HybridRetrievalResult:
    retrieval_version: str
    source_chunk_id: str
    source_repo_id: str
    rule_based_status: dict[str, Any]
    knn_status: dict[str, Any]
    timings: dict[str, Any]
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
            "timings": dict(self.timings),
            "rule_based_candidate_count": len(self.rule_based_candidates),
            "knn_candidate_count": len(self.knn_candidates),
            "merged_candidate_count": len(self.merged_candidates),
            "rule_based_candidates": [dict(candidate) for candidate in self.rule_based_candidates],
            "knn_candidates": [dict(candidate) for candidate in self.knn_candidates],
            "merged_candidates": [dict(candidate) for candidate in self.merged_candidates],
        }


def _build_repo_doc_from_source_repo(source_repo: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "_source": {
            "repo_license_spdx": _clean_text(source_repo.get("license_spdx")),
            "repo_license_name": _clean_text(source_repo.get("license_name")),
        }
    }


def _license_policy_bonus(license_spdx: str) -> tuple[float, str]:
    normalized = _clean_text(license_spdx).upper()
    if not normalized:
        return 0.05, "unknown"
    if any(marker in normalized for marker in STRONG_COPYLEFT_LICENSE_MARKERS):
        return 0.18, "strong_copyleft"
    if any(marker in normalized for marker in WEAK_COPYLEFT_LICENSE_MARKERS):
        return 0.12, "weak_copyleft"
    if any(marker in normalized for marker in NOTICE_LICENSE_MARKERS):
        return 0.08, "notice"
    if any(marker in normalized for marker in PUBLIC_DOMAIN_LICENSE_MARKERS):
        return 0.02, "public_domain"
    return 0.06, "other"


def _select_match_analysis(candidate: Mapping[str, Any]) -> dict[str, Any]:
    rule_based_match = dict((candidate.get("rule_based") or {}).get("match_analysis") or {})
    knn_match = dict((candidate.get("knn") or {}).get("match_analysis") or {})
    if rule_based_match:
        return rule_based_match
    return knn_match


def _select_evidences(candidate: Mapping[str, Any]) -> list[RetrievalEvidence]:
    evidence_payloads = list((candidate.get("rule_based") or {}).get("evidences") or [])
    evidences: list[RetrievalEvidence] = []
    for payload in evidence_payloads:
        if not isinstance(payload, Mapping):
            continue
        evidences.append(
            RetrievalEvidence(
                variant_id=_clean_text(payload.get("variant_id")),
                variant_name=_clean_text(payload.get("variant_name")),
                family=_clean_text(payload.get("family")),
                raw_score=float(payload.get("raw_score") or 0.0),
                normalized_score=float(payload.get("normalized_score") or 0.0),
                rank=int(payload.get("rank") or 0),
            )
        )
    return evidences


def _build_hybrid_license_review(candidate: Mapping[str, Any]) -> dict[str, Any]:
    source_repo = dict(candidate.get("source_repo") or {})
    candidate_source = dict(candidate.get("source") or {})
    match_analysis = _select_match_analysis(candidate)
    evidences = _select_evidences(candidate)
    hybrid = dict(candidate.get("hybrid") or {})
    normalized_similarity = min(max(float(hybrid.get("score") or 0.0) / 2.0, 0.0), 1.0)

    review = _build_license_review(
        candidate_source=candidate_source,
        source_repo_id="",
        aggregate_score=normalized_similarity,
        evidences=evidences,
        repo_doc=_build_repo_doc_from_source_repo(source_repo),
        match_analysis=match_analysis,
    )

    risk_score = float(review.get("risk_score") or 0.0)
    retrieval_sources = list(candidate.get("retrieval_sources") or [])
    strongest_evidence_type = _clean_text(review.get("strongest_evidence_type"))
    is_knn_only = retrieval_sources == ["knn"]
    is_rule_and_knn = set(retrieval_sources) == {"rule_based", "knn"}
    if len(retrieval_sources) >= 2:
        risk_score = min(1.0, risk_score + 0.1)
    elif is_knn_only:
        risk_score = min(risk_score, 0.72)

    support_category_count = int(match_analysis.get("support_category_count") or 0)
    call_overlap_count = int(match_analysis.get("call_token_overlap_count") or 0)
    identifier_overlap_count = int(match_analysis.get("identifier_term_overlap_count") or 0)
    operator_overlap_count = int(match_analysis.get("operator_token_overlap_count") or 0)
    domain_alignment_terms = list(match_analysis.get("domain_alignment_terms") or [])
    domain_family_overlap = list(match_analysis.get("domain_family_overlap") or [])
    shared_functional_traits = list(match_analysis.get("shared_functional_traits") or [])
    domain_family_conflict = bool(match_analysis.get("domain_family_conflict"))
    source_primary_domain = _clean_text(match_analysis.get("source_primary_domain"))
    candidate_primary_domain = _clean_text(match_analysis.get("candidate_primary_domain"))
    support_bonus = min(support_category_count, 3) * 0.04
    support_bonus += min(call_overlap_count, 2) * 0.03
    support_bonus += min(identifier_overlap_count, 6) * 0.008
    support_bonus += min(operator_overlap_count, 6) * 0.008
    support_bonus += min(len(domain_family_overlap), 2) * 0.05
    support_bonus += min(len(shared_functional_traits), 2) * 0.015
    risk_score = min(1.0, risk_score + support_bonus)

    license_spdx = _clean_text(source_repo.get("license_spdx"))
    license_bonus, license_family = _license_policy_bonus(license_spdx)
    risk_score = min(1.0, risk_score + license_bonus)

    if not license_spdx:
        risk_score = min(1.0, risk_score + 0.03)

    if is_knn_only and strongest_evidence_type == "unknown":
        strongest_evidence_type = "embedding_knn_match"
        knn_support_bonus = min(support_category_count, 3) * 0.05
        knn_support_bonus += min(call_overlap_count, 2) * 0.04
        knn_support_bonus += min(identifier_overlap_count, 6) * 0.01
        knn_support_bonus += min(operator_overlap_count, 6) * 0.01
        knn_support_bonus += min(len(domain_alignment_terms), 3) * 0.03
        knn_support_bonus += min(len(domain_family_overlap), 2) * 0.05
        knn_support_bonus += min(len(shared_functional_traits), 2) * 0.02
        knn_floor = 0.28 + (normalized_similarity * 0.48) + knn_support_bonus
        if domain_family_conflict:
            knn_floor = min(knn_floor, 0.58 if len(shared_functional_traits) < 2 else 0.72)
        else:
            knn_floor = min(knn_floor, 0.82)
        risk_score = max(risk_score, knn_floor)

    if len(retrieval_sources) == 1:
        if strongest_evidence_type == "normalized_hash_match":
            risk_score = min(risk_score, 0.9)
        elif strongest_evidence_type == "anonymized_hash_match":
            risk_score = min(risk_score, 0.82)
        elif strongest_evidence_type in {"raw_code_match", "normalized_code_match"}:
            risk_score = min(risk_score, 0.55 + (normalized_similarity * 0.5))
        elif strongest_evidence_type == "anonymized_code_match":
            risk_score = min(risk_score, 0.45 + (normalized_similarity * 0.45))
        elif strongest_evidence_type == "structural_similarity":
            risk_score = min(risk_score, 0.35 + (normalized_similarity * 0.35))
        elif strongest_evidence_type == "embedding_knn_match":
            risk_score = min(risk_score, 0.85)

    if retrieval_sources == ["rule_based"] and strongest_evidence_type == "anonymized_code_match":
        if support_category_count < 2:
            risk_score = min(risk_score, 0.52)
        elif not domain_alignment_terms and not domain_family_overlap and call_overlap_count == 0:
            risk_score = min(risk_score, 0.58)

    is_structural_twin = (
        call_overlap_count == 0
        and support_category_count < 2
        and not domain_alignment_terms
        and (
            strongest_evidence_type in {"structural_similarity", "embedding_knn_match"}
            or bool(domain_family_overlap)
            or domain_family_conflict
        )
    )

    if is_knn_only and strongest_evidence_type == "embedding_knn_match":
        risk_score = min(risk_score, 0.59)

    if is_rule_and_knn and strongest_evidence_type == "anonymized_code_match":
        risk_score = min(risk_score, 0.79)

    if is_structural_twin:
        risk_score = min(risk_score, 0.59)

    if strongest_evidence_type != "raw_hash_match" and domain_family_conflict:
        if candidate_primary_domain == "config_setter" and source_primary_domain != "config_setter":
            risk_score = min(risk_score, 0.38)
        elif len(shared_functional_traits) >= 2:
            risk_score = min(risk_score, 0.68)
        else:
            risk_score = min(risk_score, 0.52)

    if risk_score >= 0.95:
        risk_level = "critical"
    elif risk_score >= 0.8:
        risk_level = "high"
    elif risk_score >= 0.6:
        risk_level = "medium"
    else:
        risk_level = "low"

    reasons = list(review.get("reasons") or [])
    reasons.append(f"Hybrid similarity score after scaling: {round(normalized_similarity, 4)}.")
    if len(retrieval_sources) >= 2:
        reasons.append("Candidate matched in both rule-based retrieval and embedding kNN.")
    elif retrieval_sources == ["knn"] and strongest_evidence_type == "embedding_knn_match":
        reasons.append("Embedding-space kNN surfaced this candidate without a lexical rule-based hit.")
    if is_knn_only and strongest_evidence_type == "embedding_knn_match":
        reasons.append("kNN-only embedding match is capped below medium until stronger lexical evidence is present.")
    if is_rule_and_knn and strongest_evidence_type == "anonymized_code_match":
        reasons.append("Rule-based and kNN agreement on anonymized-code-only evidence is capped at medium.")
    if is_structural_twin:
        reasons.append("Structural-twin pattern without direct call/domain evidence is capped below medium.")
    if license_spdx:
        reasons.append(f"Candidate repository license family: {license_family.lower()} ({license_spdx}).")
    else:
        reasons.append("Candidate repository license metadata is missing, so review is conservative.")

    return {
        "risk_score": round(risk_score, 4),
        "risk_level": risk_level,
        "strongest_evidence_type": strongest_evidence_type,
        "license_spdx": license_spdx,
        "license_name": _clean_text(source_repo.get("license_name")),
        "reasons": reasons,
    }


def _build_knn_search_body(
    *,
    source_doc: Mapping[str, Any],
    query_vector: list[float],
    top_k: int,
    include_same_repo: bool,
) -> dict[str, Any]:
    filter_clauses: list[dict[str, Any]] = []
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
        "_source": KNN_EMBEDDING_SOURCE_FIELDS,
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
    repo_doc_cache: RepoDocumentCache | None = None,
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
    repo_doc_ids = [_clean_text(payload["source"].get("repo_id")) for _chunk_id, payload in ranked_items]
    if repo_doc_cache is not None:
        repo_docs = repo_doc_cache.get_many(
            store=resolved_store,
            collection_name=REPO_REGISTRY_INDEX,
            doc_ids=repo_doc_ids,
        )
    else:
        repo_docs = _multi_get_documents(
            store=resolved_store,
            collection_name=REPO_REGISTRY_INDEX,
            doc_ids=repo_doc_ids,
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
    repo_doc_cache: RepoDocumentCache | None = None,
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
        CODE_CHUNK_EMBEDDING_INDEX,
        _build_knn_search_body(
            source_doc=source_with_embedding,
            query_vector=query_vector,
            top_k=top_k,
            include_same_repo=include_same_repo,
        ),
    )
    hits = response.get("hits", {}).get("hits", [])
    chunk_doc_ids = [
        _clean_text(hit.get("_id")) or _clean_text((hit.get("_source") or {}).get("chunk_id"))
        for hit in hits
    ]
    chunk_docs = resolved_store.multi_get_documents(CODE_CHUNK_INDEX_ALIAS, chunk_doc_ids)
    repo_doc_ids = [
        _clean_text((doc.get("_source") or {}).get("repo_id"))
        for doc in chunk_docs.values()
        if isinstance(doc, dict)
    ]
    if repo_doc_cache is not None:
        repo_docs = repo_doc_cache.get_many(
            store=resolved_store,
            collection_name=REPO_REGISTRY_INDEX,
            doc_ids=repo_doc_ids,
        )
    else:
        repo_docs = _multi_get_documents(
            store=resolved_store,
            collection_name=REPO_REGISTRY_INDEX,
            doc_ids=repo_doc_ids,
        )

    candidates: list[dict[str, Any]] = []
    for rank, hit in enumerate(hits, start=1):
        embedding_source = dict(hit.get("_source", {}))
        chunk_id = _clean_text(hit.get("_id")) or _clean_text(embedding_source.get("chunk_id"))
        if not chunk_id:
            continue
        chunk_doc = chunk_docs.get(chunk_id)
        candidate_source = dict((chunk_doc or {}).get("_source", {})) if isinstance(chunk_doc, dict) else {}
        if not candidate_source:
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
    def _build_scaled_score_map(
        candidates: list[dict[str, Any]],
        *,
        source_name: str,
    ) -> dict[str, float]:
        scored_items: list[tuple[str, float]] = []
        for candidate in candidates:
            chunk_id = _clean_text(candidate.get("chunk_id"))
            if not chunk_id:
                continue
            if source_name == "rule_based":
                raw_score = float((candidate.get("rule_based") or {}).get("aggregate_score") or 0.0)
            else:
                raw_score = float((candidate.get("knn") or {}).get("score") or 0.0)
            scored_items.append((chunk_id, raw_score))

        if not scored_items:
            return {}

        score_values = [score for _chunk_id, score in scored_items]
        min_score = min(score_values)
        max_score = max(score_values)
        if max_score > min_score:
            return {
                chunk_id: round((score - min_score) / (max_score - min_score), 6)
                for chunk_id, score in scored_items
            }

        if len(scored_items) == 1:
            return {scored_items[0][0]: 1.0}

        denominator = len(scored_items) - 1
        return {
            chunk_id: round(1.0 - (index / denominator), 6)
            for index, (chunk_id, _score) in enumerate(scored_items)
        }

    rule_based_scaled_scores = _build_scaled_score_map(rule_based_candidates, source_name="rule_based")
    knn_scaled_scores = _build_scaled_score_map(knn_candidates, source_name="knn")
    merged: dict[str, dict[str, Any]] = {}

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
                "license_review": None,
                "hybrid": {
                    "score": 0.0,
                    "source_count": 0,
                    "rule_based_scaled_score": None,
                    "knn_scaled_score": None,
                },
            }
            merged[chunk_id] = payload

        retrieval_sources = payload["retrieval_sources"]
        if source_name not in retrieval_sources:
            retrieval_sources.append(source_name)
        if source_name == "rule_based":
            payload["rule_based"] = dict(candidate.get("rule_based") or {})
            payload["hybrid"]["rule_based_scaled_score"] = rule_based_scaled_scores.get(chunk_id)
        elif source_name == "knn":
            payload["knn"] = dict(candidate.get("knn") or {})
            payload["hybrid"]["knn_scaled_score"] = knn_scaled_scores.get(chunk_id)

        payload["hybrid"]["source_count"] = len(retrieval_sources)
        payload["hybrid"]["score"] = round(
            sum(
                score
                for score in (
                    payload["hybrid"].get("rule_based_scaled_score"),
                    payload["hybrid"].get("knn_scaled_score"),
                )
                if isinstance(score, (int, float))
            ),
            6,
        )

    for candidate in rule_based_candidates:
        _upsert(candidate, "rule_based")
    for candidate in knn_candidates:
        _upsert(candidate, "knn")

    for payload in merged.values():
        payload["license_review"] = _build_hybrid_license_review(payload)

    ranked_candidates = sorted(
        merged.values(),
        key=lambda item: (
            -float((item.get("hybrid") or {}).get("score") or 0.0),
            -int((item.get("hybrid") or {}).get("source_count") or 0),
            -float((item.get("hybrid") or {}).get("knn_scaled_score") or 0.0),
            -float((item.get("hybrid") or {}).get("rule_based_scaled_score") or 0.0),
            -float((item.get("license_review") or {}).get("risk_score") or 0.0),
            str(item.get("chunk_id") or ""),
        ),
    )
    return ranked_candidates[: max(1, int(top_k))]


def retrieve_hybrid_candidates(
    source_doc: Mapping[str, Any],
    *,
    store: OpenSearchStore | None = None,
    rule_based_top_k: int = DEFAULT_RULE_BASED_TOP_K,
    per_variant_k: int = DEFAULT_RULE_BASED_PER_VARIANT_K,
    knn_top_k: int = DEFAULT_KNN_TOP_K,
    merged_top_k: int = DEFAULT_MERGED_TOP_K,
    include_same_repo: bool = False,
    repo_doc_cache: RepoDocumentCache | None = None,
) -> HybridRetrievalResult:
    resolved_store = store or OpenSearchStore()
    total_started_at = perf_counter()

    def _timed_rule_based() -> tuple[dict[str, Any], float]:
        started_at = perf_counter()
        result = retrieve_rule_based_candidates(
            source_doc,
            store=resolved_store,
            top_k=rule_based_top_k,
            per_variant_k=per_variant_k,
            include_same_repo=include_same_repo,
            repo_doc_cache=repo_doc_cache,
        )
        return result, round(perf_counter() - started_at, 4)

    def _timed_knn() -> tuple[dict[str, Any], float]:
        started_at = perf_counter()
        result = retrieve_knn_candidates(
            source_doc,
            store=resolved_store,
            top_k=knn_top_k,
            include_same_repo=include_same_repo,
            repo_doc_cache=repo_doc_cache,
        )
        return result, round(perf_counter() - started_at, 4)

    with ThreadPoolExecutor(max_workers=2) as executor:
        rule_future = executor.submit(_timed_rule_based)
        knn_future = executor.submit(_timed_knn)
        rule_based_result, rule_based_seconds = rule_future.result()
        knn_result, knn_seconds = knn_future.result()

    rule_based_candidates = list(rule_based_result.get("candidates") or [])
    knn_candidates = list(knn_result.get("candidates") or [])
    merge_started_at = perf_counter()
    merged_candidates = _merge_hybrid_candidates(
        rule_based_candidates=rule_based_candidates,
        knn_candidates=knn_candidates,
        top_k=merged_top_k,
    )
    merge_seconds = round(perf_counter() - merge_started_at, 4)
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
        timings={
            "total_seconds": round(perf_counter() - total_started_at, 4),
            "rule_based_seconds": rule_based_seconds,
            "knn_seconds": knn_seconds,
            "merge_seconds": merge_seconds,
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
    return select_source_chunks(source_chunks, limit=limit)


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


def retrieve_hybrid_candidates_for_source_chunks(
    source_chunks: list[Mapping[str, Any]],
    *,
    store: OpenSearchStore | None = None,
    rule_based_top_k: int = DEFAULT_RULE_BASED_TOP_K,
    per_variant_k: int = DEFAULT_RULE_BASED_PER_VARIANT_K,
    knn_top_k: int = DEFAULT_KNN_TOP_K,
    merged_top_k: int = DEFAULT_MERGED_TOP_K,
    include_same_repo: bool = False,
) -> dict[str, Any]:
    resolved_store = store or OpenSearchStore()
    total_started_at = perf_counter()
    normalized_source_chunks = [dict(source_chunk) for source_chunk in source_chunks]
    repo_id = _clean_text(normalized_source_chunks[0].get("repo_id")) if normalized_source_chunks else ""
    shared_repo_doc_cache = RepoDocumentCache()

    def _retrieve_one_source_chunk(source_chunk: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        hybrid_result = retrieve_hybrid_candidates(
            source_chunk,
            store=resolved_store,
            rule_based_top_k=rule_based_top_k,
            per_variant_k=per_variant_k,
            knn_top_k=knn_top_k,
            merged_top_k=merged_top_k,
            include_same_repo=include_same_repo,
            repo_doc_cache=shared_repo_doc_cache,
        )
        return (
            {
                "source_chunk_id": _clean_text(source_chunk.get("chunk_id")),
                "file_path": _clean_text(source_chunk.get("file_path")),
                "symbol_name": _clean_text(source_chunk.get("symbol_name")),
                "result": hybrid_result,
            },
            {
                "source_chunk_id": _clean_text(source_chunk.get("chunk_id")),
                "file_path": _clean_text(source_chunk.get("file_path")),
                "symbol_name": _clean_text(source_chunk.get("symbol_name")),
                **dict(hybrid_result.timings),
            },
        )

    chunk_results: list[dict[str, Any]] = []
    chunk_timings: list[dict[str, Any]] = []
    chunk_parallelism = min(
        len(normalized_source_chunks) or 1,
        max(1, int(settings.retrieval_source_chunk_parallelism)),
    )
    with ThreadPoolExecutor(max_workers=chunk_parallelism) as executor:
        for chunk_result, chunk_timing in executor.map(_retrieve_one_source_chunk, normalized_source_chunks):
            chunk_results.append(chunk_result)
            chunk_timings.append(chunk_timing)

    total_elapsed_seconds = round(perf_counter() - total_started_at, 4)
    total_chunk_seconds = round(
        sum(float(chunk_timing.get("total_seconds") or 0.0) for chunk_timing in chunk_timings),
        4,
    )
    average_chunk_seconds = round(total_chunk_seconds / len(chunk_timings), 4) if chunk_timings else 0.0
    slowest_chunk = max(
        chunk_timings,
        key=lambda chunk_timing: float(chunk_timing.get("total_seconds") or 0.0),
        default={},
    )

    return {
        "retrieval_version": DEFAULT_HYBRID_RETRIEVAL_VERSION,
        "repo_id": repo_id,
        "source_chunk_count": len(normalized_source_chunks),
        "chunk_results": chunk_results,
        "timings": {
            "total_seconds": total_elapsed_seconds,
            "source_chunk_count": len(normalized_source_chunks),
            "chunk_parallelism": chunk_parallelism,
            "cumulative_chunk_seconds": total_chunk_seconds,
            "aggregate_chunk_seconds": total_chunk_seconds,
            "average_chunk_seconds": average_chunk_seconds,
            "slowest_chunk": slowest_chunk,
        },
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
