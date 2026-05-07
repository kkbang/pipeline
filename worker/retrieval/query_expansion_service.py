from dataclasses import dataclass
from typing import Any, Mapping


DEFAULT_QUERY_EXPANSION_VERSION = "qe_v1"
STRUCTURAL_TOKEN_CAP = 16


@dataclass(frozen=True, slots=True)
class QueryVariant:
    variant_id: str
    family: str
    query_text: str | None
    query_payload: dict[str, Any] | None
    normalization_steps: tuple[str, ...]
    estimated_cost: str
    priority: int

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "variant_id": self.variant_id,
            "family": self.family,
            "normalization_steps": list(self.normalization_steps),
            "estimated_cost": self.estimated_cost,
            "priority": self.priority,
        }
        if self.query_text is not None:
            payload["query_text"] = self.query_text
        if self.query_payload is not None:
            payload["query_payload"] = self.query_payload
        return payload


@dataclass(frozen=True, slots=True)
class QueryBundle:
    query_bundle_id: str
    source_chunk_id: str
    source_repo_id: str
    language: str
    expansion_version: str
    query_families: tuple[str, ...]
    query_count: int
    budget_profile: dict[str, int]
    variants: tuple[QueryVariant, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "query_bundle_id": self.query_bundle_id,
            "source_chunk_id": self.source_chunk_id,
            "source_repo_id": self.source_repo_id,
            "language": self.language,
            "expansion_version": self.expansion_version,
            "query_families": list(self.query_families),
            "query_count": self.query_count,
            "budget_profile": dict(self.budget_profile),
            "variants": [variant.as_dict() for variant in self.variants],
        }


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _clean_string_list(value: object, *, cap: int = 0) -> list[str]:
    if not isinstance(value, list):
        return []

    items: list[str] = []
    seen: set[str] = set()
    for raw_item in value:
        item = _clean_text(raw_item)
        if not item or item in seen:
            continue
        seen.add(item)
        items.append(item)
        if cap > 0 and len(items) >= cap:
            break
    return items


def _build_variant_id(source_chunk_id: str, family: str, name: str) -> str:
    normalized_chunk_id = source_chunk_id or "unknown_chunk"
    return f"{normalized_chunk_id}:{family}:{name}"


def build_query_bundle(
    source_doc: Mapping[str, Any],
    *,
    expansion_version: str = DEFAULT_QUERY_EXPANSION_VERSION,
    ast_prefix_cap: int = STRUCTURAL_TOKEN_CAP,
) -> QueryBundle:
    source_chunk_id = _clean_text(source_doc.get("chunk_id"))
    source_repo_id = _clean_text(source_doc.get("repo_id"))
    language = _clean_text(source_doc.get("language")).lower()

    variants: list[QueryVariant] = []

    raw_hash = _clean_text(source_doc.get("raw_hash"))
    if raw_hash:
        variants.append(
            QueryVariant(
                variant_id=_build_variant_id(source_chunk_id, "exact", "raw_hash"),
                family="exact",
                query_text=None,
                query_payload={"raw_hash": raw_hash},
                normalization_steps=(),
                estimated_cost="low",
                priority=100,
            )
        )

    raw_code = _clean_text(source_doc.get("raw_code"))
    if raw_code:
        variants.append(
            QueryVariant(
                variant_id=_build_variant_id(source_chunk_id, "exact", "raw_code"),
                family="exact",
                query_text=raw_code,
                query_payload=None,
                normalization_steps=(),
                estimated_cost="medium",
                priority=90,
            )
        )

    normalized_hash = _clean_text(source_doc.get("normalized_hash"))
    if normalized_hash:
        variants.append(
            QueryVariant(
                variant_id=_build_variant_id(source_chunk_id, "normalized", "normalized_hash"),
                family="normalized",
                query_text=None,
                query_payload={"normalized_hash": normalized_hash},
                normalization_steps=("normalize_code",),
                estimated_cost="low",
                priority=85,
            )
        )

    anonymized_hash = _clean_text(source_doc.get("anonymized_hash"))
    if anonymized_hash:
        variants.append(
            QueryVariant(
                variant_id=_build_variant_id(source_chunk_id, "normalized", "anonymized_hash"),
                family="normalized",
                query_text=None,
                query_payload={"anonymized_hash": anonymized_hash},
                normalization_steps=("normalize_code", "anonymize_identifiers"),
                estimated_cost="low",
                priority=85,
            )
        )

    normalized_code = _clean_text(source_doc.get("normalized_code"))
    if normalized_code:
        variants.append(
            QueryVariant(
                variant_id=_build_variant_id(source_chunk_id, "normalized", "normalized_code"),
                family="normalized",
                query_text=normalized_code,
                query_payload=None,
                normalization_steps=("normalize_code",),
                estimated_cost="medium",
                priority=80,
            )
        )

    anonymized_code = _clean_text(source_doc.get("anonymized_code"))
    if anonymized_code:
        variants.append(
            QueryVariant(
                variant_id=_build_variant_id(source_chunk_id, "normalized", "anonymized_code"),
                family="normalized",
                query_text=anonymized_code,
                query_payload=None,
                normalization_steps=("normalize_code", "anonymize_identifiers"),
                estimated_cost="medium",
                priority=80,
            )
        )

    structural_payload = {
        "chunk_type": _clean_text(source_doc.get("chunk_type")),
        "symbol_type": _clean_text(source_doc.get("symbol_type")),
        "structure_signature": _clean_text(source_doc.get("structure_signature")),
        "control_flow_tags": _clean_string_list(
            source_doc.get("control_flow_tags"),
            cap=STRUCTURAL_TOKEN_CAP,
        ),
        "call_tokens": _clean_string_list(
            source_doc.get("call_tokens"),
            cap=STRUCTURAL_TOKEN_CAP,
        ),
        "identifier_tokens": _clean_string_list(
            source_doc.get("identifier_tokens"),
            cap=STRUCTURAL_TOKEN_CAP,
        ),
        "operator_tokens": _clean_string_list(
            source_doc.get("operator_tokens"),
            cap=STRUCTURAL_TOKEN_CAP,
        ),
        "ast_node_sequence": _clean_string_list(
            source_doc.get("ast_node_sequence"),
            cap=max(1, ast_prefix_cap),
        ),
    }
    structural_payload = {
        key: value
        for key, value in structural_payload.items()
        if value not in ("", [], None)
    }
    if structural_payload:
        variants.append(
            QueryVariant(
                variant_id=_build_variant_id(source_chunk_id, "structural", "summary"),
                family="structural",
                query_text=None,
                query_payload=structural_payload,
                normalization_steps=("structure_summary",),
                estimated_cost="low",
                priority=70,
            )
        )

    query_families = tuple(sorted({variant.family for variant in variants}))
    budget_profile: dict[str, int] = {}
    for family in query_families:
        budget_profile[family] = sum(1 for variant in variants if variant.family == family)

    return QueryBundle(
        query_bundle_id=f"bundle_{source_chunk_id or 'unknown_chunk'}_{expansion_version}",
        source_chunk_id=source_chunk_id,
        source_repo_id=source_repo_id,
        language=language,
        expansion_version=expansion_version,
        query_families=query_families,
        query_count=len(variants),
        budget_profile=budget_profile,
        variants=tuple(variants),
    )
