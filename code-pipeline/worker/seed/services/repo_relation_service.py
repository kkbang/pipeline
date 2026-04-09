import re
from collections import defaultdict
from datetime import datetime, timezone

from worker.common.config import settings
from worker.storage.local_json_store import LocalJsonStore


def _safe_fragment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")


def _rule_field(base_name: str, rule_name: str) -> str:
    return f"{base_name}_{_safe_fragment(rule_name)}"


def _repo_id(owner: str, repo_name: str) -> str:
    return f"github:{owner}/{repo_name}"


def _default_rule_config() -> dict:
    return {
        "enabled": True,
        "max_registered_repos": settings.repo_relation_repo_limit,
        "include_same_owner_relations": True,
        "include_same_project_family_relations": True,
        "include_fork_relations": True,
        "max_pairs_per_group": 100,
    }


def _load_rule_config(rule_config: dict | None) -> dict:
    base_config = _default_rule_config()
    if not rule_config:
        return base_config
    return {**base_config, **rule_config}


def _family_key(repo_name: str) -> str | None:
    normalized = re.sub(r"[^a-z0-9]+", "-", repo_name.lower()).strip("-")
    if not normalized:
        return None

    tokens = [token for token in normalized.split("-") if token]
    while tokens and tokens[0] in {"python", "py", "node", "js", "go", "rust", "lib"}:
        tokens = tokens[1:]

    if not tokens:
        return None

    key = tokens[0]
    if len(key) < 4:
        return None

    return key


def _limited_pairs(items: list[dict], max_pairs: int) -> list[tuple[dict, dict]]:
    pairs = []
    for index, left in enumerate(items):
        for right in items[index + 1:]:
            pairs.append((left, right))
            if len(pairs) >= max_pairs:
                return pairs
    return pairs


def _upsert_relation(
    store: LocalJsonStore,
    *,
    relation_type: str,
    from_repo_id: str,
    to_repo_id: str,
    confidence: float,
    evidence: dict,
    rule_name: str,
    symmetric: bool,
) -> None:
    if symmetric:
        left_repo_id, right_repo_id = sorted([from_repo_id, to_repo_id])
        doc_id = f"{relation_type}:{left_repo_id}:{right_repo_id}"
        from_repo_id = left_repo_id
        to_repo_id = right_repo_id
    else:
        doc_id = f"{relation_type}:{from_repo_id}:{to_repo_id}"

    store.upsert_document(
        collection_name="repo_relation_index",
        doc_id=doc_id,
        body={
            "relation_type": relation_type,
            "from_repo_id": from_repo_id,
            "to_repo_id": to_repo_id,
            "confidence": confidence,
            "evidence": evidence,
            "source_rule": rule_name,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def _load_repo_sources(store: LocalJsonStore, max_registered_repos: int) -> list[dict]:
    repo_sources = []
    for hit in store.list_documents("repo_registry_index", size=max_registered_repos):
        source = hit.get("_source", {})
        if source.get("hosting_platform") != "github":
            continue
        owner = str(source.get("owner") or "").strip()
        repo_name = str(source.get("repo_name") or "").strip()
        if not owner or not repo_name:
            continue
        repo_sources.append(source)
    return repo_sources


def _enrich_same_owner_relations(
    store: LocalJsonStore,
    repo_sources: list[dict],
    *,
    rule_name: str,
    max_pairs_per_group: int,
) -> None:
    repos_by_owner: dict[str, list[dict]] = defaultdict(list)
    for source in repo_sources:
        repos_by_owner[str(source.get("owner") or "").lower()].append(source)

    for owner, group in repos_by_owner.items():
        if len(group) < 2:
            continue

        for left, right in _limited_pairs(group, max_pairs=max_pairs_per_group):
            _upsert_relation(
                store,
                relation_type="same_owner",
                from_repo_id=_repo_id(left["owner"], left["repo_name"]),
                to_repo_id=_repo_id(right["owner"], right["repo_name"]),
                confidence=0.95,
                evidence={"shared_owner": owner},
                rule_name=rule_name,
                symmetric=True,
            )


def _enrich_same_project_family_relations(
    store: LocalJsonStore,
    repo_sources: list[dict],
    *,
    rule_name: str,
    max_pairs_per_group: int,
) -> None:
    repos_by_family: dict[str, list[dict]] = defaultdict(list)
    for source in repo_sources:
        family_key = _family_key(str(source.get("repo_name") or ""))
        if family_key:
            repos_by_family[family_key].append(source)

    for family_key, group in repos_by_family.items():
        if len(group) < 2:
            continue

        filtered_group = sorted(
            group,
            key=lambda item: (
                item.get("owner") or "",
                item.get("repo_name") or "",
            ),
        )
        for left, right in _limited_pairs(filtered_group, max_pairs=max_pairs_per_group):
            if left["owner"] == right["owner"]:
                continue

            _upsert_relation(
                store,
                relation_type="same_project_family",
                from_repo_id=_repo_id(left["owner"], left["repo_name"]),
                to_repo_id=_repo_id(right["owner"], right["repo_name"]),
                confidence=0.45,
                evidence={"family_key": family_key},
                rule_name=rule_name,
                symmetric=True,
            )


def _enrich_fork_relations(
    store: LocalJsonStore,
    repo_sources: list[dict],
    *,
    rule_name: str,
) -> None:
    for source in repo_sources:
        owner = str(source["owner"]).lower()
        repo_name = str(source["repo_name"]).lower()
        if source.get("repo_is_fork") is not True:
            continue

        parent_full_name = str(source.get("repo_parent_full_name") or "").strip()
        if "/" not in parent_full_name:
            continue

        parent_owner, parent_repo_name = parent_full_name.split("/", 1)
        _upsert_relation(
            store,
            relation_type="fork_of",
            from_repo_id=_repo_id(owner, repo_name),
            to_repo_id=_repo_id(parent_owner, parent_repo_name),
            confidence=0.99,
            evidence={"parent_full_name": parent_full_name},
            rule_name=rule_name,
            symmetric=False,
        )


def run_repo_relation_enrichment(rule_name: str = "default", rule_config: dict | None = None) -> None:
    config = _load_rule_config(rule_config)
    if config.get("enabled") is not True:
        return

    store = LocalJsonStore()
    repo_sources = _load_repo_sources(
        store,
        max_registered_repos=int(config["max_registered_repos"]),
    )
    if not repo_sources:
        return

    if config.get("include_same_owner_relations") is True:
        _enrich_same_owner_relations(
            store,
            repo_sources,
            rule_name=rule_name,
            max_pairs_per_group=int(config["max_pairs_per_group"]),
        )

    if config.get("include_same_project_family_relations") is True:
        _enrich_same_project_family_relations(
            store,
            repo_sources,
            rule_name=rule_name,
            max_pairs_per_group=int(config["max_pairs_per_group"]),
        )

    if config.get("include_fork_relations") is True:
        _enrich_fork_relations(
            store,
            repo_sources,
            rule_name=rule_name,
        )

    enriched_at_field = _rule_field("relation_enriched_at", rule_name)
    enriched_at = datetime.now(timezone.utc).isoformat()
    for source in repo_sources:
        store.update_document(
            collection_name="repo_registry_index",
            doc_id=_repo_id(source["owner"], source["repo_name"]),
            body={enriched_at_field: enriched_at},
        )
