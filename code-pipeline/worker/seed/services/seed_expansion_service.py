import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re

from worker.common.config import settings
from worker.seed.adapters.github import GitHubApiAdapter, GitHubRepoContentAdapter
from worker.seed.extractors.dependency_repo_hint_extractor import (
    MANIFEST_PATHS,
    extract_repo_candidates_from_dependency_manifest,
)
from worker.seed.extractors.readme_link_extractor import extract_repo_candidates_from_readme
from worker.seed.resolvers.github_url_canonicalizer import canonicalize_github_repo_url
from worker.seed.services.seed_item_writer import (
    safe_path_fragment,
    stable_digest,
    write_seed_item,
)
from worker.storage.local_json_store import LocalJsonStore


@dataclass(slots=True)
class RepoContentFetchResult:
    doc_id: str
    owner: str
    repo_name: str
    canonical_repo_url: str
    parent_repo_id: str
    readme_text: str | None = None
    manifest_texts: dict[str, str] = field(default_factory=dict)
    error: Exception | None = None


def _rule_field(base_name: str, rule_name: str) -> str:
    return f"{base_name}_{safe_path_fragment(rule_name)}"


def _default_rule_config() -> dict:
    return {
        "enabled": True,
        "max_registered_repos": settings.repo_content_expansion_repo_limit,
        "max_children_per_repo": 20,
        "include_readme_links": True,
        "include_dependency_manifests": True,
    }


def _load_rule_config(rule_config: dict | None) -> dict:
    base_config = _default_rule_config()
    if not rule_config:
        return base_config
    return {**base_config, **rule_config}


def _repo_id(owner: str, repo_name: str) -> str:
    return f"github:{owner}/{repo_name}"


def _iter_target_repos(store: LocalJsonStore, rule_name: str, max_registered_repos: int) -> list[dict]:
    rule_status_field = _rule_field("content_expansion_status", rule_name)
    repo_docs = []
    for hit in store.list_documents("repo_registry_index", size=max_registered_repos * 5):
        source = hit.get("_source", {})
        if source.get("hosting_platform") != "github":
            continue
        if source.get(rule_status_field) == "completed":
            continue
        repo_docs.append(hit)

    repo_docs.sort(
        key=lambda hit: (
            -(hit.get("_source", {}).get("discovery_source_count") or 0),
            hit.get("_source", {}).get("owner") or "",
            hit.get("_source", {}).get("repo_name") or "",
        )
    )
    return repo_docs[:max_registered_repos]


async def _fetch_repo_content_batch(
    api_adapter: GitHubApiAdapter,
    repo_hits: list[dict],
    *,
    include_readme_links: bool,
    include_dependency_manifests: bool,
) -> list[RepoContentFetchResult]:
    semaphore = asyncio.Semaphore(max(1, settings.repo_content_expansion_concurrency))
    adapter = GitHubRepoContentAdapter(api_adapter)

    async def _fetch(hit: dict) -> RepoContentFetchResult:
        async with semaphore:
            source = hit["_source"]
            owner = str(source.get("owner") or "").strip()
            repo_name = str(source.get("repo_name") or "").strip()
            canonical_repo_url = str(source.get("canonical_repo_url") or "").strip()
            parent_repo_id = _repo_id(owner, repo_name)

            result = RepoContentFetchResult(
                doc_id=hit["_id"],
                owner=owner,
                repo_name=repo_name,
                canonical_repo_url=canonical_repo_url,
                parent_repo_id=parent_repo_id,
            )

            try:
                if include_readme_links:
                    result.readme_text = await adapter.fetch_readme_text(owner, repo_name)

                if include_dependency_manifests:
                    for manifest_path in MANIFEST_PATHS:
                        manifest_text = await adapter.fetch_file_text(owner, repo_name, manifest_path)
                        if manifest_text:
                            result.manifest_texts[manifest_path] = manifest_text
            except Exception as exc:
                result.error = exc

            return result

    return await asyncio.gather(*(_fetch(hit) for hit in repo_hits))


def _emit_expansion_seed(
    store: LocalJsonStore,
    *,
    rule_name: str,
    parent_repo_id: str,
    parent_canonical_repo_url: str,
    expansion_reason: str,
    source_path: str,
    candidate_repo_url: str,
) -> None:
    canonical_result = canonicalize_github_repo_url(candidate_repo_url)
    if not canonical_result:
        return

    owner, repo_name, canonical_repo_url = canonical_result
    if canonical_repo_url == parent_canonical_repo_url:
        return

    source_type = "repo_readme_link" if expansion_reason == "readme_link" else "repo_dependency_link"
    source_item_id = f"{parent_repo_id}:{source_path}:{owner}/{repo_name}"
    raw_metadata = {
        "source_type": source_type,
        "source_name": rule_name,
        "source_item_id": source_item_id,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "parent_repo_id": parent_repo_id,
        "parent_canonical_repo_url": parent_canonical_repo_url,
        "source_path": source_path,
        "expansion_depth": 1,
        "expansion_reason": expansion_reason,
        "candidate_repo_url": canonical_repo_url,
    }

    raw_metadata_path = (
        f"raw/repo_content_expansion/{safe_path_fragment(rule_name)}/"
        f"{safe_path_fragment(parent_repo_id)}/"
        f"{safe_path_fragment(source_path)}/"
        f"{safe_path_fragment(owner)}__{safe_path_fragment(repo_name)}.json"
    )

    doc_id = (
        f"{source_type}:{safe_path_fragment(rule_name)}:{safe_path_fragment(parent_repo_id)}:"
        f"{stable_digest(source_path)}:{owner}/{repo_name}"
    )
    write_seed_item(
        store,
        doc_id=doc_id,
        source_type=source_type,
        source_name=rule_name,
        source_item_id=source_item_id,
        raw_metadata_path=raw_metadata_path,
        raw_metadata=raw_metadata,
        candidate_repo_urls=[canonical_repo_url],
        extra_body={
            "parent_repo_id": parent_repo_id,
            "expansion_depth": 1,
            "expansion_reason": expansion_reason,
        },
    )


def run_repo_content_expansion(rule_name: str = "default", rule_config: dict | None = None) -> None:
    config = _load_rule_config(rule_config)
    if config.get("enabled") is not True:
        return

    store = LocalJsonStore()
    started_at = datetime.now(timezone.utc).isoformat()

    status_field = _rule_field("content_expansion_status", rule_name)
    expanded_at_field = _rule_field("content_expanded_at", rule_name)
    generated_count_field = _rule_field("content_expansion_generated_count", rule_name)
    error_field = _rule_field("content_expansion_error", rule_name)

    repo_hits = _iter_target_repos(
        store,
        rule_name=rule_name,
        max_registered_repos=int(config["max_registered_repos"]),
    )
    async def _run() -> list[RepoContentFetchResult]:
        async with GitHubApiAdapter(concurrency=settings.repo_content_expansion_concurrency) as api_adapter:
            return await _fetch_repo_content_batch(
                api_adapter,
                repo_hits,
                include_readme_links=config.get("include_readme_links") is True,
                include_dependency_manifests=config.get("include_dependency_manifests") is True,
            )

    fetch_results = asyncio.run(_run())

    first_error = None
    for result in fetch_results:
        if (
            not result.owner
            or not result.repo_name
            or not result.canonical_repo_url
        ):
            continue

        generated_count = 0
        seen_repo_urls = set()

        try:
            if result.error is not None:
                raise result.error

            if result.readme_text:
                for candidate_repo_url in extract_repo_candidates_from_readme(result.readme_text):
                    canonical_result = canonicalize_github_repo_url(candidate_repo_url)
                    if not canonical_result:
                        continue
                    _, _, canonical_target_url = canonical_result
                    if canonical_target_url in seen_repo_urls:
                        continue

                    _emit_expansion_seed(
                        store,
                        rule_name=rule_name,
                        parent_repo_id=result.parent_repo_id,
                        parent_canonical_repo_url=result.canonical_repo_url,
                        expansion_reason="readme_link",
                        source_path="README.md",
                        candidate_repo_url=canonical_target_url,
                    )
                    seen_repo_urls.add(canonical_target_url)
                    generated_count += 1

                    if generated_count >= int(config["max_children_per_repo"]):
                        break

            if generated_count < int(config["max_children_per_repo"]):
                for manifest_path, manifest_text in result.manifest_texts.items():
                    for candidate_repo_url in extract_repo_candidates_from_dependency_manifest(
                        manifest_path,
                        manifest_text,
                    ):
                        canonical_result = canonicalize_github_repo_url(candidate_repo_url)
                        if not canonical_result:
                            continue
                        _, _, canonical_target_url = canonical_result
                        if canonical_target_url in seen_repo_urls:
                            continue

                        _emit_expansion_seed(
                            store,
                            rule_name=rule_name,
                            parent_repo_id=result.parent_repo_id,
                            parent_canonical_repo_url=result.canonical_repo_url,
                            expansion_reason="dependency_link",
                            source_path=manifest_path,
                            candidate_repo_url=canonical_target_url,
                        )
                        seen_repo_urls.add(canonical_target_url)
                        generated_count += 1

                        if generated_count >= int(config["max_children_per_repo"]):
                            break

                    if generated_count >= int(config["max_children_per_repo"]):
                        break

            store.update_document(
                collection_name="repo_registry_index",
                doc_id=result.doc_id,
                body={
                    status_field: "completed",
                    expanded_at_field: started_at,
                    generated_count_field: generated_count,
                    error_field: None,
                },
            )
        except Exception as exc:
            if first_error is None:
                first_error = exc
            store.update_document(
                collection_name="repo_registry_index",
                doc_id=result.doc_id,
                body={
                    status_field: "failed",
                    expanded_at_field: started_at,
                    error_field: str(exc),
                },
            )

    if first_error is not None:
        raise first_error
