import requests

from worker.seed.resolvers.github_repo_resolver import GitHubRepoResolver
from worker.seed.qualifiers.repo_qualifier import qualify_repo
from worker.storage.local_json_store import LocalJsonStore
# from worker.storage.opensearch_store import OpenSearchStore


def _build_discovery_source(source: dict) -> dict:
    return {
        "source_type": source.get("source_type"),
        "source_name": source.get("source_name"),
        "source_item_id": source.get("source_item_id"),
        "source_context": source.get("source_context") or {},
        "selected_repository_url": source.get("selected_repository_url"),
        "canonical_repo_url": source.get("canonical_repo_url"),
    }


def _merge_discovery_sources(existing_sources: list[dict], current_source: dict) -> list[dict]:
    merged_sources = [
        item for item in existing_sources if isinstance(item, dict)
    ]
    current_key = (
        current_source.get("source_type"),
        current_source.get("source_name"),
        current_source.get("source_item_id"),
    )
    existing_keys = {
        (
            item.get("source_type"),
            item.get("source_name"),
            item.get("source_item_id"),
        )
        for item in merged_sources
    }

    if current_key not in existing_keys:
        merged_sources.append(current_source)

    return merged_sources


def run_seed_qualification() -> None:
    store = LocalJsonStore()
    # os = OpenSearchStore()
    resolver = GitHubRepoResolver()

    seed_docs = store.find_documents_by_status(collection_name="seed_item_index", status="normalized")

    for hit in seed_docs:
        doc_id = hit["_id"]
        source = hit["_source"]

        owner = source["owner"]
        repo = source["repo"]
        
        try:
            repo_metadata = resolver.fetch_repo_metadata(owner, repo)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            reason = "github_rate_limited" if status_code == 403 else "github_api_error"
            store.update_document(
                collection_name="seed_item_index",
                doc_id=doc_id,
                body={
                    "status": "qualification_failed",
                    "reason": reason,
                    "error_status_code": status_code,
                    "error_message": str(exc),
                },
            )
            continue
        except requests.RequestException as exc:
            store.update_document(
                collection_name="seed_item_index",
                doc_id=doc_id,
                body={
                    "status": "qualification_failed",
                    "reason": "github_request_failed",
                    "error_message": str(exc),
                },
            )
            continue

        is_eligible, reason, score = qualify_repo(repo_metadata)

        if not is_eligible:
            store.update_document(
                collection_name="seed_item_index",
                doc_id=doc_id,
                body={
                    "status": "rejected",
                    "reason": reason,
                },
            )
            continue

        repo_doc_id = f"github:{owner}/{repo}"
        existing_repo_doc = store.get_document(collection_name="repo_registry_index", doc_id=repo_doc_id)
        existing_repo_source = existing_repo_doc.get("_source", {})
        discovery_sources = _merge_discovery_sources(
            existing_repo_source.get("discovery_sources", []),
            _build_discovery_source(source),
        )
        repo_body = {
            "canonical_repo_url": source["canonical_repo_url"],
            "owner": owner,
            "repo_name": repo,
            "hosting_platform": "github",
            "primary_language": repo_metadata.get("language"),
            "repo_license_spdx": ((repo_metadata.get("license") or {}).get("spdx_id")),
            "stars": repo_metadata.get("stargazers_count"),
            "forks_count": repo_metadata.get("forks_count"),
            "default_branch": repo_metadata.get("default_branch"),
            "is_public": not repo_metadata.get("private", False),
            "is_archived": repo_metadata.get("archived", False),
            "is_fork": repo_metadata.get("fork", False),
            "priority_score": score,
            "crawl_status": "scheduled",
            "discovery_source_type": source.get("source_type"),
            "discovery_source_name": source.get("source_name"),
            "discovery_source_item_id": source.get("source_item_id"),
            "discovery_source_count": len(discovery_sources),
            "source_types": sorted(
                {
                    item.get("source_type")
                    for item in discovery_sources
                    if item.get("source_type")
                }
            ),
            "discovery_sources": discovery_sources,
        }

        if source.get("package_name"):
            repo_body["package_name"] = source["package_name"]
        if source.get("registry_name"):
            repo_body["discovery_source_registry"] = source["registry_name"]

        store.upsert_document(
            collection_name="repo_registry_index",
            doc_id=repo_doc_id,
            body=repo_body,
        )

        store.update_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                "status": "registered",
            },
        )
