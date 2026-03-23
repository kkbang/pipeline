import requests

from worker.seed.resolvers.github_repo_resolver import GitHubRepoResolver
from worker.seed.qualifiers.repo_qualifier import qualify_repo
from worker.storage.local_json_store import LocalJsonStore
# from worker.storage.opensearch_store import OpenSearchStore


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
        store.upsert_document(
            collection_name="repo_registry_index",
            doc_id=repo_doc_id,
            body={
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
                "discovery_source_type": "package_registry",
                "discovery_source_name": source["registry_name"],
                "package_name": source["package_name"],
                "crawl_status": "scheduled",
            },
        )

        store.update_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                "status": "registered",
            },
        )
