from worker.seed.extractors.repo_url_extractor import (
    extract_repo_candidates_from_npm,
    extract_repo_candidates_from_pypi,
)
from worker.seed.resolvers.github_url_canonicalizer import canonicalize_github_repo_url
from worker.storage.local_json_store import LocalJsonStore
# from worker.storage.s3_store import S3Store
# from worker.storage.opensearch_store import OpenSearchStore


def _extract_repo_candidates(registry_name: str, raw_metadata: dict) -> list[str]:
    if registry_name == "pypi":
        return extract_repo_candidates_from_pypi(raw_metadata)

    if registry_name == "npm":
        return extract_repo_candidates_from_npm(raw_metadata)

    return []


def _load_candidate_urls(source: dict, store: LocalJsonStore) -> list[str]:
    candidate_urls = source.get("candidate_repo_urls")
    if isinstance(candidate_urls, list):
        return [url for url in candidate_urls if isinstance(url, str) and url.strip()]

    raw_metadata = store.get_json(source["raw_metadata_path"])
    return _extract_repo_candidates(source["registry_name"], raw_metadata)


def run_seed_normalization() -> None:
    store = LocalJsonStore()
    # s3 = S3Store()
    # os = OpenSearchStore()

    seed_docs = store.find_documents_by_status(collection_name="seed_item_index", status="ingested")

    for hit in seed_docs:
        doc_id = hit["_id"]
        source = hit["_source"]

        candidate_urls = _load_candidate_urls(source, store)

        selected_url = None
        owner = None
        repo = None
        canonical_url = None

        for url in candidate_urls:
            result = canonicalize_github_repo_url(url)
            if result:
                owner, repo, canonical_url = result
                selected_url = url
                break

        if not canonical_url:
            store.update_document(
                collection_name="seed_item_index",
                doc_id=doc_id,
                body={
                    "status": "invalid",
                    "reason": "non_github_or_invalid_repository_url",
                    "raw_repository_urls": candidate_urls,
                },
            )
            continue

        store.update_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                "status": "normalized",
                "raw_repository_urls": candidate_urls,
                "selected_repository_url": selected_url,
                "owner": owner,
                "repo": repo,
                "canonical_repo_url": canonical_url,
            },
        )
