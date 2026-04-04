from worker.seed.resolvers.github_url_canonicalizer import canonicalize_github_repo_url
from worker.storage.local_json_store import LocalJsonStore
# from worker.storage.s3_store import S3Store
# from worker.storage.opensearch_store import OpenSearchStore

def _load_candidate_urls(source: dict) -> list[str]:
    candidate_urls = source.get("candidate_repo_urls")
    if isinstance(candidate_urls, list):
        return [url for url in candidate_urls if isinstance(url, str) and url.strip()]

    return []

def repo_crawler():
  store = LocalJsonStore()
    # s3 = S3Store()
    # os = OpenSearchStore()
  repo_docs = store.find_documents_by_status(collection_name="repo_registry_index", status="scheduled")
  for hit in repo_docs:
    doc_id = hit["_id"]
    source = hit["_source"]
    candidate_urls = _load_candidate_urls(source)
  


