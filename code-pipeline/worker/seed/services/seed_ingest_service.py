import re

from worker.common.config import settings
from worker.seed.adapters.benchmark_dataset import BenchmarkDatasetAdapter
from worker.seed.adapters.curated_repo import CuratedRepoListAdapter
from worker.seed.adapters.github_org import GitHubOrgAdapter
from worker.seed.adapters.npm import NPMAdapter
from worker.seed.adapters.pypi import PyPIAdapter
from worker.storage.local_json_store import LocalJsonStore
# from worker.storage.s3_store import S3Store
# from worker.storage.opensearch_store import OpenSearchStore


def _get_package_registry_adapter(registry: str) -> PyPIAdapter | NPMAdapter:
    if registry == "pypi":
        return PyPIAdapter()

    if registry == "npm":
        return NPMAdapter()

    raise ValueError(f"Unsupported package registry: {registry}")


def _safe_path_fragment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")


def run_seed_ingestion(registry: str = "pypi", package_names: list[str] | None = None) -> None:
    adapter = _get_package_registry_adapter(registry)
    store = LocalJsonStore()
    # s3 = S3Store()
    # os = OpenSearchStore()

    if not package_names:
        if registry == "pypi":
            package_names = ["requests", "flask", "django", "fastapi", "numpy", "pandas"]
        elif registry == "npm":
            package_names = ["react", "express", "lodash", "axios", "typescript", "vite"]
        else:
            package_names = []

    for package_name in package_names:
        metadata = adapter.fetch_package_metadata(package_name)

        raw_metadata_path = f"raw/package_registry/{registry}/{package_name}.json"
        store.put_json(raw_metadata_path, metadata.raw_metadata)

        if settings.keep_full_package_registry_raw and metadata.full_raw_metadata is not None:
            full_raw_metadata_path = f"raw/package_registry_full/{registry}/{package_name}.json"
            store.put_json(full_raw_metadata_path, metadata.full_raw_metadata)

        doc_id = f"{registry}:{package_name}"
        store.upsert_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                "source_type": "package_registry_repo",
                "source_name": registry,
                "source_item_id": doc_id,
                "raw_metadata_path": raw_metadata_path,
                "candidate_repo_urls": metadata.candidate_repo_urls,
                "status": "ingested",
            },
        )


def run_curated_repo_ingestion(list_name: str, repo_entries: list[str | dict] | None = None) -> None:
    adapter = CuratedRepoListAdapter()
    store = LocalJsonStore()

    if not repo_entries:
        return

    for repo_entry in repo_entries:
        metadata = adapter.build_repo_metadata(list_name, repo_entry)

        raw_metadata_path = (
            f"raw/curated_repo_list/{_safe_path_fragment(list_name)}/"
            f"{_safe_path_fragment(metadata.source_item_id)}.json"
        )
        store.put_json(raw_metadata_path, metadata.raw_metadata)

        doc_id = f"curated_repo_list:{list_name}:{metadata.source_item_id}"
        store.upsert_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                "source_type": "curated_repo_list",
                "source_name": list_name,
                "source_item_id": metadata.source_item_id,
                "raw_metadata_path": raw_metadata_path,
                "candidate_repo_urls": metadata.candidate_repo_urls,
                "status": "ingested",
            },
        )


def run_benchmark_dataset_ingestion(dataset_name: str, dataset_config: dict | None = None) -> None:
    adapter = BenchmarkDatasetAdapter()
    store = LocalJsonStore()

    if not dataset_config:
        return

    for metadata in adapter.build_repo_metadatas(dataset_name, dataset_config):

        raw_metadata_path = (
            f"raw/benchmark_dataset/{_safe_path_fragment(dataset_name)}/"
            f"{_safe_path_fragment(metadata.source_item_id)}.json"
        )
        store.put_json(raw_metadata_path, metadata.raw_metadata)

        doc_id = f"benchmark_dataset_repo:{dataset_name}:{metadata.source_item_id}"
        store.upsert_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                "source_type": "benchmark_dataset_repo",
                "source_name": dataset_name,
                "source_item_id": metadata.source_item_id,
                "raw_metadata_path": raw_metadata_path,
                "candidate_repo_urls": metadata.candidate_repo_urls,
                "status": "ingested",
            },
        )


def run_github_org_ingestion(list_name: str, org_names: list[str] | None = None) -> None:
    adapter = GitHubOrgAdapter()
    store = LocalJsonStore()

    if not org_names:
        return

    for org_name in org_names:
        repo_metadatas = adapter.fetch_org_repositories(list_name=list_name, org_name=org_name)

        for metadata in repo_metadatas:
            raw_metadata_path = (
                f"raw/github_org/{_safe_path_fragment(list_name)}/"
                f"{_safe_path_fragment(org_name)}/"
                f"{_safe_path_fragment(metadata.repo_name)}.json"
            )
            store.put_json(raw_metadata_path, metadata.raw_metadata)

            doc_id = f"org_repo:{list_name}:{metadata.source_item_id}"
            store.upsert_document(
                collection_name="seed_item_index",
                doc_id=doc_id,
                body={
                    "source_type": "org_repo",
                    "source_name": list_name,
                    "source_item_id": metadata.source_item_id,
                    "raw_metadata_path": raw_metadata_path,
                    "candidate_repo_urls": metadata.candidate_repo_urls,
                    "status": "ingested",
                },
            )
