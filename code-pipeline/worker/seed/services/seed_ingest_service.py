from worker.common.config import settings
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

        full_raw_metadata_path = None
        if settings.keep_full_package_registry_raw and metadata.full_raw_metadata is not None:
            full_raw_metadata_path = f"raw/package_registry_full/{registry}/{package_name}.json"
            store.put_json(full_raw_metadata_path, metadata.full_raw_metadata)

        doc_id = f"{registry}:{package_name}"
        store.upsert_document(
            collection_name="seed_item_index",
            doc_id=doc_id,
            body={
                "registry_name": registry,
                "package_name": package_name,
                "package_version": metadata.package_version,
                "raw_metadata_path": raw_metadata_path,
                "candidate_repo_urls": metadata.candidate_repo_urls,
                "status": "ingested",
            },
        )

        if full_raw_metadata_path:
            store.update_document(
                collection_name="seed_item_index",
                doc_id=doc_id,
                body={
                    "full_raw_metadata_path": full_raw_metadata_path,
                },
            )
