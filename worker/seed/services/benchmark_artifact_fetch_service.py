import gzip
import shutil
import tarfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests

from worker.common.config import settings
from worker.storage.opensearch_store import OpenSearchStore


def _safe_extract_archive_path(base_dir: Path, member_name: str) -> Path:
    target_path = (base_dir / member_name).resolve()
    base_path = base_dir.resolve()

    if not str(target_path).startswith(str(base_path)):
        raise ValueError(f"Unsafe archive member path detected: {member_name}")

    return target_path


def _stream_download(source_url: str, destination_path: Path) -> None:
    parsed = urlparse(source_url)

    if parsed.scheme == "file":
        source_path = Path(parsed.path)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, destination_path)
        return

    response = requests.get(
        source_url,
        stream=True,
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("wb") as output:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                output.write(chunk)


def _extract_zip(archive_path: Path, extract_dir: Path) -> None:
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            target_path = _safe_extract_archive_path(extract_dir, member.filename)
            if member.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target_path.open("wb") as destination:
                shutil.copyfileobj(source, destination)


def _extract_tar(archive_path: Path, extract_dir: Path, mode: str) -> None:
    with tarfile.open(archive_path, mode) as archive:
        for member in archive.getmembers():
            target_path = _safe_extract_archive_path(extract_dir, member.name)
            if member.isdir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue

            extracted = archive.extractfile(member)
            if extracted is None:
                continue

            target_path.parent.mkdir(parents=True, exist_ok=True)
            with extracted, target_path.open("wb") as destination:
                shutil.copyfileobj(extracted, destination)


def _extract_gz(archive_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(archive_path, "rb") as source, target_path.open("wb") as destination:
        shutil.copyfileobj(source, destination)


def _materialize_artifact(dataset_name: str, artifact_config: dict, benchmark_data_dir: Path) -> dict:
    source_url = str(artifact_config.get("source_url") or "").strip()
    if not source_url:
        raise ValueError(f"Benchmark dataset '{dataset_name}' artifact is missing 'source_url'")

    archive_format = str(artifact_config.get("archive_format") or "none").strip().lower()
    force_download = artifact_config.get("force_download") is True
    artifact_name = str(artifact_config.get("artifact_name") or "").strip()
    parsed = urlparse(source_url)
    source_name = artifact_name or Path(parsed.path).name or "artifact"

    download_dir = benchmark_data_dir / "_downloads" / dataset_name
    download_path = download_dir / source_name

    target_relpath = artifact_config.get("target_relpath")
    extract_to_relpath = artifact_config.get("extract_to_relpath")

    if archive_format == "none":
        if not isinstance(target_relpath, str) or not target_relpath.strip():
            raise ValueError(
                f"Benchmark dataset '{dataset_name}' direct artifact requires 'target_relpath'"
            )

        target_path = benchmark_data_dir / target_relpath.strip()
        if target_path.exists() and not force_download:
            return {
                "source_url": source_url,
                "archive_format": archive_format,
                "materialized_paths": [str(target_path)],
                "skipped": True,
            }

        _stream_download(source_url, target_path)
        return {
            "source_url": source_url,
            "archive_format": archive_format,
            "materialized_paths": [str(target_path)],
            "skipped": False,
        }

    if not isinstance(extract_to_relpath, str) or not extract_to_relpath.strip():
        raise ValueError(
            f"Benchmark dataset '{dataset_name}' archive artifact requires 'extract_to_relpath'"
        )

    extract_dir = benchmark_data_dir / extract_to_relpath.strip()
    if extract_dir.exists() and any(extract_dir.rglob("*")) and not force_download:
        return {
            "source_url": source_url,
            "archive_format": archive_format,
            "materialized_paths": [str(extract_dir)],
            "skipped": True,
        }

    _stream_download(source_url, download_path)

    if archive_format == "zip":
        _extract_zip(download_path, extract_dir)
    elif archive_format == "tar":
        _extract_tar(download_path, extract_dir, "r:")
    elif archive_format in {"tar.gz", "tgz"}:
        _extract_tar(download_path, extract_dir, "r:gz")
    elif archive_format == "gz":
        if not isinstance(target_relpath, str) or not target_relpath.strip():
            raise ValueError(
                f"Benchmark dataset '{dataset_name}' gzip artifact requires 'target_relpath'"
            )
        target_path = benchmark_data_dir / target_relpath.strip()
        _extract_gz(download_path, target_path)
        return {
            "source_url": source_url,
            "archive_format": archive_format,
            "materialized_paths": [str(target_path)],
            "skipped": False,
        }
    else:
        raise ValueError(
            f"Benchmark dataset '{dataset_name}' has unsupported archive_format '{archive_format}'"
        )

    return {
        "source_url": source_url,
        "archive_format": archive_format,
        "materialized_paths": [str(extract_dir)],
        "skipped": False,
    }


def run_benchmark_artifact_fetch(dataset_name: str, dataset_config: dict | None = None) -> None:
    if not dataset_config:
        return

    artifact_fetch = dataset_config.get("artifact_fetch") or {}
    if artifact_fetch.get("enabled") is not True:
        return

    artifacts = artifact_fetch.get("artifacts") or []
    if not isinstance(artifacts, list) or not artifacts:
        return

    benchmark_data_dir = Path(settings.benchmark_data_dir)
    benchmark_data_dir.mkdir(parents=True, exist_ok=True)
    store = OpenSearchStore()

    materialized_artifacts = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue

        materialized_artifacts.append(
            _materialize_artifact(
                dataset_name=dataset_name,
                artifact_config=artifact,
                benchmark_data_dir=benchmark_data_dir,
            )
        )

    store.upsert_document(
        collection_name="benchmark_artifact_fetch_index",
        doc_id=dataset_name,
        body={
            "dataset_name": dataset_name,
            "artifact_fetch": artifact_fetch,
            "materialized_artifacts": materialized_artifacts,
        },
    )
