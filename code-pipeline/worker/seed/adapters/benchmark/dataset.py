import errno
import glob
import json
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypeVar

import requests

from worker.common.config import settings


T = TypeVar("T")
RETRYABLE_IO_ERRNOS = {errno.EDEADLK}


@dataclass
class RawBenchmarkDatasetRepoMetadata:
    source_name: str
    source_item_id: str
    raw_metadata: dict
    candidate_repo_urls: list[str]


class BenchmarkDatasetAdapter:
    def _run_io_with_retry(
        self,
        operation: Callable[[], T],
        retries: int = 5,
        delay_seconds: float = 0.05,
    ) -> T:
        last_error: OSError | None = None

        for attempt in range(retries):
            try:
                return operation()
            except OSError as exc:
                last_error = exc
                if exc.errno not in RETRYABLE_IO_ERRNOS or attempt == retries - 1:
                    raise
                time.sleep(delay_seconds * (attempt + 1))

        raise RuntimeError("unreachable") from last_error

    def _read_text_with_retry(self, path: Path) -> str:
        return self._run_io_with_retry(lambda: path.read_text(encoding="utf-8"))

    def _copy_to_local_scratch(self, source_path: Path, destination_path: Path) -> None:
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        self._run_io_with_retry(lambda: shutil.copyfile(source_path, destination_path))

    def _download_to_local_scratch(self, source_url: str, destination_path: Path) -> None:
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

    def _resolve_artifact_source_url(self, dataset_path: Path, dataset_config: dict) -> str | None:
        artifact_fetch = dataset_config.get("artifact_fetch") or {}
        artifacts = artifact_fetch.get("artifacts") or []
        benchmark_data_dir = Path(settings.benchmark_data_dir)

        try:
            relative_path = dataset_path.resolve().relative_to(benchmark_data_dir.resolve())
        except ValueError:
            relative_path = None

        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue

            target_relpath = str(artifact.get("target_relpath") or "").strip()
            source_url = str(artifact.get("source_url") or "").strip()
            if not target_relpath or not source_url:
                continue

            target_path = (benchmark_data_dir / target_relpath).resolve()
            if target_path == dataset_path.resolve():
                return source_url

            if relative_path is not None and Path(target_relpath) == relative_path:
                return source_url

        return None

    def build_repo_metadatas(
        self,
        dataset_name: str,
        dataset_config: dict,
    ) -> list[RawBenchmarkDatasetRepoMetadata]:
        benchmark_type = str(dataset_config.get("benchmark_type") or "").strip()
        dataset_format = str(dataset_config.get("dataset_format") or "").strip().lower()
        benchmark_name = dataset_config.get("benchmark_name") or dataset_name
        dataset_paths = self._resolve_dataset_paths(dataset_config)
        repo_field_paths = self._get_repo_field_paths(benchmark_type, dataset_config)
        record_id_field_paths = self._get_record_id_field_paths(benchmark_type, dataset_config)

        if not benchmark_type:
            raise ValueError(f"Benchmark dataset '{dataset_name}' is missing 'benchmark_type'")

        if dataset_format not in {"json", "jsonl", "parquet"}:
            raise ValueError(
                f"Benchmark dataset '{dataset_name}' has unsupported format '{dataset_format}'"
            )

        if not dataset_paths:
            raise FileNotFoundError(
                f"Benchmark dataset '{dataset_name}' has no matched dataset_paths"
            )

        repo_metadatas: list[RawBenchmarkDatasetRepoMetadata] = []

        for dataset_path in dataset_paths:
            records = self._load_records(
                dataset_path=dataset_path,
                dataset_format=dataset_format,
                dataset_config=dataset_config,
            )

            for index, record in enumerate(records):
                raw_repo_references = self._extract_field_values(record, repo_field_paths)
                candidate_repo_urls = self._normalize_repo_references(raw_repo_references)
                if not candidate_repo_urls:
                    continue

                record_id = self._extract_first_value(record, record_id_field_paths)
                record_key = str(record_id or f"{dataset_path.stem}:{index}")
                source_item_id = f"{dataset_name}:{record_key}"

                compact_payload = {
                    "source_type": "benchmark_dataset_repo",
                    "source_name": dataset_name,
                    "source_item_id": source_item_id,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "benchmark_name": benchmark_name,
                    "benchmark_type": benchmark_type,
                    "benchmark_version": dataset_config.get("benchmark_version"),
                    "split": dataset_config.get("split"),
                    "language": dataset_config.get("language"),
                    "task_type": dataset_config.get("task_type"),
                    "dataset_format": dataset_format,
                    "dataset_path": str(dataset_path),
                    "record_id": record_id,
                    "record_index": index,
                    "repo_field_paths": repo_field_paths,
                    "raw_repo_references": raw_repo_references,
                }

                repo_metadatas.append(
                    RawBenchmarkDatasetRepoMetadata(
                        source_name=dataset_name,
                        source_item_id=source_item_id,
                        raw_metadata=compact_payload,
                        candidate_repo_urls=candidate_repo_urls,
                    )
                )

        return repo_metadatas

    def _resolve_dataset_paths(self, dataset_config: dict) -> list[Path]:
        raw_paths = dataset_config.get("dataset_paths") or []
        if isinstance(raw_paths, str):
            raw_paths = [raw_paths]

        resolved_paths: list[Path] = []
        seen_paths = set()
        benchmark_data_dir = Path(settings.benchmark_data_dir)

        for raw_path in raw_paths:
            if not isinstance(raw_path, str) or not raw_path.strip():
                continue

            normalized_path = raw_path.strip()
            candidate_path = Path(normalized_path)
            if not candidate_path.is_absolute():
                normalized_path = str(benchmark_data_dir / normalized_path)

            matched_paths = sorted(glob.glob(normalized_path, recursive=True))
            for matched_path in matched_paths:
                if matched_path not in seen_paths:
                    seen_paths.add(matched_path)
                    resolved_paths.append(Path(matched_path))

        return resolved_paths

    def _load_records(
        self,
        dataset_path: Path,
        dataset_format: str,
        dataset_config: dict | None = None,
    ) -> list[dict]:
        dataset_config = dataset_config or {}

        if dataset_format == "json":
            payload = json.loads(self._read_text_with_retry(dataset_path))

            if isinstance(payload, list):
                return [item for item in payload if isinstance(item, dict)]

            if isinstance(payload, dict):
                for key in ["instances", "records", "examples", "items", "data", "rows"]:
                    value = payload.get(key)
                    if isinstance(value, list):
                        return [item for item in value if isinstance(item, dict)]

                return [payload]

            return []

        if dataset_format == "parquet":
            try:
                import pyarrow.parquet as pq
            except ImportError as exc:
                raise ImportError(
                    "pyarrow is required to read parquet benchmark datasets"
                ) from exc

            with tempfile.TemporaryDirectory(prefix="benchmark_parquet_") as tmp_dir:
                local_path = Path(tmp_dir) / dataset_path.name
                try:
                    self._copy_to_local_scratch(dataset_path, local_path)
                except OSError:
                    source_url = self._resolve_artifact_source_url(dataset_path, dataset_config)
                    if not source_url:
                        raise
                    self._download_to_local_scratch(source_url, local_path)
                table = self._run_io_with_retry(lambda: pq.read_table(str(local_path)))

            return [item for item in table.to_pylist() if isinstance(item, dict)]

        records = []
        for line in self._read_text_with_retry(dataset_path).splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            payload = json.loads(stripped)
            if isinstance(payload, dict):
                records.append(payload)

        return records

    def _get_repo_field_paths(self, benchmark_type: str, dataset_config: dict) -> list[str]:
        configured_paths = dataset_config.get("repo_field_paths")
        if isinstance(configured_paths, list):
            return [path for path in configured_paths if isinstance(path, str) and path.strip()]

        default_paths = {
            "swe_bench": ["repo"],
            "codesearchnet": ["repo", "repository"],
            "codexglue": ["repo", "repository", "repo_url", "metadata.repo"],
            "coir": ["repo", "repository", "repo_url", "metadata.repo"],
        }
        return default_paths.get(benchmark_type, ["repo", "repository", "repo_url"])

    def _get_record_id_field_paths(self, benchmark_type: str, dataset_config: dict) -> list[str]:
        configured_paths = dataset_config.get("record_id_field_paths")
        if isinstance(configured_paths, list):
            return [path for path in configured_paths if isinstance(path, str) and path.strip()]

        default_paths = {
            "swe_bench": ["instance_id"],
            "codesearchnet": ["sha", "identifier"],
            "codexglue": ["id", "idx", "identifier"],
            "coir": ["id", "doc_id", "identifier"],
        }
        return default_paths.get(benchmark_type, ["id", "identifier"])

    def _extract_first_value(self, record: dict, field_paths: list[str]) -> str | None:
        for field_path in field_paths:
            values = self._extract_field_values(record, [field_path])
            for value in values:
                if isinstance(value, str) and value.strip():
                    return value.strip()

        return None

    def _extract_field_values(self, record: dict, field_paths: list[str]) -> list[str]:
        values: list[str] = []
        seen = set()

        for field_path in field_paths:
            extracted = self._extract_path_value(record, field_path.split("."))
            flat_values = self._flatten_values(extracted)

            for value in flat_values:
                if isinstance(value, str):
                    normalized = value.strip()
                    if normalized and normalized not in seen:
                        seen.add(normalized)
                        values.append(normalized)

        return values

    def _extract_path_value(self, payload: object, path_parts: list[str]) -> object:
        current = payload

        for part in path_parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current

    def _flatten_values(self, payload: object) -> list[object]:
        if payload is None:
            return []

        if isinstance(payload, list):
            flattened = []
            for item in payload:
                flattened.extend(self._flatten_values(item))
            return flattened

        return [payload]

    def _normalize_repo_references(self, repo_references: list[str]) -> list[str]:
        normalized_urls: list[str] = []
        seen = set()

        for value in repo_references:
            normalized = self._normalize_repo_reference(value)
            if normalized and normalized not in seen:
                seen.add(normalized)
                normalized_urls.append(normalized)

        return normalized_urls

    def _normalize_repo_reference(self, value: str) -> str | None:
        candidate = value.strip()
        if not candidate:
            return None

        if candidate.startswith("https://github.com/") or candidate.startswith("http://github.com/"):
            return candidate

        if candidate.startswith("github.com/"):
            return f"https://{candidate}"

        if candidate.count("/") == 1 and " " not in candidate and "://" not in candidate:
            owner, repo = candidate.split("/", 1)
            if owner and repo:
                return f"https://github.com/{owner}/{repo}"

        return None
