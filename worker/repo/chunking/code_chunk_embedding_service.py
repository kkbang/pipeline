import hashlib
import logging
import math
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from urllib.parse import urlparse, urlunparse

import requests

from worker.common.config import settings


logger = logging.getLogger(__name__)


def _is_running_in_docker() -> bool:
    return os.path.exists("/.dockerenv")


def _resolve_embedding_endpoint_for_runtime(endpoint: str) -> str:
    normalized = str(endpoint or "").strip()
    if not normalized or not _is_running_in_docker():
        return normalized

    parsed = urlparse(normalized)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        return normalized

    host_gateway_name = (
        os.getenv("DOCKER_HOST_GATEWAY_NAME", "host.docker.internal").strip()
        or "host.docker.internal"
    )
    if not parsed.scheme:
        return normalized

    auth_part = ""
    if parsed.username:
        auth_part = parsed.username
        if parsed.password:
            auth_part = f"{auth_part}:{parsed.password}"
        auth_part = f"{auth_part}@"

    port_part = f":{parsed.port}" if parsed.port else ""
    rewritten = parsed._replace(netloc=f"{auth_part}{host_gateway_name}{port_part}")
    return urlunparse(rewritten)


@dataclass(slots=True)
class CodeChunkEmbeddingClient:
    enabled: bool = field(init=False)
    endpoint: str = field(init=False)
    api_key: str = field(init=False)
    model: str = field(init=False)
    batch_size: int = field(init=False)
    parallelism: int = field(init=False)
    timeout_seconds: int = field(init=False)
    fail_hard: bool = field(init=False)
    anonymized_dimensions: int = field(init=False)
    _cache: dict[tuple[str, str, str], list[float]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        raw_endpoint = settings.code_chunk_embedding_endpoint
        self.endpoint = _resolve_embedding_endpoint_for_runtime(raw_endpoint)
        self.api_key = settings.code_chunk_embedding_api_key
        self.model = settings.code_chunk_embedding_model
        self.batch_size = max(1, int(settings.code_chunk_embedding_batch_size))
        self.parallelism = max(1, int(settings.code_chunk_embedding_parallelism))
        self.timeout_seconds = max(1, int(settings.code_chunk_embedding_timeout_seconds))
        self.fail_hard = settings.code_chunk_embedding_fail_hard
        self.anonymized_dimensions = max(1, int(settings.code_chunk_anonymized_embedding_dimensions))
        self.enabled = bool(
            settings.code_chunk_embedding_enabled
            and self.endpoint
            and self.model
        )
        if self.endpoint != raw_endpoint:
            logger.info(
                "Rewrote code chunk embedding endpoint for container runtime: raw=%s resolved=%s",
                raw_endpoint,
                self.endpoint,
            )

    def enrich_documents(self, documents: list[tuple[str, dict]]) -> None:
        if not self.enabled or not documents:
            return

        self._populate_embedding_field(
            documents,
            text_field="anonymized_code",
            hash_field="anonymized_hash",
            embedding_field="anonymized_embedding",
            expected_dimensions=self.anonymized_dimensions,
        )

    def _populate_embedding_field(
        self,
        documents: list[tuple[str, dict]],
        *,
        text_field: str,
        hash_field: str,
        embedding_field: str,
        expected_dimensions: int,
    ) -> None:
        pending_items: list[tuple[str, str, str]] = []
        for _doc_id, source in documents:
            if embedding_field in source:
                continue
            text = self._build_embedding_text(source, text_field=text_field)
            if not text:
                continue
            language = str(source.get("language") or "python").strip().lower() or "python"
            hash_value = hashlib.sha1(text.encode("utf-8")).hexdigest()
            cache_key = (embedding_field, language, hash_value)
            cached_embedding = self._cache.get(cache_key)
            if cached_embedding is not None:
                source[embedding_field] = cached_embedding
                continue
            pending_items.append((language, hash_value, text))

        if not pending_items:
            return

        deduped_items_by_language: dict[str, list[tuple[str, str]]] = defaultdict(list)
        seen_keys: set[tuple[str, str]] = set()
        for language, hash_value, text in pending_items:
            dedupe_key = (language, hash_value)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            deduped_items_by_language[language].append((hash_value, text))

        for language, deduped_items in deduped_items_by_language.items():
            batches = [
                deduped_items[start : start + self.batch_size]
                for start in range(0, len(deduped_items), self.batch_size)
            ]
            if not batches:
                continue

            if len(batches) == 1 or self.parallelism == 1:
                for batch in batches:
                    texts = [text for _hash_value, text in batch]
                    embeddings = self._request_embeddings(
                        texts,
                        expected_dimensions=expected_dimensions,
                        language=language,
                    )
                    if embeddings is None:
                        continue
                    for (hash_value, _text), embedding in zip(batch, embeddings, strict=True):
                        self._cache[(embedding_field, language, hash_value)] = embedding
                continue

            max_workers = min(self.parallelism, len(batches))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        self._request_embeddings,
                        [text for _hash_value, text in batch],
                        expected_dimensions=expected_dimensions,
                        language=language,
                    ): batch
                    for batch in batches
                }
                for future in as_completed(futures):
                    embeddings = future.result()
                    if embeddings is None:
                        continue
                    batch = futures[future]
                    for (hash_value, _text), embedding in zip(batch, embeddings, strict=True):
                        self._cache[(embedding_field, language, hash_value)] = embedding

        for _doc_id, source in documents:
            text = self._build_embedding_text(source, text_field=text_field)
            if not text:
                continue
            language = str(source.get("language") or "python").strip().lower() or "python"
            hash_value = hashlib.sha1(text.encode("utf-8")).hexdigest()
            embedding = self._cache.get((embedding_field, language, hash_value))
            if embedding is not None:
                source[embedding_field] = embedding

    def _build_embedding_text(self, source: dict, *, text_field: str) -> str:
        code = str(source.get(text_field) or "").strip()
        return code

    def _request_embeddings(
        self,
        texts: list[str],
        *,
        expected_dimensions: int,
        language: str = "python",
    ) -> list[list[float]] | None:
        try:
            headers = {
                "Content-Type": "application/json",
            }
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            response = requests.post(
                self.endpoint,
                headers=headers,
                json={
                    "model": self.model,
                    "input": [
                        {
                            "code": text,
                            "language": language,
                        }
                        for text in texts
                    ],
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            data = payload.get("data")
            if not isinstance(data, list):
                raise ValueError("embedding response missing data list")
            if all(isinstance(item, dict) and isinstance(item.get("index"), int) for item in data):
                data = sorted(data, key=lambda item: item["index"])
            embeddings: list[list[float]] = []
            for item in data:
                if not isinstance(item, dict):
                    raise ValueError("embedding item is not an object")
                embedding = item.get("embedding")
                if not isinstance(embedding, list):
                    raise ValueError("embedding item missing vector")
                normalized_embedding = [float(value) for value in embedding]
                if not all(math.isfinite(value) for value in normalized_embedding):
                    raise ValueError("embedding contains non-finite values")
                if len(normalized_embedding) != expected_dimensions:
                    raise ValueError(
                        f"embedding dimension mismatch: expected={expected_dimensions} actual={len(normalized_embedding)}"
                    )
                embeddings.append(normalized_embedding)
            if len(embeddings) != len(texts):
                raise ValueError(
                    f"embedding response count mismatch: expected={len(texts)} actual={len(embeddings)}"
                )
            return embeddings
        except Exception as exc:  # noqa: BLE001 - fail-soft configurable
            if self.fail_hard:
                raise
            logger.warning("Code chunk embedding request failed: %s", str(exc))
            return None


_EMBEDDING_CLIENT: CodeChunkEmbeddingClient | None = None


def get_code_chunk_embedding_client() -> CodeChunkEmbeddingClient:
    global _EMBEDDING_CLIENT
    if _EMBEDDING_CLIENT is None:
        _EMBEDDING_CLIENT = CodeChunkEmbeddingClient()
    return _EMBEDDING_CLIENT
