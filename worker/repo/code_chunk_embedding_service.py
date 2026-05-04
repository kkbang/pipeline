import logging
from dataclasses import dataclass, field

import requests

from worker.common.config import settings


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CodeChunkEmbeddingClient:
    enabled: bool = field(init=False)
    endpoint: str = field(init=False)
    api_key: str = field(init=False)
    model: str = field(init=False)
    batch_size: int = field(init=False)
    timeout_seconds: int = field(init=False)
    fail_hard: bool = field(init=False)
    raw_dimensions: int = field(init=False)
    anonymized_dimensions: int = field(init=False)
    _cache: dict[tuple[str, str], list[float]] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.endpoint = settings.code_chunk_embedding_endpoint
        self.api_key = settings.code_chunk_embedding_api_key
        self.model = settings.code_chunk_embedding_model
        self.batch_size = max(1, int(settings.code_chunk_embedding_batch_size))
        self.timeout_seconds = max(1, int(settings.code_chunk_embedding_timeout_seconds))
        self.fail_hard = settings.code_chunk_embedding_fail_hard
        self.raw_dimensions = max(1, int(settings.code_chunk_raw_embedding_dimensions))
        self.anonymized_dimensions = max(1, int(settings.code_chunk_anonymized_embedding_dimensions))
        self.enabled = bool(
            settings.code_chunk_embedding_enabled
            and self.endpoint
            and self.model
        )

    def enrich_documents(self, documents: list[tuple[str, dict]]) -> None:
        if not self.enabled or not documents:
            return

        self._populate_embedding_field(
            documents,
            text_field="raw_code",
            hash_field="raw_hash",
            embedding_field="raw_embedding",
            expected_dimensions=self.raw_dimensions,
        )
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
            text = str(source.get(text_field) or "").strip()
            hash_value = str(source.get(hash_field) or "").strip()
            if not text or not hash_value:
                continue
            cache_key = (embedding_field, hash_value)
            cached_embedding = self._cache.get(cache_key)
            if cached_embedding is not None:
                source[embedding_field] = cached_embedding
                continue
            pending_items.append((hash_value, text, cache_key[0]))

        if not pending_items:
            return

        deduped_items: list[tuple[str, str]] = []
        seen_hashes: set[str] = set()
        for hash_value, text, _embedding_field_name in pending_items:
            if hash_value in seen_hashes:
                continue
            seen_hashes.add(hash_value)
            deduped_items.append((hash_value, text))

        for start in range(0, len(deduped_items), self.batch_size):
            batch = deduped_items[start : start + self.batch_size]
            texts = [text for _hash_value, text in batch]
            embeddings = self._request_embeddings(texts, expected_dimensions=expected_dimensions)
            if embeddings is None:
                continue
            for (hash_value, _text), embedding in zip(batch, embeddings, strict=True):
                self._cache[(embedding_field, hash_value)] = embedding

        for _doc_id, source in documents:
            hash_value = str(source.get(hash_field) or "").strip()
            if not hash_value:
                continue
            embedding = self._cache.get((embedding_field, hash_value))
            if embedding is not None:
                source[embedding_field] = embedding

    def _request_embeddings(
        self,
        texts: list[str],
        *,
        expected_dimensions: int,
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
                    "input": texts,
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
