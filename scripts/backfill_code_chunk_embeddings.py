#!/usr/bin/env python3
import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file(PROJECT_ROOT / ".env")

from worker.common.config import settings
from worker.repo.code_chunk_document import CODE_CHUNK_INDEX_ALIAS, VALID_CHUNK_VALIDATION_STATUSES
from worker.repo.code_chunk_embedding_service import CodeChunkEmbeddingClient


CODE_CHUNK_INDEX = CODE_CHUNK_INDEX_ALIAS
CODE_CHUNK_EMBEDDING_INDEX = (
    f"{settings.code_chunk_embedding_index_alias}_{settings.code_chunk_embedding_index_version}"
)
EMBEDDING_BACKFILL_ALLOWED_LANGUAGES = (
    "python",
    "java",
    "javascript",
    "go",
)
EMBEDDING_BACKFILL_PER_LANGUAGE_LIMIT = 50
EMBEDDING_BACKFILL_MIN_RAW_CODE_LINES = 10
EMBEDDING_BACKFILL_SOURCE_FIELDS = [
    "repo_id",
    "owner",
    "repo_name",
    "snapshot_ref",
    "language",
    "file_path",
    "chunk_type",
    "start_line",
    "end_line",
    "symbol_type",
    "symbol_name",
    "raw_code",
    "raw_hash",
    "anonymized_code",
    "anonymized_hash",
    "chunked_at",
]
EMBEDDING_BACKFILL_SCAN_SIZE = 200
EMBEDDING_BACKFILL_WRITE_BATCH_SIZE = 100
# Keep one invocation bounded to a single fixed-size slice.
EMBEDDING_BACKFILL_RUN_LIMIT = 200
OPENSEARCH_SCROLL_TTL = "2m"


@dataclass(slots=True)
class OpenSearchHttpConfig:
    base_url: str
    auth: tuple[str, str] | None
    verify: bool
    timeout_seconds: int


class OpenSearchHttpStore:
    def __init__(self) -> None:
        scheme = "https" if settings.opensearch_use_ssl else "http"
        host = str(settings.opensearch_host or "").strip()
        if not host:
            raise RuntimeError("OPENSEARCH_HOST is required for embedding backfill")

        auth = None
        if settings.opensearch_user and settings.opensearch_password:
            auth = (settings.opensearch_user, settings.opensearch_password)

        self.config = OpenSearchHttpConfig(
            base_url=f"{scheme}://{host}:{settings.opensearch_port}",
            auth=auth,
            verify=bool(settings.opensearch_use_ssl),
            timeout_seconds=max(1, int(settings.request_timeout_seconds)),
        )

    def _index_spec_dir(self) -> Path:
        return PROJECT_ROOT / "infra" / "opensearch_index"

    def _load_index_spec(self, index_name: str) -> dict[str, Any] | None:
        spec_path = self._index_spec_dir() / f"{index_name}.json"
        if not spec_path.exists():
            return None

        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        properties = spec.get("mappings", {}).get("properties", {})
        raw_embedding = properties.get("raw_embedding")
        anonymized_embedding = properties.get("anonymized_embedding")
        if isinstance(raw_embedding, dict):
            raw_embedding["dimension"] = max(1, settings.code_chunk_raw_embedding_dimensions)
        if isinstance(anonymized_embedding, dict):
            anonymized_embedding["dimension"] = max(
                1,
                settings.code_chunk_anonymized_embedding_dimensions,
            )
        return spec

    def _request(
        self,
        *,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        data_body: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = requests.request(
            method=method,
            url=f"{self.config.base_url}{path}",
            auth=self.config.auth,
            json=json_body,
            data=data_body,
            headers=headers,
            timeout=self.config.timeout_seconds,
            verify=self.config.verify,
        )
        response.raise_for_status()
        if not response.text.strip():
            return {}
        return dict(response.json())

    def ensure_index_created(self, index_name: str) -> None:
        spec = self._load_index_spec(index_name)
        if spec is None:
            raise RuntimeError(f"OpenSearch index spec not found for '{index_name}'")

        response = requests.request(
            method="PUT",
            url=f"{self.config.base_url}/{index_name}",
            auth=self.config.auth,
            json=spec,
            timeout=self.config.timeout_seconds,
            verify=self.config.verify,
        )
        if response.status_code in (200, 201):
            return
        if response.status_code == 400:
            payload = response.json()
            error = payload.get("error")
            if isinstance(error, dict) and error.get("type") == "resource_already_exists_exception":
                return
            if "resource_already_exists_exception" in response.text:
                return
        response.raise_for_status()

    def delete_index(self, index_name: str) -> None:
        response = requests.request(
            method="DELETE",
            url=f"{self.config.base_url}/{index_name}",
            auth=self.config.auth,
            timeout=self.config.timeout_seconds,
            verify=self.config.verify,
        )
        if response.status_code in (200, 202, 404):
            return
        response.raise_for_status()

    def iterate_documents_by_query(
        self,
        collection_name: str,
        query: dict[str, Any] | None = None,
        *,
        size: int = 1000,
        source_includes: list[str] | None = None,
    ):
        scroll_id: str | None = None
        try:
            body: dict[str, Any] = {
                "size": max(1, int(size)),
                "sort": ["_doc"],
                "query": query or {"match_all": {}},
            }
            if source_includes is not None:
                body["_source"] = source_includes

            result = self._request(
                method="POST",
                path=f"/{collection_name}/_search?scroll={OPENSEARCH_SCROLL_TTL}",
                json_body=body,
            )
            scroll_id = str(result.get("_scroll_id") or "").strip() or None

            while True:
                hits = result.get("hits", {}).get("hits", [])
                if not hits:
                    return

                for hit in hits:
                    yield hit

                if not scroll_id:
                    return

                result = self._request(
                    method="POST",
                    path="/_search/scroll",
                    json_body={
                        "scroll": OPENSEARCH_SCROLL_TTL,
                        "scroll_id": scroll_id,
                    },
                )
                scroll_id = str(result.get("_scroll_id") or scroll_id).strip() or scroll_id
        finally:
            if scroll_id:
                try:
                    self._request(
                        method="DELETE",
                        path="/_search/scroll",
                        json_body={"scroll_id": [scroll_id]},
                    )
                except Exception:
                    pass

    def get_existing_document_ids(
        self,
        collection_name: str,
        doc_ids: list[str],
    ) -> set[str]:
        normalized_ids = [str(doc_id).strip() for doc_id in doc_ids if str(doc_id).strip()]
        if not normalized_ids:
            return set()

        result = self._request(
            method="POST",
            path=f"/{collection_name}/_mget",
            json_body={"ids": normalized_ids},
        )
        existing_ids: set[str] = set()
        for doc in result.get("docs") or []:
            if doc.get("found"):
                doc_id = str(doc.get("_id") or "").strip()
                if doc_id:
                    existing_ids.add(doc_id)
        return existing_ids

    def bulk_upsert_documents(
        self,
        collection_name: str,
        documents: list[tuple[str, dict[str, Any]]],
        *,
        refresh: bool = False,
        chunk_size: int = 500,
    ) -> None:
        if not documents:
            return

        safe_chunk_size = max(1, int(chunk_size))
        for start in range(0, len(documents), safe_chunk_size):
            batch = documents[start : start + safe_chunk_size]
            lines: list[str] = []
            for doc_id, body in batch:
                lines.append(
                    json.dumps(
                        {
                            "update": {
                                "_index": collection_name,
                                "_id": doc_id,
                            }
                        },
                        ensure_ascii=False,
                    )
                )
                lines.append(
                    json.dumps(
                        {
                            "doc": body,
                            "doc_as_upsert": True,
                        },
                        ensure_ascii=False,
                    )
                )

            result = self._request(
                method="POST",
                path=f"/_bulk?refresh={'true' if refresh else 'false'}",
                data_body="\n".join(lines) + "\n",
                headers={"Content-Type": "application/x-ndjson"},
            )
            if result.get("errors"):
                items = result.get("items") or []
                raise RuntimeError(
                    f"OpenSearch bulk upsert failed for {collection_name}: sample={items[:3]}"
                )


@dataclass(slots=True)
class EmbeddingBackfillStats:
    seen_count: int = 0
    updated_doc_count: int = 0
    raw_embedding_written_count: int = 0
    anonymized_embedding_written_count: int = 0
    skipped_noop_count: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "seen_count": self.seen_count,
            "updated_doc_count": self.updated_doc_count,
            "raw_embedding_written_count": self.raw_embedding_written_count,
            "anonymized_embedding_written_count": self.anonymized_embedding_written_count,
            "skipped_noop_count": self.skipped_noop_count,
        }


def _repo_id_filter(repo_id: str) -> dict[str, Any]:
    return {
        "bool": {
            "should": [
                {"term": {"repo_id.keyword": repo_id}},
                {"term": {"repo_id": repo_id}},
            ],
            "minimum_should_match": 1,
        }
    }


def _build_embedding_backfill_query(
    *,
    repo_id: str | None = None,
    force_reembed: bool = False,
    language: str | None = None,
) -> dict[str, Any]:
    must_clauses: list[dict[str, Any]] = [
        {"terms": {"validation_status": list(VALID_CHUNK_VALIDATION_STATUSES)}},
    ]
    if language:
        must_clauses.append({"term": {"language": language}})
    if repo_id:
        must_clauses.append(_repo_id_filter(repo_id))

    query: dict[str, Any] = {
        "bool": {
            "must": must_clauses,
        }
    }
    return query


def _prepare_enrichment_source(
    source: dict[str, Any],
    *,
    force_reembed: bool,
) -> dict[str, Any]:
    _ = force_reembed
    return {
        "raw_code": source.get("raw_code"),
        "raw_hash": source.get("raw_hash"),
        "anonymized_code": source.get("anonymized_code"),
        "anonymized_hash": source.get("anonymized_hash"),
    }


def _raw_code_line_count(source: dict[str, Any]) -> int:
    raw_code = str(source.get("raw_code") or "")
    if not raw_code.strip():
        return 0
    return len(raw_code.splitlines())


def _is_eligible_source_chunk(source: dict[str, Any]) -> bool:
    return _raw_code_line_count(source) >= EMBEDDING_BACKFILL_MIN_RAW_CODE_LINES


def _filter_missing_embedding_docs(
    store: Any,
    docs: list[tuple[str, dict[str, Any]]],
) -> list[tuple[str, dict[str, Any]]]:
    if not docs:
        return []
    if not hasattr(store, "get_existing_document_ids"):
        return docs

    existing_ids = store.get_existing_document_ids(
        CODE_CHUNK_EMBEDDING_INDEX,
        [doc_id for doc_id, _source in docs],
    )
    if not existing_ids:
        return docs
    return [
        (doc_id, source)
        for doc_id, source in docs
        if doc_id not in existing_ids
    ]


def _flush_embedding_backfill_batch(
    *,
    store: Any,
    embedding_client: CodeChunkEmbeddingClient,
    batch_docs: list[tuple[str, dict[str, Any]]],
    force_reembed: bool,
    refresh_writes: bool,
    write_batch_size: int,
    stats: EmbeddingBackfillStats,
) -> None:
    if not batch_docs:
        return

    docs_for_enrich = [
        (doc_id, _prepare_enrichment_source(source, force_reembed=force_reembed))
        for doc_id, source in batch_docs
    ]
    embedding_client.enrich_documents(docs_for_enrich)

    embedded_at = datetime.now(timezone.utc).isoformat()
    update_docs: list[tuple[str, dict[str, Any]]] = []
    for (doc_id, original_source), (_same_doc_id, enriched_source) in zip(
        batch_docs,
        docs_for_enrich,
        strict=True,
    ):
        update_source: dict[str, Any] = {
            "chunk_id": doc_id,
            "repo_id": original_source.get("repo_id"),
            "owner": original_source.get("owner"),
            "repo_name": original_source.get("repo_name"),
            "snapshot_ref": original_source.get("snapshot_ref"),
            "language": original_source.get("language"),
            "file_path": original_source.get("file_path"),
            "chunk_type": original_source.get("chunk_type"),
            "start_line": original_source.get("start_line"),
            "end_line": original_source.get("end_line"),
            "symbol_type": original_source.get("symbol_type"),
            "symbol_name": original_source.get("symbol_name"),
            "raw_hash": original_source.get("raw_hash"),
            "anonymized_hash": original_source.get("anonymized_hash"),
            "embedding_model": embedding_client.model,
            "chunked_at": original_source.get("chunked_at"),
            "embedded_at": embedded_at,
        }
        wrote_embedding = False

        raw_embedding = enriched_source.get("raw_embedding")
        if raw_embedding is not None:
            update_source["raw_embedding"] = raw_embedding
            stats.raw_embedding_written_count += 1
            wrote_embedding = True

        anonymized_embedding = enriched_source.get("anonymized_embedding")
        if anonymized_embedding is not None:
            update_source["anonymized_embedding"] = anonymized_embedding
            stats.anonymized_embedding_written_count += 1
            wrote_embedding = True

        if wrote_embedding:
            update_docs.append((doc_id, update_source))
        else:
            stats.skipped_noop_count += 1

    if not update_docs:
        return

    store.bulk_upsert_documents(
        collection_name=CODE_CHUNK_EMBEDDING_INDEX,
        documents=update_docs,
        refresh=refresh_writes,
        chunk_size=max(1, write_batch_size),
    )
    stats.updated_doc_count += len(update_docs)


def backfill_code_chunk_embeddings(
    *,
    store: Any | None = None,
    embedding_client: CodeChunkEmbeddingClient | None = None,
    repo_id: str | None = None,
    scan_size: int = 500,
    write_batch_size: int | None = None,
    limit: int = 0,
    force_reembed: bool = False,
    refresh_writes: bool = False,
) -> EmbeddingBackfillStats:
    resolved_store = store or OpenSearchHttpStore()
    if hasattr(resolved_store, "ensure_index_created"):
        resolved_store.ensure_index_created(CODE_CHUNK_EMBEDDING_INDEX)
    resolved_embedding_client = embedding_client or CodeChunkEmbeddingClient()
    if not resolved_embedding_client.enabled:
        raise RuntimeError(
            "code chunk embedding client is not enabled; set CODE_CHUNK_EMBEDDING_ENABLED, "
            "CODE_CHUNK_EMBEDDING_ENDPOINT, and CODE_CHUNK_EMBEDDING_MODEL"
        )

    safe_scan_size = max(1, int(scan_size))
    safe_write_batch_size = max(
        1,
        int(write_batch_size or settings.opensearch_bulk_flush_docs),
    )
    stats = EmbeddingBackfillStats()
    pending_batch: list[tuple[str, dict[str, Any]]] = []
    candidate_batch: list[tuple[str, dict[str, Any]]] = []
    stop_processing = False
    for language in EMBEDDING_BACKFILL_ALLOWED_LANGUAGES:
        if stop_processing:
            break

        language_seen_count = 0
        query = _build_embedding_backfill_query(
            repo_id=repo_id,
            force_reembed=force_reembed,
            language=language,
        )

        for hit in resolved_store.iterate_documents_by_query(
            collection_name=CODE_CHUNK_INDEX,
            query=query,
            size=safe_scan_size,
            source_includes=EMBEDDING_BACKFILL_SOURCE_FIELDS,
        ):
            doc_id = str(hit.get("_id") or "").strip()
            if not doc_id:
                continue

            source = dict(hit.get("_source", {}))
            if not _is_eligible_source_chunk(source):
                continue

            candidate_batch.append((doc_id, source))

            should_resolve_candidates = (
                len(candidate_batch) >= safe_write_batch_size
                or language_seen_count + len(candidate_batch) >= EMBEDDING_BACKFILL_PER_LANGUAGE_LIMIT
            )
            if not should_resolve_candidates:
                continue

            for missing_doc in _filter_missing_embedding_docs(resolved_store, candidate_batch):
                pending_batch.append(missing_doc)
                stats.seen_count += 1
                language_seen_count += 1

                if len(pending_batch) >= safe_write_batch_size:
                    _flush_embedding_backfill_batch(
                        store=resolved_store,
                        embedding_client=resolved_embedding_client,
                        batch_docs=pending_batch,
                        force_reembed=force_reembed,
                        refresh_writes=refresh_writes,
                        write_batch_size=safe_write_batch_size,
                        stats=stats,
                    )
                    pending_batch.clear()

                if language_seen_count >= EMBEDDING_BACKFILL_PER_LANGUAGE_LIMIT:
                    break
                if limit > 0 and stats.seen_count >= limit:
                    stop_processing = True
                    break

            candidate_batch.clear()

            if stop_processing or language_seen_count >= EMBEDDING_BACKFILL_PER_LANGUAGE_LIMIT:
                break

        if candidate_batch and not stop_processing and language_seen_count < EMBEDDING_BACKFILL_PER_LANGUAGE_LIMIT:
            for missing_doc in _filter_missing_embedding_docs(resolved_store, candidate_batch):
                pending_batch.append(missing_doc)
                stats.seen_count += 1
                language_seen_count += 1

                if len(pending_batch) >= safe_write_batch_size:
                    _flush_embedding_backfill_batch(
                        store=resolved_store,
                        embedding_client=resolved_embedding_client,
                        batch_docs=pending_batch,
                        force_reembed=force_reembed,
                        refresh_writes=refresh_writes,
                        write_batch_size=safe_write_batch_size,
                        stats=stats,
                    )
                    pending_batch.clear()

                if language_seen_count >= EMBEDDING_BACKFILL_PER_LANGUAGE_LIMIT:
                    break
                if limit > 0 and stats.seen_count >= limit:
                    stop_processing = True
                    break

            candidate_batch.clear()

        if stop_processing:
            break

    if candidate_batch:
        for missing_doc in _filter_missing_embedding_docs(resolved_store, candidate_batch):
            pending_batch.append(missing_doc)

            if len(pending_batch) >= safe_write_batch_size:
                _flush_embedding_backfill_batch(
                    store=resolved_store,
                    embedding_client=resolved_embedding_client,
                    batch_docs=pending_batch,
                    force_reembed=force_reembed,
                    refresh_writes=refresh_writes,
                    write_batch_size=safe_write_batch_size,
                    stats=stats,
                )
                pending_batch.clear()
        candidate_batch.clear()

    if pending_batch:
        _flush_embedding_backfill_batch(
            store=resolved_store,
            embedding_client=resolved_embedding_client,
            batch_docs=pending_batch,
            force_reembed=force_reembed,
            refresh_writes=refresh_writes,
            write_batch_size=safe_write_batch_size,
            stats=stats,
        )

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill raw/anonymized embeddings for existing code_chunk_index documents."
    )
    parser.add_argument(
        "--repo-id",
        default="",
        help="Optional repo_id filter. When omitted, scans the whole code_chunk_index.",
    )
    parser.add_argument(
        "--force-reembed",
        action="store_true",
        help="Recompute embeddings even if raw/anonymized embedding fields already exist.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh the OpenSearch index after each bulk update batch.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stats = backfill_code_chunk_embeddings(
        repo_id=args.repo_id.strip() or None,
        scan_size=EMBEDDING_BACKFILL_SCAN_SIZE,
        write_batch_size=EMBEDDING_BACKFILL_WRITE_BATCH_SIZE,
        limit=EMBEDDING_BACKFILL_RUN_LIMIT,
        force_reembed=args.force_reembed,
        refresh_writes=args.refresh,
    )
    print(json.dumps(stats.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
