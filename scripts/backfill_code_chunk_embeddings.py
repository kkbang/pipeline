#!/usr/bin/env python3
import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
from worker.storage.opensearch_store import OpenSearchStore


CODE_CHUNK_INDEX = CODE_CHUNK_INDEX_ALIAS
EMBEDDING_BACKFILL_SOURCE_FIELDS = [
    "raw_code",
    "raw_hash",
    "raw_embedding",
    "anonymized_code",
    "anonymized_hash",
    "anonymized_embedding",
]
EMBEDDING_BACKFILL_SCAN_SIZE = 200
EMBEDDING_BACKFILL_WRITE_BATCH_SIZE = 100
# Keep one invocation bounded to a single fixed-size slice.
EMBEDDING_BACKFILL_RUN_LIMIT = 200


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


def _missing_field_clause(field_name: str) -> dict[str, Any]:
    return {
        "bool": {
            "must_not": [
                {"exists": {"field": field_name}},
            ]
        }
    }


def _build_embedding_backfill_query(
    *,
    repo_id: str | None = None,
    force_reembed: bool = False,
) -> dict[str, Any]:
    must_clauses: list[dict[str, Any]] = [
        {"terms": {"validation_status": list(VALID_CHUNK_VALIDATION_STATUSES)}},
    ]
    if repo_id:
        must_clauses.append(_repo_id_filter(repo_id))

    query: dict[str, Any] = {
        "bool": {
            "must": must_clauses,
        }
    }
    if not force_reembed:
        query["bool"]["should"] = [
            _missing_field_clause("raw_embedding"),
            _missing_field_clause("anonymized_embedding"),
        ]
        query["bool"]["minimum_should_match"] = 1
    return query


def _prepare_enrichment_source(
    source: dict[str, Any],
    *,
    force_reembed: bool,
) -> dict[str, Any]:
    prepared = {
        "raw_code": source.get("raw_code"),
        "raw_hash": source.get("raw_hash"),
        "anonymized_code": source.get("anonymized_code"),
        "anonymized_hash": source.get("anonymized_hash"),
    }
    if not force_reembed:
        if "raw_embedding" in source:
            prepared["raw_embedding"] = source["raw_embedding"]
        if "anonymized_embedding" in source:
            prepared["anonymized_embedding"] = source["anonymized_embedding"]
    return prepared


def _flush_embedding_backfill_batch(
    *,
    store: OpenSearchStore,
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

    update_docs: list[tuple[str, dict[str, Any]]] = []
    for (doc_id, original_source), (_same_doc_id, enriched_source) in zip(
        batch_docs,
        docs_for_enrich,
        strict=True,
    ):
        update_source: dict[str, Any] = {}

        raw_embedding = enriched_source.get("raw_embedding")
        if raw_embedding is not None:
            if force_reembed or "raw_embedding" not in original_source:
                update_source["raw_embedding"] = raw_embedding
                stats.raw_embedding_written_count += 1

        anonymized_embedding = enriched_source.get("anonymized_embedding")
        if anonymized_embedding is not None:
            if force_reembed or "anonymized_embedding" not in original_source:
                update_source["anonymized_embedding"] = anonymized_embedding
                stats.anonymized_embedding_written_count += 1

        if update_source:
            update_docs.append((doc_id, update_source))
        else:
            stats.skipped_noop_count += 1

    if not update_docs:
        return

    store.bulk_upsert_documents(
        collection_name=CODE_CHUNK_INDEX,
        documents=update_docs,
        refresh=refresh_writes,
        chunk_size=max(1, write_batch_size),
    )
    stats.updated_doc_count += len(update_docs)


def backfill_code_chunk_embeddings(
    *,
    store: OpenSearchStore | None = None,
    embedding_client: CodeChunkEmbeddingClient | None = None,
    repo_id: str | None = None,
    scan_size: int = 500,
    write_batch_size: int | None = None,
    limit: int = 0,
    force_reembed: bool = False,
    refresh_writes: bool = False,
) -> EmbeddingBackfillStats:
    resolved_store = store or OpenSearchStore()
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
    query = _build_embedding_backfill_query(
        repo_id=repo_id,
        force_reembed=force_reembed,
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

        pending_batch.append((doc_id, dict(hit.get("_source", {}))))
        stats.seen_count += 1

        if limit > 0 and stats.seen_count >= limit:
            break

        if len(pending_batch) < safe_write_batch_size:
            continue

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
